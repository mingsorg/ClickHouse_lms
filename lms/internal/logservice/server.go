package logservice

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxBody = 4 * 1024 * 1024

type Server struct {
	store   Store
	batch   *Batcher
	tokens  map[string]Grant
	key     []byte
	ingress chan struct{}
	queries chan struct{}
	mux     *http.ServeMux
}

func NewServer(ctx context.Context, store Store, tokens map[string]Grant, key []byte) (*Server, error) {
	if len(tokens) == 0 || len(key) < 32 {
		return nil, errors.New("tokens and a cursor key of at least 32 bytes are required")
	}
	for token, g := range tokens {
		if len(token) < 16 || g.Tenant == "" || len(g.Datasets) == 0 {
			return nil, errors.New("each token needs at least 16 bytes, a tenant and datasets")
		}
		if g.RawMode != "" && g.RawMode != "event" && g.RawMode != "lines" {
			return nil, errors.New("raw_mode must be event or lines")
		}
		for _, d := range g.Datasets {
			if d == "" || len(d) > 128 || strings.ContainsAny(d, "/\\\n\r") {
				return nil, errors.New("invalid dataset")
			}
		}
	}
	s := &Server{store: store, batch: NewBatcher(ctx, store), tokens: tokens, key: key, ingress: make(chan struct{}, 8), queries: make(chan struct{}, 4), mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "alive"}) })
	s.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if s.store.Ping(ctx) != nil {
			fail(w, 503, "storage unavailable")
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	s.mux.HandleFunc("POST /v1/logs", s.protect("Bearer", false, s.native))
	s.mux.HandleFunc("POST /services/collector", s.protect("Splunk", false, s.hec))
	s.mux.HandleFunc("POST /services/collector/event", s.protect("Splunk", false, s.hec))
	s.mux.HandleFunc("POST /services/collector/raw", s.protect("Splunk", false, s.hecRaw))
	s.mux.HandleFunc("POST /services/collector/ack", s.protect("Splunk", false, func(w http.ResponseWriter, r *http.Request, g Grant) { fail(w, 501, "HEC ACK is not supported") }))
	s.mux.HandleFunc("POST /_bulk", s.protect("ApiKey", false, s.bulk))
	s.mux.HandleFunc("POST /{dataset}/_bulk", s.protect("ApiKey", false, s.bulk))
	s.mux.HandleFunc("POST /v1/logs/search", s.protect("Bearer", true, s.search))
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

type handler func(http.ResponseWriter, *http.Request, Grant)

func (s *Server) protect(scheme string, query bool, next handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
		if len(parts) != 2 || parts[0] != scheme {
			fail(w, 401, "authentication required")
			return
		}
		g, ok := s.tokens[parts[1]]
		if !ok {
			fail(w, 401, "invalid credential")
			return
		}
		slots := s.ingress
		if query {
			slots = s.queries
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			w.Header().Set("Retry-After", "1")
			fail(w, 429, "capacity is full")
			return
		}
		next(w, r, g)
	}
}

func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, msg string) {
	respond(w, status, map[string]any{"error": msg})
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	defer r.Body.Close()
	var reader io.Reader = r.Body
	switch r.Header.Get("Content-Encoding") {
	case "":
	case "gzip":
		z, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer z.Close()
		reader = z
	default:
		return nil, errors.New("unsupported Content-Encoding")
	}
	b, err := io.ReadAll(io.LimitReader(reader, maxBody+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxBody {
		return nil, errors.New("decoded body exceeds 4 MiB")
	}
	return b, nil
}

func (s *Server) commit(w http.ResponseWriter, r *http.Request, rows []Event) bool {
	if len(rows) == 0 {
		fail(w, 400, "empty batch")
		return false
	}
	if err := s.batch.Write(r.Context(), rows); err != nil {
		status := 503
		if errors.Is(err, ErrBusy) {
			status = 429
		}
		fail(w, status, "write not confirmed; retry may duplicate events")
		return false
	}
	return true
}

func (s *Server) native(w http.ResponseWriter, r *http.Request, g Grant) {
	if len(r.URL.Query()) != 0 {
		fail(w, 400, "query parameters are not supported")
		return
	}
	b, err := readBody(w, r)
	if err != nil {
		fail(w, 413, "invalid or oversized body")
		return
	}
	var in struct {
		Dataset string           `json:"dataset"`
		Events  []map[string]any `json:"events"`
	}
	if strictJSON(b, &in) != nil || len(in.Events) == 0 || len(in.Events) > maxEvents {
		fail(w, 400, "invalid native batch")
		return
	}
	rows := make([]Event, 0, len(in.Events))
	now := time.Now()
	for _, m := range in.Events {
		e, err := normalize(m, g, in.Dataset, "native", now)
		if err != nil {
			fail(w, 400, err.Error())
			return
		}
		rows = append(rows, e)
	}
	if s.commit(w, r, rows) {
		respond(w, 200, map[string]any{"committed": len(rows), "acknowledgement": "clickhouse_insert_completed", "delivery": "at_least_once"})
	}
}

func (s *Server) hec(w http.ResponseWriter, r *http.Request, g Grant) {
	if len(r.URL.Query()) != 0 || r.Header.Get("X-Splunk-Request-Channel") != "" {
		fail(w, 501, "HEC channels and ACK are not supported")
		return
	}
	b, err := readBody(w, r)
	if err != nil {
		fail(w, 413, "invalid or oversized body")
		return
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	rows := make([]Event, 0)
	now := time.Now()
	for {
		var in map[string]any
		err := d.Decode(&in)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || in == nil {
			fail(w, 400, "invalid HEC event stream")
			return
		}
		if len(rows) >= maxEvents {
			fail(w, 413, "too many events")
			return
		}
		for k := range in {
			switch k {
			case "event", "time", "host", "source", "sourcetype", "index", "fields":
			default:
				fail(w, 400, "unsupported HEC field")
				return
			}
		}
		m := map[string]any{}
		switch event := in["event"].(type) {
		case string:
			m["message"] = event
		case map[string]any:
			for k, v := range event {
				m[k] = v
			}
			if _, ok := m["message"]; !ok {
				encoded, _ := json.Marshal(event)
				m["message"] = string(encoded)
			}
		default:
			fail(w, 400, "event must be a string or object")
			return
		}
		if fields, exists := in["fields"]; exists {
			obj, ok := fields.(map[string]any)
			if !ok {
				fail(w, 400, "fields must be an object")
				return
			}
			// Keep HEC fields namespaced rather than permitting tenant overrides.
			m["hec_fields"] = obj
		}
		for from, to := range map[string]string{"host": "host", "source": "source_id", "sourcetype": "source_type"} {
			if v, ok := in[from]; ok {
				if _, ok := v.(string); !ok {
					fail(w, 400, from+" must be text")
					return
				}
				m[to] = v
			}
		}
		if v, exists := in["time"]; exists {
			stamp, err := hecTime(v)
			if err != nil {
				fail(w, 400, err.Error())
				return
			}
			m["@timestamp"] = stamp
		}
		dataset := g.Datasets[0]
		if v, exists := in["index"]; exists {
			var ok bool
			dataset, ok = v.(string)
			if !ok {
				fail(w, 400, "index must be text")
				return
			}
		}
		e, err := normalize(m, g, dataset, "hec", now)
		if err != nil {
			fail(w, 400, err.Error())
			return
		}
		rows = append(rows, e)
	}
	if s.commit(w, r, rows) {
		respond(w, 200, map[string]any{"text": "Success", "code": 0})
	}
}

type bulkResult struct {
	Index  string `json:"_index,omitempty"`
	ID     string `json:"_id,omitempty"`
	Status int    `json:"status"`
	Result string `json:"result,omitempty"`
	Error  any    `json:"error,omitempty"`
}

func bulkError(index string, status int, msg string) bulkResult {
	return bulkResult{Index: index, Status: status, Error: map[string]string{"type": "lms_ingest_error", "reason": msg}}
}

func (s *Server) bulk(w http.ResponseWriter, r *http.Request, g Grant) {
	if len(r.URL.Query()) != 0 {
		fail(w, 400, "Bulk query parameters are not supported")
		return
	}
	b, err := readBody(w, r)
	if err != nil {
		fail(w, 413, "invalid or oversized body")
		return
	}
	if len(b) == 0 || b[len(b)-1] != '\n' {
		fail(w, 400, "Bulk body requires a final newline")
		return
	}
	lines := bytes.Split(b[:len(b)-1], []byte{'\n'})
	items := make([]map[string]bulkResult, 0)
	rows := make([]Event, 0)
	positions := make([]int, 0)
	now := time.Now()
	for i := 0; i < len(lines); {
		var action map[string]map[string]any
		if strictJSON(lines[i], &action) != nil || len(action) != 1 {
			fail(w, 400, "invalid Bulk action framing")
			return
		}
		i++
		var op string
		var meta map[string]any
		for op, meta = range action {
		}
		if meta == nil {
			fail(w, 400, "Bulk action metadata must be an object")
			return
		}
		if op != "index" && op != "create" && op != "update" && op != "delete" {
			fail(w, 400, "unknown Bulk action")
			return
		}
		var source []byte
		if op != "delete" {
			if i >= len(lines) {
				fail(w, 400, "Bulk action missing source")
				return
			}
			source = lines[i]
			i++
		}
		if len(items) >= maxEvents {
			fail(w, 413, "too many Bulk actions")
			return
		}
		dataset := r.PathValue("dataset")
		if dataset == "" {
			dataset = g.Datasets[0]
		}
		if v, exists := meta["_index"]; exists {
			var ok bool
			dataset, ok = v.(string)
			if !ok {
				fail(w, 400, "_index must be text")
				return
			}
		}
		pos := len(items)
		items = append(items, map[string]bulkResult{})
		itemErr := ""
		if op != "index" {
			itemErr = "only append-only index is supported"
		}
		for k := range meta {
			if k != "_index" && k != "_id" {
				itemErr = "unsupported action metadata"
			}
		}
		// Ignore the caller's _id; every index action appends a fresh event.
		if itemErr != "" {
			items[pos][op] = bulkError(dataset, 400, itemErr)
			continue
		}
		var m map[string]any
		if strictJSON(source, &m) != nil || m == nil {
			items[pos][op] = bulkError(dataset, 400, "invalid source object")
			continue
		}
		e, err := normalize(m, g, dataset, "bulk", now)
		if err != nil {
			items[pos][op] = bulkError(dataset, 400, err.Error())
			continue
		}
		rows = append(rows, e)
		positions = append(positions, pos)
		items[pos][op] = bulkResult{Index: dataset, ID: e.EventID, Status: 201, Result: "created"}
	}
	if len(rows) > 0 {
		if err := s.batch.Write(r.Context(), rows); err != nil {
			for _, pos := range positions {
				items[pos]["index"] = bulkError(items[pos]["index"].Index, 503, "write not confirmed; retry may duplicate events")
			}
		}
	}
	hasErrors := false
	for _, item := range items {
		for _, v := range item {
			if v.Status >= 300 {
				hasErrors = true
			}
		}
	}
	respond(w, 200, map[string]any{"took": time.Since(now).Milliseconds(), "errors": hasErrors, "items": items})
}

type searchRequest struct {
	Dataset  string `json:"dataset"`
	From     string `json:"from"`
	To       string `json:"to"`
	Service  string `json:"service,omitempty"`
	TraceID  string `json:"trace_id,omitempty"`
	Contains string `json:"contains,omitempty"`
	Limit    int    `json:"limit"`
	Cursor   string `json:"cursor,omitempty"`
}

type cursor struct {
	Binding string `json:"binding"`
	Time    string `json:"time"`
	ID      string `json:"id"`
	Expires int64  `json:"expires"`
}

func (s *Server) sign(c cursor) string {
	b, _ := json.Marshal(c)
	mac := hmac.New(sha256.New, s.key)
	mac.Write(b)
	return base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Server) decodeCursor(encoded, binding string) (cursor, error) {
	var c cursor
	parts := strings.Split(encoded, ".")
	if len(parts) != 2 {
		return c, errors.New("invalid cursor")
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return c, err
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return c, err
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write(b)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return c, errors.New("invalid cursor signature")
	}
	if strictJSON(b, &c) != nil || c.Binding != binding || c.Expires <= time.Now().Unix() {
		return c, errors.New("cursor expired or query changed")
	}
	return c, nil
}

func (s *Server) search(w http.ResponseWriter, r *http.Request, g Grant) {
	if len(r.URL.Query()) != 0 {
		fail(w, 400, "query parameters are not supported")
		return
	}
	b, err := readBody(w, r)
	if err != nil {
		fail(w, 413, "invalid or oversized body")
		return
	}
	var in searchRequest
	if strictJSON(b, &in) != nil {
		fail(w, 400, "invalid search request")
		return
	}
	from, e1 := time.Parse(time.RFC3339Nano, in.From)
	to, e2 := time.Parse(time.RFC3339Nano, in.To)
	if e1 != nil || e2 != nil || !to.After(from) || to.Sub(from) > 10*24*time.Hour || in.Limit < 1 || in.Limit > 200 || len(in.Contains) > 1024 || len(in.Service) > 1024 || len(in.TraceID) > 1024 || !g.Allows(in.Dataset) {
		fail(w, 400, "invalid dataset, range (max 10 days), limit (1..200), or text")
		return
	}
	in.From, in.To = utc(from), utc(to)
	encoded := in.Cursor
	in.Cursor = ""
	bind, _ := json.Marshal(struct {
		Tenant  string
		Request searchRequest
	}{g.Tenant, in})
	hash := sha256.Sum256(bind)
	binding := base64.RawURLEncoding.EncodeToString(hash[:])
	q := Search{Tenant: g.Tenant, Dataset: in.Dataset, From: in.From, To: in.To, Service: in.Service, TraceID: in.TraceID, Contains: in.Contains, Limit: in.Limit + 1}
	if encoded != "" {
		c, err := s.decodeCursor(encoded, binding)
		if err != nil {
			fail(w, 400, "invalid cursor")
			return
		}
		q.AfterTime, q.AfterID = c.Time, c.ID
	}
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	rows, err := s.store.Search(ctx, q)
	if err != nil {
		fail(w, 503, "query failed or exceeded its resource budget")
		return
	}
	next := ""
	if len(rows) > in.Limit {
		rows = rows[:in.Limit]
		last := rows[len(rows)-1]
		next = s.sign(cursor{Binding: binding, Time: last.EventTime, ID: last.EventID, Expires: time.Now().Add(15 * time.Minute).Unix()})
	}
	respond(w, 200, map[string]any{"events": rows, "next_cursor": next})
}
