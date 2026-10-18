package logservice

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testToken = "test-only-token-0001"

type memoryStore struct {
	mu     sync.Mutex
	rows   []Event
	search Search
	fail   bool
}

func (s *memoryStore) Insert(ctx context.Context, rows []Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("unavailable")
	}
	s.rows = append(s.rows, rows...)
	return nil
}
func (s *memoryStore) Search(ctx context.Context, q Search) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.search = q
	if s.fail {
		return nil, errors.New("unavailable")
	}
	out := []Event{}
	for _, e := range s.rows {
		if e.Tenant == q.Tenant && e.Dataset == q.Dataset {
			out = append(out, e)
		}
	}
	return out, nil
}
func (s *memoryStore) Ping(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("unavailable")
	}
	return nil
}

func app(t *testing.T) (*Server, *memoryStore) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := &memoryStore{}
	s, err := NewServer(ctx, m, map[string]Grant{testToken: {Tenant: "tenant-a", Datasets: []string{"logs"}}, "test-only-token-0002": {Tenant: "tenant-b", Datasets: []string{"logs"}}}, bytes.Repeat([]byte{'k'}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return s, m
}
func request(s http.Handler, path, scheme, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("Authorization", scheme+" "+testToken)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func TestIngestionAndIsolation(t *testing.T) {
	s, m := app(t)
	for _, tc := range []struct {
		path, scheme, body string
		status             int
	}{
		{"/v1/logs", "Bearer", `{"dataset":"logs","events":[{"message":"native"}]}`, 200},
		{"/services/collector/event", "Splunk", `{"event":"hec","index":"logs"}{"event":{"message":"object"},"fields":{"a":1}}`, 200},
		{"/v1/logs", "Bearer", `{"dataset":"other","events":[{"message":"no"}]}`, 400},
		{"/v1/logs", "Bearer", `{"dataset":"logs","events":[{"message":"no","tenant_id":"tenant-b"}]}`, 400},
		{"/v1/logs", "Bearer", `{"dataset":"logs","events":[{"message":"no","@timestamp":"yesterday"}]}`, 400},
		{"/v1/logs", "Bearer", `{"dataset":"logs","events":[{"message":"no"}],"unknown":1}`, 400},
		{"/services/collector/event", "Splunk", `{"event":"would-be-valid"}{"event":7}`, 400},
		{"/services/collector/raw", "Splunk", "raw", 501},
		{"/services/collector/ack", "Splunk", `{"acks":[1]}`, 501},
		{"/v1/logs", "Splunk", `{}`, 401},
	} {
		w := request(s, tc.path, tc.scheme, tc.body)
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rows) != 3 {
		t.Fatalf("invalid HEC request must not commit a prefix: rows=%d", len(m.rows))
	}
	for _, e := range m.rows {
		if e.Tenant != "tenant-a" {
			t.Fatal("tenant crossed")
		}
	}
}

func TestBulkPartialFailureAndFraming(t *testing.T) {
	s, m := app(t)
	body := "{\"index\":{}}\n{\"message\":\"ok\"}\n{\"create\":{\"_id\":\"1\"}}\n{\"message\":\"unsupported\"}\n{\"delete\":{\"_id\":\"2\"}}\n{\"index\":{\"_index\":\"forbidden\"}}\n{\"message\":\"no\"}\n"
	w := request(s, "/logs/_bulk", "ApiKey", body)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var out struct {
		Errors bool                    `json:"errors"`
		Items  []map[string]bulkResult `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Errors || len(out.Items) != 4 || out.Items[0]["index"].Status != 201 || out.Items[1]["create"].Status != 400 || out.Items[2]["delete"].Status != 400 {
		t.Fatal(w.Body.String())
	}
	m.mu.Lock()
	n := len(m.rows)
	m.mu.Unlock()
	if n != 1 {
		t.Fatalf("committed %d", n)
	}
	for _, bad := range []string{"{\"index\":{}}\n", "{\"index\":null}\n{}\n", "{\"index\":{}}\n{}", "{\"index\":{}}\n{}\n{\"index\":{}}\n"} {
		w := request(s, "/_bulk", "ApiKey", bad)
		if w.Code != 400 {
			t.Fatalf("bad framing accepted %s", bad)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rows) != 1 {
		t.Fatal("malformed frame committed a prefix")
	}
}

func TestBulkIgnoresExternalIDWithoutOverwriting(t *testing.T) {
	s, m := app(t)
	ids := make(map[string]bool)
	for _, path := range []string{"/_bulk", "/logs/_bulk"} {
		body := "{\"index\":{\"_index\":\"logs\",\"_id\":\"repeated-id\"}}\n{\"message\":\"first\"}\n{\"index\":{\"_index\":\"logs\",\"_id\":\"repeated-id\"}}\n{\"message\":\"second\"}\n"
		w := request(s, path, "ApiKey", body)
		var out struct {
			Errors bool                    `json:"errors"`
			Items  []map[string]bulkResult `json:"items"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || out.Errors || len(out.Items) != 2 {
			t.Fatal(w.Body.String())
		}
		for _, item := range out.Items {
			v := item["index"]
			if v.Status != 201 || v.ID == "" || v.ID == "repeated-id" || ids[v.ID] {
				t.Fatalf("expected fresh server ID: %#v", v)
			}
			ids[v.ID] = true
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rows) != 4 {
		t.Fatalf("repeated external IDs must append four rows, got %d", len(m.rows))
	}
	counts := map[string]int{}
	for _, row := range m.rows {
		if !ids[row.EventID] || strings.Contains(row.Attributes, "repeated-id") {
			t.Fatal("external ID was retained or substituted for generated ID")
		}
		counts[row.Message]++
	}
	if counts["first"] != 2 || counts["second"] != 2 {
		t.Fatalf("events overwritten: %#v", counts)
	}
}

func TestBackendFailureNeverAcknowledged(t *testing.T) {
	s, m := app(t)
	m.fail = true
	if w := request(s, "/v1/logs", "Bearer", `{"dataset":"logs","events":[{"message":"x"}]}`); w.Code != 503 {
		t.Fatal(w.Body.String())
	}
	w := request(s, "/_bulk", "ApiKey", "{\"index\":{}}\n{\"message\":\"x\"}\n")
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if out["errors"] != true || !strings.Contains(w.Body.String(), `"status":503`) {
		t.Fatal(w.Body.String())
	}
}

func TestSearchCursorBoundToTenantAndQuery(t *testing.T) {
	s, m := app(t)
	m.rows = []Event{{Tenant: "tenant-a", Dataset: "logs", EventTime: utc(time.Now()), EventID: "a"}, {Tenant: "tenant-a", Dataset: "logs", EventTime: utc(time.Now()), EventID: "b"}}
	body := `{"dataset":"logs","from":"2026-10-17T00:00:00Z","to":"2026-10-18T00:00:00Z","limit":1}`
	w := request(s, "/v1/logs/search", "Bearer", body)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var out struct {
		Cursor string `json:"next_cursor"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if out.Cursor == "" {
		t.Fatal("missing cursor")
	}
	var in map[string]any
	_ = json.Unmarshal([]byte(body), &in)
	in["cursor"] = out.Cursor
	b, _ := json.Marshal(in)
	if w = request(s, "/v1/logs/search", "Bearer", string(b)); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	m.mu.Lock()
	if m.search.AfterID != "a" {
		t.Fatal("cursor not forwarded")
	}
	m.mu.Unlock()
	in["service"] = "changed"
	b, _ = json.Marshal(in)
	if w = request(s, "/v1/logs/search", "Bearer", string(b)); w.Code != 400 {
		t.Fatal("cursor accepted changed query")
	}
	delete(in, "service")
	b, _ = json.Marshal(in)
	r := httptest.NewRequest("POST", "/v1/logs/search", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer test-only-token-0002")
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("cursor crossed tenant")
	}
}

func TestGzipLimitAndAdmission(t *testing.T) {
	s, _ := app(t)
	var buf bytes.Buffer
	z := gzip.NewWriter(&buf)
	_, _ = z.Write(bytes.Repeat([]byte{'a'}, maxBody+1))
	_ = z.Close()
	r := httptest.NewRequest("POST", "/v1/logs", &buf)
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatal("gzip expansion was not bounded")
	}
	for i := 0; i < cap(s.ingress); i++ {
		s.ingress <- struct{}{}
	}
	if w = request(s, "/v1/logs", "Bearer", `{}`); w.Code != 429 {
		t.Fatal("admission was not bounded")
	}
}

func TestStorageParametersAndNanosecondPagination(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "lms" || pass != "secret-for-test" {
			t.Error("missing authentication")
		}
		q := r.URL.Query()
		if q.Get("param_from") != "2026-10-17 00:00:00.000000000" {
			t.Error("typed UTC timestamp not canonicalized")
		}
		if strings.Contains(q.Get("query"), "DROP") || q.Get("param_text") != "'; DROP TABLE lms.logs;--" {
			t.Error("query value was not parameterized")
		}
		_, _ = io.WriteString(w, `{"event_time":"2026-10-18 00:00:00.123456789","ingest_time":"2026-10-18 00:00:00.987654321","event_id":"id"}`+"\n")
	}))
	defer backend.Close()
	store, err := NewHTTPStore(backend.URL, "lms", "secret-for-test")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Search(context.Background(), Search{Tenant: "t", Dataset: "d", From: "2026-10-17T00:00:00Z", To: "2026-10-18T00:00:00Z", Contains: "'; DROP TABLE lms.logs;--", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].EventTime != "2026-10-18T00:00:00.123456789Z" {
		t.Fatalf("precision lost: %#v", rows)
	}
}

func TestBatcherCoalescesAndConfirms(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &memoryStore{}
	b := NewBatcher(ctx, m)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Write(ctx, []Event{{Message: "x"}}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rows) != 8 {
		t.Fatal("events lost")
	}
}
