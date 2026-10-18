package logservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type Search struct {
	Tenant, Dataset, From, To, Service, TraceID, Contains string
	AfterTime, AfterID                                    string
	Limit                                                 int
}

type Store interface {
	Insert(context.Context, []Event) error
	Search(context.Context, Search) ([]Event, error)
	Ping(context.Context) error
}

// HTTPStore uses the stable HTTP interface for the first correctness baseline.
// A Native transport can later be compared without changing the adapters.
type HTTPStore struct {
	Endpoint, User, Password string
	Client                   *http.Client
}

func (s *HTTPStore) call(ctx context.Context, sql string, params url.Values, payload []byte) ([]byte, error) {
	u, err := url.Parse(s.Endpoint)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	for k, v := range params {
		q[k] = v
	}
	q.Set("query", sql)
	q.Set("date_time_input_format", "best_effort")
	q.Set("wait_end_of_query", "1")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(s.User, s.Password)
	req.Header.Set("Content-Type", "application/octet-stream")
	res, err := s.Client.Do(req)
	if err != nil {
		return nil, errors.New("storage request failed; commit status may be unknown")
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 16*1024*1024+1))
	if err != nil || len(b) > 16*1024*1024 {
		return nil, errors.New("storage response incomplete or too large")
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("storage returned HTTP %d", res.StatusCode)
	}
	return b, nil
}

func (s *HTTPStore) Insert(ctx context.Context, rows []Event) error {
	// This matches the complete table sorting key, not only event time.
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Tenant != b.Tenant {
			return a.Tenant < b.Tenant
		}
		if a.Dataset != b.Dataset {
			return a.Dataset < b.Dataset
		}
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		if a.EventTime != b.EventTime {
			return a.EventTime < b.EventTime
		}
		return a.EventID < b.EventID
	})
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	_, err := s.call(ctx, "INSERT INTO lms.logs FORMAT JSONEachRow", url.Values{"async_insert": {"0"}}, buf.Bytes())
	return err
}

func (s *HTTPStore) Ping(ctx context.Context) error {
	_, err := s.call(ctx, "SELECT 1 FROM lms.logs LIMIT 0", nil, nil)
	return err
}

func (s *HTTPStore) Search(ctx context.Context, in Search) ([]Event, error) {
	// Typed DateTime64 parameters require a timezone-free literal; the type is UTC.
	from, err := sqlTime(in.From)
	if err != nil {
		return nil, err
	}
	to, err := sqlTime(in.To)
	if err != nil {
		return nil, err
	}
	q := `SELECT tenant_id,dataset,event_time,ingest_time,event_id,source_id,host,service,severity,trace_id,message,attributes,protocol FROM lms.logs PREWHERE tenant_id={tenant:String} AND dataset={dataset:String} AND event_time >= {from:DateTime64(9,'UTC')} AND event_time < {to:DateTime64(9,'UTC')}`
	p := url.Values{"param_tenant": {in.Tenant}, "param_dataset": {in.Dataset}, "param_from": {from}, "param_to": {to}, "param_limit": {fmt.Sprint(in.Limit)}}
	if in.Service != "" {
		q += " AND service={service:String}"
		p.Set("param_service", in.Service)
	}
	if in.TraceID != "" {
		q += " AND trace_id={trace:String}"
		p.Set("param_trace", in.TraceID)
	}
	if in.Contains != "" {
		q += " WHERE position(message,{text:String}) > 0"
		p.Set("param_text", in.Contains)
	}
	if in.AfterTime != "" {
		after, err := sqlTime(in.AfterTime)
		if err != nil {
			return nil, err
		}
		if in.Contains == "" {
			q += " WHERE "
		} else {
			q += " AND "
		}
		q += "(event_time,event_id) < ({after_time:DateTime64(9,'UTC')},{after_id:String})"
		p.Set("param_after_time", after)
		p.Set("param_after_id", in.AfterID)
	}
	q += " ORDER BY event_time DESC,event_id DESC LIMIT {limit:UInt32} FORMAT JSONEachRow"
	p.Set("max_execution_time", "5")
	p.Set("max_memory_usage", "268435456")
	p.Set("max_threads", "2")
	p.Set("date_time_output_format", "simple")
	p.Set("max_bytes_to_read", "536870912")
	p.Set("read_overflow_mode", "throw")
	p.Set("max_result_bytes", "8388608")
	p.Set("result_overflow_mode", "throw")
	b, err := s.call(ctx, q, p, nil)
	if err != nil {
		return nil, err
	}
	rows := make([]Event, 0)
	d := json.NewDecoder(strings.NewReader(string(b)))
	for {
		var row Event
		err := d.Decode(&row)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		for _, stamp := range []*string{&row.EventTime, &row.IngestTime} {
			t, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", *stamp, time.UTC)
			if err != nil {
				return nil, err
			}
			*stamp = utc(t)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func sqlTime(stamp string) (string, error) {
	t, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return "", err
	}
	return t.UTC().Format("2006-01-02 15:04:05.000000000"), nil
}

func NewHTTPStore(endpoint, user, password string) (*HTTPStore, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid storage URL")
	}
	return &HTTPStore{Endpoint: endpoint, User: user, Password: password, Client: &http.Client{Timeout: 15 * time.Second}}, nil
}
