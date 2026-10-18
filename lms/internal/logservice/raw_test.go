package logservice

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRawPreservesMultilineAndMetadata(t *testing.T) {
	s, m := app(t)
	message := "ERROR request failed\n  at example.go:12\ncaused by timeout\n"
	w := request(s, "/services/collector/raw?index=logs&host=node-1&source=app&sourcetype=stack&time=1792281600&channel=18654c68-b28b-4450-9cf0-6e7645ca60ca", "Splunk", message)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rows) != 1 || m.rows[0].Message != message || m.rows[0].Host != "node-1" || m.rows[0].Source != "app" || m.rows[0].Protocol != "hec_raw" || m.rows[0].EventTime != "2026-10-18T00:00:00.000000000Z" {
		t.Fatalf("raw changed boundaries or metadata: %#v", m.rows)
	}
	if !strings.Contains(m.rows[0].Attributes, "stack") {
		t.Fatal("sourcetype lost")
	}
}

func TestRawLineModeAndWholeRequestValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &memoryStore{}
	s, err := NewServer(ctx, m, map[string]Grant{testToken: {Tenant: "a", Datasets: []string{"logs"}, RawMode: "lines"}}, bytes.Repeat([]byte{'k'}, 32))
	if err != nil {
		t.Fatal(err)
	}
	w := request(s, "/services/collector/raw", "Splunk", "first\r\n\nsecond\n")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	m.mu.Lock()
	if len(m.rows) != 2 || m.rows[0].Message != "first" || m.rows[1].Message != "second" {
		t.Fatal("line splitting is incorrect")
	}
	m.mu.Unlock()
	w = request(s, "/services/collector/raw", "Splunk", "would-be-valid\n"+strings.Repeat("x", 64*1024+1))
	if w.Code < 400 {
		t.Fatal("oversized line accepted")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rows) != 2 {
		t.Fatal("invalid raw request committed a prefix")
	}
}

func TestRawRejectsInvalidMetadataAndSupportsGzip(t *testing.T) {
	s, m := app(t)
	for _, path := range []string{
		"/services/collector/raw?index=forbidden",
		"/services/collector/raw?index=logs&index=other",
		"/services/collector/raw?index=%zz",
		"/services/collector/raw?channel=invalid",
		"/services/collector/raw?time=not-a-number",
	} {
		if w := request(s, path, "Splunk", "test"); w.Code != 400 {
			t.Fatalf("accepted metadata %s: %d", path, w.Code)
		}
	}
	if w := request(s, "/services/collector/raw", "Splunk", string([]byte{0xff})); w.Code != 400 {
		t.Fatal("invalid UTF8 accepted")
	}
	var buf bytes.Buffer
	z := gzip.NewWriter(&buf)
	_, _ = z.Write([]byte("stack\nline"))
	_ = z.Close()
	r := httptest.NewRequest("POST", "/services/collector/raw", &buf)
	r.Header.Set("Authorization", "Splunk "+testToken)
	r.Header.Set("Content-Encoding", "gzip")
	r.Header.Set("X-Splunk-Request-Channel", "18654c68-b28b-4450-9cf0-6e7645ca60ca")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.rows) != 1 || m.rows[0].Message != "stack\nline" {
		t.Fatal("invalid inputs committed or gzip raw changed")
	}
}
