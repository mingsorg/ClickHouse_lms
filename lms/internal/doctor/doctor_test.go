package doctor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func fakeNode(t *testing.T, brokenDisk bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "observer" || p != "synthetic-test-password" {
			t.Error("missing observer credentials")
		}
		sql := r.URL.Query().Get("query")
		if !strings.HasPrefix(sql, "SELECT ") || strings.Contains(sql, ";") {
			t.Error("non-read-only query")
		}
		var rows any
		switch {
		case strings.Contains(sql, "version()"):
			rows = []any{map[string]any{"version": "26.9.21.9", "uptime_seconds": 100}}
		case strings.Contains(sql, "system.tables"):
			rows = []any{map[string]any{"engine": "ReplicatedMergeTree"}}
		case strings.Contains(sql, "system.disks"):
			if brokenDisk {
				rows = []any{map[string]any{"name": "default", "total_space": 1000, "free_space": 500, "is_read_only": 0}}
			} else {
				rows = []any{map[string]any{"name": "default", "total_space": "1000", "free_space": "500", "unreserved_space": "500", "is_read_only": 0}}
			}
		case strings.Contains(sql, "system.parts"):
			rows = []any{map[string]any{"partition_id": "day", "active_parts": "10", "rows": "100", "bytes_on_disk": "200"}}
		case strings.Contains(sql, "system.replicas"):
			rows = []any{map[string]any{"is_readonly": 0, "is_session_expired": 0, "queue_size": 0, "absolute_delay": 0, "total_replicas": 2, "active_replicas": 2}}
		case strings.Contains(sql, "system.merges"):
			rows = []any{map[string]any{"running": 0, "memory_bytes": "0"}}
		case strings.Contains(sql, "system.mutations"):
			rows = []any{map[string]any{"pending": 0, "failed": 0}}
		default:
			t.Error("unexpected probe")
		}
		for _, row := range rows.([]any) {
			_ = json.NewEncoder(w).Encode(row)
		}
	}))
}

func TestDoctorDoesNotTurnMissingEvidenceIntoHealth(t *testing.T) {
	t.Setenv("LMS_TEST_PASSWORD", "synthetic-test-password")
	for _, broken := range []bool{false, true} {
		server := fakeNode(t, broken)
		n := Node{Name: "node", URL: server.URL, User: "observer", PasswordEnv: "LMS_TEST_PASSWORD", ExpectedReplicas: 2}
		report := Run(context.Background(), server.Client(), Config{Nodes: []Node{n}})
		server.Close()
		expected := "PASS"
		if broken {
			expected = "FAIL"
		}
		if report.Status != expected || report.ProductionAcceptance != "NOT RUN" {
			t.Fatalf("invalid evidence evaluation: %#v", report)
		}
	}
}

func TestDoctorPartialNodeFailureFailsOverall(t *testing.T) {
	t.Setenv("LMS_TEST_PASSWORD", "synthetic-test-password")
	server := fakeNode(t, false)
	defer server.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer bad.Close()
	nodes := []Node{{Name: "healthy", URL: server.URL, User: "observer", PasswordEnv: "LMS_TEST_PASSWORD", ExpectedReplicas: 2}, {Name: "failed", URL: bad.URL, User: "observer", PasswordEnv: "LMS_TEST_PASSWORD", ExpectedReplicas: 2}}
	r := Run(context.Background(), server.Client(), Config{Nodes: nodes})
	if r.Status != "FAIL" || len(r.Nodes) != 2 || r.Nodes[0].Status != "PASS" || r.Nodes[1].Status != "FAIL" {
		t.Fatalf("partial failure was hidden: %#v", r)
	}
	for _, check := range r.Nodes[1].Checks {
		if check.Status != "FAIL" || len(check.Evidence) != 0 {
			t.Fatal("failed evidence was fabricated")
		}
	}
}

func TestDoctorRequiresExplicitInventory(t *testing.T) {
	for _, input := range []string{`{"nodes":[]}`, `{"nodes":[{"name":"a","url":"http://node","user":"u","password_env":"P"}]}`, `{"nodes":[{"name":"a","url":"http://user:password@node","user":"u","password_env":"P","expected_replicas":1}]}`} {
		if _, err := Parse([]byte(input)); err == nil {
			t.Fatal("invalid inventory accepted")
		}
	}
	if _, err := Parse([]byte(`{"nodes":[{"name":"a","url":"http://node","user":"u","password_env":"P","expected_replicas":1}]}`)); err != nil {
		t.Fatal(err)
	}
}
