// Package doctor collects bounded, read-only node evidence. It never repairs a cluster.
package doctor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type Node struct {
	Name             string `json:"name"`
	URL              string `json:"url"`
	User             string `json:"user"`
	PasswordEnv      string `json:"password_env"`
	ExpectedReplicas int    `json:"expected_replicas"`
}
type Config struct {
	Nodes []Node `json:"nodes"`
}
type Check struct {
	Name     string           `json:"name"`
	Status   string           `json:"status"`
	Detail   string           `json:"detail"`
	Evidence []map[string]any `json:"evidence,omitempty"`
}
type NodeReport struct {
	Name   string  `json:"name"`
	Status string  `json:"status"`
	Checks []Check `json:"checks"`
}
type Report struct {
	ObservedAt           string       `json:"observed_at"`
	Status               string       `json:"status"`
	Scope                string       `json:"scope"`
	ProductionAcceptance string       `json:"production_acceptance"`
	Nodes                []NodeReport `json:"nodes"`
}

func Parse(data []byte) (Config, error) {
	var c Config
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.New("invalid doctor configuration")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, errors.New("doctor configuration must be one JSON object")
	}
	if len(c.Nodes) < 1 || len(c.Nodes) > 32 {
		return c, errors.New("configure 1..32 nodes")
	}
	names, urls := map[string]bool{}, map[string]bool{}
	for _, n := range c.Nodes {
		u, err := url.Parse(n.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return c, errors.New("invalid node URL")
		}
		canonical := strings.TrimSuffix(n.URL, "/")
		if n.Name == "" || len(n.Name) > 128 || n.User == "" || n.PasswordEnv == "" || n.ExpectedReplicas < 1 || n.ExpectedReplicas > 32 || names[n.Name] || urls[canonical] {
			return c, errors.New("nodes need unique names/URLs, credentials and expected_replicas in 1..32")
		}
		names[n.Name], urls[canonical] = true, true
	}
	return c, nil
}

func worse(a, b string) string {
	rank := map[string]int{"PASS": 0, "WARN": 1, "FAIL": 2}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

func query(ctx context.Context, client *http.Client, n Node, sql string) ([]map[string]any, error) {
	password, exists := os.LookupEnv(n.PasswordEnv)
	if !exists || password == "" {
		return nil, errors.New("credential environment variable is missing")
	}
	u, _ := url.Parse(n.URL)
	q := u.Query()
	q.Set("query", sql+" FORMAT JSONEachRow")
	q.Set("wait_end_of_query", "1")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, errors.New("invalid node request")
	}
	req.SetBasicAuth(n.User, password)
	res, err := client.Do(req)
	if err != nil {
		return nil, errors.New("node request failed or timed out")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("node returned HTTP %d; check connectivity and observer grants", res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, 1024*1024+1))
	if err != nil || len(b) > 1024*1024 {
		return nil, errors.New("node evidence is incomplete or too large")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	rows := []map[string]any{}
	for {
		var row map[string]any
		err := d.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil || row == nil {
			return nil, errors.New("node returned malformed evidence")
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func number(row map[string]any, key string) (float64, error) {
	var n json.Number
	switch value := row[key].(type) {
	case json.Number:
		n = value
	case string:
		n = json.Number(value)
	default:
		return 0, errors.New("missing numeric evidence")
	}
	f, err := n.Float64()
	if err != nil || f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, errors.New("invalid numeric evidence")
	}
	return f, nil
}

type probe struct {
	name, sql string
	assess    func([]map[string]any, Node) (string, string, error)
}

func probes() []probe {
	return []probe{
		{"server", "SELECT version() AS version, uptime() AS uptime_seconds", func(rows []map[string]any, n Node) (string, string, error) {
			if len(rows) != 1 {
				return "", "", errors.New("missing server identity")
			}
			v, ok := rows[0]["version"].(string)
			if !ok || v == "" {
				return "", "", errors.New("missing server version")
			}
			if _, err := number(rows[0], "uptime_seconds"); err != nil {
				return "", "", err
			}
			return "PASS", "server responded; version is evidence, not upgrade approval", nil
		}},
		{"table", "SELECT engine FROM system.tables WHERE database='lms' AND name='logs'", func(rows []map[string]any, n Node) (string, string, error) {
			if len(rows) != 1 {
				return "FAIL", "lms.logs is missing", nil
			}
			engine, ok := rows[0]["engine"].(string)
			if !ok {
				return "", "", errors.New("missing table engine")
			}
			if !strings.Contains(engine, "MergeTree") {
				return "FAIL", "unexpected table engine", nil
			}
			if !strings.HasPrefix(engine, "Replicated") {
				if n.ExpectedReplicas > 1 {
					return "FAIL", "replication required but this table is not replicated", nil
				}
				return "WARN", "table is not replicated; no HA demonstrated", nil
			}
			return "PASS", "replicated engine found; inspect replica evidence too", nil
		}},
		{"disks", "SELECT name,total_space,free_space,unreserved_space,is_read_only FROM system.disks ORDER BY name LIMIT 128", func(rows []map[string]any, n Node) (string, string, error) {
			if len(rows) == 0 {
				return "", "", errors.New("missing disks")
			}
			status := "PASS"
			for _, r := range rows {
				total, e1 := number(r, "total_space")
				free, e2 := number(r, "free_space")
				available, e3 := number(r, "unreserved_space")
				ro, e4 := number(r, "is_read_only")
				if e1 != nil || e2 != nil || e3 != nil || e4 != nil || total == 0 || free > total || available > total {
					return "", "", errors.New("invalid disk evidence")
				}
				if ro != 0 || available/total < 0.1 {
					status = "FAIL"
				} else if available/total < 0.2 {
					status = worse(status, "WARN")
				}
			}
			return status, "snapshot: <10% unreserved or read-only fails; <20% warns; retention/recovery capacity not established", nil
		}},
		{"parts", "SELECT partition_id,count() AS active_parts,sum(rows) AS rows,sum(bytes_on_disk) AS bytes_on_disk FROM system.parts WHERE active AND database='lms' AND table='logs' GROUP BY partition_id ORDER BY active_parts DESC LIMIT 1000", func(rows []map[string]any, n Node) (string, string, error) {
			status := "PASS"
			for _, r := range rows {
				count, err := number(r, "active_parts")
				if err != nil {
					return "", "", err
				}
				if count >= 1000 {
					status = "WARN"
				}
			}
			if len(rows) == 1000 {
				status = "WARN"
			}
			return status, "snapshot only; >=1000 parts in a partition warrants review; no claim that merge backlog is stable", nil
		}},
		{"replicas", "SELECT is_readonly,is_session_expired,queue_size,absolute_delay,total_replicas,active_replicas FROM system.replicas WHERE database='lms' AND table='logs' LIMIT 1", func(rows []map[string]any, n Node) (string, string, error) {
			if len(rows) == 0 {
				if n.ExpectedReplicas > 1 {
					return "FAIL", "required replica evidence is absent", nil
				}
				return "WARN", "no replicated-table evidence; HA not configured or not observable", nil
			}
			if len(rows) != 1 {
				return "", "", errors.New("unexpected replica evidence")
			}
			r := rows[0]
			v := map[string]float64{}
			for _, k := range []string{"is_readonly", "is_session_expired", "queue_size", "absolute_delay", "total_replicas", "active_replicas"} {
				x, err := number(r, k)
				if err != nil {
					return "", "", err
				}
				v[k] = x
			}
			if v["is_readonly"] != 0 || v["is_session_expired"] != 0 || v["active_replicas"] < float64(n.ExpectedReplicas) || v["total_replicas"] < float64(n.ExpectedReplicas) {
				return "FAIL", "replica availability or coordination is below configured expectation", nil
			}
			if v["absolute_delay"] > 60 || v["queue_size"] > 100 {
				return "WARN", "replica delay/queue warrants investigation; measure trend", nil
			}
			return "PASS", "replica snapshot meets configured count; failure recovery remains untested", nil
		}},
		{"merges", "SELECT count() AS running,sum(memory_usage) AS memory_bytes FROM system.merges WHERE database='lms' AND table='logs'", func(rows []map[string]any, n Node) (string, string, error) {
			if len(rows) != 1 {
				return "", "", errors.New("missing merge evidence")
			}
			for _, k := range []string{"running", "memory_bytes"} {
				if _, err := number(rows[0], k); err != nil {
					return "", "", err
				}
			}
			return "PASS", "running merges observed; zero running does not prove an empty backlog", nil
		}},
		{"mutations", "SELECT countIf(NOT is_done) AS pending,countIf(NOT is_done AND latest_fail_reason!='') AS failed FROM system.mutations WHERE database='lms' AND table='logs'", func(rows []map[string]any, n Node) (string, string, error) {
			if len(rows) != 1 {
				return "", "", errors.New("missing mutation evidence")
			}
			pending, e1 := number(rows[0], "pending")
			failed, e2 := number(rows[0], "failed")
			if e1 != nil || e2 != nil {
				return "", "", errors.New("invalid mutation evidence")
			}
			if failed > 0 {
				return "FAIL", "unfinished mutations have failure evidence", nil
			}
			if pending > 0 {
				return "WARN", "mutations are pending", nil
			}
			return "PASS", "no pending mutations in this snapshot", nil
		}},
	}
}

func Run(ctx context.Context, client *http.Client, c Config) Report {
	r := Report{ObservedAt: time.Now().UTC().Format(time.RFC3339), Status: "PASS", Scope: "read-only snapshots of explicitly configured nodes; no repair or capacity certification", ProductionAcceptance: "NOT RUN", Nodes: make([]NodeReport, len(c.Nodes))}
	var wg sync.WaitGroup
	slots := make(chan struct{}, 4)
	for i, node := range c.Nodes {
		wg.Add(1)
		go func(i int, n Node) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				r.Nodes[i] = NodeReport{Name: n.Name, Status: "FAIL", Checks: []Check{{Name: "node", Status: "FAIL", Detail: "inspection canceled"}}}
				return
			}
			nodeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			report := NodeReport{Name: n.Name, Status: "PASS", Checks: []Check{}}
			for _, p := range probes() {
				rows, err := query(nodeCtx, client, n, p.sql)
				check := Check{Name: p.name, Status: "FAIL"}
				if err != nil {
					check.Detail = err.Error()
				} else {
					status, detail, err := p.assess(rows, n)
					if err != nil {
						check.Detail = "incomplete or invalid evidence"
					} else {
						check.Status, check.Detail, check.Evidence = status, detail, rows
					}
				}
				report.Status = worse(report.Status, check.Status)
				report.Checks = append(report.Checks, check)
			}
			r.Nodes[i] = report
		}(i, node)
	}
	wg.Wait()
	for _, n := range r.Nodes {
		r.Status = worse(r.Status, n.Status)
	}
	if len(r.Nodes) == 0 {
		r.Status = "FAIL"
	}
	return r
}
