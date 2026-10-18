package logservice

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

func validChannel(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil
}

// hecRaw deliberately does not infer multiline boundaries from log contents.
func (s *Server) hecRaw(w http.ResponseWriter, r *http.Request, g Grant) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fail(w, 400, "invalid raw query encoding")
		return
	}
	for key, values := range q {
		switch key {
		case "channel", "host", "source", "sourcetype", "index", "time":
		default:
			fail(w, 400, "unsupported raw parameter")
			return
		}
		if len(values) != 1 || len(values[0]) > 1024 {
			fail(w, 400, "raw parameters must be single bounded values")
			return
		}
	}
	channel := q.Get("channel")
	header := r.Header.Get("X-Splunk-Request-Channel")
	if header != "" {
		if channel != "" && channel != header {
			fail(w, 400, "conflicting channel identifiers")
			return
		}
		channel = header
	}
	if channel != "" && !validChannel(channel) {
		fail(w, 400, "channel must be a GUID")
		return
	}
	// The channel identifies a stream only; no ACK ID is issued in this version.
	b, err := readBody(w, r)
	if err != nil {
		fail(w, 413, "invalid or oversized raw body")
		return
	}
	if !utf8.Valid(b) {
		fail(w, 400, "raw body must be UTF-8")
		return
	}
	dataset := g.Datasets[0]
	if _, exists := q["index"]; exists {
		dataset = q.Get("index")
	}
	base := map[string]any{"source_id": q.Get("source"), "host": q.Get("host"), "source_type": q.Get("sourcetype")}
	if _, exists := q["time"]; exists {
		stamp, err := hecTime(json.Number(q.Get("time")))
		if err != nil {
			fail(w, 400, err.Error())
			return
		}
		base["@timestamp"] = stamp
	}
	var rows []Event
	now := time.Now()
	appendEvent := func(message string) error {
		m := make(map[string]any, len(base)+1)
		for k, v := range base {
			m[k] = v
		}
		m["message"] = message
		e, err := normalize(m, g, dataset, "hec_raw", now)
		if err != nil {
			return err
		}
		rows = append(rows, e)
		return nil
	}
	if g.RawMode == "lines" {
		scanner := bufio.NewScanner(bytes.NewReader(b))
		scanner.Buffer(make([]byte, 4096), 64*1024+2)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			if len(rows) >= maxEvents {
				fail(w, 413, "too many raw events")
				return
			}
			if err := appendEvent(line); err != nil {
				fail(w, 400, err.Error())
				return
			}
		}
		if scanner.Err() != nil {
			fail(w, 413, "raw line exceeds event limit")
			return
		}
	} else {
		if err := appendEvent(string(b)); err != nil {
			fail(w, 400, err.Error())
			return
		}
	}
	if s.commit(w, r, rows) {
		respond(w, 200, map[string]any{"text": "Success", "code": 0})
	}
}
