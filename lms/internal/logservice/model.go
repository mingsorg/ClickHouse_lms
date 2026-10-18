package logservice

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

const maxEvents = 4096

type Grant struct {
	Tenant   string   `json:"tenant"`
	Datasets []string `json:"datasets"`
	RawMode  string   `json:"raw_mode,omitempty"`
}

func (g Grant) Allows(dataset string) bool {
	for _, d := range g.Datasets {
		if d == dataset {
			return true
		}
	}
	return false
}

// Event is the immutable row shared by all ingestion adapters.
type Event struct {
	Tenant     string `json:"tenant_id"`
	Dataset    string `json:"dataset"`
	EventTime  string `json:"event_time"`
	IngestTime string `json:"ingest_time"`
	EventID    string `json:"event_id"`
	Source     string `json:"source_id"`
	Host       string `json:"host"`
	Service    string `json:"service"`
	Severity   string `json:"severity"`
	TraceID    string `json:"trace_id"`
	Message    string `json:"message"`
	Attributes string `json:"attributes"`
	Protocol   string `json:"protocol"`
}

func utc(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000000Z") }

func strictJSON(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("expected one JSON value")
	}
	return nil
}

func text(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	if len(s) > 64*1024 {
		return "", fmt.Errorf("%s exceeds 64 KiB", key)
	}
	return s, nil
}

func normalize(m map[string]any, grant Grant, dataset, protocol string, now time.Time) (Event, error) {
	var e Event
	if !boundedValue(m, 0) {
		return e, errors.New("event exceeds nesting or field limits")
	}
	if !grant.Allows(dataset) {
		return e, errors.New("dataset is not authorized")
	}
	for _, key := range []string{"tenant_id", "ingest_time", "event_id"} {
		if _, exists := m[key]; exists {
			return e, fmt.Errorf("%s is server controlled", key)
		}
	}
	e.Tenant, e.Dataset, e.Protocol = grant.Tenant, dataset, protocol
	e.IngestTime, e.EventTime = utc(now), utc(now)
	if v, exists := m["@timestamp"]; exists {
		s, ok := v.(string)
		if !ok {
			return e, errors.New("@timestamp must be RFC3339 text")
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil || t.Year() < 1970 || t.Year() > 2100 || t.After(now.Add(5*time.Minute)) {
			return e, errors.New("invalid or future @timestamp")
		}
		e.EventTime = utc(t)
	}
	var err error
	for key, dst := range map[string]*string{"message": &e.Message, "source_id": &e.Source, "host": &e.Host, "service": &e.Service, "severity": &e.Severity, "trace_id": &e.TraceID} {
		*dst, err = text(m, key)
		if err != nil {
			return e, err
		}
	}
	if strings.TrimSpace(e.Message) == "" {
		return e, errors.New("message must not be empty")
	}
	attrs := make(map[string]any)
	for k, v := range m {
		switch k {
		case "@timestamp", "message", "source_id", "host", "service", "severity", "trace_id":
		default:
			attrs[k] = v
		}
	}
	if len(attrs) > 128 {
		return e, errors.New("too many attribute fields")
	}
	b, err := json.Marshal(attrs)
	if err != nil || len(b) > 64*1024 {
		return e, errors.New("attributes exceed 64 KiB")
	}
	e.Attributes = string(b)
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return e, err
	}
	e.EventID = hex.EncodeToString(id[:])
	return e, nil
}

func boundedValue(v any, depth int) bool {
	if depth > 32 {
		return false
	}
	switch x := v.(type) {
	case map[string]any:
		if len(x) > 128 {
			return false
		}
		for k, v := range x {
			if len(k) > 1024 || !boundedValue(v, depth+1) {
				return false
			}
		}
	case []any:
		if len(x) > 4096 {
			return false
		}
		for _, v := range x {
			if !boundedValue(v, depth+1) {
				return false
			}
		}
	}
	return true
}

func hecTime(v any) (string, error) {
	n, ok := v.(json.Number)
	if !ok {
		return "", errors.New("HEC time must be epoch seconds")
	}
	f, err := strconv.ParseFloat(n.String(), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f > 4133980800 {
		return "", errors.New("invalid HEC time")
	}
	sec, frac := math.Modf(f)
	return utc(time.Unix(int64(sec), int64(frac*1e9))), nil
}
