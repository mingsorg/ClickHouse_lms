# LMS logging service

An initial runnable slice for append-only logs, living alongside the unmodified ClickHouse source. The fork remains on its original development commit; the local runtime is pinned to ClickHouse **26.9.21.9**. No engine performance improvement or production capacity is claimed.

The service provides native HTTP ingestion, a Splunk HEC event subset, an Elasticsearch Bulk append subset, bounded cross-request batching, and tenant-scoped log search. It uses Go's standard library and the ClickHouse HTTP interface with `JSONEachRow`; comparing the `Native` transport is a later performance task, not a completed optimization.

## Run locally

Prerequisites: Docker Compose and Python 3. The development ports bind to loopback. Run from this directory:

```sh
python3 tests/init_env.py
docker compose up -d --build
mkdir -p build
python3 tests/live.py > build/test_live.log 2>&1
python3 tests/restart.py > build/test_restart.log 2>&1
```

The environment initializer creates random credentials in ignored `.env`, refuses to overwrite an existing file, and does not print secrets. Keep `.env` out of Git. `.env.example` documents the shape without real credentials. The machine-readable API is in [openapi.json](openapi.json).

- Gateway: `http://127.0.0.1:18080`
- `GET /healthz`: process liveness only.
- `GET /readyz`: checks access to `lms.logs` on ClickHouse.
- ClickHouse development HTTP: `http://127.0.0.1:18123`.
- `docker compose stop` stops the demo while retaining its data. Do not remove its named volume unless you intend to lose that data.

This stack is single-node, uses a separate restricted database user for the gateway, and has a persistent development volume. The admin user is used only for initialization/health checks. There is no replication, TLS termination, durable gateway queue, load balancer or production deployment implied. Use TLS termination and network isolation before exposing endpoints outside local development. Do not expose the ClickHouse credentials to API clients: tenant isolation is enforced by the gateway, not database row policies.

For a local Go process, export the values from your local environment file using your normal secret-loading mechanism and set `CLICKHOUSE_URL=http://127.0.0.1:18123`, `CLICKHOUSE_USER=lms`, then run `go run ./cmd/logd`. Its default listener is `127.0.0.1:8080`. The program does not automatically load `.env`.

## Authentication and datasets

`LMS_TOKENS` is a JSON map from secret token to `{ "tenant": "...", "datasets": ["logs"] }`. Tokens must have at least 16 bytes; use generated random tokens. `LMS_CURSOR_KEY` must have at least 32 bytes. All instances must share the key if cursors cross instances; key rotation invalidates existing cursors. Configuration errors stop startup.

Credentials use endpoint-specific headers:

| API | Header |
|---|---|
| Native write/search | `Authorization: Bearer <token>` |
| HEC | `Authorization: Splunk <token>` |
| Bulk | `Authorization: ApiKey <token>` |

The Bulk token is this service's opaque token, not an Elasticsearch-issued API key. An authenticated credential selects the tenant and permitted datasets. Clients cannot set `tenant_id`, `ingest_time` or `event_id`. Dataset names are values, never dynamic SQL identifiers. Logical datasets share one physical table.

## Native ingestion

`POST /v1/logs`:

```json
{
  "dataset": "logs",
  "events": [
    {
      "@timestamp": "2026-10-18T04:00:00.123456789Z",
      "message": "request completed",
      "service": "api",
      "host": "host-1",
      "source_id": "source-1",
      "severity": "INFO",
      "trace_id": "trace-1",
      "duration_ms": 12
    }
  ]
}
```

Known fields become typed columns. Additional fields are preserved in a JSON-encoded `attributes` string rather than eagerly indexed. Missing event time explicitly uses receipt time; invalid timestamps are rejected, not silently replaced. Event times before 1970, after 2100, or more than five minutes ahead are rejected. Retention is **10 days from receipt (`ingest_time`)**, so late events are retained for their full receipt-based interval. This is a versioned implementation choice; change it only with an explicit retention decision and migration. `TTL` cleanup is asynchronous, not an exact deletion deadline.

The whole native request is validated before any write. A successful response reports the number committed and `acknowledgement=clickhouse_insert_completed`. There is no memory-only early acknowledgment.

## Splunk HEC subset

`POST /services/collector` and `/services/collector/event` accept concatenated event objects. Supported envelope keys are `event`, `time`, `host`, `source`, `sourcetype`, `index`, and `fields`.

- `event` is a nonempty string or an object. An object with `message` uses that field; otherwise its JSON representation becomes the message.
- Numeric `time` is epoch seconds and overrides the event object's timestamp. Fractional precision follows JSON-number-to-float conversion; it is not guaranteed nanosecond-exact.
- Envelope `host` and `source` override their event equivalents. `sourcetype` is preserved as `source_type` in attributes. `fields` is preserved under `hec_fields`; it does not create arbitrary indexed columns.
- `index` maps to an authorized dataset; absent `index` uses the first authorized dataset.
- Gzip is supported. Any validation error rejects the entire bounded request, unlike HEC configurations that may process a prefix.
- Success returns `{"text":"Success","code":0}` only after the storage insert completes.

`/services/collector/raw`, `/services/collector/ack`, channels, token management and Splunk search/SPL are unsupported. Channel-bearing event requests are explicitly rejected. Error envelopes are the service's JSON error format, not a complete reproduction of HEC error codes. This is an input subset, not full Splunk compatibility. Actual agent interoperability is still a separate test.

## Elasticsearch Bulk subset

`POST /_bulk` or `POST /{dataset}/_bulk` accepts NDJSON with a final newline:

```text
{"index":{"_index":"logs"}}
{"@timestamp":"2026-10-18T04:00:00Z","message":"bulk event","service":"api"}
```

Only `index` without `_id` is supported. The server generates an ID. `_index` overrides the path dataset; when both are absent, the first authorized dataset is used (an intentional compatibility difference).

`create`, `update`, `delete`, explicit IDs, routing/version metadata, ingest pipelines, `refresh`, shard confirmation parameters and other query parameters are rejected. The service does not emulate overwrite, uniqueness or Elasticsearch indexing management. There are no mapping/template/ILM/license/version-discovery endpoints and no Elasticsearch query DSL. Some agents need these endpoints or use `create`; they are not automatically compatible.

Items remain in original action order. Invalid items fail individually; valid items are written together. HTTP 200 can contain `errors=true`; always inspect every item. Overall malformed NDJSON framing rejects the request before any prefix is submitted. Downstream failures mark the affected items as unconfirmed. A timeout may have committed data, so a retry can create duplicates.

## Search

`POST /v1/logs/search`:

```json
{
  "dataset": "logs",
  "from": "2026-10-18T03:00:00Z",
  "to": "2026-10-18T04:00:00Z",
  "service": "api",
  "trace_id": "trace-1",
  "contains": "completed",
  "limit": 100
}
```

`from` is inclusive and `to` exclusive. The maximum range is 10 days; `limit` is 1–200. Optional predicates use equality for `service`/`trace_id` and case-sensitive literal substring matching for `contains`. The substring path is a scan baseline, not a claimed text-index acceleration. It does not imply regex, token search, case folding or a 100ms guarantee.

Results sort by `(event_time, event_id)` descending. Pass `next_cursor` back as `cursor` with the same request to continue. Cursors are signed, tenant/query-bound and expire after 15 minutes. Pagination is not a snapshot: concurrent inserts, late events and retention cleanup can change subsequent pages. Timestamp precision is preserved through the cursor.

SQL values are parameterized. Queries have fixed memory, thread, scan and execution budgets; exceeding them returns a failure, not silently partial aggregates. There is no aggregate endpoint in this first slice.

## Ordering, batching and reliability

At most eight ingestion requests and four queries are admitted per process. Request wire and decoded bodies are each limited to 4 MiB, with up to 4096 events, 64 KiB message/attribute payloads and bounded nesting. Batches coalesce for at most 5ms or eight requests, then sort by the complete storage key `(tenant_id, dataset, service, event_time, event_id)` before one synchronous insert. This reduces small-part pressure; throughput and memory overhead are not yet measured. There is no global reorder wait or per-source unbounded buffer.

ClickHouse already detects sorted blocks; pre-sorting here is an experiment whose complete CPU cost must be compared with sorting in ClickHouse. Standard background merges remain enabled. There is no custom merge scheduler, closed-window merge policy or compressed-block reuse patch in this version.

The service is **at least once**, not exactly once. Cancellation after enqueue does not roll back a write. Client retries can duplicate events, including after a response was lost. No request IDs are claimed as persistent deduplication keys. Inserts are synchronous (`async_insert=0`) at the storage boundary; acknowledgment does not promise multi-replica durability or power-loss protection. Those require explicit configuration and fault tests. Gateway process shutdown drains active HTTP requests up to its deadline.

## Development checks

Run from this directory and retain logs:

```sh
mkdir -p build
go test -race ./... > build/test_contract.log 2>&1
go vet ./... > build/test_vet.log 2>&1
python3 tests/live.py > build/test_live.log 2>&1
```

The live checks use synthetic test logs, retain their rows, and tag each run with a unique service name. They verify the protocols and real query path, not production capacity. The restart check stops and starts only this development stack's ClickHouse container; do not run it against production. See [VALIDATION.md](VALIDATION.md) for this implementation's actual results and [todo/implementation.md](todo/implementation.md) for open work.

The target remains 100TB/day raw, ten days, at most ten 8-vCPU/32GB machines, 100QPS and <100ms. This single-node development stack does **not** establish that target. Required next inputs are disk/network specifications, realistic logs/query mix, latency percentile, replication needs and target client versions. See [PLAN.zh.md](PLAN.zh.md).
