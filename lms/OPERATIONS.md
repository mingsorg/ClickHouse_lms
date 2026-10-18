# Read-only operations and measurement

These tools are the start of the local management layer. They do not deploy a production cluster, change topology, run upgrades, migrate data, repair replicas or delete files.

## Inspect explicitly configured nodes

With the development stack running:

```sh
docker compose exec -T logd /usr/local/bin/lmsctl doctor --config /etc/lms/doctor.json
```

The configured inventory is `deploy/doctor.json`. Each node needs a unique name and URL, an observer username, the name of an environment variable holding its password, and an explicit expected replica count. URLs must not contain credentials. Every listed node is required; a failed node cannot be hidden behind successful nodes. At most four nodes are inspected concurrently, with per-request and per-node deadlines and bounded response sizes.

The packaged development inventory expects one copy because the demo uses one `MergeTree` table. The report still returns **WARN** for absent replication, with production acceptance **NOT RUN**. This is deliberate. Setting `expected_replicas` to two while using an unreplicated table yields **FAIL**.

The CLI reads fixed `SELECT` statements only. The development `lms_observer` account has a read-only profile and metadata grants on `system.tables`, `system.disks`, `system.parts`, `system.replicas`, `system.merges`, and `system.mutations`; it does not receive access to business log rows. The development setup shares one generated password across database users for convenience. Provision separate secrets and equivalent restricted grants for real installations. The doctor is an operator CLI, not an endpoint exposed to tenant tokens.

| Check | Evidence and interpretation |
|---|---|
| Server | Actual version and uptime; version reporting does not approve an upgrade |
| Table | Existence and engine of `lms.logs`; unreplicated storage is explicitly identified |
| Disk | Total/free/unreserved space and read-only state; <10% unreserved or read-only fails, <20% warns |
| Parts | Active part count, rows and bytes per partition; >=1000 parts or a truncated partition list warns |
| Replicas | Active/total copies, read-only/session state, queue and delay; counts are compared with configured expectations |
| Merges | Running merge count and memory; zero does not prove that no work is waiting |
| Mutations | Pending mutations warn; unfinished mutations with failure evidence fail |

The thresholds are conservative triage hints, not replacements for table-specific limits or workload SLOs. They are not proof of steady-state health. Data uses the documented quoted/unquoted JSON numeric representations; absent or malformed evidence fails rather than becoming zero. Permission failures, missing credentials and unavailable nodes are visible **FAIL** checks. Raw backend exception text, URLs, passwords and log payloads are not emitted.

Exit codes: 0 for PASS or WARN (always inspect `status`); 1 for a FAIL report; 2 for invalid configuration or CLI errors. Output remains machine-readable JSON. There is no diagnosis of Keeper quorum, schema drift across nodes, backup recoverability, host failure recovery or capacity forecasts yet.

Configuration example for a different node (illustrative; not a deployment manifest):

```json
{
  "nodes": [
    {
      "name": "data-1",
      "url": "https://data-1.example:8443",
      "user": "lms_observer",
      "password_env": "LMS_OBSERVER_PASSWORD",
      "expected_replicas": 2
    }
  ]
}
```

For local execution use `go run ./cmd/lmsctl doctor --config path.json` after exporting the referenced secret. The program does not source an environment file or execute shell configuration.

## Bounded synthetic load

The benchmark uses only generated synthetic events tagged with a unique service. It never removes test rows, never outputs credentials, and exclusively creates its result file with owner-only permissions. It refuses to overwrite existing reports. Use a dedicated authorized dataset. Results and logs belong under ignored `build/`.

```sh
mkdir -p build
python3 bench/load.py --env-file .env --mode query --requests 200 --qps 100 --workers 16 --output build/query.json > build/test_query.log 2>&1
python3 bench/load.py --env-file .env --mode mixed --protocol native --batch 20 --requests 200 --qps 100 --output build/mixed.json > build/test_mixed.log 2>&1
python3 bench/load.py --env-file .env --mode ingest --protocol hec --batch 20 --requests 100 --qps 100 --output build/hec.json > build/test_hec.log 2>&1
python3 bench/load.py --env-file .env --mode ingest --protocol bulk --batch 20 --requests 100 --qps 100 --output build/bulk.json > build/test_bulk.log 2>&1
```

Run comparisons separately rather than concurrently. `--keepalive` reuses one HTTP connection per generator worker (default: a fresh urllib connection per request); compare transports explicitly rather than attributing a client-side difference to the engine. `--gzip` changes request compression; `--message-bytes` changes synthetic message width. The environment parser accepts the limited generated `.env` format without evaluating it. Choose `--tenant`/`--dataset` explicitly when using other grants. The tool exits nonzero on any failed/unconfirmed request or client shedding. The report must also be inspected; exit 0 is not workload acceptance.

- `query` seeds one event, then runs one selective recent-service literal-substring query. It does not represent ten days of logs or arbitrary text search.
- `mixed` offers 20% batch writes and 80% queries. At 100 total requests/s this is not a 100-query/s experiment.
- `ingest` runs the selected gateway input protocol. It is not a direct `Native` transport comparison.
- Work is paced by the offered rate. When the bounded worker set is full, the generator counts a client-shed request rather than quietly accumulating a long queue or reducing the rate. Start lag is recorded separately.
- Reports include successful and failed/unconfirmed counts, HTTP statuses, client shedding, successful-request p50/p95/p99, successful requests exceeding 100ms, wall time including drain, confirmed event count, serialized synthetic event JSON bytes and attempted wire bytes. Failed-request latency does not enter the successful latency percentiles; read the error counters alongside them.
- The synthetic JSON-byte throughput is explicitly labelled. It is neither real customer raw volume nor compressed database disk throughput, and is not extrapolated to TB/day. Timeouts or partial failures may have stored events that are not counted as confirmed.

## Validation commands

```sh
go test -race -count=1 ./... > build/test_contract.log 2>&1
go vet ./... > build/test_vet.log 2>&1
python3 bench/test_load.py > build/test_benchmark_accounting.log 2>&1
python3 tests/live.py > build/test_live.log 2>&1
python3 tests/doctor_live.py > build/test_doctor_live.log 2>&1
```

The live doctor test expects this unreplicated demo to report WARN rather than PASS. Use separate test fixtures for production or replicated clusters. Existing data volumes are retained; the observer account is managed by the mounted configuration, not a destructive database reset.

## Remaining management work

Next steps are a validated desired-state configuration, persisted operation records, configuration-drift checks, replicated deployment and recovery tests, then constrained upgrade/replace/backup workflows. Each action needs preconditions, auditability and an explicit failure policy. There is no automatic remediation, downgrade or historical data migration in the current version. These tools make operational evidence easier to obtain; they cannot manufacture the hardware capacity needed for 100TB/day.

Protocol references: [HEC event format and raw channel](https://help.splunk.com/en/splunk-enterprise/get-started/get-data-in/10.6/get-data-with-http-event-collector/format-events-for-http-event-collector), [HEC examples](https://help.splunk.com/en/splunk-enterprise/get-data-in/collect-http-event-data/http-event-collector-examples). Operational evidence uses ClickHouse's [system tables](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables). Compatibility scope remains documented in [README.md](README.md).
