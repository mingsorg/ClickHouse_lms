# Validation record

Date: 2026-10-18.

Scope: the standalone Go service in `lms/`, not a modified ClickHouse engine. Fork base: `c8d67a8a9d8e7de3b56243f8c1af0f82a2ae7379`. Tested runtime: official ClickHouse **26.9.21.9**, Docker image digest `sha256:fd4652f708ecfc2239d08c828f5a51dfcc92a1e4a57269b1837c4dbe2dbe33a2`.

## PASS

| Check | Evidence |
|---|---|
| Fresh contract/concurrency run with Go race detector | `go test -race -count=1 ./...`, exit 0; `internal/logservice` passed in 3.057s; executable package has no tests |
| Static checks | `go vet ./...`, exit 0, no diagnostics |
| Image build and local stack startup | Compose built the service and started it against healthy ClickHouse |
| All three ingestion paths | Native batch, gzip HEC event and Bulk append item confirmed in real storage |
| Protocol error behavior | Bulk partial success with explicit ID rejection; malformed HEC batch rejected; HEC ACK explicitly returns unsupported |
| Actual query path | Paginated retrieval of all four test events, no duplicate IDs, nanosecond timestamp preservation |
| Isolation and parameterization | Second tenant sees no first-tenant events; SQL-like literal substring remains a value |
| Process interruption and recovery | Readiness fails and writes are not acknowledged while ClickHouse is stopped; the previously confirmed row remains queryable after restart |

Logs are retained locally under ignored `build/`: `test_contract_verified_2026-10-18.log`, `test_vet_verified_2026-10-18.log`, `test_live_verified_2026-10-18.log`, and `test_restart_2026-10-18.log`. Per repository instructions, a separate agent reviewed each test log; command completion was also checked directly.

The first live runs failed at query timestamp conversion and alias substitution. Both were corrected before the successful final live run. They demonstrate why the mock/contract checks were insufficient on their own.

## Limits

- Engine modification/build: NOT RUN; no C++ engine changes in this slice.
- Production throughput, 100QPS tail latency and ten-day capacity: NOT RUN.
- Replication, disk/host loss, backup/restore: NOT RUN. Graceful database process stop/start is not proof of these stronger guarantees.
- Actual Splunk/Elastic agent interoperability: NOT RUN.
- The development data and generated credentials are synthetic/local, respectively; credentials stay in ignored `.env`.
- Full protocol compatibility, HEC raw/ACK, Bulk ID/create semantics, aggregations, text-index acceleration and a `Native` transport are deferred.
- Writes use at-least-once delivery. Timeouts may have committed data; retries can duplicate events. No exactly-once or unconditional power-loss durability claim.
- Retention is ten days from receipt with asynchronous `TTL` cleanup; ten days of actual data/retention behavior has not been exercised.
