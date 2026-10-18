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
| Protocol error behavior | Bulk partial success; malformed HEC batch rejected; HEC ACK explicitly returns unsupported. External-ID policy was subsequently changed as recorded below |
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
- Full protocol compatibility, HEC ACK, Bulk overwrite/create semantics, aggregations, text-index acceleration and a `Native` transport are deferred.
- Writes use at-least-once delivery. Timeouts may have committed data; retries can duplicate events. No exactly-once or unconditional power-loss durability claim.
- Retention is ten days from receipt with asynchronous `TTL` cleanup; ten days of actual data/retention behavior has not been exercised.

## Bulk external-ID policy update

User instruction: accept and ignore action metadata `_id`; each `index` action appends a fresh event with a generated ID. Repeated external IDs do not overwrite or deduplicate. Unsupported actions and other metadata remain rejected.

- PASS: fresh race/contract checks (`test_bulk_ignore_id_2026-10-18.log`, exit 0; service tests 10.691s) and vet (`test_bulk_ignore_id_vet_2026-10-18.log`, exit 0, no diagnostics). Tests cover repeated IDs within and across requests and returned/stored generated IDs.
- PASS: real ClickHouse live checks (`test_bulk_ignore_id_live_2026-10-18.log`, exit 0). Two `index` actions with the same external ID returned distinct generated IDs and both remained queryable; the unsupported `update` item failed individually. Pagination retrieved all five accepted events without duplicate IDs.

The original explicit-ID rejection above is superseded by this policy. No new throughput, latency or HA claim.

## Phase 2: raw input, read-only operations and synthetic measurement

- PASS: fresh Go race/contract run (`test_phase2_contract_2026-10-18.log`), doctor 2.846s and log service 3.516s; vet had no diagnostics.
- PASS: gzip raw multiline event preserved through actual storage; all six accepted events from all input paths retrieved with correct pagination, timestamps, isolation and Bulk ID behavior (`test_phase2_live_2026-10-18.log`).
- PASS: real metadata-only doctor observations and observer ACL denial of business-row reads and zero-row writes (`test_phase2_doctor_acl_2026-10-18.log`). Single-node storage remains WARN; production acceptance NOT RUN. Partial-node and missing-evidence failures are covered by contract tests.
- PASS: benchmark accounting tests, two tests (`test_phase2_benchmark_accounting_verified_2026-10-18.log`).
- FAIL: all six offered-load smoke runs included rejected or client-shed work. [BENCHMARK.zh.md](BENCHMARK.zh.md) records every run, including failed results; these are short synthetic tests, not engine causal comparisons or production acceptance.

Raw mode now exists as a documented subset; HEC ACK and actual agent interoperability remain deferred. The doctor is read-only and does not perform repairs, upgrades or backup/restore. C++ engine modification, replicated recovery and the original 100TB/day/ten-day/100QPS/<100ms target remain NOT RUN.
