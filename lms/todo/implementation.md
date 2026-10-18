# Implementation tracking

- [x] Clone the authorized fork and create a task branch.
- [x] Preserve the plan and distinguish the fork revision from the runtime version.
- [x] Implement bounded normalization, cross-request batching and storage acknowledgment.
- [x] Implement native, HEC event and append-only Bulk input subsets.
- [x] Implement tenant-scoped parameterized search with signed cursors.
- [x] Add an isolated, version-pinned local development stack and secret initialization.
- [x] Complete final race/contract/static checks after application edits.
- [x] Exercise all input adapters and search against real ClickHouse.
- [x] Verify outage acknowledgment and data preservation across a database process restart.
- [x] Record validation results and current limitations.
- [ ] Validate target HEC/Bulk client versions; raw subset is implemented below, while ACK and broader compatibility remain deferred.
- [ ] Measure real data, disorder distribution, input formats, sorting and indexing costs.
- [ ] Validate replication/failure recovery and sustained retention/merge equilibrium.
- [ ] Consider engine patches only after a measured bottleneck is established.
- [ ] Validate the original capacity and latency target; no production claim yet.

## Bulk external ID policy

- [x] Accept and ignore action metadata `_id`; generate a fresh ID for every `index` action.
- [x] Update the protocol contract, implementation plan and live checks.
- [x] Verify repeated external IDs append independent events: contract tests cover within/across requests; real-storage tests confirm both same-ID items remain queryable.

## Phase 2: raw input, read-only operations and measurement

- [x] HEC raw: preserve multiline requests by default; token-configured line splitting; validated optional channel and envelope metadata.
- [x] Read-only multi-node doctor: actual table/disk/merge/replica/mutation evidence; partial failures never pass; no production-readiness inference.
- [x] Bounded synthetic benchmark: offered/completed rates, errors, latency distributions, payload bytes and explicit synthetic/small-data limits.
- [x] Contract/race checks and real ClickHouse validation for this phase, including observer data/write denial.
- [x] Update API, operating guide and validation record; draft PR update follows publication.

HEC durable ACK, target agent interoperability, replicated deployment, engine patches and full production capacity remain separate work.

## Measured follow-up

- [ ] Explain synthetic offered-load failures using per-stage timings, bounded admission, connection behavior and controlled host conditions.
- [ ] Repeat controlled baselines with real data/query inputs before claiming performance improvements.
- [ ] Add durable HEC ACK, production configuration/lifecycle workflows and replicated recovery in later validated increments.
