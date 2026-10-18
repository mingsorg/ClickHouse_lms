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
- [ ] Validate target HEC/Bulk client versions; raw/ACK and broader compatibility are deferred.
- [ ] Measure real data, disorder distribution, input formats, sorting and indexing costs.
- [ ] Validate replication/failure recovery and sustained retention/merge equilibrium.
- [ ] Consider engine patches only after a measured bottleneck is established.
- [ ] Validate the original capacity and latency target; no production claim yet.
