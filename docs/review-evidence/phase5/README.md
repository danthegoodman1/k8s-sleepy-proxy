# Phase 5 store evidence

Measured 2026-09-05 using local disposable `postgres:17-alpine`, Rust debug test
build, on the same host. No production database was used. SQL fixtures and full
`EXPLAIN (ANALYZE, BUFFERS)` output are retained in [baseline.sql](baseline.sql),
[baseline.log](baseline.log), [after.sql](after.sql), and [after.log](after.log).
The fixture contains 100,000 instances/routes and 10,000 materializations with
one object reference and one exclusivity key each. Migration backfill produced
10,000 reservations of each kind.

| Query | Before | After |
| --- | --- | --- |
| Resolve route | Full identity-kind scan: 100,000 rows, 1,809 shared-buffer hits, 11.144ms execution | Indexed candidates + instance/target backend joins: 1 row, 13 hits, 0.085ms execution |
| Missing object reservation | JSON expansion over 10,000 rows: 385 hits, 5.694ms | Unique-index probe: 2 hits, 0.012ms |
| Missing logical key | JSON expansion over 10,000 rows: 385 hits, 4.792ms | Unique-index probe: 2 hits, 0.010ms |

These are illustrative warm SQL execution samples, not throughput guarantees or
end-to-end API latency claims. The before route figure omits Rust row decoding,
ranking and subsequent backend loads; the after query includes joined target
selection. Index-probe measurements establish lookup cost, while the real-store
concurrency test exercises actual trigger-enforced writes.

[Real-store test output](store-tests.log): four tests passed, including existing
full conformance. Added named contract helpers exercise:

- Four simultaneous startup connections and metadata serialization.
- A one-slot pool, duplicate class creation, a 100ms saturation deadline,
  cancellation of a blocked migration and reuse of that same pool.
- The legacy table lock timing out after 102.3ms while an unrelated row
  transaction stays open; 23 independent normalized reservations commit in
  437.0ms under the same held transaction. Exactly one competing key acquisition
  succeeds. Cross-API-version object ownership is rejected.
- Stable materialization state age through claim, renewal and release; existing
  sleep conformance now also rejects direct claims before the full grace period.
- Permanent deletion tombstones, old-key replay after same-ID replacement,
  fixed opt-in retention, unexpired-record preservation, bounded GC and key
  reuse after explicit expiry.
- Rust matcher parity across exact/wildcard/path cases beside 100,000 unrelated
  routes, target isolation with a higher-generation backend on another target,
  and coherent route resolution across an uncommitted/committed cascade delete.
- Legacy schema backfill, normalization of duplicate refs, cluster-scoped PV
  collision rejection with transactional DDL rollback, preservation of legacy
  drain deadlines, deletion tombstone backfill and metadata mismatch rejection.

The source-level lease conflict regression asserts that ownership conflicts are
not retryable while unavailable-store errors remain retryable. Runtime parsing
checks explicit pool/deadline/retention values and rejects zero/negative/overflow
configuration. Workspace adoption of the new target-aware request and the
remaining lifecycle/Kubernetes guarantees belong to the coordinator's integration
and later phase gates.

Review fixes (2026-09-07) preserve the joined route snapshot through the proxy
subscription API: it no longer discards the backend and performs a second store
read. A transport regression configures that separate read to fail and proves
the resolved backend is still returned; target and generation isolation remain
covered. The transport fake now returns target-aware resolution results.

Pool capacity, timeout and optional retention limits are validated identically
for environment and programmatic configuration before allocation or connecting.
Boundary tests cover zero, fractional milliseconds, values at and beyond each
maximum, `usize::MAX`, `i64::MAX` and `Duration::MAX`. The finite retention maximum
also prevents overflow when adding it to the database timestamp. These are
configuration and API fixes; no SQL transaction body changed during this review
round. [Focused test output](review-fix-tests.log) records 240 library and 35 proxy
transport tests passing. [All-target control-plane Clippy](review-fix-clippy.log)
passes with warnings denied; `git diff --check` also passes.
