# Phase 6A — durable lifecycle acceptance

Validated 2026-09-07 on the shared implementation branch. This is the lifecycle
intent/driver subphase of review Phase 6, including 3C/3D and target-scoped
ordinary/admin claims. Kubernetes conditional mutations, lease heartbeats and
attempt fencing, poison-work fairness, runtime supervision, and durable route
outbox delivery remain separate required subphases.

## Implementation and contract

- Wake validates and renders without Kubernetes I/O, then commits Waking and a
  complete Pending projection in one Postgres transaction. Its response is
  StillWaking. Only the reconciler applies, waits, completes, and cleans.
- Wake during Draining persists the next projection. The full drain deadline
  remains authoritative for normal and direct/manual claims. Old cleanup and
  deferred-wake promotion commit together. Delete cancels that intent atomically.
- Delete requires an explicitly present CAS revision, including zero. Its
  `accepted` response uses new wire tag 2; old `deleted` tag/name 1 is reserved.
  Acceptance marks every active target Deleting. Fresh controllers finalize
  Ready, Pending, and zero-materialization instances after proven cleanup.
- Each reconciler scans and claims only its configured cluster/namespace.
  Another cluster's absence cannot release its ownership reservations.
- `projection_generation` is persisted and remains stable across Pending,
  Ready, Draining, and Deleting while instance `generation` advances as a CAS
  revision. Sidecar reports must match that exact stamp and current Pod UID.
- Permanent instance-ID watermarks fence reuse independently of idempotency
  expiry. Known IDs deleted before generation history existed are retired.
  Backend freshness is allocated transactionally above prior rows, including
  Deleted rows; a caller's optional generation is a minimum.
- Migration 8 backfills deployed stamps and turns old unaccepted orphaned
  Waking states into Failed, including Waking3 with stale Pending1. Their old
  inventory is cleaned before a new wake is accepted. Coordinated upgrade and
  erased-history constraints are documented in the operator guide.

## Gates

| Gate | Result | Evidence |
| --- | --- | --- |
| `cargo test -p control-plane --offline` | 218 library, 46 operator, 35 proxy, 17 sidecar tests passed; 14 kind tests ignored | [Package log](control-plane-tests.log) |
| `scripts/test-postgres-store.sh` with disposable Postgres 17 | 5 passed, actual database bodies executed | [Postgres log](postgres-tests.log) |
| `cargo clippy -p control-plane --all-targets --offline -- -D warnings` | Passed | [Clippy log](clippy.log) |
| `git diff --check` | Passed | Coordinator/implementer command output |
| Skeptical independent review | Approved for 6A only after two fixes | Reviewer reran real Postgres 5/5 and maximum-wire-generation regression |

The package invocation also reports five Postgres entry points as passed while
its environment-dependent bodies skip without a database URL. The separate
real-database gate above is the transaction evidence. Kind binaries compile;
actual Kubernetes lifecycle/upgrade/finalizer gates remain required at integration.

## Focused regressions

The real database test accepts a wake through the actual API, drops the caller,
and uses a fresh reconciler to complete it without another wake RPC. It checks
concurrent wake deduplication, stable ownership stamps, drain wake promotion,
manual-claim grace enforcement, and delete cancellation of a deferred wake.
Ready, Pending and no-materialization deletes converge without RPC retries.
Low-level hard deletion cannot bypass unresolved cleanup. A stale wake or delete
cannot mutate a recreated ID, and another target's controller cannot prove its
cleanup. Backend floor 44 remains 44 initially; a later request below the stored
floor receives 45 after old cleanup.

The migration fixture reproduces Waking3 + Pending1, verifies explicit Failed4
and retained projection2, cleans the stale inventory, and then successfully
accepts and completes a new wake. The API rejects absent deletion revisions and
u64::MAX rather than overflowing predecessor/successor checks; the store also
checks its supported range. Sidecar replay arithmetic uses checked additions.

The old synchronous wake-driver unit suite was replaced by focused pure
acceptance/no-Kubernetes-I/O tests. Effect, ownership and retry coverage resides
in the existing materializer/projection/reconciler suites and the new API/real
Postgres lifecycle tests; there is no second synchronous implementation for tests.
