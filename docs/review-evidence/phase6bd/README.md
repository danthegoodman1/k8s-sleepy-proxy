# Phase 6B/6D — fenced projection and cleanup

Validated 2026-09-07 on the shared implementation branch. These gates cover the
conditional Kubernetes client, durable effect barriers and lease heartbeat.
Scheduler fairness/failure policy, cooperative shutdown and durable events remain
subsequent required Phase 6 work. No commit or push was made.

## Implemented contracts

- Lease renew/release/completion check the acquired attempt and materialization
  generation, including deterministic ID reuse after the old row is deleted.
- Each mutation registers its generation/owner/attempt/effect ID, ref and observed
  UID/resourceVersion in migration 9's `materialization_effects`. Begin, claim
  and exact ACK use row locks followed by fresh statement snapshots. Replayed ACK
  A cannot erase later effect B. Unresolved effects prevent takeover and inventory
  or reservation release. Runtime retry-store forwarding is exercised.
- Absent objects use create; owned updates carry UID/resourceVersion; deletes
  use both preconditions and foreground propagation. There is no unconditional
  fallback or partial-apply rollback. Secrets now participate in ordered apply.
- Heartbeats run during readiness. Individual mutations, reads and waits have
  finite deadlines. Uncertain mutation responses are dispatched once; readonly
  wait failures remain recoverable. Definite responses recover from transient
  ACK errors before or after commit.
- Cleanup checks recorded refs plus old Pods/ReplicaSets, including terminating
  members. Managed static volumes require Retain at class/render admission. Legacy live
  cleanup rejects destructive/unknown policy or unsupported bindings before
  deleting any PVC. Static volume data survives rematerialization. Kind role
  fixtures now include Secret mutation and ReplicaSet list permissions.
- The explicit limitation is documented in [projection safety](../../projection-safety.md):
  a lost/cancelled mutation, or a crash/cancellation at the begin/dispatch cut
  point, can require audited recovery. Absence alone cannot disprove a delayed
  create. Force recovery requires fencing the old process/request path and
  proving cleanup. No detached Drop cleanup task was introduced.

## Gates

| Gate | Result | Evidence |
| --- | --- | --- |
| `cargo test -p control-plane --offline` | 221 library, 46 operator, 35 proxy, 17 sidecar tests passed; 15 kind tests ignored | [Package log](control-plane-tests.log) |
| `scripts/test-postgres-store.sh` | Six real database tests passed, including migration recovery and new controlled races | [Postgres log](postgres-tests.log) |
| `scripts/test-kind-materializer.sh` | Two actual Kubernetes tests passed | [Kind log](kind-materializer.log) |
| `cargo clippy -p control-plane --all-targets --offline -- -D warnings` | Passed | [Clippy log](clippy.log) |
| `git diff --check`,kind script syntax | Passed;13 kind E2E scripts passed `bash -n` | Implementer command output |

Package execution without a database URL skips environment-dependent Postgres
bodies. The separate six-test database run above executed those bodies against
a disposable PostgreSQL 17 container, which the script removed afterward.
The reused isolated kind cluster was retained for coordinator integration; each
test cleaned its generated namespace and rendered PV inventory.

## Focused regressions and review fixes

Real Postgres exercises same-owner reacquisition, stale renew/release/complete,
full-operation begin replay, ACK A/B ordering, effect barrier retention after
lease expiry, claim waiting behind an uncommitted begin, ACK waiting behind an
uncommitted begin, and old-generation renew/release/ACK after deterministic ID
reuse with reset numeric counters. The durable lifecycle integration now uses
the actual `RetryingControlPlaneStore` around Postgres.

Production-client mocks replace an object between observation and update/delete,
insert a competing object before create, retain terminating Pod/ReplicaSet
members, and stall mutating versus readonly calls. Actual reconciler tests cover
readiness across multiple short leases, lease loss during reads, an old create
arriving after local cancellation and an absence observation, one dispatch of an
uncertain mutation through both retry wrappers, and ACK failures before/after
commit without permanent false quarantine.

Retention regressions reject destructive structured classes, legacy rendering,
and raw PV/PVC bypasses. The production Kube cleanup mock asserts that Delete or
missing PV policy, an unbound claim, and an external binding cause refusal before
any mutation, especially PVC deletion. Storage specs remain exclusively managed;
external policy/binding edits require quiescing managed work first.

Actual kind coverage verifies Kubernetes UID/resourceVersion preconditions on a
Secret replacement, foreground cleanup with an old Pod finalizer, and provider
data continuity across stateful materialization. The gate's old literal-name
assumptions were corrected to derive current rendered refs rather than bypass
production naming. Independent tests now include an atomic sequence in schema
and namespace names so same-clock-tick concurrency cannot collide.

Each mutating step adds one durable begin and one exact acknowledgement; these
are lifecycle operations, not data-plane hot-path work. No unmeasured throughput
claim is made. Full production-image projection-drift/exclusivity/restart/lifecycle
and soak gates remain coordinator integration work after the later phases.

## Handoff to Phase 6E

`reconciler/fenced_client.rs::mutate` knows the exact effect identity before its
begin await and does not poll the Kubernetes effect until begin succeeds. A
cooperative shutdown signal can be selected during begin and then awaited through
an exact ACK while that owned task knows dispatch has not occurred. After dispatch,
cancellation retains the barrier. Hard abort/process loss always retains the
conservative cut-point limitation. Implement this within supervised task ownership;
do not spawn unbounded detached Drop cleanup work.
