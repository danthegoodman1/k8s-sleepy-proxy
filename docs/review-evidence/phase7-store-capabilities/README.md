# Phase 7C store capability evidence

Scope: mandatory store capabilities, explicit retry classification and shared API
transport test semantics. No lifecycle/lease SQL was changed by this substage.
No commits or pushes.

## Implementation

`ControlPlaneStore` now requires all 44 operations. The production unsupported
helper is removed; an incomplete implementation is a compiler error, including a
compile-fail doc test. PostgreSQL and the retry wrapper implement the full contract.
Narrow unit fault fixtures explicitly list unreachable methods through a test-only
macro, which panics if a scenario unexpectedly calls them. It supplies no
production fallback.

The operator, proxy and sidecar transport tests now alias one `TestStore` in
`tests/support/mod.rs`. Shared instance/generation transitions, materialization
selection and cleanup, route/HTTP01 storage and seed helpers replace the three
independent fake implementations. Existing Kubernetes fault clients stay local.
The fixture retains explicit unavailable/lookup failure hooks and an optional
route-resolution gate for the runtime lane's cancellation tests. It does not
implement SQL transaction, idempotency-retention or distributed-effect semantics.
A shared observable lifecycle conformance function runs against both the fixture
and actual PostgreSQL.

The retry audit classifies all 44 capabilities. Twelve are one-shot, including
unfenced name/key changes, retention-dependent creation, materialization upsert,
claim/enqueue and durable effect begin. The remaining 32 have read, immutable,
predicate or exact generation/ownership semantics. The new forwarding probe
calls all 44 methods with distinctive arguments and verifies the forwarded error
and exact invocation count. Retention can be configured to one millisecond, so
create replay cannot assume its idempotency record survives a retry.

`postgres_store/retry_boundaries.rs` injects response loss after actual commits,
then runs a second real writer before the first caller receives `Unavailable`.
It covers operator force deletion/key release, materialization upsert, route and
challenge replacement, instance ID reuse, finite-retention creates, exact effect
ACK preserving a newer effect and failure recording incrementing once. These
checks test PostgreSQL behavior rather than assertions derived only from a fake.

## Changed files

- `crates/control-plane/src/store.rs`: mandatory declarations, the compile-fail
  contract, deliberate one-shot forwarding and retained exact-fence retries.
- `tests/support/mod.rs`: one shared transport store, targeted error/gate hooks,
  workload seed and reusable observable lifecycle conformance.
- `tests/support/unexpected_store.rs` plus test-only `src/lib.rs` wiring:
  explicit unreachable-capability declarations for narrow fault fixtures.
  Existing specialized runtime/reconciler/wake method bodies stay in place.
- `tests/{api_transport,proxy_api_transport,sidecar_api_transport}.rs`: replace
  three independent store trait implementations with aliases to the shared one.
  Kubernetes fault clients and transport assertions remain local.
- `tests/store_capabilities.rs`: invoke every required operation with distinctive
  arguments; verify forwarding, error preservation and one versus two attempts.
- `tests/postgres_store/retry_boundaries.rs` and its parent registration: actual
  PostgreSQL committed-response-loss/replacement and shared lifecycle assertions.
- `docs/postgres-store-contract.md`: required capabilities, the replay boundary,
  one-shot uncertainty and maintenance count semantics.

## Final validation

- `cargo test -p control-plane --offline`: 350 passed, zero failed, 16 explicit
  ignored kind/process fixture entries (`tests.log`). The SIGTERM parent test
  invokes its ignored subprocess fixture. The default package run does not
  replace the coordinator's required kind gates or the real database gate.
  Its nine PostgreSQL test entries include URL-gated bodies and do not prove
  database execution without the URL; the separate nine-test database run below
  supplies that evidence.
  The mandatory-store compile-fail doc test passes.
- `cargo clippy -p control-plane --all-targets --offline -- -D warnings`: passed
  (`clippy.log`). Control-plane formatting and `git diff --check` passed.
- `DOCKER_CONFIG=.generated/docker-public-config bash scripts/test-postgres-store.sh`:
  all nine registered tests passed against a fresh disposable PostgreSQL database,
  zero ignored and no conformance self-skip (`postgres-tests.log`). This includes
  the new replacement module, existing transactional/fencing/runtime tests and
  the same observable lifecycle conformance used by the shared fixture.
- Independent skeptical review reran all nine fresh PostgreSQL tests and the
  operator46/proxy41/sidecar17 transport suites plus both capability/conformance
  tests. Its database log is [reviewer-postgres.log](../phase6-runtime/reviewer-postgres.log).

The first new database run caught a test-fixture collision: replacement cases
reused one Kubernetes object name and correctly hit the real reservation
constraint. Cases now use distinct names; the original failure is retained in
[postgres-fixture-failure.log](postgres-fixture-failure.log). The runtime lane also corrected a broker-lifetime
race exposed by transport tests and a concurrent gRPC-Web test-body size-hint
error ([cp-web-fixture-failure.log](cp-web-fixture-failure.log)). All final package and database results above
include the corrections. Production store SQL was unchanged by these fixture fixes.

The small 7A follow-up adds dev-only tonic server/router features to frontline
and checks both load-smoke examples independently in CI. Its isolated frontline
example check passed; normal production dependency boundaries remain unchanged.

No data-plane hot path changed in 7C. The coordinator retains the final strict
production-image, kind and workspace release gates; their budgets are unchanged.
Independent skeptical review explicitly approved 7C after the final source,
database/transport checks and corrected evidence report, with no outstanding
source findings.
