# Final integration evidence

Implementation is integrated and independently approved through Phase 6L. The
final production images pass the complete lifecycle driver, both restart
configurations and four full soak cycles, including one-request wake/re-wake,
autonomous sleep and stateful data retention. The corrected fail-closed soak
inventory also passes all four required resource queries.

All required implementation and validation gates are complete. The final
workspace is green after the independently approved test-only startup fixture
correction. The [final independent whole-system review](final-review.md) approves closure
with no unresolved required findings.
Earlier failures and their causal limits remain in the linked evidence packets.

## Current source and local checks

The source snapshot used to build the final production images has digest
`799aa84679078f48050f9f44d8a3eb2f0facd3e442479a8319878d81c04e0989`.
The [exact approved 6L patch and merge record](../phase6-intent-supersession/merged-source.json)
cover claimed-state fencing, safe release, lock ordering, per-job cancellation
and the independently reproduced heartbeat polling self-deadlock. The
[implementation packet](../phase6-intent-supersession/README.md) retains original
failures, causal database captures and explicit independent approval.

The [startup fixture correction](../sidecar-readiness-port-ownership/README.md)
is integrated with independent approval and twelve passing focused tests.
The final [workspace checks](final-workspace-063318/checks.json) all pass: formatting,
strict all-target clippy, the complete test workspace and dependency boundaries.
Cargo reports **838 passed, zero failed and 16 ignored** across 44 targets.
Eleven URL-gated database wrappers self-return in this invocation, leaving
**827 executed local entries**. The separate [PostgreSQL gate](final-postgres-060356.log)
passes **12 entries**: eleven actual database bodies plus invalid-URL validation.
Its [metadata](final-postgres-060356.json) records stable source and removal of its
owned disposable container. Do not add these database and local counts as
unique tests. The [validation inventory](final-validation-summary.json) retains
per-target counts and the earlier passing, mixed-source and failing checkpoints.
No hosted CI run is claimed.

The final workspace source fingerprint is
`f09f9f19a59982c75eea28894c31e175fe2a327d4a51b47952e5098b70b50338`, stable
before and after all four checks. The [source continuity proof](final-test-only-source-continuity.json)
compares every file in the preserved [148-file image snapshot](final-6l-coarse-image-source-files.json).
Exactly three modules changed, each behind an unchanged `#[cfg(test)]`
registration; all 145 other files are identical. The coarse image-source digest
therefore now reads `c9ae07628c0ea5946c9fd1c0af6a517d6bc6f22edbe182e0609d7711b359b27e`.
This test-only delta does not change release code. Recorded image and deployed
provenance remains the actual `799aa...` snapshot; no rerun or rebuild is implied.

The [final image gate](final-6l-images.log) passes at stable source
([metadata](final-6l-images.json)). All images are Linux arm64, non-root and below
the unchanged 256 MiB limit. [Exact image and binary provenance](final-6l-image-provenance.json)
shows that the Frontline and sidecar images/binaries are unchanged from 6JK;
only the control-plane binary changed. The retained data-plane performance
results therefore apply to these same final data-plane images.

## Deployed gate inventory

| Gate | Proven revision and result |
| --- | --- |
| Cold H2, generated gRPC and WebSocket | [6JK pass](6jk-protocols.log), including one-request cold behavior. |
| TLS termination and SNI | [6JK pass](6jk-tls.log). |
| Actual libpq through direct SNI | [6JK pass](6jk-libpq-sni.log), including rejection of an unbound SNI host. |
| Projection drift and finalizers | [6JK pass](6jk-projection-drift.log): unowned replacement preserved and cleanup blocked until the owned finalizer is removed. |
| Conditional materialization and stateful volumes | [6JK pass](6jk-materializer.log), both actual tests. |
| Browser-shaped gRPC-Web and metrics | [6JK gRPC-Web](6jk-grpc-web.log) and [three-component metrics](6jk-metrics.log) pass. |
| Failure paths | [Final 6L pass](final-6l-failures.log), 66.43s of actual driver execution. [Metadata](final-6l-failures.json) records unchanged production; an unrelated lifecycle fixture changed the wider source hash. The current-UID FailedBinding event, HTTP 503 and cleanup assertions pass. |
| Lifecycle races | [Final 6L pass](final-6l-lifecycle-races.log), 384.23s actual driver execution and 397.789s whole command. All concurrent wake, wake/drain deletion, failed-wake retry, stale observation, current-UID/replica drift, membership cleanup and route-reassignment cases pass. [Stable-source metadata](final-6l-lifecycle-races.json) and [continuous diagnostic captures](final-lifecycle-capture/run.json) are retained. The [membership fixture packet](../lifecycle-membership-diagnostic/README.md) proves that healthy cleanup can exceed an arbitrary 60s observation while staying within its original 90s operation deadline; it does not identify the original uncaptured timeout's exact cause. Earlier [deadline/grace](../phase6-draining-deadline/README.md) and [replica conflict](../lifecycle-replica-conflict/README.md) corrections remain independently approved. |
| Routing and HTTP-01 | [Final 6L pass](final-6l-routing.log), 31.61s of actual driver execution; [stable-source metadata](final-6l-routing.json). The independently approved [HTTP-01 fixture](../http01-expiry-fixture/README.md) now passes its natural and explicit expiry checks. |
| Single-control-plane restart | [Final 6L pass](final-6l-restart.log), 334.75s actual driver execution and 448.14s whole run. Wake, sleep, deletion, HTTP-01 and route-reassignment recovery pass; notification convergence is 7.913ms under the unchanged proof deadlines. [Metadata](final-6l-restart.json) records stable source. The earlier EOF-wait cause and independently approved framed helper remain in the [diagnostic packet](../reassignment-causal-diagnostic/README.md). |
| Dual-control-plane restart | [Final 6L pass](final-6l-restart-ha.log), 331.11s actual driver execution and 440.26s whole run. Both control-plane replicas recover wake, sleep, deletion, HTTP-01 and route subscriptions; notification convergence is 124.145ms. [Metadata](final-6l-restart-ha.json) confirms the two-replica configuration and stable source. |
| Exclusivity | [Final original-script gate passes](final-6l-exclusivity.log), 135.04s of actual driver execution. [Metadata](final-6l-exclusivity.json) records unchanged production; the source-only framed test-helper extraction happened after this driver compiled. The [earlier investigation](../exclusivity-investigation/README.md) remains separately retained. |
| Full wake/sleep soak | [Final 6L lifecycle pass](final-6l-soak.log): two stateless and two stateful cycles, 1465.01s total wall time. No-traffic wake returns to Cold after 225.40s and 223.99s; one-shot cold requests and re-wakes pass, and both stateful markers survive sleep. [Metadata](final-6l-soak.json) records unchanged production; approved test-helper source changes occurred during the run. Four actual cycle bodies execute; the stateful script's other two wrappers self-return under its environment and are not counted as extra coverage. The original reader could suppress discovery errors. Its independently approved correction passes a [supplemental inventory](final-soak-inventory.log): all four original selector queries succeed with zero objects; [metadata](final-soak-inventory.json) scopes this point-in-time result. Retained diagnostic namespaces are outside those selectors. |

Prior deployed results are scoped to their recorded control-plane image. They
are not relabeled as executions of the new 6L control plane. Every resulting correction has retained failure evidence and independent
review; image continuity is stated per component and gate.

## Performance and limits

[The independently audited performance packet](final-performance.md) records
three refreshed strict default-workload rounds passing across Frontline HTTP/1,
h2c, HTTP/2 TLS, generated gRPC, WebSocket transfers, sidecar HTTP and sidecar
TCP. All recorded requests/transfers succeed and the original thresholds remain.
The paired ten-case core benchmark passes the 25% failure threshold. Idle-timeout
TCP is a significant 18.820% warning, about 0.51 microseconds per short 4 KiB round
trip; added write/shutdown deadline tracking is a plausible source attribution,
not an isolated causal measurement. The connector retry path is not exercised
by that benchmark. Criterion overwrote its raw baseline samples during the second
run; baseline logs and generated change confidence intervals remain retained.

The independently approved [route resource experiment](../phase4-resource-envelope/README.md)
records 40 measured recoveries at two tasks and zero live ownership, with 592 KiB
RSS tail growth. Its development-profile, no-op-telemetry, in-process miss-path
scope is explicit; it is not a production memory SLA.

The [checkpoint history](checkpoint-history.md) retains earlier route/cache
measurements, the admitted throughput tradeoffs and both 16-way h2c stress
failures. Passing default-workload gates do not establish 16-way capacity.
Historical one-request 502, stalled-SNI network and original deployed-Delete
interleavings were not causally recovered; independently established defects and
passing follow-up gates must not be presented as proof of those exact causes.

## Operational boundaries

The supported automatic-sleep contract is one structured replica, with live Pod
UID and generation validation. Initial Ready activation has a durable 190-second
floor; accepted lifecycle work remains discoverable across client loss/restart.
An unresolved sent Kubernetes effect can deliberately retain ownership
indefinitely until explicit settlement under the [projection recovery contract](../../projection-safety.md).
A bounded process exit does not imply cancellation of a blocking DNS worker.

This implementation-validation checkpoint preceded PR publication. Subsequent
commits, branch integration and hosted checks are recorded in
[PR preparation](../pr-preparation/README.md). No production deployment was
performed. The [remediation plan](../../review-remediation-plan.md) remains the canonical status
ledger. The reviewed [reversible test-node teardown](test-node-teardown/README.md) and
retained diagnostics are recorded separately from system correctness gates.
