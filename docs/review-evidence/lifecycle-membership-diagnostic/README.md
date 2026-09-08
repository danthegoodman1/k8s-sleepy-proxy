# Membership cleanup timing investigation

The original full lifecycle gate failed to observe deletion within 60 seconds. A focused natural run of the same membership scenario **passed**, with deletion accepted once and completed after 33.708 seconds. Its captured transient cleanup retries motivate a separate synthetic scheduler boundary test; they do not establish the uncaptured cause of the original failure. No production source, image, retry policy, operation deadline, or public API changed in this packet.

## Original failure and focused frozen-image capture

The [original gate](20260908T050752Z-lifecycle-races.log) failed after 375.42 seconds of driver execution. The [retained Kubernetes capture](20260908T0514-lifecycle-membership-kube.json) records the 1→2→1 replica exercise and terminating Pods, but the namespace was automatically removed before control-plane logs/database state were retained. That evidence alone cannot distinguish a scheduled retry from a terminal failure.

The [focused driver](driver-as-run.rs) retains the exact membership assertions, 90-second durable operation budget, and 60-second deletion observation. Its [patch](driver.patch) removes unrelated scenarios and adds timestamps only. The [runner](run-frozen-membership-diagnostic.sh) created a previously absent namespace, used frozen images, and left it retained. It did not modify old diagnostic namespaces, cluster RBAC, or PVs. One core fixture Delete was sent. Capture continued for a bounded 40 seconds after the original test result, without changing that result. Port-forward, capture, and log workers were stopped and joined.

The [actual run](deployed-run.log) passed 1 test, 0 ignored, in 36.61 seconds. [Executable provenance](driver-build-provenance.json) and [image identities](deployed-image-identities.log) are retained. The observed control-plane and frontline IDs exactly match [final 6L identities](../final-integration/final-6l-image-provenance.json). The sidecar used the frozen configured tag; its live image ID was not captured before deletion, so this packet makes no independent live-sidecar-ID claim.

The [database timeline](database-timeline.jsonl) and [Kubernetes timeline](kube-timeline.jsonl) sample metadata roughly every 1.1 seconds; they do not guarantee visibility of every short-lived lease or effect. No permanent/uncertain failure or active effect was sampled during this focused deletion. No rendered manifest, Secret data, or environment values were captured. [Live control-plane output](control-plane-live.log), [events](deployed-events.json), final logs and timestamped UID/RV patch confirmations are retained.

| Observation | Unix milliseconds | Relative to accepted Delete |
|---|---:|---:|
| Ready, instance generation 2 | 1788845112020 | −0.277 s |
| Restore replicas=1, original Deployment UID/RV confirmed | 1788845112291 | −0.006 s |
| One Delete accepted | 1788845112297 | 0 s |
| Persisted deletion deadline | 1788845202295 | 89.998 s |
| Transient cleanup failure 1; next attempt | 1788845113155 / 1788845115178 | 0.858 / 2.881 s |
| Transient cleanup failure 2; next attempt | 1788845115272 / 1788845119280 | 2.975 / 6.983 s |
| Transient cleanup failure 3; next attempt | 1788845119506 / 1788845127516 | 7.209 / 15.219 s |
| Transient cleanup failure 4; next attempt | 1788845127982 / 1788845143990 | 15.685 / 31.693 s |
| Materialization Deleted, backend cleared | 1788845144917 | 32.620 s |
| Original 60-second fixture returned success | 1788845146005 | 33.708 s |

Both the original and newly created Pod UIDs were retained, with 30-second termination grace. The instance advances to Deleting generation 3 while the materialization's accepted instance/projection generations remain 2. Deletion finalization removes the instance and cascades its materialization row.

Retained namespace: `sleepypods-e2e-membership-wire-20260908`. The cluster was released to the coordinator after this capture. This packet performs no subsequent cluster operations.

## Separate synthetic late-absence scheduler boundary

The scratch-only [test source](boundary-as-run.rs) and [registration patch](boundary-registration.patch) use real PostgreSQL and `MaterializationReconciler::run_until_shutdown` with its default one-second interval/jitter, unchanged 90-second operation deadline, and production persisted retry calculation. The narrow Kubernetes wrapper holds an owned terminating descendant until the fifth real cleanup failure is recorded, then makes it absent. There are no clock/backoff edits, enqueue calls, operation restarts, or repeated Delete calls.

The injection is specifically `ensure_no_descendants`, called after all recorded parent refs are missing. `ProjectionReconciler` attaches its readiness/Service observation reference to that error; the displayed “failed to inspect Service” therefore does not identify the injected method. This proves a synthetic late-descendant-absence timing boundary, not the original natural 30-second-grace failure.

The test records actual cleanup observations, the exact absence transition, persisted next attempt, and the original deadline. At 60 seconds it requires exact instance Deleting3/materialization generation2, absence of the controlled descendant, a definite transient cleanup failure with no effect or lease owner, and a future retry strictly before the original deadline. Final success requires that same scheduler to observe absence no earlier than the persisted retry and remove the exact instance/materialization/effect inventory before the original deadline. Returned assertion failures and timeouts still signal and join the scheduler, then drop the owned schema; the runner removes only its newly created disposable database container.

The final [real PostgreSQL run](boundary-postgres.log) passed **1 test, 0 failed, 0 ignored** in 67.35 seconds; the [runner record](boundary-postgres.json) retains command, source/executable hashes and confirmed removal of its owned container. The [derived timeline](boundary-timing-summary.json) shows the last held observation at +34.059 seconds, absence at +34.097 seconds, next eligible retry at +66.066 seconds, and exact instance/materialization/effect absence at +67.098 seconds. At +60.006 seconds the real store still held transient failure 5 and the future retry. The original persisted operation deadline remained +90.003 seconds throughout.

[Scoped strict clippy](boundary-clippy.log), [fmt check](boundary-fmt.log), and [build](boundary-build.log) passed. [Source continuity](boundary-source-continuity.json) verifies all 148 production files in scratch match the frozen root digest `799aa84679078f48050f9f44d8a3eb2f0facd3e442479a8319878d81c04e0989`. Only the new test module and its registration differ in scratch. At that scratch-run checkpoint, the root production and canonical fixture were unchanged pending review; the later test-only integration is recorded below.

Initial harness failures are retained: [compile assumptions](boundary-build-initial-error.log), [instance versus materialization generation assertion](boundary-postgres-initial-error.log), and [final snapshot of a correctly cascaded-away materialization](boundary-postgres-final-snapshot-error.log). The latter reached actual deletion after about 63 seconds before its incorrect final query failed. These are harness corrections, not production regressions. The skeptical reviewer explicitly approved this diagnostic and synthetic evidence packet, including the exact descendant injection seam, timing, hashes and final absence proof. Any canonical fixture observation-policy change remains a separate source/local review. This evidence does not close the original full lifecycle failure.


## Membership-only fixture observation correction

The independent reviewer and coordinator selected the simpler original-deadline policy, superseding an initially considered conditional check at 60 seconds. A normal active cleanup lease at that instant must not create another false failure.

The five-file [scoped patch](fixture.patch) adds the proven PostgreSQL regression and its registration, a small [test-support waiter](../../../crates/control-plane/tests/support/deletion_observation.rs), its five local gRPC tests, and only the membership call site in `kind_e2e_lifecycle_races.rs`. Existing replica UID/RV fencing, other lifecycle timeouts, and shared one-request HTTP helpers remain unchanged.

The waiter pins the current instance/generation and the exact materialization ID from the owned Deployment. It sends **one Delete**, immediately reads `status_only=true` once, and maps the original persisted deadline to one monotonic deadline capped at **Delete dispatch + 90 seconds**. Status inspection consumes that same cap. The duration is clamped before constructing an `Instant`, including a far-future invalid response timestamp. Every present instance must retain the exact deletion generation and identity; only authoritative `GetInstance` NotFound succeeds. No enqueue, Delete replay, periodic Kube status inspection, or deadline reset occurs.

A live effect/lease is not treated as an abandoned uncertain operation. Persistent terminal or stuck cleanup fails by the original deadline; this waiter does **not** claim immediate detection of such a failure. Failure output includes the pinned initial status; it is not represented as a fresh terminal snapshot. The coordinator's separately reviewed continuous full-lifecycle DB/Kubernetes capture supplies ongoing failure diagnostics without extending the test deadline.

[Local fixture tests](fixture-tests.log) passed **10 tests, 0 failed**, with the actual deployed lifecycle wrapper explicitly ignored: five real loopback gRPC tests and the five unchanged replica-mutation tests. These cover one Delete/one status query, stable identity/generation, rejected status/RPC errors, the persisted deadline and local cap including inspection latency/far-future timestamps, and deadline/external cancellation of an actual pending RPC while another client clone keeps the connection alive. [Scoped strict clippy](fixture-clippy.log), [fmt](fixture-fmt.log), and [diff check](fixture-diff-check.log) passed. [Five source hashes](fixture-source-hashes.json) identify the reviewed packet; the integrated PostgreSQL regression is byte-identical to the previously run scratch module. Its real 67-second result is reused without claiming another database execution after integration.

Review also identified the API's legitimate completion/read-assembly race: a materialization can be found before the instance cascade removes its work-status row, producing `found=true`, a valid Deleting/Deleted state, and deadline 0. This narrow shape now uses the same single bounded authoritative NotFound check as `found=false`; an instance still present or an RPC error fails. Positive elapsed/negative deadlines and unexpected states remain rejected. The [retained red regression](fixture-completion-race-red.log) shows the old helper skipped that authoritative Get; the final actual gRPC table covers both found values with absence/presence.

The skeptical reviewer explicitly approved the final five-file source/local packet after checking the completion fallback, one-Delete/fixed-deadline behavior, all ten local tests, final hashes, and unchanged 148 production files. No further source or local test changes were requested. The coordinator owns the required captured full deployed lifecycle rerun and final global validation. Production sources and image identities are unchanged.
