# Phase 6L: Pending intent supersession

The final deployed lifecycle-races gate timed out after an accepted Delete during
an image-blocked Pending wake. Its preserved log did not capture materialization
status, so it does not prove an infinite hang or the exact deployed interleaving.
Source and deterministic PostgreSQL tests independently establish two defects:
renewal accepted either Pending or Deleting under the old stamp, and a delayed
Pending failure could publish into the newly accepted Deleting work. A permanent
apply failure left it permanent with no cleanup-required flag and undiscoverable.

The isolated change adds the claimed expected state to renewal and failure
publication. Failure publication uses the same instance-before-materialization
lock order as lifecycle acceptance, with fresh locked-row validation. Successful
completion and committed failure already consume the lease; other outcomes make
one exact conditional release attempt after owned work has settled or been
dropped following bounded cooperative cleanup. Existing effect, owner, attempt,
generation, live-lease and grace guards remain intact. No migration or external
RPC shape changes are needed. Public one-shot calls also receive per-job
cancellation, preventing one superseded job from canceling siblings or later calls.

The production retry wrapper, ProxyWake API, OperatorDelete API and continuous
driver regression captures before/accepted/after database state. The read-only
case uses a 120ms lease and must complete deletion within one second. Definite
permanent/transient apply results are held until one Delete is accepted, then
released before their ten-second heartbeat; both must acknowledge their exact
effect and finish deletion without publishing the old failure or another RPC.
A fourth case cancels a dispatched apply and proves that its effect remains
visible through operator status and blocks release, claim and finalization after
lease expiry. Failure_kind in the API is synthesized as uncertain from that marker;
the new intent's durable failure count/kind remain untouched. A controlled two-
client lock queue proves Delete precedes stale failure validation without a reverse
materialization lock. All workers are joined before schema cleanup.

The known-unsent unit case waits for the actual cancellation signal before
allowing begin to settle, proves zero Kubernetes dispatch, then exact ACK and
release. The one-shot ownership case proves sibling success and handle reuse.
Existing late-create/replacement/UID/RV tests remain required.

Evidence and completion results are appended after final gates. The initial red
runs and the first scratch failure are retained; a focused passing run is not a
replacement for the full suite or the root-owned deployed lifecycle gate.

## Completed implementation gates

- Original real-Postgres red packet: 10 existing tests passed and the new three-
  case test failed. `6l-supersession-apply-red-postgres.log` contains all before /
  accepted / after snapshots, including the permanent poisoning result. The
  earlier readiness-only red run is also retained and shows transient publication.
- `postgres-final.log`: full PostgreSQL 17 conformance, exit 0, 11 reported tests
  (10 actual database bodies plus invalid-URL validation), 7.61s. All four
  supersession cases and the controlled instance-lock queue passed. No lease,
  completion, or deployed fixture deadline was increased.
- `reconciler-deterministic.log`: 28 production reconciler unit tests passed,
  including known-unsent cancellation/ACK ordering and held-gate sibling/reuse
  isolation. The latter replaces an initial delay-based test synchronization.
- `control-plane-tests.log`: package gate exit 0, 373 reported passes, 0 failures,
  16 ignored. Ten database wrappers return early without a database URL, leaving
  363 executed local cases. The ignored set is 15 deployed kind cases and the
  subprocess-only SIGTERM fixture, which its parent process test invokes. This
  full package run preceded the final two test-only synchronization/diagnostic
  refinements; the final 28-case reconciler gate covers the updated unit tests.
- `clippy-initial.log` and `fmt.log`: strict control-plane all-target clippy and
  workspace formatting checks completed with exit 0. Final independent review
  rechecks the two test-only refinements.

`postgres-initial.log` and `reviewer-postgres.log` retain failed full-suite
runs. Their pre-Delete readiness-entry timeout also prevented cooperative driver
shutdown. The bounded capture in `postgres-stall-diagnostic.log` establishes the
cause: one connection is idle in a transaction after inserting an effect, awaiting
its caller's COMMIT; renewal is blocked on that connection's transaction/row lock.
The inline heartbeat branch stopped polling the work future that must commit.
This was a production self-deadlock, not lease jitter. The short fixture lease
exposed the interleaving; no timing bound was widened to conceal it.

`renewal-deadlock-red.log` independently reproduces that dependency through an
owned gated begin/renew fixture. The fix polls one heartbeat+renewal future beside
work in the same select, allowing work to commit and cancellation to be observed.
It introduces no spawned task or overlapping renewal calls. The corresponding
regression exercises both normal forward progress and cancellation while renewal
is blocked; cancellation still awaits known-unsent begin/ACK before exact release.
`reconciler-renewal-fixed.log` passes all 29 cases, including that deterministic
regression. Earlier focused/full passes preceding this discovered interleaving
are retained as intermediate evidence, not final completion claims.

The first uncertainty assertion also incorrectly expected empty API failure_kind.
It is corrected to distinguish the new intent's untouched durable failure status
from the API's synthesized uncertain marker. Diagnostic SQL failures are logged
without bypassing cooperative worker shutdown. The original red and all diagnosed
failures remain alongside the final gates.

Independent approval and the root-owned merged/deployed gates remain distinct
from this implementer's isolated evidence. No production image or cluster change
was made by this packet.

## Final heartbeat-fix gates

`final-gates.json` records the final commands, exit codes and elapsed times.
Formatting passed; strict CP all-target clippy passed; the complete CP package
passed with 374 reported passes, 0 failures and 16 ignored. Ten DB-only wrappers
return early in that package invocation, so 364 local cases executed. The separate
`postgres-renewal-fixed.log` is the full actual-Postgres invocation: all 11 tests
passed (10 database bodies plus invalid-URL validation), 7.56s, with the original
120ms lease / one-second post-Delete bound. These gates include the final
heartbeat polling change and all causal synchronization refinements.

Independent skeptical review explicitly approved the final source/local packet.
`reviewer-reconciler-final.log` passes 29 tests and
`reviewer-postgres-final.log` passes all 11 actual-DB gate entries; the earlier
failed independent PostgreSQL run remains in `reviewer-postgres.log`. Review
checked all four supersession outcomes, instance-first lock ordering, deterministic
begin/renew progress and cancellation, and final package/clippy/fmt evidence.
Merged-image lifecycle validation and the final soak remain root-owned gates.
