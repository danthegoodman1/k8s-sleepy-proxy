# Final deployed validation

The final lifecycle gate, single- and dual-control-plane restart gates, and four
actual soak lifecycle bodies passed. A later fail-closed inventory confirms
point-in-time absence in the four original soak resource scopes. The coordinator
confirms all required deployed gates are green, and the final frozen-source
workspace/static run also passes. This packaging adds no execution. The independent
reviewer explicitly approved the final evidence and whole-system closure; no
required findings remain.

## Captured lifecycle and restart recovery

| Gate | Actual bodies | Driver seconds | Whole command seconds | Evidence |
| --- | ---: | ---: | ---: | --- |
| Lifecycle races | 1 | 384.23 | 397.789 | [log](final-6l-lifecycle-races.log), [metadata](final-6l-lifecycle-races.json) |
| Restart, one control-plane replica | 1 | 334.75 | 448.136 | [log](final-6l-restart.log), [metadata](final-6l-restart.json) |
| Restart, two control-plane replicas | 1 | 331.11 | 440.262 | [log](final-6l-restart-ha.log), [metadata](final-6l-restart-ha.json) |

Each actual ignored-wrapper invocation reports one passing body, zero failures
and zero ignored entries. The lifecycle invocation filters out five local
replica-helper tests; each restart invocation filters out one local test. These
filtered entries are not counted as deployed executions.

The lifecycle body completes concurrent wake, idle reporting during wake,
deletion during wake and drain, failed-wake retry, stale sidecar observations,
current Pod UID and external replica drift, and route-subscription reassignment.
The membership-only cleanup observer sends one Delete and pins instance identity,
generation and materialization. It observes authoritative absence against the
original persisted operation deadline, capped at Delete dispatch plus 90 seconds;
it does not replay Delete or reset the operation. The independently approved
[membership packet](../lifecycle-membership-diagnostic/README.md) retains the
separate real-Postgres scheduler-boundary proof and the completion/read-assembly
race regression. A passing corrected gate does not retrospectively identify the
exact cause of the earlier uncaptured deployed timeout.

The [capture wrapper](final-lifecycle-capture/run-final-lifecycle-captured.py)
creates a previously absent task namespace after successful empty
[setup guards](final-lifecycle-capture/setup-guards.json), then invokes the
canonical lifecycle script. [Wrapper metadata](final-lifecycle-capture/run.json)
records exit 0 for the gate, capture process, final control-plane log read and
final Kubernetes read; total wrapper time is 408.219 seconds, including its
bounded capture tail. Its two read-only workers sample
[database metadata/effects](final-lifecycle-capture/database-timeline.jsonl) and
[owned Kubernetes metadata](final-lifecycle-capture/kube-timeline.jsonl) about once
per second. Individual read failures remain in the timelines, and the workers
are signaled and joined. These sampled diagnostics do not establish visibility
of every transient lease or effect. The namespace is intentionally retained at
this checkpoint; the supplemental soak-scope check below does not claim it was
removed.

Both restart configurations pass accepted wake, sleep and deletion recovery,
HTTP-01 survival/expiry, and route reassignment. The wake/deletion proofs observe
autonomous recovery without issuing another lifecycle mutation; the deletion
fixture holds its task-owned finalizer across controller loss before releasing
it. The HA run restores the configured two replicas after stopping all control
plane processes. Route notification convergence is 7.913 ms for one replica and
124.145 ms for two, within the existing fresh-cache proof deadlines. The earlier
[HTTP framing failure](../reassignment-causal-diagnostic/README.md) remains
separately retained.

The lifecycle run has stable before/after whole-gate fingerprint `f6c741d8…`;
both restart runs have stable `23d035c5…` fingerprints. All three record the same
production-only fingerprint as the final 6L image gate:
`799aa84679078f48050f9f44d8a3eb2f0facd3e442479a8319878d81c04e0989`.
Those distinct whole-source snapshots are not relabeled as one common fixture
revision. Exact values and image provenance remain in the linked metadata and
[image identity record](final-6l-image-provenance.json).

## Completed soak and later scoped absence check

The [full 2+2 soak](final-6l-soak.log) passed with **four actual lifecycle bodies**:
two stateless and two stateful. [Metadata](final-6l-soak.json) records exit 0 and
1,465.010 seconds for the whole command, including image preparation/deployment.

| Cycle | Actual body | Driver seconds | Cargo passes | Environment self-returns |
| --- | --- | ---: | ---: | ---: |
| 1 | Stateless lifecycle | 239.79 | 1 | 0 |
| 1 | Stateful lifecycle | 288.16 | 3 | 2 |
| 2 | Stateless lifecycle | 239.20 | 1 | 0 |
| 2 | Stateful lifecycle | 285.58 | 3 | 2 |

Both stateful invocations select three ignored wrappers; exclusivity and
projection-drift wrappers explicitly return early under this environment. Eight
Cargo successes therefore represent four actual bodies and four early returns.
This soak does not rerun exclusivity or projection drift.

Each stateless body passes its single cold request, hot-cache/Prometheus checks,
autonomous sleep/cleanup and single re-wake. Separate no-traffic wakes become Cold
after 225.400 and 223.986 seconds, retaining real elapsed coverage of the
190-second activation floor. Each stateful body passes cold write, mounted read,
autonomous sleep and a single re-wake read preserving the stored marker. All
12 recorded production-image builds across the four cycles match final 6L IDs.
Production is stable; the wider source hash changes during this soak because
approved test-helper work was integrated. It is not a frozen whole-workspace run.

The original shell reader suppressed kubectl errors, so its per-cycle output
alone is not verified absence. The independently approved
[fail-closed reader](../soak-inventory-fail-closed/README.md) was then run separately
at **2026-09-08 06:14:00 UTC**. [Metadata](final-soak-inventory.json) records exit 0,
0.191 seconds, the exact helper SHA256, explicit kind context, zero polling budget
and 45-second outer bound. The [output](final-soak-inventory.log) is empty, and the
helper returns success only after all four queries succeed and return no names.
The [runner](final-lifecycle-capture/run-final-inventory.py) and
[exact reader](final-lifecycle-capture/full-wake-sleep-inventory-as-run.sh) are
retained.

This closes the later **point-in-time soak-scope absence check**: the original
label-selected namespaces, workloads, Services, PVCs, PVs and RBAC. It does not
retroactively prove every earlier per-cycle query, establish global Kubernetes
absence, or prove a process-memory bound. No soak body was rerun for this check.

## Local/database validation and retained scope

The latest [real PostgreSQL run](final-postgres-060356.log) passed 12 entries:
11 actual database bodies and one invalid-connection-URL test, with no ignored or
failed entries. [Metadata](final-postgres-060356.json) records 71.665 seconds whole
run (64.96 seconds in the driver), stable before/after runner fingerprints and
identical before/after container inventories. The new cleanup-boundary test runs
alongside all existing database conformance cases. These entries are not added
to workspace totals as unique coverage.

The final [workspace tests](final-workspace-063318/workspace.log) report
**838 passed, 0 failed and 16 explicitly ignored**, across 44 targets. Eleven
PostgreSQL wrappers return early without a database URL, leaving **827 actual
local entries**, including invalid-URL validation and one documentation test. The
16 explicit ignores are 15 kind wrappers and one subprocess fixture; they are
separate from those database early returns. The real-Postgres run above covers
all 11 database bodies plus the already-local invalid-URL check, so the two totals
are not added as unique tests.

All four [final checks](final-workspace-063318/checks.json) exit 0: formatting,
strict workspace/all-target Clippy, workspace tests and dependency boundaries.
The workspace command took 35.463 seconds. Each check's before/after fingerprint
is exactly `f09f9f19a59982c75eea28894c31e175fe2a327d4a51b47952e5098b70b50338`.
That runner's scope includes crate/tests files but excludes scripts/Dockerfile;
it is not equated to the broader deployed/Postgres or image-source hashes.

The earlier [failed attempt](workspace-20260908T060356Z-failed/workspace.log) and
[metadata](workspace-20260908T060356Z-failed/checks.json) remain intact: its workspace
step exited 101 at the startup negative-port assertion and did not reach the
dependency step. The independently approved
[test ownership correction](../sidecar-readiness-port-ownership/README.md) changes
exactly three `cfg(test)` modules. A
[148-file pre-integration snapshot](final-6l-coarse-image-source-files.json)
actually matched the original 799aa image-source fingerprint; the
[exact post-integration comparison](final-test-only-source-continuity.json) finds
all 145 other files unchanged, including the modules' test-only inclusion guards.
There are no added or removed files in that coarse scope. Its textual fingerprint
is now c9ae0762, because the algorithm includes those test modules. This is source
inclusion evidence that release code is unchanged, not a new image build or
binary-hash measurement. Validated images keep their original 799aa provenance.
The prior global summary also remains a
[historical checkpoint](checkpoint-before-060356-validation-summary.json).

The approved [performance packet](final-performance.md) remains scoped to its
recorded data-plane executables: nine strict load runs and ten primitive
comparisons, including the significant 18.820% idle-TCP warning and missing raw
baseline-sample limitation. The final image gate and prior deployed component
checkpoints retain their exact source/image scopes; none were rerun or relabeled
by this packaging task. Earlier failures and partial runs remain intact.

[Structured validation](final-validation-summary.json) records counts, commands,
source scopes, captured worker exits and artifact hashes. The skeptical reviewer
explicitly approved the final workspace/count/source-continuity packaging after
checking all 44 targets, 838 reported passes/827 local entries, all four successful
checks, actual PostgreSQL12 split, all 162 artifact records/322 source-copy hashes,
and the 148-file comparison with exactly three test-only changes. Independent
[whole-system approval](final-review.md) is also explicit; no required findings remain. Historical
pending states and failures remain in the clearly identified prior checkpoints.
The canonical integration README, remediation plan and cleanup provenance remain
coordinator-owned.
