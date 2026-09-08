# Lifecycle fixture: guarded replica mutation after a conflict

The frozen final 6L deployment completed concurrent wake, Idle while Waking,
Delete while Waking, Delete while Draining, failed wake/retry and stale sidecar
ReportIdle after the separately reviewed 90-second test operation budget. It then
failed the current-member UID/external-replica scenario with Kubernetes 409:
Deployment `lifecycle-membership-8a61a01c` had been modified. The driver took 233.35s
and exited 101. `original-deployed-failure.log` preserves that different failure;
this run is not a full lifecycle gate pass.

Both original fixture mutations used a read followed by full-object replacement:
first replicas 2, then restoration to 1. A normal Deployment controller update
between those requests invalidates the resourceVersion. The original error did
not identify which of those two mutations conflicted, so the packet does not
attribute the conflict to a particular stage or writer.

The reviewer approved a fixture-only guarded retry. The helper retains the
nonempty UID of the originally selected managed Deployment, GETs a fresh copy for
each attempt, rejects a changed/missing UID, then sends a Merge patch containing
only that UID, its current resourceVersion, and the requested replica count.
Only a returned 409 is retried, with 50ms between attempts and at most 8 patches.
One total 5-second deadline bounds GET, PATCH and pauses; transport errors, other
API errors, and timeouts are fatal. Both scale to 2 and restore to 1 use the helper,
and errors now identify the requested count. Original idle rejection, absence of
activation deferral, Running/generation, restoration and deletion assertions are
preserved. Production code, API guarantees, and images are unchanged.

`replica_fixture_tests` uses the actual `kube::Client` transport stack with
controlled responses. It verifies exact PATCH method/body/content-type, fresh
resourceVersion after conflict for both 2 and 1, no further patch after UID
replacement, no retry for 403/500, exactly 8 attempts under continuous 409, and the
whole-operation deadline even when GET never completes. These are local fixture
regressions; they require no Kubernetes cluster.

Source: `crates/control-plane/tests/kind_e2e_lifecycle_races.rs` and its new
`kind_e2e_lifecycle_races/replica_fixture_tests.rs` module.

- [Focused tests](focused-tests.log): 5 passed, 0 failed, 0 ignored, 1 filtered
  (the deployed driver); actual tests took 5.01s, exit 0. Exact command and completed
  exit are in [focused-tests.json](focused-tests.json).
- [Strict scoped Clippy](clippy.log): warnings denied, exit 0;
  [completed command](clippy.json).
- [Formatting, Bash syntax and scoped diff checks](syntax-source.json): all
  exit 0, with current source SHA256 values.

Independent source, tests and final evidence received explicit reviewer approval.
Actual corrected deployed execution remains root-owned and pending.
