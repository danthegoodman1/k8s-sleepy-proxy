# Failure fixture: accepted revision versus projection ownership

The final frozen-image [kind gate](kind-before.log) failed after 33.97 seconds
inside its driver with `PV ownership does not match accepted wake generation`.
[Provenance](kind-before.json) records unchanged production source before/after
that run. This packet changes only `crates/control-plane/tests/kind_e2e_failures.rs`
and evidence; it changes no production code, scripts, images or cluster state.

The fixture conflated two existing values. `wake.rs` selects the immutable
projected Running generation as the accepted Waking generation plus one before
rendering. The Postgres `accept_wake` transaction explicitly verifies that
relationship, while `ProjectionPlan` stamps Kubernetes objects using the stable
projection generation. For a fresh Cold generation0, acceptance is Waking1 and
PV/PVC ownership is projection2. Later instance CAS transitions do not restamp
that incarnation. The prior fixture passed the accepted Waking revision directly
to its object ownership comparison. The retained deployed log does not include
actual PV labels; the exact mismatch is reproduced locally through the current
production render and projection paths.

The [fixture patch](fixture.patch) extracts the existing exact ownership check
into one helper shared by the deployed wait and local regression. It computes
the checked projected successor, requires the accepted revision to be exactly
the Cold successor, and reports accepted, expected projection and actual labels
on mismatch. It preserves the instance ID, managed-by marker, exact retained
1Mi/2Mi PV/PVC capacity mismatch, explicit binding, Pending observation, bounded
Failed outcome, no-ready-backend and cleanup assertions.

The regression now constructs the same Pending materialization contract and
renders the projected Running incarnation, then applies the real
`ProjectionPlan` ownership stamps. The pure renderer alone does not add the final
managed-by marker, so using the projection plan is necessary to exercise the
actual deployment assertion. The test independently asserts projection2 and
rejects generation0, the accepted generation1, future generation3, a different
instance, and a different managing controller for both PV and PVC.

[Controlled red](red.log) retains the old accepted-equals-projection comparison:
it fails with accepted1/expected1 and actual managed projection2. The
[corrected package run](green.log) passes the local test; the actual kind test is
explicitly ignored because this packet does not use the shared cluster.
[Scoped strict clippy](clippy.log) and `git diff --check` pass. The coordinator
still owns the required actual failure-path kind rerun; no deployed pass is
claimed here.

Independent reviewer explicitly approved the fixture correction and retained
render/projection red/green evidence. The actual kind failure-path rerun remains
coordinator-owned.
