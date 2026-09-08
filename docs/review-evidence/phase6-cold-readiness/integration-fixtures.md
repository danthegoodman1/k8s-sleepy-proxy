# Final deployed fixture corrections

This packet changes only `kind_e2e_stateful.rs`,
`kind_e2e_lifecycle_races.rs`, and `test-kind-e2e-lifecycle-races.sh`.
No production code or kind operation is part of this packet. The coordinator
owns actual deployed validation with the rebuilt reviewed images.

## Atomic exclusivity admission

`postgres/lifecycle_ops.rs::accept_wake` updates Waking and calls
`upsert_materialization` inside one transaction, committing only after key
reservation succeeds. A conflicting key rolls back that entire acceptance;
it leaves the contender Cold at its original generation, with no materialization
or Kubernetes objects. The old fixture incorrectly waited for Failed and a
consumed generation after the503 response.

The corrected fixture sends one bounded request for the owner wake, the
contender's503 rejection, the unrelated-key wake, and the contender's successful
wake after owner cleanup. It never replays a `/write` to hide a failed first
request. After rejection, read-only GetInstance and status-only materialization
inspection assert unchangedCold/generation and no durable materialization; label
inventory asserts no managed objects. The owner's Running generation, object
inventory and existing volume marker remain intact. Existing owner deletion and
Kubernetes absence checks precede the contender's one ordinary successful wake.

## Draining deletion under a repaired permanent permission failure

The StatefulSet delete permission remains removed until ReportIdle accepts,
Draining is observed, DeleteInstance accepts with exactly the next generation,
and a status-only observation proves a settled permanent failure with retained
refs, no active lease and no uncertain effect. Its diagnostic must name the exact
StatefulSet delete and contain Forbidden/403, tying recovery to the injected fault. The Role mutation records its
owned change before dispatch and is enclosed in the restoration-covered result
scope. Cleanup re-adds only the delete verb removed from the original Role; it
does not add a permission absent before the test or overwrite other Role fields.

Definite Kubernetes403 is a permanent configuration fault. After restoring
permissions, one explicit normal ReconcileMaterialization enqueue resumes the
same scheduler. Read-only GetInstance then waits for NotFound; no second
DeleteInstance is sent. This is operator recovery from a repaired permanent
fault, not an autonomous/no-RPC recovery claim. The separate restart/finalizer
gate covers accepted deletion recovery under valid permissions.

## Repeatable delayed-image fixtures

The three intentionally absent images receive unique default tags using the
portable timestamp/process token already used by restart. Explicit image
arguments remain unchanged. Reusing the retained kind cluster therefore does
not silently reuse a previously loaded delayed image. A startup-only
`/bin/bash` check proved all three defaults differ across invocations and all
three explicit overrides survive; it executed no kind command.

## Bounded route cutover

Both old/new backends are prewarmed by one accepted Wake and read-only Running
observation before this host's first frontend lookup. The direct subscription is
installed, then the frontend's first cache population has a total1s budget;
DeleteRoute/CreateRoute commits have another total1s budget. Direct invalidation,
new subscription resolution and independent frontend cutover must complete
within3s, with original cache age below5s to rule out the10s TTL fallback. Known
old/new successful payloads are allowed during delivery, with full app marker
checks. After the first new response, ten bounded responses must all remain new.
The test no longer assumes one subscriber's receipt proves another consumed its
event, and no180s cold-start retry substitutes for event delivery evidence.

## Completed local gates

- `cargo clippy -p control-plane --test kind_e2e_stateful --test kind_e2e_lifecycle_races --offline -- -D warnings` completed exit0 —[log](6h-final-fixtures-clippy.log).
- `rustfmt --edition 2021 --check` on both drivers, `/bin/bash -n` on the script,
  and scoped `git diff --check` completed exit0.
- Portable default/override startup verification completed exit0 —[log](6h-lifecycle-script-startup.log).

These checks compile the actual deployed test bodies and verify script startup;
they are not a claim that the Kubernetes gates have run or passed.
