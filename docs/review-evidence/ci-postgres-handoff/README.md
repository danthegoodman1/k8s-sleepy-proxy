# PostgreSQL supersession observation in CI

The Rust 1.98.1 lint repair passed every local gate and both hosted lint steps.
On `f7912c7`, the [PR run](https://github.com/danthegoodman1/sleepypods/actions/runs/34252176071)
passed all 12 real-Postgres entries, while the [push run](https://github.com/danthegoodman1/sleepypods/actions/runs/34252172999)
passed 11 and failed the `apply-transient` supersession case's one-second final-absence observation.
The [paired record](initial-hosted.json) and PostgreSQL logs retain both outcomes.

The failed case had already changed from Pending attempt 1 with one unresolved
effect to Deleting attempt 2 with no effects or durable failure. The old lease
had not expired. Its diagnostic snapshot reported an active lease-renewal query
waiting on `WALWrite`, with no blocking transactions. This shows supersession had
succeeded before the deadline; it does not establish how much of the elapsed time
was spent in that sampled WAL wait.

The fixture correction gives only the definite apply-error cases one five-second
budget starting before Delete dispatch, covering acceptance, diagnostics, held
cleanup and final absence. The read-only short-lease case and unresolved-dispatch
negative observation retain their original one-second bounds.

The real Deleting cleanup is held at descendant inspection. While held, the test
checks attempt 2, the accepted instance identity and generation, the original
projection generation, unchanged operation deadline, no remaining effect and no
stale durable failure. Database time must precede the captured old lease expiry
minus twenty seconds. Review additionally requires handoff within ten seconds of
driver startup, excluding an earlier lease renewal as an explanation for handoff. The transient case holds valid cleanup beyond one second
before requiring eventual instance and managed-object absence within the same
fixed budget. The controlled delay tests the faulty observation assumption; it
does not reproduce the hosted runner's exact WAL interleaving.

The [controlled comparison](results.json) changes only the definite-case budget
from five seconds to one second; the [exact difference](controlled-budget.patch)
and both raw logs are retained. Healthy attempt-2 handoff occurred after about
25 milliseconds in the [red run](controlled-red.log), which then failed at one
second while valid cleanup was held. The [green run](candidate-green.log) passed
all four supersession cases and the lock-order check in 2.88 seconds. Its disposable
container was removed and the preexisting container inventory was preserved.

The initial candidate also passed the complete [real-Postgres suite](full-postgres-checkpoint.json)
(all 12 entries) and [workspace/static checks](workspace-checkpoint-checks.json)
(840 reported passes, 829 local executions after 11 DB-only self-returns, zero
failures, 16 ignores). These checkpoints precede the reviewer's additional
conservative driver-start assertion; final validation of that assertion is
recorded separately below. Earlier deployed, performance and compiler evidence retains its original
scope. This follow-up does not change the production lifecycle implementation.


The final [reviewed fixture](final-summary.json) adds the conservative driver-start
assertion. Its [focused actual-Postgres validation](final-results.json) passes all
four cases and lock ordering in 2.83 seconds, with strict target Clippy and
formatting passing. The exact [review strengthening](review-strengthening.patch)
is retained separately from the controlled budget experiment. Independent review
explicitly approves the final source and local packet with no remaining findings.
Hosted status is recorded on the PR for its exact head; both full Linux runs must
pass before reporting this CI repair complete.
