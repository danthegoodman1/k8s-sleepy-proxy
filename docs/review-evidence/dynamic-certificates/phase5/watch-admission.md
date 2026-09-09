# Watch admission correction

This checkpoint corrects certificate watch admission after hosted CI failed on
`5a337f3078e4ae424eafb590c4eb1f9b8a705bb3`. It does not close the deployed lifecycle,
resource or performance gates in Phase 5.

## Failure and correction

[Hosted run 34288517173](https://github.com/danthegoodman1/sleepypods/actions/runs/34288517173)
reported a closed watch during the actual PostgreSQL sixteen-stream test.
The run passed 23 PostgreSQL tests and failed one. Its exact scheduler/database
delay was not recorded, so the controlled experiments below establish a failure
mechanism rather than claiming to reconstruct that run.

Try-only acquisition allowed periodic readers to repeatedly overtake a new
registration. Reads now enter Tokio's existing FIFO semaphore. Admission bounds
the number of active or queued calls to 17: sixteen native producer calls plus
at most one prior logical read whose detached protocol drain still owns the
single watch slot. A producer can queue its next poll before its previous drain
finishes. A total cap of sixteen would exclude the last producer before it could
join the FIFO queue, recreating starvation.

Waiting reads hold no ordinary certificate operation permit or PostgreSQL
connection. Once acquired, the admission/watch/operation permits remain in the
existing shared ownership record through SQL, cancellation cleanup and discard.
No custom scheduler or registry was introduced. Native limits remain sixteen
streams, three-second setup and query deadlines, 250ms polling, one-second
delivery and sixty-second lifetime. Invalid batch limits reject before queuing.

## Focused proof

The final packet is retained at
`.generated/dynamic-certificates/phase5-watch-ci/final-packet.json`, including
five source hashes, six check-log hashes and seven experiment-log hashes.
Independent review verified all hashes and explicitly approved the correction.

| Final-source check | Result |
| --- | --- |
| Actual PostgreSQL watch target | 4 passed, 0 failed/ignored; 7.29s |
| Actual PostgreSQL outbox target | 1 passed, 0 failed/ignored; 0.46s |
| Work ownership/FIFO unit tests | 4 passed, 0 failed/ignored; 0.01s |
| Native producer unit tests | 3 passed, 0 failed/ignored; 0.53s |
| Strict control-plane/Frontline/observability all-target Clippy | Passed |
| Workspace formatting | Passed |

Paused-time tests prove queue order, cancellation of the queue head, bounded
admission, unchanged deadlines and retention through a drain-owned reference.
The actual PostgreSQL cancellation test drives a queued read for 200ms while
ordinary database work succeeds and the exact original blocker remains held.
It then cancels that read and proves recovery through the unchanged five-second
drain/discard bound. A single initial `Pending` observation would not establish
that a database query remained undispatched and is not used as that proof.

## Controlled latency experiments

An owned loopback relay delayed PostgreSQL protocol responses. The runtime test
uses the production read-retry store composition. Temporary producer diagnostics
and relay URL injection were removed before final-source checks; relay processes
were joined and unique test schemas cleaned.

| Experiment | Observed result and scope |
| --- | --- |
| Original raw store, no relay | Passed in 16.54s; diagnostic baseline only |
| Every ReadyForQuery delayed 40ms, original raw store | Registration setup expired at 3.003s |
| Same delay, production retry wrapper | Registration setup expired at 3.001s; retries alone do not fix starvation |
| Same delay, initial FIFO implementation | Last registration still exceeded the unchanged deadline; repeated prepare/execute/drain delay is a capacity limit |
| Only empty-query guard drain delayed 40ms, try-only runtime composition | Third change round timed out for one watch after 4s; test failed in 23.56s |
| Same drain-only delay, sixteen total FIFO admissions | Last registration remained excluded before FIFO admission and exceeded setup deadline |
| Same drain-only delay, seventeen total FIFO admissions | All sixteen snapshots, three change rounds, ordinary native route/lifecycle progress on pool two, and cancellation/readmission of sixteen streams passed in 14.91s |

The paired drain-only comparison demonstrates the corrected fairness and overlap
invariants. It does not promise successful setup when total database service time
exceeds the fixed deadline. Historical filenames containing `green` in the first
FIFO experiments do not denote passing results; the packet records those failures
explicitly.

## Integration gates

The coordinator validated exactly parent `5a337f3` plus the five reviewed files
in an isolated source snapshot. The aggregate Cargo/crate source hash remained
`7d18856c51b1d8941834fe2a547050489b801b046387e736235ae2fcb2bdc38f`
through every gate. Retained snapshot, identity and raw logs are under
`.generated/dynamic-certificates/phase5-watch-ci/checkpoint/`; the gate directory
within that snapshot is
`.generated/dynamic-certificates/phase5-watch-checkpoint/gates-20260908T233742Z`.

| Exact-source command | Result |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` | Passed |
| `cargo test --workspace --locked --offline` with database URL unset | 46 summaries; 889 reported passes, 0 failures, 16 explicit ignores |
| `cargo test -p control-plane --test postgres_store --locked --offline -- --nocapture` with the owned PostgreSQL URL | 24 passed, 0 failed, 0 ignored, no self-skips; 68.28s test execution |
| `python3 scripts/test-dependency-boundaries.py` | Passed |

Workspace totals include 23 optional database wrappers that return without a URL:
866 are local executions. The dedicated PostgreSQL command actually exercises
those 23 database cases plus the invalid-URL test. Workspace ignores remain
explicit deployed/subprocess gates and are not counted as this checkpoint's
proof. Compiler: local Rust 1.98.1; PostgreSQL: the task-owned 17-alpine container.

Commit `004505f70a7d6e68df5823265222ab45465d05fa` passes
[hosted CI 34292414386](https://github.com/danthegoodman1/sleepypods/actions/runs/34292414386).
All required steps pass, including actual PostgreSQL 24 passed, 0 failed,
0 ignored, no self-skips, in 73.22s. Hosted status and raw logs are retained as
`hosted-ci-34292414386.json` and `.log` in the correction's generated directory.
This closes 5C and the reopened 4G. Unfinished metrics and deployment fixtures
remain outside this commit and validation scope.
