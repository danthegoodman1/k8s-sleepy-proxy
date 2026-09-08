# Phase 4 implementation evidence

Baseline: `dc7cac2`. Validation: 2026-09-05 on the same local machine.

Implemented exact queried-identity routing authority, a shared cache with indexed
FIFO and expiry bookkeeping, independently progressing route/wake futures and
subscription events, bounded flights/tasks/waiters, and asynchronous cold-request
readiness waiting. Wildcard/path matching remains authority metadata and response
validation; it cannot route a request the proxy has never resolved.

The production HTTP/TLS/SNI runtime uses one shared coordinator. The earlier
synchronous HTTP/coordinator helpers are now compiled only for unit tests.
HTTP-01 observations were moved into the actual shared listener path. Routing
setup uses a separate 130s configurable deadline; accepted wake work waits for a
Running answer, including authoritative refreshes every 100ms. This does not
limit the duration of an established application stream.

The transport has one bounded ordered event queue. A 300-event burst over its
256-event budget fails the stream and preserves a cache-flush barrier through
reconnection. Route completions crossing an invalidation are conservatively
re-resolved. Cancellation of a pending response triggers unsubscribe when its
answer arrives; failed/bounded cleanup forces session reset. The response reader
never waits for an event-queue consumer or holds the pending-response lock across
request-channel backpressure.

Admission limits are 64 distinct flights, 64 active actor operations and 256
waiting callers. Actor operations retain their own bound even if the enclosing
readiness deadline expires earlier. Cache maintenance processes only the expiry
work that fits the deferred unsubscribe budget, and expiry never makes an old
entry usable. Separate positive/negative budgets and FIFO bookkeeping remain
bounded through identity churn.

## Validation

- `cargo test -p frontline`: 188 passed, with loopback sockets enabled.
- `cargo clippy -p frontline --all-targets --no-deps -- -D warnings`: passed.
  The workspace-wide lint gate remains the coordinator's responsibility.
- `cargo fmt -p frontline --check` and `git diff --check`: passed for owned files.
- Criterion: 10 samples, 1s warmup, 1s measurement. Raw baseline and final logs
  are retained here. A negative baseline-to-final comparison is an improvement;
  the baseline scale harness was temporarily run after an initial new-code
  measurement, so its own Criterion percentage labels have the reverse direction.

Named regressions include all four retained `review_probe_*` failures, complete
HTTP/SNI authority equivalence through cache eviction and request permutations,
new route insertion after warmup, invalidation during blocked wake and subscribe,
invalidation-before-install, generated transport overflow plus actual coordinator
recovery, cancelled subscription cleanup, shutdown, distinct-flight saturation,
same-identity waiter saturation, and deadline recovery.

`listener_first_cold_request_waits_through_accepted_and_waking_until_ready` sends a
real HTTP request through the production listener. It receives the upstream
response after Accepted → Waking → Running, with one wake RPC.

## Remaining gates

The initial report predates independent review. Review findings and their fixes
are recorded below; explicit re-review approval remains the coordinator’s gate.
Strict frontline production-image smoke, real asynchronous lifecycle kind tests,
and Phase 7 listener/protocol resource limits remain integration work. This
report does not claim whole-system production readiness.

## Skeptical review fixes — 2026-09-07

The first independent review found four additional defects. All four have been
addressed for re-review:

- Ordinary route updates now invalidate only operations observing the affected
  subscription. Stream failures retain a global epoch barrier. Per-subscription
  event history is kept only while operations are in flight, pruned as they
  finish, and capped at 4096 distinct subscriptions. History overflow explicitly
  resets the stream and cache; it never silently forgets an invalidation. A cold
  request completes after 1280 distinct unrelated events with one subscribe RPC.
- Every discarded successful subscribe is reclaimed. Same-session discards use
  bounded unsubscribe work. Ambiguous completions crossing a stream reset force
  another session/cache reset, because their returned IDs might belong to either
  session. Failed or saturated cleanup also resets authority. Deferred reset
  futures cannot destroy a replacement session after `ensure()` already consumed
  their reset request.
- Cancelled transport requests retain their response correlation within the
  existing 64-entry pending budget. A late successful response is unsubscribed;
  admitting request B before cancelled request A's late response no longer causes
  an unexpected-response failure for B. Saturation involving abandoned requests
  marks the session closed, allowing subsequent admission to reconnect.
- The actual shared listener path emits one cache hit/miss metric and one lookup
  log per logical lookup. An HTTP listener test observes exactly a miss followed
  by a hit across two requests and one subscribe RPC. `ObservabilityRecorder`
  gained a backwards-compatible `record_lazy` method so its default no-op sink
  skips event construction; enabled sinks retain the metric and log events. The
  recorder test proves lazy closures are skipped only for no-op recorders.

Validation after these fixes:

- `cargo test -p frontline -p proxy-core`, followed by the final frontline test
  addition and rerun: 195 frontline tests and 95 proxy-core unit/integration
  tests passed. Local sockets were enabled for the listener and transport tests.
  The latest ownership fix and its 196-test gate are recorded below. Raw output: [combined gate](final-tests.log),
  [final frontline suite](final-frontline-tests.log).
- `cargo clippy -p frontline -p proxy-core --all-targets --no-deps -- -D warnings`:
  passed. Raw output: [final-clippy.log](final-clippy.log).
- `cargo fmt -p frontline -p proxy-core --check` and `git diff --check` for the
  owned files: passed.
- The reused-session-ID and failed/saturated-cleanup regressions verify authority
  is flushed along with subscriptions, and that recovery can resolve again. The
  production shared-wake test rejects Ready after its own subscription is
  invalidated and accepts Ready after an unrelated subscription changes.

The final concurrent-update benchmark and interpretation follow below. All
Criterion output now uses ignored `.generated/implementation-evidence` paths;
raw text measurements relevant to review are retained alongside this report.

### Performance after review fixes

Same-machine Criterion runs used 10 samples, 1s warmup and 1s measurement. The
archived `dc7cac2` checkout includes the benchmark-only scale/update harness from
initial phase implementation. Baseline and final timed runs were sequential.

| Measurement | Baseline central estimate | Final central estimate |
| --- | ---: | ---: |
| Full hot route, default no-op recorder | 289.55ns | 198.22ns |
| Full hot route during 640 unrelated updates/sec | 287.03ns | 195.57ns |
| Full hot route, enabled metric/log construction | Not comparable: old shared path omitted observations | 357.87ns |
| Idle expiry, 1k / 10k / 100k entries | 458ns / 6.26µs / 132µs | 3.48ns / 3.75ns / 4.01ns |
| Insert/evict, 1k / 10k / 100k entries | 5.86µs / 47.2µs / 261µs | 1.05µs / 1.18µs / 1.43µs |

The full-route estimates improve approximately 32% with observations disabled.
The enabled-observation benchmark measures constructing and delivering the
metric/log to a consuming benchmark sink; it does not model Prometheus or stderr
export costs. Those remain part of the strict production-image smoke gate. The
initial eager construction measurement is retained to expose why the lazy API
was added; no performance budget was changed to hide that cost.

Raw evidence:

- [Same-machine baseline full/update lookup](baseline-full-resolve-bench.log).
- [Final disabled/enabled/update lookup](final-full-resolve-bench.log).
- [Final cache-scale/churn and pre-lazy lookup measurement](review-fixed-eager-observations-bench.log).
- [Initial baseline cache scale/churn](baseline-cache-scale.log).

Independent re-review and the coordinator's production-image/integration gates
are still required before marking the phase complete.

### Re-review: deferred unsubscribe ownership

The skeptical re-review identified one additional session race: a queued
unsubscribe acquired the transport mutex later and selected whichever stream was
current then. If a replacement stream reused the same server ID, cleanup for A
could remove B's subscription while B's cache entry remained live.

`SubscriptionId` now carries a private, process-unique originating-session stamp.
The response reader stamps every resolved, updated, and invalidated subscription
ID. Equality and hashing include that stamp; the public `as_str()` and protobuf
wire ID remain unchanged. Unsubscribe verifies the originating stamp under the
transport lock before sending. Cleanup from another session becomes a no-op;
untagged IDs are rejected rather than being allowed to target an unproven stream.
No server ID parsing or rewriting was introduced.

The generated-transport regression covers both orders: cleanup created before
reconnection and held unpolled until after B resolves the reused ID, and cleanup
created only after B has replaced A. It verifies no A unsubscribe is sent on B,
A/B typed IDs differ despite equal wire IDs, untagged cleanup is rejected, and B's
invalidation still reaches and clears B's cache entry.

- [Exact origin regression](origin-regression.log): passed both orders.
- [Full frontline suite](final-frontline-tests.log): 196 passed.
- [Frontline clippy](origin-clippy.log): passed with `-D warnings`.
- Frontline formatting and owned-file `git diff --check`: passed.

The performance measurements above precede this final ownership stamp addition;
production integration/performance gates remain the coordinator's responsibility.
