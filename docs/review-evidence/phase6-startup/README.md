# Phase 6I: bounded initial channel setup and final process teardown

## Finding and observed evidence

Frontline's nominal 60-second control-plane startup retry budget previously checked time only after `Endpoint.connect().await` returned. The endpoint had no connection timeout. A stalled DNS/TCP/HTTP2 setup could therefore exceed the budget, and a late success could be accepted. `Subscribe` is lazy and starts in route handling under its separate five-second deadline; it is not the startup failure path.

The retained [process snapshot](phase6h-stateless-frontline-process-state.log) records the initial frontline container from 2026-09-08 00:47:55 UTC through 00:49:10 UTC (75 seconds), exit 1, then a successful replacement starting 00:49:11 UTC. Its [previous-container log](phase6h-stateless-startup-previous.log) reports `transport error`. This supports the deadline defect but does not establish whether DNS, TCP, HTTP2, or Service programming caused that connection failure. Those images preceded this fix.

Independent review also identified final runtime teardown: Tokio's blocking DNS resolver can continue after its async caller is canceled. Default `#[tokio::main]` runtime destruction waits for a running blocking worker, so an otherwise completed shutdown could remain pinned in all three binaries.

## Implementation scope

- Frontline initial Channel setup now has one absolute 60-second timer covering attempts and the existing one-second retry backoff. Pre/post attempt checks reject late success and avoid another attempt/sleep after the deadline. Cancellation wins without publishing a listener. No Subscribe, HTTP request, gRPC body, or application operation is replayed.
- Frontline registers SIGINT and SIGTERM before initial connection setup and aborts/joins its signal task when its lifecycle returns. Actual process tests prove clean signal exits during a real refused connection retry.
- Each binary has a small private runtime entrypoint: execute its existing `run`/`run_from_env` future through all owned async drain/cleanup, then allow up to one second for final Tokio teardown. This bounds leftover blocking-worker joins. The frontend 60-second initial Channel budget, control-plane existing 25-second async shutdown cap, and configured data-plane drain grace periods remain intact; the final up-to-one-second runtime teardown allowance is separate. It does not shorten control-plane RPC/controller/projection drain, sidecar or frontline request draining, or active stream lifetimes. Clean/error results retain their existing exit behavior and error context.
- Production changes are only the three binary entrypoints. There is no new shared production framework or dependency. [The source delta](6i-entrypoint-source-changes.patch) is relative to saved pre-6I copies.
- `scripts/smoke-images.sh` now expects frontline's explicit `frontline control-plane startup exceeded 60 seconds` timeout message; the deterministic test asserts that exact text. Other components' expectations are unchanged.

## Validation

- [Final frontline/sidecar packages](6i-dataplane-tests.log): 287 passed, zero failed/ignored. Frontline: 202 library, eight binary, two actual process tests. Sidecar: 64 library, nine binary, two actual process tests.
- [Control-plane binary](6i-cp-bin-tests.log): one passed. No control-plane library/runtime/projection behavior changed; this packet does not claim a fresh complete control-plane or PostgreSQL suite.
- [Strict all-target clippy](6i-all-clippy.log) passes for frontline, sidecar, and control-plane. Scoped formatting, `bash -n scripts/smoke-images.sh`, and repository diff checks pass.
- Seven frontend deterministic startup tests cover a stalled attempt, repeated quick errors, exact-deadline and late success, successful retry before deadline, cancellation during an attempt, and cancellation before the first attempt. Tests use a controlled connection future and paused time. The two actual binary tests separately cover real SIGTERM/SIGINT delivery while connection retries occur and the public listener remains closed.
- [Shared child-process fixture results](6i-all-entrypoints-regression.log) execute each binary's actual private runtime helper. The recreated old runtime teardown remains pinned past 1.25 seconds and is killed by the test parent. Bounded clean and error paths each exit around 1.01 seconds, with status 0 and 1 respectively and the controlled error text preserved. This is a deterministic held `spawn_blocking` worker model, not a fabricated network/DNS outage. The earlier frontend-specific [blocking-worker investigation](6i-blocking-worker-regression.log) is retained; the shared fixture supersedes it.

No benchmarks were rerun for this packet because it changes process startup/teardown only. The coordinator owns rebuild of all affected production images and final deployed/image-smoke validation. No image tag was rebuilt or overwritten by this implementation lane.
