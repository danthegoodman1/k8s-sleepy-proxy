# Phase 7D data-plane admission and progress evidence

Scope: data-plane listeners, shared HTTP forwarding, TCP/SNI write liveness and
metrics listener supervision. Control-plane stream/subscription bounds are owned
by the separate CP lane. No commits or pushes.

## Implementation

`ProxyResourceConfig` provides validated finite bounds. Frontline's HTTP, TLS and
SNI listeners share one `ProxyAdmission`; sidecar HTTP/TCP uses its runtime's
admission state. Accepted sockets and initial setup must acquire capacity before
a connection task is spawned. HTTP handlers acquire request capacity before
application future creation, and h2 advertises a finite stream cap before service
dispatch. There are no admission wait queues. Excess sockets are closed; excess
requests return 503 with Retry-After: 1.

Socket admission lives inside I/O, surviving TLS wrapping and Hyper upgrades.
WebSocket upgrade state retains request admission after 101. Ordinary request
admission and the HTTP proxy's drain permit follow upload bodies and produced
request/response buffers through actual delivery, including final EOS bytes
queued behind h2 flow control.
Idle keepalive sockets consume socket capacity without consuming active-work
capacity.

Delivery tracking uses actual buffer advancement, so an upstream/application body
that has not produced a frame is exempt from the write deadline. A response owns
its watchdog, which holds only a weak reference to delivery state; dropping the
last queued buffer/body aborts that watchdog. The request slot also remains owned
until an aborted watchdog is dropped, bounding cleanup tasks during rapid short
responses. Publishing pending bytes and their progress timestamp is synchronized
per response. Hyper does not expose an external individual-stream reset handle;
a response delivery stall closes its HTTP connection and cancels sibling streams
on that same connection. Upload stalls cancel/poison the selected pooled upstream
HTTP connection, potentially canceling sibling requests using that upstream h2
connection. Uploads remain owned after an early response, including their last
queued EOS bytes. Empty-body requests avoid connection-metadata capture.

The HTTP connector now bounds DNS/connect setup, socket writes, and total active
plus idle pooled HTTP upstream sockets. A real Tokio pool timer evicts idle
connections, and per-origin idle retention is finite. A saturated HTTP upstream
pool fails with 503 until active sockets close or idle eviction frees capacity.
Existing same-origin keepalive reuse remains. This pool limit covers HTTP only;
WebSocket/TCP/SNI upstream sockets are bounded by admitted active sessions.

Upstream response-header inactivity resets when Hyper advances nonempty upload
buffers, not when an application merely produces or polls a frame. Established
HTTP response bodies and quiet gRPC bodies have no total lifetime deadline.
TCP/SNI keep the separate one-hour session-idle policy; the configurable write
budget applies only to pending writes and shutdown, and the setup budget governs
connection establishment. Metrics uses 32 sockets, one scrape per connection and
a fixed five-second setup/collection/write lifetime, with completed task reaping
and cancellation during shutdown.

## Validation

- Twelve tests through the actual sidecar listener pass: silent setup flood and
  partial h2 preface, keepalive socket saturation/recovery, global h2 request
  saturation and stream cancellation, advertised h2 transport capacity, retained
  WebSocket socket/request ownership beyond 101 and setup duration, header-stall
  504/recovery, slowly progressing uploads, responsive h2 peers withholding only
  stream flow-control credit, final two-byte EOS drain ownership with one byte of
  receive credit, TCP pending-write limits without shortening quiet streams, an
  early response with an open upload, and a final queued upload byte stalled by
  a one-byte upstream h2 window. The latter two have retained failing-before and
  passing-after evidence.
- Frontline tests cover shared HTTP/TLS/SNI setup saturation and TLS recovery,
  environment validation, and SNI pending-write behavior.
- A real HTTP pool test proves same-origin connection reuse, a two-socket global
  origin-churn cap, explicit saturation, timer-driven idle eviction and recovery.
- Focused delivery tests cover fresh data after a long idle gap and watchdog
  teardown retaining request capacity until its task is dropped. I/O tests prove
  successful flush cannot reset a blocked write deadline and idle I/O has no
  total-duration cap.

The final post-upload-fix package suite passed 380 tests, with no ignored tests
(`tests.log`). All-target clippy passed with warnings denied (`clippy.log`), scoped
formatting passed, and `git diff --check` passed. The workspace formatting check
only reported the separate CP lane's in-progress edits. Independent review also
passed both early-upload listener regressions and all 90 proxy-core tests
(`reviewer-early-upload.log`, `reviewer-core-final.log`). The release
comparison uses identical `sidecar_admission_bench.rs` against archived dc7cac2
and current code with production composite sinks (stderr plus Prometheus).
It is an overall before/after observation, including earlier accepted changes,
not isolated attribution to 7D. Existing load/primitive budgets are unchanged.

Independent skeptical review explicitly approved this bounded data-plane
implementation substage after reviewing the final upload ownership, regression
evidence and measured cost. Coordinator-owned release gates below remain required.

## Actual-listener measurements

The retained harness is
`crates/sidecar/examples/sidecar_admission_bench.rs`; the archived baseline uses
the identical file. It starts the production sidecar listener, a real upstream
HTTP server, and the production composite stderr/Prometheus recorder. Each path
verifies 500,000 responses using 16 concurrent clients after warmup. Every process
measures direct and sidecar paths for both HTTP/1 and HTTP/2. There are three
baseline/current pairs, ordered baseline/current, current/baseline, then
baseline/current. Other agents held CPU-heavy work throughout these measurements.

| Protocol | Baseline proxy req/s, median (range) | Current proxy req/s, median (range) | Throughput change | Baseline/current p99, median |
| --- | ---: | ---: | ---: | ---: |
| HTTP/1 | 71,342 (70,962–71,448) | 62,689 (62,348–62,721) | −12.13% | 0.3145 / 0.3707 ms |
| HTTP/2 | 39,481 (39,259–39,530) | 35,264 (34,783–35,644) | −10.68% | 0.6458 / 0.7006 ms |

All 12 million measured responses had the expected status and bytes. Individual
paths ran for several seconds; process runtimes were 30–34 seconds. Current p99
ranges were 0.3704–0.3799 ms for HTTP/1 and 0.6896–0.7070 ms for HTTP/2.
The median sidecar/direct throughput ratios were 0.4944→0.4425 for HTTP/1 and
0.5671→0.4818 for HTTP/2. The tiny response and persistent-connection workload
is deliberately different from the production image load smoke, so its ratios
are not substituted into that smoke's existing budgets. The cumulative cost is
visible and is not attributed solely to admission. This is a single-host local
observation, not a claim about deployment throughput or linked binary size.

Every process emitted only 590–594 bytes of stderr containing lifecycle drain
observations. Admission did not introduce per-request stderr emission. All six
final logs and stderr files are retained as `listener-{baseline,after}-{1,2,3}`;
`listener-results.json` contains the parsed rows. Earlier measurements before the
upload correction remain under `pre-upload-listener/` and are not used above.

Reproduce from the repository root by archiving `dc7cac2` into
`.generated/phase7-admission-baseline`, copying the retained harness into that
archive's `crates/sidecar/examples/`, and building both versions:

```sh
cargo build --manifest-path .generated/phase7-admission-baseline/Cargo.toml -p sidecar --example sidecar_admission_bench --release --offline --target-dir .generated/phase7-admission-baseline-target
cargo build -p sidecar --example sidecar_admission_bench --release --offline
python3 docs/review-evidence/phase7-admission/measure-listener.py
```

The measurement script runs prebuilt binaries sequentially and retains every
round. The coordinator owns the remaining full Criterion comparison and three
strict production image load rounds per data-plane configuration. Those gates
remain required and their budgets remain unchanged.
