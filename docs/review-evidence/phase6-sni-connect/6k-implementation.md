# Phase 6K: bounded connection recovery

This packet was implemented and independently approved in the isolated copy
`/private/tmp/sleepypods-6k-ywt2tdcf`. Development did not change production image
tags. The local checks below cover that copy, based on the frozen Phase 6I tree;
merged Phase 6J/6K checks are recorded separately below when completed. The
coordinator owns rebuilt images, deployed TLS/SNI and performance gates. Local
tests do not establish that those deployed gates pass.

## Policy and ownership

`proxy-core/src/connect.rs` centralizes pre-dispatch retry timing. A completed
connect-stage error with `ConnectionRefused` or `TimedOut` in its error chain
may retry after 50 ms. The initial probing window is
`min(1s, available setup budget / 4)`. Each attempt started in that window uses
its remaining probing time as the TCP cap; later attempts receive the full
remaining total budget. Each completed/dropped attempt precedes the next.
A continuing initial stall normally causes one short attempt and one longer
replacement. Multiple completed errors may cause additional early attempts,
bounded by that finite window and 50 ms cadence.

The clock-based window does not infer which address timed out. Hyper divides
its TCP timeout among addresses and preserves the first address error, so an
earlier refusal can hide a later timeout. An initial implementation inferred
rotation from the error; a proposed elapsed-versus-full-cap check was also
insufficient for divided timeouts. Independent review caught that nuance before
integration. The deterministic multi-address regression reports refusal after
only half each supplied cap, then proves the finite window still ends and a
three-second replacement succeeds under the remaining budget.

The existing total setup budget is unchanged. This is a connectivity tradeoff:
a slow first connection may be canceled during early probing to escape a stale
SYN flow, and a fresh attempt may recover; a continuing network outage can still exhaust the
budget. This does not attribute the original 3.79 s EOF to the distinct 13.15 s
captured pending-SYN case. Kernel Service/conntrack timing remains a hypothesis.

HTTP/1 and HTTP/2 keep one upstream admission permit around the entire
`LimitedConnector` call. Each attempt changes Hyper's `set_connect_timeout`,
then awaits `HttpConnector::call` directly. The installed hyper-util 0.1.20
`call_async` awaits DNS before constructing `ConnectingTcp`; its timeout wraps
only each TCP socket future. Its address/family fallback behavior remains
unchanged. A completed DNS Refused/TimedOut error can also be retried by the
error-chain classifier; it is not proof that a socket produced that error.
Slow DNS is never canceled by the short TCP timer or overlapped by another
attempt. At the single total deadline, setup stops without another lookup.
Blocking OS DNS work may outlive caller cancellation until the resolver returns;
this policy adds no overlapping resolver jobs within a setup.

TCP, WebSocket and SNI share `connect_tcp_attempts`: resolve once, then retry
only the resolved address slice. Public `connect_tcp` supplies the one total
TCP setup timeout for TCP and SNI. WebSocket uses the internal attempts function
inside its existing single whole-handshake timeout, preserving its 504 mapping
without competing nested overall deadlines. TCP exhaustion still returns
`TcpProxyError::ConnectTimeout`; SNI exhaustion is an `io::TimedOut` connect
error. Existing HTTP failure mapping is unchanged. All permit/drain ownership
remains outside the attempts. No handshake, HTTP request/body, ClientHello or
established stream is replayed.

The SNI adapter now uses the shared connector and retains its original
keepalive, stream/write deadlines, and exact prefixed relay. No listener,
route lookup, resource configuration, package dependency or image tag changes
are included. Phase 6J's independent HTTP activation ownership changes are not
present in this isolated packet.

## Completed checks

All final commands below completed with actual exit 0:

- [Shared policy/DNS tests](6k-connect-tests.log): 10 passed. Controlled pending
  futures assert the old socket guard is dropped before the next attempt;
  a 3 s replacement succeeds after a 1 s early timeout inside a 10 s total budget;
  persistent stalls/refusals expire at one exact paused-clock deadline;
  cancellation stops attempts; nonretryable errors return once. Actual Hyper
  with a delayed resolver proves a 100 ms DNS resolution survives a 75 ms TCP cap
  and creates one lookup; an overall deadline cancels without another lookup.
- [Actual protocol connector tests](6k-protocol-connect-tests.log): 11 passed.
  HTTP/1, HTTP/2, WebSocket and TCP recover a real initial refusal and deliver
  bytes once. Established HTTP requests are not replayed after reset; canceled
  setup stops dialing and releases pool/drain ownership. New explicit
  persistent-refusal WebSocket coverage requires 504 and permit recovery.
- [SNI adapter tests](6k-sni-tests.log): 5 passed. The previously red late-listener
  case now delivers ClientHello/tail exactly once; post-ClientHello close never
  redials; timeout and external cancellation close the client and stop dialing.
- [Complete data-plane suites](6k-data-plane-tests.log): 401 tests passed across
  proxy-core, frontline and sidecar, zero failures or ignored tests. Includes
  existing real-listener admission, timeout and process shutdown regressions.
- [All-target strict clippy](6k-clippy.log) and [workspace fmt check](6k-fmt.log)
  passed. No performance improvement is claimed from these functional tests.

The [first local-socket attempt](6k-connect-tests-sandbox-denial.log) had two
sandbox PermissionDenied failures while six pure policy tests passed. It was
rerun with approved loopback access. The first broader test/clippy commands
failed because the initially bounded scratch copy omitted unchanged root
`tests/support/runtime_shutdown.rs`; both
[failed test](6k-data-plane-tests-missing-scratch-test-support.log) and
[failed clippy](6k-clippy-missing-scratch-test-support.log) attempts are retained.
Copying that unchanged support file fixed the scratch packaging error before
the complete passing runs. None of those failed attempts is counted as a pass.

The [timer-precision fixture failure](6k-connect-tests-timer-precision-fixture-failure.log)
retains two overly exact assertions from the probing-window refinement: actual
Instant elapsed time reduced a 75 ms cap by nanoseconds, and Tokio rounded a
37.5 ms timer step to milliseconds. Assertions now require a bounded positive
DNS cap and a strictly decreasing probing sequence, while retaining exact total
paused-clock expiry and three-second replacement success.

Independent review explicitly approved the final finite-window policy and source.
[Final policy/DNS review tests](6k-reviewer-connect-policy-final.log) passed 10/10;
[protocol review tests](6k-reviewer-protocol-connect.log) passed 11/11 and
[SNI review tests](6k-reviewer-sni-connect.log) passed 5/5. The final policy review
includes the multi-address masked-timeout regression.

## Integration with Phase 6J

The exact approved patch was applied to the current root tree after Phase 6J,
preserving its HTTP initial-work ownership, listener budget validation and
operator/API changes. The [merged complete data-plane suites](6k-merged-data-plane-tests.log)
completed with exit 0: **410 tests passed, zero failed or ignored**.
[Strict all-target merged clippy](6k-merged-clippy.log), scoped rustfmt and
`git diff --check` also completed with exit 0. No images were built and no kind
operation was performed during this integration. The coordinator's deployed
and performance gates remain required.
