# Phase 7 protocol implementation evidence

Date: 2026-09-07. Scope: 7E and 7G; protocol setup/write/close liveness from 7D.
This packet does not mark the wider Phase 7 admission or dependency gates done.

## Implementation

- One `proxy_core::serve_http_connection` uses Hyper auto HTTP/1/HTTP/2 with
  upgrades. Sidecar removes its byte-prefix protocol classifier and separate
  first-request WebSocket path. Both actual listeners now accept a later
  keep-alive request's upgrade.
- One `WebSocketProxy::prepare_upgrade` validates and connects before 101. Each
  hop regenerates the key/accept challenge, strips hop-specific headers,
  forwards end-to-end request/response headers and validates selected protocol.
  Extension offers are deliberately omitted and unsolicited selections fail.
- Hyper's upstream HTTP client is used for the WebSocket handshake because
  tungstenite's connect helper returns only an already-buffered rejection-body
  prefix. Rejections preserve status, headers and the entire body under a 64 KiB
  cap and a setup deadline. Backend origins remain cleartext `ws`, matching the
  frontline routing contract; TLS termination remains at the edge.
- Per-frame writes, close writes/flushes and close replies have bounded waits.
  TCP idle tracking now observes partial-write progress and bounds write/shutdown
  stalls as well as reads. No active-stream lifetime timeout was introduced.
- Removed unused accepted-before-upstream forwarding wrappers. The standalone
  raw WebSocket convenience method runs the same HTTP/upgrade production helper.
  Sidecar and frontline own, reap and abort their upgrade tasks during drain.

## Correctness evidence

`cargo test -p proxy-core -p sidecar -p frontline --offline`: **356 passed**, no
ignored tests. `tests.log` retains the complete run. `clippy.log` records a clean
`cargo clippy -p proxy-core -p frontline -p sidecar --all-targets --offline -- -D warnings`.

New production-path regressions include:

- `authenticated_websocket_preserves_handshake_through_both_proxies_after_keepalive`:
  actual frontline → sidecar → app, Authorization/Cookie/Origin/custom headers,
  large headers, edge trust policy, selected offered protocol, duplicate cookies,
  extension omission, echo and close; a second pass upgrades a sidecar keep-alive
  connection directly because the edge creates a new sidecar connection for WS.
- `websocket_rejection_status_headers_and_complete_body_survive_both_proxies` and
  `websocket_forbidden_and_invalid_negotiation_never_publish_101`: 401, 403,
  WWW-Authenticate, Set-Cookie, body, unoffered protocol and unsupported extension.
- Silent/partial HTTP/1/partial h2 setup expires while an active HTTP request
  survives the setup deadline. Existing pooled keep-alive, h2/gRPC streaming,
  idle report, drain, SIGTERM and process-restart suites also pass.
- WebSocket upstream setup timeout returns 504; cancellation releases the permit
  and upstream connection; blocked data write, blocked close write and absent
  close reply terminate at their respective deadlines. An established idle
  WebSocket survives longer than the configured write deadline.
- TCP directions simultaneously blocked on write terminate at their idle
  deadline; one-way activity and half-close behavior remain covered.

The original WebSocket upstream-disconnect test waited to observe active count
one after the app had already disconnected. The test now explicitly releases
that disconnect after observing the active session, removing a timing race.
Obsolete sidecar sniffer implementation tests were replaced with tests of the
shared production parser and actual keep-alive upgrades.

## Remaining integration evidence

Independent skeptical review, production image/load budgets and kind
protocol/TLS gates are required before closing the phase. See `docs/proxy-protocol-contract.md` for the explicit runtime
contract and limits. Ordinary HTTP response-header idle policy and global
connection/request/subscription admission remain the subsequent resource stage.


## Primitive performance measurements

Both runs used the same Criterion harness, 40 samples, one-second warmup and
three-second measurement windows, with no other coordinator performance gate
running. The before run used the archived `dc7cac2` source tree with only the
new idle-aware TCP benchmark copied into it. Logs retain compiler output and
confidence intervals. These in-memory duplex tests measure relay work and task
setup; they do not establish production network throughput or handshake latency.

| Benchmark | Baseline central estimate | After central estimate | Change |
| --- | --- | --- | --- |
| TCP 4 KiB round trip with idle tracking | 2.8295 µs | 2.9487 µs | +4.2% |
| WebSocket 1 KiB binary round trip | 14.580 µs | 15.013 µs | +3.0% |

The measured costs include the added bounded progress/write checks. The existing
TCP benchmark without an idle deadline remains separate; comparing only that
path would not exercise the changed production primitive. No performance
improvement is claimed. Image/load gates remain the coordinator's integration
check. Subsequent IPv6 host normalization and forwarded-header policy fixes do
not execute in either measured established-stream primitive.

## Skeptical review fixes

- Normalize IPv6 authority brackets before tuple address resolution. A real
  `[::1]` upstream handshake, bidirectional echo and close regression now passes.
- Consume client Connection nominations before installing canonical edge
  forwarding metadata. Ordinary HTTP and the full authenticated WebSocket chain
  now include nominations of X-Forwarded-For/Proto/Host and require all canonical
  values to reach the app. Upgrade detection remains shared and the rebuilt
  WebSocket Connection/Upgrade fields stay valid.

`review-fixes.log` retains the initial IPv6 fix and a temporary cross-phase
compile failure. The clean final reruns are in `core-review-fixes.log` and
`frontline-review-fixes.log` (all 21 listener tests passed), with final denied-warning
checks in `clippy-review-fixes.log`. Approval is
recorded by the coordinator only after the independent reviewer is satisfied.
