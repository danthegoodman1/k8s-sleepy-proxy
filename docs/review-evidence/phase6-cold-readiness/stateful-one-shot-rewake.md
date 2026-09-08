# One-request stateful re-wake

Only the stateful lifecycle re-wake call is strengthened. After the existing
authoritative Cold/generation check, Kubernetes cleanup, and 11-second expiry
wait, it performs one physical HTTP GET with a 130-second total I/O deadline
inside the same 140-second outer budget used for initial cold delivery. It
requires HTTP 200 and the full saved `read:{marker}` line. Transport errors,
non-200 responses, and missing or wrong data fail immediately; no application
request is retried. The original cold write remains one request. Warm-state
observation and the exclusivity/projection scenarios are unchanged.

The known app (`scripts/kind-stateful-app.py`) emits Content-Length for every
response. The previous general helper reads through connection EOF; the
stateless wire captures already demonstrated complete responses with delayed
EOF on this same forwarded frontend path. A small re-wake-only helper uses
the existing Hyper HTTP/1 connection API for correct message framing. It opens
one socket and sends one request, with a 16 KiB HTTP buffer and 64 KiB body
limit. It has no reconnecting client. The 130-second budget encloses connect,
handshake, response headers, and body. The helper aborts and joins its owned
connection driver after success, error, or that timeout. A local JoinSet also
aborts the driver if outer timeout or caller cancellation drops the helper. No new dependency or
shared fixture framework was added; other HTTP helpers were left intact.

Completed local gates:

- [Three focused socket tests](stateful-one-shot-rewake-tests.log), actual exit 0:
  `stateful_one_shot_read_preserves_marker_and_never_retries_error` covers one
  marker-bearing 200 response and one 502 while the response peer stays open,
  with no second connection; `stateful_one_shot_deadline_closes_owned_connection`
  observes one GET and socket closure after a stalled response hits its deadline;
  `stateful_one_shot_external_cancellation_closes_owned_connection` cancels the
  caller after its request is observed and proves the owned driver closes the
  socket without waiting for the internal 130-second deadline.
- [Scoped strict clippy](stateful-one-shot-rewake-clippy.log), actual exit 0.
- Scoped rustfmt and diff checks, actual exit 0.

No production or script changes and no kind operation occurred in this packet.
The coordinator's preceding 115.63-second stateful run used the older retrying
re-wake check and does not validate this stronger contract. The deployed rerun
remains required after the separate first-request re-wake investigation.
