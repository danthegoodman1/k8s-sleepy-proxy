# SNI connection setup investigation

The investigation used unchanged production images and source. Only TLS fixture
error context and an initially failing local regression were added during
diagnosis. The cluster was released with both diagnosis namespaces retained.
The subsequent isolated implementation and its local evidence are described in
[Phase 6K](6k-implementation.md); reviewed rebuild and deployed gates remain
separate.

## Natural frozen-image observations

The coordinator's `20260908T014555Z-tls.log` passed termination in 2.88 seconds
then failed exact cold SNI with an unqualified UnexpectedEof after 3.79 seconds.
Its exact underlying cause was not captured.

The [first isolated frozen-image reproduction](frozen-pass.log) passed
termination in 3.37 seconds and the complete exact/wildcard/miss SNI suite in
9.99 seconds. Its namespace is `sleepypods-e2e-sni-diagnose`.

The [one additional natural capture](frozen-captured-failure.log), with packet
capture ready before the requests, passed termination in 2.70 seconds but failed
exact SNI in 13.15 seconds. The client failed while writing its initial request,
still handshaking with no negotiated TLS version, after 13.116 seconds. No HTTP
response was received. This namespace is `sleepypods-e2e-sni-before-capture`.

[Durable state](durable-state.log) records exact-SNI wake completion at
02:04:16.772–.775 UTC. The [scoped flow](sni-flow.jsonl), extracted from the
[connection-header/DNS capture](network-headers.jsonl), records an immediate
successful DNS answer for the current Service. At 02:04:16.816902, frontend
10.244.0.148 sent a SYN to Service 10.96.201.29:8080. The same tuple and sequence
retransmitted at 17.846, 18.869, 19.892, 20.917, 21.941, and 23.991 seconds. No
SYN-ACK, RST, application payload, or forwarded SYN to the selected Pod was
captured for that connection.

The [observed current objects](observed-objects.json) show the exact Service UID,
its owned EndpointSlice, and ready nonterminating Pod 10.244.0.151:15000. The
instance remained Running; this fixture's idle timeout is 15 minutes, and no
sleep transition occurred. [Later NAT rules](later-network-rules.log) point to
that Pod, but the flow had disappeared before the conntrack read. Early
Service-rule/conntrack behavior is therefore a hypothesis, not a proven kernel
root cause. This captured reproduction is a pending TCP connection exhausting
the existing SNI setup window, not a ConnectionRefused result or the separate
two-second idle race.

All runs reused the coordinator's final images without building, pulling,
loading, or retagging them: control plane `ef1f6d805759...`, frontline
`af67717ec9e4...`, sidecar `945993314983...`, and TLS app `80139ccce982...`.
The fixture's new context passed [scoped strict clippy](tls-fixture-clippy.log).

## Independently reproducible missed refusal path

`FrontlineTlsAdapter::passthrough_prefixed` directly calls
`TcpStream::connect` under its setup timeout, while the existing shared TCP
proxy uses the Phase 6H connect-refusal recovery. The listener discards the
adapter's returned error, leaving a client with EOF.

The new local
`passthrough_waits_for_late_listener_after_refusal_and_sends_prefix_once`
regression closes a real local listener, verifies ConnectionRefused, starts
SNI setup against it, and opens the real listener after a 100 ms closed window
inside a one-second setup budget. It requires the original ClientHello and
opaque tail exactly once and one upstream response. The unchanged production
path [fails immediately with ConnectionRefused](refusal-regression-before.log),
actual exit 101. That was the intentionally red result before the isolated Phase 6K fix.

The [first test-fixture attempt](refusal-fixture-failure.log) used a bound but
non-listening socket, which blackholes SYNs on macOS instead of refusing them.
That platform assumption was corrected before the production refusal failure
was recorded.

## Remediation decision after diagnosis

Reuse one shared TCP connection helper from both TcpProxy and SNI instead of
duplicating connection setup. Preserve SNI's existing overall setup deadline,
admission ownership, keepalive, stream/write timeouts, and exact prefix relay.
Retry only connection establishment before any TLS/application bytes are sent;
never retry a protocol handshake, request, or established relay.

That closes the demonstrated missed-refusal path. It alone cannot fix the
captured pending-SYN case: an unresolved connect consumes the whole current
setup deadline. Handling that case would additionally require shorter owned
connection attempts within the unchanged overall budget, canceling and dropping
each unsuccessful attempt before another socket is opened. DNS work must remain
bounded rather than multiplying uncancelable resolver jobs. The coordinator subsequently approved a bounded initial probing window
followed by attempts using the remaining overall budget. [Phase 6K](6k-implementation.md) records
the isolated implementation, DNS boundary, error mapping and tests.

Completion gates should cover late-listener recovery, one total deadline,
pending-attempt cancellation, no replay after TLS/application dispatch,
ClientHello/tail exactly once, admission/driver recovery, and the actual frozen
TLS/SNI deployed gate after a reviewed rebuild. The original 3.79-second EOF
must not be relabeled as proved by the distinct 13.15-second capture.
