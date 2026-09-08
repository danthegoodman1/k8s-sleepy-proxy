# Stateless metrics response framing

The deployed stateless gate reached its dedicated Prometheus scrape after the
single cold request, exact materialized layout/ownership checks, and hot request
passed. The test then failed with OS 35 while waiting for EOF. The
[retained diagnostic run](stateless-metrics-diagnosis.log) received all 4,994
bytes: HTTP 200 headers and their delimiter consumed 153 bytes, and the advertised
`Content-Length: 4841` body consumed the remainder. The port-forward logs show
the dedicated metrics connection and no forwarding error. There is no evidence
of an exporter collection stall; the fixture used connection EOF as the message
boundary despite having received the complete response.

The fixture now reads the private exporter's Content-Length framing and returns
when exactly that response has arrived. It preserves the one-second read budget,
decreases the remaining socket timeout between fragments, and retains the
two-second outer scrape timeout. Header and total-response bounds are 16 KiB and
1 MiB. Missing/duplicate/invalid lengths, transfer encoding, truncation, and
oversize responses fail explicitly. This is a narrow reader for the existing
known-length exporter response, not a generic HTTP client. No production code or
timeout was changed.

The dedicated-listener metric assertions remain exact:

- `sleepypods_runtime_control_plane_calls_total{operation="subscribe_route",outcome="success"}` is at least one.
- `sleepypods_runtime_route_cache_lookups_total{outcome="hit"}` is at least one.

The scrape does not pass through the public proxy and cannot generate the cache
hit under test. The single cold request and its deadline remain unchanged. Stage
context and owned port-forward logs now identify future failures before cleanup.

Completed local gates:

- [Four metrics tests](stateless-metrics-framing-tests.log), actual exit 0:
  `metrics_complete_content_length_response_does_not_wait_for_port_forward_eof`,
  `metrics_scrape_finishes_while_complete_response_socket_stays_open`,
  `metrics_truncated_or_oversized_content_length_response_is_rejected`, and
  `prometheus_metrics_match_exact_counter_labels_and_numeric_values`.
  The socket regression holds the real connection open until the one-second
  client read returns, so it would fail with the prior EOF reader.
- [Scoped strict clippy](stateless-metrics-framing-clippy.log), actual exit 0.
- Scoped rustfmt, `/bin/bash -n`, and diff checks, actual exit 0.

The diagnosis reused the reviewed 6H images in the isolated
`sleepypods-e2e-stateless-diagnose` namespace; it did not build newer source.
Separate startup evidence showed an initial frontend process exiting with a
transport error after 75 seconds and an automatic replacement becoming ready.
Pod age was not used as process age. An authorized namespace-scoped frontend
rollout was recorded after preserving process status, previous/current logs, and
events; those files remain under `.generated/phase6h-stateless-*`. This diagnostic
run still failed at the old metrics reader and is not a clean deployed pass.
The cluster reservation was released with the namespace retained. The
coordinator owns the clean rerun after the separately reviewed startup fix.
