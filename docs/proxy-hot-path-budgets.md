# Proxy Hot-Path Latency Budgets

Milestone 1F adds benchmark harnesses and deterministic allocation checks for
the proxy-core primitives that future frontline and sidecar binaries will call
on hot routing paths. The numbers are budgets and regression signals, not hard
CI latency gates yet, because local and CI hosts vary too much for stable
wall-clock assertions.

Run the harness with:

```sh
cargo bench -p proxy-core --bench proxy_primitives
```

For a quick compile-and-smoke run:

```sh
cargo bench -p proxy-core --bench proxy_primitives -- --sample-size 10 --measurement-time 1 --warm-up-time 1
```

## Frontline Route Lookup Benchmark

Milestone 9F adds a frontend-owned route lookup benchmark for hot positive
route-cache hits with thousands of unrelated cached routes present:

```sh
cargo bench -p frontline --bench route_lookup
```

For a quick compile-and-smoke run:

```sh
cargo bench -p frontline --bench route_lookup -- --sample-size 10 --measurement-time 1 --warm-up-time 1
```

The benchmark covers HTTP exact-host lookup, HTTP wildcard-suffix lookup,
same-host HTTP path-prefix lookup, and SNI lookup. These paths should stay
indexed by route identity components rather than scanning every cached positive
route on each hot-cache hit.

## Production Image Sidecar HTTP Load Smoke

Milestone 7E starts with a small production-image smoke for the sidecar HTTP/1.1
hot path:

```sh
./scripts/smoke-sidecar-load.sh
```

The script builds the final `sidecar` image with the root `Dockerfile`, starts a
small helper container that serves both a fixed HTTP backend and a fake
`SidecarControlPlane/ReportIdle` gRPC service, then runs the production sidecar
image in the helper container's network namespace. This keeps the sidecar's
`127.0.0.1:<app-port>` upstream target pointed at the same backend measured by
the direct-backend baseline.

The smoke client sends the same request count to the backend directly and
through the sidecar. It reports request count, failures, elapsed milliseconds,
and rough RPS for each path, then fails on request correctness errors, process
startup failures, missing tools or images, and a very conservative
sidecar/direct RPS ratio below `SLEEPYPODS_SIDECAR_LOAD_SMOKE_MIN_RATIO`
(default `0.10`). The ratio guard is only intended to catch extreme obvious
regressions; local Docker wall-clock numbers are not treated as stable latency
budgets.

Useful knobs:

```sh
SLEEPYPODS_SIDECAR_LOAD_SMOKE_REQUESTS=500 \
SLEEPYPODS_SIDECAR_LOAD_SMOKE_CONCURRENCY=16 \
./scripts/smoke-sidecar-load.sh
```

This HTTP smoke only covers the sidecar HTTP/1.1 hot path. It does not cover
frontline routing, Kubernetes, the real control plane, HTTP/2, h2c, gRPC, TCP,
WebSockets, streaming throughput, or tail latency.

## Production Image Sidecar TCP Load Smoke

Milestone 7E also includes a small production-image smoke for the sidecar TCP
hot path:

```sh
./scripts/smoke-sidecar-tcp-load.sh
```

The script builds the final `sidecar` image with the root `Dockerfile`
(`BIN=sidecar`), builds the sidecar load-smoke helper image, and starts the
helper with a deterministic TCP echo backend plus a fake
`SidecarControlPlane/ReportIdle` gRPC service. The production sidecar container
joins the helper container's network namespace with
`SLEEPYPODS_SIDECAR_MODE=tcp`, so the sidecar's `127.0.0.1:<app-port>` upstream
target points at the same TCP echo backend used by the direct-backend baseline.

The TCP smoke client opens a fixed number of streams, writes a known byte count
per stream, and validates the exact echoed bytes while reads and writes run
concurrently. It fails on byte mismatches, early close, timeouts, startup or
process failures, and missing tools or images. It reports stream count, echoed
bytes, expected bytes, failures, elapsed milliseconds, and rough MiB/s for the
direct and sidecar paths. A conservative
`SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MIN_RATIO` guard defaults to `0.05` and is
only meant to catch extreme obvious regressions; local Docker throughput is not
a stable latency or throughput budget.

Useful knobs:

```sh
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STREAMS=8 \
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONCURRENCY=4 \
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BYTES_PER_STREAM=8388608 \
SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CHUNK_SIZE=32768 \
./scripts/smoke-sidecar-tcp-load.sh
```

This smoke only covers plain TCP byte forwarding through the sidecar production
image. It does not cover Kubernetes, the real control plane, frontline routing,
TLS/SNI, HTTP/2, h2c, gRPC, WebSockets, protocol-specific streaming behavior,
tail latency, or stable large-stream throughput budgets.

## Production Image Frontline HTTP and h2c gRPC-Shaped Load Smoke

Milestone 7E also has a narrow production-image smoke for the frontline
HTTP/1.1 and h2c gRPC-shaped hot-cache paths:

```sh
./scripts/smoke-frontline-load.sh
```

The script builds the final `frontline` image with the root `Dockerfile`
(`BIN=frontline`), builds a small helper image, and starts that helper with a
fixed HTTP backend plus a fake `ProxyControlPlane` gRPC service. The fake
control plane returns a running route for `Host: app.example.test` and `/smoke`
whose backend URI points at `http://127.0.0.1:<backend-port>` inside the helper
network namespace. The production frontline container then joins that namespace,
so both the control-plane endpoint and backend are reached through loopback
while the load client connects to published `127.0.0.1:<port>` sockets and sends
the known Host header.

Before measuring the HTTP/1.1 frontline path, the script waits for one
successful frontline request. That request validates lazy route resolution
through `SubscribeRoute` and warms the route cache; the measured frontline phase
then uses the cached ready route. The helper exposes a small stats endpoint for
the fake control plane, and the script fails if the measured hot-cache
frontline phase increments `subscribe_route_calls`.

After the HTTP/1.1 phase, the script also warms and measures a prior-knowledge
h2c POST with `content-type: application/grpc`, a fixed gRPC-framed request
body asserted by the helper, a fixed gRPC-framed response body, and
`grpc-status: 0` as an HTTP/2 response trailer. The helper backend accepts both
HTTP/1.1 and h2c, so the h2c phase uses the same direct-backend baseline and the
same production frontline image. The h2c phase also fails if measured hot-cache
requests make additional `SubscribeRoute` calls.

The client sends the same request count directly to the backend and through
frontline for each protocol phase, reporting request count, failures, elapsed
milliseconds, and rough RPS. The smoke fails on correctness errors, startup or
process failures, missing tools or images, additional hot-cache `SubscribeRoute`
calls, and conservative frontline/direct RPS ratios below
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO` and
`SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MIN_RATIO` (both default `0.10`). As with
the sidecar smoke, these ratios only catch extreme obvious regressions; local
Docker RPS is not a stable latency budget.

Useful knobs:

```sh
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REQUESTS=500 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_CONCURRENCY=16 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_REQUESTS=500 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_CONCURRENCY=16 \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_HOST=app.example.test \
SLEEPYPODS_FRONTLINE_LOAD_SMOKE_PATH=/smoke \
./scripts/smoke-frontline-load.sh
```

This smoke only covers frontline HTTP/1.1 and prior-knowledge h2c
gRPC-shaped cached ready-route forwarding against a fixed fake-control-plane
route. It does not cover Kubernetes, the real control plane, TLS/SNI, negotiated
HTTP/2 over TLS, real generated gRPC clients, TCP, WebSockets, streaming
throughput, cold wake latency, route-cache invalidation behavior, or tail
latency.

## Budget Principles

- Hot forwarding paths must not perform control-plane calls, database access,
  global locking, per-request client construction additions, unbounded
  allocation, or heavy instrumentation.
- Cold wake latency is intentionally separate from hot routing latency. Waking a
  workload can be slower; forwarding to an already resolved backend should stay
  local and predictable.
- Benchmarks should use in-memory transports where possible and loopback only
  when a real socket is the behavior under test. They should not depend on
  external services or sleep as a timing mechanism.
- Allocation-sensitive tests guard deterministic pure/helper paths. Network
  benchmark timings are observational and should not fail CI on wall-clock
  noise.

## Measured Paths

| Path | Harness | Budget intent |
| --- | --- | --- |
| TCP forwarding | `tcp/proxy_streams_duplex_4k_round_trip` | Measure bidirectional copy overhead without external network variance. |
| HTTP helper work | `http/prepare_reverse_proxy_request`, `http/strip_hop_by_hop_headers` | Keep request URI rewrite and hop-by-hop header cleanup bounded and simple. |
| WebSocket relay | `websocket/proxy_streams_duplex_binary_round_trip` | Exercise frame relay over in-memory WebSocket streams without an external service. |
| TLS ClientHello/SNI parsing | `tls/parse_client_hello_sni_*` | Keep complete and fragmented ClientHello parsing bounded before TLS routing decisions are added. |
| Frontline route lookup | `route_lookup/http_exact_host_many_unrelated`, `route_lookup/http_wildcard_suffix_many_unrelated`, `route_lookup/http_same_host_many_paths`, `route_lookup/sni_exact_host_many_unrelated` | Keep hot positive route-cache lookup indexed with many unrelated cached routes present. |
| Admission/accounting | `admission/try_acquire_release`, `accounting/track_release` | Keep permit/guard operations allocation-free after setup. |
| Observability helpers | `observability/label_as_str_and_outcome_mapping` | Keep label and outcome mapping low-cardinality and allocation-free. |

## Provisional Latency Budgets

These initial budgets are intentionally conservative relative to the 1F smoke
run. They are review thresholds for future regressions, not CI pass/fail gates.
If a future change exceeds one of these budgets, the change should either reduce
the overhead, update the benchmark workload, or document why the new cost is
expected.

| Path | Benchmark | Initial budget |
| --- | --- | --- |
| TCP forwarding | `tcp/proxy_streams_duplex_4k_round_trip` | <= 10 us per 4 KiB in-memory round trip |
| HTTP request rewrite | `http/prepare_reverse_proxy_request` | <= 2 us per request |
| HTTP hop-by-hop header cleanup | `http/strip_hop_by_hop_headers` | <= 2 us per header map |
| WebSocket relay | `websocket/proxy_streams_duplex_binary_round_trip` | <= 50 us per 1 KiB in-memory binary round trip |
| TLS ClientHello/SNI parse, single record | `tls/parse_client_hello_sni_single_record` | <= 1 us per complete ClientHello |
| TLS ClientHello/SNI parse, fragmented records | `tls/parse_client_hello_sni_fragmented_records` | <= 2 us per fragmented ClientHello |
| Frontline route lookup | `route_lookup/*` | <= 1 us per hot-cache lookup with 4096 unrelated cached routes or same-host path routes |
| Admission acquire/release | `admission/try_acquire_release` | <= 500 ns per steady-state permit |
| Active connection track/release | `accounting/track_release` | <= 500 ns per steady-state guard |
| Observability label/outcome helpers | `observability/label_as_str_and_outcome_mapping` | <= 50 ns per mapping batch |

## Allocation Budgets

The deterministic allocation tests in `allocation_hot_paths.rs` enforce these
initial budgets. Setup allocations are outside the measured closures; the
budgets describe steady-state helper behavior.

| Path | Test label | Allocation budget |
| --- | --- | --- |
| Observability label helpers | `observability labels` | 0 allocations |
| Active connection track/release | `active connection track/release` | 0 allocations after setup warm-up |
| Admission acquire/release | `admission try_acquire/release` | 0 allocations after setup warm-up |
| HTTP upstream URI rewrite | `upstream request URI rewrite` | <= 2 allocations |
| HTTP static hop-by-hop cleanup | `strip static hop-by-hop headers` | 0 allocations |
| TLS ClientHello/SNI parse | `TLS ClientHello SNI parse` | <= 16 allocations for a complete SNI ClientHello |

Local route-key lookup benchmarks are covered by
`cargo bench -p frontline --bench route_lookup`. The current harness focuses on
frontline hot-cache positive lookup; broader route throughput and tail-latency
gates remain separate load-test work.
