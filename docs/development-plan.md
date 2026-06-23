# SleepyPods Development Plan

This plan turns the SleepyPods production north star into implementation
milestones. Each milestone should have integration coverage, and Kubernetes
behavior should be verified with kind-based end-to-end tests before it is
considered done.

## Implementation Principles

- Prefer the simplest implementation that satisfies the current milestone and
  its tests.
- Keep code surface area small. Scalability and maintainability should come from
  clear boundaries, predictable state machines, and fewer moving parts before
  they come from clever abstractions.
- Avoid "mega files". Split code by responsibility when a file starts combining
  unrelated protocol, state-machine, persistence, or Kubernetes concerns, while
  avoiding abstraction for its own sake.
- Add abstractions only when repeated behavior or testability makes the benefit
  concrete.
- Do not defer useful comments. Comments should explain protocol edge cases,
  lifecycle invariants, reconciliation assumptions, and places where a future
  maintainer could otherwise make a dangerous simplification.
- Keep each sub-phase reviewable. A phase is not done until the narrowest useful
  integration test proves the behavior works.
- Treat low-latency hot paths as an explicit design constraint. Proxy request
  forwarding, stream forwarding, and local route-cache hits should avoid
  control-plane calls, database access, global locks, unbounded allocation, and
  per-request client construction.
- Keep production containers small and explicit. The control plane, frontline
  proxy, and sidecar should use `scratch` final images when practical; if a
  component needs runtime files such as CA roots, timezone data, passwd/group
  entries, or certificates, copy in only those files deliberately.

## Testing Strategy

Use four test layers:

1. Unit tests for pure logic:
   - route key normalization
   - opaque subscription ID handling
   - wildcard and longest-prefix matching
   - state-machine transitions
   - template value validation
   - structured manifest rendering
   - PV/PVC field rendering, including names, labels, access modes, capacity,
     reclaim policy, volume source, and instance value substitution

2. Component integration tests:
   - proxy components against fake control-plane services
   - control plane against a real test database
   - Kubernetes materializer against a real or fake API server where useful
   - Kubernetes object assertions for PV/PVC binding intent, owner labels,
     generation labels, and workload volume mounts

3. Protocol integration tests:
   - HTTP/1.1
   - HTTP/2
   - h2c gRPC
   - gRPC-Web for operator-facing control-plane APIs
   - WebSockets
   - TLS termination
   - TLS/SNI passthrough
   - HTTP-01 challenge handling
   - PostgreSQL/libpq 17+ SNI passthrough with `sslnegotiation=direct`, using
     a real Postgres workload as proof that SNI routing works for application
     code we did not write

4. kind end-to-end tests:
   - build the final production-style images for the control plane, frontline
     proxy, and sidecar
   - load images into kind
   - deploy control plane, frontline proxy, sidecar, and demo workloads
   - create `WorkloadClass`, `Instance`, and `RouteBinding`
   - verify cold wake, hot routing, drain/sleep, and re-wake
   - verify PV/PVC materialization before workload creation, intended binding,
     pod mount behavior, and data continuity across sleep/re-wake
   - verify custom host/SNI routing and HTTP-01 challenge lookup
   - verify PostgreSQL/libpq 17+ connects through TLS/SNI passthrough using
     `sslnegotiation=direct`; pin the image version in CI rather than relying
     on a floating `latest` tag

The kind suite is a release gate. Unit and component tests are not enough for
this project because most failures will happen at Kubernetes object lifecycle,
networking, and readiness boundaries.

Container tests must exercise the same final images that operators would run,
not local binaries or dev-only images. At minimum, test startup, health/readiness,
TLS/CA access, database and Kubernetes API connectivity, non-root execution, and
the absence of accidental runtime dependencies such as shells or package
managers.

Each milestone should name the important success, failure, reconnect, timeout,
and race cases before implementation starts. Avoid broad "works end to end"
claims without assertions for the state that makes the behavior correct.

## Milestone 1: Rust Proxy Primitives

Build the shared network and proxy building blocks used by both frontline and
sidecar binaries.

Scope:

- Tokio runtime setup and structured shutdown.
- Bidirectional TCP stream proxying.
- HTTP reverse proxy helpers.
- WebSocket upgrade and proxying.
- Active request and connection accounting.
- Drain tracker with configurable grace timeout.
- Timeout and backpressure primitives.
- TLS ClientHello/SNI extraction helpers.
- Shared metrics and tracing conventions.
- Hot-path latency budgets and benchmark harnesses for proxy primitives.

Sub-phases:

- 1A: TCP stream proxy, active connection accounting, and drain tracker.
- 1B: HTTP reverse proxy helpers and WebSocket upgrade/proxying.
- 1C: Timeout, backpressure, structured shutdown, and cancellation behavior.
- 1D: TLS ClientHello/SNI extraction helpers.
- 1E: Shared metrics and tracing conventions.
- 1F: Proxy hot-path latency budgets, benchmark harnesses, and allocation checks.

Done when:

- Unit tests cover accounting, drain, timeout, and SNI parsing.
- Integration tests proxy TCP streams, HTTP requests, and WebSocket sessions.
- TCP tests cover byte preservation, half-close behavior, upstream reset, client
  reset, timeout, and backpressure.
- HTTP tests cover HTTP/1.1 keep-alive, chunked bodies, large bodies, streaming
  request/response bodies, HTTP/2 multiplexing, and cancellation.
- WebSocket tests cover upgrade failure, bidirectional traffic, close frames,
  peer disconnect, and backpressure.
- Drain tests prove new work is rejected while existing streams get the grace
  period.
- Hot-path benchmarks cover TCP forwarding, HTTP forwarding, WebSocket relay,
  TLS ClientHello/SNI extraction, local route-key lookup primitives, and
  admission/accounting overhead.
- Benchmark notes separate hot routing latency from cold wake latency; cold wake
  can be slower, but hot proxy paths must not depend on control-plane calls,
  database access, per-request client construction, or unbounded allocation.
- Allocation-sensitive tests or profiles exist for the hot path so regressions
  are visible before the frontline proxy is built on top of these primitives.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Tokio runtime setup and structured shutdown. | `crates/proxy-core/src/shutdown.rs` and `tests/lifecycle.rs` cover cancellation tokens, child tokens, late waiters, and shutdown propagation. |
| Complete | Bidirectional TCP stream proxying. | `crates/proxy-core/tests/tcp_proxy.rs` proves byte preservation, half-close behavior, and lifecycle accounting. |
| Complete | HTTP reverse proxy helpers. | `crates/proxy-core/src/http.rs` tests request rewriting and hop-by-hop stripping; `tests/http_proxy.rs` covers basic request/response forwarding and drain rejection. |
| Complete | WebSocket upgrade and proxying. | `crates/proxy-core/tests/websocket_proxy.rs` covers bidirectional messages, close propagation, lifecycle, and drain rejection. |
| Complete | Active request and connection accounting. | `crates/proxy-core/src/accounting.rs` and `src/admission.rs` cover guard lifetime, idempotent release, waiters, limits, and cancellation. |
| Complete | Drain tracker with configurable grace timeout. | `crates/proxy-core/src/drain.rs` covers rejecting new work, waiting for active work, and timeout reporting. |
| Complete | Timeout and backpressure primitives. | `src/timeout.rs` covers typed timeout results; TCP/WebSocket proxy tests exercise async copy paths. |
| Complete | TLS ClientHello/SNI extraction helpers. | `src/tls.rs` covers valid SNI, malformed input, missing SNI, invalid hostnames, and fragmented prefix reads. |
| Complete | Shared metrics and tracing conventions. | `crates/proxy-core/src/observability` descriptor tests assert metric names, labels, and trace fields. |
| Complete | Hot-path latency budgets and benchmark harnesses for proxy primitives. | `crates/proxy-core/benches/proxy_primitives.rs`, `crates/frontline/benches/route_lookup.rs`, and `docs/proxy-hot-path-budgets.md` cover proxy primitives plus local route-key lookup budgets. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 1A: TCP stream proxy, active connection accounting, and drain tracker. | `tests/tcp_proxy.rs`, `src/accounting.rs`, and `src/drain.rs` cover the primitive behavior. |
| Complete | 1B: HTTP reverse proxy helpers and WebSocket upgrade/proxying. | `src/http.rs`, `tests/http_proxy.rs`, and `tests/websocket_proxy.rs` cover helper behavior and core forwarding paths. |
| Complete | 1C: Timeout, backpressure, structured shutdown, and cancellation behavior. | `src/timeout.rs`, `src/shutdown.rs`, and lifecycle tests cover typed timeout and cancellation; deeper reset/backpressure protocol cases remain done-criteria gaps below. |
| Complete | 1D: TLS ClientHello/SNI extraction helpers. | `src/tls.rs` tests cover fragmented and malformed ClientHello handling. |
| Complete | 1E: Shared metrics and tracing conventions. | Observability descriptor tests cover stable metric and trace field definitions. |
| Complete | 1F: Proxy hot-path latency budgets, benchmark harnesses, and allocation checks. | Primitive benches, allocation tests, hot-path budget docs, and the frontline route-key lookup benchmark are present. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Unit tests cover accounting, drain, timeout, and SNI parsing. | `src/accounting.rs`, `src/admission.rs`, `src/drain.rs`, `src/timeout.rs`, and `src/tls.rs` contain targeted tests. |
| Complete | Integration tests proxy TCP streams, HTTP requests, and WebSocket sessions. | `tests/tcp_proxy.rs`, `tests/http_proxy.rs`, and `tests/websocket_proxy.rs` cover these paths. |
| Complete | TCP tests cover byte preservation, half-close, upstream reset, client reset, timeout, and backpressure. | `tests/tcp_proxy.rs` covers byte preservation, half-close, connect errors, deterministic connect timeout, client disconnect, clean upstream disconnect, OS-level upstream reset, drain timeout while stalled, and bounded-duplex backpressure. |
| Complete | HTTP tests cover keep-alive, chunked/large/streaming bodies, HTTP/2 multiplexing, and cancellation. | `tests/http_proxy.rs` covers HTTP/1.1 keep-alive, chunked request bodies, large request/response bodies, streaming request bodies before client completion, streaming response lifecycle, cancellation while upstream is pending, and concurrent HTTP/2 streams. |
| Complete | WebSocket tests cover upgrade failure, bidirectional traffic, close frames, peer disconnect, and backpressure. | `tests/websocket_proxy.rs` covers upgrade failure, bidirectional traffic, close frames, client and upstream peer disconnect, large-frame forwarding, and true slow-downstream-peer backpressure over a bounded stream. |
| Complete | Drain tests prove new work is rejected while existing streams get the grace period. | `src/drain.rs`, `tests/http_proxy.rs`, and `tests/websocket_proxy.rs` cover drain rejection and grace-timeout behavior. |
| Complete | Hot-path benchmarks cover TCP, HTTP, WebSocket, TLS SNI, local route-key lookup, and admission/accounting. | `proxy_primitives` covers TCP, HTTP, WebSocket, TLS SNI, admission, and accounting; `route_lookup` covers local frontline route-key lookup. |
| Complete | Benchmark notes separate hot routing latency from cold wake latency and forbid control-plane calls on hot paths. | `docs/proxy-hot-path-budgets.md` documents hot-path budgets, smoke commands, and current limitations. |
| Complete | Allocation-sensitive tests or profiles exist for the hot path. | `crates/proxy-core/tests/allocation_hot_paths.rs` covers observability, accounting/admission, HTTP helpers, and TLS SNI parsing. |

## Milestone 2: Control Plane Resource Model

Introduce the durable model behind the control-plane API.

Scope:

- Domain-specific `ControlPlaneStore` trait for persistence operations and
  transactional invariants.
- Protobuf-defined control-plane API exposed over native gRPC and gRPC-Web for
  operator-facing methods.
- Postgres as the first store provider.
- Control-plane config for selecting the store provider.
- `WorkloadClass` with immutable versions.
- `Instance` pinned to a `WorkloadClass` version.
- `RouteBinding` for host, wildcard host, SNI, and optional path prefix.
- `Materialization` for active cluster projections.
- HTTP-01 challenge records keyed by `(host, token)`.
- Instance state machine with generation checks.
- Structured manifest rendering from `WorkloadClass + Instance.values`.

Sub-phases:

- 2A: Domain-specific `ControlPlaneStore` trait and provider config shape.
- 2B: Protobuf service definitions, native gRPC server, and gRPC-Web transport
  for operator-facing APIs.
- 2C: Postgres schema, migrations, and store implementation.
- 2D: `WorkloadClass` versioning, schema validation, and immutable version
  behavior.
- 2E: `Instance` APIs, value validation, generation fields, and idempotent
  create/update behavior.
- 2F: `RouteBinding` model, host/SNI/path/wildcard resolver, and uniqueness
  constraints.
- 2G: Instance state machine with generation/CAS transitions.
- 2H: HTTP-01 challenge store with put, resolve, delete, expiry, and GC.
- 2I: Structured manifest renderer for Deployment, StatefulSet, Service, PV,
  and PVC.

Done when:

- Database migrations and store tests pass against a real test database.
- Store conformance tests cover idempotency keys, transaction rollback,
  duplicate route/domain rejection, concurrent create/update conflicts, CAS
  generation failures, materialization generation updates, and provider config
  errors.
- The control plane can construct the configured store provider.
- Native gRPC and gRPC-Web integration tests exercise the same operator-facing
  APIs for workload classes, instances, route bindings, and HTTP-01 challenges.
- gRPC-Web tests cover CORS/preflight behavior when enabled, metadata/auth
  propagation, structured error mapping, and V8-compatible generated clients or
  request encoding.
- Tests document that proxy `Subscribe` is native gRPC-only in V1 and is not
  exposed as a gRPC-Web bidirectional stream.
- State-machine tests cover wake, running, draining, failed, retry, and delete.
- State-machine tests cover concurrent wake calls, sleep while waking, delete
  while waking or draining, failed wake retry, stale sidecar reports, and stale
  materialization updates.
- Route resolver tests cover host normalization, exact host versus wildcard
  precedence, wildcard specificity, longest path-prefix match, SNI/custom-domain
  uniqueness, and misses.
- HTTP-01 store tests cover put, overwrite/idempotency rules, wrong host/token,
  expiry, delete, and garbage collection.
- Manifest rendering tests cover Deployment, StatefulSet, Service, PV, and PVC.
- WorkloadClass version updates cannot mutate existing pinned instances.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Domain-specific `ControlPlaneStore` trait. | `crates/control-plane/src/store.rs` exposes domain operations for instances, workload classes, route bindings, materialization, dependencies, and HTTP-01 records. |
| Complete | Protobuf API over native gRPC and gRPC-Web for operator-facing methods. | `crates/control-plane/tests/api_transport.rs` covers native generated dispatch plus store-backed gRPC-Web HTTP/1.1 `application/grpc-web+proto` framed unary calls for workload class, instance, route binding, and HTTP-01 operator APIs. |
| Complete | Postgres as first store provider. | `crates/control-plane/src/postgres/`, `crates/control-plane/tests/postgres_store.rs`, and migrations implement the first provider. |
| Complete | Control-plane config for selecting the store provider. | `runtime.rs` parses `SLEEPYPODS_STORE_PROVIDER` and Postgres URL config, with runtime tests. |
| Complete | `WorkloadClass` with immutable versions. | Postgres conformance creates and reloads immutable versions and proves v2 does not mutate v1. |
| Complete | `Instance` pinned to a `WorkloadClass` version. | `create_instance` conformance covers pinned class version and value validation. |
| Complete | `RouteBinding` for host, wildcard host, SNI, and optional path prefix. | `postgres/route_ops.rs` resolver tests and conformance cover host/SNI/path matching, uniqueness, and misses. |
| Complete | `Materialization` for active cluster projections. | `postgres/materialization_ops.rs` and conformance cover record/load/complete materialization and backend generation checks. |
| Complete | HTTP-01 challenge records keyed by `(host, token)`. | `postgres/http01_ops.rs` and conformance cover put, resolve, wrong-key miss, repeated put, overwrite, delete, expiry, and GC. |
| Complete | Instance state machine with generation checks. | `instance.rs` transition tests and Postgres conformance cover CAS generation failures, stale sidecar reports, and stale materialization updates. |
| Complete | Structured manifest rendering from `WorkloadClass + Instance.values`. | `crates/control-plane/src/manifest/render.rs` and `manifest/tests.rs` cover templates, Deployment, StatefulSet, Service, PV, and PVC rendering. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 2A: Store trait and provider config shape. | `store.rs` defines the trait and `runtime.rs` parses provider config. |
| Complete | 2B: Protobuf services, native gRPC server, and gRPC-Web operator transport. | `server.rs` builds native gRPC and gRPC-Web operator transports; `api_transport.rs` covers store-backed parity, CORS preflight, metadata/auth header propagation, structured errors, and V8-compatible framed HTTP/1.1 requests. |
| Complete | 2C: Postgres schema, migrations, and store implementation. | Real Postgres conformance applies migrations idempotently and exercises store operations when `SLEEPYPODS_POSTGRES_URL` is set. |
| Complete | 2D: WorkloadClass versioning, schema validation, and immutability. | Conformance covers class version creation/load, schema validation rejects missing/unknown values, and version immutability. |
| Complete | 2E: Instance APIs, value validation, generation fields, and idempotent create/update behavior. | Store-backed API and conformance cover create/get/delete, generation fields, idempotent replay/conflict, and rollback. |
| Complete | 2F: RouteBinding model, resolver, and uniqueness constraints. | Resolver tests cover matching semantics; conformance covers route creation, duplicate rejection, and dependency lookup. |
| Complete | 2G: Instance state machine with generation/CAS transitions. | `instance.rs` and conformance cover legal/illegal transitions and CAS behavior. |
| Complete | 2H: HTTP-01 challenge store. | `http01_ops.rs` and conformance cover put, resolve, wrong-key miss, repeated put, overwrite, delete, expiry, and GC. |
| Complete | 2I: Structured manifest renderer. | `manifest/tests.rs` covers Deployment, StatefulSet, Service, PV, PVC, sidecar config, and validation failures. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Database migrations and store tests pass against a real test database. | `scripts/test-postgres-store.sh` sets `SLEEPYPODS_POSTGRES_URL` from either the caller environment or a disposable `postgres:17-alpine` container and runs `cargo test -p control-plane --test postgres_store -- --nocapture`; verified passing against a disposable real database. |
| Complete | Store conformance covers idempotency, rollback, duplicate routes, conflicts, CAS, materialization generations, and provider config errors. | `postgres_store.rs` conformance covers these cases, including invalid connection URL and transactional rollback after duplicate route identity. |
| Complete | Control plane can construct the configured store provider. | `runtime.rs` tests cover env parsing and provider construction paths. |
| Complete | Native gRPC and gRPC-Web integration tests exercise the same operator APIs. | `api_transport.rs` covers native store-backed dispatch and gRPC-Web store-backed unary calls for representative workload class, instance create/get/delete, route binding create/get/delete, and HTTP-01 put/resolve/delete flows. |
| Complete | gRPC-Web tests cover CORS/preflight, metadata/auth, structured errors, and V8-compatible clients/request encoding. | `api_transport.rs` covers CORS preflight, authorization/custom metadata header propagation through the gRPC-Web layer, store-backed `NotFound` status/message mapping, HTTP/1.1, `application/grpc-web+proto`, and framed protobuf unary bodies. |
| Complete | Tests document proxy `Subscribe` is native gRPC-only in V1 and not grpc-web bidi. | `api_transport.rs` tests assert operator grpc-web unary shape and no proxy Subscribe exposure. |
| Complete | State-machine tests cover wake, running, draining, failed, retry, and delete. | `instance.rs` and Postgres conformance cover lifecycle edges and failed retry/deleting terminal behavior. |
| Complete | State-machine tests cover concurrent wake, sleep while waking, delete while waking/draining, failed retry, stale sidecar reports, and stale materialization updates. | Postgres conformance includes concurrent CAS, invalid transitions, stale reports, and stale materialization rejection. |
| Complete | Route resolver tests cover normalization, wildcard precedence/specificity, path-prefix, SNI uniqueness, and misses. | `postgres/route_ops.rs`, `crates/frontline/src/identity.rs`, and matcher tests cover these semantics. |
| Complete | HTTP-01 store tests cover put, overwrite/idempotency, wrong host/token, expiry, delete, and GC. | `postgres_store_conformance_against_real_database` covers put, resolve, wrong-host miss, wrong-token miss, repeated put, overwrite, delete, expiry, and GC. |
| Complete | Manifest rendering tests cover Deployment, StatefulSet, Service, PV, and PVC. | `manifest/tests.rs` covers all listed object kinds and serialization. |
| Complete | WorkloadClass version updates cannot mutate existing pinned instances. | Conformance proves v2 creation does not change v1 and instances remain pinned to the requested version. |

The store trait should express domain operations rather than generic CRUD or a
generic SQL abstraction. It should include methods for:

- creating instances transactionally with route bindings and idempotency keys
- loading and validating pinned `WorkloadClass` versions
- resolving route identity to route entries
- compare-and-swap instance state transitions by generation
- recording materialization state and backend generation
- route resolution and dependency lookup for active proxy subscriptions
- HTTP-01 challenge put, resolve, delete, expiry, and GC

Start with Postgres only. Future providers such as MySQL should implement the
same trait behind control-plane config. Avoid making public store semantics rely
on Postgres-only behavior such as `LISTEN/NOTIFY`, partial indexes, or JSONB
querying unless there is a portable fallback.

Provider-specific durability semantics should not leak into the control-plane
API or proxy protocol. Database-specific features may be used as latency
optimizations inside one store provider, but correctness must come from portable
state, transactions, uniqueness constraints, idempotency keys, and generation
checks.

V1 route propagation should be lazy and subscription-based. `SubscribeRoute`
handles cache misses on the `Subscribe` stream: it resolves the identity,
registers the proxy as actively interested in the returned route entry, and
returns either `RouteResolved` with an opaque `subscription_id` or `RouteMiss`
with a negative-cache policy. The proxy stores the subscription ID with the local
cache entry and uses it only to apply targeted messages from `Subscribe`.

`Subscribe` should be a bidirectional stream. Proxy-to-control-plane messages
subscribe route identities or unsubscribe opaque subscription IDs;
control-plane-to-proxy messages return lookup results and deliver targeted
invalidations or updates. The control plane should create the subscription before
sending `RouteResolved`, so there is no separate resolve-then-add race in the
proxy protocol.

Polling, provider change streams, versions, ordering keys, and cursors are
control-plane internals. The store provider may use whatever mechanism fits its
durability model, but the proxy protocol must not expose or require a global
monotonic route version, durable global route preload, or resumable cursor.
Reconnect behavior should rebuild through lazy `SubscribeRoute`, not cursor-based
resync.

## Milestone 3: Frontline Route Resolution

Build the frontline-specific routing behavior on top of the shared proxy
primitives.

Scope:

- Host/SNI/path identity extraction.
- Canonical route key generation.
- Opaque subscription ID storage and invalidation handling.
- Exact host and SNI lookup.
- Wildcard host lookup.
- Longest path-prefix matching.
- Bounded local route cache and `Subscribe` subscribe/unsubscribe stream
  handling.
- `SubscribeRoute` fallback on local miss.
- `WakeInstance` flow when a route is Cold or missing a backend.
- Stale generation rejection.
- HTTP-01 challenge lookup through `ResolveHTTP01Challenge`.

Sub-phases:

- 3A: Route key normalization and local exact/wildcard/path-prefix matcher.
- 3B: Bounded local route cache, cache TTLs, and negative caching.
- 3C: `SubscribeRoute` cache-miss path, `RouteResolved`/`RouteMiss` handling,
  and opaque subscription ID storage.
- 3D: `Subscribe` bidirectional stream with `Unsubscribe` input and
  subscription-targeted invalidations.
- 3E: `WakeInstance` flow, Waking wait behavior, and stale generation
  rejection.
- 3F: HTTP/1.1, HTTP/2, h2c gRPC, and WebSocket forwarding.
- 3G: HTTPS termination, SNI certificate selection, and TLS/SNI passthrough.
- 3H: HTTP-01 challenge interception and `ResolveHTTP01Challenge` lookup.

Done when:

- Fake-control-plane integration tests cover cold wake, hot route, miss, stale
  generation, targeted route update, targeted invalidation, and stream
  reconnect.
- Route matching tests cover exact host precedence over wildcard, wildcard suffix
  specificity, longest path-prefix precedence, host case normalization, optional
  port handling, trailing-dot handling, and wildcard misses.
- Cache tests cover positive TTL expiry, negative TTL expiry, bounded eviction,
  `Unsubscribe` on eviction, refresh after invalidation, and stale backend
  rejection after generation changes.
- Subscription tests cover route subscription, miss responses without
  subscription IDs, unsubscribe, idempotent duplicate unsubscribe, and
  invalidation after resolve.
- Subscription tests cover duplicate in-flight `SubscribeRoute` requests for the
  same identity, route reassignment to another instance, invalidation during
  wake, and stream backpressure.
- Stream reconnect tests prove the proxy does not rely on public cursors and can
  resubscribe kept identities or rebuild stale cache entries through lazy
  `SubscribeRoute`.
- Protocol tests cover HTTP/1.1, HTTP/2, h2c gRPC, WebSockets, TLS
  termination, SNI passthrough, and HTTP-01.
- Protocol tests include chunked and large HTTP bodies, streaming request and
  response bodies, HTTP/2 multiplexing, h2c gRPC trailers and status propagation,
  WebSocket close/backpressure, TLS passthrough byte preservation, and TCP
  half-close behavior.
- PostgreSQL SNI passthrough tests use PostgreSQL/libpq 17+ with
  `sslnegotiation=direct` so the frontline proxy can route on standard TLS SNI
  without understanding the PostgreSQL startup protocol.
- TLS tests cover SNI certificate selection, missing certificate behavior, cert
  rotation, and passthrough for unknown or non-HTTP TLS traffic.
- HTTP-01 tests cover wrong host, wrong token, expired challenge, deleted
  challenge, response content type, and challenge path precedence before normal
  route resolution.
- Unknown Host/SNI negative caching protects the fake control plane from repeat
  misses.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Host/SNI/path identity extraction. | `crates/frontline/src/identity.rs` and `tls.rs` tests cover host normalization, optional ports, trailing dots, path defaults, and SNI canonicalization. |
| Complete | Canonical route key generation. | `identity.rs`, `matcher.rs`, and route resolver tests use canonical `RouteIdentity` values for cache and lookup. |
| Complete | Opaque subscription ID storage and invalidation handling. | `subscription.rs` tests cover storing subscription IDs, targeted invalidation, update replacement, stale update rejection, and idempotent unsubscribe. |
| Complete | Exact host and SNI lookup. | `matcher.rs` and Postgres route resolver tests cover exact host/SNI matches and misses. |
| Complete | Wildcard host lookup. | `matcher.rs` covers wildcard suffix matching and specificity. |
| Complete | Longest path-prefix matching. | `matcher.rs` covers longest path-prefix and segment-boundary behavior. |
| Complete | Bounded local route cache and `Subscribe` subscribe/unsubscribe stream handling. | `cache/tests.rs` and `resolver/tests.rs` cover TTLs, negative cache, eviction, unsubscribe on eviction, and pushed updates/invalidations. |
| Complete | `SubscribeRoute` fallback on local miss. | `resolver/tests.rs` covers positive/negative cache hits without calls and cache-miss subscribe behavior. |
| Complete | `WakeInstance` flow when a route is Cold or missing a backend. | `route/tests.rs` and `runtime/tests.rs` cover cold wake, waiting, running-without-backend wake, stale response retry, and errors. |
| Complete | Stale generation rejection. | `subscription.rs`, `cache/tests.rs`, and `route/tests.rs` reject stale instance/backend generations. |
| Complete | HTTP-01 challenge lookup through `ResolveHTTP01Challenge`. | `http01.rs`, `runtime.rs`, `listener.rs`, and `control_plane_transport.rs` tests cover challenge interception before route resolution, hit/miss/error behavior, listener wiring, and generated operator-client lookup. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 3A: Route key normalization and local matcher. | `identity.rs` and `matcher.rs` tests cover normalization, exact/wildcard precedence, and path-prefix matching. |
| Complete | 3B: Bounded cache, TTLs, and negative caching. | `cache/tests.rs` covers positive/negative TTL, bounded eviction, stale rejection, and reassignment. |
| Complete | 3C: SubscribeRoute cache miss and opaque subscription IDs. | `resolver/tests.rs` and `subscription.rs` cover resolved/miss handling and subscription ID storage. |
| Complete | 3D: Subscribe stream with Unsubscribe and targeted invalidations. | `control_plane_transport/tests.rs` and resolver tests cover unsubscribe input, pushed update, and pushed invalidation handling on the client side. |
| Complete | 3E: WakeInstance flow, Waking wait, and stale generation rejection. | `route/tests.rs` covers cold/running/waking/deleting states, wake failures, stale responses, and retry. |
| Incomplete | 3F: HTTP/1.1, HTTP/2, h2c gRPC, and WebSocket forwarding. | Forwarding helper tests cover these paths, but the runtime listener is HTTP/1.1-only and the full protocol matrix is not wired end to end; follow-up: M9 protocol gates. |
| Incomplete | 3G: HTTPS termination, SNI certificate selection, and TLS/SNI passthrough. | `tls.rs` helper tests cover termination/passthrough behavior, but `bin/frontline.rs` does not wire TLS termination or SNI passthrough listeners; follow-up: M9 runtime TLS/SNI wiring. |
| Complete | 3H: HTTP-01 interception and `ResolveHTTP01Challenge` lookup. | Frontline runtime and listener tests cover challenge-first routing behavior; transport tests cover `GrpcOperatorHttp01Resolver` hit, miss, status error, and malformed response handling. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Fake-control-plane tests cover cold wake, hot route, miss, stale generation, targeted update, invalidation, and stream reconnect. | Runtime/resolver/route tests cover cold/hot/miss/stale and pushed update/invalidation; `control_plane_transport.rs` drains actual Subscribe response stream close as a terminal event, and resolver tests prove that event invalidates active cached positives before TTL and lazily rebuilds on the next request. |
| Complete | Route matching tests cover precedence, wildcard specificity, path-prefix, normalization, ports, trailing dots, and misses. | `identity.rs` and `matcher.rs` cover these cases. |
| Complete | Cache tests cover positive/negative TTL, eviction, unsubscribe on eviction, refresh after invalidation, and stale backend rejection. | `cache/tests.rs` and `resolver/tests.rs` cover the listed cache behavior. |
| Complete | Subscription tests cover route subscription, miss without subscription ID, unsubscribe, duplicate unsubscribe, and invalidation after resolve. | `subscription.rs` and `resolver/tests.rs` cover these flows. |
| Complete | Subscription tests cover duplicate in-flight SubscribeRoute, reassignment, invalidation during wake, and stream backpressure. | Reassignment, duplicate resolved responses, invalidation during wake, and response-buffer backpressure are covered; `listener.rs` has a deterministic shared-coordinator test proving duplicate outstanding SubscribeRoute calls for the same identity cannot occur through the serialized listener route-resolution path. |
| Complete | Stream reconnect tests prove lazy resubscribe/rebuild without public cursors. | `control_plane_transport.rs` proves a closed Subscribe stream is dropped and the next SubscribeRoute opens a fresh stream, and drains actual post-response stream close as a terminal event; resolver tests prove stream-close invalidation triggers lazy resubscribe/rebuild without public cursors. |
| Incomplete | Protocol tests cover HTTP/1.1, HTTP/2, h2c gRPC, WebSockets, TLS termination, SNI passthrough, and HTTP-01. | Helper/component tests cover pieces and HTTP-01 runtime wiring is covered; runtime TLS/SNI and full E2E protocol coverage remain missing; follow-up: M9. |
| Incomplete | Protocol tests include large/streaming HTTP, HTTP/2 multiplexing, gRPC trailers/status, WebSocket backpressure, TLS passthrough bytes, and TCP half-close. | h2c-shaped gRPC, passthrough bytes, and TCP half-close have partial coverage; full large/streaming/multiplex/backpressure matrix is missing; follow-up: M9. |
| Incomplete | PostgreSQL/libpq 17+ SNI passthrough with `sslnegotiation=direct`. | No real PostgreSQL/libpq SNI passthrough test or kind gate found; follow-up: M9. |
| Complete | TLS tests cover certificate selection, missing certificate, cert rotation, and passthrough for non-HTTP TLS traffic. | `crates/frontline/src/tls.rs` helper tests cover canonical certificate selection/rotation, missing SNI/cert rejection, and passthrough prefix preservation. |
| Complete | HTTP-01 tests cover wrong host/token, expired/deleted challenge, content type, and precedence. | `http01.rs`, `runtime.rs`, `control_plane_transport.rs`, and store tests cover challenge matching, invalid host/token, wrong-key miss, miss/expiry/delete behavior, content type, generated client lookup, and challenge-path precedence. |
| Complete | Unknown Host/SNI negative caching protects fake control plane from repeat misses. | `cache/tests.rs` and `resolver/tests.rs` cover negative cache hits and expiry without repeat control-plane calls. |

## Milestone 4: Sidecar Idle Proxy

Build the sidecar-specific local proxy and idle reporting behavior.

Scope:

- Local forwarding to `127.0.0.1:<app-port>`.
- HTTP and TCP forwarding modes.
- Active request and connection tracking.
- Idle detection.
- Drain handling.
- `ReportIdle` control-plane call.
- Graceful shutdown behavior when the workload is being deleted.

Sub-phases:

- 4A: Local HTTP forwarding to `127.0.0.1:<app-port>`.
- 4B: Local TCP forwarding to `127.0.0.1:<app-port>`.
- 4C: Active request and connection tracking.
- 4D: Idle detection and `ReportIdle` call.
- 4E: Drain and graceful shutdown behavior.

Done when:

- Integration tests prove active HTTP requests, h2c gRPC streams, WebSockets, and
  raw TCP connections prevent idle reporting.
- Idle tests prove `ReportIdle` fires after the configured timeout only after all
  active requests and connections close.
- Idle/report tests cover duplicate reports, stale generation reports, control
  plane rejection, retry/backoff, and sidecar restart.
- Drain tests prove the sidecar stops accepting new work and lets active work
  finish within the grace period.
- Drain tests cover SIGTERM, upstream failure, client disconnect, grace expiry
  with active streams, and forced shutdown after the hard deadline.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Local forwarding to `127.0.0.1:<app-port>`. | `crates/sidecar/src/tests.rs` and `runtime/tests.rs` cover forwarding to loopback upstreams. |
| Complete | HTTP and TCP forwarding modes. | `bin/sidecar.rs` selects `http` or `tcp`; component/runtime tests cover both modes. |
| Complete | Active request and connection tracking. | Sidecar tests hold HTTP bodies/TCP streams open and assert active count behavior. |
| Complete | Idle detection. | `idle.rs` tests cover no-active timeout, active work suppression, timer reset, and non-duplicated successful reports. |
| Complete | Drain handling. | Sidecar tests cover rejecting new HTTP/TCP work, waiting for active work, grace timeout, and shutdown-triggered drain. |
| Complete | `ReportIdle` control-plane call. | `idle/control_plane.rs` and `control_plane_transport/tests.rs` cover accepted, already-draining, stale generation, unavailable, and transport errors. |
| Complete | Graceful shutdown behavior when the workload is being deleted. | `runtime/tests.rs` covers shutdown stop-accepting behavior, active request wait, and timeout return. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 4A: Local HTTP forwarding. | Sidecar HTTP tests cover request/response forwarding and active request accounting. |
| Complete | 4B: Local TCP forwarding. | Sidecar TCP tests cover byte forwarding and active connection accounting. |
| Complete | 4C: Active request and connection tracking. | Component tests cover active HTTP body and open TCP stream tracking. |
| Complete | 4D: Idle detection and `ReportIdle` call. | `idle.rs` and `idle/control_plane.rs` cover timeout, retry, terminal outcomes, and active-work reset. |
| Complete | 4E: Drain and graceful shutdown behavior. | Component/runtime tests cover drain rejection, grace timeout, and shutdown-triggered drain. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Active HTTP requests, h2c gRPC streams, WebSockets, and raw TCP connections prevent idle reporting. | `runtime/tests.rs` covers HTTP, raw TCP, h2c gRPC-shaped streams, and WebSocket sessions delaying idle reports until active work closes. |
| Complete | Idle tests prove `ReportIdle` fires after timeout only after active work closes. | `idle.rs` covers active work suppression and reset before reporting. |
| Complete | Idle/report tests cover duplicate reports, stale generation, control-plane rejection, retry/backoff, and sidecar restart. | Duplicate reports, stale generation, rejection/unavailable outcomes, retry/backoff, and detector reconstruction are covered; `sidecar_process_lifecycle.rs` starts the real `sidecar` binary against a fake gRPC control plane, terminates it with SIGTERM, restarts it on the same port, and verifies it listens again without stale state. |
| Complete | Drain tests prove the sidecar stops accepting new work and lets active work finish within the grace period. | `src/tests.rs` and `runtime/tests.rs` cover drain rejection and active-work waiting. |
| Complete | Drain tests cover SIGTERM, upstream failure, client disconnect, grace expiry with active streams, and forced shutdown after hard deadline. | Shutdown-triggered drain, grace expiry, upstream HTTP disconnect, and TCP client disconnect are covered; the Unix process lifecycle tests send OS SIGTERM to the real binary and assert a hanging active request exits after `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS` with the expected drain-timeout failure status. |

## Milestone 5: Kubernetes Materializer

Render and reconcile active Kubernetes objects from control-plane state.

Scope:

- Render PV, PVC, Service, Deployment, and StatefulSet objects.
- Apply PV/PVC before workloads and wait for PVC Bound.
- Validate PV/PVC specs for names, labels, access modes, capacity, reclaim
  policy, volume source, instance value substitution, and intended binding.
- Validate workload volume mounts point at the rendered PVC and expected mount
  path.
- Inject sidecar into rendered pod templates.
- Service targets the sidecar port.
- Sidecar targets the local app port.
- Validate Service selectors and target ports route to the sidecar, and sidecar
  upstream configuration can only route to the local app port.
- Wait for readiness through Pods or EndpointSlices.
- Delete materialized objects on sleep/delete.
- Leave backing provider volumes untouched.

Sub-phases:

- 5A: Structured object rendering for PV, PVC, Service, Deployment, and
  StatefulSet.
- 5B: Kubernetes apply/update/delete client and ownership labels.
- 5C: PV/PVC spec correctness, intended binding, apply order, and PVC Bound
  wait.
- 5D: Deployment/StatefulSet rendering with sidecar injection.
- 5E: Service rendering that targets the sidecar port.
- 5F: Readiness detection through Pods or EndpointSlices.
- 5G: Sleep/delete cleanup for workloads, Services, PVCs, and PVs.
- 5H: kind materialization lifecycle suite with a static volume backend.

Done when:

- Component tests validate rendered objects and ordering.
- Component tests validate Service selector/targetPort wiring, sidecar container
  args/env for the local upstream, and that the generated config cannot proxy
  back to the Service that targets the sidecar.
- Component tests validate PV/PVC rendered fields, binding selectors or
  `volumeName`, ownership/generation labels, and workload volume mounts.
- kind tests prove cold wake creates PV/PVC before StatefulSet and the PVC binds
  to the intended PV.
- kind tests prove the workload can read/write at the expected mount path.
- kind tests prove sleep deletes workload, Service, PVC, and PV.
- kind tests prove re-wake recreates manifests from the same instance values and
  preserves data when using the same backing static volume.
- Failure tests cover missing or bad volume handles, PVCs that never bind, wrong
  access modes, and stale manifest generation.
- Readiness tests prove routes are not published until Pods or EndpointSlices are
  ready, and are withdrawn when the materialization becomes unready.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Render PV, PVC, Service, Deployment, and StatefulSet objects. | `crates/control-plane/src/manifest/tests.rs` covers all rendered object kinds and serialization. |
| Complete | Apply PV/PVC before workloads and wait for PVC Bound. | `materializer.rs` tests apply StatefulSet manifests with PVC-bound wait before Service/workload. |
| Complete | Validate PV/PVC specs, source, substitution, and intended binding. | Manifest tests cover names, labels, access modes, capacity, reclaim policy, hostPath source, `volumeName`, and template substitution failures. |
| Complete | Validate workload volume mounts point at rendered PVC and expected mount path. | Manifest tests cover rendered volume mounts for StatefulSet volume templates. |
| Complete | Inject sidecar into rendered pod templates. | Manifest render tests verify sidecar container/env wiring. |
| Complete | Service targets the sidecar port. | Manifest tests validate service port and target port wiring. |
| Complete | Sidecar targets the local app port. | Manifest tests validate sidecar local upstream env and reject app target conflicts. |
| Complete | Validate Service selectors/target ports and local-only sidecar upstream. | Manifest validation rejects invalid service ports and sidecar/app port conflicts. |
| Complete | Wait for readiness through Pods or EndpointSlices. | `kube_materializer.rs` and `materializer.rs` tests cover EndpointSlice readiness and backend URI creation. |
| Complete | Delete materialized objects on sleep/delete. | Component paths now clean recorded Kubernetes refs before finalizing state: `sidecar_api_transport.rs` covers `ReportIdle` sleep cleanup, and `api_transport.rs` covers operator delete cleanup success, no-active-materialization, target filtering, stale active materialization generation handling, failure preservation, and retry. |
| Complete | Leave backing provider volumes untouched. | `scripts/test-kind-materializer.sh` deletes rendered objects and rematerializes with preserved hostPath data. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | 5A: Structured object rendering. | Manifest tests cover PV, PVC, Service, Deployment, and StatefulSet rendering. |
| Complete | 5B: Kubernetes apply/update/delete client and ownership labels. | `materializer.rs` and `kube_materializer.rs` tests cover apply/delete refs and ownership/generation labels. |
| Complete | 5C: PV/PVC correctness, intended binding, apply order, and PVC Bound wait. | Manifest and materializer tests cover validation, apply ordering, and bound wait failures. |
| Complete | 5D: Deployment/StatefulSet rendering with sidecar injection. | Manifest tests cover both workload kinds and injected sidecar config. |
| Complete | 5E: Service rendering that targets the sidecar port. | Manifest tests cover service selectors and sidecar target port. |
| Complete | 5F: Readiness through Pods or EndpointSlices. | Kube materializer readiness tests cover ready EndpointSlice semantics. |
| Complete | 5G: Sleep/delete cleanup for workloads, Services, PVCs, and PVs. | Sleep cleanup is wired through sidecar `ReportIdle`; operator delete cleanup now receives materializer/target context and deletes recorded refs before store deletion, with component tests for success, no active materialization, target filtering, stale active materialization generation handling, failure preservation, and retry. |
| Complete | 5H: kind materialization lifecycle suite with static volume backend. | `scripts/test-kind-materializer.sh` runs the ignored kind materializer lifecycle test with static hostPath data continuity. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Complete | Component tests validate rendered objects and ordering. | `manifest/tests.rs` and `materializer.rs` cover object shape and apply order. |
| Complete | Component tests validate Service wiring, sidecar local upstream env, and no proxy-back-to-Service config. | Manifest tests cover Service target port, sidecar upstream env, and reject conflicting app/sidecar ports. |
| Complete | Component tests validate PV/PVC fields, labels, and volume mounts. | Manifest tests cover PV/PVC fields, owner/generation labels, and workload mounts. |
| Incomplete | kind tests prove cold wake creates PV/PVC before StatefulSet and PVC binds intended PV. | Materializer-only kind test proves apply/bind ordering, but not cold wake through the full platform; follow-up: M9 full-platform kind E2E. |
| Complete | kind tests prove workload can read/write expected mount path. | `scripts/test-kind-materializer.sh` writes and verifies a marker through the mounted hostPath volume. |
| Incomplete | kind tests prove sleep deletes workload, Service, PVC, and PV. | Sleep-driven cleanup is covered by component transport tests, but no kind/full-platform `ReportIdle` cleanup gate proves deletion of real workload, Service, PVC, and PV objects. |
| Complete | kind tests prove re-wake recreates manifests from same values and preserves static-volume data. | `scripts/test-kind-materializer.sh` rematerializes the manifest and verifies the previous marker remains. |
| Complete | Failure tests cover missing/bad volume handles, PVCs never bind, wrong access modes, and stale manifest generation. | Manifest tests reject missing/empty CSI volume handles plus unsupported/duplicate access modes; materializer tests cover PVC bind failures and stale generation labels/annotations being rejected before apply. |
| Incomplete | Readiness tests prove routes are not published until ready and withdrawn when unready. | Materializer readiness returns a backend only after ready, but route publication/withdrawal is not wired to readiness changes; follow-up: M9. |

## Milestone 6: End-to-End V1

Wire the control plane, frontline proxy, sidecar, and materializer together.

Scope:

- Create instance through control-plane API.
- Resolve route lazily through the frontline proxy.
- Cold request wakes instance.
- Hot request routes from local cache.
- Sidecar reports idle.
- Control plane drains and sleeps materialization.
- Custom host and wildcard host route to the right instance.
- HTTP-01 challenge insert, resolve, serve, and delete flow works.

Sub-phases:

- 6A: Stateless Deployment cold wake, hot route, idle drain, sleep, and re-wake.
- 6B: StatefulSet with static PV/PVC templates cold wake, hot route, mounted
  write/read, idle drain, sleep, and re-wake with data continuity.
- 6C: Custom host, wildcard host, SNI, and optional path-prefix routing.
- 6D: Protocol matrix: HTTP/1.1, HTTP/2, h2c gRPC, gRPC-Web control-plane
  access, WebSockets, TLS termination, SNI passthrough, and
  PostgreSQL/libpq 17+ over SNI with `sslnegotiation=direct`.
- 6E: HTTP-01 insert, resolve, serve, delete, and expired-token behavior.
- 6F: Failure-path matrix: wake timeout, bad route, missing PVC binding, bad
  volume template, stale proxy generation, and control-plane restart.
- 6G: Lifecycle race matrix: concurrent wake calls, sleep while waking, delete
  while waking, delete while draining, failed wake retry, stale sidecar report,
  and route reassignment during active traffic.

Done when:

- kind E2E passes for stateless Deployment.
- kind E2E passes for StatefulSet with static PV/PVC templates, intended binding,
  mounted write/read, sleep, re-wake, and data continuity.
- kind E2E passes for HTTP/1.1, HTTP/2, h2c gRPC, gRPC-Web control-plane access,
  WebSockets, TLS termination, and SNI passthrough.
- kind E2E proves a real PostgreSQL/libpq 17+ deployment can connect through
  TLS/SNI passthrough with `sslnegotiation=direct`, using a pinned Postgres
  image in CI.
- Failure-path E2E covers wake timeout, bad route, missing PVC binding, bad
  volume template, and stale proxy generation.
- Lifecycle-race E2E proves generation checks prevent stale sidecar reports,
  stale materializations, and stale proxy cache entries from changing current
  instance state.
- Control-plane restart E2E covers restart during wake, sleep, delete, route
  reassignment, and HTTP-01 challenge handling.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Incomplete | Create instance through control-plane API. | Store-backed API/component tests exist, but no full-platform kind E2E creates an instance through the real deployed control plane; follow-up: M9. |
| Incomplete | Resolve route lazily through the frontline proxy. | Frontline component tests use fake control-plane clients; no full-platform kind route-resolution path exists; follow-up: M9. |
| Incomplete | Cold request wakes instance. | Frontline wake and control-plane wake are tested separately, but not through deployed frontline/control-plane/materializer/sidecar; follow-up: M9. |
| Incomplete | Hot request routes from local cache. | Frontline runtime tests cover a fake control-plane hot route, but no full-platform hot-cache E2E exists; follow-up: M9. |
| Incomplete | Sidecar reports idle. | Sidecar and control-plane ReportIdle tests exist separately, but no full-platform E2E proves the deployed sidecar report path; follow-up: M9. |
| Complete | Control plane drains and sleeps materialization. | `sidecar_api_transport.rs` covers `ReportIdle` beginning sleep, deleting rendered Kubernetes object refs through the materializer, marking the materialization deleted, and returning the instance to `Cold`; `wake::tests` covers the Draining/deleting wake guard. |
| Incomplete | Custom host and wildcard host route to right instance. | Matcher/store component tests cover custom/wildcard routing; no full-platform E2E gate exists; follow-up: M9. |
| Incomplete | HTTP-01 insert, resolve, serve, and delete flow works. | Store/API/runtime/transport tests cover insert, resolve lookup, serve behavior, delete/expiry, and precedence; full-platform kind E2E remains missing; follow-up: M9 full-platform gate. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Incomplete | 6A: Stateless Deployment cold wake, hot route, idle drain, sleep, and re-wake. | No stateless full-platform kind E2E script found; follow-up: M9. |
| Incomplete | 6B: StatefulSet static PV/PVC lifecycle with data continuity. | Materializer-only kind lifecycle exists, but not full wake/sleep/re-wake through the platform; follow-up: M9. |
| Incomplete | 6C: Custom host, wildcard host, SNI, and path-prefix routing. | Component coverage exists; full-platform E2E is missing; follow-up: M9. |
| Incomplete | 6D: Full protocol matrix, including PostgreSQL/libpq SNI passthrough. | Helper/component tests cover pieces, including gRPC-Web operator transport; runtime TLS/SNI, full-platform browser/kind protocol coverage, and PostgreSQL/libpq SNI E2E are missing; follow-up: M9. |
| Incomplete | 6E: HTTP-01 insert, resolve, serve, delete, and expired-token behavior. | Store/helper/runtime/transport coverage exists; full-platform deployed flow is missing; follow-up: M9 full-platform gate. |
| Incomplete | 6F: Failure-path matrix. | Component failure tests exist, but full-platform wake/PVC/route/stale/control-plane-restart failures are missing; follow-up: M9. |
| Incomplete | 6G: Lifecycle race matrix. | Store/frontline components cover some races; full-platform lifecycle race E2E is missing; follow-up: M9. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Incomplete | kind E2E passes for stateless Deployment. | No full-platform stateless kind E2E found; follow-up: M9. |
| Incomplete | kind E2E passes for StatefulSet with PV/PVC, mounted IO, sleep, re-wake, and data continuity. | Materializer-only kind test covers PV/PVC IO and rematerialization; full-platform sleep/re-wake is missing; follow-up: M9. |
| Incomplete | kind E2E passes for HTTP/1.1, HTTP/2, h2c gRPC, grpc-web, WebSockets, TLS termination, and SNI passthrough. | Component gRPC-Web transport coverage exists in `api_transport.rs`, but no full-platform protocol kind E2E found; follow-up: M9. |
| Incomplete | kind E2E proves real PostgreSQL/libpq 17+ SNI passthrough with pinned image. | No real libpq SNI passthrough test found; follow-up: M9. |
| Incomplete | Failure-path E2E covers wake timeout, bad route, missing PVC binding, bad volume template, and stale proxy generation. | Component failures exist; full-platform failure E2E is missing; follow-up: M9. |
| Incomplete | Lifecycle-race E2E prevents stale sidecar, materialization, and proxy cache updates. | Component generation checks exist; full-platform race E2E is missing; follow-up: M9. |
| Incomplete | Control-plane restart E2E covers restart during wake, sleep, delete, route reassignment, and HTTP-01. | No restart recovery E2E found; follow-up: M9. |

## Milestone 7: Hardening

Make V1 operationally credible.

Scope:

- Metrics and tracing for wake latency, route-cache hits, control-plane calls,
  drain duration, active streams, and materialization failures.
- Structured logs with instance ID, route ID, generation, and cluster.
- Backoff and retry policies.
- Proxy `Subscribe` stream reconnect and cache rebuild through lazy
  `SubscribeRoute`, without proxy-visible versions or cursors.
- Control-plane restart recovery from database state.
- Minimal production images for the control plane, frontline proxy, and sidecar.
- Load tests for route lookup and hot proxy path, including request rate,
  streaming throughput, and tail latency against same-environment direct-backend
  baselines.
- Soak tests for repeated wake/sleep cycles.

Sub-phases:

- 7A: Metrics, tracing, and structured log fields.
- 7B: Control-plane restart recovery during wake, sleep, and delete.
- 7C: Proxy `Subscribe` reconnect, lazy cache rebuild, and stale backend
  recovery.
- 7D: Minimal final images and container runtime smoke tests.
- 7E: Load tests for route lookup, hot proxy path, HTTP request rate,
  h2/h2c/gRPC behavior, TCP throughput, and WebSocket throughput.
- 7F: kind wake/sleep soak tests and leaked-object detection.
- 7G: Operator-facing runbook and metric name documentation.

Milestone 7F starts with the existing materializer lifecycle kind test as a
small soak target:

- `./scripts/test-kind-materializer.sh` runs the single disposable-cluster
  check.
- `SLEEPYPODS_KIND_SOAK_ITERATIONS=3 ./scripts/soak-kind-materializer.sh`
  reuses one kind cluster across repeated runs.

The soak fails fast on the first failed iteration and waits for leaked
`sleepypods.io/kind-test=true` namespaces and
`sleepypods.io/instance-id=kind-materializer` PersistentVolumes to disappear
after each successful iteration.

Done when:

- Automated tests cover restart during wake, sleep, and delete.
- Metrics tests assert key counters/histograms and labels for wake latency,
  route-cache hits/misses, subscribe stream events, invalidations, active
  streams, drain duration, materialization failures, and HTTP-01 results.
- Structured log tests or golden assertions cover instance ID, route ID,
  subscription ID where relevant, generation, cluster, namespace, and error
  reason on important lifecycle paths.
- Repeated kind wake/sleep soak passes without leaked Kubernetes objects.
- Load-test targets for route lookup, hot proxy path, and cold wake latency are
  defined before 7D starts, and tests fail if those targets regress.
- Proxy load tests run against the same production images used by kind E2E and
  compare against direct-backend baselines from the same test environment.
- Hot-cache HTTP/1.1 request rate stays within 20% of direct-backend baseline.
- Hot-cache h2, h2c, and gRPC request rate stays within 25% of direct-backend
  baseline.
- TCP large-stream throughput stays within 10-15% of direct-backend baseline.
- WebSocket streaming throughput stays within 15-20% of direct-backend baseline.
- Hot-cache p99 added latency stays below a documented absolute budget where
  stable, or within 25% of direct-backend baseline where timing is noisy.
- Once baseline numbers are established, benchmark regressions warn above
  10-15% and fail above 20-25% unless the change explicitly updates the
  accepted budget.
- Hot-cache route handling makes zero control-plane calls under load.
- Route lookup and hot proxy path meet target latency under load.
- Retry/backoff tests cover transient database errors, Kubernetes API conflicts,
  proxy stream disconnects, and materializer reconcile retries.
- Container tests prove the final control-plane, frontline proxy, and sidecar
  images start successfully, run as non-root, include only required runtime
  files, can access required CA certificates, and are the images used by kind E2E
  and soak tests.
- Image-size budgets for the three production images are defined before 7D
  starts, and tests or CI checks fail if they regress without an explicit update.
- Dashboards or metric names are documented enough for operators to wire up.

Milestone 8 audit:

Scope:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Incomplete | Metrics and tracing for wake latency, cache hits, control-plane calls, drain duration, active streams, and materialization failures. | `proxy-core` descriptor tests exist, but runtime instrumentation for these lifecycle metrics is incomplete; follow-up: M9 for instrumentation gates and M10 for metric docs. |
| Incomplete | Structured logs with instance ID, route ID, generation, and cluster. | No structured log/golden assertions found for lifecycle fields; follow-up: M9 implementation gates and M10 runbooks. |
| Incomplete | Backoff and retry policies. | Sidecar idle retry/backoff and proxy Subscribe reconnect/lazy rebuild are covered; database retry, Kubernetes conflict retry, proxy reconnect backoff policy, and materializer reconcile retry gates are missing; follow-up: M9. |
| Complete | Proxy `Subscribe` reconnect and lazy cache rebuild. | `control_plane_transport.rs` covers reconnect after a closed Subscribe response stream and drains actual stream close as a terminal event; resolver tests prove active cached positives are invalidated before TTL and lazily rebuilt by the next request. |
| Incomplete | Control-plane restart recovery from database state. | No restart reconciliation tests or runtime recovery loop found; follow-up: M9. |
| Incomplete | Minimal production images for control plane, frontline, and sidecar. | Distroless non-root Dockerfiles and `scripts/smoke-images.sh` exist, but images are not used by full kind E2E/soak and full startup/connectivity gates are missing; follow-up: M9. |
| Incomplete | Load tests for route lookup and hot proxy path. | `scripts/smoke-frontline-load.sh`, sidecar load smokes, `crates/frontline/benches/route_lookup.rs`, and `docs/proxy-hot-path-budgets.md` exist; protocol throughput matrix, cold-wake load coverage, and stable tail-latency gates are still missing; follow-up: M9. |
| Complete | Indexed frontline route matcher for hot-path lookup. | `RouteCache` uses `PositiveRouteIndex` for exact hosts, wildcard suffixes, HTTP path candidates, and SNI candidates; cache tests cover precedence/lifecycle behavior and `route_lookup` benchmarks hot positive lookups with 4096 unrelated routes. |
| Incomplete | Soak tests for repeated wake/sleep cycles. | `scripts/soak-kind-materializer.sh` is materializer-only, not full wake/sleep; follow-up: M9. |

Sub-phases:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Incomplete | 7A: Metrics, tracing, and structured log fields. | Descriptor-level observability exists; runtime lifecycle instrumentation and log assertions are missing; follow-up: M9/M10. |
| Incomplete | 7B: Control-plane restart recovery during wake, sleep, and delete. | No restart recovery gate found; follow-up: M9. |
| Complete | 7C: Proxy Subscribe reconnect, lazy cache rebuild, and stale backend recovery. | Transport reconnect and stale backend recovery have component coverage; actual stream-close-driven active cache invalidation before TTL expiry and lazy rebuild on the next request are covered by transport and resolver tests. |
| Incomplete | 7D: Minimal final images and container runtime smoke tests. | Distroless images and `scripts/smoke-images.sh` enforce non-root/runtime-file/startup-error checks plus image-size budgets, but full startup/connectivity/kind-use gates are missing; follow-up: M9. |
| Incomplete | 7E: Load tests for route lookup and proxy protocols. | Conservative load smokes and the route lookup benchmark exist, but h2/h2c/gRPC, WebSocket, cold-wake, tail-latency, and enforced regression budgets are incomplete; follow-up: M9. |
| Incomplete | 7F: kind wake/sleep soak and leaked-object detection. | Existing soak is materializer-only; full wake/sleep soak is missing; follow-up: M9. |
| Incomplete | 7G: Operator runbook and metric name documentation. | Operator-facing metric/runbook docs are not present; follow-up: M10. |

Done criteria:

| Status | Item | Evidence / gap |
| --- | --- | --- |
| Incomplete | Automated tests cover restart during wake, sleep, and delete. | No restart recovery tests found; follow-up: M9. |
| Incomplete | Metrics tests assert counters/histograms and labels for lifecycle paths. | Proxy-core descriptor tests exist, but requested lifecycle metrics are not instrumented/tested; follow-up: M9. |
| Incomplete | Structured log tests/goldens cover lifecycle fields and errors. | No structured log assertions found; follow-up: M9. |
| Incomplete | Repeated kind wake/sleep soak passes without leaked Kubernetes objects. | Materializer-only soak checks leaked namespaces/PVs; full wake/sleep soak is missing; follow-up: M9. |
| Incomplete | Load-test targets for route lookup, hot proxy path, and cold wake latency are defined before 7D. | `docs/proxy-hot-path-budgets.md` documents route lookup and current load-smoke limitations, but cold wake and stable tail-latency targets are incomplete; follow-up: M9. |
| Incomplete | Proxy load tests use production images and direct-backend baselines. | Production-image load smokes exist with fake control plane/direct comparisons, but full protocol and kind production-image gates are missing; follow-up: M9. |
| Incomplete | Hot-cache HTTP/1.1 request rate stays within 20% of direct backend. | `scripts/smoke-frontline-load.sh` has a conservative HTTP/1.1 cached-route smoke, but not a stable release-gate budget; follow-up: M9. |
| Incomplete | Hot-cache h2, h2c, and gRPC request rate stays within 25%. | No h2/h2c/gRPC load gate found; follow-up: M9. |
| Incomplete | TCP large-stream throughput stays within 10-15%. | Sidecar TCP smoke exists, but no stable large-stream release-gate budget; follow-up: M9. |
| Incomplete | WebSocket streaming throughput stays within 15-20%. | No WebSocket throughput gate found; follow-up: M9. |
| Incomplete | Hot-cache p99 added latency stays below documented budget or within 25% baseline. | No stable p99/tail-latency gate found; follow-up: M9. |
| Incomplete | Benchmark regressions warn above 10-15% and fail above 20-25%. | No regression budget enforcement found; follow-up: M9. |
| Incomplete | Hot-cache route handling makes zero control-plane calls under load. | Component tests avoid calls on cache hits; no load gate asserts zero calls; follow-up: M9. |
| Incomplete | Route lookup and hot proxy path meet target latency under load. | Route lookup has a Criterion benchmark and provisional budget, and hot proxy paths have conservative smokes; stable under-load release gates for hot proxy path, cold wake, and tail latency are still missing; follow-up: M9. |
| Complete | Hot-cache route lookup avoids scanning every positive cached route. | `RouteCache::lookup` narrows positive candidates through `PositiveRouteIndex` by host/suffix/path/SNI before ranking, with cache tests preserving match semantics and route lookup benchmarks covering many unrelated cached routes. |
| Incomplete | Retry/backoff tests cover transient database errors, Kubernetes conflicts, proxy disconnects, and materializer retries. | Proxy Subscribe transport reconnect after disconnect, stream-close invalidation/lazy rebuild, and sidecar retry are covered; database retry, Kubernetes conflict retry, proxy reconnect backoff policy, and materializer retry gates are still missing; follow-up: M9. |
| Incomplete | Container tests prove images start, run non-root, include required files, access CA certs, and are used by kind E2E/soak. | `scripts/smoke-images.sh` checks non-root/no shell/files and expected startup failure without config; full startup/connectivity/kind-use is missing; follow-up: M9. |
| Complete | Image-size budgets are defined and enforced. | `scripts/smoke-images.sh` enforces Docker inspect `.Size` against positive-integer byte budgets with a 256 MiB default and per-component overrides; `scripts/smoke-images.sh` passed on 2026-06-23 with control-plane 49,541,581 bytes, frontline 39,084,845 bytes, and sidecar 39,504,389 bytes. |
| Incomplete | Dashboards or metric names are documented enough for operators. | No operator metric dashboard/runbook doc found; follow-up: M10. |

## Milestone 8: Phase Status Audit

Audit the implementation against every prior milestone before doing more
feature work.

Scope:

- Review all Milestone 1-7 scope items, sub-phases, and done criteria against
  the current code, tests, scripts, and kind/container evidence.
- Mark each prior item explicitly as either `Complete` or `Incomplete` in this
  development plan.
- For every `Complete` item, include concise evidence such as a test name,
  script name, source file, or command that proves the claim.
- For every `Incomplete` item, name the missing behavior or missing test.
- Do enough inspection to make a definitive status call; absence of evidence is
  `Incomplete`.

Sub-phases:

- 8A: Audit Milestones 1-3 covering proxy primitives, control-plane resource
  model, and frontline route resolution.
- 8B: Audit Milestones 4-5 covering sidecar idle behavior and Kubernetes
  materialization.
- 8C: Audit Milestones 6-7 covering full-platform E2E, hardening, production
  images, load tests, and soak tests.
- 8D: Update this plan with explicit status markers and evidence for every
  prior scope item, sub-phase, and done criterion.
- 8E: Promote every discovered skipped gate into Milestone 9 or a later
  explicitly deferred item.

Done when:

- Every prior scope item, sub-phase, and done criterion is marked `Complete` or
  `Incomplete`.
- Every `Complete` marker has evidence that a reviewer can run or inspect.
- Every `Incomplete` marker has a concrete follow-up location in Milestone 9,
  Milestone 10, or `Deferred`.
- The plan no longer relies on phase numbers alone as proof that behavior exists.

## Milestone 9: V1 Gap Closure and Workload Sleep Policy

Close the skipped V1 functional gates before writing operator-facing docs. Move
sidecar sleep timing from runtime-only environment defaults into explicit
WorkloadClass policy.

Scope:

- Add focused checks for the currently known gaps: sleep finalization after
  `ReportIdle`, Kubernetes cleanup on delete, full-platform kind E2E, frontline
  TLS/SNI runtime wiring, and subscribed route update/invalidation delivery.
- Close Milestone 8 audit gaps marked for M9: proxy/frontline/sidecar protocol
  matrices, indexed frontline route matching, route-key lookup benchmarks,
  grpc-web parity/browser smoke, HTTP-01 runtime wiring, PostgreSQL/libpq SNI E2E, materializer
  readiness/failure gates, restart/reconnect/retry behavior, production-image
  gates, load budgets, and full wake/sleep soak.
- Add runtime observability and structured-log gates for V1 lifecycle paths
  before operator-facing docs freeze metric and log names.
- Add WorkloadClass-owned sleep policy for idle timeout, idle report retry
  backoff, and drain grace.
- Require every WorkloadClass to specify an idle timeout explicitly; there is no
  platform default for the operator-facing sleep timeout.
- Allow per-instance idle-timeout overrides only when the WorkloadClass declares
  an override field and validation bounds.
- Render the resolved policy into sidecar env as
  `SLEEPYPODS_IDLE_TIMEOUT_MS`, `SLEEPYPODS_IDLE_RETRY_BACKOFF_MS`, and
  `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS`.
- Preserve sidecar runtime env parsing only as the execution mechanism for
  rendered manifests, not as a source of platform defaults.

Sub-phases:

- 9A: Gap-check tests for sleep finalization, delete cleanup, full-platform kind
  E2E, frontline TLS/SNI runtime wiring, and subscribed route
  updates/invalidations.
- 9B: WorkloadClass sleep-policy model, validation, and API/proto mapping.
- 9C: Validated per-instance idle-timeout overrides with WorkloadClass-declared
  bounds.
- 9D: Manifest rendering of resolved sidecar sleep-policy env.
- 9E: Component and kind E2E coverage for policy rendering and idle behavior.
- 9F: Protocol/API/runtime audit gaps: indexed frontline route matcher,
  route-key lookup benchmarks, proxy-core reset/backpressure tests, frontline
  HTTP/2/h2c/WebSocket/TLS/SNI/HTTP-01 runtime wiring, HTTP-01 store
  overwrite/idempotency and wrong host/token tests, sidecar h2c/WebSocket idle
  coverage, sidecar restart during idle/report behavior, grpc-web parity, and
  PostgreSQL/libpq SNI E2E.
- 9G: Hardening audit gaps: materializer readiness/failure route publication,
  restart/reconnect/retry gates, runtime metrics/log assertions, production
  image startup/connectivity checks, load budgets, and full wake/sleep soak.

Done when:

- Gap-check tests fail against the current incomplete behavior and pass only when
  the missing V1 behavior is implemented.
- kind E2E proves `ReportIdle` leads to drain, deletion of rendered Kubernetes
  objects, materialization state cleanup, and transition back to `Cold`.
- kind E2E proves deleting an instance cleans up any active materialization and
  leaves no workload, Service, PVC, or PV objects owned by that instance.
- full-platform kind E2E uses the real control-plane, frontline, and sidecar
  images against a real database and Kubernetes API, not fake clients.
- Live database gate `scripts/test-postgres-store.sh` runs the Postgres store
  conformance suite with `SLEEPYPODS_POSTGRES_URL` set, using either a caller
  URL or disposable `postgres:17-alpine`, and proves migrations plus store
  behavior against a real database.
- Runtime tests prove the frontline binary wires HTTP, TLS termination, and
  TLS/SNI passthrough listeners rather than leaving TLS/SNI as library-only
  primitives.
- Subscription tests prove route changes and backend changes are pushed as
  targeted updates or invalidations to actively subscribed proxies.
- Operator APIs accept and return WorkloadClass sleep policy.
- WorkloadClass creation rejects missing or invalid idle timeout values.
- Instance creation rejects idle-timeout overrides unless the WorkloadClass
  explicitly allows them.
- Out-of-bounds instance idle-timeout overrides are rejected before manifest
  rendering.
- Render tests prove the sidecar container receives the resolved timeout env
  values.
- kind E2E proves two workload classes with different idle policies sleep at
  different configured thresholds.
- Proxy-core/frontline/sidecar protocol tests cover the incomplete reset,
  timeout, backpressure, HTTP/2, h2c/gRPC, WebSocket, TLS/SNI, and HTTP-01
  cases identified by the Milestone 8 audit.
- Route-key lookup benchmarks and load gates cover hot-cache route lookup, hot
  proxy paths, cold wake latency, tail latency, and zero control-plane calls on
  hot-cache hits.
- Indexed frontline route matching avoids scanning every positive cached route
  on hot-cache hits while preserving exact-host over wildcard-host,
  more-specific wildcard-host over broader wildcard-host, longest path-prefix
  selection, SNI matching, negative-cache semantics, TTL expiry, invalidation,
  and stale generation rejection.
- grpc-web store-backed integration tests cover operator APIs, CORS/preflight,
  metadata/auth propagation, structured errors, and V8-compatible request
  encoding.
- HTTP-01 runtime tests prove challenge interception calls the control plane and
  takes precedence over normal route resolution.
- HTTP-01 store tests prove overwrite/idempotency behavior plus wrong-host and
  wrong-token misses.
- PostgreSQL/libpq 17+ SNI passthrough E2E passes with `sslnegotiation=direct`
  and a pinned test image.
- Materializer readiness/failure tests prove routes publish only after ready,
  withdraw on unready, and reject stale or invalid manifests.
- Restart/reconnect/retry tests cover control-plane recovery, proxy Subscribe
  reconnect, database errors, Kubernetes conflicts, proxy stream disconnects,
  and materializer reconcile retries.
- Sidecar restart tests prove idle/report behavior remains correct across
  process restart or detector reconstruction, including duplicate report
  handling and generation checks.
- Runtime metrics/log tests assert the V1 lifecycle fields that Milestone 10
  documents.
- Production-image tests prove the final images start, run as non-root, have the
  required runtime files and CA roots, meet image-size budgets, and are used by
  kind E2E/load/soak gates.

## Milestone 10: Operator and Contributor Documentation

Write concise documentation for the two supported audiences: operators who run
and use the platform, and contributors who build and change it.

Scope:

- Operator documentation for the resource model, including `WorkloadClass`,
  `Instance`, `RouteBinding`, custom domains, HTTP-01, storage values, wake,
  sleep, delete, and expected limitations.
- Operator task guides for creating a workload class, creating an instance,
  adding a route, adding a custom domain, attaching an existing volume, and
  understanding sleep/wake behavior.
- Operator installation and administration guides for the control plane,
  frontline proxies, sidecars, database, Kubernetes permissions,
  TLS/certificate plumbing, metrics, logs, backups, upgrades, and failure
  recovery.
- Operator runbooks keyed by observable symptoms, logs, metrics, Kubernetes
  objects, and control-plane state.
- Contributor documentation with the smallest useful commands for build, unit
  tests, protocol tests, kind E2E, and code generation.
- Agent-facing repository guide with file map, invariants, source-of-truth docs,
  generated files, test gates, and common task entry points.
- Documentation for metric names, structured log fields, load/latency budgets,
  production-image expectations, and any intentionally deferred limitations.

Sub-phases:

- 10A: Operator-facing concepts, resource model, and request/lifecycle sequence
  diagrams.
- 10B: Operator task guides for workload classes, instances, routes, custom
  domains, HTTP-01, and existing volumes.
- 10C: Operator installation, configuration, database, Kubernetes, TLS, metrics,
  and upgrade guide.
- 10D: Operator troubleshooting and incident runbooks.
- 10E: Contributor local development and test guide.
- 10F: Agent-facing repository guide with file map, invariants, and common task
  entry points.

Done when:

- An operator can create a workload class, instance, route, custom domain, and
  existing-volume-backed workload from docs alone.
- An operator can predict what happens during cold wake, hot route, idle sleep,
  drain, delete, and route/domain changes.
- An operator can install, configure, monitor, back up, upgrade, and troubleshoot
  the platform from docs alone.
- API and protocol docs match the protobuf/Rust types, native gRPC service,
  gRPC-Web operator surface, and contain no stale RPCs.
- Documentation is concise: prefer short task-oriented files, stable headings,
  examples, and explicit invariants over broad narrative prose.
- Contributor and agent-facing guidance calls out source-of-truth files,
  generated files, commands, test gates, and design constraints without
  duplicating full specs.

## Stretch

The original goal is complete when the plan reaches this line. Do not start
stretch work unless explicitly asked.

### Stretch Phase 1: SleepySockets

Keep client WebSocket connections open at the frontline proxy while allowing the
upstream sidecar/app connection and workload to sleep when no application
messages have passed within the configured TTL.

Scope:

- Make SleepySockets opt-in per route or WorkloadClass; default WebSocket
  behavior remains normal passthrough.
- Terminate/intercept WebSockets at the frontline proxy, keep the client
  connection open, and create or recreate upstream WebSockets to the sidecar/app
  only when needed.
- Change idle accounting for SleepySockets from connection-open activity to
  application-message activity.
- When the message TTL expires, close the upstream WebSocket so the sidecar can
  observe idleness, report idle, and let the control plane sleep the workload.
- When a later client message arrives, wake the instance, recreate the upstream
  WebSocket to the sidecar/app, then forward the queued message.
- Document the application contract: this only works for L7, client-driven or
  resumable WebSocket protocols where losing backend-initiated messages while
  asleep is acceptable.

Done when:

- Frontline tests prove client WebSockets stay open across upstream close,
  workload sleep, wake, upstream reconnect, and message forwarding.
- Sidecar idle tests prove open SleepySockets client sessions do not prevent
  idle reporting when no application messages pass within the TTL.
- E2E tests prove a client message after sleep wakes the workload and reaches
  the app over a newly created upstream WebSocket.
- Tests cover ordering, buffering limits, ping/pong behavior, close behavior,
  backpressure, reconnect failure, and app-level resume/session token handling.
- Operator docs clearly state that SleepySockets is not transparent generic
  WebSocket sleep and requires an app protocol that tolerates upstream reconnect.

## Deferred

These should not shape V1 implementation details beyond keeping clear extension
points:

- HTTP/3 and QUIC listener.
- Multi-cluster remote forwarding and materialization leases.
- StatefulSet scale above one.
- Managed provider volume creation/deletion.
- Rich route predicates such as headers, ALPN, or arbitrary expressions.
