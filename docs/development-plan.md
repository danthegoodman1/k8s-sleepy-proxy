# Development Plan

This plan turns the production north star into implementation milestones. Each
milestone should have integration coverage, and Kubernetes behavior should be
verified with kind-based end-to-end tests before it is considered done.

## Implementation Principles

- Prefer the simplest implementation that satisfies the current milestone and
  its tests.
- Keep code surface area small. Scalability and maintainability should come from
  clear boundaries, predictable state machines, and fewer moving parts before
  they come from clever abstractions.
- Add abstractions only when repeated behavior or testability makes the benefit
  concrete.
- Do not defer useful comments. Comments should explain protocol edge cases,
  lifecycle invariants, reconciliation assumptions, and places where a future
  maintainer could otherwise make a dangerous simplification.
- Keep each sub-phase reviewable. A phase is not done until the narrowest useful
  integration test proves the behavior works.

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
   - WebSockets
   - TLS termination
   - TLS/SNI passthrough
   - HTTP-01 challenge handling

4. kind end-to-end tests:
   - build local images
   - load images into kind
   - deploy control plane, frontline proxy, sidecar, and demo workloads
   - create `WorkloadClass`, `Instance`, and `RouteBinding`
   - verify cold wake, hot routing, drain/sleep, and re-wake
   - verify PV/PVC materialization before workload creation, intended binding,
     pod mount behavior, and data continuity across sleep/re-wake
   - verify custom host/SNI routing and HTTP-01 challenge lookup

The kind suite is a release gate. Unit and component tests are not enough for
this project because most failures will happen at Kubernetes object lifecycle,
networking, and readiness boundaries.

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

Sub-phases:

- 1A: TCP stream proxy, active connection accounting, and drain tracker.
- 1B: HTTP reverse proxy helpers and WebSocket upgrade/proxying.
- 1C: Timeout, backpressure, structured shutdown, and cancellation behavior.
- 1D: TLS ClientHello/SNI extraction helpers.
- 1E: Shared metrics and tracing conventions.

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

## Milestone 2: Control Plane Resource Model

Introduce the durable model behind the control-plane API.

Scope:

- Domain-specific `ControlPlaneStore` trait for persistence operations and
  transactional invariants.
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
- 2B: Postgres schema, migrations, and store implementation.
- 2C: `WorkloadClass` versioning, schema validation, and immutable version
  behavior.
- 2D: `Instance` APIs, value validation, generation fields, and idempotent
  create/update behavior.
- 2E: `RouteBinding` model, host/SNI/path/wildcard resolver, and uniqueness
  constraints.
- 2F: Instance state machine with generation/CAS transitions.
- 2G: HTTP-01 challenge store with put, resolve, delete, expiry, and GC.
- 2H: Structured manifest renderer for Deployment, StatefulSet, Service, PV,
  and PVC.

Done when:

- Database migrations and store tests pass against a real test database.
- Store conformance tests cover idempotency keys, transaction rollback,
  duplicate route/domain rejection, concurrent create/update conflicts, CAS
  generation failures, materialization generation updates, and provider config
  errors.
- The control plane can construct the configured store provider.
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
- TLS tests cover SNI certificate selection, missing certificate behavior, cert
  rotation, and passthrough for unknown or non-HTTP TLS traffic.
- HTTP-01 tests cover wrong host, wrong token, expired challenge, deleted
  challenge, response content type, and challenge path precedence before normal
  route resolution.
- Unknown Host/SNI negative caching protects the fake control plane from repeat
  misses.

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
- 6D: Protocol matrix: HTTP/1.1, HTTP/2, h2c gRPC, WebSockets, TLS
  termination, and SNI passthrough.
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
- kind E2E passes for HTTP/1.1, HTTP/2, h2c gRPC, WebSockets, TLS
  termination, and SNI passthrough.
- Failure-path E2E covers wake timeout, bad route, missing PVC binding, bad
  volume template, and stale proxy generation.
- Lifecycle-race E2E proves generation checks prevent stale sidecar reports,
  stale materializations, and stale proxy cache entries from changing current
  instance state.
- Control-plane restart E2E covers restart during wake, sleep, delete, route
  reassignment, and HTTP-01 challenge handling.

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
- Load tests for route lookup and hot proxy path.
- Soak tests for repeated wake/sleep cycles.

Sub-phases:

- 7A: Metrics, tracing, and structured log fields.
- 7B: Control-plane restart recovery during wake, sleep, and delete.
- 7C: Proxy `Subscribe` reconnect, lazy cache rebuild, and stale backend
  recovery.
- 7D: Load tests for route lookup and hot proxy path.
- 7E: kind wake/sleep soak tests and leaked-object detection.
- 7F: Operator-facing runbook and metric name documentation.

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
- Route lookup and hot proxy path meet target latency under load.
- Retry/backoff tests cover transient database errors, Kubernetes API conflicts,
  proxy stream disconnects, and materializer reconcile retries.
- Dashboards or metric names are documented enough for operators to wire up.

## Milestone 8: Operator and Contributor Documentation

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

Sub-phases:

- 8A: Operator-facing concepts, resource model, and request/lifecycle sequence
  diagrams.
- 8B: Operator task guides for workload classes, instances, routes, custom
  domains, HTTP-01, and existing volumes.
- 8C: Operator installation, configuration, database, Kubernetes, TLS, metrics,
  and upgrade guide.
- 8D: Operator troubleshooting and incident runbooks.
- 8E: Contributor local development and test guide.
- 8F: Agent-facing repository guide with file map, invariants, and common task
  entry points.

Done when:

- An operator can create a workload class, instance, route, custom domain, and
  existing-volume-backed workload from docs alone.
- An operator can predict what happens during cold wake, hot route, idle sleep,
  drain, delete, and route/domain changes.
- An operator can install, configure, monitor, back up, upgrade, and troubleshoot
  the platform from docs alone.
- API and protocol docs match the protobuf/Rust types and contain no stale RPCs.
- Documentation is concise: prefer short task-oriented files, stable headings,
  examples, and explicit invariants over broad narrative prose.
- Contributor and agent-facing guidance calls out source-of-truth files,
  generated files, commands, test gates, and design constraints without
  duplicating full specs.

## Deferred

These should not shape V1 implementation details beyond keeping clear extension
points:

- HTTP/3 and QUIC listener.
- Multi-cluster remote forwarding and materialization leases.
- StatefulSet scale above one.
- Managed provider volume creation/deletion.
- Rich route predicates such as headers, ALPN, or arbitrary expressions.
