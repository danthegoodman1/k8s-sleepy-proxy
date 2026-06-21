# Development Plan

This plan turns the production north star into implementation milestones. Each
milestone should have integration coverage, and Kubernetes behavior should be
verified with kind-based end-to-end tests before it is considered done.

## Testing Strategy

Use four test layers:

1. Unit tests for pure logic:
   - route key normalization
   - wildcard and longest-prefix matching
   - state-machine transitions
   - template value validation
   - structured manifest rendering

2. Component integration tests:
   - proxy components against fake control-plane services
   - control plane against a real test database
   - Kubernetes materializer against a real or fake API server where useful

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
   - verify PV/PVC materialization before workload creation
   - verify custom host/SNI routing and HTTP-01 challenge lookup

The kind suite is a release gate. Unit and component tests are not enough for
this project because most failures will happen at Kubernetes object lifecycle,
networking, and readiness boundaries.

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

Done when:

- Unit tests cover accounting, drain, timeout, and SNI parsing.
- Integration tests proxy TCP streams, HTTP requests, and WebSocket sessions.
- Drain tests prove new work is rejected while existing streams get the grace
  period.

## Milestone 2: Control Plane Resource Model

Introduce the durable model behind the control-plane API.

Scope:

- `WorkloadClass` with immutable versions.
- `Instance` pinned to a `WorkloadClass` version.
- `RouteBinding` for host, wildcard host, SNI, and optional path prefix.
- `Materialization` for active cluster projections.
- HTTP-01 challenge records keyed by `(host, token)`.
- Instance state machine with generation checks.
- Structured manifest rendering from `WorkloadClass + Instance.values`.

Done when:

- Database migrations and store tests pass against a real test database.
- State-machine tests cover wake, running, draining, failed, retry, and delete.
- Manifest rendering tests cover Deployment, StatefulSet, Service, PV, and PVC.
- WorkloadClass version updates cannot mutate existing pinned instances.

## Milestone 3: Frontline Route Resolution

Build the frontline-specific routing behavior on top of the shared proxy
primitives.

Scope:

- Host/SNI/path identity extraction.
- Canonical route key generation.
- Exact host and SNI lookup.
- Wildcard host lookup.
- Longest path-prefix matching.
- Versioned route snapshot and watch handling.
- `ResolveRoute` fallback on local miss.
- `WakeInstance` flow when a route is Cold or missing a backend.
- Stale generation rejection.
- HTTP-01 challenge lookup through `ResolveHTTP01Challenge`.

Done when:

- Fake-control-plane integration tests cover cold wake, hot route, miss, stale
  generation, route update, and watch reconnect/resync.
- Protocol tests cover HTTP/1.1, HTTP/2, h2c gRPC, WebSockets, TLS
  termination, SNI passthrough, and HTTP-01.
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

Done when:

- Integration tests prove active traffic prevents idle reporting.
- Idle tests prove `ReportIdle` fires after the configured timeout.
- Drain tests prove the sidecar stops accepting new work and lets active work
  finish within the grace period.

## Milestone 5: Kubernetes Materializer

Render and reconcile active Kubernetes objects from control-plane state.

Scope:

- Render PV, PVC, Service, Deployment, and StatefulSet objects.
- Apply PV/PVC before workloads and wait for PVC Bound.
- Inject sidecar into rendered pod templates.
- Service targets the sidecar port.
- Sidecar targets the local app port.
- Wait for readiness through Pods or EndpointSlices.
- Delete materialized objects on sleep/delete.
- Leave backing provider volumes untouched.

Done when:

- Component tests validate rendered objects and ordering.
- kind tests prove cold wake creates PV/PVC before StatefulSet.
- kind tests prove sleep deletes workload, Service, PVC, and PV.
- kind tests prove re-wake recreates manifests from the same instance values.

## Milestone 6: End-to-End V1

Wire the control plane, frontline proxy, sidecar, and materializer together.

Scope:

- Create instance through control-plane API.
- Publish route snapshot to frontline proxy.
- Cold request wakes instance.
- Hot request routes from local snapshot.
- Sidecar reports idle.
- Control plane drains and sleeps materialization.
- Custom host and wildcard host route to the right instance.
- HTTP-01 challenge insert, resolve, serve, and delete flow works.

Done when:

- kind E2E passes for stateless Deployment.
- kind E2E passes for StatefulSet with static PV/PVC templates.
- kind E2E passes for HTTP/1.1, HTTP/2, h2c gRPC, WebSockets, TLS
  termination, and SNI passthrough.
- Failure-path E2E covers wake timeout, bad route, missing PVC binding, and
  stale proxy generation.

## Milestone 7: Hardening

Make V1 operationally credible.

Scope:

- Metrics and tracing for wake latency, route-cache hits, control-plane calls,
  drain duration, active streams, and materialization failures.
- Structured logs with instance ID, route ID, generation, and cluster.
- Backoff and retry policies.
- Proxy watch reconnect and full resync.
- Control-plane restart recovery from database state.
- Load tests for route lookup and hot proxy path.
- Soak tests for repeated wake/sleep cycles.

Done when:

- Automated tests cover restart during wake, sleep, and delete.
- Repeated kind wake/sleep soak passes without leaked Kubernetes objects.
- Route lookup and hot proxy path meet target latency under load.
- Dashboards or metric names are documented enough for operators to wire up.

## Deferred

These should not shape V1 implementation details beyond keeping clear extension
points:

- HTTP/3 and QUIC listener.
- Multi-cluster remote forwarding and materialization leases.
- StatefulSet scale above one.
- Managed provider volume creation/deletion.
- Rich route predicates such as headers, ALPN, or arbitrary expressions.

