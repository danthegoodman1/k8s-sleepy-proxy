# Production North Star

This project should evolve from a Kubernetes PoC into a control-plane-driven
sleepy workload platform. Kubernetes remains the execution substrate, but the
durable product model lives in the control plane database.

## Core Principles

- The control plane API is the only public write path for instances, routes,
domains, volume identities, and lifecycle state.
- The database is the source of truth, but proxies, sidecars, and users do not
write to it directly.
- Kubernetes contains only active, warming, or draining materializations.
Sleeping instances should not require Deployments, StatefulSets, Services,
PVs, or PVCs to remain in the cluster.
- Frontline proxies use lazy route resolution: resolve identities on cache miss,
  keep a bounded local route cache, and receive targeted updates or
  invalidations through opaque subscription IDs returned by the control plane.
- Workloads are described by reusable classes plus per-instance values, not by
one permanent Kubernetes manifest per instance.

## Primary Resources

### WorkloadClass

A reusable template and policy bundle for one kind of workload.

It defines:

- `Deployment` or `StatefulSet` materialization.
- Pod template shape, sidecar injection policy, probes, resources, and env
contracts.
- Service ports and the mapping from public/service ports to sidecar ports and
local app ports.
- Volume templates, including provider, mount path, access mode, reclaim
behavior, and whether PV/PVC objects are materialized only while awake.
- Scaling and sleep policy, including min/max replicas and idle timeout bounds.
- A schema for allowed instance `values`.

`WorkloadClass` is the right name because it matches Kubernetes concepts like
`StorageClass`, `IngressClass`, and `RuntimeClass`: a reusable class of runtime
behavior rather than just a bundle of templates.

`WorkloadClass` versions should be immutable. An `Instance` pins one specific
class version, and updating a class creates a new version rather than mutating
the behavior of existing sleeping instances. Moving an instance to a new class
version should be an explicit upgrade workflow.

### Instance

One tenant/customer/app/database created from a `WorkloadClass`.

It contains:

- Stable instance ID.
- Referenced `WorkloadClass` and version.
- Validated `values` map injected into the class templates.
- Lifecycle state such as `Cold`, `Waking`, `Running`, `Draining`, `Failed`, and
`Deleting`.
- Generation number for compare-and-swap state transitions and stale update
rejection.

The instance is mostly a map, but the map must be schema-validated, versioned,
and kept separate from secrets and operational status. If a workload needs a
pre-existing provider volume, the provider volume handle is just another
validated value mapped into the `WorkloadClass` PV/PVC templates.

### RouteBinding

Maps request identity to an instance.

It should be first-class, not hidden inside arbitrary instance values, because it
needs uniqueness, indexing, validation, certificate state, and fast proxy lookup.

V1 identity inputs are:

- HTTP `Host`.
- Optional HTTP path prefix.
- Custom domains.
- TLS SNI.
- Wildcard or base-domain subdomain mapping.

Future identity inputs can include headers, ALPN, and protocol-specific fields
such as a Postgres startup database or user.

### Materialization

The active Kubernetes projection of an instance generation into one cluster.

It records:

- Instance ID and generation.
- Target cluster and namespace.
- Rendered object names.
- Backend address exposed to proxies.
- Readiness and failure state.

Materializations are transient. They can be deleted on sleep and recreated on
wake from the control-plane database.

## Control Plane Shape

The control plane owns:

- Instance creation, validation, idempotency, and audit logs.
- Route/domain registration and uniqueness.
- PV/PVC manifest rendering from `WorkloadClass` templates and instance values.
- Wake/sleep/delete state machines.
- Template rendering and generation hashes.
- Route resolution and targeted update/invalidation streams for active proxy
  subscriptions.
- Assignment of active materializations to clusters.

Example APIs:

```text
CreateInstance(workload_class, values, routes)
DeleteInstance(instance_id)
WakeInstance(instance_id)
ReportIdle(instance_id, generation, sidecar_observation)
PutHTTP01Challenge(host, token, key_authorization, expires_at)
ResolveHTTP01Challenge(host, token)
DeleteHTTP01Challenge(host, token)
Subscribe(proxy_id) bidi stream:
  proxy -> SubscribeRoute(request_id, identity)
  proxy -> Unsubscribe(subscription_id)
  control plane -> RouteResolved(request_id, subscription_id, route_entry, cache_policy)
  control plane -> RouteMiss(request_id, negative_cache_policy)
  control plane -> RouteInvalidated(subscription_id, reason)
WatchMaterializations(cluster_id)
```

The database is the source of truth for route bindings, instances, and
materializations. Live proxy update delivery is a control-plane responsibility,
not a direct database responsibility.

The control-plane API should be defined once with protobuf and exposed through
both native gRPC and gRPC-Web for operator-facing APIs. gRPC-Web support lets
browser and other V8-based environments interact with the control plane without
native gRPC transport support. Keep the proxy/control-plane `Subscribe` stream on
native gRPC in V1 because it is bidirectional; gRPC-Web should cover unary and
server-streaming operator APIs unless a future WebSocket-based proxy protocol is
explicitly added.

Template rendering should be structured and constrained. Prefer typed `WorkloadClass` fields plus limited substitution from validated
`Instance.values` over arbitrary text templating or user-supplied executable
logic. Store a rendered generation hash so the controller can reject stale
updates and explain what was materialized.

Control-plane authentication and authorization are operator policy. The platform
should expose clear API boundaries, but tenant/user/org permission models are
left to the operator integrating the control plane.

## Data Plane Shape

Frontline proxies are always on and keep local route state.

The proxy should not assume it can derive an instance ID directly from every
request. It first extracts request identity and canonicalizes it into a local
cache key. That key is only a proxy-local lookup handle; route dependency
tracking is represented by opaque subscription IDs from the control plane.

Examples:

```text
HTTP Host:
  host:app.customer.com

TLS SNI:
  sni:db.customer.com

HTTP Host + path prefix:
  host:app.customer.com|path:/api

Wildcard host:
  host:*.customer.com
```

For the first production routing model, support host-like identity, meaning HTTP
Host or TLS SNI, plus optional HTTP path prefix. Hosts may be exact names or
wildcard suffixes. Richer compound identity such as headers, ALPN, and
protocol-specific fields can be added later after this path is solid.

The proxy keeps a bounded local cache of resolved route entries. Local route
lookup maps exact keys, wildcard host rules, and ordered path-prefix rules to
cache entries:

```text
local route key or match rule
  -> subscription ID
  -> route ID
  -> instance ID
  -> state/generation
  -> backend if currently materialized
```

Request flow:

```text
client request
  -> proxy extracts Host/SNI and optional path
  -> proxy canonicalizes identity into an exact route key or match candidate
  -> proxy resolves route key/rule from local cache
  -> if missing: send SubscribeRoute(request ID, identity) on Subscribe stream
  -> control plane returns RouteResolved or RouteMiss for that request ID
  -> if route is unknown: reject
  -> if route is Running with backend: route to backend
  -> if route is Cold or missing backend: call WakeInstance(instance ID)
  -> wait for Running materialization
  -> route request or connection
```

Proxies must receive explicit invalidation or generation updates for active
subscriptions when an instance sleeps, drains, changes route ownership, or is
deleted. TTL-only cache invalidation is not sufficient for production, but TTLs
remain useful as a safety net if a proxy misses an invalidation.

Exact custom domains and SNI names should be simple cache keys. Wildcards and
path routing need deterministic local match rules. Use exact host first, then
wildcard suffix candidates, and choose the longest matching path prefix within
the selected host rule. Negative resolutions should be cached briefly to protect
the control plane from arbitrary Host/SNI scans.

`SubscribeRoute` is the authoritative cache-miss path. It atomically resolves the
identity and creates an active subscription before returning `RouteResolved`.
That response includes the current route entry, cache policy, and an opaque
`subscription_id`. The proxy stores that ID with the local cache entry and uses
it only to apply later stream messages; it must not parse the ID or assume it is
a route binding, instance, version, or cursor. The control plane maps that
subscription to the underlying dependencies, such as the matched `RouteBinding`,
`Instance`, and active `Materialization`.

`Subscribe` is a long-lived bidirectional stream. The proxy sends
`SubscribeRoute` when a request misses local cache and sends `Unsubscribe` when
it evicts a cache entry. The control plane streams lookup responses and later
targeted messages for active subscriptions:

```text
SubscribeRoute(request_id, identity)
Unsubscribe(subscription_id)
RouteResolved(request_id, subscription_id, route_entry, cache_policy)
RouteMiss(request_id, negative_cache_policy)
RouteInvalidated(subscription_id, reason)
RouteUpdated(subscription_id, route_entry, cache_policy)  // later optimization
```

V1 can be invalidation-only: remove the cache entry associated with
`subscription_id`, then let the next request send `SubscribeRoute` again. If a
proxy restarts, it loses its cache and subscriptions; it simply subscribes routes
lazily again as requests arrive.

`request_id` is stream-local and only correlates a `SubscribeRoute` request with
its first response because the stream can have multiple in-flight route misses.
`RouteMiss` should not return a subscription ID by default; unknown Host/SNI
scans should get short negative caching without creating unbounded control-plane
subscription state. `Unsubscribe` should be idempotent because cache eviction,
stream reconnect, and invalidation handling can race.

For V1, the active subscription registry can be in-memory in the control-plane
process that owns the proxy's `Subscribe` stream. Missed updates are bounded by
cache TTLs and instance generation checks. Multi-replica fanout can be added
later with a shared bus or provider-backed change feed without changing the
proxy cache-miss model.

Polling loops, provider change streams, versions, watch cursors, and ordering
tokens are entirely internal to the control plane and store provider. The proxy
does not send a cursor on reconnect. If `Subscribe` reconnects, the proxy should
drop or mark stale its subscription-backed cache entries, resubscribe any kept
identities through `SubscribeRoute`, and rebuild missing ones lazily as requests
arrive.

Route entries include instance generation so proxies can discard stale backends
after sleep, delete, route reassignment, or failed wake. Subscriptions should be
ephemeral and bounded by cache TTL, heartbeat, stream lifetime, or explicit
unsubscribe when a proxy evicts a local cache entry.

The same model should support:

- HTTP/1.1, HTTP/2, h2c, gRPC over h2c, and WebSockets.
- HTTPS with TLS termination and SNI-based certificate selection.
- TLS passthrough with SNI sniffing and byte-for-byte forwarding.
- Protocol-specific TCP listeners where the protocol exposes identity.

HTTP/3 and QUIC are explicitly out of scope for V1, but the routing core should
not assume TCP-only HTTP semantics. Protocol listeners should feed a shared
identity extraction, route resolution, wake, and backend streaming core so an
HTTP/3 listener can be added later without changing the control-plane resource
model.

TLS and certificate state come from the control plane as part of route/listener
identity. The control plane is responsible for creating and storing certificates
for custom domains. Frontline proxies must support HTTP-01 challenge handling by
calling `ResolveHTTP01Challenge(host, token)` for
`/.well-known/acme-challenge/*` requests before normal route resolution. The
control plane returns the challenge response body when the token is active, or a
miss when the request should continue through normal routing or be rejected.

HTTP-01 challenge records are short-lived control-plane entries keyed by
`(host, token)`. When ACME issuance starts, the ACME owner inserts
`(host, token, keyAuthorization, expiresAt)` through `PutHTTP01Challenge`. The
frontline proxy resolves that pair through `ResolveHTTP01Challenge` and serves
the returned `keyAuthorization` as `text/plain`. After the ACME authorization
succeeds, fails, or is cancelled, the owner calls `DeleteHTTP01Challenge`.
Expired challenge records should also be garbage-collected so stale tokens are
not served indefinitely.

## Workload Shape

Active workloads should usually be rendered as:

```text
Service
  -> sleepy sidecar port in each selected pod
     -> local app container on 127.0.0.1:<app-port>
```

The sidecar should never proxy back to the same Service that targets the
sidecar, because that can loop. The original service port contract should be
used to configure the sidecar's local upstream.

Supported workload kinds:

- `Deployment` for stateless or horizontally scalable workloads.
- `StatefulSet` for stable identity, per-replica storage, and stateful services.
For V1, StatefulSet scale above one is out of scope.

Other Kubernetes workload types are out of scope until these two are robust.

On sleep, the control plane should first stop new routing by publishing a
draining generation. Existing requests/connections get a configurable grace
period before Kubernetes objects are deleted. Use 60 seconds as the default
drain grace period, with per-class override and a hard maximum. If active
traffic remains after the grace period, the materialization is deleted anyway and
the old backend generation is invalidated.

## Instance State Machine

Instances use explicit state transitions with generation checks:

```text
Cold
  WakeInstance -> Waking

Waking
  materialization ready -> Running
  timeout/error -> Failed

Running
  ReportIdle -> Draining
  DeleteInstance -> Deleting

Draining
  drain grace elapsed and materialization deleted -> Cold
  new wake requested before deletion completes -> Waking or Running after reconcile

Failed
  WakeInstance retry -> Waking
  DeleteInstance -> Deleting

Deleting
  routes removed, materialization deleted, instance tombstoned -> deleted
```

Materializations should also carry instance ID, instance generation, rendered
hash, cluster, namespace, readiness, and failure reason. Reconciliation must be
idempotent so a controller restart during wake, sleep, or delete can continue
from database state.

## Storage Manifest Lifecycle

For stateful workloads with pre-existing CSI/provider volumes:

```text
Create instance:
  validate instance values, including any provider volume handles
  keep instance Cold

Wake:
  render PV from WorkloadClass template and instance values
  render PVC bound to PV
  wait for PVC Bound
  render Deployment/StatefulSet mounting PVC
  wait for readiness
  publish Running backend

Sleep:
  mark Draining
  invalidate proxy routes for old generation
  delete workload and Service
  delete PVC and PV
  leave backing provider volume untouched
  mark Cold

Delete instance:
  remove routes/domains
  delete any active materialization
  tombstone Instance
```

This keeps Kubernetes object count tied to active workloads while preserving
durable state across sleep. The platform owns Kubernetes manifests, not the
underlying provider volume lifecycle. Provider disk creation, deletion,
snapshotting, and recovery are external responsibilities unless a future managed
storage mode explicitly adds that ownership.

## Implementation Direction

1. Introduce the control-plane resource model in the DB: `WorkloadClass`,
   `Instance`, `RouteBinding`, and `Materialization`.
2. Move tenant creation behind a versioned protobuf control-plane API exposed
   through native gRPC and gRPC-Web for operator clients.
3. Replace header-only routing with route identity extraction for Host and SNI.
4. Add a route subscription API that resolves cache misses on the stream,
   targets later updates by opaque subscription ID, and keeps watch cursors,
   versions, and provider polling internal to the control plane.
5. Render active Kubernetes objects from `WorkloadClass + Instance.values`.
6. Add static PV/PVC materialization from `WorkloadClass` templates and
  instance values.
7. Split wake/sleep/delete into explicit reconciled state machines.
8. Keep the first production scope to `Deployment`, `StatefulSet`, HTTP/1.1,
  HTTP/2, h2c, gRPC, WebSockets, HTTPS termination, SNI passthrough, and  provider-backed durable volumes. Defer HTTP/3/QUIC while preserving protocol  listener boundaries that allow it later.
