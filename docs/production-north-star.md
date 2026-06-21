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
- Frontline proxies follow an xDS-style model: keep a local route
  snapshot, watch control-plane updates, and call the control plane only for
  misses, wakes, and exceptional paths.
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
- Route snapshots and watch streams for proxies.
- Assignment of active materializations to clusters.

Example APIs:

```text
CreateInstance(workload_class, values, routes)
DeleteInstance(instance_id)
WakeInstance(instance_id)
ReportIdle(instance_id, generation, sidecar_observation)
ResolveRoute(identity)
PutHTTP01Challenge(host, token, key_authorization, expires_at)
ResolveHTTP01Challenge(host, token)
DeleteHTTP01Challenge(host, token)
WatchRoutes(proxy_id, cursor)
WatchMaterializations(cluster_id, cursor)
```

The database can use outbox rows, `LISTEN/NOTIFY`, or a queue internally, but
that is an implementation detail behind the control plane.

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
request. It first extracts request identity and canonicalizes it into a route
key suitable for local lookup.

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

The route table maps exact keys, wildcard host rules, and ordered path-prefix
rules to route entries:

```text
route key or match rule
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
  -> proxy resolves route key/rule from local snapshot
  -> if missing: call ResolveRoute(identity or route key)
  -> if route is unknown: reject
  -> if route is Running with backend: route to backend
  -> if route is Cold or missing backend: call WakeInstance(instance ID)
  -> wait for Running materialization
  -> route request or connection
```

Proxies must receive explicit invalidation or generation updates when an
instance sleeps, drains, changes route ownership, or is deleted. TTL-only cache
invalidation is not sufficient for production.

Exact custom domains and SNI names should be simple key/value lookups in the
local route snapshot. Wildcards and path routing need deterministic local match
rules. Use exact host first, then wildcard suffix candidates, and choose the
longest matching path prefix within the selected host rule. Negative resolutions
should be cached briefly to protect the control plane from arbitrary Host/SNI
scans.

Route watch ordering uses opaque cursors. Proxies should bootstrap with a full
snapshot or assigned shard, then maintain it through a watch stream. After
reconnect, a proxy sends its last accepted cursor. The control plane may return
deltas after that cursor or require a full resync if the cursor cannot be
resumed. The proxy stores cursors but never interprets them. Route entries still
include instance generation so proxies can discard stale backends after sleep,
delete, route reassignment, or failed wake.

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
2. Move tenant creation behind a versioned control-plane API.
3. Replace header-only routing with route identity extraction for Host and SNI.
4. Add a route watch API for proxies and generation-based invalidation.
5. Render active Kubernetes objects from `WorkloadClass + Instance.values`.
6. Add static PV/PVC materialization from `WorkloadClass` templates and
  instance values.
7. Split wake/sleep/delete into explicit reconciled state machines.
8. Keep the first production scope to `Deployment`, `StatefulSet`, HTTP/1.1,
  HTTP/2, h2c, gRPC, WebSockets, HTTPS termination, SNI passthrough, and  provider-backed durable volumes. Defer HTTP/3/QUIC while preserving protocol  listener boundaries that allow it later.
