# SleepyPods Operator Guide

SleepyPods is operated through the control-plane API. Kubernetes is the
execution substrate, but operators create and change user resources through
`OperatorControlPlane`, not by applying generated workload manifests directly.

The protobuf source of truth is
`crates/control-plane/proto/sleepypods/controlplane/v1/control_plane.proto`.
Generated native gRPC clients and gRPC-Web clients use the same unary operator
RPCs. There is no stable operator CLI yet, so examples below use request shapes.

## Resource Model

- `WorkloadClassVersion`: immutable template and policy bundle. It defines a
  `Deployment` or single-replica `StatefulSet`, app container, sidecar template,
  Service, optional PV/PVC volume templates, value schema, default values, and
  sleep policy.
- `Instance`: one workload created from a pinned workload class version plus
  validated string `values`. State is `Cold`, `Waking`, `Running`, `Draining`,
  `Failed`, `Deleting`, or `Deleted`, with a generation for stale-update
  rejection.
- `RouteBinding`: maps an HTTP Host/path or TLS SNI identity to an instance.
  Exact hosts, wildcard suffix hosts, custom domains, and optional HTTP path
  prefixes are first-class resources.
- `Http01Challenge`: ACME HTTP-01 token keyed by `(host, token)`. The frontline
  checks challenge paths before normal route resolution.
- `Materialization`: transient Kubernetes projection of an active instance
  generation. It records rendered object refs, target cluster/namespace, backend
  URI, readiness, and failure state.
- `WorkloadSleepPolicy`: idle timeout, idle-report retry backoff, drain grace
  timeout, and optional per-instance idle-timeout override bounds.
- Volume templates: static PV/PVC templates rendered from class template fields
  and instance values. Existing provider volumes are attached by putting the
  provider handle/path into an allowed instance value and substituting it into a
  CSI or hostPath source template.

Sleeping instances should not require active Deployments, StatefulSets,
Services, PVs, or PVCs in the cluster. They are recreated on wake from the
control-plane database.

## Operator API

The V1 operator service is unary-only:

- `CreateWorkloadClassVersion`, `GetWorkloadClassVersion`
- `CreateInstance`, `GetInstance`, `DeleteInstance`
- `CreateRouteBinding`, `GetRouteBinding`, `DeleteRouteBinding`
- `PutHttp01Challenge`, `ResolveHttp01Challenge`,
  `DeleteHttp01Challenge`, `ExpireHttp01Challenges`

`WakeInstance`, `Subscribe`, and `ReportIdle` are runtime services for proxies
and sidecars. They are not operator or gRPC-Web APIs.

Control-plane authentication is caller authentication at this API boundary. It
does not authenticate application end users and it does not replace network
policy, gateway, or service-mesh placement for direct control-plane exposure.
Native gRPC operator, proxy, and sidecar services and the gRPC-Web operator
listener use the same role policy:

- Operator credentials call `OperatorControlPlane`.
- Proxy credentials call `ProxyControlPlane/WakeInstance` and
  `ProxyControlPlane/Subscribe`.
- Sidecar credentials call `SidecarControlPlane/ReportIdle`.

The first provider is static bearer tokens. Configure it with
`SLEEPYPODS_CONTROL_PLANE_AUTH_MODE=static-bearer-token` plus distinct
`SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN`,
`SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN`, and
`SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` values. Frontlines need
`SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN` for wake/subscribe traffic and
`SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN` when HTTP-01 challenge serving is
enabled. The control plane injects `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN`
into rendered sidecars from runtime config; operators do not put this token in
WorkloadClass templates. Browser/gRPC-Web and native operator clients send
`Authorization: Bearer <operator-token>`.

`SLEEPYPODS_CONTROL_PLANE_AUTH_MODE=no-auth` is for local development and tests
only. It is explicit; omitting the auth mode or configuring malformed, missing,
or duplicate static tokens fails startup instead of silently disabling auth.
Rotate static tokens by updating the control-plane token set and rolling
callers with the corresponding new role token. During rotation, keep exposure
behind trusted network boundaries because static bearer tokens are shared
secrets.

## Common Tasks

Create a workload class version:

```text
CreateWorkloadClassVersion({
  idempotency_key: "wc-web-v1-20260624",
  class_id: "web",
  version: 1,
  default_values: {"image": "registry.example/web:2026-06-24"},
  value_schema: {fields: {"tenant": {required: true}, "image": {required: true}},
    allow_extra: false},
  template_generation: 1,
  template: {
    workload: {
      kind: WORKLOAD_KIND_DEPLOYMENT,
      name: {parts: [{literal: "web-"}, {instance_value: "tenant"}]},
      app_container: {name: "app", image: {parts: [{instance_value: "image"}]},
        ports: [{name: "http", container_port: 8080}]}
    },
    sidecar: {name: "sleepypods-sidecar", image: {parts: [{literal: "sidecar:prod"}]}, listen_port: 15000},
    service: {name: {parts: [{literal: "web-"}, {instance_value: "tenant"}]},
      ports: [{name: "http", port: 80, target_port: 8080}]},
    volumes: []
  },
  sleep_policy: {idle_timeout_ms: 300000, idle_retry_backoff_ms: 5000,
    drain_grace_timeout_ms: 30000}
})
```

Create an instance:

```text
CreateInstance({
  idempotency_key: "instance-tenant-a-20260624",
  instance_id: "tenant-a",
  workload_class: {class_id: "web", version: 1},
  values: {"tenant": "tenant-a", "image": "registry.example/web:2026-06-24"}
})
```

### Kubernetes Naming Rules

`instance_id` must be a Kubernetes DNS label: lowercase `a-z`, digits, and
hyphens only, starting and ending alphanumeric, with a maximum length of 63
characters. Invalid IDs are rejected by `CreateInstance` before any instance is
stored.

Workload, Service, PVC, and PV template names are operator-readable base names,
not final Kubernetes object names. On wake, SleepyPods appends an instance
suffix to every instance-scoped object name:

```text
<base-name-truncated-if-needed>-<instance-id-prefix>
```

For instance IDs of at least eight characters, the prefix is the first eight
characters. Shorter valid IDs use the whole ID. The suffix is preserved and the
operator base name is truncated first so the final name remains a DNS label no
longer than 63 characters. SleepyPods does not add the Kubernetes object kind to
generated names; choose base names such as `web`, `api`, `data-pvc`, or
`tenant-pv` when kind readability is useful.

Explicit custom naming templates are allowed, including templates that render
the same base for a workload and Service. The control plane still injects the
instance suffix, validates the final Deployment, StatefulSet, Service, PVC, and
PV names, and rejects duplicate or colliding rendered object refs before any
Kubernetes apply. PersistentVolumes are cluster-scoped and are collision-checked
with an empty namespace; namespaced objects are checked with their rendered
namespace.

Add a route or custom domain:

```text
CreateRouteBinding({
  idempotency_key: "route-tenant-a-app",
  route_binding_id: "route-tenant-a-app",
  instance_id: "tenant-a",
  protocol: PROTOCOL_ROUTE_HTTP,
  identity: {http: {host: {kind: ROUTE_HOST_KIND_EXACT, host: "app.example.com"},
    path_prefix: "/"}}
})
```

Use `ROUTE_HOST_KIND_WILDCARD_SUFFIX` for wildcard suffix routing, for example
`host: "*.apps.example.com"`. Use `PROTOCOL_ROUTE_TLS_SNI` with `identity.sni`
for TLS/SNI passthrough routes.

Add an HTTP-01 challenge token:

```text
PutHttp01Challenge({
  key: {host: "app.example.com", token: "token-from-acme"},
  key_authorization: "token-from-acme.account-key-thumbprint",
  expires_at_unix_millis: 1782260000000
})
```

After the ACME check finishes, call `DeleteHttp01Challenge`. Periodically call
`ExpireHttp01Challenges` with the current Unix milliseconds to garbage-collect
expired records.

Attach an existing volume:

1. Add required value fields such as `volume_handle`, `mount_path`, or
   `host_path` to the workload class schema.
2. Reference those values from a `VolumeTemplate` `source.csi.volume_handle` or
   `source.host_path.path`, plus `pv_name`, `pvc_name`, `capacity`, access
   modes, reclaim policy, and optional storage class.
3. For singleton external resources that must not be attached by two active
   materializations at once, declare a workload-class `exclusivity_keys` entry
   such as `name: "disk"` and `value: "{{ volume_handle }}"`.
4. Create the instance with the provider volume handle/path in `values`.

The control plane renders PVs first, then PVCs, then Service and workload. PVCs
are bound before the backend is published. Exclusivity keys are opt-in and
opaque to SleepyPods: the control plane does not parse provider disk IDs or infer
shared singleton resources from template values. A rendered key is acquired
before Kubernetes apply starts and stays held until sleep/delete cleanup
finalizes the materialization.

Delete an instance or route:

- Use `DeleteRouteBinding` to remove a route/custom domain.
- Use `DeleteInstance` to remove an instance. If an active materialization
  exists, the control plane attempts Kubernetes cleanup before finalizing store
  deletion.

Sleep and wake:

- Wake is runtime-driven: a cold route request causes the frontline to call
  `ProxyControlPlane/WakeInstance`.
- Sleep is sidecar-driven: when active work reaches zero for the idle timeout,
  the sidecar drains and reports idle to the control plane.
- Operators can inspect state with `GetInstance`.

## Lifecycle Expectations

- Cold wake: a request misses or resolves to a cold route, the frontline asks
  the control plane to wake the instance, the control plane renders Kubernetes
  objects, waits for PVC binding and readiness, records a ready materialization,
  and returns a backend URI.
- Hot route: the frontline uses its local route cache and must not call the
  control plane on measured hot-cache hits.
- Idle sleep and drain: the sidecar stops accepting new work, waits for active
  streams up to the drain grace timeout, reports idle, and the control plane
  deletes recorded Kubernetes objects.
- Delete: operator delete removes active materialization objects when present,
  invalidates route dependencies, and finalizes the durable instance state.
- Route/domain change: create/delete route bindings through the API. Active
  proxy subscriptions are invalidated so the next request resolves the new
  route instead of relying only on TTL expiry.
- Restart recovery: durable state is in Postgres. Control-plane restart can
  resume wake, sleep cleanup, delete cleanup, HTTP-01 state, and route
  reassignment from database state.
- Persistent disk behavior: PV/PVC manifests are active materialization objects.
  Use reclaim policy and provider volume handles deliberately; data continuity
  comes from the external volume, not from keeping Sleeping Kubernetes objects.
  Declare workload-class exclusivity keys for external resources that require
  single-writer or single-attachment behavior across instances.

## Installation And Configuration

Run three production components:

- Control plane: native gRPC listener for operator, proxy, and sidecar services;
  optional gRPC-Web listener for operator unary APIs.
- Frontline: always-on HTTP listener; optional TLS termination and TLS/SNI
  passthrough listeners; connects to the control plane.
- Sidecar: injected into materialized workloads; proxies to the local app port
  and reports idle.

Important environment variables:

| Component | Variable |
| --- | --- |
| control plane | `SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR` |
| control plane | `SLEEPYPODS_OPERATOR_GRPC_WEB_LISTEN_ADDR` optional |
| control plane | `SLEEPYPODS_CONTROL_PLANE_AUTH_MODE=no-auth` for local tests, or `static-bearer-token` for configured auth |
| control plane | `SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN`, `SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN`, `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` when static auth is enabled |
| control plane | `SLEEPYPODS_STORE_PROVIDER=postgres` |
| control plane | `SLEEPYPODS_POSTGRES_URL` |
| control plane | `SLEEPYPODS_CLUSTER_ID`, `SLEEPYPODS_NAMESPACE` |
| frontline | `SLEEPYPODS_FRONTLINE_LISTEN_ADDR` |
| frontline | `SLEEPYPODS_CONTROL_PLANE_ENDPOINT` |
| frontline | `SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN` when control-plane static auth is enabled |
| frontline | `SLEEPYPODS_CONTROL_PLANE_OPERATOR_TOKEN` when HTTP-01 challenge serving is enabled under static auth |
| frontline | `SLEEPYPODS_ROUTE_CACHE_CAPACITY` optional, default `1024` |
| frontline | `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS` optional, default `30000` |
| frontline | `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR` optional |
| frontline | `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_CERTS` as `sni|cert|key;...` |
| frontline | `SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR` optional |
| sidecar | rendered by the control plane: listen address, app port, instance ID, generation, control-plane endpoint, idle policy, `SLEEPYPODS_SIDECAR_MODE`, and runtime-injected `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` when static auth is enabled |

Kubernetes permissions must allow the control plane service account to
server-side apply and delete rendered Deployments, StatefulSets, Services, PVs,
and PVCs, and to read PVC/Service/EndpointSlice readiness state in the target
namespace. The default field manager is `sleepypods-control-plane`.

Back up Postgres before upgrades. Treat the database as the source of truth for
instances, routes, materialization state, HTTP-01 records, and idempotency. For
upgrades, roll the control plane first, then frontlines, then sidecars through
normal workload re-materialization. Keep old images available until sleeping and
running instances have moved through the intended class-version upgrade path.

## V1 Limits

- StatefulSet replicas above one are rejected.
- HTTP/3 is deferred.
- Multi-cluster remote forwarding is deferred; V1 materializes into the
  configured target cluster/namespace.
- Rich route predicates beyond host/SNI and optional HTTP path prefix are
  deferred.
- Generated examples are illustrative request shapes until a CLI exists.
- Metrics are emitted as structured observations today; a Prometheus exporter is
  not currently shipped.
