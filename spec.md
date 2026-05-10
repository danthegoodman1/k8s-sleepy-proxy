## Sleepy Proxy PoC Spec

### Goal

Build a Kubernetes-based “sleepy proxy” system where tenant workloads are created on demand, shut down when idle, and fully removed from Kubernetes while retaining durable config in an external DB.

### Core idea

```text
External DB = source of truth for all tenants, accessed through a storage interface
Kubernetes = only contains active/warming workloads
LB = always-on front door
Controller = wake/sleep orchestrator
Sidecar = local traffic/idleness observer
```

When a tenant sleeps, delete its `StatefulSet` and `Service`. When traffic
returns, recreate them from the external DB.

---

## Components

### 1. External DB

PoC should use Postgres as the concrete external DB.

The controller should depend on a small storage interface rather than Postgres
directly, so additional backends can be added later without changing the
controller, LB, or sidecar behavior.

Example interface:

```text
TenantStore:
  GetTenant(ctx, tenantId) -> Tenant
  CreateTenant(ctx, tenant) -> Tenant
  CompareAndSwapState(ctx, tenantId, generation, fromState, toState) -> Tenant
  MarkRunning(ctx, tenantId, generation, backend) -> Tenant
  MarkFailed(ctx, tenantId, generation, reason) -> Tenant
  TouchTenant(ctx, tenantId, lastActiveAt) -> Tenant
  ListChangedSince(ctx, cursor) -> tenant changes
```

First implementation:

```text
PostgresTenantStore
```

Future implementations can satisfy `TenantStore`, but they are out of scope for
the PoC.

Table: `tenants`

```text
tenant_id        string primary key
image            string
upstream_port    int
public_host      string
idle_seconds     int
state            enum: Cold | Waking | Running | Draining | Failed
last_active_at   timestamp
generation       int
backend          string nullable
failure_reason   string nullable
```

Postgres is the durable source of truth. Kubernetes objects may be deleted at any
time and recreated from this row. All reads and writes should go through
`TenantStore`.

---

### 2. Controller

Implement with Kubebuilder/controller-runtime.

Responsibilities:

```text
POST /wake/:tenantId
  load tenant through TenantStore
  use TenantStore compare-and-swap to transition Cold -> Waking
  create Service + StatefulSet
  wait until backend is ready
  mark Running through TenantStore
  return backend DNS

POST /sleep/:tenantId
  validate sidecar request
  use TenantStore compare-and-swap to mark Draining
  remove tenant from LB routing
  delete StatefulSet + Service
  mark Cold through TenantStore

GET /state/:tenantId
  load tenant through TenantStore and return tenant state/backend info

GET /watch
  stream TenantStore state changes to LB nodes
```

For the PoC, `/watch` can be Server-Sent Events or skipped in favor of polling.

---

### 3. Generated StatefulSet

One replica max.

Pod contains:

```text
app container:
  image from tenant record
  listens on upstreamPort

sleepy-proxy sidecar:
  listens on :8080
  forwards to 127.0.0.1:upstreamPort
  tracks active connections / last request time
  calls controller /sleep when idle
```

Generated Service:

```text
sleepy-tenant-a.default.svc.cluster.local:80 -> sidecar :8080
```

---

### 4. Sidecar proxy

Responsibilities:

```text
proxy traffic to local app
track active requests/connections
track last traffic timestamp
after idle_seconds with 0 active requests:
  POST /sleep/:tenantId to controller
```

Payload:

```json
{
  "tenantId": "tenant-a",
  "podName": "sleepy-tenant-a-0",
  "activeConnections": 0,
  "lastTrafficAt": "2026-05-10T20:00:00Z",
  "reason": "idle_timeout"
}
```

PoC sidecar can be a simple HTTP reverse proxy.

---

### 5. Load balancer

Always-on process outside the sleeping workload path.

Request flow:

```text
client request for tenant-a
LB checks local cache or calls controller /state
if Running:
  proxy to backend DNS
if Cold:
  call POST /wake/tenant-a
  wait for backend ready
  proxy request
if Waking:
  wait/poll until ready
if Failed:
  return 503
```

Backend returned by controller:

```json
{
  "tenantId": "tenant-a",
  "state": "Running",
  "backend": "sleepy-tenant-a.default.svc.cluster.local:80"
}
```

For PoC, support HTTP only. Raw TCP buffering is out of scope.

---

## State machine

```text
Cold
  /wake
    -> Waking
    -> Running

Running
  idle sidecar /sleep
    -> Draining
    -> Cold

Waking
  readiness timeout
    -> Failed

Failed
  /wake retry
    -> Waking
```

Use `TenantStore` compare-and-swap semantics backed by Postgres transactions,
row locking, or equivalent atomic updates so duplicate `/wake` calls do not
create duplicate workloads.

---

## Kubernetes behavior

On wake:

```text
1. create Service
2. create StatefulSet replicas=1
3. wait for pod Ready or ready EndpointSlice
4. return backend DNS
```

On sleep:

```text
1. mark tenant Draining through TenantStore
2. stop routing new traffic
3. delete StatefulSet
4. delete Service
5. mark tenant Cold through TenantStore
```

---

## PoC shortcuts

Acceptable for first version:

```text
single namespace
single controller replica
Postgres DB
HTTP only
simple shared token auth between LB/sidecar/controller
one pod per tenant
no PVCs
no multi-cluster support
```

---

## Acceptance criteria

1. Register a tenant in the DB.
2. No Kubernetes workload exists initially.
3. First HTTP request to LB triggers `/wake`.
4. Controller creates Service + StatefulSet.
5. LB forwards request after pod is ready.
6. Sidecar detects idleness.
7. Sidecar calls `/sleep`.
8. Controller deletes Service + StatefulSet.
9. Later traffic wakes the tenant again from the DB record.
10. Duplicate wake requests do not create duplicate workloads.
