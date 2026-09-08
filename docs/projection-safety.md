# Projection ownership and uncertain effects

A materialization has a stable `projection_generation` for its Kubernetes and
sidecar ownership stamp. Its instance CAS revision can change independently.
Every lease acquisition increments an attempt. Renew, release and completion
check the acquired owner, attempt and materialization generation; recreated
instance IDs cannot reuse an old lease even when their deterministic
materialization ID and attempt counter repeat.

Before each Kubernetes mutation the sole lifecycle driver records an effect in
Postgres under the materialization row lock. The effect includes its generation,
owner, attempt, individual effect ID, object ref and observed UID/resourceVersion.
Only an active matching lease can dispatch. Claims first lock the materialization,
then inspect the effect barrier in a fresh statement. An unacknowledged effect
prevents lease transfer and automatic inventory/key release. Exact effect ACKs
also lock first, so an ACK cannot miss a still-committing begin transaction or
erase a later operation. Begin replay matches the entire operation; the runtime
forwards begin once and retries exact ACKs on transient database failures.

The Kubernetes client uses create for an absent object. Updates carry the
observed UID and resourceVersion in a merge patch; foreground deletes carry
both in DeleteOptions preconditions. A conflict triggers a later inspection,
never an unconditional fallback. Applied inventory survives partial failure;
there is no unchecked rollback. Secrets are applied before services/workloads.
Cleanup requires all recorded refs to be absent and instance-labelled Pods and
ReplicaSets in the target namespaces to be absent, including terminating
members. Every managed static PV must explicitly use `Retain`, including raw
PV/PVC inventory. Class/render validation rejects destructive policies and
unsupported bindings. Legacy live cleanup validates all PV policies and PVC
bindings before any teardown; a Delete/unknown policy or unproven binding blocks
cleanup before deleting a PVC could trigger reclamation. The supported contract
assumes exclusive control over managed object metadata, Pod templates, and
PV/PVC specs, especially reclaim policy and volume bindings. PVC delete
preconditions cannot atomically fence a separate actor changing its PV policy
after preflight. Concurrent external storage-policy or binding edits require
quiescing managed work first. New drivers never write Delete and old writers
must be quiesced for migration. Removing ownership labels, force-reparenting
descendants or bypassing normal Kubernetes garbage collection is an operator
intervention.

Default leases last 60 seconds and renew every 20 seconds during apply, PVC and
readiness work. The concrete Kubernetes client bounds mutations and short reads
with its 10-second `delete_timeout`; the fenced driver adds a 15-second mutation
ceiling. PVC and readiness waits have a 120-second ceiling, including blocked
network reads. Read timeouts do not create effect uncertainty. Lease loss cancels
local work and prevents any subsequent dispatch or publication.

Renewal and failure publication also check the work state claimed by the attempt.
An accepted Delete changes Pending to Deleting without stealing its lease or
changing its immutable projection stamp. The old worker discovers this at its
next heartbeat and cooperatively cancels readiness. A definite operation failure
that finishes earlier also cannot publish into the new Deleting work. Failure
publication locks the instance before the materialization, matching acceptance,
then checks the current state in a fresh statement. Once the owned work settles,
an exact conditional release makes definite cleanup eligible without waiting for
lease expiry. The existing grace deadline still applies. An unresolved dispatched
effect blocks that release and any replacement, even after lease expiry; operator
status continues to expose its identity independently of the new work's failure
count. This is bounded intent preemption under functioning dependencies, not a
new wall-clock deletion SLA or a way to clear uncertain operations.

The heartbeat's renewal is polled alongside work as one owned future. An effect
begin transaction can hold the materialization row that renewal needs; work must
continue to be polled so it can commit and release that lock. Awaiting renewal
inside the timer branch would freeze its own transaction and delay cancellation.
No independent heartbeat task or overlapping renewal loop is created.

Each public one-shot reconciliation job owns a fresh cancellation token, as the
continuous runtime already does. Canceling one job cannot cancel a sibling or a
later use of the same library handle.

## Primary Service readiness

The primary Service retains its stable instance/workload selector. Only the
structured primary Pod template receives its workload-name label; raw auxiliary
Deployment/StatefulSet templates receive cleanup ownership without that label.
Explicit raw uses of the reserved primary selector key are rejected, and raw
standalone Pods remain unsupported. No immutable primary selector migration is
needed.

The injected sidecar has a private HTTP readiness probe on a separate declared
port. Its health listener becomes available only after the actual proxy binds;
`/ready` additionally requires a bounded in-Pod loopback app connection. Health
traffic does not reach the proxy or its idle/drain accounting. Both initial
readiness waits and live readiness inspections require EndpointSlice ownership
by the observed Service UID and a ready, nonterminating endpoint with an address.
Deleting Services/slices and ownerless or old same-name slices are unready.
Kubernetes' unspecified `ready` condition retains its documented ready default;
explicit false always blocks. This is a current transport observation, not an
application-level health guarantee or an atomic promise about future traffic.

Custom sidecar images need the readiness interface before deployment. Existing
Running rows keep their current projection until ordinary lifecycle replacement;
upgrade and sleep/wake them to adopt the probe and auxiliary isolation. See the
[operator guide](operator-guide.md) for port selection and upgrade details.

Already-Pending work with no old effects can render the new projection. A
partially applied old same-generation projection instead has a different desired
rendered hash and is rejected before any new apply. That is a permanent wake
failure; the scheduler queues recorded-ref cleanup, whose ownership checks do
not require the old desired hash, and retains inventory until cleanup succeeds.
This also avoids relying on merge-patch omission to remove old auxiliary labels.
An ordinary wake retry is safe after cleanup. Uncertain effects remain barred.

## Recovery boundary

A successful or definite rejected Kubernetes response acknowledges its exact
effect. Transient ACK failures are retried; a lost ACK response after its commit
is recoverable on the next ordinary pass. API conflicts and failures known before
dispatch preserve ordinary retry behavior. If begin returns an error, the live
driver knows it has not polled the Kubernetes operation and attempts an exact ACK
before returning.

A mutating transport error, timeout, cancellation or process loss can leave the
API server processing a request after its client disappears. In particular, an
old create can arrive after a replacement controller observes an absent name.
No Kubernetes read can prove that such a request will never arrive. The durable
effect remains blocked, retaining inventory and exclusivity. This also includes
the irreducible crash/cancellation window between committing begin and dispatch:
a fresh process cannot determine whether the request was sent. Autonomous
recovery is deliberately limited at this boundary. Supervised cooperative shutdown
now lets an owned live task finish begin and await an exact ACK when it knows the
Kubernetes future has never been polled. After dispatch, cancellation retains the
barrier. The controller allows 20 seconds to drain and the runtime 25 seconds;
a database call that cannot finish within that budget can still require hard
abort. Hard abort or process death cannot establish whether dispatch occurred.

`ReconcileMaterialization` with `status_only=true` exposes the durable scheduling
deadline, failure classification/message and complete uncertain-effect identity,
including expected UID/resourceVersion. The bounded operational gauges
`sleepypods_materialization_effects_uncertain` and
`sleepypods_materialization_failures_blocked` expose aggregate counts without
resource-ID labels.

An operator can diagnose the barrier with this read-only query (filter the
materialization ID as appropriate). It contains refs and fencing identities, not
Secret contents:

```sql
SELECT materialization_id, instance_generation, lease_owner, lease_attempt,
       effect_id, operation, object_ref, expected_uid,
       expected_resource_version, started_at_unix_millis
FROM materialization_effects
ORDER BY started_at_unix_millis;
```

`ForceDeleteMaterialization` explicitly clears the barrier and the recorded
inventory in one audited transaction. Before using it, fence the old process and
its request path, establish that previously accepted mutating API requests have
completed or cannot take effect, then inspect/clean the objects and descendants
and verify external singleton resources cannot be attached by the old workload.
Stopping a process or observing an absent name alone is insufficient. If that
proof is unavailable, retain the barrier and reservations. Record the fencing
and cleanup evidence in the required operator/reason fields. The same condition
applies before `ForceReleaseExclusivityKey`; it bypasses the automatic guarantee.
Do not edit effect rows directly as a recovery shortcut.

Upgrade migrations 8 and 9 with old writers quiesced and their in-flight mutation
requests settled. Mixing a driver that lacks these barriers with the new driver
cannot provide the new guarantee. Restore generation watermarks and effect rows
with the rest of the database.
