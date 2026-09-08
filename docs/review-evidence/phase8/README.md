# Phase 8: Single-member automatic sleep

Implementation evidence collected on 2026-09-07. Independent review and the
deployed Kubernetes gate are recorded separately by the phase coordinator.

The supported automatic-sleep workload is one structured Deployment or
StatefulSet, with exactly one desired replica and exactly one observed ready,
non-terminating Pod. Deployment templates use Recreate. Class creation, instance
validation against stored classes, and rendering reject zero or multiple
replicas. Existing auxiliary raw controllers still render, but refuse automatic
sleep because their activity has no aggregate observation.

The sidecar carries its downward-API Pod UID in ReportIdle. The control plane
requires a current Ready materialization and checks live controller labels,
materialization ownership, desired replicas, Pod generation/UID/readiness and
the controller owner-reference chain. It includes old, unready and terminating
Pods in the membership count. Deployment Pods must belong to an existing,
non-terminating ReplicaSet owned by the current Deployment. A final controller
UID/resourceVersion read rejects replacement or scaling during observation.
Missing permissions or unsupported inspection fail closed; the Kubernetes
inspection has a deadline.

Membership verification is a snapshot. It is not an atomic Kubernetes/Postgres
transaction and cannot fence an external replacement after the observation.
The operator guide states the exclusive managed-workload ownership requirement,
failure limitations and migration order. Internally driven lifecycle changes
must still be fenced by generation; phase 6 owns that lifecycle implementation.

Validation:

- `cargo test -p control-plane --lib --test sidecar_api_transport --test api_transport --test proxy_api_transport --test kind_e2e_lifecycle_races`:
  236 library tests and 43 operator API tests passed. The lifecycle kind test
  compiled and remained ignored. Four proxy API fixture failures in the parallel
  route-store change interrupted this combined command before the sidecar API
  test; see `control-plane-tests.log`. The coordinator owns their integration.
- `cargo test -p control-plane --test sidecar_api_transport`: all 15 passed,
  including rejection for old/missing Pod UID, extra members, unsupported stored
  replicas and missing, Pending or stale materializations. Native gRPC success
  now seeds the required Ready materialization.
- `cargo test -p sidecar`: 55 library, 3 binary and 2 native process tests passed.
  Native process fixtures and standalone load/image scripts supply Pod UID.
- `cargo clippy -p control-plane -p sidecar --all-targets -- -D warnings` passed;
  see `clippy.log`. `git diff --check` passed.
- Seven `kube_materializer::idle_membership` tests pass. The production verifier
  is exercised through a mock Kubernetes HTTP service, including request paths,
  Pod selector and response decoding. Cases cover both controller kinds,
  replaced/scaled controllers, replaced/unowned/terminating ReplicaSets,
  forbidden Pod reads, extra controllers and a stalled API deadline.
- `lifecycle_races_through_deployed_platform` now includes current-generation
  wrong-UID rejection and externally scaled two-replica rejection while the
  instance stays Running. Accepted idle-report fixtures load actual Pod UID and
  generation. The deployed test has not been run in this implementation lane.

No performance claim is made by this phase. Live membership inspection adds
Kubernetes reads only when an idle report attempts to initiate sleep.
