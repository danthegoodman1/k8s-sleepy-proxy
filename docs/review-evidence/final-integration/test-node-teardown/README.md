# Reversible test-node teardown

All deployed gates completed before this teardown. The reviewed [runner](stop-final-test-node.py) stops the exact task-created kind node and retains its container, volume and diagnostic data. It deletes no Kubernetes objects, persistent volumes, RBAC objects, containers or Docker volumes.

This is the materially safer alternative to the earlier diagnostic deletion requests declined by automatic review. It is not a whole-cluster deletion through a different tool. The runner guards the full container ID, name, creation timestamp, cluster/role labels and a fresh namespace/PV UID inventory. It also verifies that every unrelated container and the node mounts remain unchanged. Preconditions use explicit runtime checks and cannot be disabled by Python optimization.

Execution completed successfully at 2026-09-08 06:34:57 UTC after all final source gates passed. The [result](result.json), [container inventory before](containers-before.json), [inventory after](containers-after.json) and [Kubernetes inventory before](kubernetes-before.json) record the guarded action. The node is stopped; its container and volume remain. All unrelated container metadata, including the preexisting PostgreSQL container, is unchanged. The task-specific kubeconfig and archived evidence remain. No claim of global resource absence is made; the separate successful soak inventory covers only its four documented selectors.

The archived runner is the exact source executed from `.generated/implementation-evidence/stop-final-test-node.py`; it is retained as provenance, not a portable invocation from this evidence directory.
