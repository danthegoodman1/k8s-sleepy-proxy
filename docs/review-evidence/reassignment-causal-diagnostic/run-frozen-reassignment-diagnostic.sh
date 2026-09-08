#!/usr/bin/env bash
set -euo pipefail
repo_root=/Users/dangoodman/code/sleepy-pods
namespace=sleepypods-e2e-reassignment-wire-20260908
cluster_name=sleepypods-review-remediation
image_prefix=sleepypods
image_tag=review-remediation
app_image=sleepypods/routing-app:review-remediation
late_app_image=sleepypods/routing-app-unused:diagnostic
postgres_image=postgres:17-alpine
control_plane_replicas=1
operator_port=19751
frontline_port=19780
kubeconfig="${repo_root}/.generated/implementation-evidence/kubeconfig"
out="${repo_root}/docs/review-evidence/reassignment-causal-diagnostic"
control_plane_pf_log="${out}/deployed-control-plane-portforward.log"
frontline_pf_log="${out}/deployed-frontline-portforward.log"
control_plane_pf=""
frontline_pf=""
driver="$1"
[[ "$(KUBECONFIG="${kubeconfig}" kubectl config current-context)" == kind-sleepypods-review-remediation ]]
if KUBECONFIG="${kubeconfig}" kubectl get namespace "${namespace}" >/dev/null 2>&1; then
  echo "Refusing to mutate existing diagnostic namespace ${namespace}" >&2; exit 2
fi
start_port_forward_loop() {
  local service="$1"
  local local_port="$2"
  local remote_port="$3"
  local log_file="$4"

  (
    child=""
    stop_loop() {
      if [[ -n "${child}" ]]; then
        kill "${child}" >/dev/null 2>&1 || true
        wait "${child}" 2>/dev/null || true
      fi
      exit 0
    }
    trap stop_loop TERM INT

    while true; do
      KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" port-forward \
        "svc/${service}" "${local_port}:${remote_port}" >>"${log_file}" 2>&1 &
      child=$!
      wait "${child}" 2>/dev/null || true
      child=""

      sleep 1 &
      child=$!
      wait "${child}" 2>/dev/null || true
      child=""
    done
  ) >/dev/null 2>&1 &
  port_forward_loop_pid="$!"
}

cleanup() {
  local status=$?
  for pid in "${control_plane_pf}" "${frontline_pf}"; do
    if [[ -n "${pid}" ]]; then kill "${pid}" >/dev/null 2>&1 || true; wait "${pid}" 2>/dev/null || true; fi
  done
  echo "Diagnostic namespace retained: ${namespace}; no Kubernetes cleanup was attempted."
  exit "${status}"
}
trap cleanup EXIT
KUBECONFIG="${kubeconfig}" kubectl create namespace "${namespace}"
KUBECONFIG="${kubeconfig}" kubectl label namespace "${namespace}" sleepypods.io/kind-e2e=reassignment-wire
echo "==> Deploying Postgres"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" create -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: restart
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-postgres
        sleepypods.io/kind-e2e: restart
    spec:
      containers:
        - name: postgres
          image: ${postgres_image}
          imagePullPolicy: IfNotPresent
          ports:
            - name: postgres
              containerPort: 5432
          env:
            - name: POSTGRES_USER
              value: sleepypods
            - name: POSTGRES_PASSWORD
              value: sleepypods
            - name: POSTGRES_DB
              value: sleepypods
          readinessProbe:
            exec:
              command:
                - pg_isready
                - -U
                - sleepypods
                - -d
                - sleepypods
            initialDelaySeconds: 2
            periodSeconds: 2
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: restart
spec:
  selector:
    app.kubernetes.io/name: sleepypods-postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
YAML
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-postgres --timeout=180s

echo "==> Deploying control-plane and frontline"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" create -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: restart
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: restart
rules:
  - apiGroups: [""]
    resources: ["services", "secrets"]
    verbs: ["get", "list", "watch", "patch", "create", "update", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get", "list", "watch", "patch", "create", "update", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: ["apps"]
    resources: ["replicasets"]
    verbs: ["get", "list"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: restart
subjects:
  - kind: ServiceAccount
    name: sleepypods-control-plane
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: sleepypods-control-plane
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: restart
spec:
  replicas: ${control_plane_replicas}
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-control-plane
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-control-plane
        sleepypods.io/kind-e2e: restart
    spec:
      serviceAccountName: sleepypods-control-plane
      containers:
        - name: control-plane
          image: ${image_prefix}/control-plane:${image_tag}
          imagePullPolicy: IfNotPresent
          ports:
            - name: grpc
              containerPort: 50051
          readinessProbe:
            tcpSocket:
              port: grpc
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR
              value: 0.0.0.0:50051
            - name: SLEEPYPODS_CONTROL_PLANE_AUTH_MODE
              value: no-auth
            - name: SLEEPYPODS_STORE_PROVIDER
              value: postgres
            - name: SLEEPYPODS_POSTGRES_URL
              value: postgres://sleepypods:sleepypods@sleepypods-postgres:5432/sleepypods
            - name: SLEEPYPODS_CLUSTER_ID
              value: kind-e2e-restart
            - name: SLEEPYPODS_NAMESPACE
              value: ${namespace}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: restart
spec:
  selector:
    app.kubernetes.io/name: sleepypods-control-plane
  ports:
    - name: grpc
      port: 50051
      targetPort: 50051
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: restart
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-frontline
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-frontline
        sleepypods.io/kind-e2e: restart
    spec:
      containers:
        - name: frontline
          image: ${image_prefix}/frontline:${image_tag}
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            tcpSocket:
              port: http
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_FRONTLINE_LISTEN_ADDR
              value: 0.0.0.0:8080
            - name: SLEEPYPODS_CONTROL_PLANE_ENDPOINT
              value: http://sleepypods-control-plane.${namespace}.svc.cluster.local:50051
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: restart
spec:
  selector:
    app.kubernetes.io/name: sleepypods-frontline
  ports:
    - name: http
      port: 8080
      targetPort: 8080
YAML

KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-control-plane --timeout=180s
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-frontline --timeout=180s

echo "==> Starting local port-forward loops"
start_port_forward_loop sleepypods-control-plane "${operator_port}" 50051 "${control_plane_pf_log}"
control_plane_pf="${port_forward_loop_pid}"
start_port_forward_loop sleepypods-frontline "${frontline_port}" 8080 "${frontline_pf_log}"
frontline_pf="${port_forward_loop_pid}"


set +e
echo "==> Running restart kind E2E driver"
KUBECONFIG="${kubeconfig}" \
  SLEEPYPODS_KIND_E2E_RESTART=1 \
  SLEEPYPODS_KIND_CLUSTER="${cluster_name}" \
  SLEEPYPODS_E2E_NAMESPACE="${namespace}" \
  SLEEPYPODS_E2E_OPERATOR_ENDPOINT="http://127.0.0.1:${operator_port}" \
  SLEEPYPODS_E2E_FRONTLINE_ADDR="127.0.0.1:${frontline_port}" \
  SLEEPYPODS_E2E_APP_IMAGE="${app_image}" \
  SLEEPYPODS_E2E_LATE_APP_IMAGE="${late_app_image}" \
  SLEEPYPODS_E2E_SIDECAR_IMAGE="${image_prefix}/sidecar:${image_tag}" \
  SLEEPYPODS_E2E_CONTROL_PLANE_REPLICAS="${control_plane_replicas}" \
  "${driver}" --ignored --nocapture control_plane_restart_recovery_through_deployed_platform

driver_status=$?
set -e
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" logs deployment/sleepypods-control-plane --timestamps >"${out}/deployed-control-plane.log" 2>&1 || true
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" logs deployment/sleepypods-frontline --timestamps >"${out}/deployed-frontline.log" 2>&1 || true
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" get pods -o wide >"${out}/deployed-pods.log" 2>&1 || true
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" get events -o json >"${out}/deployed-events.json" 2>&1 || true
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .status.containerStatuses[*]}{.name}{" "}{.imageID}{"\n"}{end}{end}' >"${out}/deployed-image-identities.log" 2>&1 || true
exit "${driver_status}"
