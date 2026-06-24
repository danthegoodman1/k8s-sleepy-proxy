#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-e2e-tls-test}"
namespace="${SLEEPYPODS_KIND_E2E_NAMESPACE:-sleepypods-e2e-tls}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
keep_namespace="${SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE:-0}"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-kind-e2e-tls}"
app_image="${SLEEPYPODS_KIND_E2E_APP_IMAGE:-${image_prefix}/tls-app:${image_tag}}"
postgres_image="${SLEEPYPODS_KIND_E2E_POSTGRES_IMAGE:-postgres:17-alpine}"
operator_port="${SLEEPYPODS_KIND_E2E_OPERATOR_PORT:-19351}"
tls_termination_port="${SLEEPYPODS_KIND_E2E_TLS_TERMINATION_PORT:-19443}"
tls_passthrough_port="${SLEEPYPODS_KIND_E2E_TLS_PASSTHROUGH_PORT:-19444}"
kubeconfig="$(mktemp)"
control_plane_pf_log="$(mktemp)"
frontline_pf_log="$(mktemp)"
created_cluster=0
control_plane_pf=""
frontline_pf=""

require_command() {
  local name="$1"

  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "${name} is required for the TLS kind E2E" >&2
    exit 127
  fi
}

stop_port_forwards() {
  if [[ -n "${control_plane_pf}" ]]; then
    kill "${control_plane_pf}" >/dev/null 2>&1 || true
    wait "${control_plane_pf}" 2>/dev/null || true
    control_plane_pf=""
  fi
  if [[ -n "${frontline_pf}" ]]; then
    kill "${frontline_pf}" >/dev/null 2>&1 || true
    wait "${frontline_pf}" 2>/dev/null || true
    frontline_pf=""
  fi
}

cleanup() {
  local status=$?

  stop_port_forwards
  if [[ "${status}" != "0" ]]; then
    if [[ -s "${control_plane_pf_log}" ]]; then
      echo "==> Last control-plane port-forward log lines (${control_plane_pf_log})" >&2
      tail -n 80 "${control_plane_pf_log}" >&2 || true
    fi
    if [[ -s "${frontline_pf_log}" ]]; then
      echo "==> Last frontline port-forward log lines (${frontline_pf_log})" >&2
      tail -n 80 "${frontline_pf_log}" >&2 || true
    fi
  fi

  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}" || true
  elif [[ "${keep_namespace}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi

  rm -f "${kubeconfig}" "${control_plane_pf_log}" "${frontline_pf_log}"
  exit "${status}"
}
trap cleanup EXIT

for command in kind kubectl docker cargo; do
  require_command "${command}"
done

if ! kind get clusters | grep -Fxq "${cluster_name}"; then
  created_cluster=1
  KUBECONFIG="${kubeconfig}" kind create cluster --name "${cluster_name}" --wait 120s
else
  kind get kubeconfig --name "${cluster_name}" >"${kubeconfig}"
fi

components=(control-plane frontline sidecar)
for component in "${components[@]}"; do
  image="${image_prefix}/${component}:${image_tag}"
  echo "==> Building ${image}"
  docker build \
    --build-arg "BIN=${component}" \
    --tag "${image}" \
    --file "${repo_root}/Dockerfile" \
    "${repo_root}"
done

echo "==> Building ${app_image}"
docker build \
  --tag "${app_image}" \
  --file "${repo_root}/scripts/Dockerfile.kind-tls-app" \
  "${repo_root}"

echo "==> Pulling ${postgres_image}"
docker pull "${postgres_image}"

for image in \
  "${image_prefix}/control-plane:${image_tag}" \
  "${image_prefix}/frontline:${image_tag}" \
  "${image_prefix}/sidecar:${image_tag}" \
  "${app_image}" \
  "${postgres_image}"; do
  echo "==> Loading ${image} into kind/${cluster_name}"
  kind load docker-image "${image}" --name "${cluster_name}"
done

echo "==> Recreating namespace ${namespace}"
KUBECONFIG="${kubeconfig}" kubectl delete namespace "${namespace}" --ignore-not-found --wait=true
KUBECONFIG="${kubeconfig}" kubectl create namespace "${namespace}"
KUBECONFIG="${kubeconfig}" kubectl label namespace "${namespace}" \
  sleepypods.io/kind-e2e=tls --overwrite

echo "==> Creating deterministic TLS certificate secret"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" create secret generic sleepypods-kind-e2e-tls \
  --from-file=tls.crt="${repo_root}/scripts/kind-e2e-tls-cert.pem" \
  --from-file=tls.key="${repo_root}/scripts/kind-e2e-tls-key.pem" \
  --dry-run=client \
  -o yaml | KUBECONFIG="${kubeconfig}" kubectl apply -f -

echo "==> Deploying Postgres"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-postgres
  labels:
    app.kubernetes.io/name: sleepypods-postgres
    sleepypods.io/kind-e2e: tls
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-postgres
        sleepypods.io/kind-e2e: tls
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
    sleepypods.io/kind-e2e: tls
spec:
  selector:
    app.kubernetes.io/name: sleepypods-postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
YAML
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-postgres --timeout=180s

echo "==> Deploying frontline"
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: tls
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-frontline
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-frontline
        sleepypods.io/kind-e2e: tls
    spec:
      containers:
        - name: frontline
          image: ${image_prefix}/frontline:${image_tag}
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8080
            - name: tls-term
              containerPort: 8443
            - name: tls-pass
              containerPort: 9443
          readinessProbe:
            tcpSocket:
              port: http
            periodSeconds: 1
            failureThreshold: 60
          env:
            - name: SLEEPYPODS_FRONTLINE_LISTEN_ADDR
              value: 0.0.0.0:8080
            - name: SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR
              value: 0.0.0.0:8443
            - name: SLEEPYPODS_FRONTLINE_TLS_TERMINATION_CERTS
              value: terminate.sleepypods.test|/etc/sleepypods/tls/tls.crt|/etc/sleepypods/tls/tls.key
            - name: SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR
              value: 0.0.0.0:9443
            - name: SLEEPYPODS_CONTROL_PLANE_ENDPOINT
              value: http://sleepypods-control-plane.${namespace}.svc.cluster.local:50051
          volumeMounts:
            - name: tls
              mountPath: /etc/sleepypods/tls
              readOnly: true
      volumes:
        - name: tls
          secret:
            secretName: sleepypods-kind-e2e-tls
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-frontline
  labels:
    app.kubernetes.io/name: sleepypods-frontline
    sleepypods.io/kind-e2e: tls
spec:
  selector:
    app.kubernetes.io/name: sleepypods-frontline
  ports:
    - name: http
      port: 8080
      targetPort: 8080
    - name: tls-term
      port: 8443
      targetPort: 8443
    - name: tls-pass
      port: 9443
      targetPort: 9443
YAML

deploy_control_plane() {
  echo "==> Deploying control-plane"
  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: tls
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: tls
rules:
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list", "watch", "patch", "create", "update", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get", "list", "watch", "patch", "create", "update", "delete"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: sleepypods-control-plane
  labels:
    sleepypods.io/kind-e2e: tls
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
    sleepypods.io/kind-e2e: tls
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: sleepypods-control-plane
  template:
    metadata:
      labels:
        app.kubernetes.io/name: sleepypods-control-plane
        sleepypods.io/kind-e2e: tls
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
            - name: SLEEPYPODS_STORE_PROVIDER
              value: postgres
            - name: SLEEPYPODS_POSTGRES_URL
              value: postgres://sleepypods:sleepypods@sleepypods-postgres:5432/sleepypods
            - name: SLEEPYPODS_CLUSTER_ID
              value: kind-e2e-tls
            - name: SLEEPYPODS_NAMESPACE
              value: ${namespace}
---
apiVersion: v1
kind: Service
metadata:
  name: sleepypods-control-plane
  labels:
    app.kubernetes.io/name: sleepypods-control-plane
    sleepypods.io/kind-e2e: tls
spec:
  selector:
    app.kubernetes.io/name: sleepypods-control-plane
  ports:
    - name: grpc
      port: 50051
      targetPort: 50051
YAML

  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-control-plane --timeout=180s
}

start_port_forwards() {
  stop_port_forwards

  echo "==> Starting local port-forwards"
  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" port-forward \
    svc/sleepypods-control-plane "${operator_port}:50051" >"${control_plane_pf_log}" 2>&1 &
  control_plane_pf=$!
  KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" port-forward \
    svc/sleepypods-frontline "${tls_termination_port}:8443" "${tls_passthrough_port}:9443" >"${frontline_pf_log}" 2>&1 &
  frontline_pf=$!
}

run_tls_driver() {
  local test_name="$1"

  echo "==> Running TLS kind E2E driver ${test_name}"
  KUBECONFIG="${kubeconfig}" \
    SLEEPYPODS_KIND_E2E_TLS=1 \
    SLEEPYPODS_E2E_NAMESPACE="${namespace}" \
    SLEEPYPODS_E2E_OPERATOR_ENDPOINT="http://127.0.0.1:${operator_port}" \
    SLEEPYPODS_E2E_TLS_TERMINATION_ADDR="127.0.0.1:${tls_termination_port}" \
    SLEEPYPODS_E2E_TLS_PASSTHROUGH_ADDR="127.0.0.1:${tls_passthrough_port}" \
    SLEEPYPODS_E2E_APP_IMAGE="${app_image}" \
    SLEEPYPODS_E2E_SIDECAR_IMAGE="${image_prefix}/sidecar:${image_tag}" \
    cargo test -p control-plane --test kind_e2e_tls "${test_name}" -- --ignored --nocapture
}

deploy_control_plane
KUBECONFIG="${kubeconfig}" kubectl -n "${namespace}" rollout status deployment/sleepypods-frontline --timeout=180s
start_port_forwards
run_tls_driver tls_termination_through_deployed_platform
run_tls_driver sni_passthrough_through_deployed_platform

echo "TLS full-platform kind E2E completed"
