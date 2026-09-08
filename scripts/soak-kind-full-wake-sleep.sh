#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-full-wake-sleep-soak}"
stateless_namespace="${SLEEPYPODS_KIND_E2E_STATELESS_NAMESPACE:-sleepypods-soak-stateless}"
stateful_namespace="${SLEEPYPODS_KIND_E2E_STATEFUL_NAMESPACE:-sleepypods-soak-stateful}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
iterations="${SLEEPYPODS_KIND_SOAK_ITERATIONS:-2}"
leak_check_timeout="${SLEEPYPODS_KIND_LEAK_CHECK_TIMEOUT:-90}"
include_stateful="${SLEEPYPODS_KIND_FULL_SOAK_INCLUDE_STATEFUL:-1}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-kind-full-wake-sleep-soak}"
kubeconfig="$(mktemp)"
created_cluster=0

require_command() {
  local name="$1"

  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "${name} is required for the full wake/sleep kind soak" >&2
    exit 127
  fi
}

cleanup_managed_objects() {
  KUBECONFIG="${kubeconfig}" kubectl delete namespace \
    "${stateless_namespace}" "${stateful_namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  KUBECONFIG="${kubeconfig}" kubectl delete persistentvolume \
    -l "sleepypods.io/instance-id in (e2e-stateless,e2e-stateless-abandoned,e2e-stateful)" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  KUBECONFIG="${kubeconfig}" kubectl delete clusterrole,clusterrolebinding \
    "sleepypods-control-plane-${stateful_namespace}" --ignore-not-found --wait=true >/dev/null 2>&1 || true
}

cleanup_temp_logs() {
  rm -f \
    /tmp/sleepypods-control-plane-port-forward.log \
    /tmp/sleepypods-frontline-port-forward.log \
    /tmp/sleepypods-control-plane-stateful-port-forward.log \
    /tmp/sleepypods-frontline-stateful-port-forward.log
}

cleanup() {
  local status=$?

  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}" || true
  else
    cleanup_managed_objects
  fi
  cleanup_temp_logs

  rm -f "${kubeconfig}"
  exit "${status}"
}
trap cleanup EXIT

for command in kind kubectl docker cargo; do
  require_command "${command}"
done

if [[ ! "${iterations}" =~ ^[1-9][0-9]*$ ]]; then
  echo "SLEEPYPODS_KIND_SOAK_ITERATIONS must be a positive integer; got ${iterations}" >&2
  exit 2
fi

if [[ ! "${leak_check_timeout}" =~ ^[1-9][0-9]*$ ]]; then
  echo "SLEEPYPODS_KIND_LEAK_CHECK_TIMEOUT must be a positive integer; got ${leak_check_timeout}" >&2
  exit 2
fi

if [[ "${include_stateful}" != "0" && "${include_stateful}" != "1" ]]; then
  echo "SLEEPYPODS_KIND_FULL_SOAK_INCLUDE_STATEFUL must be 0 or 1; got ${include_stateful}" >&2
  exit 2
fi

if ! kind get clusters | grep -Fxq "${cluster_name}"; then
  created_cluster=1
  KUBECONFIG="${kubeconfig}" kind create cluster --name "${cluster_name}" --wait 120s
else
  kind get kubeconfig --name "${cluster_name}" >"${kubeconfig}"
fi

source "${repo_root}/scripts/lib/full-wake-sleep-inventory.sh"

wait_for_no_leaks() {
  wait_for_full_wake_sleep_no_leaks "${kubeconfig}" "$1" "${leak_check_timeout}"
}

run_stateless_cycle() {
  local iteration="$1"

  echo "==> full wake/sleep soak stateless iteration ${iteration}/${iterations}"
  SLEEPYPODS_KIND_CLUSTER="${cluster_name}" \
    SLEEPYPODS_KIND_KEEP_CLUSTER=1 \
    SLEEPYPODS_KIND_E2E_NAMESPACE="${stateless_namespace}" \
    SLEEPYPODS_IMAGE_TAG="${image_tag}" \
    "${repo_root}/scripts/test-kind-e2e-stateless.sh"
  wait_for_no_leaks "stateless iteration ${iteration}/${iterations}"
}

run_stateful_cycle() {
  local iteration="$1"

  echo "==> full wake/sleep soak stateful iteration ${iteration}/${iterations}"
  SLEEPYPODS_KIND_CLUSTER="${cluster_name}" \
    SLEEPYPODS_KIND_KEEP_CLUSTER=1 \
    SLEEPYPODS_KIND_E2E_NAMESPACE="${stateful_namespace}" \
    SLEEPYPODS_IMAGE_TAG="${image_tag}" \
    "${repo_root}/scripts/test-kind-e2e-stateful.sh"
  wait_for_no_leaks "stateful iteration ${iteration}/${iterations}"
}

for ((iteration = 1; iteration <= iterations; iteration++)); do
  run_stateless_cycle "${iteration}"
  if [[ "${include_stateful}" == "1" ]]; then
    run_stateful_cycle "${iteration}"
  fi
done

if [[ "${include_stateful}" == "1" ]]; then
  echo "full wake/sleep kind soak completed ${iterations} stateless and stateful iteration(s) without leaked test objects"
else
  echo "full wake/sleep kind soak completed ${iterations} stateless iteration(s) without leaked test objects"
fi
