#!/usr/bin/env bash
set -euo pipefail

cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-materializer-test}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
iterations="${SLEEPYPODS_KIND_SOAK_ITERATIONS:-3}"
leak_check_timeout="${SLEEPYPODS_KIND_LEAK_CHECK_TIMEOUT:-90}"
kubeconfig="$(mktemp)"
created_cluster=0

cleanup() {
  local status=$?
  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}" || true
  fi
  rm -f "${kubeconfig}"
  exit "${status}"
}
trap cleanup EXIT

if [[ ! "${iterations}" =~ ^[1-9][0-9]*$ ]]; then
  echo "SLEEPYPODS_KIND_SOAK_ITERATIONS must be a positive integer; got ${iterations}" >&2
  exit 2
fi

if [[ ! "${leak_check_timeout}" =~ ^[1-9][0-9]*$ ]]; then
  echo "SLEEPYPODS_KIND_LEAK_CHECK_TIMEOUT must be a positive integer; got ${leak_check_timeout}" >&2
  exit 2
fi

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required for the kind materializer soak" >&2
  exit 127
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required for kind materializer leaked-object checks" >&2
  exit 127
fi

existing_clusters="$(kind get clusters)"
if ! grep -Fxq "${cluster_name}" <<<"${existing_clusters}"; then
  created_cluster=1
  KUBECONFIG="${kubeconfig}" kind create cluster --name "${cluster_name}" --wait 120s
else
  kind get kubeconfig --name "${cluster_name}" >"${kubeconfig}"
fi

leaked_objects() {
  # The Rust test labels namespaces directly and rendered PVs inherit the
  # materializer ownership labels from the manifest renderer.
  KUBECONFIG="${kubeconfig}" kubectl get namespaces \
    -l "sleepypods.io/kind-test=true" \
    -o name
  KUBECONFIG="${kubeconfig}" kubectl get persistentvolumes \
    -l "sleepypods.io/instance-id=kind-materializer" \
    -o name
}

wait_for_no_leaks() {
  local iteration="$1"
  local deadline=$((SECONDS + leak_check_timeout))
  local leaked

  while true; do
    leaked="$(leaked_objects)"
    if [[ -z "${leaked}" ]]; then
      return 0
    fi

    if ((SECONDS >= deadline)); then
      echo "kind materializer soak iteration ${iteration}/${iterations} leaked Kubernetes objects:" >&2
      printf '%s\n' "${leaked}" >&2
      return 1
    fi

    sleep 2
  done
}

for ((iteration = 1; iteration <= iterations; iteration++)); do
  echo "==> kind materializer soak iteration ${iteration}/${iterations}"
  if KUBECONFIG="${kubeconfig}" SLEEPYPODS_KIND_TEST=1 cargo test -p control-plane --test kind_materializer -- --ignored --nocapture; then
    wait_for_no_leaks "${iteration}"
  else
    status=$?
    echo "kind materializer soak iteration ${iteration}/${iterations} failed" >&2
    exit "${status}"
  fi
done

echo "kind materializer soak completed ${iterations} iteration(s) without leaked test namespaces or PVs"
