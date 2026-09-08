#!/usr/bin/env bash
set -euo pipefail

cluster_name="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-materializer-test}"
keep_cluster="${SLEEPYPODS_KIND_KEEP_CLUSTER:-0}"
kubeconfig="$(mktemp)"

if ! kind get clusters | grep -Fxq "${cluster_name}"; then
  KUBECONFIG="${kubeconfig}" kind create cluster --name "${cluster_name}" --wait 120s
  created_cluster=1
else
  created_cluster=0
  kind get kubeconfig --name "${cluster_name}" >"${kubeconfig}"
fi

cleanup() {
  if [[ "${created_cluster}" == "1" && "${keep_cluster}" != "1" ]]; then
    KUBECONFIG="${kubeconfig}" kind delete cluster --name "${cluster_name}"
  fi
  rm -f "${kubeconfig}"
}
trap cleanup EXIT

KUBECONFIG="${kubeconfig}" SLEEPYPODS_KIND_TEST=1 cargo test -p control-plane --test kind_materializer -- --ignored --nocapture
