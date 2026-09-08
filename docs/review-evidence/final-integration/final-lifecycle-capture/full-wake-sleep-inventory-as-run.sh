#!/usr/bin/env bash

# Read-only scope shared by the soak and a supplemental post-gate inventory.
# A partial/failed discovery is never evidence of absence. Preserve kubectl's
# diagnostics and status, and stop before issuing another query after failure.
read_full_wake_sleep_inventory() {
  local inventory_kubeconfig="$1"

  KUBECONFIG="${inventory_kubeconfig}" kubectl get namespaces \
    -l "sleepypods.io/kind-e2e in (stateless,stateful)" \
    -o name || return "$?"
  KUBECONFIG="${inventory_kubeconfig}" kubectl get deployments.apps,statefulsets.apps,services,persistentvolumeclaims \
    --all-namespaces \
    -l "sleepypods.io/instance-id in (e2e-stateless,e2e-stateless-abandoned,e2e-stateful)" \
    -o name || return "$?"
  KUBECONFIG="${inventory_kubeconfig}" kubectl get persistentvolumes \
    -l "sleepypods.io/instance-id in (e2e-stateless,e2e-stateless-abandoned,e2e-stateful)" \
    -o name || return "$?"
  KUBECONFIG="${inventory_kubeconfig}" kubectl get clusterrole,clusterrolebinding \
    -l "sleepypods.io/kind-e2e=stateful" \
    -o name || return "$?"
}

wait_for_full_wake_sleep_no_leaks() {
  local inventory_kubeconfig="$1"
  local context="$2"
  local deadline=$((SECONDS + $3))
  local leaked status

  while true; do
    if leaked="$(read_full_wake_sleep_inventory "${inventory_kubeconfig}")"; then
      if [[ -z "${leaked}" ]]; then
        return 0
      fi
    else
      status=$?
      echo "full wake/sleep kind soak ${context} could not verify Kubernetes inventory (read failed):" >&2
      if [[ -n "${leaked}" ]]; then
        printf '%s\n' "${leaked}" >&2
      fi
      return "${status}"
    fi

    if ((SECONDS >= deadline)); then
      echo "full wake/sleep kind soak ${context} leaked Kubernetes objects:" >&2
      printf '%s\n' "${leaked}" >&2
      return 1
    fi

    sleep 2
  done
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ "$#" != "1" ]]; then
    echo "usage: /bin/bash ${BASH_SOURCE[0]} KUBECONFIG_PATH" >&2
    exit 2
  fi
  read_full_wake_sleep_inventory "$1"
fi
