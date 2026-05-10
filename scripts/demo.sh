#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

load_env
require_kubeconfig

TENANT_ID="${1:-tenant-a}"
TOKEN="$(auth_token)"
IP="$(lb_ip)"
if [[ -z "$IP" ]]; then
  echo "sleepy-lb does not have an external IP yet" >&2
  exit 1
fi

echo "Initial tenant workloads:"
kube -n sleepy-system get statefulset,svc -l "sleepy.dev/tenant-id=$TENANT_ID" || true

echo
echo "First request wakes $TENANT_ID:"
curl -fsS -H "TENANT: $TENANT_ID" "http://$IP/hello"
echo

echo
echo "Workloads after wake:"
kube -n sleepy-system get statefulset,svc -l "sleepy.dev/tenant-id=$TENANT_ID"

echo
echo "Waiting for idle sleep..."
sleep "${IDLE_WAIT_SECONDS:-40}"

echo
echo "State after idle wait:"
kube -n sleepy-system port-forward svc/sleepy-controller 18080:8080 >/tmp/sleepy-controller-port-forward.log 2>&1 &
PF_PID="$!"
trap 'kill "$PF_PID" >/dev/null 2>&1 || true' EXIT
sleep 2
curl -fsS -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:18080/state/$TENANT_ID"
echo

echo
echo "Workloads after sleep:"
kube -n sleepy-system get statefulset,svc -l "sleepy.dev/tenant-id=$TENANT_ID" || true

echo
echo "Second request wakes $TENANT_ID again:"
curl -fsS -H "TENANT: $TENANT_ID" "http://$IP/hello-again"
echo

