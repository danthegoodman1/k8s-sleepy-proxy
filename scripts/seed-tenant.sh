#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

load_env
require_kubeconfig

REGISTRY="$(registry_endpoint)"
TOKEN="$(auth_token)"
TENANT_ID="${1:-tenant-a}"
IDLE_SECONDS="${IDLE_SECONDS:-30}"

kube -n sleepy-system port-forward svc/sleepy-controller 18080:8080 >/tmp/sleepy-controller-port-forward.log 2>&1 &
PF_PID="$!"
trap 'kill "$PF_PID" >/dev/null 2>&1 || true' EXIT
sleep 2

curl -fsS \
  -X PUT "http://127.0.0.1:18080/tenants/$TENANT_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"image\":\"$REGISTRY/sleepy-controller:echo\",\"upstreamPort\":9000,\"publicHost\":\"$TENANT_ID\",\"idleSeconds\":$IDLE_SECONDS}"
echo
