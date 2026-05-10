#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

load_env
ensure_generated
require_kubeconfig

REGISTRY="$(registry_endpoint)"
DB_URL="$(postgres_private_uri)"
TOKEN="$(auth_token)"

render_manifest "$REGISTRY"

kube apply -f "$ROOT_DIR/.generated/k8s.yaml"
kube -n sleepy-system create secret generic sleepy-secrets \
  --from-literal=database-url="$DB_URL" \
  --from-literal=auth-token="$TOKEN" \
  --dry-run=client -o yaml | kube apply -f -
kube apply -f "$ROOT_DIR/.generated/k8s.yaml"

kube -n sleepy-system rollout status deployment/sleepy-controller --timeout=180s
kube -n sleepy-system rollout status deployment/sleepy-lb --timeout=180s

echo "Waiting for DigitalOcean load balancer IP..."
for _ in $(seq 1 60); do
  IP="$(lb_ip || true)"
  if [[ -n "$IP" ]]; then
    echo "LB URL: http://$IP"
    exit 0
  fi
  sleep 5
done

echo "Timed out waiting for LB IP; check with: kubectl --kubeconfig $KUBECONFIG_FILE -n sleepy-system get svc sleepy-lb" >&2
exit 1

