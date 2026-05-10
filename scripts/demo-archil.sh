#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

load_env
require_kubeconfig

TENANT_ID="${1:-tenant-a}"
IP="$(lb_ip)"
if [[ -z "$IP" ]]; then
  echo "sleepy-lb does not have an external IP yet" >&2
  exit 1
fi

count_from_json() {
  sed -n 's/.*"count":\([0-9][0-9]*\).*/\1/p'
}

echo "Seeding $TENANT_ID and ensuring its Archil PVC exists..."
"$SCRIPT_DIR/seed-tenant.sh" "$TENANT_ID" >/dev/null
kube -n sleepy-system get pvc -l "sleepy.dev/tenant-id=$TENANT_ID"

echo
echo "Cold /incr:"
cold_body="$(curl -fsS -w '\ntime_total=%{time_total}\n' -H "TENANT: $TENANT_ID" "http://$IP/incr")"
echo "$cold_body"
cold_count="$(printf '%s' "$cold_body" | count_from_json)"

echo
echo "Hot /incr:"
hot_body="$(curl -fsS -w '\ntime_total=%{time_total}\n' -H "TENANT: $TENANT_ID" "http://$IP/incr")"
echo "$hot_body"
hot_count="$(printf '%s' "$hot_body" | count_from_json)"

if [[ "$hot_count" != "$((cold_count + 1))" ]]; then
  echo "hot count $hot_count did not follow cold count $cold_count" >&2
  exit 1
fi

echo
echo "Waiting for idle sleep..."
sleep "${IDLE_WAIT_SECONDS:-40}"
kube -n sleepy-system get statefulset,svc,pod -l "sleepy.dev/tenant-id=$TENANT_ID" || true
kube -n sleepy-system get pvc -l "sleepy.dev/tenant-id=$TENANT_ID"

echo
echo "Cold /incr after sleep:"
wake_body="$(curl -fsS -w '\ntime_total=%{time_total}\n' -H "TENANT: $TENANT_ID" "http://$IP/incr")"
echo "$wake_body"
wake_count="$(printf '%s' "$wake_body" | count_from_json)"

if [[ "$wake_count" != "$((hot_count + 1))" ]]; then
  echo "wake count $wake_count did not continue from hot count $hot_count" >&2
  exit 1
fi

echo
echo "Archil persistence demo passed: $cold_count -> $hot_count -> $wake_count"
