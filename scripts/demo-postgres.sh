#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

load_env
require_kubeconfig

TENANT_ID="${1:-tenant-pg}"
IP="$(tcp_lb_ip)"
PGPASSWORD="$(postgres_password)"
HOT_RUNS="${HOT_RUNS:-3}"
if [[ -z "$IP" ]]; then
  echo "sleepy-tcp-lb does not have an external IP yet" >&2
  exit 1
fi

pgincr_bin() {
  local bin="$GENERATED_DIR/bin/pgincr"
  if [[ ! -x "$bin" || "$ROOT_DIR/cmd/pgincr/main.go" -nt "$bin" ]]; then
    ensure_generated
    mkdir -p "$GENERATED_DIR/bin" "$GENERATED_DIR/go-build"
    GOCACHE="$GENERATED_DIR/go-build" go build -o "$bin" "$ROOT_DIR/cmd/pgincr"
  fi
  echo "$bin"
}

increment_postgres() {
  "$(pgincr_bin)" \
    -host "$IP" \
    -port 5432 \
    -user postgres \
    -password "$PGPASSWORD" \
    -database "$TENANT_ID"
}

field_from_result() {
  local key="$1"
  local result="$2"
  printf '%s\n' "$result" | tr ' ' '\n' | sed -n "s/^$key=//p" | tail -1
}

echo "Seeding $TENANT_ID and ensuring its Archil PVC exists..."
"$SCRIPT_DIR/seed-postgres-tenant.sh" "$TENANT_ID" >/dev/null
kube -n sleepy-system get pvc -l "sleepy.dev/tenant-id=$TENANT_ID"

echo
echo "Cold postgres increment:"
cold_result="$(increment_postgres)"
cold_count="$(field_from_result count "$cold_result")"
echo "$cold_result"

echo
echo "Hot postgres increments:"
hot_count="$cold_count"
for _ in $(seq 1 "$HOT_RUNS"); do
  hot_result="$(increment_postgres)"
  hot_count="$(field_from_result count "$hot_result")"
  echo "$hot_result"
done

if [[ "$hot_count" != "$((cold_count + HOT_RUNS))" ]]; then
  echo "hot count $hot_count did not follow cold count $cold_count after $HOT_RUNS hot runs" >&2
  exit 1
fi

echo
echo "Waiting for idle sleep..."
sleep "${IDLE_WAIT_SECONDS:-40}"
kube -n sleepy-system get statefulset,svc,pod -l "sleepy.dev/tenant-id=$TENANT_ID" || true
kube -n sleepy-system get pvc -l "sleepy.dev/tenant-id=$TENANT_ID"

echo
echo "Cold postgres increment after sleep:"
wake_result="$(increment_postgres)"
wake_count="$(field_from_result count "$wake_result")"
echo "$wake_result"

if [[ "$wake_count" != "$((hot_count + 1))" ]]; then
  echo "wake count $wake_count did not continue from hot count $hot_count" >&2
  exit 1
fi

echo
echo "Postgres persistence demo passed: $cold_count -> $hot_count -> $wake_count"
