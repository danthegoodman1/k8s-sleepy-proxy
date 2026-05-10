#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GENERATED_DIR="$ROOT_DIR/.generated"
TF_DIR="$ROOT_DIR/infra/terraform"
KUBECONFIG_FILE="$GENERATED_DIR/kubeconfig"
AUTH_TOKEN_FILE="$GENERATED_DIR/auth-token"

load_env() {
  if [[ ! -f "$ROOT_DIR/.env.local" ]]; then
    echo ".env.local is required" >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env.local"
  set +a
  if [[ -z "${DO_API_KEY:-}" ]]; then
    echo "DO_API_KEY is required in .env.local" >&2
    exit 1
  fi
  export DIGITALOCEAN_TOKEN="$DO_API_KEY"
  export TF_VAR_do_token="$DO_API_KEY"
  if [[ -n "${DO_REGISTRY_NAME:-}" ]]; then
    export TF_VAR_registry_name="$DO_REGISTRY_NAME"
  elif command -v curl >/dev/null 2>&1; then
    local registry_name
    registry_name="$(curl -fsS -H "Authorization: Bearer $DO_API_KEY" https://api.digitalocean.com/v2/registry 2>/dev/null | sed -n 's/^{"registry":{"name":"\([^"]*\)".*/\1/p' || true)"
    if [[ -n "$registry_name" ]]; then
      export TF_VAR_registry_name="$registry_name"
    fi
  fi
}

tf() {
  terraform -chdir="$TF_DIR" "$@"
}

kube() {
  kubectl --kubeconfig "$KUBECONFIG_FILE" "$@"
}

ensure_generated() {
  mkdir -p "$GENERATED_DIR"
}

require_kubeconfig() {
  if [[ ! -f "$KUBECONFIG_FILE" ]]; then
    echo "$KUBECONFIG_FILE does not exist. Run scripts/infra-up.sh first." >&2
    exit 1
  fi
}

registry_endpoint() {
  tf output -raw registry_endpoint
}

postgres_private_uri() {
  tf output -raw postgres_private_uri
}

auth_token() {
  ensure_generated
  if [[ ! -f "$AUTH_TOKEN_FILE" ]]; then
    openssl rand -hex 24 > "$AUTH_TOKEN_FILE"
    chmod 600 "$AUTH_TOKEN_FILE"
  fi
  tr -d '\n' < "$AUTH_TOKEN_FILE"
}

render_manifest() {
  local registry="$1"
  local repo="$registry/sleepy-controller"
  sed \
    -e "s|__CONTROLLER_IMAGE__|$repo:controller|g" \
    -e "s|__LB_IMAGE__|$repo:lb|g" \
    -e "s|__SIDECAR_IMAGE__|$repo:sidecar|g" \
    -e "s|__ECHO_IMAGE__|$repo:echo|g" \
    "$ROOT_DIR/deploy/k8s.yaml.tpl" > "$GENERATED_DIR/k8s.yaml"
}

lb_ip() {
  kube -n sleepy-system get svc sleepy-lb -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
}
