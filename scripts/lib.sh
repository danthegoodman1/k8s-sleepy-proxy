#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GENERATED_DIR="$ROOT_DIR/.generated"
TF_DIR="$ROOT_DIR/infra/terraform"
KUBECONFIG_FILE="$GENERATED_DIR/kubeconfig"
AUTH_TOKEN_FILE="$GENERATED_DIR/auth-token"
POSTGRES_PASSWORD_FILE="$GENERATED_DIR/postgres-password"
HELM_VERSION="${HELM_VERSION:-v3.15.4}"

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

helm_bin() {
  if command -v helm >/dev/null 2>&1; then
    command -v helm
    return
  fi

  local bin="$GENERATED_DIR/bin/helm"
  if [[ -x "$bin" ]]; then
    echo "$bin"
    return
  fi

  ensure_generated
  mkdir -p "$GENERATED_DIR/bin" "$GENERATED_DIR/helm"
  local os arch platform archive url
  case "$(uname -s)" in
    Darwin) os="darwin" ;;
    Linux) os="linux" ;;
    *) echo "unsupported OS for automatic Helm install: $(uname -s)" >&2; exit 1 ;;
  esac
  case "$(uname -m)" in
    arm64|aarch64) arch="arm64" ;;
    x86_64|amd64) arch="amd64" ;;
    *) echo "unsupported arch for automatic Helm install: $(uname -m)" >&2; exit 1 ;;
  esac
  platform="$os-$arch"
  archive="$GENERATED_DIR/helm/helm-$HELM_VERSION-$platform.tar.gz"
  url="https://get.helm.sh/helm-$HELM_VERSION-$platform.tar.gz"
  echo "Downloading Helm $HELM_VERSION for $platform..." >&2
  curl -fsSL "$url" -o "$archive"
  tar -xzf "$archive" -C "$GENERATED_DIR/helm"
  cp "$GENERATED_DIR/helm/$platform/helm" "$bin"
  chmod +x "$bin"
  echo "$bin"
}

helm_cmd() {
  HELM_CACHE_HOME="$GENERATED_DIR/helm/cache" \
  HELM_CONFIG_HOME="$GENERATED_DIR/helm/config" \
  HELM_DATA_HOME="$GENERATED_DIR/helm/data" \
    "$(helm_bin)" "$@"
}

helm_kube() {
  helm_cmd --kubeconfig "$KUBECONFIG_FILE" "$@"
}

archil_controlplane_url() {
  case "$1" in
    aws-us-east-1) echo "https://control.green.us-east-1.aws.prod.archil.com" ;;
    aws-us-west-2) echo "https://control.green.us-west-2.aws.prod.archil.com" ;;
    aws-eu-west-1) echo "https://control.green.eu-west-1.aws.prod.archil.com" ;;
    gcp-us-central1) echo "https://control.blue.us-central1.gcp.prod.archil.com" ;;
    *) echo "unsupported ARCHIL_REGION: $1" >&2; exit 1 ;;
  esac
}

ensure_archil_csi() {
  if [[ -z "${ARCHIL_API_KEY:-}" ]]; then
    echo "ARCHIL_API_KEY is required in .env.local for the Archil CSI driver" >&2
    exit 1
  fi
  local region="${ARCHIL_REGION:-aws-us-west-2}"
  local api_key="$ARCHIL_API_KEY"
  if [[ "$api_key" != key-* ]]; then
    api_key="key-$api_key"
  fi
  local csi_api_key="${ARCHIL_API_KEY#key-}"
  local controlplane_url="${ARCHIL_CONTROLPLANE_URL:-$(archil_controlplane_url "$region")}"
  local status
  status="$(curl -sS -o /dev/null -w "%{http_code}" -H "Authorization: $api_key" "$controlplane_url/api/disks?limit=1" || true)"
  if [[ "$status" != "200" ]]; then
    echo "ARCHIL_API_KEY was not accepted by $region ($controlplane_url returned HTTP $status)" >&2
    exit 1
  fi
  kube create namespace archil-system --dry-run=client -o yaml | kube apply -f -
  kube -n archil-system create secret generic archil-controlplane-api-key \
    --from-literal=api-key="$csi_api_key" \
    --dry-run=client -o yaml | kube apply -f -
  helm_kube upgrade --install archil-csi-driver \
    oci://registry-1.docker.io/archildata/csi-driver-chart \
    --namespace archil-system \
    --create-namespace \
    --set controller.enabled=true \
    --set controller.region="$region" \
    --set-string controller.controlplaneURL="$controlplane_url" \
    --set storageClass.enabled=true \
    --set storageClass.name=archil \
    --set storageClass.region="$region" \
    --set storageClass.nodeAuthType=token \
    --set storageClass.volumeBindingMode=Immediate \
    --wait \
    --timeout 5m
  kube -n archil-system rollout restart deployment/archil-csi-controller
  kube -n archil-system rollout status deployment/archil-csi-controller --timeout=180s
  kube -n archil-system rollout status daemonset/archil-csi-node --timeout=180s
}

uninstall_archil_csi() {
  if [[ ! -f "$KUBECONFIG_FILE" ]]; then
    return
  fi
  helm_kube uninstall archil-csi-driver --namespace archil-system >/dev/null 2>&1 || true
  kube delete namespace archil-system --ignore-not-found=true || true
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

postgres_password() {
  ensure_generated
  if [[ ! -f "$POSTGRES_PASSWORD_FILE" ]]; then
    openssl rand -hex 18 > "$POSTGRES_PASSWORD_FILE"
    chmod 600 "$POSTGRES_PASSWORD_FILE"
  fi
  tr -d '\n' < "$POSTGRES_PASSWORD_FILE"
}

render_manifest() {
  local registry="$1"
  local repo="$registry/sleepy-controller"
  local postgres_image="${POSTGRES_IMAGE:-postgres:18}"
  sed \
    -e "s|__CONTROLLER_IMAGE__|$repo:controller|g" \
    -e "s|__LB_IMAGE__|$repo:lb|g" \
    -e "s|__TCP_LB_IMAGE__|$repo:tcplb|g" \
    -e "s|__SIDECAR_IMAGE__|$repo:sidecar|g" \
    -e "s|__TCP_SIDECAR_IMAGE__|$repo:tcpsidecar|g" \
    -e "s|__ECHO_IMAGE__|$repo:echo|g" \
    -e "s|__POSTGRES_IMAGE__|$postgres_image|g" \
    "$ROOT_DIR/deploy/k8s.yaml.tpl" > "$GENERATED_DIR/k8s.yaml"
}

lb_ip() {
  kube -n sleepy-system get svc sleepy-lb -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
}

tcp_lb_ip() {
  kube -n sleepy-system get svc sleepy-tcp-lb -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
}
