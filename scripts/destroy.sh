#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

load_env

if [[ -f "$KUBECONFIG_FILE" && -f "$GENERATED_DIR/k8s.yaml" ]]; then
  kube delete -f "$GENERATED_DIR/k8s.yaml" --ignore-not-found=true || true
  kube delete namespace sleepy-system --ignore-not-found=true || true
fi

tf destroy
