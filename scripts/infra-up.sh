#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

load_env
ensure_generated

tf init
tf apply
tf output -raw kubeconfig > "$KUBECONFIG_FILE"
chmod 600 "$KUBECONFIG_FILE"

echo "Wrote kubeconfig to $KUBECONFIG_FILE"
echo "Registry: $(registry_endpoint)"

