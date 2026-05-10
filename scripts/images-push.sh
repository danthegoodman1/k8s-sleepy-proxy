#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

load_env
ensure_generated

REGISTRY="$(registry_endpoint)"
DOCKER_CONFIG_DIR="$GENERATED_DIR/docker"
mkdir -p "$DOCKER_CONFIG_DIR"
tf output -raw registry_docker_credentials > "$DOCKER_CONFIG_DIR/config.json"
chmod 600 "$DOCKER_CONFIG_DIR/config.json"
if [[ -d "$HOME/.docker/cli-plugins" && ! -e "$DOCKER_CONFIG_DIR/cli-plugins" ]]; then
  ln -s "$HOME/.docker/cli-plugins" "$DOCKER_CONFIG_DIR/cli-plugins"
fi
export DOCKER_CONFIG="$DOCKER_CONFIG_DIR"

for target in controller lb sidecar echo; do
  image="$REGISTRY/sleepy-controller:$target"
  echo "Building and pushing $image"
  docker buildx build \
    --platform linux/amd64 \
    --file "$ROOT_DIR/build/Dockerfile" \
    --target "$target" \
    --tag "$image" \
    --push \
    "$ROOT_DIR"
done
