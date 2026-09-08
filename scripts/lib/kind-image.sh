#!/usr/bin/env bash

# Loads a host image into a kind cluster.
#
# `kind load docker-image` exports every platform referenced by an image index.
# Docker's containerd image store keeps registry images as multi-platform
# indexes while holding blobs only for the host platform, so that export fails
# with a missing content digest. Fall back to a single-platform archive, which
# preserves the image name inside the cluster.
kind_load_image() {
  local cluster="$1"
  local image="$2"

  if kind load docker-image "${image}" --name "${cluster}" 2>/dev/null; then
    return 0
  fi

  local platform archive status
  platform="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')"
  archive="$(mktemp)"
  status=0
  {
    docker save --platform "${platform}" "${image}" --output "${archive}" &&
      kind load image-archive "${archive}" --name "${cluster}"
  } || status=$?
  rm -f "${archive}"

  return "${status}"
}
