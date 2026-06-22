#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-dev}"
components=(control-plane frontline sidecar)

expected_error() {
  case "$1" in
    control-plane) echo "failed to get Postgres connection" ;;
    frontline) echo "transport error" ;;
    sidecar) echo "transport error" ;;
    *) return 1 ;;
  esac
}

smoke_run() {
  local image="$1"
  local component="$2"

  case "${component}" in
    control-plane)
      docker run --rm \
        --env SLEEPYPODS_CONTROL_PLANE_LISTEN_ADDR=127.0.0.1:50051 \
        --env SLEEPYPODS_STORE_PROVIDER=postgres \
        --env SLEEPYPODS_POSTGRES_URL=postgres://sleepypods:sleepypods@127.0.0.1:9/sleepypods \
        --env SLEEPYPODS_CLUSTER_ID=smoke \
        --env SLEEPYPODS_NAMESPACE=default \
        "${image}"
      ;;
    frontline)
      docker run --rm \
        --env SLEEPYPODS_FRONTLINE_LISTEN_ADDR=127.0.0.1:8080 \
        --env SLEEPYPODS_CONTROL_PLANE_ENDPOINT=http://127.0.0.1:9 \
        "${image}"
      ;;
    sidecar)
      docker run --rm \
        --env SLEEPYPODS_SIDECAR_LISTEN_ADDR=127.0.0.1:8081 \
        --env SLEEPYPODS_APP_PORT=8080 \
        --env SLEEPYPODS_INSTANCE_ID=smoke-instance \
        --env SLEEPYPODS_INSTANCE_GENERATION=1 \
        --env SLEEPYPODS_CONTROL_PLANE_ENDPOINT=http://127.0.0.1:9 \
        "${image}"
      ;;
    *) return 1 ;;
  esac
}

assert_runtime_files() {
  local image="$1"
  local component="$2"
  local container_id contents
  contents="$(mktemp)"
  container_id="$(docker create "${image}")"

  if ! docker export "${container_id}" | tar -t >"${contents}"; then
    docker rm -f "${container_id}" >/dev/null 2>&1 || true
    rm -f "${contents}"
    return 1
  fi
  docker rm -f "${container_id}" >/dev/null

  if ! grep -Fxq "usr/local/bin/${component}" "${contents}"; then
    echo "${image} is missing /usr/local/bin/${component}" >&2
    rm -f "${contents}"
    return 1
  fi

  if ! grep -Fxq "etc/ssl/certs/ca-certificates.crt" "${contents}"; then
    echo "${image} is missing CA roots" >&2
    rm -f "${contents}"
    return 1
  fi

  if grep -Eq '^(bin|usr/bin)/(sh|bash|apt|apt-get)$' "${contents}"; then
    echo "${image} contains a shell or package-manager binary" >&2
    rm -f "${contents}"
    return 1
  fi

  rm -f "${contents}"
}

for component in "${components[@]}"; do
  image="${image_prefix}/${component}:${image_tag}"
  echo "Building ${image}"
  docker build \
    --build-arg "BIN=${component}" \
    --tag "${image}" \
    --file "${repo_root}/Dockerfile" \
    "${repo_root}"

  user="$(docker image inspect "${image}" --format '{{.Config.User}}')"
  if [[ -z "${user}" || "${user}" == "0" || "${user}" == "root" || "${user}" == "0:0" ]]; then
    echo "${image} does not declare a non-root user: ${user:-<empty>}" >&2
    exit 1
  fi

  assert_runtime_files "${image}" "${component}"

  set +e
  output="$(smoke_run "${image}" "${component}" 2>&1)"
  status=$?
  set -e

  if [[ "${status}" -eq 0 ]]; then
    echo "${image} unexpectedly reached a ready runtime during smoke" >&2
    exit 1
  fi

  if ! grep -Fq "$(expected_error "${component}")" <<<"${output}"; then
    echo "${image} did not report the expected startup error" >&2
    echo "${output}" >&2
    exit 1
  fi

  echo "Smoked ${image}"
done
