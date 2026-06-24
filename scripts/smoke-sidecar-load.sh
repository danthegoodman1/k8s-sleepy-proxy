#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/load-budget.sh"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-dev}"
sidecar_image="${SLEEPYPODS_SIDECAR_IMAGE:-${image_prefix}/sidecar:${image_tag}}"
helper_image="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_HELPER_IMAGE:-${image_prefix}/sidecar-load-smoke-helper:${image_tag}}"
requests="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_REQUESTS:-200}"
concurrency="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_CONCURRENCY:-8}"
strict_budgets="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_STRICT_BUDGETS:-0}"
if [[ "${strict_budgets}" == "1" ]]; then
  min_ratio="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_MIN_RATIO:-0.80}"
  default_max_added_p99_ms="25"
else
  min_ratio="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_MIN_RATIO:-0.10}"
  default_max_added_p99_ms="1000"
fi
max_added_p99_ms="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_MAX_ADDED_P99_MS:-${default_max_added_p99_ms}}"
backend_container_port="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_BACKEND_PORT:-18080}"
sidecar_container_port="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_SIDECAR_PORT:-18081}"
control_plane_container_port="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_CONTROL_PLANE_PORT:-19090}"
skip_build="${SLEEPYPODS_SIDECAR_LOAD_SMOKE_SKIP_BUILD:-0}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_positive_integer() {
  local name="$1"
  local value="$2"

  if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer, got ${value}" >&2
    exit 1
  fi
}

require_decimal() {
  local name="$1"
  local value="$2"

  if [[ ! "${value}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "${name} must be a decimal number, got ${value}" >&2
    exit 1
  fi
}

require_command awk
require_command cargo
require_command docker

require_positive_integer SLEEPYPODS_SIDECAR_LOAD_SMOKE_REQUESTS "${requests}"
require_positive_integer SLEEPYPODS_SIDECAR_LOAD_SMOKE_CONCURRENCY "${concurrency}"
require_positive_integer SLEEPYPODS_SIDECAR_LOAD_SMOKE_BACKEND_PORT "${backend_container_port}"
require_positive_integer SLEEPYPODS_SIDECAR_LOAD_SMOKE_SIDECAR_PORT "${sidecar_container_port}"
require_positive_integer SLEEPYPODS_SIDECAR_LOAD_SMOKE_CONTROL_PLANE_PORT "${control_plane_container_port}"
require_decimal SLEEPYPODS_SIDECAR_LOAD_SMOKE_MIN_RATIO "${min_ratio}"
require_decimal SLEEPYPODS_SIDECAR_LOAD_SMOKE_MAX_ADDED_P99_MS "${max_added_p99_ms}"
if [[ "${strict_budgets}" != "0" && "${strict_budgets}" != "1" ]]; then
  echo "SLEEPYPODS_SIDECAR_LOAD_SMOKE_STRICT_BUDGETS must be 0 or 1, got ${strict_budgets}" >&2
  exit 1
fi

client_bin="${repo_root}/target/debug/examples/sidecar_load_smoke"
run_id="sleepypods-sidecar-load-smoke-$(date +%s)-$$"
helper_name="${run_id}-helper"
sidecar_name="${run_id}-sidecar"
direct_output_file="$(mktemp)"
sidecar_output_file="$(mktemp)"

cleanup() {
  rm -f "${direct_output_file}" "${sidecar_output_file}"
  docker rm -f "${sidecar_name}" "${helper_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

dump_logs() {
  echo "---- ${helper_name} logs ----" >&2
  docker logs "${helper_name}" >&2 || true
  echo "---- ${sidecar_name} logs ----" >&2
  docker logs "${sidecar_name}" >&2 || true
}

container_is_running() {
  [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == "true" ]]
}

published_port() {
  local container="$1"
  local port="$2"
  local mapping
  mapping="$(docker port "${container}" "${port}/tcp")"
  echo "${mapping##*:}"
}

wait_for_url() {
  local label="$1"
  local url="$2"
  local attempt

  for attempt in $(seq 1 80); do
    if "${client_bin}" client --label "${label}" --url "${url}" --requests 1 --concurrency 1 >/dev/null 2>&1; then
      return 0
    fi

    if ! container_is_running "${helper_name}"; then
      echo "${helper_name} exited before ${label} became ready" >&2
      dump_logs
      return 1
    fi

    if [[ "${label}" == "sidecar" ]] && ! container_is_running "${sidecar_name}"; then
      echo "${sidecar_name} exited before it became ready" >&2
      dump_logs
      return 1
    fi

    sleep 0.25
  done

  echo "timed out waiting for ${label} at ${url}" >&2
  dump_logs
  return 1
}

run_load() {
  local label="$1"
  local url="$2"
  local output_file="$3"
  local output
  local status

  set +e
  output="$("${client_bin}" client \
    --label "${label}" \
    --url "${url}" \
    --requests "${requests}" \
    --concurrency "${concurrency}" 2>&1)"
  status=$?
  set -e

  printf '%s\n' "${output}"
  printf '%s\n' "${output}" >"${output_file}"

  if [[ "${status}" -ne 0 ]]; then
    dump_logs
    exit "${status}"
  fi
}

extract_metric() {
  local line="$1"
  local key="$2"

  load_budget_extract_metric "${line}" "${key}"
}

echo "Building local load-smoke client"
cargo build -p sidecar --example sidecar_load_smoke

if [[ "${skip_build}" == "1" ]]; then
  docker image inspect "${sidecar_image}" >/dev/null
  docker image inspect "${helper_image}" >/dev/null
else
  echo "Building ${sidecar_image}"
  docker build \
    --build-arg "BIN=sidecar" \
    --tag "${sidecar_image}" \
    --file "${repo_root}/Dockerfile" \
    "${repo_root}"

  echo "Building ${helper_image}"
  docker build \
    --tag "${helper_image}" \
    --file "${repo_root}/scripts/Dockerfile.sidecar-load-smoke" \
    "${repo_root}"
fi

echo "Starting sidecar load-smoke helper"
docker run -d \
  --name "${helper_name}" \
  --publish "127.0.0.1::${backend_container_port}" \
  --publish "127.0.0.1::${sidecar_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_BACKEND_ADDR=0.0.0.0:${backend_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_CONTROL_PLANE_ADDR=0.0.0.0:${control_plane_container_port}" \
  "${helper_image}" >/dev/null

direct_port="$(published_port "${helper_name}" "${backend_container_port}")"
sidecar_port="$(published_port "${helper_name}" "${sidecar_container_port}")"
direct_url="http://127.0.0.1:${direct_port}/smoke"
sidecar_url="http://127.0.0.1:${sidecar_port}/smoke"

wait_for_url direct "${direct_url}"

echo "Starting ${sidecar_image}"
docker run -d \
  --name "${sidecar_name}" \
  --network "container:${helper_name}" \
  --env "SLEEPYPODS_SIDECAR_LISTEN_ADDR=0.0.0.0:${sidecar_container_port}" \
  --env "SLEEPYPODS_APP_PORT=${backend_container_port}" \
  --env "SLEEPYPODS_INSTANCE_ID=sidecar-load-smoke" \
  --env "SLEEPYPODS_INSTANCE_GENERATION=1" \
  --env "SLEEPYPODS_CONTROL_PLANE_ENDPOINT=http://127.0.0.1:${control_plane_container_port}" \
  --env "SLEEPYPODS_IDLE_TIMEOUT_MS=600000" \
  --env "SLEEPYPODS_IDLE_RETRY_BACKOFF_MS=1000" \
  --env "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS=5000" \
  "${sidecar_image}" >/dev/null

wait_for_url sidecar "${sidecar_url}"

echo "Running direct-backend smoke load"
run_load direct "${direct_url}" "${direct_output_file}"

echo "Running sidecar smoke load"
run_load sidecar "${sidecar_url}" "${sidecar_output_file}"

direct_result="$(cat "${direct_output_file}")"
sidecar_result="$(cat "${sidecar_output_file}")"
direct_rps="$(extract_metric "${direct_result}" rps)"
sidecar_rps="$(extract_metric "${sidecar_result}" rps)"
direct_p99_ms="$(extract_metric "${direct_result}" p99_ms)"
sidecar_p99_ms="$(extract_metric "${sidecar_result}" p99_ms)"

if ! load_budget_assert_ratio_at_least "sidecar_direct_http1_rps" "${direct_rps}" "${sidecar_rps}" "${min_ratio}"; then
  dump_logs
  exit 1
fi

if ! load_budget_assert_added_p99_at_most "sidecar_direct_http1" "${direct_p99_ms}" "${sidecar_p99_ms}" "${max_added_p99_ms}"; then
  dump_logs
  exit 1
fi
