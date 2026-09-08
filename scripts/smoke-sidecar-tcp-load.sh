#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/load-budget.sh"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-dev}"
sidecar_image="${SLEEPYPODS_SIDECAR_IMAGE:-${image_prefix}/sidecar:${image_tag}}"
helper_image="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_HELPER_IMAGE:-${SLEEPYPODS_SIDECAR_LOAD_SMOKE_HELPER_IMAGE:-${image_prefix}/sidecar-load-smoke-helper:${image_tag}}}"
streams="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STREAMS:-4}"
concurrency="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONCURRENCY:-2}"
bytes_per_stream="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BYTES_PER_STREAM:-4194304}"
chunk_size="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CHUNK_SIZE:-16384}"
strict_budgets="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STRICT_BUDGETS:-0}"
if [[ "${strict_budgets}" == "1" ]]; then
  min_ratio="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MIN_RATIO:-0.85}"
  default_max_added_p99_ms="500"
else
  min_ratio="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MIN_RATIO:-0.05}"
  default_max_added_p99_ms="10000"
fi
max_added_p99_ms="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MAX_ADDED_P99_MS:-${default_max_added_p99_ms}}"
http_backend_container_port="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_HTTP_BACKEND_PORT:-18080}"
tcp_backend_container_port="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BACKEND_PORT:-18082}"
sidecar_container_port="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_SIDECAR_PORT:-18083}"
control_plane_container_port="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONTROL_PLANE_PORT:-19090}"
skip_build="${SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_SKIP_BUILD:-0}"

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

require_distinct_ports() {
  local left_name="$1"
  local left_value="$2"
  local right_name="$3"
  local right_value="$4"

  if [[ "${left_value}" == "${right_value}" ]]; then
    echo "${left_name} and ${right_name} must be distinct, both were ${left_value}" >&2
    exit 1
  fi
}

require_command awk
require_command cargo
require_command docker

require_positive_integer SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STREAMS "${streams}"
require_positive_integer SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONCURRENCY "${concurrency}"
require_positive_integer SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BYTES_PER_STREAM "${bytes_per_stream}"
require_positive_integer SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CHUNK_SIZE "${chunk_size}"
require_positive_integer SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_HTTP_BACKEND_PORT "${http_backend_container_port}"
require_positive_integer SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BACKEND_PORT "${tcp_backend_container_port}"
require_positive_integer SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_SIDECAR_PORT "${sidecar_container_port}"
require_positive_integer SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONTROL_PLANE_PORT "${control_plane_container_port}"
require_decimal SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MIN_RATIO "${min_ratio}"
require_decimal SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_MAX_ADDED_P99_MS "${max_added_p99_ms}"
if [[ "${strict_budgets}" != "0" && "${strict_budgets}" != "1" ]]; then
  echo "SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_STRICT_BUDGETS must be 0 or 1, got ${strict_budgets}" >&2
  exit 1
fi
require_distinct_ports SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_HTTP_BACKEND_PORT "${http_backend_container_port}" SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BACKEND_PORT "${tcp_backend_container_port}"
require_distinct_ports SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_HTTP_BACKEND_PORT "${http_backend_container_port}" SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_SIDECAR_PORT "${sidecar_container_port}"
require_distinct_ports SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_HTTP_BACKEND_PORT "${http_backend_container_port}" SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONTROL_PLANE_PORT "${control_plane_container_port}"
require_distinct_ports SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BACKEND_PORT "${tcp_backend_container_port}" SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_SIDECAR_PORT "${sidecar_container_port}"
require_distinct_ports SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_BACKEND_PORT "${tcp_backend_container_port}" SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONTROL_PLANE_PORT "${control_plane_container_port}"
require_distinct_ports SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_SIDECAR_PORT "${sidecar_container_port}" SLEEPYPODS_SIDECAR_TCP_LOAD_SMOKE_CONTROL_PLANE_PORT "${control_plane_container_port}"

client_bin="${repo_root}/target/debug/examples/sidecar_load_smoke"
run_id="sleepypods-sidecar-tcp-load-smoke-$(date +%s)-$$"
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

wait_for_tcp() {
  local label="$1"
  local addr="$2"
  local attempt

  for attempt in $(seq 1 80); do
    if "${client_bin}" tcp-client --label "${label}" --addr "${addr}" --streams 1 --concurrency 1 --bytes-per-stream 1024 --chunk-size 1024 >/dev/null 2>&1; then
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

  echo "timed out waiting for ${label} at ${addr}" >&2
  dump_logs
  return 1
}

run_load() {
  local label="$1"
  local addr="$2"
  local output_file="$3"
  local output
  local status

  set +e
  output="$("${client_bin}" tcp-client \
    --label "${label}" \
    --addr "${addr}" \
    --streams "${streams}" \
    --concurrency "${concurrency}" \
    --bytes-per-stream "${bytes_per_stream}" \
    --chunk-size "${chunk_size}" 2>&1)"
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

echo "Building local TCP load-smoke client"
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

echo "Starting sidecar TCP load-smoke helper"
docker run -d \
  --name "${helper_name}" \
  --publish "127.0.0.1::${tcp_backend_container_port}" \
  --publish "127.0.0.1::${sidecar_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_BACKEND_ADDR=0.0.0.0:${http_backend_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_TCP_BACKEND_ADDR=0.0.0.0:${tcp_backend_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_CONTROL_PLANE_ADDR=0.0.0.0:${control_plane_container_port}" \
  "${helper_image}" >/dev/null

direct_port="$(published_port "${helper_name}" "${tcp_backend_container_port}")"
sidecar_port="$(published_port "${helper_name}" "${sidecar_container_port}")"
direct_addr="127.0.0.1:${direct_port}"
sidecar_addr="127.0.0.1:${sidecar_port}"

wait_for_tcp direct "${direct_addr}"

echo "Starting ${sidecar_image} in TCP mode"
docker run -d \
  --name "${sidecar_name}" \
  --network "container:${helper_name}" \
  --env "SLEEPYPODS_SIDECAR_MODE=tcp" \
  --env "SLEEPYPODS_SIDECAR_LISTEN_ADDR=0.0.0.0:${sidecar_container_port}" \
  --env "SLEEPYPODS_APP_PORT=${tcp_backend_container_port}" \
  --env "SLEEPYPODS_INSTANCE_ID=sidecar-tcp-load-smoke" \
  --env "SLEEPYPODS_INSTANCE_GENERATION=1" \
  --env SLEEPYPODS_POD_UID=standalone-smoke \
  --env "SLEEPYPODS_CONTROL_PLANE_ENDPOINT=http://127.0.0.1:${control_plane_container_port}" \
  --env "SLEEPYPODS_IDLE_TIMEOUT_MS=600000" \
  --env "SLEEPYPODS_IDLE_RETRY_BACKOFF_MS=1000" \
  --env "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS=5000" \
  "${sidecar_image}" >/dev/null

wait_for_tcp sidecar "${sidecar_addr}"

echo "Running direct TCP echo smoke load"
run_load direct "${direct_addr}" "${direct_output_file}"

echo "Running sidecar TCP echo smoke load"
run_load sidecar "${sidecar_addr}" "${sidecar_output_file}"

direct_result="$(cat "${direct_output_file}")"
sidecar_result="$(cat "${sidecar_output_file}")"
direct_throughput="$(extract_metric "${direct_result}" throughput_mib_s)"
sidecar_throughput="$(extract_metric "${sidecar_result}" throughput_mib_s)"
direct_p99_ms="$(extract_metric "${direct_result}" p99_ms)"
sidecar_p99_ms="$(extract_metric "${sidecar_result}" p99_ms)"

if ! load_budget_assert_ratio_at_least "sidecar_direct_tcp_throughput" "${direct_throughput}" "${sidecar_throughput}" "${min_ratio}"; then
  dump_logs
  exit 1
fi

if ! load_budget_assert_added_p99_at_most "sidecar_direct_tcp_stream" "${direct_p99_ms}" "${sidecar_p99_ms}" "${max_added_p99_ms}"; then
  dump_logs
  exit 1
fi
