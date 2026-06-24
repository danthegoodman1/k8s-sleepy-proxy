#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/load-budget.sh"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-dev}"
frontline_image="${SLEEPYPODS_FRONTLINE_IMAGE:-${image_prefix}/frontline:${image_tag}}"
helper_image="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_HELPER_IMAGE:-${image_prefix}/frontline-load-smoke-helper:${image_tag}}"
requests="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REQUESTS:-200}"
concurrency="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_CONCURRENCY:-8}"
strict_budgets="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_STRICT_BUDGETS:-0}"
if [[ "${strict_budgets}" == "1" ]]; then
  min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO:-0.80}"
  default_max_added_p99_ms="25"
else
  min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO:-0.10}"
  default_max_added_p99_ms="1000"
fi
max_added_p99_ms="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MAX_ADDED_P99_MS:-${default_max_added_p99_ms}}"
grpc_requests="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_REQUESTS:-${requests}}"
grpc_concurrency="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_CONCURRENCY:-${concurrency}}"
if [[ "${strict_budgets}" == "1" ]]; then
  grpc_min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MIN_RATIO:-0.75}"
else
  grpc_min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MIN_RATIO:-${min_ratio}}"
fi
grpc_max_added_p99_ms="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MAX_ADDED_P99_MS:-${default_max_added_p99_ms}}"
websocket_requests="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_REQUESTS:-${requests}}"
websocket_concurrency="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_CONCURRENCY:-${concurrency}}"
websocket_stream_bytes="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_BYTES:-262144}"
websocket_stream_chunk_size="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_CHUNK_SIZE:-16384}"
if [[ "${strict_budgets}" == "1" ]]; then
  websocket_min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MIN_RATIO:-0.80}"
  websocket_stream_min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_MIN_RATIO:-0.80}"
  default_cold_wake_max_latency_ms="250"
else
  websocket_min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MIN_RATIO:-${min_ratio}}"
  websocket_stream_min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_MIN_RATIO:-${min_ratio}}"
  default_cold_wake_max_latency_ms="5000"
fi
websocket_max_added_p99_ms="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MAX_ADDED_P99_MS:-${default_max_added_p99_ms}}"
cold_wake_max_latency_ms="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_WAKE_MAX_LATENCY_MS:-${default_cold_wake_max_latency_ms}}"
route_host="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_HOST:-app.example.test}"
route_path="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_PATH:-/smoke}"
cold_route_path="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_PATH:-/cold-smoke}"
backend_container_port="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_BACKEND_PORT:-18080}"
frontline_container_port="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_FRONTLINE_PORT:-18081}"
control_plane_container_port="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_CONTROL_PLANE_PORT:-19090}"
skip_build="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_SKIP_BUILD:-0}"

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

require_header_value() {
  local name="$1"
  local value="$2"

  if [[ -z "${value//[[:space:]]/}" ]]; then
    echo "${name} must not be empty" >&2
    exit 1
  fi

  if [[ "${value}" == *$'\r'* || "${value}" == *$'\n'* ]]; then
    echo "${name} must not contain CR or LF" >&2
    exit 1
  fi
}

require_route_path() {
  local name="$1"
  local value="$2"

  require_header_value "${name}" "${value}"

  if [[ "${value}" != /* ]]; then
    echo "${name} must start with /, got ${value}" >&2
    exit 1
  fi
}

require_command awk
require_command cargo
require_command curl
require_command docker

require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REQUESTS "${requests}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_CONCURRENCY "${concurrency}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_REQUESTS "${grpc_requests}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_CONCURRENCY "${grpc_concurrency}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_REQUESTS "${websocket_requests}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_CONCURRENCY "${websocket_concurrency}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_BYTES "${websocket_stream_bytes}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_CHUNK_SIZE "${websocket_stream_chunk_size}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_BACKEND_PORT "${backend_container_port}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_FRONTLINE_PORT "${frontline_container_port}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_CONTROL_PLANE_PORT "${control_plane_container_port}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO "${min_ratio}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MIN_RATIO "${grpc_min_ratio}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MIN_RATIO "${websocket_min_ratio}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_STREAM_MIN_RATIO "${websocket_stream_min_ratio}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MAX_ADDED_P99_MS "${max_added_p99_ms}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_GRPC_MAX_ADDED_P99_MS "${grpc_max_added_p99_ms}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_WEBSOCKET_MAX_ADDED_P99_MS "${websocket_max_added_p99_ms}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_WAKE_MAX_LATENCY_MS "${cold_wake_max_latency_ms}"
if [[ "${strict_budgets}" != "0" && "${strict_budgets}" != "1" ]]; then
  echo "SLEEPYPODS_FRONTLINE_LOAD_SMOKE_STRICT_BUDGETS must be 0 or 1, got ${strict_budgets}" >&2
  exit 1
fi
require_header_value SLEEPYPODS_FRONTLINE_LOAD_SMOKE_HOST "${route_host}"
require_route_path SLEEPYPODS_FRONTLINE_LOAD_SMOKE_PATH "${route_path}"
require_route_path SLEEPYPODS_FRONTLINE_LOAD_SMOKE_COLD_PATH "${cold_route_path}"

client_bin="${repo_root}/target/debug/examples/frontline_load_smoke"
run_id="sleepypods-frontline-load-smoke-$(date +%s)-$$"
helper_name="${run_id}-helper"
frontline_name="${run_id}-frontline"
direct_output_file="$(mktemp)"
frontline_output_file="$(mktemp)"
grpc_direct_output_file="$(mktemp)"
grpc_frontline_output_file="$(mktemp)"
websocket_direct_output_file="$(mktemp)"
websocket_frontline_output_file="$(mktemp)"
cold_frontline_output_file="$(mktemp)"

cleanup() {
  rm -f "${direct_output_file}" "${frontline_output_file}" "${grpc_direct_output_file}" "${grpc_frontline_output_file}" "${websocket_direct_output_file}" "${websocket_frontline_output_file}" "${cold_frontline_output_file}"
  docker rm -f "${frontline_name}" "${helper_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

dump_logs() {
  echo "---- ${helper_name} logs ----" >&2
  docker logs "${helper_name}" >&2 || true
  echo "---- ${frontline_name} logs ----" >&2
  docker logs "${frontline_name}" >&2 || true
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
  local protocol="${3:-http1}"
  local attempt

  for attempt in $(seq 1 80); do
    if "${client_bin}" client \
      --label "${label}" \
      --url "${url}" \
      --host "${route_host}" \
      --requests 1 \
      --concurrency 1 \
      --protocol "${protocol}" \
      --websocket-stream-bytes "${websocket_stream_bytes}" \
      --websocket-stream-chunk-size "${websocket_stream_chunk_size}" >/dev/null 2>&1; then
      return 0
    fi

    if ! container_is_running "${helper_name}"; then
      echo "${helper_name} exited before ${label} became ready" >&2
      dump_logs
      return 1
    fi

    if [[ "${label}" == "frontline" ]] && ! container_is_running "${frontline_name}"; then
      echo "${frontline_name} exited before it became ready" >&2
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
  local request_count="${4:-${requests}}"
  local concurrency_count="${5:-${concurrency}}"
  local protocol="${6:-http1}"
  local output
  local status

  set +e
  output="$("${client_bin}" client \
    --label "${label}" \
    --url "${url}" \
    --host "${route_host}" \
    --requests "${request_count}" \
    --concurrency "${concurrency_count}" \
    --protocol "${protocol}" \
    --websocket-stream-bytes "${websocket_stream_bytes}" \
    --websocket-stream-chunk-size "${websocket_stream_chunk_size}" 2>&1)"
  status=$?
  set -e

  printf '%s\n' "${output}"
  printf '%s\n' "${output}" >"${output_file}"

  if [[ "${status}" -ne 0 ]]; then
    dump_logs
    exit "${status}"
  fi
}

assert_ratio_at_least() {
  local label="$1"
  local direct="$2"
  local proxied="$3"
  local min="$4"

  if ! load_budget_assert_ratio_at_least "frontline_direct_${label}_rps" "${direct}" "${proxied}" "${min}"; then
    dump_logs
    exit 1
  fi
}

assert_throughput_ratio_at_least() {
  local label="$1"
  local direct="$2"
  local proxied="$3"
  local min="$4"

  if ! load_budget_assert_ratio_at_least "frontline_direct_${label}_mib_per_s" "${direct}" "${proxied}" "${min}"; then
    dump_logs
    exit 1
  fi
}

assert_added_p99_at_most() {
  local label="$1"
  local direct="$2"
  local proxied="$3"
  local max="$4"

  if ! load_budget_assert_added_p99_at_most "frontline_direct_${label}" "${direct}" "${proxied}" "${max}"; then
    dump_logs
    exit 1
  fi
}

assert_value_at_most() {
  local label="$1"
  local value="$2"
  local max="$3"

  if ! load_budget_assert_value_at_most "${label}" "${value}" "${max}"; then
    dump_logs
    exit 1
  fi
}

extract_metric() {
  local line="$1"
  local key="$2"

  load_budget_extract_metric "${line}" "${key}"
}

read_helper_metric() {
  local url="$1"
  local key="$2"
  local output
  local count

  if ! output="$(curl -fsS "${url}")"; then
    echo "failed to read frontline load-smoke helper stats from ${url}" >&2
    dump_logs
    exit 1
  fi

  if ! count="$(extract_metric "${output}" "${key}")"; then
    echo "helper stats did not include ${key}: ${output}" >&2
    dump_logs
    exit 1
  fi

  if [[ ! "${count}" =~ ^[0-9]+$ ]]; then
    echo "helper ${key} must be a non-negative integer, got ${count}" >&2
    dump_logs
    exit 1
  fi

  echo "${count}"
}

read_subscribe_route_calls() {
  read_helper_metric "$1" subscribe_route_calls
}

read_wake_instance_calls() {
  read_helper_metric "$1" wake_instance_calls
}

read_backend_http_requests() {
  read_helper_metric "$1" backend_http_requests
}

echo "Building local frontline load-smoke client"
cargo build -p frontline --example frontline_load_smoke

if [[ "${skip_build}" == "1" ]]; then
  docker image inspect "${frontline_image}" >/dev/null
  docker image inspect "${helper_image}" >/dev/null
else
  echo "Building ${frontline_image}"
  docker build \
    --build-arg "BIN=frontline" \
    --tag "${frontline_image}" \
    --file "${repo_root}/Dockerfile" \
    "${repo_root}"

  echo "Building ${helper_image}"
  docker build \
    --tag "${helper_image}" \
    --file "${repo_root}/scripts/Dockerfile.frontline-load-smoke" \
    "${repo_root}"
fi

echo "Starting frontline load-smoke helper"
docker run -d \
  --name "${helper_name}" \
  --publish "127.0.0.1::${backend_container_port}" \
  --publish "127.0.0.1::${frontline_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_BACKEND_ADDR=0.0.0.0:${backend_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_CONTROL_PLANE_ADDR=0.0.0.0:${control_plane_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_BACKEND_URI=http://127.0.0.1:${backend_container_port}" \
  --env "SLEEPYPODS_LOAD_SMOKE_ROUTE_HOST=${route_host}" \
  --env "SLEEPYPODS_LOAD_SMOKE_ROUTE_PATH=${route_path}" \
  --env "SLEEPYPODS_LOAD_SMOKE_COLD_ROUTE_PATH=${cold_route_path}" \
  --env "SLEEPYPODS_LOAD_SMOKE_WEBSOCKET_STREAM_BYTES=${websocket_stream_bytes}" \
  --env "SLEEPYPODS_LOAD_SMOKE_WEBSOCKET_STREAM_CHUNK_SIZE=${websocket_stream_chunk_size}" \
  "${helper_image}" >/dev/null

direct_port="$(published_port "${helper_name}" "${backend_container_port}")"
frontline_port="$(published_port "${helper_name}" "${frontline_container_port}")"
direct_url="http://127.0.0.1:${direct_port}${route_path}"
frontline_url="http://127.0.0.1:${frontline_port}${route_path}"
cold_frontline_url="http://127.0.0.1:${frontline_port}${cold_route_path}"
stats_url="http://127.0.0.1:${direct_port}/__sleepypods_load_smoke_stats"

wait_for_url direct "${direct_url}" http1

echo "Starting ${frontline_image}"
docker run -d \
  --name "${frontline_name}" \
  --network "container:${helper_name}" \
  --env "SLEEPYPODS_FRONTLINE_LISTEN_ADDR=0.0.0.0:${frontline_container_port}" \
  --env "SLEEPYPODS_CONTROL_PLANE_ENDPOINT=http://127.0.0.1:${control_plane_container_port}" \
  --env "SLEEPYPODS_ROUTE_CACHE_CAPACITY=32" \
  --env "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS=5000" \
  "${frontline_image}" >/dev/null

wait_for_url frontline "${frontline_url}" http1
echo "Frontline route cache warmed for host ${route_host} path ${route_path}"

echo "Running direct-backend smoke load"
run_load direct "${direct_url}" "${direct_output_file}" "${requests}" "${concurrency}" http1

subscribe_route_calls_before="$(read_subscribe_route_calls "${stats_url}")"
echo "SubscribeRoute calls before measured frontline phase=${subscribe_route_calls_before}"

echo "Running frontline smoke load"
run_load frontline "${frontline_url}" "${frontline_output_file}" "${requests}" "${concurrency}" http1

subscribe_route_calls_after="$(read_subscribe_route_calls "${stats_url}")"

if [[ "${subscribe_route_calls_after}" != "${subscribe_route_calls_before}" ]]; then
  echo "hot-cache frontline load made additional SubscribeRoute calls: before=${subscribe_route_calls_before} after=${subscribe_route_calls_after}" >&2
  dump_logs
  exit 1
fi

direct_result="$(cat "${direct_output_file}")"
frontline_result="$(cat "${frontline_output_file}")"
direct_rps="$(extract_metric "${direct_result}" rps)"
frontline_rps="$(extract_metric "${frontline_result}" rps)"
direct_p99_ms="$(extract_metric "${direct_result}" p99_ms)"
frontline_p99_ms="$(extract_metric "${frontline_result}" p99_ms)"
assert_ratio_at_least "http1" "${direct_rps}" "${frontline_rps}" "${min_ratio}"
assert_added_p99_at_most "http1" "${direct_p99_ms}" "${frontline_p99_ms}" "${max_added_p99_ms}"

echo "hot_cache_http1_subscribe_route_calls=0 subscribe_route_calls_before=${subscribe_route_calls_before} subscribe_route_calls_after=${subscribe_route_calls_after}"

echo "Warming h2c gRPC-shaped hot-cache route"
wait_for_url frontline "${frontline_url}" h2c-grpc

echo "Running direct-backend h2c gRPC-shaped smoke load"
run_load direct-grpc "${direct_url}" "${grpc_direct_output_file}" "${grpc_requests}" "${grpc_concurrency}" h2c-grpc

grpc_subscribe_route_calls_before="$(read_subscribe_route_calls "${stats_url}")"
echo "SubscribeRoute calls before measured h2c gRPC-shaped frontline phase=${grpc_subscribe_route_calls_before}"

echo "Running frontline h2c gRPC-shaped smoke load"
run_load frontline-grpc "${frontline_url}" "${grpc_frontline_output_file}" "${grpc_requests}" "${grpc_concurrency}" h2c-grpc

grpc_subscribe_route_calls_after="$(read_subscribe_route_calls "${stats_url}")"

if [[ "${grpc_subscribe_route_calls_after}" != "${grpc_subscribe_route_calls_before}" ]]; then
  echo "hot-cache h2c gRPC-shaped frontline load made additional SubscribeRoute calls: before=${grpc_subscribe_route_calls_before} after=${grpc_subscribe_route_calls_after}" >&2
  dump_logs
  exit 1
fi

grpc_direct_result="$(cat "${grpc_direct_output_file}")"
grpc_frontline_result="$(cat "${grpc_frontline_output_file}")"
grpc_direct_rps="$(extract_metric "${grpc_direct_result}" rps)"
grpc_frontline_rps="$(extract_metric "${grpc_frontline_result}" rps)"
grpc_direct_p99_ms="$(extract_metric "${grpc_direct_result}" p99_ms)"
grpc_frontline_p99_ms="$(extract_metric "${grpc_frontline_result}" p99_ms)"
assert_ratio_at_least "h2c_grpc" "${grpc_direct_rps}" "${grpc_frontline_rps}" "${grpc_min_ratio}"
assert_added_p99_at_most "h2c_grpc" "${grpc_direct_p99_ms}" "${grpc_frontline_p99_ms}" "${grpc_max_added_p99_ms}"

echo "hot_cache_h2c_grpc_subscribe_route_calls=0 subscribe_route_calls_before=${grpc_subscribe_route_calls_before} subscribe_route_calls_after=${grpc_subscribe_route_calls_after}"

echo "Warming WebSocket hot-cache route"
wait_for_url frontline "${frontline_url}" websocket

echo "Running direct-backend WebSocket smoke load"
run_load direct-websocket "${direct_url}" "${websocket_direct_output_file}" "${websocket_requests}" "${websocket_concurrency}" websocket

websocket_subscribe_route_calls_before="$(read_subscribe_route_calls "${stats_url}")"
echo "SubscribeRoute calls before measured WebSocket frontline phase=${websocket_subscribe_route_calls_before}"

echo "Running frontline WebSocket smoke load"
run_load frontline-websocket "${frontline_url}" "${websocket_frontline_output_file}" "${websocket_requests}" "${websocket_concurrency}" websocket

websocket_subscribe_route_calls_after="$(read_subscribe_route_calls "${stats_url}")"

if [[ "${websocket_subscribe_route_calls_after}" != "${websocket_subscribe_route_calls_before}" ]]; then
  echo "hot-cache WebSocket frontline load made additional SubscribeRoute calls: before=${websocket_subscribe_route_calls_before} after=${websocket_subscribe_route_calls_after}" >&2
  dump_logs
  exit 1
fi

websocket_direct_result="$(cat "${websocket_direct_output_file}")"
websocket_frontline_result="$(cat "${websocket_frontline_output_file}")"
websocket_direct_rps="$(extract_metric "${websocket_direct_result}" rps)"
websocket_frontline_rps="$(extract_metric "${websocket_frontline_result}" rps)"
websocket_direct_p99_ms="$(extract_metric "${websocket_direct_result}" p99_ms)"
websocket_frontline_p99_ms="$(extract_metric "${websocket_frontline_result}" p99_ms)"
websocket_direct_mib_per_s="$(extract_metric "${websocket_direct_result}" mib_per_s)"
websocket_frontline_mib_per_s="$(extract_metric "${websocket_frontline_result}" mib_per_s)"
assert_ratio_at_least "websocket" "${websocket_direct_rps}" "${websocket_frontline_rps}" "${websocket_min_ratio}"
assert_throughput_ratio_at_least "websocket_stream" "${websocket_direct_mib_per_s}" "${websocket_frontline_mib_per_s}" "${websocket_stream_min_ratio}"
assert_added_p99_at_most "websocket" "${websocket_direct_p99_ms}" "${websocket_frontline_p99_ms}" "${websocket_max_added_p99_ms}"

echo "hot_cache_websocket_subscribe_route_calls=0 subscribe_route_calls_before=${websocket_subscribe_route_calls_before} subscribe_route_calls_after=${websocket_subscribe_route_calls_after}"

cold_subscribe_route_calls_before="$(read_subscribe_route_calls "${stats_url}")"
cold_wake_instance_calls_before="$(read_wake_instance_calls "${stats_url}")"
cold_backend_http_requests_before="$(read_backend_http_requests "${stats_url}")"
echo "Running cold-wake frontline smoke for host ${route_host} path ${cold_route_path}"
run_load frontline-cold-wake "${cold_frontline_url}" "${cold_frontline_output_file}" 1 1 http1
cold_frontline_result="$(cat "${cold_frontline_output_file}")"
cold_wake_latency_ms="$(extract_metric "${cold_frontline_result}" p99_ms)"
assert_value_at_most "frontline_fake_control_plane_cold_wake_latency_ms" "${cold_wake_latency_ms}" "${cold_wake_max_latency_ms}"

cold_subscribe_route_calls_after="$(read_subscribe_route_calls "${stats_url}")"
cold_wake_instance_calls_after="$(read_wake_instance_calls "${stats_url}")"
cold_backend_http_requests_after="$(read_backend_http_requests "${stats_url}")"

if [[ "${cold_subscribe_route_calls_after}" -ne $((cold_subscribe_route_calls_before + 1)) ]]; then
  echo "cold-wake smoke did not make exactly one SubscribeRoute call: before=${cold_subscribe_route_calls_before} after=${cold_subscribe_route_calls_after}" >&2
  dump_logs
  exit 1
fi

if [[ "${cold_wake_instance_calls_after}" -ne $((cold_wake_instance_calls_before + 1)) ]]; then
  echo "cold-wake smoke did not make exactly one WakeInstance call: before=${cold_wake_instance_calls_before} after=${cold_wake_instance_calls_after}" >&2
  dump_logs
  exit 1
fi

if [[ "${cold_backend_http_requests_after}" -ne $((cold_backend_http_requests_before + 1)) ]]; then
  echo "cold-wake smoke did not reach backend exactly once: before=${cold_backend_http_requests_before} after=${cold_backend_http_requests_after}" >&2
  dump_logs
  exit 1
fi

echo "cold_wake_subscribe_route_calls=1 cold_wake_wake_instance_calls=1 cold_wake_backend_http_requests=1 cold_wake_latency_ms=${cold_wake_latency_ms} cold_wake_max_latency_ms=${cold_wake_max_latency_ms}"
