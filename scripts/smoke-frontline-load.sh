#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image_prefix="${SLEEPYPODS_IMAGE_PREFIX:-sleepypods}"
image_tag="${SLEEPYPODS_IMAGE_TAG:-dev}"
frontline_image="${SLEEPYPODS_FRONTLINE_IMAGE:-${image_prefix}/frontline:${image_tag}}"
helper_image="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_HELPER_IMAGE:-${image_prefix}/frontline-load-smoke-helper:${image_tag}}"
requests="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_REQUESTS:-200}"
concurrency="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_CONCURRENCY:-8}"
min_ratio="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO:-0.10}"
route_host="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_HOST:-app.example.test}"
route_path="${SLEEPYPODS_FRONTLINE_LOAD_SMOKE_PATH:-/smoke}"
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
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_BACKEND_PORT "${backend_container_port}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_FRONTLINE_PORT "${frontline_container_port}"
require_positive_integer SLEEPYPODS_FRONTLINE_LOAD_SMOKE_CONTROL_PLANE_PORT "${control_plane_container_port}"
require_decimal SLEEPYPODS_FRONTLINE_LOAD_SMOKE_MIN_RATIO "${min_ratio}"
require_header_value SLEEPYPODS_FRONTLINE_LOAD_SMOKE_HOST "${route_host}"
require_route_path SLEEPYPODS_FRONTLINE_LOAD_SMOKE_PATH "${route_path}"

client_bin="${repo_root}/target/debug/examples/frontline_load_smoke"
run_id="sleepypods-frontline-load-smoke-$(date +%s)-$$"
helper_name="${run_id}-helper"
frontline_name="${run_id}-frontline"
direct_output_file="$(mktemp)"
frontline_output_file="$(mktemp)"

cleanup() {
  rm -f "${direct_output_file}" "${frontline_output_file}"
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
  local attempt

  for attempt in $(seq 1 80); do
    if "${client_bin}" client --label "${label}" --url "${url}" --host "${route_host}" --requests 1 --concurrency 1 >/dev/null 2>&1; then
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
  local output
  local status

  set +e
  output="$("${client_bin}" client \
    --label "${label}" \
    --url "${url}" \
    --host "${route_host}" \
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
  local field

  for field in ${line}; do
    case "${field}" in
      "${key}"=*)
        echo "${field#*=}"
        return 0
        ;;
    esac
  done

  return 1
}

read_subscribe_route_calls() {
  local url="$1"
  local output
  local count

  if ! output="$(curl -fsS "${url}")"; then
    echo "failed to read frontline load-smoke helper stats from ${url}" >&2
    dump_logs
    exit 1
  fi

  if ! count="$(extract_metric "${output}" subscribe_route_calls)"; then
    echo "helper stats did not include subscribe_route_calls: ${output}" >&2
    dump_logs
    exit 1
  fi

  if [[ ! "${count}" =~ ^[0-9]+$ ]]; then
    echo "helper subscribe_route_calls must be a non-negative integer, got ${count}" >&2
    dump_logs
    exit 1
  fi

  echo "${count}"
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
  "${helper_image}" >/dev/null

direct_port="$(published_port "${helper_name}" "${backend_container_port}")"
frontline_port="$(published_port "${helper_name}" "${frontline_container_port}")"
direct_url="http://127.0.0.1:${direct_port}${route_path}"
frontline_url="http://127.0.0.1:${frontline_port}${route_path}"
stats_url="http://127.0.0.1:${direct_port}/__sleepypods_load_smoke_stats"

wait_for_url direct "${direct_url}"

echo "Starting ${frontline_image}"
docker run -d \
  --name "${frontline_name}" \
  --network "container:${helper_name}" \
  --env "SLEEPYPODS_FRONTLINE_LISTEN_ADDR=0.0.0.0:${frontline_container_port}" \
  --env "SLEEPYPODS_CONTROL_PLANE_ENDPOINT=http://127.0.0.1:${control_plane_container_port}" \
  --env "SLEEPYPODS_ROUTE_CACHE_CAPACITY=32" \
  --env "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS=5000" \
  "${frontline_image}" >/dev/null

wait_for_url frontline "${frontline_url}"
echo "Frontline route cache warmed for host ${route_host} path ${route_path}"

echo "Running direct-backend smoke load"
run_load direct "${direct_url}" "${direct_output_file}"

subscribe_route_calls_before="$(read_subscribe_route_calls "${stats_url}")"
echo "SubscribeRoute calls before measured frontline phase=${subscribe_route_calls_before}"

echo "Running frontline smoke load"
run_load frontline "${frontline_url}" "${frontline_output_file}"

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

set +e
ratio="$(awk -v direct="${direct_rps}" -v frontline="${frontline_rps}" -v min="${min_ratio}" 'BEGIN {
  if (direct <= 0) {
    print "nan"
    exit 2
  }
  ratio = frontline / direct
  printf "%.3f", ratio
  if (ratio < min) {
    exit 1
  }
}')"
ratio_status=$?
set -e

if [[ "${ratio_status}" -eq 2 ]]; then
  echo "direct RPS was not positive; cannot compare frontline smoke load" >&2
  dump_logs
  exit 1
fi

if [[ "${ratio_status}" -ne 0 ]]; then
  echo "frontline/direct RPS ratio ${ratio} is below conservative smoke threshold ${min_ratio}" >&2
  dump_logs
  exit 1
fi

echo "frontline/direct_rps_ratio=${ratio} min=${min_ratio}"
echo "hot_cache_subscribe_route_calls=0 subscribe_route_calls_before=${subscribe_route_calls_before} subscribe_route_calls_after=${subscribe_route_calls_after}"
