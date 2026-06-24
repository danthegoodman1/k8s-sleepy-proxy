#!/usr/bin/env bash

load_budget_require_decimal() {
  local name="$1"
  local value="$2"

  if [[ ! "${value}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "${name} must be a non-negative decimal number, got ${value}" >&2
    return 1
  fi
}

load_budget_extract_metric() {
  local line="$1"
  local key="$2"
  local field
  local value

  for field in ${line}; do
    case "${field}" in
      "${key}"=*)
        value="${field#*=}"
        load_budget_require_decimal "${key}" "${value}" || return 1
        echo "${value}"
        return 0
        ;;
    esac
  done

  echo "missing metric ${key} in result: ${line}" >&2
  return 1
}

load_budget_assert_ratio_at_least() {
  local label="$1"
  local direct="$2"
  local proxied="$3"
  local min="$4"
  local ratio
  local status

  load_budget_require_decimal "${label} direct value" "${direct}" || return 1
  load_budget_require_decimal "${label} proxied value" "${proxied}" || return 1
  load_budget_require_decimal "${label} min ratio" "${min}" || return 1

  set +e
  ratio="$(awk -v direct="${direct}" -v proxied="${proxied}" -v min="${min}" 'BEGIN {
    if (direct <= 0 || proxied < 0) {
      print "nan"
      exit 2
    }
    ratio = proxied / direct
    printf "%.3f", ratio
    if (ratio < min) {
      exit 1
    }
  }')"
  status=$?
  set -e

  if [[ "${status}" -eq 2 ]]; then
    echo "${label} direct value must be positive and proxied value non-negative: direct=${direct} proxied=${proxied}" >&2
    return 1
  fi

  if [[ "${status}" -ne 0 ]]; then
    echo "${label} ratio ${ratio} is below threshold ${min} (direct=${direct} proxied=${proxied})" >&2
    return 1
  fi

  echo "${label}_ratio=${ratio} direct=${direct} proxied=${proxied} min=${min}"
}

load_budget_assert_added_p99_at_most() {
  local label="$1"
  local direct_p99_ms="$2"
  local proxied_p99_ms="$3"
  local max_added_ms="$4"
  local added
  local status

  load_budget_require_decimal "${label} direct p99_ms" "${direct_p99_ms}" || return 1
  load_budget_require_decimal "${label} proxied p99_ms" "${proxied_p99_ms}" || return 1
  load_budget_require_decimal "${label} max added p99_ms" "${max_added_ms}" || return 1

  set +e
  added="$(awk -v direct="${direct_p99_ms}" -v proxied="${proxied_p99_ms}" -v max="${max_added_ms}" 'BEGIN {
    if (direct <= 0 || proxied <= 0) {
      print "nan"
      exit 2
    }
    added = proxied - direct
    printf "%.3f", added
    if (added > max) {
      exit 1
    }
  }')"
  status=$?
  set -e

  if [[ "${status}" -eq 2 ]]; then
    echo "${label} p99_ms values must be positive: direct=${direct_p99_ms} proxied=${proxied_p99_ms}" >&2
    return 1
  fi

  if [[ "${status}" -ne 0 ]]; then
    echo "${label} added p99_ms ${added} is above threshold ${max_added_ms} (direct_p99_ms=${direct_p99_ms} proxied_p99_ms=${proxied_p99_ms})" >&2
    return 1
  fi

  echo "${label}_added_p99_ms=${added} direct_p99_ms=${direct_p99_ms} proxied_p99_ms=${proxied_p99_ms} max_added_p99_ms=${max_added_ms}"
}
