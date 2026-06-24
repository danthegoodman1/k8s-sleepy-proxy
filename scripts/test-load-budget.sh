#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/lib/load-budget.sh"

failures=0

expect_pass() {
  local label="$1"
  shift

  if "$@" >/dev/null 2>&1; then
    echo "ok ${label}"
  else
    echo "not ok ${label}: expected success" >&2
    failures=$((failures + 1))
  fi
}

expect_fail() {
  local label="$1"
  shift

  if "$@" >/dev/null 2>&1; then
    echo "not ok ${label}: expected failure" >&2
    failures=$((failures + 1))
  else
    echo "ok ${label}"
  fi
}

expect_fail "missing metric fails" \
  load_budget_extract_metric "requests=10 rps=100.0" p99_ms

expect_fail "malformed metric fails" \
  load_budget_extract_metric "requests=10 p99_ms=not-a-number" p99_ms

expect_fail "non-positive direct baseline fails" \
  load_budget_assert_ratio_at_least test_rps 0 10 0.80

expect_fail "ratio below threshold fails" \
  load_budget_assert_ratio_at_least test_rps 100 70 0.80

expect_fail "excessive p99 delta fails" \
  load_budget_assert_added_p99_at_most test_latency 10 40 25

expect_fail "non-positive absolute value fails" \
  load_budget_assert_value_at_most test_cold_wake 0 1000

expect_fail "absolute value above threshold fails" \
  load_budget_assert_value_at_most test_cold_wake 1200 1000

expect_pass "valid ratio passes" \
  load_budget_assert_ratio_at_least test_rps 100 85 0.80

expect_pass "valid p99 delta passes" \
  load_budget_assert_added_p99_at_most test_latency 10 30 25

expect_pass "absolute value within threshold passes" \
  load_budget_assert_value_at_most test_cold_wake 900 1000

if [[ "${failures}" -ne 0 ]]; then
  echo "${failures} load-budget helper test(s) failed" >&2
  exit 1
fi
