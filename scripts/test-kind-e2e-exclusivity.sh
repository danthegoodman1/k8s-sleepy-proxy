#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export SLEEPYPODS_KIND_CLUSTER="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-e2e-exclusivity-test}"
export SLEEPYPODS_KIND_E2E_NAMESPACE="${SLEEPYPODS_KIND_E2E_NAMESPACE:-sleepypods-e2e-exclusivity}"
export SLEEPYPODS_IMAGE_TAG="${SLEEPYPODS_IMAGE_TAG:-kind-e2e-exclusivity}"
export SLEEPYPODS_KIND_E2E_OPERATOR_PORT="${SLEEPYPODS_KIND_E2E_OPERATOR_PORT:-19251}"
export SLEEPYPODS_KIND_E2E_FRONTLINE_PORT="${SLEEPYPODS_KIND_E2E_FRONTLINE_PORT:-19280}"
export SLEEPYPODS_KIND_E2E_EXCLUSIVITY=1
export SLEEPYPODS_KIND_E2E_TEST_FILTER="${SLEEPYPODS_KIND_E2E_TEST_FILTER:-stateful_exclusivity_keys_through_deployed_platform}"

exec "${script_dir}/test-kind-e2e-stateful.sh"
