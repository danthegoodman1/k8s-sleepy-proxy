#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export SLEEPYPODS_KIND_CLUSTER="${SLEEPYPODS_KIND_CLUSTER:-sleepypods-e2e-projection-drift-test}"
export SLEEPYPODS_KIND_E2E_NAMESPACE="${SLEEPYPODS_KIND_E2E_NAMESPACE:-sleepypods-e2e-projection-drift}"
export SLEEPYPODS_IMAGE_TAG="${SLEEPYPODS_IMAGE_TAG:-kind-e2e-projection-drift}"
export SLEEPYPODS_KIND_E2E_OPERATOR_PORT="${SLEEPYPODS_KIND_E2E_OPERATOR_PORT:-19451}"
export SLEEPYPODS_KIND_E2E_FRONTLINE_PORT="${SLEEPYPODS_KIND_E2E_FRONTLINE_PORT:-19480}"
export SLEEPYPODS_KIND_E2E_PROJECTION_DRIFT=1
export SLEEPYPODS_KIND_E2E_TEST_FILTER="${SLEEPYPODS_KIND_E2E_TEST_FILTER:-projection_drift_and_finalizer_safety_through_deployed_platform}"

exec "${script_dir}/test-kind-e2e-stateful.sh"
