#!/usr/bin/env python3
"""Supplement the completed soak with a bounded, fail-closed read-only inventory."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

root = Path(__file__).resolve().parents[2]
output = root / '.generated/implementation-evidence'
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
stem = output / (stamp + '-final-soak-inventory')
kubeconfig = output / 'kubeconfig'
env = os.environ.copy()
env['KUBECONFIG'] = str(kubeconfig)
context = subprocess.check_output(
    ['kubectl', 'config', 'current-context'], env=env, text=True, timeout=10
).strip()
if context != 'kind-sleepypods-review-remediation':
    raise SystemExit('Unexpected kubeconfig context: ' + context)
command = [
    '/bin/bash', '-euo', 'pipefail', '-c',
    'source scripts/lib/full-wake-sleep-inventory.sh; '
    'wait_for_full_wake_sleep_no_leaks "$1" "final supplemental inventory" 0',
    'inventory-check', str(kubeconfig),
]
helper = root / 'scripts/lib/full-wake-sleep-inventory.sh'
record = {
    'started_utc': stamp,
    'context': context,
    'command': command,
    'read_only': True,
    'outer_timeout_seconds': 45,
    'polling_budget_seconds': 0,
    'helper_sha256': hashlib.sha256(helper.read_bytes()).hexdigest(),
    'scope': 'The four unchanged namespace/workload/PV/RBAC soak selectors in the helper.',
    'interpretation': 'Success requires all four discovery commands to succeed and return no object names.',
}
started = time.monotonic()
with stem.with_suffix('.log').open('w') as log:
    process = subprocess.Popen(command, cwd=root, env=env, stdout=log,
                               stderr=subprocess.STDOUT, start_new_session=True)
    try:
        record['exit_code'] = process.wait(timeout=45)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        record['exit_code'] = 124
        record['timeout'] = True
record['elapsed_seconds'] = time.monotonic() - started
stem.with_suffix('.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record), flush=True)
raise SystemExit(record['exit_code'])
