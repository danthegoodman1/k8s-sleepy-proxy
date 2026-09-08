#!/usr/bin/env python3
"""Run the canonical lifecycle gate with retained namespace and live diagnostics."""
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

root = Path(__file__).resolve().parents[2]
out = root / '.generated/implementation-evidence/final-lifecycle-capture'
out.mkdir(exist_ok=True)
namespace = 'sleepypods-e2e-lifecycle-final'
kubeconfig = root / '.generated/implementation-evidence/kubeconfig'
base = ['kubectl', '--kubeconfig', str(kubeconfig), '--context',
        'kind-sleepypods-review-remediation', '--request-timeout=5s']
# The canonical script's setup deletes must find nothing preexisting. Fail closed
# before it runs; this serial gate owns its newly created test resources only.
guards = [
    ['get', 'namespace', namespace, '--ignore-not-found', '-o', 'name'],
    ['get', 'persistentvolume', '-l',
     'sleepypods.io/workload-class-id in (lifecycle-delete-waking,lifecycle-delete-draining)', '-o', 'name'],
    ['get', 'clusterrole,clusterrolebinding',
     'sleepypods-control-plane-' + namespace, '--ignore-not-found', '-o', 'name'],
]
guard_results = []
for args in guards:
    result = subprocess.run(base + args, capture_output=True, text=True, timeout=8)
    guard_results.append({'args': args, 'exit_code': result.returncode,
                          'stdout': result.stdout, 'stderr': result.stderr})
    if result.returncode or result.stdout.strip():
        (out / 'setup-guards.json').write_text(json.dumps(guard_results, indent=2) + '\n')
        raise SystemExit('Refusing setup: failed discovery or preexisting gate resources.')
(out / 'setup-guards.json').write_text(json.dumps(guard_results, indent=2) + '\n')
env = os.environ.copy()
env['SLEEPYPODS_KIND_E2E_NAMESPACE'] = namespace
env['SLEEPYPODS_KIND_E2E_KEEP_NAMESPACE'] = '1'
record = {'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'namespace': namespace, 'keep_namespace': True,
          'gate_command': [sys.executable, '.generated/implementation-evidence/run-production-gates.py', 'lifecycle-races'],
          'capture': '.generated/implementation-evidence/capture-final-lifecycle.py',
          'gate_outer_timeout_seconds': 1200}
started = time.monotonic()
capture = subprocess.Popen([sys.executable, record['capture']], cwd=root, start_new_session=True)
gate = None
try:
    gate = subprocess.Popen(record['gate_command'], cwd=root, env=env, start_new_session=True)
    try:
        record['exit_code'] = gate.wait(timeout=1200)
    except subprocess.TimeoutExpired:
        os.killpg(gate.pid, signal.SIGKILL)
        gate.wait(timeout=5)
        record['exit_code'] = 124
    # Preserve a short, bounded tail after the original gate outcome.
    time.sleep(10)
finally:
    if gate is not None and gate.poll() is None:
        os.killpg(gate.pid, signal.SIGKILL)
        gate.wait(timeout=5)
    capture.terminate()
    try:
        capture.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(capture.pid, signal.SIGKILL)
        capture.wait(timeout=5)
    record['capture_exit_code'] = capture.returncode
for name, args in [
    ('control-plane.log', ['-n', namespace, 'logs', 'deployment/sleepypods-control-plane', '--timestamps=true']),
    ('kubernetes.json', ['-n', namespace, 'get', 'pods,deployments,replicasets,events', '-o', 'json']),
]:
    try:
        with (out / name).open('w') as log:
            result = subprocess.run(base + args, stdout=log, stderr=subprocess.STDOUT, timeout=10)
        record[name + '_exit_code'] = result.returncode
    except subprocess.TimeoutExpired:
        record[name + '_exit_code'] = 124
record['elapsed_seconds'] = time.monotonic() - started
(out / 'run.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record), flush=True)
raise SystemExit(record['exit_code'])
