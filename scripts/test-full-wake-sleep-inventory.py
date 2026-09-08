#!/usr/bin/env python3
"""Exercise the soak's real shell reader/poller with a task-local kubectl stub."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
READER = ROOT / "scripts/lib/full-wake-sleep-inventory.sh"
SCOPES = [
    ["get", "namespaces", "-l", "sleepypods.io/kind-e2e in (stateless,stateful)", "-o", "name"],
    ["get", "deployments.apps,statefulsets.apps,services,persistentvolumeclaims",
     "--all-namespaces", "-l",
     "sleepypods.io/instance-id in (e2e-stateless,e2e-stateless-abandoned,e2e-stateful)",
     "-o", "name"],
    ["get", "persistentvolumes", "-l",
     "sleepypods.io/instance-id in (e2e-stateless,e2e-stateless-abandoned,e2e-stateful)",
     "-o", "name"],
    ["get", "clusterrole,clusterrolebinding", "-l", "sleepypods.io/kind-e2e=stateful",
     "-o", "name"],
]


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="sleepypods-inventory-test-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        self.kubeconfig = self.path / "kubeconfig"
        self.kubeconfig.touch()
        mock = self.path / "kubectl"
        mock.write_text(f"#!{sys.executable}\n" + r'''
import json, os, pathlib, sys
root = pathlib.Path(os.environ["INVENTORY_MOCK_DIR"])
log = root / "calls.jsonl"
calls = log.read_text().splitlines() if log.exists() else []
responses = json.loads((root / "responses.json").read_text())
with log.open("a") as output:
    output.write(json.dumps({"args": sys.argv[1:], "kubeconfig": os.environ.get("KUBECONFIG")}) + "\n")
if len(calls) >= len(responses):
    print("unexpected kubectl call", file=sys.stderr)
    sys.exit(97)
reply = responses[len(calls)]
sys.stdout.write(reply.get("stdout", ""))
sys.stderr.write(reply.get("stderr", ""))
sys.exit(reply.get("code", 0))
''')
        mock.chmod(0o755)
        self.environment = dict(os.environ, PATH=f"{self.path}:{os.environ['PATH']}",
                                INVENTORY_MOCK_DIR=str(self.path))

    def invoke(self, replies, *, poll_timeout=None):
        (self.path / "responses.json").write_text(json.dumps(replies))
        log = self.path / "calls.jsonl"
        if log.exists():
            log.unlink()
        if poll_timeout is None:
            command = ["/bin/bash", "-euo", "pipefail", str(READER), str(self.kubeconfig)]
        else:
            command = ["/bin/bash", "-euo", "pipefail", "-c",
                       'source "$1"; wait_for_full_wake_sleep_no_leaks "$2" "mock cycle" "$3"',
                       "inventory-test", str(READER), str(self.kubeconfig), str(poll_timeout)]
        result = subprocess.run(command, env=self.environment, capture_output=True,
                                text=True, timeout=10)
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        for index, call in enumerate(calls):
            self.assertEqual(call["args"], SCOPES[index % 4])
            self.assertEqual(call["kubeconfig"], str(self.kubeconfig))
        return result, calls

    def test_successful_empty_inventory_requires_all_four_reads(self):
        for poll_timeout in [None, 90]:
            with self.subTest(poll_timeout=poll_timeout):
                result, calls = self.invoke([{}] * 4, poll_timeout=poll_timeout)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertEqual(len(calls), 4)

    def test_failure_in_any_read_preserves_status_and_stderr(self):
        for failed_index in range(4):
            with self.subTest(failed_index=failed_index):
                responses = [{"stdout": "namespace/retained\n"}] * failed_index
                responses.append({"code": 23, "stderr": "discovery permission denied\n"})
                result, calls = self.invoke(responses)
                self.assertEqual(result.returncode, 23)
                self.assertIn("discovery permission denied", result.stderr)
                self.assertEqual(len(calls), failed_index + 1)

    def test_nonempty_success_is_reported_and_fails_at_poll_deadline(self):
        replies = [{}, {"stdout": "deployment.apps/retained\n"}, {}, {}]
        result, calls = self.invoke(replies)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "deployment.apps/retained\n")
        self.assertEqual(len(calls), 4)
        result, calls = self.invoke(replies, poll_timeout=0)
        self.assertEqual(result.returncode, 1)
        self.assertIn("mock cycle leaked Kubernetes objects", result.stderr)
        self.assertIn("deployment.apps/retained", result.stderr)
        self.assertEqual(len(calls), 4)

    def test_poll_retries_observed_objects_then_requires_complete_empty_inventory(self):
        replies = [{"stdout": "namespace/terminating\n"}, {}, {}, {}] + [{}] * 4
        result, calls = self.invoke(replies, poll_timeout=90)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 8)

    def test_poll_failure_after_nonempty_does_not_become_success_or_retry(self):
        replies = [{"stdout": "namespace/terminating\n"}, {}, {}, {},
                   {"code": 17, "stderr": "API unavailable\n"}]
        result, calls = self.invoke(replies, poll_timeout=90)
        self.assertEqual(result.returncode, 17)
        self.assertIn("API unavailable", result.stderr)
        self.assertIn("mock cycle could not verify Kubernetes inventory", result.stderr)
        self.assertEqual(len(calls), 5)

    def test_poll_first_read_failure_never_claims_absence(self):
        result, calls = self.invoke([{"code": 19, "stderr": "connection refused\n"}],
                                    poll_timeout=90)
        self.assertEqual(result.returncode, 19)
        self.assertIn("connection refused", result.stderr)
        self.assertIn("read failed", result.stderr)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
