#!/usr/bin/env python3
"""Exercise the real checker CLI and local shell's status/cleanup composition."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts/check-postgres-execution.py"
VALID = """   Finished `test` profile
     Running tests/postgres_store.rs

running 5 tests
test postgres_provider_reports_invalid_connection_url ... ok
test postgres_store_conformance_against_real_database ... ok
test certificates::postgres_certificate_cas_rotation_tombstones_and_sealing ... ok
test certificates::api::postgres_certificate_native_tls_role_boundary_and_http01 ... ok
test certificates::api::watch::postgres_tls_watch_two_replicas_registration_shared_rotation_rebind_and_retention ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.23s
"""


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="sleepypods-pg-check-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.log = self.directory / "postgres.out"

    def check(self, output):
        self.log.write_text(output)
        return subprocess.run([sys.executable, str(CHECKER), str(self.log)],
                              text=True, capture_output=True, timeout=5)

    def rejected(self, output, reason):
        result = self.check(output)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(reason, result.stderr)

    def test_complete_target_passes(self):
        result = self.check(VALID)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("5 passed", result.stdout)

    def test_mixed_pass_and_every_current_skip_vocabulary_fails(self):
        for message in ["skipping real certificate PostgreSQL test: URL absent",
                        "skipping activation floor", "skipping real-time cleanup boundary",
                        "SKIPPED database fixture", "skip PostgreSQL", "1 skips"]:
            with self.subTest(message=message):
                self.rejected(message + "\n" + VALID, "reported a skip")

    def test_zero_or_url_only_execution_fails(self):
        self.rejected("running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n", "nonzero")
        self.rejected("running 1 test\ntest postgres_provider_reports_invalid_connection_url ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n", "store conformance")

    def test_ignored_failed_measured_and_filtered_targets_fail(self):
        for field in ["failed", "ignored", "measured", "filtered out"]:
            with self.subTest(field=field):
                self.rejected(VALID.replace(f"0 {field}", f"1 {field}"), "zero failed")
        self.rejected(VALID.replace("test result: ok.", "test result: FAILED."), "zero failed")
        self.rejected(VALID + "error: test failed after output\n", "Cargo reported")

    def test_missing_malformed_or_multiple_summaries_fail(self):
        for output in ["", VALID.split("test result:")[0],
                       VALID.replace("5 passed;", "many passed;"), VALID + VALID]:
            with self.subTest(output=output):
                self.rejected(output, "exactly one well-formed")

    def test_inventory_count_duplicates_and_required_coverage_fail(self):
        self.rejected(VALID.replace("running 5 tests", "running 6 tests"), "running-test count")
        self.rejected(VALID.replace("test postgres_provider_reports_invalid_connection_url ... ok\n", ""), "inventory")
        self.rejected(VALID.replace("postgres_provider_reports_invalid_connection_url", "postgres_store_conformance_against_real_database"), "duplicates")
        for name, reason in [("certificates::postgres_certificate_cas_rotation_tombstones_and_sealing", "certificate store"),
                             ("certificates::api::postgres_certificate_native_tls_role_boundary_and_http01", "certificate API"),
                             ("certificates::api::watch::postgres_tls_watch_two_replicas_registration_shared_rotation_rebind_and_retention", "certificate watch")]:
            with self.subTest(name=name):
                self.rejected(VALID.replace(name, "some_other_test"), reason)

    def test_contradictory_results_and_out_of_order_summary_fail(self):
        self.rejected(VALID + "test hidden ... ignored\n", "non-passing")
        self.rejected(VALID + "test hidden ... FAILED\n", "non-passing")
        before, summary = VALID.split("test result:")
        self.rejected("test result:" + summary + before, "boundaries")

    def test_missing_or_invalid_utf8_log_fails(self):
        for data in [None, b"\xff"]:
            if data is not None:
                self.log.write_bytes(data)
            result = subprocess.run([sys.executable, str(CHECKER), str(self.log)],
                                    text=True, capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 1)

    def test_local_script_preserves_cargo_status_checks_output_and_cleans_own_log(self):
        cargo = self.directory / "cargo"
        cargo.write_text(f"#!{sys.executable}\n" + "import json,os,sys\nfrom pathlib import Path\nPath(os.environ['MOCK_ARGS']).write_text(json.dumps(sys.argv[1:]))\nsys.stdout.write(os.environ['MOCK_LOG'])\nsys.exit(int(os.environ['MOCK_STATUS']))\n")
        cargo.chmod(0o755)
        marker = self.directory / "keep"
        marker.write_text("unrelated")
        for output, cargo_status, expected in [(VALID, 0, 0),
                ("skipping real certificate PostgreSQL test\n" + VALID, 0, 1),
                (VALID, 37, 37)]:
            with self.subTest(cargo_status=cargo_status, expected=expected):
                environment = dict(os.environ, PATH=f"{self.directory}:{os.environ['PATH']}",
                                   TMPDIR=str(self.directory), SLEEPYPODS_POSTGRES_URL="postgres://unused/mock",
                                   MOCK_ARGS=str(self.directory / "args.json"), MOCK_LOG=output,
                                   MOCK_STATUS=str(cargo_status))
                result = subprocess.run(["bash", str(ROOT / "scripts/test-postgres-store.sh")],
                                        env=environment, text=True, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                self.assertEqual(json.loads((self.directory / "args.json").read_text()),
                    ["test", "--locked", "-p", "control-plane", "--test", "postgres_store", "--", "--nocapture", "--color", "never"])
                self.assertEqual(list(self.directory.glob("sleepypods-postgres-store.*")), [])
                self.assertEqual(marker.read_text(), "unrelated")


if __name__ == "__main__":
    unittest.main()
