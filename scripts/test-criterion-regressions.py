#!/usr/bin/env python3
"""Fixture tests for check-criterion-regressions.py."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER = REPO_ROOT / "scripts" / "check-criterion-regressions.py"


def write_estimate(path: Path, point_estimate: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"mean": {"point_estimate": point_estimate}}),
        encoding="utf-8",
    )


class CriterionRegressionCheckTests(unittest.TestCase):
    def run_checker(self, root: Path, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(CHECKER), "--root", str(root), *args],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_improvement_is_ok(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "change" / "estimates.json", -0.10)

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("benchmark=bench", result.stdout)
        self.assertIn("status=ok", result.stdout)
        self.assertIn("reason=improvement", result.stdout)
        self.assertIn("change_percent=-10.000", result.stdout)

    def test_no_change_is_ok(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "change" / "estimates.json", 0.0)

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("status=ok", result.stdout)
        self.assertIn("reason=no_change", result.stdout)
        self.assertIn("change_percent=0.000", result.stdout)

    def test_warning_regression_exits_successfully(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "change" / "estimates.json", 0.16)

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("status=warn", result.stdout)
        self.assertIn("reason=warning_regression", result.stdout)
        self.assertIn("change_percent=16.000", result.stdout)

    def test_failing_regression_exits_with_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "change" / "estimates.json", 0.30)

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 1)
        self.assertIn("status=fail", result.stdout)
        self.assertIn("reason=failing_regression", result.stdout)
        self.assertIn("change_percent=30.000", result.stdout)

    def test_missing_data_fails_unless_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "new" / "estimates.json", 100.0)

            result = self.run_checker(root)
            allowed = self.run_checker(root, "--allow-missing")

        self.assertEqual(result.returncode, 1)
        self.assertIn("status=fail", result.stdout)
        self.assertIn("reason=missing_data", result.stdout)
        self.assertEqual(allowed.returncode, 0, allowed.stderr)
        self.assertIn("status=ok", allowed.stdout)
        self.assertIn("reason=missing_data", allowed.stdout)

    def test_malformed_data_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "bench" / "change" / "estimates.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{not-json", encoding="utf-8")

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 1)
        self.assertIn("status=fail", result.stdout)
        self.assertIn("reason=malformed_data", result.stdout)

    def test_missing_mean_field_is_malformed_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "bench" / "change" / "estimates.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps({"median": {"point_estimate": 0.01}}),
                encoding="utf-8",
            )

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 1)
        self.assertIn("status=fail", result.stdout)
        self.assertIn("reason=malformed_data", result.stdout)

    def test_non_finite_change_estimate_is_malformed_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "bench" / "change" / "estimates.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                '{"mean": {"point_estimate": NaN}}',
                encoding="utf-8",
            )

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 1)
        self.assertIn("status=fail", result.stdout)
        self.assertIn("reason=malformed_data", result.stdout)

    def test_base_new_fallback_uses_mean_estimate_ratio(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "base" / "estimates.json", 100.0)
            write_estimate(root / "bench" / "new" / "estimates.json", 121.0)

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("source=base_new", result.stdout)
        self.assertIn("status=warn", result.stdout)
        self.assertIn("change_percent=21.000", result.stdout)

    def test_base_new_fallback_rejects_non_positive_new_estimate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "base" / "estimates.json", 100.0)
            write_estimate(root / "bench" / "new" / "estimates.json", 0.0)

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 1)
        self.assertIn("status=fail", result.stdout)
        self.assertIn("reason=malformed_data", result.stdout)

    def test_base_new_fallback_rejects_non_finite_base_estimate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            base_path = root / "bench" / "base" / "estimates.json"
            base_path.parent.mkdir(parents=True, exist_ok=True)
            base_path.write_text(
                '{"mean": {"point_estimate": Infinity}}',
                encoding="utf-8",
            )
            write_estimate(root / "bench" / "new" / "estimates.json", 100.0)

            result = self.run_checker(root)

        self.assertEqual(result.returncode, 1)
        self.assertIn("status=fail", result.stdout)
        self.assertIn("reason=malformed_data", result.stdout)

    def test_threshold_overrides_and_config_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "change" / "estimates.json", 0.06)

            warned = self.run_checker(
                root, "--warn-percent", "5", "--fail-percent", "50"
            )
            rejected = self.run_checker(
                root, "--warn-percent", "50", "--fail-percent", "5"
            )

        self.assertEqual(warned.returncode, 0, warned.stderr)
        self.assertIn("status=warn", warned.stdout)
        self.assertIn("warn_percent=5.000", warned.stdout)
        self.assertIn("fail_percent=50.000", warned.stdout)
        self.assertEqual(rejected.returncode, 2)
        self.assertIn("configuration_error=", rejected.stderr)

    def test_threshold_overrides_reject_non_finite_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_estimate(root / "bench" / "change" / "estimates.json", 0.50)

            nan_rejected = self.run_checker(
                root, "--warn-percent", "nan", "--fail-percent", "nan"
            )
            inf_rejected = self.run_checker(
                root, "--warn-percent", "inf", "--fail-percent", "inf"
            )

        self.assertEqual(nan_rejected.returncode, 2)
        self.assertIn("configuration_error=", nan_rejected.stderr)
        self.assertEqual(inf_rejected.returncode, 2)
        self.assertIn("configuration_error=", inf_rejected.stderr)


if __name__ == "__main__":
    unittest.main()
