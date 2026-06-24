#!/usr/bin/env python3
"""Check Criterion benchmark results for configured mean-time regressions."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_ROOT = "target/criterion"
DEFAULT_WARN_PERCENT = 15.0
DEFAULT_FAIL_PERCENT = 25.0


@dataclass(frozen=True)
class BenchmarkResult:
    name: str
    status: str
    percent_change: float | None
    source: str
    reason: str


class ConfigError(ValueError):
    pass


class EstimateError(ValueError):
    pass


class MissingEstimateError(EstimateError):
    pass


class MalformedEstimateError(EstimateError):
    pass


def parse_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.lower() in {"1", "true", "yes", "on"}


def parse_percent(value: str, name: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a non-negative number, got {value}") from exc
    if not math.isfinite(parsed):
        raise ConfigError(f"{name} must be finite, got {value}")
    if parsed < 0:
        raise ConfigError(f"{name} must be non-negative, got {value}")
    return parsed


def load_json(path: Path) -> object:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise MissingEstimateError(f"missing {path}") from exc
    except json.JSONDecodeError as exc:
        raise MalformedEstimateError(f"malformed JSON in {path}: {exc}") from exc


def mean_point_estimate(path: Path) -> float:
    data = load_json(path)
    if not isinstance(data, dict):
        raise MalformedEstimateError(f"{path} must contain a JSON object")

    mean = data.get("mean")
    if not isinstance(mean, dict):
        raise MalformedEstimateError(f"{path} is missing mean estimate")

    estimate = mean.get("point_estimate")
    if not isinstance(estimate, (int, float)):
        raise MalformedEstimateError(
            f"{path} is missing numeric mean.point_estimate"
        )
    parsed = float(estimate)
    if not math.isfinite(parsed):
        raise MalformedEstimateError(
            f"{path} mean.point_estimate must be finite"
        )
    return parsed


def change_percent_from_artifacts(benchmark_dir: Path) -> tuple[float, str]:
    change_path = benchmark_dir / "change" / "estimates.json"
    if change_path.exists():
        return mean_point_estimate(change_path) * 100.0, "change"

    base_path = benchmark_dir / "base" / "estimates.json"
    new_path = benchmark_dir / "new" / "estimates.json"
    if not base_path.exists() or not new_path.exists():
        raise MissingEstimateError(
            "missing change/estimates.json and complete base/new estimates"
        )

    base = mean_point_estimate(base_path)
    new = mean_point_estimate(new_path)
    if base <= 0:
        raise MalformedEstimateError(
            f"{base_path} mean.point_estimate must be positive"
        )
    if new <= 0:
        raise MalformedEstimateError(
            f"{new_path} mean.point_estimate must be positive"
        )
    return ((new - base) / base) * 100.0, "base_new"


def discover_benchmark_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []

    benchmark_dirs: set[Path] = set()
    for path in root.rglob("estimates.json"):
        parent = path.parent
        if parent.name in {"base", "new", "change"}:
            benchmark_dirs.add(parent.parent)

    return sorted(benchmark_dirs, key=lambda path: path.relative_to(root).as_posix())


def classify(percent_change: float, warn_percent: float, fail_percent: float) -> str:
    if percent_change > fail_percent:
        return "fail"
    if percent_change > warn_percent:
        return "warn"
    return "ok"


def check_benchmark(
    root: Path,
    benchmark_dir: Path,
    warn_percent: float,
    fail_percent: float,
    allow_missing: bool,
) -> BenchmarkResult:
    name = benchmark_dir.relative_to(root).as_posix()
    try:
        percent_change, source = change_percent_from_artifacts(benchmark_dir)
    except EstimateError as exc:
        missing = isinstance(exc, MissingEstimateError)
        if missing and allow_missing:
            return BenchmarkResult(name, "ok", None, "missing", "missing_data")
        return BenchmarkResult(
            name,
            "fail",
            None,
            "missing" if missing else "malformed",
            "missing_data" if missing else "malformed_data",
        )

    status = classify(percent_change, warn_percent, fail_percent)
    if percent_change < 0:
        reason = "improvement"
    elif percent_change == 0:
        reason = "no_change"
    elif status == "ok":
        reason = "within_budget"
    elif status == "warn":
        reason = "warning_regression"
    else:
        reason = "failing_regression"
    return BenchmarkResult(name, status, percent_change, source, reason)


def format_result(
    result: BenchmarkResult, warn_percent: float, fail_percent: float
) -> str:
    fields = [
        f"benchmark={result.name}",
        f"status={result.status}",
        f"reason={result.reason}",
    ]
    if result.percent_change is not None:
        fields.append(f"change_percent={result.percent_change:.3f}")
    fields.extend(
        [
            f"warn_percent={warn_percent:.3f}",
            f"fail_percent={fail_percent:.3f}",
            f"source={result.source}",
        ]
    )
    return " ".join(fields)


def check_all(
    root: Path,
    warn_percent: float,
    fail_percent: float,
    allow_missing: bool,
) -> list[BenchmarkResult]:
    benchmark_dirs = discover_benchmark_dirs(root)
    if not benchmark_dirs:
        if allow_missing:
            return [
                BenchmarkResult(
                    root.as_posix(), "ok", None, "missing", "missing_data"
                )
            ]
        return [
            BenchmarkResult(
                root.as_posix(), "fail", None, "missing", "missing_data"
            )
        ]

    return [
        check_benchmark(root, benchmark_dir, warn_percent, fail_percent, allow_missing)
        for benchmark_dir in benchmark_dirs
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Check Criterion benchmark mean estimates for regressions. "
            "Positive change means slower/higher time."
        )
    )
    parser.add_argument(
        "--root",
        default=os.environ.get("SLEEPYPODS_CRITERION_ROOT", DEFAULT_ROOT),
        help="Criterion result root (default: target/criterion)",
    )
    parser.add_argument(
        "--warn-percent",
        default=os.environ.get(
            "SLEEPYPODS_BENCH_REGRESSION_WARN_PERCENT",
            str(DEFAULT_WARN_PERCENT),
        ),
        help="warn when regression is above this percent (default: 15)",
    )
    parser.add_argument(
        "--fail-percent",
        default=os.environ.get(
            "SLEEPYPODS_BENCH_REGRESSION_FAIL_PERCENT",
            str(DEFAULT_FAIL_PERCENT),
        ),
        help="fail when regression is above this percent (default: 25)",
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        default=parse_bool(os.environ.get("SLEEPYPODS_BENCH_REGRESSION_ALLOW_MISSING")),
        help="treat missing Criterion artifacts as ok with reason=missing_data",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        warn_percent = parse_percent(args.warn_percent, "warn percent")
        fail_percent = parse_percent(args.fail_percent, "fail percent")
        if fail_percent < warn_percent:
            raise ConfigError(
                "fail percent must be greater than or equal to warn percent"
            )
    except ConfigError as exc:
        print(f"configuration_error={exc}", file=sys.stderr)
        return 2

    results = check_all(
        Path(args.root), warn_percent, fail_percent, bool(args.allow_missing)
    )
    for result in results:
        print(format_result(result, warn_percent, fail_percent))

    return 1 if any(result.status == "fail" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
