#!/usr/bin/env python3
"""Fail closed on incomplete or self-skipped `postgres_store --nocapture` output.

Callers must also preserve Cargo's exit status (including through tee/pipefail).
This checks one complete, unfiltered target, including certificate store/API/watch
coverage; it does not make optional local workspace wrappers database executions.
"""

import argparse
from pathlib import Path
import re
import sys


SUMMARY = re.compile(
    r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; "
    r"(\d+) measured; (\d+) filtered out; finished in \d+(?:\.\d+)?s$",
    re.MULTILINE,
)
REQUIRED = (
    ("store conformance", r"postgres_store_conformance_against_real_database"),
    ("certificate store", r"certificates::postgres_\w+"),
    ("certificate API", r"certificates::api::postgres_\w+"),
    ("certificate watch", r"certificates::api::watch::postgres_\w+"),
)


def validate(output: str) -> int:
    if re.search(r"\bskip(?:ping|ped|s)?\b", output, re.IGNORECASE):
        raise ValueError("PostgreSQL target reported a skip")
    if re.search(r"^error(?:\[|:)", output, re.MULTILINE | re.IGNORECASE):
        raise ValueError("Cargo reported an error")
    summaries = list(SUMMARY.finditer(output))
    if len(summaries) != 1 or len(re.findall(r"^test result:", output, re.MULTILINE)) != 1:
        raise ValueError("expected exactly one well-formed test summary")
    status, passed, failed, ignored, measured, filtered = summaries[0].groups()
    passed, failed, ignored, measured, filtered = map(int, (passed, failed, ignored, measured, filtered))
    if status != "ok" or failed or ignored or measured or filtered:
        raise ValueError("target must pass with zero failed, ignored, measured, or filtered tests")
    announced = list(re.finditer(r"^running (\d+) tests?$", output, re.MULTILINE))
    if len(announced) != 1 or announced[0][1] != str(passed) or passed == 0:
        raise ValueError("nonzero running-test count must match passed count")
    results = list(re.finditer(r"^test (\S+) \.\.\. (.+)$", output, re.MULTILINE))
    if any(result[2] != "ok" for result in results):
        raise ValueError("test inventory contains a non-passing result")
    if any(not announced[0].end() < result.start() < summaries[0].start()
           for result in results):
        raise ValueError("test inventory is outside its running/summary boundaries")
    completed = [result[1] for result in results]
    if len(completed) != passed or len(set(completed)) != passed:
        raise ValueError("completed test inventory must match passed count without duplicates")
    for label, pattern in REQUIRED:
        if not any(re.fullmatch(pattern, name) for name in completed):
            raise ValueError(f"missing actual {label} test completion")
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    arguments = parser.parse_args()
    try:
        passed = validate(arguments.log.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        print(f"PostgreSQL execution check failed: {error}", file=sys.stderr)
        return 1
    print(f"Verified PostgreSQL target: {passed} passed, complete inventory.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
