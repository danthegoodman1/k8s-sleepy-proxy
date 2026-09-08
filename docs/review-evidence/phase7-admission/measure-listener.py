#!/usr/bin/env python3
"""Run already-built baseline/current actual sidecar listeners, without overlap."""
import os
from pathlib import Path
import subprocess
import time

EVIDENCE = Path(__file__).resolve().parent
ROOT = EVIDENCE.parents[2]
BINARIES = {
    "baseline": ROOT / ".generated/phase7-admission-baseline-target/release/examples/sidecar_admission_bench",
    "after": ROOT / "target/release/examples/sidecar_admission_bench",
}
ENV = {**os.environ, "BENCH_REQUESTS": "500000", "BENCH_CONCURRENCY": "16"}
for round_number, order in enumerate([("baseline", "after"), ("after", "baseline"), ("baseline", "after")], 1):
    for label in order:
        started = time.monotonic()
        print(f"starting round={round_number} label={label}", flush=True)
        with (EVIDENCE / f"listener-{label}-{round_number}.log").open("w") as output, (EVIDENCE / f"listener-{label}-{round_number}.stderr").open("w") as errors:
            result = subprocess.run([str(BINARIES[label])], cwd=ROOT, env=ENV, stdout=output, stderr=errors, timeout=300)
        print(f"completed round={round_number} label={label} exit={result.returncode} elapsed_s={time.monotonic()-started:.3f}", flush=True)
        result.check_returncode()
