#!/usr/bin/env python3
"""Check production crate boundaries without including dev-only test services."""
import subprocess
import sys

BOUNDARIES = {
    "frontline": {"control-plane", "kube", "kube-core", "kube-client", "k8s-openapi", "deadpool-postgres", "tokio-postgres", "tonic-web", "axum"},
    "sidecar": {"control-plane", "kube", "kube-core", "kube-client", "k8s-openapi", "deadpool-postgres", "tokio-postgres", "tonic-web", "axum"},
    "control-plane": {"proxy-core", "frontline", "sidecar"},
}


def main():
    failed = False
    for package, forbidden in BOUNDARIES.items():
        result = subprocess.run(
            ["cargo", "tree", "--offline", "-p", package, "--edges", "normal", "--prefix", "none", "--format", "{p}"],
            check=True, text=True, stdout=subprocess.PIPE,
        )
        dependencies = {line.split()[0] for line in result.stdout.splitlines() if line}
        violations = sorted(dependencies & forbidden)
        if violations:
            print(f"FAIL {package}: forbidden normal dependencies: {', '.join(violations)}", file=sys.stderr)
            failed = True
        else:
            print(f"PASS {package}: {len(dependencies) - 1} unique normal dependencies; boundary intact")
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
