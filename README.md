# SleepyPods

SleepyPods is a Rust control plane and proxy system that materializes Kubernetes
workloads on demand. The review and remediation plan tracks current implementation
evidence and remaining production gates.

Start here:

- [Production north star](docs/production-north-star.md)
- [Development plan](docs/development-plan.md)
- [Codebase review and remediation plan](docs/review-remediation-plan.md)
- [Operator guide](docs/operator-guide.md)
- [Operator runbook](docs/operator-runbook.md)
- [Contributor and agent guide](docs/contributor-guide.md)
- [Proxy hot-path budgets](docs/proxy-hot-path-budgets.md)

The old Go, DigitalOcean, Terraform, and demo-script implementation has been
removed from the `north-star` branch so SleepyPods can start from a clean
Rust/local development baseline.
