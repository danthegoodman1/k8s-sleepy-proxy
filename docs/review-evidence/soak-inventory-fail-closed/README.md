# Soak inventory: fail closed on discovery errors

The existing soak executed its two stateless and two stateful bodies successfully,
but source review found that each of four leak-discovery queries suppressed
stderr and used `|| true`. A failed empty read could therefore satisfy the absence
check. This is a harness correctness finding; it does not establish that any
particular earlier inventory failed or that the deployed workload leaked.

The exact four scoped reads now live in
`scripts/lib/full-wake-sleep-inventory.sh`. Each preserves kubectl stderr and
returns its failure status immediately. Polling reports an incomplete inventory
and fails on any read error. Only a complete successful nonempty inventory is
retried, using the existing two-second interval and existing timeout. An empty
inventory succeeds only after all four reads succeed. The soak's two-plus-two
workload, selectors, namespace/configuration choices, cleanup, timing settings,
and other assertions are unchanged; `soak-only.patch` and the exact-scope check
record that boundary. No production source, image, Docker or Kubernetes state was
changed for this correction, and the full soak was not rerun.

The callable reader is read-only and takes an explicit kubeconfig path. It prints
observed object names and preserves a discovery failure's nonzero status:

```bash
/bin/bash scripts/lib/full-wake-sleep-inventory.sh \
  .generated/implementation-evidence/kubeconfig
```

For the coordinator's final single-pass supplemental check, this invocation also
fails on nonempty inventory (zero polling budget, no repeated workload):

```bash
/bin/bash -euo pipefail -c '
  source scripts/lib/full-wake-sleep-inventory.sh
  wait_for_full_wake_sleep_no_leaks "$1" "final supplemental inventory" 0
' inventory-check .generated/implementation-evidence/kubeconfig
```

The supplemental check must be run after the coordinator's active kind gates;
this implementation lane has not run either command against the cluster.

[Mocked kubectl tests](mocked-tests.log) passed all six cases, exit 0, using the
real shell functions under `/bin/bash -euo pipefail`. Cases cover empty success
only after all four queries, every failure position with original error/status,
nonempty reporting and deadline failure, successful nonempty-to-empty polling,
and immediate or later polling discovery failures. Every mock request checks the
exact resource/selector arguments and explicit kubeconfig. The harness uses only
Python's standard library and a task-local executable stub; no real kubectl runs.
[Command and completed exit](mocked-tests.json) are retained.

[Syntax/source checks](syntax-source.json) record Bash syntax for both scripts,
scoped diff whitespace validation, source SHA256 values, and the passed assertion
that only the old reader/poller block changed in the soak. All commands exited 0.
The existing CI Rust job now runs the inexpensive mocked inventory command next
to dependency-boundary checks. The contributor command table lists the same
cluster-free gate. [CI/source checks](ci-source-checks.json) record successful
YAML parsing and rust-job step validation, scoped diff checks, and exact-delta
assertions for just one added CI step and one command row. `ci-only.patch` and
`contributor-only.patch` retain those additions. No repeated Rust tests were
needed for these shell/CI/documentation changes.

The skeptical reviewer independently ran all six mocked tests successfully and
explicitly approved the shell/test packet, then verified and approved the final
CI/documentation additions. Actual supplemental inventory remains coordinator-owned
and separate from these local checks.
