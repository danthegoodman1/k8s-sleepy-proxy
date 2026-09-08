# Rust 1.98 CI lint repair

The initial PR and push runs on `fadbe7f` failed at strict Clippy under Rust
1.98.1 (`48a229cea`). The previous local validation used Rust 1.97.0. The
[original diagnostics](original-diagnostics.log) retain the two new findings;
full hosted logs remain in the [PR run](https://github.com/danthegoodman1/sleepypods/actions/runs/34249877866)
and [push run](https://github.com/danthegoodman1/sleepypods/actions/runs/34249833316).

`WakeInstanceError` stores its owned `InstanceRecord` payload behind a `Box`,
keeping the error result small enough for `result_large_err`. Allocation happens
only in error constructors. Success variants, lifecycle ordering, RPC responses,
error messages and source chains are unchanged. This changes the public Rust
enum field representation, but not its protobuf/wire contract.

The sidecar HTTP/2 settings test uses fixed-array chunk containment. It checks
the same six-byte setting and retains the previous handling of any remainder.
The workflow keeps warnings denied and follows stable Rust; neither a lint
suppression nor an older compiler pin is introduced.

The final validation uses the user's upgraded local Rust 1.98.1, matching the
exact version installed by CI. Local and hosted follow-up results are recorded
separately; the original production validation keeps its recorded revision and
compiler scope.

The [local check record](checks.json) passes formatting, strict all-target
Clippy, the full workspace and production dependency boundaries. The source
digest is `f0754362ff1d95ed158edc45e9dab8a44763644156a4a0f76d082c12066c1e7a`
before and after every command. Cargo reports 840 passes, zero failures and 16
explicit ignores across 44 targets. Eleven database-only wrappers self-return
without a configured database, leaving 829 local executions; hosted CI runs its
separate real-Postgres gate. [Compiler metadata](local-metadata.json) also records
the six passing fail-closed inventory checks and independent source approval.

Both [isolated-package load examples](isolated-examples.json) also compile with
locked dependencies under the same Rust 1.98.1 compiler.

No deployed image, Kubernetes or performance run is relabeled by this repair.
Those historical results retain the scopes in the original validation packet.
Follow-up hosted status is reported on the pull request for its exact head.

Both follow-up hosted Clippy gates passed. The push run then exposed a separate
PostgreSQL observation failure while the PR run passed; the
[supersession fixture correction](../ci-postgres-handoff/README.md) records that
diagnosis, controlled regression and follow-up validation.
