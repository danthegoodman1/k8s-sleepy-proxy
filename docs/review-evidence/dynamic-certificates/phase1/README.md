# Phase 1: Dynamic Certificate Storage Evidence

Status: source, focused regressions and the broad workspace/PostgreSQL gates
pass. The TLS baseline is complete and has its own limited scope; runtime dynamic
delivery belongs to later phases.

## Final phase gate

Executed on Rust 1.98.1 on 2026-09-08. The final gate runner retained identical
Cargo/crate source hashes before and after every command:
`c0f656770d4c599386206809ccc3dbc3ebd29fe1ed315d2e875038e08e15ba43`.
Records and command output are under
`.generated/dynamic-certificates/phase1/gates-20260908T194753Z`.

| Command | Result |
| --- | --- |
| `cargo fmt --all --check` | Pass |
| `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` | Pass |
| `cargo test --workspace --locked --offline` | 44 targets; 852 reported passed (836 executions, 16 database self-returns), 0 failed, 16 ignored; 90.15 seconds |
| `SLEEPYPODS_POSTGRES_URL=… cargo test -p control-plane --test postgres_store --locked --offline -- --nocapture` | 17 passed (16 real database cases and one invalid-URL case), 0 failed, 0 ignored; no self-skips; 64.87 seconds test time |
| `python3 scripts/test-dependency-boundaries.py` | Pass: Frontline 111, Sidecar 103, control plane 194 normal dependencies |

The workspace command deliberately omits the database URL, so its count includes
database wrappers that return early. The separately executed PostgreSQL command
is the database evidence and rejects any skip output. Ignored deployed/image
tests remain for later phases. The four new certificate cases run against the
task-owned PostgreSQL 17 container `a6c50eed5695`, not an in-memory substitute.
The dependency lockfile adds edges to already locked packages; no package version
or checksum changed. Tokio's `test-util` is enabled only for control-plane tests.

## Resource and transaction coverage

The implementation and independent review cover exact canonical DNS bindings,
bounded bundles, key/chain/hostname validation, effective whole-chain validity,
authenticated encrypted key envelopes, and conditional resource mutations.
Private issuance is supported; accepting a supplied terminal trust anchor does
not make public-CA trust a publication requirement.

Four actual PostgreSQL certificate cases exercise publication, rotation,
unbinding/rebinding, permanent certificate deletion identity, atomic reads across
independent pools, binding/rotation/re-encryption races, commit-ordered events,
rollback, retention gaps, the 1,024-binding limit and response loss without
blindly replaying writes. Metadata reads avoid selecting private material. Key
re-encryption uses its own conditional revision and does not alter proxy views.

The reviewer required additional coverage for a hostname rebound to a different
certificate with a smaller certificate version, full fanout revisions, and
explicit non-NULL active-material constraints. Those corrections are included.

## Lifecycle deadline correction found by the broad gate

The first full gate (`gates-20260908T192047Z`) passed formatting, strict workspace
Clippy and workspace tests, but the actual PostgreSQL suite reported 15 passed
and one failed. All four certificate cases passed. The existing lifecycle
scheduler case expected a durable deadline failure and observed a transient
failure. Its original uninstrumented cause is unknown: ten isolated repeats and
six runs paired with heavy database work passed.

Investigation produced a deterministic, separate correctness regression:
PostgreSQL could evaluate the deadline predicate before a row-lock wait and
publish that stale classification after the deadline. A barrier test failed on
the original implementation and passed after checking a fresh database timestamp
following the lock. The worker now also derives its operation budget from the
database's remaining duration, subtracting monotonic time measured from before
the status request. It no longer subtracts a process wall clock from a database
timestamp. An early conservative local cancellation may remain transient until
the database deadline; local timers do not force a terminal database state.

The old scheduler fixture now proves readiness entry and read cancellation,
holds failure publication through the original database deadline, and retains
the terminal/no-effect assertions. A paused-clock test exercises the actual
reconciler across delayed status responses; moving the timer anchor after the
await makes that regression test fail. The restored implementation passes all
31 reconciler tests and strict scoped Clippy. These narrow changes and the
contract wording received explicit independent approval. Raw diagnostic logs,
the controlled failing variants and source identities are retained under
`.generated/dynamic-certificates/phase1/deadline-diagnostic`.

## Warm TLS baseline

Captured 2026-09-08 on macOS 26.6.2 arm64 using Rust 1.98.1, from merged main
`456d37ef09dbb3ef71b29473dd33873d1c509b98` plus the new benchmark harness and
dependency edges for Phase 1. The application TLS lookup implementation was
unchanged. No concurrent workspace builds ran during measurement.

Compile command:

```sh
cargo bench -p frontline --bench tls_handshake --no-run --locked --offline
```

The resulting executable was copied to the ignored local artifact directory
`.generated/dynamic-certificates/phase1/tls-baseline/tls_handshake-before` and run
three times with `--bench --save-baseline before-r1` (then `before-r2`,
`before-r3`). Each round uses the same workload documented in
[`proxy-hot-path-budgets.md`](../../../proxy-hot-path-budgets.md): TLS 1.3,
P-256, no resumption, warm SNI, verified peer certificate and `h2` ALPN, one
connection over a 64 KiB duplex transport and one response byte. Each round
contains 100 Criterion samples, 2 seconds warmup and 10 seconds measurement.

| Round | Mean handshake duration |
| --- | ---: |
| before-r1 | 242.629 µs |
| before-r2 | 244.951 µs |
| before-r3 | 243.454 µs |
| Median of round means | 243.454 µs |

All handshakes verified the peer certificate, full-handshake kind, ALPN and
response byte. The original executable, raw Criterion estimates/samples, logs
and machine/source metadata remain in the ignored artifact directory. Baseline
sources did not change during measurement.

After measurement, `rustfmt --edition 2021` only wrapped the benchmark's
`TlsConnector::connect` arguments onto separate lines. The exact as-compiled
source is retained as `tls_handshake-before.rs` beside the original executable;
its hash matches the compilation record. The formatted tracked source has
SHA-256 `4fa85859ffcde8981a18b3d68a6a0227c80921491251a8b4721a52a32761ab15`.

SHA-256 provenance:

- Application TLS implementation: `03d4fea743e7b9393fb8acc8ede7e23fa1d7fcec01c730d99681c47a308b275a`
- Benchmark source at compilation: `54f22862209a610670d7e0b8875ad7940e0ee3e705d1746a987be1c74413abf9`
- Preserved executable: `5cec7e6ab9a0da6674558dae0c6bbfc6d976031c297a71e6f68c345e32df115a`

The final implementation must repeat three matched rounds, prove zero additional
certificate RPCs during warm measurement, and compare the median of round means.
The frozen thresholds warn above 15% regression and fail above 25%. Deployed
throughput, resumption correctness and cache RSS need separate evidence.

## Actual database restart

The [restart probe](restart-probe.rs) uses the production `PostgresStore` and
certificate sealer. It publishes a generated certificate, binds an exact hostname
and resolves/decrypts it. A fresh process then verifies the same fingerprint,
certificate version (1) and hostname view revision (2) after PostgreSQL restarts.
The standalone probe's resolved dependency versions match the workspace lockfile;
its temporary Cargo package is outside the production workspace.

The task-owned PostgreSQL 17 container `a6c50eed5695` was restarted using
`docker restart --timeout 5`. Its start timestamp changed from
`2026-09-08T19:13:41.379740338Z` to `2026-09-08T19:15:17.498198924Z`. The probe
then passed with fingerprint
`c8d1217fd2a93bfff6ccdc165669804c89b23d0050ff15c253a8256cc8183c70` and successful
decryption/key/chain validation. The isolated schema was
`certificate_restart_probe_20260908`.

The initial verification attempt failed to connect because Docker reassigned
the ephemeral host port at restart. The corrected runner discovers the new
mapping before connecting; this infrastructure failure was not a certificate
resolution result. The successful run, commands, original executable, source,
lockfile and metadata are retained in
`.generated/dynamic-certificates/phase1/db-restart`.
