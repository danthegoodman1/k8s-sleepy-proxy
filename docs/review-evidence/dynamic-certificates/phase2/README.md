# Phase 2: Secure Certificate API Evidence

Status: source/focused review is approved, and the frozen broad and actual
runtime gates pass. Dynamic Frontline caching and watch delivery remain later
phases.

## Final phase gate

Executed on Rust 1.98.1 on 2026-09-08. Cargo/crate source remained identical
through all commands and the actual process run, with SHA-256
`11b5cbe40eeab56abda7a5d01b0c3566326d06a0a35d705bc52bfc14af99b682` using the
same aggregate algorithm as Phase 1. The implementer's separate 205-file hash
manifest and 14 focused check records are retained in
`.generated/dynamic-certificates/phase2-implementation-return.json`; its 67
focused executions include three actual PostgreSQL/TLS cases.

| Command | Result |
| --- | --- |
| `cargo fmt --all --check` | Pass |
| `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` | Pass |
| `cargo test --workspace --locked --offline` | 44 targets; 862 reported passed (843 executions, 19 database self-returns), 0 failed, 16 ignored; 121.32 seconds |
| `SLEEPYPODS_POSTGRES_URL=… cargo test -p control-plane --test postgres_store --locked --offline -- --nocapture` | 20 passed (19 real database cases and one invalid-URL case), 0 failed, 0 ignored; no self-skips; 67.77 seconds test time |
| `python3 scripts/test-dependency-boundaries.py` | Pass: Frontline 112, Sidecar 111, control plane 195 normal dependencies |
| `bash -n scripts/test-kind-e2e-stateless.sh` | Pass; syntax only |

Raw broad-gate records are under
`.generated/dynamic-certificates/phase2/gates-20260908T203453Z`. The workspace
command deliberately omits the database URL; only the separate real PostgreSQL
run supports database claims. Ignored deployed tests remain for later phases.
The only new dependency package is `webpki-roots` 1.0.9; existing package versions
and checksums are unchanged. Added features supply verified native TLS and
zeroizing sealing-file parsing; `tokio-rustls` is also a control-plane test edge.

The final rebuilt process run is
`.generated/dynamic-certificates/phase2/runtime-20260908T203807Z`. It repeated the
positive matrix below, rejected four invalid startup configurations (no-auth
with sealing, missing identity key, no reserved database capacity, malformed
sealing file), and verified that malformed-file contents and private-material
encodings never appeared in logs. Source and client hashes stayed unchanged;
the process exited on SIGTERM with code zero, and all temporary keys and the
owned schema were removed. Original binaries, client source, runner, manifest
and lockfile are retained beside the logs and record.

- Control-plane binary SHA-256: `6ee333d04743457526c900c97e97a644e32bc8cfe9054629e6b5d7e1217318f7`
- Native probe binary SHA-256: `07fa693d76abc64c2dd9178252e304ab2c7183a36631252236d98b2822761b42`
- Client source SHA-256: `bd4a4a322f4f19465d6794518ea50410b028c63d0f9b07f3bff3d2191399c75c`
- Resolved certificate fingerprint: `6ea825eacc4c8dd8c428d2c391c2c8357cbf6f1d9aac1f7286c3d7aea84f117b`

## Actual runtime checkpoint

On 2026-09-08, the coordinator built the production `control-plane` binary and a
standalone native client using Rust 1.98.1. The client's resolved dependencies
match the workspace lockfile, apart from its own temporary package. It runs
outside the production workspace and introduces no production dependency.

The process probe generated a private platform CA, issued a native server
identity, wrote a temporary external sealing-key file, and started the real
binary against an isolated schema in the task-owned PostgreSQL 17 container.
It used a dummy Kubernetes client configuration and created no workloads.

The checkpoint passed proxy-authorized HTTP-01 before certificate publication,
the operator/proxy/sidecar credential boundary, missing/wrong credentials,
publication and binding, exact DER/key resolution, conditional `Unchanged`, stale
CAS, and removal followed by a higher-revision `Missing`. Wrong server names and
untrusted roots failed TLS setup. The process exited successfully on SIGTERM;
private-material marker scans passed and the schema and temporary keys were
removed.

Raw output and binary hashes are under
`.generated/dynamic-certificates/phase2/runtime-20260908T200817Z`. The runner is
`.generated/dynamic-certificates/run-runtime-probe.py`; the standalone client is
`/private/tmp/sleepypods-certificate-runtime-probe-20260908`. This first run
precedes final source review and is superseded by the frozen-source run above.
Continued API availability across the setup deadline is not proof of reusing one
TCP connection: Tonic channels can reconnect. Dedicated transport tests own that
stronger assertion. Deployed images and dynamic Frontline handshakes belong to
later phases.

## Cancellation regression

Independent review identified a sent-query cancellation problem beyond blocking
crypto: dropping a request future could return a pooled PostgreSQL client while
its query remained outstanding. A real PostgreSQL test holds the exact
certificate writer lock, observes the blocked query, cancels its caller, and
attempts another certificate operation. The original implementation admitted
that operation and blocked, instead of rejecting it at the certificate limit.
The controlled failure is retained as
`.generated/dynamic-certificates/phase2-sql-cancel-red.log`.

The correction retains the certificate permit and checked-out client through a
protocol drain, including rollback queued by a dropped transaction. A failed,
timed-out or canceled drain discards the session instead of recycling it. This
bounds locally admitted work and protects session reuse; it does not claim that
closing a transport instantly cancels a remote PostgreSQL command. Independent
review approved both the normal cancellation and held-lock timeout/discard tests;
both pass in the final real PostgreSQL suite. An initial incorrectly filtered
timeout-test command ran zero tests and was not counted as validation.

The real same-peer TLS/H2 test separately proves that native requests mark setup
progress through Tonic's TLS connection wrapper, that a healthy connection
survives its setup deadline, and that withheld response credit triggers bounded
delivery cancellation and capacity recovery for unary and route Subscribe calls.
Actual TLS/PG tests prove role, trust, plaintext/no-auth and encrypted gRPC-Web
denial, oversized input rejection, and decryption errors remaining errors.
HTTP-01 evidence combines the actual proxy client, existing runtime interception
tests with zero route/wake calls, and real PostgreSQL hit/expiry/error behavior.
Recursive protobuf Debug tests cover enclosing publication and Found responses.

## Hosted validation

Commit `4688c231faa43d1547c4d7a7d0e27504a6948abe` passed
[CI run 34276472052](https://github.com/danthegoodman1/sleepypods/actions/runs/34276472052),
including formatting, strict Clippy, workspace tests, production dependency
boundaries, inventory tests, isolated load-helper checks and actual PostgreSQL.
The raw log is retained at
`.generated/dynamic-certificates/phase2/hosted-ci-34276472052.log`.
