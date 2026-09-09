# Phase 3 evidence: dynamic Frontline certificate cache

The implementation, focused tests and final evidence are independently approved.
The final workspace, actual PostgreSQL and rebuilt-process gates pass, and the
phase ledger is complete. This phase starts at
`4688c231faa43d1547c4d7a7d0e27504a6948abe`.

## Implementation and review

Frontline resolves certificates between ClientHello/SNI and Rustls configuration
selection. Its production static loader and configuration types are removed.
`crates/sleepypods-certificate` shares the unchanged maintained validator between
publication and installation without adding storage or Kubernetes to Frontline.
`rustls-pemfile` remains a dev dependency only for the load client's public CA.
Registry package versions, sources and checksums are unchanged.

One supervisor owns bounded fetch and refresh work. Hostname misses coalesce;
entry generations fence late responses after eviction/reinsertion. Warm selection
checks both the fixed monotonic lease and current whole-chain wall-clock validity.
The normal refresh interval is capped at half a short positive lease. A one-second
poll and bounded lookup make refresh best effort; failed refresh never extends
permission to serve.

The 64 MiB accounting budget reserves 1 MiB for persistent table/channel/task
structures, 1 KiB per hostname entry, 1 MiB per fetch and 128 KiB plus four times
bundle bytes per retained configuration. Entry count is capped at 1,024 and fetch
slots at 32; the supervisor also bounds completed but unreaped task handles.
A configuration's resolver retains its charge until the final owning reference
disappears, including after eviction. Rustls may release its configuration when
entering established traffic. Connection buffers and traffic secrets have a
separate connection admission envelope; accounted bytes are not a process RSS cap.

Server session storage, tickets and early data are disabled. Tests seed real old
TLS 1.2 sessions and TLS 1.3 tickets, verify the client retrieves them, and prove
that the dynamic listener requires a full handshake/current SNI authorization.
Authorized handshakes inspect exact peer DER and h2 ALPN; missing bindings fail.
The benchmark retains the original TLS 1.3 full-handshake workload and asserts
zero additional measured certificate RPCs. Its `--test` sanity pass is not a
latency result; three matched rounds and resource measurements remain Phase 5.

Independent review resolved due-list eviction/panic, retained task bounds,
forward-clock expiry and equal-view substitution cases. An unchanged response
accepts bounded sealing-only metadata changes, while rejecting different material
or invalid revisions. The short-lease worker test was captured failing under the
old refresh schedule and passing after the correction. Paused-clock helper and
session-cache fixture problems were corrected without attributing them to
production failures.

## Source and focused evidence

The frozen implementation packet is
`.generated/dynamic-certificates/phase3-implementation-return.json`. The reviewer
verified its 201 enumerated source hashes and all 12 check-log hashes:

`c8cd1174253c1955dd37e3f917bbb3acddb7da46989d5332e4a380130934f08c`

The packet records 58 focused executions: 14 cache/handshake cases, 8 existing TLS
cases, 25 listener cases, 8 configuration cases and 3 validator/sealer cases.
It also records strict affected-package/all-target Clippy, isolated load-example
and all-target compilation, formatting, dependency boundaries and benchmark
sanity. Prior checks retain their narrower source scope; the final gate below
reruns the whole workspace after the two-file short-lease correction.

Cache tests cover coalescing, unrelated progress, one/all waiter cancellation,
blocking validation ownership and shutdown joining, byte/entry/fetch/task bounds,
old-configuration accounting, eviction fencing, negative expiry, exact conditional
renewal, clock jumps, malformed/delayed responses and hard-lease outage behavior.
A real ClientHello consumes three seconds before lookup starts; the adapter still
expires at its original five-second setup deadline. Lookup's separate three-second
budget cannot restart that overall deadline.

The short-lease red/green logs are
`.generated/dynamic-certificates/phase3-short-lease-{red,green}.log`; the adjacent
red JSON and prior implementation packet preserve the original schedule identity.

## Final integration gate

All five checks passed at
`.generated/dynamic-certificates/phase3/gates-20260908T212808Z`, with the same root
aggregate before and after every check:

`4205782f9718821c8d91b6de5194a6876a027ba64aaff92b94220b2a2e6a13ea`

This root aggregate hashes sorted paths plus raw bytes for both root Cargo files
and all files under `crates`; the implementation packet uses file hashes and a
narrower enumerated source set. Their different digest values are intentional.

| Check | Result |
| --- | --- |
| `cargo fmt --all --check` | Pass |
| Strict workspace/all-target Clippy, locked/offline | Pass |
| Workspace tests, locked/offline | 46 targets, 871 reported passes: 852 local executions plus 19 database wrappers returning without a URL; 0 failed, 16 explicitly ignored |
| Actual PostgreSQL store target | 20 passed: 19 real database tests and one invalid-URL test; 0 failed/ignored/skipped; 66.12 seconds of test execution |
| Production dependency boundaries | Pass: Frontline 130, Sidecar 111, control plane 196 unique normal dependencies |

## Rebuilt process proof

The actual control-plane and Frontline binaries passed at
`.generated/dynamic-certificates/phase3/runtime-20260908T212957Z` on the same root
source aggregate. The operator client is [runtime-probe.rs](runtime-probe.rs);
the runner is `.generated/dynamic-certificates/run-frontline-runtime-probe.py`.
Exact binaries, client/runner sources and the standalone Cargo manifest/lock are
retained beside the record.

The probe used an isolated schema in the task-owned PostgreSQL container and
independently generated platform identity, sealing keys and application material.
A dummy kubeconfig caused no Kubernetes effects. It passed:

- Three startup rejections: plaintext certificate delivery, missing proxy
  credential and malformed HTTPS endpoint.
- Empty-cache TLS listener startup, server refusal before publication, and actual
  HTTP-01 success before any application certificate existed.
- API publication/binding followed by eight verified TLS 1.2/1.3 handshakes with
  exact fingerprint `d7a70b7609098449ccb77509f99ccdcc230b152913d02c092160bcec44abe7ae`
  and h2 ALPN. Reading server SETTINGS confirms both peers completed TLS without
  an application route/wake request.
- Unbound-host refusal, warm TLS while the control-plane process was stopped,
  and removal followed by refusal from a freshly restarted Frontline.
- Clean exits from both runs of each process, secret-marker scans, removal of the
  isolated schema and temporary keys, and unchanged source/client hashes.

Frontline received only the proxy credential and public platform trust. Its empty
working directory remained empty after both runs. This is a limited filesystem
observation; source inspection and Phase 5 image inspection supply the broader
no-key-file evidence. Negative clients disable hostname checking so a wrongly
served fallback cannot be masked by client hostname rejection; positive peers
perform normal trust and hostname verification.

This process probe does not establish deployed image behavior, real application
wake/sleep, watch convergence, offered-session behavior or the full five-minute
outage boundary. Those claims have separate tests. The deployed TLS, libpq and
load scripts still need API-seeded native TLS and removal of their old Frontline
application-key mounts under Phase 5A; they are not valid dynamic gates yet.

Subsequent Phase 4 review identified that bounded protobuf wire messages can
allocate substantially more decoded repeated-field storage than the Phase 3
one-MiB fetch reservation. Phase 4 tracks the adversarial decoder proof and
corrected reservations/concurrency. The earlier cache tests above do not prove
that malformed-wire allocation bound.

Earlier passing integration records at `gates-20260908T211925Z` and
`runtime-20260908T212253Z` precede the final short-lease correction. They are
retained as diagnostics and superseded by the final-source records above.

Hosted [CI run 34281390487](https://github.com/danthegoodman1/sleepypods/actions/runs/34281390487)
passed on committed Phase 3 source `9d0cfa946802e906ade77900bd9d49ab7bf1b7a4`.
The raw log is `.generated/dynamic-certificates/phase3/hosted-ci-34281390487.log`;
the actual PostgreSQL target reports 20 passed, zero failed/ignored, with no
self-skipped database cases.
