# Final dynamic certificate integration

Phase 6 starts at `e1db93d3ce75ecabcef3483131a52ae00112428a`. Final integration
and the fresh independent whole-feature review have passed their local, release
and hosted gates. The
[Phase 5 results](../phase5/README.md) establish the deployed certificate lifecycle
and measured workload limits for their recorded source and image identities.
Any subsequent runtime correction must identify and rerun its affected gates.

| Gate | Required scope | Status |
| --- | --- | --- |
| CI execution | Shared actual-PostgreSQL skip/count rejection, meaningful checker regressions, explicit protocol example tests and isolated load-helper checks | Source and focused checks approved |
| Operator contract | Current guide, runbook, store contract and configuration describe dynamic-only application TLS and independent platform trust | Independent current-doc approval |
| Fresh skeptical review | Entire feature from merged `456d37e`; all three findings resolved with paired TLS regression and 37 focused passing tests | Source and focused evidence approved |
| Final local checks | Formatting, strict all-target Clippy, workspace, actual PostgreSQL, dependency/inventory/checker gates and protocol example tests | All twelve pass; independently verified |
| Final release checks | Production images, dynamic TLS lifecycle, routing, protocols, libpq and restart; exact running image identities and applicable performance gates | All pass; independently approved |
| Hosted checks | Successful required CI on the final implementation commit | `15a308e` passes hosted CI 34308484699 |
| Completion | Approved final evidence and docs, reviewable commits, clean branch and cleanup of owned fixtures | Independent full-feature closure approval; owned fixtures removed; documentation-only closure commit |

The final review keeps correctness fences that prevent stale certificate
installation, ambiguous database writes or premature capacity release. It
challenges duplicate state, unused abstractions and unbounded work before adding
new machinery. Existing established connections and the five-minute maximum
authorization lease retain their documented behavior unless a reviewed finding
requires an explicit contract change.

The four-file CI slice has independent source and evidence approval. Its nine
checker tests exercise the real CLI and shell composition, including mixed
pass/skip output, missing or malformed summaries, zero/ignored/filtered execution,
inconsistent test inventory, preservation of Cargo's failure status and cleanup
of only the owned temporary log. The protocol example executes both tests. The
actual PostgreSQL target passes 24 cases (23 database cases and one invalid-URL
case) with no failures, skips, ignores or filters in 66.86 seconds; the shared
checker accepts that complete inventory. Exact source and artifact hashes are
retained in `.generated/dynamic-certificates/phase6-ci/review.json`.

The CI checkpoint `0e77279308ad889f0828bd453865c635deec6ac7` passes
[hosted CI 34305141692](https://github.com/danthegoodman1/sleepypods/actions/runs/34305141692).
The hosted run executes all nine checker tests, both protocol example tests and
all 24 PostgreSQL target cases, followed by explicit complete-inventory acceptance;
the database target takes 72.65 seconds. Raw log and exact-SHA metadata remain
under `phase6-ci/hosted-ci-34305141692.{log,json}`. The final runtime/API changes
require their own later hosted checkpoint.

The fresh review identified a narrow native TLS deadline gap: the accepted
socket's absolute setup timer is checked on reads, while progressing writes
reset an inactivity timer. A stalled or trickling TLS server flight can therefore
avoid the absolute read-side check. The existing socket cap still bounds
concurrent ownership, and silent clients remain read-bounded. The correction
applies the existing configured setup timeout to the complete TLS handshake;
it introduces no new setting or admission framework. An unused local watch cursor
and the watch request's discarded client revision are removed. Registration now
contains only its ID and exact hostname list. Per-message validation, full atomic
snapshots, local per-host floors/incarnations, reset/reconnect behavior and unary
conditional revisions remain intact. Conservative memory reservations are unchanged.

The fresh reviewer approved the final source and focused evidence in
`.generated/dynamic-certificates/phase6-runtime/review.json`, independently
verifying 762 source/artifact hash checks. The frozen aggregate covers 274 source
inputs at `b20528871f402fb49bc40e9ab7a1389e329ee0f8c156132f1d1d375c06f7ce92`.
Two retained 227-file trees differ only by the outer TLS timeout and use the
identical final regression. A real ClientHello fills a 64-byte test transport;
the peer receives one byte every ten milliseconds while server writes progress
without another read poll. The fixed version releases its owned capacity after
the configured hundred-millisecond deadline. Without the deadline, the precise
release barrier fails its two-second guard. A separate actual TCP test checks
silent-peer expiry, socket capacity recovery and a verified native RPC.

All 37 focused cases pass: 33 local and four actual PostgreSQL watch cases, with
zero failures or ignores. Malformed/maximum-valid watch encoding, producer
validation, cache/fence/reset behavior and native load-fixture requests remain
covered. Strict affected all-target Clippy and formatting pass. The finite-input
fixture debugging failure is retained separately and is not attributed to the
product. Whole-feature completion still requires the final integration gates.

The review retained explicit per-host revisions and incarnations because they
reject stale results after invalidation or eviction. Database mutation ordering
and retained hostname revisions prevent rebind/removal races. Bounded worker and
SQL-drain ownership prevent cancellation from releasing capacity while work is
still retained. These mechanisms implement tested guarantees; the discarded
watch revision and local cursor provided no such behavior.

`phase6/gates-20260909T030609Z/{record,summary}.json` and twelve raw logs retain
the full local checkpoint at the same frozen source digest. Independent review
verified every source/log hash and all counts. Formatting and strict workspace
all-target Clippy pass. The workspace reports 897 successes: 874 actual local
executions and 23 optional database self-returns, with zero failures and 17
explicit ignores (16 deployed/Kubernetes tests and one subprocess fixture). The actual repository PostgreSQL script separately
executes 24 cases (23 database cases plus invalid URL), with no skips, ignores or
filters, in 65.80 test seconds; the shared checker accepts all unique completions.
Dependency boundaries, six inventory tests, nine checker tests, two protocol
example tests, both isolated load-helper checks, ten load-budget cases and
thirteen Criterion-checker tests also pass. Rust is 1.98.1 on local ARM64 macOS.

The retained production build `phase6/image-build-20260909T031132Z/record.json`
freezes 169 production inputs. The unmodified image smoke passes in 63.41 seconds
and preserves all three image IDs. Builder and runtime base-image tags remain
unchanged through the build; the builder's immutable image ID matches the
recorded Rust 1.98.0 toolchain.

| Component | Final image ID | Bytes |
| --- | --- | --- |
| control-plane | `sha256:a5f9b5a0ca48d040dcfe8ccf51a61332367acfcc274d9ca5083579f3e2c952fa` | 46,939,234 |
| frontline | `sha256:54c5e71b4f47a00cd9aebbdf0eecf4b693118fdab06a63e0203e3a899d742fba` | 40,057,378 |
| sidecar | `sha256:d57a01ebaafbf6dd6f61e15dbf8425a147b757feda3cd4d4f29d2ed1aa2d2c72` | 38,943,202 |

Each image is ARM64 and runs as UID/GID 65532. Packaging verifies the existing
256 MiB image cap, executable and CA contents, no shell/package manager, and the
expected startup failures. Deployed behavior is verified separately below.

The final expanded image gate passes in 649.39 seconds. Its three selected actual
test bodies take 2.98, 8.77 and 578.15 seconds, with no failures, ignores or
self-skips. Retained records are under
`phase6/expanded-final-20260909T031627Z`,
`phase6/expanded-final-public-20260909T031627Z`, and
`phase6/expanded-final-combined-20260909T031626Z`. The deployed runner hashes
275 inputs, including `Dockerfile`, at
`d8d4b8d405918278177f72d9c02cfff69abb130c924d8e1418fb983f94b26188`;
its common 274 inputs match the local checkpoint above. Before/after manifests
are unchanged, and actual running control-plane, Frontline and Sidecar image
identities match the production build.

The two-control-plane/two-Frontline lifecycle covers pre-publication HTTP-01
without an app wake, verified TLS 1.2/1.3 and h2, publication and first-request
wake, rotation across live h2/WebSocket sessions, invalid replacement preservation,
rebind/unbind/removal, and offered-session denial after removal. Terminating all
old control planes and publishing an unreceived version through a replacement
checks retained valid service and convergence after delivery recovers. Certificate
expiry is enforced by the proxy while client time verification is disabled. An
actual sleep transition and subsequent wake retain certificate delivery.

During the final outage, all control-plane containers terminate and the Service
has no ready endpoint. The warm proxy denies new handshakes after the unchanged
five-minute lease; the observation begins at 301 seconds and completes at
303.825 seconds. The separately restarted empty-cache proxy remains unready and
its first container exits at the existing 60-second startup bound. Both warm and
cold proxies recover after control-plane restoration. This does not imply app
routing remains available throughout an outage or existing connections are revoked.

All three resource cycles pass the original structural and sampled RSS criteria.
The two exact Frontline process/container identities peak at 17,506,304 and
17,326,080 bytes, below 256 MiB. Final post-drain median growth is 786,432 and
1,310,720 bytes, below the unchanged 8 MiB limit. Each of the six seven-second
tails contains seven samples; workload/tail gaps remain at most 1,057 ms.
These are sampled bounds for this workload, not unsampled production peak claims.

The six flood waves record 5,379 successful warm handshakes out of 5,421 attempts,
42 refusals (seven per wave), and no timeouts. All 900 unknown-host attempts are
refused, and all 192 post-wave warm attempts succeed without another certificate
fetch. The measured progress and recovery criteria pass; the structured warm
refusals are retained as a limitation, with their cause unproved rather than
attributed to noise or claimed to be absent. Certificate rotation continues
through every wave.

The strict comparative load gate
`phase6/strict-frontline-load-20260909T033218Z` passes in 8.97 seconds using the
verified production Frontline image and separately built native TLS control-plane
fixture. Each path runs 200 requests at concurrency eight, with zero failures;
WebSocket sessions exchange 262,144 bytes each. All five measured hot-cache route
RPC deltas are zero. Original throughput and added-p99 budgets remain enforced.

| Path | Proxy/direct throughput | Added p99 (ms) |
| --- | --- | --- |
| HTTP/1 | 0.991 | -0.133 |
| h2c gRPC | 0.882 | 0.460 |
| h2 TLS gRPC | 0.841 | 0.377 |
| Generated gRPC | 1.025 | -0.277 |
| WebSocket | 1.009 | -0.126 |

The fake-control-plane cold wake takes 1.379 ms, with one subscription, one wake
and one backend request, below the existing 250 ms budget. These comparative
results do not establish a real PostgreSQL/Kubernetes cold-wake SLO.

The final warm TLS comparison `phase6/tls-after-20260909T033244Z` preserves the
original Phase 1 baseline record (SHA-256 `382ac7938b1f73c0214bbc81be19a1d63e70b9d23c2d0ef708d6f823641e6833`).
Three quiet rounds on the same ARM64 macOS/Rust 1.98.1 host measure 221.562,
220.388 and 220.550 microseconds. The median is 220.550 microseconds versus the
original 243.454, a 9.41% reduction; the original 15% warning/25% failure budgets
pass. The frozen current binary is `9bd1fdab492689ff80646bd8625749c86da485033eab4eb3db69db003ff6f9a8`.
Both sides use verified P-256, full TLS 1.3, h2, one connection on a current-thread
runtime, a 64 KiB duplex transport, 100 samples, two-second warmup and ten-second
measurement. Certificate construction/validation and the sole initial fetch are
outside timing; the fetch count stays one. This measures the warm handshake path,
not cold certificate resolution or network latency.

The unmodified deployed routing gate
`phase6/routing-final-20260909T033448Z` passes its actual routing/HTTP-01 test in
33.17 seconds (88.42 seconds including build/deploy/cleanup), without skips,
ignores or filters. The separate read-only identity sampler
`phase6/identities-routing-final-20260909T033448Z` confirms all three running
production components match the final image IDs above; input hashes stay unchanged.

The unmodified protocol gate `phase6/protocols-final-20260909T033647Z` passes its
actual HTTP/2/gRPC/WebSocket test in 9.28 seconds (66.84 seconds overall), without
skips, ignores or filters. `phase6/identities-protocols-final-20260909T033647Z`
confirms all three actual running production components use the same final IDs;
source inputs remain unchanged and the sampler reports no errors.

The unmodified restart gate `phase6/restart-final-20260909T033825Z` passes its
actual deployed test in 303.45 seconds (358.91 seconds overall). It covers wake,
sleep, accepted deletion, HTTP-01 expiry/update and route-reassignment recovery
across control-plane restarts. The existing activation floor and deadlines remain
unchanged; route reassignment converges through notification in 6.06 ms in this
run. There are no failures, ignores or self-skips. The separate local framed-HTTP
regression is intentionally filtered by `--ignored` here and already passes in
the workspace gate. `phase6/identities-restart-final-20260909T033825Z` verifies
all observed running production image IDs, including restarted control planes;
inputs stay unchanged and the sampler reports no errors.

The real libpq gate `phase6/libpq-final-20260909T034458Z` passes in 32.36 seconds.
Its operator fixture executes one actual case, then an in-cluster `psql` job uses
verified direct TLS/SNI to make one connection through a cold workload and read
the expected database marker. A separate unbound-host connection is refused.
Recorded running control-plane and Frontline IDs match the final build; the
PostgreSQL fixture image matches the frozen fixture build. Sidecar template checks
here are not substituted for the actual Sidecar identities captured by the other
final deployed gates. Source hashes remain unchanged.

The normal reviewer and the fresh skeptical reviewer independently approve the
completed source, local, image, certificate lifecycle/RSS, performance, routing,
protocol, restart, libpq and current-documentation evidence for commit. No
actionable finding remains. Hosted CI is successful. Both reviewers approve Phase 6 and implementation
closure, including the current documentation and owned fixture cleanup.

Final implementation commit `15a308ea880d8130ac94912952e71a557bcdd482` contains the
reviewed corrections and the same source tested above. The owned fixture cleanup
completes at 2026-09-09 03:48:18 UTC: the recorded kind node and PostgreSQL
container are removed after identity/ownership checks. All three unrelated local
containers retain their IDs and prior running/stopped states. The cleanup record
is `phase6/final-cleanup.json`; retained public evidence and the original TLS
baseline remain available.

The final implementation passes
[hosted CI 34308484699](https://github.com/danthegoodman1/sleepypods/actions/runs/34308484699)
on exact commit `15a308ea880d8130ac94912952e71a557bcdd482`. Formatting, strict
all-target lint, the workspace, dependency/inventory checks, all nine PostgreSQL
checker regressions, both protocol example tests and isolated load-helper builds
pass. The real PostgreSQL target executes all 24 cases in 72.61 seconds, with
zero failures, skips, ignores or filters and explicit complete-inventory
acceptance. Raw metadata/logs and the extracted PostgreSQL log/check result are
retained as `phase6/hosted-ci-34308484699*`. The final documentation closure does
not change the source or images used by any gate above.
