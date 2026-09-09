# Phase 5 evidence: deployed lifecycle and resource bounds

Phase 5 starts at `5a337f3078e4ae424eafb590c4eb1f9b8a705bb3`. Its source,
deployed, resource and performance gates pass with independent evidence approval.
Phase 6 retains final integration, hosted CI and the fresh whole-feature review. Phase 4's
process-level results do not establish production-image, Kubernetes lifecycle,
default-lease outage or load behavior.

The three existing TLS/libpq/load fixtures use native verified
certificate delivery. Frontline receives a proxy credential and public platform
trust, with no application certificate/key mount. Platform control-plane TLS
identity and backend passthrough test keys remain separate infrastructure.

The coordinator owns the disposable `sleepypods-dynamic-certificates` kind cluster
and explicit task namespaces. Local PostgreSQL and cluster ownership records are
under `.generated/dynamic-certificates/`. Unrelated local fixtures are preserved.
No production cluster is used. Test keys must be removed during cleanup, while
non-secret peer, image and resource measurements remain available.

| Gate | Required evidence | Status |
| --- | --- | --- |
| Source and focused tests | Independent metrics approval: 23 cache tests and 22 vocabulary/exposition tests; basic fixtures and two test-only corrections approved; expanded driver and RSS method approved | Pass |
| Dynamic lifecycle | Exact two-CP/two-Frontline pod identities, verified fingerprints, HTTP-01/no-wake, actual wake/sleep/re-wake, live H2/WS rotation, invalid renewal and removal/rebind pass in expanded v3 | Pass |
| Outage and recovery | All old control planes terminated, missed update converges within 2.072s; default five-minute lease expiry, cold startup refusal and both-proxy recovery pass | Pass |
| Protocol preservation | Current-image dynamic TLS/exact-wildcard SNI, real libpq and expanded live H2/WS pass; independent routing/protocol/restart release scripts remain Phase 6 obligations | Pass within Phase 5 scope |
| Memory and concurrency | Three fixed cycles pass structural and sampled RSS limits; peaks 18.875/19.875 MiB, final-tail growth 2.875/0.875 MiB. Warm refusals under load are recorded below. | Pass |
| Matched TLS timing | Current runtime passes at median 220.161 versus immutable baseline 243.454 µs, -9.568%; unchanged warn 15% / fail 25% | Pass |
| Existing load budgets | Current production Frontline and native TLS fixture pass unchanged strict thresholds; 200 requests per path, concurrency eight | Pass |
| Closure | Independent review of actual final-source/image evidence and documented limits | Pass; final integration remains Phase 6 |

The local wrappers `run-recorded-gate.py`, `run-image-release-gate.py`
and `run-matched-tls-after.py` record exact commands, source identities and raw
outputs. They are coordinator evidence utilities, not a new runtime or repository
test framework. A retained production-input snapshot built the three runtime
images and passed the existing image smoke; expanded v3 passes the deployed and
resource gates with independent evidence approval. A Docker tag's
resolution after a run is recorded separately from the actual pod/container image
identity used during a test.

The reconnect correction's current production build is retained at
`phase5/image-build-20260909T021104Z/record.json`, covering 168 unchanged inputs
through the build. The unmodified packaging smoke passed in 63.43 seconds and
preserved these immutable IDs:

| Component | Current image ID | Size |
| --- | --- | --- |
| control-plane | `sha256:e60fcafcb3ffb69f1ce899f89d45de16f62fc5041828173ce71bb5ee6e2ab5ef` | 46,939,234 bytes |
| frontline | `sha256:f480b647c5edc7b41f6cced667e5f3fa5de513aec6fe5376539ad722af08efd7` | 39,991,842 bytes |
| sidecar | `sha256:36514d81b04a79e4dc109cb8bf0988fa8bff390feab9412f7f5802ea100818ea` | 38,943,202 bytes |

All three are ARM64, built with image Rust 1.98.0 and configured for UID/GID
65532. The expanded v3 lifecycle/RSS run uses these images. Earlier checkpoints
below retain their original image and behavioral scope.

The [watch-admission correction](watch-admission.md) is an independently reviewed
checkpoint within this phase. Its exact-source broad gate passes, including 24
actual PostgreSQL tests without skips or ignores, and hosted CI 34292414386
passes on `004505f`. This closes the correction separately from the remaining
production-image obligations above.

The metrics slice reuses existing ownership and fixed vocabulary: eight sampled
gauges and one counter with bounded operation/outcome labels. It adds no hostname
or certificate-ID label and no cached-handshake instrumentation. Local evidence
covers actual conditional `Unchanged`, processed reset, malformed response versus
installation outcome, task/watch destruction and secret-marker absence. This
establishes local instrumentation behavior, not RSS or production-image results.

## Production image checkpoint

Runtime inputs are retained in
`.generated/dynamic-certificates/phase5/image-build-20260908T234316Z/record.json`
and its source snapshot. Input hashes cover root Cargo manifests, the Dockerfile
and crate files except tests, examples, benchmarks and build output. All inputs
remained unchanged through the build. Deployed fixture drivers may continue to
change independently; final release must compare runtime inputs again.

| Image component | Immutable local image ID | Size |
| --- | --- | --- |
| control-plane | `sha256:2ebe1349d76d14d87c699cf4c359a896d74e009ceec2420ea4dedfa209882aaf` | 47,004,770 bytes |
| frontline | `sha256:880f5ab19cb67ee9c12193801e5bce656b93154d70646dd149e09ea83211a05e` | 39,991,842 bytes |
| sidecar | `sha256:ea4e823ca9d3e2ac9db156b86d79085dafecb829b88587ec7b24b3eb0386cedd` | 38,943,202 bytes |

All three are ARM64 and declare user `65532:65532`. The repository's unmodified
`scripts/smoke-images.sh` ran from this retained snapshot and passed in 63.62s:
declared 256 MiB size budgets, non-root identity, expected executable/CA files,
absence of a shell/package manager and expected startup failure behavior. Its
rebuilds preserved each exact image ID above. Raw smoke output and identity
comparison are in `smoke-images.log` and `smoke-images-record.json` beside the
build record. This proves packaging/startup behavior, not dynamic delivery.

## Basic fixture checkpoint

Independent review approved the nine-file native fixture conversion and retained
checks in `.generated/dynamic-certificates/phase5-basic-conversion/packet.json`.
The source snapshot overlays those exact files and the frozen runtime inputs on
`004505f`; `snapshot.json` records the identity. Scoped strict Clippy, formatting,
one actual native TLS load-helper test and shell trust/permission/role/public
artifact checks pass. The comparative load helper retains fake route/wake
semantics with real encrypted certificate delivery; it does not prove production
certificate publication. The first actual-image TLS attempt found trailing
newlines in generated role-token files: Kubernetes preserves those bytes in
Secret-backed environment values, and strict runtime authentication rejects them.
The startup gate failed before driver execution (zero tests) and cleaned its
owned namespace. Logs and exact unchanged image/source identities remain under
`phase5-basic-conversion/deployed-tls/`. The fixture-only correction emits exactly
64 ASCII hexadecimal bytes for each role and has independently reviewed raw-byte
red/green proof. A second run verified the published TLS peer, then exposed a
fixture public-CA comparison that depended on the shell's removal of a terminal
PEM newline. The final comparison checks ordered nonempty DER identity, with
reviewed red/green coverage for formatting, changed certificates and malformed/
empty trust. Native endpoint and other environment checks remain exact. Runtime
inputs and images remain unchanged.

The retained `source-ca-fix/` snapshot applies exactly those two corrections to
the basic conversion; `snapshot-ca-fix.json`, `token-fix.json` and
`ca-identity-fix.json` identify the sources and focused evidence. Actual-image
reruns now pass:

| Gate | Actual result |
| --- | --- |
| `test-kind-e2e-tls.sh` | 2 passed, 0 failed/ignored, no self-skips; 92.91s script execution. API-published termination peer verified with TLS 1.3/HTTP 1.1; exact and wildcard SNI passthrough verified. |
| `test-kind-e2e-libpq-sni.sh` | Operator fixture 1/1 plus real in-cluster libpq positive and negative jobs; 88.42s script execution. A direct verified SNI query wakes the Cold PostgreSQL workload and returns the expected marker; unbound SNI rejects. |

Raw logs, actual control-plane/Frontline pod UID/container/image identities and
successful namespace cleanup are retained under `deployed-tls-ca-fix/` and
`deployed-libpq-sni-ca-fix/`. These running image IDs match the recorded builds,
and snapshot sources remained unchanged. Sidecar evidence here establishes the
loaded image/tag identity and workload specification assertion; it does not
retain a running sidecar container's image ID. The expanded gate must capture
that stronger identity observation. Declaring two replicas in the TLS fixture
is not proof of both-peer coverage; the expanded driver targets each actual pod.

## Resource acceptance declared before measurement

The fixed workload uses three cycles. Each exact Frontline pod receives a
ten-second unique-SNI flood with at most fifteen miss lanes and one measured warm
lane; a bounded API rotation occurs during each cycle. Thirty-two verified warm
handshakes and a seven-second quiescent tail follow. Two separated metric samples
must show zero fetches, queued work and retained tasks; an established watch may
remain. Attempts, successful/rejected/timed-out results and latency denominators
are recorded separately.

The independent reviewer approved a workload-specific sampled RSS guard: all
cycle peaks at most 256 MiB per pod, and the final quiescent median no more than
8 MiB above the preceding cycle's median. Cycle one remains included in the peak
check. Each tail is exactly seven seconds from its start, even if observing final
recovery takes longer. It requires at least five valid samples spanning five
seconds, with no gap above two seconds including boundaries. Every per-pod flood
through recovery interval also requires samples with no gap above two seconds.
Ten synthetic analyzer checks include missing flood samples with intact tails
and delayed recovery with low RSS that must not rescue a failing fixed tail.
Structural cache/work limits
are checked independently. The one-second sampler reads Linux `/proc/PID/status`
and verifies exact pod/container/PID identity through the cgroup; accounted cache
bytes remain a separate measure. These are sampled observations for this bounded
workload, not allocator peak measurements or a general bound for arbitrary
production traffic. Raw predeclared criteria are retained in
`.generated/dynamic-certificates/phase5/resource-criteria.json`; measured resource
results and matched latency are recorded separately below.

The expanded source packet and focused logs are retained in
`.generated/dynamic-certificates/phase5-expanded/review-source-packet.json`:
four local Rust tests pass, three deployed cases are explicitly ignored locally,
three Python WebSocket fixture tests pass, and strict scoped Clippy/fmt pass.
Review required an active rotating warm hostname during pressure, refusal checks
that reject network stalls and client validation errors, a pinned cold-restart
process deadline, and narrow fixture-owned Kubernetes patches. All four fixes
are approved. The backend fixture image was rebuilt as
`sha256:4858d8094964fe0322b3b565477b6f4cd58ec70b94ba0e1c1252b2114aeef020`;
that fixture build preserved the three production runtime images and their
168 inputs. The subsequent workspace assertion correction is described below.

## Workspace integration corrections

The first full workspace attempt found stale shared-target observability
metadata. The exact cached metadata rejected a direct symbol probe even though
the declarations existed in source; narrowly cleaning that package made the same
compile and probe pass. All source hashes, sizes and modification times remained
unchanged. The retained diagnosis establishes the artifact mismatch, without
attributing it to a particular earlier snapshot build.

The next workspace run exposed an old proxy-core assertion containing only the
18 pre-certificate operation labels. Its expected list now also includes the
five reviewed certificate operations. The focused assertion passes; the diff is
entirely within `cfg(test)` and the runtime prefix is byte-identical. Raw evidence
is retained in `phase5-cargo-artifact-diagnostic/` and
`phase5-label-expectation/` under `.generated/dynamic-certificates/`.

The corrected full gate `phase5/gates-20260909T005412Z/record.json` passes with
the same source digest before and after all five checks: formatting, strict
workspace/all-target Clippy, workspace tests, the PostgreSQL target and dependency
boundaries. Workspace results contain 46 summaries, 894 reported passes, no
failures and 17 explicit deployed ignores; subtracting 23 optional database
self-returns gives 871 actual local executions. The actual PostgreSQL target
passes 24 cases (23 database cases and one invalid-URL case), with no skips or
ignores, in 66.75s test time. Refreshed build record
`phase5/image-build-20260909T005753Z/record.json` covers all 168 current inputs
and reproduces the same three immutable image IDs, preserving applicability of
their recorded packaging and basic deployment checks.

## Expanded execution

The first expanded run, `phase5/expanded-v1-20260909T005849Z`, failed on an
HTTP/2 request returning 502 instead of 200. The two basic TLS/SNI cases passed,
followed by exact two-CP/two-Frontline identity capture, HTTP-01 without app wake,
verified TLS 1.2/1.3 peers on both proxies, first HTTP wakes and actual running
sidecar UID/container/image IDs. The live-connection phase then failed before
rotation, resource measurements or outage tests. The source remained unchanged,
the sampler exited successfully, and the owned namespace was deleted. The raw
log, public checkpoint events and independent sampler data remain retained;
these observations do not establish a passing expanded or RSS gate.

The correction reuses the existing `kind_protocol_app` for dynamic workloads and
removes the additional Python WebSocket implementation. Basic TLS/passthrough
continues to use the original Python image. Independent review verified the
eight source hashes and seven logs in `phase5-h2-fixture/review.json`: direct H2,
two production proxy hops and exact WebSocket responses pass two local tests;
the TLS helper target passes four local tests with three explicit deployed
ignores. Both scoped strict Clippy checks, formatting and shell syntax pass.
The production inputs are unchanged. Direct exact-pod backend H2 and request
version assertions now precede the live-connection rotation check. The controlled
Python preface probe returns 505; missing deployed wire logs limit attribution of
the first run's 502.

The corrected run `phase5/expanded-v2-20260909T011556Z` passed the direct
exact-pod HTTP/2 probe and live H2/WebSocket rotation on the unchanged production
images. It also passed invalid rotation, high-to-low certificate-version rebind,
unbind/removal and denial of offered TLS 1.2/1.3 sessions. After terminating all
old control planes, it proved an empty delivery service and publication of an
unreceived update while proxies still served their retained valid view.
Convergence then timed out after restoration. The original ten-second verdict
remains failed; the script exited in 94.16s with unchanged source, successful
sampler shutdown and namespace cleanup. Resource, sleep-floor and default-lease
outage checks were not reached. A bounded diagnostic is investigating service
readiness, transport and watch recovery before any acceptance change.

The first focused diagnostic (`phase5-reconnect-diagnostic/results.json`) also
failed the original ten-second window and retained a fixed 25-second observation
tail. Both new control-plane EndpointSlice targets were Ready by the first
post-restoration sample, but every independent native RPC probe through the
actual Service IP timed out. Both proxies retained the valid old peer and kept
retrying watches. Later controls, about eight minutes after restoration, passed
against both exact control-plane pods and the Service path; the same proxies had
recovered. These retrospective controls establish eventual recovery, not timely
Service availability. A second bounded diagnostic separates contemporaneous TCP,
TLS, connection and RPC outcomes before selecting a correction.

The second run (`phase5-reconnect-diagnostic/v2/run.json`) failed the original
window again. Contemporaneous probes from the relay pod completed verified TLS
and received HTTP/2 SETTINGS through the Service IP, with the first host-observed
completion at 3.532 seconds after restoration and further completions at 5.819
and 8.323 seconds. Direct native reads
against an exact control-plane pod returned binding revision 11 in 6–12 ms.
Both proxies still retained the old peer. The host-to-relay port forward refused
connections, so that probe cannot measure Service availability; the independent
in-cluster probes supply the positive control.

Source review found that the shared native endpoint configures neither a TCP
connection timeout nor a TLS handshake timeout. Tonic retains a pending
connection attempt in the shared channel after an individual caller times out.
The correction sets separate two-second TCP and TLS handshake deadlines. Its
regression first completes a verified native RPC, disconnects that physical
connection, then holds the next TLS handshake while canceling the caller. The
same channel must close the held socket, open another connection and complete a
native RPC without recreating the client. Old source fails after 4.08 seconds;
the corrected source passes in 2.03 seconds. Together with existing transport
checks, 49 focused tests pass; scoped strict Clippy and formatting also pass.
`phase5-native-reconnect/review.json` retains the two source hashes, eight
evidence hashes and the one-file production delta. The deployed result alone
does not identify the stalled
transport stage, and the per-stage limits are not a combined two-second RPC
deadline or a guarantee that operating-system DNS work stops on cancellation.

The full runtime checkpoint `phase5/gates-20260909T020344Z` subsequently passed
formatting, strict workspace/all-target Clippy, workspace tests, real PostgreSQL
and dependency boundaries at one unchanged source digest. Independent review
verified 47 workspace summaries: 872 actual local executions plus 23 optional
database self-returns, zero failures and 17 explicit ignores. The separate real
PostgreSQL target passed all 24 cases with no skips or ignores in 67.08 seconds.
The strengthened cancellation assertion owns one demonstrably pending RPC. The
identical final test fails against the isolated old transport after 4.08 seconds
and passes with the fix in 2.03 seconds; focused strict Clippy and formatting
pass. Independent final source/local approval verified the artifact hashes,
identical old/current test and unchanged production code since the broad gate.
The rebuilt-image lifecycle and resource results follow.

## Complete deployed lifecycle and resource result

`phase5/expanded-v3-20260909T021448Z/record.json` records three actual targeted
deployed tests passing, with no failures, ignores or self-skips. The script took
669.04 seconds; the expanded dynamic test took 600.68 seconds. Its Cargo,
Dockerfile, crate, script and workflow inputs remained unchanged. The combined
record under `phase5/expanded-v3-combined-20260909T021447Z/` retains the source
archive, separate RSS stream and successful analysis. Public event and metric
artifacts are in `phase5/expanded-v3-public-20260909T021448Z/`.

Both exact Frontlines verify TLS 1.2/1.3 peers after publication while the
applications remain Cold. HTTP-01 also leaves generation zero. The first HTTP requests wake
both routed applications; running sidecar container IDs match the current image
above. Direct backend HTTP/2 succeeds before live H2/WebSocket connections survive
rotation. Invalid renewal preserves the original peer; rebind to a lower version
on another certificate, unbind, removal and actually offered old TLS sessions all
produce the required peer or refusal.

After all old control planes terminate and their Service is empty, publication
through an exact new control-plane pod leaves the proxies on their retained valid
view. Restoring delivery converges both within 2.072 seconds under the original
ten-second limit. Leaf expiry is refused with client-side time checking disabled.
The real activation floor elapses before both applications become Cold at
generation four; one-shot requests wake them to generation six with the retained
certificate. A later complete control-plane outage makes a new empty-cache proxy
remain unready and exit at the actual sixty-second startup deadline. Warm lease
refusal observation starts at 301 seconds and completes at 303.044 seconds. Both
warm and restarted proxies recover after control-plane restoration.

Each of three cycles runs one ten-second flood against each exact Frontline,
bounded to fifteen miss lanes and one warm lane. Across all six waves, 5,418 warm
attempts comprise 5,376 successful handshakes and 42 refusals, with no timeouts;
900 unique-SNI misses are all refused. Each wave records 896 warm successes and
seven refusals. These results establish continued warm progress during the flood,
not rejection-free service under that workload; the cause of the refusals has not
been attributed. All 192 post-flood warm handshakes succeed without additional
certificate fetches. Each cycle observes the newly rotated peer, and two separated
recovery samples show zero fetches, queued work and retained tasks.

The sampler verifies the same pod UID, container, PID, image and cgroup through
all resource cycles. Pod peaks are 18.875 and 19.875 MiB against the predeclared
256 MiB cap. Final quiescent medians grow 2.875 and 0.875 MiB from the preceding
cycle, below eight MiB. All six fixed seven-second tails contain seven samples
spanning more than six seconds; workload and tail gaps remain below 1.072 seconds.
This passes the workload-specific sampled RSS criteria. It does not bound
unsampled allocator peaks or arbitrary production connection counts. The owned
namespace is cleaned after the run, and sampler and analyzer both exit zero.

The separate current-image libpq renewal
`phase5/libpq-current-20260909T023635Z/record.json` passes in 30.33 seconds with
unchanged inputs. One native operator fixture executes, followed by actual
in-cluster libpq positive/negative jobs: verified SNI wakes the Cold PostgreSQL
workload and returns the expected marker, while an unbound hostname rejects.
Actual control-plane and Frontline container image IDs match the current build;
the script cleans its namespace. This refreshes the earlier basic libpq result
without expanding its Sidecar identity scope.

## Strict comparative load

`phase5/strict-frontline-load-20260909T023325Z/record.json` records a successful
7.62s run with unchanged source and all original strict thresholds. Each measured
protocol path uses 200 requests and concurrency eight; WebSocket sessions echo
262,144 bytes each. Every measured hot route makes zero additional
`SubscribeRoute` calls. The separate fake-control-plane cold wake makes exactly
one route lookup, wake call and backend request, completing in 1.283 ms against
the unchanged 250 ms budget.

| Path | Proxied/direct throughput ratio | Required ratio | Added p99 |
| --- | --- | --- | --- |
| HTTP/1 | 0.932 | 0.80 | 0.244 ms |
| h2c gRPC-shaped | 0.956 | 0.75 | 0.125 ms |
| TLS h2 gRPC-shaped | 0.861 | 0.75 | -0.236 ms |
| Generated gRPC | 1.077 | 0.75 | -2.052 ms |
| WebSocket sessions and bytes | 0.977 | 0.80 | 2.577 ms |

Every added-p99 budget remains 25 ms. Image records identify the unchanged
production Frontline `f480b647…` and helper `41c5f7c…`, built from the retained
source in `phase5/load-helper-build-20260909T022631Z/`. The helper uses native
verified TLS for certificate delivery with fake routing/wake responses; this
comparative gate does not establish production publication, database storage or
Kubernetes materialization performance. Matched handshake and sustained RSS
results are separate gates.

## Matched TLS handshake timing

`phase5/tls-after-20260909T023412Z/record.json` retains the final-source executable,
source, commands and three independent raw Criterion directories. The round
means are 220.161, 219.915 and 220.472 µs. Their median is 220.161 µs versus the
preserved Phase 1 median of 243.454 µs, a 9.568% reduction, passing the unchanged
15% warning and 25% failure thresholds.

Both measurements use Rust 1.98.1 on the same macOS 26.6.2 ARM64 machine, P-256,
verified TLS 1.3 full handshakes with `h2`, one connection at a time and a 64 KiB
in-memory transport. Each round retains 100 samples, two seconds of warmup and
ten seconds of measurement. Certificate generation/validation and cache warmup
remain outside timing, and the harness asserts no extra certificate RPCs during
warm handshakes. Source stayed unchanged through compilation and all rounds;
other build/load jobs were paused. This proves the matched handshake regression
gate, not deployed network throughput or a general latency improvement.
