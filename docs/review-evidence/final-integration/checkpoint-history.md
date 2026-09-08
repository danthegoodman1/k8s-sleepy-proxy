# Final integration evidence

Coordinator-owned integration ledger. Overall implementation and production
gates remain in progress; this file records only completed checks.

## Final handoff and connection revision

The [6J activation and handoff implementation](../phase6-idle-activation/README.md),
its separate lifecycle fixture packet, and the [6K shared connector](../phase6-sni-connect/6k-implementation.md)
are integrated and explicitly approved by independent review. Merged data-plane
coverage passes 410 tests, strict clippy and formatting. The production source
digest is `3656d037660aec14c7e9e26126542b167db72c69ecc649a0f293916c595c20fa`.

The final [image build and smoke gate](6jk-images.log) passes; its
[metadata](6jk-images.json) records unchanged production source. The separately
approved test-fixture integration changed the wider workspace hash during the
build. [Exact Linux arm64 identities](6jk-image-identities.json) record all three
images below the unchanged 256 MiB limit. Final deployed, workspace and quiet
performance gates are running. Earlier checkpoints below retain both successes
and failures; they do not substitute for the final revision's remaining gates.

### Final checks completed so far

Formatting, strict workspace clippy, workspace tests and dependency boundaries
pass at stable source. Cargo reports 817 passes and 16 ignored entries: nine
reported passes are database wrappers that return early without a configured
server, so 808 local tests actually execute. The separate final [real-Postgres run](6jk-postgres.log) passes all ten entries: nine actual database bodies and one local invalid-URL validation. [Command/source record](6jk-postgres.json) and [full verification inventory](6jk-verification-summary.json) retain exact scope and clean disposable-container teardown. [Exact commands and provenance](6jk-workspace-checks.json),
[per-suite counts](6jk-workspace-suite-counts.json), [test output](6jk-workspace-workspace.log)
and [clippy](6jk-workspace-clippy.log) distinguish these results.

Final cold [H2/gRPC/WebSocket](6jk-protocols.log), [TLS termination and SNI](6jk-tls.log),
and [actual libpq direct-SNI](6jk-libpq-sni.log) gates pass. The libpq gate also
rejects an unbound SNI host. All have unchanged source during execution:
[protocol metadata](6jk-protocols.json), [TLS metadata](6jk-tls.json),
[libpq metadata](6jk-libpq-sni.json).

The following [failure-path gate](6jk-failures-before.log) stopped at
`PV ownership does not match accepted wake generation` after 33.97 seconds of
driver execution ([stable source record](6jk-failures-before.json)). This led to
an independently approved [fixture correction](../failure-projection-fixture/README.md): accepted Waking revision and immutable projection generation are distinct. The real-render regression fails with the old expectation and passes the corrected exact ownership checks. The deployed rerun remains required.

The first final [restart gate](6jk-restart-before.log) progressed through wake, sleep, deletion and HTTP-01 recovery, then failed its final reassignment check at `initial cache warmup exceeded total 1s fixture budget` ([source record](6jk-restart-before.json)). Production source was unchanged; the unrelated failure-path fixture changed the wider source hash during this run. Its pre-TTL timing proof is being reviewed separately; later gates in that batch did not run.

The final [projection-drift and finalizer gate](6jk-projection-drift.log) passes
in 45.17 seconds of driver execution. It preserves a same-name unowned Service,
keeps accepted cleanup blocked by an owned StatefulSet finalizer, and completes
after the test removes that finalizer. [Metadata](6jk-projection-drift.json)
records unchanged production source; an unrelated resource-measurement example
changed the wider workspace hash.

The first final [lifecycle-race gate](6jk-lifecycle-races-before.log) passed
concurrent-wake and idle-report-during-Waking checks, then failed accepted deletion
during Waking: the instance remained Deleting after the 60s fixture deadline.
[Source](6jk-lifecycle-races-before.json) was unchanged. Actual-Postgres regressions now prove stale Pending renewal and late failure publication across accepted Delete; the narrowly scoped 6L implementation is underway. The original deployed status was not captured, so its exact cause remains unproved. Later scenarios and gates in that batch did not run.

The final exclusivity script also timed out in a shared deletion helper. Its
[preserved investigation](../exclusivity-investigation/README.md) distinguishes
the ambiguous original log from an unchanged successful frozen-image repeat.
The later snapshot shows ordinary transient cleanup retry and eventual absence
of all instances, effect barriers and owned objects. No timeout increase or
production correction is attributed to that repeat; the original-script final
gate still needs to pass.

The independently approved [route resource experiment](../phase4-resource-envelope/README.md)
passes all predeclared admission, task recovery and RSS gates. Across ten measured
cycles, all 40 recoveries return to exactly two tasks and zero live ownership;
quiescent RSS tail growth is 592KiB and span is 960KiB. The packet explicitly limits
this to the development-profile listener/coordinator miss path with no-op telemetry
and in-process clients. It supplements the structural and protocol tests; it is
not a production memory SLA or a throughput result.

The corrected failure-path gate progressed past projection ownership but then
received HTTP 200 in its supposedly unbindable-PVC scenario ([log](6jk-binding-before.log),
[stable source](6jk-binding-before.json)). A preserved frozen-image repeat
reproduced it. Captured Kubernetes state shows both objects Bound: the PV has
1Mi capacity, the PVC requests 2Mi but reports 1Mi capacity, and an
`ExternalExpanding` event precedes Pod creation. This is a false fixture
nonbindability assumption; the production materializer does wait for Bound.
The [corrected Pending-claim fixture](../failure-binding-diagnostic/README.md) now passes the entire unchanged failure driver in a fresh add-only test environment: 1 actual test, 66.66s. It proves a current-UID FailedBinding event, one HTTP503, Failed instance, no backend and successful retained-inventory cleanup. Setup/teardown leaves the earlier diagnostic resources untouched. The final fixture/evidence packet also has explicit independent approval.

The final 6JK [materializer](6jk-materializer.log), [gRPC-Web](6jk-grpc-web.log)
and [three-component metrics](6jk-metrics.log) gates pass, with stable source in
all three records ([materializer](6jk-materializer.json), [gRPC-Web](6jk-grpc-web.json),
[metrics](6jk-metrics.json)). The materializer executes both replacement and
stateful-volume/finalizer tests in 39.66s.

The final [routing attempt](6jk-routing-before.log) proves natural HTTP-01 expiry
but then fails a manual-expiry count assertion that races automatic maintenance
([source](6jk-routing-before.json)). Its deterministic manual-expiry fixture is
being corrected without disabling maintenance or changing production TTLs.

The new 6L database diagnostic also proves a polling self-deadlock: an effect
transaction waits for the client to send COMMIT while inline heartbeat renewal
waits for that transaction's row lock. The worker future cannot advance until
the inline renewal finishes. An owned renewal future polled beside work is being
implemented and independently gated. Because this affects ordinary lifecycle
progress, the full repeated soak will run on the final merged revision.

## Earlier startup and stateless checkpoint

The independently approved [startup and teardown fix](../phase6-startup/README.md)
is now in all three production images. The [image gate](6i-images.log) passes,
with unchanged source during the build ([metadata](6i-images.json)). Exact
[Linux arm64 image identities](6i-final-image-identities.json) record control-plane
43.83 MiB, frontline 37.39 MiB and sidecar 35.89 MiB, all below the unchanged
256 MiB budget. Production source digest is
`f7ebfbdc76722a98f22d20ee59d4e42b774f51d4d1c21b4fab55e406d38a91dc`.

The integrated workspace passes formatting, strict all-target clippy, dependency
boundaries and 791 reported tests, with 16 ignored fixture/child entry points.
The real-Postgres bodies still require their separately recorded actual database
runs. [Commands and exits](6i-workspace-checks.json), [workspace output](6i-workspace.log),
[clippy](6i-clippy.log), and [count/provenance supplement](6i-workspace-summary.json)
retain this checkpoint. Subsequent stateless fixture changes have their own
focused checks; this is not an assertion that later source was tested here.

A dedicated metrics scrape exposed a test that sent only one hot request and
then waited for a cache hit under metrics-only traffic; its [failed run](6i-stateless-cache-observation-before.log)
is retained. The independently approved [hot-traffic correction](../phase6-cold-readiness/stateless-hot-traffic.md)
passes the full frozen-image stateless driver in 54.05 seconds. Two successful
hot requests produce a new hit in 120 ms with no additional Subscribe call.
The run covers initial one-request cold delivery, layout, ownership and idle
cleanup. The subsequently approved [one-request re-wake fixture](../phase6-cold-readiness/stateless-one-shot-rewake.md)
now also passes in the [full deployed gate](6i-stateless.log), with
[stable source metadata](6i-stateless.json). The separate earlier diagnostic
single re-wake 502 is retained in the [idle activation investigation](../phase6-idle-activation/README.md).
Three captured natural follow-ups passed, but did not recover that exact error.
Independent source review confirms an activation-to-first-traffic idle ownership
gap; its explicit bounded contract and correction were still in progress at this checkpoint. The final 6J revision above now has independent approval.

The final 6I [cold HTTP/2, gRPC and WebSocket gate](6i-protocols.log)
also passes: one deployed test in 9.65 seconds, with no caller retries
and [unchanged source](6i-protocols.json). Remaining deployed gates are running.

The current [stateful lifecycle gate](6i-stateful.log) passes cold write, warm
read, idle cleanup and retained-data re-wake in 115.63 seconds of driver time
([stable source metadata](6i-stateful.json)). This run executes one actual
stateful body; the two reported exclusivity/drift entries self-skip because their
separate gate flags are absent. The re-wake reader still permits retries in this
checkpoint; its stronger one-request follow-up remains required.

The deployed [routing and HTTP-01 gate](6i-routing.log) passes in 41.64 seconds
of driver execution ([stable source metadata](6i-routing.json)). The following
[TLS gate](6i-tls-sni-eof-before.log) passes TLS termination in 2.88 seconds, then
fails its one-request SNI passthrough case with `UnexpectedEof` after 3.79 seconds.
This stops the batch; later gates did not run. The [record](6i-tls-sni-eof-before.json)
has stable production source; only the unrelated stateful fixture changed during
the run. The exact initial SNI failure was unrecovered. The separately captured TCP stall and controlled refusal regression led to the approved final 6K revision above.

Three independent integration gates now pass: [materializer](6i-materializer.log)
(two actual tests, 24.66 seconds), [browser-shaped gRPC-Web](6i-grpc-web.log), and
[dedicated metrics on all components](6i-metrics.log).
Their [materializer](6i-materializer.json), [gRPC-Web](6i-grpc-web.json) and
[metrics](6i-metrics.json) records have stable hashes. The crate-source hash
`791af3995a096286463547b10cfe94f706638c570d97c7bd5c08dd2532f9d113`
differs from 6I because it also covers a new, intentionally red SNI unit-test
module under `src`; released production behavior remains unchanged at this
checkpoint. The final 6K connector fix above makes that regression pass; this earlier checkpoint predates it.

## Earlier reviewed readiness revision

All three readiness subphases and the final lifecycle fixture packet received
explicit independent approval. [Implementation and focused evidence](../phase6-cold-readiness/README.md)
include 380 passing data-plane tests, independently rerun 59 renderer tests,
10 actual connector tests, and eight sidecar supervision tests.

The [rebuilt image gate](6h-images.log) passes startup, runtime-file, non-root and
unchanged 256 MiB size budgets. Control-plane is 43.83 MiB, frontline 37.33 MiB,
and sidecar 35.89 MiB. [Image identities](6h-final-image-identities.json) and
[binary hashes](6h-binary-provenance.json) identify the Linux arm64 artifacts;
all three binaries differ from the earlier checkpoint. The
[source record](6h-images.json) shows unchanged production digest
`9e978892892feba5c5868182015377b8f6a6093265f0e896682952ea46f6d44e`.
Only unrelated lifecycle test/script edits overlapped the image build.

The previously failing one-request cold HTTP/2, gRPC and WebSocket gate now
passes in 8.72 seconds of test execution, with no caller retries:
[deployed protocol log](6h-protocols.log), [unchanged source record](6h-protocols.json).
Remaining deployed and refreshed performance gates are still in progress.

The following stateless run passed cold routing/layout/hot routing, then failed
with a socket timeout while scraping metrics ([original log](6h-stateless-socket-timeout-before.log)).
The retained reproduction received the entire advertised HTTP response but the
fixture waited for port-forward EOF. Its [framing correction and diagnosis](../phase6-cold-readiness/stateless-metrics-framing.md)
have independent approval and four passing regressions, preserving the timeout,
metric assertions and single cold request. The current startup/stateless checkpoint above records the approved follow-up
and passing deployed fixture. The separately observed one-request re-wake failure
remains open.

## Full Criterion comparisons

2026-09-07, same quiet host, precompiled baseline/current binaries, 40 samples,
one-second warmup and three-second measurement per case. All other agent builds
and tests were paused. Production baseline Rust, manifests and lockfile are
byte-identical to dc7cac2; only benchmark harness additions differ. The route
port is retained in [baseline-route-harness.patch](baseline-route-harness.patch).
Raw Criterion estimates remain in the ignored local implementation-evidence
directory; logs and executable hashes are retained here.

All 26 comparisons pass the existing 25% failure threshold. One exceeds the
unchanged 15% warning threshold:

| Comparison | Result |
| --- | --- |
| All 10 proxy primitives | Pass without warnings; largest increase is idle-aware TCP at 10.703% |
| All 16 route/cache cases | Pass; 15 improve, one warning |
| Full hot route resolution, observations disabled | 206.972 ns current mean; 29.234% improvement |
| Full route resolution during 640 unrelated updates/s | 191.437 ns current mean; 34.623% improvement |
| Full route resolution, observations enabled | 343.822 ns current mean; 20.599% warning |
| Expiry pass with 100,000 unexpired entries | 3.990 ns current mean; 99.997% improvement |
| Insert/evict at 100,000 entries | 1551.567 ns current mean; 99.550% improvement |

The enabled-observation comparison includes newly restored visibility. The
baseline coordinator returns hot hits without emitting observations; the new
coordinator emits a metric and lifecycle event. The same configured sink and
benchmark function run in both versions. This is a cumulative comparison, not
equal observation serialization work. The independent reviewer verified that
interpretation and the unchanged baseline source; no repeat was requested.

Retained evidence: [primitive gate](primitives-gate.log),
[route/cache gate](routing-gate.log), [primitive baseline](primitives-baseline.log),
[primitive current](primitives-current.log), [route baseline](routing-baseline.log),
[route current](routing-current.log), [primitive commands/hashes](primitives-runs.json),
[route commands/hashes](routing-runs.json).

The separate actual-listener measurements and their throughput cost are recorded
in [Phase 7 admission evidence](../phase7-admission/README.md). Neither these
microbenchmarks nor those listener measurements replace the strict production
image load gates.

## Static and dependency checks

- Budget-checker tests: 10 shell cases and 13 Criterion-checker cases pass.
- All 23 shell scripts pass `bash -n`.
- Production dependency boundary check passes: frontline 111, sidecar 102,
  control-plane 174 unique normal dependencies.
- `git diff --check` passes at this integration checkpoint.

## Earlier production image integration

Final production images pass build, startup-failure, runtime-file and unchanged
256 MiB size checks: control-plane 43.83 MiB, frontline 37.33 MiB and sidecar
35.83 MiB. [Full image log](final-images.log) and [source metadata](final-images.json)
record the integrated source, unchanged during the run. Exact Linux arm64
[image identities](final-image-identities.json) are retained; no cross-architecture
image validation is implied.

Both updated proxy images and both load helper images build. The isolated
frontline helper initially failed because its Tonic server features were only
available through combined-package development feature unification. A dev-only
Tonic declaration fixes it; CI now checks each helper separately. Independent
review confirmed unchanged production dependency boundaries, and the actual
Docker rebuild passes. [Before](frontline-helper-before.log) and
[after](frontline-helper-after.log) are retained.

The first strict frontline load run passes HTTP/1 but fails h2c throughput:
17,730.9 direct RPS versus 12,351.5 proxied RPS, ratio 0.697 against the unchanged
minimum 0.75. All 50,000 responses are correct; added p99 is 0.389 ms. The run stopped
on that failure, so subsequent protocol and repeated-round gates are not claimed.
The [failed log](strict-frontline-first.log) and [workload/source metadata](strict-frontline-first.json)
are retained; no threshold was changed.

The unchanged original image also fails the identical sustained workload:
17,658.5 direct versus 12,751.2 proxied RPS, ratio 0.722. The new image is about
3.1% lower in raw proxy throughput in this pair; both versions miss the budget.
This is an additional stress workload: the coordinator raised concurrency from
the script's documented default of eight to 16. The documented default concurrency
with sustained request counts passes the repeated gate below. The two 16-way
failures remain capacity evidence; this review does not claim that workload
meets the strict throughput budget. [Baseline log](strict-frontline-original-baseline.log)
and [baseline image metadata](strict-frontline-original-baseline.json).

## Repeated strict production-image loads

Three rounds pass at the documented default concurrency of eight (TCP two),
with unchanged throughput and latency thresholds and zero response errors.
Each round includes 5,000 HTTP requests per path, 50,000 h2c requests, 50,000
HTTP/2 TLS requests, 2,000 generated gRPC calls, 500 one-MiB WebSocket sessions,
and 32 TCP streams of 32 MiB each. Production servers use release images; load
clients use the scripts' existing debug builds. The separate release-listener
benchmark is recorded in the admission evidence linked above.

| Current image path | Throughput ratio, rounds 1 / 2 / 3 | Required minimum |
| --- | --- | --- |
| Frontline HTTP/1 | 0.943 / 0.928 / 0.951 | 0.80 |
| Frontline h2c | 0.964 / 0.942 / 0.944 | 0.75 |
| Frontline HTTP/2 TLS | 0.912 / 1.003 / 0.969 | 0.75 |
| Frontline generated gRPC | 0.905 / 0.929 / 0.836 | 0.75 |
| Frontline WebSocket sessions | 0.974 / 0.972 / 0.982 | 0.80 |
| Sidecar HTTP | 0.930 / 1.002 / 0.960 | 0.80 |
| Sidecar TCP | 1.003 / 0.999 / 1.007 | 0.85 |

Maximum positive added p99 is 0.387 ms for frontline HTTP/gRPC, 0.156 ms for
sidecar HTTP, and 9.556 ms for TCP, all within the existing limits. WebSocket
added p99 is negative in every round. Fake-control-plane cold lookup takes
1.560–1.667 ms; scripts verify exact cold counter increments and no hot
Subscribe calls. This cold lookup measurement does not measure Kubernetes wake
latency; the deployed lifecycle gates cover that behavior.

All three paired original-image frontend references pass as well. Every current
gate record has unchanged source hashes before and after the run; baseline
reference metadata records the image and workload. Independent review checked
all twelve logs/records and approved this gate at its stated concurrency.
[Workload](strict-default/workload.json), [all measurements](strict-default/summary.json)
and the per-run logs/metadata in this directory retain the evidence.

## Integrated workspace checkpoint before 6H

`cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked
--offline -- -D warnings`, and `cargo test --workspace --locked --offline` all
pass on the integrated source before the readiness follow-up: 745 reported passing tests and 16 ignored
Kubernetes/subprocess fixtures. The subprocess parent invokes its ignored child;
Kubernetes bodies run through the separate deployed gates.
[Commands](final-workspace-checks.json), [Clippy](final-clippy.log) and
[workspace log](final-workspace.log) retain the results.
Separate real-Postgres tests passed during final
store/runtime review; those results are retained in the owning phase evidence.

## Deployed gates, in progress

The first stateless run reached a successful cold HTTP response and Running,
then failed when the fixture queried an obsolete prefix-based object name.
Production already uses a hash of the full instance ID. The failed
[log](stateless-stale-name-before.log) and [source record](stateless-stale-name-before.json)
are retained. Fixture correction and stronger one-request cold checks are in the
implementation/review loop.

The routing fixture independently exposed the same obsolete-name issue after
successful route requests ([log](routing-stale-name-before.log)). The strengthened
restart fixture initially stopped before deployment because macOS Bash 3.2 lacks
`BASHPID` ([log](restart-shell-portability-before.log)); its portability correction
is under review. These failures are retained and do not count as passing gates.

The final real-Kubernetes materializer gate passes both tests: conditional Secret
replacement protection and static-volume lifecycle with retained data and a held
Pod finalizer. [Log](final-materializer.log) and [metadata](final-materializer.json).
Only unrelated end-to-end fixture files changed during this run; the recorded
production-source digest is identical before and after. The materializer test
source itself was unchanged.

Deployed browser-shaped gRPC-Web passes authorization/CORS, create/read and
asynchronous deletion to NotFound ([log](final-grpc-web.log),
[metadata](final-grpc-web.json)). Dedicated metrics listeners pass for all three
components, including rejection of unrelated paths and separation from main
listeners ([log](final-metrics.log), [metadata](final-metrics.json)).
Their production-source digests are unchanged. The gRPC-Web run overlapped only
unrelated fixture edits; its own test and script remained unchanged.

The restart test design is independently approved after strengthening acceptance
before restart, stopping all old CP Pods and proving a fresh blocked cleanup
pass after restoration. Finalizer cleanup uses exact UID/resourceVersion and
removes only the test's finalizer. The portable late-image tag preserves a
controlled missing-image wake even across repeated/HA runs. Actual restart gates
remain open. Documentation received independent approval after aligning admin
enqueue semantics, new operational gauges and frontend-versus-CP delivery bounds.

The next stateless run proves its first HTTP request succeeds, then exposes a
plaintext-token layout assertion that predates the Secret reference contract
([log](stateless-secret-reference-before.log)). The corrected assertion will check
the exact Secret/key and configured token source, preserving the auth check.

The next restart run passes accepted-wake, idle-sleep and finalizer-blocked
accepted-delete recovery before an obsolete HTTP-01 cleanup-count assertion
fails ([log](restart-expiry-before.log)). Expired tokens already return 404;
autonomous maintenance can remove the row before manual expiry runs. The revised
fixture separately proves persisted expiry, authoritative absence and physical
row cleanup, regardless of which cleanup path wins. Independent review approved
that correction; the entire restart gate must still pass.

The single-request protocol gate exposes a cold HTTP/2 502 after 12.60s
([log](protocol-cold-http2-502-before.log),
[metadata](protocol-cold-http2-502-before.json)). This is a production defect,
not a passing gate. A second
preserved-namespace run reproduces the first-request failure in 2.49s; subsequent
direct app, sidecar and complete-path HTTP/1 and HTTP/2 requests pass. Rendered
Pods have no readiness probes, and the sidecar connects to CP before opening its
proxy listener. This is a concrete publication gap; the diagnostic now identifies a
Hyper connector `ConnectionRefused` to the new Service IP before application
request dispatch. Kubernetes Service programming follows EndpointSlice updates
asynchronously, so readiness alone is insufficient as a connection barrier.

6H is split into control-plane projection/selection, bounded private sidecar
readiness, and safe connection-only retry. Review also found that auxiliary raw
Pods inherit the primary Service labels, and name-only EndpointSlice lookup can
accept slices from an old Service UID. Both need correction before the new gate
can close. Probes must support loopback-only apps and must not count as proxied
activity or postpone TCP idle sleep. Transport acceptance is the promised default;
it does not prove arbitrary application-level health. No dispatched HTTP/gRPC
request may be replayed to hide a failure. Subsequent
protocol deployments preserve their namespace for diagnosis.

The latest stateless run passes its cold/hot responses, current layout and
Secret ownership checks, then fails a metrics assertion that parses filtered
stderr ([log](stateless-stderr-metrics-before.log)). Its replacement uses the
real dedicated Prometheus exporter; compiler/reviewer and deployed checks remain
required. The failure suite's old duplicate-PV fixture is rejected before
acceptance, preserving Cold ([log](failure-invalid-volume-fixture-before.log)).
The corrected test separates that rejection/no-effect guarantee from a valid
retained PV/PVC capacity mismatch that must fail binding after acceptance.

Other required Kubernetes lifecycle/protocol/soak gates remain open. The canonical
[remediation plan](../../review-remediation-plan.md) tracks completion.
