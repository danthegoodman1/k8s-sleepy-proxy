# Cold readiness: sidecar implementation evidence

The deployed one-shot cold protocol regression exposed a refused TCP connection to a newly published Service before application request bytes were sent. The diagnostic frontline log records `ECONNREFUSED` to `10.96.75.133:8080` about 50 ms after route publication. Existing rendered Pods lacked readiness probes. The control-plane render/Service identity correction is a separate reviewed packet. A bounded connector-only refusal retry is also required because Pod readiness does not establish that kube-proxy has programmed the Service.

## Sidecar readiness contract (6H2)

`SLEEPYPODS_SIDECAR_READINESS_LISTEN_ADDR` optionally enables a private HTTP `/ready` listener. It is bound only after initial control-plane connection and the actual proxy listener bind. GET returns 200 only if a TCP connection to the configured loopback application port succeeds within 500 ms; otherwise it returns 503. It does not send application bytes, enter proxy accounting, or depend on control-plane health after startup. This checks TCP availability only: the structured template does not expose semantic app probes. An app needing semantic readiness must delay binding its port until ready, or use a compatible custom sidecar implementing that stronger check.

The private endpoint has fixed limits of eight sockets, requests, and handshakes, one HTTP/2 stream per connection, and a one-second whole health connection lifetime. These limits apply to health traffic only. All health tasks are owned and canceled on shutdown. The configured port must be nonzero and distinct from app, proxy, and metrics ports. Initial control-plane setup now uses the configured proxy setup deadline.

Changed source: `crates/sidecar/src/readiness.rs`, its tests, the module export in `lib.rs`, and binary startup composition in `src/bin/sidecar.rs` with `src/bin/sidecar/readiness_tests.rs`. No dependencies were added. Existing protocol and idle/drain behavior is retained.

## Validation

- Initial [sidecar package](6h2-sidecar-tests.log), before supervision review correction: 64 library, six binary, and two actual process lifecycle tests passed; zero failures or ignored tests.
- [All-target check](6h2-sidecar-check.log) and [all-target strict clippy](6h2-sidecar-clippy.log) passed. `cargo fmt -p sidecar --check` and `git diff --check` passed.
- New regressions cover delayed loopback app startup, health socket saturation/expiry/recovery, shutdown, and repeated health probes leaving TCP activity at zero while idle reporting proceeds.
- Binary composition tests hold the initial connection future behind a deterministic gate, prove both listeners stay unpublished until it completes, then prove a delayed app becomes ready and receives exactly one HTTP request. This is a controlled startup dependency test, not a real control-plane network fault. Separate tests bound stalled startup and cancellation.

## Retained diagnosis and remaining integration

The original failure is retained at `.generated/implementation-evidence/20260907T234630Z-protocols.log`; the reproduction is `20260907T235111Z-protocols.log`. The distinct `review-remediation-diagnose` image produced `protocols-diagnostic-gate.log` and `protocols-diagnostic-frontline.log`. Temporary error instrumentation was removed and the HTTP source compared against its pre-diagnostic copy. Original `review-remediation` image identities were preserved. Local kubeconfig credentials are excluded from this evidence packet.

These are focused local checks, not a claim that the deployed cold protocol gate or refreshed production performance gates pass. The coordinator owns reviewed rebuild and those final integration gates.

## Control-plane projection and Service membership (6H1)

The primary sidecar receives a typed HTTP readiness probe (`/ready`, one-second
period/timeout/failure threshold1) and the agreed private-listener environment
variable. The deterministic port search starts at15001, skips all declared app
ports, the proxy port and known SleepyPods metrics-listener environment ports,
then wraps to1024; exhaustion fails render. Empty optional metrics settings stay
disabled. The health port is never added to the generated Service.

The stable primary Service selector is unchanged. Raw Deployment/StatefulSet
Pod templates retain instance cleanup ownership but no primary workload-name
label. Explicit raw metadata/template labels, selectors and expressions using
that reserved key are rejected; raw standalone Pods remain unsupported. The
renderer and materializer use stable apply-order sorting, and ProjectionPlan
retains that order, so the generated Service remains ahead of auxiliary raw
Services. The raw-Service render test exercises this complete ordering chain.

Both production Kubernetes readiness paths require a controller ownerReference
matching the observed current Service UID/name, a nondeleting Service and slice,
and an addressed ready nonterminating endpoint. Unspecified `ready` retains
Kubernetes' default interpretation. A mock API test rejects eight invalid GET/list
snapshots before allowing readiness: old same-name Service owner, explicitly
unready endpoint, terminating endpoint, absent owner, deleting slice, missing
Service UID, deleting Service, and empty endpoint address.

Compatibility is explicit in the operator/projection guides. Custom/pinned
sidecar images need the new interface. Existing Running projections require an
ordinary sleep/wake to adopt it. Partially applied old same-generation Pending
projections have obsolete rendered hashes: they fail permanently before any new
apply and proceed through safe recorded-ref cleanup, retaining uncertainty and
inventory guarantees. The migration regression proves permanent classification,
zero applies/completions, and successful recorded-ref cleanup. It does not
pretend the narrow fake store models the production scheduler's durable failure
transition (that transition has separate Phase6 runtime/actual-PG coverage).

The isolated `kind_materializer` PV/projection fixture intentionally uses nginx
instead of a real sidecar/control-plane pair. It keeps a meaningful nginx probe
on port80 `/`; production private-sidecar readiness is tested by6H2 and the
coordinator's cold E2E gates, not that fixture.

Completed focused gates (actual exit0):

- [CP library check](6h1-check.log).
- [Manifest tests](6h1-manifest-tests.log):58 passed, including
  `primary_http_and_tcp_workloads_have_private_transport_readiness`,
  `readiness_port_skips_declared_app_proxy_and_metrics_ports`,
  `raw_auxiliary_workloads_never_match_primary_service_selector`,
  `raw_auxiliaries_cannot_supply_reserved_primary_selector_labels_or_expressions`
  and the generated/raw Service ordering assertion in
  `renders_valid_raw_pv_pvc_service_and_stateful_set`.
- [Production Kubernetes client tests](6h1-kube-tests.log):28 passed, including
  `production_readiness_waits_for_current_service_nonterminating_ready_membership`.
- [Pending-upgrade regression](6h1-upgrade-test.log):1 passed,
  `pending_pre_readiness_projection_fails_permanently_without_rewrite_and_can_be_cleaned`.
- [All-target strict CP clippy](6h1-clippy.log); scoped rustfmt and diff-check pass.

The first full CP attempt had six loopback `PermissionDenied` failures under the
sandbox ([retained attempt](6h1-control-plane-tests-sandbox-denied.log)); the
[final loopback-enabled full CP suite](6h1-control-plane-tests.log) completed
exit0:240 library tests,46 operator,41 proxy,17 sidecar transport and the remaining
local tests passed; deployed kind tests remained ignored. The nine Postgres test
entry points had no actual-PG environment and are not actual-PG evidence here.
No kind operation or Postgres container was run for6H1. Final deployed gates
remain coordinator-owned.

## Sidecar supervision review correction

A sibling listener failure previously caused `try_join!` to drop the proxy future before its idle-reporting task cleanup. The binary now signals shared shutdown on the first error, preserves that error, and joins all listener futures through the existing bounded drain and task cleanup. The signal task is also explicitly aborted and joined on lifecycle return.

[Final binary supervision tests](6h2-supervision-tests.log) pass eight tests, including an occupied metrics port closing both listeners, and an injected sibling failure after an actual TCP runtime idle-report future starts. The latter observes the idle client being dropped before supervision returns. [Strict all-target sidecar clippy](6h2-sidecar-clippy.log) was refreshed after this correction. The combined data-plane suite will be refreshed with the separately scoped connector change.


### 6H1 named-port compatibility follow-up

The new health container port is unnamed. The existing proxy port keeps its
`sleepypods` name unless an app port already claims that name; only that
previously invalid collision case omits the injected proxy name. This preserves
valid raw Services using named `targetPort: sleepypods` while allowing app port
names `sleepypods` and `sp-readiness`. Generated Service/probe targets remain
numeric. The old-Pending migration fixture removes the health port by captured
probe number and restores the legacy proxy name.

Final [manifest tests](6h1-port-manifest-tests.log) pass59/59, including named raw
Service compatibility and both app-name collisions. The focused
[readiness/upgrade tests](6h1-port-compatibility-tests.log) pass16/16. Final
[all-target strict clippy](6h1-port-compatibility-clippy.log) exits0. An
[intermediate clippy failure](6h1-port-compatibility-clippy-unused-helper.log)
reported only the test helper made unused by the separate exclusivity fixture
correction; that helper was removed before the passing run. No new kind gate
was run for this follow-up.

## Connector-only refusal retry (6H3)

The new private `proxy-core/src/connect.rs` helper retries only `io::ErrorKind::ConnectionRefused` from the connector error chain. Each failed attempt waits 50 ms. HTTP's existing connect deadline, TCP's existing connect deadline, and WebSocket's existing whole connect/handshake deadline enclose this loop; none is restarted per attempt. The HTTP upstream permit is acquired once before entering the loop. No HTTP request future, protocol handshake, body, or established relay is retried. A completed or reset application request is never replayed by this policy.

This is a connection-setup bridge for Service endpoint programming, alongside honest readiness. It is not an unlimited first-request guarantee when the app or Service remains unavailable beyond the configured setup budget. Persistently refused WebSocket setup now returns 504 rather than immediate 502. The real frontline listener regression checks a 120 ms setup budget, no101 response, and request permit release.

Production changes are limited to the private module and its declaration plus the existing HTTP, WebSocket, and TCP connector call sites. [The retained source delta](6h3-source-changes.patch) compares against the saved pre-6H3 source. Fresh successful connections pass through the small helper once; pooled requests and established relays bypass it. No dependency was added.

### Final local validation

- [Combined data-plane suite](6h3-dataplane-tests.log): **380 passed**, zero failures or ignored tests: frontline 202; proxy-core 104 (48 library, four allocation, ten new connection integrations, 12 HTTP, two observability, one process signal, 12 TCP, 15 WebSocket); sidecar 74 (64 library, eight binary, two real process lifecycle).
- [Strict all-target clippy](6h3-dataplane-clippy.log) passes for all three packages. Scoped format and repository diff checks pass.
- The [before-fix regression](6h3-before-regression.log) fails both actual HTTP/1 and HTTP/2 first requests while their upstream ports are closed. After the fix each waits for a real listener and sends one POST body. Separate reset-after-dispatch tests prove neither version reconnects/replays after response loss.
- Actual TCP and WebSocket late-listen tests verify one application exchange. Cancellation tests cover all four paths; fresh HTTP/1 and HTTP/2 requests release their single pool permit and drain count. Private helper tests observe a real `ConnectionRefused` before allowing the test to bind the port, and cover one total deadline, cancellation, and immediate propagation of non-refusal errors. The setup timeout also remains the outer bound if Hyper independently continues an internal pool connection attempt.
- The [first combined suite](6h3-initial-dataplane-tests.log) retains the expected stale WebSocket fixture failure (502 versus new bounded 504); its corrected test uses the configured deadline and retains pre-upgrade and permit checks.

Existing `proxy_primitives` benchmarks exercise established duplex TCP/WebSocket relays and HTTP header helpers, not fresh socket connects. No isolated connector performance number is claimed. The coordinator owns the required refreshed production-image strict loads, relevant primitive checks, and one-shot cold deployed HTTP/gRPC/WebSocket/SNI gates after independent source approval and rebuild. Original gated image tags remain unchanged by this local work.


## Final deployed fixture packet

The separately reviewed [integration fixture corrections](integration-fixtures.md) cover atomic exclusivity rejection, repaired permanent cleanup permissions, repeatable delayed images, and pre-TTL route notification convergence. Actual deployed runs remain coordinator-owned.

The later [stateless metrics framing packet](stateless-metrics-framing.md) retains
the deployed complete-response/EOF failure and the bounded fixture correction,
including a held-open socket regression. It does not count the diagnostic run
as a clean deployed pass.

The follow-up [bounded hot-traffic packet](stateless-hot-traffic.md) proves a new
cache hit under successful traffic in 121 ms on the frozen final images. It
retains the public response EOF failure and keeps the separate one-request
re-wake 502 investigation visible.

The [one-request re-wake follow-up](stateless-one-shot-rewake.md) removes the
remaining HTTP failure retry path and retains that deployed gate as outstanding
while the separate 502 investigation proceeds.

The matching [stateful one-request re-wake packet](stateful-one-shot-rewake.md)
preserves the saved-data marker while making a failed first read fatal. Its
stronger deployed check also remains outstanding.
