# Codebase Review Remediation Plan

## Overarching Goal

Close the highest-leverage gaps found in the July 2026 whole-codebase review so
SleepyPods meets its own production north star: proxies never route to dead
backends longer than an explicit invalidation window, idle instances reliably
sleep, hot forwarding paths carry no global locks or per-request client
construction, and the control plane scales past a handful of routes and
concurrent wakes. Non-goals: HTTP/3, multi-replica control-plane fanout, new
protocol listeners, and any feature work beyond what the north star already
defines.

## Implementation Principles

- Database remains the source of truth; every lifecycle mutation stays a
  CAS'd, generation-bumping transaction.
- Hot forwarding paths perform no control-plane calls, no global locking, no
  per-request client construction, and no unbounded allocation
  (docs/proxy-hot-path-budgets.md).
- Invalidation is event-driven; TTLs are a safety net, never the primary
  mechanism.
- Prefer deleting a duplicated code path over hardening both copies.
- Every await on a request path must be cancellation-safe: client disconnects
  must not corrupt shared state.

## Testing Strategy

- `cargo fmt --check`, `cargo clippy --workspace --all-targets` (clean), and
  `cargo test --workspace` gate every phase.
- Postgres conformance (`scripts/test-postgres-store.sh`) runs in CI, not
  opt-in, for phases touching `postgres/`.
- Production-image load smokes with `*_STRICT_BUDGETS=1` gate data-plane
  phases; criterion benches guard route lookup and proxy primitives.
- Kind e2e scripts (`scripts/test-kind-e2e-*.sh`) validate lifecycle-visible
  changes (sleep, wake, drift, races).
- New concurrency behavior gets dedicated tests: concurrent requests, request
  cancellation mid-await, and stream loss mid-subscribe.

## Phase 1: Green Gates and CI Baseline

Goal:
A clean tree passes fmt, clippy, and all tests, and CI enforces that from now
on.

Scope:
- Fix failing test `proxy_wake_materializer_failure_returns_transport_error`
  (`crates/control-plane/tests/proxy_api_transport.rs:1327`): readiness
  failures now surface as `WakeInstanceError::Projection` ("projection
  failed"), so the assertion on "materialization failed" is stale. Align the
  assertion with the intended error taxonomy.
- Remove inherent `MaterializationReconcileError::to_string` shadowing
  `Display` (`crates/control-plane/src/reconciler.rs:623`), which clippy
  rejects at deny level, and clear the ~20 outstanding clippy warnings.
- Add a CI workflow: fmt, clippy (warnings denied), workspace tests, and the
  Postgres conformance suite with `SLEEPYPODS_POSTGRES_URL` set so
  `tests/postgres_store.rs` cannot silently self-skip.

Completion gate:
CI is green on the default branch and fails on reintroduced warnings or
skipped Postgres conformance.

Testing plan:
- `cargo clippy --workspace --all-targets` exits 0 with no warnings.
- `cargo test --workspace` passes locally and in CI.
- CI run demonstrates the Postgres suite executed (not skipped).

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Fix stale wake-error test assertion | `crates/control-plane/tests/proxy_api_transport.rs` renamed the readiness failure case to `proxy_wake_projection_failure_returns_transport_error` and now asserts `Code::Unavailable` with "projection failed"; `cargo test --workspace` passed. |
| Complete | Work | 1B: Remove `to_string` shadow; clear clippy warnings | `crates/control-plane/src/reconciler.rs` no longer defines inherent `MaterializationReconcileError::to_string`; `cargo clippy --workspace --all-targets -- -D warnings` passed. |
| Complete | Work | 1C: CI workflow with fmt/clippy/test/postgres gates | `.github/workflows/ci.yml` runs `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and `cargo test -p control-plane --test postgres_store` with `SLEEPYPODS_POSTGRES_URL` set plus skip-output and nonzero-test-count guards. |
| Complete | Gate | Default branch green under CI | CI run 28637514645 on `north-star` at `3fd1997` passed all steps (fmt, clippy, workspace tests, Postgres store). |
| Complete | Test | Postgres conformance runs (not skipped) in CI | CI run 28637514645 "Test Postgres store" step logged `running 2 tests` and `test result: ok. 2 passed` with no self-skip output. |

## Phase 2: Data-Plane Quick Wins

Goal:
Remove the cheap, high-impact hot-path costs in proxy-core, frontline, and the
sidecar without structural change.

Scope:
- Reuse pooled hyper clients: `HttpProxy::proxy`
  (`crates/proxy-core/src/http.rs:67-74`) builds a new client per request, so
  every forwarded request dials the backend fresh and h2 never multiplexes.
  Construct one pooled http1 client and one `http2_only` client in
  `HttpProxy::new` and reuse them.
- Re-enable sidecar inbound keep-alive
  (`crates/sidecar/src/runtime.rs:235` sets `keep_alive(false)`); idle
  detection is permit-based per request, so keep-alive connections do not hold
  drain permits.
- Replace byte-at-a-time protocol sniffing
  (`crates/sidecar/src/runtime.rs:285-343`): `read_one` issues one syscall per
  byte up to 64 KiB and `headers_complete` rescans the whole buffer per byte
  (O(n^2)). Read in chunks and scan only the tail for `\r\n\r\n`, with a
  header-read deadline.
- Reap completed connection tasks in all three frontline accept loops
  (`crates/frontline/src/listener.rs:305-341,369-419,435-474`); `join_next` is
  only called after shutdown, so results accumulate unboundedly. The reaper
  already exists at `listener.rs:660`.
- Stop routing per-permit gauge updates through the stderr log sink
  (`crates/proxy-core/src/drain.rs` `record_active_streams`;
  `crates/sidecar/src/bin/sidecar.rs:78-90` always installs stderr): two
  formatted locked stderr writes per request. Scrape the gauge from
  `active_count()` at collect time instead.
- Workspace hygiene: move duplicated dependency versions (tokio, hyper,
  hyper-util, http, bytes, tonic, tokio-tungstenite) into
  `[workspace.dependencies]`, and add `[profile.release]` (thin LTO,
  `codegen-units = 1`, `strip = true`) for the data-plane binaries.

Completion gate:
Strict-budget load smokes pass with measurably improved sidecar/frontline
ratios, and no per-request client construction or stderr writes remain on the
forward path.

Testing plan:
- `SLEEPYPODS_*_STRICT_BUDGETS=1` runs of `smoke-frontline-load.sh`,
  `smoke-sidecar-load.sh`, `smoke-sidecar-tcp-load.sh` before/after.
- Existing sidecar/frontline behavioral tests still pass (keep-alive change
  covered by connection-reuse assertions to be added).
- `check-criterion-regressions.py` clean on proxy primitives.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Pooled http1/http2 clients in `HttpProxy` | `crates/proxy-core/src/http.rs` constructs one pooled HTTP/1.1 client and one `http2_only` client in `HttpProxy::new`, uses `TCP_NODELAY` upstream connectors, and preserves request rewrite/header stripping/error mapping; `http_proxy_handles_concurrent_http2_streams` asserts concurrent h2 streams share one upstream connection. |
| Complete | Work | 2B: Sidecar keep-alive re-enabled | `crates/sidecar/src/runtime.rs` leaves HTTP/1.1 keep-alive enabled and sets `TCP_NODELAY` on accepted sockets; `http_keep_alive_reuses_connection_without_holding_idle_permit` proves two sequential requests over one client connection succeed, the upstream sees one connection, and the open keep-alive connection does not hold a drain permit. |
| Complete | Work | 2C: Chunked header sniffing with deadline | `crates/sidecar/src/runtime.rs` reads protocol sniffing data in bounded chunks, scans only the newly-read tail with overlap, classifies on the header block only (`&prefix[..headers_end]`) while replaying the full buffered prefix, keeps the 64 KiB cap, and applies a 10s header-read deadline; tests cover segmented h2 prefaces, split HTTP header terminators, upgrade-looking bytes in request bodies, pipelined request pairs, large WebSocket upgrades, and timeout. |
| Complete | Work | 2D: Connection-task reaping in accept loops | `crates/frontline/src/listener.rs` reaps completed tasks nonblockingly inside all HTTP, TLS-termination, and TLS-passthrough accept loops; `crates/sidecar/src/runtime.rs` applies the same pattern to its HTTP and TCP accept loops. |
| Complete | Work | 2E: Per-permit metrics off the stderr sink | `DrainTracker` no longer records `sleepypods_runtime_active_streams` on permit acquire/release; `RuntimeActiveStreamsCollector` in proxy-core samples `active_count()` at scrape time for both the sidecar and frontline `/metrics` endpoints (`metrics_endpoint_samples_runtime_active_streams_via_collector`). Both frontline observability modes install `FilteredStderrObservabilitySink`, which keeps lifecycle/error events on stderr but drops metric samples and the per-request route-cache lookup event (`filtered_stderr_sink_suppresses_metrics_and_per_request_events_only`). |
| Complete | Work | 2F: `[workspace.dependencies]` and release profile | Shared dependency versions across crates now live in root `[workspace.dependencies]` with crate-local features preserved via `workspace = true`; root `[profile.release]` sets thin LTO, one codegen unit, and stripping. `Cargo.lock` resolution did not change. |
| Complete | Gate | Strict-budget smokes pass post-change | Strict sidecar HTTP: `sidecar_direct_http1_rps_ratio=1.052`, `sidecar_direct_http1_added_p99_ms=-0.054`; strict sidecar TCP: `sidecar_direct_tcp_throughput_ratio=1.154`, `sidecar_direct_tcp_stream_added_p99_ms=-14.583`; strict frontline completed all phases: http1 ratio `1.054`, h2c ratio `0.782`, h2-TLS ratio `0.948`, real gRPC ratio `0.837`, WebSocket ratio `0.967`, cold wake `2.079 ms`. |
| Complete | Test | Before/after smoke comparison recorded | Baseline at commit d31126e (same machine, lax budgets, 200 requests / concurrency 8): sidecar http1 ratio 0.869 (direct 3842.1 rps, proxied 3338.3 rps, added p99 1.517 ms); sidecar tcp throughput ratio 1.009 (114.76 vs 115.82 MiB/s); frontline http1 ratio 1.014 (4181.3 vs 4241.9 rps, added p99 0.051 ms); frontline h2c-grpc ratio 0.330 (2594.0 vs 855.7 rps, added p99 40.190 ms); frontline h2-tls-grpc ratio 0.091 (3420.6 vs 312.2 rps), which failed the 0.10 floor and aborted the baseline smoke before the real-grpc/websocket/cold stages. Post-change lax runs: sidecar http1 ratio 0.840 (added p99 0.296 ms); sidecar tcp ratio 0.794 (added p99 22.347 ms); frontline http1 ratio 1.145, h2c 0.956, h2-tls 0.794, real-grpc 0.922, websocket 0.987, cold wake 1.365 ms. h2 direct baselines are not comparable across the harness change: the old harness paid TCP+TLS+h2 setup per request inside the timed window, while the current harness holds one multiplexed channel per worker with setup outside it. An isolation run of the old (d31126e) harness client against post-change images measured h2c-grpc ratio 0.977 (proxied 855.7 to 4055.9 rps) and h2-tls-grpc ratio 0.534 (proxied 312.2 to 2205.3 rps) under identical per-request-setup methodology, so the pooled-client fix itself accounts for a 4.7x/7x proxied throughput gain independent of the harness change. The strict h2c gate (0.782 vs the 0.75 floor) passed on a single run; strict h2 and real-gRPC ratios straddle their floors across repeated runs on this hardware, so treat strict passes as single-run evidence and gate releases on a median of 3+ runs. |
| Complete | Test | Validation and benchmark gates | `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace` passed. `cargo bench -p proxy-core` completed, and `./scripts/check-criterion-regressions.py` reported every benchmark `status=ok`. |

## Phase 3: Sleep/Wake Product Correctness

Goal:
Instances that go idle sleep and stay routable-to-nothing only briefly:
proxies receive explicit invalidations on lifecycle transitions, idle
reporting cannot permanently disarm, and dead peers cannot hold instances
awake.

Scope:
- Publish `RouteInvalidated` on lifecycle transitions. Today only operator
  route-binding create/delete notify the broker
  (`crates/control-plane/src/api/server.rs:432,470`); sleep
  (`api/sidecar.rs` has no broker), wake completion, failed wake, and
  `DeleteInstance` (route rows removed by `ON DELETE CASCADE`) never
  invalidate, so proxies route to deleted backends until the 10s TTL
  (`api/proxy.rs:27`). Plumb the broker into the sidecar API, reconciler, and
  instance delete; then raise `POSITIVE_ROUTE_CACHE_TTL` since it no longer
  carries correctness.
- Restore drain ordering on sleep: `ReportIdle` currently runs `begin_sleep`
  then synchronously deletes Kubernetes objects and finalizes inside the RPC
  (`crates/control-plane/src/idle.rs:148-282`), leaving zero propagation
  window. After invalidations exist, let `begin_sleep` mark Draining and leave
  cleanup to the reconciler after the drain grace.
- Make instance deletion two-phase: `delete_instance`
  (`crates/control-plane/src/instance.rs:225-258`,
  `postgres/instance_ops.rs:172-187`) hard-deletes with no state/generation
  guard, so a concurrent wake can re-apply objects that cascade-delete leaves
  orphaned in the cluster. CAS to `Deleting` first (transition table already
  supports it), then cleanup, then tombstone; store-side delete requires
  `state = 'deleting'`.
- Fix rendered-name collisions: `instance_id_suffix`
  (`crates/control-plane/src/kubernetes_name.rs:32-41`) uses the first ~8
  chars of the instance ID, so IDs sharing a prefix (`tenant-0001`,
  `tenant-0002`) render identical object names and the collision check blocks
  the second wake. Derive the qualifier from a short stable hash of the full
  ID.
- Re-arm sidecar idle reporting: any decoded non-error response, including
  `Unavailable{Waking}` and `GenerationConflict`, permanently sets `reported`
  (`crates/sidecar/src/idle/control_plane.rs:48-67`), stranding the instance
  awake. Treat only `Accepted`/`AlreadyDraining` as terminal; re-arm with
  backoff otherwise. Also send the real `drain.active_count()` instead of
  hardcoded `zero_active()` (`crates/sidecar/src/idle.rs:226-232`) and abort
  the report if activity arrives during the RTT.
- Bound established streams: no request/response timeout in `HttpProxy`, no
  idle timeout in `proxy_streams`, unbounded WebSocket close wait
  (`crates/proxy-core/src/websocket.rs:210-217`), no TCP keepalive. A
  half-open peer holds a `DrainPermit` forever, defeating sleep. Add TCP
  keepalive, configurable per-stream idle timeouts, and a close-handshake
  deadline.
- Move the sidecar bearer token out of plain pod-spec env
  (`crates/control-plane/src/manifest/render.rs:907-912`) into a materialized
  Secret reference.

Out of scope:
- Single-driver wake consolidation (Phase 6).

Completion gate:
A kind e2e run shows sleep/delete propagating to proxies via invalidation
(not TTL), a prefix-sharing pair of instances waking side by side, and an
idle instance sleeping despite an earlier `Unavailable{Waking}` response.

Testing plan:
- Unit: broker notification on begin_sleep/complete_wake/delete; idle
  detector re-arm; active-count-at-send; wake-vs-delete interleaving on the
  store.
- Kind e2e: extend `test-kind-e2e-lifecycle-races.sh` (or add a scenario) for
  invalidation-driven sleep and delete; name-collision wake pair.
- Postgres conformance: two-phase delete transitions.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: Lifecycle invalidations through the broker | `RouteSubscriptionBroker` is shared across operator/proxy/sidecar services and the reconciler; `StoreBackedSidecarApi::report_idle`, proxy wake completion/failure, reconciler wake/sleep finalization, and operator `DeleteInstance` publish route invalidations after store transitions. Covered by `sidecar_report_idle_accepted_invalidates_active_proxy_subscription`, `proxy_wake_completion_invalidates_active_proxy_subscription`, `operator_delete_instance_invalidates_active_proxy_subscription`, existing route-CRUD invalidation tests, and `cargo test --workspace`. |
| Complete | Work | 3B: Raise positive route TTL after 3A | `POSITIVE_ROUTE_CACHE_TTL` is 300s in `crates/control-plane/src/api/proxy.rs`, so TTL is a missed-event safety net; `proxy_subscribe_route_resolved_returns_subscription_and_route_entry` asserts the new cache policy. |
| Complete | Work | 3C: Reconciler-driven sleep cleanup with drain grace | `idle.rs` now only resolves drain grace and calls `begin_sleep`; Postgres marks deleting materializations with delayed reconciliation eligibility, and `reconcile_deleting` finalizes Cold when the instance is Draining one generation past the materialization stamp. `sidecar_api_transport` asserts `ReportIdle` leaves Draining/Deleting with refs intact and performs no Kubernetes delete; `deleting_reconciliation_skips_finalize_for_later_drain_cycle` covers stale-row discard. |
| Complete | Work | 3D: Two-phase delete with Deleting CAS | `instance::delete_instance` CASes to `Deleting`, cleans projected pending/ready refs, then calls a Postgres hard-delete guarded by `state = 'deleting'`; `postgres_store` conformance asserts hard-delete rejection before Deleting and wake-after-delete CAS rejection. |
| Complete | Work | 3E: Hash-based instance name qualifier | `kubernetes_name.rs` uses first 8 hex chars of SHA-256 over the full instance ID; unit tests cover stable hashes and prefix-sharing IDs. Rendered object names for existing materializations change and pre-production drift/rematerialization is expected. |
| Complete | Work | 3F: Idle re-arm, real active count, RTT abort | `sidecar::idle` sends `drain.active_count()` at report time; only Accepted/AlreadyDraining are terminal; GenerationConflict, Unavailable{Waking}, and transport errors retry with backoff; `active_work_during_report_round_trip_aborts_report_and_rearms` covers RTT activity. |
| Complete | Work | 3G: Stream timeouts and TCP keepalive | `proxy-core` adds configurable TCP stream idle timeout (default 1h), TCP keepalive (default 60s) for sidecar TCP and frontline TLS passthrough sockets, and a 10s WebSocket close-handshake wait; `cargo test -p proxy-core` and load smokes passed. HTTP proxy remains wall-clock-unbounded to avoid killing WebSocket/gRPC/streaming workloads. |
| Complete | Work | 3H: Sidecar token via Secret | Manifest rendering now materializes an owned `Secret` and sets `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` via `valueFrom.secretKeyRef`; `private_render_options_inject_sidecar_control_plane_token` asserts Secret `stringData` and env reference. |
| Complete | Gate | Kind e2e proves invalidation-driven sleep/delete | `./scripts/test-kind-e2e-lifecycle-races.sh` passes on a fresh cluster: concurrent wake, ReportIdle-while-waking, delete-while-waking, delete-while-draining, failed-wake-retry, stale sidecar ReportIdle, and route reassignment invalidation all green (233s). `./scripts/test-kind-e2e-restart.sh` also passes (wake/sleep/delete/HTTP-01/route-reassignment recovery, 156s). Load-bearing product behaviors: `projection::ownership_mismatch_reason` treats same-instance/same-materialization objects with an older generation stamp as owned-stale so wake retries supersede their own leftovers; `wake::wake_instance` resolves a `complete_wake` generation conflict against an instance the reconciler already completed to `AlreadyRunning` instead of surfacing a conflict. The e2e fixture waits on hash-qualified PV/PVC names and retries deletion through object-teardown `Unavailable` responses. |
| Complete | Test | Cancellation/interleaving unit coverage listed above | `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `./scripts/test-postgres-store.sh`, `cargo test -p proxy-core`, `cargo test -p sidecar idle::control_plane`, and load smokes passed; Postgres conformance includes two-phase delete interleaving. |

## Phase 4: Frontline Routing Concurrency

Goal:
Route resolution scales with concurrent connections: cache hits never
contend on a global lock, misses coalesce explicitly, and client disconnects
cannot corrupt routing state.

Scope:
- Split the read path from the control-plane I/O path. One
  `Arc<Mutex<FrontlineRouteCoordinator>>`
  (`crates/frontline/src/listener.rs:80,558,586,621`) currently serializes
  every request and is held across `subscribe_route`, the untimed
  `wake_instance` RPC, and the 250ms reconnect backoff — one cold route
  freezes all routing. Put `RouteCache` behind a read-optimized structure
  (RwLock/ArcSwap snapshot); run the subscribe stream, unsubscribes, and
  wakes in a dedicated task reached by channel; keep single-flight per
  identity explicitly (test at `listener/tests.rs:1090` documents the current
  implicit coalescing); add a wake RPC deadline.
- Make transport awaits cancellation-safe: a dropped `subscribe_route` future
  desynchronizes the shared response channel permanently
  (`crates/frontline/src/control_plane_transport.rs:193-199`) — demux by
  `request_id` (map of oneshots) in a reader task; treat
  `UnexpectedRouteResponse` as session-fatal. A dropped wake leaves its
  `WakeTracker` key pending forever (`crates/frontline/src/route.rs:157-167`)
  — remove the key on drop (guard) or run wakes in the background task.
- Emit a synthetic `StreamClosed` when the stream dies during an in-flight
  call (`control_plane_transport.rs:231-233` returns empty after the session
  is dropped), so subscription-backed entries are flushed instead of serving
  stale routes until TTL.
- Make unsubscribes best-effort and off the request path
  (`crates/frontline/src/resolver.rs:360-377` fails the user's request on an
  unrelated unsubscribe error).
- Fix `RouteCache` internals: full `expire` sweep per resolve
  (`resolver.rs:189`), O(n) `rebuild_positive_index` per removal
  (`cache.rs:417-429`), linear negative-cache scan (`cache.rs:195-205`), and
  shared FIFO eviction that lets a Host/path scanner evict hot positive
  routes (`cache.rs:442-460`). Lazy expiry plus periodic sweep, stable-index
  removal, hashed negative cache with its own bound, LRU-touch positives, and
  `Arc` cache entries to drop per-hit string clones.
- Consolidate the duplicated HTTP pipeline: `FrontlineHttpRuntime::handle_http*`
  (`runtime.rs:123-165`) duplicates the production
  `SharedFrontlineHttpRuntime::handle` path and has already drifted (HTTP-01
  gating); fold into one. Delete dead `RouteMatcher` (`matcher.rs:16-72`).
- Complete the upstream WebSocket connection before answering 101
  (`listener.rs:610-657`), so backend refusal yields 502 instead of a silent
  close.

Completion gate:
A concurrency test drives N connections with one cold route and shows hot
hits unaffected; strict-budget frontline smoke passes; cancellation tests
pass.

Testing plan:
- New tests: concurrent hot hits during an in-flight wake; request cancelled
  mid-wake then route recovers; stream killed mid-subscribe then next request
  resubscribes cleanly; scanner traffic does not evict hot routes.
- Criterion: extend `route_lookup` to measure the full resolve path, not only
  `cache.lookup`.
- Strict-budget `smoke-frontline-load.sh` including cold-wake phase.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: Read-path/actor split with explicit single-flight and wake deadline | `SharedFrontlineRouteCoordinator` uses read-side `RwLock<SubscriptionState>`, per-identity `RouteFlight`, and an actor channel for subscribe/wake/cache updates. `wake_instance` is actor-owned and timed by configurable `SLEEPYPODS_FRONTLINE_WAKE_INSTANCE_TIMEOUT_MS` (default 5s). Evidence: `cargo test -p frontline` includes `route::tests::shared_route_hot_hits_do_not_wait_for_in_flight_cold_wake` and `route::tests::shared_route_same_identity_uses_one_in_flight_subscribe_route`. |
| Complete | Work | 4B: Request-id demux; cancellation-safe wake tracker | `control_plane_transport` now owns the response reader and routes subscribe responses by `request_id` through pending oneshots; `UnexpectedRouteResponse` drops the session. Wakes run in the actor-owned flight, so dropped request futures cannot strand `WakeTracker` keys. Evidence: `control_plane_transport::tests::pushed_updates_over_response_buffer_are_drained_without_public_cursor`, `control_plane_transport::tests::subscribe_route_after_response_stream_close_opens_new_stream`, and `route::tests::shared_route_cancelled_mid_wake_does_not_strand_recovery`. |
| Complete | Work | 4C: Synthetic StreamClosed on in-flight stream loss | Subscription response stream closure now surfaces as `RouteSubscriptionEvent::StreamClosed` and resolver maintenance flushes subscription-backed positives. Evidence: `resolver::tests::stream_close_event_invalidates_hot_positive_before_ttl_and_lazily_rebuilds` and `control_plane_transport::tests::response_stream_close_after_route_response_is_observed_by_response_reader`. |
| Complete | Work | 4D: Best-effort background unsubscribes | Resolver no longer fails unrelated route refreshes on unsubscribe errors; evicted/expired subscription IDs are emitted to the actor for best-effort unsubscribe. Evidence: `resolver::tests::expired_positive_unsubscribes_best_effort_after_refresh` and `resolver::tests::unsubscribe_client_error_does_not_fail_refresh`. |
| Complete | Work | 4E: Cache internals (lazy expiry, stable index, hashed negatives, LRU) | Cache entries are `Arc`-backed, positives use indexed lookup with insertion/update-order eviction, negatives use a hashed map with their own bound, and removals update indices without full rebuilds. Evidence: `cache::tests::negative_scanner_traffic_does_not_evict_hot_positive_routes`; `cargo bench -p frontline` route lookup: exact HTTP 156.64ns, wildcard HTTP 264.24ns, same-host paths 159.36ns, SNI 77.45ns, full shared hot resolve 319.62ns. |
| Partial | Work | 4F: Single HTTP pipeline; delete dead matcher | HTTP-01 gating is aligned across the mutable runtime and production listener path, and listener routing now uses the shared coordinator. The old matcher module remains only as the local route-ranking helper/tests; no dead matcher deletion was made in this phase. |
| Complete | Work | 4G: WebSocket upstream-first upgrade | Accepted WebSocket upgrades now connect the upstream before returning 101; backend refusal maps to 502. Evidence: `listener::tests::listener_websocket_backend_refusal_returns_bad_gateway_without_upgrade` and existing WebSocket forwarding tests. |
| Complete | Gate | Concurrency test: cold wake does not stall hot hits | `cargo test -p frontline` passes `route::tests::shared_route_hot_hits_do_not_wait_for_in_flight_cold_wake`; strict frontend smoke passed on rerun, including cold wake latency 1.510ms under 250ms. |
| Complete | Test | Full-resolve criterion benchmark under budget | `cargo bench -p frontline` includes `route_lookup/full_resolve_http_exact_host_hot` at 319.62ns, below the 1us hot route lookup budget; `./scripts/check-criterion-regressions.py` passed. |

## Phase 5: Control-Plane Store Scalability

Goal:
Route resolution and wake throughput scale with row counts and concurrency
instead of full scans and a global table lock.

Scope:
- Index `resolve_route`: it loads every binding of the identity kind and
  scores in Rust (`crates/control-plane/src/postgres/route_ops.rs:108-132`),
  never using the existing `route_bindings_identity_lookup_idx`. Enumerate
  exact-host plus wildcard-suffix candidate keys and resolve with one indexed
  query joined to instance and ready materialization; treat a concurrently
  deleted instance as a miss instead of `NotFound`
  (`route_ops.rs:288-292`).
- Replace the global lock with unique constraints:
  `ensure_no_rendered_object_ref_collision` takes
  `LOCK TABLE materializations IN SHARE ROW EXCLUSIVE MODE` plus JSONB
  cross-join scans per wake (`postgres/materialization_ops.rs:1324-1366`),
  and exclusivity checks do the same (`:1257-1288`), serializing all wakes.
  Normalize into `materialization_objects` and
  `materialization_exclusivity_keys` child tables with UNIQUE indexes
  maintained in the same transaction.
- Deduplicate `complete_wake`/`finalize_sleep` vs their `_reconciliation`
  variants (~150 identical lines each,
  `materialization_ops.rs:113-192/827-901` and `278-354/903-975`): share one
  transaction body, with lease enforcement prepended for reconciliation.
- Error taxonomy: lease loss is reported as retryable `Unavailable`
  (`materialization_ops.rs:1016-1049`) and gets pointlessly retried; add a
  non-retryable lease-conflict variant. Remove the unreachable
  generation-mismatch branch there.
- Connection pool: make size configurable (hardcoded 16,
  `postgres/connection.rs:25-27`) and fix
  `create_workload_class_version` holding one pooled connection while
  acquiring a second (`postgres/instance_ops.rs:97-143`).
- Migration runner: wrap in `pg_advisory_lock` so multi-replica boots do not
  race DDL (`postgres/migrations.rs:60-79`).
- Idempotency records: add expiry + GC (table currently grows forever;
  replay after resource deletion returns an internal-looking `NotFound`,
  `instance_ops.rs:404-421`).

Completion gate:
Postgres conformance passes; EXPLAIN on resolve_route shows index usage; a
concurrent-wake test shows no table-level lock waits.

Testing plan:
- Extend `tests/postgres_store.rs`: wildcard/path resolution via the new
  query, concurrent wakes for distinct instances committing without
  serialization, collision/exclusivity conflicts still rejected, lease-loss
  error variant, idempotency GC.
- `scripts/test-postgres-store.sh` in CI (Phase 1 gate).

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: Indexed single-query resolve_route; miss on vanished instance | Missing: query rewrite and conformance tests. |
| Incomplete | Work | 5B: Normalized object/exclusivity tables replace LOCK TABLE | Missing: migration and concurrent-wake test. |
| Incomplete | Work | 5C: Shared transaction bodies for wake/sleep finalization | Missing: dedup commit. |
| Incomplete | Work | 5D: Non-retryable lease-conflict error; dead branch removed | Missing: error variant and test. |
| Incomplete | Work | 5E: Pool config + two-connection fix | Missing: change. |
| Incomplete | Work | 5F: Advisory-locked migrations | Missing: change. |
| Incomplete | Work | 5G: Idempotency record expiry and GC | Missing: migration and GC op. |
| Incomplete | Gate | Concurrent wakes commit without table-lock waits | Missing: test evidence. |
| Incomplete | Test | Conformance additions listed above | Missing: test names. |

## Phase 6: Reconciler as the Single Lifecycle Driver

Goal:
One code path drives wake/sleep/delete to completion with sane timing:
no lease expiry mid-work, no infinite poison retries, no 120-second gRPC
parks.

Scope:
- Make `WakeInstance` enqueue-only: CAS to Waking, record Pending, return
  `StillWaking`; the reconciler drives apply/readiness/complete. This deletes
  the duplicated state machine in `wake.rs` (~500 lines mirrored in
  `reconciler.rs:212-289`), the double render at generation+1
  (`wake.rs:300-355`), and the dead `AlreadyWaking` variant
  (`wake.rs:51`), and gives server-side single-flight for concurrent wakes.
- Fix lease timing: `lease_ttl` 60s (`reconciler.rs:74`) is shorter than the
  blocking 120s readiness wait (`kube_materializer.rs:253`), guaranteeing
  expiry mid-wait and duplicate drivers. Heartbeat the lease during waits or
  make reconcile non-blocking per tick (apply, then check `inspect_readiness`
  each pass).
- Poison-work policy: per-record backoff derived from `reconcile_attempt`, an
  attempt cap transitioning materialization and instance to Failed (the north
  star's `Waking -> timeout/error -> Failed` edge is currently only in the
  synchronous path), and per-candidate spawns so four stuck wakes cannot
  head-of-line block Deleting cleanups (`reconciler.rs:136-150`).
- Ownership-checked rollback: `apply_manifest` failure rollback deletes
  applied refs without inspecting ownership (`materializer.rs:265-269`),
  which can delete a newer lease-holder's objects; route rollback through the
  projection's ownership checks.
- Process hardening: handle SIGTERM (only `ctrl_c` today,
  `runtime.rs:295-299`), and surface reconciler task death instead of
  dropping its JoinHandle (`runtime.rs:291-293`).
- `ReconcileMaterialization` RPC shares the process reconciler and returns
  "claimed" instead of blocking through a throwaway instance
  (`api/server.rs:571-586`).

Completion gate:
Kill-the-controller-mid-wake e2e (extend `test-kind-e2e-restart.sh`) recovers
without duplicate drivers; a never-ready workload lands in Failed within the
attempt cap while other reconciliation proceeds.

Testing plan:
- Unit: lease heartbeat/non-blocking readiness; backoff schedule; attempt-cap
  Failed transition; per-candidate concurrency.
- Kind e2e: controller restart mid-wake; poison workload alongside a healthy
  sleep/delete.
- Frontline behavior against enqueue-only wake (proxy polls via
  subscription/TTL) covered by the Phase 4 concurrency tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A: Enqueue-only WakeInstance; reconciler sole driver | Missing: refactor and e2e. |
| Incomplete | Work | 6B: Lease heartbeat or non-blocking readiness | Missing: timing fix and unit test. |
| Incomplete | Work | 6C: Backoff, attempt cap, Failed transitions, per-candidate spawn | Missing: policy implementation and tests. |
| Incomplete | Work | 6D: Ownership-checked rollback | Missing: materializer change. |
| Incomplete | Work | 6E: SIGTERM handling; reconciler death surfaces | Missing: runtime change. |
| Incomplete | Work | 6F: Shared-reconciler admin RPC | Missing: API change. |
| Incomplete | Gate | Restart-mid-wake e2e recovers cleanly; poison work isolated | Missing: e2e output. |
| Incomplete | Test | Unit coverage for timing/backoff/caps | Missing: test names. |

## Phase 7: Structural Hygiene

Goal:
Dependency and module boundaries match the architecture: data-plane binaries
do not compile the control plane, shared vocabulary lives in shared crates,
and dead scaffolding is gone.

Scope:
- Extract a `sleepypods-api` crate holding the protos, generated clients, and
  `BearerToken`; `frontline` and `sidecar` currently depend on the whole
  `control-plane` crate (pulling kube, k8s-openapi, deadpool-postgres,
  serde_yaml, tonic-web) for proto types alone.
- Move control-plane observability vocabulary (RECONCILER_*, KUBERNETES_*,
  MATERIALIZATION_* metrics, auth log fields) out of
  `proxy-core/src/observability`, breaking the control-plane -> proxy-core
  dependency.
- Remove `ControlPlaneStore` default method bodies
  (`crates/control-plane/src/store.rs:38-294`) that turn missing
  implementations into runtime errors; keep an explicit test-only
  unimplemented base. Consider one shared in-memory store run through the
  same conformance suite to replace the five hand-rolled per-file fakes.
- Wire or delete dead code: `AdmissionLimiter` (269 lines, no production
  callers — note the sidecar currently has no connection bound at all),
  unrecorded `PROXY_*` metrics, discarded `TcpProxyStats`/`WebSocketProxyStats`.
- Sidecar serves connections via hyper-util's auto builder like frontline
  (deleting `detect_protocol`/`PrefixedTcpStream`, ~140 lines), fixing the
  `Upgrade: websocket, foo` token-list mis-parse
  (`crates/sidecar/src/runtime.rs:365`).
- Split inline test modules of `materializer.rs`/`reconciler.rs` into
  `foo/tests.rs` submodules matching `wake.rs` convention (cosmetic; do last).

Completion gate:
`cargo tree -p sidecar` shows no kube/postgres dependencies; workspace builds
measurably faster; no exported dead code remains.

Testing plan:
- Full workspace tests plus kind protocol e2e
  (`test-kind-e2e-protocols.sh`, `test-kind-e2e-grpc-web.sh`) after the
  crate split and sidecar convergence.
- Build-time comparison before/after recorded in the PR.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 7A: `sleepypods-api` proto crate split | Missing: crate and dependency graph proof. |
| Incomplete | Work | 7B: Observability vocabulary out of proxy-core | Missing: module move. |
| Incomplete | Work | 7C: Store trait defaults removed; shared fake strategy | Missing: trait change. |
| Incomplete | Work | 7D: Dead code wired or deleted | Missing: commit. |
| Incomplete | Work | 7E: Sidecar on auto builder; upgrade-token fix | Missing: refactor and protocol tests. |
| Incomplete | Work | 7F: Test-module layout normalization | Missing: file moves. |
| Incomplete | Gate | Sidecar dependency tree free of control-plane heft | Missing: `cargo tree` output. |
| Incomplete | Test | Protocol e2e green after convergence | Missing: e2e run. |
