# SleepyPods Operator Runbook

Runtime observability is backend-neutral. Production binaries install the
process-wide stderr sink, which emits structured lines:

```text
observability.type=metric metric.name=<name> metric.value=<number> metric.labels=<k=v,...>
observability.type=event event.name=<name> <field=value ...>
```

Production binaries can also expose an opt-in Prometheus `/metrics` listener.
It is disabled unless an explicit listen address is configured:

| Binary | Env var |
| --- | --- |
| control-plane | `SLEEPYPODS_CONTROL_PLANE_METRICS_LISTEN_ADDR` |
| frontline | `SLEEPYPODS_FRONTLINE_METRICS_LISTEN_ADDR` |
| sidecar | `SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR` |

Bind metrics listeners on an internal address or behind your normal scrape
auth/network policy. Do not expose them on the public proxy listener.

## Metric Names

Low-cardinality label keys are `protocol`, `direction`, `operation`,
`outcome`, and `state`.

| Metric | Kind | Labels | Use |
| --- | --- | --- | --- |
| `sleepypods_proxy_active_streams` | gauge | `protocol` | Active proxy requests, streams, or upgraded sessions. |
| `sleepypods_proxy_admission_decisions_total` | counter | `protocol`, `outcome` | Accepted/rejected new proxy work. |
| `sleepypods_proxy_drain_events_total` | counter | `state`, `outcome` | Drain start, idle completion, rejection, timeout. |
| `sleepypods_proxy_forwarded_bytes_total` | counter | `protocol`, `direction` | TCP/TLS byte forwarding. |
| `sleepypods_proxy_forwarded_messages_total` | counter | `protocol`, `direction` | Message-oriented forwarding, including WebSocket. |
| `sleepypods_proxy_operation_duration_seconds` | histogram | `protocol`, `operation`, `outcome` | Bounded proxy primitive duration. |
| `sleepypods_proxy_tls_client_hello_total` | counter | `protocol`, `outcome` | TLS ClientHello/SNI parser outcomes. |
| `sleepypods_runtime_control_plane_calls_total` | counter | `operation`, `outcome` | Runtime calls such as subscribe, wake, unsubscribe, report idle. |
| `sleepypods_runtime_active_streams` | gauge | none | Protocol-neutral drain permit count. |
| `sleepypods_runtime_drain_duration_seconds` | histogram | `outcome` | Runtime drain duration. |
| `sleepypods_runtime_http01_results_total` | counter | `outcome` | HTTP-01 challenge hits, misses, and errors. |
| `sleepypods_runtime_materialization_failures_total` | counter | `operation`, `outcome` | Kubernetes/materialization failure path. |
| `sleepypods_runtime_route_cache_lookups_total` | counter | `outcome` | Frontline route-cache hit/miss results. |
| `sleepypods_runtime_subscribe_stream_events_total` | counter | `outcome` | Subscribe stream close/update/invalidation events. |
| `sleepypods_runtime_wake_latency_seconds` | histogram | `outcome` | Control-plane wake latency. |
| `sleepypods_reconciler_runs_total` | counter | `outcome` | Materialization reconciler loop runs. |
| `sleepypods_reconciler_run_duration_seconds` | histogram | `outcome` | Duration of one materialization reconciler pass. |
| `sleepypods_reconciler_candidates_total` | counter | `state` | Rows selected for reconciliation. |
| `sleepypods_reconciler_claims_total` | counter | `state`, `outcome` | Reconciliation lease claim outcomes. |
| `sleepypods_reconciler_lease_renewals_total` | counter | `outcome` | Lease renewal success, error, and lease-loss outcomes. |
| `sleepypods_materializations_nonterminal` | gauge | `state` | Current pending/deleting materialization backlog by state. |
| `sleepypods_materialization_oldest_nonterminal_age_seconds` | gauge | `state` | Oldest pending/deleting materialization age by state. |
| `sleepypods_exclusivity_keys_held` | gauge | `state` | Held rendered exclusivity keys by non-deleted materialization state. |
| `sleepypods_kubernetes_operations_total` | counter | `operation`, `outcome` | Controller apply/delete/readiness operation outcomes. |
| `sleepypods_kubernetes_operation_duration_seconds` | histogram | `operation`, `outcome` | Controller apply/delete/readiness operation duration. |

Known bounded label values:

- `protocol`: `tcp`, `http`, `websocket`, `tls`
- `direction`: `client_to_upstream`, `upstream_to_client`
- `operation`: `accept`, `admit`, `connect`, `forward`, `rewrite_request`,
  `drain`, `tls_client_hello`, `route_cache_lookup`, `subscribe_route`,
  `unsubscribe`, `subscribe_stream`, `wake_instance`, `materialize`,
  `http01_resolve`, `report_idle`, `apply`, `delete`, `readiness`
- `outcome`: `success`, `error`, `timeout`, `rejected`, `canceled`, `hit`,
  `miss`, `started`, `closed`, `updated`, `invalidated`, `already_running`,
  `already_waking`, `already_draining`
- `state`: `accepting`, `active`, `draining`, `idle`, `pending`, `ready`,
  `failed`, `deleting`
- TLS ClientHello outcomes use the `outcome` label with `sni`, `no_sni`,
  `incomplete`, `not_tls`, `unsupported_version`, `not_client_hello`,
  `record_too_large`, `malformed`, or `invalid_hostname`.

## Structured Events

Event names:

- `runtime.drain.started`
- `runtime.drain.completed`
- `runtime.drain.timeout`
- `runtime.route_cache.lookup`
- `runtime.subscribe_stream.event`
- `runtime.wake.event`
- `runtime.materialization.failure`
- `runtime.materialization.reconciliation`
- `runtime.idle_report.event`
- `runtime.http01.event`
- `control_plane.auth.decision`

Lifecycle field keys:

- `instance.id`
- `route.id`
- `subscription.id`
- `generation`
- `backend.generation`
- `cluster.id`
- `namespace`
- `error.reason`
- `active.count`
- `duration.ms`
- `auth.decision`
- `auth.reason`
- `auth.caller.role`
- `auth.required.role`
- `grpc.service`

Use IDs and generations for correlation, but do not turn high-cardinality IDs
into metric labels.

## Dashboards And Alerts

Build dashboards from the exact names above:

- Route-cache hit rate:
  `sleepypods_runtime_route_cache_lookups_total{outcome=hit}` divided by hit
  plus miss. Alert on sustained miss spikes or hot-cache load phases making
  `subscribe_route` calls.
- Control-plane calls: rate of
  `sleepypods_runtime_control_plane_calls_total` by `operation,outcome`.
  Alert on `outcome=error` for `subscribe_route`, `wake_instance`,
  `report_idle`, or `unsubscribe`.
- Wake latency: p95/p99 over `sleepypods_runtime_wake_latency_seconds` by
  `outcome`; alert on high latency and any sustained `error` or `timeout`.
- Materialization failures: rate of
  `sleepypods_runtime_materialization_failures_total` by `operation,outcome`;
  page on nonzero sustained failures.
- Reconciler health: rate of `sleepypods_reconciler_runs_total`,
  p95/p99 over `sleepypods_reconciler_run_duration_seconds`, and
  `sleepypods_reconciler_claims_total` by `state,outcome`. Alert when runs stop
  or claim errors/rejections spike across replicas.
- Materialization backlog and locks:
  `sleepypods_materializations_nonterminal`,
  `sleepypods_materialization_oldest_nonterminal_age_seconds`, and
  `sleepypods_exclusivity_keys_held` by `state`. Page on old pending/deleting
  work or held keys that do not fall after sleep cleanup.
- Kubernetes controller operations: rate and latency for
  `sleepypods_kubernetes_operations_total` and
  `sleepypods_kubernetes_operation_duration_seconds` by `operation,outcome`.
  Use Kubernetes/container metrics for CPU, memory, restarts, pod scheduling,
  network, and filesystem signals instead of adding SleepyPods labels for them.
- Drain duration and active streams:
  `sleepypods_runtime_drain_duration_seconds`,
  `sleepypods_runtime_active_streams`, and
  `sleepypods_proxy_drain_events_total{outcome=timeout}`.
- Sidecar idle reports:
  `sleepypods_runtime_control_plane_calls_total{operation=report_idle}` plus
  `runtime.idle_report.event` fields `instance.id`, `generation`, and
  `active.count`.
- HTTP-01:
  `sleepypods_runtime_http01_results_total` and `runtime.http01.event`.
- Proxy throughput:
  `sleepypods_proxy_forwarded_bytes_total`,
  `sleepypods_proxy_forwarded_messages_total`, and
  `sleepypods_proxy_active_streams`.
- TLS/SNI:
  `sleepypods_proxy_tls_client_hello_total` and TLS listener errors.
- Load-budget gates: show the latest results from
  `docs/proxy-hot-path-budgets.md` scripts, especially p99 added latency,
  request-rate/throughput ratios, hot-cache zero-control-plane-call assertions,
  and benchmark regression checker output.
- Control-plane auth: `control_plane.auth.decision` by `grpc.service`,
  `auth.required.role`, `auth.reason`, and `auth.caller.role`. Reasons are
  `accepted`, `missing`, `malformed`, `invalid`, and `wrong_role`; token values
  are not emitted.

## Symptom Runbooks

Cold wakes are slow or failing:

1. Check `sleepypods_runtime_wake_latency_seconds` by `outcome`.
2. Search `event.name=runtime.wake.event` and
   `event.name=runtime.materialization.failure` for `instance.id`,
   `generation`, `cluster.id`, `namespace`, and `error.reason`.
3. Check Kubernetes for rendered PV/PVC, Service, workload readiness, image pull,
   and EndpointSlice publication in the materialization namespace.
4. Check Postgres availability and the instance state/generation with
   `GetInstance`.

Rendered object name collision:

1. Search `runtime.wake.event` for `error.reason=store` and an error containing
   `rendered Kubernetes object ref collision`.
2. The wake failed before Kubernetes apply; inspect the failed instance and the
   owner instance named in the error.
3. Use DNS-label-safe `instance_id` values and readable base templates. The
   control plane appends `-<instance-id-prefix>` to final object names and
   truncates the base first.
4. Retry after changing the naming template or deleting/finalizing the
   conflicting active materialization.

Workload exclusivity key conflict:

1. Search `runtime.wake.event` for `error.reason=exclusivity_conflict`.
2. Use `exclusivity.key.name`, `exclusivity.owner.instance.id`, `cluster.id`,
   and `namespace` to identify the held key without relying on provider-specific
   disk or license parsing. Rendered key values are intentionally not logged.
3. Inspect the owner instance and active materialization. Pending, Ready, and
   Deleting materializations keep keys held; Deleted materializations do not.
4. If cleanup is stuck, resolve Kubernetes deletion errors first. Do not wake a
   second same-key instance until the first materialization has safely finalized
   or an operator has deliberately changed the workload values/key declaration.
5. If cleanup was externally verified but the database row remains non-terminal,
   use `ForceDeleteMaterialization` with the materialization id, operator, and
   reason. If only the lock must be released, use `ForceReleaseExclusivityKey`
   with the exact target, key name, and key value. Treat key release as unsafe
   unless the singleton resource cannot still be attached by the old instance.

Non-terminal materialization is stuck:

1. Search `runtime.materialization.reconciliation` for `operation=claim`,
   `operation=reconcile`, `operation=lease_lost`, and `outcome=error`.
2. Call `ReconcileMaterialization` with the materialization id. The response
   reports current state, lease metadata, and recorded refs, and attempts one
   reconciliation pass for `Pending` or `Deleting` rows.
3. Check Kubernetes refs reported by the materialization. Missing refs during
   delete are safe only when every recorded ref is gone or cleanup is otherwise
   proven unnecessary.
4. If a finalizer or Kubernetes outage blocks delete, do not force-release the
   key. Fix the Kubernetes blocker and let reconciliation retry.
5. If an operator has manually removed every recorded ref and verified no
   singleton resource can be attached by the old workload, call
   `ForceDeleteMaterialization`. Record a specific reason.

Hot routes are missing or unexpectedly cold:

1. Check route-cache miss rate and
   `sleepypods_runtime_control_plane_calls_total{operation=subscribe_route}`.
2. Confirm the `RouteBinding` identity is exact or wildcard as intended and that
   the route protocol matches HTTP or TLS SNI.
3. Search `runtime.route_cache.lookup` for `route.id` and `subscription.id`.
4. Verify the instance has a ready materialization backend if state is
   `Running`.

A route change still reaches the old backend:

1. Check `sleepypods_runtime_subscribe_stream_events_total` for
   `outcome=invalidated` or `closed`.
2. Search `runtime.subscribe_stream.event` for the old `subscription.id`.
3. Confirm frontlines can reconnect to the control plane and that route
   reassignment committed in the database.
4. Send a fresh request after invalidation; stale serving before positive TTL
   expiry is a release-blocking bug.

Idle sleep is not happening:

1. Check `sleepypods_runtime_active_streams` and
   `sleepypods_proxy_active_streams` for stuck work.
2. Search `runtime.idle_report.event` for `active.count` and `generation`.
3. Check sidecar `SLEEPYPODS_IDLE_TIMEOUT_MS`,
   `SLEEPYPODS_IDLE_RETRY_BACKOFF_MS`, and
   `SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS`.
4. Check control-plane call errors for `operation=report_idle`.

Delete cleanup is stuck or objects leak:

1. Confirm `DeleteInstance` returned and inspect the instance state.
2. Search `runtime.materialization.failure` and `runtime.wake.event` for cleanup
   `error.reason`.
3. Check Kubernetes delete permissions for StatefulSet, Deployment, Service,
   PVC, and PV objects and whether finalizers block deletion.
4. Compare leaked objects' SleepyPods labels with the recorded materialization
   generation before manual cleanup.
5. After manual cleanup, prefer waiting for materialization reconciliation to
   finalize. Use `ForceDeleteMaterialization` only when cleanup has been proven
   and reconciliation cannot make progress.

HTTP-01 challenge fails:

1. Check `sleepypods_runtime_http01_results_total` and `runtime.http01.event`.
2. Resolve the challenge through `ResolveHttp01Challenge` using the exact host
   and token.
3. Confirm the client path is
   `/.well-known/acme-challenge/{token}` and that the Host header matches.
4. Check token expiry and run `ExpireHttp01Challenges` after the expected
   validation window.

TLS termination or SNI passthrough fails:

1. Check `sleepypods_proxy_tls_client_hello_total` outcomes, especially
   `no_sni`, `invalid_hostname`, `not_tls`, or `malformed`.
2. For termination, verify
   `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR` and
   `SLEEPYPODS_FRONTLINE_TLS_TERMINATION_CERTS` entries use
   `sni|certificate_path|private_key_path`.
3. For passthrough, verify
   `SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR` and a TLS SNI route
   binding.
4. Confirm client SNI matches an exact or wildcard route/certificate.

Postgres or database outage:

1. Check control-plane process logs for store provider errors and runtime
   control-plane call `outcome=error`.
2. Verify `SLEEPYPODS_STORE_PROVIDER=postgres` and `SLEEPYPODS_POSTGRES_URL`.
3. Restore database availability before restarting frontlines aggressively;
   proxies can keep hot cached routes briefly but cold wakes and misses need the
   control plane.
4. Use backups as the source for disaster recovery; Kubernetes objects are not
   the durable resource model.

Proxy `Subscribe` disconnects:

1. Check `sleepypods_runtime_subscribe_stream_events_total{outcome=closed}`.
2. Confirm frontlines can reach `SLEEPYPODS_CONTROL_PLANE_ENDPOINT`.
3. Expect lazy cache rebuild on later requests; sustained reconnect loops should
   also show control-plane call errors.
4. Verify route invalidations are received after reconnect by changing a test
   route and watching for cache miss/re-resolve.

Control-plane API returns `Unauthenticated` or `PermissionDenied`:

1. Search `event.name=control_plane.auth.decision` and group by
   `grpc.service`, `auth.required.role`, `auth.reason`, and
   `auth.caller.role`.
2. `auth.reason=missing`, `malformed`, or `invalid` maps to
   `Unauthenticated`; check that the caller sends `Authorization: Bearer ...`
   and that its configured token matches the control-plane static token for
   that role.
3. `auth.reason=wrong_role` maps to `PermissionDenied`; verify operator,
   frontline/proxy, and sidecar credentials are not swapped.
4. Do not look for token values in logs. They are intentionally redacted and
   should be rotated through the deployment secret source instead.

Image or container startup failures:

1. Run `./scripts/smoke-images.sh` for production image startup, non-root,
   runtime-file, CA, and image-size checks.
2. Inspect pod events for image pull, missing env, missing certificate files,
   or service-account permission errors.
3. For sidecars, verify rendered env includes app port, instance ID/generation,
   control-plane endpoint, idle policy, runtime-injected
   `SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN` when static auth is enabled, and
   `SLEEPYPODS_SIDECAR_MODE`.
4. For workload startup, check app container readiness before blaming the
   sidecar or frontline.
