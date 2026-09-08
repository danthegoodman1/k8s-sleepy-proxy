# Phase 7 API and observability boundary evidence

Date: 2026-09-07. Scope: 7A and 7B. No lifecycle, routing, handshake or metric
semantics were intentionally changed. This packet does not close store-capability
work (7C) or production admission/progress budgets (7D).

## Ownership

`sleepypods-api` now owns the single protobuf source and its build script,
generated client/service code, ID reexports, route/HTTP-01 contracts,
InstanceState, BackendEndpoint, MaterializationTarget and client bearer-token
validation/interception. It contains no store, reconciler, server implementation,
manifest renderer or Kubernetes provider. Control-plane reexports the same types
at its existing module paths. Workload definitions and lifecycle commands that
clients do not need remain in control-plane.

`sleepypods-observability` owns stable names/labels, recorder and sinks, the
process-global sink, tracing vocabulary and Prometheus exporter. Proxy-core
reexports those exact definitions and retains local error conversions plus the
DrainTracker scrape collector. A conversion helper is generic over its result
payload/error so the shared vocabulary does not import proxy parser types.
There are no duplicated recorder statics or metric descriptors.

The data planes depend on the API contract instead of control-plane. Their tonic
features are channel/codegen; actual generated-service transport tests enable
server/router only as dev dependencies. All data-plane transport fakes use the
thin generated services, so even a dev dependency on control-plane was
unnecessary. Frontline initializes its own Rustls provider for its TLS listener;
sidecar no longer loads Rustls just to initialize a provider it never uses.
Control-plane uses the shared observability crate directly and drops proxy-core.

Existing protocol regressions remain intact, including authenticated full-chain
WebSockets, keep-alive upgrades, h2/gRPC bodies/trailers, timeout/drain behavior,
and the IPv6/forwarded-header review fixes. The protobuf relocation preserves
existing accepted-wake tags, generation fields and PodUID messages.

## Dependency evidence

Normal dependency trees are retained beside this report. The baseline trees
come from the coordinator's pre-implementation `dc7cac2` snapshot; numbers count
unique normal package dependencies and exclude the root package.

| Root package | Baseline | After | Enforced boundary |
| --- | --- | --- | --- |
| frontline | 197 | 111 | No control-plane, Kubernetes, Postgres, tonic-web or Axum |
| sidecar | 196 | 102 | Same boundary; Rustls is also absent |
| control-plane | 195 | 174 | No proxy-core, frontline or sidecar |

`./scripts/test-dependency-boundaries.py` is a repeatable production graph gate,
also enforced by CI after workspace tests have populated Cargo's dependency cache.
It queries each root separately with `cargo tree --edges normal`, keeping
test-only service dependencies out of the production claim. Shared workspace
test builds may still compile those service features through normal Cargo
feature unification; that does not make them production package dependencies.

These graphs establish source/dependency boundaries, not linked-binary shrinkage
or a universal throughput improvement.

## Build observation

Both measurements used an isolated target directory for the clean check, followed
by an unchanged second check against that directory. The after commands were
`cargo check -p sidecar --offline --target-dir .generated/sidecar-boundary-after-build`.
Other implementation lanes paused CPU-heavy commands during the after measurement.

| Sidecar check | Baseline | After |
| --- | --- | --- |
| Clean isolated target | 12.889 s | 7.717 s |
| Unchanged incremental | 0.134 s | 0.101 s |

Logs are `baseline-sidecar-build.log` and `after-sidecar-build.log`. Clean elapsed
time was 40.1% lower in this single host-local observation. The baseline is the
coordinator's pre-implementation snapshot, so accepted protocol changes and host
or cache conditions are also part of the comparison. These timings do not isolate
causality, establish a universal build-speed gain, or measure linked binary size.

## Validation

- Initial extraction run: **366 tests passed**, none ignored, across API,
  observability, proxy-core, frontline and sidecar (`tests.log`). Three existing
  backend/target validation tests were then moved from control-plane into their
  API owner; the final API-only run passes all **11 tests** (`api-final-tests.log`).
- `compatibility_reexport_and_shared_crate_use_one_global_recorder` installs one
  sink and proves both crate paths record through it. Existing lazy/noop recorder
  and allocation tests remain covered.
- Shared metric vocabulary/exporter tests moved with their owner. The real
  active-stream scrape test stays beside the proxy-specific collector.
- All-target clippy with denied warnings passes for the extracted crates and
  data planes (`clippy.log`). The control-plane production library also checks
  cleanly after the reexports/import updates (`control-plane-check.log`).
- Dependencies were created/added/removed using Cargo commands. Lock changes are
  local crate edges plus dependency relocation; no third-party version upgrades
  were introduced. Source-of-truth documentation points to the new protobuf,
  generation and observability locations.

Independent review and final coordinator integration remain completion gates.
No commits or pushes were made by this lane.
