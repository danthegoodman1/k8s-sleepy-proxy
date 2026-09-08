# Phase 6J: idle activation investigation

## Confirmed finding and limits

This is a read-only diagnostic packet against the frozen, approved 6I production images, not a completed fix. No production source, image, configuration, or request retry policy changed during capture.

The original one-shot re-wake returned a complete empty HTTP 502 after 5.648 seconds ([wire](original-502-wire.log)). Durable outbox timing places Ready/Running publication at 2026-09-08 01:12:47.335 UTC and the accepted idle sleep intent at 01:12:48.149, only 814 ms later, while the request remained pending until 01:12:50.822 ([history](original-502-state-history.log)). The workload class idle timeout was 2 seconds. Full recovered control-plane logs also show successful readiness at 01:12:47.329 and the sidecar idle RPC at 01:12:48.137, followed by deletion work at 01:12:49.489.

Source inspection confirms that `sidecar/src/runtime.rs` starts the idle detector when the actual listener starts, before routable activation. `sidecar/src/idle.rs` measures an initially zero-active interval then `idle/control_plane.rs` retries unavailable reports without restarting that interval unless activity appears. `control-plane/src/idle.rs` accepts a current Ready member's zero-active report without a minimum duration since Running. Thus pre-activation quiet time can authorize sleep immediately after activation. Resetting only after a Waking rejection cannot cover a first report that arrives after Running.

The exact transport error behind the original 502 was **not recovered**. The approved binaries do not log the underlying Hyper client error; the original sidecar Pod was already deleted. Early idle acceptance is a confirmed timing defect, but these observations do not prove whether that particular request failed due to DNS, connection setup, a closed upstream stream, or another cause. No post-dispatch replay is proposed.

## Three natural follow-up cycles

Each cycle sent exactly one cold GET through the existing frontline, with no caller retry and correct Content-Length framing. All returned HTTP 200 with the expected 25-byte response: 3.096 s, 3.364 s, and 2.709 s. They do not erase the retained failure or constitute a fix gate.

- [Cycle 1](natural-cycle-1/response.json): DNS/TCP headers and CP/frontline logs captured. The attempted combined Kubernetes watch rejected multiple resource types; its error is retained and no object-watch evidence is claimed for this cycle.
- [Cycle 2](natural-cycle-2/response.json) and [cycle 3](natural-cycle-3/response.json): separate Pod/Service/EndpointSlice watches, immediate sidecar log followers, CP/frontline logs, and DNS/TCP header capture. Current Service UIDs match Slice owner UIDs. The packet captures show exactly one dispatched GET and a backend HTTP 200. Sidecar idle acceptance follows completed traffic by the configured two seconds.

The packet observer ran Python AF_PACKET inside the existing kind node, bounded to 35 seconds, recording only connection headers, DNS questions/result codes, request verbs, and response status lines. It did not record request payloads or authorization headers. [Capture source](capture-natural-cycle.py) is retained; its frontend Pod IP and namespace are intentionally specific to this diagnosis. Duplicate packet observations occur across pre/post-NAT interfaces.

[CoreDNS configuration](coredns-config.json) disables both success and denial caching for `cluster.local`; it does not support a cached-old-Service explanation for this deployment. Fresh successful cycles resolve newly allocated Service addresses and establish new sockets. This does not establish the original failure's DNS outcome.

## Retained namespaces and cleanup handoff

All needed currently recoverable CP/frontline logs, Pod/image identities, current instance states, and full outbox histories are archived under [retained-namespaces](retained-namespaces). Exact namespaces are:

- `sleepypods-e2e-stateless-hot-cache`: original diagnostic 502 plus these three follow-up cycles.
- `sleepypods-e2e-stateless-hot-framing`: separate complete-200/EOF-wait fixture diagnosis.
- `sleepypods-e2e-stateless-hot-fixed`: passing corrected stateless fixture.

The cluster reservation is released to the coordinator after capture. The coordinator may remove these retained namespaces before the global stateless/stateful soak selector runs. Production image provenance remains the frozen 6I identities in the final-integration packet. No builds, tests, diagnostic images, or production edits were made in this investigation.

## 6J implementation: finite activation ownership

The implementation was developed in `/private/tmp/sleepypods-6j-t7bhxyqr` while
the coordinator's production tree and images remained frozen. The independently
approved production/local-regression patch was then applied to the root with
`git apply --check` and `git apply`; pending deployed-fixture changes were excluded. The
[review patch](6j-production-and-local-tests.patch) and
[source hashes](6j-source-sha256.tsv) compare the frozen root files with this
scratch implementation. They exclude the separately reviewed deployed-fixture
adjustments described below. No dependency or protobuf schema changed.

Automatic sleep now requires the exact Ready generation to have reached
`max(190 seconds, resolved class idle timeout)` using its existing persisted
state-entry timestamp. The instance and materialization row locks enforce the
last check atomically before the Running-to-Draining state/outbox mutation.
Repeated Ready updates and process reconnects do not restart the timestamp. The
floor applies even after a quick successful request, so one completed caller
cannot release protection for another pending caller. No-traffic wakes remain
finite; the default 300-second class idle interval dominates. Explicit internal
`BeginSleep` calls can bypass this automatic-idle rule; there is no public
operator Sleep RPC.

The shared API declares the 190-second contract. Frontline environment parsing
and actual public listener boundaries enforce checked
`route + max(setup, upstream header idle) <= 190 seconds`, including the default
`130 + max(10, 60)`. Sidecar public HTTP accepts acquire temporary drain work
through the first handler response; ordinary HTTP upload/response or WebSocket
work already owns the request at that point. Initial setup timeout, cancellation,
overload and errors release that guard. Idle keepalive and private health do not
hold it. The existing runtime gauge now documents setup/stream overlap rather
than implying exact user-stream cardinality. There is no active-stream lifetime
cap and no request replay.

A cheap advisory database-clock Ready-age read rejects known deferrals before
Kubernetes membership inspection; the final locked check is still authoritative.
The existing FailedPrecondition status carries a positive retry hint capped at
190,000 ms. Updated sidecars wait without repeated membership polling, with real
activity interrupting the delay and restarting a full quiet interval. The same
activity-watch revision is retained through the RPC and retry wait, fixing a
race where a complete burst during the response poll could previously be lost.
Generation/projection validation precedes deferral; deferral is not membership
approval. Malformed or absent hints fall back to the configured retry policy.

Production files in the patch are the API constants; CP materialization request,
store error, atomic sleep transaction, advisory status read and idle/API mapping;
frontline config/coordinator/listener validation; the shared HTTP server helper;
sidecar runtime/readiness and idle/transport paths; and the metric help text.
The remaining patch files are focused local regression tests and the operator,
runbook and protocol contract documentation. The inventory includes exact paths
and hashes for every file.

## Local validation and retained failures

- [Actual PostgreSQL](postgres.log): all 10 registered tests passed, zero ignored,
  including exact-generation deferred sleep with no state/outbox mutation,
  persisted age across reconnect/same-state update, concurrent eligible sleep
  with one winner, a longer class quiet floor, and internal manual bypass. The
  boundary test backdates only its disposable test row; it does not claim a
  real 190-second elapsed-time observation.
- [Independent fresh PostgreSQL](reviewer-postgres.log): the reviewer separately
  passed all 10 tests against a new disposable database.
- [Sidecar library](sidecar-fourth.log): 71/71 passed, including delayed first
  HTTP setup while a second request completes, setup expiry, saturation release,
  bounded/interruptible idle hints, uploads, delivery and health exclusion.
- [HTTP/2 ownership](h2-handoff.log), also [independently verified](reviewer-h2.log):
  an established preface waits three ordinary idle intervals before its first
  stream, then the one gRPC-shaped request retains drain through its open body.
  After body completion idle reporting resumes. Existing flow-control, early-upload and cancellation tests remain.
- [Proxy-core package](proxy-core.log): all 104 unit/integration tests passed.
- [Frontline and CP package](frontline-control-plane.log): 580 passed in the
  initial combined run, with deployed kind cases intentionally ignored. Its
  10 URL-gated PG wrappers are distinct from the actual database run above.
  [Current API/capability checks](api-tests.log) passed 19 + 2 after the advisory
  test addition. [Strict all-target clippy](clippy-final.log) is clean.
- [Controlled race failure](idle-race-red.log) restores the old fresh-watch retry
  behavior temporarily: the new same-poll request burst test observes a second
  report before a complete quiet interval and fails. The source was restored in
  `finally`; the [fixed run](idle-race-green.log) passes. Both production and
  generic idle-report loops retain the original watch.
- [Initial local socket attempt](sidecar-first.log) failed because the sandbox
  forbade binding sockets. The approved local-socket rerun exposed two fixture
  assumptions in [the next run](sidecar-second.log): unsupported fixture paths
  and an exact drain count of one where initial setup deliberately overlaps
  normal HTTP work. Corrected existing paths and explicit work-count semantics
  passed in the final sidecar run above.

The coordinator owns post-integration production-image rebuilds, repeated strict
load gates and deployed protocol/lifecycle gates. None is claimed for this
scratch source. The helper adds ownership only to the first sidecar HTTP request
on each connection; established relays and subsequent pooled requests gain no
new task. Existing established TCP/WS primitive timings do not measure this new
initial HTTP guard, so actual image/listener measurements remain necessary.

## Deployed fixture update, separate test-only packet

The [seven-file fixture patch](6j-deployed-fixtures.patch) changes:

- `kind_e2e_stateless.rs`: a distinct unrouted instance receives exactly one Wake
  RPC alongside the normal stateless lifecycle and no application traffic. Both
  activation floors overlap in real elapsed time. Polling rejects early
  Draining/Cold, requires the exact first Running generation and single sleep
  transition, and requires the abandoned instance to progress Running2 to Cold4
  within a finite readiness/floor/cleanup allowance. Request-start clocks precede
  Ready and therefore provide conservative lower bounds without mixing clock
  domains; the PG test separately proves the exact database Ready boundary. The
  original one physical cold and re-wake requests and framed-response checks
  remain intact.
- `kind_e2e_stateful.rs` and `kind_e2e_restart.rs`: existing automatic-Cold waits
  include the real 190-second activation floor. They do not backdate the clock;
  the restart scenario exercises the same persisted age across the actual
  control-plane restart. Current stateful framing/cancellation tests are retained.
- `kind_e2e_lifecycle_races.rs` and `support/ready_age_fixture.rs`: exactly three
  synthetic idle/drain/membership cases use a controlled test-only Ready age.
  The SQL first locks the exact Running instance generation, then changes only
  the timestamp of the exact cluster/namespace/instance Ready materialization
  with matching instance and projection generations. A row count other than one
  raises an exception and rolls back. Identifiers are restricted before SQL
  construction. The kubectl child has a 15-second cap, and the SQL statement a
  five-second cap. Existing Pod UID, membership, generation and cleanup checks
  remain, and membership failures must not carry an activation retry hint. These
  cases prove their Kubernetes invariants with controlled time; they do not prove
  a real 300-second wait or alter any production sleep API.
- `postgres_store/activation_idle.rs`: the real database gate exercises the same
  controlled fixture SQL, rejects wrong target/generation/projection without a
  timestamp change, and rejects unsafe identifier text. After proving the helper
  works, it restores exactly 190001 ms of age for the original concurrent sleep
  boundary check. Thus controlled fixture coverage does not replace the precise
  activation-boundary proof.
- `scripts/soak-kind-full-wake-sleep.sh`: existing cleanup and leak selectors also
  include the new abandoned stateless instance.

The final [fresh PostgreSQL run](postgres-controlled-fixtures-final.log) passed
all 10 tests, zero ignored. [Local fixture helpers](fixtures-tests-final.log)
passed 8 tests; six deployed tests are explicitly gated/ignored in that local
command. [All-target CP clippy](fixtures-clippy-final.log) and
[workspace format check](fixtures-fmt-final.log) are clean; `bash -n` passed for
the soak script. The reviewer requested retention of the exact 190001-ms boundary
and exact lifecycle generations; both are present in this final patch. Actual
kind execution, the real 190-second no-traffic observation, restart and soak
remain coordinator-owned deployed gates and are not claimed by these local logs.

## Integrated production gate

The exact independently approved production/local patch applied cleanly in the
root, preserving the newer stateful framing/cancellation, TLS diagnostic fixture
and intentional SNI regression files. The merged [sidecar suite](merged-sidecar.log)
passed 71/71, [actual frontline budget checks](merged-frontline-budget.log) 2/2,
and [sidecar API/capability tests](merged-api.log) 19 + 2.
[All-target clippy](merged-clippy.log) passed with warnings denied.
`git diff --check` is clean. No images or cluster gates were run during integration.

## Integrated fixture gate

The independently approved seven-file fixture patch applied cleanly after the
production patch and the separate6K connection fix. It preserves the current
stateful one-shot/cancellation checks, stateless framing and SNI code.
[Merged local fixture tests](merged-fixtures-tests.log) passed 8 tests with six
explicitly deployed-only ignores. [Merged all-target CP clippy](merged-fixtures-clippy.log),
[workspace formatting](merged-fixtures-fmt.log), `git diff --check` and the soak
script's `bash -n` check passed. Source and fixtures are stable for the coordinator's
serialized actual kind gates; this integration ran no images or cluster commands.
