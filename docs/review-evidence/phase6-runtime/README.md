# Phase 6C/6E/6F/6G and control-plane 7D

Implementation review packet, 2026-09-07. No commit or push. Prior atomic intent,
projection generation, conditional mutation, retention and attempt/effect fences
remain in force. Independent approval and coordinator deployed gates are tracked
in the main plan; this packet describes the focused implementation and evidence.

## Runtime and failure policy

A continuous bounded scheduler discovers work while previous jobs wait for
readiness. PostgreSQL orders eligible deletion before wake, and the driver
reserves a deletion slot. Eight never-ready wakes with batch size two cannot
starve an accepted healthy deletion. Transient failures persist backoff (2–32 s),
count, bounded reason and operation deadline. Definite forbidden/invalid errors
are permanent immediately; readiness/transient failures retry until the persisted
deadline. Wake terminalization publishes Failed and queues safe cleanup; its
original reason survives cleanup and an ordinary wake can retry after cleanup.
Cleanup failure or uncertainty retains inventory and reservations.

The runtime owns controller, dispatcher, maintenance and listener tasks. Every
controller exit funnels through cooperative cancellation and owned draining,
including fatal scans/finalizers and child panics. Known-unsent begin completes
and receives exact ACK before lease release. After dispatch, cancellation remains
uncertain. Controller/runtime drain deadlines are 20 s/25 s; database or process loss
that prevents owned cleanup still retains the conservative barrier. There are no
Drop-spawned cleanup tasks. SIGTERM is tested in an actual subprocess. Critical
failure stops other components and propagates; errors during shutdown are also
reported after the other tasks drain.

Admin ReconcileMaterialization enqueues the same durable scheduler and returns
status. status_only=true performs inspection without scheduling; the real-PG
operator API test checks failure/deadline and uncertain generation/owner/attempt/
effect/ref/expected UID/resourceVersion. Aggregate uncertainty/blocked gauges have
no instance labels. Production run metrics represent discovery/scheduling scans.

## Durable notification proof

Migration 10 records semantic route/instance/materialization changes in the same
transaction, including bundled route creation. Statement transition tables reserve
one revision range through a locked singleton row; later transactions cannot
publish an overtaking revision. Rollback publishes nothing, and lease-only writes
emit no event. Every CP tracks its own cursor and receives each committed change.
Normal events target dependencies, including route-ID changes and exact/wildcard/
path precedence; unrelated lifecycle churn does not reset hot subscriptions.

History is bounded to 100,000 records. Ten-minute TTL collection removes only a
revision prefix, protecting readers from clock-skew interior holes. Gaps and
failed dispatcher reads reset subscribers. Default poll 250 ms, batch 1024; healthy
unbacklogged delivery is one interval plus database/runtime scheduling, while
backlogs take additional batches. Positive/negative fallback TTLs are 10 s/1 s.
Five consecutive dispatcher/maintenance errors fail supervision. Maintenance runs
every 5 s in bounded batches and never removes permanent generation watermarks.

The real-PG gate blocks transaction B behind uncommitted A, observes no early
revision, commits both and checks order; it also runs two actual independent
production dispatchers, rollback, skew retention and bulk 100,003 INSERT followed
by bulk UPDATE/DELETE. The current bulk gate completed in 3.016 s with the full
100,000-route scale test running in the same database test process. The original
row-trigger draft had quadratic transaction overhead and was replaced; it is not
the final implementation.

## Resource ownership and recovery

Both listeners share accepted-socket and RPC semaphores. Defaults: 256 sockets,
128 unary RPCs, 64 subscription streams, 256 entries/stream, 32 H2 streams/connection,
256 KiB decode/1 MiB encode limits. Setup and pending writes are bounded 5 s, with
independent write/flush/shutdown timers. Cancellation safely remains latched
across repeated I/O polls.

Unary permits remain in the response body and final queued Bytes. An owned
watchdog retains its own permit until teardown. A 5 s total unary delivery deadline
closes a responsive H2 peer that withholds credit. Subscriptions have their own
permit and 60 s lifetime; they do not consume the unary budget for that lifetime.
Producer, body, queued Bytes and watchdog each retain ownership until their work
is dropped. Public tonic/Hyper cannot individually reset an already queued final
DATA frame, so deadline cleanup cancels the whole connection, including its other
streams. The actual H2 gate uses 1 byte initial credit and a 4,096-byte EOS frame to
prove final queued-data ownership, overload rejection and subsequent recovery
for both unary and subscription paths while the peer responds to PING.

Subscription producers bound lookups 3 s and output enqueue 1 s with 16 queued
responses. Producer timeout is not a promise that transport capacity returns in
1 s: queued H2 data can retain it until the separate subscription deadline.
Tests cover oversized fields, stream/entry admission before lookup, slow lookup,
unconsumed output, expiry/drop recovery, subscribe-before-snapshot event ordering,
unrelated churn, and service-value drop after headers. The latter exposed a real
broker sender lifetime race; the owned producer now retains the broker.

## Verification

| Gate | Result | Evidence |
| --- | --- | --- |
| Full CP package | 231 library, 46 operator, 41 proxy, 17 sidecar and mandatory-capability test passed before the final three focused regressions | [Package](control-plane-tests.log) |
| Actual PostgreSQL 17 | Implementer 8/8; final independent 9/9 including 7C retry boundary | [Implementer](postgres-tests.log), [Independent](reviewer-postgres.log) |
| Admission and reconciler | Independent 2/2 admission and 25/25 reconciliation | [Admission](reviewer-admission.log), [Reconciler](reviewer-reconciler.log) |
| Actual listener ownership | Final independent 5/5, including encoded gRPC-web final bytes | [Delivery](delivery.log), [Independent final](reviewer-runtime-io-final.log) |
| Runtime supervision | Independent 4/4 plus ignored child-only fixture, including actual SIGTERM and failed dispatcher with active subscription | [Supervision](reviewer-supervision.log) |
| All-target CP clippy | Passed with warnings denied | [Clippy](clippy.log) |
| Whitespace/script syntax | git diff --check and both modified kind scripts pass | Implementer command output |

Package execution without a database URL skips environment-dependent database
bodies; the separate PostgreSQL logs execute those bodies in disposable real
containers. The final listener fixes preserve ownership through gRPC-web's fresh
base64 allocation by placing admission outside the encoder, while CORS stays
outside admission. Actual text negotiation and overloaded response Content-Type
are checked. The producer/broker and watchdog permit teardown fixes have focused
regressions; root's final workspace run will cover their final shared composition.

Kind fixtures explicitly set a 30 s operation deadline for terminal-failure cases,
accept asynchronous StillWaking during concurrent wake and allow autonomous failed
projection cleanup while still requiring no ready backend. Coordinator owns final
production-image, restart at 1/2 CP replicas, lifecycle/failure and all-platform
gates. These are separate from focused CP implementation approval.
