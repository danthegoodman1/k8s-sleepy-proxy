# Phase 4 evidence: certificate watch convergence

Starting commit: `9d0cfa946802e906ade77900bd9d49ab7bf1b7a4`. The frozen
implementation contains 217 enumerated Cargo/crate files. The initial focused
manifest is `cb70b98d7ea25e2b68822a70a54ef1894139462fd24fe52cfecbdab972e5d954`;
the outbox test correction produces
`2286bd2940361c643b944d8aa55ced8f45d94c94db771b78a01fb9513f495a4e`.
The final source, including the sidecar test correction below, is
`49cf4e8ad12588f5c450adcae09ac57a92e5495f29564a5100e5c4268c982472`.
The exact file manifest, commands, log hashes and implementation packet are in
`.generated/dynamic-certificates/phase4-focused/`. The coordinator process proof and final broad checks pass on that source.
Independent whole-phase approval is explicit; no actionable findings remain.

## Implementation and review resolutions

One lazy, bounded native bidirectional watch carries complete hostname interests
and metadata-only snapshots/changes/resets. A single database statement captures
current binding metadata and the global durable cursor. Each stream then polls
committed certificate events. There is no additional process-local broker or
history cache, and synchronization never decrypts private material.

Per-host revisions, fetch generations and cache-entry incarnations serve distinct
purposes: authority fencing, stale fetch rejection, and protection against an old
registration affecting a newly admitted entry after eviction. The global cursor
only orders durable history. Review caught an early return that discarded an
older event page even when it contained a newly received removal for another
hostname; the implementation now applies individual host revisions and retains
the maximum global cursor. A two-host regression covers that ordering.

A received removal, unbind or rebind clears the affected configuration before
fetch admission. A same-certificate publication may retain the valid previous
configuration while replacement is fetched, within its original lease. Reset,
reconnect, duplicates and errors never renew that lease. Tests hold replacement
work and check both the old negotiated peer and unchanged expiry after the
notification has demonstrably been processed.

The server admits 16 certificate watches separately from unary material RPCs.
Initial registration has a 3s deadline, each stream lives at most 60s, and
registration replacements consume the same minimum 250ms spacing as polling.
Two output slots and a 1s delivery deadline bound a stalled client. One
store-owned watch-read slot remains held through SQL checkout and cancellation
drain/discard, preserving ordinary work capacity on a two-connection pool.
Review removed a redundant broker semaphore and prevented slow delivery from
holding shared query admission. Transient store admission retains the pending
snapshot/cursor for a later poll rather than amplifying reconnects.

These limits permit at most 64 metadata poll starts/s per control-plane process;
each successful operation also performs the existing guarded checkout drain.
This is an admission bound, not a measured database throughput guarantee.

## Memory envelope

Review found that wire length alone did not justify Phase 3's decoded-memory
reservation: many empty repeated protobuf messages can allocate much more RAM
than their encoding. The final implementation keeps the 64 MiB total and reserves
4 MiB for structures, 32 MiB for the watch, and 8 MiB for each of three concurrent
fetches. Cache entries and retained parsed configurations consume the remainder
of the same budget. Fetch permits include completed work awaiting reaping; the
outer worker retains the watch reservation until both owned futures exit.
Attaching a watcher reserves its budget synchronously or returns capacity error.
An empty HTTP-only cache opens no watch.

The generated Prost decoder is exercised with adversarial messages under its
separate 256 KiB unary and 512 KiB watch limits. The watch fixture includes an old
Snapshot while a replacement Changes value grows; measured decoded capacities,
string storage and a conservative old-buffer allowance during moving Vec growth,
plus wire/bookkeeping, derive a peak of 36,830,208 bytes, below the combined
36 MiB watch/structure reservation. The unary fixture uses 262,028 wire bytes
with 32,700 old Found chain entries and a replacement Unchanged value; its
moving-growth peak is 7,077,888 bytes, or 7,864,320 including wire/bookkeeping.
Sequential validated-domain installation peaks below that 8 MiB reservation.
These are analytical peaks derived from actual generated-message capacities,
not allocator instrumentation or RSS measurements. No custom protobuf parser or
allocator was introduced. Phase 5 separately measures process RSS and load.

## Focused validation

The implementer packet records 47 passing focused executions: API 15, production
watch producer 3, store capabilities 2, actual PostgreSQL watch cases 4, Frontline
cache/watch/TLS cases 22, and native TLS delivery composition 1. Formatting,
strict all-target Clippy for the three affected packages, and the isolated
Frontline load example check pass. The four actual PostgreSQL cases report
4 passed, 0 failed, 0 ignored, no self-skip, in 9.64s against the owned
PostgreSQL 17 container. Source hashes remain unchanged across the recorded gates.

Coverage includes two independent store/API replicas sharing committed changes;
verified native TLS and role/admission boundaries; largest valid interest sets
and oversized input; all 16 active polling streams on a two-connection pool while
native GetInstance and route Subscribe progress across three mutation rounds;
canceled SQL remaining accounted through drain; and cancellation/readmission of
all streams. Frontline tests cover publication after cached Missing, held stale
resolve/replacement responses, removal/rebind races, reordered and duplicate
pages, global reset versus older per-host revisions, invalid renewal/outage hard
expiry, eviction/reconnect and owned teardown.

The native TLS/H2 withheld-delivery test uses production BoundedIncoming and a
controlled response service for Wake, Subscribe and Watch. Concrete API/database
behavior is proved separately; neither scope is represented as the other.
Initial producer-test failure required a decoded-input barrier in the fixture;
a capacity assertion was updated from four to three after the accounting change.
The retained failed attempts are not attributed to production defects.

## Coordinator process and broad gates

The [operator probe](runtime-probe.rs) and local runner
`.generated/dynamic-certificates/run-convergence-runtime-probe.py` passed at
`.generated/dynamic-certificates/phase4/runtime-20260908T225350Z` with the same
final source hash before and after. The runner builds two actual debug binaries
and a native operator client, retains exact executable/source/manifest copies,
and uses one isolated PostgreSQL schema. Each Frontline connects to a distinct
control-plane process and starts in an empty directory with only a proxy token
and public platform trust; neither receives an application key-file setting.

Thirteen measured convergence cases pass, the slowest 0.476s after the mutation
RPC completes. Clients verify actual peer fingerprints, hostname/trust, TLS 1.3
and h2; both proxies additionally pass TLS 1.2. Initial H2 SETTINGS complete both
sides' handshakes without making an application request. Two held H2 connections
reply to PING after certificate rotation. The matrix covers cached Missing to
shared publication, rotation through the other replica, invalid replacement,
certificate A version 3 to B version 1 rebind, removal, unbind and restoration.
After CP0 stops, its proxy still serves a valid warm view while CP1 publishes a
replacement. The runner deletes only its owned schema's outbox to simulate
pruned history, restarts CP0, and verifies both the new peer and an unchanged
host whose authoritative revision predates the global cursor. A subsequent
removal makes both proxies refuse new TLS connections.

All five process lifetimes exit cleanly with SIGTERM, both empty Frontline
working directories remain unchanged, and log scans find no test private-key
markers. The temporary keys and database schema are removed. Peer fingerprints,
commands, startup inputs and timings are retained in the runtime record.

| Artifact | SHA-256 |
| --- | --- |
| Control-plane executable | `385269ebe0696802e5f40dcd124c8d1cec7c8fc8d0e442c4038a69a8f6ef0dfe` |
| Frontline executable | `811b8a4be292296efa7524b8e8c0c561f872dc4acefbfee60b48e8462959f7e5` |
| Operator executable | `61e0357d691f925610ed9c3ef4d49a02839a3eb449d48366a6de70b1d4efddf9` |
| Operator source | `97decbda3ab69578898137ab0d9f79edca44ccf4eba63da144cbbaf775885fd0` |
| Process runner | `1c17dbd9bbd8b6ffde319455f582e91486b6cd791357c9f63ac2dcecc3856cf2` |

This proves process-level convergence, not a Kubernetes lifecycle or performance
load result. Production images, default five-minute outage enforcement, resource
load measurements and matched performance remain Phase 5 gates.

The first coordinator broad run at
`.generated/dynamic-certificates/phase4/gates-20260908T224032Z` passed formatting,
strict workspace Clippy and 46 workspace summaries (887 reported passes,
including 23 optional database self-returns; 864 local executions, 16 ignored).
The separate actual database gate failed: 23 passed and
`postgres_certificate_outbox_commit_order_rollback_and_retention` returned
`certificate watch read capacity exhausted`. The test performed back-to-back page reads without allowing the first read's
asynchronous PostgreSQL protocol drain to release watch admission. Its page and
reset observations now use the existing bounded read-only retry policy; all
mutations remain direct and one-shot. Exact ordering/rollback/count assertions
remain, and invalid page limits must specifically return InvalidArgument.
The original target reproduced in 0.24s (0 passed, 1 failed), then passed in
0.78s after correction. Formatting and scoped strict Clippy also pass. The
`.generated/dynamic-certificates/phase4-outbox-remediation/` packet verifies that
only this test file changed among the 217 enumerated files; production code and
ownership are unchanged. The failed raw run is retained and the final broad rerun is recorded below.

The next broad attempt, `gates-20260908T224835Z`, passed formatting and strict
workspace Clippy, then stopped at the sidecar binary test
`occupied_metrics_port_shuts_down_and_joins_both_listeners`: it saw AddrInUse
without shutdown. Source inspection by implementer and reviewer narrows this to
an early proxy/readiness bind before supervised listeners start. The fixture
released its two reserved ports before production rebound them; a competing
endpoint can cause that early error. The natural competing owner in this run is
unknown. The controlled reproduction holds a competing listener across that gap and
reproduces the old assertion failure in 0.01s. Only this test now uses port 0 for
proxy/readiness, allowing production binds to choose and own endpoints atomically.
All 10 sidecar binary tests pass in 3.31s, and strict scoped Clippy/formatting pass.
The metrics listener remains deliberately occupied; the exact AddrInUse error,
shutdown assertion and one-second joined-return budget remain. The reviewer
approved this test-only delta. Exact original/controlled source, logs and final
217-file manifest are in
`.generated/dynamic-certificates/phase4-sidecar-port-remediation/`. No production
startup behavior, deadline, serialization or injection hook changed.

## Final broad result

All five coordinator checks pass at final source `49cf4e8a`, with identical source
hashes before and after each command. The exact commands, output and durations
are in `.generated/dynamic-certificates/phase4/gates-20260908T225430Z/`:

- `cargo fmt --all --check` and strict workspace/all-target Clippy pass.
- `cargo test --workspace --locked --offline`: 46 summaries, 887 reported passes,
  0 failures and 16 explicit ignores (15 deployed tests and one subprocess fixture), in 36.14s including invocation
  overhead. Of these, 23 optional PostgreSQL wrappers return without a database;
  the local execution count is 864. These returns are not database evidence.
- The separate actual PostgreSQL target reports 24 passed, 0 failed, 0 ignored,
  no self-skips, in 65.84s test time. This comprises 23 database cases and the
  invalid-connection-URL case, against the owned PostgreSQL 17 container.
- Dependency boundaries pass: Frontline 130, sidecar 111, control plane 196
  unique normal dependencies.

Earlier failed broad attempts above are superseded by this final run, with both
causes addressed by reviewed test-only corrections. Source/focused approval and
process-proof approval are explicit. Phase 5 retains the deployment, metrics,
resource/RSS and matched performance obligations.

The independent reviewer approved whole-phase closure after verifying source and
artifact hashes, raw counts, process peer evidence, both test-only corrections
and the current contracts. Phase 4 is complete and ready for its stage commit.

## Hosted follow-up

[CI run 34288517173](https://github.com/danthegoodman1/sleepypods/actions/runs/34288517173)
on commit `5a337f3078e4ae424eafb590c4eb1f9b8a705bb3` passed formatting, lint,
workspace tests, dependency/inventory checks and isolated examples, but its
actual PostgreSQL target failed: 23 passed, 1 failed, 0 ignored, in 72.51s.
`postgres_sixteen_registered_watches_poll_with_two_connection_pool_and_ordinary_native_work`
reported a closed watch stream. The full raw log is retained at
`.generated/dynamic-certificates/phase4/hosted-ci-34288517173.log`.

Controlled protocol-delay experiments establish fixed-phase starvation with both
the raw store and production read-retry wrapper. The reviewed bounded FIFO
correction also accounts for the overlap between a completed logical read's
detached drain and its producer's next call. Same-relay red/green results,
cancellation ownership and unchanged deadlines are recorded in the
[Phase 5C correction evidence](../phase5/watch-admission.md). The original hosted
run's exact network/scheduling delay remains unknown. Correction commit `004505f`
passes exact-source broad gates and
[hosted CI 34292414386](https://github.com/danthegoodman1/sleepypods/actions/runs/34292414386),
including actual PostgreSQL 24/24 with no skips or ignores. Gate 4G is closed
again. The local passing results above retain
their exact scope; final integration and production-image confidence require the
remaining gates.
