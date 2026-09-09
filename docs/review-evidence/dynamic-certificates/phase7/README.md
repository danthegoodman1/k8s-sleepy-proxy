# Certificate snapshot simplification

This change replaces certificate event history with server-pushed authoritative
binding snapshots. It starts at `387161a` on `dynamic-certificates`; the earlier
[Phase 6 evidence](../phase6/README.md) applies to its recorded implementation.

## Architecture and guarantees

The certificate outbox, event insertion, history pagination, retention pruning,
reset protocol/metric and wire global cursor are removed. Publication, binding
mutation and removal each advance one global revision; affected bindings share
that revision. Key re-encryption preserves authorization revisions. A conditional
SQL snapshot checks the private global revision and skips the hostname join when
unchanged. Registration forces a complete snapshot; acknowledged empty interests
perform no further SQL until replaced. Route notifications are unchanged.

A durable `last_invalidating_revision` records binding mutations and removal,
including a same-ID rebind. Ordinary rotation preserves it. The proxy compares
that watermark with the actual retained configuration revision, so an
unbind/rebind/rotation sequence between polls cannot disguise lost authorization.
Whole-message validation, registration/incarnation correlation, stale-response
fences, fixed leases and cancellation ownership remain in place. There is one
watch response shape and no compatibility adapter.

Full snapshots trade event-log machinery for bounded repeated binding reads when
any certificate mutation advances the global revision. The load test below
measures that cost at maximum cardinality. It does not establish a universal
control-plane throughput guarantee or maximum encoded-payload throughput.

## Focused proof

The frozen implementation packet is
`.generated/dynamic-certificates/phase7/implementation-final/review.json`.
It records 276 source inputs, their hashes, exact commands and retained logs.
The aggregate `fb8f382b7b5927109130aac5ff4ce8be1aa908aab9a6f0b268bc596626b48f9f`
uses its documented path/file-hash algorithm. Focused results include 41 local
and 11 actual PostgreSQL successes. The retained subset command contains an old
drain-test failure; its corrected focused case passes separately. It is not
presented as an all-green aggregate command.

- Cache tests cover same-ID rotation, coalesced destructive transitions, held
  stale `Found`/`Unchanged`, malformed snapshots without partial mutation,
  registration acknowledgments and eviction incarnations.
- Real PostgreSQL tests cover atomic conditional snapshots, CAS/rollback,
  shared-host updates, same-ID binding, removal and two-replica delivery. EXPLAIN
  of the exact production statement shows zero hostname-relation loops for an
  unchanged global revision and one for a forced snapshot.
- The cancellation test drives a queued read while SQL remains blocked, proves
  ordinary progress, and observes a different backend after bounded local drain
  releases capacity. It then releases the blocker and joins the new read. It
  does not assume local connection discard immediately cancels remote SQL.
- Decoder tests cover a 419,851-byte maximum valid response and a 524,274-byte
  adversarial message. The conservative decoded envelope is 29,884,416 bytes,
  within the existing 32 MiB watch reservation. Total accounted memory remains
  64 MiB and concurrent fetches remain three.

The focused native load run uses 16 distinct certificates and 16,384 actual
bindings, with disjoint 1,024-host interests on each of 16 streams. The watched
service uses the minimum two-connection database pool. Each of idle, unrelated
mutation and related mutation phases performs 32 ordinary instance reads and 32
route subscriptions at a requested 100 ms interval. Mutations are real one-shot
conditional store operations. Initial ACKs, final related revision convergence,
and complete cancellation/readmission are bounded by three seconds; each complete
ordinary operation is bounded by two seconds.

The latest focused run delivers zero idle snapshots, 62.83 aggregate snapshots/s
under unrelated changes and 63.97/s under related changes. Maximum ordinary
latencies are 9.15 ms for instance reads and 25.96 ms for route subscriptions;
maximum initial ACK is 240.85 ms. These are measured delivery rates across short
phase windows, not idle query counts or a promise that every stream polls at 4 Hz.
The related phase proves final per-host convergence; the unrelated phase proves
progress. Generated hostnames are short; encoded size is tested separately above.

The independent reviewer approves this frozen source and focused evidence with no
remaining actionable findings. Verification includes all 276 source files and
retained copies, 17 log hashes and the exact changed-source inventory.

## Final integration

All twelve local gates pass on unchanged source in
`.generated/dynamic-certificates/phase7/gates-20260909T155245Z/record.json`:
formatting, strict all-target workspace Clippy, workspace, actual PostgreSQL,
dependency boundaries, inventory/checker tests, protocol fixture tests, isolated
load-helper checks and load/Criterion budget checkers. Workspace reports 901
passes: 878 local executions and 23 optional database wrappers; 17 deployed or
subprocess cases remain explicitly ignored. The separate complete PostgreSQL
target executes all 24 cases with zero failures, skips, ignores or filters in
66.30 seconds. Its shared execution checker accepts the complete inventory.
The raw-content source aggregate is
`0b710e68141aeb8a9fff02c9b8e0ec3d9fa5c55b7e7e903285a9d06ab62211e0`;
this uses a different documented hashing convention from the focused packet.

The independently reviewed image build and packaging smoke pass against all
169 unchanged production inputs. Build record:
`phase7/image-build-20260909T155516Z/record.json` under the same generated-artifact
root. All three images are ARM64 and run as UID 65532:

| Component | Image ID |
| --- | --- |
| Control plane | `109fd1b5a38cd4d215ae84cfc8d1dfab8a79505f580caad98e024847be9668b8` |
| Frontline | `323489c23f4306a9584b306c018deb62e849033d1f52fe78eafa9c90dfaa1571` |
| Sidecar | `719ac400b471cca10f493057423588e350856b28942b4064e43b146f9e2735e5` |

The first expanded image run failed in cycle 3's first-proxy flood with a bare
local `ConnectionRefused` error. It completed only cycles 1 and 2; the real sleep
and lease-outage checks were not reached. Both Frontline container IDs/PIDs remain
unchanged in the sampled failure window until namespace cleanup. The original
fixture deleted its per-Pod forwarder logs, so the refused port and forwarder exit
cause cannot be established from that run. Its complete failed record is retained
in `phase7/expanded-final-20260909T155846Z`, with corresponding public artifacts
and sampled RSS. This is not a passing resource gate or evidence for a runtime fix.
The fixture now adds operation/port/Pod context and retains at most 64 KiB of
forwarder output with child status observed before cleanup. On failure, the
release script captures bounded public pod status, logs and events before deleting
its namespace. It records no Pod specifications, environment or Secrets, and adds
no retries or wider deadlines. Focused Rust, shell-capture and strict checks pass.
The earlier full local result retains its pre-diagnostic test-source scope; all
169 production inputs are unchanged.

The unchanged-image rerun passes in 641.62 seconds, including both basic TLS
cases and the 573.48-second dynamic lifecycle case. The latter verifies both
replicas, HTTP-01 before publication without wake, live HTTP/2 and WebSocket
connections across rotation, rebinding/removal, denied session reuse, lost-update
recovery, certificate expiry, three resource cycles, real application sleep and
re-wake, and cold/warm recovery after a control-plane outage. The warm proxy denies
new handshakes after 303.44 seconds of outage; a cold proxy reaches its setup
deadline without becoming ready. This is fresh passing evidence, not an
explanation of the first run's refused connection.

The rerun's source record is `phase7/expanded-rerun-20260909T161750Z/record.json`;
its raw-content aggregate is
`4cddfb0a258526b361cd88884058c650d2a3460489bb97e93d432b762ab1d772`.
The corresponding `expanded-rerun-public-20260909T161750Z` directory retains
lifecycle, resource and forwarder records. The joined sampler and unchanged
analysis pass in `expanded-rerun-combined-20260909T161750Z`: both exact proxy
processes peak at a sampled 17.88 MiB, with 0.375 MiB cycle-3 tail growth over
cycle 2. All six seven-second tails contain six or seven samples spanning at least
5.06 seconds. Original limits remain 256 MiB sampled peak and 8 MiB final-tail
growth. These are observed bounds for this workload, not unsampled allocator peaks.
Warm flood traffic records 5,376/5,418 successes, 42 refusals and zero timeouts;
miss traffic records 900/900 denials and zero timeouts. All 192 subsequent warm
recovery probes pass. The refusal cause remains unproved; this establishes bounded
progress and recovery, not rejection-free overload service.

Strict production-image Frontline load passes all original comparative budgets
with 200 requests per measured path at concurrency eight and zero failures. HTTP/1,
h2c, negotiated h2/TLS, generated gRPC and WebSocket throughput ratios are 0.990,
0.995, 0.899, 0.797 and 0.979; maximum added p99 is 2.374 ms against a 25 ms limit.
Warm paths perform no route subscriptions; the synthetic cold wake performs
exactly one subscription, wake and backend request in 2.166 ms. These are short
fixture comparisons, not sustained production throughput measurements. Record:
`phase7/strict-frontline-load-20260909T162849Z/record.json`.

Three sequential warm TLS runs on the same ARM64 machine and Rust 1.98.1 produce
219.81, 220.00 and 221.11 microsecond means. Their median is 220.00 microseconds,
9.63% below the untouched Phase 1 median of 243.45 microseconds. The original 15%
warning and 25% failure thresholds pass. The benchmark uses verified full TLS 1.3
with P-256, h2, one connection on a current-thread runtime and no extra certificate
fetch after warm-up. No other builds or test workloads run during timing. Record:
`phase7/tls-after-20260909T162856Z/record.json`, including the frozen executable,
source hashes and immutable baseline hash.

The independent reviewer approves the deployed, RSS and performance evidence.
Source archives, actual image identities and raw measurements match, with all
original thresholds preserved and the earlier refusal explicitly unresolved.

All six sequential final checks pass in
`phase7/final-checks-20260909T162849Z/record.json`. In addition to the load and TLS
measurements above, routing/HTTP-01, HTTP/2/gRPC/WebSocket, control-plane restart
recovery and real libpq direct-SNI cold wake pass. The libpq client obtains the
expected query response, while an unbound SNI is rejected. Restart recovery covers
wake, sleep, deletion and notification-driven route reassignment.

Release records are `routing-final-20260909T162938Z`,
`protocols-final-20260909T163108Z`, `restart-final-20260909T163218Z` and
`libpq-final-20260909T163851Z` under `phase7`. Source hashes remain unchanged.
The first three have corresponding `identities-*` records with verified running
control-plane, Frontline and sidecar image IDs and no sampler errors; libpq retains
its public Pod identities. The owned PostgreSQL container and kind cluster are
removed, with unrelated containers preserved (`phase7/final-cleanup.json`).

Final independent integration review approves the scoped source, documentation,
all release results and cleanup for commit and PR update, with no actionable
findings. It verifies all 277 current source hashes, 169 production inputs,
release log hashes, 26 running production observations across routing/protocol/
restart, and the narrower control-plane/Frontline identity record for libpq.

Publication and hosted CI are tracked in
[PR #2](https://github.com/danthegoodman1/sleepypods/pull/2), whose description links
the exact published commit's CI result. This document records local source/image
evidence; a previous commit's CI result does not validate a later branch head.
The current phase ledger remains in the
[implementation plan](../../../dynamic-certificates-plan.md#phase-7-authoritative-snapshot-simplification).
