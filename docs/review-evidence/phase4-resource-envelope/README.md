# Route resource envelope

The actual frontend listener and shared route coordinator passed the predeclared resource experiment: 5 warmup cycles followed by 10 measured cycles. All 40 measured wave recoveries returned to exactly two live Tokio tasks and zero accepted sockets, request permits, handshake permits, drain work, or active fake control-plane calls. Final listener/coordinator shutdown also reached zero tasks and zero ownership before printing `PASS`.

This packet adds the standalone [example](../../../crates/frontline/examples/route_resource_envelope.rs) and evidence only. It changes no production behavior. The example was handed off before its first measurement; no experiment thresholds or source changed during this run.

## Reproduction and retained inputs

Run from the repository root on a host with loopback sockets and `ps` available:

```sh
cargo run -p frontline --example route_resource_envelope --locked --offline
```

The measured invocation used the development profile, a two-worker Tokio runtime, macOS 26.6.2 on arm64, and rustc 1.97.0. [Metadata](metadata.json) retains the exact command, platform, exit code, elapsed time, and before/after source digests and per-file hashes. The included data-plane source/manifests/protobuf graph was unchanged (digest `917567cc25fafa7212b3c32fc61113912ba55de6f55b5f312b3e5caebe1d81ec`). The graph includes workspace manifests but does not claim a snapshot of unrelated control-plane implementation sources. [Source as run](source-as-run.rs) and [toolchain details](rustc.txt) are retained.

The isolated library constructor uses no-op telemetry. The production binary's default filtered stderr sink is outside this measurement. Root-owned kind gates ran concurrently; this is a process resource experiment, not an isolated throughput comparison. The invocation exited 0 in 93.809 seconds, including a 3.97-second Cargo build. This lane ran no images or cluster commands.

## Workloads and unchanged gates

Every cycle runs these four waves through the production HTTP listener/coordinator. Clients own their HTTP driver tasks, use framed bounded bodies, and close their connections. The fake control plane blocks actual route lookup futures at a semaphore barrier; it does not substitute for listener or coordinator behavior.

| Wave | Callers | Exact held route lookups | Exact held request permits | Expected overload responses |
| --- | ---: | ---: | ---: | ---: |
| Distinct identities | 128 | 64 | 64 | 64 |
| Same identity | 384 | 1 | 256 | 128 |
| Canceled callers | 128 | 64 | 64 | 64 before cancellation |
| Recovery after cancellation | 128 | 64 | 64 | 64 |

The barrier requires every caller to reach its expected held/overload state, the exact lookup count, and the exact held request count. Overload responses must be HTTP 503. Accepted released lookups must return route misses. The cancellation wave aborts and joins all client tasks without releasing the fake lookup gate; the real production 5-second Subscribe deadline must reclaim that work, followed immediately by a successful recovery wave.

Before and after each wave, quiescence requires two consecutive samples 50 ms apart with the exact task baseline and all ownership counters at zero, within 6 seconds. The task baseline is the listener and coordinator actor, not an OS thread count. The sampled task ceiling is `baseline + 4 * callers + 2 * 64 + 16`: 658 tasks for 128 callers and 1,682 for 384 callers. It covers clients, owned HTTP drivers, accepted connections, response delivery tasks, and route/control-plane tasks.

The RSS gates were fixed before the run: the maximum quiescent RSS of the last three measured cycles may exceed that of the first three by at most 8 MiB (8,192 KiB); the span across all measured quiescent samples may not exceed 16 MiB (16,384 KiB). Five warmup cycles are excluded from these gates. The experiment has a 150-second outer bound.

## Results

| Check | Observed | Predeclared bound |
| --- | ---: | ---: |
| First three measured cycles, maximum quiescent RSS | 30,928 KiB | Reference |
| Last three measured cycles, maximum quiescent RSS | 31,520 KiB | Reference + 8,192 KiB |
| Tail growth | 592 KiB | 8,192 KiB |
| Measured quiescent RSS span | 960 KiB (30,560–31,520) | 16,384 KiB |
| Distinct wave sampled task maximum | 511 | 658 |
| Same-identity wave sampled task maximum | 1,051 | 1,682 |
| Cancellation wave sampled task maximum | 503 | 658 |
| Recovery wave sampled task maximum | 513 | 658 |
| Measured quiescent task count | 2 in all 40 rows | Exactly 2 |
| Measured quiescent ownership counters | All zero in all 40 rows | Exactly zero |

[Raw stdout](stdout.log) retains every one of the 180 rows, the computed RSS summary, and final `PASS`; [stderr](stderr.log) retains build/run output. [CSV](samples.csv) contains all rows without the non-CSV summary, and [analysis](analysis.json) records an independent parse of exact held counts, zero-ownership recovery, and the RSS/task summaries. There are 120 measured rows (idle, held observed maximum, and quiescent for each of four waves across ten cycles), including the 40 measured quiescent rows. Warmup rows are retained too. Task/RSS maxima are sampled observations, not continuous peak measurements.

Scoped validation passed against this source: [clippy](clippy.log), `cargo clippy -p frontline --example route_resource_envelope --locked --offline -- -D warnings`; [format check](fmt.log), `cargo fmt -p frontline -- --check`; and `git diff --check`.

## Interpretation and limits

This one host-local run demonstrates bounded admitted route work, exact overload behavior, cancellation reclamation, immediate recovery, and no growing retained task/permit count across the selected waves. Process RSS includes the in-process clients, HTTP drivers, allocator behavior, and fixed measurement storage. The example keeps only 40 measured quiescent values, with no per-request event history.

This is not a production memory SLA, throughput comparison, or proof about arbitrary-duration allocator behavior. Miss-only traffic isolates listener/route/cache ownership; it does not exercise positive backend routes, pooled upstream connections, active response bodies, WebSocket/TCP sessions, or the production telemetry sink. Drain remains zero while these routes wait because no backend work has begun. Separate existing protocol/admission/pool and production-image gates cover those paths; this experiment does not replace them.
