# Final validation and performance evidence

This packet preserves the merged 6L workspace/PostgreSQL checkpoint, three serialized rounds of strict current data-plane load, and the latest matched primitive comparison. Subsequent fixture-only additions are covered by the [latest validation inventory](final-validation-summary.json), which records 838 reported passes / 827 local executions and the separate actual PostgreSQL 12-entry run. The performance and PostgreSQL results below retain their exact source scope. The canonical integration README and remediation ledger remain coordinator-owned.

[Machine-readable summary](final-6l-validation-summary.json) contains the exact per-target test/ignore inventory, commands, run metadata, workload variables, raw parsed measurements, thresholds, and Criterion change confidence estimates. Earlier comparisons and failures remain intact; this packet does not overwrite the historical `primitives-*`, `routing-*`, or `strict-default/` evidence.

## Merged source validation

All four final checks exited 0 with the same before/after source snapshot within each run: [metadata](final-6l-workspace/checks.json), [format](final-6l-workspace/fmt.log), [workspace/all-target clippy with warnings denied](final-6l-workspace/clippy.log), [workspace tests](final-6l-workspace/workspace.log), and [dependency boundaries](final-6l-workspace/dependency-boundaries.log). Normal dependency counts remain frontline 111, sidecar 102, and control-plane 174, with the enforced boundaries intact.

Cargo reported **822 passed, 0 failed, 16 explicitly ignored**. Ten of those reported passes are PostgreSQL wrappers that return successfully when `SLEEPYPODS_POSTGRES_URL` is unset, so the workspace invocation exercised **812 local test entries**. The ignore inventory is 15 explicitly gated kind tests and one subprocess fixture entry; the exact names/reasons are retained in the summary. The local total includes one control-plane documentation test. A Cargo pass for a URL-gated early return is not counted as database conformance here.

The separate [real PostgreSQL run](final-6l-postgres.log) passed **11 tests, 0 ignored**: ten actual database tests plus the invalid-connection-URL test. Its [metadata](final-6l-postgres.json) records a disposable `postgres:17-alpine` database, 17.524 seconds total (8.11 seconds in the test driver), stable source snapshots, and identical before/after Postgres-container inventories. The preexisting container remained and the run left no added container. Its ten database tests exercise the same wrappers excluded from the local count; these totals must not be added as unique coverage. The log includes the new 6L pending-wake supersession cases reaching absence, as well as conformance, migrations, real replacements, activation-floor persistence, runtime retention and the 100k-route checks.

The workspace digest is `0ecb5f4e98955557c790a9216493115b068ea2b680afeaf9552e281e38025af7`; the PostgreSQL runner's recorded digest is `813687e6b250967c5feadc9d473e220465513e74095ae9b1cf9b2a3adb1edd99`. Each is compared only with its own runner's after-snapshot; they are not treated as interchangeable scope definitions.

The coordinator also ran the two load-client examples independently, avoiding workspace feature-unification ambiguity:

```sh
cargo check --locked --offline -p frontline --example frontline_load_smoke
cargo check --locked --offline -p sidecar --example sidecar_load_smoke
```

Both passed (reported 2.08 and 1.42 seconds). Their evidence source is coordinator tool output from session 81338, which ran the two commands joined with `&&` and exited 0. No standalone command log was captured; this is not attributed to the copied workspace logs.

## Strict current data-plane load

All **nine runs passed with strict budgets enabled**, three each for frontline, sidecar HTTP, and sidecar TCP. Every parsed request/stream sample reports zero failures. Every metadata record confirms unchanged source and gate snapshots across its run. These measurements preceded the control-plane-only 6L merge; the data-plane/shared-contract/script scope remained fixed. The coordinator's [final 6L image provenance](final-6l-image-provenance.json) subsequently confirms that frontline and sidecar image IDs and executable hashes are identical to the measured 6JK images, while the control-plane image changed. These load helpers do not exercise the new durable 6L control-plane behavior; the database and deployed lifecycle gates cover it separately.

Per direct and proxied leg, at concurrency 8: HTTP/1 uses 5,000 requests, h2c and negotiated TLS/h2 each use 50,000 gRPC-shaped requests, generated gRPC uses 2,000 calls, and WebSocket uses 500 exchanges of 1 MiB each. Sidecar HTTP uses 5,000 requests at concurrency 8. TCP uses 32 streams of 32 MiB each (1 GiB total per leg), at concurrency 2. These are the recorded workloads, not a claim that every short request mode ran for the same duration.

The measured proxied/direct ratios retain the unchanged minimums:

| Workload | Round 1 | Round 2 | Round 3 | Minimum |
| --- | ---: | ---: | ---: | ---: |
| Frontline HTTP/1 | 0.983 | 0.959 | 0.931 | 0.80 |
| Frontline h2c gRPC-shaped | 1.009 | 1.023 | 0.998 | 0.75 |
| Frontline negotiated TLS/h2 gRPC-shaped | 0.933 | 1.003 | 0.996 | 0.75 |
| Frontline generated gRPC | 0.846 | 0.897 | 0.907 | 0.75 |
| Frontline WebSocket exchanges | 0.975 | 1.000 | 0.988 | 0.80 |
| Frontline WebSocket byte throughput | 0.976 | 1.001 | 0.988 | 0.80 |
| Sidecar HTTP/1 | 0.933 | 0.900 | 0.980 | 0.80 |
| Sidecar TCP byte throughput | 1.007 | 0.996 | 1.013 | 0.85 |

All added-p99 checks passed: HTTP/gRPC/WebSocket allow at most 25 ms added latency; TCP allows 500 ms. The largest positive addition observed was 0.463 ms among the HTTP/gRPC/WebSocket checks and 5.440 ms for TCP. The summary retains every value, including negative differences; these are paired host-local observations, not a guarantee of faster proxied traffic.

All five measured frontline hot paths added **zero SubscribeRoute calls** in each round. Each fake-control-plane cold check used exactly one SubscribeRoute, one WakeInstance, and one backend HTTP request; measured latency was 1.448, 1.390, and 1.568 ms against the unchanged 250 ms smoke threshold. This is a helper's cold-wake protocol check, not Kubernetes materialization latency or a substitute for first-cold deployed tests.

| Round | Gate | Whole-run seconds | Retained evidence |
| --- | --- | ---: | --- |
| 1 | frontline-load | 43.794 | [log](final-dp-20260908T035254Z-frontline-load.log), [metadata](final-dp-20260908T035254Z-frontline-load.json) |
| 1 | sidecar-load | 6.869 | [log](final-dp-20260908T035338Z-sidecar-load.log), [metadata](final-dp-20260908T035338Z-sidecar-load.json) |
| 1 | sidecar-tcp-load | 18.734 | [log](final-dp-20260908T035345Z-sidecar-tcp-load.log), [metadata](final-dp-20260908T035345Z-sidecar-tcp-load.json) |
| 2 | frontline-load | 41.684 | [log](final-dp-20260908T035404Z-frontline-load.log), [metadata](final-dp-20260908T035404Z-frontline-load.json) |
| 2 | sidecar-load | 3.578 | [log](final-dp-20260908T035445Z-sidecar-load.log), [metadata](final-dp-20260908T035445Z-sidecar-load.json) |
| 2 | sidecar-tcp-load | 18.611 | [log](final-dp-20260908T035449Z-sidecar-tcp-load.log), [metadata](final-dp-20260908T035449Z-sidecar-tcp-load.json) |
| 3 | frontline-load | 41.684 | [log](final-dp-20260908T035508Z-frontline-load.log), [metadata](final-dp-20260908T035508Z-frontline-load.json) |
| 3 | sidecar-load | 3.637 | [log](final-dp-20260908T035550Z-sidecar-load.log), [metadata](final-dp-20260908T035550Z-sidecar-load.json) |
| 3 | sidecar-tcp-load | 18.630 | [log](final-dp-20260908T035553Z-sidecar-tcp-load.log), [metadata](final-dp-20260908T035553Z-sidecar-tcp-load.json) |

Whole-run durations include helper preparation where shown in the logs and are not throughput timing denominators. The direct/proxy sample durations and all request, byte, latency, and throughput values are retained in the raw logs and summary. Previous unsuccessful concurrency-16 h2c measurements and earlier original/current comparisons remain available in this directory and [strict-default history](strict-default/summary.json); the successful current series does not erase them or change their budgets.

## Latest primitive comparison

The [baseline](final-connect-primitives/baseline.log) and [current](final-connect-primitives/current.log) runs used the matched primitive harness, 40 samples, 1 second warmup, and 3 seconds measurement per case. [Run metadata](final-connect-primitives/runs.json) preserves commands, executable hashes, and exit status. The baseline executable comes from archived `dc7cac2` production source with the corresponding benchmark harness. This is an overall original/current comparison, not isolated attribution to 6K or 6L.

All ten required comparisons pass the unchanged 25% failure threshold, with nine `ok` and one `warn` at the unchanged 15% warning threshold. [Gate output](final-connect-primitives/gate.log) and [change estimates](final-connect-primitives/criterion/) are retained.

| Primitive | Reported change | Gate result |
| --- | ---: | --- |
| `accounting_track_release` | -0.174% | ok |
| `admission_try_acquire_release` | -0.190% | ok |
| `http_prepare_reverse_proxy_request` | +2.960% | ok |
| `http_strip_hop_by_hop_headers` | +0.414% | ok |
| `observability_label_as_str_and_outcome_mapping` | +0.122% | ok |
| `tcp_proxy_streams_duplex_4k_round_trip` | +3.587% | ok |
| `tcp_proxy_streams_with_idle_timeout_duplex_4k_round_trip` | +18.820% | warn |
| `tls_parse_client_hello_sni_fragmented_records` | +0.134% | ok |
| `tls_parse_client_hello_sni_single_record` | +0.654% | ok |
| `websocket_proxy_streams_duplex_binary_round_trip` | +2.411% | ok |

The idle-TCP relay warning is a **statistically significant cost**, accepted by the skeptical reviewer rather than dismissed as noise: Criterion reports +18.820% with a 95% relative-mean interval of +17.450% to +20.061%. Printed point estimates move from 2.7735 to 3.2797 microseconds, about **0.51 microseconds** added per 4 KiB round trip. The relative-mean change and printed time estimate are different Criterion statistics; the latter is not used to recalculate or replace the recorded percentage. This benchmark uses established in-memory duplex streams and does **not invoke the connector**. It cannot measure connection refusal recovery or attribute its cost to that connector.

There is one retention limitation: Criterion's default `base/` was overwritten by the current run. For all ten cases the archived `base/sample.json` equals `new/sample.json`; those files are **current samples**, not the original baseline. The original baseline raw samples were not preserved for this round. The paired executable hashes, baseline console estimates, current raw samples, and relative change estimates/confidence intervals survive, but independent re-bootstrap of this exact pair from two raw sample sets is unavailable. Historical comparison packets remain intact and are not presented as replacements for the missing samples.

## Independent packaging review

The skeptical reviewer explicitly approved this source/evidence packaging after independently checking all 119 copied-artifact hashes, test counts, all nine strict-load results and thresholds, data-plane image provenance, and the ten overwritten Criterion baseline sample sets. This approval covers the accuracy and scope of the packet. Lifecycle completion is assessed separately in the [current integration ledger](README.md). The earlier Delete-while-Draining failure has a captured fixture deadline/grace cause and an independently approved correction; this performance packet is not evidence of that cause or a whole-system completion gate.
