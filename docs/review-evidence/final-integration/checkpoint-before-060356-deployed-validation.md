# Final deployed validation: completed soak

The [full 2+2 soak](final-6l-soak.log) passed with **four actual lifecycle bodies**: two stateless and two stateful. [Metadata](final-6l-soak.json) records exit 0 and 1,465.010 seconds for the whole command, including image preparation and deployment. The [structured inventory](final-validation-summary.json) retains each cycle and the verified source/copy hashes.

| Cycle | Actual body | Driver seconds | Cargo passes | Environment self-returns |
| --- | --- | ---: | ---: | ---: |
| 1 | Stateless lifecycle | 239.79 | 1 | 0 |
| 1 | Stateful lifecycle | 288.16 | 3 | 2 |
| 2 | Stateless lifecycle | 239.20 | 1 | 0 |
| 2 | Stateful lifecycle | 285.58 | 3 | 2 |

Both stateful invocations select the three ignored wrappers, but the exclusivity and projection-drift wrappers return early because their separate environment flags are absent. The log explicitly prints those skips. Thus eight Cargo successes represent four actual bodies and four successful early returns; this soak does not rerun exclusivity or projection drift.

Each stateless body passes its single cold request, hot-cache/Prometheus checks, autonomous sleep and cleanup, and single re-wake request. The separate no-traffic wake becomes Cold after 225.400 and 223.986 seconds respectively, retaining real elapsed coverage of the 190-second activation floor. Each stateful body passes cold write, mounted read, autonomous sleep, and a single re-wake read preserving its stored marker. The script reports completion of its per-cycle cleanup, but independent review found that its original inventory reader suppresses `kubectl` errors with `|| true`. The four lifecycle bodies remain valid; the original shell output alone does **not establish verified resource absence**. A fail-closed reader and a separate final inventory are coordinator-owned follow-ups. Their intended scope is the selected test namespaces, workloads, Services, PVCs, PVs and RBAC, not global Kubernetes or memory leaks.

Production source remains `799aa84679078f48050f9f44d8a3eb2f0facd3e442479a8319878d81c04e0989` before and after the soak. All twelve recorded production-image builds—control plane, frontline and sidecar in each of four cycles—match the [final 6L image IDs](final-6l-image-provenance.json). The wider source hash changes during the run because separately approved fixture/helper work was integrated. The soak is therefore not described as a frozen whole-workspace run. The subsequent [final workspace checkpoint](final-validation-summary.json) separately verifies the integrated fixture source.

This packet adds no execution. Independent source/evidence review explicitly approved this packaging after verifying both source/copy hashes, all four actual successes and durations, the four environment self-returns, both no-traffic intervals and all twelve production-image build IDs. The original inventory-read ambiguity remains open. The subsequent final lifecycle-race attempt (20260908T050752Z) failed at membership cleanup after the accepted Delete remained Deleting for its unchanged 60-second fixture deadline; its cause is under separate coordinator-owned investigation. The queued single-control-plane and dual-control-plane restart gates did not run. The soak pass does not mark those gates complete. Earlier failures, partial runs, resource measurements and performance limitations remain retained in their original packets.
