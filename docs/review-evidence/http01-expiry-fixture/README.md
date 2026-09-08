# HTTP-01 natural and explicit expiry fixture

The routing fixture incorrectly required a manual expiry RPC to delete a row that was already naturally expired. The [deployed failure](kind-before.log) occurred after 30.78 seconds at that assertion; [run metadata](kind-before.json) is retained. The preceding frontend HTTP 404 and absence of the challenge authorization had already passed.

Production `runtime::maintain_runtime` runs maintenance every five seconds. PostgreSQL `runtime_work::maintain` calls HTTP-01 expiry using the current wall clock and a bounded batch. Therefore the natural row can legitimately be physically deleted before the manual call, whose correct deleted count is then zero. The failed log does not identify the winning collector; the source establishes that the old assertion cannot require the manual caller to win this race. No production garbage collection, API time semantics, or TTL is changed.

## Narrow correction

[Patch](fixture.patch) and [source hashes](fixture-hashes.json) cover two test files:

- `crates/control-plane/tests/kind_e2e_routing.rs` retains the inserted challenge response/content type, normal application routing, explicit deletion and 404/no-authorization proof, and natural before/after-expiry frontend response checks. It then deletes the natural token if still present, accepting either boolean because the physical collection race is intentional.
- A separate manual phase, bounded to 30 seconds, creates distinct target and sentinel tokens expiring in one and two hours. Both must resolve with their exact authorization values. `ExpireHttp01Challenges` uses the target's stored millisecond expiry as its explicit cutoff with `limit=1`: exactly one row must be deleted, that exact target must be absent, and the later sentinel must remain. Repeating the same cutoff must delete zero rows. Explicit sentinel deletion must succeed and leave it absent. Ordinary application routing after cleanup remains checked. This isolates manual RPC causality from wall-clock maintenance without disabling it.
- `crates/control-plane/tests/api_transport.rs` adds one deterministic regression using the existing shared `TestStore` and real `StoreBackedOperatorApi` mapping. Both maintenance-first and caller-first orders are exercised at a controlled cutoff. The naturally expired count is respectively zero or one, while the independent future target has exact count one, target absence, sentinel survival, repeat count zero, and final sentinel deletion/absence in both cases. It uses future-valid inserts and explicit later cutoffs; it does not pretend to run the background scheduler or PostgreSQL.

## Validation and limits

The [focused regression](local-test.log) passed: **1 passed, 0 failed**, covering both deterministic orders. The [routing target](routing-compile.log) compiles and retains **1 explicit deployed-test ignore**; this is not a new kind pass. [Scoped clippy](clippy.log), [format check](fmt.log), and `git diff --check` passed. No cluster, image, production-source, or shared test-store changes were made in this packet. Root owns the actual routing rerun.

Initial fixture-development failures are retained separately: [timestamp type compile error](initial-local-test.log), [initial clippy](initial-clippy.log), [copy-only clippy finding](initial-copy-clippy.log), and [invalid expired insertion](initial-expired-insert-test.log). They were corrected to the protobuf's `i64` milliseconds, direct use of a Copy request, and valid future inserts with controlled later cutoffs. The deployed failure and these local test-construction failures are not presented as new production defects.

The root-owned [final merged kind gate](../final-integration/final-6l-routing.log)
passes the complete driver: one actual test, 31.61s, with stable production and
fixture source in its [metadata](../final-integration/final-6l-routing.json).
Natural expiry, explicit expiry and application routing all pass on the final
6L control-plane image.
