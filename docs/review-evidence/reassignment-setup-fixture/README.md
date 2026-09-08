# Route freshness fixture: establish paths before measuring cache age

The [retained restart run](kind-before.log) progressed through wake, automatic
sleep, delete and HTTP-01 recovery, then failed after 314.18 driver seconds at
`initial cache warmup exceeded total 1s fixture budget`.
[Provenance](kind-before.json) retains the frozen production-source identity.
The log does not expose the underlying connection/response error, so this packet
does not assign a specific network cause.

The fixture waited for both instances to be Running, restarted the control plane,
then gave the first ever frontend request for its proof key one second. Running
establishes control-plane readiness, not a completed frontend subscription
reconnection or actual end-to-end request through the Service. That one-second
freshness setup assumption was stricter than the existing connection/readiness
budgets. The lifecycle fixture used the same assumption without the restart.

The [two-file patch](fixture.patch) adds distinct exact alias routes to the same
old and new instances in `kind_e2e_restart.rs` and the late
`route_reassignment_invalidates_active_subscription` function in
`kind_e2e_lifecycle_races.rs`. After the restart/readiness preparation, each alias
must return a complete successful response identifying its exact backend. There
is one request per alias, using the existing 130-second response allowance and
140-second outer setup allowance; any failure is fatal. No proof request or
alias request is retried. The fixture HTTP helper's existing blocking socket
limits remain unchanged. No claim is made that the Python HTTP/1.0 application
keeps its upstream connection pooled.

Aliases exercise the current frontend/control-plane/backend paths before the
cache-age clock starts. The production exact-query cache contract prevents an
alias from populating the separate proof key. All original freshness checks
remain: the never-resolved proof key has a one-second request budget, route
cutover commits have one second, notification convergence has three seconds, and
the whole proof must finish while that cache entry is younger than five seconds,
well before its unchanged ten-second TTL. Once the new backend is observed, ten
further complete responses must remain fresh. The lifecycle test also retains
its independent direct-subscription invalidation assertions.

[Compilation](check.log) succeeds for both drivers; their two deployed tests are
explicitly ignored locally, so no local execution pass is claimed.
[Scoped clippy](clippy.log), formatting and `git diff --check` are clean. A test
that merely checked the order of this short preparation sequence would not prove
real subscription or Service readiness, so the retained failing kind gate and
coordinator-owned actual reruns are the meaningful execution gate. This packet
changes only these test regions and evidence, with no production code, scripts,
images, cluster operations or cache-TTL changes. The earlier lifecycle
pending-delete investigation is separate and is not addressed by this patch.

Independent reviewer explicitly approved this source/fixture packet. The existing
blocking HTTP helper waits for EOF; if a deployed timeout recurs, capture complete
wire framing and connection timing before changing the preparation or freshness
budgets. This approval does not attribute the original timeout or substitute for
the actual restart/lifecycle reruns.
