# Dynamic Certificate Delivery Plan

## Overarching Goal

Frontline terminates application TLS using certificates published to the control
plane, fetched on demand, and refreshed without restarting proxies or waking
applications. Postgres durably stores encrypted private keys. Frontline keeps
only a bounded in-memory cache; it writes no application certificates or keys to
disk. Static application certificate loading, environment settings, mounts and
fallbacks are removed. No backwards compatibility or migration of static files
is required.

Certificates and hostname bindings are independent of application routes and
instance lifecycle. A hostname may route different paths to different apps, and
several hostnames may use one certificate. Issuance and renewal tooling publishes
already-issued bundles through the API. Building an ACME client, DNS-01 support,
wildcard TLS binding lookup, proxy disk persistence, external KMS integrations,
and a new tenant authorization system are outside this plan. V1 TLS bindings use
exact canonical DNS hostnames; a supplied SAN/wildcard certificate is acceptable
only when standards-based validation covers each explicitly bound hostname.

Implementation is active on `dynamic-certificates`, starting from merged main
`456d37e`. Phase 1 is committed and pushed as `f138053`; Phase 2 is active. Prior TLS and routing evidence is a baseline, not proof that dynamic
delivery works. Execute phases in order, preserving a reviewable commit and
independent review at each completed boundary.

## Implementation Principles

- Use two resources: `Certificate` (ID, version, chain, sealed private key,
  validity, active/deleted state) and `TlsBinding` (hostname, certificate ID,
  revision). Keep private material out of routes and route-change events.
- Resolve a binding and its active certificate in one database snapshot. Use
  monotonic view revisions spanning both resources, including removal/rebinding;
  comparing certificate versions alone cannot prevent stale binding reuse.
- Require expected versions for mutation. Preserve enough deletion identity to
  reject stale operations and ID reuse. Mutations with ambiguous commit outcomes
  are not blindly retried; callers inspect metadata before another conditional
  write. Extend the existing explicit store/retry capabilities without runtime
  "unsupported" defaults or a second persistence framework.
- Validate before publication and again before installation. Check supported
  key/chain encoding, matching public key, certificate validity, hostname SAN
  coverage, server-auth usage and size limits with maintained libraries. Accept
  privately issued certificates; public CA trust is not a publication requirement.
  Use canonical ASCII DNS/A-label names and reject ports, URLs, IP literals and
  wildcard binding keys. A rotation must cover all current bindings. Invalid
  publication leaves the active version unchanged.
- Use authenticated encryption with a versioned envelope and key ID; bind
  ciphertext to certificate ID/version. Runtime sealing keys live outside the
  database. Support active/read key IDs and an explicit bounded, conditional
  re-encryption operation; never invent cryptographic primitives.
- Separate secret-bearing native gRPC from ordinary metadata and notifications.
  Existing operator credentials publish/manage; proxy credentials resolve;
  sidecar credentials cannot access material. This remains the existing shared
  role model, not per-tenant isolation. No private-key readback in operator
  metadata, no plaintext/no-auth material delivery, and no secret-bearing logs.
- Control-plane transport identity/trust is independently provisioned platform
  infrastructure. It cannot be bootstrapped through application certificate
  resolution. Removing Frontline's static application certificates does not
  remove server verification on its control-plane connection.
- Keep parsing, decryption and network calls off cached handshakes. Reuse the
  current `Arc<ServerConfig>` replacement model and existing admission/deadline
  primitives; add small cohesive modules instead of a generic secret platform.
- Install dependencies with ecosystem commands and inspect manifest/lockfile
  changes. Keep Kubernetes, Postgres, issuance and sealing dependencies out of
  Frontline's normal dependency graph.

## Delivery and Failure Contract

| Operation or event | Required behavior |
| --- | --- |
| Publish/rotate certificate | Validate and atomically commit material, metadata, version and durable change record. Return metadata only. |
| Bind/rebind/unbind hostname | Conditional atomic mutation with a revision that survives unbinding. Resolve never returns mixed old/new binding and material. |
| Remove certificate | Mark inactive and erase its stored private material atomically; invalidate every referencing hostname. Retain non-secret identity/version needed for fencing. |
| `ResolveTlsCertificate(server_name, known_view_revision)` | Return authoritative `Found`, `Unchanged`, or `Missing`, with view revision and validity/freshness information. Only `Found` carries the bundle. Store/decryption/auth errors are errors, not authoritative misses. |
| `WatchTlsCertificates` | One bounded native stream per Frontline, with bounded hostname interests, current-state synchronization and subsequent version/removal/reset events. Events contain metadata only. |
| Warm handshake | In-memory configuration lookup and freshness check; no control-plane RPC, database, certificate parsing or filesystem access. |
| Rotation notification | Refresh promptly; retain the valid current configuration while validating a replacement, only within its existing authorization lease. |
| Unbind, removal or rebind notification | Invalidate the affected view immediately on receipt. Fence older in-flight fetches; require an authoritative current view before another handshake. |
| Outage or invalid response | Existing configurations remain usable only within their certificate validity and fixed authorization lease. Failed refreshes, duplicate events and reconnects never renew that lease. |
| New connection / resumed TLS session | Apply the same SNI authorization and freshness check before choosing a TLS configuration. Session state must not bypass removal or rotation; no TLS early data. |
| Already established connection | Continue under the existing connection/drain policy. Certificate removal does not promise to terminate established sessions. |
| Restart | Start with an empty certificate cache. TLS requires a successful control-plane resolution; no local-file or disk-cache fallback. |
| HTTP-01 | Serve through proxy-authorized resolution before any certificate exists, with no route fallback or app wake. A control-plane lookup failure remains an error. |

Initial defaults are 60s background revalidation with jitter, a 5-minute maximum
authorization lease, a 1s negative cache, and a 3s certificate lookup bounded by
the existing overall handshake setup deadline. These are certificate-delivery
bounds, not new guarantees for application routing during control-plane outages.
Healthy watch delivery normally removes stale views sooner; a partition can
delay enforcement until the original lease expires. Derive local deadlines
conservatively from RPC start/authoritative validity using monotonic time; delayed
responses or wall-clock rollback must not extend permission to serve.

Start with at most 1,024 cached hostname views, 64 MiB of accounted cache memory,
32 concurrent fetches, 128 KiB per certificate bundle, 16 chain entries and 100
SANs. Include negative entries, pending work, notification queues, parsed key/
configuration overhead and bounded TLS session state in resource accounting.
Handshake waiters remain under connection admission; there is no unbounded
secondary queue. Reject invalid configurations before allocating work. Measure
RSS separately from accounted cache bytes and document the envelope; these limits
may be adjusted only with recorded measurements and unchanged safety semantics.
The watch interest cap must cover the cache's hostname cap. Bound certificate
RPCs and parsing/decryption work on the server as well as on each proxy, leaving
capacity for route and lifecycle traffic under a multi-proxy miss flood.

## Testing Strategy

- Use injected clocks and explicit barriers for expiry, invalidation, cancellation
  and races. Wall-clock timeouts are failure bounds, not evidence of ordering.
- Exercise real PostgreSQL transactions, rollback, conditional writes, uncertain
  responses, retention and two independent control-plane replicas. Self-skipped
  database wrappers do not count as database execution.
- Use generated test certificates and real Rustls clients. Inspect the negotiated
  peer certificate/fingerprint, hostname verification, ALPN and live connections;
  checking only cache pointers or an HTTP success response is insufficient.
- Test the actual native API, role policy and encrypted transport. Use known test
  secret markers to detect disclosure in metadata, events, diagnostics and logs.
- Capture a fresh warm TLS baseline before changing the lookup path. Preserve
  separate raw before/after artifacts on the same machine, certificate algorithm,
  connection/resumption settings and load. Freeze workload and TLS budget before
  comparison; retain the existing shared hot-path failure budgets.
- Each phase ledger records exact commands, source revisions, test counts and
  remaining gaps. Final image/deployment claims require actual final-image runs;
  mock tests and historical runs retain their narrower scopes.

## Phase 1: Certificate Resources and Durable Storage

Goal: make certificate publication and hostname ownership atomic, validated and
recoverable before introducing proxy delivery.

Scope: add contracts in `sleepypods-api`, cohesive validation/sealing code in the
control plane, a new migration after the existing migrations, explicit store and
retry implementations, and metadata-only durable certificate events. Reuse the
route outbox's commit-order/resynchronization pattern with a certificate-specific
feed. Keep validation and binding writes consistent under one documented lock
order. Capture the existing TLS performance baseline.

Completion gate: real Postgres proves coherent resolution, safe concurrent
mutation/removal and encrypted persistence; failed writes never replace a valid
view or emit a committed event.

Testing plan: canonical names and SAN boundaries; invalid/expired/not-yet-valid
chains and mismatched keys; wrong sealing key, tamper and ciphertext substitution;
rotation concurrent with binding creation; unbind/rebind and stale CAS; database
restart, rollback, lost responses and key re-encryption races.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Resource contracts, limits and mutation semantics | Reviewed exact-host resources, bounded bundles, permanent deletion identity and conditional mutation semantics; [phase evidence](review-evidence/dynamic-certificates/phase1/README.md). |
| Complete | Work | 1B: Validated encrypted persistence and key handling | Validity/key/SAN checks, AES-GCM authenticated envelopes and conditional re-encryption; actual database restart retained the same verified certificate. |
| Complete | Work | 1C: Atomic views, revisions, removal and durable events | Joined resolution, serialized administrative writes, commit-ordered fanout and bounded retained outbox; independent-pool races, rollback and response-loss tests pass. |
| Complete | Work | 1D: Resolve lifecycle deadline failure exposed by full regression gate | Independently reviewed DB-relative worker budget and fresh post-lock expiry; deterministic red/green lock and timer-anchor regressions. Original intermittent failure attribution remains unknown; see phase evidence. |
| Complete | Test | 1T: Store conformance and adversarial transaction cases | Full actual PostgreSQL 17 gate: 17 passed, 0 failed, 0 ignored, no self-skips; all four new certificate cases exercised. |
| Complete | Test | 1P: Matched warm TLS baseline | [Baseline evidence](review-evidence/dynamic-certificates/phase1/README.md): three real TLS rounds, median 243.454 µs; original executable/raw data retained; warn 15% / fail 25% budget frozen. |
| Complete | Gate | 1G: Durable resource contract | Independent source/focused-test approval; refreshed fmt, strict workspace Clippy, workspace tests, actual PostgreSQL and dependency boundaries all pass on frozen source. |

## Phase 2: Secure Publication and Proxy Resolution APIs

Goal: allow authorized publication and atomic certificate resolution through the
production API boundary, with no operator credential on Frontline.

Scope: implement operator publish/metadata/binding/removal operations and proxy
`ResolveTlsCertificate`. Move `ResolveHttp01Challenge` from the operator service
to the proxy service and delete Frontline's operator client/token configuration.
Add verified TLS to the native control-plane delivery path using independently
provisioned service identity/trust and the existing bearer roles. Integrate TLS
with current incoming connection, setup, body-size and delivery admission limits;
do not replace bounded incoming handling with an unbounded accept loop. Secret
publication over gRPC-Web, if exposed through the operator service, must have the
same encrypted/authenticated boundary; never trust client-supplied forwarding
headers as proof of encryption.

Completion gate: a real encrypted RPC can publish and resolve a complete view;
unauthorized or insecure calls cannot publish/read material, and Frontline serves
HTTP-01 using only its proxy credential.

Testing plan: operator/proxy/sidecar role matrix, missing/wrong credentials,
untrusted server/wrong server name, insecure/no-auth denial, oversized requests,
slow consumers, conditional `Unchanged`, authoritative `Missing`, decryption
errors, and secret-marker scans. Test HTTP-01 hit/miss/expiry/error interception
through the new proxy API with zero wake/route calls.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Operator management and proxy resolution APIs | Reviewed native-only certificate CRUD/reencryption, mandatory CAS and coherent Found/Unchanged/Missing; [phase evidence](review-evidence/dynamic-certificates/phase2/README.md). |
| Complete | Work | 2B: Encrypted authenticated delivery with bounded transport | Real TLS/role/body/withheld-delivery tests; dedicated RPC/crypto/database admission and cancellation-safe session drain; external platform identity and sealing-key configuration. |
| Complete | Work | 2C: Proxy HTTP-01 resolution; remove Frontline operator access | Protocol/client/config and deployed fixture credential removed; proxy mapping and zero-route/zero-wake interception regression tests pass. |
| Complete | Test | 2T: Real transport, authorization and disclosure checks | Three actual PostgreSQL/TLS cases plus real same-peer TLS/H2, recursive Debug markers and rebuilt process startup/role/trust/key-file tests pass. |
| Complete | Gate | 2G: Safe runtime API boundary | Independent source/focused approval; frozen fmt, strict workspace Clippy, 843 local workspace executions, actual PostgreSQL 20-target gate, dependency checks and real process proof all pass. |

## Phase 3: Dynamic Frontline Cache and Handshakes

Goal: serve application TLS entirely from demand-loaded, bounded memory state.

Scope: add a certificate resolver/cache owned by Frontline; connect it between
ClientHello/SNI parsing and `into_stream` in `FrontlineTlsAdapter`. Coalesce
concurrent misses, use bounded negative caching, and refresh cached positives
before the fixed lease expires. A validated conditional response may renew a
lease only for the exact matching locally retained view; `Unchanged` cannot
populate an empty/evicted entry. Parsing failures or transport errors may not
renew a lease. Keep unrelated hostnames
independent and give all background work owned cancellation/join behavior.

Remove `FrontlineTlsCertificateConfig`, startup PEM loaders and
`SLEEPYPODS_FRONTLINE_TLS_TERMINATION_CERTS`, along with production application
certificate mounts/examples. TLS-listener enablement no longer requires any
preloaded certificate; an empty cache alone does not fail process readiness.
Delete the configuration surface without retaining a compatibility loader.
Generated test material may seed API calls, but tests must not preload the
production proxy. Existing control-plane startup bounds still apply; an empty
cache does not promise HTTP-01 lookup or routing while that service is unavailable.

Completion gate: an empty-cache proxy successfully serves a published certificate
without restart; warm handshakes are local and the cache/deadline/no-disk bounds
hold, including during errors and shutdown.

Testing plan: real TLS cold/hot handshakes, one fetch for concurrent identical
misses, independent SNI progress, unknown/missing SNI, input/byte/entry limits,
negative-cache expiry, slow/failed lookup, cancellation of one/all waiters,
eviction during a fetch, clock jumps, expired/delayed responses, conditional
refresh without a matching local bundle, wrong-host response and control-plane
outage. Exercise TLS 1.2/1.3 and supported resumption
paths with version-specific session state and authorization checks.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 3A: Bounded cache, coalesced fetch and lease refresh | Missing: cache/resolver implementation and deadline/resource accounting tests. |
| Incomplete | Work | 3B: Dynamic handshake and owned background tasks | Missing: adapter/runtime integration, cancellation and resumption tests. |
| Incomplete | Work | 3C: Remove static application certificate loading | Missing: loader/config/mount removal and empty-cache process tests. |
| Incomplete | Test | 3T: Real TLS, cache limits and failure behavior | Missing: peer-certificate assertions, zero-RPC warm proof and no-disk proof. |
| Incomplete | Gate | 3G: Dynamic-only TLS operation | Missing: independent review and passing process/cache/handshake gates. |

## Phase 4: Rotation, Removal and Reconnect Convergence

Goal: promptly update active proxies without letting ordering gaps restore stale
certificates or leave removals unenforced beyond the authorization lease.

Scope: implement the bounded certificate watch protocol and feed it from durable
Postgres changes across control-plane replicas. Register interests with the
client's observed view revision and synchronize current state before relying on
live events; this closes the resolve-then-watch race. Reconnect/resets synchronize
all current interests, including bounded cached misses. Metadata notifications
trigger authoritative resolution; they do not themselves renew leases.

Use a local fetch generation/view floor so late responses cannot overwrite a
newer invalidation. Release interest state on eviction; bound history, tombstones,
queues and retries. Retention gaps and lagged receivers cause resynchronization,
not silent continuation. Update API admission's current route-Subscribe-specific
stream handling so certificate watches own correct stream/delivery permits and
cannot starve route or lifecycle traffic.

Completion gate: rotation, unbind/removal and rebind converge on two proxies
through either control-plane replica; loss, duplication, reordering or disconnect
cannot install a stale view or extend its original lease.

Testing plan: publish between resolve and watch registration; rotation during
fetch; deletion followed by delayed `Found`/`Unchanged`; hostname rebind to a
different certificate whose version is numerically smaller; shared-certificate
rotation/removal; malformed replacement; reconnect to another replica; outbox
retention gap; slow consumer; repeated subscribe/evict/shutdown; hard lease expiry
during a complete notification outage. Prove version/removal fences with barriers.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: Durable watch, initial synchronization and reset | Missing: protocol/broker/client implementation and cursor-gap proof. |
| Incomplete | Work | 4B: Rotation/removal/rebind race fencing | Missing: atomic installation and stale-response regression tests. |
| Incomplete | Work | 4C: Bounded interests, streams and independent progress | Missing: admission integration and repeated disconnect/eviction evidence. |
| Incomplete | Test | 4T: Multi-replica convergence and adversarial ordering | Missing: actual Postgres/two-replica race and outage tests. |
| Incomplete | Gate | 4G: Reliable propagation with bounded stale service | Missing: independent review and measured convergence/freshness results. |

## Phase 5: Deployed Certificate Lifecycle and Resource Proof

Goal: verify the complete feature using production images and realistic concurrent
traffic, including the conditions under which availability is intentionally lost.

Scope: convert `scripts/test-kind-e2e-tls.sh` and its Rust driver to publish
certificates through the API; remove Frontline's certificate Secret mounts.
Use ephemeral platform transport identity for the test control plane. Run two
Frontlines and two control-plane replicas with explicit certificate fingerprints,
resource ownership and fault barriers. Add bounded metrics for cache entries/
bytes, fetch/refresh results, watch resets, installation failures and expiry risk;
keep hostnames/certificate IDs out of unbounded metric labels and keys out of all
diagnostics. Add a repeatable warm/miss/rotation/unique-SNI flood workload.

Completion gate: fresh final images pass dynamic delivery, renewal, removal,
outage and restart cases; resource usage stabilizes within the declared envelope
and existing protocol/lifecycle behavior remains intact.

Testing plan: HTTP-01 before publication without waking a Cold app; publish then
first TLS handshake and first HTTP request wake; sleep/re-wake with the certificate
retained; certificate rotation while HTTP/2/WebSocket connections remain active;
invalid renewal; shared-host path routes; SNI passthrough; expiry/removal/rebind
including resumed sessions; warm proxy during control-plane outage; cold restart
with control plane unavailable and subsequent recovery. Inject update loss and
switch replicas. Measure RSS, fetch counts, task/queue limits and warm handshake
latency under sustained misses and rotations, and verify no proxy key files.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: Dynamic-only deployed TLS fixtures | Missing: API-seeded final-image test driver with no Frontline cert mount. |
| Incomplete | Work | 5B: Operational metrics and resource/load harness | Missing: bounded metrics, secret-safe diagnostics and repeatable workloads. |
| Incomplete | Test | 5T: HA, lifecycle, protocol and outage matrix | Missing: actual kind executions with fingerprints and state assertions. |
| Incomplete | Test | 5P: Matched performance and bounded resource results | Missing: three matched rounds, stable RSS/task/cache measurements and existing budget passes. |
| Incomplete | Gate | 5G: Production-image behavioral and resource proof | Missing: independent review of deployed results and remaining limitations. |

## Phase 6: CI, Operator Contract and Final Integration

Goal: make the feature reproducible, operable and continuously checked with no
static-certificate instructions or unverified release claims.

Scope: document publication/binding/removal, sealing-key backup/rotation,
control-plane transport trust, proxy credential configuration, cache bounds,
freshness/outage tradeoffs, and diagnosis of missing/expired/invalid certificates.
Update current operator/runbook/store-contract/README examples and remove obsolete
static settings; retain historical evidence only with its original scope. Wire
deterministic certificate/API tests and actual certificate store conformance into
required CI. Keep deployed tests as explicit release gates unless CI actually
executes them; counts must expose skips and ignored tests.

Finish with a fresh skeptical simplification review of the complete feature.
Challenge unnecessary abstractions, duplicate validation/state, fallback paths,
queue/task ownership and proof gaps. Implement justified simplifications and
rerun their affected gates; record explicit reasons where complexity is required
by a contract. Final approval requires every actionable finding to be resolved.

Completion gate: all preceding ledgers have concrete passing evidence, current
docs describe the actual dynamic-only behavior, required hosted checks pass on
the final code, and the deployed/performance gates use the final relevant images.

Testing plan: run `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`,
`python3 scripts/test-dependency-boundaries.py`,
`python3 scripts/test-full-wake-sleep-inventory.py`, both isolated load-example
checks, `scripts/test-postgres-store.sh`, image smokes and the targeted deployed
TLS/routing/protocol/restart gates. Validate new secret-bearing RPC admission and
dependency boundaries. Audit current docs/config/image manifests for removed
static settings; distinguish TLS test fixtures and infrastructure trust from
application certificate loading. Record exact compiler, source/image revisions,
commands, counts, raw benchmark references and hosted run links.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Doc | 6A: Current operator/security/failure contract | Missing: reviewed guide, runbook, store contract and examples. |
| Incomplete | Work | 6B: Required CI and release gate wiring | Missing: automated certificate/store tests and fail-closed skip/count checks. |
| Incomplete | Work | 6C: Final skeptical review and simplification pass | Missing: independent full-feature findings, resolved simplifications and affected regression gates. |
| Incomplete | Test | 6T: Final workspace, store, images and hosted checks | Missing: passing exact-revision commands and CI links. |
| Incomplete | Gate | 6G: Implementation complete | Missing: independent final review, all prior gates and a clean final diff. |

## Starting Points

- [TLS store and handshake adapter](../crates/frontline/src/tls.rs),
  [startup/configuration](../crates/frontline/src/config.rs), and
  [Frontline binary](../crates/frontline/src/bin/frontline.rs).
- [Shared API](../crates/sleepypods-api/proto/sleepypods/controlplane/v1/control_plane.proto),
  [store contract](../crates/control-plane/src/store.rs),
  [Postgres provider](../crates/control-plane/src/postgres/mod.rs),
  [route event pattern](../crates/control-plane/src/api/route_events.rs), and
  [RPC admission](../crates/control-plane/src/api/admission.rs).
- [Contributor validation requirements](contributor-guide.md),
  [current operator guide](operator-guide.md),
  [Postgres semantics](postgres-store-contract.md), and
  [existing performance budgets](proxy-hot-path-budgets.md).
