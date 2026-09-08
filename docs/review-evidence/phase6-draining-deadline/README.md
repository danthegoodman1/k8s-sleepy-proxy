# Phase 6M: lifecycle fixture operation deadline

The unchanged final 6L image passed concurrent wake, Idle while Waking, and Delete
while Waking, then the lifecycle-races gate timed out waiting for Delete while
Draining. Its original log has no materialization snapshot; the retained frozen
repeat provides the causal evidence. The production source digest before and
after that repeat was
`799aa84679078f48050f9f44d8a3eb2f0facd3e442479a8319878d81c04e0989`.
The repeat used a new namespace, no production image build/retag, and the unchanged
driver. It failed with exit 101 after 177.86s including deployment (109.69s driver).
The original failure remains at
`.generated/implementation-evidence/20260908T040133Z-lifecycle-races.log`.

The script had globally set this test deployment's operation deadline to 30s to
exercise failed-wake expiry. During the Draining case, the intentional forbidden
StatefulSet delete settled, the fixture restored its removed Role permission,
and ordinary ReconcileMaterialization enqueued recovery. The recorded new
operation deadline was **04:09:41.062 UTC**. Kubernetes accepted the StatefulSet
foreground deletion around **04:09:11 UTC** and assigned its Pod the normal
**30-second termination grace**, ending **04:09:41 UTC**. PVC protection and the
StatefulSet foreground finalizer retained objects until then.

Cleanup returned transient incomplete observations, with persisted backoff:
next attempts at 04:09:13.992, 19.014, 27.059 and 43.148. At the last attempt the
operation deadline had expired. The driver correctly published deadline failure,
retaining Deleting state, generation 4, refs and ownership bookkeeping. There was
no unresolved effect or live lease. The **04:09:43** capture shows the Kubernetes
namespaced objects and the cluster-scoped PV already absent, but ordinary discovery
excludes terminal cleanup until
operator recovery; the unchanged 60-second Delete observation then timed out.
This is the configured operation policy colliding with normal Kubernetes grace
and retry timing, not evidence of a lease or effect-barrier hang.

The independently approved correction changes only this test deployment's operation
budget to **90 seconds**. The production default remains 600 seconds. The driver
still requires Delete completion within 60 seconds, retains its 500ms durable
drain grace, keeps normal 30-second Pod grace, and keeps all ownership/absence and
forbidden-error assertions. Its intentional failed-wake case still requires HTTP
503 within the existing 180-second window and observes Failed with the existing
30-second follow-up bound. The fixture budget is not a promise that all Kubernetes
environments complete cleanup within 90 seconds.

The retained runner is diagnostic infrastructure: it creates a fresh namespace,
refuses existing fixture PV names, retains resources, and captures read-only
Postgres state/effects/activity, CP logs and Kubernetes observations. The diagnostic runner used the then-unchanged canonical script and Rust driver.
The subsequent reviewed correction changes only the canonical script's operation
timeout and adjacent comment; the Rust driver stays unchanged. All full captures remain in
`.generated/implementation-evidence/6m-draining-diagnostic`; selected causal captures
and the complete state transition timeline are archived here. No additional
mutation was sent to the retained failed instance after the test failed.

The skeptical reviewer explicitly approved this design after checking the paired
04:09:43 namespaced-object and PV captures. The exact two-line correction is in
`fixture-only.patch`; `fixture-checks.json` records completed `/bin/bash -n` and
scoped `git diff --check` exits of 0, the exact-delta assertion, and script/driver
SHA256 values. The skeptical reviewer then explicitly approved the final source
after checking the exact patch, current script and unchanged driver hashes, and
completed checks. No production file changed and no additional cluster action
ran for this correction.

Actual corrected lifecycle execution remains a separate root-owned completion
gate. The diagnostic exit 101 is retained; this packet does not claim a corrected
deployed pass.
