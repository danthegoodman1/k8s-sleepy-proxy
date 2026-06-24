ALTER TABLE materializations
    ADD COLUMN reconcile_owner text,
    ADD COLUMN reconcile_lease_expires_at_unix_millis bigint,
    ADD COLUMN reconcile_attempt bigint NOT NULL DEFAULT 0 CHECK (reconcile_attempt >= 0);

ALTER TABLE materializations
    ADD CONSTRAINT materializations_reconcile_lease_pair_check
    CHECK (
        (reconcile_owner IS NULL AND reconcile_lease_expires_at_unix_millis IS NULL)
        OR (reconcile_owner IS NOT NULL AND reconcile_lease_expires_at_unix_millis IS NOT NULL)
    );

CREATE INDEX materializations_reconcile_scan_idx
    ON materializations (state, reconcile_lease_expires_at_unix_millis, updated_at_unix_millis)
    WHERE state IN ('pending', 'deleting');

CREATE TABLE materialization_operator_audit_events (
    audit_id bigserial PRIMARY KEY,
    operation text NOT NULL,
    materialization_id text,
    cluster_id text,
    namespace text,
    key_name text,
    operator text NOT NULL,
    reason text NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint
);
