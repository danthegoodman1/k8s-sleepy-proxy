-- A missing object cannot prove that a previously dispatched create will never arrive.
-- Keep one outstanding operation per materialization until its driver has a definite
-- response. An ambiguous/cancelled operation intentionally blocks lease transfer and
-- automatic reservation release; an audited force operation is the recovery boundary.
CREATE TABLE materialization_effects (
    materialization_id text PRIMARY KEY REFERENCES materializations(materialization_id),
    effect_id bigint NOT NULL CHECK (effect_id > 0),
    lease_owner text NOT NULL,
    lease_attempt bigint NOT NULL CHECK (lease_attempt > 0),
    instance_generation bigint NOT NULL,
    operation text NOT NULL CHECK (operation IN ('apply', 'delete')),
    object_ref jsonb NOT NULL,
    expected_uid text,
    expected_resource_version text,
    started_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint
);
