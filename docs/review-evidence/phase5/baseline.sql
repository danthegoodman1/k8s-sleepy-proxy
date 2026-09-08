CREATE SCHEMA phase5_baseline;
SET search_path=phase5_baseline;
CREATE TABLE workload_class_versions (
    class_id text NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    template_generation bigint NOT NULL CHECK (template_generation >= 0),
    default_values jsonb NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint,
    PRIMARY KEY (class_id, version)
);

CREATE TABLE instances (
    instance_id text PRIMARY KEY,
    workload_class_id text NOT NULL,
    workload_class_version bigint NOT NULL CHECK (workload_class_version >= 0),
    values jsonb NOT NULL,
    state text NOT NULL,
    generation bigint NOT NULL CHECK (generation >= 0),
    created_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint,
    updated_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint,
    CONSTRAINT instances_workload_class_fk
        FOREIGN KEY (workload_class_id, workload_class_version)
        REFERENCES workload_class_versions (class_id, version),
    CONSTRAINT instances_state_check
        CHECK (state IN ('cold', 'waking', 'running', 'draining', 'failed', 'deleting', 'deleted'))
);

CREATE TABLE route_bindings (
    route_binding_id text PRIMARY KEY,
    instance_id text NOT NULL REFERENCES instances (instance_id) ON DELETE CASCADE,
    identity_key text NOT NULL UNIQUE,
    identity_kind text NOT NULL,
    host_kind text NOT NULL,
    host text NOT NULL,
    path_prefix text,
    protocol text NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint,
    CONSTRAINT route_bindings_identity_kind_check
        CHECK (identity_kind IN ('http', 'sni')),
    CONSTRAINT route_bindings_host_kind_check
        CHECK (host_kind IN ('exact', 'wildcard_suffix')),
    CONSTRAINT route_bindings_protocol_check
        CHECK (protocol IN ('http', 'tls_sni')),
    CONSTRAINT route_bindings_http_path_check
        CHECK (identity_kind = 'http' OR path_prefix IS NULL)
);

CREATE INDEX route_bindings_instance_id_idx ON route_bindings (instance_id);
CREATE INDEX route_bindings_identity_lookup_idx
    ON route_bindings (identity_kind, host_kind, host);

CREATE TABLE materializations (
    materialization_id text PRIMARY KEY,
    instance_id text NOT NULL REFERENCES instances (instance_id) ON DELETE CASCADE,
    instance_generation bigint NOT NULL CHECK (instance_generation >= 0),
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    state text NOT NULL,
    backend_uri text,
    backend_generation bigint NOT NULL CHECK (backend_generation >= 0),
    rendered_objects jsonb NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint,
    updated_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint,
    UNIQUE (instance_id, cluster_id, namespace),
    CONSTRAINT materializations_state_check
        CHECK (state IN ('pending', 'ready', 'failed', 'deleting', 'deleted'))
);

CREATE INDEX materializations_instance_id_idx ON materializations (instance_id);

CREATE TABLE http01_challenges (
    host text NOT NULL,
    token text NOT NULL,
    key_authorization text NOT NULL,
    expires_at_unix_millis bigint NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint,
    updated_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint,
    PRIMARY KEY (host, token)
);

CREATE INDEX http01_challenges_expires_at_idx ON http01_challenges (expires_at_unix_millis);

CREATE TABLE idempotency_records (
    idempotency_key text PRIMARY KEY,
    operation text NOT NULL,
    request_fingerprint jsonb NOT NULL,
    resource_id text NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (
        extract(epoch from clock_timestamp()) * 1000
    )::bigint
);

ALTER TABLE workload_class_versions
    ADD COLUMN value_schema jsonb NOT NULL DEFAULT
        '{"allow_extra": false, "fields": {}}'::jsonb;

ALTER TABLE workload_class_versions
    ALTER COLUMN value_schema DROP DEFAULT;

ALTER TABLE workload_class_versions
    ADD COLUMN manifest_template jsonb NOT NULL;

ALTER TABLE workload_class_versions
    ADD COLUMN sleep_policy jsonb NOT NULL DEFAULT
        '{
            "idle_timeout_ms": 300000,
            "idle_retry_backoff_ms": 5000,
            "drain_grace_timeout_ms": 30000,
            "idle_timeout_override": null
        }'::jsonb;

ALTER TABLE workload_class_versions
    ALTER COLUMN sleep_policy DROP DEFAULT;

ALTER TABLE workload_class_versions
    ADD COLUMN exclusivity_keys jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE workload_class_versions
    ALTER COLUMN exclusivity_keys DROP DEFAULT;

ALTER TABLE materializations
    ADD COLUMN exclusivity_keys jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE materializations
    ALTER COLUMN exclusivity_keys DROP DEFAULT;

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

INSERT INTO workload_class_versions(class_id, version,template_generation,default_values,value_schema,manifest_template,sleep_policy,exclusivity_keys) VALUES('class',1,1,'{}','{}','{}','{}','[]');
INSERT INTO instances(instance_id,workload_class_id,workload_class_version,values,state,generation) SELECT 'instance-'||i,'class',1,'{}','cold',1 FROM generate_series(1,100000) i;
INSERT INTO route_bindings(route_binding_id,instance_id,identity_key,identity_kind,host_kind,host,protocol) SELECT 'route-'||i,'instance-'||i,'http:exact:app-'||i||'.example.com','http','exact','app-'||i||'.example.com','http' FROM generate_series(1,100000) i;
INSERT INTO materializations(materialization_id,instance_id,instance_generation,cluster_id,namespace,state,backend_generation,rendered_objects,exclusivity_keys) SELECT 'materialization-'||i,'instance-'||i,1,'cluster','namespace','pending',1,jsonb_build_array(jsonb_build_object('api_version','v1','kind','Service','namespace','namespace','name','service-'||i)),jsonb_build_array(jsonb_build_object('name','logical','value','value-'||i)) FROM generate_series(1,10000) i;
ANALYZE;
EXPLAIN(ANALYZE,BUFFERS) SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol FROM route_bindings WHERE identity_kind='http';
EXPLAIN(ANALYZE,BUFFERS) SELECT m.instance_id FROM materializations m CROSS JOIN LATERAL jsonb_array_elements(m.rendered_objects) e(object) WHERE m.cluster_id='cluster' AND m.instance_id<>'new' AND m.state<>'deleted' AND e.object->>'api_version'='v1' AND e.object->>'kind'='Service' AND e.object->>'namespace'='namespace' AND e.object->>'name'='unrelated-new';
EXPLAIN(ANALYZE,BUFFERS) SELECT m.instance_id FROM materializations m CROSS JOIN LATERAL jsonb_array_elements(m.exclusivity_keys) e(key) WHERE m.cluster_id='cluster' AND m.namespace='namespace' AND m.state<>'deleted' AND e.key->>'name'='logical' AND e.key->>'value'='unrelated-new';
