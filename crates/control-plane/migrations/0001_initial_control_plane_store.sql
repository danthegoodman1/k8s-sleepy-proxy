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
