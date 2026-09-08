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
