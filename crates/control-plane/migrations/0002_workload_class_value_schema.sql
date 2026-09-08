ALTER TABLE workload_class_versions
    ADD COLUMN value_schema jsonb NOT NULL DEFAULT
        '{"allow_extra": false, "fields": {}}'::jsonb;

ALTER TABLE workload_class_versions
    ALTER COLUMN value_schema DROP DEFAULT;
