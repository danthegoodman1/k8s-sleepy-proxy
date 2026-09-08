ALTER TABLE workload_class_versions
    ADD COLUMN exclusivity_keys jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE workload_class_versions
    ALTER COLUMN exclusivity_keys DROP DEFAULT;

ALTER TABLE materializations
    ADD COLUMN exclusivity_keys jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE materializations
    ALTER COLUMN exclusivity_keys DROP DEFAULT;
