-- A projection stamp is an incarnation, not an instance state CAS revision.
-- Legacy Pending objects were rendered using their future Running generation.
ALTER TABLE materializations ADD COLUMN projection_generation bigint;
UPDATE materializations SET projection_generation = CASE
    WHEN state = 'pending' THEN instance_generation + 1 ELSE instance_generation END;
ALTER TABLE materializations ALTER COLUMN projection_generation SET NOT NULL;
ALTER TABLE materializations ADD CONSTRAINT materializations_projection_generation_check
    CHECK (projection_generation >= 0);

-- Keep old low-level inserts compatible while making the selected stamp durable.
CREATE FUNCTION materialization_projection_generation_default() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.projection_generation IS NULL THEN
        NEW.projection_generation := CASE WHEN NEW.state = 'pending'
            THEN NEW.instance_generation + 1 ELSE NEW.instance_generation END;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER materialization_projection_generation_default
    BEFORE INSERT ON materializations FOR EACH ROW
    EXECUTE FUNCTION materialization_projection_generation_default();

-- These watermarks intentionally outlive both instances and idempotency keys.
CREATE TABLE instance_generation_watermarks (
    instance_id text PRIMARY KEY,
    last_generation bigint NOT NULL CHECK (last_generation >= 0),
    retired boolean NOT NULL DEFAULT false
);
INSERT INTO instance_generation_watermarks(instance_id, last_generation) SELECT instance_id, generation FROM instances;
-- A pre-upgrade deletion has no trustworthy final revision. Retire that ID
-- permanently instead of guessing a number that an old request might match.
INSERT INTO instance_generation_watermarks(instance_id, last_generation, retired)
    SELECT DISTINCT resource_id, 0, true FROM idempotency_records
    WHERE operation = 'create_instance'
      AND NOT EXISTS (SELECT 1 FROM instances WHERE instance_id = resource_id)
    ON CONFLICT (instance_id) DO NOTHING;
CREATE FUNCTION retain_instance_generation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous_generation bigint;
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO instance_generation_watermarks(instance_id, last_generation)
            VALUES (NEW.instance_id, NEW.generation)
            ON CONFLICT (instance_id) DO NOTHING;
        IF FOUND THEN RETURN NEW; END IF;
        IF EXISTS (SELECT 1 FROM instance_generation_watermarks WHERE instance_id = NEW.instance_id AND retired) THEN
            RAISE EXCEPTION 'instance_id_retired' USING ERRCODE = '23514';
        END IF;
        SELECT last_generation INTO previous_generation FROM instance_generation_watermarks
            WHERE instance_id = NEW.instance_id FOR UPDATE;
        NEW.generation := GREATEST(NEW.generation, previous_generation + 1);
    END IF;
    INSERT INTO instance_generation_watermarks(instance_id, last_generation)
        VALUES (NEW.instance_id, NEW.generation)
        ON CONFLICT (instance_id) DO UPDATE
        SET last_generation = GREATEST(instance_generation_watermarks.last_generation, EXCLUDED.last_generation);
    RETURN NEW;
END $$;
CREATE TRIGGER retain_instance_generation BEFORE INSERT OR UPDATE OF generation ON instances
    FOR EACH ROW EXECUTE FUNCTION retain_instance_generation();

CREATE TABLE deferred_wake_intents (
    instance_id text PRIMARY KEY REFERENCES instances(instance_id) ON DELETE CASCADE,
    instance_generation bigint NOT NULL,
    projection_generation bigint NOT NULL,
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    backend_generation bigint NOT NULL,
    rendered_objects jsonb NOT NULL,
    exclusivity_keys jsonb NOT NULL
);
CREATE INDEX instances_deleting_work_idx ON instances(instance_id) WHERE state = 'deleting';

-- Deletion accepted by an older process may have stranded Ready work.
UPDATE materializations SET state = 'deleting'
    WHERE state <> 'deleted' AND instance_id IN (
        SELECT instance_id FROM instances WHERE state IN ('deleting', 'deleted')
    );

-- The old synchronous API could crash between Waking CAS and Pending insert.
-- No target is durable in that gap, so expose a retryable Failed instance rather
-- than inventing a cluster or leaving an undiscoverable Waking row indefinitely.
UPDATE instances SET state = 'failed', generation = generation + 1
    WHERE state = 'waking' AND NOT EXISTS (
        SELECT 1 FROM materializations m WHERE m.instance_id = instances.instance_id AND m.state = 'pending'
            AND m.instance_generation = instances.generation
    );

-- Legacy failed wakes may have partially applied refs. Keep that inventory
-- discoverable for cleanup before another wake can replace the incarnation.
UPDATE materializations SET state = 'deleting'
    WHERE state NOT IN ('deleting', 'deleted') AND instance_id IN (SELECT instance_id FROM instances WHERE state = 'failed');
