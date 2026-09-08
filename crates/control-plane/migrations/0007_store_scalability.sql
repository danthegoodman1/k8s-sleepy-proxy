-- Reservations are maintained by the database so every state/ref mutation,
-- including force operations and cascading instance deletion, stays atomic.
CREATE TABLE materialization_object_reservations (
    cluster_id text NOT NULL,
    api_group text NOT NULL,
    kind text NOT NULL,
    namespace text NOT NULL,
    name text NOT NULL,
    materialization_id text NOT NULL REFERENCES materializations ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (cluster_id, api_group, kind, namespace, name)
);
CREATE INDEX materialization_object_reservations_owner_idx
    ON materialization_object_reservations (materialization_id);
CREATE TABLE materialization_key_reservations (
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    key_name text NOT NULL,
    key_value text NOT NULL,
    materialization_id text NOT NULL REFERENCES materializations ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (cluster_id, namespace, key_name, key_value)
);
CREATE INDEX materialization_key_reservations_owner_idx
    ON materialization_key_reservations (materialization_id);

-- DISTINCT tolerates repeated references within one projection. Existing owners
-- colliding across projections fail the migration rather than choosing a winner.
INSERT INTO materialization_object_reservations
SELECT DISTINCT m.cluster_id,
    CASE WHEN strpos(o->>'api_version', '/') > 0 THEN split_part(o->>'api_version', '/', 1) ELSE '' END,
    o->>'kind', CASE WHEN o->>'kind' = 'PersistentVolume' THEN '' ELSE o->>'namespace' END,
    o->>'name', m.materialization_id
FROM materializations m CROSS JOIN LATERAL jsonb_array_elements(m.rendered_objects) o
WHERE m.state <> 'deleted';
INSERT INTO materialization_key_reservations
SELECT DISTINCT m.cluster_id, m.namespace, k->>'name', k->>'value', m.materialization_id
FROM materializations m CROSS JOIN LATERAL jsonb_array_elements(m.exclusivity_keys) k
WHERE m.state <> 'deleted';

CREATE FUNCTION sync_materialization_reservations() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    ref record;
    owner_id text;
    owner_generation bigint;
BEGIN
    -- Lease renewals and scheduling do not rewrite the reservations.
    IF TG_OP = 'UPDATE' AND NEW.state = OLD.state
        AND NEW.rendered_objects = OLD.rendered_objects
        AND NEW.exclusivity_keys = OLD.exclusivity_keys
        AND NEW.cluster_id = OLD.cluster_id AND NEW.namespace = OLD.namespace THEN
        RETURN NEW;
    END IF;
    DELETE FROM materialization_object_reservations WHERE materialization_id = NEW.materialization_id;
    DELETE FROM materialization_key_reservations WHERE materialization_id = NEW.materialization_id;
    IF NEW.state = 'deleted' THEN RETURN NEW; END IF;
    -- Stable acquisition order bounds contention to conflicting identities and
    -- prevents inversions for two projections containing several common refs.
    FOR ref IN
        SELECT DISTINCT
            CASE WHEN strpos(o->>'api_version', '/') > 0 THEN split_part(o->>'api_version', '/', 1) ELSE '' END AS api_group,
            o->>'kind' AS kind,
            CASE WHEN o->>'kind' = 'PersistentVolume' THEN '' ELSE o->>'namespace' END AS namespace,
            o->>'name' AS name
        FROM jsonb_array_elements(NEW.rendered_objects) o
        ORDER BY 1, 2, 3, 4
    LOOP
        INSERT INTO materialization_object_reservations
        VALUES (NEW.cluster_id, ref.api_group, ref.kind, ref.namespace, ref.name, NEW.materialization_id)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            SELECT m.instance_id INTO owner_id
            FROM materialization_object_reservations r JOIN materializations m USING (materialization_id)
            WHERE r.cluster_id = NEW.cluster_id AND r.api_group = ref.api_group
                AND r.kind = ref.kind AND r.namespace = ref.namespace AND r.name = ref.name;
            RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'rendered_object_ref_collision',
                DETAIL = format('rendered Kubernetes object ref collision: %s %s %s/%s is already owned by active materialization for instance %s', COALESCE((SELECT o->>'api_version' FROM jsonb_array_elements(NEW.rendered_objects) o WHERE o->>'kind' = ref.kind AND o->>'name' = ref.name LIMIT 1), ref.api_group), ref.kind, ref.namespace, ref.name, owner_id);
        END IF;
    END LOOP;
    FOR ref IN
        SELECT DISTINCT k->>'name' AS name, k->>'value' AS value
        FROM jsonb_array_elements(NEW.exclusivity_keys) k ORDER BY 1, 2
    LOOP
        INSERT INTO materialization_key_reservations
        VALUES (NEW.cluster_id, NEW.namespace, ref.name, ref.value, NEW.materialization_id)
        ON CONFLICT DO NOTHING;
        IF NOT FOUND THEN
            SELECT m.instance_id, m.instance_generation INTO owner_id, owner_generation
            FROM materialization_key_reservations r JOIN materializations m USING (materialization_id)
            WHERE r.cluster_id = NEW.cluster_id AND r.namespace = NEW.namespace
                AND r.key_name = ref.name AND r.key_value = ref.value;
            RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'exclusivity_conflict',
                DETAIL = jsonb_build_object('cluster_id', NEW.cluster_id, 'namespace', NEW.namespace,
                    'key_name', ref.name, 'owner_instance_id', owner_id, 'owner_generation', owner_generation)::text;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;
CREATE TRIGGER materialization_reservations
    AFTER INSERT OR UPDATE ON materializations
    FOR EACH ROW EXECUTE FUNCTION sync_materialization_reservations();

ALTER TABLE materializations
    ADD COLUMN state_entered_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint,
    ADD COLUMN next_attempt_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint,
    ADD COLUMN drain_not_before_unix_millis bigint NOT NULL DEFAULT 0;
-- Legacy updated_at represented both queue position and the drain deadline.
-- Exact historical state age cannot be recovered, so cap future legacy values.
UPDATE materializations SET
    state_entered_at_unix_millis = LEAST(updated_at_unix_millis, (extract(epoch from clock_timestamp()) * 1000)::bigint),
    next_attempt_at_unix_millis = updated_at_unix_millis,
    drain_not_before_unix_millis = CASE WHEN state = 'deleting' THEN updated_at_unix_millis ELSE 0 END;
CREATE FUNCTION track_materialization_state_age() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IS DISTINCT FROM OLD.state THEN
        NEW.state_entered_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint;
        IF NEW.next_attempt_at_unix_millis = OLD.next_attempt_at_unix_millis THEN
            NEW.next_attempt_at_unix_millis = NEW.state_entered_at_unix_millis;
        END IF;
        IF NEW.state <> 'deleting' THEN NEW.drain_not_before_unix_millis = 0; END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER materialization_state_age BEFORE UPDATE ON materializations
    FOR EACH ROW EXECUTE FUNCTION track_materialization_state_age();
DROP INDEX materializations_reconcile_scan_idx;
CREATE INDEX materializations_reconcile_scan_idx
    ON materializations (next_attempt_at_unix_millis, materialization_id)
    WHERE state IN ('pending', 'deleting');

ALTER TABLE idempotency_records ADD COLUMN expires_at_unix_millis bigint;
CREATE INDEX idempotency_records_expiry_idx ON idempotency_records (expires_at_unix_millis)
    WHERE expires_at_unix_millis IS NOT NULL;

ALTER TABLE idempotency_records ADD COLUMN resource_deleted_at_unix_millis bigint;
CREATE FUNCTION mark_idempotency_resource_deleted() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE idempotency_records
    SET resource_deleted_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
    WHERE operation = TG_ARGV[0] AND resource_id = to_jsonb(OLD)->>TG_ARGV[1]
        AND resource_deleted_at_unix_millis IS NULL;
    RETURN OLD;
END;
$$;
CREATE INDEX idempotency_records_resource_idx ON idempotency_records (operation, resource_id);
CREATE TRIGGER instance_idempotency_tombstone BEFORE DELETE ON instances
    FOR EACH ROW EXECUTE FUNCTION mark_idempotency_resource_deleted('create_instance', 'instance_id');
CREATE TRIGGER route_idempotency_tombstone BEFORE DELETE ON route_bindings
    FOR EACH ROW EXECUTE FUNCTION mark_idempotency_resource_deleted('create_route_binding', 'route_binding_id');
-- Backfill deletions that occurred before explicit tombstones existed.
UPDATE idempotency_records SET resource_deleted_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
WHERE (operation = 'create_instance' AND NOT EXISTS (SELECT 1 FROM instances WHERE instance_id = resource_id))
    OR (operation = 'create_route_binding' AND NOT EXISTS (SELECT 1 FROM route_bindings WHERE route_binding_id = resource_id));
