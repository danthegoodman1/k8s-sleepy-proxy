-- A bounded transactional outbox. Updating this single row serializes revision
-- visibility with commit; sequences alone cannot supply that ordering. Every
-- process reads it independently; no consumer acknowledges/deletes another's work.
CREATE TABLE route_change_revision (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL CHECK (revision >= 0)
);
INSERT INTO route_change_revision VALUES (true, 0);
CREATE TABLE route_change_outbox (
    revision bigint PRIMARY KEY,
    payload jsonb NOT NULL,
    created_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint
);
CREATE FUNCTION record_route_changes() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    next_revision bigint;
    event_count bigint;
    source_query text;
    payload_query text;
BEGIN
    source_query = CASE WHEN TG_OP = 'DELETE' THEN 'SELECT to_jsonb(o) AS value FROM old_rows o' ELSE 'SELECT to_jsonb(n) AS value FROM new_rows n' END;
    IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'instances' THEN
        source_query = 'SELECT to_jsonb(n) AS value FROM new_rows n JOIN old_rows o USING(instance_id) WHERE (n.state, n.generation) IS DISTINCT FROM (o.state, o.generation)';
    ELSIF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'materializations' THEN
        source_query = 'SELECT to_jsonb(n) AS value FROM new_rows n JOIN old_rows o USING(materialization_id) WHERE (n.state, n.backend_uri, n.backend_generation, n.instance_generation, n.failure_kind) IS DISTINCT FROM (o.state, o.backend_uri, o.backend_generation, o.instance_generation, o.failure_kind)';
    END IF;
    IF TG_TABLE_NAME = 'route_bindings' THEN
        payload_query = format('SELECT jsonb_build_object(''kind'', ''route'', ''removed'', %L::boolean,
            ''route_binding_id'', value->''route_binding_id'', ''instance_id'', value->''instance_id'',
            ''identity_kind'', value->''identity_kind'', ''host_kind'', value->''host_kind'',
            ''host'', value->''host'', ''path_prefix'', value->''path_prefix'', ''protocol'', value->''protocol'') AS payload FROM (%s) changed', TG_OP = 'DELETE', source_query);
    ELSE
        payload_query = format('SELECT DISTINCT jsonb_build_object(''kind'', ''instance'', ''instance_id'', value->''instance_id'') AS payload FROM (%s) changed', source_query);
    END IF;
    EXECUTE 'SELECT count(*) FROM (' || payload_query || ') p' INTO event_count;
    IF event_count = 0 THEN RETURN NULL; END IF;
    -- One update per statement avoids quadratic same-row version chains for
    -- bulk writes. The held row lock orders every allocated range with commit.
    UPDATE route_change_revision SET revision = revision + event_count WHERE singleton RETURNING revision INTO next_revision;
    EXECUTE 'INSERT INTO route_change_outbox(revision, payload) SELECT $1 + row_number() OVER (), payload FROM (' || payload_query || ') p' USING next_revision - event_count;
    DELETE FROM route_change_outbox WHERE revision <= next_revision - 100000;
    RETURN NULL;
END;
$$;

CREATE FUNCTION lifecycle_operation_timeout_millis() RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT COALESCE(NULLIF(current_setting('sleepypods.operation_timeout_ms', true), '')::bigint, 600000);
$$;
ALTER TABLE materializations
    ADD COLUMN wake_failure_message text NOT NULL DEFAULT '' CHECK (octet_length(wake_failure_message) <= 2048),
    ADD COLUMN failure_requires_cleanup boolean NOT NULL DEFAULT false,
    ADD COLUMN failure_count integer NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    ADD COLUMN failure_kind text CHECK (failure_kind IN ('transient', 'permanent', 'deadline', 'uncertain')),
    ADD COLUMN failure_message text NOT NULL DEFAULT '' CHECK (octet_length(failure_message) <= 2048),
    ADD COLUMN operation_deadline_unix_millis bigint NOT NULL DEFAULT ((extract(epoch from clock_timestamp()) * 1000)::bigint + lifecycle_operation_timeout_millis());
CREATE FUNCTION reset_materialization_schedule() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.state IS DISTINCT FROM OLD.state AND NEW.state IN ('pending', 'deleting')) OR NEW.instance_generation IS DISTINCT FROM OLD.instance_generation THEN
        IF NEW.state = 'pending' THEN NEW.wake_failure_message = ''; END IF;
        NEW.failure_requires_cleanup = false;
        NEW.failure_count = 0;
        NEW.failure_kind = NULL;
        NEW.failure_message = '';
        NEW.operation_deadline_unix_millis = GREATEST((extract(epoch from clock_timestamp()) * 1000)::bigint, NEW.drain_not_before_unix_millis) + lifecycle_operation_timeout_millis();
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER materialization_schedule BEFORE UPDATE ON materializations
    FOR EACH ROW EXECUTE FUNCTION reset_materialization_schedule();

CREATE TRIGGER route_bindings_route_change_insert AFTER INSERT ON route_bindings
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER route_bindings_route_change_update AFTER UPDATE ON route_bindings
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER route_bindings_route_change_delete AFTER DELETE ON route_bindings
    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER instances_route_change_insert AFTER INSERT ON instances
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER instances_route_change_update AFTER UPDATE ON instances
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER instances_route_change_delete AFTER DELETE ON instances
    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER materializations_route_change_insert AFTER INSERT ON materializations
    REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER materializations_route_change_update AFTER UPDATE ON materializations
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
CREATE TRIGGER materializations_route_change_delete AFTER DELETE ON materializations
    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION record_route_changes();
