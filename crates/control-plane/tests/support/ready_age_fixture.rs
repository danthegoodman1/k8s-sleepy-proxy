//! Controlled-time fixture only. This does not test elapsed activation time.
//! It changes one existing Ready timestamp, never lifecycle state or generation.

pub fn age_ready_sql(
    cluster: &str,
    namespace: &str,
    instance: &str,
    generation: u64,
    projection_generation: u64,
) -> Result<String, &'static str> {
    for value in [cluster, namespace, instance] {
        if value.is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        {
            return Err("controlled Ready-age fixture requires literal-safe identifiers");
        }
    }
    Ok(format!(
        r#"DO $fixture$
DECLARE changed bigint;
BEGIN
  PERFORM 1 FROM instances
    WHERE instance_id = '{instance}' AND state = 'running' AND generation = {generation}
    FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'fixture expected exact Running instance generation'; END IF;
  UPDATE materializations
    SET state_entered_at_unix_millis =
      (extract(epoch from clock_timestamp()) * 1000)::bigint - 300001
    WHERE instance_id = '{instance}' AND cluster_id = '{cluster}' AND namespace = '{namespace}'
      AND state = 'ready' AND instance_generation = {generation}
      AND projection_generation = {projection_generation};
  GET DIAGNOSTICS changed = ROW_COUNT;
  IF changed <> 1 THEN RAISE EXCEPTION 'fixture expected exactly one matching Ready row, got %', changed; END IF;
END $fixture$;
SELECT 'aged-ready:1';"#
    ))
}
