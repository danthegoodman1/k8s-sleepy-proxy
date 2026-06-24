use deadpool_postgres::GenericClient;

use crate::{
    ids::Generation,
    instance::{validate_instance_state_transition, InstanceState, StateTransitionReason},
    materialization::{
        BeginSleepRequest, BeginSleepResult, CompleteWakeRequest, CompleteWakeResult,
        FinalizeSleepRequest, FinalizeSleepResult, LoadActiveMaterializationRequest,
        LoadReadyMaterializationRequest, MaterializationRecord, MaterializationState,
        RecordMaterializationRequest,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{
        backend_generation_to_i64, generation_to_i64, instance_from_row, instance_state_to_db,
        materialization_from_row, materialization_id, materialization_state_to_db,
        rendered_objects_to_json,
    },
};

pub(crate) async fn record_materialization(
    store: &PostgresStore,
    request: RecordMaterializationRequest,
) -> StoreResult<MaterializationRecord> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    ensure_instance_generation(&transaction, &request).await?;
    let record = upsert_materialization(&transaction, &request).await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(record)
}

pub(crate) async fn load_ready_materialization(
    store: &PostgresStore,
    request: LoadReadyMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let client = store.client().await?;
    let instance_id = request.instance_id.as_str();
    let instance_generation = generation_to_i64(request.instance_generation)?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let row = client
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects
            FROM materializations
            WHERE instance_id = $1
                AND instance_generation = $2
                AND cluster_id = $3
                AND namespace = $4
                AND state = 'ready'
            ",
            &[&instance_id, &instance_generation, &cluster_id, &namespace],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}

pub(crate) async fn load_active_materialization(
    store: &PostgresStore,
    request: LoadActiveMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let client = store.client().await?;

    load_active_materialization_from_client(&client, &request).await
}

pub(crate) async fn complete_wake(
    store: &PostgresStore,
    request: CompleteWakeRequest,
) -> StoreResult<CompleteWakeResult> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let instance_id = request.instance_id.as_str();
    let current = transaction
        .query_opt(
            "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            FOR UPDATE
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = current else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let current = instance_from_row(&row)?;

    if current.generation != request.expected_waking_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_waking_generation,
            actual: current.generation,
        });
    }

    validate_instance_state_transition(
        current.state,
        InstanceState::Running,
        &StateTransitionReason::MaterializationReady,
    )
    .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

    let running_generation = request.expected_waking_generation.next();
    let running_generation_db = generation_to_i64(running_generation)?;
    let running_state = instance_state_to_db(InstanceState::Running);
    let row = transaction
        .query_one(
            "
            UPDATE instances
            SET state = $2,
                generation = $3,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[&instance_id, &running_state, &running_generation_db],
        )
        .await
        .map_err(map_postgres_error)?;
    let instance = instance_from_row(&row)?;

    let mut materialization_request = RecordMaterializationRequest::new(
        request.instance_id,
        running_generation,
        request.target,
        MaterializationState::Ready,
        request.backend_generation,
    );
    materialization_request.backend = Some(request.backend);
    materialization_request.rendered_objects = request.rendered_objects;
    let materialization = upsert_materialization(&transaction, &materialization_request).await?;

    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(CompleteWakeResult {
        instance,
        materialization,
    })
}

pub(crate) async fn begin_sleep(
    store: &PostgresStore,
    request: BeginSleepRequest,
) -> StoreResult<BeginSleepResult> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let instance_id = request.instance_id.as_str();
    let current = transaction
        .query_opt(
            "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            FOR UPDATE
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = current else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let current = instance_from_row(&row)?;

    if current.generation != request.expected_running_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_running_generation,
            actual: current.generation,
        });
    }

    validate_instance_state_transition(
        current.state,
        InstanceState::Draining,
        &StateTransitionReason::IdleReported,
    )
    .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

    let draining_generation = request.expected_running_generation.next();
    let draining_generation_db = generation_to_i64(draining_generation)?;
    let draining_state = instance_state_to_db(InstanceState::Draining);
    let row = transaction
        .query_one(
            "
            UPDATE instances
            SET state = $2,
                generation = $3,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[&instance_id, &draining_state, &draining_generation_db],
        )
        .await
        .map_err(map_postgres_error)?;
    let instance = instance_from_row(&row)?;
    let materialization = mark_active_materialization_deleting_for_sleep(
        &transaction,
        &request.instance_id,
        &request.target,
        request.expected_running_generation,
    )
    .await?;

    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(BeginSleepResult {
        instance,
        materialization,
    })
}

pub(crate) async fn finalize_sleep(
    store: &PostgresStore,
    request: FinalizeSleepRequest,
) -> StoreResult<FinalizeSleepResult> {
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let instance_id = request.instance_id.as_str();
    let current = transaction
        .query_opt(
            "
            SELECT instance_id, workload_class_id, workload_class_version, values, state, generation
            FROM instances
            WHERE instance_id = $1
            FOR UPDATE
            ",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = current else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let current = instance_from_row(&row)?;

    if current.generation != request.expected_draining_generation {
        return Err(StoreError::GenerationConflict {
            expected: request.expected_draining_generation,
            actual: current.generation,
        });
    }

    validate_instance_state_transition(
        current.state,
        InstanceState::Cold,
        &StateTransitionReason::DrainCompleted,
    )
    .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

    let cold_generation = request.expected_draining_generation.next();
    let cold_generation_db = generation_to_i64(cold_generation)?;
    let cold_state = instance_state_to_db(InstanceState::Cold);
    let row = transaction
        .query_one(
            "
            UPDATE instances
            SET state = $2,
                generation = $3,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE instance_id = $1
            RETURNING instance_id, workload_class_id, workload_class_version, values, state, generation
            ",
            &[&instance_id, &cold_state, &cold_generation_db],
        )
        .await
        .map_err(map_postgres_error)?;
    let instance = instance_from_row(&row)?;
    let materialization = mark_active_materialization_state(
        &transaction,
        &request.instance_id,
        &request.target,
        MaterializationState::Deleted,
        Some(cold_generation),
        Some(&[]),
    )
    .await?;

    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(FinalizeSleepResult {
        instance,
        materialization,
    })
}

async fn upsert_materialization(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<MaterializationRecord> {
    ensure_no_rendered_object_ref_collision(client, request).await?;

    let id = materialization_id(&request.instance_id, &request.target)?;
    let instance_id = request.instance_id.as_str();
    let instance_generation = generation_to_i64(request.instance_generation)?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let state = materialization_state_to_db(request.state);
    let backend_uri = request.backend.as_ref().map(|backend| backend.uri());
    let backend_generation = backend_generation_to_i64(request.backend_generation)?;
    let rendered_objects = rendered_objects_to_json(&request.rendered_objects);
    let materialization_id = id.as_str();

    let row = client
        .query_opt(
            "
            INSERT INTO materializations (
                materialization_id,
                instance_id,
                instance_generation,
                cluster_id,
                namespace,
                state,
                backend_uri,
                backend_generation,
                rendered_objects
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (instance_id, cluster_id, namespace)
            DO UPDATE SET
                materialization_id = EXCLUDED.materialization_id,
                instance_generation = EXCLUDED.instance_generation,
                state = EXCLUDED.state,
                backend_uri = EXCLUDED.backend_uri,
                backend_generation = EXCLUDED.backend_generation,
                rendered_objects = EXCLUDED.rendered_objects,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materializations.backend_generation <= EXCLUDED.backend_generation
            RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects
            ",
            &[
                &materialization_id,
                &instance_id,
                &instance_generation,
                &cluster_id,
                &namespace,
                &state,
                &backend_uri,
                &backend_generation,
                &rendered_objects,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = row else {
        return Err(backend_generation_rewind_error(
            client,
            instance_id,
            cluster_id,
            namespace,
            request.backend_generation,
        )
        .await);
    };

    materialization_from_row(&row)
}

async fn ensure_no_rendered_object_ref_collision(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<()> {
    if request.rendered_objects.is_empty() || request.state == MaterializationState::Deleted {
        return Ok(());
    }

    client
        .batch_execute("LOCK TABLE materializations IN SHARE ROW EXCLUSIVE MODE")
        .await
        .map_err(map_postgres_error)?;

    let cluster_id = request.target.cluster_id();
    let instance_id = request.instance_id.as_str();
    let rendered_objects = rendered_objects_to_json(&request.rendered_objects);
    let collision = client
        .query_opt(
            "
            SELECT
                materializations.instance_id AS owner_instance_id,
                existing.object ->> 'api_version' AS api_version,
                existing.object ->> 'kind' AS kind,
                existing.object ->> 'namespace' AS namespace,
                existing.object ->> 'name' AS name
            FROM materializations
            CROSS JOIN LATERAL jsonb_array_elements(materializations.rendered_objects)
                AS existing(object)
            CROSS JOIN LATERAL jsonb_array_elements($3::jsonb)
                AS incoming(object)
            WHERE materializations.cluster_id = $1
                AND materializations.instance_id <> $2
                AND materializations.state <> 'deleted'
                AND existing.object ->> 'api_version' = incoming.object ->> 'api_version'
                AND existing.object ->> 'kind' = incoming.object ->> 'kind'
                AND existing.object ->> 'namespace' = incoming.object ->> 'namespace'
                AND existing.object ->> 'name' = incoming.object ->> 'name'
            LIMIT 1
            ",
            &[&cluster_id, &instance_id, &rendered_objects],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = collision else {
        return Ok(());
    };

    let owner_instance_id: String = row.get("owner_instance_id");
    let api_version: String = row.get("api_version");
    let kind: String = row.get("kind");
    let namespace: String = row.get("namespace");
    let name: String = row.get("name");
    Err(StoreError::invalid_argument(format!(
        "rendered Kubernetes object ref collision in cluster {cluster_id}: {api_version} {kind} {namespace}/{name} is already owned by active materialization for instance {owner_instance_id}"
    )))
}

async fn load_active_materialization_from_client(
    client: &impl GenericClient,
    request: &LoadActiveMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_id = request.instance_id.as_str();
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let row = client
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects
            FROM materializations
            WHERE instance_id = $1
                AND cluster_id = $2
                AND namespace = $3
                AND state <> 'deleted'
            ",
            &[&instance_id, &cluster_id, &namespace],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}

async fn mark_active_materialization_state(
    client: &impl GenericClient,
    instance_id: &crate::ids::InstanceId,
    target: &crate::materialization::MaterializationTarget,
    state: MaterializationState,
    instance_generation: Option<Generation>,
    rendered_objects: Option<&[crate::materialization::RenderedObjectRef]>,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_id = instance_id.as_str();
    let cluster_id = target.cluster_id();
    let namespace = target.namespace();
    let state = materialization_state_to_db(state);
    let rendered_objects = rendered_objects.map(rendered_objects_to_json);

    let row = if let Some(instance_generation) = instance_generation {
        let instance_generation = generation_to_i64(instance_generation)?;
        client
            .query_opt(
                "
                UPDATE materializations
                SET state = $4,
                    instance_generation = $5,
                    backend_uri = NULL,
                    rendered_objects = COALESCE($6, rendered_objects),
                    updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
                WHERE instance_id = $1
                    AND cluster_id = $2
                    AND namespace = $3
                    AND state <> 'deleted'
                RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                    namespace, state, backend_uri, backend_generation, rendered_objects
                ",
                &[
                    &instance_id,
                    &cluster_id,
                    &namespace,
                    &state,
                    &instance_generation,
                    &rendered_objects,
                ],
            )
            .await
            .map_err(map_postgres_error)?
    } else {
        client
            .query_opt(
                "
                UPDATE materializations
                SET state = $4,
                    backend_uri = NULL,
                    updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
                WHERE instance_id = $1
                    AND cluster_id = $2
                    AND namespace = $3
                    AND state <> 'deleted'
                RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                    namespace, state, backend_uri, backend_generation, rendered_objects
                ",
                &[&instance_id, &cluster_id, &namespace, &state],
            )
            .await
            .map_err(map_postgres_error)?
    };

    row.as_ref().map(materialization_from_row).transpose()
}

async fn mark_active_materialization_deleting_for_sleep(
    client: &impl GenericClient,
    instance_id: &crate::ids::InstanceId,
    target: &crate::materialization::MaterializationTarget,
    expected_instance_generation: Generation,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_id_value = instance_id.as_str();
    let cluster_id = target.cluster_id();
    let namespace = target.namespace();
    let state = materialization_state_to_db(MaterializationState::Deleting);
    let expected_generation_db = generation_to_i64(expected_instance_generation)?;
    let row = client
        .query_opt(
            "
            UPDATE materializations
            SET state = $5,
                backend_uri = NULL,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE instance_id = $1
                AND cluster_id = $2
                AND namespace = $3
                AND instance_generation = $4
                AND state <> 'deleted'
            RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects
            ",
            &[
                &instance_id_value,
                &cluster_id,
                &namespace,
                &expected_generation_db,
                &state,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    if let Some(row) = row {
        return materialization_from_row(&row).map(Some);
    }

    reject_active_materialization_generation_mismatch(
        client,
        instance_id,
        target,
        expected_instance_generation,
    )
    .await?;

    Ok(None)
}

async fn reject_active_materialization_generation_mismatch(
    client: &impl GenericClient,
    instance_id: &crate::ids::InstanceId,
    target: &crate::materialization::MaterializationTarget,
    expected_instance_generation: Generation,
) -> StoreResult<()> {
    let request = LoadActiveMaterializationRequest::new(instance_id.clone(), target.clone());
    let Some(active) = load_active_materialization_from_client(client, &request).await? else {
        return Ok(());
    };

    if active.instance_generation == expected_instance_generation {
        return Ok(());
    }

    Err(StoreError::GenerationConflict {
        expected: expected_instance_generation,
        actual: active.instance_generation,
    })
}

async fn ensure_instance_generation(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<()> {
    let instance_id = request.instance_id.as_str();
    let row = client
        .query_opt(
            "SELECT generation FROM instances WHERE instance_id = $1 FOR UPDATE",
            &[&instance_id],
        )
        .await
        .map_err(map_postgres_error)?;
    let Some(row) = row else {
        return Err(StoreError::NotFound {
            resource: "instance",
        });
    };
    let actual: i64 = row.get("generation");
    let expected = generation_to_i64(request.instance_generation)?;

    if actual == expected {
        Ok(())
    } else {
        Err(StoreError::GenerationConflict {
            expected: request.instance_generation,
            actual: Generation::new(
                u64::try_from(actual)
                    .map_err(|_| StoreError::internal("stored instance generation was negative"))?,
            ),
        })
    }
}

async fn backend_generation_rewind_error(
    client: &impl GenericClient,
    instance_id: &str,
    cluster_id: &str,
    namespace: &str,
    reported: crate::ids::BackendGeneration,
) -> StoreError {
    match client
        .query_opt(
            "
            SELECT backend_generation
            FROM materializations
            WHERE instance_id = $1 AND cluster_id = $2 AND namespace = $3
            ",
            &[&instance_id, &cluster_id, &namespace],
        )
        .await
    {
        Ok(Some(row)) => {
            let existing: i64 = row.get("backend_generation");
            StoreError::invalid_argument(format!(
                "materialization backend generation rewind rejected: existing backend generation {existing} is newer than reported backend generation {}",
                reported.get()
            ))
        }
        Ok(None) => StoreError::internal(
            "materialization backend generation rewind rejected but existing projection was not found",
        ),
        Err(error) => map_postgres_error(error),
    }
}
