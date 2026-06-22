use deadpool_postgres::GenericClient;

use crate::{
    ids::Generation,
    instance::{validate_instance_state_transition, InstanceState, StateTransitionReason},
    materialization::{
        CompleteWakeRequest, CompleteWakeResult, MaterializationRecord, MaterializationState,
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

async fn upsert_materialization(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<MaterializationRecord> {
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
