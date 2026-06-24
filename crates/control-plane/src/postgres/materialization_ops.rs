use deadpool_postgres::GenericClient;

use crate::{
    ids::Generation,
    instance::{validate_instance_state_transition, InstanceState, StateTransitionReason},
    materialization::{
        unix_millis_from_system_time, BeginSleepRequest, BeginSleepResult,
        ClaimMaterializationReconciliationRequest, CompleteWakeReconciliationRequest,
        CompleteWakeRequest, CompleteWakeResult, DeleteMaterializationReconciliationRequest,
        FinalizeSleepReconciliationRequest, FinalizeSleepRequest, FinalizeSleepResult,
        ForceDeleteMaterializationRequest, ForceReleaseExclusivityKeyRequest,
        ForceReleaseExclusivityKeyResult, ListMaterializationReconciliationCandidatesRequest,
        LoadActiveMaterializationRequest, LoadMaterializationRequest,
        LoadReadyMaterializationRequest, MaterializationRecord, MaterializationState,
        RecordMaterializationRequest, ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    mapping::{
        backend_generation_to_i64, generation_to_i64, instance_from_row, instance_state_to_db,
        materialization_from_row, materialization_id, materialization_state_to_db,
        rendered_exclusivity_keys_to_json, rendered_objects_to_json,
    },
};

pub(crate) async fn record_materialization(
    store: &PostgresStore,
    mut request: RecordMaterializationRequest,
) -> StoreResult<MaterializationRecord> {
    normalize_exclusivity_keys(&mut request.exclusivity_keys);
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
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
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

pub(crate) async fn load_materialization(
    store: &PostgresStore,
    request: LoadMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let client = store.client().await?;
    let materialization_id = request.materialization_id.as_str();
    let row = client
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE materialization_id = $1
            ",
            &[&materialization_id],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}

pub(crate) async fn complete_wake(
    store: &PostgresStore,
    mut request: CompleteWakeRequest,
) -> StoreResult<CompleteWakeResult> {
    normalize_exclusivity_keys(&mut request.exclusivity_keys);
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
    materialization_request.exclusivity_keys = request.exclusivity_keys;
    let materialization = upsert_materialization(&transaction, &materialization_request).await?;

    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(CompleteWakeResult {
        instance,
        materialization,
    })
}

fn normalize_exclusivity_keys(keys: &mut Vec<crate::workload::RenderedExclusivityKey>) {
    keys.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.value.cmp(&right.value))
    });
    keys.dedup();
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
        Some(&[]),
    )
    .await?;

    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(FinalizeSleepResult {
        instance,
        materialization,
    })
}

pub(crate) async fn list_materialization_reconciliation_candidates(
    store: &PostgresStore,
    request: ListMaterializationReconciliationCandidatesRequest,
) -> StoreResult<Vec<MaterializationRecord>> {
    let client = store.client().await?;
    let now = unix_millis_from_system_time(request.now).map_err(StoreError::invalid_argument)?;
    let limit = i64::try_from(request.limit)
        .map_err(|_| StoreError::invalid_argument("reconciliation candidate limit is too large"))?;
    let rows = client
        .query(
            "
            SELECT materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE state IN ('pending', 'deleting')
                AND (
                    reconcile_owner IS NULL
                    OR reconcile_lease_expires_at_unix_millis <= $1
                )
            ORDER BY updated_at_unix_millis, materialization_id
            LIMIT $2
            ",
            &[&now, &limit],
        )
        .await
        .map_err(map_postgres_error)?;

    rows.iter().map(materialization_from_row).collect()
}

pub(crate) async fn claim_materialization_reconciliation(
    store: &PostgresStore,
    request: ClaimMaterializationReconciliationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    let client = store.client().await?;
    validate_lease_owner(&request.owner)?;
    let now = unix_millis_from_system_time(request.now).map_err(StoreError::invalid_argument)?;
    let lease_expires_at = unix_millis_from_system_time(request.lease_expires_at)
        .map_err(StoreError::invalid_argument)?;
    if lease_expires_at <= now {
        return Err(StoreError::invalid_argument(
            "materialization reconciliation lease expiry must be after now",
        ));
    }

    let materialization_id = request.materialization_id.as_str();
    let owner = request.owner.as_str();
    let row = client
        .query_opt(
            "
            UPDATE materializations
            SET reconcile_owner = $2,
                reconcile_lease_expires_at_unix_millis = $3,
                reconcile_attempt = reconcile_attempt + 1,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND state IN ('pending', 'deleting')
                AND (
                    reconcile_owner IS NULL
                    OR reconcile_lease_expires_at_unix_millis <= $4
                )
            RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            ",
            &[&materialization_id, &owner, &lease_expires_at, &now],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}

pub(crate) async fn renew_materialization_reconciliation_lease(
    store: &PostgresStore,
    request: RenewMaterializationReconciliationLeaseRequest,
) -> StoreResult<bool> {
    let client = store.client().await?;
    validate_lease_owner(&request.owner)?;
    let lease_expires_at = unix_millis_from_system_time(request.lease_expires_at)
        .map_err(StoreError::invalid_argument)?;
    let materialization_id = request.materialization_id.as_str();
    let owner = request.owner.as_str();
    let updated = client
        .execute(
            "
            UPDATE materializations
            SET reconcile_lease_expires_at_unix_millis = $3,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND reconcile_owner = $2
                AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND state IN ('pending', 'deleting')
            ",
            &[&materialization_id, &owner, &lease_expires_at],
        )
        .await
        .map_err(map_postgres_error)?;

    Ok(updated == 1)
}

pub(crate) async fn release_materialization_reconciliation_lease(
    store: &PostgresStore,
    request: ReleaseMaterializationReconciliationLeaseRequest,
) -> StoreResult<bool> {
    let client = store.client().await?;
    validate_lease_owner(&request.owner)?;
    let materialization_id = request.materialization_id.as_str();
    let owner = request.owner.as_str();
    let updated = client
        .execute(
            "
            UPDATE materializations
            SET reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND reconcile_owner = $2
                AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
                AND state IN ('pending', 'deleting')
            ",
            &[&materialization_id, &owner],
        )
        .await
        .map_err(map_postgres_error)?;

    Ok(updated == 1)
}

pub(crate) async fn complete_wake_reconciliation(
    store: &PostgresStore,
    request: CompleteWakeReconciliationRequest,
) -> StoreResult<CompleteWakeResult> {
    validate_lease_owner(&request.lease_owner)?;
    let mut complete = request.complete;
    normalize_exclusivity_keys(&mut complete.exclusivity_keys);
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    ensure_reconciliation_lease(
        &transaction,
        request.materialization_id.as_str(),
        &request.lease_owner,
        MaterializationState::Pending,
        &complete.instance_id,
        complete.expected_waking_generation,
        &complete.target,
    )
    .await?;

    let result = complete_wake_in_transaction(&transaction, complete).await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(result)
}

pub(crate) async fn finalize_sleep_reconciliation(
    store: &PostgresStore,
    request: FinalizeSleepReconciliationRequest,
) -> StoreResult<FinalizeSleepResult> {
    validate_lease_owner(&request.lease_owner)?;
    let finalize = request.finalize;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let materialization_generation = previous_generation(finalize.expected_draining_generation)?;
    ensure_reconciliation_lease(
        &transaction,
        request.materialization_id.as_str(),
        &request.lease_owner,
        MaterializationState::Deleting,
        &finalize.instance_id,
        materialization_generation,
        &finalize.target,
    )
    .await?;

    let result = finalize_sleep_in_transaction(&transaction, finalize).await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(result)
}

pub(crate) async fn delete_materialization_reconciliation(
    store: &PostgresStore,
    request: DeleteMaterializationReconciliationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    validate_lease_owner(&request.lease_owner)?;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    ensure_reconciliation_lease(
        &transaction,
        request.materialization_id.as_str(),
        &request.lease_owner,
        request.expected_state,
        &request.instance_id,
        request.instance_generation,
        &request.target,
    )
    .await?;

    let materialization_id = request.materialization_id.as_str();
    let state = materialization_state_to_db(request.expected_state);
    let instance_id = request.instance_id.as_str();
    let instance_generation = generation_to_i64(request.instance_generation)?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let owner = request.lease_owner.as_str();
    let row = transaction
        .query_opt(
            "
            UPDATE materializations
            SET state = 'deleted',
                backend_uri = NULL,
                rendered_objects = '[]'::jsonb,
                exclusivity_keys = '[]'::jsonb,
                reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materialization_id = $1
                AND state = $2
                AND instance_id = $3
                AND instance_generation = $4
                AND cluster_id = $5
                AND namespace = $6
                AND reconcile_owner = $7
                AND reconcile_lease_expires_at_unix_millis > (extract(epoch from clock_timestamp()) * 1000)::bigint
            RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            ",
            &[
                &materialization_id,
                &state,
                &instance_id,
                &instance_generation,
                &cluster_id,
                &namespace,
                &owner,
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    transaction.commit().await.map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}

pub(crate) async fn force_delete_materialization(
    store: &PostgresStore,
    request: ForceDeleteMaterializationRequest,
) -> StoreResult<Option<MaterializationRecord>> {
    validate_operator_audit(&request.operator, &request.reason)?;
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let materialization_id = request.materialization_id.as_str();
    insert_operator_audit_event(
        &transaction,
        "force_delete_materialization",
        Some(materialization_id),
        None,
        None,
        None,
        &request.operator,
        &request.reason,
    )
    .await?;
    let existing = transaction
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE materialization_id = $1
            FOR UPDATE
            ",
            &[&materialization_id],
        )
        .await
        .map_err(map_postgres_error)?;
    if existing.is_some() {
        transaction
            .execute(
                "
                UPDATE materializations
                SET state = 'deleted',
                    backend_uri = NULL,
                    rendered_objects = '[]'::jsonb,
                    exclusivity_keys = '[]'::jsonb,
                    reconcile_owner = NULL,
                    reconcile_lease_expires_at_unix_millis = NULL,
                    updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
                WHERE materialization_id = $1
                ",
                &[&materialization_id],
            )
            .await
            .map_err(map_postgres_error)?;
    }
    transaction.commit().await.map_err(map_postgres_error)?;

    existing.as_ref().map(materialization_from_row).transpose()
}

pub(crate) async fn force_release_exclusivity_key(
    store: &PostgresStore,
    request: ForceReleaseExclusivityKeyRequest,
) -> StoreResult<ForceReleaseExclusivityKeyResult> {
    validate_operator_audit(&request.operator, &request.reason)?;
    if request.key_name.trim().is_empty() || request.key_value.trim().is_empty() {
        return Err(StoreError::invalid_argument(
            "force-release key name and value are required",
        ));
    }
    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    insert_operator_audit_event(
        &transaction,
        "force_release_exclusivity_key",
        None,
        Some(request.target.cluster_id()),
        Some(request.target.namespace()),
        Some(&request.key_name),
        &request.operator,
        &request.reason,
    )
    .await?;
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let key_name = request.key_name.as_str();
    let key_value = request.key_value.as_str();
    let updated = transaction
        .execute(
            "
            UPDATE materializations
            SET exclusivity_keys = COALESCE((
                    SELECT jsonb_agg(key)
                    FROM jsonb_array_elements(exclusivity_keys) AS key
                    WHERE NOT (
                        key ->> 'name' = $3
                        AND key ->> 'value' = $4
                    )
                ), '[]'::jsonb),
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE cluster_id = $1
                AND namespace = $2
                AND state <> 'deleted'
                AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(exclusivity_keys) AS key
                    WHERE key ->> 'name' = $3
                        AND key ->> 'value' = $4
                )
            ",
            &[&cluster_id, &namespace, &key_name, &key_value],
        )
        .await
        .map_err(map_postgres_error)?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(ForceReleaseExclusivityKeyResult {
        updated_materializations: usize::try_from(updated).map_err(|_| {
            StoreError::internal("force-release updated row count did not fit usize")
        })?,
    })
}

async fn complete_wake_in_transaction(
    transaction: &impl GenericClient,
    request: CompleteWakeRequest,
) -> StoreResult<CompleteWakeResult> {
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
    materialization_request.exclusivity_keys = request.exclusivity_keys;
    let materialization = upsert_materialization(transaction, &materialization_request).await?;

    Ok(CompleteWakeResult {
        instance,
        materialization,
    })
}

async fn finalize_sleep_in_transaction(
    transaction: &impl GenericClient,
    request: FinalizeSleepRequest,
) -> StoreResult<FinalizeSleepResult> {
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
        transaction,
        &request.instance_id,
        &request.target,
        MaterializationState::Deleted,
        Some(cold_generation),
        Some(&[]),
        Some(&[]),
    )
    .await?;

    Ok(FinalizeSleepResult {
        instance,
        materialization,
    })
}

async fn ensure_reconciliation_lease(
    client: &impl GenericClient,
    materialization_id: &str,
    owner: &str,
    expected_state: MaterializationState,
    instance_id: &crate::ids::InstanceId,
    instance_generation: Generation,
    target: &crate::materialization::MaterializationTarget,
) -> StoreResult<()> {
    let expected_state = materialization_state_to_db(expected_state);
    let instance_id_value = instance_id.as_str();
    let instance_generation = generation_to_i64(instance_generation)?;
    let cluster_id = target.cluster_id();
    let namespace = target.namespace();
    let row = client
        .query_opt(
            "
            SELECT instance_generation, reconcile_owner, reconcile_lease_expires_at_unix_millis
            FROM materializations
            WHERE materialization_id = $1
                AND state = $2
                AND instance_id = $3
                AND instance_generation = $4
                AND cluster_id = $5
                AND namespace = $6
            FOR UPDATE
            ",
            &[
                &materialization_id,
                &expected_state,
                &instance_id_value,
                &instance_generation,
                &cluster_id,
                &namespace,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = row else {
        return Err(StoreError::NotFound {
            resource: "materialization",
        });
    };
    let actual_generation: i64 = row.get("instance_generation");
    if actual_generation != instance_generation {
        return Err(StoreError::GenerationConflict {
            expected: Generation::new(instance_generation as u64),
            actual: Generation::new(u64::try_from(actual_generation).map_err(|_| {
                StoreError::internal("stored materialization generation was negative")
            })?),
        });
    }
    let actual_owner: Option<String> = row.get("reconcile_owner");
    if actual_owner.as_deref() != Some(owner) {
        return Err(StoreError::unavailable(
            "materialization reconciliation lease is not currently owned",
        ));
    }
    let expires_at: Option<i64> = row.get("reconcile_lease_expires_at_unix_millis");
    let now: i64 = client
        .query_one(
            "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint AS now",
            &[],
        )
        .await
        .map_err(map_postgres_error)?
        .get("now");
    if expires_at.map_or(true, |expires_at| expires_at <= now) {
        return Err(StoreError::unavailable(
            "materialization reconciliation lease is expired",
        ));
    }

    Ok(())
}

fn validate_lease_owner(owner: &str) -> StoreResult<()> {
    if owner.trim().is_empty() {
        return Err(StoreError::invalid_argument(
            "materialization reconciliation owner is required",
        ));
    }
    Ok(())
}

fn previous_generation(generation: Generation) -> StoreResult<Generation> {
    generation
        .get()
        .checked_sub(1)
        .map(Generation::new)
        .ok_or_else(|| StoreError::invalid_argument("generation has no predecessor"))
}

fn validate_operator_audit(operator: &str, reason: &str) -> StoreResult<()> {
    if operator.trim().is_empty() || reason.trim().is_empty() {
        return Err(StoreError::invalid_argument(
            "operator and reason are required for force materialization operations",
        ));
    }
    Ok(())
}

async fn insert_operator_audit_event(
    client: &impl GenericClient,
    operation: &str,
    materialization_id: Option<&str>,
    cluster_id: Option<&str>,
    namespace: Option<&str>,
    key_name: Option<&str>,
    operator: &str,
    reason: &str,
) -> StoreResult<()> {
    client
        .execute(
            "
            INSERT INTO materialization_operator_audit_events (
                operation, materialization_id, cluster_id, namespace,
                key_name, operator, reason
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ",
            &[
                &operation,
                &materialization_id,
                &cluster_id,
                &namespace,
                &key_name,
                &operator,
                &reason,
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    Ok(())
}

async fn upsert_materialization(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<MaterializationRecord> {
    acquire_exclusivity_keys(client, request).await?;
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
    let exclusivity_keys = rendered_exclusivity_keys_to_json(&request.exclusivity_keys);
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
                rendered_objects,
                exclusivity_keys
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (instance_id, cluster_id, namespace)
            DO UPDATE SET
                materialization_id = EXCLUDED.materialization_id,
                instance_generation = EXCLUDED.instance_generation,
                state = EXCLUDED.state,
                backend_uri = EXCLUDED.backend_uri,
                backend_generation = EXCLUDED.backend_generation,
                rendered_objects = EXCLUDED.rendered_objects,
                exclusivity_keys = EXCLUDED.exclusivity_keys,
                reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE materializations.backend_generation <= EXCLUDED.backend_generation
            RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
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
                &exclusivity_keys,
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

async fn acquire_exclusivity_keys(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<()> {
    if request.exclusivity_keys.is_empty() || request.state == MaterializationState::Deleted {
        return Ok(());
    }

    for key in &request.exclusivity_keys {
        let lock_key = exclusivity_advisory_lock_id(request, key);
        let row = client
            .query_one(
                "SELECT pg_try_advisory_xact_lock($1) AS acquired",
                &[&lock_key],
            )
            .await
            .map_err(map_postgres_error)?;
        let acquired: bool = row.get("acquired");
        if !acquired {
            return Err(exclusivity_key_conflict_error(
                request.target.cluster_id(),
                request.target.namespace(),
                &key.name,
                None,
                None,
            ));
        }
    }

    ensure_no_exclusivity_key_conflict(client, request).await
}

fn exclusivity_advisory_lock_id(
    request: &RecordMaterializationRequest,
    key: &crate::workload::RenderedExclusivityKey,
) -> i64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    fn feed(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    fn feed_part(hash: &mut u64, part: &str) {
        feed(hash, &(part.len() as u64).to_be_bytes());
        feed(hash, part.as_bytes());
    }

    let mut hash = FNV_OFFSET_BASIS;
    feed_part(&mut hash, request.target.cluster_id());
    feed_part(&mut hash, request.target.namespace());
    feed_part(&mut hash, &key.name);
    feed_part(&mut hash, &key.value);

    i64::from_ne_bytes(hash.to_ne_bytes())
}

async fn ensure_no_exclusivity_key_conflict(
    client: &impl GenericClient,
    request: &RecordMaterializationRequest,
) -> StoreResult<()> {
    let cluster_id = request.target.cluster_id();
    let namespace = request.target.namespace();
    let instance_id = request.instance_id.as_str();
    let incoming = rendered_exclusivity_keys_to_json(&request.exclusivity_keys);
    let collision = client
        .query_opt(
            "
            SELECT
                materializations.instance_id AS owner_instance_id,
                materializations.instance_generation AS owner_instance_generation,
                existing.key ->> 'name' AS key_name
            FROM materializations
            CROSS JOIN LATERAL jsonb_array_elements(materializations.exclusivity_keys)
                AS existing(key)
            CROSS JOIN LATERAL jsonb_array_elements($4::jsonb)
                AS incoming(key)
            WHERE materializations.cluster_id = $1
                AND materializations.namespace = $2
                AND materializations.instance_id <> $3
                AND materializations.state <> 'deleted'
                AND existing.key ->> 'name' = incoming.key ->> 'name'
                AND existing.key ->> 'value' = incoming.key ->> 'value'
            LIMIT 1
            ",
            &[&cluster_id, &namespace, &instance_id, &incoming],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(row) = collision else {
        return Ok(());
    };

    let owner_instance_id: String = row.get("owner_instance_id");
    let owner_instance_generation: i64 = row.get("owner_instance_generation");
    let key_name: String = row.get("key_name");
    Err(exclusivity_key_conflict_error(
        cluster_id,
        namespace,
        &key_name,
        Some(owner_instance_id),
        u64::try_from(owner_instance_generation)
            .ok()
            .map(crate::ids::Generation::new),
    ))
}

fn exclusivity_key_conflict_error(
    cluster_id: &str,
    namespace: &str,
    key_name: &str,
    owner_instance_id: Option<String>,
    owner_generation: Option<crate::ids::Generation>,
) -> StoreError {
    StoreError::ExclusivityConflict {
        cluster_id: cluster_id.to_owned(),
        namespace: namespace.to_owned(),
        key_name: key_name.to_owned(),
        owner_instance_id,
        owner_generation,
    }
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
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
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
    exclusivity_keys: Option<&[crate::workload::RenderedExclusivityKey]>,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_id = instance_id.as_str();
    let cluster_id = target.cluster_id();
    let namespace = target.namespace();
    let state = materialization_state_to_db(state);
    let rendered_objects = rendered_objects.map(rendered_objects_to_json);
    let exclusivity_keys = exclusivity_keys.map(rendered_exclusivity_keys_to_json);

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
                    exclusivity_keys = COALESCE($7, exclusivity_keys),
                    reconcile_owner = NULL,
                    reconcile_lease_expires_at_unix_millis = NULL,
                    updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
                WHERE instance_id = $1
                    AND cluster_id = $2
                    AND namespace = $3
                    AND state <> 'deleted'
                RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                    namespace, state, backend_uri, backend_generation, rendered_objects,
                    exclusivity_keys, reconcile_owner,
                    reconcile_lease_expires_at_unix_millis, reconcile_attempt
                ",
                &[
                    &instance_id,
                    &cluster_id,
                    &namespace,
                    &state,
                    &instance_generation,
                    &rendered_objects,
                    &exclusivity_keys,
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
                    reconcile_owner = NULL,
                    reconcile_lease_expires_at_unix_millis = NULL,
                    updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
                WHERE instance_id = $1
                    AND cluster_id = $2
                    AND namespace = $3
                    AND state <> 'deleted'
                RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                    namespace, state, backend_uri, backend_generation, rendered_objects,
                    exclusivity_keys, reconcile_owner,
                    reconcile_lease_expires_at_unix_millis, reconcile_attempt
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
                reconcile_owner = NULL,
                reconcile_lease_expires_at_unix_millis = NULL,
                updated_at_unix_millis = (extract(epoch from clock_timestamp()) * 1000)::bigint
            WHERE instance_id = $1
                AND cluster_id = $2
                AND namespace = $3
                AND instance_generation = $4
                AND state <> 'deleted'
            RETURNING materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
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
