use serde_json::Value;

use deadpool_postgres::GenericClient;

use crate::{
    materialization::MaterializationRecord,
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
        ListRouteBindingsForInstanceRequest, RouteBindingRecord, RouteDependencyLookup,
        RouteDependencySet, RouteEntry, RouteIdentity, RouteResolution,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    idempotency::{self, CREATE_ROUTE_BINDING_OPERATION},
    instance_ops::load_instance,
    mapping::{
        default_negative_cache_policy, generation_to_i64, materialization_from_row, protocol_to_db,
        route_binding_from_row, route_binding_row_from_row, route_entry_from_rows,
        route_identity_parts, RouteBindingRow,
    },
};

pub(crate) async fn create_route_binding(
    store: &PostgresStore,
    request: CreateRouteBindingRequest,
) -> StoreResult<RouteBindingRecord> {
    super::mapping::validate_route_protocol(&request.spec())?;

    let mut client = store.client().await?;
    let transaction = client.transaction().await.map_err(map_postgres_error)?;
    let fingerprint = idempotency::create_route_binding_fingerprint(&request)?;
    let idempotency_key = request.idempotency_key.as_str();
    let route_binding_id = request.route_binding_id.as_str();
    let inserted = transaction
        .execute(
            "
            INSERT INTO idempotency_records (
                idempotency_key,
                operation,
                request_fingerprint,
                resource_id
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (idempotency_key) DO NOTHING
            ",
            &[
                &idempotency_key,
                &CREATE_ROUTE_BINDING_OPERATION,
                &fingerprint,
                &route_binding_id,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    if inserted == 0 {
        let result =
            replay_create_route_binding(&transaction, idempotency_key, &fingerprint).await?;
        transaction.commit().await.map_err(map_postgres_error)?;
        return Ok(result);
    }

    let result = insert_route_binding(&transaction, &request).await?;
    transaction.commit().await.map_err(map_postgres_error)?;

    Ok(result)
}

pub(crate) async fn get_route_binding(
    store: &PostgresStore,
    request: GetRouteBindingRequest,
) -> StoreResult<Option<RouteBindingRecord>> {
    let client = store.client().await?;

    load_route_binding_record(&client, request.route_binding_id.as_str()).await
}

pub(crate) async fn delete_route_binding(
    store: &PostgresStore,
    request: DeleteRouteBindingRequest,
) -> StoreResult<bool> {
    let client = store.client().await?;
    let route_binding_id = request.route_binding_id.as_str();
    let deleted = client
        .execute(
            "DELETE FROM route_bindings WHERE route_binding_id = $1",
            &[&route_binding_id],
        )
        .await
        .map_err(map_postgres_error)?;

    Ok(deleted > 0)
}

pub(crate) async fn list_route_bindings_for_instance(
    store: &PostgresStore,
    request: ListRouteBindingsForInstanceRequest,
) -> StoreResult<Vec<RouteBindingRecord>> {
    let client = store.client().await?;
    let rows = client
        .query(
            "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            FROM route_bindings
            WHERE instance_id = $1
            ORDER BY route_binding_id
            ",
            &[&request.instance_id.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;

    rows.iter().map(route_binding_from_row).collect()
}

pub(crate) async fn resolve_route(
    store: &PostgresStore,
    identity: RouteIdentity,
) -> StoreResult<RouteResolution> {
    let client = store.client().await?;
    let identity_kind = match identity {
        RouteIdentity::Http { .. } => "http",
        RouteIdentity::Sni { .. } => "sni",
    };
    let rows = client
        .query(
            "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            FROM route_bindings
            WHERE identity_kind = $1
            ",
            &[&identity_kind],
        )
        .await
        .map_err(map_postgres_error)?;

    let mut best: Option<(crate::route::RouteMatchScore, RouteBindingRow)> = None;
    for row in rows {
        let route = route_binding_row_from_row(&row)?;
        if let Some(score) = crate::route::route_match_score(&route.identity, &identity) {
            if best
                .as_ref()
                .map(|(best_score, _)| score > *best_score)
                .unwrap_or(true)
            {
                best = Some((score, route));
            }
        }
    }

    let Some((_, route)) = best else {
        return Ok(RouteResolution::Miss {
            negative_cache: default_negative_cache_policy(),
        });
    };

    let matched_identity = route.identity.clone();
    let entry = load_route_entry(&client, &route).await?;
    Ok(RouteResolution::Resolved {
        matched_identity,
        entry,
    })
}

pub(crate) async fn lookup_route_dependencies(
    store: &PostgresStore,
    request: RouteDependencyLookup,
) -> StoreResult<Option<RouteDependencySet>> {
    let client = store.client().await?;
    let route_binding_id = request.route_binding_id().as_str();
    let Some(route) = load_route_binding(&client, route_binding_id).await? else {
        return Ok(None);
    };
    let instance = load_instance(&client, route.instance_id.as_str())
        .await?
        .ok_or(StoreError::NotFound {
            resource: "instance",
        })?;
    let materialization =
        load_ready_materialization(&client, route.instance_id.as_str(), instance.generation)
            .await?;

    Ok(Some(RouteDependencySet {
        route_binding_id: route.id,
        instance_id: instance.id,
        materialization_generation: materialization.map(|record| record.backend_generation),
    }))
}

async fn replay_create_route_binding(
    client: &impl GenericClient,
    idempotency_key: &str,
    fingerprint: &Value,
) -> StoreResult<RouteBindingRecord> {
    let row = client
        .query_one(
            "
            SELECT operation, request_fingerprint, resource_id
            FROM idempotency_records
            WHERE idempotency_key = $1
            ",
            &[&idempotency_key],
        )
        .await
        .map_err(map_postgres_error)?;
    let operation: String = row.get("operation");
    let stored_fingerprint: Value = row.get("request_fingerprint");
    let resource_id: String = row.get("resource_id");

    if operation != CREATE_ROUTE_BINDING_OPERATION || stored_fingerprint != *fingerprint {
        return Err(idempotency::idempotency_conflict());
    }

    load_route_binding_record(client, &resource_id)
        .await?
        .ok_or(StoreError::NotFound {
            resource: "route binding",
        })
}

async fn insert_route_binding(
    client: &impl GenericClient,
    request: &CreateRouteBindingRequest,
) -> StoreResult<RouteBindingRecord> {
    let parts = route_identity_parts(&request.identity);
    let route_binding_id = request.route_binding_id.as_str();
    let instance_id = request.instance_id.as_str();
    let protocol = protocol_to_db(request.protocol);
    let path_prefix = parts.path_prefix.as_deref();
    let row = client
        .query_one(
            "
            INSERT INTO route_bindings (
                route_binding_id,
                instance_id,
                identity_key,
                identity_kind,
                host_kind,
                host,
                path_prefix,
                protocol
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            ",
            &[
                &route_binding_id,
                &instance_id,
                &parts.key,
                &parts.identity_kind,
                &parts.host_kind,
                &parts.host,
                &path_prefix,
                &protocol,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    route_binding_from_row(&row)
}

async fn load_route_binding_record(
    client: &impl GenericClient,
    route_binding_id: &str,
) -> StoreResult<Option<RouteBindingRecord>> {
    let row = client
        .query_opt(
            "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            FROM route_bindings
            WHERE route_binding_id = $1
            ",
            &[&route_binding_id],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(route_binding_from_row).transpose()
}

async fn load_route_binding(
    client: &impl GenericClient,
    route_binding_id: &str,
) -> StoreResult<Option<RouteBindingRow>> {
    let row = client
        .query_opt(
            "
            SELECT route_binding_id, instance_id, identity_kind, host_kind, host, path_prefix, protocol
            FROM route_bindings
            WHERE route_binding_id = $1
            ",
            &[&route_binding_id],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(route_binding_row_from_row).transpose()
}

async fn load_route_entry(
    client: &impl GenericClient,
    route: &RouteBindingRow,
) -> StoreResult<RouteEntry> {
    let instance = load_instance(client, route.instance_id.as_str())
        .await?
        .ok_or(StoreError::NotFound {
            resource: "instance",
        })?;
    let materialization =
        load_ready_materialization(client, route.instance_id.as_str(), instance.generation).await?;

    Ok(route_entry_from_rows(route, instance, materialization))
}

pub(crate) async fn load_ready_materialization(
    client: &impl GenericClient,
    instance_id: &str,
    instance_generation: crate::ids::Generation,
) -> StoreResult<Option<MaterializationRecord>> {
    let instance_generation = generation_to_i64(instance_generation)?;
    let row = client
        .query_opt(
            "
            SELECT materialization_id, instance_id, instance_generation, cluster_id,
                namespace, state, backend_uri, backend_generation, rendered_objects,
                exclusivity_keys, reconcile_owner,
                reconcile_lease_expires_at_unix_millis, reconcile_attempt
            FROM materializations
            WHERE instance_id = $1 AND instance_generation = $2 AND state = 'ready'
            ORDER BY backend_generation DESC, materialization_id
            LIMIT 1
            ",
            &[&instance_id, &instance_generation],
        )
        .await
        .map_err(map_postgres_error)?;

    row.as_ref().map(materialization_from_row).transpose()
}
