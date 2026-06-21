use deadpool_postgres::GenericClient;

use crate::{
    materialization::MaterializationRecord,
    route::{
        RouteDependencyLookup, RouteDependencySet, RouteEntry, RouteIdentity, RouteResolution,
    },
    store::{StoreError, StoreResult},
};

use super::{
    connection::PostgresStore,
    error::map_postgres_error,
    instance_ops::load_instance,
    mapping::{
        default_negative_cache_policy, generation_to_i64, materialization_from_row,
        route_binding_row_from_row, route_entry_from_rows, route_matches, RouteBindingRow,
    },
};

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

    let mut best: Option<(super::mapping::RouteScore, RouteBindingRow)> = None;
    for row in rows {
        let route = route_binding_row_from_row(&row)?;
        if let Some(score) = route_matches(&route.identity, &identity) {
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

    let entry = load_route_entry(&client, &route).await?;
    Ok(RouteResolution::Resolved(entry))
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
                namespace, state, backend_uri, backend_generation, rendered_objects
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
