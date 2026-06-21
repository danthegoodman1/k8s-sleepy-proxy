use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{json, Value};
use tokio_postgres::Row;

use crate::{
    http01::{Http01ChallengeKey, Http01ChallengeRecord},
    ids::{
        BackendGeneration, Generation, InstanceId, MaterializationId, RouteBindingId,
        WorkloadClassId,
    },
    instance::{InstanceRecord, InstanceState, InstanceValues},
    materialization::{
        BackendEndpoint, MaterializationRecord, MaterializationState, MaterializationTarget,
        RenderedObjectRef,
    },
    route::{
        CachePolicy, PathPrefix, ProtocolRoute, RouteBindingRecord, RouteBindingSpec, RouteEntry,
        RouteHost, RouteHostKind, RouteIdentity,
    },
    store::{StoreError, StoreResult},
    workload::{WorkloadClassVersion, WorkloadClassVersionRef},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RouteBindingRow {
    pub id: RouteBindingId,
    pub instance_id: InstanceId,
    pub identity: RouteIdentity,
    pub protocol: ProtocolRoute,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RouteIdentityParts {
    pub key: String,
    pub identity_kind: &'static str,
    pub host_kind: &'static str,
    pub host: String,
    pub path_prefix: Option<String>,
}

pub(crate) fn workload_class_version_from_row(row: &Row) -> StoreResult<WorkloadClassVersion> {
    let class_id: String = row.get("class_id");
    let version: i64 = row.get("version");
    let template_generation: i64 = row.get("template_generation");
    let default_values: Value = row.get("default_values");

    Ok(WorkloadClassVersion {
        reference: WorkloadClassVersionRef {
            class_id: WorkloadClassId::new(class_id).map_err(invalid_stored_data)?,
            version: generation_from_i64(version)?,
        },
        template_generation: generation_from_i64(template_generation)?,
        default_values: values_from_json(default_values)?,
    })
}

pub(crate) fn instance_from_row(row: &Row) -> StoreResult<InstanceRecord> {
    let instance_id: String = row.get("instance_id");
    let workload_class_id: String = row.get("workload_class_id");
    let workload_class_version: i64 = row.get("workload_class_version");
    let values: Value = row.get("values");
    let state: String = row.get("state");
    let generation: i64 = row.get("generation");

    Ok(InstanceRecord {
        id: InstanceId::new(instance_id).map_err(invalid_stored_data)?,
        workload_class: WorkloadClassVersionRef {
            class_id: WorkloadClassId::new(workload_class_id).map_err(invalid_stored_data)?,
            version: generation_from_i64(workload_class_version)?,
        },
        values: values_from_json(values)?,
        state: instance_state_from_db(&state)?,
        generation: generation_from_i64(generation)?,
    })
}

pub(crate) fn route_binding_from_row(row: &Row) -> StoreResult<RouteBindingRecord> {
    let binding = route_binding_row_from_row(row)?;

    Ok(RouteBindingRecord {
        id: binding.id,
        instance_id: binding.instance_id,
        identity: binding.identity,
        protocol: binding.protocol,
    })
}

pub(crate) fn route_binding_row_from_row(row: &Row) -> StoreResult<RouteBindingRow> {
    let route_binding_id: String = row.get("route_binding_id");
    let instance_id: String = row.get("instance_id");
    let identity_kind: String = row.get("identity_kind");
    let host_kind: String = row.get("host_kind");
    let host: String = row.get("host");
    let path_prefix: Option<String> = row.get("path_prefix");
    let protocol: String = row.get("protocol");

    Ok(RouteBindingRow {
        id: RouteBindingId::new(route_binding_id).map_err(invalid_stored_data)?,
        instance_id: InstanceId::new(instance_id).map_err(invalid_stored_data)?,
        identity: route_identity_from_parts(&identity_kind, &host_kind, &host, path_prefix)?,
        protocol: protocol_from_db(&protocol)?,
    })
}

pub(crate) fn route_entry_from_rows(
    route: &RouteBindingRow,
    instance: InstanceRecord,
    materialization: Option<MaterializationRecord>,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: route.id.clone(),
        instance_id: instance.id,
        instance_state: instance.state,
        instance_generation: instance.generation,
        backend: materialization
            .as_ref()
            .and_then(|record| record.backend.clone()),
        backend_generation: materialization.map(|record| record.backend_generation),
    }
}

pub(crate) fn materialization_from_row(row: &Row) -> StoreResult<MaterializationRecord> {
    let materialization_id: String = row.get("materialization_id");
    let instance_id: String = row.get("instance_id");
    let instance_generation: i64 = row.get("instance_generation");
    let cluster_id: String = row.get("cluster_id");
    let namespace: String = row.get("namespace");
    let state: String = row.get("state");
    let backend_uri: Option<String> = row.get("backend_uri");
    let backend_generation: i64 = row.get("backend_generation");
    let rendered_objects: Value = row.get("rendered_objects");

    Ok(MaterializationRecord {
        id: MaterializationId::new(materialization_id).map_err(invalid_stored_data)?,
        instance_id: InstanceId::new(instance_id).map_err(invalid_stored_data)?,
        instance_generation: generation_from_i64(instance_generation)?,
        target: MaterializationTarget::new(cluster_id, namespace).map_err(invalid_stored_data)?,
        state: materialization_state_from_db(&state)?,
        backend: backend_uri
            .map(BackendEndpoint::new)
            .transpose()
            .map_err(invalid_stored_data)?,
        backend_generation: backend_generation_from_i64(backend_generation)?,
        rendered_objects: rendered_objects_from_json(rendered_objects)?,
    })
}

pub(crate) fn http01_from_row(row: &Row) -> StoreResult<Http01ChallengeRecord> {
    let host: String = row.get("host");
    let token: String = row.get("token");
    let key_authorization: String = row.get("key_authorization");
    let expires_at_unix_millis: i64 = row.get("expires_at_unix_millis");
    let expires_at = system_time_from_unix_millis(expires_at_unix_millis)?;
    let key = Http01ChallengeKey::new(host, token).map_err(invalid_stored_data)?;

    Http01ChallengeRecord::new(key, key_authorization, expires_at, UNIX_EPOCH)
        .map_err(invalid_stored_data)
}

pub(crate) fn values_to_json(values: &InstanceValues) -> StoreResult<Value> {
    serde_json::to_value(values)
        .map_err(|error| StoreError::internal(format!("failed to encode instance values: {error}")))
}

pub(crate) fn values_from_json(value: Value) -> StoreResult<InstanceValues> {
    serde_json::from_value::<BTreeMap<String, String>>(value).map_err(|error| {
        StoreError::internal(format!(
            "stored instance values were not a string map: {error}"
        ))
    })
}

pub(crate) fn rendered_objects_to_json(objects: &[RenderedObjectRef]) -> Value {
    Value::Array(
        objects
            .iter()
            .map(|object| {
                json!({
                    "api_version": object.api_version,
                    "kind": object.kind,
                    "namespace": object.namespace,
                    "name": object.name,
                })
            })
            .collect(),
    )
}

pub(crate) fn rendered_objects_from_json(value: Value) -> StoreResult<Vec<RenderedObjectRef>> {
    let Value::Array(values) = value else {
        return Err(StoreError::internal(
            "stored rendered objects were not an array",
        ));
    };

    values
        .into_iter()
        .map(|value| {
            let Value::Object(mut object) = value else {
                return Err(StoreError::internal(
                    "stored rendered object entry was not an object",
                ));
            };

            Ok(RenderedObjectRef {
                api_version: take_json_string(&mut object, "api_version")?,
                kind: take_json_string(&mut object, "kind")?,
                namespace: take_json_string(&mut object, "namespace")?,
                name: take_json_string(&mut object, "name")?,
            })
        })
        .collect()
}

pub(crate) fn route_identity_parts(identity: &RouteIdentity) -> RouteIdentityParts {
    match identity {
        RouteIdentity::Http { host, path } => RouteIdentityParts {
            key: route_identity_key(identity),
            identity_kind: "http",
            host_kind: route_host_kind_to_db(host.kind()),
            host: host.as_str().to_owned(),
            path_prefix: path.as_ref().map(|path| path.as_str().to_owned()),
        },
        RouteIdentity::Sni { host } => RouteIdentityParts {
            key: route_identity_key(identity),
            identity_kind: "sni",
            host_kind: route_host_kind_to_db(host.kind()),
            host: host.as_str().to_owned(),
            path_prefix: None,
        },
    }
}

pub(crate) fn route_identity_key(identity: &RouteIdentity) -> String {
    match identity {
        RouteIdentity::Http { host, path } => format!(
            "http:{}:{}:{}",
            route_host_kind_to_db(host.kind()),
            host.as_str(),
            path.as_ref().map(PathPrefix::as_str).unwrap_or("")
        ),
        RouteIdentity::Sni { host } => {
            format!(
                "sni:{}:{}",
                route_host_kind_to_db(host.kind()),
                host.as_str()
            )
        }
    }
}

pub(crate) fn validate_route_protocol(spec: &RouteBindingSpec) -> StoreResult<()> {
    match (&spec.identity, spec.protocol) {
        (RouteIdentity::Http { .. }, ProtocolRoute::Http)
        | (RouteIdentity::Sni { .. }, ProtocolRoute::TlsSni) => Ok(()),
        (RouteIdentity::Http { .. }, ProtocolRoute::TlsSni) => Err(StoreError::invalid_argument(
            "HTTP route identities must use the HTTP protocol route",
        )),
        (RouteIdentity::Sni { .. }, ProtocolRoute::Http) => Err(StoreError::invalid_argument(
            "SNI route identities must use the TLS SNI protocol route",
        )),
    }
}

pub(crate) fn route_matches(binding: &RouteIdentity, lookup: &RouteIdentity) -> Option<RouteScore> {
    match (binding, lookup) {
        (
            RouteIdentity::Http {
                host: binding_host,
                path: binding_path,
            },
            RouteIdentity::Http {
                host: lookup_host,
                path: lookup_path,
            },
        ) => host_score(binding_host, lookup_host).and_then(|host| {
            path_score(binding_path.as_ref(), lookup_path.as_ref())
                .map(|path| RouteScore { host, path })
        }),
        (RouteIdentity::Sni { host: binding_host }, RouteIdentity::Sni { host: lookup_host }) => {
            host_score(binding_host, lookup_host).map(|host| RouteScore { host, path: 0 })
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RouteScore {
    host: usize,
    path: usize,
}

pub(crate) fn materialization_id(
    instance_id: &InstanceId,
    target: &MaterializationTarget,
) -> StoreResult<MaterializationId> {
    MaterializationId::new(format!(
        "{}:{}:{}",
        instance_id.as_str(),
        target.cluster_id(),
        target.namespace()
    ))
    .map_err(invalid_stored_data)
}

pub(crate) fn instance_state_to_db(state: InstanceState) -> &'static str {
    match state {
        InstanceState::Cold => "cold",
        InstanceState::Waking => "waking",
        InstanceState::Running => "running",
        InstanceState::Draining => "draining",
        InstanceState::Failed => "failed",
        InstanceState::Deleting => "deleting",
        InstanceState::Deleted => "deleted",
    }
}

pub(crate) fn materialization_state_to_db(state: MaterializationState) -> &'static str {
    match state {
        MaterializationState::Pending => "pending",
        MaterializationState::Ready => "ready",
        MaterializationState::Failed => "failed",
        MaterializationState::Deleting => "deleting",
        MaterializationState::Deleted => "deleted",
    }
}

pub(crate) fn protocol_to_db(protocol: ProtocolRoute) -> &'static str {
    match protocol {
        ProtocolRoute::Http => "http",
        ProtocolRoute::TlsSni => "tls_sni",
    }
}

pub(crate) fn generation_to_i64(value: Generation) -> StoreResult<i64> {
    i64::try_from(value.get())
        .map_err(|_| StoreError::invalid_argument("generation does not fit in Postgres bigint"))
}

pub(crate) fn backend_generation_to_i64(value: BackendGeneration) -> StoreResult<i64> {
    i64::try_from(value.get()).map_err(|_| {
        StoreError::invalid_argument("backend generation does not fit in Postgres bigint")
    })
}

pub(crate) fn unix_millis_from_system_time(value: SystemTime) -> StoreResult<i64> {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| StoreError::invalid_argument("system time does not fit in unix millis")),
        Err(error) => {
            let millis = i64::try_from(error.duration().as_millis()).map_err(|_| {
                StoreError::invalid_argument("system time does not fit in unix millis")
            })?;
            Ok(-millis)
        }
    }
}

pub(crate) fn default_negative_cache_policy() -> CachePolicy {
    CachePolicy::new(Duration::from_secs(5))
}

fn route_identity_from_parts(
    identity_kind: &str,
    host_kind: &str,
    host: &str,
    path_prefix: Option<String>,
) -> StoreResult<RouteIdentity> {
    let host = route_host_from_db(host_kind, host)?;
    match identity_kind {
        "http" => Ok(RouteIdentity::Http {
            host,
            path: path_prefix
                .map(PathPrefix::new)
                .transpose()
                .map_err(invalid_stored_data)?,
        }),
        "sni" => Ok(RouteIdentity::Sni { host }),
        other => Err(StoreError::internal(format!(
            "stored route identity kind {other:?} is invalid"
        ))),
    }
}

fn route_host_from_db(host_kind: &str, host: &str) -> StoreResult<RouteHost> {
    match host_kind {
        "exact" => RouteHost::exact(host).map_err(invalid_stored_data),
        "wildcard_suffix" => RouteHost::wildcard_suffix(host).map_err(invalid_stored_data),
        other => Err(StoreError::internal(format!(
            "stored route host kind {other:?} is invalid"
        ))),
    }
}

fn route_host_kind_to_db(kind: RouteHostKind) -> &'static str {
    match kind {
        RouteHostKind::Exact => "exact",
        RouteHostKind::WildcardSuffix => "wildcard_suffix",
    }
}

fn host_score(binding: &RouteHost, lookup: &RouteHost) -> Option<usize> {
    match binding.kind() {
        RouteHostKind::Exact if binding.as_str() == lookup.as_str() => Some(usize::MAX),
        RouteHostKind::Exact => None,
        RouteHostKind::WildcardSuffix => {
            let lookup_host = lookup.as_str();
            let suffix = binding.as_str();
            let suffix_start = lookup_host.len().checked_sub(suffix.len())?;
            let prefix = lookup_host.get(..suffix_start)?;
            if lookup_host.ends_with(suffix) && prefix.ends_with('.') {
                Some(suffix.len())
            } else {
                None
            }
        }
    }
}

fn path_score(binding: Option<&PathPrefix>, lookup: Option<&PathPrefix>) -> Option<usize> {
    match (binding, lookup) {
        (None, _) => Some(0),
        (Some(_), None) => None,
        (Some(binding), Some(lookup)) if lookup.as_str().starts_with(binding.as_str()) => {
            Some(binding.as_str().len())
        }
        (Some(_), Some(_)) => None,
    }
}

fn instance_state_from_db(value: &str) -> StoreResult<InstanceState> {
    match value {
        "cold" => Ok(InstanceState::Cold),
        "waking" => Ok(InstanceState::Waking),
        "running" => Ok(InstanceState::Running),
        "draining" => Ok(InstanceState::Draining),
        "failed" => Ok(InstanceState::Failed),
        "deleting" => Ok(InstanceState::Deleting),
        "deleted" => Ok(InstanceState::Deleted),
        other => Err(StoreError::internal(format!(
            "stored instance state {other:?} is invalid"
        ))),
    }
}

fn materialization_state_from_db(value: &str) -> StoreResult<MaterializationState> {
    match value {
        "pending" => Ok(MaterializationState::Pending),
        "ready" => Ok(MaterializationState::Ready),
        "failed" => Ok(MaterializationState::Failed),
        "deleting" => Ok(MaterializationState::Deleting),
        "deleted" => Ok(MaterializationState::Deleted),
        other => Err(StoreError::internal(format!(
            "stored materialization state {other:?} is invalid"
        ))),
    }
}

fn protocol_from_db(value: &str) -> StoreResult<ProtocolRoute> {
    match value {
        "http" => Ok(ProtocolRoute::Http),
        "tls_sni" => Ok(ProtocolRoute::TlsSni),
        other => Err(StoreError::internal(format!(
            "stored route protocol {other:?} is invalid"
        ))),
    }
}

fn generation_from_i64(value: i64) -> StoreResult<Generation> {
    u64::try_from(value)
        .map(Generation::new)
        .map_err(|_| StoreError::internal(format!("stored generation {value} is invalid")))
}

fn backend_generation_from_i64(value: i64) -> StoreResult<BackendGeneration> {
    u64::try_from(value)
        .map(BackendGeneration::new)
        .map_err(|_| StoreError::internal(format!("stored backend generation {value} is invalid")))
}

fn system_time_from_unix_millis(value: i64) -> StoreResult<SystemTime> {
    if value >= 0 {
        Ok(UNIX_EPOCH + Duration::from_millis(value as u64))
    } else {
        Ok(UNIX_EPOCH - Duration::from_millis(value.unsigned_abs()))
    }
}

fn take_json_string(
    object: &mut serde_json::Map<String, Value>,
    field: &'static str,
) -> StoreResult<String> {
    match object.remove(field) {
        Some(Value::String(value)) => Ok(value),
        _ => Err(StoreError::internal(format!(
            "stored rendered object field {field:?} was not a string"
        ))),
    }
}

fn invalid_stored_data(error: impl std::fmt::Display) -> StoreError {
    StoreError::internal(format!("stored Postgres data is invalid: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{route_identity_key, route_matches};
    use crate::route::{PathPrefix, ProtocolRoute, RouteBindingSpec, RouteHost, RouteIdentity};

    #[test]
    fn route_identity_key_uses_normalized_domain_parts() {
        let identity = RouteIdentity::Http {
            host: RouteHost::wildcard_suffix("*.Example.COM.").expect("valid host"),
            path: Some(PathPrefix::new("/api").expect("valid prefix")),
        };

        assert_eq!(
            route_identity_key(&identity),
            "http:wildcard_suffix:example.com:/api"
        );
    }

    #[test]
    fn route_match_prefers_specific_hosts_and_paths() {
        let exact = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api").expect("valid prefix")),
        };
        let wildcard = RouteIdentity::Http {
            host: RouteHost::wildcard_suffix("example.com").expect("valid host"),
            path: None,
        };
        let lookup = RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        };

        assert!(route_matches(&exact, &lookup) > route_matches(&wildcard, &lookup));
    }

    #[test]
    fn route_protocol_validation_rejects_mismatched_identity() {
        let spec = RouteBindingSpec::new(
            RouteIdentity::Sni {
                host: RouteHost::exact("db.example.com").expect("valid host"),
            },
            ProtocolRoute::Http,
        );

        assert!(super::validate_route_protocol(&spec).is_err());
    }
}
