use serde_json::{json, Value};

use crate::{
    instance::CreateInstanceRequest,
    route::{ProtocolRoute, RouteBindingSpec, RouteHostKind, RouteIdentity},
    store::{StoreError, StoreResult},
};

use super::mapping;

pub(crate) const CREATE_INSTANCE_OPERATION: &str = "create_instance";

pub(crate) fn create_instance_fingerprint(request: &CreateInstanceRequest) -> StoreResult<Value> {
    let route_bindings = request
        .route_bindings
        .iter()
        .map(route_binding_fingerprint)
        .collect::<StoreResult<Vec<_>>>()?;

    Ok(json!({
        "operation": CREATE_INSTANCE_OPERATION,
        "instance_id": request.instance_id.as_str(),
        "workload_class": {
            "class_id": request.workload_class.class_id.as_str(),
            "version": request.workload_class.version.get(),
        },
        "values": request.values,
        "route_bindings": route_bindings,
    }))
}

fn route_binding_fingerprint(spec: &RouteBindingSpec) -> StoreResult<Value> {
    mapping::validate_route_protocol(spec)?;

    Ok(json!({
        "identity": route_identity_fingerprint(&spec.identity),
        "protocol": protocol_fingerprint(spec.protocol),
    }))
}

fn route_identity_fingerprint(identity: &RouteIdentity) -> Value {
    match identity {
        RouteIdentity::Http { host, path } => json!({
            "kind": "http",
            "host": {
                "kind": host_kind_fingerprint(host.kind()),
                "host": host.as_str(),
            },
            "path": path.as_ref().map(|path| path.as_str()),
        }),
        RouteIdentity::Sni { host } => json!({
            "kind": "sni",
            "host": {
                "kind": host_kind_fingerprint(host.kind()),
                "host": host.as_str(),
            },
        }),
    }
}

fn host_kind_fingerprint(kind: RouteHostKind) -> &'static str {
    match kind {
        RouteHostKind::Exact => "exact",
        RouteHostKind::WildcardSuffix => "wildcard_suffix",
    }
}

fn protocol_fingerprint(protocol: ProtocolRoute) -> &'static str {
    match protocol {
        ProtocolRoute::Http => "http",
        ProtocolRoute::TlsSni => "tls_sni",
    }
}

pub(crate) fn idempotency_conflict() -> StoreError {
    StoreError::IdempotencyConflict
}

#[cfg(test)]
mod tests {
    use super::create_instance_fingerprint;
    use crate::{
        ids::{Generation, IdempotencyKey, InstanceId, WorkloadClassId},
        instance::CreateInstanceRequest,
        route::{PathPrefix, ProtocolRoute, RouteBindingSpec, RouteHost, RouteIdentity},
        workload::WorkloadClassVersionRef,
    };

    #[test]
    fn create_instance_fingerprint_is_canonical_for_normalized_routes() {
        let request = CreateInstanceRequest::new(
            IdempotencyKey::new("key-1").expect("valid key"),
            InstanceId::new("instance-1").expect("valid instance ID"),
            WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-1").expect("valid class ID"),
                Generation::new(1),
            ),
        )
        .with_route_bindings(vec![RouteBindingSpec::new(
            RouteIdentity::Http {
                host: RouteHost::exact("App.Example.COM.").expect("valid host"),
                path: Some(PathPrefix::new("/api").expect("valid prefix")),
            },
            ProtocolRoute::Http,
        )]);

        let fingerprint = create_instance_fingerprint(&request).expect("fingerprint builds");

        assert_eq!(
            fingerprint["route_bindings"][0]["identity"]["host"]["host"],
            "app.example.com"
        );
    }
}
