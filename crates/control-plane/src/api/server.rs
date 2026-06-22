use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tonic::{transport::Server, Request, Response, Status};
use tower::layer::util::{Identity, Stack};

use crate::{
    api::pb::{
        self,
        operator_control_plane_server::{OperatorControlPlane, OperatorControlPlaneServer},
    },
    http01 as domain_http01,
    ids::{IdempotencyKey, InstanceId, WorkloadClassId},
    instance::{self as domain_instance, InstanceState},
    route as domain_route,
    store::{ControlPlaneStore, StoreError},
    workload::{self as domain_workload, WorkloadClassVersion, WorkloadClassVersionRef},
};

use super::template::{manifest_template_from_proto, manifest_template_to_proto};

pub const OPERATOR_SERVICE_NAME: &str = "sleepypods.controlplane.v1.OperatorControlPlane";

pub const OPERATOR_UNARY_METHODS: &[&str] = &[
    "CreateWorkloadClassVersion",
    "GetWorkloadClassVersion",
    "CreateInstance",
    "GetInstance",
    "DeleteInstance",
    "CreateRouteBinding",
    "GetRouteBinding",
    "DeleteRouteBinding",
    "PutHttp01Challenge",
    "ResolveHttp01Challenge",
    "DeleteHttp01Challenge",
    "ExpireHttp01Challenges",
];

#[derive(Clone, Debug, Default)]
pub struct OperatorApiPlaceholder;

#[derive(Clone)]
pub struct StoreBackedOperatorApi {
    store: Arc<dyn ControlPlaneStore>,
}

impl OperatorApiPlaceholder {
    pub fn new() -> Self {
        Self
    }
}

impl StoreBackedOperatorApi {
    pub fn new(store: Arc<dyn ControlPlaneStore>) -> Self {
        Self { store }
    }
}

pub type OperatorGrpcService = OperatorControlPlaneServer<OperatorApiPlaceholder>;
pub type StoreBackedOperatorGrpcService = OperatorControlPlaneServer<StoreBackedOperatorApi>;
pub type OperatorGrpcWebServerBuilder = Server<Stack<tonic_web::GrpcWebLayer, Identity>>;

pub fn operator_grpc_service() -> OperatorGrpcService {
    OperatorControlPlaneServer::new(OperatorApiPlaceholder::new())
}

pub fn operator_grpc_service_with_store(
    store: Arc<dyn ControlPlaneStore>,
) -> StoreBackedOperatorGrpcService {
    OperatorControlPlaneServer::new(StoreBackedOperatorApi::new(store))
}

pub fn operator_grpc_server_builder() -> Server {
    Server::builder()
}

pub fn operator_grpc_web_server_builder() -> OperatorGrpcWebServerBuilder {
    Server::builder()
        .accept_http1(true)
        .layer(tonic_web::GrpcWebLayer::new())
}

fn placeholder_status(method: &'static str) -> Status {
    Status::unimplemented(format!(
        "{method} transport is scaffolded; store-backed behavior is deferred"
    ))
}

#[tonic::async_trait]
impl OperatorControlPlane for OperatorApiPlaceholder {
    async fn create_workload_class_version(
        &self,
        _request: Request<pb::CreateWorkloadClassVersionRequest>,
    ) -> Result<Response<pb::WorkloadClassVersion>, Status> {
        Err(placeholder_status("CreateWorkloadClassVersion"))
    }

    async fn get_workload_class_version(
        &self,
        _request: Request<pb::GetWorkloadClassVersionRequest>,
    ) -> Result<Response<pb::WorkloadClassVersion>, Status> {
        Err(placeholder_status("GetWorkloadClassVersion"))
    }

    async fn create_instance(
        &self,
        _request: Request<pb::CreateInstanceRequest>,
    ) -> Result<Response<pb::Instance>, Status> {
        Err(placeholder_status("CreateInstance"))
    }

    async fn get_instance(
        &self,
        _request: Request<pb::GetInstanceRequest>,
    ) -> Result<Response<pb::Instance>, Status> {
        Err(placeholder_status("GetInstance"))
    }

    async fn delete_instance(
        &self,
        _request: Request<pb::DeleteInstanceRequest>,
    ) -> Result<Response<pb::DeleteInstanceResponse>, Status> {
        Err(placeholder_status("DeleteInstance"))
    }

    async fn create_route_binding(
        &self,
        _request: Request<pb::CreateRouteBindingRequest>,
    ) -> Result<Response<pb::RouteBinding>, Status> {
        Err(placeholder_status("CreateRouteBinding"))
    }

    async fn get_route_binding(
        &self,
        _request: Request<pb::GetRouteBindingRequest>,
    ) -> Result<Response<pb::RouteBinding>, Status> {
        Err(placeholder_status("GetRouteBinding"))
    }

    async fn delete_route_binding(
        &self,
        _request: Request<pb::DeleteRouteBindingRequest>,
    ) -> Result<Response<pb::DeleteRouteBindingResponse>, Status> {
        Err(placeholder_status("DeleteRouteBinding"))
    }

    async fn put_http01_challenge(
        &self,
        _request: Request<pb::PutHttp01ChallengeRequest>,
    ) -> Result<Response<pb::Http01Challenge>, Status> {
        Err(placeholder_status("PutHttp01Challenge"))
    }

    async fn resolve_http01_challenge(
        &self,
        _request: Request<pb::ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<pb::ResolveHttp01ChallengeResponse>, Status> {
        Err(placeholder_status("ResolveHttp01Challenge"))
    }

    async fn delete_http01_challenge(
        &self,
        _request: Request<pb::DeleteHttp01ChallengeRequest>,
    ) -> Result<Response<pb::DeleteHttp01ChallengeResponse>, Status> {
        Err(placeholder_status("DeleteHttp01Challenge"))
    }

    async fn expire_http01_challenges(
        &self,
        _request: Request<pb::ExpireHttp01ChallengesRequest>,
    ) -> Result<Response<pb::ExpireHttp01ChallengesResponse>, Status> {
        Err(placeholder_status("ExpireHttp01Challenges"))
    }
}

#[tonic::async_trait]
impl OperatorControlPlane for StoreBackedOperatorApi {
    async fn create_workload_class_version(
        &self,
        request: Request<pb::CreateWorkloadClassVersionRequest>,
    ) -> Result<Response<pb::WorkloadClassVersion>, Status> {
        let workload_class = self
            .store
            .create_workload_class_version(create_workload_class_request_from_proto(
                request.into_inner(),
            )?)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(workload_class_to_proto(workload_class)))
    }

    async fn get_workload_class_version(
        &self,
        request: Request<pb::GetWorkloadClassVersionRequest>,
    ) -> Result<Response<pb::WorkloadClassVersion>, Status> {
        let reference = request
            .into_inner()
            .reference
            .ok_or_else(|| Status::invalid_argument("reference is required"))
            .and_then(workload_class_ref_from_proto)?;
        let workload_class = self
            .store
            .load_workload_class_version(domain_workload::LoadWorkloadClassVersionRequest::new(
                reference,
            ))
            .await
            .map_err(store_error_to_status)?
            .ok_or_else(|| Status::not_found("workload class version not found"))?;

        Ok(Response::new(workload_class_to_proto(workload_class)))
    }

    async fn create_instance(
        &self,
        request: Request<pb::CreateInstanceRequest>,
    ) -> Result<Response<pb::Instance>, Status> {
        let result = self
            .store
            .create_instance(create_instance_request_from_proto(request.into_inner())?)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(instance_to_proto(result.instance)))
    }

    async fn get_instance(
        &self,
        request: Request<pb::GetInstanceRequest>,
    ) -> Result<Response<pb::Instance>, Status> {
        let request = domain_instance::GetInstanceRequest::new(parse_instance_id(
            request.into_inner().instance_id,
        )?);
        let instance = self
            .store
            .get_instance(request)
            .await
            .map_err(store_error_to_status)?
            .ok_or_else(|| Status::not_found("instance not found"))?;

        Ok(Response::new(instance_to_proto(instance)))
    }

    async fn delete_instance(
        &self,
        request: Request<pb::DeleteInstanceRequest>,
    ) -> Result<Response<pb::DeleteInstanceResponse>, Status> {
        let request = domain_instance::DeleteInstanceRequest::new(parse_instance_id(
            request.into_inner().instance_id,
        )?);
        let deleted = self
            .store
            .delete_instance(request)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(pb::DeleteInstanceResponse { deleted }))
    }

    async fn create_route_binding(
        &self,
        request: Request<pb::CreateRouteBindingRequest>,
    ) -> Result<Response<pb::RouteBinding>, Status> {
        let route_binding = self
            .store
            .create_route_binding(create_route_binding_request_from_proto(
                request.into_inner(),
            )?)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(route_binding_to_proto(route_binding)))
    }

    async fn get_route_binding(
        &self,
        request: Request<pb::GetRouteBindingRequest>,
    ) -> Result<Response<pb::RouteBinding>, Status> {
        let request = domain_route::GetRouteBindingRequest::new(parse_route_binding_id(
            request.into_inner().route_binding_id,
        )?);
        let route_binding = self
            .store
            .get_route_binding(request)
            .await
            .map_err(store_error_to_status)?
            .ok_or_else(|| Status::not_found("route binding not found"))?;

        Ok(Response::new(route_binding_to_proto(route_binding)))
    }

    async fn delete_route_binding(
        &self,
        request: Request<pb::DeleteRouteBindingRequest>,
    ) -> Result<Response<pb::DeleteRouteBindingResponse>, Status> {
        let request = domain_route::DeleteRouteBindingRequest::new(parse_route_binding_id(
            request.into_inner().route_binding_id,
        )?);
        let deleted = self
            .store
            .delete_route_binding(request)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(pb::DeleteRouteBindingResponse { deleted }))
    }

    async fn put_http01_challenge(
        &self,
        request: Request<pb::PutHttp01ChallengeRequest>,
    ) -> Result<Response<pb::Http01Challenge>, Status> {
        let challenge = self
            .store
            .put_http01_challenge(put_http01_request_from_proto(request.into_inner())?)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(http01_to_proto(challenge)?))
    }

    async fn resolve_http01_challenge(
        &self,
        request: Request<pb::ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<pb::ResolveHttp01ChallengeResponse>, Status> {
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("key is required"))
            .and_then(http01_key_from_proto)?;
        let challenge = self
            .store
            .resolve_http01_challenge(key)
            .await
            .map_err(store_error_to_status)?
            .map(http01_to_proto)
            .transpose()?;

        Ok(Response::new(pb::ResolveHttp01ChallengeResponse {
            challenge,
        }))
    }

    async fn delete_http01_challenge(
        &self,
        request: Request<pb::DeleteHttp01ChallengeRequest>,
    ) -> Result<Response<pb::DeleteHttp01ChallengeResponse>, Status> {
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("key is required"))
            .and_then(http01_key_from_proto)?;
        let deleted = self
            .store
            .delete_http01_challenge(domain_http01::DeleteHttp01ChallengeRequest::new(key))
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(pb::DeleteHttp01ChallengeResponse { deleted }))
    }

    async fn expire_http01_challenges(
        &self,
        request: Request<pb::ExpireHttp01ChallengesRequest>,
    ) -> Result<Response<pb::ExpireHttp01ChallengesResponse>, Status> {
        let request = expire_http01_request_from_proto(request.into_inner())?;
        let expired = self
            .store
            .expire_http01_challenges(request)
            .await
            .map_err(store_error_to_status)?;

        Ok(Response::new(pb::ExpireHttp01ChallengesResponse {
            expired: expired as u64,
        }))
    }
}

fn create_instance_request_from_proto(
    request: pb::CreateInstanceRequest,
) -> Result<domain_instance::CreateInstanceRequest, Status> {
    let workload_class = request
        .workload_class
        .ok_or_else(|| Status::invalid_argument("workload_class is required"))
        .and_then(workload_class_ref_from_proto)?;

    Ok(domain_instance::CreateInstanceRequest::new(
        parse_idempotency_key(request.idempotency_key)?,
        parse_instance_id(request.instance_id)?,
        workload_class,
    )
    .with_values(request.values.into_iter().collect()))
}

fn create_workload_class_request_from_proto(
    request: pb::CreateWorkloadClassVersionRequest,
) -> Result<domain_workload::CreateWorkloadClassVersionRequest, Status> {
    let _ = parse_idempotency_key(request.idempotency_key)?;
    let workload_class = WorkloadClassVersion {
        reference: WorkloadClassVersionRef::new(
            WorkloadClassId::new(request.class_id).map_err(invalid_argument_status)?,
            crate::ids::Generation::new(request.version),
        ),
        template_generation: crate::ids::Generation::new(request.template_generation),
        template: request
            .template
            .ok_or_else(|| Status::invalid_argument("template is required"))
            .and_then(manifest_template_from_proto)?,
        default_values: request.default_values.into_iter().collect(),
        value_schema: request
            .value_schema
            .map(workload_value_schema_from_proto)
            .transpose()?
            .unwrap_or_default(),
    };

    Ok(domain_workload::CreateWorkloadClassVersionRequest::new(
        workload_class,
    ))
}

fn create_route_binding_request_from_proto(
    request: pb::CreateRouteBindingRequest,
) -> Result<domain_route::CreateRouteBindingRequest, Status> {
    let identity = request
        .identity
        .ok_or_else(|| Status::invalid_argument("identity is required"))
        .and_then(route_identity_from_proto)?;
    let protocol = protocol_from_proto(request.protocol)?;

    Ok(domain_route::CreateRouteBindingRequest::new(
        parse_idempotency_key(request.idempotency_key)?,
        parse_route_binding_id(request.route_binding_id)?,
        parse_instance_id(request.instance_id)?,
        identity,
        protocol,
    ))
}

fn workload_class_ref_from_proto(
    reference: pb::WorkloadClassVersionRef,
) -> Result<WorkloadClassVersionRef, Status> {
    Ok(WorkloadClassVersionRef::new(
        WorkloadClassId::new(reference.class_id).map_err(invalid_argument_status)?,
        crate::ids::Generation::new(reference.version),
    ))
}

fn workload_value_schema_from_proto(
    schema: pb::WorkloadValueSchema,
) -> Result<domain_workload::WorkloadValueSchema, Status> {
    Ok(domain_workload::WorkloadValueSchema {
        fields: schema
            .fields
            .into_iter()
            .map(|(field, rule)| {
                (
                    field,
                    domain_workload::WorkloadValueFieldRule {
                        required: rule.required,
                        default: rule.default_value,
                    },
                )
            })
            .collect(),
        allow_extra: schema.allow_extra,
    })
}

fn route_identity_from_proto(
    identity: pb::RouteIdentity,
) -> Result<domain_route::RouteIdentity, Status> {
    match identity
        .kind
        .ok_or_else(|| Status::invalid_argument("route identity kind is required"))?
    {
        pb::route_identity::Kind::Http(http) => Ok(domain_route::RouteIdentity::Http {
            host: http
                .host
                .ok_or_else(|| Status::invalid_argument("HTTP route host is required"))
                .and_then(route_host_from_proto)?,
            path: http
                .path_prefix
                .map(domain_route::PathPrefix::new)
                .transpose()
                .map_err(invalid_argument_status)?,
        }),
        pb::route_identity::Kind::Sni(sni) => Ok(domain_route::RouteIdentity::Sni {
            host: sni
                .host
                .ok_or_else(|| Status::invalid_argument("SNI route host is required"))
                .and_then(route_host_from_proto)?,
        }),
    }
}

fn route_host_from_proto(host: pb::RouteHost) -> Result<domain_route::RouteHost, Status> {
    match pb::RouteHostKind::try_from(host.kind)
        .map_err(|_| Status::invalid_argument("route host kind is invalid"))?
    {
        pb::RouteHostKind::Exact => {
            domain_route::RouteHost::exact(host.host).map_err(invalid_argument_status)
        }
        pb::RouteHostKind::WildcardSuffix => {
            domain_route::RouteHost::wildcard_suffix(host.host).map_err(invalid_argument_status)
        }
        pb::RouteHostKind::Unspecified => {
            Err(Status::invalid_argument("route host kind is required"))
        }
    }
}

fn protocol_from_proto(protocol: i32) -> Result<domain_route::ProtocolRoute, Status> {
    match pb::ProtocolRoute::try_from(protocol)
        .map_err(|_| Status::invalid_argument("protocol route is invalid"))?
    {
        pb::ProtocolRoute::Http => Ok(domain_route::ProtocolRoute::Http),
        pb::ProtocolRoute::TlsSni => Ok(domain_route::ProtocolRoute::TlsSni),
        pb::ProtocolRoute::Unspecified => {
            Err(Status::invalid_argument("protocol route is required"))
        }
    }
}

fn put_http01_request_from_proto(
    request: pb::PutHttp01ChallengeRequest,
) -> Result<domain_http01::PutHttp01ChallengeRequest, Status> {
    let key = request
        .key
        .ok_or_else(|| Status::invalid_argument("key is required"))
        .and_then(http01_key_from_proto)?;
    domain_http01::PutHttp01ChallengeRequest::new(
        key,
        request.key_authorization,
        system_time_from_unix_millis(request.expires_at_unix_millis),
        SystemTime::now(),
    )
    .map_err(invalid_argument_status)
}

fn http01_key_from_proto(
    key: pb::Http01ChallengeKey,
) -> Result<domain_http01::Http01ChallengeKey, Status> {
    domain_http01::Http01ChallengeKey::new(key.host, key.token).map_err(invalid_argument_status)
}

fn expire_http01_request_from_proto(
    request: pb::ExpireHttp01ChallengesRequest,
) -> Result<domain_http01::ExpireHttp01ChallengesRequest, Status> {
    let mut domain_request = domain_http01::ExpireHttp01ChallengesRequest::new(
        system_time_from_unix_millis(request.now_unix_millis),
    );
    if let Some(limit) = request.limit {
        let limit = usize::try_from(limit)
            .map_err(|_| Status::invalid_argument("HTTP-01 expire limit is too large"))?;
        domain_request = domain_request.with_limit(limit);
    }

    Ok(domain_request)
}

fn instance_to_proto(instance: domain_instance::InstanceRecord) -> pb::Instance {
    pb::Instance {
        instance_id: instance.id.as_str().to_owned(),
        workload_class: Some(workload_class_ref_to_proto(&instance.workload_class)),
        values: instance.values.into_iter().collect(),
        state: instance_state_to_proto(instance.state) as i32,
        generation: instance.generation.get(),
    }
}

fn workload_class_to_proto(workload_class: WorkloadClassVersion) -> pb::WorkloadClassVersion {
    pb::WorkloadClassVersion {
        reference: Some(workload_class_ref_to_proto(&workload_class.reference)),
        template_generation: workload_class.template_generation.get(),
        default_values: workload_class.default_values.into_iter().collect(),
        value_schema: Some(workload_value_schema_to_proto(workload_class.value_schema)),
        template: Some(manifest_template_to_proto(workload_class.template)),
    }
}

fn workload_value_schema_to_proto(
    schema: domain_workload::WorkloadValueSchema,
) -> pb::WorkloadValueSchema {
    pb::WorkloadValueSchema {
        fields: schema
            .fields
            .into_iter()
            .map(|(field, rule)| {
                (
                    field,
                    pb::WorkloadValueFieldRule {
                        required: rule.required,
                        default_value: rule.default,
                    },
                )
            })
            .collect(),
        allow_extra: schema.allow_extra,
    }
}

fn workload_class_ref_to_proto(
    reference: &domain_workload::WorkloadClassVersionRef,
) -> pb::WorkloadClassVersionRef {
    pb::WorkloadClassVersionRef {
        class_id: reference.class_id.as_str().to_owned(),
        version: reference.version.get(),
    }
}

fn route_binding_to_proto(route_binding: domain_route::RouteBindingRecord) -> pb::RouteBinding {
    pb::RouteBinding {
        route_binding_id: route_binding.id.as_str().to_owned(),
        instance_id: route_binding.instance_id.as_str().to_owned(),
        identity: Some(route_identity_to_proto(route_binding.identity)),
        protocol: protocol_to_proto(route_binding.protocol) as i32,
    }
}

fn route_identity_to_proto(identity: domain_route::RouteIdentity) -> pb::RouteIdentity {
    let kind = match identity {
        domain_route::RouteIdentity::Http { host, path } => {
            pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
                host: Some(route_host_to_proto(host)),
                path_prefix: path.map(|path| path.as_str().to_owned()),
            })
        }
        domain_route::RouteIdentity::Sni { host } => {
            pb::route_identity::Kind::Sni(pb::SniRouteIdentity {
                host: Some(route_host_to_proto(host)),
            })
        }
    };

    pb::RouteIdentity { kind: Some(kind) }
}

fn route_host_to_proto(host: domain_route::RouteHost) -> pb::RouteHost {
    pb::RouteHost {
        kind: route_host_kind_to_proto(host.kind()) as i32,
        host: host.as_str().to_owned(),
    }
}

fn route_host_kind_to_proto(kind: domain_route::RouteHostKind) -> pb::RouteHostKind {
    match kind {
        domain_route::RouteHostKind::Exact => pb::RouteHostKind::Exact,
        domain_route::RouteHostKind::WildcardSuffix => pb::RouteHostKind::WildcardSuffix,
    }
}

fn protocol_to_proto(protocol: domain_route::ProtocolRoute) -> pb::ProtocolRoute {
    match protocol {
        domain_route::ProtocolRoute::Http => pb::ProtocolRoute::Http,
        domain_route::ProtocolRoute::TlsSni => pb::ProtocolRoute::TlsSni,
    }
}

fn instance_state_to_proto(state: InstanceState) -> pb::InstanceState {
    match state {
        InstanceState::Cold => pb::InstanceState::Cold,
        InstanceState::Waking => pb::InstanceState::Waking,
        InstanceState::Running => pb::InstanceState::Running,
        InstanceState::Draining => pb::InstanceState::Draining,
        InstanceState::Failed => pb::InstanceState::Failed,
        InstanceState::Deleting => pb::InstanceState::Deleting,
        InstanceState::Deleted => pb::InstanceState::Deleted,
    }
}

fn http01_to_proto(
    challenge: domain_http01::Http01ChallengeRecord,
) -> Result<pb::Http01Challenge, Status> {
    Ok(pb::Http01Challenge {
        key: Some(pb::Http01ChallengeKey {
            host: challenge.key().host().as_str().to_owned(),
            token: challenge.key().token().to_owned(),
        }),
        key_authorization: challenge.key_authorization().to_owned(),
        expires_at_unix_millis: unix_millis_from_system_time(challenge.expires_at())?,
    })
}

fn unix_millis_from_system_time(value: SystemTime) -> Result<i64, Status> {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| Status::invalid_argument("system time does not fit in unix millis")),
        Err(error) => {
            let millis = i64::try_from(error.duration().as_millis())
                .map_err(|_| Status::invalid_argument("system time does not fit in unix millis"))?;
            Ok(-millis)
        }
    }
}

fn system_time_from_unix_millis(value: i64) -> SystemTime {
    if value >= 0 {
        UNIX_EPOCH + Duration::from_millis(value as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(value.unsigned_abs())
    }
}

fn parse_idempotency_key(value: String) -> Result<IdempotencyKey, Status> {
    IdempotencyKey::new(value).map_err(invalid_argument_status)
}

fn parse_instance_id(value: String) -> Result<InstanceId, Status> {
    InstanceId::new(value).map_err(invalid_argument_status)
}

fn parse_route_binding_id(value: String) -> Result<crate::ids::RouteBindingId, Status> {
    crate::ids::RouteBindingId::new(value).map_err(invalid_argument_status)
}

fn invalid_argument_status(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}

fn store_error_to_status(error: StoreError) -> Status {
    match error {
        StoreError::InvalidArgument { message } => Status::invalid_argument(message),
        StoreError::NotFound { resource } => Status::not_found(format!("{resource} not found")),
        StoreError::AlreadyExists { resource } => {
            Status::already_exists(format!("{resource} already exists"))
        }
        StoreError::GenerationConflict { expected, actual } => Status::failed_precondition(
            format!("generation conflict: expected generation {expected}, found {actual}"),
        ),
        StoreError::IdempotencyConflict => {
            Status::already_exists("idempotency key was already used for a different request")
        }
        StoreError::Unavailable { message } => Status::unavailable(message),
        StoreError::Internal { message } => Status::internal(message),
    }
}
