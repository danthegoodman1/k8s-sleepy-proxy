use std::sync::Arc;

use tonic::{transport::Server, Request, Response, Status};
use tower::layer::util::{Identity, Stack};

use crate::{
    api::pb::{
        self,
        operator_control_plane_server::{OperatorControlPlane, OperatorControlPlaneServer},
    },
    ids::{IdempotencyKey, InstanceId, WorkloadClassId},
    instance::{self as domain_instance, InstanceState},
    store::{ControlPlaneStore, StoreError},
    workload::{self as domain_workload, WorkloadClassVersionRef},
};

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

fn workload_class_ref_from_proto(
    reference: pb::WorkloadClassVersionRef,
) -> Result<WorkloadClassVersionRef, Status> {
    Ok(WorkloadClassVersionRef::new(
        WorkloadClassId::new(reference.class_id).map_err(invalid_argument_status)?,
        crate::ids::Generation::new(reference.version),
    ))
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

fn workload_class_ref_to_proto(
    reference: &domain_workload::WorkloadClassVersionRef,
) -> pb::WorkloadClassVersionRef {
    pb::WorkloadClassVersionRef {
        class_id: reference.class_id.as_str().to_owned(),
        version: reference.version.get(),
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

fn parse_idempotency_key(value: String) -> Result<IdempotencyKey, Status> {
    IdempotencyKey::new(value).map_err(invalid_argument_status)
}

fn parse_instance_id(value: String) -> Result<InstanceId, Status> {
    InstanceId::new(value).map_err(invalid_argument_status)
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
