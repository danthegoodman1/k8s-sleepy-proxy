use tonic::{transport::Server, Request, Response, Status};
use tower::layer::util::{Identity, Stack};

use crate::api::pb::{
    operator_control_plane_server::{OperatorControlPlane, OperatorControlPlaneServer},
    CreateInstanceRequest, CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
    DeleteHttp01ChallengeRequest, DeleteHttp01ChallengeResponse, DeleteInstanceRequest,
    DeleteInstanceResponse, DeleteRouteBindingRequest, DeleteRouteBindingResponse,
    ExpireHttp01ChallengesRequest, ExpireHttp01ChallengesResponse, GetInstanceRequest,
    GetRouteBindingRequest, GetWorkloadClassVersionRequest, Http01Challenge, Instance,
    PutHttp01ChallengeRequest, ResolveHttp01ChallengeRequest, ResolveHttp01ChallengeResponse,
    RouteBinding, WorkloadClassVersion,
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

impl OperatorApiPlaceholder {
    pub fn new() -> Self {
        Self
    }
}

pub type OperatorGrpcService = OperatorControlPlaneServer<OperatorApiPlaceholder>;
pub type OperatorGrpcWebServerBuilder = Server<Stack<tonic_web::GrpcWebLayer, Identity>>;

pub fn operator_grpc_service() -> OperatorGrpcService {
    OperatorControlPlaneServer::new(OperatorApiPlaceholder::new())
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
        _request: Request<CreateWorkloadClassVersionRequest>,
    ) -> Result<Response<WorkloadClassVersion>, Status> {
        Err(placeholder_status("CreateWorkloadClassVersion"))
    }

    async fn get_workload_class_version(
        &self,
        _request: Request<GetWorkloadClassVersionRequest>,
    ) -> Result<Response<WorkloadClassVersion>, Status> {
        Err(placeholder_status("GetWorkloadClassVersion"))
    }

    async fn create_instance(
        &self,
        _request: Request<CreateInstanceRequest>,
    ) -> Result<Response<Instance>, Status> {
        Err(placeholder_status("CreateInstance"))
    }

    async fn get_instance(
        &self,
        _request: Request<GetInstanceRequest>,
    ) -> Result<Response<Instance>, Status> {
        Err(placeholder_status("GetInstance"))
    }

    async fn delete_instance(
        &self,
        _request: Request<DeleteInstanceRequest>,
    ) -> Result<Response<DeleteInstanceResponse>, Status> {
        Err(placeholder_status("DeleteInstance"))
    }

    async fn create_route_binding(
        &self,
        _request: Request<CreateRouteBindingRequest>,
    ) -> Result<Response<RouteBinding>, Status> {
        Err(placeholder_status("CreateRouteBinding"))
    }

    async fn get_route_binding(
        &self,
        _request: Request<GetRouteBindingRequest>,
    ) -> Result<Response<RouteBinding>, Status> {
        Err(placeholder_status("GetRouteBinding"))
    }

    async fn delete_route_binding(
        &self,
        _request: Request<DeleteRouteBindingRequest>,
    ) -> Result<Response<DeleteRouteBindingResponse>, Status> {
        Err(placeholder_status("DeleteRouteBinding"))
    }

    async fn put_http01_challenge(
        &self,
        _request: Request<PutHttp01ChallengeRequest>,
    ) -> Result<Response<Http01Challenge>, Status> {
        Err(placeholder_status("PutHttp01Challenge"))
    }

    async fn resolve_http01_challenge(
        &self,
        _request: Request<ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<ResolveHttp01ChallengeResponse>, Status> {
        Err(placeholder_status("ResolveHttp01Challenge"))
    }

    async fn delete_http01_challenge(
        &self,
        _request: Request<DeleteHttp01ChallengeRequest>,
    ) -> Result<Response<DeleteHttp01ChallengeResponse>, Status> {
        Err(placeholder_status("DeleteHttp01Challenge"))
    }

    async fn expire_http01_challenges(
        &self,
        _request: Request<ExpireHttp01ChallengesRequest>,
    ) -> Result<Response<ExpireHttp01ChallengesResponse>, Status> {
        Err(placeholder_status("ExpireHttp01Challenges"))
    }
}
