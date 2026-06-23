use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{
    api::pb::{self, sidecar_control_plane_server::SidecarControlPlaneServer},
    idle::{self, ReportIdleError, ReportIdleResult, ReportIdleUnavailableReason},
    ids::{Generation, InstanceId},
    materialization::MaterializationTarget,
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    store::{ControlPlaneStore, StoreError},
};

pub const SIDECAR_SERVICE_NAME: &str = "sleepypods.controlplane.v1.SidecarControlPlane";

#[derive(Clone)]
pub struct StoreBackedSidecarApi<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
}

impl<C> StoreBackedSidecarApi<C> {
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            store,
            materializer,
            target,
        }
    }
}

pub type StoreBackedSidecarGrpcService<C> = SidecarControlPlaneServer<StoreBackedSidecarApi<C>>;

pub fn sidecar_grpc_service_with_store<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> StoreBackedSidecarGrpcService<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    SidecarControlPlaneServer::new(StoreBackedSidecarApi::new(store, materializer, target))
}

#[tonic::async_trait]
impl<C> pb::sidecar_control_plane_server::SidecarControlPlane for StoreBackedSidecarApi<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    async fn report_idle(
        &self,
        request: Request<pb::SidecarReportIdleRequest>,
    ) -> Result<Response<pb::SidecarReportIdleResponse>, Status> {
        let request = report_idle_request_from_proto(request.into_inner())?;
        let request_instance_id = request.instance_id.as_str().to_owned();

        match idle::report_idle(
            self.store.as_ref(),
            &self.materializer,
            self.target.clone(),
            request,
        )
        .await
        {
            Ok(result) => Ok(Response::new(report_idle_result_to_proto(result))),
            Err(error) => report_idle_error_response(request_instance_id, error).map(Response::new),
        }
    }
}

fn report_idle_request_from_proto(
    request: pb::SidecarReportIdleRequest,
) -> Result<idle::ReportIdleRequest, Status> {
    Ok(idle::ReportIdleRequest::new(
        InstanceId::new(request.instance_id).map_err(invalid_argument_status)?,
        Generation::new(request.expected_generation),
        request.active_count,
    ))
}

fn report_idle_result_to_proto(result: ReportIdleResult) -> pb::SidecarReportIdleResponse {
    let outcome = match result {
        ReportIdleResult::Accepted { instance } => {
            pb::sidecar_report_idle_response::Outcome::Accepted(pb::SidecarReportIdleAccepted {
                instance_id: instance.id.as_str().to_owned(),
                instance_generation: instance.generation.get(),
            })
        }
        ReportIdleResult::AlreadyDraining { instance } => {
            pb::sidecar_report_idle_response::Outcome::AlreadyDraining(
                pb::SidecarReportIdleAlreadyDraining {
                    instance_id: instance.id.as_str().to_owned(),
                    instance_generation: instance.generation.get(),
                },
            )
        }
        ReportIdleResult::Unavailable { instance, reason } => {
            pb::sidecar_report_idle_response::Outcome::Unavailable(
                pb::SidecarReportIdleUnavailable {
                    instance_id: instance.id.as_str().to_owned(),
                    instance_generation: instance.generation.get(),
                    reason: report_idle_unavailable_reason_to_proto(reason) as i32,
                },
            )
        }
    };

    pb::SidecarReportIdleResponse {
        outcome: Some(outcome),
    }
}

fn report_idle_error_response(
    request_instance_id: String,
    error: ReportIdleError,
) -> Result<pb::SidecarReportIdleResponse, Status> {
    match error {
        ReportIdleError::ActiveRequestsPresent { active_count } => {
            Err(Status::failed_precondition(format!(
                "idle report requires active_count 0, found {active_count}"
            )))
        }
        ReportIdleError::NotFound => Err(Status::not_found("instance not found")),
        ReportIdleError::GenerationConflict { expected, actual } => {
            Ok(pb::SidecarReportIdleResponse {
                outcome: Some(
                    pb::sidecar_report_idle_response::Outcome::GenerationConflict(
                        pb::SidecarReportIdleGenerationConflict {
                            instance_id: request_instance_id,
                            expected_generation: expected.get(),
                            actual_generation: actual.get(),
                        },
                    ),
                ),
            })
        }
        ReportIdleError::Materializer { instance, source } => Err(Status::unavailable(format!(
            "sleep cleanup failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        ReportIdleError::Store(error) => Err(store_error_to_status(error)),
    }
}

fn report_idle_unavailable_reason_to_proto(
    reason: ReportIdleUnavailableReason,
) -> pb::SidecarReportIdleUnavailableReason {
    match reason {
        ReportIdleUnavailableReason::Cold => pb::SidecarReportIdleUnavailableReason::Cold,
        ReportIdleUnavailableReason::Waking => pb::SidecarReportIdleUnavailableReason::Waking,
        ReportIdleUnavailableReason::Draining => pb::SidecarReportIdleUnavailableReason::Draining,
        ReportIdleUnavailableReason::Failed => pb::SidecarReportIdleUnavailableReason::Failed,
        ReportIdleUnavailableReason::Deleting => pb::SidecarReportIdleUnavailableReason::Deleting,
        ReportIdleUnavailableReason::Deleted => pb::SidecarReportIdleUnavailableReason::Deleted,
    }
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
