use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{
    api::{
        pb::{self, sidecar_control_plane_server::SidecarControlPlaneServer},
        route_events::RouteSubscriptionBroker,
    },
    idle::{self, ReportIdleError, ReportIdleResult, ReportIdleUnavailableReason},
    ids::{Generation, InstanceId},
    materialization::MaterializationTarget,
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    route::ListRouteBindingsForInstanceRequest,
    store::{ControlPlaneStore, StoreError},
};

pub const SIDECAR_SERVICE_NAME: &str = "sleepypods.controlplane.v1.SidecarControlPlane";

#[derive(Clone)]
pub struct StoreBackedSidecarApi<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    route_events: RouteSubscriptionBroker,
}

impl<C> StoreBackedSidecarApi<C> {
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
    ) -> Self {
        Self::with_route_events(store, materializer, target, RouteSubscriptionBroker::new())
    }

    pub fn with_route_events(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        target: MaterializationTarget,
        route_events: RouteSubscriptionBroker,
    ) -> Self {
        Self {
            store,
            materializer,
            target,
            route_events,
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

pub fn sidecar_grpc_service_with_store_and_route_events<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
    route_events: RouteSubscriptionBroker,
) -> StoreBackedSidecarGrpcService<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    SidecarControlPlaneServer::new(StoreBackedSidecarApi::with_route_events(
        store,
        materializer,
        target,
        route_events,
    ))
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
            Ok(result) => {
                if let ReportIdleResult::Accepted { instance } = &result {
                    notify_instance_routes_changed(
                        self.store.as_ref(),
                        &self.route_events,
                        instance.id.clone(),
                    )
                    .await?;
                }
                Ok(Response::new(report_idle_result_to_proto(result)))
            }
            Err(error) => report_idle_error_response(request_instance_id, error).map(Response::new),
        }
    }
}

async fn notify_instance_routes_changed(
    store: &dyn ControlPlaneStore,
    route_events: &RouteSubscriptionBroker,
    instance_id: InstanceId,
) -> Result<(), Status> {
    let route_bindings = store
        .list_route_bindings_for_instance(ListRouteBindingsForInstanceRequest::new(instance_id))
        .await
        .map_err(store_error_to_status)?;
    route_events.notify_routes_changed(&route_bindings);
    Ok(())
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
        ReportIdleError::WorkloadClassNotFound => {
            Err(Status::not_found("workload class version not found"))
        }
        ReportIdleError::SleepPolicy(error) => Err(Status::failed_precondition(format!(
            "sleep policy invalid: {error}"
        ))),
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
        StoreError::ExclusivityConflict {
            cluster_id,
            namespace,
            key_name,
            owner_instance_id,
            owner_generation,
        } => {
            let mut message = format!(
                "exclusivity key {key_name:?} is already held for target {cluster_id}/{namespace}"
            );
            if let Some(owner_instance_id) = owner_instance_id {
                message.push_str(&format!(" by instance {owner_instance_id}"));
            }
            if let Some(owner_generation) = owner_generation {
                message.push_str(&format!(" generation {owner_generation}"));
            }
            Status::failed_precondition(message)
        }
        StoreError::IdempotencyConflict => {
            Status::already_exists("idempotency key was already used for a different request")
        }
        StoreError::Unavailable { message } => Status::unavailable(message),
        StoreError::Internal { message } => Status::internal(message),
    }
}
