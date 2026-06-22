use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{
    api::pb::{self, proxy_control_plane_server::ProxyControlPlaneServer},
    ids::{BackendGeneration, Generation, InstanceId},
    instance as domain_instance,
    materialization::{MaterializationRecord, MaterializationTarget},
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    store::{ControlPlaneStore, StoreError},
    wake::{self, WakeInstanceError, WakeInstanceResult, WakeUnavailableReason},
};

pub const PROXY_SERVICE_NAME: &str = "sleepypods.controlplane.v1.ProxyControlPlane";

#[derive(Clone)]
pub struct StoreBackedProxyApi<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
}

impl<C> StoreBackedProxyApi<C> {
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

pub type StoreBackedProxyGrpcService<C> = ProxyControlPlaneServer<StoreBackedProxyApi<C>>;

pub fn proxy_grpc_service_with_store<C>(
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    target: MaterializationTarget,
) -> StoreBackedProxyGrpcService<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    ProxyControlPlaneServer::new(StoreBackedProxyApi::new(store, materializer, target))
}

#[tonic::async_trait]
impl<C> pb::proxy_control_plane_server::ProxyControlPlane for StoreBackedProxyApi<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    async fn wake_instance(
        &self,
        request: Request<pb::ProxyWakeInstanceRequest>,
    ) -> Result<Response<pb::ProxyWakeInstanceResponse>, Status> {
        let request = proxy_wake_request_from_proto(request.into_inner(), self.target.clone())?;
        let instance_id = request.instance_id.as_str().to_owned();

        let result = wake::wake_instance(self.store.as_ref(), &self.materializer, request).await;
        let response = match result {
            Ok(WakeInstanceResult::Completed { result }) => {
                proxy_ready_response(&result.instance, &result.materialization)?
            }
            Ok(WakeInstanceResult::AlreadyRunning {
                instance,
                materialization,
            }) => proxy_ready_response(&instance, &materialization)?,
            Ok(WakeInstanceResult::AlreadyWaking { instance }) => pb::ProxyWakeInstanceResponse {
                outcome: Some(pb::proxy_wake_instance_response::Outcome::StillWaking(
                    pb::ProxyWakeStillWakingResult {
                        instance_id: instance.id.as_str().to_owned(),
                        instance_generation: instance.generation.get(),
                    },
                )),
            },
            Err(error) => proxy_wake_error_response(instance_id, error)?,
        };

        Ok(Response::new(response))
    }
}

fn proxy_wake_request_from_proto(
    request: pb::ProxyWakeInstanceRequest,
    target: MaterializationTarget,
) -> Result<wake::WakeInstanceRequest, Status> {
    let mut wake_request = wake::WakeInstanceRequest::new(
        InstanceId::new(request.instance_id).map_err(invalid_argument_status)?,
        Generation::new(request.expected_generation),
        target,
    );
    if let Some(backend_generation) = request.backend_generation {
        wake_request =
            wake_request.with_backend_generation(BackendGeneration::new(backend_generation));
    }

    Ok(wake_request)
}

fn proxy_ready_response(
    instance: &domain_instance::InstanceRecord,
    materialization: &MaterializationRecord,
) -> Result<pb::ProxyWakeInstanceResponse, Status> {
    match materialization.backend.as_ref() {
        Some(backend) => Ok(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
                pb::ProxyWakeReadyResult {
                    instance_id: instance.id.as_str().to_owned(),
                    instance_generation: instance.generation.get(),
                    backend_uri: backend.uri().to_owned(),
                    backend_generation: materialization.backend_generation.get(),
                },
            )),
        }),
        None => Err(Status::failed_precondition(format!(
            "ready materialization for instance {} has no backend endpoint",
            instance.id.as_str()
        ))),
    }
}

fn proxy_wake_error_response(
    request_instance_id: String,
    error: WakeInstanceError,
) -> Result<pb::ProxyWakeInstanceResponse, Status> {
    match error {
        WakeInstanceError::NotFound => Err(Status::not_found("instance not found")),
        WakeInstanceError::GenerationConflict { expected, actual } => {
            Ok(pb::ProxyWakeInstanceResponse {
                outcome: Some(
                    pb::proxy_wake_instance_response::Outcome::GenerationConflict(
                        pb::ProxyWakeGenerationConflictResult {
                            instance_id: request_instance_id,
                            expected_generation: expected.get(),
                            actual_generation: actual.get(),
                        },
                    ),
                ),
            })
        }
        WakeInstanceError::Unavailable { instance, reason } => Ok(pb::ProxyWakeInstanceResponse {
            outcome: Some(pb::proxy_wake_instance_response::Outcome::Unavailable(
                pb::ProxyWakeUnavailableResult {
                    instance_id: instance.id.as_str().to_owned(),
                    instance_generation: instance.generation.get(),
                    reason: proxy_unavailable_reason_to_proto(reason) as i32,
                },
            )),
        }),
        WakeInstanceError::ReadyMaterializationNotFound { instance, .. } => {
            Err(Status::failed_precondition(format!(
                "ready materialization was not found for instance {} and configured target",
                instance.id.as_str()
            )))
        }
        WakeInstanceError::WorkloadClassNotFound { instance } => {
            Err(Status::failed_precondition(format!(
                "workload class version was not found for instance {}",
                instance.id.as_str()
            )))
        }
        WakeInstanceError::Render { instance, source } => Err(Status::internal(format!(
            "manifest render failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        WakeInstanceError::Materializer { instance, source } => Err(Status::unavailable(format!(
            "materialization failed for instance {}: {source}",
            instance.id.as_str()
        ))),
        WakeInstanceError::Store(error) => Err(store_error_to_status(error)),
    }
}

fn proxy_unavailable_reason_to_proto(
    reason: WakeUnavailableReason,
) -> pb::ProxyWakeUnavailableReason {
    match reason {
        WakeUnavailableReason::Deleting => pb::ProxyWakeUnavailableReason::Deleting,
        WakeUnavailableReason::Deleted => pb::ProxyWakeUnavailableReason::Deleted,
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
