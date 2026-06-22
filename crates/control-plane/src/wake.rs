use std::{error::Error, fmt};

use crate::{
    ids::{BackendGeneration, Generation, InstanceId},
    instance::{
        CompareAndSwapInstanceStateRequest, GetInstanceRequest, InstanceRecord, InstanceState,
        StateTransitionReason,
    },
    manifest::{render_manifests, ManifestRenderError, RenderManifestRequest},
    materialization::{
        CompleteWakeRequest, CompleteWakeResult, LoadReadyMaterializationRequest,
        MaterializationRecord, MaterializationTarget, RenderedObjectRef,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient, MaterializerError},
    store::{ControlPlaneStore, StoreError},
    workload::LoadWorkloadClassVersionRequest,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WakeInstanceRequest {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
    pub target: MaterializationTarget,
    pub backend_generation: Option<BackendGeneration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeInstanceResult {
    Completed {
        result: CompleteWakeResult,
    },
    AlreadyRunning {
        instance: InstanceRecord,
        materialization: MaterializationRecord,
    },
    AlreadyWaking {
        instance: InstanceRecord,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeUnavailableReason {
    Deleting,
    Deleted,
}

#[derive(Debug)]
pub enum WakeInstanceError {
    NotFound,
    GenerationConflict {
        expected: Generation,
        actual: Generation,
    },
    Unavailable {
        instance: InstanceRecord,
        reason: WakeUnavailableReason,
    },
    ReadyMaterializationNotFound {
        instance: InstanceRecord,
        target: MaterializationTarget,
    },
    WorkloadClassNotFound,
    Store(StoreError),
    Render(ManifestRenderError),
    Materializer(MaterializerError),
}

pub async fn wake_instance<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    request: WakeInstanceRequest,
) -> Result<WakeInstanceResult, WakeInstanceError>
where
    S: ControlPlaneStore,
    C: KubernetesMaterializerClient,
{
    let instance = store
        .get_instance(GetInstanceRequest::new(request.instance_id.clone()))
        .await
        .map_err(map_store_error)?
        .ok_or(WakeInstanceError::NotFound)?;

    if instance.generation != request.expected_generation {
        return Err(WakeInstanceError::GenerationConflict {
            expected: request.expected_generation,
            actual: instance.generation,
        });
    }

    match instance.state {
        InstanceState::Running => {
            let target = request.target.clone();
            let materialization = store
                .load_ready_materialization(LoadReadyMaterializationRequest::new(
                    request.instance_id,
                    instance.generation,
                    request.target,
                ))
                .await
                .map_err(map_store_error)?
                .ok_or_else(|| WakeInstanceError::ReadyMaterializationNotFound {
                    instance: instance.clone(),
                    target,
                })?;

            return Ok(WakeInstanceResult::AlreadyRunning {
                instance,
                materialization,
            });
        }
        InstanceState::Waking => {
            return Ok(WakeInstanceResult::AlreadyWaking { instance });
        }
        InstanceState::Deleting => {
            return Err(WakeInstanceError::Unavailable {
                instance,
                reason: WakeUnavailableReason::Deleting,
            });
        }
        InstanceState::Deleted => {
            return Err(WakeInstanceError::Unavailable {
                instance,
                reason: WakeUnavailableReason::Deleted,
            });
        }
        InstanceState::Cold | InstanceState::Failed | InstanceState::Draining => {}
    }

    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            request.instance_id.clone(),
            request.expected_generation,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await
        .map_err(map_store_error)?;

    let workload_class = match store
        .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
            waking.workload_class.clone(),
        ))
        .await
    {
        Ok(Some(workload_class)) => workload_class,
        Ok(None) => {
            return Err(
                fail_waking(store, &waking, WakeInstanceError::WorkloadClassNotFound).await,
            );
        }
        Err(error) => return Err(fail_waking_with_store_error(store, &waking, error).await),
    };

    let manifest = match render_manifests(RenderManifestRequest {
        template: &workload_class.template,
        instance: &waking,
        namespace: request.target.namespace(),
        template_generation: Some(workload_class.template_generation),
    }) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Err(fail_waking(store, &waking, WakeInstanceError::Render(error)).await);
        }
    };

    let applied = match materializer.apply_manifest_until_ready(&manifest).await {
        Ok(applied) => applied,
        Err(error) => {
            return Err(fail_waking(store, &waking, WakeInstanceError::Materializer(error)).await);
        }
    };

    let backend_generation = request
        .backend_generation
        .unwrap_or_else(|| BackendGeneration::new(waking.generation.get()));
    let mut complete = CompleteWakeRequest::new(
        request.instance_id,
        waking.generation,
        request.target,
        applied.backend,
        backend_generation,
    );
    complete.rendered_objects = applied.rendered_objects;

    store
        .complete_wake(complete)
        .await
        .map(|result| WakeInstanceResult::Completed { result })
        .map_err(map_store_error)
}

impl WakeInstanceRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            instance_id,
            expected_generation,
            target,
            backend_generation: None,
        }
    }

    pub fn with_backend_generation(mut self, backend_generation: BackendGeneration) -> Self {
        self.backend_generation = Some(backend_generation);
        self
    }
}

impl WakeInstanceResult {
    pub fn instance(&self) -> &InstanceRecord {
        match self {
            Self::Completed { result } => &result.instance,
            Self::AlreadyRunning { instance, .. } | Self::AlreadyWaking { instance } => instance,
        }
    }

    pub fn rendered_objects(&self) -> &[RenderedObjectRef] {
        match self {
            Self::Completed { result } => &result.materialization.rendered_objects,
            Self::AlreadyRunning {
                materialization, ..
            } => &materialization.rendered_objects,
            Self::AlreadyWaking { .. } => &[],
        }
    }
}

fn map_store_error(error: StoreError) -> WakeInstanceError {
    match error {
        StoreError::NotFound {
            resource: "instance",
        } => WakeInstanceError::NotFound,
        StoreError::GenerationConflict { expected, actual } => {
            WakeInstanceError::GenerationConflict { expected, actual }
        }
        other => WakeInstanceError::Store(other),
    }
}

async fn fail_waking<S>(
    store: &S,
    waking: &InstanceRecord,
    original: WakeInstanceError,
) -> WakeInstanceError
where
    S: ControlPlaneStore,
{
    best_effort_mark_failed(store, waking, failure_message(&original)).await;
    original
}

async fn fail_waking_with_store_error<S>(
    store: &S,
    waking: &InstanceRecord,
    error: StoreError,
) -> WakeInstanceError
where
    S: ControlPlaneStore,
{
    let mapped = map_store_error(error);
    fail_waking(store, waking, mapped).await
}

async fn best_effort_mark_failed<S>(store: &S, waking: &InstanceRecord, message: String)
where
    S: ControlPlaneStore,
{
    let request = CompareAndSwapInstanceStateRequest::new(
        waking.id.clone(),
        waking.generation,
        InstanceState::Failed,
        StateTransitionReason::FailureReported(message),
    );
    let _ = store.compare_and_swap_instance_state(request).await;
}

fn failure_message(error: &WakeInstanceError) -> String {
    match error {
        WakeInstanceError::WorkloadClassNotFound => "workload class version not found".to_owned(),
        WakeInstanceError::Render(error) => format!("manifest render failed: {error}"),
        WakeInstanceError::Materializer(error) => format!("materialization failed: {error}"),
        WakeInstanceError::Store(error) => format!("store operation failed: {error}"),
        WakeInstanceError::NotFound => "instance not found".to_owned(),
        WakeInstanceError::GenerationConflict { expected, actual } => {
            format!("generation conflict: expected {expected}, actual {actual}")
        }
        WakeInstanceError::Unavailable { reason, .. } => {
            format!("instance is unavailable for wake: {reason:?}")
        }
        WakeInstanceError::ReadyMaterializationNotFound { target, .. } => format!(
            "ready materialization not found for target {}/{}",
            target.cluster_id(),
            target.namespace()
        ),
    }
}

impl fmt::Display for WakeInstanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("instance not found"),
            Self::GenerationConflict { expected, actual } => {
                write!(
                    f,
                    "wake generation conflict: expected generation {expected}, found {actual}"
                )
            }
            Self::Unavailable { reason, .. } => {
                write!(f, "instance is unavailable for wake: {reason:?}")
            }
            Self::ReadyMaterializationNotFound { target, .. } => write!(
                f,
                "ready materialization not found for target {}/{}",
                target.cluster_id(),
                target.namespace()
            ),
            Self::WorkloadClassNotFound => f.write_str("workload class version not found"),
            Self::Store(error) => write!(f, "wake store operation failed: {error}"),
            Self::Render(error) => write!(f, "wake manifest render failed: {error}"),
            Self::Materializer(error) => write!(f, "wake materializer failed: {error}"),
        }
    }
}

impl Error for WakeInstanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Render(error) => Some(error),
            Self::Materializer(error) => Some(error),
            Self::NotFound
            | Self::GenerationConflict { .. }
            | Self::Unavailable { .. }
            | Self::ReadyMaterializationNotFound { .. }
            | Self::WorkloadClassNotFound => None,
        }
    }
}

#[cfg(test)]
mod tests;
