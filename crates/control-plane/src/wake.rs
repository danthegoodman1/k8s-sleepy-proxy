use std::{error::Error, fmt, time::Instant};

use proxy_core::observability::{
    metrics::{RUNTIME_MATERIALIZATION_FAILURES_TOTAL, RUNTIME_WAKE_LATENCY_SECONDS},
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder,
        EVENT_MATERIALIZATION_FAILURE, EVENT_WAKE,
    },
    Operation, Outcome,
};

use crate::{
    ids::{BackendGeneration, Generation, InstanceId},
    instance::{
        CompareAndSwapInstanceStateRequest, GetInstanceRequest, InstanceRecord, InstanceState,
        StateTransitionReason,
    },
    manifest::{render_manifests, ManifestRenderError, RenderManifestRequest},
    materialization::{
        CompleteWakeRequest, CompleteWakeResult, FinalizeSleepRequest,
        LoadActiveMaterializationRequest, LoadReadyMaterializationRequest, MaterializationRecord,
        MaterializationState, MaterializationTarget, RecordMaterializationRequest,
        RenderedObjectRef,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient, MaterializerError},
    sleep_policy::SleepPolicyError,
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
    #[allow(dead_code)]
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
    WorkloadClassNotFound {
        instance: InstanceRecord,
    },
    Store(StoreError),
    Render {
        instance: InstanceRecord,
        source: ManifestRenderError,
    },
    SleepPolicy {
        instance: InstanceRecord,
        source: SleepPolicyError,
    },
    Materializer {
        instance: InstanceRecord,
        source: MaterializerError,
    },
}

pub async fn wake_instance_with_observability<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    request: WakeInstanceRequest,
    observability: ObservabilityRecorder,
) -> Result<WakeInstanceResult, WakeInstanceError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    let started = Instant::now();
    let request_fields = vec![
        LogField::instance_id(request.instance_id.as_str()),
        LogField::generation(request.expected_generation.get()),
        LogField::cluster_id(request.target.cluster_id()),
        LogField::namespace(request.target.namespace()),
    ];
    let result = wake_instance(store, materializer, request).await;
    record_wake_observation(&observability, started, &request_fields, &result);
    result
}

async fn wake_instance<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    request: WakeInstanceRequest,
) -> Result<WakeInstanceResult, WakeInstanceError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    let mut instance = store
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

    let should_consider_active_backend_generation = request.backend_generation.is_none()
        && matches!(
            instance.state,
            InstanceState::Waking | InstanceState::Failed
        );

    let waking = match instance.state {
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
        InstanceState::Waking => Some(instance.clone()),
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
        InstanceState::Draining => {
            if let Some(materialization) = store
                .load_active_materialization(LoadActiveMaterializationRequest::new(
                    request.instance_id.clone(),
                    request.target.clone(),
                ))
                .await
                .map_err(map_store_error)?
            {
                if materialization.state == MaterializationState::Deleting {
                    instance = resume_deleting_sleep(
                        store,
                        materializer,
                        request.target.clone(),
                        instance,
                        materialization,
                    )
                    .await?;
                }
            }

            None
        }
        InstanceState::Cold | InstanceState::Failed => None,
    };

    let waking = if let Some(waking) = waking {
        waking
    } else {
        store
            .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
                request.instance_id.clone(),
                instance.generation,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ))
            .await
            .map_err(map_store_error)?
    };

    let workload_class = match store
        .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
            waking.workload_class.clone(),
        ))
        .await
    {
        Ok(Some(workload_class)) => workload_class,
        Ok(None) => {
            return Err(fail_waking(
                store,
                &waking,
                WakeInstanceError::WorkloadClassNotFound {
                    instance: waking.clone(),
                },
            )
            .await);
        }
        Err(error) => return Err(fail_waking_with_store_error(store, &waking, error).await),
    };

    let sleep_policy = match workload_class.sleep_policy.resolve(&waking.values) {
        Ok(policy) => policy,
        Err(error) => {
            return Err(fail_waking(
                store,
                &waking,
                WakeInstanceError::SleepPolicy {
                    instance: waking.clone(),
                    source: error,
                },
            )
            .await);
        }
    };

    let manifest = match render_manifests(RenderManifestRequest {
        template: &workload_class.template,
        instance: &waking,
        sleep_policy,
        namespace: request.target.namespace(),
        template_generation: Some(workload_class.template_generation),
    }) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Err(fail_waking(
                store,
                &waking,
                WakeInstanceError::Render {
                    instance: waking.clone(),
                    source: error,
                },
            )
            .await);
        }
    };

    let rendered_objects = match materializer.rendered_object_refs(&manifest) {
        Ok(rendered_objects) => rendered_objects,
        Err(error) => {
            return Err(fail_waking(
                store,
                &waking,
                WakeInstanceError::Materializer {
                    instance: waking.clone(),
                    source: error,
                },
            )
            .await);
        }
    };

    let backend_generation = match request.backend_generation {
        Some(backend_generation) => backend_generation,
        None => {
            let default = BackendGeneration::new(waking.generation.get());
            if should_consider_active_backend_generation {
                match store
                    .load_active_materialization(LoadActiveMaterializationRequest::new(
                        request.instance_id.clone(),
                        request.target.clone(),
                    ))
                    .await
                {
                    Ok(Some(materialization)) => {
                        std::cmp::max(default, materialization.backend_generation)
                    }
                    Ok(None) => default,
                    Err(error) => {
                        return Err(fail_waking_with_store_error(store, &waking, error).await);
                    }
                }
            } else {
                default
            }
        }
    };
    let mut pending = RecordMaterializationRequest::new(
        request.instance_id.clone(),
        waking.generation,
        request.target.clone(),
        MaterializationState::Pending,
        backend_generation,
    );
    pending.rendered_objects = rendered_objects.clone();
    if let Err(error) = store.record_materialization(pending).await {
        return Err(fail_waking_with_store_error(store, &waking, error).await);
    }

    if let Err(error) = materializer.apply_manifest(&manifest).await {
        return Err(fail_waking(
            store,
            &waking,
            WakeInstanceError::Materializer {
                instance: waking.clone(),
                source: error,
            },
        )
        .await);
    }

    let backend = match materializer.wait_for_readiness(&rendered_objects).await {
        Ok(backend) => backend,
        Err(error) => {
            let original = fail_waking(
                store,
                &waking,
                WakeInstanceError::Materializer {
                    instance: waking.clone(),
                    source: error,
                },
            )
            .await;
            cleanup_rendered_objects_if_instance_missing_or_terminal(
                store,
                materializer,
                &waking.id,
                &rendered_objects,
            )
            .await;
            return Err(original);
        }
    };

    let complete_instance_id = request.instance_id.clone();
    let mut complete = CompleteWakeRequest::new(
        request.instance_id,
        waking.generation,
        request.target,
        backend,
        backend_generation,
    );
    complete.rendered_objects = rendered_objects.clone();

    match store.complete_wake(complete).await {
        Ok(result) => Ok(WakeInstanceResult::Completed { result }),
        Err(error) => {
            cleanup_rendered_objects_if_instance_missing_or_terminal(
                store,
                materializer,
                &complete_instance_id,
                &rendered_objects,
            )
            .await;
            Err(map_store_error(error))
        }
    }
}

async fn cleanup_rendered_objects_if_instance_missing_or_terminal<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    instance_id: &InstanceId,
    rendered_objects: &[RenderedObjectRef],
) where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    let should_cleanup = match store
        .get_instance(GetInstanceRequest::new(instance_id.clone()))
        .await
    {
        Ok(None) => true,
        Ok(Some(instance)) => matches!(
            instance.state,
            InstanceState::Deleting | InstanceState::Deleted
        ),
        Err(_) => false,
    };

    if should_cleanup {
        let _ = materializer.delete_rendered_objects(rendered_objects).await;
    }
}

async fn resume_deleting_sleep<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: MaterializationTarget,
    instance: InstanceRecord,
    materialization: MaterializationRecord,
) -> Result<InstanceRecord, WakeInstanceError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    materializer
        .delete_rendered_objects(&materialization.rendered_objects)
        .await
        .map_err(|source| WakeInstanceError::Materializer {
            instance: instance.clone(),
            source,
        })?;

    store
        .finalize_sleep(FinalizeSleepRequest::new(
            instance.id,
            instance.generation,
            target,
        ))
        .await
        .map(|result| result.instance)
        .map_err(map_store_error)
}

fn record_wake_observation(
    observability: &ObservabilityRecorder,
    started: Instant,
    request_fields: &[LogField],
    result: &Result<WakeInstanceResult, WakeInstanceError>,
) {
    let outcome = match result {
        Ok(WakeInstanceResult::Completed { .. }) => Outcome::Success,
        Ok(WakeInstanceResult::AlreadyRunning { .. }) => Outcome::AlreadyRunning,
        Ok(WakeInstanceResult::AlreadyWaking { .. }) => Outcome::AlreadyWaking,
        Err(WakeInstanceError::GenerationConflict { .. })
        | Err(WakeInstanceError::Unavailable { .. }) => Outcome::Rejected,
        Err(_) => Outcome::Error,
    };
    observability.record_metric(MetricObservation::new(
        RUNTIME_WAKE_LATENCY_SECONDS,
        vec![outcome.metric_label()],
        started.elapsed().as_secs_f64(),
    ));

    let mut fields = request_fields.to_vec();
    if let Err(error) = result {
        fields.push(LogField::error_reason(wake_error_reason(error)));
    }
    observability.record_log(LifecycleLogEvent::new(EVENT_WAKE, fields));

    if let Err(WakeInstanceError::Materializer { source, instance }) = result {
        observability.record_metric(MetricObservation::new(
            RUNTIME_MATERIALIZATION_FAILURES_TOTAL,
            vec![
                Operation::Materialize.metric_label(),
                Outcome::Error.metric_label(),
            ],
            1.0,
        ));
        observability.record_log(LifecycleLogEvent::new(
            EVENT_MATERIALIZATION_FAILURE,
            vec![
                LogField::instance_id(instance.id.as_str()),
                LogField::generation(instance.generation.get()),
                LogField::error_reason(source.to_string()),
            ],
        ));
    }
}

fn wake_error_reason(error: &WakeInstanceError) -> &'static str {
    match error {
        WakeInstanceError::NotFound => "not_found",
        WakeInstanceError::GenerationConflict { .. } => "generation_conflict",
        WakeInstanceError::Unavailable { .. } => "unavailable",
        WakeInstanceError::ReadyMaterializationNotFound { .. } => "ready_materialization_not_found",
        WakeInstanceError::WorkloadClassNotFound { .. } => "workload_class_not_found",
        WakeInstanceError::Store(_) => "store",
        WakeInstanceError::Render { .. } => "render",
        WakeInstanceError::SleepPolicy { .. } => "sleep_policy",
        WakeInstanceError::Materializer { .. } => "materializer",
    }
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
    #[cfg(test)]
    pub fn instance(&self) -> &InstanceRecord {
        match self {
            Self::Completed { result } => &result.instance,
            Self::AlreadyRunning { instance, .. } | Self::AlreadyWaking { instance } => instance,
        }
    }

    #[cfg(test)]
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
    S: ControlPlaneStore + ?Sized,
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
    S: ControlPlaneStore + ?Sized,
{
    let mapped = map_store_error(error);
    fail_waking(store, waking, mapped).await
}

async fn best_effort_mark_failed<S>(store: &S, waking: &InstanceRecord, message: String)
where
    S: ControlPlaneStore + ?Sized,
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
        WakeInstanceError::WorkloadClassNotFound { .. } => {
            "workload class version not found".to_owned()
        }
        WakeInstanceError::Render { source, .. } => format!("manifest render failed: {source}"),
        WakeInstanceError::SleepPolicy { source, .. } => {
            format!("sleep policy resolution failed: {source}")
        }
        WakeInstanceError::Materializer { source, .. } => {
            format!("materialization failed: {source}")
        }
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
            Self::WorkloadClassNotFound { .. } => f.write_str("workload class version not found"),
            Self::Store(error) => write!(f, "wake store operation failed: {error}"),
            Self::Render { source, .. } => write!(f, "wake manifest render failed: {source}"),
            Self::SleepPolicy { source, .. } => {
                write!(f, "wake sleep policy resolution failed: {source}")
            }
            Self::Materializer { source, .. } => {
                write!(f, "wake materializer failed: {source}")
            }
        }
    }
}

impl Error for WakeInstanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Render { source, .. } => Some(source),
            Self::SleepPolicy { source, .. } => Some(source),
            Self::Materializer { source, .. } => Some(source),
            Self::NotFound
            | Self::GenerationConflict { .. }
            | Self::Unavailable { .. }
            | Self::ReadyMaterializationNotFound { .. }
            | Self::WorkloadClassNotFound { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;
