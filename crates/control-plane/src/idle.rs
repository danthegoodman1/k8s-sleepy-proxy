use crate::{
    ids::{Generation, InstanceId},
    instance::{GetInstanceRequest, InstanceRecord, InstanceState},
    materialization::{
        BeginSleepRequest, BeginSleepResult, FinalizeSleepRequest,
        LoadActiveMaterializationRequest, MaterializationRecord, MaterializationState,
        MaterializationTarget,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    projection::{ProjectionError, ProjectionPlan, ProjectionReconciler},
    store::{ControlPlaneStore, StoreError},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportIdleRequest {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
    pub active_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportIdleResult {
    Accepted {
        instance: InstanceRecord,
    },
    AlreadyDraining {
        instance: InstanceRecord,
    },
    Unavailable {
        instance: InstanceRecord,
        reason: ReportIdleUnavailableReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReportIdleUnavailableReason {
    Cold,
    Waking,
    Draining,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Debug)]
pub enum ReportIdleError {
    ActiveRequestsPresent {
        active_count: u64,
    },
    NotFound,
    GenerationConflict {
        expected: Generation,
        actual: Generation,
    },
    Projection {
        instance: InstanceRecord,
        source: ProjectionError,
    },
    Store(StoreError),
}

impl ReportIdleRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_generation: Generation,
        active_count: u64,
    ) -> Self {
        Self {
            instance_id,
            expected_generation,
            active_count,
        }
    }
}

pub async fn report_idle<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: MaterializationTarget,
    request: ReportIdleRequest,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    if request.active_count > 0 {
        return Err(ReportIdleError::ActiveRequestsPresent {
            active_count: request.active_count,
        });
    }

    let current = load_instance(store, request.instance_id.clone()).await?;
    if current.generation != request.expected_generation {
        return generation_mismatch_result(store, materializer, target, &request, current).await;
    }

    match current.state {
        InstanceState::Running => {
            finalize_running_sleep(store, materializer, target, request).await
        }
        state => Ok(ReportIdleResult::Unavailable {
            instance: current,
            reason: unavailable_reason_for_state(state),
        }),
    }
}

async fn finalize_running_sleep<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: MaterializationTarget,
    request: ReportIdleRequest,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    let begin = match store
        .begin_sleep(BeginSleepRequest::new(
            request.instance_id.clone(),
            request.expected_generation,
            target.clone(),
        ))
        .await
    {
        Ok(begin) => Ok(begin),
        Err(StoreError::NotFound { .. }) => Err(ReportIdleError::NotFound),
        Err(StoreError::GenerationConflict { expected, actual }) => {
            if actual == expected.next() {
                let current = load_instance(store, request.instance_id.clone()).await?;
                if current.generation == actual {
                    return generation_mismatch_result(
                        store,
                        materializer,
                        target,
                        &request,
                        current,
                    )
                    .await;
                }
            }

            Err(ReportIdleError::GenerationConflict { expected, actual })
        }
        Err(error) => Err(ReportIdleError::Store(error)),
    }?;

    cleanup_and_finalize_sleep(
        store,
        materializer,
        target,
        begin.instance,
        begin.materialization,
    )
    .await
}

async fn generation_mismatch_result<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: MaterializationTarget,
    request: &ReportIdleRequest,
    current: InstanceRecord,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    if current.generation == request.expected_generation.next() {
        if current.state == InstanceState::Running {
            let begin = begin_sleep_at_generation(
                store,
                request.instance_id.clone(),
                current.generation,
                target.clone(),
            )
            .await?;
            return cleanup_and_finalize_sleep(
                store,
                materializer,
                target,
                begin.instance,
                begin.materialization,
            )
            .await;
        }

        if current.state == InstanceState::Draining {
            let materialization = store
                .load_active_materialization(LoadActiveMaterializationRequest::new(
                    request.instance_id.clone(),
                    target.clone(),
                ))
                .await
                .map_err(ReportIdleError::Store)?;

            if materialization.as_ref().is_some_and(|materialization| {
                materialization.state == MaterializationState::Deleting
            }) {
                return cleanup_and_finalize_sleep(
                    store,
                    materializer,
                    target,
                    current,
                    materialization,
                )
                .await;
            }

            return Ok(ReportIdleResult::AlreadyDraining { instance: current });
        }
    }

    if current.generation == request.expected_generation.next().next()
        && current.state == InstanceState::Cold
    {
        return Ok(ReportIdleResult::Accepted { instance: current });
    }

    Err(ReportIdleError::GenerationConflict {
        expected: request.expected_generation,
        actual: current.generation,
    })
}

async fn begin_sleep_at_generation<S>(
    store: &S,
    instance_id: InstanceId,
    generation: Generation,
    target: MaterializationTarget,
) -> Result<BeginSleepResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    match store
        .begin_sleep(BeginSleepRequest::new(instance_id, generation, target))
        .await
    {
        Ok(begin) => Ok(begin),
        Err(StoreError::NotFound { .. }) => Err(ReportIdleError::NotFound),
        Err(StoreError::GenerationConflict { expected, actual }) => {
            Err(ReportIdleError::GenerationConflict { expected, actual })
        }
        Err(error) => Err(ReportIdleError::Store(error)),
    }
}

async fn cleanup_and_finalize_sleep<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: MaterializationTarget,
    instance: InstanceRecord,
    materialization: Option<MaterializationRecord>,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    if let Some(materialization) = materialization.as_ref() {
        let plan = ProjectionPlan::from_recorded_refs(materialization);
        ProjectionReconciler::new(materializer)
            .delete_owned(&plan)
            .await
            .map_err(|source| ReportIdleError::Projection {
                instance: instance.clone(),
                source,
            })?;
    }

    let finalized = store
        .finalize_sleep(FinalizeSleepRequest::new(
            instance.id,
            instance.generation,
            target,
        ))
        .await
        .map_err(map_finalize_store_error)?;

    Ok(ReportIdleResult::Accepted {
        instance: finalized.instance,
    })
}

async fn load_instance<S>(
    store: &S,
    instance_id: InstanceId,
) -> Result<InstanceRecord, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    store
        .get_instance(GetInstanceRequest::new(instance_id))
        .await
        .map_err(ReportIdleError::Store)?
        .ok_or(ReportIdleError::NotFound)
}

fn unavailable_reason_for_state(state: InstanceState) -> ReportIdleUnavailableReason {
    match state {
        InstanceState::Cold => ReportIdleUnavailableReason::Cold,
        InstanceState::Waking => ReportIdleUnavailableReason::Waking,
        InstanceState::Running => {
            unreachable!("running instances are handled before unavailable mapping")
        }
        InstanceState::Draining => ReportIdleUnavailableReason::Draining,
        InstanceState::Failed => ReportIdleUnavailableReason::Failed,
        InstanceState::Deleting => ReportIdleUnavailableReason::Deleting,
        InstanceState::Deleted => ReportIdleUnavailableReason::Deleted,
    }
}

fn map_finalize_store_error(error: StoreError) -> ReportIdleError {
    match error {
        StoreError::NotFound { .. } => ReportIdleError::NotFound,
        StoreError::GenerationConflict { expected, actual } => {
            ReportIdleError::GenerationConflict { expected, actual }
        }
        other => ReportIdleError::Store(other),
    }
}
