use std::time::Duration;

use crate::{
    ids::{Generation, InstanceId},
    instance::{GetInstanceRequest, InstanceRecord, InstanceState},
    materialization::{BeginSleepRequest, BeginSleepResult, MaterializationTarget},
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    store::{ControlPlaneStore, StoreError},
    workload::LoadWorkloadClassVersionRequest,
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
    WorkloadClassNotFound,
    SleepPolicy(String),
    GenerationConflict {
        expected: Generation,
        actual: Generation,
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
    _materializer: &KubernetesMaterializer<C>,
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
        return generation_mismatch_result(store, target, &request, current).await;
    }

    match current.state {
        InstanceState::Running => begin_running_sleep(store, target, request, &current).await,
        state => Ok(ReportIdleResult::Unavailable {
            instance: current,
            reason: unavailable_reason_for_state(state),
        }),
    }
}

async fn begin_running_sleep<S>(
    store: &S,
    target: MaterializationTarget,
    request: ReportIdleRequest,
    current: &InstanceRecord,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    let drain_grace_timeout = drain_grace_timeout(store, current).await?;
    let begin = match store
        .begin_sleep(
            BeginSleepRequest::new(
                request.instance_id.clone(),
                request.expected_generation,
                target.clone(),
            )
            .with_drain_grace_timeout(drain_grace_timeout),
        )
        .await
    {
        Ok(begin) => Ok(begin),
        Err(StoreError::NotFound { .. }) => Err(ReportIdleError::NotFound),
        Err(StoreError::GenerationConflict { expected, actual }) => {
            if actual == expected.next() {
                let current = load_instance(store, request.instance_id.clone()).await?;
                if current.generation == actual {
                    return generation_mismatch_result(store, target, &request, current).await;
                }
            }

            Err(ReportIdleError::GenerationConflict { expected, actual })
        }
        Err(error) => Err(ReportIdleError::Store(error)),
    }?;

    Ok(ReportIdleResult::Accepted {
        instance: begin.instance,
    })
}

async fn generation_mismatch_result<S>(
    store: &S,
    target: MaterializationTarget,
    request: &ReportIdleRequest,
    current: InstanceRecord,
) -> Result<ReportIdleResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    if current.generation == request.expected_generation.next() {
        if current.state == InstanceState::Running {
            let begin = begin_sleep_at_generation(
                store,
                request.instance_id.clone(),
                current.generation,
                target.clone(),
                drain_grace_timeout(store, &current).await?,
            )
            .await?;
            return Ok(ReportIdleResult::Accepted {
                instance: begin.instance,
            });
        }

        if current.state == InstanceState::Draining {
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
    drain_grace_timeout: Duration,
) -> Result<BeginSleepResult, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    match store
        .begin_sleep(
            BeginSleepRequest::new(instance_id, generation, target)
                .with_drain_grace_timeout(drain_grace_timeout),
        )
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

async fn drain_grace_timeout<S>(
    store: &S,
    instance: &InstanceRecord,
) -> Result<Duration, ReportIdleError>
where
    S: ControlPlaneStore + ?Sized,
{
    let workload_class = store
        .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
            instance.workload_class.clone(),
        ))
        .await
        .map_err(ReportIdleError::Store)?
        .ok_or(ReportIdleError::WorkloadClassNotFound)?;
    let sleep_policy = workload_class
        .sleep_policy
        .resolve(&instance.values)
        .map_err(|error| ReportIdleError::SleepPolicy(error.to_string()))?;
    Ok(Duration::from_millis(sleep_policy.drain_grace_timeout_ms))
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
