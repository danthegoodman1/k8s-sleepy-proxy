use crate::{
    ids::{Generation, InstanceId},
    instance::{
        CompareAndSwapInstanceStateRequest, GetInstanceRequest, InstanceRecord, InstanceState,
        StateTransitionReason,
    },
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

pub async fn report_idle(
    store: &dyn ControlPlaneStore,
    request: ReportIdleRequest,
) -> Result<ReportIdleResult, ReportIdleError> {
    if request.active_count > 0 {
        return Err(ReportIdleError::ActiveRequestsPresent {
            active_count: request.active_count,
        });
    }

    let current = load_instance(store, request.instance_id.clone()).await?;
    if current.generation != request.expected_generation {
        return generation_mismatch_result(&request, current);
    }

    match current.state {
        InstanceState::Running => transition_running_to_draining(store, request).await,
        state => Ok(ReportIdleResult::Unavailable {
            instance: current,
            reason: unavailable_reason_for_state(state),
        }),
    }
}

async fn transition_running_to_draining(
    store: &dyn ControlPlaneStore,
    request: ReportIdleRequest,
) -> Result<ReportIdleResult, ReportIdleError> {
    match store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            request.instance_id.clone(),
            request.expected_generation,
            InstanceState::Draining,
            StateTransitionReason::IdleReported,
        ))
        .await
    {
        Ok(instance) => Ok(ReportIdleResult::Accepted { instance }),
        Err(StoreError::NotFound { .. }) => Err(ReportIdleError::NotFound),
        Err(StoreError::GenerationConflict { expected, actual }) => {
            if actual == expected.next() {
                let current = load_instance(store, request.instance_id.clone()).await?;
                if current.generation == actual && current.state == InstanceState::Draining {
                    return Ok(ReportIdleResult::AlreadyDraining { instance: current });
                }
            }

            Err(ReportIdleError::GenerationConflict { expected, actual })
        }
        Err(error) => Err(ReportIdleError::Store(error)),
    }
}

fn generation_mismatch_result(
    request: &ReportIdleRequest,
    current: InstanceRecord,
) -> Result<ReportIdleResult, ReportIdleError> {
    if current.generation == request.expected_generation.next()
        && current.state == InstanceState::Draining
    {
        return Ok(ReportIdleResult::AlreadyDraining { instance: current });
    }

    Err(ReportIdleError::GenerationConflict {
        expected: request.expected_generation,
        actual: current.generation,
    })
}

async fn load_instance(
    store: &dyn ControlPlaneStore,
    instance_id: InstanceId,
) -> Result<InstanceRecord, ReportIdleError> {
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
