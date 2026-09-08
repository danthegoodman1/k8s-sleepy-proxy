use std::collections::BTreeSet;

use sleepypods_api::{
    BackendEndpoint, BackendGeneration, Generation, InstanceId, InstanceState, RouteEntry,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WakeInstanceRequest {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteWakeDecision {
    Ready(ReadyBackend),
    Wake {
        request: WakeInstanceRequest,
        reason: WakeReason,
    },
    Wait(WakeWait),
    Unavailable(WakeUnavailable),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyBackend {
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    pub backend: BackendEndpoint,
    pub backend_generation: Option<BackendGeneration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeReason {
    Cold,
    Waking,
    Failed,
    Draining,
    RunningMissingBackend,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WakeWait {
    pub instance_id: InstanceId,
    pub generation: Generation,
    pub reason: WakeWaitReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeWaitReason {
    AlreadyWaking,
    DuplicatePendingWake,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WakeUnavailable {
    pub instance_id: InstanceId,
    pub generation: Generation,
    pub reason: WakeUnavailableReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeUnavailableReason {
    Deleting,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeAdmission {
    Start(WakeInstanceRequest),
    Wait(WakeWait),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeInstanceResponse {
    WakeStarted {
        instance_id: InstanceId,
        generation: Generation,
    },
    AlreadyRunning {
        instance_id: InstanceId,
        generation: Generation,
        backend: BackendEndpoint,
        backend_generation: Option<BackendGeneration>,
    },
    StillWaking {
        instance_id: InstanceId,
        generation: Generation,
    },
    Failed {
        instance_id: InstanceId,
        generation: Generation,
        reason: String,
    },
    Unavailable {
        instance_id: InstanceId,
        generation: Generation,
        reason: String,
    },
    GenerationConflict {
        instance_id: InstanceId,
        expected_generation: Generation,
        actual_generation: Generation,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeResponseDisposition {
    WakeStarted {
        instance_id: InstanceId,
        generation: Generation,
    },
    Ready(ReadyBackend),
    StillWaking {
        instance_id: InstanceId,
        generation: Generation,
    },
    Failed {
        instance_id: InstanceId,
        generation: Generation,
        reason: String,
    },
    Unavailable {
        instance_id: InstanceId,
        generation: Generation,
        reason: String,
    },
    GenerationConflict {
        instance_id: InstanceId,
        expected_generation: Generation,
        actual_generation: Generation,
    },
    Rejected(StaleWakeObservation),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteUpdateDisposition {
    Accept,
    Reject(StaleWakeObservation),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StaleWakeObservation {
    InstanceMismatch {
        observed: InstanceId,
        incoming: InstanceId,
    },
    StaleInstanceGeneration {
        observed: Generation,
        incoming: Generation,
    },
    StaleBackendGeneration {
        observed: BackendGeneration,
        incoming: BackendGeneration,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WakeTracker {
    pending: BTreeSet<WakeKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct WakeKey {
    instance_id: InstanceId,
    generation: Generation,
}

pub fn route_wake_decision(entry: &RouteEntry) -> RouteWakeDecision {
    match entry.instance_state {
        InstanceState::Running => {
            if let Some(backend) = entry.backend.clone() {
                RouteWakeDecision::Ready(ReadyBackend {
                    instance_id: entry.instance_id.clone(),
                    instance_generation: entry.instance_generation,
                    backend,
                    backend_generation: entry.backend_generation,
                })
            } else {
                wake_decision(entry, WakeReason::RunningMissingBackend)
            }
        }
        InstanceState::Cold => wake_decision(entry, WakeReason::Cold),
        InstanceState::Failed => wake_decision(entry, WakeReason::Failed),
        InstanceState::Draining => wake_decision(entry, WakeReason::Draining),
        InstanceState::Waking => wake_decision(entry, WakeReason::Waking),
        InstanceState::Deleting => RouteWakeDecision::Unavailable(WakeUnavailable {
            instance_id: entry.instance_id.clone(),
            generation: entry.instance_generation,
            reason: WakeUnavailableReason::Deleting,
        }),
        InstanceState::Deleted => RouteWakeDecision::Unavailable(WakeUnavailable {
            instance_id: entry.instance_id.clone(),
            generation: entry.instance_generation,
            reason: WakeUnavailableReason::Deleted,
        }),
    }
}

pub fn validate_route_update(
    observed: &RouteEntry,
    incoming: &RouteEntry,
) -> RouteUpdateDisposition {
    match stale_route_update(
        &observed.instance_id,
        observed.instance_generation,
        observed.backend_generation,
        &incoming.instance_id,
        incoming.instance_generation,
        incoming.backend_generation,
    ) {
        Some(stale) => RouteUpdateDisposition::Reject(stale),
        None => RouteUpdateDisposition::Accept,
    }
}

pub fn validate_wake_response(
    observed: &RouteEntry,
    response: WakeInstanceResponse,
) -> WakeResponseDisposition {
    let stale = match &response {
        WakeInstanceResponse::WakeStarted {
            instance_id,
            generation,
        }
        | WakeInstanceResponse::StillWaking {
            instance_id,
            generation,
        }
        | WakeInstanceResponse::Failed {
            instance_id,
            generation,
            ..
        }
        | WakeInstanceResponse::Unavailable {
            instance_id,
            generation,
            ..
        } => stale_observation(
            &observed.instance_id,
            observed.instance_generation,
            observed.backend_generation,
            instance_id,
            *generation,
            None,
        ),
        WakeInstanceResponse::AlreadyRunning {
            instance_id,
            generation,
            backend_generation,
            ..
        } => stale_observation(
            &observed.instance_id,
            observed.instance_generation,
            observed.backend_generation,
            instance_id,
            *generation,
            *backend_generation,
        ),
        WakeInstanceResponse::GenerationConflict {
            instance_id,
            actual_generation,
            ..
        } => stale_observation(
            &observed.instance_id,
            observed.instance_generation,
            observed.backend_generation,
            instance_id,
            *actual_generation,
            None,
        ),
    };

    if let Some(stale) = stale {
        return WakeResponseDisposition::Rejected(stale);
    }

    match response {
        WakeInstanceResponse::WakeStarted {
            instance_id,
            generation,
        } => WakeResponseDisposition::WakeStarted {
            instance_id,
            generation,
        },
        WakeInstanceResponse::AlreadyRunning {
            instance_id,
            generation,
            backend,
            backend_generation,
        } => WakeResponseDisposition::Ready(ReadyBackend {
            instance_id,
            instance_generation: generation,
            backend,
            backend_generation,
        }),
        WakeInstanceResponse::StillWaking {
            instance_id,
            generation,
        } => WakeResponseDisposition::StillWaking {
            instance_id,
            generation,
        },
        WakeInstanceResponse::Failed {
            instance_id,
            generation,
            reason,
        } => WakeResponseDisposition::Failed {
            instance_id,
            generation,
            reason,
        },
        WakeInstanceResponse::Unavailable {
            instance_id,
            generation,
            reason,
        } => WakeResponseDisposition::Unavailable {
            instance_id,
            generation,
            reason,
        },
        WakeInstanceResponse::GenerationConflict {
            instance_id,
            expected_generation,
            actual_generation,
        } => WakeResponseDisposition::GenerationConflict {
            instance_id,
            expected_generation,
            actual_generation,
        },
    }
}

impl WakeInstanceRequest {
    pub fn from_route_entry(entry: &RouteEntry) -> Self {
        Self {
            instance_id: entry.instance_id.clone(),
            expected_generation: entry.instance_generation,
        }
    }
}

impl WakeTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_pending(&self, instance_id: &InstanceId, generation: Generation) -> bool {
        self.pending
            .contains(&WakeKey::new(instance_id.clone(), generation))
    }

    pub fn admit(&mut self, request: WakeInstanceRequest) -> WakeAdmission {
        let key = WakeKey::new(request.instance_id.clone(), request.expected_generation);
        if self.pending.insert(key) {
            WakeAdmission::Start(request)
        } else {
            WakeAdmission::Wait(WakeWait {
                instance_id: request.instance_id,
                generation: request.expected_generation,
                reason: WakeWaitReason::DuplicatePendingWake,
            })
        }
    }

    pub fn complete(&mut self, instance_id: &InstanceId, generation: Generation) -> bool {
        self.pending
            .remove(&WakeKey::new(instance_id.clone(), generation))
    }
}

impl WakeKey {
    fn new(instance_id: InstanceId, generation: Generation) -> Self {
        Self {
            instance_id,
            generation,
        }
    }
}

fn wake_decision(entry: &RouteEntry, reason: WakeReason) -> RouteWakeDecision {
    RouteWakeDecision::Wake {
        request: WakeInstanceRequest::from_route_entry(entry),
        reason,
    }
}

fn stale_observation(
    observed_instance_id: &InstanceId,
    observed_generation: Generation,
    observed_backend_generation: Option<BackendGeneration>,
    incoming_instance_id: &InstanceId,
    incoming_generation: Generation,
    incoming_backend_generation: Option<BackendGeneration>,
) -> Option<StaleWakeObservation> {
    if observed_instance_id != incoming_instance_id {
        return Some(StaleWakeObservation::InstanceMismatch {
            observed: observed_instance_id.clone(),
            incoming: incoming_instance_id.clone(),
        });
    }

    // Generation checks are scoped to a single instance. A lower instance
    // generation can otherwise revive a backend from before a sleep/delete cycle.
    if incoming_generation < observed_generation {
        return Some(StaleWakeObservation::StaleInstanceGeneration {
            observed: observed_generation,
            incoming: incoming_generation,
        });
    }

    if incoming_generation == observed_generation {
        if let (Some(observed), Some(incoming)) =
            (observed_backend_generation, incoming_backend_generation)
        {
            if incoming < observed {
                return Some(StaleWakeObservation::StaleBackendGeneration { observed, incoming });
            }
        }
    }

    None
}

fn stale_route_update(
    observed_instance_id: &InstanceId,
    observed_generation: Generation,
    observed_backend_generation: Option<BackendGeneration>,
    incoming_instance_id: &InstanceId,
    incoming_generation: Generation,
    incoming_backend_generation: Option<BackendGeneration>,
) -> Option<StaleWakeObservation> {
    if observed_instance_id != incoming_instance_id {
        return None;
    }

    stale_observation(
        observed_instance_id,
        observed_generation,
        observed_backend_generation,
        incoming_instance_id,
        incoming_generation,
        incoming_backend_generation,
    )
}

#[cfg(test)]
mod tests;
