use sleepypods_api::{
    BackendEndpoint, BackendGeneration, Generation, InstanceId, InstanceState, RouteBindingId,
    RouteEntry,
};

use super::{
    route_wake_decision, validate_route_update, validate_wake_response, ReadyBackend,
    RouteUpdateDisposition, RouteWakeDecision, StaleWakeObservation, WakeAdmission,
    WakeInstanceRequest, WakeInstanceResponse, WakeReason, WakeResponseDisposition, WakeTracker,
    WakeUnavailable, WakeUnavailableReason, WakeWait, WakeWaitReason,
};

fn route(state: InstanceState, generation: u64, backend_generation: Option<u64>) -> RouteEntry {
    route_for_instance("instance-a", state, generation, backend_generation)
}

fn route_for_instance(
    instance_id: &str,
    state: InstanceState,
    generation: u64,
    backend_generation: Option<u64>,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: RouteBindingId::new("route-a").expect("route binding ID"),
        instance_id: InstanceId::new(instance_id).expect("instance ID"),
        instance_state: state,
        instance_generation: Generation::new(generation),
        backend: backend_generation.map(backend),
        backend_generation: backend_generation.map(BackendGeneration::new),
    }
}

fn backend(generation: u64) -> BackendEndpoint {
    BackendEndpoint::new(format!("http://10.0.0.{generation}:8080")).expect("backend")
}

fn wake_request(generation: u64) -> WakeInstanceRequest {
    WakeInstanceRequest {
        instance_id: InstanceId::new("instance-a").expect("instance ID"),
        expected_generation: Generation::new(generation),
    }
}

#[test]
fn running_with_backend_is_ready() {
    assert_eq!(
        route_wake_decision(&route(InstanceState::Running, 7, Some(3))),
        RouteWakeDecision::Ready(ReadyBackend {
            instance_id: InstanceId::new("instance-a").expect("instance ID"),
            instance_generation: Generation::new(7),
            backend: backend(3),
            backend_generation: Some(BackendGeneration::new(3)),
        })
    );
}

#[test]
fn cold_waking_failed_and_draining_routes_are_wakeable() {
    for (state, reason) in [
        (InstanceState::Cold, WakeReason::Cold),
        (InstanceState::Waking, WakeReason::Waking),
        (InstanceState::Failed, WakeReason::Failed),
        (InstanceState::Draining, WakeReason::Draining),
    ] {
        assert_eq!(
            route_wake_decision(&route(state, 4, None)),
            RouteWakeDecision::Wake {
                request: wake_request(4),
                reason,
            }
        );
    }
}

#[test]
fn running_without_backend_is_not_ready_and_requests_reconcile_wake() {
    assert_eq!(
        route_wake_decision(&route(InstanceState::Running, 8, None)),
        RouteWakeDecision::Wake {
            request: wake_request(8),
            reason: WakeReason::RunningMissingBackend,
        }
    );
}

#[test]
fn deleting_and_deleted_routes_are_unavailable() {
    for (state, reason) in [
        (InstanceState::Deleting, WakeUnavailableReason::Deleting),
        (InstanceState::Deleted, WakeUnavailableReason::Deleted),
    ] {
        assert_eq!(
            route_wake_decision(&route(state, 9, None)),
            RouteWakeDecision::Unavailable(WakeUnavailable {
                instance_id: InstanceId::new("instance-a").expect("instance ID"),
                generation: Generation::new(9),
                reason,
            })
        );
    }
}

#[test]
fn duplicate_wake_requests_for_same_instance_and_generation_coalesce() {
    let mut tracker = WakeTracker::new();

    assert_eq!(
        tracker.admit(wake_request(10)),
        WakeAdmission::Start(wake_request(10))
    );
    assert_eq!(
        tracker.admit(wake_request(10)),
        WakeAdmission::Wait(WakeWait {
            instance_id: InstanceId::new("instance-a").expect("instance ID"),
            generation: Generation::new(10),
            reason: WakeWaitReason::DuplicatePendingWake,
        })
    );
    assert_eq!(tracker.pending_len(), 1);
}

#[test]
fn newer_generation_can_start_while_older_generation_is_pending() {
    let mut tracker = WakeTracker::new();

    assert_eq!(
        tracker.admit(wake_request(10)),
        WakeAdmission::Start(wake_request(10))
    );
    assert_eq!(
        tracker.admit(wake_request(11)),
        WakeAdmission::Start(wake_request(11))
    );
    assert_eq!(tracker.pending_len(), 2);
}

#[test]
fn completing_pending_wake_removes_only_matching_key() {
    let mut tracker = WakeTracker::new();
    tracker.admit(wake_request(10));
    tracker.admit(wake_request(11));
    let instance_id = InstanceId::new("instance-a").expect("instance ID");

    assert!(tracker.complete(&instance_id, Generation::new(10)));
    assert!(!tracker.is_pending(&instance_id, Generation::new(10)));
    assert!(tracker.is_pending(&instance_id, Generation::new(11)));
    assert_eq!(tracker.pending_len(), 1);
}

#[test]
fn stale_lower_instance_generation_completion_is_rejected() {
    let observed = route(InstanceState::Waking, 12, None);
    let response = WakeInstanceResponse::AlreadyRunning {
        instance_id: InstanceId::new("instance-a").expect("instance ID"),
        generation: Generation::new(11),
        backend: backend(5),
        backend_generation: Some(BackendGeneration::new(5)),
    };

    assert_eq!(
        validate_wake_response(&observed, response),
        WakeResponseDisposition::Rejected(StaleWakeObservation::StaleInstanceGeneration {
            observed: Generation::new(12),
            incoming: Generation::new(11),
        })
    );
}

#[test]
fn same_instance_generation_lower_backend_generation_completion_is_rejected() {
    let observed = route(InstanceState::Running, 12, Some(6));
    let response = WakeInstanceResponse::AlreadyRunning {
        instance_id: InstanceId::new("instance-a").expect("instance ID"),
        generation: Generation::new(12),
        backend: backend(5),
        backend_generation: Some(BackendGeneration::new(5)),
    };

    assert_eq!(
        validate_wake_response(&observed, response),
        WakeResponseDisposition::Rejected(StaleWakeObservation::StaleBackendGeneration {
            observed: BackendGeneration::new(6),
            incoming: BackendGeneration::new(5),
        })
    );
}

#[test]
fn stale_route_update_for_lower_generation_is_rejected() {
    let observed = route(InstanceState::Waking, 12, None);
    let incoming = route(InstanceState::Running, 11, Some(5));

    assert_eq!(
        validate_route_update(&observed, &incoming),
        RouteUpdateDisposition::Reject(StaleWakeObservation::StaleInstanceGeneration {
            observed: Generation::new(12),
            incoming: Generation::new(11),
        })
    );
}

#[test]
fn route_update_for_different_instance_starts_new_lineage() {
    let observed = route_for_instance("instance-a", InstanceState::Running, 12, Some(6));
    let incoming = route_for_instance("instance-b", InstanceState::Running, 1, Some(1));

    assert_eq!(
        validate_route_update(&observed, &incoming),
        RouteUpdateDisposition::Accept
    );
}

#[test]
fn wake_response_for_different_instance_is_rejected() {
    let observed = route_for_instance("instance-a", InstanceState::Waking, 12, None);
    let response = WakeInstanceResponse::WakeStarted {
        instance_id: InstanceId::new("instance-b").expect("instance ID"),
        generation: Generation::new(1),
    };

    assert_eq!(
        validate_wake_response(&observed, response),
        WakeResponseDisposition::Rejected(StaleWakeObservation::InstanceMismatch {
            observed: InstanceId::new("instance-a").expect("instance ID"),
            incoming: InstanceId::new("instance-b").expect("instance ID"),
        })
    );
}

#[test]
fn accepted_completion_can_produce_routable_backend() {
    let observed = route(InstanceState::Waking, 12, None);
    let response = WakeInstanceResponse::AlreadyRunning {
        instance_id: InstanceId::new("instance-a").expect("instance ID"),
        generation: Generation::new(12),
        backend: backend(7),
        backend_generation: Some(BackendGeneration::new(7)),
    };

    assert_eq!(
        validate_wake_response(&observed, response),
        WakeResponseDisposition::Ready(ReadyBackend {
            instance_id: InstanceId::new("instance-a").expect("instance ID"),
            instance_generation: Generation::new(12),
            backend: backend(7),
            backend_generation: Some(BackendGeneration::new(7)),
        })
    );
}
