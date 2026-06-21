use std::collections::BTreeMap;

use crate::{
    ids::{Generation, IdempotencyKey, InstanceId},
    route::{RouteBindingRecord, RouteBindingSpec},
    workload::WorkloadClassVersionRef,
};

pub type InstanceValues = BTreeMap<String, String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceRecord {
    pub id: InstanceId,
    pub workload_class: WorkloadClassVersionRef,
    pub values: InstanceValues,
    pub state: InstanceState,
    pub generation: Generation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InstanceState {
    Cold,
    Waking,
    Running,
    Draining,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateInstanceRequest {
    pub idempotency_key: IdempotencyKey,
    pub instance_id: InstanceId,
    pub workload_class: WorkloadClassVersionRef,
    pub values: InstanceValues,
    pub route_bindings: Vec<RouteBindingSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateInstanceResult {
    pub instance: InstanceRecord,
    pub route_bindings: Vec<RouteBindingRecord>,
    pub idempotency_replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompareAndSwapInstanceStateRequest {
    pub instance_id: InstanceId,
    pub expected_generation: Generation,
    pub next_state: InstanceState,
    pub reason: StateTransitionReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateTransitionReason {
    OperatorRequested,
    WakeRequested,
    MaterializationReady,
    SleepRequested,
    IdleReported,
    FailureReported(String),
    DeleteRequested,
}

impl CreateInstanceRequest {
    pub fn new(
        idempotency_key: IdempotencyKey,
        instance_id: InstanceId,
        workload_class: WorkloadClassVersionRef,
    ) -> Self {
        Self {
            idempotency_key,
            instance_id,
            workload_class,
            values: InstanceValues::new(),
            route_bindings: Vec::new(),
        }
    }

    pub fn with_values(mut self, values: InstanceValues) -> Self {
        self.values = values;
        self
    }

    pub fn with_route_bindings(mut self, route_bindings: Vec<RouteBindingSpec>) -> Self {
        self.route_bindings = route_bindings;
        self
    }
}

impl CompareAndSwapInstanceStateRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_generation: Generation,
        next_state: InstanceState,
        reason: StateTransitionReason,
    ) -> Self {
        Self {
            instance_id,
            expected_generation,
            next_state,
            reason,
        }
    }
}
