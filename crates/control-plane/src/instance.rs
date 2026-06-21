use std::collections::BTreeMap;

use crate::{
    ids::{Generation, IdempotencyKey, InstanceId},
    route::{RouteBindingRecord, RouteBindingSpec},
    workload::{ValueSchemaError, WorkloadClassVersion, WorkloadClassVersionRef},
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
pub struct GetInstanceRequest {
    pub instance_id: InstanceId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteInstanceRequest {
    pub instance_id: InstanceId,
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

    pub fn validate_values_against(
        mut self,
        workload_class: &WorkloadClassVersion,
    ) -> Result<Self, ValueSchemaError> {
        self.values = workload_class.value_schema.validate_values(&self.values)?;
        Ok(self)
    }
}

impl GetInstanceRequest {
    pub fn new(instance_id: InstanceId) -> Self {
        Self { instance_id }
    }
}

impl DeleteInstanceRequest {
    pub fn new(instance_id: InstanceId) -> Self {
        Self { instance_id }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        ids::{Generation, IdempotencyKey, InstanceId, WorkloadClassId},
        workload::{
            WorkloadClassVersion, WorkloadClassVersionRef, WorkloadValueFieldRule,
            WorkloadValueSchema,
        },
    };

    use super::{CreateInstanceRequest, InstanceValues};

    #[test]
    fn create_instance_validation_applies_schema_defaults() {
        let request = create_request().with_values(values([("tenant", "acme")]));
        let validated = request
            .validate_values_against(&workload_class(
                WorkloadValueSchema::new(false)
                    .with_field("tenant", WorkloadValueFieldRule::required())
                    .with_field(
                        "image",
                        WorkloadValueFieldRule::optional_with_default("example/app:1"),
                    ),
            ))
            .expect("defaulted values are valid");

        assert_eq!(
            validated.values,
            values([("image", "example/app:1"), ("tenant", "acme")])
        );
    }

    #[test]
    fn create_instance_validation_rejects_missing_required_values() {
        let error = create_request()
            .validate_values_against(&workload_class(
                WorkloadValueSchema::new(false)
                    .with_field("tenant", WorkloadValueFieldRule::required()),
            ))
            .expect_err("missing required value is rejected");

        assert_eq!(
            error.to_string(),
            "missing required instance value \"tenant\""
        );
    }

    #[test]
    fn create_instance_validation_rejects_unknown_values_when_extra_is_disallowed() {
        let error = create_request()
            .with_values(values([("extra", "value")]))
            .validate_values_against(&workload_class(WorkloadValueSchema::new(false)))
            .expect_err("unknown value is rejected");

        assert_eq!(error.to_string(), "unknown instance value \"extra\"");
    }

    #[test]
    fn create_instance_validation_allows_unknown_values_when_extra_is_allowed() {
        let validated = create_request()
            .with_values(values([("tenant", "acme")]))
            .validate_values_against(&workload_class(WorkloadValueSchema::new(true).with_field(
                "image",
                WorkloadValueFieldRule::optional_with_default("example/app:1"),
            )))
            .expect("unknown value is allowed");

        assert_eq!(
            validated.values,
            values([("image", "example/app:1"), ("tenant", "acme")])
        );
    }

    fn create_request() -> CreateInstanceRequest {
        CreateInstanceRequest::new(
            IdempotencyKey::new("key-1").expect("valid key"),
            InstanceId::new("instance-1").expect("valid instance ID"),
            WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-1").expect("valid class ID"),
                Generation::new(1),
            ),
        )
    }

    fn workload_class(value_schema: WorkloadValueSchema) -> WorkloadClassVersion {
        WorkloadClassVersion {
            reference: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-1").expect("valid class ID"),
                Generation::new(1),
            ),
            template_generation: Generation::new(1),
            default_values: BTreeMap::new(),
            value_schema,
        }
    }

    fn values<const N: usize>(pairs: [(&str, &str); N]) -> InstanceValues {
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }
}
