use std::{collections::BTreeMap, error::Error, fmt};

use crate::{
    ids::{Generation, IdempotencyKey, InstanceId},
    materialization::{
        LoadActiveMaterializationRequest, MaterializationRecord, MaterializationState,
        MaterializationTarget,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient},
    projection::{ProjectionError, ProjectionPlan, ProjectionReconciler},
    route::{RouteBindingRecord, RouteBindingSpec},
    sleep_policy::SleepPolicyError,
    store::{ControlPlaneStore, StoreError},
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
    DrainCompleted,
    FailureReported(String),
    DeleteRequested,
    DeleteFinalized,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidStateTransition {
    pub current: InstanceState,
    pub next: InstanceState,
    pub reason: StateTransitionReason,
}

pub fn validate_instance_state_transition(
    current: InstanceState,
    next: InstanceState,
    reason: &StateTransitionReason,
) -> Result<(), InvalidStateTransition> {
    let valid = matches!(
        (current, next, reason),
        (
            InstanceState::Cold,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested
        ) | (
            InstanceState::Cold,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Waking,
            InstanceState::Running,
            StateTransitionReason::MaterializationReady
        ) | (
            InstanceState::Waking,
            InstanceState::Failed,
            StateTransitionReason::FailureReported(_)
        ) | (
            InstanceState::Waking,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Running,
            InstanceState::Draining,
            StateTransitionReason::SleepRequested | StateTransitionReason::IdleReported
        ) | (
            InstanceState::Running,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Draining,
            InstanceState::Cold,
            StateTransitionReason::DrainCompleted
        ) | (
            InstanceState::Draining,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested
        ) | (
            InstanceState::Draining,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Failed,
            InstanceState::Waking,
            StateTransitionReason::WakeRequested
        ) | (
            InstanceState::Failed,
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested
        ) | (
            InstanceState::Deleting,
            InstanceState::Deleted,
            StateTransitionReason::DeleteFinalized
        )
    );

    if valid {
        Ok(())
    } else {
        Err(InvalidStateTransition {
            current,
            next,
            reason: reason.clone(),
        })
    }
}

impl fmt::Display for InvalidStateTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid instance state transition: {:?} -> {:?} for {:?}",
            self.current, self.next, self.reason
        )
    }
}

impl Error for InvalidStateTransition {}

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
    ) -> Result<Self, CreateInstanceValidationError> {
        self.values = workload_class.value_schema.validate_values(&self.values)?;
        workload_class.sleep_policy.resolve(&self.values)?;
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateInstanceValidationError {
    ValueSchema(ValueSchemaError),
    SleepPolicy(SleepPolicyError),
}

#[derive(Debug)]
pub enum DeleteInstanceError {
    Projection {
        instance_id: InstanceId,
        source: ProjectionError,
    },
    Store(StoreError),
}

pub async fn delete_instance<S, C>(
    store: &S,
    materializer: &KubernetesMaterializer<C>,
    target: MaterializationTarget,
    request: DeleteInstanceRequest,
) -> Result<bool, DeleteInstanceError>
where
    S: ControlPlaneStore + ?Sized,
    C: KubernetesMaterializerClient,
{
    let Some(current) = store
        .get_instance(GetInstanceRequest::new(request.instance_id.clone()))
        .await
        .map_err(DeleteInstanceError::Store)?
    else {
        return Ok(false);
    };

    let deleting = if current.state == InstanceState::Deleting {
        current
    } else {
        store
            .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
                request.instance_id.clone(),
                current.generation,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ))
            .await
            .map_err(DeleteInstanceError::Store)?
    };

    let materialization = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            request.instance_id.clone(),
            target,
        ))
        .await
        .map_err(DeleteInstanceError::Store)?;

    if let Some(materialization) = materialization.as_ref() {
        let cleanup_materialization =
            projected_recorded_ref_materialization_for_cleanup(materialization);
        let plan = ProjectionPlan::from_recorded_refs(&cleanup_materialization);
        ProjectionReconciler::new(materializer)
            .delete_owned(&plan)
            .await
            .map_err(|source| DeleteInstanceError::Projection {
                instance_id: request.instance_id.clone(),
                source,
            })?;
    }

    store
        .delete_instance(DeleteInstanceRequest::new(deleting.id))
        .await
        .map_err(DeleteInstanceError::Store)
}

fn projected_recorded_ref_materialization_for_cleanup(
    materialization: &MaterializationRecord,
) -> MaterializationRecord {
    let mut projected = materialization.clone();
    if materialization.state == MaterializationState::Pending {
        projected.instance_generation = materialization.instance_generation.next();
    }
    projected
}

impl fmt::Display for CreateInstanceValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ValueSchema(error) => error.fmt(f),
            Self::SleepPolicy(error) => error.fmt(f),
        }
    }
}

impl Error for CreateInstanceValidationError {}

impl From<ValueSchemaError> for CreateInstanceValidationError {
    fn from(error: ValueSchemaError) -> Self {
        Self::ValueSchema(error)
    }
}

impl From<SleepPolicyError> for CreateInstanceValidationError {
    fn from(error: SleepPolicyError) -> Self {
        Self::SleepPolicy(error)
    }
}

impl fmt::Display for DeleteInstanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Projection {
                instance_id,
                source,
            } => write!(
                f,
                "Kubernetes projection cleanup failed for instance {}: {source}",
                instance_id.as_str()
            ),
            Self::Store(error) => error.fmt(f),
        }
    }
}

impl Error for DeleteInstanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Projection { source, .. } => Some(source),
            Self::Store(error) => Some(error),
        }
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
        manifest::{
            ContainerPortTemplate, ContainerTemplate, ManifestTemplate, ServicePortTemplate,
            ServiceTemplate, SidecarTemplate, TemplateText, WorkloadKind, WorkloadTemplate,
        },
        sleep_policy::{IdleTimeoutOverridePolicy, WorkloadSleepPolicy},
        workload::{
            WorkloadClassVersion, WorkloadClassVersionRef, WorkloadValueFieldRule,
            WorkloadValueSchema,
        },
    };

    use super::{
        validate_instance_state_transition, CreateInstanceRequest, InstanceState, InstanceValues,
        StateTransitionReason,
    };

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

    #[test]
    fn create_instance_validation_resolves_defaulted_sleep_policy_override() {
        let validated = create_request()
            .validate_values_against(&workload_class_with_sleep_policy(
                WorkloadValueSchema::new(false).with_field(
                    "idle_ms",
                    WorkloadValueFieldRule::optional_with_default("120000"),
                ),
                override_sleep_policy(),
            ))
            .expect("defaulted override is valid");

        assert_eq!(validated.values, values([("idle_ms", "120000")]));
    }

    #[test]
    fn create_instance_validation_rejects_malformed_sleep_policy_override() {
        let error = create_request()
            .with_values(values([("idle_ms", "two-minutes")]))
            .validate_values_against(&workload_class_with_sleep_policy(
                WorkloadValueSchema::new(false)
                    .with_field("idle_ms", WorkloadValueFieldRule::optional()),
                override_sleep_policy(),
            ))
            .expect_err("malformed override is rejected");

        assert_eq!(
            error.to_string(),
            "idle timeout override value \"idle_ms\"=\"two-minutes\" is not a decimal millisecond value"
        );
    }

    #[test]
    fn create_instance_validation_rejects_out_of_bounds_sleep_policy_override() {
        let error = create_request()
            .with_values(values([("idle_ms", "50000")]))
            .validate_values_against(&workload_class_with_sleep_policy(
                WorkloadValueSchema::new(false)
                    .with_field("idle_ms", WorkloadValueFieldRule::optional()),
                override_sleep_policy(),
            ))
            .expect_err("out-of-bounds override is rejected");

        assert_eq!(
            error.to_string(),
            "idle timeout override value \"idle_ms\"=50000 is outside allowed range 60000..=600000"
        );
    }

    #[test]
    fn instance_state_validator_accepts_lifecycle_edges() {
        for (current, next, reason) in [
            (
                InstanceState::Cold,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Cold,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Waking,
                InstanceState::Running,
                StateTransitionReason::MaterializationReady,
            ),
            (
                InstanceState::Waking,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Running,
                InstanceState::Draining,
                StateTransitionReason::SleepRequested,
            ),
            (
                InstanceState::Running,
                InstanceState::Draining,
                StateTransitionReason::IdleReported,
            ),
            (
                InstanceState::Running,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Draining,
                InstanceState::Cold,
                StateTransitionReason::DrainCompleted,
            ),
            (
                InstanceState::Draining,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Draining,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Failed,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Failed,
                InstanceState::Deleting,
                StateTransitionReason::DeleteRequested,
            ),
            (
                InstanceState::Deleting,
                InstanceState::Deleted,
                StateTransitionReason::DeleteFinalized,
            ),
        ] {
            validate_instance_state_transition(current, next, &reason)
                .expect("transition should be allowed");
        }

        validate_instance_state_transition(
            InstanceState::Waking,
            InstanceState::Failed,
            &StateTransitionReason::FailureReported("readiness timeout".to_owned()),
        )
        .expect("waking failures should be allowed");
    }

    #[test]
    fn instance_state_validator_rejects_nonsensical_edges() {
        for (current, next, reason) in [
            (
                InstanceState::Cold,
                InstanceState::Running,
                StateTransitionReason::MaterializationReady,
            ),
            (
                InstanceState::Running,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Failed,
                InstanceState::Running,
                StateTransitionReason::MaterializationReady,
            ),
            (
                InstanceState::Deleting,
                InstanceState::Running,
                StateTransitionReason::MaterializationReady,
            ),
            (
                InstanceState::Waking,
                InstanceState::Draining,
                StateTransitionReason::SleepRequested,
            ),
            (
                InstanceState::Deleted,
                InstanceState::Waking,
                StateTransitionReason::WakeRequested,
            ),
            (
                InstanceState::Deleting,
                InstanceState::Deleted,
                StateTransitionReason::DeleteRequested,
            ),
        ] {
            let error = validate_instance_state_transition(current, next, &reason)
                .expect_err("transition should be rejected");
            assert_eq!(error.current, current);
            assert_eq!(error.next, next);
            assert_eq!(error.reason, reason);
        }
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
        workload_class_with_sleep_policy(value_schema, default_sleep_policy())
    }

    fn workload_class_with_sleep_policy(
        value_schema: WorkloadValueSchema,
        sleep_policy: WorkloadSleepPolicy,
    ) -> WorkloadClassVersion {
        WorkloadClassVersion {
            reference: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-1").expect("valid class ID"),
                Generation::new(1),
            ),
            template_generation: Generation::new(1),
            template: test_manifest_template(),
            default_values: BTreeMap::new(),
            value_schema,
            sleep_policy,
            exclusivity_keys: vec![],
        }
    }

    fn default_sleep_policy() -> WorkloadSleepPolicy {
        WorkloadSleepPolicy::new(300_000, 5_000, 30_000).expect("valid sleep policy")
    }

    fn override_sleep_policy() -> WorkloadSleepPolicy {
        default_sleep_policy()
            .with_idle_timeout_override(
                IdleTimeoutOverridePolicy::new("idle_ms", 60_000, 600_000)
                    .expect("valid override policy"),
            )
            .expect("override attaches")
    }

    fn test_manifest_template() -> ManifestTemplate {
        ManifestTemplate {
            workload: WorkloadTemplate {
                kind: WorkloadKind::Deployment,
                name: TemplateText::literal("app"),
                replicas: None,
                app_container: ContainerTemplate {
                    name: "app".to_owned(),
                    image: TemplateText::literal("example/app:1"),
                    ports: vec![ContainerPortTemplate {
                        name: Some("http".to_owned()),
                        container_port: 8080,
                    }],
                    env: Vec::new(),
                },
            },
            sidecar: SidecarTemplate {
                name: "sleepypods-sidecar".to_owned(),
                image: TemplateText::literal("sleepypods/sidecar:test"),
                listen_port: 15000,
                mode: None,
            },
            service: Some(ServiceTemplate {
                name: TemplateText::literal("app"),
                ports: vec![ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            volumes: Vec::new(),
            raw_objects: Vec::new(),
        }
    }

    fn values<const N: usize>(pairs: [(&str, &str); N]) -> InstanceValues {
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }
}
