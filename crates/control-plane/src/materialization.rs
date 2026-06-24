use std::{
    error::Error,
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::ids::{
    BackendGeneration, EmptyStringError, Generation, InstanceId, MaterializationId, NonEmptyString,
};
use crate::instance::InstanceRecord;
use crate::workload::RenderedExclusivityKey;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationRecord {
    pub id: MaterializationId,
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    pub target: MaterializationTarget,
    pub state: MaterializationState,
    pub backend: Option<BackendEndpoint>,
    pub backend_generation: BackendGeneration,
    pub rendered_objects: Vec<RenderedObjectRef>,
    pub exclusivity_keys: Vec<RenderedExclusivityKey>,
    pub reconciliation_lease: Option<MaterializationReconciliationLease>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordMaterializationRequest {
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    pub target: MaterializationTarget,
    pub state: MaterializationState,
    pub backend: Option<BackendEndpoint>,
    pub backend_generation: BackendGeneration,
    pub rendered_objects: Vec<RenderedObjectRef>,
    pub exclusivity_keys: Vec<RenderedExclusivityKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteWakeRequest {
    pub instance_id: InstanceId,
    pub expected_waking_generation: Generation,
    pub target: MaterializationTarget,
    pub backend: BackendEndpoint,
    pub backend_generation: BackendGeneration,
    pub rendered_objects: Vec<RenderedObjectRef>,
    pub exclusivity_keys: Vec<RenderedExclusivityKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadReadyMaterializationRequest {
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    pub target: MaterializationTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadActiveMaterializationRequest {
    pub instance_id: InstanceId,
    pub target: MaterializationTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadMaterializationRequest {
    pub materialization_id: MaterializationId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginSleepRequest {
    pub instance_id: InstanceId,
    pub expected_running_generation: Generation,
    pub target: MaterializationTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeSleepRequest {
    pub instance_id: InstanceId,
    pub expected_draining_generation: Generation,
    pub target: MaterializationTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationReconciliationLease {
    pub owner: String,
    pub expires_at: SystemTime,
    pub attempt: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListMaterializationReconciliationCandidatesRequest {
    pub now: SystemTime,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadMaterializationOperationalMetricsRequest {
    pub now: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationOperationalMetrics {
    pub backlog_states: Vec<MaterializationBacklogOperationalMetrics>,
    pub held_key_states: Vec<MaterializationHeldKeysOperationalMetrics>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationBacklogOperationalMetrics {
    pub state: MaterializationState,
    pub count: u64,
    pub oldest_age: Option<std::time::Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationHeldKeysOperationalMetrics {
    pub state: MaterializationState,
    pub exclusivity_keys_held: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimMaterializationReconciliationRequest {
    pub materialization_id: MaterializationId,
    pub owner: String,
    pub now: SystemTime,
    pub lease_expires_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewMaterializationReconciliationLeaseRequest {
    pub materialization_id: MaterializationId,
    pub owner: String,
    pub lease_expires_at: SystemTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseMaterializationReconciliationLeaseRequest {
    pub materialization_id: MaterializationId,
    pub owner: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteWakeReconciliationRequest {
    pub materialization_id: MaterializationId,
    pub lease_owner: String,
    pub complete: CompleteWakeRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeSleepReconciliationRequest {
    pub materialization_id: MaterializationId,
    pub lease_owner: String,
    pub finalize: FinalizeSleepRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteMaterializationReconciliationRequest {
    pub materialization_id: MaterializationId,
    pub lease_owner: String,
    pub expected_state: MaterializationState,
    pub instance_id: InstanceId,
    pub instance_generation: Generation,
    pub target: MaterializationTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForceDeleteMaterializationRequest {
    pub materialization_id: MaterializationId,
    pub operator: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForceReleaseExclusivityKeyRequest {
    pub target: MaterializationTarget,
    pub key_name: String,
    pub key_value: String,
    pub operator: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForceReleaseExclusivityKeyResult {
    pub updated_materializations: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteWakeResult {
    pub instance: InstanceRecord,
    pub materialization: MaterializationRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginSleepResult {
    pub instance: InstanceRecord,
    pub materialization: Option<MaterializationRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeSleepResult {
    pub instance: InstanceRecord,
    pub materialization: Option<MaterializationRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationTarget {
    cluster_id: String,
    namespace: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MaterializationState {
    Pending,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendEndpoint {
    uri: NonEmptyString,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedObjectRef {
    pub api_version: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidMaterializationTarget {
    field: &'static str,
}

impl RecordMaterializationRequest {
    pub fn new(
        instance_id: InstanceId,
        instance_generation: Generation,
        target: MaterializationTarget,
        state: MaterializationState,
        backend_generation: BackendGeneration,
    ) -> Self {
        Self {
            instance_id,
            instance_generation,
            target,
            state,
            backend: None,
            backend_generation,
            rendered_objects: Vec::new(),
            exclusivity_keys: Vec::new(),
        }
    }
}

impl CompleteWakeRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_waking_generation: Generation,
        target: MaterializationTarget,
        backend: BackendEndpoint,
        backend_generation: BackendGeneration,
    ) -> Self {
        Self {
            instance_id,
            expected_waking_generation,
            target,
            backend,
            backend_generation,
            rendered_objects: Vec::new(),
            exclusivity_keys: Vec::new(),
        }
    }
}

impl LoadReadyMaterializationRequest {
    pub fn new(
        instance_id: InstanceId,
        instance_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            instance_id,
            instance_generation,
            target,
        }
    }
}

impl LoadActiveMaterializationRequest {
    pub fn new(instance_id: InstanceId, target: MaterializationTarget) -> Self {
        Self {
            instance_id,
            target,
        }
    }
}

impl LoadMaterializationRequest {
    pub fn new(materialization_id: MaterializationId) -> Self {
        Self { materialization_id }
    }
}

impl BeginSleepRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_running_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            instance_id,
            expected_running_generation,
            target,
        }
    }
}

impl FinalizeSleepRequest {
    pub fn new(
        instance_id: InstanceId,
        expected_draining_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            instance_id,
            expected_draining_generation,
            target,
        }
    }
}

impl ListMaterializationReconciliationCandidatesRequest {
    pub fn new(now: SystemTime, limit: usize) -> Self {
        Self { now, limit }
    }
}

impl LoadMaterializationOperationalMetricsRequest {
    pub fn new(now: SystemTime) -> Self {
        Self { now }
    }
}

impl MaterializationOperationalMetrics {
    pub fn new(
        backlog_states: Vec<MaterializationBacklogOperationalMetrics>,
        held_key_states: Vec<MaterializationHeldKeysOperationalMetrics>,
    ) -> Self {
        Self {
            backlog_states,
            held_key_states,
        }
    }
}

impl MaterializationBacklogOperationalMetrics {
    pub fn new(
        state: MaterializationState,
        count: u64,
        oldest_age: Option<std::time::Duration>,
    ) -> Self {
        Self {
            state,
            count,
            oldest_age,
        }
    }
}

impl MaterializationHeldKeysOperationalMetrics {
    pub fn new(state: MaterializationState, exclusivity_keys_held: u64) -> Self {
        Self {
            state,
            exclusivity_keys_held,
        }
    }
}

impl ClaimMaterializationReconciliationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        owner: impl Into<String>,
        now: SystemTime,
        lease_expires_at: SystemTime,
    ) -> Self {
        Self {
            materialization_id,
            owner: owner.into(),
            now,
            lease_expires_at,
        }
    }
}

impl RenewMaterializationReconciliationLeaseRequest {
    pub fn new(
        materialization_id: MaterializationId,
        owner: impl Into<String>,
        lease_expires_at: SystemTime,
    ) -> Self {
        Self {
            materialization_id,
            owner: owner.into(),
            lease_expires_at,
        }
    }
}

impl ReleaseMaterializationReconciliationLeaseRequest {
    pub fn new(materialization_id: MaterializationId, owner: impl Into<String>) -> Self {
        Self {
            materialization_id,
            owner: owner.into(),
        }
    }
}

impl CompleteWakeReconciliationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        lease_owner: impl Into<String>,
        complete: CompleteWakeRequest,
    ) -> Self {
        Self {
            materialization_id,
            lease_owner: lease_owner.into(),
            complete,
        }
    }
}

impl FinalizeSleepReconciliationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        lease_owner: impl Into<String>,
        finalize: FinalizeSleepRequest,
    ) -> Self {
        Self {
            materialization_id,
            lease_owner: lease_owner.into(),
            finalize,
        }
    }
}

impl DeleteMaterializationReconciliationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        lease_owner: impl Into<String>,
        expected_state: MaterializationState,
        instance_id: InstanceId,
        instance_generation: Generation,
        target: MaterializationTarget,
    ) -> Self {
        Self {
            materialization_id,
            lease_owner: lease_owner.into(),
            expected_state,
            instance_id,
            instance_generation,
            target,
        }
    }
}

impl ForceDeleteMaterializationRequest {
    pub fn new(
        materialization_id: MaterializationId,
        operator: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            materialization_id,
            operator: operator.into(),
            reason: reason.into(),
        }
    }
}

impl ForceReleaseExclusivityKeyRequest {
    pub fn new(
        target: MaterializationTarget,
        key_name: impl Into<String>,
        key_value: impl Into<String>,
        operator: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            target,
            key_name: key_name.into(),
            key_value: key_value.into(),
            operator: operator.into(),
            reason: reason.into(),
        }
    }
}

impl MaterializationTarget {
    pub fn new(
        cluster_id: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, InvalidMaterializationTarget> {
        let cluster_id = cluster_id.into();
        if cluster_id.trim().is_empty() {
            return Err(InvalidMaterializationTarget {
                field: "cluster_id",
            });
        }

        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(InvalidMaterializationTarget { field: "namespace" });
        }

        Ok(Self {
            cluster_id,
            namespace,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl MaterializationState {
    pub const BACKLOG_STATES: &'static [Self] = &[Self::Pending, Self::Deleting];
    pub const HELD_KEY_STATES: &'static [Self] =
        &[Self::Pending, Self::Ready, Self::Failed, Self::Deleting];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
        }
    }

    pub const fn metric_label(self) -> proxy_core::observability::metrics::MetricLabel {
        proxy_core::observability::metrics::MetricLabel::state(self.as_str())
    }
}

impl BackendEndpoint {
    pub fn new(uri: impl Into<String>) -> Result<Self, EmptyStringError> {
        Ok(Self {
            uri: NonEmptyString::new("backend.uri", uri)?,
        })
    }

    pub fn uri(&self) -> &str {
        self.uri.as_str()
    }
}

impl InvalidMaterializationTarget {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for InvalidMaterializationTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "materialization target {} must not be empty", self.field)
    }
}

impl Error for InvalidMaterializationTarget {}

pub(crate) fn unix_millis_from_system_time(value: SystemTime) -> Result<i64, String> {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| "system time does not fit in unix millis".to_owned()),
        Err(error) => {
            let millis = i64::try_from(error.duration().as_millis())
                .map_err(|_| "system time does not fit in unix millis".to_owned())?;
            Ok(-millis)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BackendEndpoint, MaterializationTarget};

    #[test]
    fn target_requires_cluster_and_namespace() {
        let error = MaterializationTarget::new("cluster-a", "").expect_err("namespace required");

        assert_eq!(error.field(), "namespace");
    }

    #[test]
    fn target_exposes_validated_fields_by_accessor() {
        let target = MaterializationTarget::new("cluster-a", "default").expect("valid target");

        assert_eq!(target.cluster_id(), "cluster-a");
        assert_eq!(target.namespace(), "default");
    }

    #[test]
    fn backend_endpoint_requires_uri() {
        let error = BackendEndpoint::new(" ").expect_err("backend URI required");

        assert_eq!(error.field(), "backend.uri");
    }
}
