use std::{error::Error, fmt};

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
