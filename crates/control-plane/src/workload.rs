use crate::{
    ids::{Generation, WorkloadClassId},
    instance::InstanceValues,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadClassVersionRef {
    pub class_id: WorkloadClassId,
    pub version: Generation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadWorkloadClassVersionRequest {
    pub reference: WorkloadClassVersionRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadClassVersion {
    pub reference: WorkloadClassVersionRef,
    pub template_generation: Generation,
    pub default_values: InstanceValues,
}

impl WorkloadClassVersionRef {
    pub fn new(class_id: WorkloadClassId, version: Generation) -> Self {
        Self { class_id, version }
    }
}

impl LoadWorkloadClassVersionRequest {
    pub fn new(reference: WorkloadClassVersionRef) -> Self {
        Self { reference }
    }
}
