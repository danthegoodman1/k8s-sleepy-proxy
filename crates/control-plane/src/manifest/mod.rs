use std::{error::Error, fmt};

use crate::instance::InstanceRecord;

mod objects;
mod render;
mod serialize;
mod template;

#[cfg(test)]
mod tests;

pub use objects::{
    ApplyOrder, Container, ContainerPort, CsiPersistentVolumeSource, Deployment, DeploymentSpec,
    EnvVar, KubernetesObject, LabelSelector, ObjectMeta, PersistentVolume, PersistentVolumeClaim,
    PersistentVolumeClaimRef, PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource,
    PersistentVolumeSource, PersistentVolumeSpec, PodSpec, PodTemplateMetadata, PodTemplateSpec,
    PodVolume, RenderedManifest, RenderedManifestObject, Service, ServicePort, ServiceSpec,
    StatefulSet, StatefulSetSpec, VolumeMount, VolumeResourceRequirements,
};
pub use render::render_manifests;
pub use template::{
    ContainerPortTemplate, ContainerTemplate, EnvVarTemplate, ManifestTemplate,
    PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate,
    ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart,
    VolumeTemplate, WorkloadKind, WorkloadTemplate,
};

const LABEL_INSTANCE_ID: &str = "sleepypods.io/instance-id";
const LABEL_INSTANCE_GENERATION: &str = "sleepypods.io/instance-generation";
const LABEL_WORKLOAD_CLASS_ID: &str = "sleepypods.io/workload-class-id";
const LABEL_WORKLOAD_CLASS_VERSION: &str = "sleepypods.io/workload-class-version";
const LABEL_WORKLOAD_NAME: &str = "sleepypods.io/workload-name";
const ANNOTATION_TEMPLATE_GENERATION: &str = "sleepypods.io/template-generation";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderManifestRequest<'a> {
    pub template: &'a ManifestTemplate,
    pub instance: &'a InstanceRecord,
    pub namespace: &'a str,
    pub template_generation: Option<crate::ids::Generation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestRenderError {
    MissingInstanceValue {
        field: String,
    },
    InvalidName {
        field: &'static str,
        value: String,
    },
    InvalidField {
        field: &'static str,
        message: String,
    },
    InvalidReplicas {
        kind: WorkloadKind,
        replicas: u32,
        message: String,
    },
}

impl fmt::Display for ManifestRenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingInstanceValue { field } => {
                write!(f, "missing instance value {field:?}")
            }
            Self::InvalidName { field, value } => {
                write!(f, "{field} rendered invalid Kubernetes name {value:?}")
            }
            Self::InvalidField { field, message } => write!(f, "{field} is invalid: {message}"),
            Self::InvalidReplicas {
                kind,
                replicas,
                message,
            } => write!(f, "{kind:?} replicas {replicas} are invalid: {message}"),
        }
    }
}

impl Error for ManifestRenderError {}
