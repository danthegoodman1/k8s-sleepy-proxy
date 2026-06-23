use std::collections::BTreeMap;

use crate::ids::Generation;

use super::{PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedManifest {
    pub instance_generation: Generation,
    pub template_generation: Option<Generation>,
    pub objects: Vec<RenderedManifestObject>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedManifestObject {
    pub apply_order: ApplyOrder,
    pub object: KubernetesObject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ApplyOrder {
    PersistentVolume,
    PersistentVolumeClaim,
    Service,
    Workload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KubernetesObject {
    Deployment(Deployment),
    StatefulSet(StatefulSet),
    Service(Service),
    PersistentVolume(PersistentVolume),
    PersistentVolumeClaim(PersistentVolumeClaim),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub name: String,
    pub namespace: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Deployment {
    pub metadata: ObjectMeta,
    pub spec: DeploymentSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeploymentSpec {
    pub replicas: u32,
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatefulSet {
    pub metadata: ObjectMeta,
    pub spec: StatefulSetSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatefulSetSpec {
    pub replicas: u32,
    pub service_name: String,
    pub selector: LabelSelector,
    pub template: PodTemplateSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelSelector {
    pub match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodTemplateSpec {
    pub metadata: PodTemplateMetadata,
    pub spec: PodSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodTemplateMetadata {
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodSpec {
    pub containers: Vec<Container>,
    pub volumes: Vec<PodVolume>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Container {
    pub name: String,
    pub image: String,
    pub ports: Vec<ContainerPort>,
    pub env: Vec<EnvVar>,
    pub volume_mounts: Vec<VolumeMount>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerPort {
    pub name: Option<String>,
    pub container_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodVolume {
    pub name: String,
    pub persistent_volume_claim: PersistentVolumeClaimVolumeSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeClaimVolumeSource {
    pub claim_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    pub metadata: ObjectMeta,
    pub spec: ServiceSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSpec {
    pub selector: BTreeMap<String, String>,
    pub ports: Vec<ServicePort>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServicePort {
    pub name: Option<String>,
    pub port: u16,
    pub target_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolume {
    pub metadata: ObjectMeta,
    pub spec: PersistentVolumeSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeSpec {
    pub capacity: String,
    pub access_modes: Vec<PersistentVolumeAccessMode>,
    pub persistent_volume_reclaim_policy: PersistentVolumeReclaimPolicy,
    pub storage_class_name: Option<String>,
    pub claim_ref: PersistentVolumeClaimRef,
    pub source: PersistentVolumeSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeClaimRef {
    pub namespace: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PersistentVolumeSource {
    Csi(CsiPersistentVolumeSource),
    HostPath(HostPathPersistentVolumeSource),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsiPersistentVolumeSource {
    pub driver: String,
    pub volume_handle: String,
    pub fs_type: Option<String>,
    pub read_only: bool,
    pub volume_attributes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPathPersistentVolumeSource {
    pub path: String,
    pub type_: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeClaim {
    pub metadata: ObjectMeta,
    pub spec: PersistentVolumeClaimSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentVolumeClaimSpec {
    pub access_modes: Vec<PersistentVolumeAccessMode>,
    pub resources: VolumeResourceRequirements,
    pub storage_class_name: Option<String>,
    pub volume_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeResourceRequirements {
    pub requests_storage: String,
}

impl KubernetesObject {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Deployment(_) => "Deployment",
            Self::StatefulSet(_) => "StatefulSet",
            Self::Service(_) => "Service",
            Self::PersistentVolume(_) => "PersistentVolume",
            Self::PersistentVolumeClaim(_) => "PersistentVolumeClaim",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Deployment(object) => &object.metadata.name,
            Self::StatefulSet(object) => &object.metadata.name,
            Self::Service(object) => &object.metadata.name,
            Self::PersistentVolume(object) => &object.metadata.name,
            Self::PersistentVolumeClaim(object) => &object.metadata.name,
        }
    }
}
