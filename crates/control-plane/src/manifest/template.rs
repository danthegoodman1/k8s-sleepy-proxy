use std::collections::BTreeMap;

use crate::instance::InstanceValues;

use super::ManifestRenderError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateText {
    parts: Vec<TemplateTextPart>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TemplateTextPart {
    Literal(String),
    InstanceValue(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestTemplate {
    pub workload: WorkloadTemplate,
    pub sidecar: SidecarTemplate,
    pub service: Option<ServiceTemplate>,
    pub volumes: Vec<VolumeTemplate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadTemplate {
    pub kind: WorkloadKind,
    pub name: TemplateText,
    pub replicas: Option<u32>,
    pub app_container: ContainerTemplate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerTemplate {
    pub name: String,
    pub image: TemplateText,
    pub ports: Vec<ContainerPortTemplate>,
    pub env: Vec<EnvVarTemplate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerPortTemplate {
    pub name: Option<String>,
    pub container_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvVarTemplate {
    pub name: String,
    pub value: TemplateText,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SidecarTemplate {
    pub name: String,
    pub image: TemplateText,
    pub listen_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceTemplate {
    pub name: TemplateText,
    pub ports: Vec<ServicePortTemplate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServicePortTemplate {
    pub name: Option<String>,
    pub port: u16,
    pub target_port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeTemplate {
    pub name: String,
    pub mount_path: TemplateText,
    pub pv_name: TemplateText,
    pub pvc_name: TemplateText,
    pub access_modes: Vec<PersistentVolumeAccessMode>,
    pub capacity: TemplateText,
    pub reclaim_policy: PersistentVolumeReclaimPolicy,
    pub storage_class_name: Option<TemplateText>,
    pub source: PersistentVolumeSourceTemplate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistentVolumeAccessMode {
    ReadWriteOnce,
    ReadOnlyMany,
    ReadWriteMany,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistentVolumeReclaimPolicy {
    Retain,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PersistentVolumeSourceTemplate {
    Csi {
        driver: TemplateText,
        volume_handle: TemplateText,
        fs_type: Option<TemplateText>,
        read_only: bool,
        volume_attributes: BTreeMap<String, TemplateText>,
    },
    HostPath {
        path: TemplateText,
        type_: Option<TemplateText>,
    },
}

impl TemplateText {
    pub fn literal(value: impl Into<String>) -> Self {
        Self {
            parts: vec![TemplateTextPart::Literal(value.into())],
        }
    }

    pub fn instance_value(field: impl Into<String>) -> Self {
        Self {
            parts: vec![TemplateTextPart::InstanceValue(field.into())],
        }
    }

    pub fn from_parts(parts: impl Into<Vec<TemplateTextPart>>) -> Self {
        Self {
            parts: parts.into(),
        }
    }

    pub fn render(&self, values: &InstanceValues) -> Result<String, ManifestRenderError> {
        let mut rendered = String::new();
        for part in &self.parts {
            match part {
                TemplateTextPart::Literal(value) => rendered.push_str(value),
                TemplateTextPart::InstanceValue(field) => {
                    let value = values.get(field).ok_or_else(|| {
                        ManifestRenderError::MissingInstanceValue {
                            field: field.clone(),
                        }
                    })?;
                    rendered.push_str(value);
                }
            }
        }
        Ok(rendered)
    }
}

impl TemplateTextPart {
    pub fn literal(value: impl Into<String>) -> Self {
        Self::Literal(value.into())
    }

    pub fn instance_value(field: impl Into<String>) -> Self {
        Self::InstanceValue(field.into())
    }
}
