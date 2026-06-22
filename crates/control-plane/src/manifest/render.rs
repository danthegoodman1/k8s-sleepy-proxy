use std::collections::BTreeMap;

use crate::instance::InstanceRecord;

use super::{
    ApplyOrder, Container, ContainerPort, ContainerTemplate, CsiPersistentVolumeSource, Deployment,
    DeploymentSpec, EnvVar, HostPathPersistentVolumeSource, KubernetesObject, LabelSelector,
    ManifestRenderError, ManifestTemplate, ObjectMeta, PersistentVolume,
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimRef,
    PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource, PersistentVolumeReclaimPolicy,
    PersistentVolumeSource, PersistentVolumeSourceTemplate, PersistentVolumeSpec, PodSpec,
    PodTemplateMetadata, PodTemplateSpec, PodVolume, RenderManifestRequest, RenderedManifest,
    RenderedManifestObject, Service, ServicePort, ServiceSpec, ServiceTemplate, SidecarTemplate,
    StatefulSet, StatefulSetSpec, TemplateText, VolumeMount, VolumeResourceRequirements,
    VolumeTemplate, WorkloadKind, ANNOTATION_TEMPLATE_GENERATION, LABEL_INSTANCE_GENERATION,
    LABEL_INSTANCE_ID, LABEL_WORKLOAD_CLASS_ID, LABEL_WORKLOAD_CLASS_VERSION, LABEL_WORKLOAD_NAME,
};

const SIDECAR_PORT_NAME: &str = "sleepypods";
const ENV_LISTEN_PORT: &str = "SLEEPYPODS_LISTEN_PORT";
const ENV_APP_PORT: &str = "SLEEPYPODS_APP_PORT";
const ENV_INSTANCE_ID: &str = "SLEEPYPODS_INSTANCE_ID";
const ENV_INSTANCE_GENERATION: &str = "SLEEPYPODS_INSTANCE_GENERATION";

pub fn render_manifests(
    request: RenderManifestRequest<'_>,
) -> Result<RenderedManifest, ManifestRenderError> {
    validate_namespace(request.namespace)?;

    let workload_name = render_object_name(
        "workload.name",
        &request.template.workload.name,
        request.instance,
    )?;
    let service_name = request
        .template
        .service
        .as_ref()
        .map(|service| render_object_name("service.name", &service.name, request.instance))
        .transpose()?;
    let selector_labels = selector_labels(request.instance, &workload_name)?;
    let metadata_labels = metadata_labels(request.instance, &workload_name)?;
    let annotations = metadata_annotations(&request);
    let sidecar = render_sidecar_config(request.template, request.instance)?;

    let rendered_volumes = request
        .template
        .volumes
        .iter()
        .map(|volume| render_volume(volume, request.instance, request.namespace))
        .collect::<Result<Vec<_>, _>>()?;

    let mut objects = Vec::new();
    for rendered in &rendered_volumes {
        objects.push(RenderedManifestObject {
            apply_order: ApplyOrder::PersistentVolume,
            object: KubernetesObject::PersistentVolume(PersistentVolume {
                metadata: ObjectMeta {
                    name: rendered.pv_name.clone(),
                    namespace: None,
                    labels: metadata_labels.clone(),
                    annotations: annotations.clone(),
                },
                spec: PersistentVolumeSpec {
                    capacity: rendered.capacity.clone(),
                    access_modes: rendered.access_modes.clone(),
                    persistent_volume_reclaim_policy: rendered.reclaim_policy,
                    storage_class_name: rendered.storage_class_name.clone(),
                    claim_ref: PersistentVolumeClaimRef {
                        namespace: request.namespace.to_owned(),
                        name: rendered.pvc_name.clone(),
                    },
                    source: rendered.source.clone(),
                },
            }),
        });
    }
    for rendered in &rendered_volumes {
        objects.push(RenderedManifestObject {
            apply_order: ApplyOrder::PersistentVolumeClaim,
            object: KubernetesObject::PersistentVolumeClaim(PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: rendered.pvc_name.clone(),
                    namespace: Some(request.namespace.to_owned()),
                    labels: metadata_labels.clone(),
                    annotations: annotations.clone(),
                },
                spec: PersistentVolumeClaimSpec {
                    access_modes: rendered.access_modes.clone(),
                    resources: VolumeResourceRequirements {
                        requests_storage: rendered.capacity.clone(),
                    },
                    storage_class_name: rendered.storage_class_name.clone(),
                    volume_name: rendered.pv_name.clone(),
                },
            }),
        });
    }

    if let Some(service) = &request.template.service {
        objects.push(RenderedManifestObject {
            apply_order: ApplyOrder::Service,
            object: KubernetesObject::Service(Service {
                metadata: ObjectMeta {
                    name: service_name.clone().expect("service name was rendered"),
                    namespace: Some(request.namespace.to_owned()),
                    labels: metadata_labels.clone(),
                    annotations: annotations.clone(),
                },
                spec: ServiceSpec {
                    selector: selector_labels.clone(),
                    ports: render_service_ports(service, sidecar.listen_port)?,
                },
            }),
        });
    }

    let pod_template = PodTemplateSpec {
        metadata: PodTemplateMetadata {
            labels: metadata_labels.clone(),
            annotations: annotations.clone(),
        },
        spec: PodSpec {
            containers: vec![
                render_app_container(
                    &request.template.workload.app_container,
                    request.instance,
                    &rendered_volumes,
                )?,
                render_sidecar_container(&request.template.sidecar, request.instance, &sidecar)?,
            ],
            volumes: rendered_volumes
                .iter()
                .map(|volume| PodVolume {
                    name: volume.volume_name.clone(),
                    persistent_volume_claim: PersistentVolumeClaimVolumeSource {
                        claim_name: volume.pvc_name.clone(),
                    },
                })
                .collect(),
        },
    };

    let replicas = request.template.workload.replicas.unwrap_or(1);
    match request.template.workload.kind {
        WorkloadKind::Deployment => {
            objects.push(RenderedManifestObject {
                apply_order: ApplyOrder::Workload,
                object: KubernetesObject::Deployment(Deployment {
                    metadata: ObjectMeta {
                        name: workload_name,
                        namespace: Some(request.namespace.to_owned()),
                        labels: metadata_labels,
                        annotations,
                    },
                    spec: DeploymentSpec {
                        replicas,
                        selector: LabelSelector {
                            match_labels: selector_labels,
                        },
                        template: pod_template,
                    },
                }),
            });
        }
        WorkloadKind::StatefulSet => {
            if replicas > 1 {
                return Err(ManifestRenderError::InvalidReplicas {
                    kind: WorkloadKind::StatefulSet,
                    replicas,
                    message: "StatefulSet replicas above one are not supported in V1".to_owned(),
                });
            }
            let service_name = service_name.ok_or_else(|| ManifestRenderError::InvalidField {
                field: "stateful_set.service_name",
                message: "StatefulSet rendering requires a service template".to_owned(),
            })?;
            objects.push(RenderedManifestObject {
                apply_order: ApplyOrder::Workload,
                object: KubernetesObject::StatefulSet(StatefulSet {
                    metadata: ObjectMeta {
                        name: workload_name,
                        namespace: Some(request.namespace.to_owned()),
                        labels: metadata_labels,
                        annotations,
                    },
                    spec: StatefulSetSpec {
                        replicas,
                        service_name,
                        selector: LabelSelector {
                            match_labels: selector_labels,
                        },
                        template: pod_template,
                    },
                }),
            });
        }
    }

    Ok(RenderedManifest { objects })
}

struct RenderedVolume {
    volume_name: String,
    mount_path: String,
    pv_name: String,
    pvc_name: String,
    access_modes: Vec<PersistentVolumeAccessMode>,
    capacity: String,
    reclaim_policy: PersistentVolumeReclaimPolicy,
    storage_class_name: Option<String>,
    source: PersistentVolumeSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SidecarRenderConfig {
    listen_port: u16,
    app_port: u16,
}

fn render_sidecar_config(
    template: &ManifestTemplate,
    instance: &InstanceRecord,
) -> Result<SidecarRenderConfig, ManifestRenderError> {
    validate_port("sidecar.listen_port", template.sidecar.listen_port)?;

    let service = template
        .service
        .as_ref()
        .ok_or_else(|| ManifestRenderError::InvalidField {
            field: "service",
            message: "sidecar-routed workloads require a service template".to_owned(),
        })?;
    if service.ports.len() != 1 {
        return Err(ManifestRenderError::InvalidField {
            field: "service.ports",
            message: "exactly one service port is supported for sidecar-routed workloads"
                .to_owned(),
        });
    }

    let app_port = service.ports[0].target_port;
    validate_port("service.ports.target_port", app_port)?;
    if app_port == template.sidecar.listen_port {
        return Err(ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: "original app target port must differ from the sidecar listen port".to_owned(),
        });
    }
    let app_ports = &template.workload.app_container.ports;
    if app_ports.iter().all(|port| port.container_port != 0)
        && app_ports.iter().all(|port| port.container_port != app_port)
    {
        return Err(ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: format!("target port {app_port} must match an app container port"),
        });
    }
    render_non_empty("sidecar.image", &template.sidecar.image, instance)?;
    validate_dns_label("sidecar.name", &template.sidecar.name)?;

    Ok(SidecarRenderConfig {
        listen_port: template.sidecar.listen_port,
        app_port,
    })
}

fn render_volume(
    template: &VolumeTemplate,
    instance: &InstanceRecord,
    namespace: &str,
) -> Result<RenderedVolume, ManifestRenderError> {
    validate_dns_label("volume.name", &template.name)?;
    if template.access_modes.is_empty() {
        return Err(ManifestRenderError::InvalidField {
            field: "volume.access_modes",
            message: "at least one access mode is required".to_owned(),
        });
    }

    let mount_path = render_non_empty("volume.mount_path", &template.mount_path, instance)?;
    if !mount_path.starts_with('/') {
        return Err(ManifestRenderError::InvalidField {
            field: "volume.mount_path",
            message: format!("mount path {mount_path:?} must be absolute"),
        });
    }

    let pv_name = render_object_name("volume.pv_name", &template.pv_name, instance)?;
    let pvc_name = render_object_name("volume.pvc_name", &template.pvc_name, instance)?;
    let capacity = render_non_empty("volume.capacity", &template.capacity, instance)?;
    let storage_class_name = template
        .storage_class_name
        .as_ref()
        .map(|value| render_non_empty("volume.storage_class_name", value, instance))
        .transpose()?;

    let source = match &template.source {
        PersistentVolumeSourceTemplate::Csi {
            driver,
            volume_handle,
            fs_type,
            read_only,
            volume_attributes,
        } => {
            let rendered_attributes = volume_attributes
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        render_non_empty("volume.source.csi.volume_attributes", value, instance)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, ManifestRenderError>>()?;
            PersistentVolumeSource::Csi(CsiPersistentVolumeSource {
                driver: render_non_empty("volume.source.csi.driver", driver, instance)?,
                volume_handle: render_non_empty(
                    "volume.source.csi.volume_handle",
                    volume_handle,
                    instance,
                )?,
                fs_type: fs_type
                    .as_ref()
                    .map(|value| render_non_empty("volume.source.csi.fs_type", value, instance))
                    .transpose()?,
                read_only: *read_only,
                volume_attributes: rendered_attributes,
            })
        }
        PersistentVolumeSourceTemplate::HostPath { path, type_ } => {
            let path = render_non_empty("volume.source.host_path.path", path, instance)?;
            if !path.starts_with('/') {
                return Err(ManifestRenderError::InvalidField {
                    field: "volume.source.host_path.path",
                    message: format!("hostPath path {path:?} must be absolute"),
                });
            }
            PersistentVolumeSource::HostPath(HostPathPersistentVolumeSource {
                path,
                type_: type_
                    .as_ref()
                    .map(|value| render_non_empty("volume.source.host_path.type", value, instance))
                    .transpose()?,
            })
        }
    };

    validate_namespace(namespace)?;

    Ok(RenderedVolume {
        volume_name: template.name.clone(),
        mount_path,
        pv_name,
        pvc_name,
        access_modes: template.access_modes.clone(),
        capacity,
        reclaim_policy: template.reclaim_policy,
        storage_class_name,
        source,
    })
}

fn render_app_container(
    template: &ContainerTemplate,
    instance: &InstanceRecord,
    volumes: &[RenderedVolume],
) -> Result<Container, ManifestRenderError> {
    validate_dns_label("container.name", &template.name)?;
    Ok(Container {
        name: template.name.clone(),
        image: render_non_empty("container.image", &template.image, instance)?,
        ports: template
            .ports
            .iter()
            .map(|port| {
                validate_port("container.ports.container_port", port.container_port)?;
                Ok(ContainerPort {
                    name: port.name.clone(),
                    container_port: port.container_port,
                })
            })
            .collect::<Result<Vec<_>, ManifestRenderError>>()?,
        env: template
            .env
            .iter()
            .map(|env| {
                Ok(EnvVar {
                    name: env.name.clone(),
                    value: env.value.render(&instance.values)?,
                })
            })
            .collect::<Result<Vec<_>, ManifestRenderError>>()?,
        volume_mounts: volumes
            .iter()
            .map(|volume| VolumeMount {
                name: volume.volume_name.clone(),
                mount_path: volume.mount_path.clone(),
            })
            .collect(),
    })
}

fn render_sidecar_container(
    template: &SidecarTemplate,
    instance: &InstanceRecord,
    config: &SidecarRenderConfig,
) -> Result<Container, ManifestRenderError> {
    Ok(Container {
        name: template.name.clone(),
        image: render_non_empty("sidecar.image", &template.image, instance)?,
        ports: vec![ContainerPort {
            name: Some(SIDECAR_PORT_NAME.to_owned()),
            container_port: config.listen_port,
        }],
        env: vec![
            EnvVar {
                name: ENV_LISTEN_PORT.to_owned(),
                value: config.listen_port.to_string(),
            },
            EnvVar {
                name: ENV_APP_PORT.to_owned(),
                value: config.app_port.to_string(),
            },
            EnvVar {
                name: ENV_INSTANCE_ID.to_owned(),
                value: instance.id.to_string(),
            },
            EnvVar {
                name: ENV_INSTANCE_GENERATION.to_owned(),
                value: instance.generation.to_string(),
            },
        ],
        volume_mounts: Vec::new(),
    })
}

fn render_service_ports(
    template: &ServiceTemplate,
    sidecar_listen_port: u16,
) -> Result<Vec<ServicePort>, ManifestRenderError> {
    template
        .ports
        .iter()
        .map(|port| {
            validate_port("service.ports.port", port.port)?;
            validate_port("service.ports.target_port", port.target_port)?;
            Ok(ServicePort {
                name: port.name.clone(),
                port: port.port,
                target_port: sidecar_listen_port,
            })
        })
        .collect()
}

fn render_object_name(
    field: &'static str,
    template: &TemplateText,
    instance: &InstanceRecord,
) -> Result<String, ManifestRenderError> {
    let value = render_non_empty(field, template, instance)?;
    validate_dns_label(field, &value)?;
    Ok(value)
}

fn render_non_empty(
    field: &'static str,
    template: &TemplateText,
    instance: &InstanceRecord,
) -> Result<String, ManifestRenderError> {
    let value = template.render(&instance.values)?;
    if value.trim().is_empty() {
        return Err(ManifestRenderError::InvalidField {
            field,
            message: "rendered value must not be empty".to_owned(),
        });
    }
    Ok(value)
}

fn validate_namespace(namespace: &str) -> Result<(), ManifestRenderError> {
    validate_dns_label("namespace", namespace)
}

fn validate_port(field: &'static str, value: u16) -> Result<(), ManifestRenderError> {
    if value == 0 {
        Err(ManifestRenderError::InvalidField {
            field,
            message: "port must be between 1 and 65535".to_owned(),
        })
    } else {
        Ok(())
    }
}

fn validate_dns_label(field: &'static str, value: &str) -> Result<(), ManifestRenderError> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if valid {
        Ok(())
    } else {
        Err(ManifestRenderError::InvalidName {
            field,
            value: value.to_owned(),
        })
    }
}

fn validate_label_value(field: &'static str, value: &str) -> Result<(), ManifestRenderError> {
    let valid = value.len() <= 63
        && (value.is_empty()
            || (value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || byte == b'.'
            }) && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
                && value
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)));

    if valid {
        Ok(())
    } else {
        Err(ManifestRenderError::InvalidField {
            field,
            message: format!("label value {value:?} is not Kubernetes label-value-safe"),
        })
    }
}

fn label(
    field: &'static str,
    value: impl Into<String>,
) -> Result<(String, String), ManifestRenderError> {
    let value = value.into();
    validate_label_value(field, &value)?;
    Ok((field.to_owned(), value))
}

fn selector_labels(
    instance: &InstanceRecord,
    workload_name: &str,
) -> Result<BTreeMap<String, String>, ManifestRenderError> {
    Ok(BTreeMap::from([
        label(LABEL_INSTANCE_ID, instance.id.as_str())?,
        label(LABEL_WORKLOAD_NAME, workload_name)?,
    ]))
}

fn metadata_labels(
    instance: &InstanceRecord,
    workload_name: &str,
) -> Result<BTreeMap<String, String>, ManifestRenderError> {
    Ok(BTreeMap::from([
        label(LABEL_INSTANCE_ID, instance.id.as_str())?,
        label(LABEL_INSTANCE_GENERATION, instance.generation.to_string())?,
        label(
            LABEL_WORKLOAD_CLASS_ID,
            instance.workload_class.class_id.as_str(),
        )?,
        label(
            LABEL_WORKLOAD_CLASS_VERSION,
            instance.workload_class.version.to_string(),
        )?,
        label(LABEL_WORKLOAD_NAME, workload_name)?,
    ]))
}

fn metadata_annotations(request: &RenderManifestRequest<'_>) -> BTreeMap<String, String> {
    request
        .template_generation
        .map(|generation| {
            BTreeMap::from([(
                ANNOTATION_TEMPLATE_GENERATION.to_owned(),
                generation.to_string(),
            )])
        })
        .unwrap_or_default()
}
