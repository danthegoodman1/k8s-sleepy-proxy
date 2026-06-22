use serde_json::{json, Map, Value};

use super::{
    Container, ContainerPort, CsiPersistentVolumeSource, Deployment, EnvVar,
    HostPathPersistentVolumeSource, KubernetesObject, ObjectMeta, PersistentVolume,
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeReclaimPolicy,
    PersistentVolumeSource, PodTemplateMetadata, PodTemplateSpec, PodVolume, RenderedManifest,
    RenderedManifestObject, Service, ServicePort, StatefulSet, VolumeMount,
};

impl RenderedManifest {
    pub fn to_kubernetes_json_values(&self) -> Vec<Value> {
        self.objects
            .iter()
            .map(RenderedManifestObject::to_kubernetes_json)
            .collect()
    }
}

impl RenderedManifestObject {
    pub fn to_kubernetes_json(&self) -> Value {
        self.object.to_kubernetes_json()
    }
}

impl KubernetesObject {
    pub fn api_version(&self) -> &'static str {
        match self {
            Self::Deployment(_) | Self::StatefulSet(_) => "apps/v1",
            Self::Service(_) | Self::PersistentVolume(_) | Self::PersistentVolumeClaim(_) => "v1",
        }
    }

    pub fn to_kubernetes_json(&self) -> Value {
        match self {
            Self::Deployment(object) => deployment_to_value(object),
            Self::StatefulSet(object) => stateful_set_to_value(object),
            Self::Service(object) => service_to_value(object),
            Self::PersistentVolume(object) => persistent_volume_to_value(object),
            Self::PersistentVolumeClaim(object) => persistent_volume_claim_to_value(object),
        }
    }
}

fn deployment_to_value(object: &Deployment) -> Value {
    object_to_value(
        "apps/v1",
        "Deployment",
        &object.metadata,
        json!({
            "replicas": object.spec.replicas,
            "selector": label_selector_to_value(&object.spec.selector.match_labels),
            "template": pod_template_to_value(&object.spec.template),
        }),
    )
}

fn stateful_set_to_value(object: &StatefulSet) -> Value {
    object_to_value(
        "apps/v1",
        "StatefulSet",
        &object.metadata,
        json!({
            "replicas": object.spec.replicas,
            "serviceName": object.spec.service_name,
            "selector": label_selector_to_value(&object.spec.selector.match_labels),
            "template": pod_template_to_value(&object.spec.template),
        }),
    )
}

fn service_to_value(object: &Service) -> Value {
    object_to_value(
        "v1",
        "Service",
        &object.metadata,
        json!({
            "selector": object.spec.selector,
            "ports": object.spec.ports.iter().map(service_port_to_value).collect::<Vec<_>>(),
        }),
    )
}

fn persistent_volume_to_value(object: &PersistentVolume) -> Value {
    let mut spec = Map::new();
    spec.insert(
        "capacity".to_owned(),
        json!({
            "storage": object.spec.capacity,
        }),
    );
    spec.insert(
        "accessModes".to_owned(),
        access_modes_to_value(&object.spec.access_modes),
    );
    spec.insert(
        "persistentVolumeReclaimPolicy".to_owned(),
        json!(reclaim_policy_to_str(
            object.spec.persistent_volume_reclaim_policy
        )),
    );
    insert_optional_string(
        &mut spec,
        "storageClassName",
        object.spec.storage_class_name.as_deref(),
    );
    spec.insert(
        "claimRef".to_owned(),
        json!({
            "namespace": object.spec.claim_ref.namespace,
            "name": object.spec.claim_ref.name,
        }),
    );
    match &object.spec.source {
        PersistentVolumeSource::Csi(source) => {
            spec.insert("csi".to_owned(), csi_source_to_value(source));
        }
        PersistentVolumeSource::HostPath(source) => {
            spec.insert("hostPath".to_owned(), host_path_source_to_value(source));
        }
    }

    object_to_value(
        "v1",
        "PersistentVolume",
        &object.metadata,
        Value::Object(spec),
    )
}

fn persistent_volume_claim_to_value(object: &PersistentVolumeClaim) -> Value {
    let mut spec = Map::new();
    spec.insert(
        "accessModes".to_owned(),
        access_modes_to_value(&object.spec.access_modes),
    );
    spec.insert(
        "resources".to_owned(),
        json!({
            "requests": {
                "storage": object.spec.resources.requests_storage,
            },
        }),
    );
    insert_optional_string(
        &mut spec,
        "storageClassName",
        object.spec.storage_class_name.as_deref(),
    );
    spec.insert("volumeName".to_owned(), json!(object.spec.volume_name));

    object_to_value(
        "v1",
        "PersistentVolumeClaim",
        &object.metadata,
        Value::Object(spec),
    )
}

fn object_to_value(api_version: &str, kind: &str, metadata: &ObjectMeta, spec: Value) -> Value {
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": metadata_to_value(metadata),
        "spec": spec,
    })
}

fn metadata_to_value(metadata: &ObjectMeta) -> Value {
    let mut value = Map::new();
    value.insert("name".to_owned(), json!(metadata.name));
    insert_optional_string(&mut value, "namespace", metadata.namespace.as_deref());
    value.insert("labels".to_owned(), json!(metadata.labels));
    value.insert("annotations".to_owned(), json!(metadata.annotations));
    Value::Object(value)
}

fn pod_template_to_value(template: &PodTemplateSpec) -> Value {
    json!({
        "metadata": pod_template_metadata_to_value(&template.metadata),
        "spec": {
            "containers": template.spec.containers.iter().map(container_to_value).collect::<Vec<_>>(),
            "volumes": template.spec.volumes.iter().map(pod_volume_to_value).collect::<Vec<_>>(),
        },
    })
}

fn pod_template_metadata_to_value(metadata: &PodTemplateMetadata) -> Value {
    json!({
        "labels": metadata.labels,
        "annotations": metadata.annotations,
    })
}

fn container_to_value(container: &Container) -> Value {
    json!({
        "name": container.name,
        "image": container.image,
        "ports": container.ports.iter().map(container_port_to_value).collect::<Vec<_>>(),
        "env": container.env.iter().map(env_var_to_value).collect::<Vec<_>>(),
        "volumeMounts": container.volume_mounts.iter().map(volume_mount_to_value).collect::<Vec<_>>(),
    })
}

fn container_port_to_value(port: &ContainerPort) -> Value {
    let mut value = Map::new();
    insert_optional_string(&mut value, "name", port.name.as_deref());
    value.insert("containerPort".to_owned(), json!(port.container_port));
    Value::Object(value)
}

fn env_var_to_value(env: &EnvVar) -> Value {
    json!({
        "name": env.name,
        "value": env.value,
    })
}

fn volume_mount_to_value(mount: &VolumeMount) -> Value {
    json!({
        "name": mount.name,
        "mountPath": mount.mount_path,
    })
}

fn pod_volume_to_value(volume: &PodVolume) -> Value {
    json!({
        "name": volume.name,
        "persistentVolumeClaim": {
            "claimName": volume.persistent_volume_claim.claim_name,
        },
    })
}

fn service_port_to_value(port: &ServicePort) -> Value {
    let mut value = Map::new();
    insert_optional_string(&mut value, "name", port.name.as_deref());
    value.insert("port".to_owned(), json!(port.port));
    value.insert("targetPort".to_owned(), json!(port.target_port));
    Value::Object(value)
}

fn csi_source_to_value(source: &CsiPersistentVolumeSource) -> Value {
    let mut value = Map::new();
    value.insert("driver".to_owned(), json!(source.driver));
    value.insert("volumeHandle".to_owned(), json!(source.volume_handle));
    insert_optional_string(&mut value, "fsType", source.fs_type.as_deref());
    value.insert("readOnly".to_owned(), json!(source.read_only));
    value.insert(
        "volumeAttributes".to_owned(),
        json!(source.volume_attributes),
    );
    Value::Object(value)
}

fn host_path_source_to_value(source: &HostPathPersistentVolumeSource) -> Value {
    let mut value = Map::new();
    value.insert("path".to_owned(), json!(source.path));
    insert_optional_string(&mut value, "type", source.type_.as_deref());
    Value::Object(value)
}

fn label_selector_to_value(match_labels: &std::collections::BTreeMap<String, String>) -> Value {
    json!({
        "matchLabels": match_labels,
    })
}

fn access_modes_to_value(access_modes: &[PersistentVolumeAccessMode]) -> Value {
    json!(access_modes
        .iter()
        .map(|mode| match mode {
            PersistentVolumeAccessMode::ReadWriteOnce => "ReadWriteOnce",
            PersistentVolumeAccessMode::ReadOnlyMany => "ReadOnlyMany",
            PersistentVolumeAccessMode::ReadWriteMany => "ReadWriteMany",
        })
        .collect::<Vec<_>>())
}

fn reclaim_policy_to_str(policy: PersistentVolumeReclaimPolicy) -> &'static str {
    match policy {
        PersistentVolumeReclaimPolicy::Retain => "Retain",
        PersistentVolumeReclaimPolicy::Delete => "Delete",
    }
}

fn insert_optional_string(map: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        map.insert(key.to_owned(), json!(value));
    }
}
