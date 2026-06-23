use std::collections::BTreeMap;

use serde_json::json;

use super::{
    render_manifests, ApplyOrder, ContainerPortTemplate, ContainerTemplate,
    CsiPersistentVolumeSource, EnvVar, EnvVarTemplate, HostPathPersistentVolumeSource,
    KubernetesObject, ManifestRenderError, ManifestTemplate, PersistentVolumeAccessMode,
    PersistentVolumeReclaimPolicy, PersistentVolumeSource, PersistentVolumeSourceTemplate,
    RenderManifestRequest, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
    TemplateTextPart, VolumeTemplate, WorkloadKind, WorkloadTemplate, LABEL_INSTANCE_GENERATION,
    LABEL_INSTANCE_ID, LABEL_WORKLOAD_CLASS_ID, LABEL_WORKLOAD_CLASS_VERSION,
};
use crate::{
    ids::{Generation, InstanceId, WorkloadClassId},
    instance::{InstanceRecord, InstanceState, InstanceValues},
    sleep_policy::ResolvedSleepPolicy,
    workload::WorkloadClassVersionRef,
};

#[test]
fn template_text_renders_literal() {
    let rendered = TemplateText::literal("api").render(&InstanceValues::new());

    assert_eq!(rendered.expect("literal renders"), "api");
}

#[test]
fn template_text_renders_instance_value() {
    let rendered = TemplateText::instance_value("tenant").render(&values([("tenant", "acme")]));

    assert_eq!(rendered.expect("value renders"), "acme");
}

#[test]
fn template_text_renders_composed_parts() {
    let rendered = TemplateText::from_parts([
        TemplateTextPart::literal("app-"),
        TemplateTextPart::instance_value("tenant"),
        TemplateTextPart::literal("-v1"),
    ])
    .render(&values([("tenant", "acme")]));

    assert_eq!(rendered.expect("composition renders"), "app-acme-v1");
}

#[test]
fn template_text_reports_missing_instance_value() {
    let error = TemplateText::instance_value("tenant")
        .render(&InstanceValues::new())
        .expect_err("missing value is rejected");

    assert_eq!(
        error,
        ManifestRenderError::MissingInstanceValue {
            field: "tenant".to_owned()
        }
    );
}

#[test]
fn manifest_templates_survive_json_round_trips() {
    for template in [
        deployment_template(),
        stateful_template(),
        host_path_stateful_template(),
    ] {
        let encoded = serde_json::to_value(&template).expect("manifest template encodes");
        let decoded: ManifestTemplate =
            serde_json::from_value(encoded).expect("manifest template decodes");

        assert_eq!(decoded, template);
    }
}

#[test]
fn renders_deployment_and_service_without_volumes() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("deployment renders");

    assert_eq!(rendered.objects.len(), 2);
    assert_eq!(rendered.objects[0].apply_order, ApplyOrder::Service);
    assert_eq!(rendered.objects[1].apply_order, ApplyOrder::Workload);

    let service = match &rendered.objects[0].object {
        KubernetesObject::Service(service) => service,
        other => panic!("expected Service, got {}", other.kind()),
    };
    assert_eq!(service.metadata.name, "svc-acme");
    assert_eq!(service.spec.selector[LABEL_INSTANCE_ID], "instance-a");
    assert_eq!(service.spec.ports[0].port, 80);
    assert_eq!(service.spec.ports[0].target_port, 15000);

    let deployment = match &rendered.objects[1].object {
        KubernetesObject::Deployment(deployment) => deployment,
        other => panic!("expected Deployment, got {}", other.kind()),
    };
    assert_eq!(deployment.metadata.name, "app-acme");
    assert_eq!(deployment.metadata.labels[LABEL_INSTANCE_ID], "instance-a");
    assert_eq!(deployment.metadata.labels[LABEL_INSTANCE_GENERATION], "7");
    assert_eq!(deployment.metadata.labels[LABEL_WORKLOAD_CLASS_ID], "web");
    assert_eq!(
        deployment.metadata.labels[LABEL_WORKLOAD_CLASS_VERSION],
        "1"
    );
    assert_eq!(
        deployment.metadata.annotations["sleepypods.io/template-generation"],
        "3"
    );
    assert_eq!(deployment.spec.replicas, 1);
    assert_eq!(
        deployment.spec.selector.match_labels, service.spec.selector,
        "service and workload selectors should target the same pods"
    );
    assert_eq!(
        deployment.spec.template.metadata.labels[LABEL_INSTANCE_ID],
        "instance-a"
    );
    assert_eq!(
        deployment.spec.template.metadata.annotations["sleepypods.io/template-generation"],
        "3"
    );
    assert_eq!(deployment.spec.template.spec.containers.len(), 2);
    assert_eq!(
        deployment.spec.template.spec.containers[0].image,
        "example/app:1"
    );
    assert_eq!(
        deployment.spec.template.spec.containers[0].ports[0].container_port,
        8080
    );
    assert_eq!(
        deployment.spec.template.spec.containers[1].name,
        "sleepypods-sidecar"
    );
    assert_eq!(
        deployment.spec.template.spec.containers[1].image,
        "sleepypods/sidecar:test"
    );
    assert_eq!(
        deployment.spec.template.spec.containers[1].ports[0].container_port,
        15000
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_LISTEN_PORT",
        "15000",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_SIDECAR_LISTEN_ADDR",
        "0.0.0.0:15000",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_APP_PORT",
        "8080",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_INSTANCE_ID",
        "instance-a",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_INSTANCE_GENERATION",
        "7",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
        "http://sleepypods-control-plane.apps.svc.cluster.local:50051",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_IDLE_TIMEOUT_MS",
        "120000",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_IDLE_RETRY_BACKOFF_MS",
        "5000",
    );
    assert_env(
        &deployment.spec.template.spec.containers[1].env,
        "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS",
        "30000",
    );
    assert!(deployment.spec.template.spec.volumes.is_empty());
    assert!(deployment.spec.template.spec.containers[0]
        .volume_mounts
        .is_empty());
    assert!(deployment.spec.template.spec.containers[1]
        .volume_mounts
        .is_empty());
}

#[test]
fn serializes_deployment_and_service_as_kubernetes_json() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: Some(Generation::new(3)),
    })
    .expect("deployment renders");

    let objects = rendered.to_kubernetes_json_values();
    let service = &objects[0];
    assert_eq!(service["apiVersion"], json!("v1"));
    assert_eq!(service["kind"], json!("Service"));
    assert_eq!(service["metadata"]["name"], json!("svc-acme"));
    assert_eq!(service["metadata"]["namespace"], json!("apps"));
    assert_eq!(
        service["metadata"]["labels"][LABEL_INSTANCE_ID],
        json!("instance-a")
    );
    assert_eq!(
        service["metadata"]["annotations"]["sleepypods.io/template-generation"],
        json!("3")
    );
    assert_eq!(
        service["spec"]["selector"][LABEL_INSTANCE_ID],
        json!("instance-a")
    );
    assert_eq!(
        service["spec"]["ports"][0],
        json!({
            "name": "http",
            "port": 80,
            "targetPort": 15000,
        })
    );

    let deployment = &objects[1];
    assert_eq!(deployment["apiVersion"], json!("apps/v1"));
    assert_eq!(deployment["kind"], json!("Deployment"));
    assert_eq!(deployment["metadata"]["name"], json!("app-acme"));
    assert_eq!(deployment["metadata"]["namespace"], json!("apps"));
    assert_eq!(deployment["spec"]["replicas"], json!(1));
    assert_eq!(
        deployment["spec"]["selector"]["matchLabels"],
        service["spec"]["selector"]
    );
    assert_eq!(
        deployment["spec"]["template"]["metadata"]["labels"][LABEL_INSTANCE_ID],
        json!("instance-a")
    );
    assert_eq!(
        deployment["spec"]["template"]["spec"]["containers"][0],
        json!({
            "name": "app",
            "image": "example/app:1",
            "ports": [{
                "name": "http",
                "containerPort": 8080,
            }],
            "env": [{
                "name": "TENANT",
                "value": "acme",
            }],
            "volumeMounts": [],
        })
    );
    assert_eq!(
        deployment["spec"]["template"]["spec"]["containers"][1]["ports"][0],
        json!({
            "name": "sleepypods",
            "containerPort": 15000,
        })
    );
    assert_eq!(deployment["spec"]["template"]["spec"]["volumes"], json!([]));
}

#[test]
fn renders_stateful_set_service_pv_and_pvc_with_bound_volume() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &stateful_template(),
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("stateful workload renders");

    assert_eq!(
        rendered
            .objects
            .iter()
            .map(|object| object.apply_order)
            .collect::<Vec<_>>(),
        vec![
            ApplyOrder::PersistentVolume,
            ApplyOrder::PersistentVolumeClaim,
            ApplyOrder::Service,
            ApplyOrder::Workload
        ]
    );

    let pv = match &rendered.objects[0].object {
        KubernetesObject::PersistentVolume(pv) => pv,
        other => panic!("expected PersistentVolume, got {}", other.kind()),
    };
    assert_eq!(pv.metadata.name, "pv-acme");
    assert_eq!(pv.metadata.labels[LABEL_INSTANCE_ID], "postgres-a");
    assert_eq!(
        pv.spec.access_modes,
        vec![PersistentVolumeAccessMode::ReadWriteOnce]
    );
    assert_eq!(pv.spec.capacity, "10Gi");
    assert_eq!(
        pv.spec.persistent_volume_reclaim_policy,
        PersistentVolumeReclaimPolicy::Retain
    );
    assert_eq!(pv.spec.storage_class_name.as_deref(), Some("manual"));
    assert_eq!(pv.spec.claim_ref.namespace, "data");
    assert_eq!(pv.spec.claim_ref.name, "pvc-acme");
    assert_eq!(
        pv.spec.source,
        PersistentVolumeSource::Csi(CsiPersistentVolumeSource {
            driver: "csi.example.com".to_owned(),
            volume_handle: "provider-vol-123".to_owned(),
            fs_type: Some("ext4".to_owned()),
            read_only: false,
            volume_attributes: BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]),
        })
    );

    let pvc = match &rendered.objects[1].object {
        KubernetesObject::PersistentVolumeClaim(pvc) => pvc,
        other => panic!("expected PersistentVolumeClaim, got {}", other.kind()),
    };
    assert_eq!(pvc.metadata.name, "pvc-acme");
    assert_eq!(pvc.metadata.namespace.as_deref(), Some("data"));
    assert_eq!(pvc.spec.volume_name, "pv-acme");
    assert_eq!(pvc.spec.resources.requests_storage, "10Gi");

    let stateful_set = match &rendered.objects[3].object {
        KubernetesObject::StatefulSet(stateful_set) => stateful_set,
        other => panic!("expected StatefulSet, got {}", other.kind()),
    };
    assert_eq!(stateful_set.metadata.name, "db-acme");
    assert_eq!(stateful_set.spec.service_name, "db-acme");
    assert_eq!(stateful_set.spec.replicas, 1);
    assert_eq!(stateful_set.spec.template.spec.containers.len(), 2);
    assert_eq!(
        stateful_set.spec.template.spec.containers[0].ports[0].container_port,
        5432
    );
    assert_eq!(
        stateful_set.spec.template.spec.containers[1].name,
        "sleepypods-sidecar"
    );
    assert_eq!(
        stateful_set.spec.template.spec.containers[1].ports[0].container_port,
        15000
    );
    assert_env(
        &stateful_set.spec.template.spec.containers[1].env,
        "SLEEPYPODS_APP_PORT",
        "5432",
    );
    assert_eq!(
        stateful_set.spec.template.spec.volumes[0]
            .persistent_volume_claim
            .claim_name,
        "pvc-acme"
    );
    assert_eq!(
        stateful_set.spec.template.spec.containers[0].volume_mounts[0].name,
        "data"
    );
    assert_eq!(
        stateful_set.spec.template.spec.containers[0].volume_mounts[0].mount_path,
        "/var/lib/postgresql/data"
    );
    assert!(stateful_set.spec.template.spec.containers[1]
        .volume_mounts
        .is_empty());
}

#[test]
fn serializes_stateful_set_pv_and_pvc_as_kubernetes_json() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &stateful_template(),
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("stateful workload renders");

    let objects = rendered.to_kubernetes_json_values();
    let pv = &objects[0];
    assert_eq!(pv["apiVersion"], json!("v1"));
    assert_eq!(pv["kind"], json!("PersistentVolume"));
    assert_eq!(pv["metadata"]["name"], json!("pv-acme"));
    assert!(
        pv["metadata"].get("namespace").is_none(),
        "PersistentVolumes are cluster-scoped"
    );
    assert_eq!(
        pv["spec"],
        json!({
            "capacity": {
                "storage": "10Gi",
            },
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "manual",
            "claimRef": {
                "namespace": "data",
                "name": "pvc-acme",
            },
            "csi": {
                "driver": "csi.example.com",
                "volumeHandle": "provider-vol-123",
                "fsType": "ext4",
                "readOnly": false,
                "volumeAttributes": {
                    "tenant": "acme",
                },
            },
        })
    );

    let pvc = &objects[1];
    assert_eq!(pvc["apiVersion"], json!("v1"));
    assert_eq!(pvc["kind"], json!("PersistentVolumeClaim"));
    assert_eq!(pvc["metadata"]["name"], json!("pvc-acme"));
    assert_eq!(pvc["metadata"]["namespace"], json!("data"));
    assert_eq!(
        pvc["spec"],
        json!({
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "10Gi",
                },
            },
            "storageClassName": "manual",
            "volumeName": "pv-acme",
        })
    );

    let service = &objects[2];
    assert_eq!(service["kind"], json!("Service"));
    assert_eq!(service["spec"]["ports"][0]["targetPort"], json!(15000));

    let stateful_set = &objects[3];
    assert_eq!(stateful_set["apiVersion"], json!("apps/v1"));
    assert_eq!(stateful_set["kind"], json!("StatefulSet"));
    assert_eq!(stateful_set["metadata"]["name"], json!("db-acme"));
    assert_eq!(stateful_set["spec"]["serviceName"], json!("db-acme"));
    assert_eq!(stateful_set["spec"]["replicas"], json!(1));
    assert_eq!(
        stateful_set["spec"]["template"]["spec"]["volumes"][0],
        json!({
            "name": "data",
            "persistentVolumeClaim": {
                "claimName": "pvc-acme",
            },
        })
    );
    assert_eq!(
        stateful_set["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][0],
        json!({
            "name": "data",
            "mountPath": "/var/lib/postgresql/data",
        })
    );
    assert_eq!(
        stateful_set["spec"]["template"]["spec"]["containers"][1]["volumeMounts"],
        json!([])
    );
}

#[test]
fn renders_host_path_persistent_volume_source() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &host_path_stateful_template(),
        instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("hostPath stateful workload renders");

    let pv = match &rendered.objects[0].object {
        KubernetesObject::PersistentVolume(pv) => pv,
        other => panic!("expected PersistentVolume, got {}", other.kind()),
    };

    assert_eq!(
        pv.spec.source,
        PersistentVolumeSource::HostPath(HostPathPersistentVolumeSource {
            path: "/var/local/sleepypods/acme".to_owned(),
            type_: Some("DirectoryOrCreate".to_owned()),
        })
    );
}

#[test]
fn serializes_host_path_persistent_volume_source_as_kubernetes_json() {
    let rendered = render_manifests(RenderManifestRequest {
        template: &host_path_stateful_template(),
        instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect("hostPath stateful workload renders");

    let pv = &rendered.to_kubernetes_json_values()[0];

    assert_eq!(
        pv["spec"]["hostPath"],
        json!({
            "path": "/var/local/sleepypods/acme",
            "type": "DirectoryOrCreate",
        })
    );
    assert!(
        pv["spec"].get("csi").is_none(),
        "hostPath PVs should not serialize a CSI source"
    );
}

#[test]
fn rejects_stateful_set_scale_above_one() {
    let mut template = stateful_template();
    template.workload.replicas = Some(2);

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("scaled StatefulSet is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidReplicas {
            kind: WorkloadKind::StatefulSet,
            replicas: 2,
            message: "StatefulSet replicas above one are not supported in V1".to_owned(),
        }
    );
}

#[test]
fn rejects_instance_id_that_is_not_label_value_safe() {
    let error = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance("tenant/foo", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("invalid instance ID label is rejected");

    assert_invalid_field(error, LABEL_INSTANCE_ID, "tenant/foo");
}

#[test]
fn rejects_workload_class_id_that_is_not_label_value_safe() {
    let overlong_class_id = "a".repeat(64);
    let error = render_manifests(RenderManifestRequest {
        template: &deployment_template(),
        instance: &instance_with_class(
            "instance-a",
            &overlong_class_id,
            7,
            values([("tenant", "acme")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("invalid workload class label is rejected");

    assert_invalid_field(error, LABEL_WORKLOAD_CLASS_ID, &overlong_class_id);
}

#[test]
fn rejects_zero_container_port() {
    let mut template = deployment_template();
    template.workload.app_container.ports[0].container_port = 0;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("zero container port is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "container.ports.container_port",
            message: "port must be between 1 and 65535".to_owned(),
        }
    );
}

#[test]
fn rejects_zero_service_port() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports[0].port = 0;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("zero service port is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports.port",
            message: "port must be between 1 and 65535".to_owned(),
        }
    );
}

#[test]
fn rejects_zero_service_target_port() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports[0].target_port = 0;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("zero service target port is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: "port must be between 1 and 65535".to_owned(),
        }
    );
}

#[test]
fn rejects_zero_sidecar_listen_port() {
    let mut template = deployment_template();
    template.sidecar.listen_port = 0;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("zero sidecar listen port is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "sidecar.listen_port",
            message: "port must be between 1 and 65535".to_owned(),
        }
    );
}

#[test]
fn rejects_service_without_ports_for_sidecar_routing() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports = Vec::new();

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("service port is required");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports",
            message: "exactly one service port is supported for sidecar-routed workloads"
                .to_owned(),
        }
    );
}

#[test]
fn rejects_missing_service_for_sidecar_routing() {
    let mut template = deployment_template();
    template.service = None;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("service is required");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service",
            message: "sidecar-routed workloads require a service template".to_owned(),
        }
    );
}

#[test]
fn rejects_multiple_service_ports_for_sidecar_routing() {
    let mut template = deployment_template();
    template
        .service
        .as_mut()
        .expect("service")
        .ports
        .push(ServicePortTemplate {
            name: Some("admin".to_owned()),
            port: 8081,
            target_port: 8081,
        });

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("ambiguous service ports are rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports",
            message: "exactly one service port is supported for sidecar-routed workloads"
                .to_owned(),
        }
    );
}

#[test]
fn rejects_app_target_port_matching_sidecar_listen_port() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports[0].target_port = template.sidecar.listen_port;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("loop-prone sidecar target is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: "original app target port must differ from the sidecar listen port".to_owned(),
        }
    );
}

#[test]
fn rejects_service_target_port_not_declared_on_app_container() {
    let mut template = deployment_template();
    template.service.as_mut().expect("service").ports[0].target_port = 9090;

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("instance-a", 7, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "apps",
        template_generation: None,
    })
    .expect_err("service target port must be an app container port");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "service.ports.target_port",
            message: "target port 9090 must match an app container port".to_owned(),
        }
    );
}

#[test]
fn rejects_volume_without_access_modes() {
    let mut template = stateful_template();
    template.volumes[0].access_modes = Vec::new();

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("empty access modes are rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.access_modes",
            message: "at least one access mode is required".to_owned(),
        }
    );
}

#[test]
fn rejects_duplicate_volume_access_modes() {
    let mut template = stateful_template();
    template.volumes[0].access_modes = vec![
        PersistentVolumeAccessMode::ReadWriteOnce,
        PersistentVolumeAccessMode::ReadWriteOnce,
    ];

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "provider-vol-123")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("duplicate access modes are rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.access_modes",
            message: "access modes must not contain duplicates".to_owned(),
        }
    );
}

#[test]
fn rejects_unsupported_volume_access_mode_at_decode_boundary() {
    let mut encoded = serde_json::to_value(stateful_template()).expect("template encodes");
    encoded["volumes"][0]["access_modes"][0] = json!("ReadWriteOncePod");

    let error = serde_json::from_value::<ManifestTemplate>(encoded)
        .expect_err("unsupported access mode is rejected while decoding");

    assert!(
        error.to_string().contains("ReadWriteOncePod"),
        "decode error {error} should identify the unsupported access mode"
    );
}

#[test]
fn rejects_missing_csi_volume_handle_value() {
    let template = stateful_template();

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("missing CSI volume handle is rejected");

    assert_eq!(
        error,
        ManifestRenderError::MissingInstanceValue {
            field: "volume".to_owned(),
        }
    );
}

#[test]
fn rejects_empty_csi_volume_handle() {
    let template = stateful_template();

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance(
            "postgres-a",
            2,
            values([("tenant", "acme"), ("volume", "   ")]),
        ),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("empty CSI volume handle is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.source.csi.volume_handle",
            message: "rendered value must not be empty".to_owned(),
        }
    );
}

#[test]
fn rejects_relative_host_path_persistent_volume_source() {
    let mut template = host_path_stateful_template();
    template.volumes[0].source = PersistentVolumeSourceTemplate::HostPath {
        path: TemplateText::literal("var/local/sleepypods/acme"),
        type_: None,
    };

    let error = render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance("postgres-a", 2, values([("tenant", "acme")])),
        sleep_policy: sleep_policy(),
        namespace: "data",
        template_generation: None,
    })
    .expect_err("relative hostPath source is rejected");

    assert_eq!(
        error,
        ManifestRenderError::InvalidField {
            field: "volume.source.host_path.path",
            message: "hostPath path \"var/local/sleepypods/acme\" must be absolute".to_owned(),
        }
    );
}

fn deployment_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::Deployment,
            name: composed("app-", "tenant"),
            replicas: None,
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::literal("example/app:1"),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: 8080,
                }],
                env: vec![EnvVarTemplate {
                    name: "TENANT".to_owned(),
                    value: TemplateText::instance_value("tenant"),
                }],
            },
        },
        service: Some(ServiceTemplate {
            name: composed("svc-", "tenant"),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 80,
                target_port: 8080,
            }],
        }),
        sidecar: sidecar_template(),
        volumes: Vec::new(),
    }
}

fn stateful_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::StatefulSet,
            name: composed("db-", "tenant"),
            replicas: Some(1),
            app_container: ContainerTemplate {
                name: "postgres".to_owned(),
                image: TemplateText::literal("postgres:17"),
                ports: vec![ContainerPortTemplate {
                    name: Some("postgres".to_owned()),
                    container_port: 5432,
                }],
                env: Vec::new(),
            },
        },
        service: Some(ServiceTemplate {
            name: composed("db-", "tenant"),
            ports: vec![ServicePortTemplate {
                name: Some("postgres".to_owned()),
                port: 5432,
                target_port: 5432,
            }],
        }),
        sidecar: sidecar_template(),
        volumes: vec![VolumeTemplate {
            name: "data".to_owned(),
            mount_path: TemplateText::literal("/var/lib/postgresql/data"),
            pv_name: composed("pv-", "tenant"),
            pvc_name: composed("pvc-", "tenant"),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            capacity: TemplateText::literal("10Gi"),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain,
            storage_class_name: Some(TemplateText::literal("manual")),
            source: PersistentVolumeSourceTemplate::Csi {
                driver: TemplateText::literal("csi.example.com"),
                volume_handle: TemplateText::instance_value("volume"),
                fs_type: Some(TemplateText::literal("ext4")),
                read_only: false,
                volume_attributes: BTreeMap::from([(
                    "tenant".to_owned(),
                    TemplateText::instance_value("tenant"),
                )]),
            },
        }],
    }
}

fn host_path_stateful_template() -> ManifestTemplate {
    let mut template = stateful_template();
    template.volumes[0].storage_class_name = Some(TemplateText::literal("manual-host-path"));
    template.volumes[0].source = PersistentVolumeSourceTemplate::HostPath {
        path: TemplateText::from_parts([
            TemplateTextPart::literal("/var/local/sleepypods/"),
            TemplateTextPart::instance_value("tenant"),
        ]),
        type_: Some(TemplateText::literal("DirectoryOrCreate")),
    };
    template
}

fn sidecar_template() -> SidecarTemplate {
    SidecarTemplate {
        name: "sleepypods-sidecar".to_owned(),
        image: TemplateText::literal("sleepypods/sidecar:test"),
        listen_port: 15000,
    }
}

fn sleep_policy() -> ResolvedSleepPolicy {
    ResolvedSleepPolicy {
        idle_timeout_ms: 120_000,
        idle_retry_backoff_ms: 5_000,
        drain_grace_timeout_ms: 30_000,
    }
}

fn assert_env(env: &[EnvVar], name: &str, expected: &str) {
    let value = env
        .iter()
        .find(|var| var.name == name)
        .unwrap_or_else(|| panic!("missing env var {name}"));

    assert_eq!(value.value, expected);
}

fn composed(prefix: &str, field: &str) -> TemplateText {
    TemplateText::from_parts([
        TemplateTextPart::literal(prefix),
        TemplateTextPart::instance_value(field),
    ])
}

fn instance(id: &str, generation: u64, values: InstanceValues) -> InstanceRecord {
    instance_with_class(id, "web", generation, values)
}

fn instance_with_class(
    id: &str,
    class_id: &str,
    generation: u64,
    values: InstanceValues,
) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id).expect("valid instance ID"),
        workload_class: WorkloadClassVersionRef::new(
            WorkloadClassId::new(class_id).expect("valid class ID"),
            Generation::new(1),
        ),
        values,
        state: InstanceState::Cold,
        generation: Generation::new(generation),
    }
}

fn values<const N: usize>(pairs: [(&str, &str); N]) -> InstanceValues {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn assert_invalid_field(error: ManifestRenderError, field: &'static str, value: &str) {
    match error {
        ManifestRenderError::InvalidField {
            field: actual,
            message,
        } => {
            assert_eq!(actual, field);
            assert!(
                message.contains(value),
                "message {message:?} should identify invalid value {value:?}"
            );
        }
        other => panic!("expected InvalidField for {field}, got {other:?}"),
    }
}
