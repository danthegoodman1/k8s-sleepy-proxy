use std::{
    collections::BTreeMap,
    env,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::{
    render_manifests, rendered_object_ref, ContainerPortTemplate, ContainerTemplate, Generation,
    InstanceId, InstanceRecord, InstanceState, InstanceValues, KubeMaterializerClient,
    KubeMaterializerClientConfig, KubernetesMaterializer, KubernetesMaterializerClient,
    ManifestTemplate, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
    PersistentVolumeSourceTemplate, RenderManifestRequest, RenderedManifest, RenderedObjectRef,
    ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart,
    VolumeTemplate, WorkloadClassId, WorkloadClassVersionRef, WorkloadKind, WorkloadTemplate,
};
use k8s_openapi::api::{
    apps::v1::StatefulSet,
    core::v1::{Namespace, PersistentVolume, PersistentVolumeClaim, Service},
};
use kube::{
    api::{DeleteParams, ObjectMeta, PostParams},
    Api, Client, Error as KubeError,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires SLEEPYPODS_KIND_TEST=1 and a disposable kind/current kube context"]
async fn materializes_stateful_set_with_static_host_path_volume() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_TEST").as_deref() != Ok("1") {
        eprintln!("skipping kind materializer test because SLEEPYPODS_KIND_TEST=1 is not set");
        return Ok(());
    }

    install_rustls_crypto_provider();

    let client = Client::try_default().await?;
    let namespace = unique_namespace();
    create_namespace(client.clone(), &namespace).await?;

    let result = run_materializer_lifecycle(client.clone(), &namespace).await;
    let namespace_delete = delete_namespace(client, &namespace).await;

    result?;
    namespace_delete?;
    Ok(())
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

async fn run_materializer_lifecycle(client: Client, namespace: &str) -> TestResult<()> {
    let pv_name = pv_name(namespace);
    let config = KubeMaterializerClientConfig {
        pvc_bound_timeout: Duration::from_secs(60),
        readiness_timeout: Duration::from_secs(90),
        poll_interval: Duration::from_secs(1),
        ..KubeMaterializerClientConfig::default()
    };
    let kube_client = KubeMaterializerClient::with_config(client.clone(), config)?;
    let materializer = KubernetesMaterializer::new(kube_client);
    let manifest = kind_manifest(namespace, &pv_name)?;
    let refs = rendered_refs(&manifest);

    let lifecycle_result: TestResult<String> = async {
        let applied_refs = materializer.apply_manifest(&manifest).await?;
        assert_pv_and_bound_pvc_exist(client.clone(), namespace, &pv_name).await?;
        let backend = materializer
            .client()
            .wait_for_readiness(&applied_refs)
            .await?;
        Ok(backend.uri().to_owned())
    }
    .await;
    let delete_result = materializer.delete_rendered_objects(&refs).await;

    let backend_uri = lifecycle_result?;
    assert_eq!(
        backend_uri,
        format!("http://sleepypods-kind-app.{namespace}.svc.cluster.local:8080")
    );

    delete_result?;
    assert_deleted_or_deleting(client, namespace, &pv_name).await?;
    Ok(())
}

fn kind_manifest(namespace: &str, pv_name: &str) -> TestResult<RenderedManifest> {
    let template = ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::StatefulSet,
            name: TemplateText::literal("sleepypods-kind-app"),
            replicas: Some(1),
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::literal("registry.k8s.io/pause:3.10"),
                ports: vec![ContainerPortTemplate {
                    name: Some("app".to_owned()),
                    container_port: 8080,
                }],
                env: Vec::new(),
            },
        },
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("nginx:1.27-alpine"),
            listen_port: 80,
        },
        service: Some(ServiceTemplate {
            name: TemplateText::literal("sleepypods-kind-app"),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 8080,
                target_port: 8080,
            }],
        }),
        volumes: vec![VolumeTemplate {
            name: "data".to_owned(),
            mount_path: TemplateText::literal("/data"),
            pv_name: TemplateText::literal(pv_name),
            pvc_name: TemplateText::literal("sleepypods-kind-pvc"),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            capacity: TemplateText::literal("1Mi"),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain,
            storage_class_name: Some(TemplateText::literal("sleepypods-kind-static")),
            source: PersistentVolumeSourceTemplate::HostPath {
                path: TemplateText::from_parts([
                    TemplateTextPart::literal("/tmp/sleepypods-kind/"),
                    TemplateTextPart::literal(namespace),
                ]),
                type_: Some(TemplateText::literal("DirectoryOrCreate")),
            },
        }],
    };
    let instance = InstanceRecord {
        id: InstanceId::new("kind-materializer").expect("valid instance ID"),
        workload_class: WorkloadClassVersionRef::new(
            WorkloadClassId::new("kind").expect("valid workload class ID"),
            Generation::new(1),
        ),
        values: InstanceValues::new(),
        state: InstanceState::Cold,
        generation: Generation::new(1),
    };

    Ok(render_manifests(RenderManifestRequest {
        template: &template,
        instance: &instance,
        namespace,
        template_generation: Some(Generation::new(1)),
    })?)
}

async fn assert_pv_and_bound_pvc_exist(
    client: Client,
    namespace: &str,
    pv_name: &str,
) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(client.clone());
    let pv = pvs.get(pv_name).await?;
    let expected_path = format!("/tmp/sleepypods-kind/{namespace}");
    assert_eq!(
        pv.spec
            .as_ref()
            .and_then(|spec| spec.host_path.as_ref())
            .map(|source| source.path.as_str()),
        Some(expected_path.as_str())
    );

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client, namespace);
    let pvc = pvcs.get("sleepypods-kind-pvc").await?;
    assert_eq!(
        pvc.status
            .as_ref()
            .and_then(|status| status.phase.as_deref()),
        Some("Bound")
    );
    Ok(())
}

async fn assert_deleted_or_deleting(
    client: Client,
    namespace: &str,
    pv_name: &str,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    assert_deleted_or_deleting_object(stateful_sets.get("sleepypods-kind-app").await, |object| {
        object.metadata.deletion_timestamp.is_some()
    })?;

    let services: Api<Service> = Api::namespaced(client.clone(), namespace);
    assert_deleted_or_deleting_object(services.get("sleepypods-kind-app").await, |object| {
        object.metadata.deletion_timestamp.is_some()
    })?;

    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    assert_deleted_or_deleting_object(pvcs.get("sleepypods-kind-pvc").await, |object| {
        object.metadata.deletion_timestamp.is_some()
    })?;

    let pvs: Api<PersistentVolume> = Api::all(client);
    assert_deleted_or_deleting_object(pvs.get(pv_name).await, |object| {
        object.metadata.deletion_timestamp.is_some()
    })?;
    Ok(())
}

fn assert_deleted_or_deleting_object<K>(
    result: Result<K, KubeError>,
    is_deleting: impl FnOnce(&K) -> bool,
) -> TestResult<()>
where
{
    match result {
        Ok(object) => {
            assert!(
                is_deleting(&object),
                "object still exists and Kubernetes has not accepted deletion"
            );
            Ok(())
        }
        Err(error) if is_not_found(&error) => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

async fn create_namespace(client: Client, namespace: &str) -> TestResult<()> {
    let namespaces: Api<Namespace> = Api::all(client);
    let object = Namespace {
        metadata: ObjectMeta {
            name: Some(namespace.to_owned()),
            labels: Some(BTreeMap::from([(
                "sleepypods.io/kind-test".to_owned(),
                "true".to_owned(),
            )])),
            ..ObjectMeta::default()
        },
        ..Namespace::default()
    };
    namespaces.create(&PostParams::default(), &object).await?;
    Ok(())
}

async fn delete_namespace(client: Client, namespace: &str) -> TestResult<()> {
    let namespaces: Api<Namespace> = Api::all(client);
    match namespaces.delete(namespace, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(error) if is_not_found(&error) => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

fn rendered_refs(manifest: &RenderedManifest) -> Vec<RenderedObjectRef> {
    manifest
        .objects
        .iter()
        .map(|object| rendered_object_ref(&object.object))
        .collect()
}

fn unique_namespace() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after Unix epoch")
        .as_millis();
    format!("sleepypods-kind-{millis}-{}", std::process::id())
}

fn pv_name(namespace: &str) -> String {
    format!("{namespace}-pv")
}

fn is_not_found(error: &KubeError) -> bool {
    matches!(error, KubeError::Api(status) if status.is_not_found())
}
