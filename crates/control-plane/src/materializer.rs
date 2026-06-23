use std::{collections::BTreeMap, error::Error, fmt, future::Future, pin::Pin};

use crate::{
    manifest::{
        ApplyOrder, KubernetesObject, RenderedManifest, RenderedManifestObject,
        ANNOTATION_TEMPLATE_GENERATION, LABEL_INSTANCE_GENERATION,
    },
    materialization::{BackendEndpoint, RenderedObjectRef},
};

pub type KubernetesClientFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type KubernetesClientResult<T> = Result<T, KubernetesClientError>;

pub trait KubernetesMaterializerClient: Send + Sync {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>>;

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>>;

    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>>;

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>>;
}

#[derive(Clone, Debug)]
pub struct KubernetesMaterializer<C> {
    client: C,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubernetesClientError {
    message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedMaterialization {
    pub rendered_objects: Vec<RenderedObjectRef>,
    pub backend: BackendEndpoint,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaterializerError {
    InvalidManifest {
        message: String,
    },
    Apply {
        object: RenderedObjectRef,
        source: KubernetesClientError,
    },
    Delete {
        object: RenderedObjectRef,
        source: KubernetesClientError,
    },
    PvcBoundWait {
        namespace: String,
        name: String,
        source: KubernetesClientError,
    },
    ReadinessWait {
        rendered_objects: Vec<RenderedObjectRef>,
        source: KubernetesClientError,
    },
}

impl<C> KubernetesMaterializer<C>
where
    C: KubernetesMaterializerClient,
{
    pub fn new(client: C) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &C {
        &self.client
    }

    pub async fn apply_manifest_until_ready(
        &self,
        manifest: &RenderedManifest,
    ) -> Result<AppliedMaterialization, MaterializerError> {
        let rendered_objects = self.apply_manifest(manifest).await?;
        let backend = self
            .client
            .wait_for_readiness(&rendered_objects)
            .await
            .map_err(|source| MaterializerError::ReadinessWait {
                rendered_objects: rendered_objects.clone(),
                source,
            })?;

        Ok(AppliedMaterialization {
            rendered_objects,
            backend,
        })
    }

    pub async fn apply_manifest(
        &self,
        manifest: &RenderedManifest,
    ) -> Result<Vec<RenderedObjectRef>, MaterializerError> {
        validate_manifest_generations(manifest)?;

        let objects = ordered_objects(manifest);
        let mut applied_refs = Vec::with_capacity(objects.len());

        for object in objects
            .iter()
            .filter(|object| object.apply_order == ApplyOrder::PersistentVolume)
        {
            applied_refs.push(self.apply_object(&object.object).await?);
        }

        let mut pvc_refs = Vec::new();
        for object in objects
            .iter()
            .filter(|object| object.apply_order == ApplyOrder::PersistentVolumeClaim)
        {
            let object_ref = self.apply_object(&object.object).await?;
            pvc_refs.push(object_ref.clone());
            applied_refs.push(object_ref);
        }

        for pvc in pvc_refs {
            self.wait_for_pvc_bound(&pvc).await?;
        }

        for apply_order in [ApplyOrder::Service, ApplyOrder::Workload] {
            for object in objects
                .iter()
                .filter(|object| object.apply_order == apply_order)
            {
                applied_refs.push(self.apply_object(&object.object).await?);
            }
        }

        Ok(applied_refs)
    }

    pub async fn delete_rendered_objects(
        &self,
        objects: &[RenderedObjectRef],
    ) -> Result<(), MaterializerError> {
        for object in objects.iter().rev() {
            self.client.delete_object(object).await.map_err(|source| {
                MaterializerError::Delete {
                    object: object.clone(),
                    source,
                }
            })?;
        }

        Ok(())
    }

    async fn apply_object(
        &self,
        object: &KubernetesObject,
    ) -> Result<RenderedObjectRef, MaterializerError> {
        let object_ref = rendered_object_ref(object);
        self.client
            .apply_object(object)
            .await
            .map_err(|source| MaterializerError::Apply {
                object: object_ref.clone(),
                source,
            })?;
        Ok(object_ref)
    }

    async fn wait_for_pvc_bound(&self, pvc: &RenderedObjectRef) -> Result<(), MaterializerError> {
        self.client
            .wait_for_pvc_bound(&pvc.namespace, &pvc.name)
            .await
            .map_err(|source| MaterializerError::PvcBoundWait {
                namespace: pvc.namespace.clone(),
                name: pvc.name.clone(),
                source,
            })
    }
}

pub fn rendered_object_ref(object: &KubernetesObject) -> RenderedObjectRef {
    let (api_version, namespace) = match object {
        KubernetesObject::Deployment(object) => ("apps/v1", object.metadata.namespace.clone()),
        KubernetesObject::StatefulSet(object) => ("apps/v1", object.metadata.namespace.clone()),
        KubernetesObject::Service(object) => ("v1", object.metadata.namespace.clone()),
        KubernetesObject::PersistentVolume(object) => ("v1", object.metadata.namespace.clone()),
        KubernetesObject::PersistentVolumeClaim(object) => {
            ("v1", object.metadata.namespace.clone())
        }
    };

    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: object.kind().to_owned(),
        namespace: namespace.unwrap_or_default(),
        name: object.name().to_owned(),
    }
}

fn ordered_objects(manifest: &RenderedManifest) -> Vec<&RenderedManifestObject> {
    let mut objects = manifest.objects.iter().collect::<Vec<_>>();
    objects.sort_by_key(|object| object.apply_order);
    objects
}

impl KubernetesClientError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for KubernetesClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for KubernetesClientError {}

impl fmt::Display for MaterializerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidManifest { message } => write!(f, "invalid rendered manifest: {message}"),
            Self::Apply { object, source } => {
                write!(
                    f,
                    "failed to apply {} {}/{}: {}",
                    object.kind, object.namespace, object.name, source
                )
            }
            Self::Delete { object, source } => {
                write!(
                    f,
                    "failed to delete {} {}/{}: {}",
                    object.kind, object.namespace, object.name, source
                )
            }
            Self::PvcBoundWait {
                namespace,
                name,
                source,
            } => write!(
                f,
                "failed waiting for PersistentVolumeClaim {namespace}/{name} to become Bound: {source}"
            ),
            Self::ReadinessWait {
                rendered_objects,
                source,
            } => write!(
                f,
                "failed waiting for readiness across {} rendered Kubernetes objects: {source}",
                rendered_objects.len()
            ),
        }
    }
}

impl Error for MaterializerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidManifest { .. } => None,
            Self::Apply { source, .. }
            | Self::Delete { source, .. }
            | Self::PvcBoundWait { source, .. }
            | Self::ReadinessWait { source, .. } => Some(source),
        }
    }
}

fn validate_manifest_generations(manifest: &RenderedManifest) -> Result<(), MaterializerError> {
    let expected_instance_generation = manifest.instance_generation.to_string();
    let expected_template_generation = manifest
        .template_generation
        .map(|generation| generation.to_string());

    for rendered in &manifest.objects {
        let (labels, annotations) = object_metadata(&rendered.object);
        validate_metadata_generation(
            &rendered.object,
            labels,
            annotations,
            &expected_instance_generation,
            expected_template_generation.as_deref(),
            "metadata",
        )?;

        if let Some((labels, annotations)) = pod_template_metadata(&rendered.object) {
            validate_metadata_generation(
                &rendered.object,
                labels,
                annotations,
                &expected_instance_generation,
                expected_template_generation.as_deref(),
                "pod template metadata",
            )?;
        }
    }

    Ok(())
}

fn validate_metadata_generation(
    object: &KubernetesObject,
    labels: &BTreeMap<String, String>,
    annotations: &BTreeMap<String, String>,
    expected_instance_generation: &str,
    expected_template_generation: Option<&str>,
    context: &'static str,
) -> Result<(), MaterializerError> {
    validate_generation_value(
        object,
        context,
        LABEL_INSTANCE_GENERATION,
        labels.get(LABEL_INSTANCE_GENERATION),
        expected_instance_generation,
    )?;

    if let Some(expected) = expected_template_generation {
        validate_generation_value(
            object,
            context,
            ANNOTATION_TEMPLATE_GENERATION,
            annotations.get(ANNOTATION_TEMPLATE_GENERATION),
            expected,
        )?;
    }

    Ok(())
}

fn validate_generation_value(
    object: &KubernetesObject,
    context: &'static str,
    key: &'static str,
    actual: Option<&String>,
    expected: &str,
) -> Result<(), MaterializerError> {
    if actual.is_some_and(|actual| actual == expected) {
        return Ok(());
    }

    let object_ref = rendered_object_ref(object);
    Err(MaterializerError::InvalidManifest {
        message: format!(
            "{} {}/{} {context} {key} must be {expected:?}, got {:?}",
            object_ref.kind, object_ref.namespace, object_ref.name, actual
        ),
    })
}

fn object_metadata(
    object: &KubernetesObject,
) -> (&BTreeMap<String, String>, &BTreeMap<String, String>) {
    match object {
        KubernetesObject::Deployment(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::StatefulSet(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::Service(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::PersistentVolume(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
        KubernetesObject::PersistentVolumeClaim(object) => {
            (&object.metadata.labels, &object.metadata.annotations)
        }
    }
}

fn pod_template_metadata(
    object: &KubernetesObject,
) -> Option<(&BTreeMap<String, String>, &BTreeMap<String, String>)> {
    match object {
        KubernetesObject::Deployment(object) => Some((
            &object.spec.template.metadata.labels,
            &object.spec.template.metadata.annotations,
        )),
        KubernetesObject::StatefulSet(object) => Some((
            &object.spec.template.metadata.labels,
            &object.spec.template.metadata.annotations,
        )),
        KubernetesObject::Service(_)
        | KubernetesObject::PersistentVolume(_)
        | KubernetesObject::PersistentVolumeClaim(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use crate::{
        ids::{Generation, InstanceId, WorkloadClassId},
        instance::{InstanceRecord, InstanceState, InstanceValues},
        manifest::{
            render_manifests, ContainerPortTemplate, ContainerTemplate, EnvVarTemplate,
            KubernetesObject, ManifestTemplate, PersistentVolumeAccessMode,
            PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate, RenderManifestRequest,
            ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText, TemplateTextPart,
            VolumeTemplate, WorkloadKind, WorkloadTemplate,
        },
        sleep_policy::ResolvedSleepPolicy,
        workload::WorkloadClassVersionRef,
    };

    use super::{
        rendered_object_ref, AppliedMaterialization, BackendEndpoint, KubernetesClientError,
        KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer,
        KubernetesMaterializerClient, MaterializerError, RenderedObjectRef,
    };

    #[derive(Clone, Debug, Default)]
    struct FakeKubernetesClient {
        inner: Arc<Mutex<FakeState>>,
    }

    #[derive(Clone, Debug, Default)]
    struct FakeState {
        operations: Vec<FakeOperation>,
        applied_objects: Vec<KubernetesObject>,
        readiness_backend: Option<BackendEndpoint>,
        fail_pvc_wait: Option<(String, String)>,
        fail_delete: Option<RenderedObjectRef>,
        fail_apply: Option<RenderedObjectRef>,
        fail_readiness: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum FakeOperation {
        Apply(RenderedObjectRef),
        WaitPvcBound { namespace: String, name: String },
        WaitReadiness(Vec<RenderedObjectRef>),
        Delete(RenderedObjectRef),
    }

    #[test]
    fn kubernetes_materializer_client_trait_is_dyn_safe() {
        fn assert_dyn_safe<T: KubernetesMaterializerClient + ?Sized>() {}

        assert_dyn_safe::<dyn KubernetesMaterializerClient>();
    }

    #[tokio::test]
    async fn applies_stateful_manifest_with_pvc_bound_wait_before_service_and_workload() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = stateful_manifest();

        let refs = materializer
            .apply_manifest(&manifest)
            .await
            .expect("stateful manifest applies");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(object_ref("v1", "PersistentVolume", "", "pv-acme")),
                FakeOperation::Apply(object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme"
                )),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme".to_owned(),
                },
                FakeOperation::Apply(object_ref("v1", "Service", "data", "db-acme")),
                FakeOperation::Apply(object_ref("apps/v1", "StatefulSet", "data", "db-acme")),
            ]
        );
        assert_eq!(
            refs,
            vec![
                object_ref("v1", "PersistentVolume", "", "pv-acme"),
                object_ref("v1", "PersistentVolumeClaim", "data", "pvc-acme"),
                object_ref("v1", "Service", "data", "db-acme"),
                object_ref("apps/v1", "StatefulSet", "data", "db-acme"),
            ]
        );
    }

    #[tokio::test]
    async fn applies_deployment_service_before_workload_and_returns_refs_in_apply_order() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = deployment_manifest();

        let refs = materializer
            .apply_manifest(&manifest)
            .await
            .expect("deployment manifest applies");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(object_ref("v1", "Service", "apps", "svc-acme")),
                FakeOperation::Apply(object_ref("apps/v1", "Deployment", "apps", "app-acme")),
            ]
        );
        assert_eq!(
            refs,
            vec![
                object_ref("v1", "Service", "apps", "svc-acme"),
                object_ref("apps/v1", "Deployment", "apps", "app-acme"),
            ]
        );
    }

    #[tokio::test]
    async fn applies_deployment_manifest_until_ready_after_all_objects_and_returns_backend() {
        let client = FakeKubernetesClient::default();
        let backend = backend_endpoint("http://svc-acme.apps.svc.cluster.local:80");
        client.set_readiness_backend(backend.clone());
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = deployment_manifest();
        let refs = vec![
            object_ref("v1", "Service", "apps", "svc-acme"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme"),
        ];

        let applied = materializer
            .apply_manifest_until_ready(&manifest)
            .await
            .expect("deployment manifest applies and becomes ready");

        assert_eq!(
            applied,
            AppliedMaterialization {
                rendered_objects: refs.clone(),
                backend
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(refs[0].clone()),
                FakeOperation::Apply(refs[1].clone()),
                FakeOperation::WaitReadiness(refs),
            ]
        );
    }

    #[tokio::test]
    async fn applies_stateful_manifest_until_ready_after_pvc_bound_service_and_workload() {
        let client = FakeKubernetesClient::default();
        let backend = backend_endpoint("tcp://db-acme.data.svc.cluster.local:5432");
        client.set_readiness_backend(backend.clone());
        let materializer = KubernetesMaterializer::new(client.clone());
        let manifest = stateful_manifest();
        let refs = vec![
            object_ref("v1", "PersistentVolume", "", "pv-acme"),
            object_ref("v1", "PersistentVolumeClaim", "data", "pvc-acme"),
            object_ref("v1", "Service", "data", "db-acme"),
            object_ref("apps/v1", "StatefulSet", "data", "db-acme"),
        ];

        let applied = materializer
            .apply_manifest_until_ready(&manifest)
            .await
            .expect("stateful manifest applies and becomes ready");

        assert_eq!(
            applied,
            AppliedMaterialization {
                rendered_objects: refs.clone(),
                backend
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(refs[0].clone()),
                FakeOperation::Apply(refs[1].clone()),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme".to_owned(),
                },
                FakeOperation::Apply(refs[2].clone()),
                FakeOperation::Apply(refs[3].clone()),
                FakeOperation::WaitReadiness(refs),
            ]
        );
    }

    #[tokio::test]
    async fn apply_failure_returns_typed_error_for_failed_object() {
        let client = FakeKubernetesClient::default();
        let service_ref = object_ref("v1", "Service", "apps", "svc-acme");
        client.fail_apply(service_ref.clone());
        let materializer = KubernetesMaterializer::new(client.clone());

        let error = materializer
            .apply_manifest(&deployment_manifest())
            .await
            .expect_err("service apply failure stops materialization");

        assert_eq!(
            error,
            MaterializerError::Apply {
                object: service_ref.clone(),
                source: KubernetesClientError::new("apply failed"),
            }
        );
        assert_eq!(client.operations(), vec![FakeOperation::Apply(service_ref)]);
    }

    #[tokio::test]
    async fn pvc_bound_wait_failure_prevents_service_and_workload_apply() {
        let client = FakeKubernetesClient::default();
        client.fail_pvc_wait("data", "pvc-acme");
        let materializer = KubernetesMaterializer::new(client.clone());

        let error = materializer
            .apply_manifest(&stateful_manifest())
            .await
            .expect_err("pvc wait failure stops materialization");

        assert_eq!(
            error,
            MaterializerError::PvcBoundWait {
                namespace: "data".to_owned(),
                name: "pvc-acme".to_owned(),
                source: KubernetesClientError::new("pvc did not bind"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(object_ref("v1", "PersistentVolume", "", "pv-acme")),
                FakeOperation::Apply(object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme"
                )),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme".to_owned(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn pvc_bound_wait_failure_prevents_readiness_wait() {
        let client = FakeKubernetesClient::default();
        client.fail_pvc_wait("data", "pvc-acme");
        let materializer = KubernetesMaterializer::new(client.clone());

        let error = materializer
            .apply_manifest_until_ready(&stateful_manifest())
            .await
            .expect_err("pvc wait failure stops ready materialization");

        assert_eq!(
            error,
            MaterializerError::PvcBoundWait {
                namespace: "data".to_owned(),
                name: "pvc-acme".to_owned(),
                source: KubernetesClientError::new("pvc did not bind"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(object_ref("v1", "PersistentVolume", "", "pv-acme")),
                FakeOperation::Apply(object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme"
                )),
                FakeOperation::WaitPvcBound {
                    namespace: "data".to_owned(),
                    name: "pvc-acme".to_owned(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn readiness_wait_failure_returns_typed_error_with_rendered_refs() {
        let client = FakeKubernetesClient::default();
        client.fail_readiness();
        let materializer = KubernetesMaterializer::new(client.clone());
        let refs = vec![
            object_ref("v1", "Service", "apps", "svc-acme"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme"),
        ];

        let error = materializer
            .apply_manifest_until_ready(&deployment_manifest())
            .await
            .expect_err("readiness wait failure stops ready materialization");

        assert_eq!(
            error,
            MaterializerError::ReadinessWait {
                rendered_objects: refs.clone(),
                source: KubernetesClientError::new("readiness wait failed"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Apply(refs[0].clone()),
                FakeOperation::Apply(refs[1].clone()),
                FakeOperation::WaitReadiness(refs),
            ]
        );
    }

    #[tokio::test]
    async fn deletes_rendered_refs_in_reverse_order() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let refs = KubernetesMaterializer::new(FakeKubernetesClient::default())
            .apply_manifest(&stateful_manifest())
            .await
            .expect("refs are derived");

        materializer
            .delete_rendered_objects(&refs)
            .await
            .expect("refs delete");

        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Delete(object_ref("apps/v1", "StatefulSet", "data", "db-acme")),
                FakeOperation::Delete(object_ref("v1", "Service", "data", "db-acme")),
                FakeOperation::Delete(object_ref(
                    "v1",
                    "PersistentVolumeClaim",
                    "data",
                    "pvc-acme"
                )),
                FakeOperation::Delete(object_ref("v1", "PersistentVolume", "", "pv-acme")),
            ]
        );
    }

    #[test]
    fn object_refs_use_expected_api_versions_kinds_names_and_namespaces() {
        let manifest = stateful_manifest();
        let refs = manifest
            .objects
            .iter()
            .map(|object| rendered_object_ref(&object.object))
            .collect::<Vec<_>>();

        assert_eq!(
            refs,
            vec![
                object_ref("v1", "PersistentVolume", "", "pv-acme"),
                object_ref("v1", "PersistentVolumeClaim", "data", "pvc-acme"),
                object_ref("v1", "Service", "data", "db-acme"),
                object_ref("apps/v1", "StatefulSet", "data", "db-acme"),
            ]
        );
    }

    #[tokio::test]
    async fn delete_failure_returns_typed_error_and_stops() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let service_ref = object_ref("v1", "Service", "apps", "svc-acme");
        let workload_ref = object_ref("apps/v1", "Deployment", "apps", "app-acme");
        client.fail_delete(service_ref.clone());

        let error = materializer
            .delete_rendered_objects(&[service_ref.clone(), workload_ref.clone()])
            .await
            .expect_err("delete failure stops cleanup");

        assert_eq!(
            error,
            MaterializerError::Delete {
                object: service_ref.clone(),
                source: KubernetesClientError::new("delete failed"),
            }
        );
        assert_eq!(
            client.operations(),
            vec![
                FakeOperation::Delete(workload_ref),
                FakeOperation::Delete(service_ref),
            ]
        );
    }

    #[tokio::test]
    async fn applied_objects_retain_ownership_and_generation_labels() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());

        materializer
            .apply_manifest(&deployment_manifest())
            .await
            .expect("deployment applies");

        for object in client.applied_objects() {
            let labels = object_labels(&object);
            assert_eq!(labels["sleepypods.io/instance-id"], "instance-a");
            assert_eq!(labels["sleepypods.io/instance-generation"], "7");
        }
    }

    #[tokio::test]
    async fn stale_object_generation_manifest_is_rejected_before_apply() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let mut manifest = deployment_manifest();
        let deployment = manifest
            .objects
            .iter_mut()
            .find_map(|object| match &mut object.object {
                KubernetesObject::Deployment(deployment) => Some(deployment),
                _ => None,
            })
            .expect("deployment object");
        deployment.metadata.labels.insert(
            "sleepypods.io/instance-generation".to_owned(),
            "6".to_owned(),
        );

        let error = materializer
            .apply_manifest(&manifest)
            .await
            .expect_err("stale generation label is rejected");

        assert_invalid_manifest(
            error,
            "Deployment apps/app-acme metadata sleepypods.io/instance-generation must be \"7\"",
        );
        assert!(
            client.operations().is_empty(),
            "stale manifest must not be applied"
        );
    }

    #[tokio::test]
    async fn stale_template_generation_manifest_is_rejected_before_apply() {
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let mut manifest = deployment_manifest();
        let deployment = manifest
            .objects
            .iter_mut()
            .find_map(|object| match &mut object.object {
                KubernetesObject::Deployment(deployment) => Some(deployment),
                _ => None,
            })
            .expect("deployment object");
        deployment.spec.template.metadata.annotations.insert(
            "sleepypods.io/template-generation".to_owned(),
            "2".to_owned(),
        );

        let error = materializer
            .apply_manifest(&manifest)
            .await
            .expect_err("stale template generation annotation is rejected");

        assert_invalid_manifest(
            error,
            "Deployment apps/app-acme pod template metadata sleepypods.io/template-generation must be \"3\"",
        );
        assert!(
            client.operations().is_empty(),
            "stale manifest must not be applied"
        );
    }

    impl KubernetesMaterializerClient for FakeKubernetesClient {
        fn apply_object<'a>(
            &'a self,
            object: &'a KubernetesObject,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            let object = object.clone();
            let client = self.clone();
            Box::pin(async move {
                let object_ref = rendered_object_ref(&object);
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                inner
                    .operations
                    .push(FakeOperation::Apply(object_ref.clone()));
                inner.applied_objects.push(object);

                if inner.fail_apply.as_ref() == Some(&object_ref) {
                    Err(KubernetesClientError::new("apply failed"))
                } else {
                    Ok(())
                }
            })
        }

        fn delete_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            let object = object.clone();
            let client = self.clone();
            Box::pin(async move {
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                inner.operations.push(FakeOperation::Delete(object.clone()));

                if inner.fail_delete.as_ref() == Some(&object) {
                    Err(KubernetesClientError::new("delete failed"))
                } else {
                    Ok(())
                }
            })
        }

        fn wait_for_pvc_bound<'a>(
            &'a self,
            namespace: &'a str,
            name: &'a str,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            let namespace = namespace.to_owned();
            let name = name.to_owned();
            let client = self.clone();
            Box::pin(async move {
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                inner.operations.push(FakeOperation::WaitPvcBound {
                    namespace: namespace.clone(),
                    name: name.clone(),
                });

                if inner.fail_pvc_wait.as_ref() == Some(&(namespace, name)) {
                    Err(KubernetesClientError::new("pvc did not bind"))
                } else {
                    Ok(())
                }
            })
        }

        fn wait_for_readiness<'a>(
            &'a self,
            objects: &'a [RenderedObjectRef],
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
            let objects = objects.to_vec();
            let client = self.clone();
            Box::pin(async move {
                let mut inner = client.inner.lock().expect("fake client lock not poisoned");
                inner.operations.push(FakeOperation::WaitReadiness(objects));

                if inner.fail_readiness {
                    Err(KubernetesClientError::new("readiness wait failed"))
                } else {
                    Ok(inner
                        .readiness_backend
                        .clone()
                        .unwrap_or_else(default_backend_endpoint))
                }
            })
        }
    }

    impl FakeKubernetesClient {
        fn operations(&self) -> Vec<FakeOperation> {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .operations
                .clone()
        }

        fn applied_objects(&self) -> Vec<KubernetesObject> {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .applied_objects
                .clone()
        }

        fn fail_pvc_wait(&self, namespace: &str, name: &str) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .fail_pvc_wait = Some((namespace.to_owned(), name.to_owned()));
        }

        fn fail_apply(&self, object: RenderedObjectRef) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .fail_apply = Some(object);
        }

        fn fail_delete(&self, object: RenderedObjectRef) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .fail_delete = Some(object);
        }

        fn set_readiness_backend(&self, backend: BackendEndpoint) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .readiness_backend = Some(backend);
        }

        fn fail_readiness(&self) {
            self.inner
                .lock()
                .expect("fake client lock not poisoned")
                .fail_readiness = true;
        }
    }

    fn deployment_manifest() -> crate::manifest::RenderedManifest {
        render_manifests(RenderManifestRequest {
            template: &deployment_template(),
            instance: &instance("instance-a", 7, values([("tenant", "acme")])),
            sleep_policy: resolved_sleep_policy(),
            namespace: "apps",
            template_generation: Some(Generation::new(3)),
        })
        .expect("deployment renders")
    }

    fn stateful_manifest() -> crate::manifest::RenderedManifest {
        render_manifests(RenderManifestRequest {
            template: &stateful_template(),
            instance: &instance(
                "postgres-a",
                2,
                values([("tenant", "acme"), ("volume", "provider-vol-123")]),
            ),
            sleep_policy: resolved_sleep_policy(),
            namespace: "data",
            template_generation: None,
        })
        .expect("stateful workload renders")
    }

    fn resolved_sleep_policy() -> ResolvedSleepPolicy {
        ResolvedSleepPolicy {
            idle_timeout_ms: 300_000,
            idle_retry_backoff_ms: 5_000,
            drain_grace_timeout_ms: 30_000,
        }
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
                    volume_attributes: BTreeMap::new(),
                },
            }],
        }
    }

    fn sidecar_template() -> SidecarTemplate {
        SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
        }
    }

    fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
        RenderedObjectRef {
            api_version: api_version.to_owned(),
            kind: kind.to_owned(),
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
    }

    fn backend_endpoint(uri: &str) -> BackendEndpoint {
        BackendEndpoint::new(uri).expect("valid backend endpoint")
    }

    fn default_backend_endpoint() -> BackendEndpoint {
        backend_endpoint("http://ready.backend")
    }

    fn object_labels(object: &KubernetesObject) -> &BTreeMap<String, String> {
        match object {
            KubernetesObject::Deployment(object) => &object.metadata.labels,
            KubernetesObject::StatefulSet(object) => &object.metadata.labels,
            KubernetesObject::Service(object) => &object.metadata.labels,
            KubernetesObject::PersistentVolume(object) => &object.metadata.labels,
            KubernetesObject::PersistentVolumeClaim(object) => &object.metadata.labels,
        }
    }

    fn assert_invalid_manifest(error: MaterializerError, expected_message: &str) {
        match error {
            MaterializerError::InvalidManifest { message } => {
                assert!(
                    message.contains(expected_message),
                    "message {message:?} should contain {expected_message:?}"
                );
            }
            other => panic!("expected invalid manifest error, got {other:?}"),
        }
    }

    fn composed(prefix: &str, field: &str) -> TemplateText {
        TemplateText::from_parts([
            TemplateTextPart::literal(prefix),
            TemplateTextPart::instance_value(field),
        ])
    }

    fn instance(id: &str, generation: u64, values: InstanceValues) -> InstanceRecord {
        InstanceRecord {
            id: InstanceId::new(id).expect("valid instance ID"),
            workload_class: WorkloadClassVersionRef::new(
                WorkloadClassId::new("web").expect("valid class ID"),
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
}
