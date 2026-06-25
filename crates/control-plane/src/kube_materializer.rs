use std::{error::Error, fmt, time::Duration};

use k8s_openapi::api::{
    core::v1::{PersistentVolumeClaim, Service},
    discovery::v1::EndpointSlice,
};
use kube::{
    api::{Api, ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams},
    Client, Error as KubeError,
};
use tokio::time::{sleep, timeout, Instant};

use crate::{
    manifest::KubernetesObject,
    materialization::{BackendEndpoint, RenderedObjectRef},
    materializer::{
        rendered_object_ref, KubernetesClientError, KubernetesClientFuture, KubernetesClientResult,
        KubernetesMaterializerClient,
    },
    projection::{LiveObjectMetadata, ProjectionObjectInspection},
};

const DEFAULT_FIELD_MANAGER: &str = "sleepypods-control-plane";
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";
const BACKEND_SCHEME_ANNOTATION: &str = "sleepypods.io/backend-scheme";

#[derive(Clone)]
pub struct KubeMaterializerClient {
    client: Client,
    config: KubeMaterializerClientConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubeMaterializerClientConfig {
    pub field_manager: String,
    pub backend_scheme: String,
    pub delete_timeout: Duration,
    pub pvc_bound_timeout: Duration,
    pub readiness_timeout: Duration,
    pub poll_interval: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidKubeMaterializerClientConfig {
    field: &'static str,
}

impl KubeMaterializerClient {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            config: KubeMaterializerClientConfig::default(),
        }
    }

    pub fn with_config(
        client: Client,
        config: KubeMaterializerClientConfig,
    ) -> Result<Self, InvalidKubeMaterializerClientConfig> {
        config.validate()?;
        Ok(Self { client, config })
    }

    pub async fn try_default() -> Result<Self, KubernetesClientError> {
        Client::try_default()
            .await
            .map(Self::new)
            .map_err(kube_error)
    }

    pub fn config(&self) -> &KubeMaterializerClientConfig {
        &self.config
    }

    fn dynamic_api(
        &self,
        object: &RenderedObjectRef,
    ) -> KubernetesClientResult<Api<DynamicObject>> {
        let resource = api_resource_for_ref(object)?;
        Ok(if object.namespace.is_empty() {
            Api::all_with(self.client.clone(), &resource)
        } else {
            Api::namespaced_with(self.client.clone(), &object.namespace, &resource)
        })
    }
}

impl KubernetesMaterializerClient for KubeMaterializerClient {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let object_ref = rendered_object_ref(object);
            let api = self.dynamic_api(&object_ref)?;
            let params = PatchParams::apply(&self.config.field_manager);
            let body = object.to_kubernetes_json();
            api.patch(&object_ref.name, &params, &Patch::Apply(&body))
                .await
                .map(|_| ())
                .map_err(kube_error)
        })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let api = self.dynamic_api(object)?;
            let delete = timeout(
                self.config.delete_timeout,
                api.delete(&object.name, &DeleteParams::default()),
            )
            .await
            .map_err(|_| {
                KubernetesClientError::transient(format!(
                    "timed out deleting Kubernetes object {} {} {}/{}",
                    object.api_version, object.kind, object.namespace, object.name
                ))
            })?;
            match delete {
                Ok(_) => Ok(()),
                Err(error) if is_not_found(&error) => Ok(()),
                Err(error) => Err(kube_error(error)),
            }
        })
    }

    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), namespace);
            let deadline = Instant::now() + self.config.pvc_bound_timeout;

            loop {
                let pvc = pvcs.get(name).await.map_err(kube_error)?;
                if pvc_phase_is_bound(&pvc) {
                    return Ok(());
                }

                sleep_until_next_poll(
                    deadline,
                    self.config.poll_interval,
                    format!("timed out waiting for PersistentVolumeClaim {namespace}/{name} to become Bound"),
                )
                .await?;
            }
        })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async move {
            let service_ref = rendered_service_ref(objects)?;
            let services: Api<Service> =
                Api::namespaced(self.client.clone(), &service_ref.namespace);
            let endpoint_slices: Api<EndpointSlice> =
                Api::namespaced(self.client.clone(), &service_ref.namespace);
            let deadline = Instant::now() + self.config.readiness_timeout;

            loop {
                let service = services.get(&service_ref.name).await.map_err(kube_error)?;
                let backend = backend_endpoint_for_service(&service, &self.config)?;
                let selector = format!("{SERVICE_NAME_LABEL}={}", service_ref.name);
                let slices = endpoint_slices
                    .list(&ListParams::default().labels(&selector))
                    .await
                    .map_err(kube_error)?;

                if slices.iter().any(endpoint_slice_has_ready_endpoint) {
                    return Ok(backend);
                }

                sleep_until_next_poll(
                    deadline,
                    self.config.poll_interval,
                    format!(
                        "timed out waiting for ready EndpointSlice endpoints for Service {}/{}",
                        service_ref.namespace, service_ref.name
                    ),
                )
                .await?;
            }
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            let api = self.dynamic_api(object)?;
            match api.get(&object.name).await {
                Ok(live) => Ok(ProjectionObjectInspection::Present(LiveObjectMetadata {
                    labels: live.metadata.labels.unwrap_or_default(),
                    annotations: live.metadata.annotations.unwrap_or_default(),
                    deleting: live.metadata.deletion_timestamp.is_some(),
                    finalizers: live.metadata.finalizers.unwrap_or_default(),
                })),
                Err(error) if is_not_found(&error) => Ok(ProjectionObjectInspection::Missing),
                Err(error) => Err(kube_error(error)),
            }
        })
    }
}

impl Default for KubeMaterializerClientConfig {
    fn default() -> Self {
        Self {
            field_manager: DEFAULT_FIELD_MANAGER.to_owned(),
            backend_scheme: "http".to_owned(),
            delete_timeout: Duration::from_secs(10),
            pvc_bound_timeout: Duration::from_secs(120),
            readiness_timeout: Duration::from_secs(120),
            poll_interval: Duration::from_secs(2),
        }
    }
}

impl KubeMaterializerClientConfig {
    pub fn validate(&self) -> Result<(), InvalidKubeMaterializerClientConfig> {
        if self.field_manager.trim().is_empty() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "field_manager",
            });
        }
        if !is_valid_uri_scheme(&self.backend_scheme) {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "backend_scheme",
            });
        }
        if self.delete_timeout.is_zero() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "delete_timeout",
            });
        }
        if self.pvc_bound_timeout.is_zero() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "pvc_bound_timeout",
            });
        }
        if self.readiness_timeout.is_zero() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "readiness_timeout",
            });
        }
        if self.poll_interval.is_zero() {
            return Err(InvalidKubeMaterializerClientConfig {
                field: "poll_interval",
            });
        }
        Ok(())
    }
}

impl InvalidKubeMaterializerClientConfig {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for InvalidKubeMaterializerClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Kubernetes materializer config {} is invalid",
            self.field
        )
    }
}

impl Error for InvalidKubeMaterializerClientConfig {}

fn api_resource_for_ref(object: &RenderedObjectRef) -> KubernetesClientResult<ApiResource> {
    let (group, version) = split_api_version(&object.api_version);
    let plural = match (
        object.api_version.as_str(),
        object.kind.as_str(),
        object.namespace.is_empty(),
    ) {
        ("apps/v1", "Deployment", false) => "deployments",
        ("apps/v1", "StatefulSet", false) => "statefulsets",
        ("v1", "Service", false) => "services",
        ("v1", "PersistentVolume", true) => "persistentvolumes",
        ("v1", "PersistentVolumeClaim", false) => "persistentvolumeclaims",
        _ => {
            return Err(KubernetesClientError::new(format!(
                "unsupported rendered Kubernetes object {} {} {}/{}",
                object.api_version, object.kind, object.namespace, object.name
            )));
        }
    };

    Ok(ApiResource {
        group: group.to_owned(),
        version: version.to_owned(),
        api_version: object.api_version.clone(),
        kind: object.kind.clone(),
        plural: plural.to_owned(),
    })
}

fn backend_endpoint_for_service(
    service: &Service,
    config: &KubeMaterializerClientConfig,
) -> KubernetesClientResult<BackendEndpoint> {
    let backend_scheme = backend_scheme_for_service(service, config)?;

    let name = service
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| KubernetesClientError::new("live Service is missing metadata.name"))?;
    let namespace =
        service.metadata.namespace.as_deref().ok_or_else(|| {
            KubernetesClientError::new("live Service is missing metadata.namespace")
        })?;
    let port = service_port(service)?;

    BackendEndpoint::new(format!(
        "{}://{name}.{namespace}.svc.cluster.local:{port}",
        backend_scheme
    ))
    .map_err(|error| KubernetesClientError::new(error.to_string()))
}

fn backend_scheme_for_service<'a>(
    service: &'a Service,
    config: &'a KubeMaterializerClientConfig,
) -> KubernetesClientResult<&'a str> {
    if let Some(annotation) = service
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(BACKEND_SCHEME_ANNOTATION))
        .map(String::as_str)
    {
        if !is_valid_uri_scheme(annotation) {
            return Err(KubernetesClientError::new(format!(
                "Service backend scheme annotation {BACKEND_SCHEME_ANNOTATION} is invalid"
            )));
        }

        return Ok(annotation);
    }

    if !is_valid_uri_scheme(&config.backend_scheme) {
        return Err(KubernetesClientError::new(
            "Kubernetes materializer backend scheme is invalid",
        ));
    }

    Ok(&config.backend_scheme)
}

fn endpoint_slice_has_ready_endpoint(slice: &EndpointSlice) -> bool {
    slice.endpoints.iter().any(|endpoint| {
        endpoint
            .conditions
            .as_ref()
            .and_then(|conditions| conditions.ready)
            .unwrap_or(true)
    })
}

fn is_valid_uri_scheme(scheme: &str) -> bool {
    let mut chars = scheme.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
}

fn split_api_version(api_version: &str) -> (&str, &str) {
    api_version.rsplit_once('/').unwrap_or(("", api_version))
}

fn rendered_service_ref(
    objects: &[RenderedObjectRef],
) -> KubernetesClientResult<&RenderedObjectRef> {
    objects
        .iter()
        .find(|object| object.api_version == "v1" && object.kind == "Service")
        .ok_or_else(|| {
            KubernetesClientError::new(
                "rendered Kubernetes objects do not include a Service for readiness",
            )
        })
}

fn service_port(service: &Service) -> KubernetesClientResult<i32> {
    let ports = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .ok_or_else(|| KubernetesClientError::new("live Service has no spec.ports"))?;
    let port = ports
        .first()
        .ok_or_else(|| KubernetesClientError::new("live Service has no ports"))?
        .port;
    if port <= 0 {
        return Err(KubernetesClientError::new(format!(
            "live Service port {port} is not positive"
        )));
    }
    Ok(port)
}

fn pvc_phase_is_bound(pvc: &PersistentVolumeClaim) -> bool {
    pvc.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        == Some("Bound")
}

async fn sleep_until_next_poll(
    deadline: Instant,
    poll_interval: Duration,
    timeout_message: String,
) -> KubernetesClientResult<()> {
    let now = Instant::now();
    if now >= deadline {
        return Err(KubernetesClientError::new(timeout_message));
    }

    sleep(poll_interval.min(deadline - now)).await;
    Ok(())
}

fn is_not_found(error: &KubeError) -> bool {
    matches!(error, KubeError::Api(status) if status.is_not_found())
}

fn kube_error(error: KubeError) -> KubernetesClientError {
    let message = error.to_string();
    if is_transient_kube_error(&error) {
        KubernetesClientError::transient(message)
    } else {
        KubernetesClientError::new(message)
    }
}

fn is_transient_kube_error(error: &KubeError) -> bool {
    match error {
        KubeError::Api(status) => {
            status.is_conflict() || status.code == 429 || (500..=599).contains(&status.code)
        }
        KubeError::HyperError(_) | KubeError::Service(_) | KubeError::ReadEvents(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use k8s_openapi::{
        api::{
            core::v1::{Service, ServicePort, ServiceSpec},
            discovery::v1::{Endpoint, EndpointConditions, EndpointSlice},
        },
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };
    use kube::{core::Status, Error as KubeError};

    use crate::materialization::RenderedObjectRef;

    use super::{
        api_resource_for_ref, backend_endpoint_for_service, endpoint_slice_has_ready_endpoint,
        is_not_found, is_valid_uri_scheme, kube_error, rendered_service_ref,
        KubeMaterializerClientConfig,
    };

    #[test]
    fn maps_rendered_refs_to_api_resources() {
        let cases = [
            (
                object_ref("apps/v1", "Deployment", "apps", "app-a"),
                ("apps", "v1", "apps/v1", "Deployment", "deployments"),
            ),
            (
                object_ref("apps/v1", "StatefulSet", "data", "db-a"),
                ("apps", "v1", "apps/v1", "StatefulSet", "statefulsets"),
            ),
            (
                object_ref("v1", "Service", "apps", "svc-a"),
                ("", "v1", "v1", "Service", "services"),
            ),
            (
                object_ref("v1", "PersistentVolume", "", "pv-a"),
                ("", "v1", "v1", "PersistentVolume", "persistentvolumes"),
            ),
            (
                object_ref("v1", "PersistentVolumeClaim", "data", "pvc-a"),
                (
                    "",
                    "v1",
                    "v1",
                    "PersistentVolumeClaim",
                    "persistentvolumeclaims",
                ),
            ),
        ];

        for (object, expected) in cases {
            let resource = api_resource_for_ref(&object).expect("supported API resource");

            assert_eq!(
                (
                    resource.group.as_str(),
                    resource.version.as_str(),
                    resource.api_version.as_str(),
                    resource.kind.as_str(),
                    resource.plural.as_str(),
                ),
                expected
            );
        }
    }

    #[test]
    fn rejects_namespaced_persistent_volume_ref() {
        let error = api_resource_for_ref(&object_ref("v1", "PersistentVolume", "data", "pv-a"))
            .expect_err("PV is cluster scoped");

        assert!(error
            .message()
            .contains("unsupported rendered Kubernetes object"));
    }

    #[test]
    fn default_config_uses_http_service_backend_uri() {
        let service = service("api", "apps", [80, 8080]);
        let config = KubeMaterializerClientConfig::default();

        let backend =
            backend_endpoint_for_service(&service, &config).expect("service backend endpoint");

        assert_eq!(backend.uri(), "http://api.apps.svc.cluster.local:80");
    }

    #[test]
    fn configurable_scheme_changes_service_backend_uri() {
        let service = service("postgres", "data", [5432]);
        let config = KubeMaterializerClientConfig {
            backend_scheme: "tcp".to_owned(),
            ..KubeMaterializerClientConfig::default()
        };

        let backend =
            backend_endpoint_for_service(&service, &config).expect("service backend endpoint");

        assert_eq!(backend.uri(), "tcp://postgres.data.svc.cluster.local:5432");
    }

    #[test]
    fn service_annotation_overrides_default_backend_scheme() {
        let mut service = service("passthrough", "apps", [9443]);
        service.metadata.annotations = Some(BTreeMap::from([(
            "sleepypods.io/backend-scheme".to_owned(),
            "tcp".to_owned(),
        )]));
        let config = KubeMaterializerClientConfig::default();

        let backend =
            backend_endpoint_for_service(&service, &config).expect("service backend endpoint");

        assert_eq!(
            backend.uri(),
            "tcp://passthrough.apps.svc.cluster.local:9443"
        );
    }

    #[test]
    fn endpoint_slice_ready_defaults_match_kubernetes_semantics() {
        assert!(endpoint_slice_has_ready_endpoint(&endpoint_slice([None])));
        assert!(endpoint_slice_has_ready_endpoint(&endpoint_slice([Some(
            true
        )])));
        assert!(!endpoint_slice_has_ready_endpoint(&endpoint_slice([Some(
            false
        )])));
    }

    #[test]
    fn validates_timeout_configuration() {
        let config = KubeMaterializerClientConfig {
            poll_interval: Duration::ZERO,
            ..KubeMaterializerClientConfig::default()
        };

        let error = config.validate().expect_err("zero poll interval rejected");

        assert_eq!(error.field(), "poll_interval");
    }

    #[test]
    fn validates_backend_scheme_configuration() {
        assert!(is_valid_uri_scheme("http"));
        assert!(is_valid_uri_scheme("tcp"));
        assert!(is_valid_uri_scheme("h2c+unix"));

        for backend_scheme in ["", " ", "1http", "http://", "tcp:"] {
            let config = KubeMaterializerClientConfig {
                backend_scheme: backend_scheme.to_owned(),
                ..KubeMaterializerClientConfig::default()
            };

            let error = config
                .validate()
                .expect_err("invalid backend scheme rejected");

            assert_eq!(error.field(), "backend_scheme");
        }
    }

    #[test]
    fn delete_treats_not_found_as_success() {
        let error = KubeError::Api(Box::new(Status {
            code: 404,
            reason: "NotFound".to_owned(),
            ..Status::default()
        }));

        assert!(is_not_found(&error));
    }

    #[test]
    fn kube_conflict_errors_are_retryable_materializer_errors() {
        let error = kube_error(KubeError::Api(Box::new(Status {
            code: 409,
            reason: "Conflict".to_owned(),
            message: "resource version conflict".to_owned(),
            ..Status::default()
        })));

        assert!(error.is_retryable());
        assert!(error.message().contains("resource version conflict"));
    }

    #[test]
    fn kube_invalid_errors_are_permanent_materializer_errors() {
        let error = kube_error(KubeError::Api(Box::new(Status {
            code: 422,
            reason: "Invalid".to_owned(),
            message: "manifest is invalid".to_owned(),
            ..Status::default()
        })));

        assert!(!error.is_retryable());
        assert!(error.message().contains("manifest is invalid"));
    }

    #[test]
    fn finds_rendered_service_ref_for_readiness() {
        let refs = [
            object_ref("apps/v1", "Deployment", "apps", "app-a"),
            object_ref("v1", "Service", "apps", "svc-a"),
        ];

        let service = rendered_service_ref(&refs).expect("service ref");

        assert_eq!(service.name, "svc-a");
    }

    fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
        RenderedObjectRef {
            api_version: api_version.to_owned(),
            kind: kind.to_owned(),
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
    }

    fn service<const N: usize>(name: &str, namespace: &str, ports: [i32; N]) -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(namespace.to_owned()),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(
                    ports
                        .into_iter()
                        .map(|port| ServicePort {
                            port,
                            ..ServicePort::default()
                        })
                        .collect(),
                ),
                ..ServiceSpec::default()
            }),
            ..Service::default()
        }
    }

    fn endpoint_slice<const N: usize>(ready_values: [Option<bool>; N]) -> EndpointSlice {
        EndpointSlice {
            endpoints: ready_values
                .into_iter()
                .map(|ready| Endpoint {
                    conditions: Some(EndpointConditions {
                        ready,
                        ..EndpointConditions::default()
                    }),
                    ..Endpoint::default()
                })
                .collect(),
            ..EndpointSlice::default()
        }
    }
}
