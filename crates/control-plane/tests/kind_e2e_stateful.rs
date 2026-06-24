use std::{
    collections::HashMap,
    env,
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient, persistent_volume_source_template,
    route_identity, template_text_part, ContainerPortTemplate, ContainerTemplate,
    CreateInstanceRequest, CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
    DeleteInstanceRequest, GetInstanceRequest, HostPathVolumeSourceTemplate, HttpRouteIdentity,
    Instance, InstanceState as PbInstanceState, ManifestTemplate, PersistentVolumeAccessMode,
    PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate, ProtocolRoute, RouteHost,
    RouteHostKind, RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
    TemplateText, TemplateTextPart, VolumeTemplate, WorkloadClassVersionRef, WorkloadKind,
    WorkloadSleepPolicy, WorkloadTemplate, WorkloadValueSchema,
};
use k8s_openapi::{
    api::{
        apps::v1::StatefulSet,
        core::v1::{PersistentVolume, PersistentVolumeClaim, Pod, Service},
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{api::ListParams, Api, Client, Error as KubeError};
use tokio::time::{sleep, Instant};
use tonic::transport::{Channel, Endpoint};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CLASS_ID: &str = "stateful-web";
const INSTANCE_ID: &str = "e2e-stateful";
const ROUTE_ID: &str = "e2e-stateful-route";
const ROUTE_HOST: &str = "stateful.sleepypods.test";
const TENANT_VALUE: &str = "stateful";
const WORKLOAD_NAME: &str = "e2e-stateful-app";
const RENDERED_WORKLOAD_NAME: &str = "e2e-stateful-app-e2e-stat";
const RENDERED_PVC_NAME: &str = "e2e-stateful-pvc-e2e-stat";
const RENDERED_PV_NAME: &str = "e2e-stateful-pv-e2e-stat";
const VOLUME_NAME: &str = "data";
const MOUNT_PATH: &str = "/data";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const STATEFUL_CLEANUP_TIMEOUT: Duration = Duration::from_secs(180);

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-stateful.sh or an equivalent kind deployment"]
async fn stateful_volume_lifecycle_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_STATEFUL").as_deref() != Ok("1") {
        eprintln!("skipping stateful kind E2E because SLEEPYPODS_KIND_E2E_STATEFUL=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let marker = unique_marker()?;

    assert_single_node_cluster(kube.clone()).await?;
    create_operator_resources(&mut operator, &config).await?;
    let created = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;
    assert_eq!(created.generation, 0);

    let write_path = format!("/write/{marker}");
    eprintln!("stateful E2E: cold write through frontline");
    let first = wait_for_frontline_response(
        &config,
        "cold write request",
        &write_path,
        "wrote:",
        Duration::from_secs(180),
    )
    .await?;
    assert_response(&first, "cold write request", "wrote:")?;
    let running = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    if running.generation <= created.generation {
        return Err(format!(
            "expected wake to advance generation beyond {}, got {}",
            created.generation, running.generation
        )
        .into());
    }
    assert_materialized_stateful_objects(kube.clone(), &config).await?;

    let read_path = format!("/read/{marker}");
    eprintln!("stateful E2E: mounted read through warm frontend path");
    let second = wait_for_frontline_response(
        &config,
        "mounted read request",
        &read_path,
        "read:",
        Duration::from_secs(30),
    )
    .await?;
    assert_response(&second, "hot read request", "read:")?;

    let cold_after_idle = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Cold,
        STATEFUL_CLEANUP_TIMEOUT,
    )
    .await?;
    if cold_after_idle.generation <= running.generation {
        return Err(format!(
            "expected idle sleep to advance generation beyond {}, got {}",
            running.generation, cold_after_idle.generation
        )
        .into());
    }
    wait_for_materialized_objects_deleted(
        kube.clone(),
        &config.namespace,
        STATEFUL_CLEANUP_TIMEOUT,
    )
    .await?;

    sleep(Duration::from_secs(11)).await;
    eprintln!("stateful E2E: re-wake read through frontline");
    let rewake = wait_for_frontline_response(
        &config,
        "re-wake read request",
        &read_path,
        "read:",
        Duration::from_secs(180),
    )
    .await?;
    assert_response(&rewake, "re-wake read request", "read:")?;
    let running_after_rewake = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    if running_after_rewake.generation <= cold_after_idle.generation {
        return Err(format!(
            "expected re-wake to advance generation beyond {}, got {}",
            cold_after_idle.generation, running_after_rewake.generation
        )
        .into());
    }
    assert_materialized_stateful_objects(kube.clone(), &config).await?;

    let deleted = operator
        .delete_instance(DeleteInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err(
            "expected deployed operator DeleteInstance to delete the running instance".into(),
        );
    }
    wait_for_materialized_objects_deleted(kube, &config.namespace, STATEFUL_CLEANUP_TIMEOUT)
        .await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    frontline_addr: SocketAddr,
    app_image: String,
    sidecar_image: String,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-stateful".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19151".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19180".to_owned())
                .parse()?,
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/stateful-app:kind-e2e-stateful".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-stateful".to_owned()),
        })
    }

    fn host_path(&self) -> String {
        format!("/tmp/sleepypods-kind-e2e/{TENANT_VALUE}")
    }
}

async fn connect_operator(endpoint: &str) -> TestResult<OperatorControlPlaneClient<Channel>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let channel = Endpoint::from_shared(endpoint.to_owned())?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .connect()
            .await;
        match channel {
            Ok(channel) => return Ok(OperatorControlPlaneClient::new(channel)),
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for operator gRPC endpoint {endpoint}: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn create_operator_resources(
    operator: &mut OperatorControlPlaneClient<Channel>,
    config: &E2eConfig,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: "kind-e2e-stateful-create-class".to_owned(),
            class_id: CLASS_ID.to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: Some(WorkloadValueSchema {
                fields: Default::default(),
                allow_extra: true,
            }),
            template_generation: 1,
            template: Some(manifest_template(config)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 15_000,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
        })
        .await?;

    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: "kind-e2e-stateful-create-instance".to_owned(),
            instance_id: INSTANCE_ID.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: HashMap::from([("tenant".to_owned(), TENANT_VALUE.to_owned())]),
        })
        .await?;

    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: "kind-e2e-stateful-create-route".to_owned(),
            route_binding_id: ROUTE_ID.to_owned(),
            instance_id: INSTANCE_ID.to_owned(),
            identity: Some(RouteIdentity {
                kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
                    host: Some(RouteHost {
                        kind: RouteHostKind::Exact as i32,
                        host: ROUTE_HOST.to_owned(),
                    }),
                    path_prefix: None,
                })),
            }),
            protocol: ProtocolRoute::Http as i32,
        })
        .await?;

    Ok(())
}

async fn wait_for_instance_state(
    operator: &mut OperatorControlPlaneClient<Channel>,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let instance = operator
            .get_instance(GetInstanceRequest {
                instance_id: INSTANCE_ID.to_owned(),
            })
            .await?
            .into_inner();
        let actual =
            PbInstanceState::try_from(instance.state).unwrap_or(PbInstanceState::Unspecified);
        if actual == expected {
            return Ok(instance);
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for instance {INSTANCE_ID} to reach {expected:?}; last state was {actual:?} generation {}",
                instance.generation
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_frontline_response(
    config: &E2eConfig,
    context: &str,
    path: &str,
    expected_body: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, ROUTE_HOST, path).await {
            Ok(response) if response.status == 200 && response.body.contains(expected_body) => {
                return Ok(response);
            }
            Ok(response) => format!(
                "frontline returned HTTP {} with body {:?}",
                response.status, response.body
            ),
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for successful frontline response for {context} path {path}: {}",
                last_error
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn http_get(addr: SocketAddr, host: &str, path: &str) -> TestResult<HttpResponse> {
    let host = host.to_owned();
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || http_get_blocking(addr, &host, &path))
        .await
        .map_err(|error| format!("HTTP request task failed: {error}"))?
}

fn http_get_blocking(addr: SocketAddr, host: &str, path: &str) -> TestResult<HttpResponse> {
    let timeout = Duration::from_secs(180);
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;

    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    let raw = String::from_utf8_lossy(&bytes);
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| format!("HTTP response from {addr} did not include a status line"))?
        .parse::<u16>()?;

    Ok(HttpResponse {
        status,
        body: body.to_owned(),
    })
}

fn assert_response(response: &HttpResponse, context: &str, expected_body: &str) -> TestResult<()> {
    if response.status != 200 {
        return Err(format!("{context} returned HTTP {}", response.status).into());
    }
    if !response.body.contains(expected_body) {
        return Err(format!(
            "{context} body did not include {expected_body:?}: {:?}",
            response.body
        )
        .into());
    }

    Ok(())
}

async fn assert_materialized_stateful_objects(kube: Client, config: &E2eConfig) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(kube.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), &config.namespace);
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), &config.namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), &config.namespace);
    let pods: Api<Pod> = Api::namespaced(kube, &config.namespace);

    let pv = pvs.get(RENDERED_PV_NAME).await?;
    let pvc = pvcs.get(RENDERED_PVC_NAME).await?;
    let stateful_set = stateful_sets.get(RENDERED_WORKLOAD_NAME).await?;
    let service = services.get(RENDERED_WORKLOAD_NAME).await?;

    assert_resource_order(&pv, &pvc, &stateful_set)?;
    assert_pv_and_pvc(&pv, &pvc, config)?;
    assert_stateful_set_shape(&stateful_set, config)?;
    assert_service_targets_sidecar(&service)?;
    assert_stateful_pod_mounts_pvc(&pods).await?;

    Ok(())
}

fn assert_resource_order(
    pv: &PersistentVolume,
    pvc: &PersistentVolumeClaim,
    stateful_set: &StatefulSet,
) -> TestResult<()> {
    let pv_rv = resource_version(pv.metadata.resource_version.as_deref(), RENDERED_PV_NAME)?;
    let pvc_rv = resource_version(pvc.metadata.resource_version.as_deref(), RENDERED_PVC_NAME)?;
    let stateful_rv = resource_version(
        stateful_set.metadata.resource_version.as_deref(),
        RENDERED_WORKLOAD_NAME,
    )?;
    if !(pv_rv < pvc_rv && pvc_rv < stateful_rv) {
        return Err(format!(
            "expected PV/PVC/StatefulSet resourceVersions to reflect apply order {RENDERED_PV_NAME} < {RENDERED_PVC_NAME} < {RENDERED_WORKLOAD_NAME}; got {pv_rv}, {pvc_rv}, {stateful_rv}"
        )
        .into());
    }
    Ok(())
}

fn resource_version(value: Option<&str>, name: &str) -> TestResult<u64> {
    value
        .ok_or_else(|| format!("{name} is missing metadata.resourceVersion"))?
        .parse()
        .map_err(|error| format!("{name} resourceVersion is not numeric: {error}").into())
}

fn assert_pv_and_pvc(
    pv: &PersistentVolume,
    pvc: &PersistentVolumeClaim,
    config: &E2eConfig,
) -> TestResult<()> {
    let actual_path = pv
        .spec
        .as_ref()
        .and_then(|spec| spec.host_path.as_ref())
        .map(|source| source.path.as_str());
    if actual_path != Some(config.host_path().as_str()) {
        return Err(format!(
            "expected PersistentVolume {RENDERED_PV_NAME} hostPath {}, got {actual_path:?}",
            config.host_path()
        )
        .into());
    }
    let claim_ref = pv
        .spec
        .as_ref()
        .and_then(|spec| spec.claim_ref.as_ref())
        .ok_or("PersistentVolume is missing claimRef")?;
    if claim_ref.namespace.as_deref() != Some(config.namespace.as_str())
        || claim_ref.name.as_deref() != Some(RENDERED_PVC_NAME)
    {
        return Err(format!(
            "expected PersistentVolume claimRef {}/{}, got {:?}/{:?}",
            config.namespace, RENDERED_PVC_NAME, claim_ref.namespace, claim_ref.name
        )
        .into());
    }

    let volume_name = pvc
        .spec
        .as_ref()
        .and_then(|spec| spec.volume_name.as_deref());
    if volume_name != Some(RENDERED_PV_NAME) {
        return Err(format!(
            "expected PersistentVolumeClaim {RENDERED_PVC_NAME} to target PV {RENDERED_PV_NAME}, got {volume_name:?}"
        )
        .into());
    }
    let phase = pvc
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    if phase != Some("Bound") {
        return Err(format!(
            "expected PersistentVolumeClaim {RENDERED_PVC_NAME} to be Bound, got {phase:?}"
        )
        .into());
    }
    Ok(())
}

fn assert_stateful_set_shape(stateful_set: &StatefulSet, config: &E2eConfig) -> TestResult<()> {
    let spec = stateful_set
        .spec
        .as_ref()
        .ok_or("materialized StatefulSet is missing spec")?;
    if spec.service_name.as_deref() != Some(RENDERED_WORKLOAD_NAME) {
        return Err(format!(
            "expected StatefulSet serviceName {RENDERED_WORKLOAD_NAME}, got {:?}",
            spec.service_name
        )
        .into());
    }
    let pod_spec = spec
        .template
        .spec
        .as_ref()
        .ok_or("materialized StatefulSet is missing pod spec")?;
    let app = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "app")
        .ok_or("materialized StatefulSet is missing app container")?;
    if app.image.as_deref() != Some(config.app_image.as_str()) {
        return Err(format!(
            "expected app image {}, got {:?}",
            config.app_image, app.image
        )
        .into());
    }
    let app_mount = app
        .volume_mounts
        .as_ref()
        .and_then(|mounts| mounts.iter().find(|mount| mount.name == VOLUME_NAME))
        .ok_or("app container is missing data volume mount")?;
    if app_mount.mount_path != MOUNT_PATH {
        return Err(format!(
            "expected app mount path {MOUNT_PATH}, got {}",
            app_mount.mount_path
        )
        .into());
    }

    let sidecar = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "sleepypods-sidecar")
        .ok_or("materialized StatefulSet is missing sidecar container")?;
    if sidecar.image.as_deref() != Some(config.sidecar_image.as_str()) {
        return Err(format!(
            "expected sidecar image {}, got {:?}",
            config.sidecar_image, sidecar.image
        )
        .into());
    }
    assert_env(sidecar, "SLEEPYPODS_SIDECAR_LISTEN_ADDR", "0.0.0.0:15000")?;
    assert_env(
        sidecar,
        "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
        &format!(
            "http://sleepypods-control-plane.{}.svc.cluster.local:50051",
            config.namespace
        ),
    )?;

    let volume = pod_spec
        .volumes
        .as_ref()
        .and_then(|volumes| volumes.iter().find(|volume| volume.name == VOLUME_NAME))
        .ok_or("StatefulSet pod spec is missing data volume")?;
    let claim_name = volume
        .persistent_volume_claim
        .as_ref()
        .map(|claim| claim.claim_name.as_str());
    if claim_name != Some(RENDERED_PVC_NAME) {
        return Err(format!(
            "expected pod volume to use PVC {RENDERED_PVC_NAME}, got {claim_name:?}"
        )
        .into());
    }

    Ok(())
}

fn assert_service_targets_sidecar(service: &Service) -> TestResult<()> {
    let target_port = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .and_then(|ports| ports.first())
        .and_then(|port| port.target_port.as_ref())
        .ok_or("materialized Service is missing targetPort")?;
    if target_port != &IntOrString::Int(SIDECAR_PORT as i32) {
        return Err(
            format!("expected Service targetPort {SIDECAR_PORT}, got {target_port:?}").into(),
        );
    }
    Ok(())
}

async fn assert_stateful_pod_mounts_pvc(pods: &Api<Pod>) -> TestResult<()> {
    let pod = pods
        .list(&ListParams::default().labels(&format!(
            "sleepypods.io/instance-id={INSTANCE_ID},sleepypods.io/workload-name={RENDERED_WORKLOAD_NAME}"
        )))
        .await?
        .items
        .into_iter()
        .next()
        .ok_or("materialized StatefulSet pod was not found")?;
    let pod_name = pod
        .metadata
        .name
        .as_deref()
        .ok_or("StatefulSet pod is missing metadata.name")?
        .to_owned();
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    if phase != Some("Running") {
        return Err(
            format!("expected StatefulSet pod {pod_name} to be Running, got {phase:?}").into(),
        );
    }
    let volume = pod
        .spec
        .as_ref()
        .and_then(|spec| spec.volumes.as_ref())
        .and_then(|volumes| volumes.iter().find(|volume| volume.name == VOLUME_NAME))
        .ok_or("StatefulSet pod is missing data volume")?;
    let claim_name = volume
        .persistent_volume_claim
        .as_ref()
        .map(|claim| claim.claim_name.as_str());
    if claim_name != Some(RENDERED_PVC_NAME) {
        return Err(format!(
            "expected StatefulSet pod to use PVC {RENDERED_PVC_NAME}, got {claim_name:?}"
        )
        .into());
    }
    Ok(())
}

fn assert_env(
    container: &k8s_openapi::api::core::v1::Container,
    name: &str,
    expected: &str,
) -> TestResult<()> {
    let value = container
        .env
        .as_ref()
        .and_then(|vars| vars.iter().find(|var| var.name == name))
        .and_then(|var| var.value.as_deref())
        .ok_or_else(|| format!("missing env var {name}"))?;
    if value != expected {
        return Err(format!("expected env {name}={expected:?}, got {value:?}").into());
    }
    Ok(())
}

async fn wait_for_materialized_objects_deleted(
    kube: Client,
    namespace: &str,
    timeout: Duration,
) -> TestResult<()> {
    let pvs: Api<PersistentVolume> = Api::all(kube.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + timeout;
    loop {
        let stateful_absent = is_not_found(stateful_sets.get(RENDERED_WORKLOAD_NAME).await);
        let service_absent = is_not_found(services.get(RENDERED_WORKLOAD_NAME).await);
        let pvc_absent = is_not_found(pvcs.get(RENDERED_PVC_NAME).await);
        let pv_absent = is_not_found(pvs.get(RENDERED_PV_NAME).await);
        if stateful_absent && service_absent && pvc_absent && pv_absent {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for materialized StatefulSet/Service/PVC/PV objects for {namespace}/{RENDERED_WORKLOAD_NAME} to be deleted"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn assert_single_node_cluster(kube: Client) -> TestResult<()> {
    let nodes: Api<k8s_openapi::api::core::v1::Node> = Api::all(kube);
    let node_count = nodes.list(&ListParams::default()).await?.items.len();
    if node_count != 1 {
        return Err(format!(
            "stateful hostPath continuity E2E requires exactly one Kubernetes node; found {node_count}"
        )
        .into());
    }
    Ok(())
}

fn is_not_found<T>(result: Result<T, KubeError>) -> bool {
    matches!(result, Err(KubeError::Api(status)) if status.is_not_found())
}

fn manifest_template(config: &E2eConfig) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::StatefulSet as i32,
            name: Some(literal_text(WORKLOAD_NAME)),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "app".to_owned(),
                image: Some(literal_text(&config.app_image)),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: APP_PORT,
                }],
                env: Vec::new(),
            }),
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text(&config.sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: None,
        }),
        service: Some(ServiceTemplate {
            name: Some(literal_text(WORKLOAD_NAME)),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: APP_PORT,
                target_port: APP_PORT,
            }],
        }),
        volumes: vec![VolumeTemplate {
            name: VOLUME_NAME.to_owned(),
            mount_path: Some(literal_text(MOUNT_PATH)),
            pv_name: Some(tenant_suffixed_text("e2e-", "-pv")),
            pvc_name: Some(tenant_suffixed_text("e2e-", "-pvc")),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
            capacity: Some(literal_text("1Mi")),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
            storage_class_name: Some(literal_text("sleepypods-kind-static")),
            source: Some(PersistentVolumeSourceTemplate {
                kind: Some(persistent_volume_source_template::Kind::HostPath(
                    HostPathVolumeSourceTemplate {
                        path: Some(tenant_suffixed_text("/tmp/sleepypods-kind-e2e/", "")),
                        r#type: Some(literal_text("DirectoryOrCreate")),
                    },
                )),
            }),
        }],
    }
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}

fn tenant_suffixed_text(prefix: &str, suffix: &str) -> TemplateText {
    TemplateText {
        parts: vec![
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(prefix.to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::InstanceValue("tenant".to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(suffix.to_owned())),
            },
        ],
    }
}

fn unique_marker() -> TestResult<String> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(format!("marker-{millis}-{}", std::process::id()))
}
