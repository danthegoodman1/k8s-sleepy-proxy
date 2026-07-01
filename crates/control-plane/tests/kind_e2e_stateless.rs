use std::{
    env,
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use control_plane::{
    api::pb::{
        operator_control_plane_client::OperatorControlPlaneClient,
        proxy_control_plane_client::ProxyControlPlaneClient, route_identity,
        sidecar_control_plane_client::SidecarControlPlaneClient, template_text_part,
        ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest, CreateRouteBindingRequest,
        CreateWorkloadClassVersionRequest, GetInstanceRequest, HttpRouteIdentity, Instance,
        InstanceState as PbInstanceState, ManifestTemplate, ProtocolRoute,
        ProxyWakeInstanceRequest, RouteHost, RouteHostKind, RouteIdentity, ServicePortTemplate,
        ServiceTemplate, SidecarReportIdleRequest, SidecarTemplate, TemplateText, TemplateTextPart,
        WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy, WorkloadTemplate,
        WorkloadValueSchema,
    },
    BearerToken, OptionalBearerTokenInterceptor,
};
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{Pod, Service},
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{
    api::{ListParams, LogParams},
    Api, Client, Error as KubeError,
};
use tokio::time::{sleep, Instant};
use tonic::{
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
    Code,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CLASS_ID: &str = "stateless-web";
const INSTANCE_ID: &str = "e2e-stateless";
const ROUTE_ID: &str = "e2e-stateless-route";
const ROUTE_HOST: &str = "e2e.sleepypods.test";
const WORKLOAD_NAME: &str = "e2e-app";
const RENDERED_WORKLOAD_NAME: &str = "e2e-app-e2e-stat";
const APP_RESPONSE: &str = "sleepypods-stateless-app";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-stateless.sh or an equivalent kind deployment"]
async fn stateless_http_lifecycle_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_STATELESS").as_deref() != Ok("1") {
        eprintln!("skipping stateless kind E2E because SLEEPYPODS_KIND_E2E_STATELESS=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint, &config.operator_token).await?;

    assert_invalid_control_plane_credentials_fail(&config).await?;
    create_operator_resources(&mut operator, &config).await?;
    let created = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;
    assert_eq!(created.generation, 0);

    let first = wait_for_frontline_response(&config, Duration::from_secs(180)).await?;
    assert_response(&first, "cold wake request")?;
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
    assert_materialized_deployment_and_service(kube.clone(), &config).await?;

    let second = wait_for_frontline_response(&config, Duration::from_secs(30)).await?;
    assert_response(&second, "second request")?;
    assert_frontline_hot_cache_metrics(kube.clone(), &config.namespace, Duration::from_secs(30))
        .await?;

    let cold_after_idle = wait_for_instance_state(
        &mut operator,
        PbInstanceState::Cold,
        Duration::from_secs(90),
    )
    .await?;
    if cold_after_idle.generation <= running.generation {
        return Err(format!(
            "expected idle sleep to advance generation beyond {}, got {}",
            running.generation, cold_after_idle.generation
        )
        .into());
    }
    wait_for_materialized_objects_deleted(kube, &config.namespace, Duration::from_secs(90)).await?;

    sleep(Duration::from_secs(11)).await;
    let rewake = wait_for_frontline_response(&config, Duration::from_secs(180)).await?;
    assert_response(&rewake, "re-wake request")?;
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

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    frontline_addr: SocketAddr,
    app_image: String,
    sidecar_image: String,
    operator_token: String,
    sidecar_token: String,
    invalid_token: String,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FrontlineMetricCounts {
    subscribe_route_success: usize,
    cache_hits: usize,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-stateless".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19051".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19080".to_owned())
                .parse()?,
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/stateless-app:kind-e2e-stateless".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-stateless".to_owned()),
            operator_token: env::var("SLEEPYPODS_E2E_OPERATOR_TOKEN")
                .unwrap_or_else(|_| "operator-token".to_owned()),
            sidecar_token: env::var("SLEEPYPODS_E2E_SIDECAR_TOKEN")
                .unwrap_or_else(|_| "sidecar-token".to_owned()),
            invalid_token: env::var("SLEEPYPODS_E2E_INVALID_TOKEN")
                .unwrap_or_else(|_| "invalid-token".to_owned()),
        })
    }
}

type AuthenticatedChannel = InterceptedService<Channel, OptionalBearerTokenInterceptor>;

async fn connect_operator(
    endpoint: &str,
    token: &str,
) -> TestResult<OperatorControlPlaneClient<AuthenticatedChannel>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let channel = Endpoint::from_shared(endpoint.to_owned())?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .connect()
            .await;
        match channel {
            Ok(channel) => {
                return Ok(OperatorControlPlaneClient::with_interceptor(
                    channel,
                    token_interceptor(token)?,
                ));
            }
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for operator gRPC endpoint {endpoint}: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn token_interceptor(token: &str) -> TestResult<OptionalBearerTokenInterceptor> {
    let token = BearerToken::new("kind_e2e_token", token.to_owned())?;
    Ok(OptionalBearerTokenInterceptor::new(Some(&token))?)
}

async fn assert_invalid_control_plane_credentials_fail(config: &E2eConfig) -> TestResult<()> {
    let channel = Endpoint::from_shared(config.operator_endpoint.clone())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(10))
        .connect()
        .await?;

    let mut operator = OperatorControlPlaneClient::with_interceptor(
        channel.clone(),
        token_interceptor(&config.invalid_token)?,
    );
    let operator_error = operator
        .get_instance(GetInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
        })
        .await
        .expect_err("invalid operator credentials must fail");
    assert_eq!(operator_error.code(), Code::Unauthenticated);

    let mut proxy = ProxyControlPlaneClient::with_interceptor(
        channel.clone(),
        token_interceptor(&config.invalid_token)?,
    );
    let proxy_error = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: INSTANCE_ID.to_owned(),
            expected_generation: 0,
            backend_generation: None,
        })
        .await
        .expect_err("invalid proxy credentials must fail");
    assert_eq!(proxy_error.code(), Code::Unauthenticated);

    let mut sidecar = SidecarControlPlaneClient::with_interceptor(
        channel,
        token_interceptor(&config.invalid_token)?,
    );
    let sidecar_error = sidecar
        .report_idle(SidecarReportIdleRequest {
            instance_id: INSTANCE_ID.to_owned(),
            expected_generation: 0,
            active_count: 0,
        })
        .await
        .expect_err("invalid sidecar credentials must fail");
    assert_eq!(sidecar_error.code(), Code::Unauthenticated);

    Ok(())
}

async fn create_operator_resources(
    operator: &mut OperatorControlPlaneClient<AuthenticatedChannel>,
    config: &E2eConfig,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: "kind-e2e-create-class".to_owned(),
            class_id: CLASS_ID.to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: Some(WorkloadValueSchema {
                fields: Default::default(),
                allow_extra: false,
            }),
            template_generation: 1,
            template: Some(manifest_template(config)),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 2_000,
                idle_retry_backoff_ms: 500,
                drain_grace_timeout_ms: 500,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        })
        .await?;

    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: "kind-e2e-create-instance".to_owned(),
            instance_id: INSTANCE_ID.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: CLASS_ID.to_owned(),
                version: 1,
            }),
            values: Default::default(),
        })
        .await?;

    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: "kind-e2e-create-route".to_owned(),
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
    operator: &mut OperatorControlPlaneClient<AuthenticatedChannel>,
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
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    let mut last_error: String;
    loop {
        match http_get(config.frontline_addr, ROUTE_HOST, "/").await {
            Ok(response) if response.status == 200 && response.body.contains(APP_RESPONSE) => {
                return Ok(response);
            }
            Ok(response) => {
                last_error = format!(
                    "frontline returned HTTP {} with body {:?}",
                    response.status, response.body
                );
            }
            Err(error) => {
                last_error = error.to_string();
            }
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for successful frontline response: {}",
                last_error
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn http_get(
    addr: SocketAddr,
    host: &'static str,
    path: &'static str,
) -> TestResult<HttpResponse> {
    tokio::task::spawn_blocking(move || http_get_blocking(addr, host, path))
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

fn assert_response(response: &HttpResponse, context: &str) -> TestResult<()> {
    if response.status != 200 {
        return Err(format!("{context} returned HTTP {}", response.status).into());
    }
    if !response.body.contains(APP_RESPONSE) {
        return Err(format!(
            "{context} body did not include {APP_RESPONSE:?}: {:?}",
            response.body
        )
        .into());
    }

    Ok(())
}

async fn assert_materialized_deployment_and_service(
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), &config.namespace);
    let services: Api<Service> = Api::namespaced(kube, &config.namespace);
    let deployment = deployments.get(RENDERED_WORKLOAD_NAME).await?;
    let pod_spec = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .ok_or("materialized Deployment is missing pod spec")?;
    let app = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "app")
        .ok_or("materialized Deployment is missing app container")?;
    if app.image.as_deref() != Some(config.app_image.as_str()) {
        return Err(format!(
            "expected app image {}, got {:?}",
            config.app_image, app.image
        )
        .into());
    }
    let sidecar = pod_spec
        .containers
        .iter()
        .find(|container| container.name == "sleepypods-sidecar")
        .ok_or("materialized Deployment is missing sidecar container")?;
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
    assert_env(
        sidecar,
        "SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN",
        &config.sidecar_token,
    )?;

    let service = services.get(RENDERED_WORKLOAD_NAME).await?;
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
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + timeout;
    loop {
        let deployment_absent = is_not_found(deployments.get(RENDERED_WORKLOAD_NAME).await);
        let service_absent = is_not_found(services.get(RENDERED_WORKLOAD_NAME).await);
        if deployment_absent && service_absent {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for materialized Deployment/Service {namespace}/{RENDERED_WORKLOAD_NAME} to be deleted"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn assert_frontline_hot_cache_metrics(
    kube: Client,
    namespace: &str,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let counts = frontline_metric_counts(kube.clone(), namespace).await?;
        if counts.subscribe_route_success >= 1 && counts.cache_hits >= 1 {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "expected at least one SubscribeRoute success and one route-cache hit in frontline logs; saw {counts:?}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn frontline_metric_counts(
    kube: Client,
    namespace: &str,
) -> TestResult<FrontlineMetricCounts> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let pod = pods
        .list(&ListParams::default().labels("app.kubernetes.io/name=sleepypods-frontline"))
        .await?
        .items
        .into_iter()
        .next()
        .ok_or("frontline pod was not found")?;
    let pod_name = pod
        .metadata
        .name
        .ok_or("frontline pod is missing metadata.name")?;
    let logs = pods
        .logs(
            &pod_name,
            &LogParams {
                container: Some("frontline".to_owned()),
                ..LogParams::default()
            },
        )
        .await?;
    Ok(FrontlineMetricCounts {
        subscribe_route_success: logs
            .lines()
            .filter(|line| {
                line.contains("metric.name=sleepypods_runtime_control_plane_calls_total")
                    && line.contains("metric.labels=operation=subscribe_route,outcome=success")
            })
            .count(),
        cache_hits: logs
            .lines()
            .filter(|line| {
                line.contains("metric.name=sleepypods_runtime_route_cache_lookups_total")
                    && line.contains("metric.labels=outcome=hit")
            })
            .count(),
    })
}

fn is_not_found<T>(result: Result<T, KubeError>) -> bool {
    matches!(result, Err(KubeError::Api(status)) if status.is_not_found())
}

fn manifest_template(config: &E2eConfig) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
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
        volumes: Vec::new(),
        raw_objects: Vec::new(),
    }
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}
