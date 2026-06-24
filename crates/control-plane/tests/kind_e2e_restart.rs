use std::{
    collections::HashMap,
    env,
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient, route_identity, template_text_part,
    ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest, CreateRouteBindingRequest,
    CreateWorkloadClassVersionRequest, DeleteHttp01ChallengeRequest, DeleteInstanceRequest,
    DeleteRouteBindingRequest, EnvVarTemplate, ExpireHttp01ChallengesRequest, GetInstanceRequest,
    Http01ChallengeKey, HttpRouteIdentity, Instance, InstanceState as PbInstanceState,
    ManifestTemplate, ProtocolRoute, PutHttp01ChallengeRequest, ResolveHttp01ChallengeRequest,
    RouteHost, RouteHostKind, RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
    TemplateText, TemplateTextPart, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy,
    WorkloadTemplate, WorkloadValueFieldRule, WorkloadValueSchema,
};
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{Pod, Service},
};
use kube::{
    api::{DeleteParams, ListParams, Patch, PatchParams},
    Api, Client, Error as KubeError,
};
use serde_json::json;
use tokio::time::{sleep, timeout, Instant};
use tonic::{
    transport::{Channel, Endpoint},
    Code,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const NORMAL_CLASS_ID: &str = "restart-routing";
const FAST_SLEEP_CLASS_ID: &str = "restart-fast-sleep";
const LATE_CLASS_ID: &str = "restart-late-wake";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const APP_MARKER: &str = "sleepypods-routing-app";
const CONTROL_PLANE_DEPLOYMENT: &str = "sleepypods-control-plane";
const CONTROL_PLANE_LABEL: &str = "app.kubernetes.io/name=sleepypods-control-plane";
const WORKLOAD_NAME_LABEL: &str = "sleepypods.io/workload-name";
const INSTANCE_GENERATION_LABEL: &str = "sleepypods.io/instance-generation";
const WAKE_HOST: &str = "wake.restart.sleepypods.test";
const SLEEP_HOST: &str = "sleep.restart.sleepypods.test";
const DELETE_HOST: &str = "delete.restart.sleepypods.test";
const REASSIGN_HOST: &str = "reassign.restart.sleepypods.test";
const HTTP01_HOST: &str = "http01.restart.sleepypods.test";
const HTTP01_TOKEN: &str = "restart-token";
const HTTP01_KEY_AUTHORIZATION: &str = "restart-token.key-authorization";
const HTTP01_EXPIRING_TOKEN: &str = "restart-expiring-token";
const HTTP01_EXPIRING_KEY_AUTHORIZATION: &str = "restart-expiring-token.key-authorization";

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-restart.sh or an equivalent kind deployment"]
async fn control_plane_restart_recovery_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_RESTART").as_deref() != Ok("1") {
        eprintln!("skipping restart kind E2E because SLEEPYPODS_KIND_E2E_RESTART=1 is not set");
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    create_workload_class(
        &mut operator,
        LATE_CLASS_ID,
        &config.late_app_image,
        &config.sidecar_image,
        sleep_policy(900_000),
        "late",
    )
    .await?;
    create_workload_class(
        &mut operator,
        NORMAL_CLASS_ID,
        &config.app_image,
        &config.sidecar_image,
        sleep_policy(900_000),
        "normal",
    )
    .await?;
    create_workload_class(
        &mut operator,
        FAST_SLEEP_CLASS_ID,
        &config.app_image,
        &config.sidecar_image,
        sleep_policy(5_000),
        "fast-sleep",
    )
    .await?;

    eprintln!("==> restart E2E: wake recovery");
    restart_during_wake_recovers_without_stale_backend(&mut operator, kube.clone(), &config)
        .await?;
    operator = connect_operator(&config.operator_endpoint).await?;

    eprintln!("==> restart E2E: sleep recovery");
    restart_during_sleep_report_recovers(&mut operator, kube.clone(), &config).await?;
    operator = connect_operator(&config.operator_endpoint).await?;

    eprintln!("==> restart E2E: delete recovery");
    restart_before_delete_retry_finalizes_cleanup(&mut operator, kube.clone(), &config).await?;
    operator = connect_operator(&config.operator_endpoint).await?;

    eprintln!("==> restart E2E: HTTP-01 recovery");
    http01_challenges_survive_restart_and_cleanup(&mut operator, kube.clone(), &config).await?;
    operator = connect_operator(&config.operator_endpoint).await?;

    eprintln!("==> restart E2E: route reassignment recovery");
    route_reassignment_after_restart_does_not_serve_stale_backend(&mut operator, kube, &config)
        .await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    frontline_addr: SocketAddr,
    cluster_name: String,
    app_image: String,
    late_app_image: String,
    sidecar_image: String,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: String,
}

impl E2eConfig {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            namespace: env::var("SLEEPYPODS_E2E_NAMESPACE")
                .unwrap_or_else(|_| "sleepypods-e2e-restart".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19751".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19780".to_owned())
                .parse()?,
            cluster_name: env::var("SLEEPYPODS_KIND_CLUSTER")
                .unwrap_or_else(|_| "sleepypods-e2e-restart-test".to_owned()),
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/routing-app:kind-e2e-restart".to_owned()),
            late_app_image: env::var("SLEEPYPODS_E2E_LATE_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/routing-app-late:kind-e2e-restart".to_owned()),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-restart".to_owned()),
        })
    }
}

async fn restart_during_wake_recovers_without_stale_backend(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        InstanceRouteSpec {
            class_id: LATE_CLASS_ID,
            instance_id: "restart-wake",
            route_id: "restart-wake-route",
            host: WAKE_HOST,
            target: "wake",
        },
        "wake",
    )
    .await?;
    let created = wait_for_instance_state(
        operator,
        "restart-wake",
        PbInstanceState::Cold,
        Duration::from_secs(30),
    )
    .await?;

    let wake_addr = config.frontline_addr;
    let wake = tokio::spawn(async move {
        http_get_with_timeout(wake_addr, WAKE_HOST, "/", Duration::from_secs(60)).await
    });

    let waking = wait_for_instance_state(
        operator,
        "restart-wake",
        PbInstanceState::Waking,
        Duration::from_secs(60),
    )
    .await?;
    if waking.generation <= created.generation {
        return Err(format!(
            "wake interruption did not advance generation beyond {}; got {}",
            created.generation, waking.generation
        )
        .into());
    }

    restart_control_plane_pod(kube.clone(), &config.namespace, &config.operator_endpoint).await?;
    load_late_image_into_kind(config)?;
    delete_workload_pods(kube.clone(), &config.namespace, "restart-wake").await?;
    let _ = timeout(Duration::from_secs(5), wake).await;

    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let response = wait_for_instance_response(
        config,
        "wake retry after control-plane restart",
        WAKE_HOST,
        "/",
        "wake",
        Duration::from_secs(180),
    )
    .await?;
    assert_instance_response(&response, "wake retry after control-plane restart", "wake")?;

    let running = wait_for_instance_state(
        &mut operator,
        "restart-wake",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    let expected_running_generation = waking.generation + 1;
    if running.generation != expected_running_generation {
        return Err(format!(
            "wake retry should complete exactly one Waking-to-Running transition from generation {} to {}; got {}",
            waking.generation, expected_running_generation, running.generation
        )
        .into());
    }
    assert_workload_generation(kube, &config.namespace, "restart-wake", waking.generation).await?;

    Ok(())
}

async fn restart_during_sleep_report_recovers(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        InstanceRouteSpec {
            class_id: FAST_SLEEP_CLASS_ID,
            instance_id: "restart-sleep",
            route_id: "restart-sleep-route",
            host: SLEEP_HOST,
            target: "sleep",
        },
        "sleep",
    )
    .await?;
    eprintln!("==> restart E2E: sleep recovery setup created");
    wait_for_instance_response(
        config,
        "sleep setup wake",
        SLEEP_HOST,
        "/",
        "sleep",
        Duration::from_secs(180),
    )
    .await?;
    wait_for_instance_state(
        operator,
        "restart-sleep",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;
    eprintln!("==> restart E2E: sleep recovery instance running");

    scale_control_plane(kube.clone(), &config.namespace, 0).await?;
    eprintln!("==> restart E2E: sleep recovery control-plane scaled down");
    sleep(Duration::from_secs(7)).await;
    scale_control_plane(kube.clone(), &config.namespace, 1).await?;
    eprintln!("==> restart E2E: sleep recovery control-plane scaled up");
    wait_for_instance_state_reconnecting(
        &config.operator_endpoint,
        "restart-sleep",
        PbInstanceState::Cold,
        Duration::from_secs(180),
    )
    .await?;
    eprintln!("==> restart E2E: sleep recovery instance cold");
    wait_for_workload_absent(
        kube,
        &config.namespace,
        "restart-sleep",
        Duration::from_secs(60),
    )
    .await?;

    Ok(())
}

async fn restart_before_delete_retry_finalizes_cleanup(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        InstanceRouteSpec {
            class_id: NORMAL_CLASS_ID,
            instance_id: "restart-delete",
            route_id: "restart-delete-route",
            host: DELETE_HOST,
            target: "delete",
        },
        "delete",
    )
    .await?;
    wait_for_instance_response(
        config,
        "delete setup wake",
        DELETE_HOST,
        "/",
        "delete",
        Duration::from_secs(180),
    )
    .await?;
    wait_for_instance_state(
        operator,
        "restart-delete",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;

    scale_control_plane(kube.clone(), &config.namespace, 0).await?;
    let unavailable_delete = timeout(
        Duration::from_secs(15),
        operator.delete_instance(DeleteInstanceRequest {
            instance_id: "restart-delete".to_owned(),
        }),
    )
    .await;
    if matches!(unavailable_delete, Ok(Ok(_))) {
        return Err(
            "delete unexpectedly succeeded while control-plane deployment was scaled to zero"
                .into(),
        );
    }

    scale_control_plane(kube.clone(), &config.namespace, 1).await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let deleted = operator
        .delete_instance(DeleteInstanceRequest {
            instance_id: "restart-delete".to_owned(),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err("delete retry after restart did not report deletion".into());
    }
    assert_instance_not_found(&mut operator, "restart-delete").await?;
    wait_for_workload_absent(
        kube,
        &config.namespace,
        "restart-delete",
        Duration::from_secs(60),
    )
    .await?;

    Ok(())
}

async fn route_reassignment_after_restart_does_not_serve_stale_backend(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance(
        operator,
        NORMAL_CLASS_ID,
        "restart-reassign-old",
        "old",
        "reassign-old",
    )
    .await?;
    create_instance(
        operator,
        NORMAL_CLASS_ID,
        "restart-reassign-new",
        "new",
        "reassign-new",
    )
    .await?;
    create_route(
        operator,
        "restart-reassign-old-route",
        "restart-reassign-old",
        REASSIGN_HOST,
        "reassign-old",
    )
    .await?;
    wait_for_instance_response(
        config,
        "route reassignment old route",
        REASSIGN_HOST,
        "/",
        "old",
        Duration::from_secs(180),
    )
    .await?;

    restart_control_plane_pod(kube, &config.namespace, &config.operator_endpoint).await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let deleted = operator
        .delete_route_binding(DeleteRouteBindingRequest {
            route_binding_id: "restart-reassign-old-route".to_owned(),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err("route reassignment did not delete the old route binding".into());
    }
    create_route(
        &mut operator,
        "restart-reassign-new-route",
        "restart-reassign-new",
        REASSIGN_HOST,
        "reassign-new",
    )
    .await?;

    wait_for_instance_response_rejecting_stale(
        config,
        "route reassignment immediately after control-plane restart",
        REASSIGN_HOST,
        "/",
        "new",
        &["old"],
        Duration::from_secs(180),
    )
    .await?;
    wait_for_instance_state(
        &mut operator,
        "restart-reassign-new",
        PbInstanceState::Running,
        Duration::from_secs(30),
    )
    .await?;

    Ok(())
}

async fn http01_challenges_survive_restart_and_cleanup(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        InstanceRouteSpec {
            class_id: NORMAL_CLASS_ID,
            instance_id: "restart-http01",
            route_id: "restart-http01-route",
            host: HTTP01_HOST,
            target: "http01",
        },
        "http01",
    )
    .await?;
    wait_for_instance_response(
        config,
        "HTTP-01 normal route setup",
        HTTP01_HOST,
        "/",
        "http01",
        Duration::from_secs(180),
    )
    .await?;

    put_http01_challenge(
        operator,
        HTTP01_HOST,
        HTTP01_TOKEN,
        HTTP01_KEY_AUTHORIZATION,
        SystemTime::now() + Duration::from_secs(90),
    )
    .await?;

    restart_control_plane_pod(kube.clone(), &config.namespace, &config.operator_endpoint).await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let resolved = operator
        .resolve_http01_challenge(ResolveHttp01ChallengeRequest {
            key: Some(http01_key(HTTP01_HOST, HTTP01_TOKEN)),
        })
        .await?
        .into_inner()
        .challenge
        .ok_or("HTTP-01 challenge did not resolve after control-plane restart")?;
    if resolved.key_authorization != HTTP01_KEY_AUTHORIZATION {
        return Err(format!(
            "resolved key authorization {:?}, expected {:?}",
            resolved.key_authorization, HTTP01_KEY_AUTHORIZATION
        )
        .into());
    }

    let inserted_challenge_path = challenge_path(HTTP01_TOKEN);
    let challenge = wait_for_http01_response(
        config,
        "HTTP-01 inserted challenge after restart",
        HTTP01_HOST,
        &inserted_challenge_path,
        HTTP01_KEY_AUTHORIZATION,
        Duration::from_secs(30),
    )
    .await?;
    assert_http01_content_type(&challenge, "inserted challenge after restart")?;

    let deleted = operator
        .delete_http01_challenge(DeleteHttp01ChallengeRequest {
            key: Some(http01_key(HTTP01_HOST, HTTP01_TOKEN)),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err("expected HTTP-01 delete after restart to remove challenge".into());
    }
    let after_delete = wait_for_status_without_app(
        config,
        "HTTP-01 deleted challenge after restart",
        HTTP01_HOST,
        &inserted_challenge_path,
        404,
        Duration::from_secs(30),
    )
    .await?;
    if after_delete.body.contains(HTTP01_KEY_AUTHORIZATION) {
        return Err("deleted HTTP-01 key authorization was still served after restart".into());
    }

    put_http01_challenge(
        &mut operator,
        HTTP01_HOST,
        HTTP01_EXPIRING_TOKEN,
        HTTP01_EXPIRING_KEY_AUTHORIZATION,
        SystemTime::now() + Duration::from_secs(5),
    )
    .await?;
    restart_control_plane_pod(kube, &config.namespace, &config.operator_endpoint).await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;
    let expiring_path = challenge_path(HTTP01_EXPIRING_TOKEN);
    wait_for_http01_response(
        config,
        "HTTP-01 expiring challenge after restart",
        HTTP01_HOST,
        &expiring_path,
        HTTP01_EXPIRING_KEY_AUTHORIZATION,
        Duration::from_secs(30),
    )
    .await?;

    sleep(Duration::from_secs(6)).await;
    let after_expiry = wait_for_status_without_app(
        config,
        "HTTP-01 expiring challenge after expiry",
        HTTP01_HOST,
        &expiring_path,
        404,
        Duration::from_secs(30),
    )
    .await?;
    if after_expiry
        .body
        .contains(HTTP01_EXPIRING_KEY_AUTHORIZATION)
    {
        return Err("expired HTTP-01 key authorization was still served after restart".into());
    }
    let expired = operator
        .expire_http01_challenges(ExpireHttp01ChallengesRequest {
            now_unix_millis: unix_millis(SystemTime::now())?,
            limit: Some(10),
        })
        .await?
        .into_inner();
    if expired.expired < 1 {
        return Err("expected expired HTTP-01 challenge to be removed after restart".into());
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct InstanceRouteSpec<'a> {
    class_id: &'a str,
    instance_id: &'a str,
    route_id: &'a str,
    host: &'a str,
    target: &'a str,
}

async fn create_workload_class(
    operator: &mut OperatorControlPlaneClient<Channel>,
    class_id: &str,
    app_image: &str,
    sidecar_image: &str,
    sleep_policy: WorkloadSleepPolicy,
    idempotency_suffix: &str,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: format!("kind-e2e-restart-{idempotency_suffix}-class"),
            class_id: class_id.to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: Some(WorkloadValueSchema {
                fields: HashMap::from([(
                    "target".to_owned(),
                    WorkloadValueFieldRule {
                        required: true,
                        default_value: None,
                    },
                )]),
                allow_extra: false,
            }),
            template_generation: 1,
            template: Some(manifest_template(app_image, sidecar_image)),
            sleep_policy: Some(sleep_policy),
        })
        .await?;

    Ok(())
}

async fn create_instance_and_route(
    operator: &mut OperatorControlPlaneClient<Channel>,
    spec: InstanceRouteSpec<'_>,
    idempotency_suffix: &str,
) -> TestResult<()> {
    create_instance(
        operator,
        spec.class_id,
        spec.instance_id,
        spec.target,
        idempotency_suffix,
    )
    .await?;
    create_route(
        operator,
        spec.route_id,
        spec.instance_id,
        spec.host,
        idempotency_suffix,
    )
    .await
}

async fn create_instance(
    operator: &mut OperatorControlPlaneClient<Channel>,
    class_id: &str,
    instance_id: &str,
    target: &str,
    idempotency_suffix: &str,
) -> TestResult<()> {
    operator
        .create_instance(CreateInstanceRequest {
            idempotency_key: format!("kind-e2e-restart-{idempotency_suffix}-instance"),
            instance_id: instance_id.to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: class_id.to_owned(),
                version: 1,
            }),
            values: HashMap::from([("target".to_owned(), target.to_owned())]),
        })
        .await?;

    Ok(())
}

async fn create_route(
    operator: &mut OperatorControlPlaneClient<Channel>,
    route_id: &str,
    instance_id: &str,
    host: &str,
    idempotency_suffix: &str,
) -> TestResult<()> {
    operator
        .create_route_binding(CreateRouteBindingRequest {
            idempotency_key: format!("kind-e2e-restart-{idempotency_suffix}-route"),
            route_binding_id: route_id.to_owned(),
            instance_id: instance_id.to_owned(),
            identity: Some(http_route_identity(host)),
            protocol: ProtocolRoute::Http as i32,
        })
        .await?;

    Ok(())
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
            Ok(channel) => {
                let mut client = OperatorControlPlaneClient::new(channel);
                match probe_operator(&mut client).await {
                    Ok(()) => {
                        sleep(Duration::from_millis(500)).await;
                        match probe_operator(&mut client).await {
                            Ok(()) => return Ok(client),
                            Err(error) if Instant::now() < deadline => {
                                eprintln!(
                                    "waiting for stable operator gRPC endpoint {endpoint}: {error}"
                                );
                                sleep(Duration::from_secs(1)).await;
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    Err(error) if Instant::now() < deadline => {
                        eprintln!("waiting for stable operator gRPC endpoint {endpoint}: {error}");
                        sleep(Duration::from_secs(1)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for operator gRPC endpoint {endpoint}: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn probe_operator(
    client: &mut OperatorControlPlaneClient<Channel>,
) -> Result<(), tonic::Status> {
    match client
        .get_instance(GetInstanceRequest {
            instance_id: "connectivity-probe".to_owned(),
        })
        .await
    {
        Err(status) if status.code() == Code::NotFound => Ok(()),
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn get_instance(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
) -> TestResult<Instance> {
    Ok(operator
        .get_instance(GetInstanceRequest {
            instance_id: instance_id.to_owned(),
        })
        .await?
        .into_inner())
}

async fn assert_instance_not_found(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
) -> TestResult<()> {
    let error = operator
        .get_instance(GetInstanceRequest {
            instance_id: instance_id.to_owned(),
        })
        .await
        .expect_err("deleted instance should not be found");
    if error.code() != tonic::Code::NotFound {
        return Err(format!(
            "expected deleted instance {instance_id} to return NotFound, got {:?}: {}",
            error.code(),
            error.message()
        )
        .into());
    }
    Ok(())
}

async fn wait_for_instance_state(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let instance = get_instance(operator, instance_id).await?;
        let actual = instance_state(&instance);
        if actual == expected {
            return Ok(instance);
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for instance {instance_id} to reach {expected:?}; last state was {actual:?} generation {}",
                instance.generation
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_instance_state_reconnecting(
    operator_endpoint: &str,
    instance_id: &str,
    expected: PbInstanceState,
    timeout: Duration,
) -> TestResult<Instance> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut operator = connect_operator(operator_endpoint).await?;
        match get_instance(&mut operator, instance_id).await {
            Ok(instance) => {
                let actual = instance_state(&instance);
                if actual == expected {
                    return Ok(instance);
                }

                if Instant::now() >= deadline {
                    return Err(format!(
                        "timed out waiting for instance {instance_id} to reach {expected:?}; last state was {actual:?} generation {}",
                        instance.generation
                    )
                    .into());
                }
            }
            Err(error) if Instant::now() < deadline => {
                eprintln!(
                    "waiting for instance {instance_id} after reconnectable operator error: {error}"
                );
            }
            Err(error) => return Err(error),
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn instance_state(instance: &Instance) -> PbInstanceState {
    PbInstanceState::try_from(instance.state).unwrap_or(PbInstanceState::Unspecified)
}

async fn restart_control_plane_pod(
    kube: Client,
    namespace: &str,
    operator_endpoint: &str,
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let selected = pods
        .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
        .await?
        .into_iter()
        .find(|pod| pod_ready(pod))
        .or_else(|| None)
        .ok_or("no control-plane pod found to restart")?;
    let old_name = selected
        .metadata
        .name
        .clone()
        .ok_or("control-plane pod is missing name")?;
    let old_uid = selected.metadata.uid.clone();

    pods.delete(&old_name, &DeleteParams::default()).await?;
    wait_for_replacement_control_plane_pod(pods, old_uid, Duration::from_secs(120)).await?;
    connect_operator(operator_endpoint).await?;

    Ok(())
}

async fn scale_control_plane(kube: Client, namespace: &str, replicas: i32) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    deployments
        .patch(
            CONTROL_PLANE_DEPLOYMENT,
            &PatchParams::default(),
            &Patch::Merge(json!({ "spec": { "replicas": replicas } })),
        )
        .await?;
    wait_for_control_plane_ready_replicas(
        kube,
        namespace,
        replicas as usize,
        Duration::from_secs(120),
    )
    .await
}

async fn wait_for_replacement_control_plane_pod(
    pods: Api<Pod>,
    old_uid: Option<String>,
    timeout: Duration,
) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let ready = pods
            .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
            .await?
            .into_iter()
            .any(|pod| {
                pod_ready(&pod)
                    && old_uid
                        .as_ref()
                        .is_none_or(|uid| pod.metadata.uid.as_ref() != Some(uid))
            });
        if ready {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err("timed out waiting for replacement control-plane pod".into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_control_plane_ready_replicas(
    kube: Client,
    namespace: &str,
    expected: usize,
    timeout: Duration,
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + timeout;
    loop {
        let listed = pods
            .list(&ListParams::default().labels(CONTROL_PLANE_LABEL))
            .await?;
        let ready = listed.iter().filter(|pod| pod_ready(pod)).count();
        if expected == 0 {
            if listed.items.is_empty() {
                return Ok(());
            }
        } else if ready == expected {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for {expected} ready control-plane replicas; got {ready}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn pod_ready(pod: &Pod) -> bool {
    if pod.metadata.deletion_timestamp.is_some() {
        return false;
    }

    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}

fn load_late_image_into_kind(config: &E2eConfig) -> TestResult<()> {
    let status = Command::new("kind")
        .args([
            "load",
            "docker-image",
            config.late_app_image.as_str(),
            "--name",
            config.cluster_name.as_str(),
        ])
        .status()?;
    if !status.success() {
        return Err(format!(
            "kind load docker-image {} --name {} failed with status {status}",
            config.late_app_image, config.cluster_name
        )
        .into());
    }

    Ok(())
}

async fn delete_workload_pods(
    kube: Client,
    namespace: &str,
    workload_name: &str,
) -> TestResult<()> {
    let pods: Api<Pod> = Api::namespaced(kube, namespace);
    let selector = format!("{WORKLOAD_NAME_LABEL}={workload_name}");
    for pod in pods.list(&ListParams::default().labels(&selector)).await? {
        if let Some(name) = pod.metadata.name {
            let _ = pods.delete(&name, &DeleteParams::default()).await;
        }
    }

    Ok(())
}

async fn assert_workload_generation(
    kube: Client,
    namespace: &str,
    workload_name: &str,
    generation: u64,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let expected = generation.to_string();
    let deployment = deployments.get(workload_name).await?;
    let service = services.get(workload_name).await?;
    assert_object_generation_label(
        "Deployment",
        workload_name,
        &deployment.metadata.labels,
        &expected,
    )?;
    assert_object_generation_label(
        "Service",
        workload_name,
        &service.metadata.labels,
        &expected,
    )?;

    Ok(())
}

fn assert_object_generation_label(
    kind: &str,
    name: &str,
    labels: &Option<std::collections::BTreeMap<String, String>>,
    expected: &str,
) -> TestResult<()> {
    let actual = labels
        .as_ref()
        .and_then(|labels| labels.get(INSTANCE_GENERATION_LABEL))
        .ok_or_else(|| format!("{kind} {name} is missing {INSTANCE_GENERATION_LABEL} label"))?;
    if actual != expected {
        return Err(format!(
            "{kind} {name} generation label was {actual:?}, expected {expected:?}"
        )
        .into());
    }

    Ok(())
}

async fn wait_for_workload_absent(
    kube: Client,
    namespace: &str,
    workload_name: &str,
    timeout: Duration,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + timeout;
    loop {
        let deployment_absent = is_not_found(deployments.get(workload_name).await);
        let service_absent = is_not_found(services.get(workload_name).await);
        if deployment_absent && service_absent {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for workload {namespace}/{workload_name} Deployment and Service to be deleted"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn is_not_found<T>(result: Result<T, KubeError>) -> bool {
    matches!(result, Err(KubeError::Api(status)) if status.is_not_found())
}

async fn wait_for_instance_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    wait_for_instance_response_rejecting_stale(config, context, host, path, target, &[], timeout)
        .await
}

async fn wait_for_instance_response_rejecting_stale(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    stale_targets: &[&str],
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response)
                if response.status == 200 && response_body_identifies(&response, target) =>
            {
                assert_instance_response(&response, context, target)?;
                return Ok(response);
            }
            Ok(response) => {
                for stale in stale_targets {
                    if response.status == 200 && response_body_identifies(&response, stale) {
                        return Err(format!(
                            "{context} served stale target {stale:?} after restart/reassignment: {:?}",
                            response.body
                        )
                        .into());
                    }
                }
                format!(
                    "frontline returned HTTP {} with body {:?}",
                    response.status, response.body
                )
            }
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for successful frontend response for {context} host {host} path {path}: {last_error}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_status_without_app(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    expected_status: u16,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response) if response.status == expected_status => {
                if response.body.contains(APP_MARKER) {
                    return Err(format!(
                        "{context} unexpectedly reached an app body for host {host} path {path}: {:?}",
                        response.body
                    )
                    .into());
                }
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
                "timed out waiting for HTTP {expected_status} for {context} host {host} path {path}: {last_error}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn put_http01_challenge(
    operator: &mut OperatorControlPlaneClient<Channel>,
    host: &str,
    token: &str,
    key_authorization: &str,
    expires_at: SystemTime,
) -> TestResult<()> {
    operator
        .put_http01_challenge(PutHttp01ChallengeRequest {
            key: Some(http01_key(host, token)),
            key_authorization: key_authorization.to_owned(),
            expires_at_unix_millis: unix_millis(expires_at)?,
        })
        .await?;
    Ok(())
}

async fn wait_for_http01_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    expected_body: &str,
    timeout: Duration,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response) if response.status == 200 && response.body == expected_body => {
                return Ok(response);
            }
            Ok(response) => format!(
                "frontline returned HTTP {} with body {:?}",
                response.status, response.body
            ),
            Err(error) => error.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {context} at {path}: {last_error}").into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn http_get(addr: SocketAddr, host: &str, path: &str) -> TestResult<HttpResponse> {
    http_get_with_timeout(addr, host, path, Duration::from_secs(180)).await
}

async fn http_get_with_timeout(
    addr: SocketAddr,
    host: &str,
    path: &str,
    request_timeout: Duration,
) -> TestResult<HttpResponse> {
    let host = host.to_owned();
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || http_get_blocking(addr, &host, &path, request_timeout))
        .await
        .map_err(|error| format!("HTTP request task failed: {error}"))?
}

fn http_get_blocking(
    addr: SocketAddr,
    host: &str,
    path: &str,
    request_timeout: Duration,
) -> TestResult<HttpResponse> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(request_timeout))?;
    stream.set_write_timeout(Some(request_timeout))?;
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
    let headers = head
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect();

    Ok(HttpResponse {
        status,
        headers,
        body: body.to_owned(),
    })
}

fn assert_instance_response(
    response: &HttpResponse,
    context: &str,
    target: &str,
) -> TestResult<()> {
    if response.status != 200 {
        return Err(format!("{context} returned HTTP {}", response.status).into());
    }
    if !response.body.contains(APP_MARKER) {
        return Err(format!(
            "{context} body did not include routing app marker: {:?}",
            response.body
        )
        .into());
    }
    if !response_body_identifies(response, target) {
        return Err(format!(
            "{context} body did not identify target {target:?}: {:?}",
            response.body
        )
        .into());
    }

    Ok(())
}

fn response_body_identifies(response: &HttpResponse, target: &str) -> bool {
    response.body.contains(&format!("instance={target}\n"))
}

fn assert_http01_content_type(response: &HttpResponse, context: &str) -> TestResult<()> {
    let content_type = response
        .headers
        .get("content-type")
        .ok_or_else(|| format!("{context} HTTP-01 response is missing content-type"))?;
    if content_type != "text/plain" {
        return Err(format!(
            "{context} HTTP-01 content-type was {content_type:?}, expected \"text/plain\""
        )
        .into());
    }
    Ok(())
}

fn manifest_template(app_image: &str, sidecar_image: &str) -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::Deployment as i32,
            name: Some(target_text("restart-", "")),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "app".to_owned(),
                image: Some(literal_text(app_image)),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: APP_PORT,
                }],
                env: vec![EnvVarTemplate {
                    name: "SLEEPYPODS_E2E_INSTANCE".to_owned(),
                    value: Some(target_text("", "")),
                }],
            }),
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text(sidecar_image)),
            listen_port: SIDECAR_PORT,
            mode: None,
        }),
        service: Some(ServiceTemplate {
            name: Some(target_text("restart-", "")),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: APP_PORT,
                target_port: APP_PORT,
            }],
        }),
        volumes: Vec::new(),
    }
}

fn http_route_identity(host: &str) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
            host: Some(RouteHost {
                kind: RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
            path_prefix: None,
        })),
    }
}

fn sleep_policy(idle_timeout_ms: u64) -> WorkloadSleepPolicy {
    WorkloadSleepPolicy {
        idle_timeout_ms,
        idle_retry_backoff_ms: 500,
        drain_grace_timeout_ms: 500,
        idle_timeout_override: None,
    }
}

fn http01_key(host: &str, token: &str) -> Http01ChallengeKey {
    Http01ChallengeKey {
        host: host.to_owned(),
        token: token.to_owned(),
    }
}

fn challenge_path(token: &str) -> String {
    format!("/.well-known/acme-challenge/{token}")
}

fn unix_millis(time: SystemTime) -> TestResult<i64> {
    Ok(i64::try_from(time.duration_since(UNIX_EPOCH)?.as_millis())?)
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}

fn target_text(prefix: &str, suffix: &str) -> TemplateText {
    TemplateText {
        parts: vec![
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(prefix.to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::InstanceValue("target".to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(suffix.to_owned())),
            },
        ],
    }
}
