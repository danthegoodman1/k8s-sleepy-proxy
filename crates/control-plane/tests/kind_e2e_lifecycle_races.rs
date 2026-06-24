use std::{
    collections::HashMap,
    env,
    error::Error,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    process::Command,
    time::Duration,
};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient,
    proxy_control_plane_client::ProxyControlPlaneClient, proxy_subscribe_request,
    proxy_subscribe_response, proxy_wake_instance_response, route_identity,
    sidecar_control_plane_client::SidecarControlPlaneClient, sidecar_report_idle_response,
    template_text_part, ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest,
    CreateRouteBindingRequest, CreateWorkloadClassVersionRequest, DeleteInstanceRequest,
    DeleteRouteBindingRequest, HostPathVolumeSourceTemplate, HttpRouteIdentity, Instance,
    InstanceState as PbInstanceState, ManifestTemplate, PersistentVolumeAccessMode,
    PersistentVolumeReclaimPolicy, PersistentVolumeSourceTemplate, ProtocolRoute,
    ProxyRouteInvalidationReason, ProxySubscribeRequest, ProxySubscribeRouteRequest,
    ProxyWakeInstanceRequest, RouteHost, RouteHostKind, RouteIdentity, ServicePortTemplate,
    ServiceTemplate, SidecarReportIdleRequest, SidecarTemplate, TemplateText, TemplateTextPart,
    VolumeTemplate, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy, WorkloadTemplate,
    WorkloadValueFieldRule, WorkloadValueSchema,
};
use k8s_openapi::api::{
    apps::v1::{Deployment, StatefulSet},
    core::v1::{PersistentVolume, PersistentVolumeClaim, Pod, Service},
    rbac::v1::Role,
};
use kube::{
    api::{DeleteParams, ListParams, PostParams},
    Api, Client, Error as KubeError,
};
use tokio::time::{sleep, timeout, Instant};
use tonic::{
    codegen::tokio_stream::{wrappers::ReceiverStream, StreamExt},
    transport::{Channel, Endpoint},
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const NORMAL_CLASS_ID: &str = "lifecycle-normal";
const SLEEP_WHILE_WAKING_CLASS_ID: &str = "lifecycle-sleep-waking";
const DELETE_WHILE_WAKING_CLASS_ID: &str = "lifecycle-delete-waking";
const DELETE_WHILE_DRAINING_CLASS_ID: &str = "lifecycle-delete-draining";
const FAILED_RETRY_CLASS_ID: &str = "lifecycle-failed-retry";
const SIDECAR_PORT: u32 = 15_000;
const APP_PORT: u32 = 8080;
const APP_MARKER: &str = "sleepypods-routing-app";
const WORKLOAD_NAME_LABEL: &str = "sleepypods.io/workload-name";
const INSTANCE_GENERATION_LABEL: &str = "sleepypods.io/instance-generation";

#[tokio::test]
#[ignore = "requires scripts/test-kind-e2e-lifecycle-races.sh or an equivalent kind deployment"]
async fn lifecycle_races_through_deployed_platform() -> TestResult<()> {
    if env::var("SLEEPYPODS_KIND_E2E_LIFECYCLE_RACES").as_deref() != Ok("1") {
        eprintln!(
            "skipping lifecycle-race kind E2E because SLEEPYPODS_KIND_E2E_LIFECYCLE_RACES=1 is not set"
        );
        return Ok(());
    }

    control_plane::install_rustls_crypto_provider();

    let config = E2eConfig::from_env()?;
    let kube = Client::try_default().await?;
    let mut operator = connect_operator(&config.operator_endpoint).await?;

    create_workload_class(
        &mut operator,
        NORMAL_CLASS_ID,
        &config.app_image,
        &config.sidecar_image,
        WorkloadKind::Deployment,
        "lifecycle-normal-app",
        Vec::new(),
        "normal",
    )
    .await?;
    create_workload_class(
        &mut operator,
        SLEEP_WHILE_WAKING_CLASS_ID,
        &config.sleep_while_waking_image,
        &config.sidecar_image,
        WorkloadKind::Deployment,
        "lifecycle-sleep-waking-app",
        Vec::new(),
        "sleep-waking",
    )
    .await?;
    create_workload_class(
        &mut operator,
        DELETE_WHILE_WAKING_CLASS_ID,
        &config.delete_while_waking_image,
        &config.sidecar_image,
        WorkloadKind::StatefulSet,
        "lifecycle-delete-waking-app",
        stateful_volumes(
            "lifecycle-delete-waking-pv",
            "lifecycle-delete-waking-pvc",
            "/tmp/sleepypods-kind-e2e-lifecycle-races/delete-waking",
        ),
        "delete-waking",
    )
    .await?;
    create_workload_class(
        &mut operator,
        DELETE_WHILE_DRAINING_CLASS_ID,
        &config.app_image,
        &config.sidecar_image,
        WorkloadKind::StatefulSet,
        "lifecycle-delete-draining-app",
        stateful_volumes(
            "lifecycle-delete-draining-pv",
            "lifecycle-delete-draining-pvc",
            "/tmp/sleepypods-kind-e2e-lifecycle-races/delete-draining",
        ),
        "delete-draining",
    )
    .await?;
    create_workload_class(
        &mut operator,
        FAILED_RETRY_CLASS_ID,
        &config.failed_retry_image,
        &config.sidecar_image,
        WorkloadKind::Deployment,
        "lifecycle-failed-retry-app",
        Vec::new(),
        "failed-retry",
    )
    .await?;

    eprintln!("==> lifecycle race E2E: concurrent wake");
    concurrent_wake_calls_converge(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: ReportIdle while waking");
    report_idle_while_waking_cannot_finalize_cleanup(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: delete while waking");
    delete_while_waking_cleans_pending_objects(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: delete while draining");
    delete_while_draining_cleans_deleting_materialization(&mut operator, kube.clone(), &config)
        .await?;

    eprintln!("==> lifecycle race E2E: failed wake retry");
    failed_wake_retry_rejects_stale_generation(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: stale sidecar ReportIdle");
    stale_sidecar_report_is_rejected(&mut operator, kube.clone(), &config).await?;

    eprintln!("==> lifecycle race E2E: route reassignment subscription invalidation");
    route_reassignment_invalidates_active_subscription(&mut operator, kube, &config).await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct E2eConfig {
    namespace: String,
    operator_endpoint: String,
    frontline_addr: SocketAddr,
    cluster_name: String,
    app_image: String,
    sleep_while_waking_image: String,
    delete_while_waking_image: String,
    failed_retry_image: String,
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
                .unwrap_or_else(|_| "sleepypods-e2e-lifecycle-races".to_owned()),
            operator_endpoint: env::var("SLEEPYPODS_E2E_OPERATOR_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19851".to_owned()),
            frontline_addr: env::var("SLEEPYPODS_E2E_FRONTLINE_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:19880".to_owned())
                .parse()?,
            cluster_name: env::var("SLEEPYPODS_KIND_CLUSTER")
                .unwrap_or_else(|_| "sleepypods-e2e-lifecycle-races-test".to_owned()),
            app_image: env::var("SLEEPYPODS_E2E_APP_IMAGE")
                .unwrap_or_else(|_| "sleepypods/routing-app:kind-e2e-lifecycle-races".to_owned()),
            sleep_while_waking_image: env::var("SLEEPYPODS_E2E_SLEEP_WHILE_WAKING_IMAGE")
                .unwrap_or_else(|_| {
                    "sleepypods/routing-app-sleep-waking:kind-e2e-lifecycle-races".to_owned()
                }),
            delete_while_waking_image: env::var("SLEEPYPODS_E2E_DELETE_WHILE_WAKING_IMAGE")
                .unwrap_or_else(|_| {
                    "sleepypods/routing-app-delete-waking:kind-e2e-lifecycle-races".to_owned()
                }),
            failed_retry_image: env::var("SLEEPYPODS_E2E_FAILED_RETRY_IMAGE").unwrap_or_else(
                |_| "sleepypods/routing-app-failed-retry:kind-e2e-lifecycle-races".to_owned(),
            ),
            sidecar_image: env::var("SLEEPYPODS_E2E_SIDECAR_IMAGE")
                .unwrap_or_else(|_| "sleepypods/sidecar:kind-e2e-lifecycle-races".to_owned()),
        })
    }
}

async fn concurrent_wake_calls_converge(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        NORMAL_CLASS_ID,
        "lifecycle-concurrent",
        "lifecycle-concurrent-route",
        "concurrent.lifecycle.sleepypods.test",
        "concurrent",
        "concurrent",
    )
    .await?;
    let created =
        wait_for_instance_state(operator, "lifecycle-concurrent", PbInstanceState::Cold, 30)
            .await?;

    let mut tasks = Vec::new();
    for _ in 0..4 {
        let endpoint = config.operator_endpoint.clone();
        tasks.push(tokio::spawn(async move {
            let mut proxy = connect_proxy(&endpoint).await?;
            let response = proxy
                .wake_instance(ProxyWakeInstanceRequest {
                    instance_id: "lifecycle-concurrent".to_owned(),
                    expected_generation: created.generation,
                    backend_generation: None,
                })
                .await?
                .into_inner();
            Ok::<_, Box<dyn Error + Send + Sync>>(response)
        }));
    }

    let mut ready = 0;
    let mut conflicts = 0;
    for task in tasks {
        match task.await??.outcome {
            Some(proxy_wake_instance_response::Outcome::Ready(result)) => {
                ready += 1;
                if result.instance_generation <= created.generation {
                    return Err(format!(
                        "concurrent wake returned non-advanced generation {}",
                        result.instance_generation
                    )
                    .into());
                }
            }
            Some(proxy_wake_instance_response::Outcome::GenerationConflict(conflict)) => {
                conflicts += 1;
                if conflict.expected_generation != created.generation {
                    return Err(format!(
                        "wake conflict expected generation {}, wanted {}",
                        conflict.expected_generation, created.generation
                    )
                    .into());
                }
            }
            other => return Err(format!("unexpected concurrent wake outcome: {other:?}").into()),
        }
    }
    if ready != 1 || conflicts != 3 {
        return Err(format!(
            "expected one ready wake and three conflicts, got {ready}/{conflicts}"
        )
        .into());
    }

    let running = wait_for_instance_state(
        operator,
        "lifecycle-concurrent",
        PbInstanceState::Running,
        30,
    )
    .await?;
    assert_workload_generation(
        kube,
        &config.namespace,
        "lifecycle-concurrent",
        running.generation - 1,
    )
    .await?;
    assert_response_identifies(
        &wait_for_instance_response(
            config,
            "concurrent wake route",
            "concurrent.lifecycle.sleepypods.test",
            "/",
            "concurrent",
            60,
        )
        .await?,
        "concurrent",
    )?;

    Ok(())
}

async fn report_idle_while_waking_cannot_finalize_cleanup(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        SLEEP_WHILE_WAKING_CLASS_ID,
        "lifecycle-sleep-waking",
        "lifecycle-sleep-waking-route",
        "sleep-waking.lifecycle.sleepypods.test",
        "sleep-waking",
        "sleep-waking",
    )
    .await?;
    let wake_addr = config.frontline_addr;
    let wake = tokio::spawn(async move {
        http_get_with_timeout(
            wake_addr,
            "sleep-waking.lifecycle.sleepypods.test",
            "/",
            Duration::from_secs(180),
        )
        .await
    });
    let waking = wait_for_instance_state(
        operator,
        "lifecycle-sleep-waking",
        PbInstanceState::Waking,
        60,
    )
    .await?;

    let mut sidecar = connect_sidecar(&config.operator_endpoint).await?;
    let stale = sidecar
        .report_idle(SidecarReportIdleRequest {
            instance_id: "lifecycle-sleep-waking".to_owned(),
            expected_generation: waking.generation,
            active_count: 0,
        })
        .await?
        .into_inner();
    match stale.outcome {
        Some(sidecar_report_idle_response::Outcome::Unavailable(unavailable)) => {
            if unavailable.instance_generation != waking.generation {
                return Err("ReportIdle while waking returned the wrong generation".into());
            }
        }
        other => return Err(format!("ReportIdle while waking was not rejected: {other:?}").into()),
    }

    load_image_into_kind(config, &config.sleep_while_waking_image)?;
    delete_workload_pods(kube.clone(), &config.namespace, "lifecycle-sleep-waking").await?;
    let _ = timeout(Duration::from_secs(5), wake).await;
    wait_for_instance_response(
        config,
        "sleep-while-waking retry",
        "sleep-waking.lifecycle.sleepypods.test",
        "/",
        "sleep-waking",
        180,
    )
    .await?;
    let running = wait_for_instance_state(
        operator,
        "lifecycle-sleep-waking",
        PbInstanceState::Running,
        30,
    )
    .await?;
    assert_workload_generation(
        kube,
        &config.namespace,
        "lifecycle-sleep-waking",
        running.generation - 1,
    )
    .await?;

    Ok(())
}

async fn delete_while_waking_cleans_pending_objects(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        DELETE_WHILE_WAKING_CLASS_ID,
        "lifecycle-delete-waking",
        "lifecycle-delete-waking-route",
        "delete-waking.lifecycle.sleepypods.test",
        "delete-waking",
        "delete-waking",
    )
    .await?;
    let wake_addr = config.frontline_addr;
    let wake = tokio::spawn(async move {
        http_get_with_timeout(
            wake_addr,
            "delete-waking.lifecycle.sleepypods.test",
            "/",
            Duration::from_secs(180),
        )
        .await
    });
    wait_for_instance_state(
        operator,
        "lifecycle-delete-waking",
        PbInstanceState::Waking,
        60,
    )
    .await?;
    wait_for_stateful_objects_present(
        kube.clone(),
        &config.namespace,
        "lifecycle-delete-waking",
        "lifecycle-delete-waking-pvc",
        "lifecycle-delete-waking-pv",
        60,
    )
    .await?;

    let deleted = operator
        .delete_instance(DeleteInstanceRequest {
            instance_id: "lifecycle-delete-waking".to_owned(),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err("delete while waking did not report deletion".into());
    }
    let _ = timeout(Duration::from_secs(5), wake).await;
    assert_instance_not_found(operator, "lifecycle-delete-waking").await?;
    wait_for_stateful_objects_absent(
        kube,
        &config.namespace,
        "lifecycle-delete-waking",
        "lifecycle-delete-waking-pvc",
        "lifecycle-delete-waking-pv",
        60,
    )
    .await?;
    assert_no_backend_response(
        config,
        "delete-waking.lifecycle.sleepypods.test",
        "delete-waking",
        5,
    )
    .await?;

    Ok(())
}

async fn delete_while_draining_cleans_deleting_materialization(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        DELETE_WHILE_DRAINING_CLASS_ID,
        "lifecycle-delete-draining",
        "lifecycle-delete-draining-route",
        "delete-draining.lifecycle.sleepypods.test",
        "delete-draining",
        "delete-draining",
    )
    .await?;
    wait_for_instance_response(
        config,
        "delete-draining wake",
        "delete-draining.lifecycle.sleepypods.test",
        "/",
        "delete-draining",
        180,
    )
    .await?;
    let running = wait_for_instance_state(
        operator,
        "lifecycle-delete-draining",
        PbInstanceState::Running,
        30,
    )
    .await?;
    wait_for_stateful_objects_present(
        kube.clone(),
        &config.namespace,
        "lifecycle-delete-draining",
        "lifecycle-delete-draining-pvc",
        "lifecycle-delete-draining-pv",
        60,
    )
    .await?;

    set_control_plane_statefulset_delete_permission(kube.clone(), &config.namespace, false).await?;
    sleep(Duration::from_secs(2)).await;
    let mut sidecar = connect_sidecar(&config.operator_endpoint).await?;
    let idle_result = sidecar
        .report_idle(SidecarReportIdleRequest {
            instance_id: "lifecycle-delete-draining".to_owned(),
            expected_generation: running.generation,
            active_count: 0,
        })
        .await;
    set_control_plane_statefulset_delete_permission(kube.clone(), &config.namespace, true).await?;

    let idle_error =
        idle_result.expect_err("sleep cleanup should fail while StatefulSet delete is forbidden");
    if idle_error.code() != tonic::Code::Unavailable
        || !idle_error.message().contains("sleep cleanup failed")
    {
        return Err(format!(
            "expected forbidden sleep cleanup to return Unavailable cleanup failure, got {:?}: {}",
            idle_error.code(),
            idle_error.message()
        )
        .into());
    }
    let draining = wait_for_instance_state(
        operator,
        "lifecycle-delete-draining",
        PbInstanceState::Draining,
        30,
    )
    .await?;
    if draining.generation <= running.generation {
        return Err(format!(
            "delete-draining generation {} did not advance beyond running generation {}",
            draining.generation, running.generation
        )
        .into());
    }

    let deleted = operator
        .delete_instance(DeleteInstanceRequest {
            instance_id: "lifecycle-delete-draining".to_owned(),
        })
        .await?
        .into_inner();
    if !deleted.deleted {
        return Err("delete while draining did not report deletion".into());
    }
    assert_instance_not_found(operator, "lifecycle-delete-draining").await?;
    wait_for_stateful_objects_absent(
        kube,
        &config.namespace,
        "lifecycle-delete-draining",
        "lifecycle-delete-draining-pvc",
        "lifecycle-delete-draining-pv",
        60,
    )
    .await?;
    assert_no_backend_response(
        config,
        "delete-draining.lifecycle.sleepypods.test",
        "delete-draining",
        5,
    )
    .await?;

    Ok(())
}

async fn failed_wake_retry_rejects_stale_generation(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        FAILED_RETRY_CLASS_ID,
        "lifecycle-failed-retry",
        "lifecycle-failed-retry-route",
        "failed-retry.lifecycle.sleepypods.test",
        "failed-retry",
        "failed-retry",
    )
    .await?;
    wait_for_frontline_status(
        config,
        "failed-retry.lifecycle.sleepypods.test",
        "/",
        503,
        180,
    )
    .await?;
    let failed = wait_for_instance_state(
        operator,
        "lifecycle-failed-retry",
        PbInstanceState::Failed,
        30,
    )
    .await?;

    load_image_into_kind(config, &config.failed_retry_image)?;
    delete_workload_pods(kube.clone(), &config.namespace, "lifecycle-failed-retry").await?;
    wait_for_instance_response(
        config,
        "failed wake retry",
        "failed-retry.lifecycle.sleepypods.test",
        "/",
        "failed-retry",
        180,
    )
    .await?;
    let running = wait_for_instance_state(
        operator,
        "lifecycle-failed-retry",
        PbInstanceState::Running,
        30,
    )
    .await?;

    let mut proxy = connect_proxy(&config.operator_endpoint).await?;
    let stale = proxy
        .wake_instance(ProxyWakeInstanceRequest {
            instance_id: "lifecycle-failed-retry".to_owned(),
            expected_generation: failed.generation,
            backend_generation: None,
        })
        .await?
        .into_inner();
    match stale.outcome {
        Some(proxy_wake_instance_response::Outcome::GenerationConflict(conflict)) => {
            if conflict.actual_generation != running.generation {
                return Err(format!(
                    "stale failed wake actual generation was {}, wanted {}",
                    conflict.actual_generation, running.generation
                )
                .into());
            }
        }
        other => return Err(format!("stale failed generation was not rejected: {other:?}").into()),
    }
    assert_workload_generation(
        kube,
        &config.namespace,
        "lifecycle-failed-retry",
        running.generation - 1,
    )
    .await?;

    Ok(())
}

async fn stale_sidecar_report_is_rejected(
    operator: &mut OperatorControlPlaneClient<Channel>,
    kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance_and_route(
        operator,
        NORMAL_CLASS_ID,
        "lifecycle-stale-sidecar",
        "lifecycle-stale-sidecar-route",
        "stale-sidecar.lifecycle.sleepypods.test",
        "stale-sidecar",
        "stale-sidecar",
    )
    .await?;
    wait_for_instance_response(
        config,
        "stale sidecar first wake",
        "stale-sidecar.lifecycle.sleepypods.test",
        "/",
        "stale-sidecar",
        180,
    )
    .await?;
    let first_running = wait_for_instance_state(
        operator,
        "lifecycle-stale-sidecar",
        PbInstanceState::Running,
        30,
    )
    .await?;
    let mut sidecar = connect_sidecar(&config.operator_endpoint).await?;
    sidecar
        .report_idle(SidecarReportIdleRequest {
            instance_id: "lifecycle-stale-sidecar".to_owned(),
            expected_generation: first_running.generation,
            active_count: 0,
        })
        .await?;
    wait_for_workload_absent(
        kube.clone(),
        &config.namespace,
        "lifecycle-stale-sidecar",
        60,
    )
    .await?;
    wait_for_instance_response(
        config,
        "stale sidecar second wake",
        "stale-sidecar.lifecycle.sleepypods.test",
        "/",
        "stale-sidecar",
        180,
    )
    .await?;
    let second_running = wait_for_instance_state(
        operator,
        "lifecycle-stale-sidecar",
        PbInstanceState::Running,
        30,
    )
    .await?;
    if second_running.generation <= first_running.generation + 2 {
        return Err(format!(
            "second wake generation {} did not move far enough beyond first running {}",
            second_running.generation, first_running.generation
        )
        .into());
    }

    let stale = sidecar
        .report_idle(SidecarReportIdleRequest {
            instance_id: "lifecycle-stale-sidecar".to_owned(),
            expected_generation: first_running.generation,
            active_count: 0,
        })
        .await?
        .into_inner();
    match stale.outcome {
        Some(sidecar_report_idle_response::Outcome::GenerationConflict(conflict)) => {
            if conflict.actual_generation != second_running.generation {
                return Err(format!(
                    "stale sidecar conflict actual generation {}, wanted {}",
                    conflict.actual_generation, second_running.generation
                )
                .into());
            }
        }
        other => return Err(format!("stale sidecar report was not rejected: {other:?}").into()),
    }
    let after = get_instance(operator, "lifecycle-stale-sidecar").await?;
    assert_state(&after, PbInstanceState::Running)?;
    assert_eq!(after.generation, second_running.generation);

    Ok(())
}

async fn route_reassignment_invalidates_active_subscription(
    operator: &mut OperatorControlPlaneClient<Channel>,
    _kube: Client,
    config: &E2eConfig,
) -> TestResult<()> {
    create_instance(
        operator,
        NORMAL_CLASS_ID,
        "lifecycle-reassign-old",
        "old",
        "reassign-old",
    )
    .await?;
    create_instance(
        operator,
        NORMAL_CLASS_ID,
        "lifecycle-reassign-new",
        "new",
        "reassign-new",
    )
    .await?;
    create_route(
        operator,
        "lifecycle-reassign-old-route",
        "lifecycle-reassign-old",
        "reassign.lifecycle.sleepypods.test",
        "reassign-old",
    )
    .await?;
    wait_for_instance_response(
        config,
        "route reassignment old",
        "reassign.lifecycle.sleepypods.test",
        "/",
        "old",
        180,
    )
    .await?;

    let mut proxy = connect_proxy(&config.operator_endpoint).await?;
    let (requests, request_stream) = tokio::sync::mpsc::channel(4);
    let mut responses = proxy
        .subscribe(ReceiverStream::new(request_stream))
        .await?
        .into_inner();
    requests
        .send(subscribe_route_request(
            "lifecycle-reassign-old-subscribe",
            "reassign.lifecycle.sleepypods.test",
        ))
        .await?;
    let resolved = expect_route_resolved(next_subscribe_response(&mut responses).await?);
    if resolved
        .route
        .as_ref()
        .map(|route| route.instance_id.as_str())
        != Some("lifecycle-reassign-old")
    {
        return Err(format!("active subscription did not resolve old route: {resolved:?}").into());
    }

    operator
        .delete_route_binding(DeleteRouteBindingRequest {
            route_binding_id: "lifecycle-reassign-old-route".to_owned(),
        })
        .await?;
    let invalidated = expect_route_invalidated(next_subscribe_response(&mut responses).await?);
    if invalidated.subscription_id != resolved.subscription_id
        || invalidated.reason != ProxyRouteInvalidationReason::RouteRemoved as i32
    {
        return Err(format!("unexpected route invalidation: {invalidated:?}").into());
    }

    create_route(
        operator,
        "lifecycle-reassign-new-route",
        "lifecycle-reassign-new",
        "reassign.lifecycle.sleepypods.test",
        "reassign-new",
    )
    .await?;
    requests
        .send(subscribe_route_request(
            "lifecycle-reassign-new-subscribe",
            "reassign.lifecycle.sleepypods.test",
        ))
        .await?;
    let new_resolved = expect_route_resolved(next_subscribe_response(&mut responses).await?);
    if new_resolved
        .route
        .as_ref()
        .map(|route| route.instance_id.as_str())
        != Some("lifecycle-reassign-new")
    {
        return Err(format!("new subscription did not resolve new route: {new_resolved:?}").into());
    }
    wait_for_instance_response_rejecting_stale(
        config,
        "route reassignment new",
        "reassign.lifecycle.sleepypods.test",
        "/",
        "new",
        &["old"],
        180,
    )
    .await?;

    Ok(())
}

async fn create_workload_class(
    operator: &mut OperatorControlPlaneClient<Channel>,
    class_id: &str,
    app_image: &str,
    sidecar_image: &str,
    kind: WorkloadKind,
    workload_name: &str,
    volumes: Vec<VolumeTemplate>,
    idempotency_suffix: &str,
) -> TestResult<()> {
    operator
        .create_workload_class_version(CreateWorkloadClassVersionRequest {
            idempotency_key: format!("kind-e2e-lifecycle-{idempotency_suffix}-class"),
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
            template: Some(ManifestTemplate {
                workload: Some(workload_template(kind, workload_name, app_image)),
                sidecar: Some(sidecar_template(sidecar_image)),
                service: Some(service_template(workload_name)),
                volumes,
            }),
            sleep_policy: Some(sleep_policy()),
            exclusivity_keys: vec![],
        })
        .await?;
    Ok(())
}

async fn create_instance_and_route(
    operator: &mut OperatorControlPlaneClient<Channel>,
    class_id: &str,
    instance_id: &str,
    route_id: &str,
    host: &str,
    target: &str,
    idempotency_suffix: &str,
) -> TestResult<()> {
    create_instance(operator, class_id, instance_id, target, idempotency_suffix).await?;
    create_route(operator, route_id, instance_id, host, idempotency_suffix).await
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
            idempotency_key: format!("kind-e2e-lifecycle-{idempotency_suffix}-instance"),
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
            idempotency_key: format!("kind-e2e-lifecycle-{idempotency_suffix}-route"),
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
            Ok(channel) => return Ok(OperatorControlPlaneClient::new(channel)),
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for operator gRPC endpoint {endpoint}: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn connect_proxy(endpoint: &str) -> TestResult<ProxyControlPlaneClient<Channel>> {
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(30))
        .connect()
        .await?;
    Ok(ProxyControlPlaneClient::new(channel))
}

async fn connect_sidecar(endpoint: &str) -> TestResult<SidecarControlPlaneClient<Channel>> {
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(30))
        .connect()
        .await?;
    Ok(SidecarControlPlaneClient::new(channel))
}

async fn get_instance(
    operator: &mut OperatorControlPlaneClient<Channel>,
    instance_id: &str,
) -> TestResult<Instance> {
    Ok(operator
        .get_instance(control_plane::api::pb::GetInstanceRequest {
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
        .get_instance(control_plane::api::pb::GetInstanceRequest {
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
    timeout_secs: u64,
) -> TestResult<Instance> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
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

fn assert_state(instance: &Instance, expected: PbInstanceState) -> TestResult<()> {
    let actual = instance_state(instance);
    if actual != expected {
        return Err(format!(
            "expected instance {} state {expected:?}, got {actual:?}",
            instance.instance_id
        )
        .into());
    }
    Ok(())
}

fn instance_state(instance: &Instance) -> PbInstanceState {
    PbInstanceState::try_from(instance.state).unwrap_or(PbInstanceState::Unspecified)
}

async fn wait_for_instance_response(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    timeout_secs: u64,
) -> TestResult<HttpResponse> {
    wait_for_instance_response_rejecting_stale(
        config,
        context,
        host,
        path,
        target,
        &[],
        timeout_secs,
    )
    .await
}

async fn wait_for_instance_response_rejecting_stale(
    config: &E2eConfig,
    context: &str,
    host: &str,
    path: &str,
    target: &str,
    stale_targets: &[&str],
    timeout_secs: u64,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let last_error = match http_get(config.frontline_addr, host, path).await {
            Ok(response)
                if response.status == 200 && response_body_identifies(&response, target) =>
            {
                assert_response_identifies(&response, target)?;
                return Ok(response);
            }
            Ok(response) => {
                for stale in stale_targets {
                    if response.status == 200 && response_body_identifies(&response, stale) {
                        return Err(format!(
                            "{context} served stale target {stale:?}: {:?}",
                            response.body
                        )
                        .into());
                    }
                }
                format!("HTTP {} body {:?}", response.status, response.body)
            }
            Err(error) => error.to_string(),
        };
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for {context} host {host} path {path}: {last_error}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_frontline_status(
    config: &E2eConfig,
    host: &str,
    path: &str,
    status: u16,
    timeout_secs: u64,
) -> TestResult<HttpResponse> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match http_get(config.frontline_addr, host, path).await {
            Ok(response) if response.status == status => return Ok(response),
            Ok(response) if Instant::now() >= deadline => {
                return Err(format!(
                    "timed out waiting for HTTP {status}; got {} body {:?}",
                    response.status, response.body
                )
                .into());
            }
            Err(error) if Instant::now() >= deadline => return Err(error),
            _ => sleep(Duration::from_secs(1)).await,
        }
    }
}

async fn assert_no_backend_response(
    config: &E2eConfig,
    host: &str,
    stale_target: &str,
    attempts: usize,
) -> TestResult<()> {
    for _ in 0..attempts {
        if let Ok(response) = http_get(config.frontline_addr, host, "/").await {
            if response.status == 200 && response_body_identifies(&response, stale_target) {
                return Err(format!(
                    "deleted route served stale backend target {stale_target:?}: {:?}",
                    response.body
                )
                .into());
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    Ok(())
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
    Ok(HttpResponse {
        status,
        body: body.to_owned(),
    })
}

fn response_body_identifies(response: &HttpResponse, target: &str) -> bool {
    response.body.contains(APP_MARKER) && response.body.contains(&format!("instance={target}\n"))
}

fn assert_response_identifies(response: &HttpResponse, target: &str) -> TestResult<()> {
    if response.status != 200 || !response_body_identifies(response, target) {
        return Err(format!(
            "expected HTTP 200 response for target {target:?}, got {} body {:?}",
            response.status, response.body
        )
        .into());
    }
    Ok(())
}

async fn next_subscribe_response(
    responses: &mut tonic::Streaming<control_plane::api::pb::ProxySubscribeResponse>,
) -> TestResult<control_plane::api::pb::ProxySubscribeResponse> {
    timeout(Duration::from_secs(30), responses.next())
        .await?
        .ok_or("subscribe stream closed")?
        .map_err(|error| error.into())
}

fn subscribe_route_request(request_id: &str, host: &str) -> ProxySubscribeRequest {
    ProxySubscribeRequest {
        input: Some(proxy_subscribe_request::Input::SubscribeRoute(
            ProxySubscribeRouteRequest {
                request_id: request_id.to_owned(),
                identity: Some(http_route_identity(host)),
            },
        )),
    }
}

fn expect_route_resolved(
    response: control_plane::api::pb::ProxySubscribeResponse,
) -> control_plane::api::pb::ProxyRouteResolvedResponse {
    let Some(proxy_subscribe_response::Output::RouteResolved(resolved)) = response.output else {
        panic!("expected route resolved response");
    };
    resolved
}

fn expect_route_invalidated(
    response: control_plane::api::pb::ProxySubscribeResponse,
) -> control_plane::api::pb::ProxyRouteInvalidatedResponse {
    let Some(proxy_subscribe_response::Output::RouteInvalidated(invalidated)) = response.output
    else {
        panic!("expected route invalidated response");
    };
    invalidated
}

fn load_image_into_kind(config: &E2eConfig, image: &str) -> TestResult<()> {
    let status = Command::new("kind")
        .args([
            "load",
            "docker-image",
            image,
            "--name",
            &config.cluster_name,
        ])
        .status()?;
    if !status.success() {
        return Err(format!(
            "kind load docker-image {image} --name {} failed with status {status}",
            config.cluster_name
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

async fn set_control_plane_statefulset_delete_permission(
    kube: Client,
    namespace: &str,
    allow: bool,
) -> TestResult<()> {
    let roles: Api<Role> = Api::namespaced(kube, namespace);
    let mut role = roles.get("sleepypods-control-plane").await?;
    let rules = role
        .rules
        .as_mut()
        .ok_or("sleepypods-control-plane Role has no rules")?;
    let rule = rules
        .iter_mut()
        .find(|rule| {
            rule.api_groups
                .as_ref()
                .is_some_and(|groups| groups.iter().any(|group| group == "apps"))
                && rule.resources.as_ref().is_some_and(|resources| {
                    resources.iter().any(|resource| resource == "statefulsets")
                })
        })
        .ok_or("sleepypods-control-plane Role has no StatefulSet rule")?;

    if allow {
        if !rule.verbs.iter().any(|verb| verb == "delete") {
            rule.verbs.push("delete".to_owned());
        }
    } else {
        rule.verbs.retain(|verb| verb != "delete");
    }

    roles
        .replace("sleepypods-control-plane", &PostParams::default(), &role)
        .await?;
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
    timeout_secs: u64,
) -> TestResult<()> {
    let deployments: Api<Deployment> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube, namespace);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if is_not_found(deployments.get(workload_name).await)
            && is_not_found(services.get(workload_name).await)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for workload {namespace}/{workload_name} absence"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_stateful_objects_present(
    kube: Client,
    namespace: &str,
    workload_name: &str,
    pvc_name: &str,
    pv_name: &str,
    timeout_secs: u64,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let pvs: Api<PersistentVolume> = Api::all(kube);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if stateful_sets.get(workload_name).await.is_ok()
            && services.get(workload_name).await.is_ok()
            && pvcs.get(pvc_name).await.is_ok()
            && pvs.get(pv_name).await.is_ok()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for StatefulSet/Service/PVC/PV for {namespace}/{workload_name}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_stateful_objects_absent(
    kube: Client,
    namespace: &str,
    workload_name: &str,
    pvc_name: &str,
    pv_name: &str,
    timeout_secs: u64,
) -> TestResult<()> {
    let stateful_sets: Api<StatefulSet> = Api::namespaced(kube.clone(), namespace);
    let services: Api<Service> = Api::namespaced(kube.clone(), namespace);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(kube.clone(), namespace);
    let pvs: Api<PersistentVolume> = Api::all(kube);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if is_not_found(stateful_sets.get(workload_name).await)
            && is_not_found(services.get(workload_name).await)
            && is_not_found(pvcs.get(pvc_name).await)
            && is_not_found(pvs.get(pv_name).await)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for StatefulSet/Service/PVC/PV cleanup for {namespace}/{workload_name}"
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn is_not_found<T>(result: Result<T, KubeError>) -> bool {
    matches!(result, Err(KubeError::Api(status)) if status.is_not_found())
}

fn workload_template(kind: WorkloadKind, _name: &str, image: &str) -> WorkloadTemplate {
    WorkloadTemplate {
        kind: kind as i32,
        name: Some(target_text("lifecycle-", "")),
        replicas: Some(1),
        app_container: Some(ContainerTemplate {
            name: "app".to_owned(),
            image: Some(literal_text(image)),
            ports: vec![ContainerPortTemplate {
                name: Some("http".to_owned()),
                container_port: APP_PORT,
            }],
            env: vec![control_plane::api::pb::EnvVarTemplate {
                name: "SLEEPYPODS_E2E_INSTANCE".to_owned(),
                value: Some(target_text("", "")),
            }],
        }),
    }
}

fn sidecar_template(image: &str) -> SidecarTemplate {
    SidecarTemplate {
        name: "sleepypods-sidecar".to_owned(),
        image: Some(literal_text(image)),
        listen_port: SIDECAR_PORT,
        mode: None,
    }
}

fn service_template(_name: &str) -> ServiceTemplate {
    ServiceTemplate {
        name: Some(target_text("lifecycle-", "")),
        ports: vec![ServicePortTemplate {
            name: Some("http".to_owned()),
            port: APP_PORT,
            target_port: APP_PORT,
        }],
    }
}

fn sleep_policy() -> WorkloadSleepPolicy {
    WorkloadSleepPolicy {
        idle_timeout_ms: 300_000,
        idle_retry_backoff_ms: 500,
        drain_grace_timeout_ms: 500,
        idle_timeout_override: None,
    }
}

fn stateful_volumes(pv_name: &str, pvc_name: &str, host_path: &str) -> Vec<VolumeTemplate> {
    vec![VolumeTemplate {
        name: "data".to_owned(),
        mount_path: Some(literal_text("/data")),
        pv_name: Some(literal_text(pv_name)),
        pvc_name: Some(literal_text(pvc_name)),
        access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
        capacity: Some(literal_text("1Mi")),
        reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
        storage_class_name: Some(literal_text("sleepypods-kind-static")),
        source: Some(PersistentVolumeSourceTemplate {
            kind: Some(
                control_plane::api::pb::persistent_volume_source_template::Kind::HostPath(
                    HostPathVolumeSourceTemplate {
                        path: Some(literal_text(host_path)),
                        r#type: Some(literal_text("DirectoryOrCreate")),
                    },
                ),
            ),
        }),
    }]
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
