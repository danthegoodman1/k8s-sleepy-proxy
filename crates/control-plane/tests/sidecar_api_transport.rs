use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use control_plane::api::{
    pb::{
        sidecar_control_plane_server::SidecarControlPlane, sidecar_report_idle_response,
        SidecarReportIdleRequest, SidecarReportIdleResponse, SidecarReportIdleUnavailableReason,
    },
    sidecar_grpc_service_with_store, StoreBackedSidecarApi, StoreBackedSidecarGrpcService,
    OPERATOR_UNARY_METHODS, SIDECAR_SERVICE_NAME,
};
use control_plane::{
    AuthConfig, BackendEndpoint, BeginSleepRequest, BeginSleepResult, CallerRole, ControlPlaneAuth,
    ControlPlaneStore, FinalizeSleepRequest, FinalizeSleepResult, Generation, InstanceId,
    InstanceRecord, InstanceState as DomainInstanceState, KubernetesClientFuture,
    KubernetesClientResult, KubernetesMaterializer, KubernetesMaterializerClient,
    MaterializationId, MaterializationRecord, MaterializationState, MaterializationTarget,
    RenderedObjectRef, StateTransitionReason, StaticBearerTokens, StoreError, StoreFuture,
    StoreResult,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{header, HeaderMap, HeaderValue, Request, Version};
use tonic::server::NamedService;
use tonic::Code;
use tower::ServiceExt;

fn auth_config() -> AuthConfig {
    AuthConfig::static_bearer_tokens(
        StaticBearerTokens::new("operator-token", "proxy-token", "sidecar-token")
            .expect("auth tokens are valid"),
    )
}

fn control_plane_auth() -> ControlPlaneAuth {
    ControlPlaneAuth::from_config(auth_config(), Default::default())
}

fn authenticated_sidecar_service(
    store: Arc<dyn ControlPlaneStore>,
    client: FakeKubernetesClient,
) -> tonic::service::interceptor::InterceptedService<
    StoreBackedSidecarGrpcService<FakeKubernetesClient>,
    control_plane::ControlPlaneAuthInterceptor,
> {
    tonic::service::interceptor::InterceptedService::new(
        sidecar_grpc_service_with_store(store, KubernetesMaterializer::new(client), target()),
        control_plane_auth().interceptor(SIDECAR_SERVICE_NAME, CallerRole::Sidecar),
    )
}

#[test]
fn generated_api_contains_sidecar_report_idle_shape_without_operator_surface_change() {
    let request = SidecarReportIdleRequest {
        instance_id: "instance-1".to_owned(),
        expected_generation: 7,
        active_count: 0,
    };
    let response = SidecarReportIdleResponse {
        outcome: Some(sidecar_report_idle_response::Outcome::Accepted(
            control_plane::api::pb::SidecarReportIdleAccepted {
                instance_id: request.instance_id.clone(),
                instance_generation: request.expected_generation + 1,
            },
        )),
    };

    assert_eq!(request.instance_id, "instance-1");
    assert_eq!(request.active_count, 0);
    assert!(matches!(
        response.outcome,
        Some(sidecar_report_idle_response::Outcome::Accepted(_))
    ));
    assert_eq!(
        <StoreBackedSidecarGrpcService<FakeKubernetesClient> as NamedService>::NAME,
        SIDECAR_SERVICE_NAME
    );
    assert!(!OPERATOR_UNARY_METHODS.contains(&"ReportIdle"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"SidecarControlPlane/ReportIdle"));
}

#[tokio::test]
async fn report_idle_running_instance_transitions_to_draining() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-running",
        DomainInstanceState::Running,
        7,
    ));
    store.seed_materialization(ready_materialization("instance-running", 7));
    let client = FakeKubernetesClient::default();
    let service = sidecar_api(store.clone(), client.clone());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            instance_id: "instance-running".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .expect("idle report succeeds")
        .into_inner();

    let accepted = expect_accepted(response);
    assert_eq!(accepted.instance_id, "instance-running");
    assert_eq!(accepted.instance_generation, 9);

    let instance = store.instance();
    assert_eq!(instance.state, DomainInstanceState::Cold);
    assert_eq!(instance.generation, Generation::new(9));
    let transitions = store.transition_requests();
    assert_eq!(transitions.len(), 2);
    assert_eq!(transitions[0].expected_generation, Generation::new(7));
    assert_eq!(transitions[0].next_state, DomainInstanceState::Draining);
    assert_eq!(transitions[0].reason, StateTransitionReason::IdleReported);
    assert_eq!(transitions[1].expected_generation, Generation::new(8));
    assert_eq!(transitions[1].next_state, DomainInstanceState::Cold);
    assert_eq!(transitions[1].reason, StateTransitionReason::DrainCompleted);
    assert_eq!(
        client.deleted_objects(),
        vec![object_ref(
            "apps/v1",
            "Deployment",
            "apps",
            "instance-running"
        )]
    );
    let materialization = store
        .materialization()
        .expect("materialization remains recorded");
    assert_eq!(materialization.state, MaterializationState::Deleted);
    assert!(materialization.rendered_objects.is_empty());
}

#[tokio::test]
async fn report_idle_from_rendered_waking_generation_sleeps_current_running_instance() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-rendered-generation",
        DomainInstanceState::Running,
        8,
    ));
    store.seed_materialization(ready_materialization("instance-rendered-generation", 8));
    let client = FakeKubernetesClient::default();
    let service = sidecar_api(store.clone(), client.clone());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            instance_id: "instance-rendered-generation".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .expect("idle report from rendered wake generation succeeds")
        .into_inner();

    let accepted = expect_accepted(response);
    assert_eq!(accepted.instance_id, "instance-rendered-generation");
    assert_eq!(accepted.instance_generation, 10);
    assert_eq!(store.instance().state, DomainInstanceState::Cold);
    assert_eq!(store.instance().generation, Generation::new(10));
    let transitions = store.transition_requests();
    assert_eq!(transitions.len(), 2);
    assert_eq!(transitions[0].expected_generation, Generation::new(8));
    assert_eq!(transitions[0].next_state, DomainInstanceState::Draining);
    assert_eq!(transitions[1].expected_generation, Generation::new(9));
    assert_eq!(transitions[1].next_state, DomainInstanceState::Cold);
    assert_eq!(
        client.deleted_objects(),
        vec![object_ref(
            "apps/v1",
            "Deployment",
            "apps",
            "instance-rendered-generation"
        )]
    );
}

#[tokio::test]
async fn report_idle_restart_during_sleep_resumes_cleanup_from_store_state() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-retry",
        DomainInstanceState::Running,
        7,
    ));
    store.seed_materialization(ready_materialization("instance-delete-retry", 7));
    let client = FakeKubernetesClient::failing_deletes(1);
    let service = sidecar_api(store.clone(), client.clone());
    let request = SidecarReportIdleRequest {
        instance_id: "instance-delete-retry".to_owned(),
        expected_generation: 7,
        active_count: 0,
    };

    let error = service
        .report_idle(tonic::Request::new(request.clone()))
        .await
        .expect_err("first delete failure is retryable");

    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("sleep cleanup failed"));
    assert_eq!(store.instance().state, DomainInstanceState::Draining);
    assert_eq!(store.instance().generation, Generation::new(8));
    let materialization = store
        .materialization()
        .expect("materialization remains after failed delete");
    assert_eq!(materialization.state, MaterializationState::Deleting);
    assert_eq!(
        materialization.rendered_objects,
        vec![object_ref(
            "apps/v1",
            "Deployment",
            "apps",
            "instance-delete-retry"
        )]
    );

    let retry_client = FakeKubernetesClient::default();
    let retry_service = sidecar_api(store.clone(), retry_client.clone());
    let response = retry_service
        .report_idle(tonic::Request::new(request))
        .await
        .expect("recreated service resumes cleanup")
        .into_inner();

    let accepted = expect_accepted(response);
    assert_eq!(accepted.instance_id, "instance-delete-retry");
    assert_eq!(accepted.instance_generation, 9);
    assert_eq!(store.instance().state, DomainInstanceState::Cold);
    let materialization = store
        .materialization()
        .expect("materialization remains recorded");
    assert_eq!(materialization.state, MaterializationState::Deleted);
    assert!(materialization.rendered_objects.is_empty());
    assert_eq!(
        retry_client.deleted_objects(),
        vec![object_ref(
            "apps/v1",
            "Deployment",
            "apps",
            "instance-delete-retry"
        )]
    );
}

#[tokio::test]
async fn report_idle_nonzero_active_count_returns_failed_precondition_without_mutation() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-active",
        DomainInstanceState::Running,
        7,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());

    let error = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            instance_id: "instance-active".to_owned(),
            expected_generation: 7,
            active_count: 1,
        }))
        .await
        .expect_err("active requests prevent idle report");

    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("active_count 0"));
    assert!(error.message().contains("found 1"));
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Running);
    assert_eq!(store.instance().generation, Generation::new(7));
}

#[tokio::test]
async fn report_idle_duplicate_at_next_draining_generation_is_idempotent() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-draining",
        DomainInstanceState::Draining,
        8,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            instance_id: "instance-draining".to_owned(),
            expected_generation: 7,
            active_count: 0,
        }))
        .await
        .expect("duplicate idle report is a domain response")
        .into_inner();

    let already = expect_already_draining(response);
    assert_eq!(already.instance_id, "instance-draining");
    assert_eq!(already.instance_generation, 8);
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Draining);
    assert_eq!(store.instance().generation, Generation::new(8));
}

#[tokio::test]
async fn report_idle_stale_generation_conflict_is_structured_response() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-conflict",
        DomainInstanceState::Draining,
        8,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            instance_id: "instance-conflict".to_owned(),
            expected_generation: 6,
            active_count: 0,
        }))
        .await
        .expect("generation conflict is a domain response")
        .into_inner();

    let conflict = expect_generation_conflict(response);
    assert_eq!(conflict.instance_id, "instance-conflict");
    assert_eq!(conflict.expected_generation, 6);
    assert_eq!(conflict.actual_generation, 8);
    assert_eq!(store.transition_requests().len(), 0);
}

#[tokio::test]
async fn report_idle_non_running_matching_generation_returns_unavailable() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-waking",
        DomainInstanceState::Waking,
        4,
    ));
    let service = sidecar_api(store.clone(), FakeKubernetesClient::default());

    let response = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            instance_id: "instance-waking".to_owned(),
            expected_generation: 4,
            active_count: 0,
        }))
        .await
        .expect("unavailable state is a domain response")
        .into_inner();

    let unavailable = expect_unavailable(response);
    assert_eq!(unavailable.instance_id, "instance-waking");
    assert_eq!(unavailable.instance_generation, 4);
    assert_eq!(
        unavailable.reason,
        SidecarReportIdleUnavailableReason::Waking as i32
    );
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Waking);
}

#[tokio::test]
async fn report_idle_invalid_instance_id_returns_invalid_argument() {
    let service = sidecar_api(
        Arc::new(FakeSidecarStore::default()),
        FakeKubernetesClient::default(),
    );

    let error = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            instance_id: "".to_owned(),
            expected_generation: 1,
            active_count: 0,
        }))
        .await
        .expect_err("empty instance id is invalid");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("must not be empty"));
}

#[tokio::test]
async fn report_idle_store_unavailable_returns_grpc_unavailable() {
    let store = Arc::new(FakeSidecarStore::default());
    store.fail_get_with(FakeStoreError::Unavailable("database is down"));
    let service = sidecar_api(store, FakeKubernetesClient::default());

    let error = service
        .report_idle(tonic::Request::new(SidecarReportIdleRequest {
            instance_id: "instance-store-error".to_owned(),
            expected_generation: 1,
            active_count: 0,
        }))
        .await
        .expect_err("store unavailability is a transport error");

    assert_eq!(error.code(), Code::Unavailable);
    assert_eq!(error.message(), "database is down");
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_sidecar_report_idle() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-transport",
        DomainInstanceState::Running,
        2,
    ));

    let response = sidecar_grpc_service_with_store(
        store.clone(),
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target(),
    )
    .oneshot(grpc_sidecar_report_idle_request(
        SidecarReportIdleRequest {
            instance_id: "instance-transport".to_owned(),
            expected_generation: 2,
            active_count: 0,
        },
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("native gRPC request should route through sidecar service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("successful gRPC status is returned");

    assert_eq!(status, "0");
    let accepted = expect_accepted(decode_grpc_sidecar_report_idle_response(
        collected.to_bytes().as_ref(),
    ));
    assert_eq!(accepted.instance_id, "instance-transport");
    assert_eq!(accepted.instance_generation, 4);
    assert_eq!(store.instance().state, DomainInstanceState::Cold);
}

#[tokio::test]
async fn native_grpc_sidecar_auth_rejects_report_idle_before_sleep_logic() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-auth-idle",
        DomainInstanceState::Running,
        7,
    ));
    store.seed_materialization(ready_materialization("instance-auth-idle", 7));
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let missing_status = collect_grpc_status(
        authenticated_sidecar_service(
            Arc::clone(&store_for_service),
            FakeKubernetesClient::default(),
        )
        .oneshot(grpc_sidecar_report_idle_request(
            SidecarReportIdleRequest {
                instance_id: "instance-auth-idle".to_owned(),
                expected_generation: 7,
                active_count: 0,
            },
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("missing auth returns gRPC response"),
    )
    .await;
    assert_eq!(missing_status, "16");
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Running);

    let wrong_role_status = collect_grpc_status(
        authenticated_sidecar_service(store_for_service, FakeKubernetesClient::default())
            .oneshot(with_authorization(
                grpc_sidecar_report_idle_request(
                    SidecarReportIdleRequest {
                        instance_id: "instance-auth-idle".to_owned(),
                        expected_generation: 7,
                        active_count: 0,
                    },
                    "application/grpc",
                    Version::HTTP_2,
                ),
                "Bearer proxy-token",
            ))
            .await
            .expect("wrong role returns gRPC response"),
    )
    .await;
    assert_eq!(wrong_role_status, "7");
    assert_eq!(store.transition_requests().len(), 0);
    assert_eq!(store.instance().state, DomainInstanceState::Running);
}

#[tokio::test]
async fn native_grpc_sidecar_auth_accepts_valid_report_idle_credentials() {
    let store = Arc::new(FakeSidecarStore::default());
    store.seed_instance(domain_instance(
        "instance-auth-valid-idle",
        DomainInstanceState::Running,
        7,
    ));
    store.seed_materialization(ready_materialization("instance-auth-valid-idle", 7));
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();

    let response =
        authenticated_sidecar_service(store_for_service, FakeKubernetesClient::default())
            .oneshot(with_authorization(
                grpc_sidecar_report_idle_request(
                    SidecarReportIdleRequest {
                        instance_id: "instance-auth-valid-idle".to_owned(),
                        expected_generation: 7,
                        active_count: 0,
                    },
                    "application/grpc",
                    Version::HTTP_2,
                ),
                "Bearer sidecar-token",
            ))
            .await
            .expect("valid sidecar credentials dispatch report idle");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    assert_eq!(grpc_status(&headers, trailers.as_ref()), "0");
    assert_eq!(store.transition_requests().len(), 2);
    assert_eq!(store.instance().state, DomainInstanceState::Cold);
}

fn grpc_sidecar_report_idle_request(
    request: SidecarReportIdleRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    let mut body = BytesMut::new();
    encode_grpc_message(request, &mut body);

    Request::builder()
        .version(version)
        .method("POST")
        .uri("/sleepypods.controlplane.v1.SidecarControlPlane/ReportIdle")
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::new(Full::new(body.freeze())))
        .expect("request builds")
}

fn with_authorization(mut request: Request<Body>, value: &'static str) -> Request<Body> {
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, HeaderValue::from_static(value));
    request
}

fn encode_grpc_message<M: Message>(request: M, body: &mut BytesMut) {
    let mut message = BytesMut::new();
    request.encode(&mut message).expect("request encodes");

    body.put_u8(0);
    body.put_u32(message.len() as u32);
    body.extend_from_slice(&message);
}

fn decode_grpc_sidecar_report_idle_response(bytes: &[u8]) -> SidecarReportIdleResponse {
    assert_eq!(bytes.first(), Some(&0), "gRPC message is uncompressed");
    let length = u32::from_be_bytes(
        bytes[1..5]
            .try_into()
            .expect("gRPC response frame has a length prefix"),
    ) as usize;
    SidecarReportIdleResponse::decode(&bytes[5..5 + length]).expect("gRPC response decodes")
}

async fn collect_grpc_status<B>(response: http::Response<B>) -> String
where
    B: tonic::codegen::Body<Data = Bytes>,
    B::Error: std::fmt::Debug,
{
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    grpc_status(&headers, trailers.as_ref())
}

fn grpc_status(headers: &HeaderMap, trailers: Option<&HeaderMap>) -> String {
    trailers
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("gRPC status is returned")
        .to_str()
        .expect("gRPC status is valid")
        .to_owned()
}

fn sidecar_api(
    store: Arc<FakeSidecarStore>,
    client: FakeKubernetesClient,
) -> StoreBackedSidecarApi<FakeKubernetesClient> {
    StoreBackedSidecarApi::new(store, KubernetesMaterializer::new(client), target())
}

fn expect_accepted(
    response: SidecarReportIdleResponse,
) -> control_plane::api::pb::SidecarReportIdleAccepted {
    let Some(sidecar_report_idle_response::Outcome::Accepted(accepted)) = response.outcome else {
        panic!("expected accepted response");
    };
    accepted
}

fn expect_already_draining(
    response: SidecarReportIdleResponse,
) -> control_plane::api::pb::SidecarReportIdleAlreadyDraining {
    let Some(sidecar_report_idle_response::Outcome::AlreadyDraining(already_draining)) =
        response.outcome
    else {
        panic!("expected already-draining response");
    };
    already_draining
}

fn expect_generation_conflict(
    response: SidecarReportIdleResponse,
) -> control_plane::api::pb::SidecarReportIdleGenerationConflict {
    let Some(sidecar_report_idle_response::Outcome::GenerationConflict(conflict)) =
        response.outcome
    else {
        panic!("expected generation-conflict response");
    };
    conflict
}

fn expect_unavailable(
    response: SidecarReportIdleResponse,
) -> control_plane::api::pb::SidecarReportIdleUnavailable {
    let Some(sidecar_report_idle_response::Outcome::Unavailable(unavailable)) = response.outcome
    else {
        panic!("expected unavailable response");
    };
    unavailable
}

#[derive(Clone, Copy)]
enum FakeStoreError {
    Unavailable(&'static str),
}

#[derive(Clone, Debug, Default)]
struct FakeKubernetesClient {
    deleted_objects: Arc<Mutex<Vec<RenderedObjectRef>>>,
    failing_deletes: Arc<Mutex<usize>>,
}

impl FakeKubernetesClient {
    fn failing_deletes(count: usize) -> Self {
        Self {
            deleted_objects: Arc::new(Mutex::new(Vec::new())),
            failing_deletes: Arc::new(Mutex::new(count)),
        }
    }

    fn deleted_objects(&self) -> Vec<RenderedObjectRef> {
        self.deleted_objects
            .lock()
            .expect("fake kubernetes lock is available")
            .clone()
    }
}

impl KubernetesMaterializerClient for FakeKubernetesClient {
    fn apply_object<'a>(
        &'a self,
        _object: &'a control_plane::KubernetesObject,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let mut failing_deletes = self
                .failing_deletes
                .lock()
                .expect("fake kubernetes lock is available");
            if *failing_deletes > 0 {
                *failing_deletes -= 1;
                return Err(control_plane::KubernetesClientError::new(
                    "transient delete failure",
                ));
            }
            drop(failing_deletes);

            self.deleted_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .push(object.clone());
            Ok(())
        })
    }

    fn wait_for_pvc_bound<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async {
            BackendEndpoint::new("http://example")
                .map_err(|error| control_plane::KubernetesClientError::new(error.to_string()))
        })
    }
}

#[derive(Default)]
struct FakeSidecarStore {
    instance: Mutex<Option<InstanceRecord>>,
    materialization: Mutex<Option<MaterializationRecord>>,
    transition_requests: Mutex<Vec<control_plane::CompareAndSwapInstanceStateRequest>>,
    get_error: Mutex<Option<FakeStoreError>>,
}

impl FakeSidecarStore {
    fn seed_instance(&self, instance: InstanceRecord) {
        *self.instance.lock().expect("fake store lock is available") = Some(instance);
    }

    fn seed_materialization(&self, materialization: MaterializationRecord) {
        *self
            .materialization
            .lock()
            .expect("fake store lock is available") = Some(materialization);
    }

    fn fail_get_with(&self, error: FakeStoreError) {
        *self.get_error.lock().expect("fake store lock is available") = Some(error);
    }

    fn instance(&self) -> InstanceRecord {
        self.instance
            .lock()
            .expect("fake store lock is available")
            .clone()
            .expect("fake instance is present")
    }

    fn transition_requests(&self) -> Vec<control_plane::CompareAndSwapInstanceStateRequest> {
        self.transition_requests
            .lock()
            .expect("fake store lock is available")
            .clone()
    }

    fn materialization(&self) -> Option<MaterializationRecord> {
        self.materialization
            .lock()
            .expect("fake store lock is available")
            .clone()
    }

    fn transition_instance(
        &self,
        instance_id: InstanceId,
        expected_generation: Generation,
        next_state: DomainInstanceState,
        reason: StateTransitionReason,
    ) -> StoreResult<InstanceRecord> {
        let mut instance = self.instance.lock().expect("fake store lock is available");
        let instance = instance.as_mut().ok_or(StoreError::NotFound {
            resource: "instance",
        })?;
        if instance.id != instance_id {
            return Err(StoreError::NotFound {
                resource: "instance",
            });
        }
        if instance.generation != expected_generation {
            return Err(StoreError::GenerationConflict {
                expected: expected_generation,
                actual: instance.generation,
            });
        }

        control_plane::instance::validate_instance_state_transition(
            instance.state,
            next_state,
            &reason,
        )
        .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

        self.transition_requests
            .lock()
            .expect("fake store lock is available")
            .push(control_plane::CompareAndSwapInstanceStateRequest {
                instance_id,
                expected_generation,
                next_state,
                reason,
            });
        instance.state = next_state;
        instance.generation = expected_generation.next();
        Ok(instance.clone())
    }

    fn mark_materialization(
        &self,
        instance_id: InstanceId,
        target: MaterializationTarget,
        state: MaterializationState,
        instance_generation: Option<Generation>,
        rendered_objects: Option<Vec<RenderedObjectRef>>,
    ) -> Option<MaterializationRecord> {
        let mut materialization = self
            .materialization
            .lock()
            .expect("fake store lock is available");
        let materialization = materialization.as_mut().filter(|materialization| {
            materialization.instance_id == instance_id
                && materialization.target == target
                && materialization.state != MaterializationState::Deleted
        })?;

        materialization.state = state;
        materialization.backend = None;
        if let Some(instance_generation) = instance_generation {
            materialization.instance_generation = instance_generation;
        }
        if let Some(rendered_objects) = rendered_objects {
            materialization.rendered_objects = rendered_objects;
        }

        Some(materialization.clone())
    }
}

impl ControlPlaneStore for FakeSidecarStore {
    fn create_instance<'a>(
        &'a self,
        _request: control_plane::CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::CreateInstanceResult>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn get_instance<'a>(
        &'a self,
        request: control_plane::GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async move {
            if let Some(error) = *self.get_error.lock().expect("fake store lock is available") {
                return Err(match error {
                    FakeStoreError::Unavailable(message) => StoreError::unavailable(message),
                });
            }

            Ok(self
                .instance
                .lock()
                .expect("fake store lock is available")
                .as_ref()
                .filter(|instance| instance.id == request.instance_id)
                .cloned())
        })
    }

    fn delete_instance<'a>(
        &'a self,
        _request: control_plane::DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn create_workload_class_version<'a>(
        &'a self,
        _request: control_plane::CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::WorkloadClassVersion>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        _request: control_plane::LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::WorkloadClassVersion>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn create_route_binding<'a>(
        &'a self,
        _request: control_plane::CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteBindingRecord>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn get_route_binding<'a>(
        &'a self,
        _request: control_plane::GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::RouteBindingRecord>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn delete_route_binding<'a>(
        &'a self,
        _request: control_plane::DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn resolve_route<'a>(
        &'a self,
        _identity: control_plane::RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteResolution>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: control_plane::CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        Box::pin(async move {
            let mut instance = self.instance.lock().expect("fake store lock is available");
            let instance = instance.as_mut().ok_or(StoreError::NotFound {
                resource: "instance",
            })?;
            if instance.id != request.instance_id {
                return Err(StoreError::NotFound {
                    resource: "instance",
                });
            }
            if instance.generation != request.expected_generation {
                return Err(StoreError::GenerationConflict {
                    expected: request.expected_generation,
                    actual: instance.generation,
                });
            }

            control_plane::instance::validate_instance_state_transition(
                instance.state,
                request.next_state,
                &request.reason,
            )
            .map_err(|error| StoreError::invalid_argument(error.to_string()))?;

            self.transition_requests
                .lock()
                .expect("fake store lock is available")
                .push(request.clone());
            instance.state = request.next_state;
            instance.generation = request.expected_generation.next();
            Ok(instance.clone())
        })
    }

    fn record_materialization<'a>(
        &'a self,
        _request: control_plane::RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::MaterializationRecord>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn load_ready_materialization<'a>(
        &'a self,
        _request: control_plane::materialization::LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::MaterializationRecord>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn load_active_materialization<'a>(
        &'a self,
        request: control_plane::LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::MaterializationRecord>>> {
        Box::pin(async move {
            Ok(self
                .materialization
                .lock()
                .expect("fake store lock is available")
                .as_ref()
                .filter(|materialization| {
                    materialization.instance_id == request.instance_id
                        && materialization.target == request.target
                        && materialization.state != MaterializationState::Deleted
                })
                .cloned())
        })
    }

    fn complete_wake<'a>(
        &'a self,
        _request: control_plane::CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::CompleteWakeResult>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        Box::pin(async move {
            let updated = self.transition_instance(
                request.instance_id.clone(),
                request.expected_running_generation,
                DomainInstanceState::Draining,
                StateTransitionReason::IdleReported,
            )?;
            let materialization = self.mark_materialization(
                request.instance_id,
                request.target,
                MaterializationState::Deleting,
                None,
                None,
            );

            Ok(BeginSleepResult {
                instance: updated,
                materialization,
            })
        })
    }

    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        Box::pin(async move {
            let updated = self.transition_instance(
                request.instance_id.clone(),
                request.expected_draining_generation,
                DomainInstanceState::Cold,
                StateTransitionReason::DrainCompleted,
            )?;
            let materialization = self.mark_materialization(
                request.instance_id,
                request.target,
                MaterializationState::Deleted,
                Some(updated.generation),
                Some(Vec::new()),
            );

            Ok(FinalizeSleepResult {
                instance: updated,
                materialization,
            })
        })
    }

    fn lookup_route_dependencies<'a>(
        &'a self,
        _request: control_plane::RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::RouteDependencySet>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn put_http01_challenge<'a>(
        &'a self,
        _request: control_plane::PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::Http01ChallengeRecord>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        _key: control_plane::Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::Http01ChallengeRecord>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        _request: control_plane::DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        _request: control_plane::ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }
}

fn domain_instance(id: &str, state: DomainInstanceState, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id.to_owned()).expect("instance id is valid"),
        workload_class: control_plane::WorkloadClassVersionRef::new(
            control_plane::WorkloadClassId::new("class-1").expect("class id is valid"),
            Generation::new(1),
        ),
        values: Default::default(),
        state,
        generation: Generation::new(generation),
    }
}

fn ready_materialization(instance_id: &str, generation: u64) -> MaterializationRecord {
    MaterializationRecord {
        id: MaterializationId::new(format!("{}:cluster-a:apps", instance_id))
            .expect("materialization id is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance id is valid"),
        instance_generation: Generation::new(generation),
        target: target(),
        state: MaterializationState::Ready,
        backend: Some(BackendEndpoint::new("http://example").expect("backend is valid")),
        backend_generation: control_plane::BackendGeneration::new(generation),
        rendered_objects: vec![object_ref("apps/v1", "Deployment", "apps", instance_id)],
        exclusivity_keys: vec![],
        reconciliation_lease: None,
    }
}

fn target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("target is valid")
}

fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    }
}
