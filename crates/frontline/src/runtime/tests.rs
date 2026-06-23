use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    net::SocketAddr,
    time::{Duration, Instant, SystemTime},
};

use bytes::Bytes;
use control_plane::{
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, Http01ChallengeKey,
    Http01ChallengeRecord, InstanceId, InstanceState, PathPrefix, RouteBindingId, RouteEntry,
    RouteHost, RouteIdentity,
};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::{
    observability::{
        metrics::RUNTIME_HTTP01_RESULTS_TOTAL_NAME,
        recorder::{InMemoryObservability, ObservabilityEvent, EVENT_HTTP01},
    },
    DrainTracker,
};
use tokio::{net::TcpListener, task::JoinHandle};

use super::FrontlineHttpRuntime;
use crate::{
    FrontlineRouteCoordinator, FrontlineRouteResolver, Http01ChallengeResolveFuture,
    Http01ChallengeResolver, RouteRequestId, RouteSubscriptionClient, RouteSubscriptionFuture,
    SubscribeControlPlaneOutput, SubscriptionId, SubscriptionState, WakeClient, WakeClientFuture,
    WakeInstanceRequest, WakeInstanceResponse, WakeTracker,
};

const READY_RESPONSE: &[u8] = b"ready-from-upstream";
const COLD_RESPONSE: &[u8] = b"cold-woke-upstream";

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestRouteClientError {
    SubscribeFailed,
}

impl fmt::Display for TestRouteClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubscribeFailed => f.write_str("subscribe failed"),
        }
    }
}

impl std::error::Error for TestRouteClientError {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestWakeClientError;

impl fmt::Display for TestWakeClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("wake failed")
    }
}

impl std::error::Error for TestWakeClientError {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestHttp01ResolverError;

impl fmt::Display for TestHttp01ResolverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HTTP-01 resolver failed")
    }
}

impl std::error::Error for TestHttp01ResolverError {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RouteClientCall {
    Subscribe {
        request_id: RouteRequestId,
        identity: RouteIdentity,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FakeRouteClient {
    calls: Vec<RouteClientCall>,
    subscribe_responses: VecDeque<Result<SubscribeControlPlaneOutput, TestRouteClientError>>,
}

impl FakeRouteClient {
    fn push_subscribe_response(&mut self, response: SubscribeControlPlaneOutput) {
        self.subscribe_responses.push_back(Ok(response));
    }

    fn push_subscribe_error(&mut self, error: TestRouteClientError) {
        self.subscribe_responses.push_back(Err(error));
    }
}

impl RouteSubscriptionClient for FakeRouteClient {
    type Error = TestRouteClientError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'_, SubscribeControlPlaneOutput, Self::Error> {
        self.calls.push(RouteClientCall::Subscribe {
            request_id,
            identity,
        });
        let response = self
            .subscribe_responses
            .pop_front()
            .expect("queued subscribe response");
        Box::pin(async move { response })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'_, (), Self::Error> {
        self.calls
            .push(RouteClientCall::Unsubscribe { subscription_id });
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FakeWakeClient {
    calls: Vec<WakeInstanceRequest>,
    responses: VecDeque<Result<WakeInstanceResponse, TestWakeClientError>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FakeHttp01Resolver {
    calls: Vec<(String, String)>,
    responses: VecDeque<Result<Option<Http01ChallengeRecord>, TestHttp01ResolverError>>,
}

impl FakeHttp01Resolver {
    fn push_response(&mut self, response: Option<Http01ChallengeRecord>) {
        self.responses.push_back(Ok(response));
    }

    fn push_error(&mut self, error: TestHttp01ResolverError) {
        self.responses.push_back(Err(error));
    }
}

impl Http01ChallengeResolver for FakeHttp01Resolver {
    type Error = TestHttp01ResolverError;

    fn resolve_http01_challenge(
        &mut self,
        key: Http01ChallengeKey,
    ) -> Http01ChallengeResolveFuture<'_, Self::Error> {
        self.calls
            .push((key.host().as_str().to_owned(), key.token().to_owned()));
        let response = self.responses.pop_front().expect("queued HTTP-01 response");
        Box::pin(async move { response })
    }
}

impl FakeWakeClient {
    fn push_response(&mut self, response: WakeInstanceResponse) {
        self.responses.push_back(Ok(response));
    }
}

impl WakeClient for FakeWakeClient {
    type Error = TestWakeClientError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'_, WakeInstanceResponse, Self::Error> {
        self.calls.push(request);
        let response = self.responses.pop_front().expect("queued wake response");
        Box::pin(async move { response })
    }
}

#[tokio::test]
async fn ready_route_forwards_http_request_to_loopback_upstream() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let request_identity = http_identity("app.example.com", "/ready");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            request_identity.clone(),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let mut runtime =
        runtime_with_state(state, FakeRouteClient::default(), FakeWakeClient::default());

    let response = runtime
        .handle_http(
            Request::builder()
                .method(Method::PUT)
                .uri("/ready?via=runtime")
                .header("host", "app.example.com")
                .body(Full::new(Bytes::from_static(b"request-body")))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        collect_body(response).await,
        Bytes::from_static(READY_RESPONSE)
    );
    assert!(runtime.coordinator().resolver().client().calls.is_empty());
    assert!(runtime.coordinator().wake_client().calls.is_empty());

    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn missing_or_invalid_host_returns_bad_request_without_control_plane_call() {
    let mut runtime = runtime_with_state(
        SubscriptionState::new(4),
        FakeRouteClient::default(),
        FakeWakeClient::default(),
    );

    let missing = runtime
        .handle_http(
            Request::builder()
                .uri("/")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);

    let invalid = runtime
        .handle_http(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    assert!(runtime.coordinator().resolver().client().calls.is_empty());
    assert!(runtime.coordinator().wake_client().calls.is_empty());
}

#[tokio::test]
async fn route_miss_returns_not_found_without_wake() {
    let request_identity = http_identity("missing.example.com", "/");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(miss_response(
        generated_request_id(1),
        request_identity.clone(),
    ));
    let mut runtime = runtime_with_state(
        SubscriptionState::new(4),
        route_client,
        FakeWakeClient::default(),
    );

    let response = runtime
        .handle_http(
            Request::builder()
                .uri("/")
                .header("host", "missing.example.com")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        runtime.coordinator().resolver().client().calls,
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request_identity,
        }]
    );
    assert!(runtime.coordinator().wake_client().calls.is_empty());
}

#[tokio::test]
async fn cold_route_wakes_to_ready_forwards_and_updates_cache() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(2, StatusCode::OK, COLD_RESPONSE).await;
    let request_identity = http_identity("cold.example.com", "/cold");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-cold"),
        request_identity.clone(),
        route_entry(InstanceState::Cold, 4, None),
    ));
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(WakeInstanceResponse::AlreadyRunning {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(4),
        backend: backend(format!("http://{upstream_addr}")),
        backend_generation: Some(BackendGeneration::new(9)),
    });
    let mut runtime = runtime_with_state(SubscriptionState::new(4), route_client, wake_client);

    let first = runtime
        .handle_http(
            Request::builder()
                .uri("/cold")
                .header("host", "cold.example.com")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(collect_body(first).await, Bytes::from_static(COLD_RESPONSE));
    assert_eq!(
        runtime.coordinator().wake_client().calls,
        vec![WakeInstanceRequest {
            instance_id: instance_id("instance-a"),
            expected_generation: Generation::new(4),
        }]
    );

    let second = runtime
        .handle_http(
            Request::builder()
                .uri("/cold")
                .header("host", "cold.example.com")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        collect_body(second).await,
        Bytes::from_static(COLD_RESPONSE)
    );
    assert_eq!(runtime.coordinator().resolver().client().calls.len(), 1);
    assert_eq!(runtime.coordinator().wake_client().calls.len(), 1);

    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn unavailable_route_states_and_control_plane_errors_return_service_unavailable() {
    let unavailable = cached_runtime(InstanceState::Deleted, "deleted.example.com");
    assert_status(
        unavailable,
        "deleted.example.com",
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

    let mut waiting_state = SubscriptionState::new(4);
    waiting_state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-waiting"),
            http_identity("waiting.example.com", "/"),
            route_entry(InstanceState::Waking, 6, None),
        ),
        now(),
    );
    let mut waiting_wake_client = FakeWakeClient::default();
    waiting_wake_client.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(6),
    });
    let mut waiting = runtime_with_state(
        waiting_state,
        FakeRouteClient::default(),
        waiting_wake_client,
    );
    let response = waiting
        .handle_http(
            Request::builder()
                .uri("/")
                .header("host", "waiting.example.com")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        waiting.coordinator().wake_client().calls,
        vec![WakeInstanceRequest {
            instance_id: instance_id("instance-a"),
            expected_generation: Generation::new(6),
        }]
    );

    let request_identity = http_identity("waking.example.com", "/");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-waking"),
        request_identity,
        route_entry(InstanceState::Cold, 4, None),
    ));
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(4),
    });
    assert_status(
        runtime_with_state(SubscriptionState::new(4), route_client, wake_client),
        "waking.example.com",
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_error(TestRouteClientError::SubscribeFailed);
    assert_status(
        runtime_with_state(
            SubscriptionState::new(4),
            route_client,
            FakeWakeClient::default(),
        ),
        "error.example.com",
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
}

#[tokio::test]
async fn forwarding_error_returns_bad_gateway() {
    let request_identity = http_identity("bad-backend.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-bad-backend"),
            request_identity,
            route_entry(
                InstanceState::Running,
                7,
                Some(("ftp://127.0.0.1:1".to_owned(), 3)),
            ),
        ),
        now(),
    );
    let mut runtime =
        runtime_with_state(state, FakeRouteClient::default(), FakeWakeClient::default());

    let response = runtime
        .handle_http(
            Request::builder()
                .uri("/")
                .header("host", "bad-backend.example.com")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn http01_challenge_hit_calls_resolver_before_route_resolution() {
    let key = Http01ChallengeKey::new("app.example.com", "token-a").expect("HTTP-01 key");
    let mut http01_resolver = FakeHttp01Resolver::default();
    http01_resolver.push_response(Some(challenge_record(key, "token-a.key")));
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(miss_response(
        generated_request_id(1),
        http_identity("app.example.com", "/.well-known/acme-challenge/token-a"),
    ));
    let mut runtime = runtime_with_http01_resolver(
        SubscriptionState::new(4),
        route_client,
        FakeWakeClient::default(),
        http01_resolver,
    );

    let response = runtime
        .handle_http(
            Request::builder()
                .uri("/.well-known/acme-challenge/token-a")
                .header("host", "App.Example.COM:80")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        collect_body(response).await,
        Bytes::from_static(b"token-a.key")
    );
    assert_eq!(
        runtime.http01_resolver().calls,
        vec![("app.example.com".to_owned(), "token-a".to_owned())]
    );
    assert!(runtime.coordinator().resolver().client().calls.is_empty());
    assert!(runtime.coordinator().wake_client().calls.is_empty());
}

#[tokio::test]
async fn http01_challenge_hit_records_result_metric_and_log_event() {
    let key = Http01ChallengeKey::new("app.example.com", "token-a").expect("HTTP-01 key");
    let mut http01_resolver = FakeHttp01Resolver::default();
    http01_resolver.push_response(Some(challenge_record(key, "token-a.key")));
    let sink = InMemoryObservability::default();
    let mut runtime = FrontlineHttpRuntime::with_http01_resolver_and_observability(
        FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(
                SubscriptionState::new(4),
                FakeRouteClient::default(),
            ),
            WakeTracker::new(),
            FakeWakeClient::default(),
        ),
        http01_resolver,
        DrainTracker::new(Duration::from_secs(5)),
        sink.recorder(),
    );

    let response = runtime
        .handle_http(
            Request::builder()
                .uri("/.well-known/acme-challenge/token-a")
                .header("host", "app.example.com")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let events = sink.events();
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_HTTP01_RESULTS_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "success")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Log(log) if log.name() == EVENT_HTTP01
    )));
}

#[tokio::test]
async fn http01_challenge_miss_returns_not_found_without_route_fallback() {
    let mut http01_resolver = FakeHttp01Resolver::default();
    http01_resolver.push_response(None);
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-app"),
        http_identity(
            "app.example.com",
            "/.well-known/acme-challenge/missing-token",
        ),
        route_entry(InstanceState::Running, 7, None),
    ));
    let mut runtime = runtime_with_http01_resolver(
        SubscriptionState::new(4),
        route_client,
        FakeWakeClient::default(),
        http01_resolver,
    );

    let response = runtime
        .handle_http(
            Request::builder()
                .uri("/.well-known/acme-challenge/missing-token")
                .header("host", "app.example.com")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        runtime.http01_resolver().calls,
        vec![("app.example.com".to_owned(), "missing-token".to_owned())]
    );
    assert!(runtime.coordinator().resolver().client().calls.is_empty());
    assert!(runtime.coordinator().wake_client().calls.is_empty());
}

#[tokio::test]
async fn http01_invalid_requests_and_resolver_errors_do_not_fall_through_to_routes() {
    let invalid_host = runtime_with_http01_resolver(
        SubscriptionState::new(4),
        FakeRouteClient::default(),
        FakeWakeClient::default(),
        FakeHttp01Resolver::default(),
    );
    assert_status_for_path(
        invalid_host,
        "localhost",
        "/.well-known/acme-challenge/token-a",
        StatusCode::BAD_REQUEST,
    )
    .await;

    let invalid_token = runtime_with_http01_resolver(
        SubscriptionState::new(4),
        FakeRouteClient::default(),
        FakeWakeClient::default(),
        FakeHttp01Resolver::default(),
    );
    assert_status_for_path(
        invalid_token,
        "app.example.com",
        "/.well-known/acme-challenge/token-a/extra",
        StatusCode::BAD_REQUEST,
    )
    .await;

    let mut http01_resolver = FakeHttp01Resolver::default();
    http01_resolver.push_error(TestHttp01ResolverError);
    let mut runtime = runtime_with_http01_resolver(
        SubscriptionState::new(4),
        FakeRouteClient::default(),
        FakeWakeClient::default(),
        http01_resolver,
    );
    let response = runtime
        .handle_http(
            Request::builder()
                .uri("/.well-known/acme-challenge/token-a")
                .header("host", "app.example.com")
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        runtime.http01_resolver().calls,
        vec![("app.example.com".to_owned(), "token-a".to_owned())]
    );
    assert!(runtime.coordinator().resolver().client().calls.is_empty());
    assert!(runtime.coordinator().wake_client().calls.is_empty());
}

async fn assert_status(
    mut runtime: FrontlineHttpRuntime<FakeRouteClient, FakeWakeClient>,
    host: &str,
    expected: StatusCode,
) {
    let response = runtime
        .handle_http(
            Request::builder()
                .uri("/")
                .header("host", host)
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), expected);
}

async fn assert_status_for_path(
    mut runtime: FrontlineHttpRuntime<FakeRouteClient, FakeWakeClient, FakeHttp01Resolver>,
    host: &str,
    path: &str,
    expected: StatusCode,
) {
    let response = runtime
        .handle_http(
            Request::builder()
                .uri(path)
                .header("host", host)
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
            now(),
        )
        .await;

    assert_eq!(response.status(), expected);
    assert!(runtime.coordinator().resolver().client().calls.is_empty());
    assert!(runtime.coordinator().wake_client().calls.is_empty());
}

fn cached_runtime(
    state: InstanceState,
    host: &str,
) -> FrontlineHttpRuntime<FakeRouteClient, FakeWakeClient> {
    let mut subscription_state = SubscriptionState::new(4);
    subscription_state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id(&format!("sub-{host}")),
            http_identity(host, "/"),
            route_entry(state, 6, None),
        ),
        now(),
    );
    runtime_with_state(
        subscription_state,
        FakeRouteClient::default(),
        FakeWakeClient::default(),
    )
}

fn runtime_with_state(
    state: SubscriptionState,
    route_client: FakeRouteClient,
    wake_client: FakeWakeClient,
) -> FrontlineHttpRuntime<FakeRouteClient, FakeWakeClient> {
    FrontlineHttpRuntime::new(
        FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(state, route_client),
            WakeTracker::new(),
            wake_client,
        ),
        DrainTracker::new(Duration::from_secs(5)),
    )
}

fn runtime_with_http01_resolver(
    state: SubscriptionState,
    route_client: FakeRouteClient,
    wake_client: FakeWakeClient,
    http01_resolver: FakeHttp01Resolver,
) -> FrontlineHttpRuntime<FakeRouteClient, FakeWakeClient, FakeHttp01Resolver> {
    FrontlineHttpRuntime::with_http01_resolver(
        FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(state, route_client),
            WakeTracker::new(),
            wake_client,
        ),
        http01_resolver,
        DrainTracker::new(Duration::from_secs(5)),
    )
}

async fn spawn_http_upstream(
    requests: usize,
    status: StatusCode,
    body: &'static [u8],
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let addr = listener.local_addr().expect("upstream addr");
    let task = tokio::spawn(async move {
        for _ in 0..requests {
            let (stream, _) = listener.accept().await.expect("upstream accepts");
            http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |mut request: Request<Incoming>| async move {
                        let _ = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("request body reads");
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .body(Full::new(Bytes::from_static(body)))
                                .expect("response builds"),
                        )
                    }),
                )
                .await
                .expect("upstream serves");
        }
    });

    (addr, task)
}

async fn collect_body(response: Response<super::FrontlineRuntimeBody>) -> Bytes {
    response
        .into_body()
        .collect()
        .await
        .expect("response body reads")
        .to_bytes()
}

fn now() -> Instant {
    Instant::now()
}

fn ttl(seconds: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_secs(seconds))
}

fn request_id(value: &str) -> RouteRequestId {
    RouteRequestId::new(value).expect("request ID")
}

fn generated_request_id(index: u64) -> RouteRequestId {
    request_id(&format!("req:{index}"))
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription ID")
}

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("instance ID")
}

fn route_binding_id(value: &str) -> RouteBindingId {
    RouteBindingId::new(value).expect("route binding ID")
}

fn backend(uri: impl Into<String>) -> BackendEndpoint {
    BackendEndpoint::new(uri).expect("backend")
}

fn http_identity(host: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("host"),
        path: Some(PathPrefix::new(path).expect("path")),
    }
}

fn route_entry(
    state: InstanceState,
    instance_generation: u64,
    backend_uri_and_generation: Option<(String, u64)>,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: route_binding_id("route-a"),
        instance_id: instance_id("instance-a"),
        instance_state: state,
        instance_generation: Generation::new(instance_generation),
        backend: backend_uri_and_generation
            .as_ref()
            .map(|(uri, _generation)| backend(uri.clone())),
        backend_generation: backend_uri_and_generation
            .map(|(_uri, generation)| BackendGeneration::new(generation)),
    }
}

fn resolved_response(
    request_id: RouteRequestId,
    subscription_id: SubscriptionId,
    matched_identity: RouteIdentity,
    entry: RouteEntry,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteResolved {
        request_id,
        subscription_id,
        matched_identity,
        entry,
        cache_policy: ttl(30),
    }
}

fn miss_response(
    request_id: RouteRequestId,
    identity: RouteIdentity,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteMiss {
        request_id,
        request_identity: identity,
        negative_cache_policy: ttl(30),
    }
}

fn challenge_record(key: Http01ChallengeKey, key_authorization: &str) -> Http01ChallengeRecord {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    Http01ChallengeRecord::new(key, key_authorization, now + Duration::from_secs(60), now)
        .expect("challenge record builds")
}
