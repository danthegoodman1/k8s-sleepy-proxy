use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    net::SocketAddr,
    time::{Duration, Instant},
};

use bytes::Bytes;
use control_plane::{
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState,
    PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::DrainTracker;
use tokio::{net::TcpListener, task::JoinHandle};

use super::FrontlineHttpRuntime;
use crate::{
    FrontlineRouteCoordinator, FrontlineRouteResolver, RouteRequestId, RouteSubscriptionClient,
    RouteSubscriptionFuture, SubscribeControlPlaneOutput, SubscriptionId, SubscriptionState,
    WakeClient, WakeClientFuture, WakeInstanceRequest, WakeInstanceResponse, WakeTracker,
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
    let waiting = cached_runtime(InstanceState::Waking, "waiting.example.com");
    assert_status(
        waiting,
        "waiting.example.com",
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

    let unavailable = cached_runtime(InstanceState::Deleted, "deleted.example.com");
    assert_status(
        unavailable,
        "deleted.example.com",
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;

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
