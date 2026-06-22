use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use control_plane::{
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState,
    PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
};
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use proxy_core::{DrainTracker, Shutdown};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

use super::serve_http_listener;
use crate::{
    FrontlineHttpRuntime, FrontlineRouteCoordinator, FrontlineRouteResolver, RouteRequestId,
    RouteSubscriptionClient, RouteSubscriptionFuture, SubscribeControlPlaneOutput, SubscriptionId,
    SubscriptionState, WakeClient, WakeClientFuture, WakeInstanceRequest, WakeInstanceResponse,
    WakeTracker,
};

const READY_RESPONSE: &[u8] = b"ready-from-listener-upstream";

#[derive(Clone, Debug, Default)]
struct FakeRouteClient {
    calls: Arc<Mutex<Vec<RouteClientCall>>>,
    subscribe_responses: Arc<Mutex<VecDeque<Result<SubscribeControlPlaneOutput, TestRouteError>>>>,
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestRouteError;

#[derive(Clone, Debug, Default)]
struct FakeWakeClient {
    calls: Arc<Mutex<Vec<WakeInstanceRequest>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestWakeError;

impl FakeRouteClient {
    fn push_subscribe_response(&self, response: SubscribeControlPlaneOutput) {
        self.subscribe_responses
            .lock()
            .expect("responses lock")
            .push_back(Ok(response));
    }

    fn calls(&self) -> Vec<RouteClientCall> {
        self.calls.lock().expect("calls lock").clone()
    }
}

impl RouteSubscriptionClient for FakeRouteClient {
    type Error = TestRouteError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'_, SubscribeControlPlaneOutput, Self::Error> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(RouteClientCall::Subscribe {
                request_id,
                identity,
            });
        let response = self
            .subscribe_responses
            .lock()
            .expect("responses lock")
            .pop_front()
            .expect("queued subscribe response");
        Box::pin(async move { response })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'_, (), Self::Error> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(RouteClientCall::Unsubscribe { subscription_id });
        Box::pin(async { Ok(()) })
    }
}

impl WakeClient for FakeWakeClient {
    type Error = TestWakeError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'_, WakeInstanceResponse, Self::Error> {
        self.calls.lock().expect("calls lock").push(request);
        Box::pin(async { panic!("wake should not be called in listener tests") })
    }
}

impl fmt::Display for TestRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("route error")
    }
}

impl std::error::Error for TestRouteError {}

impl fmt::Display for TestWakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("wake error")
    }
}

impl std::error::Error for TestWakeError {}

#[tokio::test]
async fn listener_forwards_http_request_through_ready_route() {
    let (upstream_addr, upstream_task) =
        spawn_http_upstream(1, StatusCode::ACCEPTED, READY_RESPONSE).await;
    let route_client = FakeRouteClient::default();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-ready"),
            http_identity("app.example.com", "/ready"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let (addr, shutdown, task) = spawn_frontline_listener(state, route_client.clone()).await;

    let response = listener_request(addr, "app.example.com", "/ready?via=listener").await;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .expect("response body reads")
            .to_bytes(),
        Bytes::from_static(READY_RESPONSE)
    );
    assert!(route_client.calls().is_empty());

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn listener_route_miss_returns_not_found() {
    let route_client = FakeRouteClient::default();
    let request_identity = http_identity("missing.example.com", "/");
    route_client.push_subscribe_response(miss_response(
        generated_request_id(1),
        request_identity.clone(),
    ));
    let (addr, shutdown, task) =
        spawn_frontline_listener(SubscriptionState::new(4), route_client.clone()).await;

    let response = listener_request(addr, "missing.example.com", "/").await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request_identity,
        }]
    );

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_invalid_or_missing_host_returns_bad_request_without_control_plane_call() {
    let route_client = FakeRouteClient::default();
    let (addr, shutdown, task) =
        spawn_frontline_listener(SubscriptionState::new(4), route_client.clone()).await;

    let invalid = listener_request(addr, "localhost", "/").await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let mut raw = TcpStream::connect(addr).await.expect("raw client connects");
    raw.write_all(b"GET / HTTP/1.1\r\n\r\n")
        .await
        .expect("raw request writes");
    let status = read_status_line(&mut raw).await;
    assert!(status.starts_with("HTTP/1.1 400"), "{status}");

    assert!(route_client.calls().is_empty());

    shutdown.shutdown();
    task.await
        .expect("listener task joins")
        .expect("listener exits");
}

#[tokio::test]
async fn listener_shutdown_drains_active_http_response() {
    let (upstream_addr, release_upstream, upstream_task) = spawn_chunked_upstream().await;
    let route_client = FakeRouteClient::default();
    let drain = DrainTracker::new(Duration::from_secs(5));
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-stream"),
            http_identity("stream.example.com", "/stream"),
            route_entry(
                InstanceState::Running,
                7,
                Some((format!("http://{upstream_addr}"), 3)),
            ),
        ),
        now(),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown = Shutdown::new();
    let runtime = runtime_with_state(state, route_client, drain.clone());
    let mut task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));

    let mut client = TcpStream::connect(addr).await.expect("raw client connects");
    client
        .write_all(b"GET /stream HTTP/1.1\r\nHost: stream.example.com\r\n\r\n")
        .await
        .expect("raw request writes");
    read_until(&mut client, b"hello").await;
    drain.wait_for_active_count(1).await;

    shutdown.shutdown();
    tokio::select! {
        result = &mut task => panic!("listener exited before response drained: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }

    release_upstream.send(()).expect("release upstream");
    read_until(&mut client, b"0\r\n\r\n").await;

    task.await
        .expect("listener task joins")
        .expect("listener exits");
    upstream_task.await.expect("upstream task joins");
}

async fn spawn_frontline_listener(
    state: SubscriptionState,
    route_client: FakeRouteClient,
) -> (
    SocketAddr,
    Shutdown,
    JoinHandle<Result<(), super::FrontlineHttpListenerError>>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown = Shutdown::new();
    let drain = DrainTracker::new(Duration::from_secs(5));
    let runtime = runtime_with_state(state, route_client, drain.clone());
    let task = tokio::spawn(serve_http_listener(listener, runtime, shutdown.clone()));

    (addr, shutdown, task)
}

fn runtime_with_state(
    state: SubscriptionState,
    route_client: FakeRouteClient,
    drain: DrainTracker,
) -> FrontlineHttpRuntime<FakeRouteClient, FakeWakeClient> {
    FrontlineHttpRuntime::new(
        FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(state, route_client),
            WakeTracker::new(),
            FakeWakeClient::default(),
        ),
        drain,
    )
}

async fn listener_request(addr: SocketAddr, host: &str, path: &str) -> Response<Incoming> {
    let client = Client::builder(TokioExecutor::new()).build_http();
    client
        .request(
            Request::builder()
                .uri(format!("http://{addr}{path}"))
                .header("host", host)
                .body(Full::new(Bytes::new()))
                .expect("request builds"),
        )
        .await
        .expect("listener request succeeds")
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

async fn spawn_chunked_upstream() -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream binds");
    let addr = listener.local_addr().expect("upstream addr");
    let (release_tx, release_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("upstream accepts");
        read_until(&mut stream, b"\r\n\r\n").await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n")
            .await
            .expect("first response chunk writes");
        release_rx.await.expect("release signal");
        stream
            .write_all(b"0\r\n\r\n")
            .await
            .expect("final response chunk writes");
    });

    (addr, release_tx, task)
}

async fn read_status_line(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut one = [0; 1];
    loop {
        let read = stream.read(&mut one).await.expect("status reads");
        assert_ne!(read, 0, "connection closed before status line");
        bytes.push(one[0]);
        if bytes.ends_with(b"\r\n") {
            break;
        }
    }

    String::from_utf8(bytes).expect("status is utf8")
}

async fn read_until(stream: &mut TcpStream, needle: &[u8]) {
    let mut bytes = Vec::new();
    let mut buffer = [0; 256];
    while !bytes.windows(needle.len()).any(|window| window == needle) {
        let read = stream.read(&mut buffer).await.expect("stream reads");
        assert_ne!(read, 0, "connection closed before needle");
        bytes.extend_from_slice(&buffer[..read]);
    }
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
