use std::{
    convert::Infallible,
    error::Error,
    fmt,
    future::pending,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::{DrainError, Shutdown};
use sleepypods_types::{Generation, InstanceId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

use crate::{
    runtime::{serve_http_listener_with_idle, SidecarRuntimeConfig, SidecarRuntimeError},
    IdleReportConfig, ReportIdleClient, ReportIdleFuture, ReportIdleRequest, ReportIdleResponse,
};

const IDLE_TIMEOUT: Duration = Duration::from_millis(50);
const RETRY_BACKOFF: Duration = Duration::from_millis(10);
const DRAIN_GRACE_TIMEOUT: Duration = Duration::from_secs(5);

type RecordedRequests = Arc<Mutex<Vec<ReportIdleRequest>>>;

#[derive(Debug, Clone)]
struct FakeReportIdleClient {
    requests: RecordedRequests,
}

#[derive(Debug, Clone, Copy)]
struct FakeReportIdleError;

#[tokio::test]
async fn http_request_reaches_upstream_and_activity_delays_idle_report() {
    let (release_upstream, upstream_released) = oneshot::channel();
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) = spawn_blocked_upstream(
        upstream_released,
        upstream_received,
        "/workload?from=runtime",
        Bytes::from_static(b"runtime-ok"),
    )
    .await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let requests = client.requests();
    let (runtime_addr, runtime_task) =
        spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to runtime");
    write_http_request(&mut stream, "/workload?from=runtime").await;
    upstream_received_rx
        .await
        .expect("upstream receives request");

    tokio::time::sleep(IDLE_TIMEOUT + RETRY_BACKOFF).await;
    assert_recorded_count(&requests, 0);

    release_upstream.send(()).expect("upstream release sent");
    let response = read_http_response(&mut stream, b"runtime-ok").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.ends_with("runtime-ok"), "{response}");

    tokio::time::sleep(IDLE_TIMEOUT).await;
    wait_for_recorded_count(&requests, 1).await;

    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn idle_report_fires_without_http_activity() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("unused upstream listener binds");
    let upstream_addr = upstream_listener.local_addr().expect("upstream has addr");
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let requests = client.requests();
    let (_runtime_addr, runtime_task) =
        spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    wait_for_recorded_count(&requests, 1).await;

    shutdown.shutdown();
    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
}

#[tokio::test]
async fn shutdown_stops_accepting_and_waits_for_active_request_to_finish() {
    let (release_upstream, upstream_released) = oneshot::channel();
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) = spawn_blocked_upstream(
        upstream_released,
        upstream_received,
        "/during-shutdown",
        Bytes::from_static(b"drained"),
    )
    .await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let (runtime_addr, runtime_task) =
        spawn_runtime(upstream_addr.port(), client, shutdown.clone()).await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to runtime");
    write_http_request(&mut stream, "/during-shutdown").await;
    upstream_received_rx
        .await
        .expect("upstream receives request");

    shutdown.shutdown();
    tokio::task::yield_now().await;
    assert!(!runtime_task.is_finished());

    release_upstream.send(()).expect("upstream release sent");
    let response = read_http_response(&mut stream, b"drained").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.ends_with("drained"), "{response}");

    runtime_task
        .await
        .expect("runtime task joins")
        .expect("runtime exits");
    upstream_task.await.expect("upstream task joins");
}

#[tokio::test]
async fn shutdown_returns_after_drain_timeout_when_request_hangs() {
    let (upstream_received, upstream_received_rx) = oneshot::channel();
    let (upstream_addr, upstream_task) =
        spawn_hanging_upstream(upstream_received, "/hangs-past-drain").await;
    let shutdown = Shutdown::new();
    let client = FakeReportIdleClient::new();
    let (runtime_addr, runtime_task) = spawn_runtime_with_drain_timeout(
        upstream_addr.port(),
        client,
        shutdown.clone(),
        Duration::from_millis(50),
    )
    .await;

    let mut stream = TcpStream::connect(runtime_addr)
        .await
        .expect("client connects to runtime");
    write_http_request(&mut stream, "/hangs-past-drain").await;
    upstream_received_rx
        .await
        .expect("upstream receives request");

    shutdown.shutdown();

    let runtime_result = tokio::time::timeout(Duration::from_secs(1), runtime_task)
        .await
        .expect("runtime returns after drain timeout")
        .expect("runtime task joins");

    assert!(matches!(
        runtime_result,
        Err(SidecarRuntimeError::Drain(DrainError::GraceTimeout {
            active: 1,
            ..
        }))
    ));

    upstream_task.abort();
    let _ = upstream_task.await;
}

impl FakeReportIdleClient {
    fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> RecordedRequests {
        Arc::clone(&self.requests)
    }
}

impl ReportIdleClient for FakeReportIdleClient {
    type Error = FakeReportIdleError;

    fn report_idle(
        &mut self,
        request: ReportIdleRequest,
    ) -> ReportIdleFuture<'_, ReportIdleResponse, Self::Error> {
        let requests = self.requests();
        Box::pin(async move {
            requests
                .lock()
                .expect("recorded requests lock is not poisoned")
                .push(request.clone());

            Ok(ReportIdleResponse::Accepted {
                instance_id: request.instance_id().clone(),
                generation: request.generation(),
            })
        })
    }
}

impl fmt::Display for FakeReportIdleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fake report-idle error")
    }
}

impl Error for FakeReportIdleError {}

async fn spawn_runtime(
    app_port: u16,
    client: FakeReportIdleClient,
    shutdown: Shutdown,
) -> (
    SocketAddr,
    JoinHandle<Result<(), crate::runtime::SidecarRuntimeError>>,
) {
    spawn_runtime_with_drain_timeout(app_port, client, shutdown, DRAIN_GRACE_TIMEOUT).await
}

async fn spawn_runtime_with_drain_timeout(
    app_port: u16,
    client: FakeReportIdleClient,
    shutdown: Shutdown,
    drain_grace_timeout: Duration,
) -> (
    SocketAddr,
    JoinHandle<Result<(), crate::runtime::SidecarRuntimeError>>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("runtime listener binds");
    let runtime_addr = listener.local_addr().expect("runtime listener has addr");
    let config = SidecarRuntimeConfig::new(
        runtime_addr,
        app_port,
        InstanceId::new("test-instance").expect("instance id is valid"),
        Generation::new(7),
        IdleReportConfig::new(IDLE_TIMEOUT, RETRY_BACKOFF).expect("idle config is valid"),
        drain_grace_timeout,
    )
    .expect("runtime config is valid");

    let task = tokio::spawn(serve_http_listener_with_idle(
        listener, config, client, shutdown,
    ));
    tokio::task::yield_now().await;

    (runtime_addr, task)
}

async fn spawn_blocked_upstream(
    release: oneshot::Receiver<()>,
    received: oneshot::Sender<()>,
    expected_path: &'static str,
    body: Bytes,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let addr = listener.local_addr().expect("upstream listener has addr");
    let release = Arc::new(Mutex::new(Some(release)));

    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("upstream accepts runtime");
        let release = Arc::clone(&release);
        let received = Arc::new(Mutex::new(Some(received)));

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request: Request<Incoming>| {
                    let release = Arc::clone(&release);
                    let received = Arc::clone(&received);
                    let body = body.clone();
                    async move {
                        assert_eq!(
                            request
                                .uri()
                                .path_and_query()
                                .expect("path and query present")
                                .as_str(),
                            expected_path
                        );
                        if let Some(received) = received
                            .lock()
                            .expect("received lock is not poisoned")
                            .take()
                        {
                            received.send(()).expect("test waits for upstream request");
                        }
                        let release = release
                            .lock()
                            .expect("release lock is not poisoned")
                            .take()
                            .expect("upstream handles one request");
                        release.await.expect("upstream release received");

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(body))
                                .expect("upstream response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("upstream serves request");
    });

    (addr, task)
}

async fn spawn_hanging_upstream(
    received: oneshot::Sender<()>,
    expected_path: &'static str,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let addr = listener.local_addr().expect("upstream listener has addr");
    let received = Arc::new(Mutex::new(Some(received)));

    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("upstream accepts runtime");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request: Request<Incoming>| {
                    let received = Arc::clone(&received);
                    async move {
                        assert_eq!(
                            request
                                .uri()
                                .path_and_query()
                                .expect("path and query present")
                                .as_str(),
                            expected_path
                        );
                        if let Some(received) = received
                            .lock()
                            .expect("received lock is not poisoned")
                            .take()
                        {
                            received.send(()).expect("test waits for upstream request");
                        }
                        pending::<Result<Response<Full<Bytes>>, Infallible>>().await
                    }
                }),
            )
            .await
            .expect("upstream serves request");
    });

    (addr, task)
}

async fn write_http_request(stream: &mut TcpStream, path: &str) {
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
        .await
        .expect("client writes request");
}

async fn read_http_response(stream: &mut TcpStream, expected_body: &[u8]) -> String {
    let mut response = Vec::new();
    let mut chunk = [0; 1024];

    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .expect("client reads response");
        assert_ne!(read, 0, "connection closed before response body arrived");
        response.extend_from_slice(&chunk[..read]);
        if response.ends_with(expected_body) {
            break;
        }
    }

    String::from_utf8(response).expect("response is utf8")
}

async fn wait_for_recorded_count(requests: &RecordedRequests, expected: usize) {
    tokio::time::timeout(
        IDLE_TIMEOUT + RETRY_BACKOFF + Duration::from_secs(1),
        async {
            loop {
                if recorded_count(requests) == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        },
    )
    .await
    .expect("recorded request count reached before timeout");

    assert_recorded_count(requests, expected);
}

fn assert_recorded_count(requests: &RecordedRequests, expected: usize) {
    assert_eq!(recorded_count(requests), expected);
}

fn recorded_count(requests: &RecordedRequests) -> usize {
    requests
        .lock()
        .expect("recorded requests lock is not poisoned")
        .len()
}
