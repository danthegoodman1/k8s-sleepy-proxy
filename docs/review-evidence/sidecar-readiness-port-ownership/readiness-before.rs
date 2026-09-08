use super::*;
use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};

async fn unused_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
}
fn config(proxy: SocketAddr, app: SocketAddr, ready: SocketAddr) -> EnvConfig {
    EnvConfig {
        runtime: SidecarRuntimeConfig::new(
            proxy,
            app.port(),
            InstanceId::new("readiness-test").unwrap(),
            Generation::new(1),
            IdleReportConfig::new(Duration::from_secs(300), Duration::from_secs(1)).unwrap(),
            Duration::from_millis(100),
        )
        .unwrap(),
        runtime_mode: SidecarRuntimeMode::Http,
        pod_uid: "fixture-pod".into(),
        control_plane_endpoint: "http://127.0.0.1:1".into(),
        control_plane_sidecar_token: None,
        metrics_listen_addr: None,
        readiness_listen_addr: Some(ready),
    }
}
async fn get(addr: SocketAddr, path: &str) -> std::io::Result<String> {
    let mut socket = TcpStream::connect(addr).await?;
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await?;
    let mut bytes = Vec::new();
    socket.read_to_end(&mut bytes).await?;
    Ok(String::from_utf8(bytes).unwrap())
}

#[tokio::test]
async fn delayed_initial_cp_and_loopback_app_gate_health_before_one_application_request() {
    let proxy = unused_addr().await;
    let app = unused_addr().await;
    let ready = unused_addr().await;
    let shutdown = Shutdown::new();
    let (connected, connection) = tokio::sync::oneshot::channel();
    // Hold exactly the production startup connection dependency; everything after it is real.
    let task = tokio::spawn(run_with_connection(
        config(proxy, app, ready),
        async { Ok(connection.await.unwrap()) },
        shutdown.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(TcpStream::connect(proxy).await.is_err());
    assert!(TcpStream::connect(ready).await.is_err());
    connected
        .send(Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(response) = get(ready, "/ready").await {
                assert!(response.starts_with("HTTP/1.1 503"));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let app_listener = TcpListener::bind(app).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let app_calls = calls.clone();
    let app_shutdown = shutdown.clone();
    let app_task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = app_shutdown.cancelled() => break,
                accepted = app_listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let calls = app_calls.clone();
                    connections.spawn(async move {
                        let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service_fn(move |_| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            async { Ok::<_, std::convert::Infallible>(http::Response::new(Full::new(Bytes::from_static(b"first request succeeds")))) }
                        })).await;
                    });
                }
            }
        }
        connections.shutdown().await;
    });
    assert!(get(ready, "/ready")
        .await
        .unwrap()
        .starts_with("HTTP/1.1 200"));
    // A cold-route coordinator can publish only after this check; no application caller retry.
    let response = tokio::time::timeout(Duration::from_secs(2), get(proxy, "/first"))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 200") && response.ends_with("first request succeeds"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "health sends no HTTP request to app"
    );
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    app_task.await.unwrap();
    assert!(TcpStream::connect(ready).await.is_err());
}

#[tokio::test]
async fn startup_cancellation_never_publishes_health() {
    let proxy = unused_addr().await;
    let app = unused_addr().await;
    let ready = unused_addr().await;
    let shutdown = Shutdown::new();
    let task = tokio::spawn(run_with_connection(
        config(proxy, app, ready),
        std::future::pending(),
        shutdown.clone(),
    ));
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(TcpStream::connect(proxy).await.is_err());
    assert!(TcpStream::connect(ready).await.is_err());
}

#[tokio::test]
async fn stalled_initial_cp_setup_expires_without_publishing_health() {
    let proxy = unused_addr().await;
    let app = unused_addr().await;
    let ready = unused_addr().await;
    let mut env = config(proxy, app, ready);
    env.runtime = env.runtime.with_resource_config(
        proxy_core::ProxyResourceConfig::default()
            .with_timeouts(
                Duration::from_millis(30),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .unwrap(),
    );
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        run_with_connection(env, std::future::pending(), Shutdown::new()),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert!(TcpStream::connect(proxy).await.is_err());
    assert!(TcpStream::connect(ready).await.is_err());
}

#[tokio::test]
async fn occupied_metrics_port_shuts_down_and_joins_both_listeners() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = unused_addr().await;
    let app = unused_addr().await;
    let ready = unused_addr().await;
    let mut env = config(proxy, app, ready);
    env.metrics_listen_addr = Some(occupied.local_addr().unwrap());
    let shutdown = Shutdown::new();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        run_with_connection(
            env,
            async { Ok(Endpoint::from_static("http://127.0.0.1:1").connect_lazy()) },
            shutdown.clone(),
        ),
    )
    .await
    .unwrap();
    let error = result.unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::AddrInUse
    );
    assert!(shutdown.is_shutdown());
    assert!(TcpStream::connect(proxy).await.is_err());
    assert!(TcpStream::connect(ready).await.is_err());
}

#[tokio::test]
async fn sibling_failure_joins_an_in_progress_idle_report_before_returning() {
    struct IdleClient {
        started: Arc<tokio::sync::Notify>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Drop for IdleClient {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    impl sidecar::ReportIdleClient for IdleClient {
        type Error = std::convert::Infallible;
        fn report_idle(
            &mut self,
            _: sidecar::ReportIdleRequest,
        ) -> sidecar::ReportIdleFuture<'_, sidecar::ReportIdleResponse, Self::Error> {
            self.started.notify_one();
            Box::pin(std::future::pending())
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = unused_addr().await;
    let runtime = SidecarRuntimeConfig::new(
        addr,
        app.port(),
        InstanceId::new("idle-supervision").unwrap(),
        Generation::new(1),
        IdleReportConfig::new(Duration::from_millis(20), Duration::from_millis(20)).unwrap(),
        Duration::from_millis(100),
    )
    .unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let client = IdleClient {
        started: started.clone(),
        dropped: dropped.clone(),
    };
    let shutdown = Shutdown::new();
    let first_error = std::sync::Mutex::new(None);
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            finish_listener(&shutdown, &first_error, async {
                serve_tcp_listener_with_idle(listener, runtime, client, shutdown.clone())
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
            }),
            finish_listener(&shutdown, &first_error, async {
                started.notified().await;
                Err("injected sibling failure".into())
            }),
        );
    })
    .await
    .unwrap();
    assert_eq!(
        first_error.into_inner().unwrap().unwrap().to_string(),
        "injected sibling failure"
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "the idle report client is dropped before supervision returns"
    );
    assert!(TcpStream::connect(addr).await.is_err());
}
