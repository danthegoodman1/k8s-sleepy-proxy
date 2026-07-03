#![cfg(unix)]

use std::{
    convert::Infallible,
    future::pending,
    net::SocketAddr,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use control_plane::api::pb::{
    self,
    sidecar_control_plane_server::{SidecarControlPlane, SidecarControlPlaneServer},
};
use http::{Request as HttpRequest, Response as HttpResponse};
use http_body_util::Full;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tonic::{transport::Server, Request, Response, Status};

const SIGTERM: i32 = 15;
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

#[tokio::test]
async fn sidecar_process_can_restart_on_same_port_after_sigterm() {
    let control_plane = spawn_control_plane().await;
    let sidecar_addr = unused_loopback_addr().await;
    let app_addr = unused_loopback_addr().await;

    let mut first = spawn_sidecar(
        sidecar_addr,
        app_addr.port(),
        control_plane.addr,
        Duration::from_secs(5),
    );
    wait_for_listen(sidecar_addr).await;
    send_sigterm(&mut first);
    let first_status = wait_for_exit(&mut first, TEST_TIMEOUT).await;
    assert!(first_status.success(), "{first_status}");

    let mut second = spawn_sidecar(
        sidecar_addr,
        app_addr.port(),
        control_plane.addr,
        Duration::from_secs(5),
    );
    wait_for_listen(sidecar_addr).await;
    send_sigterm(&mut second);
    let second_status = wait_for_exit(&mut second, TEST_TIMEOUT).await;
    assert!(second_status.success(), "{second_status}");

    control_plane.shutdown().await;
}

#[tokio::test]
async fn sigterm_exits_after_drain_deadline_when_active_request_hangs() {
    let control_plane = spawn_control_plane().await;
    let (backend_addr, backend_received, backend_task) = spawn_hanging_backend().await;
    let sidecar_addr = unused_loopback_addr().await;
    let drain_timeout = Duration::from_millis(50);

    let mut sidecar = spawn_sidecar(
        sidecar_addr,
        backend_addr.port(),
        control_plane.addr,
        drain_timeout,
    );
    wait_for_listen(sidecar_addr).await;

    let mut client = TcpStream::connect(sidecar_addr)
        .await
        .expect("client connects to sidecar");
    client
        .write_all(b"GET /hang HTTP/1.1\r\nhost: localhost\r\n\r\n")
        .await
        .expect("client writes hanging request");
    expect_within(backend_received, "backend request receipt")
        .await
        .expect("backend receives proxied request");

    let started = Instant::now();
    send_sigterm(&mut sidecar);
    let status = wait_for_exit(&mut sidecar, TEST_TIMEOUT).await;
    assert!(
        started.elapsed() >= drain_timeout,
        "sidecar exited before drain deadline elapsed"
    );
    assert!(
        !status.success(),
        "drain timeout is reported as process failure"
    );

    drop(client);
    backend_task.abort();
    let _ = backend_task.await;
    control_plane.shutdown().await;
}

fn spawn_sidecar(
    listen_addr: SocketAddr,
    app_port: u16,
    control_plane_addr: SocketAddr,
    drain_grace_timeout: Duration,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_sidecar"))
        .env("SLEEPYPODS_SIDECAR_LISTEN_ADDR", listen_addr.to_string())
        .env("SLEEPYPODS_APP_PORT", app_port.to_string())
        .env("SLEEPYPODS_INSTANCE_ID", "process-lifecycle-test")
        .env("SLEEPYPODS_INSTANCE_GENERATION", "7")
        .env(
            "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
            format!("http://{control_plane_addr}"),
        )
        .env("SLEEPYPODS_IDLE_TIMEOUT_MS", "60000")
        .env("SLEEPYPODS_IDLE_RETRY_BACKOFF_MS", "10")
        .env(
            "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS",
            drain_grace_timeout.as_millis().to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("sidecar process starts")
}

fn send_sigterm(child: &mut Child) {
    let result = unsafe { kill(child.id() as i32, SIGTERM) };
    assert_eq!(
        result,
        0,
        "SIGTERM delivery failed: {}",
        std::io::Error::last_os_error()
    );
}

async fn wait_for_exit(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;

    loop {
        if let Some(status) = child.try_wait().expect("child status is readable") {
            return status;
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("sidecar process did not exit within {timeout:?}");
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_listen(addr: SocketAddr) {
    let deadline = Instant::now() + TEST_TIMEOUT;

    loop {
        match TcpStream::connect(addr).await {
            Ok(_stream) => return,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("{addr} did not become ready: {error}"),
        }
    }
}

async fn unused_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("unused loopback port binds");
    listener.local_addr().expect("listener has local addr")
}

struct ControlPlaneServer {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl ControlPlaneServer {
    async fn shutdown(self) {
        let _ = self.shutdown.send(());
        self.task
            .await
            .expect("control-plane task joins")
            .expect("control-plane server exits cleanly");
    }
}

async fn spawn_control_plane() -> ControlPlaneServer {
    let addr = unused_loopback_addr().await;
    let (shutdown, shutdown_rx) = oneshot::channel();

    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(SidecarControlPlaneServer::new(FakeSidecarControlPlane))
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    wait_for_listen(addr).await;

    ControlPlaneServer {
        addr,
        shutdown,
        task,
    }
}

#[derive(Clone, Debug, Default)]
struct FakeSidecarControlPlane;

#[tonic::async_trait]
impl SidecarControlPlane for FakeSidecarControlPlane {
    async fn report_idle(
        &self,
        request: Request<pb::SidecarReportIdleRequest>,
    ) -> Result<Response<pb::SidecarReportIdleResponse>, Status> {
        let request = request.into_inner();

        Ok(Response::new(pb::SidecarReportIdleResponse {
            outcome: Some(pb::sidecar_report_idle_response::Outcome::Accepted(
                pb::SidecarReportIdleAccepted {
                    instance_id: request.instance_id,
                    instance_generation: request.expected_generation,
                },
            )),
        }))
    }
}

async fn spawn_hanging_backend() -> (SocketAddr, oneshot::Receiver<()>, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("backend listener binds");
    let addr = listener.local_addr().expect("backend listener has addr");
    let (received_tx, received_rx) = oneshot::channel();
    let received_tx = Arc::new(Mutex::new(Some(received_tx)));

    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("backend accepts sidecar");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request: HttpRequest<Incoming>| {
                    let received_tx = Arc::clone(&received_tx);

                    async move {
                        assert_eq!(request.uri().path(), "/hang");
                        let received_tx = received_tx
                            .lock()
                            .expect("received lock is not poisoned")
                            .take();
                        if let Some(received_tx) = received_tx {
                            received_tx
                                .send(())
                                .expect("test waits for backend request");
                        }
                        pending::<Result<HttpResponse<Full<Bytes>>, Infallible>>().await
                    }
                }),
            )
            .await
            .expect("backend serves hanging request");
    });

    (addr, received_rx, task)
}

async fn expect_within<F>(future: F, label: &'static str) -> F::Output
where
    F: std::future::Future,
{
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("{label} timed out"))
}
