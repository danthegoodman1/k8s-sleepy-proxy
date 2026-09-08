use std::{convert::Infallible, error::Error, fmt, io, net::SocketAddr, time::Duration};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::body::Incoming;
use proxy_core::{
    configure_tcp_keepalive,
    observability::{prometheus::RuntimeActiveStreamsCollector, recorder::ObservabilityRecorder},
    DrainError, DrainTracker, HttpProxyError, Shutdown,
};
use sleepypods_types::{Generation, InstanceId};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};

use crate::{
    IdleDetector, IdleReportConfig, ReportIdleClient, SidecarConfigError, SidecarProxy,
    SidecarProxyConfig,
};

type BoxError = Box<dyn Error + Send + Sync>;
type RuntimeBody = UnsyncBoxBody<Bytes, BoxError>;
#[derive(Clone, Debug)]
pub struct SidecarRuntimeConfig {
    listen_addr: SocketAddr,
    proxy: SidecarProxyConfig,
    instance_id: InstanceId,
    generation: Generation,
    idle_report: IdleReportConfig,
    drain_grace_timeout: Duration,
    active_streams_collector: RuntimeActiveStreamsCollector,
    admission: proxy_core::ProxyAdmission,
}

#[derive(Debug)]
pub enum SidecarRuntimeError {
    Config(SidecarConfigError),
    Bind { addr: SocketAddr, source: io::Error },
    Accept(io::Error),
    Drain(DrainError),
}

impl SidecarRuntimeConfig {
    pub fn new(
        listen_addr: SocketAddr,
        app_port: u16,
        instance_id: InstanceId,
        generation: Generation,
        idle_report: IdleReportConfig,
        drain_grace_timeout: Duration,
    ) -> Result<Self, SidecarRuntimeError> {
        Ok(Self {
            listen_addr,
            proxy: SidecarProxyConfig::new(app_port)?,
            instance_id,
            generation,
            idle_report,
            drain_grace_timeout,
            active_streams_collector: RuntimeActiveStreamsCollector::default(),
            admission: proxy_core::ProxyAdmission::new(proxy_core::ProxyResourceConfig::default()),
        })
    }

    pub fn with_resource_config(mut self, config: proxy_core::ProxyResourceConfig) -> Self {
        self.proxy = self.proxy.with_resource_config(config);
        self.admission = proxy_core::ProxyAdmission::new(config);
        self
    }

    pub fn admission(&self) -> &proxy_core::ProxyAdmission {
        &self.admission
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub fn proxy(&self) -> &SidecarProxyConfig {
        &self.proxy
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn idle_report(&self) -> IdleReportConfig {
        self.idle_report
    }

    pub fn drain_grace_timeout(&self) -> Duration {
        self.drain_grace_timeout
    }

    pub fn active_streams_collector(&self) -> RuntimeActiveStreamsCollector {
        self.active_streams_collector.clone()
    }
}

pub async fn serve_http_with_idle<Client>(
    config: SidecarRuntimeConfig,
    client: Client,
    shutdown: Shutdown,
) -> Result<(), SidecarRuntimeError>
where
    Client: ReportIdleClient + Send + 'static,
    Client::Error: Send + 'static,
{
    let listener = TcpListener::bind(config.listen_addr())
        .await
        .map_err(|source| SidecarRuntimeError::Bind {
            addr: config.listen_addr(),
            source,
        })?;

    serve_http_listener_with_idle(listener, config, client, shutdown).await
}

pub async fn serve_tcp_with_idle<Client>(
    config: SidecarRuntimeConfig,
    client: Client,
    shutdown: Shutdown,
) -> Result<(), SidecarRuntimeError>
where
    Client: ReportIdleClient + Send + 'static,
    Client::Error: Send + 'static,
{
    let listener = TcpListener::bind(config.listen_addr())
        .await
        .map_err(|source| SidecarRuntimeError::Bind {
            addr: config.listen_addr(),
            source,
        })?;

    serve_tcp_listener_with_idle(listener, config, client, shutdown).await
}

pub async fn serve_http_listener_with_idle<Client>(
    listener: TcpListener,
    config: SidecarRuntimeConfig,
    mut client: Client,
    shutdown: Shutdown,
) -> Result<(), SidecarRuntimeError>
where
    Client: ReportIdleClient + Send + 'static,
    Client::Error: Send + 'static,
{
    let observability = ObservabilityRecorder::global();
    let drain =
        DrainTracker::with_observability(config.drain_grace_timeout(), observability.clone());
    config.active_streams_collector.attach(drain.clone());
    let proxy = SidecarProxy::new(config.proxy().clone(), drain.clone());
    let mut idle = IdleDetector::with_observability(
        config.instance_id().clone(),
        config.generation(),
        config.idle_report(),
        drain.clone(),
        observability,
    );
    let idle_task =
        tokio::spawn(async move { idle.report_to_control_plane_when_idle(&mut client).await });
    let mut connections = JoinSet::new();
    let upgrades = Arc::new(Mutex::new(JoinSet::new()));
    let mut exit_error = None;

    loop {
        reap_completed_connections(&mut connections);
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(SidecarRuntimeError::Accept(error));
                        break;
                    }
                };
                let _ = stream.set_nodelay(true);
                let _ = configure_tcp_keepalive(&stream, proxy_core::TcpProxyConfig::default().tcp_keepalive);
                let stream = match config.admission.admit_io(stream) { Ok(stream) => stream, Err(_) => continue };
                let handshake = match config.admission.handshakes.try_acquire() { Ok(permit) => permit, Err(_) => continue };
                let initial_work = match drain.try_acquire() { Ok(permit) => permit, Err(_) => continue };
                let admission = config.admission.clone();
                let proxy = proxy.clone();
                let connection_shutdown = shutdown.clone();
                let upgrades = upgrades.clone();
                connections.spawn(async move {
                    serve_http_connection(proxy, stream, connection_shutdown, upgrades, admission, handshake, initial_work).await;
                });
            }
        }
    }

    let drain_result = proxy.drain().await;
    idle_task.abort();
    let _ = idle_task.await;
    {
        let mut upgrades = upgrades.lock().await;
        upgrades.abort_all();
        while upgrades.join_next().await.is_some() {}
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(SidecarRuntimeError::Drain)
}

async fn serve_http_connection(
    proxy: SidecarProxy,
    stream: proxy_core::AdmittedIo<TcpStream>,
    shutdown: Shutdown,
    upgrades: Arc<Mutex<JoinSet<()>>>,
    admission: proxy_core::ProxyAdmission,
    handshake: proxy_core::AdmissionPermit,
    initial_work: proxy_core::DrainPermit,
) {
    proxy_core::serve_http_connection_admitted(
        stream,
        shutdown,
        admission,
        handshake,
        Some(initial_work),
        move |request| {
            let proxy = proxy.clone();
            let upgrades = upgrades.clone();
            async move { Ok::<_, Infallible>(forward_or_error(proxy, request, upgrades).await) }
        },
    )
    .await;
}

fn reap_completed_connections(connections: &mut JoinSet<()>) {
    while connections.try_join_next().is_some() {}
}

pub async fn serve_tcp_listener_with_idle<Client>(
    listener: TcpListener,
    config: SidecarRuntimeConfig,
    mut client: Client,
    shutdown: Shutdown,
) -> Result<(), SidecarRuntimeError>
where
    Client: ReportIdleClient + Send + 'static,
    Client::Error: Send + 'static,
{
    let observability = ObservabilityRecorder::global();
    let drain =
        DrainTracker::with_observability(config.drain_grace_timeout(), observability.clone());
    config.active_streams_collector.attach(drain.clone());
    let proxy = SidecarProxy::new(config.proxy().clone(), drain.clone());
    let mut idle = IdleDetector::with_observability(
        config.instance_id().clone(),
        config.generation(),
        config.idle_report(),
        drain,
        observability,
    );
    let idle_task =
        tokio::spawn(async move { idle.report_to_control_plane_when_idle(&mut client).await });
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
        reap_completed_connections(&mut connections);
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(SidecarRuntimeError::Accept(error));
                        break;
                    }
                };
                let _ = stream.set_nodelay(true);
                let _ = configure_tcp_keepalive(
                    &stream,
                    proxy_core::TcpProxyConfig::default().tcp_keepalive,
                );
                let connection = match config.admission.connections.try_acquire() { Ok(permit) => permit, Err(_) => continue };
                let request = match config.admission.requests.try_acquire() { Ok(permit) => permit, Err(_) => continue };
                let proxy = proxy.clone();
                connections.spawn(async move {
                    let (_connection, _request) = (connection, request);
                    let _ = proxy.forward_tcp(stream).await;
                });
            }
        }
    }

    let drain_result = proxy.drain().await;
    idle_task.abort();
    let _ = idle_task.await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(SidecarRuntimeError::Drain)
}

async fn forward_or_error(
    proxy: SidecarProxy,
    mut request: Request<Incoming>,
    upgrades: Arc<Mutex<JoinSet<()>>>,
) -> Response<RuntimeBody> {
    if proxy_core::is_websocket_upgrade(&request) {
        return match proxy.prepare_websocket_upgrade(&mut request).await {
            Ok((response, upgrade)) => {
                let mut tasks = upgrades.lock().await;
                reap_completed_connections(&mut tasks);
                tasks.spawn(async move {
                    let _ = upgrade.run().await;
                });
                response.map(box_runtime_body)
            }
            Err(error) => proxy_core::websocket_error_response(&error).map(box_runtime_body),
        };
    }
    match proxy.forward_http(request).await {
        Ok(response) => response.map(box_runtime_body),
        Err(error) => proxy_error_response(error),
    }
}

fn box_runtime_body<B>(body: B) -> RuntimeBody
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(Into::into).boxed_unsync()
}

fn proxy_error_response(error: HttpProxyError) -> Response<RuntimeBody> {
    let status = match error {
        HttpProxyError::Drain(_) | HttpProxyError::UpstreamSaturated => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        HttpProxyError::UpstreamHeaderTimeout => StatusCode::GATEWAY_TIMEOUT,
        HttpProxyError::RequestRewrite(_) | HttpProxyError::Client(_) => StatusCode::BAD_GATEWAY,
    };

    Response::builder()
        .status(status)
        .body(
            Full::new(Bytes::new())
                .map_err(|error| match error {})
                .boxed_unsync(),
        )
        .expect("status-only runtime error response builds")
}

impl From<SidecarConfigError> for SidecarRuntimeError {
    fn from(error: SidecarConfigError) -> Self {
        Self::Config(error)
    }
}

impl fmt::Display for SidecarRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{error}"),
            Self::Bind { addr, source } => {
                write!(f, "failed to bind sidecar listener on {addr}: {source}")
            }
            Self::Accept(error) => write!(f, "failed to accept sidecar connection: {error}"),
            Self::Drain(error) => write!(f, "sidecar drain failed: {error}"),
        }
    }
}

impl Error for SidecarRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Bind { source, .. } => Some(source),
            Self::Accept(error) => Some(error),
            Self::Drain(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;
