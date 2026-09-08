//! Sidecar-local proxy wiring.

pub mod control_plane_transport;
pub mod idle;
pub mod readiness;
pub mod runtime;

use std::{
    error::Error,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    time::Duration,
};

use bytes::Bytes;
pub use control_plane_transport::{
    GrpcSidecarControlPlaneClient, GrpcSidecarControlPlaneError, ReportIdleClient,
    ReportIdleFuture, ReportIdleResponse, ReportIdleUnavailableReason, SidecarProtocolAdapterError,
};
use http::{Request, Response, Uri};
use http_body::Body;
use hyper::body::Incoming;
pub use idle::{
    ControlPlaneIdleReportOutcome, IdleDetector, IdleObservation, IdleReportConfig,
    IdleReportConfigError, IdleReportOutcome, ReportIdleRequest,
};
use proxy_core::{
    DrainError, DrainTracker, HttpProxy, HttpProxyError, Shutdown, TcpProxy, TcpProxyConfig,
    TcpProxyError, TcpProxyStats, TrackedBody, WebSocketProxy, WebSocketProxyError,
};
use tokio::net::TcpStream;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SidecarProxyConfig {
    app_port: NonZeroU16,
    http_upstream_origin: Uri,
    websocket_upstream_url: String,
    tcp_upstream_addr: SocketAddr,
    tcp_connect_timeout: Duration,
    resources: proxy_core::ProxyResourceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SidecarConfigError {
    ZeroAppPort,
}

#[derive(Clone, Debug)]
pub struct SidecarProxy {
    config: SidecarProxyConfig,
    drain: DrainTracker,
    http: HttpProxy,
    websocket: WebSocketProxy,
    tcp: TcpProxy,
}

impl SidecarProxyConfig {
    pub fn new(app_port: u16) -> Result<Self, SidecarConfigError> {
        Self::with_tcp_connect_timeout(app_port, TcpProxyConfig::default().connect_timeout)
    }

    pub fn with_tcp_connect_timeout(
        app_port: u16,
        tcp_connect_timeout: Duration,
    ) -> Result<Self, SidecarConfigError> {
        let app_port = NonZeroU16::new(app_port).ok_or(SidecarConfigError::ZeroAppPort)?;
        let http_upstream_origin = format!("http://127.0.0.1:{app_port}")
            .parse()
            .expect("loopback HTTP origin built from a non-zero u16 is valid");
        let websocket_upstream_url = format!("ws://127.0.0.1:{app_port}");
        let tcp_upstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), app_port.get());

        Ok(Self {
            app_port,
            http_upstream_origin,
            websocket_upstream_url,
            tcp_upstream_addr,
            tcp_connect_timeout,
            resources: proxy_core::ProxyResourceConfig::default(),
        })
    }

    pub fn with_resource_config(mut self, config: proxy_core::ProxyResourceConfig) -> Self {
        self.resources = config;
        self.tcp_connect_timeout = config.setup_timeout();
        self
    }

    pub fn app_port(&self) -> u16 {
        self.app_port.get()
    }

    pub fn http_upstream_origin(&self) -> &Uri {
        &self.http_upstream_origin
    }

    pub fn websocket_upstream_url(&self) -> &str {
        &self.websocket_upstream_url
    }

    pub fn tcp_upstream_addr(&self) -> SocketAddr {
        self.tcp_upstream_addr
    }

    pub fn tcp_connect_timeout(&self) -> Duration {
        self.tcp_connect_timeout
    }
}

impl SidecarProxy {
    pub fn new(config: SidecarProxyConfig, drain: DrainTracker) -> Self {
        let http = HttpProxy::with_config(drain.clone(), config.resources);
        let websocket = WebSocketProxy::with_config(
            drain.clone(),
            proxy_core::WebSocketProxyConfig {
                handshake_timeout: config.resources.setup_timeout(),
                write_timeout: config.resources.write_idle_timeout(),
                ..proxy_core::WebSocketProxyConfig::default()
            },
        );
        let tcp = TcpProxy::new(
            drain.clone(),
            TcpProxyConfig {
                connect_timeout: config.tcp_connect_timeout(),
                write_idle_timeout: config.resources.write_idle_timeout(),
                ..TcpProxyConfig::default()
            },
        );

        Self {
            config,
            drain,
            http,
            websocket,
            tcp,
        }
    }

    pub fn config(&self) -> &SidecarProxyConfig {
        &self.config
    }

    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    pub fn active_count(&self) -> usize {
        self.drain.active_count()
    }

    pub fn is_draining(&self) -> bool {
        self.drain.is_draining()
    }

    pub fn start_drain(&self) {
        self.drain.start_drain();
    }

    pub async fn wait_for_active_count(&self, expected: usize) {
        self.drain.wait_for_active_count(expected).await;
    }

    pub async fn wait_for_idle(&self) -> Result<(), DrainError> {
        self.drain.wait_for_idle().await
    }

    pub async fn drain(&self) -> Result<(), DrainError> {
        self.drain.drain().await
    }

    pub async fn drain_on_shutdown(&self, shutdown: Shutdown) -> Result<(), DrainError> {
        shutdown.cancelled().await;
        self.drain().await
    }

    pub async fn forward_http<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<TrackedBody<Incoming>>, HttpProxyError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        self.http
            .proxy(request, self.config.http_upstream_origin())
            .await
    }

    pub async fn forward_tcp(&self, client: TcpStream) -> Result<TcpProxyStats, TcpProxyError> {
        self.tcp
            .proxy(client, self.config.tcp_upstream_addr())
            .await
    }

    pub async fn prepare_websocket_upgrade<B>(
        &self,
        request: &mut Request<B>,
    ) -> Result<
        (
            Response<http_body_util::Full<Bytes>>,
            proxy_core::WebSocketUpgrade,
        ),
        WebSocketProxyError,
    > {
        let path = request
            .uri()
            .path_and_query()
            .map(|path| path.as_str())
            .unwrap_or("/");
        let url = format!("{}{path}", self.config.websocket_upstream_url());
        self.websocket.prepare_upgrade(request, &url).await
    }
}

impl fmt::Display for SidecarConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroAppPort => write!(f, "sidecar app port must be non-zero"),
        }
    }
}

impl Error for SidecarConfigError {}

#[cfg(test)]
mod tests;
