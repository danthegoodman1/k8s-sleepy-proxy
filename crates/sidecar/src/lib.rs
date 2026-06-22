//! Sidecar-local proxy wiring.

pub mod idle;

use std::{
    error::Error,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, Uri};
use http_body::Body;
use hyper::body::Incoming;
pub use idle::{
    IdleDetector, IdleObservation, IdleReportConfig, IdleReportConfigError, IdleReportOutcome,
    ReportIdleRequest,
};
use proxy_core::{
    DrainError, DrainTracker, HttpProxy, HttpProxyError, Shutdown, TcpProxy, TcpProxyConfig,
    TcpProxyError, TcpProxyStats, TrackedBody,
};
use tokio::net::TcpStream;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SidecarProxyConfig {
    app_port: NonZeroU16,
    http_upstream_origin: Uri,
    tcp_upstream_addr: SocketAddr,
    tcp_connect_timeout: Duration,
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
        let tcp_upstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), app_port.get());

        Ok(Self {
            app_port,
            http_upstream_origin,
            tcp_upstream_addr,
            tcp_connect_timeout,
        })
    }

    pub fn app_port(&self) -> u16 {
        self.app_port.get()
    }

    pub fn http_upstream_origin(&self) -> &Uri {
        &self.http_upstream_origin
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
        let http = HttpProxy::new(drain.clone());
        let tcp = TcpProxy::new(
            drain.clone(),
            TcpProxyConfig {
                connect_timeout: config.tcp_connect_timeout(),
            },
        );

        Self {
            config,
            drain,
            http,
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
