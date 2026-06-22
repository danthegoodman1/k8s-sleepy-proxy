use std::{convert::Infallible, error::Error, fmt, io, net::SocketAddr, time::Duration};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::{DrainError, DrainTracker, HttpProxyError, Shutdown};
use sleepypods_types::{Generation, InstanceId};
use tokio::{net::TcpListener, task::JoinSet};

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
        })
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
    let drain = DrainTracker::new(config.drain_grace_timeout());
    let proxy = SidecarProxy::new(config.proxy().clone(), drain.clone());
    let mut idle = IdleDetector::new(
        config.instance_id().clone(),
        config.generation(),
        config.idle_report(),
        drain,
    );
    let idle_task =
        tokio::spawn(async move { idle.report_to_control_plane_when_idle(&mut client).await });
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
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
                let proxy = proxy.clone();
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request| {
                        let proxy = proxy.clone();
                        async move { Ok::<_, Infallible>(forward_or_error(proxy, request).await) }
                    });
                    let mut builder = http1::Builder::new();
                    builder.keep_alive(false);
                    let connection = builder.serve_connection(TokioIo::new(stream), service);
                    tokio::pin!(connection);

                    tokio::select! {
                        result = connection.as_mut() => {
                            let _ = result;
                        }
                        _ = connection_shutdown.cancelled() => {
                            connection.as_mut().graceful_shutdown();
                            let _ = connection.await;
                        }
                    }
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
    let drain = DrainTracker::new(config.drain_grace_timeout());
    let proxy = SidecarProxy::new(config.proxy().clone(), drain.clone());
    let mut idle = IdleDetector::new(
        config.instance_id().clone(),
        config.generation(),
        config.idle_report(),
        drain,
    );
    let idle_task =
        tokio::spawn(async move { idle.report_to_control_plane_when_idle(&mut client).await });
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
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
                let proxy = proxy.clone();
                connections.spawn(async move {
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
    request: Request<Incoming>,
) -> Response<RuntimeBody> {
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
        HttpProxyError::Drain(_) => StatusCode::SERVICE_UNAVAILABLE,
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
