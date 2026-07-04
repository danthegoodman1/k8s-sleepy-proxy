use std::{
    convert::Infallible,
    error::Error,
    fmt, io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::{
    body::Incoming,
    server::conn::{http1, http2},
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use proxy_core::{
    configure_tcp_keepalive,
    observability::{prometheus::RuntimeActiveStreamsCollector, recorder::ObservabilityRecorder},
    DrainError, DrainTracker, HttpProxyError, Shutdown,
};
use sleepypods_types::{Generation, InstanceId};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};

use crate::{
    IdleDetector, IdleReportConfig, ReportIdleClient, SidecarConfigError, SidecarProxy,
    SidecarProxyConfig,
};

type BoxError = Box<dyn Error + Send + Sync>;
type RuntimeBody = UnsyncBoxBody<Bytes, BoxError>;
const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const MAX_HTTP1_HEADER_BYTES: usize = 64 * 1024;
const PROTOCOL_SNIFF_CHUNK_BYTES: usize = 4096;
const HTTP1_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct SidecarRuntimeConfig {
    listen_addr: SocketAddr,
    proxy: SidecarProxyConfig,
    instance_id: InstanceId,
    generation: Generation,
    idle_report: IdleReportConfig,
    drain_grace_timeout: Duration,
    active_streams_collector: RuntimeActiveStreamsCollector,
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
                let proxy = proxy.clone();
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    serve_http_connection(proxy, stream, connection_shutdown).await;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcceptedProtocol {
    Http1,
    Http2,
    WebSocket,
}

async fn serve_http_connection(proxy: SidecarProxy, stream: TcpStream, shutdown: Shutdown) {
    let _ = configure_tcp_keepalive(&stream, proxy_core::TcpProxyConfig::default().tcp_keepalive);
    let Some(accepted) = detect_protocol(stream, &shutdown).await else {
        return;
    };

    match accepted.protocol {
        AcceptedProtocol::WebSocket => {
            let _ = proxy.forward_websocket(accepted.into_stream()).await;
        }
        AcceptedProtocol::Http2 => {
            serve_http2_connection(proxy, accepted.into_stream(), shutdown).await;
        }
        AcceptedProtocol::Http1 => {
            serve_http1_connection(proxy, accepted.into_stream(), shutdown).await
        }
    }
}

async fn serve_http1_connection(
    proxy: SidecarProxy,
    stream: PrefixedTcpStream,
    shutdown: Shutdown,
) {
    let service = service_fn(move |request| {
        let proxy = proxy.clone();
        async move { Ok::<_, Infallible>(forward_or_error(proxy, request).await) }
    });
    let builder = http1::Builder::new();
    let connection = builder.serve_connection(TokioIo::new(stream), service);
    tokio::pin!(connection);

    tokio::select! {
        result = connection.as_mut() => {
            let _ = result;
        }
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            let _ = connection.await;
        }
    }
}

async fn serve_http2_connection(
    proxy: SidecarProxy,
    stream: PrefixedTcpStream,
    shutdown: Shutdown,
) {
    let service = service_fn(move |request| {
        let proxy = proxy.clone();
        async move { Ok::<_, Infallible>(forward_or_error(proxy, request).await) }
    });
    let connection =
        http2::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(stream), service);
    tokio::pin!(connection);

    tokio::select! {
        result = connection.as_mut() => {
            let _ = result;
        }
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            let _ = connection.await;
        }
    }
}

struct AcceptedStream {
    protocol: AcceptedProtocol,
    stream: PrefixedTcpStream,
}

impl AcceptedStream {
    fn into_stream(self) -> PrefixedTcpStream {
        self.stream
    }
}

async fn detect_protocol(stream: TcpStream, shutdown: &Shutdown) -> Option<AcceptedStream> {
    let (prefix, headers_end) = tokio::time::timeout(
        HTTP1_HEADER_READ_TIMEOUT,
        read_protocol_prefix(&stream, shutdown),
    )
    .await
    .ok()??;

    if prefix.starts_with(HTTP2_PREFACE) {
        return Some(accepted(AcceptedProtocol::Http2, prefix, stream));
    }

    // Chunked reads can pull request body or pipelined bytes past the header
    // terminator into the prefix; classification must only see the header
    // block, while the full prefix is still replayed to the served protocol.
    let protocol = if is_websocket_upgrade(&prefix[..headers_end]) {
        AcceptedProtocol::WebSocket
    } else {
        AcceptedProtocol::Http1
    };

    Some(accepted(protocol, prefix, stream))
}

/// Reads until the h2 preface is confirmed, the HTTP/1.1 header terminator
/// arrives, or the size cap is hit. Returns the buffered prefix and the end
/// index of the header block within it.
async fn read_protocol_prefix(stream: &TcpStream, shutdown: &Shutdown) -> Option<(Vec<u8>, usize)> {
    let mut prefix = Vec::with_capacity(HTTP2_PREFACE.len());
    let mut h2_possible = true;
    let mut scan_from = 0;

    loop {
        if h2_possible {
            if prefix.starts_with(HTTP2_PREFACE) {
                let end = prefix.len();
                return Some((prefix, end));
            }

            if !HTTP2_PREFACE.starts_with(&prefix) {
                h2_possible = false;
                scan_from = 0;
            }
        }

        if !h2_possible {
            if let Some(end) = find_headers_end(&prefix, scan_from) {
                return Some((prefix, end));
            }
            if prefix.len() >= MAX_HTTP1_HEADER_BYTES {
                let end = prefix.len();
                return Some((prefix, end));
            }
            scan_from = prefix.len().saturating_sub(3);
        }

        read_chunk(stream, shutdown, &mut prefix).await?;
    }
}

async fn read_chunk(stream: &TcpStream, shutdown: &Shutdown, buffer: &mut Vec<u8>) -> Option<()> {
    let mut chunk = [0; PROTOCOL_SNIFF_CHUNK_BYTES];
    // read_protocol_prefix stops at MAX_HTTP1_HEADER_BYTES before requesting
    // another chunk, so at least one more byte can always be read here.
    let read_len = (MAX_HTTP1_HEADER_BYTES - buffer.len()).min(chunk.len());

    loop {
        match stream.try_read(&mut chunk[..read_len]) {
            Ok(0) => return None,
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                return Some(());
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(_error) => return None,
        }

        tokio::select! {
            result = stream.readable() => {
                if result.is_err() {
                    return None;
                }
            }
            _ = shutdown.cancelled() => return None,
        }
    }
}

fn accepted(protocol: AcceptedProtocol, prefix: Vec<u8>, stream: TcpStream) -> AcceptedStream {
    AcceptedStream {
        protocol,
        stream: PrefixedTcpStream::new(prefix, stream),
    }
}

fn reap_completed_connections(connections: &mut JoinSet<()>) {
    while connections.try_join_next().is_some() {}
}

fn find_headers_end(buffer: &[u8], start: usize) -> Option<usize> {
    let start = start.min(buffer.len());
    buffer[start..]
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| start + position + 4)
}

fn is_websocket_upgrade(buffer: &[u8]) -> bool {
    let headers = String::from_utf8_lossy(buffer);
    let mut has_connection_upgrade = false;
    let mut has_upgrade_websocket = false;

    for line in headers.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_ascii_lowercase();

        if name == "connection"
            && value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        {
            has_connection_upgrade = true;
        }

        if name == "upgrade" && value == "websocket" {
            has_upgrade_websocket = true;
        }
    }

    has_connection_upgrade && has_upgrade_websocket
}

struct PrefixedTcpStream {
    prefix: Vec<u8>,
    prefix_offset: usize,
    stream: TcpStream,
}

impl PrefixedTcpStream {
    fn new(prefix: Vec<u8>, stream: TcpStream) -> Self {
        Self {
            prefix,
            prefix_offset: 0,
            stream,
        }
    }
}

impl AsyncRead for PrefixedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.prefix_offset < self.prefix.len() {
            let available = &self.prefix[self.prefix_offset..];
            let copy_len = available.len().min(buffer.remaining());
            buffer.put_slice(&available[..copy_len]);
            self.prefix_offset += copy_len;
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for PrefixedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, data)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
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
