use std::{
    convert::Infallible,
    error::Error,
    fmt, io,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{
    header::{CONNECTION, UPGRADE},
    HeaderMap, Request, Response, StatusCode,
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::{
    tungstenite::{
        client::IntoClientRequest,
        handshake::client::Response as UpstreamResponse,
        handshake::{derive_accept_key, server::create_response_with_body},
        protocol::Role,
        Error as TungsteniteError, Message,
    },
    WebSocketStream,
};

use crate::{
    drain::{DrainError, DrainPermit, DrainTracker},
    http::strip_hop_by_hop_headers,
    Shutdown,
};

#[derive(Clone, Copy, Debug)]
pub struct WebSocketProxyConfig {
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    pub close_timeout: Duration,
}

impl Default for WebSocketProxyConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(60),
            close_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WebSocketProxy {
    drain: DrainTracker,
    config: WebSocketProxyConfig,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebSocketProxyStats {
    pub client_to_upstream_messages: u64,
    pub upstream_to_client_messages: u64,
    pub client_to_upstream_bytes: u64,
    pub upstream_to_client_bytes: u64,
}

#[derive(Debug)]
pub struct AcceptedWebSocketUpstream {
    upstream: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    response: UpstreamResponse,
    _permit: DrainPermit,
}

#[derive(Debug)]
pub struct WebSocketUpgrade {
    upgraded: hyper::upgrade::OnUpgrade,
    admission: Option<crate::http::UpgradeAdmission>,
    upstream: AcceptedWebSocketUpstream,
    config: WebSocketProxyConfig,
}

#[derive(Debug)]
pub enum WebSocketProxyError {
    Drain(DrainError),
    ClientHandshake(TungsteniteError),
    UpstreamConnect(TungsteniteError),
    Proxy(TungsteniteError),
}

impl WebSocketProxy {
    pub fn new(drain: DrainTracker) -> Self {
        Self::with_config(drain, WebSocketProxyConfig::default())
    }

    pub fn with_config(drain: DrainTracker, config: WebSocketProxyConfig) -> Self {
        Self { drain, config }
    }

    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    /// Validates and completes the upstream handshake before publishing a client 101.
    pub async fn prepare_upgrade<B>(
        &self,
        request: &mut Request<B>,
        upstream_url: &str,
    ) -> Result<(Response<Full<Bytes>>, WebSocketUpgrade), WebSocketProxyError> {
        let mut response = websocket_upgrade_response(request, Full::new(Bytes::new()))?;
        let upstream = self
            .connect_accepted_upstream_with_headers(upstream_url, request.headers())
            .await?;
        copy_upstream_response_headers(upstream.response.headers(), response.headers_mut());
        let upgrade = WebSocketUpgrade {
            admission: request
                .extensions_mut()
                .remove::<crate::http::UpgradeAdmission>(),
            upgraded: hyper::upgrade::on(request),
            upstream,
            config: self.config,
        };
        Ok((response, upgrade))
    }

    /// Convenience entry point for one WebSocket connection. Uses the same HTTP
    /// parser and connect-before-101 handshake as the production listeners.
    pub async fn accept_and_proxy<Client>(
        &self,
        client: Client,
        upstream_url: &str,
    ) -> Result<WebSocketProxyStats, WebSocketProxyError>
    where
        Client: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        drop(self.drain.try_acquire()?);
        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));
        let proxy = self.clone();
        let upstream_url = upstream_url.to_owned();
        let connection =
            crate::http::serve_http_connection(client, Shutdown::new(), move |mut request| {
                let proxy = proxy.clone();
                let tx = tx.clone();
                let upstream_url = upstream_url.clone();
                async move {
                    let result = proxy.prepare_upgrade(&mut request, &upstream_url).await;
                    let (response, result) = match result {
                        Ok((response, upgrade)) => (response, Ok(upgrade)),
                        Err(error) => {
                            let mut response = websocket_error_response(&error);
                            response
                                .headers_mut()
                                .insert(CONNECTION, http::HeaderValue::from_static("close"));
                            (response, Err(error))
                        }
                    };
                    if let Some(tx) = tx.lock().expect("WebSocket result lock").take() {
                        let _ = tx.send(result);
                    }
                    Ok::<_, Infallible>(response)
                }
            });
        let mut driver = ConnectionDriver(tokio::spawn(connection));
        let prepared = timeout(Duration::from_secs(10) + self.config.handshake_timeout, rx)
            .await
            .map_err(|_| {
                WebSocketProxyError::ClientHandshake(io_error(
                    io::ErrorKind::TimedOut,
                    "client WebSocket handshake timeout",
                ))
            })?
            .map_err(|_| {
                WebSocketProxyError::ClientHandshake(io_error(
                    io::ErrorKind::UnexpectedEof,
                    "client closed before upgrade",
                ))
            })?;
        match prepared {
            Ok(upgrade) => upgrade.run().await,
            Err(error) => {
                let _ = timeout(self.config.handshake_timeout, &mut driver.0).await;
                Err(error)
            }
        }
    }

    pub async fn connect_accepted_upstream_with_headers(
        &self,
        upstream_url: &str,
        upstream_headers: &HeaderMap,
    ) -> Result<AcceptedWebSocketUpstream, WebSocketProxyError> {
        let permit = self.drain.try_acquire()?;
        let mut request = upstream_url
            .into_client_request()
            .map_err(WebSocketProxyError::UpstreamConnect)?;
        let mut headers = upstream_headers.clone();
        strip_hop_by_hop_headers(&mut headers);
        // Every hop has its own challenge. Extensions are intentionally not
        // offered: our frame relay implements plain RFC 6455 frames only.
        for name in [
            "sec-websocket-key",
            "sec-websocket-version",
            "sec-websocket-accept",
            "sec-websocket-extensions",
            "content-length",
        ] {
            headers.remove(name);
        }
        for name in headers.keys() {
            request.headers_mut().remove(name);
        }
        request.headers_mut().extend(headers);
        let (upstream, response) = timeout(
            self.config.handshake_timeout,
            connect_upstream(request, self.config.handshake_timeout),
        )
        .await
        .map_err(|_| {
            WebSocketProxyError::UpstreamConnect(io_error(
                io::ErrorKind::TimedOut,
                "upstream WebSocket handshake timeout",
            ))
        })?
        .map_err(WebSocketProxyError::UpstreamConnect)?;
        Ok(AcceptedWebSocketUpstream {
            upstream,
            response,
            _permit: permit,
        })
    }
}

// Hyper reads the complete bounded rejection body; tungstenite's connect helper
// returns only the body bytes already buffered beside the response headers.
async fn connect_upstream(
    mut request: Request<()>,
    setup_budget: Duration,
) -> Result<
    (
        WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
        UpstreamResponse,
    ),
    TungsteniteError,
> {
    let host = request
        .uri()
        .host()
        .ok_or_else(|| io_error(io::ErrorKind::InvalidInput, "missing upstream host"))?
        .to_owned();
    if request.uri().scheme_str() != Some("ws") {
        return Err(io_error(
            io::ErrorKind::InvalidInput,
            "only cleartext ws upstreams are supported",
        ));
    }
    let port = request.uri().port_u16().unwrap_or(80);
    // http::Uri retains brackets around an IPv6 authority. Tuple resolution
    // expects the bare address; brackets otherwise become an invalid DNS name.
    let dial_host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(&host);
    let stream = crate::connect::connect_tcp_attempts((dial_host, port), setup_budget).await?;
    stream.set_nodelay(true)?;
    let expected_accept = derive_accept_key(request.headers()["sec-websocket-key"].as_bytes());
    let offered = request
        .headers()
        .get_all("sec-websocket-protocol")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_owned())
        .collect::<Vec<_>>();
    *request.uri_mut() = request
        .uri()
        .path_and_query()
        .map(|path| path.as_str())
        .unwrap_or("/")
        .parse()
        .expect("validated path and query");
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(64 * 1024)
        .handshake(TokioIo::new(stream))
        .await
        .map_err(|error| io_error(io::ErrorKind::ConnectionAborted, error.to_string()))?;
    let _driver = ConnectionDriver(tokio::spawn(connection.with_upgrades()));
    let mut response = sender
        .send_request(request.map(|_| Full::new(Bytes::new())))
        .await
        .map_err(|error| io_error(io::ErrorKind::ConnectionAborted, error.to_string()))?;
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        let (parts, mut body) = response.into_parts();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame
                .map_err(|error| io_error(io::ErrorKind::ConnectionAborted, error.to_string()))?;
            if let Some(data) = frame.data_ref() {
                if bytes.len() + data.len() > 64 * 1024 {
                    return Err(io_error(
                        io::ErrorKind::InvalidData,
                        "WebSocket rejection body exceeds 64 KiB",
                    ));
                }
                bytes.extend_from_slice(data);
            }
        }
        return Err(TungsteniteError::Http(Box::new(Response::from_parts(
            parts,
            Some(bytes),
        ))));
    }
    if !header_contains_token(response.headers(), CONNECTION, "upgrade")
        || !header_contains_token(response.headers(), UPGRADE, "websocket")
        || response
            .headers()
            .get_all("sec-websocket-accept")
            .iter()
            .count()
            != 1
        || response
            .headers()
            .get("sec-websocket-accept")
            .map(|value| value.as_bytes())
            != Some(expected_accept.as_bytes())
        || response.headers().contains_key("sec-websocket-extensions")
    {
        return Err(io_error(
            io::ErrorKind::InvalidData,
            "invalid upstream WebSocket handshake or unsupported extension",
        ));
    }
    let selected = response
        .headers()
        .get_all("sec-websocket-protocol")
        .iter()
        .collect::<Vec<_>>();
    if selected.len() > 1
        || selected.first().is_some_and(|value| {
            value
                .to_str()
                .map(|value| !offered.iter().any(|offer| offer == value))
                .unwrap_or(true)
        })
    {
        return Err(io_error(
            io::ErrorKind::InvalidData,
            "upstream selected a WebSocket subprotocol the client did not offer",
        ));
    }
    let upgraded = hyper::upgrade::on(&mut response)
        .await
        .map_err(|error| io_error(io::ErrorKind::ConnectionAborted, error.to_string()))?;
    let upstream =
        WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Client, None).await;
    Ok((upstream, response.map(|_| None)))
}

struct ConnectionDriver<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for ConnectionDriver<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl WebSocketUpgrade {
    pub async fn run(self) -> Result<WebSocketProxyStats, WebSocketProxyError> {
        let (request_admission, handshake_admission) = self
            .admission
            .map(|guard| (Some(guard.request), Some(guard.handshake)))
            .unwrap_or((None, None));
        let _request_admission = request_admission;
        let upgraded = timeout(self.config.handshake_timeout, self.upgraded)
            .await
            .map_err(|_| {
                WebSocketProxyError::ClientHandshake(io_error(
                    io::ErrorKind::TimedOut,
                    "client WebSocket upgrade timeout",
                ))
            })?
            .map_err(|error| {
                WebSocketProxyError::ClientHandshake(io_error(
                    io::ErrorKind::ConnectionAborted,
                    error.to_string(),
                ))
            })?;
        drop(handshake_admission);
        let client =
            WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None).await;
        proxy_websocket_streams_with_config(client, self.upstream.upstream, self.config)
            .await
            .map_err(WebSocketProxyError::Proxy)
    }
}

pub fn is_websocket_upgrade<B>(request: &Request<B>) -> bool {
    request.method() == http::Method::GET
        && header_contains_token(request.headers(), CONNECTION, "upgrade")
        && header_contains_token(request.headers(), UPGRADE, "websocket")
}

fn header_contains_token(headers: &HeaderMap, name: http::header::HeaderName, token: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case(token))
}

pub fn websocket_upgrade_response<B, R>(
    request: &Request<B>,
    body: R,
) -> Result<Response<R>, WebSocketProxyError> {
    if !is_websocket_upgrade(request) {
        return Err(WebSocketProxyError::ClientHandshake(io_error(
            io::ErrorKind::InvalidInput,
            "invalid WebSocket upgrade request",
        )));
    }
    let mut validated = Request::new(());
    *validated.method_mut() = request.method().clone();
    *validated.uri_mut() = request.uri().clone();
    *validated.version_mut() = request.version();
    *validated.headers_mut() = request.headers().clone();
    // The shared parser accepts tokens across repeated fields. Normalize these
    // two fields before tungstenite validates the remaining RFC 6455 challenge.
    validated
        .headers_mut()
        .insert(CONNECTION, http::HeaderValue::from_static("upgrade"));
    validated
        .headers_mut()
        .insert(UPGRADE, http::HeaderValue::from_static("websocket"));
    create_response_with_body(&validated, || body).map_err(WebSocketProxyError::ClientHandshake)
}

fn copy_upstream_response_headers(upstream: &HeaderMap, downstream: &mut HeaderMap) {
    let mut headers = upstream.clone();
    strip_hop_by_hop_headers(&mut headers);
    for name in [
        "sec-websocket-accept",
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-extensions",
        "content-length",
    ] {
        headers.remove(name);
    }
    downstream.extend(headers);
}

pub fn websocket_error_response(error: &WebSocketProxyError) -> Response<Full<Bytes>> {
    let status = match error {
        WebSocketProxyError::Drain(_) => StatusCode::SERVICE_UNAVAILABLE,
        WebSocketProxyError::ClientHandshake(_) => StatusCode::BAD_REQUEST,
        WebSocketProxyError::UpstreamConnect(TungsteniteError::Http(response)) => response.status(),
        WebSocketProxyError::UpstreamConnect(TungsteniteError::Io(error))
            if error.kind() == io::ErrorKind::TimedOut =>
        {
            StatusCode::GATEWAY_TIMEOUT
        }
        _ => StatusCode::BAD_GATEWAY,
    };
    let mut response = Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("valid error response");
    if let WebSocketProxyError::UpstreamConnect(TungsteniteError::Http(upstream)) = error {
        copy_upstream_response_headers(upstream.headers(), response.headers_mut());
        *response.body_mut() = Full::new(Bytes::from(upstream.body().clone().unwrap_or_default()));
    }
    response
}

fn io_error(kind: io::ErrorKind, message: impl Into<String>) -> TungsteniteError {
    TungsteniteError::Io(io::Error::new(kind, message.into()))
}

pub async fn proxy_websocket_streams<Client, Upstream>(
    client: WebSocketStream<Client>,
    upstream: WebSocketStream<Upstream>,
) -> Result<WebSocketProxyStats, TungsteniteError>
where
    Client: AsyncRead + AsyncWrite + Unpin,
    Upstream: AsyncRead + AsyncWrite + Unpin,
{
    proxy_websocket_streams_with_config(client, upstream, WebSocketProxyConfig::default()).await
}

pub async fn proxy_websocket_streams_with_config<Client, Upstream>(
    mut client: WebSocketStream<Client>,
    mut upstream: WebSocketStream<Upstream>,
    config: WebSocketProxyConfig,
) -> Result<WebSocketProxyStats, TungsteniteError>
where
    Client: AsyncRead + AsyncWrite + Unpin,
    Upstream: AsyncRead + AsyncWrite + Unpin,
{
    let mut stats = WebSocketProxyStats::default();

    loop {
        tokio::select! {
            message = client.next() => {
                let Some(message) = message else {
                    let _ = timeout(config.close_timeout, upstream.close(None)).await;
                    return Ok(stats);
                };

                let message = message?;
                let is_close = message.is_close();
                stats.client_to_upstream_messages += 1;
                stats.client_to_upstream_bytes += message_payload_len(&message) as u64;
                bounded_write(if is_close { config.close_timeout } else { config.write_timeout }, upstream.send(message)).await?;

                if is_close {
                    bounded_write(config.close_timeout, flush_close_reply(&mut client)).await?;
                    wait_for_close_response(&mut upstream, config.close_timeout).await?;
                    return Ok(stats);
                }
            }
            message = upstream.next() => {
                let Some(message) = message else {
                    let _ = timeout(config.close_timeout, client.close(None)).await;
                    return Ok(stats);
                };

                let message = message?;
                let is_close = message.is_close();
                stats.upstream_to_client_messages += 1;
                stats.upstream_to_client_bytes += message_payload_len(&message) as u64;
                bounded_write(if is_close { config.close_timeout } else { config.write_timeout }, client.send(message)).await?;

                if is_close {
                    bounded_write(config.close_timeout, flush_close_reply(&mut upstream)).await?;
                    wait_for_close_response(&mut client, config.close_timeout).await?;
                    return Ok(stats);
                }
            }
        }
    }
}

async fn bounded_write<F>(duration: Duration, future: F) -> Result<(), TungsteniteError>
where
    F: std::future::Future<Output = Result<(), TungsteniteError>>,
{
    timeout(duration, future)
        .await
        .map_err(|_| io_error(io::ErrorKind::TimedOut, "WebSocket write timeout"))?
}

async fn flush_close_reply<S>(websocket: &mut WebSocketStream<S>) -> Result<(), TungsteniteError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match websocket.flush().await {
        Ok(()) | Err(TungsteniteError::ConnectionClosed) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn wait_for_close_response<S>(
    websocket: &mut WebSocketStream<S>,
    close_timeout: Duration,
) -> Result<(), TungsteniteError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match timeout(close_timeout, wait_for_close_frame(websocket)).await {
        Ok(result) => result,
        Err(_) => Ok(()),
    }
}

async fn wait_for_close_frame<S>(websocket: &mut WebSocketStream<S>) -> Result<(), TungsteniteError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match websocket.next().await {
            Some(Ok(message)) if message.is_close() => return Ok(()),
            Some(Ok(_)) => continue,
            Some(Err(TungsteniteError::ConnectionClosed)) | None => return Ok(()),
            Some(Err(error)) => return Err(error),
        }
    }
}

fn message_payload_len(message: &Message) -> usize {
    match message {
        Message::Text(payload) => payload.len(),
        Message::Binary(payload) | Message::Ping(payload) | Message::Pong(payload) => payload.len(),
        Message::Close(close) => close
            .as_ref()
            .map(|frame| frame.reason.len())
            .unwrap_or_default(),
        Message::Frame(frame) => frame.payload().len(),
    }
}

impl From<DrainError> for WebSocketProxyError {
    fn from(error: DrainError) -> Self {
        Self::Drain(error)
    }
}

impl fmt::Display for WebSocketProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drain(error) => write!(f, "{error}"),
            Self::ClientHandshake(error) => write!(f, "websocket client handshake failed: {error}"),
            Self::UpstreamConnect(error) => write!(f, "websocket upstream connect failed: {error}"),
            Self::Proxy(error) => write!(f, "websocket proxy failed: {error}"),
        }
    }
}

impl Error for WebSocketProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Drain(error) => Some(error),
            Self::ClientHandshake(error) | Self::UpstreamConnect(error) | Self::Proxy(error) => {
                Some(error)
            }
        }
    }
}
