use std::{
    error::Error,
    fmt,
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::{Buf, Bytes};
use http::{
    header::{CONNECTION, HOST},
    uri::InvalidUriParts,
    HeaderMap, HeaderName, HeaderValue, Request, Response, Uri,
};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt};
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client, Error as ClientError},
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto,
};
use pin_project_lite::pin_project;

use crate::{
    drain::{DrainError, DrainPermit, DrainTracker},
    AdmissionPermit, ProxyAdmission, ProxyResourceConfig, Shutdown,
};
use tokio::io::{AsyncRead, AsyncWrite};

type BoxError = Box<dyn Error + Send + Sync>;
type UpstreamBody = UnsyncBoxBody<UploadBuf, BoxError>;
type UpstreamClient = Client<LimitedConnector, UpstreamBody>;

#[derive(Clone, Debug)]
pub struct HttpProxy {
    drain: DrainTracker,
    http1_client: UpstreamClient,
    http2_client: UpstreamClient,
    config: ProxyResourceConfig,
    upstream_connections: crate::AdmissionLimiter,
}

#[derive(Debug)]
pub enum HttpProxyError {
    Drain(DrainError),
    RequestRewrite(ReverseProxyRequestError),
    Client(ClientError),
    UpstreamHeaderTimeout,
    UpstreamSaturated,
}

#[derive(Debug)]
pub enum ReverseProxyRequestError {
    MissingUpstreamScheme,
    MissingUpstreamAuthority,
    InvalidUri(InvalidUriParts),
}

impl HttpProxy {
    pub fn new(drain: DrainTracker) -> Self {
        Self::with_config(drain, ProxyResourceConfig::default())
    }

    pub fn with_config(drain: DrainTracker, config: ProxyResourceConfig) -> Self {
        let upstream_connections = crate::AdmissionLimiter::new(config.max_upstream_connections());
        let connector = LimitedConnector {
            inner: http_connector(config),
            capacity: upstream_connections.clone(),
            write_timeout: config.write_idle_timeout(),
            connect_timeout: config.setup_timeout(),
        };
        let mut http1_builder = Client::builder(TokioExecutor::new());
        configure_pool(&mut http1_builder, config);
        let http1_client = http1_builder.build(connector.clone());
        let mut http2_builder = Client::builder(TokioExecutor::new());
        // h2c has no ALPN signal, so HTTP/2 inbound requests use prior knowledge
        // when dialing a cleartext upstream.
        http2_builder.http2_only(true);
        configure_pool(&mut http2_builder, config);
        let http2_client = http2_builder.build(connector);

        Self {
            drain,
            http1_client,
            http2_client,
            config,
            upstream_connections,
        }
    }

    pub fn upstream_connections(&self) -> &crate::AdmissionLimiter {
        &self.upstream_connections
    }

    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    pub async fn proxy<B>(
        &self,
        request: Request<B>,
        upstream_origin: &Uri,
    ) -> Result<Response<TrackedBody<Incoming>>, HttpProxyError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        let permit = Arc::new(self.drain.try_acquire()?);
        let admitted = request
            .extensions()
            .get::<RequestAdmission>()
            .map(|guard| guard.0.clone());
        let track_delivery = admitted.is_some();
        let use_http2_upstream = request.version() == http::Version::HTTP_2;
        let mut request = prepare_reverse_proxy_request(request, upstream_origin)?;
        let capture = (!request.body().is_end_stream())
            .then(|| hyper_util::client::legacy::connect::capture_connection(&mut request));
        let mut cancel_on_error = CancelUpstream {
            capture: capture.clone(),
            armed: true,
        };
        let upload_delivery = crate::delivery::DeliveryState::new(
            admitted,
            None,
            Some(permit.clone()),
            self.config.write_idle_timeout(),
            crate::delivery::DeliveryFailure::Upstream(capture),
        );
        let progress = Arc::new(UploadProgress {
            started: tokio::time::Instant::now(),
            latest_nanos: AtomicU64::new(0),
        });
        let request = request.map(|body| {
            box_upstream_body(UploadBody {
                inner: body,
                progress: progress.clone(),
                delivery: upload_delivery,
            })
        });
        let client = if use_http2_upstream {
            &self.http2_client
        } else {
            &self.http1_client
        };
        let response = client.request(request);
        tokio::pin!(response);
        let mut deadline = progress.started + self.config.upstream_header_idle_timeout();
        let mut response = loop {
            tokio::select! {
                response = &mut response => break response.map_err(classify_client_error)?,
                _ = tokio::time::sleep_until(deadline) => {
                    deadline = progress.started + Duration::from_nanos(progress.latest_nanos.load(Ordering::Relaxed)) + self.config.upstream_header_idle_timeout();
                    if deadline <= tokio::time::Instant::now() { return Err(HttpProxyError::UpstreamHeaderTimeout); }
                }
            }
        };

        strip_hop_by_hop_headers(response.headers_mut());

        cancel_on_error.armed = false;
        if track_delivery {
            response
                .extensions_mut()
                .insert(ResponseDrain(permit.clone()));
        }
        Ok(response.map(|body| TrackedBody {
            inner: body,
            permit: Some(permit),
        }))
    }
}

/// Shared HTTP server with default process-local bounds. Production listeners
/// call the admitted variant with one admission state shared by all connections.
pub async fn serve_http_connection<IO, F, Fut, B, E>(io: IO, shutdown: Shutdown, handler: F)
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Fn(Request<Incoming>) -> Fut,
    Fut: Future<Output = Result<Response<B>, E>> + Send + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    E: Into<BoxError>,
{
    let admission = ProxyAdmission::new(ProxyResourceConfig::default());
    let handshake = admission
        .handshakes
        .try_acquire()
        .expect("new admission has capacity");
    let io = admission.admit_io(io).expect("new admission has capacity");
    serve_http_connection_admitted(io, shutdown, admission, handshake, None, handler).await;
}

/// Admission runs synchronously before creating handler futures. HTTP/2 also
/// rejects streams above its finite transport cap before service dispatch.
pub async fn serve_http_connection_admitted<IO, F, Fut, B, E>(
    io: IO,
    shutdown: Shutdown,
    admission: ProxyAdmission,
    handshake: AdmissionPermit,
    initial_work: Option<crate::DrainPermit>,
    handler: F,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Fn(Request<Incoming>) -> Fut,
    Fut: Future<Output = Result<Response<B>, E>> + Send + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    E: Into<BoxError>,
{
    let config = admission.config();
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(config.setup_timeout())
        .max_buf_size(64 * 1024);
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(config.setup_timeout())
        .max_concurrent_streams(config.max_http2_streams())
        .max_header_list_size(64 * 1024);
    let delivery_failed = Shutdown::new();
    let request_started = AtomicBool::new(false);
    let handshake = Mutex::new((Some(handshake), initial_work));
    let service = hyper::service::service_fn(|mut request| {
        let initial_work = if !request_started.swap(true, Ordering::Relaxed) {
            let mut handshake = handshake.lock().expect("handshake lock");
            drop(handshake.0.take());
            handshake.1.take()
        } else {
            None
        };
        let admitted = admission.requests.try_acquire().ok().map(Arc::new);
        let upgrade = crate::is_websocket_upgrade(&request);
        let handshake = if upgrade {
            admission.handshakes.try_acquire().ok()
        } else {
            None
        };
        let allowed = admitted.is_some() && (!upgrade || handshake.is_some());
        let (permit, upgrade_permit) = if upgrade && allowed {
            let guard = UpgradeAdmission {
                request: admitted.expect("admitted"),
                handshake: Arc::new(handshake.expect("handshake admitted")),
            };
            request.extensions_mut().insert(guard.clone());
            (None, Some(guard))
        } else {
            (admitted, None)
        };
        // Do not call the application or allocate its future when saturated.
        if let Some(permit) = &permit {
            request
                .extensions_mut()
                .insert(RequestAdmission(permit.clone()));
        }
        let future = allowed.then(|| handler(request));
        let delivery_failed = delivery_failed.clone();
        async move {
            // A sidecar owns the initial public connection setup before any
            // application bytes arrive. Keep it through the first handler's
            // response, when ordinary HTTP/upgrade guards already own work.
            // Overload, cancellation and errors also release it; idle keepalive
            // connections do not retain it.
            let _initial_work = initial_work;
            let Some(future) = future else {
                return Ok::<_, E>(
                    Response::builder()
                        .status(http::StatusCode::SERVICE_UNAVAILABLE)
                        .header(http::header::RETRY_AFTER, "1")
                        .body(AdmittedBody::<B> {
                            inner: None,
                            delivery: None,
                        })
                        .expect("overload response"),
                );
            };
            let mut response = future.await?;
            let drain = response
                .extensions_mut()
                .remove::<ResponseDrain>()
                .map(|guard| guard.0);
            Ok(response.map(|body| AdmittedBody {
                inner: Some(body),
                delivery: Some(crate::delivery::DeliveryState::new(
                    permit,
                    upgrade_permit.map(|guard| guard.request),
                    drain,
                    config.write_idle_timeout(),
                    delivery_failed,
                )),
            }))
        }
    });
    let connection = builder.serve_connection_with_upgrades(TokioIo::new(io), service);
    tokio::pin!(connection);
    tokio::select! {
        _ = delivery_failed.cancelled() => {},
        _ = &mut connection => {},
        _ = tokio::time::sleep(config.setup_timeout()) => {
            if request_started.load(Ordering::Relaxed) {
                tokio::select! {
                    _ = delivery_failed.cancelled() => {},
                    _ = &mut connection => {},
                    _ = shutdown.cancelled() => { connection.as_mut().graceful_shutdown(); let _ = connection.await; }
                }
            }
        },
        _ = shutdown.cancelled() => { connection.as_mut().graceful_shutdown(); let _ = connection.await; }
    }
}

#[derive(Clone, Debug)]
struct RequestAdmission(Arc<AdmissionPermit>);
#[derive(Clone, Debug)]
struct ResponseDrain(Arc<DrainPermit>);

#[derive(Clone, Debug)]
pub(crate) struct UpgradeAdmission {
    pub request: Arc<AdmissionPermit>,
    pub handshake: Arc<AdmissionPermit>,
}

pin_project! {
    struct AdmittedBody<B> {
        #[pin]
        inner: Option<B>,
        delivery: Option<Arc<crate::delivery::DeliveryState>>,
    }
}
impl<B: Body> Body for AdmittedBody<B> {
    type Data = crate::delivery::DeliveryBuf<B::Data>;
    type Error = B::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match this.inner.as_pin_mut() {
            Some(body) => body.poll_frame(cx).map(|frame| {
                frame.map(|frame| {
                    frame.map(|frame| {
                        frame.map_data(|data| {
                            crate::delivery::DeliveryBuf::new(
                                data,
                                this.delivery
                                    .as_ref()
                                    .expect("admitted body has delivery state")
                                    .clone(),
                            )
                        })
                    })
                })
            }),
            None => Poll::Ready(None),
        }
    }
    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(Body::is_end_stream)
    }
    fn size_hint(&self) -> SizeHint {
        self.inner
            .as_ref()
            .map(Body::size_hint)
            .unwrap_or_else(|| SizeHint::with_exact(0))
    }
}

struct UploadProgress {
    started: tokio::time::Instant,
    latest_nanos: AtomicU64,
}
pin_project! {
    struct UploadBody<B> {
        #[pin]
        inner: B,
        progress: Arc<UploadProgress>,
        delivery: Arc<crate::delivery::DeliveryState>,
    }
}
impl<B: Body<Data = Bytes>> Body for UploadBody<B> {
    type Data = UploadBuf;
    type Error = B::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        this.inner.poll_frame(cx).map(|frame| {
            frame.map(|frame| {
                frame.map(|frame| {
                    frame.map_data(|inner| UploadBuf {
                        inner: crate::delivery::DeliveryBuf::new(inner, this.delivery.clone()),
                        progress: this.progress.clone(),
                    })
                })
            })
        })
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
struct UploadBuf {
    inner: crate::delivery::DeliveryBuf<Bytes>,
    progress: Arc<UploadProgress>,
}
impl Buf for UploadBuf {
    fn remaining(&self) -> usize {
        self.inner.remaining()
    }
    fn chunk(&self) -> &[u8] {
        self.inner.chunk()
    }
    fn advance(&mut self, count: usize) {
        self.inner.advance(count);
        if count > 0 {
            self.progress.latest_nanos.store(
                self.progress.started.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
        }
    }
    fn chunks_vectored<'a>(&'a self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        self.inner.chunks_vectored(dst)
    }
}

fn configure_pool(builder: &mut hyper_util::client::legacy::Builder, config: ProxyResourceConfig) {
    builder
        .pool_timer(TokioTimer::new())
        .pool_idle_timeout(config.pool_idle_timeout())
        .pool_max_idle_per_host(config.max_idle_per_host());
}

#[derive(Clone, Debug)]
struct LimitedConnector {
    inner: HttpConnector,
    capacity: crate::AdmissionLimiter,
    write_timeout: Duration,
    connect_timeout: Duration,
}
impl tower_service::Service<Uri> for LimitedConnector {
    type Response = TokioIo<crate::AdmittedIo<tokio::net::TcpStream>>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }
    fn call(&mut self, uri: Uri) -> Self::Future {
        let permit = self.capacity.try_acquire();
        if let Err(error) = permit {
            return Box::pin(async move { Err(Box::new(error) as BoxError) });
        }
        let mut connector = self.inner.clone();
        let write_timeout = self.write_timeout;
        let connect_timeout = self.connect_timeout;
        Box::pin(async move {
            let stream = tokio::time::timeout(
                connect_timeout,
                crate::connect::retry_connect(connect_timeout, |tcp_timeout| {
                    connector.set_connect_timeout(Some(tcp_timeout));
                    connector.call(uri.clone())
                }),
            )
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "upstream HTTP connect timeout",
                )
            })??;
            Ok(TokioIo::new(
                crate::AdmittedIo::new(
                    stream.into_inner(),
                    permit.expect("admitted connection"),
                    write_timeout,
                )
                .cancellable(),
            ))
        })
    }
}
struct CancelUpstream {
    capture: Option<hyper_util::client::legacy::connect::CaptureConnection>,
    armed: bool,
}
impl Drop for CancelUpstream {
    fn drop(&mut self) {
        if self.armed {
            crate::delivery::DeliveryFailure::Upstream(self.capture.take()).cancel();
        }
    }
}

fn classify_client_error(error: ClientError) -> HttpProxyError {
    let mut source = error.source();
    while let Some(cause) = source {
        if cause.is::<crate::AdmissionError>() {
            return HttpProxyError::UpstreamSaturated;
        }
        source = cause.source();
    }
    HttpProxyError::Client(error)
}

fn http_connector(config: ProxyResourceConfig) -> HttpConnector {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_connect_timeout(Some(config.setup_timeout()));
    connector
}

fn box_upstream_body<B>(body: B) -> UpstreamBody
where
    B: Body<Data = UploadBuf> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(|error| error.into()).boxed_unsync()
}

pub fn prepare_reverse_proxy_request<B>(
    mut request: Request<B>,
    upstream_origin: &Uri,
) -> Result<Request<B>, ReverseProxyRequestError> {
    *request.uri_mut() = upstream_request_uri(upstream_origin, request.uri())?;
    strip_hop_by_hop_headers(request.headers_mut());
    Ok(request)
}

pub fn apply_forwarded_header_policy<B>(request: &mut Request<B>, peer_ip: IpAddr, proto: &str) {
    let original_host = request.headers().get(HOST).cloned();
    let websocket = crate::websocket::is_websocket_upgrade(request);
    // Consume the client's hop-by-hop nominations before installing trusted
    // metadata. A later upstream rewrite must not let Connection remove it.
    strip_hop_by_hop_headers(request.headers_mut());
    if websocket {
        request
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("upgrade"));
        request
            .headers_mut()
            .insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
    }
    strip_forwarded_headers(request.headers_mut());

    request.headers_mut().insert(
        HeaderName::from_static("x-forwarded-for"),
        HeaderValue::from_str(&peer_ip.to_string()).expect("IP address is a valid header value"),
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_str(proto).expect("forwarded proto is a valid header value"),
    );

    if let Some(original_host) = original_host {
        request
            .headers_mut()
            .insert(HeaderName::from_static("x-forwarded-host"), original_host);
    }
}

pub fn forwarded_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = HeaderMap::new();
    for (name, value) in headers.iter() {
        if is_forwarded_header_name(name) {
            forwarded.append(name.clone(), value.clone());
        }
    }
    forwarded
}

pub fn strip_forwarded_headers(headers: &mut HeaderMap) {
    let forwarded_names: Vec<HeaderName> = headers
        .keys()
        .filter(|name| is_forwarded_header_name(name))
        .cloned()
        .collect();

    for name in forwarded_names {
        headers.remove(name);
    }
}

pub fn upstream_request_uri(
    upstream_origin: &Uri,
    original_uri: &Uri,
) -> Result<Uri, ReverseProxyRequestError> {
    let Some(scheme) = upstream_origin.scheme().cloned() else {
        return Err(ReverseProxyRequestError::MissingUpstreamScheme);
    };
    let Some(authority) = upstream_origin.authority().cloned() else {
        return Err(ReverseProxyRequestError::MissingUpstreamAuthority);
    };

    let mut parts = Uri::default().into_parts();
    parts.scheme = Some(scheme);
    parts.authority = Some(authority);
    parts.path_and_query = original_uri
        .path_and_query()
        .cloned()
        .or_else(|| Some(http::uri::PathAndQuery::from_static("/")));

    Uri::from_parts(parts).map_err(ReverseProxyRequestError::InvalidUri)
}

pub fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    let connection_tokens = connection_header_tokens(headers);

    for name in connection_tokens {
        headers.remove(name);
    }

    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn connection_header_tokens(headers: &HeaderMap) -> Vec<HeaderName> {
    headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|token| {
            let token = token.trim();
            if token.is_empty() {
                return None;
            }

            HeaderName::from_bytes(token.as_bytes()).ok()
        })
        .collect()
}

fn is_forwarded_header_name(name: &HeaderName) -> bool {
    let name = name.as_str();
    name.eq_ignore_ascii_case("forwarded")
        || name
            .get(..12)
            .map(|prefix| prefix.eq_ignore_ascii_case("x-forwarded-"))
            .unwrap_or(false)
}

pin_project! {
    #[derive(Debug)]
    pub struct TrackedBody<B> {
        #[pin]
        inner: B,
        permit: Option<Arc<DrainPermit>>,
    }
}

impl<B> TrackedBody<B> {
    pub fn new(inner: B, permit: DrainPermit) -> Self {
        Self {
            inner,
            permit: Some(Arc::new(permit)),
        }
    }
}

impl<B> Body for TrackedBody<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let poll = this.inner.poll_frame(cx);

        if matches!(poll, Poll::Ready(None)) {
            drop(this.permit.take());
        }

        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl From<DrainError> for HttpProxyError {
    fn from(error: DrainError) -> Self {
        Self::Drain(error)
    }
}

impl From<ReverseProxyRequestError> for HttpProxyError {
    fn from(error: ReverseProxyRequestError) -> Self {
        Self::RequestRewrite(error)
    }
}

impl From<ClientError> for HttpProxyError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl fmt::Display for HttpProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drain(error) => write!(f, "{error}"),
            Self::RequestRewrite(error) => write!(f, "http request rewrite failed: {error}"),
            Self::Client(error) => write!(f, "http proxy request failed: {error}"),
            Self::UpstreamHeaderTimeout => f.write_str("upstream response headers stalled"),
            Self::UpstreamSaturated => f.write_str("upstream connection capacity exhausted"),
        }
    }
}

impl Error for HttpProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Drain(error) => Some(error),
            Self::RequestRewrite(error) => Some(error),
            Self::Client(error) => Some(error),
            Self::UpstreamHeaderTimeout | Self::UpstreamSaturated => None,
        }
    }
}

impl fmt::Display for ReverseProxyRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingUpstreamScheme => write!(f, "upstream origin is missing a scheme"),
            Self::MissingUpstreamAuthority => {
                write!(f, "upstream origin is missing an authority")
            }
            Self::InvalidUri(error) => write!(f, "invalid upstream request URI: {error}"),
        }
    }
}

impl Error for ReverseProxyRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUri(error) => Some(error),
            Self::MissingUpstreamScheme | Self::MissingUpstreamAuthority => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, str::FromStr};

    use http::{Method, Request};

    use super::{
        apply_forwarded_header_policy, prepare_reverse_proxy_request, strip_hop_by_hop_headers,
        upstream_request_uri, ReverseProxyRequestError,
    };

    #[test]
    fn request_rewrite_preserves_method_path_query_and_forwardable_headers() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/things?name=value")
            .header("host", "app.example.test")
            .header("x-forwarded-value", "kept")
            .header("connection", "x-remove, upgrade")
            .header("x-remove", "drop")
            .header("upgrade", "websocket")
            .body(())
            .expect("request builds");
        let upstream = "http://127.0.0.1:8080".parse().expect("valid upstream");

        let rewritten =
            prepare_reverse_proxy_request(request, &upstream).expect("request rewrites");

        assert_eq!(rewritten.method(), Method::POST);
        assert_eq!(
            rewritten.uri().to_string(),
            "http://127.0.0.1:8080/api/things?name=value"
        );
        assert_eq!(
            rewritten.headers().get("host").expect("host is preserved"),
            "app.example.test"
        );
        assert_eq!(
            rewritten
                .headers()
                .get("x-forwarded-value")
                .expect("forwardable header is preserved"),
            "kept"
        );
        assert!(rewritten.headers().get("connection").is_none());
        assert!(rewritten.headers().get("upgrade").is_none());
        assert!(rewritten.headers().get("x-remove").is_none());
    }

    #[test]
    fn request_rewrite_uses_root_path_when_original_uri_has_no_path() {
        let upstream = "http://127.0.0.1:8080".parse().expect("valid upstream");
        let original = "http://incoming.example.test"
            .parse()
            .expect("absolute URI without explicit path");

        let rewritten = upstream_request_uri(&upstream, &original).expect("uri rewrites");

        assert_eq!(rewritten.to_string(), "http://127.0.0.1:8080/");
    }

    #[test]
    fn request_rewrite_requires_absolute_upstream_origin() {
        let upstream = "/relative".parse().expect("relative URI parses");
        let original = "/".parse().expect("relative URI parses");

        assert!(matches!(
            upstream_request_uri(&upstream, &original).expect_err("scheme is required"),
            ReverseProxyRequestError::MissingUpstreamScheme
        ));
    }

    #[test]
    fn strip_removes_standard_and_connection_named_hop_by_hop_headers() {
        let mut request = Request::builder()
            .uri("/")
            .header("connection", "x-drop-one, x-drop-two")
            .header("x-drop-one", "no")
            .header("x-drop-two", "no")
            .header("transfer-encoding", "chunked")
            .header("x-keep", "yes")
            .body(())
            .expect("request builds");

        strip_hop_by_hop_headers(request.headers_mut());

        assert!(request.headers().get("connection").is_none());
        assert!(request.headers().get("x-drop-one").is_none());
        assert!(request.headers().get("x-drop-two").is_none());
        assert!(request.headers().get("transfer-encoding").is_none());
        assert_eq!(request.headers().get("x-keep").expect("kept"), "yes");
    }

    #[test]
    fn forwarded_header_policy_replaces_spoofable_edge_headers() {
        let mut request = Request::builder()
            .uri("/")
            .header("host", "app.example.test")
            .header("forwarded", "for=198.51.100.1;proto=https")
            .header("x-forwarded-for", "198.51.100.2")
            .header("x-forwarded-proto", "https")
            .header("x-forwarded-host", "spoof.example.test")
            .header("x-forwarded-prefix", "/spoofed")
            .header("x-real-ip", "198.51.100.3")
            .header(
                "connection",
                "x-forwarded-for, x-forwarded-proto, x-forwarded-host",
            )
            .body(())
            .expect("request builds");

        apply_forwarded_header_policy(
            &mut request,
            IpAddr::from_str("203.0.113.10").expect("peer IP parses"),
            "http",
        );

        assert!(request.headers().get("forwarded").is_none());
        assert_eq!(
            request
                .headers()
                .get("x-forwarded-for")
                .expect("canonical x-forwarded-for"),
            "203.0.113.10"
        );
        assert_eq!(
            request
                .headers()
                .get("x-forwarded-proto")
                .expect("canonical x-forwarded-proto"),
            "http"
        );
        assert_eq!(
            request
                .headers()
                .get("x-forwarded-host")
                .expect("canonical x-forwarded-host"),
            "app.example.test"
        );
        assert!(request.headers().get("x-forwarded-prefix").is_none());
        assert_eq!(
            request
                .headers()
                .get("x-real-ip")
                .expect("normal header kept"),
            "198.51.100.3"
        );
    }
}
