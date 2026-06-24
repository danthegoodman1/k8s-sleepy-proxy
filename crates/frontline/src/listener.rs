use std::{convert::Infallible, error::Error, fmt, io, net::SocketAddr, time::Instant};

use bytes::Bytes;
use http::{header::CONNECTION, header::UPGRADE, Method, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use proxy_core::{websocket_upgrade_response, DrainError, Shutdown};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::Mutex,
    task::{JoinError, JoinSet},
};

use crate::{
    is_http01_challenge_candidate_path,
    runtime::{resolve_http01_response, resolve_http_route, route_outcome_or_forward_response},
    FrontlineForwardContext, FrontlineForwarder, FrontlineHttpRuntime, FrontlineRouteCoordinator,
    FrontlineRouteOutcome, FrontlineTlsAdapter, Http01ChallengeResolver, RouteSubscriptionClient,
    WakeClient,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineHttpListenerConfig {
    listen_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineTlsTerminationListenerConfig {
    listen_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineTlsPassthroughListenerConfig {
    listen_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineListenersConfig {
    http: FrontlineHttpListenerConfig,
    tls_termination: Option<FrontlineTlsTerminationListenerConfig>,
    tls_passthrough: Option<FrontlineTlsPassthroughListenerConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrontlineListenerKind {
    Http,
    TlsTermination,
    TlsPassthrough,
}

#[derive(Debug)]
pub enum FrontlineHttpListenerError {
    Bind { addr: SocketAddr, source: io::Error },
    Accept(io::Error),
    Drain(DrainError),
}

#[derive(Debug)]
pub enum FrontlineListenerError {
    Bind {
        kind: FrontlineListenerKind,
        addr: SocketAddr,
        source: io::Error,
    },
    Accept {
        kind: FrontlineListenerKind,
        source: io::Error,
    },
    Drain(DrainError),
    Task(JoinError),
}

#[derive(Debug)]
struct SharedFrontlineHttpRuntime<RouteClient, Wake, Http01> {
    coordinator: std::sync::Arc<Mutex<FrontlineRouteCoordinator<RouteClient, Wake>>>,
    http01_resolver: std::sync::Arc<Mutex<Http01>>,
    forwarder: FrontlineForwarder,
    drain: proxy_core::DrainTracker,
    websocket_tasks: std::sync::Arc<Mutex<JoinSet<()>>>,
}

impl FrontlineHttpListenerConfig {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self { listen_addr }
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

impl FrontlineTlsTerminationListenerConfig {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self { listen_addr }
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

impl FrontlineTlsPassthroughListenerConfig {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self { listen_addr }
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

impl FrontlineListenersConfig {
    pub fn new(http: FrontlineHttpListenerConfig) -> Self {
        Self {
            http,
            tls_termination: None,
            tls_passthrough: None,
        }
    }

    pub fn with_tls_termination(
        mut self,
        tls_termination: Option<FrontlineTlsTerminationListenerConfig>,
    ) -> Self {
        self.tls_termination = tls_termination;
        self
    }

    pub fn with_tls_passthrough(
        mut self,
        tls_passthrough: Option<FrontlineTlsPassthroughListenerConfig>,
    ) -> Self {
        self.tls_passthrough = tls_passthrough;
        self
    }

    pub fn http(&self) -> FrontlineHttpListenerConfig {
        self.http
    }

    pub fn tls_termination(&self) -> Option<FrontlineTlsTerminationListenerConfig> {
        self.tls_termination
    }

    pub fn tls_passthrough(&self) -> Option<FrontlineTlsPassthroughListenerConfig> {
        self.tls_passthrough
    }
}

pub async fn serve_http<RouteClient, Wake, Http01>(
    config: FrontlineHttpListenerConfig,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let listener = TcpListener::bind(config.listen_addr())
        .await
        .map_err(|source| FrontlineHttpListenerError::Bind {
            addr: config.listen_addr(),
            source,
        })?;

    serve_http_listener(listener, runtime, shutdown).await
}

pub async fn serve_frontline<RouteClient, Wake, Http01>(
    config: FrontlineListenersConfig,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    tls_adapter: FrontlineTlsAdapter,
    shutdown: Shutdown,
) -> Result<(), FrontlineListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let shared = SharedFrontlineHttpRuntime::from_runtime(runtime);
    let mut listeners = JoinSet::new();

    listeners.spawn({
        let shared = shared.clone();
        let shutdown = shutdown.clone();
        async move {
            let listener =
                bind_listener(FrontlineListenerKind::Http, config.http.listen_addr()).await?;
            serve_http_listener_with_shared(listener, shared, shutdown)
                .await
                .map_err(FrontlineListenerError::from)
        }
    });

    if let Some(tls_termination) = config.tls_termination {
        listeners.spawn({
            let shared = shared.clone();
            let tls_adapter = tls_adapter.clone();
            let shutdown = shutdown.clone();
            async move {
                let listener = bind_listener(
                    FrontlineListenerKind::TlsTermination,
                    tls_termination.listen_addr(),
                )
                .await?;
                serve_tls_termination_listener_with_shared(listener, shared, tls_adapter, shutdown)
                    .await
            }
        });
    }

    if let Some(tls_passthrough) = config.tls_passthrough {
        listeners.spawn({
            let shared = shared.clone();
            let tls_adapter = tls_adapter.clone();
            let shutdown = shutdown.clone();
            async move {
                let listener = bind_listener(
                    FrontlineListenerKind::TlsPassthrough,
                    tls_passthrough.listen_addr(),
                )
                .await?;
                serve_tls_passthrough_listener_with_shared(listener, shared, tls_adapter, shutdown)
                    .await
            }
        });
    }

    let mut first_error = None;
    while let Some(result) = listeners.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                shutdown.shutdown();
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(error) => {
                shutdown.shutdown();
                if first_error.is_none() {
                    first_error = Some(FrontlineListenerError::Task(error));
                }
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub async fn serve_http_listener<RouteClient, Wake, Http01>(
    listener: TcpListener,
    runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let shared = SharedFrontlineHttpRuntime::from_runtime(runtime);
    serve_http_listener_with_shared(listener, shared, shutdown).await
}

async fn bind_listener(
    kind: FrontlineListenerKind,
    addr: SocketAddr,
) -> Result<TcpListener, FrontlineListenerError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| FrontlineListenerError::Bind { kind, addr, source })
}

async fn serve_http_listener_with_shared<RouteClient, Wake, Http01>(
    listener: TcpListener,
    shared: SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer_addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(FrontlineHttpListenerError::Accept(error));
                        break;
                    }
                };
                let runtime = shared.clone();
                let connection_shutdown = shutdown.clone();
                let forwarding_context = FrontlineForwardContext::http(peer_addr.ip());

                connections.spawn(async move {
                    serve_http_connection(stream, runtime, connection_shutdown, forwarding_context)
                        .await;
                });
            }
        }
    }

    let drain_result = shared.drain.drain().await;
    shared.abort_websocket_tasks().await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(FrontlineHttpListenerError::Drain)
}

impl<RouteClient, Wake, Http01> Clone for SharedFrontlineHttpRuntime<RouteClient, Wake, Http01> {
    fn clone(&self) -> Self {
        Self {
            coordinator: self.coordinator.clone(),
            http01_resolver: self.http01_resolver.clone(),
            forwarder: self.forwarder.clone(),
            drain: self.drain.clone(),
            websocket_tasks: self.websocket_tasks.clone(),
        }
    }
}

async fn serve_tls_termination_listener_with_shared<RouteClient, Wake, Http01>(
    listener: TcpListener,
    shared: SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>,
    tls_adapter: FrontlineTlsAdapter,
    shutdown: Shutdown,
) -> Result<(), FrontlineListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer_addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(FrontlineListenerError::Accept {
                            kind: FrontlineListenerKind::TlsTermination,
                            source: error,
                        });
                        break;
                    }
                };
                let runtime = shared.clone();
                let tls_adapter = tls_adapter.clone();
                let connection_shutdown = shutdown.clone();
                let forwarding_context = FrontlineForwardContext::https(peer_addr.ip());

                connections.spawn(async move {
                    let terminated = match tls_adapter.terminate(stream).await {
                        Ok(terminated) => terminated,
                        Err(_error) => return,
                    };

                    serve_http_connection(
                        terminated.stream,
                        runtime,
                        connection_shutdown,
                        forwarding_context,
                    )
                    .await;
                });
            }
        }
    }

    let drain_result = shared.drain.drain().await;
    shared.abort_websocket_tasks().await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(FrontlineListenerError::Drain)
}

async fn serve_tls_passthrough_listener_with_shared<RouteClient, Wake, Http01>(
    listener: TcpListener,
    shared: SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>,
    tls_adapter: FrontlineTlsAdapter,
    shutdown: Shutdown,
) -> Result<(), FrontlineListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
{
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(FrontlineListenerError::Accept {
                            kind: FrontlineListenerKind::TlsPassthrough,
                            source: error,
                        });
                        break;
                    }
                };
                let runtime = shared.clone();
                let tls_adapter = tls_adapter.clone();

                connections.spawn(async move {
                    runtime
                        .handle_tls_passthrough_connection(stream, tls_adapter)
                        .await;
                });
            }
        }
    }

    let drain_result = shared.drain.drain().await;
    shared.abort_websocket_tasks().await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(FrontlineListenerError::Drain)
}

async fn serve_http_connection<RouteClient, Wake, Http01, IO>(
    stream: IO,
    runtime: SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>,
    shutdown: Shutdown,
    forwarding_context: FrontlineForwardContext,
) where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
    Http01: Http01ChallengeResolver + Send + 'static,
    Http01::Error: Send,
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let service = service_fn(move |request| {
        let runtime = runtime.clone();
        async move { Ok::<_, Infallible>(runtime.handle(request, forwarding_context).await) }
    });
    let builder = auto::Builder::new(TokioExecutor::new());
    let connection = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
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

impl<RouteClient, Wake, Http01> SharedFrontlineHttpRuntime<RouteClient, Wake, Http01>
where
    RouteClient: RouteSubscriptionClient + Send,
    RouteClient::Error: Send,
    Wake: WakeClient + Send,
    Wake::Error: Send,
    Http01: Http01ChallengeResolver + Send,
    Http01::Error: Send,
{
    fn from_runtime(runtime: FrontlineHttpRuntime<RouteClient, Wake, Http01>) -> Self {
        let (coordinator, http01_resolver, forwarder, drain) = runtime.into_parts();

        Self {
            coordinator: std::sync::Arc::new(Mutex::new(coordinator)),
            http01_resolver: std::sync::Arc::new(Mutex::new(http01_resolver)),
            forwarder,
            drain,
            websocket_tasks: std::sync::Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    async fn abort_websocket_tasks(&self) {
        let mut websocket_tasks = self.websocket_tasks.lock().await;
        websocket_tasks.abort_all();
        while websocket_tasks.join_next().await.is_some() {}
    }

    async fn handle(
        &self,
        request: http::Request<Incoming>,
        forwarding_context: FrontlineForwardContext,
    ) -> http::Response<crate::FrontlineRuntimeBody> {
        if is_http01_challenge_candidate_path(request.uri().path()) {
            let http01_response = {
                let mut resolver = self.http01_resolver.lock().await;
                resolve_http01_response(&mut *resolver, &request).await
            };
            match http01_response {
                Ok(Some(response)) => return response,
                Ok(None) => {}
                Err(error) => return crate::runtime::http01_intercept_error_response(error),
            }
        }

        if is_websocket_upgrade_candidate(&request) {
            return self.handle_websocket(request, forwarding_context).await;
        }

        let outcome = {
            let mut coordinator = self.coordinator.lock().await;
            resolve_http_route(&mut coordinator, &request, Instant::now()).await
        };

        match outcome {
            Ok(outcome) => {
                route_outcome_or_forward_response(
                    &self.forwarder,
                    outcome,
                    request,
                    Some(forwarding_context),
                )
                .await
            }
            Err(error) => crate::runtime::route_resolution_error_response(error),
        }
    }

    async fn handle_tls_passthrough_connection(
        &self,
        mut stream: tokio::net::TcpStream,
        tls_adapter: FrontlineTlsAdapter,
    ) {
        let client_hello = match tls_adapter.read_passthrough_client_hello(&mut stream).await {
            Ok(client_hello) => client_hello,
            Err(_error) => return,
        };
        let outcome = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator
                .route(
                    client_hello.identity().clone().into_identity(),
                    Instant::now(),
                )
                .await
        };
        let ready = match outcome {
            Ok(FrontlineRouteOutcome::Ready(ready)) => ready,
            Ok(_) | Err(_) => return,
        };
        let _permit = match self.drain.try_acquire() {
            Ok(permit) => permit,
            Err(_error) => return,
        };

        // TLS passthrough forwards encrypted bytes after ClientHello routing; HTTP
        // headers are opaque here and cannot be mutated.
        let _ = tls_adapter
            .passthrough_prefixed(&ready, stream, client_hello)
            .await;
    }

    async fn handle_websocket(
        &self,
        mut request: http::Request<Incoming>,
        forwarding_context: FrontlineForwardContext,
    ) -> http::Response<crate::FrontlineRuntimeBody> {
        let switching_protocols = match websocket_upgrade_response(&request, empty_body()) {
            Ok(response) => response,
            Err(_error) => return status_response(StatusCode::BAD_REQUEST),
        };

        let outcome = {
            let mut coordinator = self.coordinator.lock().await;
            resolve_http_route(&mut coordinator, &request, Instant::now()).await
        };

        match outcome {
            Ok(FrontlineRouteOutcome::Ready(ready)) => {
                let path_and_query = request
                    .uri()
                    .path_and_query()
                    .map(|value| value.as_str().to_owned())
                    .unwrap_or_else(|| "/".to_owned());
                let upstream_headers = forwarding_context.headers_for_request(&mut request);
                let upgraded = hyper::upgrade::on(&mut request);
                let forwarder = self.forwarder.clone();

                let mut websocket_tasks = self.websocket_tasks.lock().await;
                reap_completed_tasks(&mut websocket_tasks);
                websocket_tasks.spawn(async move {
                    let Ok(upgraded) = upgraded.await else {
                        return;
                    };
                    let _ = forwarder
                        .forward_accepted_websocket_with_headers(
                            &ready,
                            TokioIo::new(upgraded),
                            &path_and_query,
                            &upstream_headers,
                        )
                        .await;
                });

                switching_protocols
            }
            Ok(outcome) => route_outcome_response(outcome),
            Err(error) => crate::runtime::route_resolution_error_response(error),
        }
    }
}

fn reap_completed_tasks(tasks: &mut JoinSet<()>) {
    while tasks.try_join_next().is_some() {}
}

fn is_websocket_upgrade_candidate(request: &http::Request<Incoming>) -> bool {
    request.method() == Method::GET
        && header_contains_token(request.headers(), CONNECTION, "upgrade")
        && header_contains_token(request.headers(), UPGRADE, "websocket")
}

fn header_contains_token(
    headers: &http::HeaderMap,
    name: http::header::HeaderName,
    token: &str,
) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case(token))
        })
        .unwrap_or(false)
}

fn route_outcome_response(
    outcome: FrontlineRouteOutcome,
) -> http::Response<crate::FrontlineRuntimeBody> {
    match outcome {
        FrontlineRouteOutcome::Ready(_) => {
            unreachable!("ready route outcomes are upgraded before response mapping")
        }
        FrontlineRouteOutcome::Miss(_) => status_response(StatusCode::NOT_FOUND),
        FrontlineRouteOutcome::Waiting(_)
        | FrontlineRouteOutcome::Waking { .. }
        | FrontlineRouteOutcome::Unavailable(_)
        | FrontlineRouteOutcome::WakeFailed { .. }
        | FrontlineRouteOutcome::WakeUnavailable { .. }
        | FrontlineRouteOutcome::GenerationConflict { .. }
        | FrontlineRouteOutcome::RejectedWakeObservation(_) => {
            status_response(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

fn status_response(status: StatusCode) -> http::Response<crate::FrontlineRuntimeBody> {
    http::Response::builder()
        .status(status)
        .body(empty_body())
        .expect("status-only frontline listener response builds")
}

fn empty_body() -> crate::FrontlineRuntimeBody {
    Full::new(Bytes::new())
        .map_err(|error| match error {})
        .boxed_unsync()
}

impl fmt::Display for FrontlineHttpListenerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind { addr, source } => {
                write!(
                    f,
                    "failed to bind frontline HTTP listener on {addr}: {source}"
                )
            }
            Self::Accept(error) => write!(f, "failed to accept frontline HTTP connection: {error}"),
            Self::Drain(error) => write!(f, "frontline HTTP listener drain failed: {error}"),
        }
    }
}

impl From<FrontlineHttpListenerError> for FrontlineListenerError {
    fn from(error: FrontlineHttpListenerError) -> Self {
        match error {
            FrontlineHttpListenerError::Bind { addr, source } => Self::Bind {
                kind: FrontlineListenerKind::Http,
                addr,
                source,
            },
            FrontlineHttpListenerError::Accept(source) => Self::Accept {
                kind: FrontlineListenerKind::Http,
                source,
            },
            FrontlineHttpListenerError::Drain(error) => Self::Drain(error),
        }
    }
}

impl fmt::Display for FrontlineListenerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http => f.write_str("HTTP"),
            Self::TlsTermination => f.write_str("TLS termination"),
            Self::TlsPassthrough => f.write_str("TLS passthrough"),
        }
    }
}

impl fmt::Display for FrontlineListenerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind { kind, addr, source } => {
                write!(
                    f,
                    "failed to bind frontline {kind} listener on {addr}: {source}"
                )
            }
            Self::Accept { kind, source } => {
                write!(f, "failed to accept frontline {kind} connection: {source}")
            }
            Self::Drain(error) => write!(f, "frontline listener drain failed: {error}"),
            Self::Task(error) => write!(f, "frontline listener task failed: {error}"),
        }
    }
}

impl Error for FrontlineHttpListenerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind { source, .. } => Some(source),
            Self::Accept(error) => Some(error),
            Self::Drain(error) => Some(error),
        }
    }
}

impl Error for FrontlineListenerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind { source, .. } | Self::Accept { source, .. } => Some(source),
            Self::Drain(error) => Some(error),
            Self::Task(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;
