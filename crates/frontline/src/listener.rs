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
use tokio::{net::TcpListener, sync::Mutex, task::JoinSet};

use crate::{
    is_http01_challenge_candidate_path,
    runtime::{resolve_http01_response, resolve_http_route, route_outcome_or_forward_response},
    FrontlineForwarder, FrontlineHttpRuntime, FrontlineRouteCoordinator, FrontlineRouteOutcome,
    Http01ChallengeResolver, RouteSubscriptionClient, WakeClient,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontlineHttpListenerConfig {
    listen_addr: SocketAddr,
}

#[derive(Debug)]
pub enum FrontlineHttpListenerError {
    Bind { addr: SocketAddr, source: io::Error },
    Accept(io::Error),
    Drain(DrainError),
}

#[derive(Debug)]
struct SharedFrontlineHttpRuntime<RouteClient, Wake, Http01> {
    coordinator: std::sync::Arc<Mutex<FrontlineRouteCoordinator<RouteClient, Wake>>>,
    http01_resolver: std::sync::Arc<Mutex<Http01>>,
    forwarder: FrontlineForwarder,
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
    let (coordinator, http01_resolver, forwarder, drain) = runtime.into_parts();
    let shared = SharedFrontlineHttpRuntime {
        coordinator: std::sync::Arc::new(Mutex::new(coordinator)),
        http01_resolver: std::sync::Arc::new(Mutex::new(http01_resolver)),
        forwarder,
        websocket_tasks: std::sync::Arc::new(Mutex::new(JoinSet::new())),
    };
    let mut connections = JoinSet::new();
    let mut exit_error = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        exit_error = Some(FrontlineHttpListenerError::Accept(error));
                        break;
                    }
                };
                let runtime = shared.clone();
                let connection_shutdown = shutdown.clone();

                connections.spawn(async move {
                    let service = service_fn(move |request| {
                        let runtime = runtime.clone();
                        async move {
                            Ok::<_, Infallible>(runtime.handle(request).await)
                        }
                    });
                    let builder = auto::Builder::new(TokioExecutor::new());
                    let connection =
                        builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
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

    let drain_result = drain.drain().await;
    {
        let mut websocket_tasks = shared.websocket_tasks.lock().await;
        websocket_tasks.abort_all();
        while websocket_tasks.join_next().await.is_some() {}
    }
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
            websocket_tasks: self.websocket_tasks.clone(),
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
    async fn handle(
        &self,
        request: http::Request<Incoming>,
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
            return self.handle_websocket(request).await;
        }

        let outcome = {
            let mut coordinator = self.coordinator.lock().await;
            resolve_http_route(&mut coordinator, &request, Instant::now()).await
        };

        match outcome {
            Ok(outcome) => {
                route_outcome_or_forward_response(&self.forwarder, outcome, request).await
            }
            Err(error) => crate::runtime::route_resolution_error_response(error),
        }
    }

    async fn handle_websocket(
        &self,
        mut request: http::Request<Incoming>,
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
                let upgraded = hyper::upgrade::on(&mut request);
                let forwarder = self.forwarder.clone();

                let mut websocket_tasks = self.websocket_tasks.lock().await;
                reap_completed_tasks(&mut websocket_tasks);
                websocket_tasks.spawn(async move {
                    let Ok(upgraded) = upgraded.await else {
                        return;
                    };
                    let _ = forwarder
                        .forward_accepted_websocket(&ready, TokioIo::new(upgraded), &path_and_query)
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

impl Error for FrontlineHttpListenerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind { source, .. } => Some(source),
            Self::Accept(error) => Some(error),
            Self::Drain(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;
