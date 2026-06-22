use std::{convert::Infallible, error::Error, fmt, io, net::SocketAddr, time::Instant};

use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::{DrainError, Shutdown};
use tokio::{net::TcpListener, sync::Mutex, task::JoinSet};

use crate::{
    runtime::{resolve_http_route, route_outcome_or_forward_response},
    FrontlineForwarder, FrontlineHttpRuntime, FrontlineRouteCoordinator, RouteSubscriptionClient,
    WakeClient,
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
struct SharedFrontlineHttpRuntime<RouteClient, Wake> {
    coordinator: std::sync::Arc<Mutex<FrontlineRouteCoordinator<RouteClient, Wake>>>,
    forwarder: FrontlineForwarder,
}

impl FrontlineHttpListenerConfig {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self { listen_addr }
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

pub async fn serve_http<RouteClient, Wake>(
    config: FrontlineHttpListenerConfig,
    runtime: FrontlineHttpRuntime<RouteClient, Wake>,
    shutdown: Shutdown,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
{
    let listener = TcpListener::bind(config.listen_addr())
        .await
        .map_err(|source| FrontlineHttpListenerError::Bind {
            addr: config.listen_addr(),
            source,
        })?;

    serve_http_listener(listener, runtime, shutdown).await
}

pub async fn serve_http_listener<RouteClient, Wake>(
    listener: TcpListener,
    runtime: FrontlineHttpRuntime<RouteClient, Wake>,
    shutdown: Shutdown,
) -> Result<(), FrontlineHttpListenerError>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send,
{
    let (coordinator, forwarder, drain) = runtime.into_parts();
    let shared = SharedFrontlineHttpRuntime {
        coordinator: std::sync::Arc::new(Mutex::new(coordinator)),
        forwarder,
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

    let drain_result = drain.drain().await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}

    if let Some(error) = exit_error {
        return Err(error);
    }

    drain_result.map_err(FrontlineHttpListenerError::Drain)
}

impl<RouteClient, Wake> Clone for SharedFrontlineHttpRuntime<RouteClient, Wake> {
    fn clone(&self) -> Self {
        Self {
            coordinator: self.coordinator.clone(),
            forwarder: self.forwarder.clone(),
        }
    }
}

impl<RouteClient, Wake> SharedFrontlineHttpRuntime<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient + Send,
    RouteClient::Error: Send,
    Wake: WakeClient + Send,
    Wake::Error: Send,
{
    async fn handle(
        &self,
        request: http::Request<Incoming>,
    ) -> http::Response<crate::FrontlineRuntimeBody> {
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
