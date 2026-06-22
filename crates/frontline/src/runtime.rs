use std::{error::Error, time::Instant};

use bytes::Bytes;
use http::{header::HOST, Request, Response, StatusCode};
use http_body::Body;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use proxy_core::DrainTracker;

use crate::{
    FrontlineForwardError, FrontlineForwarder, FrontlineRouteCoordinator,
    FrontlineRouteCoordinatorError, FrontlineRouteOutcome, ReadyBackend, RequestIdentityError,
    RouteRequestIdentity, RouteSubscriptionClient, WakeClient,
};

type BoxError = Box<dyn Error + Send + Sync>;

pub type FrontlineRuntimeBody = UnsyncBoxBody<Bytes, BoxError>;

#[derive(Clone, Debug)]
pub struct FrontlineHttpRuntime<RouteClient, Wake> {
    coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
    forwarder: FrontlineForwarder,
    drain: DrainTracker,
}

impl<RouteClient, Wake> FrontlineHttpRuntime<RouteClient, Wake> {
    pub fn new(
        coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
        drain: DrainTracker,
    ) -> Self {
        Self {
            coordinator,
            forwarder: FrontlineForwarder::new(drain.clone()),
            drain,
        }
    }

    pub fn coordinator(&self) -> &FrontlineRouteCoordinator<RouteClient, Wake> {
        &self.coordinator
    }

    pub fn coordinator_mut(&mut self) -> &mut FrontlineRouteCoordinator<RouteClient, Wake> {
        &mut self.coordinator
    }

    pub fn forwarder(&self) -> &FrontlineForwarder {
        &self.forwarder
    }

    pub fn drain(&self) -> &DrainTracker {
        &self.drain
    }

    pub fn into_parts(
        self,
    ) -> (
        FrontlineRouteCoordinator<RouteClient, Wake>,
        FrontlineForwarder,
        DrainTracker,
    ) {
        (self.coordinator, self.forwarder, self.drain)
    }
}

impl<RouteClient, Wake> FrontlineHttpRuntime<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    pub async fn handle_http<B>(
        &mut self,
        request: Request<B>,
        now: Instant,
    ) -> Response<FrontlineRuntimeBody>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        let outcome = match resolve_http_route(&mut self.coordinator, &request, now).await {
            Ok(outcome) => outcome,
            Err(error) => return route_resolution_error_response(error),
        };

        route_outcome_or_forward_response(&self.forwarder, outcome, request).await
    }
}

#[derive(Debug)]
pub(crate) enum FrontlineHttpRouteError<RouteClientError, WakeClientError> {
    Identity(RequestIdentityError),
    Coordinator(FrontlineRouteCoordinatorError<RouteClientError, WakeClientError>),
}

pub(crate) async fn resolve_http_route<RouteClient, Wake, B>(
    coordinator: &mut FrontlineRouteCoordinator<RouteClient, Wake>,
    request: &Request<B>,
    now: Instant,
) -> Result<FrontlineRouteOutcome, FrontlineHttpRouteError<RouteClient::Error, Wake::Error>>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    let identity = http_request_identity(request).map_err(FrontlineHttpRouteError::Identity)?;

    coordinator
        .route(identity.into_identity(), now)
        .await
        .map_err(FrontlineHttpRouteError::Coordinator)
}

pub(crate) async fn route_outcome_or_forward_response<B>(
    forwarder: &FrontlineForwarder,
    outcome: FrontlineRouteOutcome,
    request: Request<B>,
) -> Response<FrontlineRuntimeBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<BoxError>,
{
    match outcome {
        FrontlineRouteOutcome::Ready(ready) => {
            forward_ready_response(forwarder, ready, request).await
        }
        outcome => route_outcome_response(outcome),
    }
}

fn http_request_identity<B>(
    request: &Request<B>,
) -> Result<RouteRequestIdentity, RequestIdentityError> {
    let host = request
        .headers()
        .get(HOST)
        .ok_or(RequestIdentityError::EmptyHost)?
        .to_str()
        .map_err(|_| RequestIdentityError::UnsupportedHostSyntax)?;
    let path = request.uri().path_and_query().map(|value| value.path());

    RouteRequestIdentity::http(host, path)
}

async fn forward_ready_response<B>(
    forwarder: &FrontlineForwarder,
    ready: ReadyBackend,
    request: Request<B>,
) -> Response<FrontlineRuntimeBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<BoxError>,
{
    match forwarder.forward_http(&ready, request).await {
        Ok(response) => response.map(box_runtime_body),
        Err(error) => forward_error_response(error),
    }
}

fn route_outcome_response(outcome: FrontlineRouteOutcome) -> Response<FrontlineRuntimeBody> {
    match outcome {
        FrontlineRouteOutcome::Ready(_) => {
            unreachable!("ready route outcomes are forwarded before response mapping")
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

pub(crate) fn route_resolution_error_response<RouteClientError, WakeClientError>(
    error: FrontlineHttpRouteError<RouteClientError, WakeClientError>,
) -> Response<FrontlineRuntimeBody> {
    match error {
        FrontlineHttpRouteError::Identity(error) => identity_error_response(error),
        FrontlineHttpRouteError::Coordinator(_error) => {
            status_response(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

fn forward_error_response(error: FrontlineForwardError) -> Response<FrontlineRuntimeBody> {
    let status = match error {
        FrontlineForwardError::Http(proxy_core::HttpProxyError::Drain(_)) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        FrontlineForwardError::Backend(_)
        | FrontlineForwardError::Http(
            proxy_core::HttpProxyError::RequestRewrite(_) | proxy_core::HttpProxyError::Client(_),
        )
        | FrontlineForwardError::WebSocket(_) => StatusCode::BAD_GATEWAY,
    };

    status_response(status)
}

fn identity_error_response(_error: RequestIdentityError) -> Response<FrontlineRuntimeBody> {
    status_response(StatusCode::BAD_REQUEST)
}

fn status_response(status: StatusCode) -> Response<FrontlineRuntimeBody> {
    Response::builder()
        .status(status)
        .body(empty_body())
        .expect("status-only frontline runtime response builds")
}

fn box_runtime_body<B>(body: B) -> FrontlineRuntimeBody
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(Into::into).boxed_unsync()
}

fn empty_body() -> FrontlineRuntimeBody {
    Full::new(Bytes::new())
        .map_err(|error| match error {})
        .boxed_unsync()
}

#[cfg(test)]
mod tests;
