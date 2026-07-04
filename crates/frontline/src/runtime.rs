use std::{error::Error, time::Instant};

use bytes::Bytes;
use http::{header::HOST, Request, Response, StatusCode};
use http_body::Body;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use proxy_core::observability::{
    metrics::RUNTIME_HTTP01_RESULTS_TOTAL,
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder, EVENT_HTTP01,
    },
    Outcome,
};
use proxy_core::DrainTracker;

use crate::{
    intercept_http01_challenge, is_http01_challenge_candidate_path, FrontlineForwardContext,
    FrontlineForwardError, FrontlineForwarder, FrontlineRouteCoordinator,
    FrontlineRouteCoordinatorError, FrontlineRouteOutcome, Http01ChallengeResolver,
    Http01InterceptDecision, Http01InterceptError, NoopHttp01ChallengeResolver, ReadyBackend,
    RequestIdentityError, RouteRequestIdentity, RouteSubscriptionClient,
    SharedFrontlineRouteCoordinator, WakeClient,
};

type BoxError = Box<dyn Error + Send + Sync>;

pub type FrontlineRuntimeBody = UnsyncBoxBody<Bytes, BoxError>;

#[derive(Clone, Debug)]
pub struct FrontlineHttpRuntime<RouteClient, Wake, Http01 = NoopHttp01ChallengeResolver> {
    coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
    http01_resolver: Http01,
    forwarder: FrontlineForwarder,
    drain: DrainTracker,
    observability: ObservabilityRecorder,
}

impl<RouteClient, Wake> FrontlineHttpRuntime<RouteClient, Wake, NoopHttp01ChallengeResolver> {
    pub fn new(
        coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
        drain: DrainTracker,
    ) -> Self {
        Self::with_http01_resolver(coordinator, NoopHttp01ChallengeResolver, drain)
    }
}

impl<RouteClient, Wake, Http01> FrontlineHttpRuntime<RouteClient, Wake, Http01> {
    pub fn with_http01_resolver(
        coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
        http01_resolver: Http01,
        drain: DrainTracker,
    ) -> Self {
        Self {
            coordinator,
            http01_resolver,
            forwarder: FrontlineForwarder::new(drain.clone()),
            drain,
            observability: ObservabilityRecorder::default(),
        }
    }

    pub fn with_http01_resolver_and_observability(
        coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
        http01_resolver: Http01,
        drain: DrainTracker,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            coordinator,
            http01_resolver,
            forwarder: FrontlineForwarder::new(drain.clone()),
            drain,
            observability,
        }
    }

    pub fn coordinator(&self) -> &FrontlineRouteCoordinator<RouteClient, Wake> {
        &self.coordinator
    }

    pub fn coordinator_mut(&mut self) -> &mut FrontlineRouteCoordinator<RouteClient, Wake> {
        &mut self.coordinator
    }

    pub fn http01_resolver(&self) -> &Http01 {
        &self.http01_resolver
    }

    pub fn http01_resolver_mut(&mut self) -> &mut Http01 {
        &mut self.http01_resolver
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
        Http01,
        FrontlineForwarder,
        DrainTracker,
    ) {
        (
            self.coordinator,
            self.http01_resolver,
            self.forwarder,
            self.drain,
        )
    }
}

impl<RouteClient, Wake, Http01> FrontlineHttpRuntime<RouteClient, Wake, Http01>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
    Http01: Http01ChallengeResolver,
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
        self.handle_http_with_forwarding_context(request, now, None)
            .await
    }

    pub async fn handle_http_with_forwarding_context<B>(
        &mut self,
        request: Request<B>,
        now: Instant,
        forwarding_context: Option<FrontlineForwardContext>,
    ) -> Response<FrontlineRuntimeBody>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        if is_http01_challenge_candidate_path(request.uri().path()) {
            match resolve_http01_response(&mut self.http01_resolver, &request).await {
                Ok(Some(response)) => {
                    record_http01_response(&self.observability, response.status());
                    return response;
                }
                Ok(None) => {}
                Err(error) => {
                    record_http01_error(&self.observability, &error);
                    return http01_intercept_error_response(error);
                }
            }
        }

        let outcome = match resolve_http_route(&mut self.coordinator, &request, now).await {
            Ok(outcome) => outcome,
            Err(error) => return route_resolution_error_response(error),
        };

        route_outcome_or_forward_response(&self.forwarder, outcome, request, forwarding_context)
            .await
    }
}

fn record_http01_response(observability: &ObservabilityRecorder, status: StatusCode) {
    let outcome = if status == StatusCode::OK {
        Outcome::Success
    } else {
        Outcome::Miss
    };
    observability.record_metric(MetricObservation::new(
        RUNTIME_HTTP01_RESULTS_TOTAL,
        vec![outcome.metric_label()],
        1.0,
    ));
    observability.record_log(LifecycleLogEvent::new(
        EVENT_HTTP01,
        vec![LogField::new("http.status", status.as_u16())],
    ));
}

fn record_http01_error<E>(observability: &ObservabilityRecorder, error: &Http01InterceptError<E>) {
    let reason = match error {
        Http01InterceptError::MissingHost => "missing_host",
        Http01InterceptError::InvalidHostHeader => "invalid_host_header",
        Http01InterceptError::InvalidTokenSegment => "invalid_token_segment",
        Http01InterceptError::InvalidHost(_) => "invalid_host",
        Http01InterceptError::InvalidChallenge(_) => "invalid_challenge",
        Http01InterceptError::Resolve(_) => "resolve",
    };
    observability.record_metric(MetricObservation::new(
        RUNTIME_HTTP01_RESULTS_TOTAL,
        vec![Outcome::Error.metric_label()],
        1.0,
    ));
    observability.record_log(LifecycleLogEvent::new(
        EVENT_HTTP01,
        vec![LogField::error_reason(reason)],
    ));
}

pub(crate) async fn resolve_http01_response<Http01, B>(
    resolver: &mut Http01,
    request: &Request<B>,
) -> Result<Option<Response<FrontlineRuntimeBody>>, Http01InterceptError<Http01::Error>>
where
    Http01: Http01ChallengeResolver,
{
    match intercept_http01_challenge(request, |key| resolver.resolve_http01_challenge(key)).await? {
        Http01InterceptDecision::PassThrough => Ok(None),
        Http01InterceptDecision::Miss { .. } => Ok(Some(status_response(StatusCode::NOT_FOUND))),
        Http01InterceptDecision::Serve { response, .. } => {
            Ok(Some(response.into_http_response().map(full_runtime_body)))
        }
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

pub(crate) async fn resolve_http_route_shared<RouteClient, Wake, B>(
    coordinator: &SharedFrontlineRouteCoordinator<RouteClient, Wake>,
    request: &Request<B>,
    now: Instant,
) -> Result<FrontlineRouteOutcome, FrontlineHttpRouteError<RouteClient::Error, Wake::Error>>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send + 'static,
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
    forwarding_context: Option<FrontlineForwardContext>,
) -> Response<FrontlineRuntimeBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<BoxError>,
{
    match outcome {
        FrontlineRouteOutcome::Ready(ready) => {
            forward_ready_response(forwarder, ready, request, forwarding_context).await
        }
        outcome => route_outcome_response(outcome),
    }
}

fn http_request_identity<B>(
    request: &Request<B>,
) -> Result<RouteRequestIdentity, RequestIdentityError> {
    let host_header = request
        .headers()
        .get(HOST)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| RequestIdentityError::UnsupportedHostSyntax)
        })
        .transpose()?;
    let host = host_header
        .or_else(|| {
            request
                .uri()
                .authority()
                .map(|authority| authority.as_str())
        })
        .ok_or(RequestIdentityError::EmptyHost)?;
    let path = request.uri().path_and_query().map(|value| value.path());

    RouteRequestIdentity::http(host, path)
}

async fn forward_ready_response<B>(
    forwarder: &FrontlineForwarder,
    ready: ReadyBackend,
    request: Request<B>,
    forwarding_context: Option<FrontlineForwardContext>,
) -> Response<FrontlineRuntimeBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<BoxError>,
{
    let forwarded = match forwarding_context {
        Some(context) => {
            forwarder
                .forward_http_with_context(&ready, request, context)
                .await
        }
        None => forwarder.forward_http(&ready, request).await,
    };

    match forwarded {
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

pub(crate) fn http01_intercept_error_response<E>(
    error: Http01InterceptError<E>,
) -> Response<FrontlineRuntimeBody> {
    match error {
        Http01InterceptError::MissingHost
        | Http01InterceptError::InvalidHostHeader
        | Http01InterceptError::InvalidTokenSegment
        | Http01InterceptError::InvalidHost(_)
        | Http01InterceptError::InvalidChallenge(_) => status_response(StatusCode::BAD_REQUEST),
        Http01InterceptError::Resolve(_) => status_response(StatusCode::SERVICE_UNAVAILABLE),
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

fn full_runtime_body(body: Bytes) -> FrontlineRuntimeBody {
    Full::new(body)
        .map_err(|error| match error {})
        .boxed_unsync()
}

fn empty_body() -> FrontlineRuntimeBody {
    Full::new(Bytes::new())
        .map_err(|error| match error {})
        .boxed_unsync()
}

#[cfg(test)]
mod tests;
