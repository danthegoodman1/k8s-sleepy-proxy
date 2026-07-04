use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    sync::Arc,
    time::Duration,
};

use control_plane::{
    api::pb::{
        self, operator_control_plane_client::OperatorControlPlaneClient,
        proxy_control_plane_client::ProxyControlPlaneClient,
    },
    Http01ChallengeKey, Http01ChallengeRecord, RouteIdentity,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tonic::codegen::{tokio_stream::wrappers::ReceiverStream, Body};

use crate::{
    http01_challenge_key_to_proto, http01_challenge_record_from_proto,
    proxy_subscribe_input_to_proto, proxy_subscribe_response_from_proto,
    proxy_wake_response_from_proto, wake_instance_request_to_proto, Http01ChallengeResolveFuture,
    Http01ChallengeResolver, ProxyProtocolAdapterError, ProxySubscribeInput, RouteRequestId,
    RouteSubscriptionClient, RouteSubscriptionEvent, RouteSubscriptionFuture,
    SubscribeControlPlaneOutput, SubscriptionId, WakeClient, WakeClientFuture, WakeInstanceRequest,
    WakeInstanceResponse,
};

const SUBSCRIBE_REQUEST_BUFFER: usize = 16;
// Pushed updates get enough room to drain bursts, while request-side sends stay
// tightly bounded because each request waits for its matching response.
const SUBSCRIBE_RESPONSE_BUFFER: usize = 256;
const DEFAULT_SUBSCRIBE_RECONNECT_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub struct GrpcProxyControlPlaneClient<T> {
    client: ProxyControlPlaneClient<T>,
    subscription: Option<GrpcRouteSubscriptionSession>,
    subscribe_reconnect_backoff: Duration,
    backoff_before_next_subscription: bool,
    pending_stream_closed_event: bool,
}

#[derive(Debug)]
pub struct GrpcOperatorHttp01Resolver<T> {
    client: OperatorControlPlaneClient<T>,
}

#[derive(Debug)]
struct GrpcRouteSubscriptionSession {
    requests: mpsc::Sender<pb::ProxySubscribeRequest>,
    responses: mpsc::Receiver<GrpcRouteSubscriptionEvent>,
    buffered_updates: VecDeque<SubscribeControlPlaneOutput>,
    pending_route_responses: PendingRouteResponses,
}

type PendingRouteResponses = Arc<
    Mutex<
        HashMap<
            RouteRequestId,
            oneshot::Sender<Result<SubscribeControlPlaneOutput, GrpcProxyControlPlaneError>>,
        >,
    >,
>;

#[derive(Debug)]
enum GrpcRouteSubscriptionEvent {
    Message(SubscribeControlPlaneOutput),
    ResponseStreamClosed,
    Status(tonic::Status),
    Protocol(ProxyProtocolAdapterError),
    UnexpectedRouteResponse { request_id: RouteRequestId },
}

#[derive(Clone, Debug)]
pub enum GrpcProxyControlPlaneError {
    Status(tonic::Status),
    SubscribeRequestStreamClosed,
    SubscribeResponseStreamClosed,
    Protocol(ProxyProtocolAdapterError),
    UnexpectedRouteResponse { request_id: RouteRequestId },
}

#[derive(Clone, Debug)]
pub enum GrpcOperatorHttp01ResolverError {
    Status(tonic::Status),
    Protocol(ProxyProtocolAdapterError),
}

impl<T> GrpcProxyControlPlaneClient<T> {
    pub fn new(client: ProxyControlPlaneClient<T>) -> Self {
        Self::with_subscribe_reconnect_backoff(client, DEFAULT_SUBSCRIBE_RECONNECT_BACKOFF)
    }

    pub fn with_subscribe_reconnect_backoff(
        client: ProxyControlPlaneClient<T>,
        subscribe_reconnect_backoff: Duration,
    ) -> Self {
        Self {
            client,
            subscription: None,
            subscribe_reconnect_backoff,
            backoff_before_next_subscription: false,
            pending_stream_closed_event: false,
        }
    }

    pub fn inner(&self) -> &ProxyControlPlaneClient<T> {
        &self.client
    }

    pub fn inner_mut(&mut self) -> &mut ProxyControlPlaneClient<T> {
        &mut self.client
    }

    pub fn into_inner(self) -> ProxyControlPlaneClient<T> {
        self.client
    }

    fn drop_failed_subscription(&mut self) {
        self.subscription = None;
        self.backoff_before_next_subscription = true;
        self.pending_stream_closed_event = true;
    }
}

impl<T> GrpcOperatorHttp01Resolver<T> {
    pub fn new(client: OperatorControlPlaneClient<T>) -> Self {
        Self { client }
    }

    pub fn inner(&self) -> &OperatorControlPlaneClient<T> {
        &self.client
    }

    pub fn inner_mut(&mut self) -> &mut OperatorControlPlaneClient<T> {
        &mut self.client
    }

    pub fn into_inner(self) -> OperatorControlPlaneClient<T> {
        self.client
    }
}

impl<T> GrpcProxyControlPlaneClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    pub async fn next_update(
        &mut self,
    ) -> Result<SubscribeControlPlaneOutput, GrpcProxyControlPlaneError> {
        self.ensure_subscription().await?;
        if let Some(update) = self
            .subscription
            .as_mut()
            .expect("subscription exists after ensure")
            .buffered_updates
            .pop_front()
        {
            return Ok(update);
        }

        let message = self.next_subscription_message().await?;
        if is_subscription_update(&message) {
            Ok(message)
        } else {
            Err(GrpcProxyControlPlaneError::UnexpectedRouteResponse {
                request_id: response_request_id(&message)
                    .expect("route responses always carry a request ID")
                    .clone(),
            })
        }
    }

    async fn wake_instance_via_transport(
        &mut self,
        request: WakeInstanceRequest,
    ) -> Result<WakeInstanceResponse, GrpcProxyControlPlaneError> {
        let response = self
            .client
            .wake_instance(wake_instance_request_to_proto(request))
            .await
            .map_err(GrpcProxyControlPlaneError::Status)?
            .into_inner();

        proxy_wake_response_from_proto(response).map_err(GrpcProxyControlPlaneError::Protocol)
    }

    async fn subscribe_route_via_transport(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> Result<SubscribeControlPlaneOutput, GrpcProxyControlPlaneError> {
        self.refresh_subscription_status()?;
        let request = proxy_subscribe_input_to_proto(ProxySubscribeInput::SubscribeRoute {
            request_id: request_id.clone(),
            identity,
        });
        for retry in 0..2 {
            let (response_tx, response_rx) = oneshot::channel();
            let send_result = {
                let session = self.ensure_subscription().await?;
                session
                    .pending_route_responses
                    .lock()
                    .await
                    .insert(request_id.clone(), response_tx);
                session.requests.send(request.clone()).await
            };
            if send_result.is_err() {
                if let Some(session) = self.subscription.as_mut() {
                    session
                        .pending_route_responses
                        .lock()
                        .await
                        .remove(&request_id);
                }
                self.drop_failed_subscription();
                if retry == 0 {
                    continue;
                }
                return Err(GrpcProxyControlPlaneError::SubscribeRequestStreamClosed);
            }

            return match response_rx.await {
                Ok(result) => result,
                Err(_closed) => {
                    self.drop_failed_subscription();
                    Err(GrpcProxyControlPlaneError::SubscribeResponseStreamClosed)
                }
            };
        }

        Err(GrpcProxyControlPlaneError::SubscribeRequestStreamClosed)
    }

    async fn unsubscribe_via_transport(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> Result<(), GrpcProxyControlPlaneError> {
        let request =
            proxy_subscribe_input_to_proto(ProxySubscribeInput::Unsubscribe { subscription_id });
        let send_result = {
            let session = self.ensure_subscription().await?;
            session.requests.send(request).await
        };
        if send_result.is_err() {
            self.drop_failed_subscription();
            return Err(GrpcProxyControlPlaneError::SubscribeRequestStreamClosed);
        }

        Ok(())
    }

    fn drain_subscription_events_via_transport(
        &mut self,
    ) -> Result<Vec<RouteSubscriptionEvent>, GrpcProxyControlPlaneError> {
        let Some(session) = self.subscription.as_mut() else {
            if self.pending_stream_closed_event {
                self.pending_stream_closed_event = false;
                return Ok(vec![RouteSubscriptionEvent::StreamClosed]);
            }
            return Ok(Vec::new());
        };

        let mut events = Vec::new();
        let mut drop_subscription = false;
        let mut protocol_error = None;
        while let Ok(event) = session.responses.try_recv() {
            match event {
                GrpcRouteSubscriptionEvent::Message(message) => {
                    if is_subscription_update(&message) {
                        events.push(RouteSubscriptionEvent::Update(Box::new(message)));
                    } else {
                        return Err(GrpcProxyControlPlaneError::UnexpectedRouteResponse {
                            request_id: response_request_id(&message)
                                .expect("route responses always carry a request ID")
                                .clone(),
                        });
                    }
                }
                GrpcRouteSubscriptionEvent::ResponseStreamClosed
                | GrpcRouteSubscriptionEvent::Status(_) => {
                    events.push(RouteSubscriptionEvent::StreamClosed);
                    drop_subscription = true;
                    break;
                }
                GrpcRouteSubscriptionEvent::Protocol(error) => {
                    protocol_error = Some(error);
                    drop_subscription = true;
                    break;
                }
                GrpcRouteSubscriptionEvent::UnexpectedRouteResponse { request_id } => {
                    self.drop_failed_subscription();
                    return Err(GrpcProxyControlPlaneError::UnexpectedRouteResponse { request_id });
                }
            }
        }

        if drop_subscription {
            self.drop_failed_subscription();
        }
        if let Some(error) = protocol_error {
            return Err(GrpcProxyControlPlaneError::Protocol(error));
        }

        Ok(events)
    }

    fn refresh_subscription_status(&mut self) -> Result<(), GrpcProxyControlPlaneError> {
        let Some(session) = self.subscription.as_mut() else {
            return Ok(());
        };

        let mut drop_subscription = false;
        let mut error = None;
        while let Ok(event) = session.responses.try_recv() {
            match event {
                GrpcRouteSubscriptionEvent::Message(message) => {
                    session.buffered_updates.push_back(message);
                }
                GrpcRouteSubscriptionEvent::ResponseStreamClosed
                | GrpcRouteSubscriptionEvent::Status(_) => {
                    drop_subscription = true;
                    break;
                }
                GrpcRouteSubscriptionEvent::Protocol(source) => {
                    error = Some(GrpcProxyControlPlaneError::Protocol(source));
                    drop_subscription = true;
                    break;
                }
                GrpcRouteSubscriptionEvent::UnexpectedRouteResponse { request_id } => {
                    error =
                        Some(GrpcProxyControlPlaneError::UnexpectedRouteResponse { request_id });
                    drop_subscription = true;
                    break;
                }
            }
        }

        if drop_subscription {
            self.drop_failed_subscription();
        }

        match error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn next_subscription_message(
        &mut self,
    ) -> Result<SubscribeControlPlaneOutput, GrpcProxyControlPlaneError> {
        let event = {
            let session = self
                .subscription
                .as_mut()
                .expect("subscription exists while waiting for response");
            session.responses.recv().await
        };

        match event {
            Some(GrpcRouteSubscriptionEvent::Message(message)) => Ok(message),
            Some(GrpcRouteSubscriptionEvent::ResponseStreamClosed) | None => {
                self.drop_failed_subscription();
                Err(GrpcProxyControlPlaneError::SubscribeResponseStreamClosed)
            }
            Some(GrpcRouteSubscriptionEvent::Status(status)) => {
                self.drop_failed_subscription();
                Err(GrpcProxyControlPlaneError::Status(status))
            }
            Some(GrpcRouteSubscriptionEvent::Protocol(error)) => {
                self.drop_failed_subscription();
                Err(GrpcProxyControlPlaneError::Protocol(error))
            }
            Some(GrpcRouteSubscriptionEvent::UnexpectedRouteResponse { request_id }) => {
                self.drop_failed_subscription();
                Err(GrpcProxyControlPlaneError::UnexpectedRouteResponse { request_id })
            }
        }
    }

    async fn ensure_subscription(
        &mut self,
    ) -> Result<&mut GrpcRouteSubscriptionSession, GrpcProxyControlPlaneError> {
        if self.subscription.is_none() {
            if self.backoff_before_next_subscription {
                tokio::time::sleep(self.subscribe_reconnect_backoff).await;
            }

            let (requests, request_stream) = mpsc::channel(SUBSCRIBE_REQUEST_BUFFER);
            let responses = self
                .client
                .subscribe(ReceiverStream::new(request_stream))
                .await
                .map_err(|status| {
                    self.backoff_before_next_subscription = true;
                    GrpcProxyControlPlaneError::Status(status)
                })?
                .into_inner();
            let (response_tx, response_rx) = mpsc::channel(SUBSCRIBE_RESPONSE_BUFFER);
            let pending_route_responses = Arc::new(Mutex::new(HashMap::new()));
            tokio::spawn(read_subscription_responses(
                responses,
                response_tx,
                pending_route_responses.clone(),
            ));

            self.backoff_before_next_subscription = false;
            self.pending_stream_closed_event = false;
            self.subscription = Some(GrpcRouteSubscriptionSession {
                requests,
                responses: response_rx,
                buffered_updates: VecDeque::new(),
                pending_route_responses,
            });
        }

        Ok(self
            .subscription
            .as_mut()
            .expect("subscription was just initialized"))
    }
}

impl<T> GrpcOperatorHttp01Resolver<T>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    async fn resolve_http01_challenge_via_transport(
        &mut self,
        key: Http01ChallengeKey,
    ) -> Result<Option<Http01ChallengeRecord>, GrpcOperatorHttp01ResolverError> {
        let response = self
            .client
            .resolve_http01_challenge(pb::ResolveHttp01ChallengeRequest {
                key: Some(http01_challenge_key_to_proto(key)),
            })
            .await
            .map_err(GrpcOperatorHttp01ResolverError::Status)?
            .into_inner();

        response
            .challenge
            .map(http01_challenge_record_from_proto)
            .transpose()
            .map_err(GrpcOperatorHttp01ResolverError::Protocol)
    }
}

impl<T> RouteSubscriptionClient for GrpcProxyControlPlaneClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type Error = GrpcProxyControlPlaneError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'_, SubscribeControlPlaneOutput, Self::Error> {
        Box::pin(async move {
            self.subscribe_route_via_transport(request_id, identity)
                .await
        })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'_, (), Self::Error> {
        Box::pin(async move { self.unsubscribe_via_transport(subscription_id).await })
    }

    fn drain_subscription_events(
        &mut self,
    ) -> RouteSubscriptionFuture<'_, Vec<RouteSubscriptionEvent>, Self::Error> {
        Box::pin(async move { self.drain_subscription_events_via_transport() })
    }
}

impl<T> WakeClient for GrpcProxyControlPlaneClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type Error = GrpcProxyControlPlaneError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'_, WakeInstanceResponse, Self::Error> {
        Box::pin(async move { self.wake_instance_via_transport(request).await })
    }
}

impl<T> Http01ChallengeResolver for GrpcOperatorHttp01Resolver<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    T::ResponseBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type Error = GrpcOperatorHttp01ResolverError;

    fn resolve_http01_challenge(
        &mut self,
        key: Http01ChallengeKey,
    ) -> Http01ChallengeResolveFuture<'_, Self::Error> {
        Box::pin(async move { self.resolve_http01_challenge_via_transport(key).await })
    }
}

impl fmt::Display for GrpcProxyControlPlaneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => write!(f, "proxy control-plane gRPC status: {status}"),
            Self::SubscribeRequestStreamClosed => {
                f.write_str("proxy subscribe request stream is closed")
            }
            Self::SubscribeResponseStreamClosed => {
                f.write_str("proxy subscribe response stream closed before a route response")
            }
            Self::Protocol(error) => write!(f, "proxy control-plane protocol error: {error}"),
            Self::UnexpectedRouteResponse { request_id } => write!(
                f,
                "unexpected route response for request {}",
                request_id.as_str()
            ),
        }
    }
}

impl Error for GrpcProxyControlPlaneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Status(status) => Some(status),
            Self::Protocol(error) => Some(error),
            Self::SubscribeRequestStreamClosed
            | Self::SubscribeResponseStreamClosed
            | Self::UnexpectedRouteResponse { .. } => None,
        }
    }
}

impl fmt::Display for GrpcOperatorHttp01ResolverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => write!(f, "operator HTTP-01 gRPC status: {status}"),
            Self::Protocol(error) => write!(f, "operator HTTP-01 protocol error: {error}"),
        }
    }
}

impl Error for GrpcOperatorHttp01ResolverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Status(status) => Some(status),
            Self::Protocol(error) => Some(error),
        }
    }
}

fn is_subscription_update(message: &SubscribeControlPlaneOutput) -> bool {
    matches!(
        message,
        SubscribeControlPlaneOutput::RouteUpdated { .. }
            | SubscribeControlPlaneOutput::RouteInvalidated { .. }
    )
}

fn response_request_id(message: &SubscribeControlPlaneOutput) -> Option<&RouteRequestId> {
    match message {
        SubscribeControlPlaneOutput::RouteResolved { request_id, .. }
        | SubscribeControlPlaneOutput::RouteMiss { request_id, .. } => Some(request_id),
        SubscribeControlPlaneOutput::RouteUpdated { .. }
        | SubscribeControlPlaneOutput::RouteInvalidated { .. } => None,
    }
}

async fn read_subscription_responses(
    mut responses: tonic::codec::Streaming<pb::ProxySubscribeResponse>,
    events: mpsc::Sender<GrpcRouteSubscriptionEvent>,
    pending_route_responses: PendingRouteResponses,
) {
    loop {
        match responses.message().await {
            Ok(Some(response)) => {
                let message = match proxy_subscribe_response_from_proto(response) {
                    Ok(message) => message,
                    Err(error) => {
                        fail_pending_route_responses(
                            &pending_route_responses,
                            GrpcProxyControlPlaneError::Protocol(error.clone()),
                        )
                        .await;
                        let _ = events
                            .send(GrpcRouteSubscriptionEvent::Protocol(error))
                            .await;
                        return;
                    }
                };

                if let Some(request_id) = response_request_id(&message).cloned() {
                    let response = pending_route_responses.lock().await.remove(&request_id);
                    if let Some(response) = response {
                        let _ = response.send(Ok(message));
                    } else {
                        fail_pending_route_responses(
                            &pending_route_responses,
                            GrpcProxyControlPlaneError::UnexpectedRouteResponse {
                                request_id: request_id.clone(),
                            },
                        )
                        .await;
                        let _ = events
                            .send(GrpcRouteSubscriptionEvent::UnexpectedRouteResponse {
                                request_id,
                            })
                            .await;
                        return;
                    }
                } else if events
                    .send(GrpcRouteSubscriptionEvent::Message(message))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Ok(None) => {
                fail_pending_route_responses(
                    &pending_route_responses,
                    GrpcProxyControlPlaneError::SubscribeResponseStreamClosed,
                )
                .await;
                let _ = events
                    .send(GrpcRouteSubscriptionEvent::ResponseStreamClosed)
                    .await;
                return;
            }
            Err(status) => {
                fail_pending_route_responses(
                    &pending_route_responses,
                    GrpcProxyControlPlaneError::Status(status.clone()),
                )
                .await;
                let _ = events
                    .send(GrpcRouteSubscriptionEvent::Status(status))
                    .await;
                return;
            }
        }
    }
}

async fn fail_pending_route_responses(
    pending_route_responses: &PendingRouteResponses,
    error: GrpcProxyControlPlaneError,
) {
    let pending = std::mem::take(&mut *pending_route_responses.lock().await);
    for response in pending.into_values() {
        let _ = response.send(Err(error.clone()));
    }
}

#[cfg(test)]
mod tests;
