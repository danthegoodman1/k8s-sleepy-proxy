use std::{collections::VecDeque, error::Error, fmt};

use control_plane::{
    api::pb::{
        self, operator_control_plane_client::OperatorControlPlaneClient,
        proxy_control_plane_client::ProxyControlPlaneClient,
    },
    Http01ChallengeKey, Http01ChallengeRecord, RouteIdentity,
};
use tokio::sync::mpsc;
use tonic::codegen::{tokio_stream::wrappers::ReceiverStream, Body};

use crate::{
    http01_challenge_key_to_proto, http01_challenge_record_from_proto,
    proxy_subscribe_input_to_proto, proxy_subscribe_response_from_proto,
    proxy_wake_response_from_proto, wake_instance_request_to_proto, Http01ChallengeResolveFuture,
    Http01ChallengeResolver, ProxyProtocolAdapterError, ProxySubscribeInput, RouteRequestId,
    RouteSubscriptionClient, RouteSubscriptionFuture, SubscribeControlPlaneOutput, SubscriptionId,
    WakeClient, WakeClientFuture, WakeInstanceRequest, WakeInstanceResponse,
};

const SUBSCRIBE_REQUEST_BUFFER: usize = 16;

#[derive(Debug)]
pub struct GrpcProxyControlPlaneClient<T> {
    client: ProxyControlPlaneClient<T>,
    subscription: Option<GrpcRouteSubscriptionSession>,
}

#[derive(Debug)]
pub struct GrpcOperatorHttp01Resolver<T> {
    client: OperatorControlPlaneClient<T>,
}

#[derive(Debug)]
struct GrpcRouteSubscriptionSession {
    requests: mpsc::Sender<pb::ProxySubscribeRequest>,
    responses: tonic::codec::Streaming<pb::ProxySubscribeResponse>,
    buffered_updates: VecDeque<SubscribeControlPlaneOutput>,
}

#[derive(Debug)]
pub enum GrpcProxyControlPlaneError {
    Status(tonic::Status),
    SubscribeRequestStreamClosed,
    SubscribeResponseStreamClosed,
    Protocol(ProxyProtocolAdapterError),
    UnexpectedRouteResponse { request_id: RouteRequestId },
}

#[derive(Debug)]
pub enum GrpcOperatorHttp01ResolverError {
    Status(tonic::Status),
    Protocol(ProxyProtocolAdapterError),
}

impl<T> GrpcProxyControlPlaneClient<T> {
    pub fn new(client: ProxyControlPlaneClient<T>) -> Self {
        Self {
            client,
            subscription: None,
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
        let response = {
            let session = self.ensure_subscription().await?;
            if let Some(update) = session.buffered_updates.pop_front() {
                return Ok(update);
            }
            session.responses.message().await
        };

        let response = match response {
            Ok(Some(response)) => response,
            Ok(None) => {
                self.subscription = None;
                return Err(GrpcProxyControlPlaneError::SubscribeResponseStreamClosed);
            }
            Err(status) => {
                self.subscription = None;
                return Err(GrpcProxyControlPlaneError::Status(status));
            }
        };
        let message = proxy_subscribe_response_from_proto(response)
            .map_err(GrpcProxyControlPlaneError::Protocol)?;
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
        let request = proxy_subscribe_input_to_proto(ProxySubscribeInput::SubscribeRoute {
            request_id: request_id.clone(),
            identity,
        });
        let send_result = {
            let session = self.ensure_subscription().await?;
            session.requests.send(request).await
        };
        if send_result.is_err() {
            self.subscription = None;
            return Err(GrpcProxyControlPlaneError::SubscribeRequestStreamClosed);
        }

        loop {
            let response = {
                let session = self
                    .subscription
                    .as_mut()
                    .expect("subscription exists after successful request send");
                session.responses.message().await
            };
            let response = match response {
                Ok(Some(response)) => response,
                Ok(None) => {
                    self.subscription = None;
                    return Err(GrpcProxyControlPlaneError::SubscribeResponseStreamClosed);
                }
                Err(status) => {
                    self.subscription = None;
                    return Err(GrpcProxyControlPlaneError::Status(status));
                }
            };
            let message = proxy_subscribe_response_from_proto(response)
                .map_err(GrpcProxyControlPlaneError::Protocol)?;

            match response_request_id(&message) {
                Some(actual) if actual == &request_id => return Ok(message),
                Some(actual) => {
                    return Err(GrpcProxyControlPlaneError::UnexpectedRouteResponse {
                        request_id: actual.clone(),
                    });
                }
                None => self
                    .subscription
                    .as_mut()
                    .expect("subscription exists while buffering pushed updates")
                    .buffered_updates
                    .push_back(message),
            }
        }
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
            self.subscription = None;
            return Err(GrpcProxyControlPlaneError::SubscribeRequestStreamClosed);
        }

        Ok(())
    }

    async fn ensure_subscription(
        &mut self,
    ) -> Result<&mut GrpcRouteSubscriptionSession, GrpcProxyControlPlaneError> {
        if self.subscription.is_none() {
            let (requests, request_stream) = mpsc::channel(SUBSCRIBE_REQUEST_BUFFER);
            let responses = self
                .client
                .subscribe(ReceiverStream::new(request_stream))
                .await
                .map_err(GrpcProxyControlPlaneError::Status)?
                .into_inner();

            self.subscription = Some(GrpcRouteSubscriptionSession {
                requests,
                responses,
                buffered_updates: VecDeque::new(),
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

#[cfg(test)]
mod tests;
