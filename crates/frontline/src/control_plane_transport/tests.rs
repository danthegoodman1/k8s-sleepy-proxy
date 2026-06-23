use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use control_plane::{
    api::pb::{
        self,
        operator_control_plane_client::OperatorControlPlaneClient,
        operator_control_plane_server::{OperatorControlPlane, OperatorControlPlaneServer},
        proxy_control_plane_client::ProxyControlPlaneClient,
        proxy_control_plane_server::{ProxyControlPlane, ProxyControlPlaneServer},
    },
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, Http01ChallengeKey, InstanceId,
    InstanceState, PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
};
use tokio::sync::mpsc;
use tonic::{
    codegen::{http, tokio_stream::wrappers::ReceiverStream, Service},
    Request, Response, Status,
};

use super::{
    GrpcOperatorHttp01Resolver, GrpcOperatorHttp01ResolverError, GrpcProxyControlPlaneClient,
    GrpcProxyControlPlaneError,
};
use crate::{
    Http01ChallengeResolver, InvalidationReason, RouteRequestId, RouteSubscriptionClient,
    SubscribeControlPlaneOutput, SubscriptionId, WakeClient, WakeInstanceRequest,
    WakeInstanceResponse,
};

const HTTP01_EXPIRES_AT_UNIX_MILLIS: i64 = 2_000;

#[tokio::test]
async fn wake_instance_ready_response_maps_through_generated_client() {
    let service = FakeProxyControlPlane::default();
    service.set_wake_response(Ok(pb::ProxyWakeInstanceResponse {
        outcome: Some(pb::proxy_wake_instance_response::Outcome::Ready(
            pb::ProxyWakeReadyResult {
                instance_id: "instance-a".to_owned(),
                instance_generation: 7,
                backend_uri: "http://10.0.0.7:8080".to_owned(),
                backend_generation: 3,
            },
        )),
    }));
    let mut client = test_client(service.clone());

    let response = client
        .wake_instance(WakeInstanceRequest {
            instance_id: instance_id("instance-a"),
            expected_generation: Generation::new(7),
        })
        .await
        .expect("wake succeeds");

    assert_eq!(
        response,
        WakeInstanceResponse::AlreadyRunning {
            instance_id: instance_id("instance-a"),
            generation: Generation::new(7),
            backend: backend("http://10.0.0.7:8080"),
            backend_generation: Some(BackendGeneration::new(3)),
        }
    );
    assert_eq!(
        service.wake_requests(),
        vec![pb::ProxyWakeInstanceRequest {
            instance_id: "instance-a".to_owned(),
            expected_generation: 7,
            backend_generation: None,
        }]
    );
}

#[tokio::test]
async fn subscribe_route_resolved_response_maps_through_generated_client() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_resolved_response(
        "req-1", "sub-1",
    ))]));
    let mut client = test_client(service.clone());

    let response = client
        .subscribe_route(
            route_request_id("req-1"),
            http_identity("app.example.com", None),
        )
        .await
        .expect("subscribe route succeeds");

    assert_eq!(
        response,
        SubscribeControlPlaneOutput::RouteResolved {
            request_id: route_request_id("req-1"),
            subscription_id: subscription_id("sub-1"),
            matched_identity: http_identity("app.example.com", None),
            entry: route_entry(),
            cache_policy: cache_policy(10_000),
        }
    );
    assert_subscribe_route_request(
        &service.wait_for_subscribe_requests(1).await[0],
        "req-1",
        "app.example.com",
    );
}

#[tokio::test]
async fn subscribe_route_miss_response_maps_through_generated_client() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(route_miss_response(
        "req-miss",
    ))]));
    let mut client = test_client(service);

    let response = client
        .subscribe_route(
            route_request_id("req-miss"),
            http_identity("missing.example.com", None),
        )
        .await
        .expect("subscribe route miss succeeds");

    assert_eq!(
        response,
        SubscribeControlPlaneOutput::RouteMiss {
            request_id: route_request_id("req-miss"),
            request_identity: http_identity("missing.example.com", None),
            negative_cache_policy: cache_policy(5_000),
        }
    );
}

#[tokio::test]
async fn pushed_updates_before_route_response_are_buffered_for_next_update() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![
        Ok(route_updated_response("sub-update")),
        Ok(route_invalidated_response("sub-invalidated")),
        Ok(route_resolved_response("req-buffered", "sub-resolved")),
    ]));
    let mut client = test_client(service);

    let response = client
        .subscribe_route(
            route_request_id("req-buffered"),
            http_identity("app.example.com", None),
        )
        .await
        .expect("matching route response is returned");

    assert!(matches!(
        response,
        SubscribeControlPlaneOutput::RouteResolved { .. }
    ));
    assert_eq!(
        client.next_update().await.expect("first buffered update"),
        SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: subscription_id("sub-update"),
            matched_identity: http_identity("app.example.com", None),
            entry: route_entry(),
            cache_policy: cache_policy(15_000),
        }
    );
    assert_eq!(
        client.next_update().await.expect("second buffered update"),
        SubscribeControlPlaneOutput::RouteInvalidated {
            subscription_id: subscription_id("sub-invalidated"),
            reason: InvalidationReason::BackendChanged,
        }
    );
}

#[tokio::test]
async fn unsubscribe_sends_request_and_does_not_wait_for_ack() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(Vec::new()));
    let mut client = test_client(service.clone());

    client
        .unsubscribe(subscription_id("sub-opaque"))
        .await
        .expect("unsubscribe send succeeds");

    let requests = service.wait_for_subscribe_requests(1).await;
    match requests[0].input.as_ref().expect("input") {
        pb::proxy_subscribe_request::Input::Unsubscribe(request) => {
            assert_eq!(request.subscription_id, "sub-opaque");
        }
        pb::proxy_subscribe_request::Input::SubscribeRoute(_) => {
            panic!("expected unsubscribe request")
        }
    }
}

#[tokio::test]
async fn subscribe_status_error_is_surfaced() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(vec![Err(
        Status::unavailable("store unavailable"),
    )]));
    let mut client = test_client(service);

    let error = client
        .subscribe_route(
            route_request_id("req-status"),
            http_identity("app.example.com", None),
        )
        .await
        .expect_err("status should surface");

    assert!(matches!(
        error,
        GrpcProxyControlPlaneError::Status(status) if status.code() == tonic::Code::Unavailable
    ));
}

#[tokio::test]
async fn malformed_subscribe_response_is_protocol_error() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::respond(vec![Ok(
        pb::ProxySubscribeResponse { output: None },
    )]));
    let mut client = test_client(service);

    let error = client
        .subscribe_route(
            route_request_id("req-malformed"),
            http_identity("app.example.com", None),
        )
        .await
        .expect_err("malformed response should surface");

    assert!(matches!(error, GrpcProxyControlPlaneError::Protocol(_)));
}

#[tokio::test]
async fn closed_subscribe_response_stream_is_surfaced() {
    let service = FakeProxyControlPlane::default();
    service.push_subscribe_action(SubscribeAction::close_after(Vec::new()));
    let mut client = test_client(service);

    let error = client
        .subscribe_route(
            route_request_id("req-closed"),
            http_identity("app.example.com", None),
        )
        .await
        .expect_err("closed response stream should surface");

    assert!(matches!(
        error,
        GrpcProxyControlPlaneError::SubscribeResponseStreamClosed
    ));
}

#[tokio::test]
async fn closed_subscribe_request_stream_is_surfaced() {
    let service = FakeProxyControlPlane::default();
    let mut client = test_client(service);
    client
        .ensure_subscription()
        .await
        .expect("subscription starts");
    let (closed_requests, closed_receiver) = mpsc::channel(1);
    drop(closed_receiver);
    client.subscription.as_mut().expect("session").requests = closed_requests;

    let error = client
        .unsubscribe(subscription_id("sub-closed"))
        .await
        .expect_err("closed request stream should surface");

    assert!(matches!(
        error,
        GrpcProxyControlPlaneError::SubscribeRequestStreamClosed
    ));
}

#[tokio::test]
async fn http01_resolve_hit_sends_key_and_maps_challenge_record() {
    let service = FakeOperatorControlPlane::default();
    service.set_resolve_http01_response(Ok(pb::ResolveHttp01ChallengeResponse {
        challenge: Some(http01_challenge(
            "app.example.com",
            "token-a",
            "token-a.key",
        )),
    }));
    let mut resolver = test_http01_resolver(service.clone());

    let response = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("App.Example.COM.", "token-a").expect("valid key"),
        )
        .await
        .expect("HTTP-01 resolve succeeds")
        .expect("challenge resolves");

    assert_eq!(response.key().host().as_str(), "app.example.com");
    assert_eq!(response.key().token(), "token-a");
    assert_eq!(response.key_authorization(), "token-a.key");
    assert_eq!(
        service.resolve_http01_requests(),
        vec![pb::ResolveHttp01ChallengeRequest {
            key: Some(pb::Http01ChallengeKey {
                host: "app.example.com".to_owned(),
                token: "token-a".to_owned(),
            })
        }]
    );
}

#[tokio::test]
async fn http01_resolve_miss_maps_to_none() {
    let service = FakeOperatorControlPlane::default();
    service.set_resolve_http01_response(Ok(pb::ResolveHttp01ChallengeResponse { challenge: None }));
    let mut resolver = test_http01_resolver(service);

    let response = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("missing.example.com", "token-a").expect("valid key"),
        )
        .await
        .expect("HTTP-01 resolve succeeds");

    assert!(response.is_none());
}

#[tokio::test]
async fn http01_resolve_status_error_is_surfaced() {
    let service = FakeOperatorControlPlane::default();
    service.set_resolve_http01_response(Err(Status::unavailable("store unavailable")));
    let mut resolver = test_http01_resolver(service);

    let error = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("app.example.com", "token-a").expect("valid key"),
        )
        .await
        .expect_err("status should surface");

    assert!(matches!(
        error,
        GrpcOperatorHttp01ResolverError::Status(status)
            if status.code() == tonic::Code::Unavailable
    ));
}

#[tokio::test]
async fn http01_resolve_malformed_challenge_is_protocol_error() {
    let service = FakeOperatorControlPlane::default();
    service.set_resolve_http01_response(Ok(pb::ResolveHttp01ChallengeResponse {
        challenge: Some(pb::Http01Challenge {
            key: None,
            key_authorization: "token-a.key".to_owned(),
            expires_at_unix_millis: HTTP01_EXPIRES_AT_UNIX_MILLIS,
        }),
    }));
    let mut resolver = test_http01_resolver(service);

    let error = resolver
        .resolve_http01_challenge(
            Http01ChallengeKey::new("app.example.com", "token-a").expect("valid key"),
        )
        .await
        .expect_err("malformed challenge should surface");

    assert!(matches!(
        error,
        GrpcOperatorHttp01ResolverError::Protocol(_)
    ));
}

#[derive(Clone)]
struct InProcessService<S> {
    inner: S,
}

impl<S> InProcessService<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<http::Request<tonic::body::Body>> for InProcessService<S>
where
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        self.inner.call(request)
    }
}

#[derive(Clone, Default)]
struct FakeProxyControlPlane {
    state: Arc<Mutex<FakeProxyControlPlaneState>>,
}

#[derive(Clone, Default)]
struct FakeOperatorControlPlane {
    state: Arc<Mutex<FakeOperatorControlPlaneState>>,
}

#[derive(Default)]
struct FakeProxyControlPlaneState {
    wake_response: Option<Result<pb::ProxyWakeInstanceResponse, Status>>,
    wake_requests: Vec<pb::ProxyWakeInstanceRequest>,
    subscribe_actions: VecDeque<SubscribeAction>,
    subscribe_requests: Vec<pb::ProxySubscribeRequest>,
}

#[derive(Default)]
struct FakeOperatorControlPlaneState {
    resolve_http01_response: Option<Result<pb::ResolveHttp01ChallengeResponse, Status>>,
    resolve_http01_requests: Vec<pb::ResolveHttp01ChallengeRequest>,
}

struct SubscribeAction {
    responses: Vec<Result<pb::ProxySubscribeResponse, Status>>,
    close_after: bool,
}

impl SubscribeAction {
    fn respond(responses: Vec<Result<pb::ProxySubscribeResponse, Status>>) -> Self {
        Self {
            responses,
            close_after: false,
        }
    }

    fn close_after(responses: Vec<Result<pb::ProxySubscribeResponse, Status>>) -> Self {
        Self {
            responses,
            close_after: true,
        }
    }
}

#[tonic::async_trait]
impl ProxyControlPlane for FakeProxyControlPlane {
    type SubscribeStream = ReceiverStream<Result<pb::ProxySubscribeResponse, Status>>;

    async fn wake_instance(
        &self,
        request: Request<pb::ProxyWakeInstanceRequest>,
    ) -> Result<Response<pb::ProxyWakeInstanceResponse>, Status> {
        let response = {
            let mut state = self.state.lock().expect("fake state");
            state.wake_requests.push(request.into_inner());
            state.wake_response.take().unwrap_or_else(|| {
                Err(Status::failed_precondition(
                    "test did not configure wake response",
                ))
            })
        };

        response.map(Response::new)
    }

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<pb::ProxySubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut requests = request.into_inner();
        let state = Arc::clone(&self.state);
        let (responses, response_stream) = mpsc::channel(16);

        tokio::spawn(async move {
            loop {
                let request = match requests.message().await {
                    Ok(Some(request)) => request,
                    Ok(None) => return,
                    Err(status) => {
                        let _ = responses.send(Err(status)).await;
                        return;
                    }
                };
                let action = {
                    let mut state = state.lock().expect("fake state");
                    state.subscribe_requests.push(request);
                    state
                        .subscribe_actions
                        .pop_front()
                        .unwrap_or_else(|| SubscribeAction::respond(Vec::new()))
                };

                for response in action.responses {
                    if responses.send(response).await.is_err() {
                        return;
                    }
                }
                if action.close_after {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(response_stream)))
    }
}

impl FakeProxyControlPlane {
    fn set_wake_response(&self, response: Result<pb::ProxyWakeInstanceResponse, Status>) {
        self.state.lock().expect("fake state").wake_response = Some(response);
    }

    fn push_subscribe_action(&self, action: SubscribeAction) {
        self.state
            .lock()
            .expect("fake state")
            .subscribe_actions
            .push_back(action);
    }

    fn wake_requests(&self) -> Vec<pb::ProxyWakeInstanceRequest> {
        self.state.lock().expect("fake state").wake_requests.clone()
    }

    async fn wait_for_subscribe_requests(&self, count: usize) -> Vec<pb::ProxySubscribeRequest> {
        for _ in 0..100 {
            let requests = self
                .state
                .lock()
                .expect("fake state")
                .subscribe_requests
                .clone();
            if requests.len() >= count {
                return requests;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        panic!("timed out waiting for {count} subscribe requests");
    }
}

#[tonic::async_trait]
impl OperatorControlPlane for FakeOperatorControlPlane {
    async fn create_workload_class_version(
        &self,
        _request: Request<pb::CreateWorkloadClassVersionRequest>,
    ) -> Result<Response<pb::WorkloadClassVersion>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn get_workload_class_version(
        &self,
        _request: Request<pb::GetWorkloadClassVersionRequest>,
    ) -> Result<Response<pb::WorkloadClassVersion>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn create_instance(
        &self,
        _request: Request<pb::CreateInstanceRequest>,
    ) -> Result<Response<pb::Instance>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn get_instance(
        &self,
        _request: Request<pb::GetInstanceRequest>,
    ) -> Result<Response<pb::Instance>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn delete_instance(
        &self,
        _request: Request<pb::DeleteInstanceRequest>,
    ) -> Result<Response<pb::DeleteInstanceResponse>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn create_route_binding(
        &self,
        _request: Request<pb::CreateRouteBindingRequest>,
    ) -> Result<Response<pb::RouteBinding>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn get_route_binding(
        &self,
        _request: Request<pb::GetRouteBindingRequest>,
    ) -> Result<Response<pb::RouteBinding>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn delete_route_binding(
        &self,
        _request: Request<pb::DeleteRouteBindingRequest>,
    ) -> Result<Response<pb::DeleteRouteBindingResponse>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn put_http01_challenge(
        &self,
        _request: Request<pb::PutHttp01ChallengeRequest>,
    ) -> Result<Response<pb::Http01Challenge>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn resolve_http01_challenge(
        &self,
        request: Request<pb::ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<pb::ResolveHttp01ChallengeResponse>, Status> {
        let response = {
            let mut state = self.state.lock().expect("fake state");
            state.resolve_http01_requests.push(request.into_inner());
            state.resolve_http01_response.take().unwrap_or_else(|| {
                Err(Status::failed_precondition(
                    "test did not configure HTTP-01 resolve response",
                ))
            })
        };

        response.map(Response::new)
    }

    async fn delete_http01_challenge(
        &self,
        _request: Request<pb::DeleteHttp01ChallengeRequest>,
    ) -> Result<Response<pb::DeleteHttp01ChallengeResponse>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }

    async fn expire_http01_challenges(
        &self,
        _request: Request<pb::ExpireHttp01ChallengesRequest>,
    ) -> Result<Response<pb::ExpireHttp01ChallengesResponse>, Status> {
        Err(Status::unimplemented("unused fake method"))
    }
}

impl FakeOperatorControlPlane {
    fn set_resolve_http01_response(
        &self,
        response: Result<pb::ResolveHttp01ChallengeResponse, Status>,
    ) {
        self.state
            .lock()
            .expect("fake state")
            .resolve_http01_response = Some(response);
    }

    fn resolve_http01_requests(&self) -> Vec<pb::ResolveHttp01ChallengeRequest> {
        self.state
            .lock()
            .expect("fake state")
            .resolve_http01_requests
            .clone()
    }
}

fn test_client(
    service: FakeProxyControlPlane,
) -> GrpcProxyControlPlaneClient<InProcessService<ProxyControlPlaneServer<FakeProxyControlPlane>>> {
    let server = ProxyControlPlaneServer::new(service);
    GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::new(InProcessService::new(server)))
}

fn test_http01_resolver(
    service: FakeOperatorControlPlane,
) -> GrpcOperatorHttp01Resolver<
    InProcessService<OperatorControlPlaneServer<FakeOperatorControlPlane>>,
> {
    let server = OperatorControlPlaneServer::new(service);
    GrpcOperatorHttp01Resolver::new(OperatorControlPlaneClient::new(InProcessService::new(
        server,
    )))
}

fn route_resolved_response(request_id: &str, subscription_id: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteResolved(
            pb::ProxyRouteResolvedResponse {
                request_id: request_id.to_owned(),
                subscription_id: subscription_id.to_owned(),
                matched_identity: Some(pb_http_identity("app.example.com", None)),
                route: Some(pb_route_entry()),
                cache_policy: Some(pb::ProxyCachePolicy { ttl_millis: 10_000 }),
            },
        )),
    }
}

fn route_miss_response(request_id: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteMiss(
            pb::ProxyRouteMissResponse {
                request_id: request_id.to_owned(),
                request_identity: Some(pb_http_identity("missing.example.com", None)),
                negative_cache_policy: Some(pb::ProxyCachePolicy { ttl_millis: 5_000 }),
            },
        )),
    }
}

fn route_updated_response(subscription_id: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteUpdated(
            pb::ProxyRouteUpdatedResponse {
                subscription_id: subscription_id.to_owned(),
                matched_identity: Some(pb_http_identity("app.example.com", None)),
                route: Some(pb_route_entry()),
                cache_policy: Some(pb::ProxyCachePolicy { ttl_millis: 15_000 }),
            },
        )),
    }
}

fn route_invalidated_response(subscription_id: &str) -> pb::ProxySubscribeResponse {
    pb::ProxySubscribeResponse {
        output: Some(pb::proxy_subscribe_response::Output::RouteInvalidated(
            pb::ProxyRouteInvalidatedResponse {
                subscription_id: subscription_id.to_owned(),
                reason: pb::ProxyRouteInvalidationReason::BackendChanged as i32,
            },
        )),
    }
}

fn assert_subscribe_route_request(
    request: &pb::ProxySubscribeRequest,
    expected_request_id: &str,
    expected_host: &str,
) {
    match request.input.as_ref().expect("input") {
        pb::proxy_subscribe_request::Input::SubscribeRoute(request) => {
            assert_eq!(request.request_id, expected_request_id);
            assert_eq!(
                request.identity,
                Some(pb_http_identity(expected_host, None))
            );
        }
        pb::proxy_subscribe_request::Input::Unsubscribe(_) => {
            panic!("expected subscribe route request")
        }
    }
}

fn pb_http_identity(host: &str, path: Option<&str>) -> pb::RouteIdentity {
    pb::RouteIdentity {
        kind: Some(pb::route_identity::Kind::Http(pb::HttpRouteIdentity {
            host: Some(pb::RouteHost {
                kind: pb::RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
            path_prefix: path.map(str::to_owned),
        })),
    }
}

fn pb_route_entry() -> pb::ProxyRouteEntry {
    pb::ProxyRouteEntry {
        route_binding_id: "route-a".to_owned(),
        instance_id: "instance-a".to_owned(),
        instance_state: pb::InstanceState::Running as i32,
        instance_generation: 7,
        backend_uri: Some("http://10.0.0.7:8080".to_owned()),
        backend_generation: Some(3),
    }
}

fn http01_challenge(host: &str, token: &str, key_authorization: &str) -> pb::Http01Challenge {
    pb::Http01Challenge {
        key: Some(pb::Http01ChallengeKey {
            host: host.to_owned(),
            token: token.to_owned(),
        }),
        key_authorization: key_authorization.to_owned(),
        expires_at_unix_millis: HTTP01_EXPIRES_AT_UNIX_MILLIS,
    }
}

fn route_entry() -> RouteEntry {
    RouteEntry {
        route_binding_id: route_binding_id("route-a"),
        instance_id: instance_id("instance-a"),
        instance_state: InstanceState::Running,
        instance_generation: Generation::new(7),
        backend: Some(backend("http://10.0.0.7:8080")),
        backend_generation: Some(BackendGeneration::new(3)),
    }
}

fn http_identity(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("host"),
        path: path.map(|path| PathPrefix::new(path).expect("path")),
    }
}

fn cache_policy(ttl_millis: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_millis(ttl_millis))
}

fn route_request_id(value: &str) -> RouteRequestId {
    RouteRequestId::new(value).expect("request ID")
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription ID")
}

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("instance ID")
}

fn route_binding_id(value: &str) -> RouteBindingId {
    RouteBindingId::new(value).expect("route binding ID")
}

fn backend(value: &str) -> BackendEndpoint {
    BackendEndpoint::new(value).expect("backend")
}
