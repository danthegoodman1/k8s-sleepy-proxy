use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use control_plane::api::{
    operator_grpc_server_builder,
    pb::{
        operator_control_plane_server::OperatorControlPlane,
        proxy_control_plane_client::ProxyControlPlaneClient,
        proxy_control_plane_server::ProxyControlPlane, proxy_subscribe_request,
        proxy_subscribe_response, proxy_wake_instance_response, HttpRouteIdentity, InstanceState,
        ProtocolRoute, ProxyCachePolicy, ProxyRouteEntry, ProxyRouteInvalidatedResponse,
        ProxyRouteInvalidationReason, ProxyRouteMissResponse, ProxyRouteResolvedResponse,
        ProxySubscribeRequest, ProxySubscribeResponse, ProxySubscribeRouteRequest,
        ProxyWakeInstanceRequest, ProxyWakeInstanceResponse, ProxyWakeUnavailableReason,
        RouteHost as ProtoRouteHost, RouteHostKind, RouteIdentity as ProtoRouteIdentity,
        SniRouteIdentity,
    },
    proxy_grpc_service_with_store, proxy_grpc_service_with_store_and_route_events,
    RouteSubscriptionBroker, StoreBackedOperatorApi, StoreBackedProxyApi,
    StoreBackedProxyGrpcService, OPERATOR_UNARY_METHODS, PROXY_SERVICE_NAME,
};
use control_plane::{
    AuthConfig, BackendEndpoint, BackendGeneration, CachePolicy, CallerRole, CompleteWakeResult,
    ControlPlaneAuth, ControlPlaneStore, CreateInstanceResult, Generation, InstanceId,
    InstanceRecord, InstanceState as DomainInstanceState, KubernetesClientError,
    KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer,
    KubernetesMaterializerClient, MaterializationId, MaterializationRecord, MaterializationState,
    MaterializationTarget, PathPrefix, RenderedObjectRef, RouteBindingId, RouteEntry, RouteHost,
    RouteIdentity, RouteResolution, StaticBearerTokens, StoreError, StoreFuture, StoreResult,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{
    header, HeaderMap, HeaderValue, Request, Response as HttpResponse, Version,
};
use tonic::codegen::tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::server::NamedService;
use tonic::Code;
use tower::{Service, ServiceExt};

fn auth_config() -> AuthConfig {
    AuthConfig::static_bearer_tokens(
        StaticBearerTokens::new("operator-token", "proxy-token", "sidecar-token")
            .expect("auth tokens are valid"),
    )
}

fn control_plane_auth() -> ControlPlaneAuth {
    ControlPlaneAuth::from_config(auth_config(), Default::default())
}

fn authenticated_proxy_service(
    store: Arc<dyn ControlPlaneStore>,
    client: FakeKubernetesClient,
) -> tonic::service::interceptor::InterceptedService<
    StoreBackedProxyGrpcService<FakeKubernetesClient>,
    control_plane::ControlPlaneAuthInterceptor,
> {
    tonic::service::interceptor::InterceptedService::new(
        proxy_grpc_service_with_store(store, KubernetesMaterializer::new(client), proxy_target()),
        control_plane_auth().interceptor(PROXY_SERVICE_NAME, CallerRole::Proxy),
    )
}

#[test]
fn generated_api_contains_proxy_wake_shape_without_operator_surface_change() {
    let request = ProxyWakeInstanceRequest {
        instance_id: "instance-1".to_owned(),
        expected_generation: 7,
        backend_generation: Some(11),
    };
    let response = ProxyWakeInstanceResponse {
        outcome: Some(proxy_wake_instance_response::Outcome::StillWaking(
            control_plane::api::pb::ProxyWakeStillWakingResult {
                instance_id: request.instance_id.clone(),
                instance_generation: request.expected_generation,
            },
        )),
    };

    assert_eq!(request.instance_id, "instance-1");
    assert_eq!(request.backend_generation, Some(11));
    assert!(matches!(
        response.outcome,
        Some(proxy_wake_instance_response::Outcome::StillWaking(_))
    ));
    assert_eq!(
        <StoreBackedProxyGrpcService<FakeKubernetesClient> as NamedService>::NAME,
        PROXY_SERVICE_NAME
    );
    assert!(!OPERATOR_UNARY_METHODS.contains(&"WakeInstance"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"Subscribe"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"ProxyControlPlane/WakeInstance"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"ProxyControlPlane/Subscribe"));

    let subscribe = ProxySubscribeRequest {
        input: Some(proxy_subscribe_request::Input::SubscribeRoute(
            ProxySubscribeRouteRequest {
                request_id: "request-1".to_owned(),
                identity: Some(proto_http_identity(
                    RouteHostKind::Exact,
                    "app.example.com",
                    Some("/"),
                )),
            },
        )),
    };
    assert!(matches!(
        subscribe.input,
        Some(proxy_subscribe_request::Input::SubscribeRoute(_))
    ));
}

#[tokio::test]
async fn proxy_wake_cold_instance_returns_ready_backend() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-cold",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    let service = proxy_api(store, client.clone());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-cold".to_owned(),
            expected_generation: 1,
            backend_generation: Some(44),
        }))
        .await
        .expect("cold wake succeeds")
        .into_inner();

    let ready = expect_ready(response);
    assert_eq!(ready.instance_id, "instance-cold");
    assert_eq!(ready.instance_generation, 3);
    assert_eq!(
        ready.backend_uri,
        "http://svc-acme-instance.apps.svc.cluster.local:80"
    );
    assert_eq!(ready.backend_generation, 44);
    assert_eq!(client.applied_objects_len(), 2);
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_proxy_wake_instance() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-transport",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();

    let response = proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(client.clone()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_wake_request(
        ProxyWakeInstanceRequest {
            instance_id: "instance-transport".to_owned(),
            expected_generation: 1,
            backend_generation: Some(55),
        },
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("native gRPC request should route through proxy wake service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("successful gRPC status is returned");

    assert_eq!(status, "0");

    let ready = expect_ready(decode_grpc_proxy_wake_response(
        collected.to_bytes().as_ref(),
    ));
    assert_eq!(ready.instance_id, "instance-transport");
    assert_eq!(ready.instance_generation, 3);
    assert_eq!(ready.backend_generation, 55);
    assert_eq!(client.applied_objects_len(), 2);
}

#[tokio::test]
async fn native_grpc_proxy_auth_rejects_wake_before_materialization_logic() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-auth-wake",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    let store_for_service: Arc<dyn ControlPlaneStore> = store;

    let missing_status = collect_grpc_status(
        authenticated_proxy_service(Arc::clone(&store_for_service), client.clone())
            .oneshot(grpc_proxy_wake_request(
                ProxyWakeInstanceRequest {
                    instance_id: "instance-auth-wake".to_owned(),
                    expected_generation: 1,
                    backend_generation: Some(55),
                },
                "application/grpc",
                Version::HTTP_2,
            ))
            .await
            .expect("missing auth returns gRPC response"),
    )
    .await;
    assert_eq!(missing_status, "16");
    assert_eq!(client.applied_objects_len(), 0);

    let wrong_role_status = collect_grpc_status(
        authenticated_proxy_service(store_for_service, client.clone())
            .oneshot(with_authorization(
                grpc_proxy_wake_request(
                    ProxyWakeInstanceRequest {
                        instance_id: "instance-auth-wake".to_owned(),
                        expected_generation: 1,
                        backend_generation: Some(55),
                    },
                    "application/grpc",
                    Version::HTTP_2,
                ),
                "Bearer operator-token",
            ))
            .await
            .expect("wrong role returns gRPC response"),
    )
    .await;
    assert_eq!(wrong_role_status, "7");
    assert_eq!(client.applied_objects_len(), 0);
}

#[tokio::test]
async fn native_grpc_proxy_auth_accepts_valid_wake_credentials() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-auth-valid",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    let store_for_service: Arc<dyn ControlPlaneStore> = store;

    let response = authenticated_proxy_service(store_for_service, client.clone())
        .oneshot(with_authorization(
            grpc_proxy_wake_request(
                ProxyWakeInstanceRequest {
                    instance_id: "instance-auth-valid".to_owned(),
                    expected_generation: 1,
                    backend_generation: Some(56),
                },
                "application/grpc",
                Version::HTTP_2,
            ),
            "Bearer proxy-token",
        ))
        .await
        .expect("valid proxy credentials dispatch to wake handler");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    assert_eq!(grpc_status(&headers, trailers.as_ref()), "0");
    let ready = expect_ready(decode_grpc_proxy_wake_response(
        collected.to_bytes().as_ref(),
    ));
    assert_eq!(ready.instance_id, "instance-auth-valid");
    assert_eq!(ready.backend_generation, 56);
    assert_eq!(client.applied_objects_len(), 2);
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_proxy_subscribe() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity("example.com", Some("/")),
        domain_route_entry(
            "route-transport",
            "instance-transport",
            DomainInstanceState::Running,
        ),
    );

    let response = proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![subscribe_route_request(
            "request-transport",
            proto_http_identity(RouteHostKind::Exact, "app.example.com", Some("/v1")),
        )],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("native gRPC request should route through proxy subscribe service");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert_eq!(status, "0");
    let resolved = expect_route_resolved(single_message(messages));
    assert_eq!(resolved.request_id, "request-transport");
    assert!(!resolved.subscription_id.is_empty());
    assert!(!resolved.subscription_id.contains("route-transport"));
    assert!(!resolved.subscription_id.contains("instance-transport"));
    assert!(!resolved.subscription_id.contains("example.com"));
    assert_eq!(
        resolved
            .matched_identity
            .as_ref()
            .and_then(|identity| identity.kind.as_ref())
            .and_then(|kind| match kind {
                control_plane::api::pb::route_identity::Kind::Http(http) => http.host.as_ref(),
                _ => None,
            })
            .map(|host| (host.kind, host.host.as_str())),
        Some((RouteHostKind::WildcardSuffix as i32, "example.com"))
    );
}

#[tokio::test]
async fn native_grpc_proxy_auth_rejects_subscribe_before_route_resolution() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity("auth.example.com", Some("/")),
        domain_route_entry("route-auth", "instance-auth", DomainInstanceState::Running),
    );
    let store_for_service: Arc<dyn ControlPlaneStore> = store.clone();
    let request = subscribe_route_request(
        "request-auth",
        proto_http_identity(RouteHostKind::Exact, "auth.example.com", Some("/")),
    );

    let missing_status = collect_grpc_status(
        authenticated_proxy_service(
            Arc::clone(&store_for_service),
            FakeKubernetesClient::default(),
        )
        .oneshot(grpc_proxy_subscribe_request(
            vec![request.clone()],
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("missing auth returns gRPC response"),
    )
    .await;
    assert_eq!(missing_status, "16");
    assert_eq!(store.resolve_route_requests(), 0);

    let wrong_role_status = collect_grpc_status(
        authenticated_proxy_service(
            Arc::clone(&store_for_service),
            FakeKubernetesClient::default(),
        )
        .oneshot(with_authorization(
            grpc_proxy_subscribe_request(
                vec![request.clone()],
                "application/grpc",
                Version::HTTP_2,
            ),
            "Bearer sidecar-token",
        ))
        .await
        .expect("wrong role returns gRPC response"),
    )
    .await;
    assert_eq!(wrong_role_status, "7");
    assert_eq!(store.resolve_route_requests(), 0);

    let response = authenticated_proxy_service(store_for_service, FakeKubernetesClient::default())
        .oneshot(with_authorization(
            grpc_proxy_subscribe_request(vec![request], "application/grpc", Version::HTTP_2),
            "Bearer proxy-token",
        ))
        .await
        .expect("valid proxy credentials dispatch subscribe");
    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert_eq!(status, "0");
    assert_eq!(store.resolve_route_requests(), 1);
    assert_eq!(
        expect_route_resolved(single_message(messages)).request_id,
        "request-auth"
    );
}

#[tokio::test]
async fn proxy_subscribe_route_resolved_returns_subscription_and_route_entry() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity("service.example.com", Some("/api")),
        domain_route_entry(
            "route-resolved",
            "instance-resolved",
            DomainInstanceState::Cold,
        ),
    );
    store.seed_ready_materialization(ready_materialization("instance-resolved", 7));

    let response = proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![subscribe_route_request(
            "request-resolved",
            proto_http_identity(RouteHostKind::Exact, "service.example.com", Some("/api/v1")),
        )],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("subscribe request should dispatch");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert_eq!(status, "0");
    let resolved = expect_route_resolved(single_message(messages));
    assert_eq!(resolved.request_id, "request-resolved");
    assert!(!resolved.subscription_id.is_empty());
    assert!(!resolved.subscription_id.contains("route-resolved"));
    assert!(!resolved.subscription_id.contains("instance-resolved"));
    assert!(!resolved.subscription_id.contains("service.example.com"));
    assert_eq!(
        resolved.matched_identity,
        Some(proto_http_identity(
            RouteHostKind::WildcardSuffix,
            "service.example.com",
            Some("/api")
        ))
    );
    assert_eq!(
        resolved.route,
        Some(ProxyRouteEntry {
            route_binding_id: "route-resolved".to_owned(),
            instance_id: "instance-resolved".to_owned(),
            instance_state: InstanceState::Cold as i32,
            instance_generation: 7,
            backend_uri: Some("http://svc-acme.apps.svc.cluster.local:80".to_owned()),
            backend_generation: Some(7),
        })
    );
    assert_eq!(
        resolved.cache_policy,
        Some(ProxyCachePolicy { ttl_millis: 10_000 })
    );
}

#[tokio::test]
async fn proxy_subscribe_publishes_backend_only_from_ready_materialization_for_target() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity("ready.example.com", None),
        domain_route_entry(
            "route-ready",
            "instance-ready",
            DomainInstanceState::Running,
        ),
    );
    store.seed_ready_materialization(ready_materialization("instance-ready", 7));

    let resolved = subscribe_resolved_route(
        store,
        "request-ready",
        proto_http_identity(RouteHostKind::Exact, "ready.example.com", None),
        proxy_target(),
    )
    .await;

    let route = resolved.route.expect("route entry is returned");
    assert_eq!(
        route.backend_uri,
        Some("http://svc-acme.apps.svc.cluster.local:80".to_owned())
    );
    assert_eq!(route.backend_generation, Some(7));
}

#[tokio::test]
async fn proxy_subscribe_withholds_backend_without_ready_materialization() {
    let cases = [
        ("absent", None),
        ("pending", Some(MaterializationState::Pending)),
        ("failed", Some(MaterializationState::Failed)),
        ("deleting", Some(MaterializationState::Deleting)),
        ("deleted", Some(MaterializationState::Deleted)),
    ];

    for (suffix, materialization_state) in cases {
        let instance_id = format!("instance-{suffix}");
        let store = Arc::new(FakeWakeStore::default());
        store.seed_route_resolved(
            domain_http_identity(&format!("{suffix}.example.com"), None),
            domain_route_entry(
                &format!("route-{suffix}"),
                &instance_id,
                DomainInstanceState::Running,
            ),
        );
        if let Some(state) = materialization_state {
            store.seed_materialization(materialization_with_state(&instance_id, 7, state));
        }

        let resolved = subscribe_resolved_route(
            store,
            &format!("request-{suffix}"),
            proto_http_identity(RouteHostKind::Exact, &format!("{suffix}.example.com"), None),
            proxy_target(),
        )
        .await;

        let route = resolved.route.expect("route entry is returned");
        assert_eq!(route.backend_uri, None, "{suffix} must not publish backend");
        assert_eq!(
            route.backend_generation, None,
            "{suffix} must not publish backend generation"
        );
    }
}

#[tokio::test]
async fn proxy_subscribe_withholds_backend_for_stale_generation_or_wrong_target() {
    let stale_store = Arc::new(FakeWakeStore::default());
    stale_store.seed_route_resolved(
        domain_http_identity("stale.example.com", None),
        domain_route_entry(
            "route-stale",
            "instance-stale",
            DomainInstanceState::Running,
        ),
    );
    stale_store.seed_ready_materialization(ready_materialization("instance-stale", 6));

    let stale = subscribe_resolved_route(
        stale_store,
        "request-stale",
        proto_http_identity(RouteHostKind::Exact, "stale.example.com", None),
        proxy_target(),
    )
    .await;
    let stale_route = stale.route.expect("route entry is returned");
    assert_eq!(stale_route.instance_generation, 7);
    assert_eq!(stale_route.backend_uri, None);
    assert_eq!(stale_route.backend_generation, None);

    let wrong_target_store = Arc::new(FakeWakeStore::default());
    wrong_target_store.seed_route_resolved(
        domain_http_identity("wrong-target.example.com", None),
        domain_route_entry(
            "route-wrong-target",
            "instance-wrong-target",
            DomainInstanceState::Running,
        ),
    );
    wrong_target_store.seed_materialization(materialization_with_state_and_target(
        "instance-wrong-target",
        7,
        MaterializationState::Ready,
        MaterializationTarget::new("cluster-other", "apps").expect("target is valid"),
    ));

    let wrong_target = subscribe_resolved_route(
        wrong_target_store,
        "request-wrong-target",
        proto_http_identity(RouteHostKind::Exact, "wrong-target.example.com", None),
        proxy_target(),
    )
    .await;
    let wrong_target_route = wrong_target.route.expect("route entry is returned");
    assert_eq!(wrong_target_route.backend_uri, None);
    assert_eq!(wrong_target_route.backend_generation, None);
}

#[tokio::test]
async fn proxy_subscribe_route_miss_returns_negative_cache_policy() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_miss(Duration::from_secs(5));
    let request_identity = proto_http_identity(RouteHostKind::Exact, "missing.example.com", None);

    let response = proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![subscribe_route_request(
            "request-miss",
            request_identity.clone(),
        )],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("subscribe request should dispatch");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert_eq!(status, "0");
    let miss = expect_route_miss(single_message(messages));
    assert_eq!(miss.request_id, "request-miss");
    assert_eq!(miss.request_identity, Some(request_identity));
    assert_eq!(
        miss.negative_cache_policy,
        Some(ProxyCachePolicy { ttl_millis: 5_000 })
    );
}

#[tokio::test]
async fn proxy_subscribe_unsubscribe_is_idempotent_and_unacknowledged() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity("unsubscribe.example.com", None),
        domain_route_entry(
            "route-unsubscribe",
            "instance-unsubscribe",
            DomainInstanceState::Running,
        ),
    );

    let response = proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![
            subscribe_route_request(
                "request-unsubscribe",
                proto_http_identity(RouteHostKind::Exact, "unsubscribe.example.com", None),
            ),
            unsubscribe_request("sub:1"),
            unsubscribe_request("sub:1"),
            unsubscribe_request("unknown-subscription"),
        ],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("subscribe request should dispatch");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert_eq!(status, "0");
    assert_eq!(messages.len(), 1, "unsubscribe does not produce an ack");
    let resolved = expect_route_resolved(single_message(messages));
    assert_eq!(resolved.request_id, "request-unsubscribe");
}

#[tokio::test]
async fn operator_route_binding_delete_invalidates_active_proxy_subscription() {
    let broker = RouteSubscriptionBroker::new();
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity("delete-route.example.com", None),
        domain_route_entry(
            "route-delete-active",
            "instance-delete-active",
            DomainInstanceState::Running,
        ),
    );
    let mut proxy = proxy_client_with_route_events(Arc::clone(&store), broker.clone());
    let operator = operator_api_with_route_events(store, broker);
    let (requests, request_stream) = tokio::sync::mpsc::channel(4);
    let mut responses = proxy
        .subscribe(ReceiverStream::new(request_stream))
        .await
        .expect("subscribe stream opens")
        .into_inner();

    requests
        .send(subscribe_route_request(
            "request-delete-active",
            proto_http_identity(RouteHostKind::Exact, "delete-route.example.com", None),
        ))
        .await
        .expect("subscribe request sends");
    let resolved = expect_route_resolved(next_subscribe_response(&mut responses).await);
    assert_eq!(
        resolved.route.expect("route returned").route_binding_id,
        "route-delete-active"
    );

    let deleted = operator
        .delete_route_binding(tonic::Request::new(
            control_plane::api::pb::DeleteRouteBindingRequest {
                route_binding_id: "route-delete-active".to_owned(),
            },
        ))
        .await
        .expect("operator delete route succeeds")
        .into_inner();
    assert!(deleted.deleted);

    let invalidated = expect_route_invalidated(next_subscribe_response(&mut responses).await);
    assert_eq!(invalidated.subscription_id, resolved.subscription_id);
    assert_eq!(
        invalidated.reason,
        ProxyRouteInvalidationReason::RouteRemoved as i32
    );
}

#[tokio::test]
async fn operator_route_binding_reassignment_invalidates_old_route_before_resubscribe() {
    let broker = RouteSubscriptionBroker::new();
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity("reassign-route.example.com", None),
        domain_route_entry(
            "route-reassign-old",
            "instance-reassign-old",
            DomainInstanceState::Running,
        ),
    );
    let mut proxy = proxy_client_with_route_events(Arc::clone(&store), broker.clone());
    let operator = operator_api_with_route_events(Arc::clone(&store), broker);
    let (requests, request_stream) = tokio::sync::mpsc::channel(4);
    let mut responses = proxy
        .subscribe(ReceiverStream::new(request_stream))
        .await
        .expect("subscribe stream opens")
        .into_inner();

    requests
        .send(subscribe_route_request(
            "request-reassign-old",
            proto_http_identity(RouteHostKind::Exact, "reassign-route.example.com", None),
        ))
        .await
        .expect("old subscribe request sends");
    let old = expect_route_resolved(next_subscribe_response(&mut responses).await);
    assert_eq!(
        old.route
            .as_ref()
            .expect("old route returned")
            .route_binding_id,
        "route-reassign-old"
    );

    let deleted = operator
        .delete_route_binding(tonic::Request::new(
            control_plane::api::pb::DeleteRouteBindingRequest {
                route_binding_id: "route-reassign-old".to_owned(),
            },
        ))
        .await
        .expect("old route delete succeeds")
        .into_inner();
    assert!(deleted.deleted);
    let invalidated = expect_route_invalidated(next_subscribe_response(&mut responses).await);
    assert_eq!(invalidated.subscription_id, old.subscription_id);
    assert_eq!(
        invalidated.reason,
        ProxyRouteInvalidationReason::RouteRemoved as i32
    );

    store.seed_route_resolved(
        domain_http_identity("reassign-route.example.com", None),
        domain_route_entry(
            "route-reassign-new",
            "instance-reassign-new",
            DomainInstanceState::Running,
        ),
    );
    operator
        .create_route_binding(tonic::Request::new(
            control_plane::api::pb::CreateRouteBindingRequest {
                idempotency_key: "create-reassigned-route".to_owned(),
                route_binding_id: "route-reassign-new".to_owned(),
                instance_id: "instance-reassign-new".to_owned(),
                identity: Some(proto_http_identity(
                    RouteHostKind::Exact,
                    "reassign-route.example.com",
                    None,
                )),
                protocol: ProtocolRoute::Http as i32,
            },
        ))
        .await
        .expect("new route create succeeds");

    requests
        .send(subscribe_route_request(
            "request-reassign-new",
            proto_http_identity(RouteHostKind::Exact, "reassign-route.example.com", None),
        ))
        .await
        .expect("new subscribe request sends");
    let new = expect_route_resolved(next_subscribe_response(&mut responses).await);
    let new_route = new.route.expect("new route returned");
    assert_eq!(new_route.route_binding_id, "route-reassign-new");
    assert_eq!(new_route.instance_id, "instance-reassign-new");
}

#[tokio::test]
async fn operator_exact_http_host_create_invalidates_cached_wildcard_subscription() {
    let broker = RouteSubscriptionBroker::new();
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity("example.com", None),
        domain_route_entry(
            "route-wildcard-http",
            "instance-wildcard-http",
            DomainInstanceState::Running,
        ),
    );
    let mut proxy = proxy_client_with_route_events(Arc::clone(&store), broker.clone());
    let operator = operator_api_with_route_events(store, broker);
    let (requests, request_stream) = tokio::sync::mpsc::channel(4);
    let mut responses = proxy
        .subscribe(ReceiverStream::new(request_stream))
        .await
        .expect("subscribe stream opens")
        .into_inner();

    requests
        .send(subscribe_route_request(
            "request-http-shadow-host",
            proto_http_identity(RouteHostKind::Exact, "app.example.com", None),
        ))
        .await
        .expect("subscribe request sends");
    let cached = expect_route_resolved(next_subscribe_response(&mut responses).await);
    assert_eq!(
        cached
            .route
            .as_ref()
            .expect("cached route returned")
            .route_binding_id,
        "route-wildcard-http"
    );

    operator
        .create_route_binding(tonic::Request::new(
            control_plane::api::pb::CreateRouteBindingRequest {
                idempotency_key: "create-http-shadow-host".to_owned(),
                route_binding_id: "route-exact-http".to_owned(),
                instance_id: "instance-exact-http".to_owned(),
                identity: Some(proto_http_identity(
                    RouteHostKind::Exact,
                    "app.example.com",
                    None,
                )),
                protocol: ProtocolRoute::Http as i32,
            },
        ))
        .await
        .expect("more specific route create succeeds");

    let invalidated = expect_route_invalidated(next_subscribe_response(&mut responses).await);
    assert_eq!(invalidated.subscription_id, cached.subscription_id);
    assert_eq!(
        invalidated.reason,
        ProxyRouteInvalidationReason::RouteChanged as i32
    );
}

#[tokio::test]
async fn operator_longer_http_path_create_invalidates_cached_shorter_path_subscription() {
    let broker = RouteSubscriptionBroker::new();
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_http_identity_exact("path.example.com", Some("/api")),
        domain_route_entry(
            "route-short-path",
            "instance-short-path",
            DomainInstanceState::Running,
        ),
    );
    let mut proxy = proxy_client_with_route_events(Arc::clone(&store), broker.clone());
    let operator = operator_api_with_route_events(store, broker);
    let (requests, request_stream) = tokio::sync::mpsc::channel(4);
    let mut responses = proxy
        .subscribe(ReceiverStream::new(request_stream))
        .await
        .expect("subscribe stream opens")
        .into_inner();

    requests
        .send(subscribe_route_request(
            "request-http-shadow-path",
            proto_http_identity(
                RouteHostKind::Exact,
                "path.example.com",
                Some("/api/v1/users"),
            ),
        ))
        .await
        .expect("subscribe request sends");
    let cached = expect_route_resolved(next_subscribe_response(&mut responses).await);
    assert_eq!(
        cached
            .route
            .as_ref()
            .expect("cached route returned")
            .route_binding_id,
        "route-short-path"
    );

    operator
        .create_route_binding(tonic::Request::new(
            control_plane::api::pb::CreateRouteBindingRequest {
                idempotency_key: "create-http-shadow-path".to_owned(),
                route_binding_id: "route-long-path".to_owned(),
                instance_id: "instance-long-path".to_owned(),
                identity: Some(proto_http_identity(
                    RouteHostKind::Exact,
                    "path.example.com",
                    Some("/api/v1"),
                )),
                protocol: ProtocolRoute::Http as i32,
            },
        ))
        .await
        .expect("more specific route create succeeds");

    let invalidated = expect_route_invalidated(next_subscribe_response(&mut responses).await);
    assert_eq!(invalidated.subscription_id, cached.subscription_id);
    assert_eq!(
        invalidated.reason,
        ProxyRouteInvalidationReason::RouteChanged as i32
    );
}

#[tokio::test]
async fn operator_exact_sni_create_invalidates_cached_wildcard_sni_subscription() {
    let broker = RouteSubscriptionBroker::new();
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_resolved(
        domain_sni_identity("example.com", RouteHostKind::WildcardSuffix),
        domain_route_entry(
            "route-wildcard-sni",
            "instance-wildcard-sni",
            DomainInstanceState::Running,
        ),
    );
    let mut proxy = proxy_client_with_route_events(Arc::clone(&store), broker.clone());
    let operator = operator_api_with_route_events(store, broker);
    let (requests, request_stream) = tokio::sync::mpsc::channel(4);
    let mut responses = proxy
        .subscribe(ReceiverStream::new(request_stream))
        .await
        .expect("subscribe stream opens")
        .into_inner();

    requests
        .send(subscribe_route_request(
            "request-sni-shadow-host",
            proto_sni_identity(RouteHostKind::Exact, "db.example.com"),
        ))
        .await
        .expect("subscribe request sends");
    let cached = expect_route_resolved(next_subscribe_response(&mut responses).await);
    assert_eq!(
        cached
            .route
            .as_ref()
            .expect("cached route returned")
            .route_binding_id,
        "route-wildcard-sni"
    );

    operator
        .create_route_binding(tonic::Request::new(
            control_plane::api::pb::CreateRouteBindingRequest {
                idempotency_key: "create-sni-shadow-host".to_owned(),
                route_binding_id: "route-exact-sni".to_owned(),
                instance_id: "instance-exact-sni".to_owned(),
                identity: Some(proto_sni_identity(RouteHostKind::Exact, "db.example.com")),
                protocol: ProtocolRoute::TlsSni as i32,
            },
        ))
        .await
        .expect("more specific SNI route create succeeds");

    let invalidated = expect_route_invalidated(next_subscribe_response(&mut responses).await);
    assert_eq!(invalidated.subscription_id, cached.subscription_id);
    assert_eq!(
        invalidated.reason,
        ProxyRouteInvalidationReason::RouteChanged as i32
    );
}

#[tokio::test]
async fn proxy_subscribe_invalid_request_returns_invalid_argument() {
    let response = proxy_grpc_service_with_store(
        Arc::new(FakeWakeStore::default()),
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![subscribe_route_request(
            "",
            proto_http_identity(RouteHostKind::Exact, "invalid.example.com", None),
        )],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("subscribe request should dispatch");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert!(messages.is_empty());
    assert_eq!(status, "3");
}

#[tokio::test]
async fn proxy_subscribe_invalid_route_identity_returns_invalid_argument() {
    let response = proxy_grpc_service_with_store(
        Arc::new(FakeWakeStore::default()),
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![subscribe_route_request(
            "request-invalid-identity",
            proto_http_identity(RouteHostKind::Unspecified, "invalid.example.com", None),
        )],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("subscribe request should dispatch");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert!(messages.is_empty());
    assert_eq!(status, "3");
}

#[tokio::test]
async fn proxy_subscribe_store_unavailable_terminates_stream() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_route_error_unavailable("store is offline");

    let response = proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![subscribe_route_request(
            "request-store-error",
            proto_http_identity(RouteHostKind::Exact, "offline.example.com", None),
        )],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("subscribe request should dispatch");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert!(messages.is_empty());
    assert_eq!(status, "14");
}

#[tokio::test]
async fn proxy_subscribe_store_internal_error_terminates_stream() {
    let response = proxy_grpc_service_with_store(
        Arc::new(FakeWakeStore::default()),
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![subscribe_route_request(
            "request-store-internal-error",
            proto_http_identity(RouteHostKind::Exact, "internal.example.com", None),
        )],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("subscribe request should dispatch");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert!(messages.is_empty());
    assert_eq!(status, "13");
}

#[tokio::test]
async fn proxy_wake_already_running_returns_ready_backend_without_apply() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-running",
        DomainInstanceState::Running,
        5,
    ));
    store.seed_ready_materialization(ready_materialization("instance-running", 5));
    let client = FakeKubernetesClient::default();
    let service = proxy_api(store, client.clone());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-running".to_owned(),
            expected_generation: 5,
            backend_generation: None,
        }))
        .await
        .expect("already-running wake succeeds")
        .into_inner();

    let ready = expect_ready(response);
    assert_eq!(ready.instance_id, "instance-running");
    assert_eq!(ready.instance_generation, 5);
    assert_eq!(
        ready.backend_uri,
        "http://svc-acme.apps.svc.cluster.local:80"
    );
    assert_eq!(ready.backend_generation, 5);
    assert_eq!(client.applied_objects_len(), 0);
}

#[tokio::test]
async fn proxy_wake_after_restart_resumes_waking_generation() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-waking",
        DomainInstanceState::Waking,
        6,
    ));
    store.seed_workload_class(domain_workload_class());
    let client = FakeKubernetesClient::default();
    let service = proxy_api(store, client.clone());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-waking".to_owned(),
            expected_generation: 6,
            backend_generation: None,
        }))
        .await
        .expect("already-waking wake succeeds")
        .into_inner();

    let ready = expect_ready(response);
    assert_eq!(ready.instance_id, "instance-waking");
    assert_eq!(ready.instance_generation, 7);
    assert_eq!(
        ready.backend_uri,
        "http://svc-acme-instance.apps.svc.cluster.local:80"
    );
    assert_eq!(ready.backend_generation, 6);
    assert_eq!(client.applied_objects_len(), 2);
}

#[tokio::test]
async fn proxy_wake_generation_conflict_is_structured_response() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-conflict",
        DomainInstanceState::Cold,
        9,
    ));
    let service = proxy_api(store, FakeKubernetesClient::default());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-conflict".to_owned(),
            expected_generation: 8,
            backend_generation: None,
        }))
        .await
        .expect("generation conflict is a domain response")
        .into_inner();

    let Some(proxy_wake_instance_response::Outcome::GenerationConflict(conflict)) =
        response.outcome
    else {
        panic!("expected generation-conflict response");
    };
    assert_eq!(conflict.instance_id, "instance-conflict");
    assert_eq!(conflict.expected_generation, 8);
    assert_eq!(conflict.actual_generation, 9);
}

#[tokio::test]
async fn proxy_wake_deleting_instance_returns_unavailable() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-deleting",
        DomainInstanceState::Deleting,
        4,
    ));
    let service = proxy_api(store, FakeKubernetesClient::default());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-deleting".to_owned(),
            expected_generation: 4,
            backend_generation: None,
        }))
        .await
        .expect("deleting instance is a domain response")
        .into_inner();

    let Some(proxy_wake_instance_response::Outcome::Unavailable(unavailable)) = response.outcome
    else {
        panic!("expected unavailable response");
    };
    assert_eq!(unavailable.instance_id, "instance-deleting");
    assert_eq!(unavailable.instance_generation, 4);
    assert_eq!(
        unavailable.reason,
        ProxyWakeUnavailableReason::Deleting as i32
    );
}

#[tokio::test]
async fn proxy_wake_invalid_instance_id_returns_invalid_argument() {
    let service = proxy_api(
        Arc::new(FakeWakeStore::default()),
        FakeKubernetesClient::default(),
    );

    let error = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "".to_owned(),
            expected_generation: 1,
            backend_generation: None,
        }))
        .await
        .expect_err("invalid instance ID is rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn proxy_wake_render_failure_returns_transport_error() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-render-failure",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let service = proxy_api_with_target(
        store,
        FakeKubernetesClient::default(),
        MaterializationTarget::new("cluster-a", "Bad_Namespace").expect("target is non-empty"),
    );

    let error = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-render-failure".to_owned(),
            expected_generation: 1,
            backend_generation: None,
        }))
        .await
        .expect_err("render failure is a transport error");

    assert_eq!(error.code(), Code::Internal);
    assert!(error.message().contains("manifest render failed"));
}

#[tokio::test]
async fn proxy_wake_materializer_failure_returns_transport_error() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-materializer-failure",
        DomainInstanceState::Cold,
        1,
    ));
    store.seed_workload_class(domain_workload_class());
    let service = proxy_api(store, FakeKubernetesClient::failing_readiness());

    let error = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-materializer-failure".to_owned(),
            expected_generation: 1,
            backend_generation: None,
        }))
        .await
        .expect_err("materializer failure is a transport error");

    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("materialization failed"));
}

#[test]
fn native_grpc_server_can_be_constructed_with_store_backed_proxy_service() {
    let _router = operator_grpc_server_builder().add_service(proxy_grpc_service_with_store(
        Arc::new(FakeWakeStore::default()),
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
    ));

    assert_eq!(
        <StoreBackedProxyGrpcService<FakeKubernetesClient> as NamedService>::NAME,
        PROXY_SERVICE_NAME
    );
}

fn grpc_proxy_wake_request(
    request: ProxyWakeInstanceRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_unary_request(
        request,
        "/sleepypods.controlplane.v1.ProxyControlPlane/WakeInstance",
        content_type,
        version,
    )
}

fn grpc_proxy_subscribe_request(
    requests: Vec<ProxySubscribeRequest>,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_stream_request(
        requests,
        "/sleepypods.controlplane.v1.ProxyControlPlane/Subscribe",
        content_type,
        version,
    )
}

fn grpc_unary_request<M: Message>(
    request: M,
    uri: &'static str,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_stream_request(vec![request], uri, content_type, version)
}

fn grpc_stream_request<M: Message>(
    requests: Vec<M>,
    uri: &'static str,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    let mut body = BytesMut::new();
    for request in requests {
        encode_grpc_message(request, &mut body);
    }

    Request::builder()
        .version(version)
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::new(Full::new(body.freeze())))
        .expect("request builds")
}

fn with_authorization(mut request: Request<Body>, value: &'static str) -> Request<Body> {
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, HeaderValue::from_static(value));
    request
}

fn encode_grpc_message<M: Message>(request: M, body: &mut BytesMut) {
    let mut message = BytesMut::new();
    request.encode(&mut message).expect("request encodes");

    body.put_u8(0);
    body.put_u32(message.len() as u32);
    body.extend_from_slice(&message);
}

fn decode_grpc_proxy_wake_response(bytes: &[u8]) -> ProxyWakeInstanceResponse {
    decode_grpc_message(bytes)
}

async fn collect_grpc_proxy_subscribe_response<B>(
    response: HttpResponse<B>,
) -> (Vec<ProxySubscribeResponse>, String)
where
    B: tonic::codegen::Body<Data = Bytes>,
    B::Error: std::fmt::Debug,
{
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("gRPC status is returned")
        .to_str()
        .expect("gRPC status is valid")
        .to_owned();

    (decode_grpc_messages(collected.to_bytes().as_ref()), status)
}

async fn collect_grpc_status<B>(response: HttpResponse<B>) -> String
where
    B: tonic::codegen::Body<Data = Bytes>,
    B::Error: std::fmt::Debug,
{
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let trailers = collected.trailers().cloned();
    grpc_status(&headers, trailers.as_ref())
}

fn grpc_status(headers: &HeaderMap, trailers: Option<&HeaderMap>) -> String {
    trailers
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| headers.get("grpc-status"))
        .expect("gRPC status is returned")
        .to_str()
        .expect("gRPC status is valid")
        .to_owned()
}

fn decode_grpc_message<M: Message + Default>(bytes: &[u8]) -> M {
    assert_eq!(bytes.first(), Some(&0), "gRPC message is uncompressed");
    let length = u32::from_be_bytes(
        bytes[1..5]
            .try_into()
            .expect("gRPC response frame has a length prefix"),
    ) as usize;
    M::decode(&bytes[5..5 + length]).expect("gRPC response decodes")
}

fn decode_grpc_messages<M: Message + Default>(bytes: &[u8]) -> Vec<M> {
    let mut messages = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        assert_eq!(bytes.get(offset), Some(&0), "gRPC message is uncompressed");
        let length = u32::from_be_bytes(
            bytes[offset + 1..offset + 5]
                .try_into()
                .expect("gRPC response frame has a length prefix"),
        ) as usize;
        offset += 5;
        messages.push(M::decode(&bytes[offset..offset + length]).expect("gRPC response decodes"));
        offset += length;
    }

    messages
}

fn single_message(messages: Vec<ProxySubscribeResponse>) -> ProxySubscribeResponse {
    assert_eq!(messages.len(), 1, "expected exactly one subscribe response");
    messages
        .into_iter()
        .next()
        .expect("one response is present")
}

fn subscribe_route_request(
    request_id: &str,
    identity: ProtoRouteIdentity,
) -> ProxySubscribeRequest {
    ProxySubscribeRequest {
        input: Some(proxy_subscribe_request::Input::SubscribeRoute(
            ProxySubscribeRouteRequest {
                request_id: request_id.to_owned(),
                identity: Some(identity),
            },
        )),
    }
}

fn unsubscribe_request(subscription_id: &str) -> ProxySubscribeRequest {
    ProxySubscribeRequest {
        input: Some(proxy_subscribe_request::Input::Unsubscribe(
            control_plane::api::pb::ProxyUnsubscribeRequest {
                subscription_id: subscription_id.to_owned(),
            },
        )),
    }
}

fn expect_route_resolved(response: ProxySubscribeResponse) -> ProxyRouteResolvedResponse {
    let Some(proxy_subscribe_response::Output::RouteResolved(resolved)) = response.output else {
        panic!("expected route resolved response");
    };
    resolved
}

fn expect_route_miss(response: ProxySubscribeResponse) -> ProxyRouteMissResponse {
    let Some(proxy_subscribe_response::Output::RouteMiss(miss)) = response.output else {
        panic!("expected route miss response");
    };
    miss
}

fn expect_route_invalidated(response: ProxySubscribeResponse) -> ProxyRouteInvalidatedResponse {
    let Some(proxy_subscribe_response::Output::RouteInvalidated(invalidated)) = response.output
    else {
        panic!("expected route invalidated response");
    };
    invalidated
}

async fn next_subscribe_response<S>(responses: &mut S) -> ProxySubscribeResponse
where
    S: tonic::codegen::tokio_stream::Stream<Item = Result<ProxySubscribeResponse, tonic::Status>>
        + Unpin,
{
    tokio::time::timeout(Duration::from_secs(1), responses.next())
        .await
        .expect("subscribe response arrives")
        .expect("subscribe stream remains open")
        .expect("subscribe response succeeds")
}

fn proto_http_identity(
    kind: RouteHostKind,
    host: &str,
    path_prefix: Option<&str>,
) -> ProtoRouteIdentity {
    ProtoRouteIdentity {
        kind: Some(control_plane::api::pb::route_identity::Kind::Http(
            HttpRouteIdentity {
                host: Some(ProtoRouteHost {
                    kind: kind as i32,
                    host: host.to_owned(),
                }),
                path_prefix: path_prefix.map(str::to_owned),
            },
        )),
    }
}

fn proto_sni_identity(kind: RouteHostKind, host: &str) -> ProtoRouteIdentity {
    ProtoRouteIdentity {
        kind: Some(control_plane::api::pb::route_identity::Kind::Sni(
            SniRouteIdentity {
                host: Some(ProtoRouteHost {
                    kind: kind as i32,
                    host: host.to_owned(),
                }),
            },
        )),
    }
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

impl<S> Service<Request<Body>> for InProcessService<S>
where
    S: Service<Request<Body>, Response = HttpResponse<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        self.inner.call(request)
    }
}

#[derive(Default)]
struct FakeWakeStore {
    instance: Mutex<Option<InstanceRecord>>,
    workload_class: Mutex<Option<control_plane::WorkloadClassVersion>>,
    materializations: Mutex<Vec<MaterializationRecord>>,
    route_resolution: Mutex<Option<FakeRouteResolution>>,
    resolve_route_requests: Mutex<usize>,
}

impl FakeWakeStore {
    fn seed_instance(&self, instance: InstanceRecord) {
        *self.instance.lock().expect("fake store lock is available") = Some(instance);
    }

    fn seed_workload_class(&self, workload_class: control_plane::WorkloadClassVersion) {
        *self
            .workload_class
            .lock()
            .expect("fake store lock is available") = Some(workload_class);
    }

    fn seed_ready_materialization(&self, materialization: MaterializationRecord) {
        self.seed_materialization(materialization);
    }

    fn seed_materialization(&self, materialization: MaterializationRecord) {
        self.materializations
            .lock()
            .expect("fake store lock is available")
            .push(materialization);
    }

    fn seed_route_resolved(&self, matched_identity: RouteIdentity, entry: RouteEntry) {
        *self
            .route_resolution
            .lock()
            .expect("fake store lock is available") = Some(FakeRouteResolution::Resolved {
            matched_identity,
            entry,
        });
    }

    fn seed_route_miss(&self, ttl: Duration) {
        *self
            .route_resolution
            .lock()
            .expect("fake store lock is available") = Some(FakeRouteResolution::Miss {
            negative_cache: CachePolicy::new(ttl),
        });
    }

    fn seed_route_error_unavailable(&self, message: &str) {
        *self
            .route_resolution
            .lock()
            .expect("fake store lock is available") =
            Some(FakeRouteResolution::Unavailable(message.to_owned()));
    }

    fn resolve_route_requests(&self) -> usize {
        *self
            .resolve_route_requests
            .lock()
            .expect("fake store lock is available")
    }
}

#[derive(Clone, Debug)]
enum FakeRouteResolution {
    Resolved {
        matched_identity: RouteIdentity,
        entry: RouteEntry,
    },
    Miss {
        negative_cache: CachePolicy,
    },
    Unavailable(String),
}

#[derive(Clone, Debug, Default)]
struct FakeKubernetesClient {
    applied_objects: Arc<Mutex<Vec<control_plane::KubernetesObject>>>,
    fail_readiness: bool,
}

impl FakeKubernetesClient {
    fn failing_readiness() -> Self {
        Self {
            applied_objects: Arc::new(Mutex::new(Vec::new())),
            fail_readiness: true,
        }
    }

    fn applied_objects_len(&self) -> usize {
        self.applied_objects
            .lock()
            .expect("fake kubernetes lock is available")
            .len()
    }
}

impl ControlPlaneStore for FakeWakeStore {
    fn create_instance<'a>(
        &'a self,
        _request: control_plane::CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn get_instance<'a>(
        &'a self,
        request: control_plane::GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async move {
            Ok(self
                .instance
                .lock()
                .expect("fake store lock is available")
                .as_ref()
                .filter(|instance| instance.id == request.instance_id)
                .cloned())
        })
    }

    fn delete_instance<'a>(
        &'a self,
        _request: control_plane::DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn create_workload_class_version<'a>(
        &'a self,
        _request: control_plane::CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::WorkloadClassVersion>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        request: control_plane::LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::WorkloadClassVersion>>> {
        Box::pin(async move {
            Ok(self
                .workload_class
                .lock()
                .expect("fake store lock is available")
                .as_ref()
                .filter(|workload_class| workload_class.reference == request.reference)
                .cloned())
        })
    }

    fn create_route_binding<'a>(
        &'a self,
        request: control_plane::CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteBindingRecord>> {
        Box::pin(async move {
            Ok(control_plane::RouteBindingRecord {
                id: request.route_binding_id,
                instance_id: request.instance_id,
                identity: request.identity,
                protocol: request.protocol,
            })
        })
    }

    fn get_route_binding<'a>(
        &'a self,
        _request: control_plane::GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::RouteBindingRecord>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn delete_route_binding<'a>(
        &'a self,
        request: control_plane::DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            let deleted = self
                .route_resolution
                .lock()
                .expect("fake store lock is available")
                .as_ref()
                .and_then(|resolution| match resolution {
                    FakeRouteResolution::Resolved { entry, .. } => Some(entry),
                    FakeRouteResolution::Miss { .. } | FakeRouteResolution::Unavailable(_) => None,
                })
                .is_some_and(|entry| entry.route_binding_id == request.route_binding_id);
            Ok(deleted)
        })
    }

    fn resolve_route<'a>(
        &'a self,
        _identity: control_plane::RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteResolution>> {
        Box::pin(async move {
            *self
                .resolve_route_requests
                .lock()
                .expect("fake store lock is available") += 1;
            match self
                .route_resolution
                .lock()
                .expect("fake store lock is available")
                .clone()
            {
                Some(FakeRouteResolution::Resolved {
                    matched_identity,
                    entry,
                }) => Ok(RouteResolution::Resolved {
                    matched_identity,
                    entry,
                }),
                Some(FakeRouteResolution::Miss { negative_cache }) => {
                    Ok(RouteResolution::Miss { negative_cache })
                }
                Some(FakeRouteResolution::Unavailable(message)) => {
                    Err(StoreError::unavailable(message))
                }
                None => Err(StoreError::internal("fake store method is not implemented")),
            }
        })
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: control_plane::CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        Box::pin(async move {
            let mut instance = self.instance.lock().expect("fake store lock is available");
            let instance = instance.as_mut().ok_or(StoreError::NotFound {
                resource: "instance",
            })?;
            if instance.generation != request.expected_generation {
                return Err(StoreError::GenerationConflict {
                    expected: request.expected_generation,
                    actual: instance.generation,
                });
            }

            instance.state = request.next_state;
            instance.generation = request.expected_generation.next();
            Ok(instance.clone())
        })
    }

    fn record_materialization<'a>(
        &'a self,
        request: control_plane::RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::MaterializationRecord>> {
        Box::pin(async move {
            let record = MaterializationRecord {
                id: MaterializationId::new(format!(
                    "{}:{}:{}",
                    request.instance_id.as_str(),
                    request.target.cluster_id(),
                    request.target.namespace()
                ))
                .expect("materialization ID is valid"),
                instance_id: request.instance_id,
                instance_generation: request.instance_generation,
                target: request.target,
                state: request.state,
                backend: request.backend,
                backend_generation: request.backend_generation,
                rendered_objects: request.rendered_objects,
                exclusivity_keys: request.exclusivity_keys,
            };
            let mut materializations = self
                .materializations
                .lock()
                .expect("fake store lock is available");
            materializations.retain(|existing| {
                !(existing.instance_id == record.instance_id && existing.target == record.target)
            });
            materializations.push(record.clone());

            Ok(record)
        })
    }

    fn load_ready_materialization<'a>(
        &'a self,
        request: control_plane::materialization::LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::MaterializationRecord>>> {
        Box::pin(async move {
            Ok(self
                .materializations
                .lock()
                .expect("fake store lock is available")
                .iter()
                .find(|materialization| {
                    materialization.instance_id == request.instance_id
                        && materialization.instance_generation == request.instance_generation
                        && materialization.target == request.target
                        && materialization.state == MaterializationState::Ready
                })
                .cloned())
        })
    }

    fn load_active_materialization<'a>(
        &'a self,
        request: control_plane::LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::MaterializationRecord>>> {
        Box::pin(async move {
            Ok(self
                .materializations
                .lock()
                .expect("fake store lock is available")
                .iter()
                .find(|materialization| {
                    materialization.instance_id == request.instance_id
                        && materialization.target == request.target
                        && materialization.state != MaterializationState::Deleted
                })
                .cloned())
        })
    }

    fn complete_wake<'a>(
        &'a self,
        request: control_plane::CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::CompleteWakeResult>> {
        Box::pin(async move {
            let instance = {
                let mut instance = self.instance.lock().expect("fake store lock is available");
                let instance = instance.as_mut().ok_or(StoreError::NotFound {
                    resource: "instance",
                })?;
                if instance.generation != request.expected_waking_generation {
                    return Err(StoreError::GenerationConflict {
                        expected: request.expected_waking_generation,
                        actual: instance.generation,
                    });
                }

                instance.state = DomainInstanceState::Running;
                instance.generation = request.expected_waking_generation.next();
                instance.clone()
            };
            let materialization = MaterializationRecord {
                id: MaterializationId::new(format!(
                    "{}:{}:{}",
                    request.instance_id.as_str(),
                    request.target.cluster_id(),
                    request.target.namespace()
                ))
                .expect("materialization ID is valid"),
                instance_id: request.instance_id,
                instance_generation: instance.generation,
                target: request.target,
                state: MaterializationState::Ready,
                backend: Some(request.backend),
                backend_generation: request.backend_generation,
                rendered_objects: request.rendered_objects,
                exclusivity_keys: request.exclusivity_keys,
            };
            self.materializations
                .lock()
                .expect("fake store lock is available")
                .push(materialization.clone());

            Ok(CompleteWakeResult {
                instance,
                materialization,
            })
        })
    }

    fn begin_sleep<'a>(
        &'a self,
        _request: control_plane::BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::BeginSleepResult>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn finalize_sleep<'a>(
        &'a self,
        _request: control_plane::FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::FinalizeSleepResult>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn lookup_route_dependencies<'a>(
        &'a self,
        _request: control_plane::RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::RouteDependencySet>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn put_http01_challenge<'a>(
        &'a self,
        _request: control_plane::PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::Http01ChallengeRecord>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        _key: control_plane::Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::Http01ChallengeRecord>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        _request: control_plane::DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        _request: control_plane::ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }
}

impl KubernetesMaterializerClient for FakeKubernetesClient {
    fn apply_object<'a>(
        &'a self,
        object: &'a control_plane::KubernetesObject,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.applied_objects
                .lock()
                .expect("fake kubernetes lock is available")
                .push(object.clone());
            Ok(())
        })
    }

    fn delete_object<'a>(
        &'a self,
        _object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn wait_for_pvc_bound<'a>(
        &'a self,
        _namespace: &'a str,
        _name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn wait_for_readiness<'a>(
        &'a self,
        _objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async {
            if self.fail_readiness {
                return Err(KubernetesClientError::new("not ready"));
            }

            BackendEndpoint::new("http://svc-acme-instance.apps.svc.cluster.local:80")
                .map_err(|error| KubernetesClientError::new(error.to_string()))
        })
    }
}

fn proxy_api(
    store: Arc<FakeWakeStore>,
    client: FakeKubernetesClient,
) -> StoreBackedProxyApi<FakeKubernetesClient> {
    proxy_api_with_target(store, client, proxy_target())
}

fn proxy_api_with_target(
    store: Arc<FakeWakeStore>,
    client: FakeKubernetesClient,
    target: MaterializationTarget,
) -> StoreBackedProxyApi<FakeKubernetesClient> {
    StoreBackedProxyApi::new(store, KubernetesMaterializer::new(client), target)
}

fn operator_api_with_route_events(
    store: Arc<FakeWakeStore>,
    route_events: RouteSubscriptionBroker,
) -> StoreBackedOperatorApi<FakeKubernetesClient> {
    StoreBackedOperatorApi::with_route_events(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        proxy_target(),
        route_events,
    )
}

fn proxy_client_with_route_events(
    store: Arc<FakeWakeStore>,
    route_events: RouteSubscriptionBroker,
) -> ProxyControlPlaneClient<InProcessService<StoreBackedProxyGrpcService<FakeKubernetesClient>>> {
    ProxyControlPlaneClient::new(InProcessService::new(
        proxy_grpc_service_with_store_and_route_events(
            store,
            KubernetesMaterializer::new(FakeKubernetesClient::default()),
            proxy_target(),
            route_events,
        ),
    ))
}

fn proxy_target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("proxy target is valid")
}

async fn subscribe_resolved_route(
    store: Arc<FakeWakeStore>,
    request_id: &str,
    identity: ProtoRouteIdentity,
    target: MaterializationTarget,
) -> ProxyRouteResolvedResponse {
    let response = proxy_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target,
    )
    .oneshot(grpc_proxy_subscribe_request(
        vec![subscribe_route_request(request_id, identity)],
        "application/grpc",
        Version::HTTP_2,
    ))
    .await
    .expect("subscribe request should dispatch");

    let (messages, status) = collect_grpc_proxy_subscribe_response(response).await;
    assert_eq!(status, "0");
    expect_route_resolved(single_message(messages))
}

fn expect_ready(
    response: control_plane::api::pb::ProxyWakeInstanceResponse,
) -> control_plane::api::pb::ProxyWakeReadyResult {
    let Some(proxy_wake_instance_response::Outcome::Ready(ready)) = response.outcome else {
        panic!("expected ready response");
    };
    ready
}

fn domain_instance(id: &str, state: DomainInstanceState, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id).expect("instance ID is valid"),
        workload_class: domain_workload_ref(),
        values: control_plane::InstanceValues::new(),
        state,
        generation: Generation::new(generation),
    }
}

fn domain_workload_class() -> control_plane::WorkloadClassVersion {
    control_plane::WorkloadClassVersion {
        reference: domain_workload_ref(),
        template_generation: Generation::new(3),
        template: control_plane::ManifestTemplate {
            workload: control_plane::WorkloadTemplate {
                kind: control_plane::WorkloadKind::Deployment,
                name: control_plane::TemplateText::literal("app-acme"),
                replicas: None,
                app_container: control_plane::ContainerTemplate {
                    name: "app".to_owned(),
                    image: control_plane::TemplateText::literal("example/app:1"),
                    ports: vec![control_plane::ContainerPortTemplate {
                        name: Some("http".to_owned()),
                        container_port: 8080,
                    }],
                    env: Vec::new(),
                },
            },
            service: Some(control_plane::ServiceTemplate {
                name: control_plane::TemplateText::literal("svc-acme"),
                ports: vec![control_plane::ServicePortTemplate {
                    name: Some("http".to_owned()),
                    port: 80,
                    target_port: 8080,
                }],
            }),
            sidecar: control_plane::SidecarTemplate {
                name: "sleepypods-sidecar".to_owned(),
                image: control_plane::TemplateText::literal("sleepypods/sidecar:test"),
                listen_port: 15000,
                mode: None,
            },
            volumes: Vec::new(),
        },
        default_values: control_plane::InstanceValues::new(),
        value_schema: control_plane::WorkloadValueSchema::new(true),
        sleep_policy: control_plane::WorkloadSleepPolicy::new(300_000, 5_000, 30_000)
            .expect("valid sleep policy"),
        exclusivity_keys: vec![],
    }
}

fn domain_workload_ref() -> control_plane::WorkloadClassVersionRef {
    control_plane::WorkloadClassVersionRef::new(
        control_plane::WorkloadClassId::new("class-1").expect("workload class ID is valid"),
        Generation::new(1),
    )
}

fn ready_materialization(instance_id: &str, generation: u64) -> MaterializationRecord {
    materialization_with_state(instance_id, generation, MaterializationState::Ready)
}

fn materialization_with_state(
    instance_id: &str,
    generation: u64,
    state: MaterializationState,
) -> MaterializationRecord {
    materialization_with_state_and_target(instance_id, generation, state, proxy_target())
}

fn materialization_with_state_and_target(
    instance_id: &str,
    generation: u64,
    state: MaterializationState,
    target: MaterializationTarget,
) -> MaterializationRecord {
    MaterializationRecord {
        id: MaterializationId::new(format!(
            "{}:{}:{}",
            instance_id,
            target.cluster_id(),
            target.namespace()
        ))
        .expect("materialization ID is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance ID is valid"),
        instance_generation: Generation::new(generation),
        target,
        state,
        backend: Some(
            BackendEndpoint::new("http://svc-acme.apps.svc.cluster.local:80")
                .expect("backend URI is valid"),
        ),
        backend_generation: BackendGeneration::new(generation),
        rendered_objects: Vec::new(),
        exclusivity_keys: vec![],
    }
}

fn domain_http_identity(host: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(host).expect("route host is valid"),
        path: path_prefix.map(|path| PathPrefix::new(path).expect("path prefix is valid")),
    }
}

fn domain_http_identity_exact(host: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("route host is valid"),
        path: path_prefix.map(|path| PathPrefix::new(path).expect("path prefix is valid")),
    }
}

fn domain_sni_identity(host: &str, kind: RouteHostKind) -> RouteIdentity {
    let host = match kind {
        RouteHostKind::Exact => RouteHost::exact(host),
        RouteHostKind::WildcardSuffix => RouteHost::wildcard_suffix(host),
        RouteHostKind::Unspecified => panic!("unspecified route host kind is invalid"),
    }
    .expect("route host is valid");

    RouteIdentity::Sni { host }
}

fn domain_route_entry(
    route_binding_id: &str,
    instance_id: &str,
    state: DomainInstanceState,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: RouteBindingId::new(route_binding_id).expect("route binding ID is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance ID is valid"),
        instance_state: state,
        instance_generation: Generation::new(7),
        backend: Some(
            BackendEndpoint::new("http://backend.example.local:8080").expect("backend is valid"),
        ),
        backend_generation: Some(BackendGeneration::new(9)),
    }
}
