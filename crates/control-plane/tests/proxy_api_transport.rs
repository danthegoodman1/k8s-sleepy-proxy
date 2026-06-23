use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use control_plane::api::{
    operator_grpc_server_builder,
    pb::{
        proxy_control_plane_server::ProxyControlPlane, proxy_subscribe_request,
        proxy_subscribe_response, proxy_wake_instance_response, HttpRouteIdentity, InstanceState,
        ProxyCachePolicy, ProxyRouteEntry, ProxyRouteMissResponse, ProxyRouteResolvedResponse,
        ProxySubscribeRequest, ProxySubscribeResponse, ProxySubscribeRouteRequest,
        ProxyWakeInstanceRequest, ProxyWakeInstanceResponse, ProxyWakeUnavailableReason,
        RouteHost as ProtoRouteHost, RouteHostKind, RouteIdentity as ProtoRouteIdentity,
    },
    proxy_grpc_service_with_store, StoreBackedProxyApi, StoreBackedProxyGrpcService,
    OPERATOR_UNARY_METHODS, PROXY_SERVICE_NAME,
};
use control_plane::{
    BackendEndpoint, BackendGeneration, CachePolicy, CompleteWakeResult, ControlPlaneStore,
    CreateInstanceResult, Generation, InstanceId, InstanceRecord,
    InstanceState as DomainInstanceState, KubernetesClientError, KubernetesClientFuture,
    KubernetesClientResult, KubernetesMaterializer, KubernetesMaterializerClient,
    MaterializationId, MaterializationRecord, MaterializationState, MaterializationTarget,
    PathPrefix, RenderedObjectRef, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
    RouteResolution, StoreError, StoreFuture, StoreResult,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{header, Request, Response as HttpResponse, Version};
use tonic::server::NamedService;
use tonic::Code;
use tower::ServiceExt;

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
        "http://svc-acme.apps.svc.cluster.local:80"
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
            backend_uri: Some("http://backend.example.local:8080".to_owned()),
            backend_generation: Some(9),
        })
    );
    assert_eq!(
        resolved.cache_policy,
        Some(ProxyCachePolicy { ttl_millis: 10_000 })
    );
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
async fn proxy_wake_already_waking_returns_still_waking() {
    let store = Arc::new(FakeWakeStore::default());
    store.seed_instance(domain_instance(
        "instance-waking",
        DomainInstanceState::Waking,
        6,
    ));
    let service = proxy_api(store, FakeKubernetesClient::default());

    let response = service
        .wake_instance(tonic::Request::new(ProxyWakeInstanceRequest {
            instance_id: "instance-waking".to_owned(),
            expected_generation: 6,
            backend_generation: None,
        }))
        .await
        .expect("already-waking wake succeeds")
        .into_inner();

    let Some(proxy_wake_instance_response::Outcome::StillWaking(still_waking)) = response.outcome
    else {
        panic!("expected still-waking response");
    };
    assert_eq!(still_waking.instance_id, "instance-waking");
    assert_eq!(still_waking.instance_generation, 6);
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

async fn collect_grpc_proxy_subscribe_response(
    response: HttpResponse<Body>,
) -> (Vec<ProxySubscribeResponse>, String) {
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

#[derive(Default)]
struct FakeWakeStore {
    instance: Mutex<Option<InstanceRecord>>,
    workload_class: Mutex<Option<control_plane::WorkloadClassVersion>>,
    materializations: Mutex<Vec<MaterializationRecord>>,
    route_resolution: Mutex<Option<FakeRouteResolution>>,
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
        _request: control_plane::CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteBindingRecord>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn get_route_binding<'a>(
        &'a self,
        _request: control_plane::GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::RouteBindingRecord>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn delete_route_binding<'a>(
        &'a self,
        _request: control_plane::DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn resolve_route<'a>(
        &'a self,
        _identity: control_plane::RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteResolution>> {
        Box::pin(async move {
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
        _request: control_plane::RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::MaterializationRecord>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
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

            BackendEndpoint::new("http://svc-acme.apps.svc.cluster.local:80")
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

fn proxy_target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("proxy target is valid")
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
            },
            volumes: Vec::new(),
        },
        default_values: control_plane::InstanceValues::new(),
        value_schema: control_plane::WorkloadValueSchema::new(true),
        sleep_policy: control_plane::WorkloadSleepPolicy::new(300_000, 5_000, 30_000)
            .expect("valid sleep policy"),
    }
}

fn domain_workload_ref() -> control_plane::WorkloadClassVersionRef {
    control_plane::WorkloadClassVersionRef::new(
        control_plane::WorkloadClassId::new("class-1").expect("workload class ID is valid"),
        Generation::new(1),
    )
}

fn ready_materialization(instance_id: &str, generation: u64) -> MaterializationRecord {
    MaterializationRecord {
        id: MaterializationId::new(format!("{instance_id}:cluster-a:apps"))
            .expect("materialization ID is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance ID is valid"),
        instance_generation: Generation::new(generation),
        target: proxy_target(),
        state: MaterializationState::Ready,
        backend: Some(
            BackendEndpoint::new("http://svc-acme.apps.svc.cluster.local:80")
                .expect("backend URI is valid"),
        ),
        backend_generation: BackendGeneration::new(generation),
        rendered_objects: Vec::new(),
    }
}

fn domain_http_identity(host: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(host).expect("route host is valid"),
        path: path_prefix.map(|path| PathPrefix::new(path).expect("path prefix is valid")),
    }
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
