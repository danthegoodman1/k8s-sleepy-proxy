use bytes::{BufMut, BytesMut};
use control_plane::api::{
    operator_grpc_server_builder, operator_grpc_service, operator_grpc_web_server_builder,
    pb::{
        operator_control_plane_server::OperatorControlPlane, CreateInstanceRequest,
        CreateRouteBindingRequest, CreateWorkloadClassVersionRequest, InstanceState, ProtocolRoute,
        RouteHostKind, WorkloadValueFieldRule, WorkloadValueSchema,
    },
    OperatorApiPlaceholder, OPERATOR_SERVICE_NAME, OPERATOR_UNARY_METHODS,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{header, Request, Version};
use tonic::server::NamedService;
use tonic::Code;
use tower::{Layer, ServiceExt};

#[test]
fn generated_api_contains_expected_v1_resource_shape() {
    let request = CreateInstanceRequest {
        idempotency_key: "create-instance-1".to_owned(),
        instance_id: "instance-1".to_owned(),
        workload_class: None,
        values: [("tenant".to_owned(), "acme".to_owned())].into(),
    };
    let route_binding = CreateRouteBindingRequest {
        idempotency_key: "create-route-1".to_owned(),
        route_binding_id: "route-1".to_owned(),
        instance_id: request.instance_id.clone(),
        identity: None,
        protocol: ProtocolRoute::Http as i32,
    };
    let workload_class = CreateWorkloadClassVersionRequest {
        idempotency_key: "create-class-1".to_owned(),
        class_id: "class-1".to_owned(),
        version: 1,
        default_values: [("image".to_owned(), "example/app:1".to_owned())].into(),
        value_schema: Some(WorkloadValueSchema {
            fields: [(
                "tenant".to_owned(),
                WorkloadValueFieldRule {
                    required: true,
                    default_value: None,
                },
            )]
            .into(),
            allow_extra: false,
        }),
    };

    assert_eq!(request.values["tenant"], "acme");
    assert_eq!(route_binding.instance_id, "instance-1");
    assert!(
        workload_class
            .value_schema
            .as_ref()
            .expect("value schema is present")
            .fields["tenant"]
            .required
    );
    assert_eq!(InstanceState::Cold as i32, 1);
    assert_eq!(ProtocolRoute::Http as i32, 1);
    assert_eq!(RouteHostKind::WildcardSuffix as i32, 2);
}

#[tokio::test]
async fn placeholder_methods_are_explicitly_unimplemented() {
    let service = OperatorApiPlaceholder::new();

    let error = service
        .create_instance(tonic::Request::new(CreateInstanceRequest::default()))
        .await
        .expect_err("2B does not implement store-backed CRUD");

    assert_eq!(error.code(), Code::Unimplemented);
    assert!(error.message().contains("transport is scaffolded"));
}

#[test]
fn native_grpc_server_can_be_constructed_with_operator_service() {
    let _router = operator_grpc_server_builder().add_service(operator_grpc_service());

    assert_eq!(
        <control_plane::api::server::OperatorGrpcService as NamedService>::NAME,
        OPERATOR_SERVICE_NAME
    );
}

#[test]
fn grpc_web_server_wraps_same_operator_service_surface() {
    let _router = operator_grpc_web_server_builder().add_service(operator_grpc_service());

    assert_eq!(
        <control_plane::api::server::OperatorGrpcService as NamedService>::NAME,
        OPERATOR_SERVICE_NAME
    );
}

#[tokio::test]
async fn native_grpc_and_grpc_web_requests_dispatch_to_same_placeholder_method() {
    let native_response = operator_grpc_service()
        .oneshot(grpc_request("application/grpc", Version::HTTP_2))
        .await
        .expect("native gRPC request should route through generated service");
    let native_headers = native_response.headers().clone();
    let native_body = native_response
        .into_body()
        .collect()
        .await
        .expect("native response body should collect");
    let native_trailers = native_body.trailers();
    let native_status = native_trailers
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| native_headers.get("grpc-status"))
        .expect("unimplemented gRPC status is returned");
    let native_message = native_trailers
        .and_then(|trailers| trailers.get("grpc-message"))
        .or_else(|| native_headers.get("grpc-message"))
        .expect("unimplemented gRPC message is returned");

    assert_eq!(native_status, "12");
    assert!(native_message
        .to_str()
        .expect("grpc-message is valid")
        .contains("CreateInstance%20transport%20is%20scaffolded"));

    let web_response = tonic_web::GrpcWebLayer::new()
        .layer(operator_grpc_service())
        .oneshot(grpc_request("application/grpc-web+proto", Version::HTTP_11))
        .await
        .expect("gRPC-Web request should route through generated service");
    let web_headers = web_response.headers().clone();
    let web_content_type = web_response
        .headers()
        .get(header::CONTENT_TYPE)
        .expect("gRPC-Web content type")
        .clone();
    let web_body = web_response
        .into_body()
        .collect()
        .await
        .expect("gRPC-Web response body should collect")
        .trailers()
        .cloned();
    let web_status = web_body
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"))
        .or_else(|| web_headers.get("grpc-status"))
        .expect("gRPC-Web status is returned");
    let web_message = web_body
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-message"))
        .or_else(|| web_headers.get("grpc-message"))
        .expect("gRPC-Web message is returned");

    assert_eq!(web_content_type, "application/grpc-web+proto");
    assert_eq!(web_status, "12");
    assert!(web_message
        .to_str()
        .expect("grpc-message is valid")
        .contains("CreateInstance%20transport%20is%20scaffolded"));
}

#[test]
fn operator_grpc_web_surface_is_unary_and_does_not_expose_proxy_subscribe() {
    assert_eq!(OPERATOR_UNARY_METHODS.len(), 12);
    assert!(OPERATOR_UNARY_METHODS.contains(&"CreateWorkloadClassVersion"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"GetWorkloadClassVersion"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"CreateInstance"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"GetInstance"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"DeleteInstance"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"CreateRouteBinding"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"GetRouteBinding"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"DeleteRouteBinding"));
    assert!(OPERATOR_UNARY_METHODS.contains(&"ResolveHttp01Challenge"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"Subscribe"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"ResolveRoute"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"LookupRouteDependencies"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"RecordMaterialization"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"CompareAndSwapInstanceState"));
}

fn grpc_request(content_type: &'static str, version: Version) -> Request<Body> {
    let mut message = BytesMut::new();
    CreateInstanceRequest::default()
        .encode(&mut message)
        .expect("request encodes");

    let mut frame = BytesMut::with_capacity(5 + message.len());
    frame.put_u8(0);
    frame.put_u32(message.len() as u32);
    frame.extend_from_slice(&message);

    Request::builder()
        .version(version)
        .method("POST")
        .uri("/sleepypods.controlplane.v1.OperatorControlPlane/CreateInstance")
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::new(Full::new(frame.freeze())))
        .expect("request builds")
}
