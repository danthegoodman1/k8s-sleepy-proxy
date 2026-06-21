use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bytes::{BufMut, BytesMut};
use control_plane::api::{
    operator_grpc_server_builder, operator_grpc_service, operator_grpc_service_with_store,
    operator_grpc_web_server_builder,
    pb::{
        operator_control_plane_server::OperatorControlPlane, CreateInstanceRequest,
        CreateRouteBindingRequest, CreateWorkloadClassVersionRequest, DeleteInstanceRequest,
        GetInstanceRequest, GetRouteBindingRequest, Instance, InstanceState, ProtocolRoute,
        RouteHostKind, WorkloadClassVersionRef, WorkloadValueFieldRule, WorkloadValueSchema,
    },
    OperatorApiPlaceholder, StoreBackedOperatorApi, OPERATOR_SERVICE_NAME, OPERATOR_UNARY_METHODS,
};
use control_plane::{
    ControlPlaneStore, CreateInstanceResult, Generation, InstanceRecord,
    InstanceState as DomainInstanceState, StoreError, StoreFuture, StoreResult,
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

#[tokio::test]
async fn store_backed_instance_methods_create_get_and_delete_instances() {
    let store = Arc::new(FakeInstanceStore::default());
    let service = StoreBackedOperatorApi::new(store);

    let created = service
        .create_instance(tonic::Request::new(CreateInstanceRequest {
            idempotency_key: "create-instance-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-1".to_owned(),
                version: 7,
            }),
            values: [("tenant".to_owned(), "acme".to_owned())].into(),
        }))
        .await
        .expect("create instance succeeds")
        .into_inner();

    assert_eq!(created.instance_id, "instance-1");
    assert_eq!(created.generation, 0);
    assert_eq!(created.state, InstanceState::Cold as i32);
    assert_eq!(
        created
            .workload_class
            .as_ref()
            .expect("workload class is returned")
            .version,
        7
    );

    let loaded = service
        .get_instance(tonic::Request::new(GetInstanceRequest {
            instance_id: "instance-1".to_owned(),
        }))
        .await
        .expect("get instance succeeds")
        .into_inner();
    assert_eq!(loaded, created);

    let deleted = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-1".to_owned(),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();
    assert!(deleted.deleted);

    let missing = service
        .get_instance(tonic::Request::new(GetInstanceRequest {
            instance_id: "instance-1".to_owned(),
        }))
        .await
        .expect_err("deleted instance no longer loads");
    assert_eq!(missing.code(), Code::NotFound);
}

#[tokio::test]
async fn store_backed_api_keeps_out_of_scope_methods_explicitly_unimplemented() {
    let service = StoreBackedOperatorApi::new(Arc::new(FakeInstanceStore::default()));

    let error = service
        .get_route_binding(tonic::Request::new(GetRouteBindingRequest::default()))
        .await
        .expect_err("route binding APIs are deferred to a later phase");

    assert_eq!(error.code(), Code::Unimplemented);
    assert!(error.message().contains("transport is scaffolded"));
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_create_instance() {
    let response = operator_grpc_service_with_store(Arc::new(FakeInstanceStore::default()))
        .oneshot(grpc_create_instance_request(
            CreateInstanceRequest {
                idempotency_key: "create-instance-transport".to_owned(),
                instance_id: "instance-transport".to_owned(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: "class-transport".to_owned(),
                    version: 3,
                }),
                values: [("tenant".to_owned(), "transport".to_owned())].into(),
            },
            "application/grpc",
            Version::HTTP_2,
        ))
        .await
        .expect("native gRPC request should route through store-backed service");
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

    let created = decode_grpc_instance_response(collected.to_bytes().as_ref());
    assert_eq!(created.instance_id, "instance-transport");
    assert_eq!(created.state, InstanceState::Cold as i32);
    assert_eq!(created.generation, 0);
    assert_eq!(created.values["tenant"], "transport");
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
fn native_grpc_server_can_be_constructed_with_store_backed_operator_service() {
    let _router = operator_grpc_server_builder().add_service(operator_grpc_service_with_store(
        Arc::new(FakeInstanceStore::default()),
    ));

    assert_eq!(
        <control_plane::api::server::StoreBackedOperatorGrpcService as NamedService>::NAME,
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
    grpc_create_instance_request(CreateInstanceRequest::default(), content_type, version)
}

fn grpc_create_instance_request(
    request: CreateInstanceRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    let mut message = BytesMut::new();
    request.encode(&mut message).expect("request encodes");

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

fn decode_grpc_instance_response(bytes: &[u8]) -> Instance {
    assert_eq!(bytes.first(), Some(&0), "gRPC message is uncompressed");
    let length = u32::from_be_bytes(
        bytes[1..5]
            .try_into()
            .expect("gRPC response frame has a length prefix"),
    ) as usize;
    Instance::decode(&bytes[5..5 + length]).expect("instance response decodes")
}

#[derive(Default)]
struct FakeInstanceStore {
    instances: Mutex<BTreeMap<String, InstanceRecord>>,
}

impl ControlPlaneStore for FakeInstanceStore {
    fn create_instance<'a>(
        &'a self,
        request: control_plane::CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        Box::pin(async move {
            let instance = InstanceRecord {
                id: request.instance_id,
                workload_class: request.workload_class,
                values: request.values,
                state: DomainInstanceState::Cold,
                generation: Generation::new(0),
            };
            self.instances
                .lock()
                .expect("fake store lock is available")
                .insert(instance.id.as_str().to_owned(), instance.clone());

            Ok(CreateInstanceResult {
                instance,
                route_bindings: Vec::new(),
                idempotency_replayed: false,
            })
        })
    }

    fn get_instance<'a>(
        &'a self,
        request: control_plane::GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async move {
            Ok(self
                .instances
                .lock()
                .expect("fake store lock is available")
                .get(request.instance_id.as_str())
                .cloned())
        })
    }

    fn delete_instance<'a>(
        &'a self,
        request: control_plane::DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            Ok(self
                .instances
                .lock()
                .expect("fake store lock is available")
                .remove(request.instance_id.as_str())
                .is_some())
        })
    }

    fn create_workload_class_version<'a>(
        &'a self,
        _request: control_plane::CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::WorkloadClassVersion>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        _request: control_plane::LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::WorkloadClassVersion>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn resolve_route<'a>(
        &'a self,
        _identity: control_plane::RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteResolution>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        _request: control_plane::CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn record_materialization<'a>(
        &'a self,
        _request: control_plane::RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::MaterializationRecord>> {
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
