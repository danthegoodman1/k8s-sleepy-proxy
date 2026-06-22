use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{BufMut, BytesMut};
use control_plane::api::{
    operator_grpc_server_builder, operator_grpc_service, operator_grpc_service_with_store,
    operator_grpc_web_server_builder,
    pb::{
        operator_control_plane_server::OperatorControlPlane, persistent_volume_source_template,
        route_identity, template_text_part, ContainerPortTemplate, ContainerTemplate,
        CreateInstanceRequest, CreateRouteBindingRequest, CreateWorkloadClassVersionRequest,
        CsiVolumeSourceTemplate, DeleteHttp01ChallengeRequest, DeleteInstanceRequest,
        DeleteRouteBindingRequest, EnvVarTemplate, ExpireHttp01ChallengesRequest,
        GetInstanceRequest, GetRouteBindingRequest, GetWorkloadClassVersionRequest,
        HostPathVolumeSourceTemplate, Http01ChallengeKey, HttpRouteIdentity, Instance,
        InstanceState, ManifestTemplate, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
        PersistentVolumeSourceTemplate, ProtocolRoute, PutHttp01ChallengeRequest,
        ResolveHttp01ChallengeRequest, RouteBinding, RouteHost, RouteHostKind, RouteIdentity,
        ServicePortTemplate, ServiceTemplate, SidecarTemplate, SniRouteIdentity, TemplateText,
        TemplateTextPart, VolumeTemplate, WorkloadClassVersionRef, WorkloadKind, WorkloadTemplate,
        WorkloadValueFieldRule, WorkloadValueSchema,
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
        template_generation: 1,
        template: Some(stateful_manifest_template_proto()),
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
    let template = workload_class
        .template
        .as_ref()
        .expect("template is present");
    assert_eq!(
        template
            .workload
            .as_ref()
            .expect("workload template is present")
            .kind,
        WorkloadKind::StatefulSet as i32
    );
    assert_eq!(
        template.volumes[0]
            .access_modes
            .first()
            .copied()
            .expect("access mode is generated"),
        PersistentVolumeAccessMode::ReadWriteOnce as i32
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
async fn store_backed_operator_methods_cover_workload_routes_and_http01() {
    let service = StoreBackedOperatorApi::new(Arc::new(FakeInstanceStore::default()));
    let template = stateful_manifest_template_proto();

    let created_class = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
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
            template_generation: 9,
            template: Some(template.clone()),
        }))
        .await
        .expect("create workload class succeeds")
        .into_inner();
    assert_eq!(created_class.template_generation, 9);
    assert_eq!(created_class.template, Some(template));

    let loaded_class = service
        .get_workload_class_version(tonic::Request::new(GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "class-1".to_owned(),
                version: 1,
            }),
        }))
        .await
        .expect("get workload class succeeds")
        .into_inner();
    assert_eq!(loaded_class, created_class);

    let missing_template = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-class-missing-template".to_owned(),
            class_id: "class-missing-template".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: None,
        }))
        .await
        .expect_err("template is required");
    assert_eq!(missing_template.code(), Code::InvalidArgument);
    assert!(missing_template.message().contains("template is required"));

    let created_route = service
        .create_route_binding(tonic::Request::new(CreateRouteBindingRequest {
            idempotency_key: "create-route-1".to_owned(),
            route_binding_id: "route-1".to_owned(),
            instance_id: "instance-1".to_owned(),
            identity: Some(http_identity("App.Example.COM.", Some("/api"))),
            protocol: ProtocolRoute::Http as i32,
        }))
        .await
        .expect("create route binding succeeds")
        .into_inner();
    assert_eq!(created_route.route_binding_id, "route-1");
    let http = match created_route
        .identity
        .as_ref()
        .and_then(|identity| identity.kind.as_ref())
        .expect("route identity returned")
    {
        route_identity::Kind::Http(http) => http,
        route_identity::Kind::Sni(_) => panic!("expected HTTP route"),
    };
    assert_eq!(
        http.host.as_ref().expect("host returned").host,
        "app.example.com"
    );

    let loaded_route = service
        .get_route_binding(tonic::Request::new(GetRouteBindingRequest {
            route_binding_id: "route-1".to_owned(),
        }))
        .await
        .expect("get route binding succeeds")
        .into_inner();
    assert_eq!(loaded_route, created_route);
    let deleted_route = service
        .delete_route_binding(tonic::Request::new(DeleteRouteBindingRequest {
            route_binding_id: "route-1".to_owned(),
        }))
        .await
        .expect("delete route binding succeeds")
        .into_inner();
    assert!(deleted_route.deleted);

    let expires_at = UNIX_EPOCH + Duration::from_secs(4_102_444_800);
    let challenge_key = Http01ChallengeKey {
        host: "Acme.Example.COM.".to_owned(),
        token: "token-a".to_owned(),
    };
    let put_challenge = service
        .put_http01_challenge(tonic::Request::new(PutHttp01ChallengeRequest {
            key: Some(challenge_key.clone()),
            key_authorization: "key-auth-a".to_owned(),
            expires_at_unix_millis: 4_102_444_800_000,
        }))
        .await
        .expect("put HTTP-01 challenge succeeds")
        .into_inner();
    assert_eq!(
        put_challenge.key.as_ref().expect("key returned").host,
        "acme.example.com"
    );

    let resolved = service
        .resolve_http01_challenge(tonic::Request::new(ResolveHttp01ChallengeRequest {
            key: Some(challenge_key.clone()),
        }))
        .await
        .expect("resolve HTTP-01 challenge succeeds")
        .into_inner()
        .challenge
        .expect("challenge resolves");
    assert_eq!(resolved.key_authorization, "key-auth-a");

    let expired = service
        .expire_http01_challenges(tonic::Request::new(ExpireHttp01ChallengesRequest {
            now_unix_millis: 1,
            limit: Some(10),
        }))
        .await
        .expect("expire HTTP-01 challenges succeeds")
        .into_inner();
    assert_eq!(expired.expired, 0);

    let deleted = service
        .delete_http01_challenge(tonic::Request::new(DeleteHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "acme.example.com".to_owned(),
                token: "token-a".to_owned(),
            }),
        }))
        .await
        .expect("delete HTTP-01 challenge succeeds")
        .into_inner();
    assert!(deleted.deleted);
    assert!(expires_at > SystemTime::now());
}

#[tokio::test]
async fn store_backed_workload_class_api_round_trips_host_path_template() {
    let service = StoreBackedOperatorApi::new(Arc::new(FakeInstanceStore::default()));
    let template = host_path_manifest_template_proto();

    let created = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-host-path-class".to_owned(),
            class_id: "host-path-class".to_owned(),
            version: 1,
            default_values: [("tenant".to_owned(), "acme".to_owned())].into(),
            value_schema: None,
            template_generation: 1,
            template: Some(template.clone()),
        }))
        .await
        .expect("create workload class with hostPath template succeeds")
        .into_inner();

    assert_eq!(created.template, Some(template.clone()));

    let loaded = service
        .get_workload_class_version(tonic::Request::new(GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "host-path-class".to_owned(),
                version: 1,
            }),
        }))
        .await
        .expect("get workload class with hostPath template succeeds")
        .into_inner();

    assert_eq!(loaded, created);
    let source = loaded.template.as_ref().expect("template returned").volumes[0]
        .source
        .as_ref()
        .and_then(|source| source.kind.as_ref())
        .expect("volume source returned");
    assert!(matches!(
        source,
        persistent_volume_source_template::Kind::HostPath(_)
    ));
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_empty_template_static_strings() {
    let service = StoreBackedOperatorApi::new(Arc::new(FakeInstanceStore::default()));
    let mut template = stateful_manifest_template_proto();
    template
        .workload
        .as_mut()
        .expect("workload template exists")
        .app_container
        .as_mut()
        .expect("app container exists")
        .name
        .clear();

    let error = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-empty-container-name-class".to_owned(),
            class_id: "empty-container-name-class".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: Some(template),
        }))
        .await
        .expect_err("empty static template names are rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("template.container.name"));
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_empty_template_text_parts() {
    let service = StoreBackedOperatorApi::new(Arc::new(FakeInstanceStore::default()));
    let mut template = stateful_manifest_template_proto();
    template
        .workload
        .as_mut()
        .expect("workload template exists")
        .name = Some(TemplateText { parts: Vec::new() });

    let error = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-empty-template-text-class".to_owned(),
            class_id: "empty-template-text-class".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: Some(template),
        }))
        .await
        .expect_err("empty template text parts are rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("template text parts"));
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

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_create_route_binding() {
    let response = operator_grpc_service_with_store(Arc::new(FakeInstanceStore::default()))
        .oneshot(grpc_create_route_binding_request(
            CreateRouteBindingRequest {
                idempotency_key: "create-route-transport".to_owned(),
                route_binding_id: "route-transport".to_owned(),
                instance_id: "instance-transport".to_owned(),
                identity: Some(sni_identity("DB.Example.COM.")),
                protocol: ProtocolRoute::TlsSni as i32,
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

    let created = decode_grpc_route_binding_response(collected.to_bytes().as_ref());
    assert_eq!(created.route_binding_id, "route-transport");
    assert_eq!(created.protocol, ProtocolRoute::TlsSni as i32);
    let sni = match created
        .identity
        .as_ref()
        .and_then(|identity| identity.kind.as_ref())
        .expect("route identity returned")
    {
        route_identity::Kind::Sni(sni) => sni,
        route_identity::Kind::Http(_) => panic!("expected SNI route"),
    };
    assert_eq!(
        sni.host.as_ref().expect("host returned").host,
        "db.example.com"
    );
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
    grpc_unary_request(
        request,
        "/sleepypods.controlplane.v1.OperatorControlPlane/CreateInstance",
        content_type,
        version,
    )
}

fn grpc_create_route_binding_request(
    request: CreateRouteBindingRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_unary_request(
        request,
        "/sleepypods.controlplane.v1.OperatorControlPlane/CreateRouteBinding",
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
    let mut message = BytesMut::new();
    request.encode(&mut message).expect("request encodes");

    let mut frame = BytesMut::with_capacity(5 + message.len());
    frame.put_u8(0);
    frame.put_u32(message.len() as u32);
    frame.extend_from_slice(&message);

    Request::builder()
        .version(version)
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::new(Full::new(frame.freeze())))
        .expect("request builds")
}

fn decode_grpc_instance_response(bytes: &[u8]) -> Instance {
    decode_grpc_message(bytes)
}

fn decode_grpc_route_binding_response(bytes: &[u8]) -> RouteBinding {
    decode_grpc_message(bytes)
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

fn http_identity(host: &str, path_prefix: Option<&str>) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Http(HttpRouteIdentity {
            host: Some(RouteHost {
                kind: RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
            path_prefix: path_prefix.map(str::to_owned),
        })),
    }
}

fn sni_identity(host: &str) -> RouteIdentity {
    RouteIdentity {
        kind: Some(route_identity::Kind::Sni(SniRouteIdentity {
            host: Some(RouteHost {
                kind: RouteHostKind::Exact as i32,
                host: host.to_owned(),
            }),
        })),
    }
}

fn stateful_manifest_template_proto() -> ManifestTemplate {
    ManifestTemplate {
        workload: Some(WorkloadTemplate {
            kind: WorkloadKind::StatefulSet as i32,
            name: Some(composed_text("db-", "tenant")),
            replicas: Some(1),
            app_container: Some(ContainerTemplate {
                name: "postgres".to_owned(),
                image: Some(literal_text("postgres:17")),
                ports: vec![ContainerPortTemplate {
                    name: Some("postgres".to_owned()),
                    container_port: 5432,
                }],
                env: vec![EnvVarTemplate {
                    name: "TENANT".to_owned(),
                    value: Some(instance_value_text("tenant")),
                }],
            }),
        }),
        sidecar: Some(SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: Some(literal_text("sleepypods/sidecar:test")),
            listen_port: 15000,
        }),
        service: Some(ServiceTemplate {
            name: Some(composed_text("db-", "tenant")),
            ports: vec![ServicePortTemplate {
                name: Some("postgres".to_owned()),
                port: 5432,
                target_port: 5432,
            }],
        }),
        volumes: vec![VolumeTemplate {
            name: "data".to_owned(),
            mount_path: Some(literal_text("/var/lib/postgresql/data")),
            pv_name: Some(composed_text("pv-", "tenant")),
            pvc_name: Some(composed_text("pvc-", "tenant")),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce as i32],
            capacity: Some(literal_text("10Gi")),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain as i32,
            storage_class_name: Some(literal_text("manual")),
            source: Some(PersistentVolumeSourceTemplate {
                kind: Some(persistent_volume_source_template::Kind::Csi(
                    CsiVolumeSourceTemplate {
                        driver: Some(literal_text("csi.example.com")),
                        volume_handle: Some(instance_value_text("volume")),
                        fs_type: Some(literal_text("ext4")),
                        read_only: false,
                        volume_attributes: [("tenant".to_owned(), instance_value_text("tenant"))]
                            .into(),
                    },
                )),
            }),
        }],
    }
}

fn host_path_manifest_template_proto() -> ManifestTemplate {
    let mut template = stateful_manifest_template_proto();
    template.volumes[0].source = Some(PersistentVolumeSourceTemplate {
        kind: Some(persistent_volume_source_template::Kind::HostPath(
            HostPathVolumeSourceTemplate {
                path: Some(composed_text("/var/local/sleepypods/", "tenant")),
                r#type: Some(literal_text("DirectoryOrCreate")),
            },
        )),
    });
    template
}

fn literal_text(value: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::Literal(value.to_owned())),
        }],
    }
}

fn instance_value_text(field: &str) -> TemplateText {
    TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::InstanceValue(field.to_owned())),
        }],
    }
}

fn composed_text(prefix: &str, field: &str) -> TemplateText {
    TemplateText {
        parts: vec![
            TemplateTextPart {
                kind: Some(template_text_part::Kind::Literal(prefix.to_owned())),
            },
            TemplateTextPart {
                kind: Some(template_text_part::Kind::InstanceValue(field.to_owned())),
            },
        ],
    }
}

#[derive(Default)]
struct FakeInstanceStore {
    instances: Mutex<BTreeMap<String, InstanceRecord>>,
    workload_classes: Mutex<BTreeMap<(String, u64), control_plane::WorkloadClassVersion>>,
    route_bindings: Mutex<BTreeMap<String, control_plane::RouteBindingRecord>>,
    http01: Mutex<BTreeMap<(String, String), control_plane::Http01ChallengeRecord>>,
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
        request: control_plane::CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::WorkloadClassVersion>> {
        Box::pin(async move {
            let workload_class = request.workload_class_version;
            let key = (
                workload_class.reference.class_id.as_str().to_owned(),
                workload_class.reference.version.get(),
            );
            self.workload_classes
                .lock()
                .expect("fake store lock is available")
                .insert(key, workload_class.clone());

            Ok(workload_class)
        })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        request: control_plane::LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::WorkloadClassVersion>>> {
        Box::pin(async move {
            let key = (
                request.reference.class_id.as_str().to_owned(),
                request.reference.version.get(),
            );
            Ok(self
                .workload_classes
                .lock()
                .expect("fake store lock is available")
                .get(&key)
                .cloned())
        })
    }

    fn create_route_binding<'a>(
        &'a self,
        request: control_plane::CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::RouteBindingRecord>> {
        Box::pin(async move {
            let record = control_plane::RouteBindingRecord {
                id: request.route_binding_id,
                instance_id: request.instance_id,
                identity: request.identity,
                protocol: request.protocol,
            };
            self.route_bindings
                .lock()
                .expect("fake store lock is available")
                .insert(record.id.as_str().to_owned(), record.clone());

            Ok(record)
        })
    }

    fn get_route_binding<'a>(
        &'a self,
        request: control_plane::GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::RouteBindingRecord>>> {
        Box::pin(async move {
            Ok(self
                .route_bindings
                .lock()
                .expect("fake store lock is available")
                .get(request.route_binding_id.as_str())
                .cloned())
        })
    }

    fn delete_route_binding<'a>(
        &'a self,
        request: control_plane::DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            Ok(self
                .route_bindings
                .lock()
                .expect("fake store lock is available")
                .remove(request.route_binding_id.as_str())
                .is_some())
        })
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

    fn load_ready_materialization<'a>(
        &'a self,
        _request: control_plane::materialization::LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::MaterializationRecord>>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }

    fn complete_wake<'a>(
        &'a self,
        _request: control_plane::CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::CompleteWakeResult>> {
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
        request: control_plane::PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::Http01ChallengeRecord>> {
        Box::pin(async move {
            let record = control_plane::Http01ChallengeRecord::new(
                request.key().clone(),
                request.key_authorization().to_owned(),
                request.expires_at(),
                UNIX_EPOCH,
            )
            .expect("service parsed a valid HTTP-01 challenge");
            let key = (
                record.key().host().as_str().to_owned(),
                record.key().token().to_owned(),
            );
            self.http01
                .lock()
                .expect("fake store lock is available")
                .insert(key, record.clone());

            Ok(record)
        })
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        key: control_plane::Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::Http01ChallengeRecord>>> {
        Box::pin(async move {
            Ok(self
                .http01
                .lock()
                .expect("fake store lock is available")
                .get(&(key.host().as_str().to_owned(), key.token().to_owned()))
                .cloned()
                .filter(|record| record.expires_at() > SystemTime::now()))
        })
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        request: control_plane::DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            Ok(self
                .http01
                .lock()
                .expect("fake store lock is available")
                .remove(&(
                    request.key().host().as_str().to_owned(),
                    request.key().token().to_owned(),
                ))
                .is_some())
        })
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        request: control_plane::ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        Box::pin(async move {
            let mut records = self.http01.lock().expect("fake store lock is available");
            let expired_keys = records
                .iter()
                .filter(|(_, record)| record.expires_at() <= request.now)
                .take(request.limit.unwrap_or(usize::MAX))
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let expired = expired_keys.len();
            for key in expired_keys {
                records.remove(&key);
            }

            Ok(expired)
        })
    }
}
