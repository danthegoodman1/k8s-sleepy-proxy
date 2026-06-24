use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{BufMut, Bytes, BytesMut};
use control_plane::api::{
    operator_grpc_server_builder, operator_grpc_service, operator_grpc_service_with_store,
    operator_grpc_web_cors_layer, operator_grpc_web_server_builder,
    pb::{
        operator_control_plane_server::{OperatorControlPlane, OperatorControlPlaneServer},
        persistent_volume_source_template, route_identity, template_text_part,
        ContainerPortTemplate, ContainerTemplate, CreateInstanceRequest, CreateRouteBindingRequest,
        CreateWorkloadClassVersionRequest, CsiVolumeSourceTemplate, DeleteHttp01ChallengeRequest,
        DeleteHttp01ChallengeResponse, DeleteInstanceRequest, DeleteInstanceResponse,
        DeleteRouteBindingRequest, DeleteRouteBindingResponse, EnvVarTemplate,
        ExpireHttp01ChallengesRequest, GetInstanceRequest, GetRouteBindingRequest,
        GetWorkloadClassVersionRequest, HostPathVolumeSourceTemplate, Http01Challenge,
        Http01ChallengeKey, HttpRouteIdentity, IdleTimeoutOverridePolicy, Instance, InstanceState,
        ManifestTemplate, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
        PersistentVolumeSourceTemplate, ProtocolRoute, PutHttp01ChallengeRequest,
        ResolveHttp01ChallengeRequest, ResolveHttp01ChallengeResponse, RouteBinding, RouteHost,
        RouteHostKind, RouteIdentity, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
        SniRouteIdentity, TemplateText, TemplateTextPart, VolumeTemplate, WorkloadClassVersion,
        WorkloadClassVersionRef, WorkloadExclusivityKey, WorkloadKind, WorkloadSleepPolicy,
        WorkloadTemplate, WorkloadValueFieldRule, WorkloadValueSchema,
    },
    OperatorApiPlaceholder, StoreBackedOperatorApi, OPERATOR_SERVICE_NAME, OPERATOR_UNARY_METHODS,
};
use control_plane::{
    BackendEndpoint, BackendGeneration, ControlPlaneStore, CreateInstanceResult, Generation,
    InstanceId, InstanceRecord, InstanceState as DomainInstanceState, KubernetesClientError,
    KubernetesClientFuture, KubernetesClientResult, KubernetesMaterializer,
    KubernetesMaterializerClient, MaterializationId, MaterializationRecord, MaterializationState,
    MaterializationTarget, RenderedObjectRef, StoreError, StoreFuture, StoreResult,
};
use http_body_util::{BodyExt, Full};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::http::{header, HeaderMap, HeaderName, HeaderValue, Method, Request, Version};
use tonic::server::NamedService;
use tonic::{Code, Response, Status};
use tower::{Layer, ServiceExt};

fn store_operator_api(
    store: Arc<dyn ControlPlaneStore>,
) -> StoreBackedOperatorApi<FakeKubernetesClient> {
    StoreBackedOperatorApi::new(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target(),
    )
}

fn store_operator_grpc_service(
    store: Arc<dyn ControlPlaneStore>,
) -> control_plane::api::server::StoreBackedOperatorGrpcService<FakeKubernetesClient> {
    operator_grpc_service_with_store(
        store,
        KubernetesMaterializer::new(FakeKubernetesClient::default()),
        target(),
    )
}

fn operator_materializer(
    client: FakeKubernetesClient,
) -> KubernetesMaterializer<FakeKubernetesClient> {
    KubernetesMaterializer::new(client)
}

fn target() -> MaterializationTarget {
    MaterializationTarget::new("cluster-a", "apps").expect("target is valid")
}

fn domain_instance(id: &str, state: DomainInstanceState, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: InstanceId::new(id).expect("instance id is valid"),
        workload_class: control_plane::WorkloadClassVersionRef::new(
            control_plane::WorkloadClassId::new("class-1").expect("class id is valid"),
            Generation::new(1),
        ),
        values: Default::default(),
        state,
        generation: Generation::new(generation),
    }
}

fn ready_materialization(instance_id: &str, generation: u64) -> MaterializationRecord {
    materialization(
        instance_id,
        generation,
        target(),
        MaterializationState::Ready,
    )
}

fn materialization(
    instance_id: &str,
    generation: u64,
    target: MaterializationTarget,
    state: MaterializationState,
) -> MaterializationRecord {
    MaterializationRecord {
        id: MaterializationId::new(format!(
            "{}:{}:{}",
            instance_id,
            target.cluster_id(),
            target.namespace()
        ))
        .expect("materialization id is valid"),
        instance_id: InstanceId::new(instance_id).expect("instance id is valid"),
        instance_generation: Generation::new(generation),
        target,
        state,
        backend: Some(BackendEndpoint::new("http://example").expect("backend is valid")),
        backend_generation: BackendGeneration::new(generation),
        rendered_objects: vec![
            object_ref("v1", "Service", "apps", &format!("{instance_id}-svc")),
            object_ref("apps/v1", "Deployment", "apps", instance_id),
        ],
        exclusivity_keys: vec![],
    }
}

fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    }
}

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
        sleep_policy: Some(sleep_policy_proto()),
        exclusivity_keys: vec![],
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
    assert_eq!(
        workload_class
            .sleep_policy
            .as_ref()
            .expect("sleep policy is present")
            .idle_timeout_ms,
        300_000
    );
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
    let service = store_operator_api(store);

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
async fn store_backed_create_instance_rejects_invalid_kubernetes_instance_id() {
    let store = Arc::new(FakeInstanceStore::default());
    let service = store_operator_api(store);

    let error = service
        .create_instance(tonic::Request::new(CreateInstanceRequest {
            idempotency_key: "create-invalid-instance".to_owned(),
            instance_id: "Tenant_One".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-1".to_owned(),
                version: 7,
            }),
            values: [("tenant".to_owned(), "tenant-one".to_owned())].into(),
        }))
        .await
        .expect_err("invalid instance IDs are rejected at the API boundary");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("Kubernetes DNS label"));
}

#[tokio::test]
async fn operator_delete_cleans_active_materialization_before_store_delete() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-active",
        DomainInstanceState::Running,
        7,
    ));
    store.seed_materialization(ready_materialization("instance-delete-active", 7));
    let client = FakeKubernetesClient::default();
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-active".to_owned(),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();

    assert!(response.deleted);
    assert!(!store.instance_exists("instance-delete-active"));
    assert!(store.materialization().is_none());
    assert_eq!(
        client.deleted(),
        vec![
            object_ref("v1", "Service", "apps", "instance-delete-active-svc"),
            object_ref("apps/v1", "Deployment", "apps", "instance-delete-active"),
        ]
    );
    assert_eq!(store.delete_requests().len(), 1);
}

#[tokio::test]
async fn operator_delete_without_active_materialization_deletes_store_only() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-cold",
        DomainInstanceState::Cold,
        0,
    ));
    let client = FakeKubernetesClient::default();
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-cold".to_owned(),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();

    assert!(response.deleted);
    assert_eq!(client.deleted(), Vec::<RenderedObjectRef>::new());

    let missing = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-cold".to_owned(),
        }))
        .await
        .expect("missing delete remains idempotent")
        .into_inner();

    assert!(!missing.deleted);
    assert_eq!(client.deleted(), Vec::<RenderedObjectRef>::new());
}

#[tokio::test]
async fn operator_delete_ignores_materialization_for_other_target() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-other-target",
        DomainInstanceState::Running,
        3,
    ));
    store.seed_materialization(materialization(
        "instance-delete-other-target",
        3,
        MaterializationTarget::new("cluster-b", "apps").expect("target is valid"),
        MaterializationState::Ready,
    ));
    let client = FakeKubernetesClient::default();
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-other-target".to_owned(),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();

    assert!(response.deleted);
    assert_eq!(client.deleted(), Vec::<RenderedObjectRef>::new());
}

#[tokio::test]
async fn operator_delete_cleans_stale_generation_active_materialization_for_target() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-stale-generation",
        DomainInstanceState::Running,
        9,
    ));
    store.seed_materialization(ready_materialization("instance-delete-stale-generation", 8));
    let client = FakeKubernetesClient::default();
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let response = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-stale-generation".to_owned(),
        }))
        .await
        .expect("delete instance succeeds")
        .into_inner();

    assert!(response.deleted);
    assert!(!store.instance_exists("instance-delete-stale-generation"));
    assert!(store.materialization().is_none());
    assert_eq!(
        client.deleted(),
        vec![
            object_ref(
                "v1",
                "Service",
                "apps",
                "instance-delete-stale-generation-svc"
            ),
            object_ref(
                "apps/v1",
                "Deployment",
                "apps",
                "instance-delete-stale-generation"
            ),
        ]
    );
}

#[tokio::test]
async fn operator_delete_kubernetes_failure_preserves_store_state_for_retry() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-fails",
        DomainInstanceState::Running,
        5,
    ));
    store.seed_materialization(ready_materialization("instance-delete-fails", 5));
    let client = FakeKubernetesClient::default();
    client.fail_delete(object_ref(
        "v1",
        "Service",
        "apps",
        "instance-delete-fails-svc",
    ));
    let service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(client.clone()),
        target(),
    );

    let error = service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-fails".to_owned(),
        }))
        .await
        .expect_err("Kubernetes cleanup failure rejects delete");

    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("delete cleanup failed"));
    assert!(store.instance_exists("instance-delete-fails"));
    assert_eq!(store.delete_requests(), Vec::new());
    assert_eq!(
        client.deleted(),
        vec![object_ref(
            "v1",
            "Service",
            "apps",
            "instance-delete-fails-svc"
        ),]
    );
}

#[tokio::test]
async fn operator_delete_restart_replays_cleanup_then_finalizes_store_delete() {
    let store = Arc::new(FakeInstanceStore::default());
    store.seed_instance(domain_instance(
        "instance-delete-retry",
        DomainInstanceState::Running,
        8,
    ));
    store.seed_materialization(ready_materialization("instance-delete-retry", 8));
    let failing_client = FakeKubernetesClient::default();
    failing_client.fail_delete(object_ref(
        "v1",
        "Service",
        "apps",
        "instance-delete-retry-svc",
    ));
    let failing_service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(failing_client),
        target(),
    );
    failing_service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-retry".to_owned(),
        }))
        .await
        .expect_err("first cleanup attempt fails");

    let retry_client = FakeKubernetesClient::default();
    let retry_service = StoreBackedOperatorApi::new(
        store.clone(),
        operator_materializer(retry_client.clone()),
        target(),
    );
    let response = retry_service
        .delete_instance(tonic::Request::new(DeleteInstanceRequest {
            instance_id: "instance-delete-retry".to_owned(),
        }))
        .await
        .expect("retry cleanup succeeds")
        .into_inner();

    assert!(response.deleted);
    assert!(!store.instance_exists("instance-delete-retry"));
    assert!(store.materialization().is_none());
    assert_eq!(
        retry_client.deleted(),
        vec![
            object_ref("v1", "Service", "apps", "instance-delete-retry-svc"),
            object_ref("apps/v1", "Deployment", "apps", "instance-delete-retry"),
        ]
    );
}

#[tokio::test]
async fn store_backed_operator_methods_cover_workload_routes_and_http01() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
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
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect("create workload class succeeds")
        .into_inner();
    assert_eq!(created_class.template_generation, 9);
    assert_eq!(created_class.template, Some(template.clone()));
    assert_eq!(created_class.sleep_policy, Some(sleep_policy_proto()));

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
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("template is required");
    assert_eq!(missing_template.code(), Code::InvalidArgument);
    assert!(missing_template.message().contains("template is required"));

    let missing_policy = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-class-missing-policy".to_owned(),
            class_id: "class-missing-policy".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: Some(template.clone()),
            sleep_policy: None,
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("sleep policy is required");
    assert_eq!(missing_policy.code(), Code::InvalidArgument);
    assert!(missing_policy
        .message()
        .contains("sleep_policy is required"));

    let invalid_policy = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-class-invalid-policy".to_owned(),
            class_id: "class-invalid-policy".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: Some(template.clone()),
            sleep_policy: Some(WorkloadSleepPolicy {
                idle_timeout_ms: 0,
                idle_retry_backoff_ms: 5_000,
                drain_grace_timeout_ms: 30_000,
                idle_timeout_override: None,
            }),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("invalid sleep policy is rejected");
    assert_eq!(invalid_policy.code(), Code::InvalidArgument);
    assert!(invalid_policy
        .message()
        .contains("sleep_policy.idle_timeout_ms"));

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
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
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
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
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
async fn store_backed_workload_class_api_round_trips_exclusivity_keys() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let key = WorkloadExclusivityKey {
        name: "disk".to_owned(),
        value: Some(TemplateText {
            parts: vec![TemplateTextPart {
                kind: Some(template_text_part::Kind::InstanceValue(
                    "volume_handle".to_owned(),
                )),
            }],
        }),
    };

    let created = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-exclusive-class".to_owned(),
            class_id: "exclusive-class".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: Some(WorkloadValueSchema {
                fields: [(
                    "volume_handle".to_owned(),
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
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![key.clone()],
        }))
        .await
        .expect("create workload class with exclusivity key succeeds")
        .into_inner();

    assert_eq!(created.exclusivity_keys, vec![key.clone()]);

    let loaded = service
        .get_workload_class_version(tonic::Request::new(GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "exclusive-class".to_owned(),
                version: 1,
            }),
        }))
        .await
        .expect("get workload class with exclusivity key succeeds")
        .into_inner();

    assert_eq!(loaded, created);
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_invalid_exclusivity_keys() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
    let valid_value = TemplateText {
        parts: vec![TemplateTextPart {
            kind: Some(template_text_part::Kind::InstanceValue(
                "volume_handle".to_owned(),
            )),
        }],
    };

    let invalid_name = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-invalid-exclusive-name".to_owned(),
            class_id: "invalid-exclusive-name".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk/name".to_owned(),
                value: Some(valid_value.clone()),
            }],
        }))
        .await
        .expect_err("unsafe key name is rejected");
    assert_eq!(invalid_name.code(), Code::InvalidArgument);
    assert!(invalid_name.message().contains("exclusivity key"));

    let missing_value = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-missing-exclusive-value".to_owned(),
            class_id: "missing-exclusive-value".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk".to_owned(),
                value: None,
            }],
        }))
        .await
        .expect_err("missing key value template is rejected");
    assert_eq!(missing_value.code(), Code::InvalidArgument);
    assert!(missing_value.message().contains("exclusivity_keys.value"));

    let empty_parts = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-empty-exclusive-value".to_owned(),
            class_id: "empty-exclusive-value".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk".to_owned(),
                value: Some(TemplateText { parts: vec![] }),
            }],
        }))
        .await
        .expect_err("empty key value template is rejected");
    assert_eq!(empty_parts.code(), Code::InvalidArgument);
    assert!(empty_parts.message().contains("template text parts"));

    let blank_instance_value = service
        .create_workload_class_version(tonic::Request::new(CreateWorkloadClassVersionRequest {
            idempotency_key: "create-blank-exclusive-value-field".to_owned(),
            class_id: "blank-exclusive-value-field".to_owned(),
            version: 1,
            default_values: Default::default(),
            value_schema: None,
            template_generation: 1,
            template: Some(stateful_manifest_template_proto()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![WorkloadExclusivityKey {
                name: "disk".to_owned(),
                value: Some(TemplateText {
                    parts: vec![TemplateTextPart {
                        kind: Some(template_text_part::Kind::InstanceValue(" ".to_owned())),
                    }],
                }),
            }],
        }))
        .await
        .expect_err("blank key value field is rejected");
    assert_eq!(blank_instance_value.code(), Code::InvalidArgument);
    assert!(blank_instance_value
        .message()
        .contains("instance value field names"));
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_empty_template_static_strings() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
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
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("empty static template names are rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("template.container.name"));
}

#[tokio::test]
async fn store_backed_workload_class_api_rejects_empty_template_text_parts() {
    let service = store_operator_api(Arc::new(FakeInstanceStore::default()));
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
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        }))
        .await
        .expect_err("empty template text parts are rejected");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("template text parts"));
}

#[tokio::test]
async fn native_grpc_request_dispatches_to_store_backed_create_instance() {
    let response = store_operator_grpc_service(Arc::new(FakeInstanceStore::default()))
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
    let response = store_operator_grpc_service(Arc::new(FakeInstanceStore::default()))
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
    let _router = operator_grpc_server_builder().add_service(store_operator_grpc_service(
        Arc::new(FakeInstanceStore::default()),
    ));

    assert_eq!(
        <control_plane::api::server::StoreBackedOperatorGrpcService<
            FakeKubernetesClient,
        > as NamedService>::NAME,
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
async fn grpc_web_store_backed_requests_cover_operator_api_parity() {
    let store: Arc<dyn ControlPlaneStore> = Arc::new(FakeInstanceStore::default());
    let template = stateful_manifest_template_proto();

    let created_class: WorkloadClassVersion = grpc_web_store_unary(
        Arc::clone(&store),
        "CreateWorkloadClassVersion",
        CreateWorkloadClassVersionRequest {
            idempotency_key: "grpc-web-create-class".to_owned(),
            class_id: "class-grpc-web".to_owned(),
            version: 11,
            default_values: [("image".to_owned(), "example/app:grpc-web".to_owned())].into(),
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
            template_generation: 5,
            template: Some(template.clone()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        },
    )
    .await;
    assert_eq!(created_class.template_generation, 5);

    let loaded_class: WorkloadClassVersion = grpc_web_store_unary(
        Arc::clone(&store),
        "GetWorkloadClassVersion",
        GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "class-grpc-web".to_owned(),
                version: 11,
            }),
        },
    )
    .await;
    assert_eq!(loaded_class, created_class);

    let created_instance: Instance = grpc_web_store_unary(
        Arc::clone(&store),
        "CreateInstance",
        CreateInstanceRequest {
            idempotency_key: "grpc-web-create-instance".to_owned(),
            instance_id: "instance-grpc-web".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-grpc-web".to_owned(),
                version: 11,
            }),
            values: [("tenant".to_owned(), "acme".to_owned())].into(),
        },
    )
    .await;
    assert_eq!(created_instance.instance_id, "instance-grpc-web");
    assert_eq!(created_instance.state, InstanceState::Cold as i32);

    let loaded_instance: Instance = grpc_web_store_unary(
        Arc::clone(&store),
        "GetInstance",
        GetInstanceRequest {
            instance_id: "instance-grpc-web".to_owned(),
        },
    )
    .await;
    assert_eq!(loaded_instance, created_instance);

    let created_route: RouteBinding = grpc_web_store_unary(
        Arc::clone(&store),
        "CreateRouteBinding",
        CreateRouteBindingRequest {
            idempotency_key: "grpc-web-create-route".to_owned(),
            route_binding_id: "route-grpc-web".to_owned(),
            instance_id: "instance-grpc-web".to_owned(),
            identity: Some(http_identity("App.Example.COM.", Some("/app"))),
            protocol: ProtocolRoute::Http as i32,
        },
    )
    .await;
    assert_eq!(created_route.route_binding_id, "route-grpc-web");

    let loaded_route: RouteBinding = grpc_web_store_unary(
        Arc::clone(&store),
        "GetRouteBinding",
        GetRouteBindingRequest {
            route_binding_id: "route-grpc-web".to_owned(),
        },
    )
    .await;
    assert_eq!(loaded_route, created_route);

    let challenge: Http01Challenge = grpc_web_store_unary(
        Arc::clone(&store),
        "PutHttp01Challenge",
        PutHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "Acme.Example.COM.".to_owned(),
                token: "token-grpc-web".to_owned(),
            }),
            key_authorization: "key-auth-grpc-web".to_owned(),
            expires_at_unix_millis: 4_102_444_800_000,
        },
    )
    .await;
    assert_eq!(
        challenge.key.as_ref().expect("HTTP-01 key returned").host,
        "acme.example.com"
    );

    let resolved: ResolveHttp01ChallengeResponse = grpc_web_store_unary(
        Arc::clone(&store),
        "ResolveHttp01Challenge",
        ResolveHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "acme.example.com".to_owned(),
                token: "token-grpc-web".to_owned(),
            }),
        },
    )
    .await;
    assert_eq!(
        resolved
            .challenge
            .as_ref()
            .expect("HTTP-01 challenge resolves")
            .key_authorization,
        "key-auth-grpc-web"
    );

    let deleted_challenge: DeleteHttp01ChallengeResponse = grpc_web_store_unary(
        Arc::clone(&store),
        "DeleteHttp01Challenge",
        DeleteHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "acme.example.com".to_owned(),
                token: "token-grpc-web".to_owned(),
            }),
        },
    )
    .await;
    assert!(deleted_challenge.deleted);

    let deleted_route: DeleteRouteBindingResponse = grpc_web_store_unary(
        Arc::clone(&store),
        "DeleteRouteBinding",
        DeleteRouteBindingRequest {
            route_binding_id: "route-grpc-web".to_owned(),
        },
    )
    .await;
    assert!(deleted_route.deleted);

    let deleted_instance: DeleteInstanceResponse = grpc_web_store_unary(
        store,
        "DeleteInstance",
        DeleteInstanceRequest {
            instance_id: "instance-grpc-web".to_owned(),
        },
    )
    .await;
    assert!(deleted_instance.deleted);
}

#[tokio::test]
async fn native_grpc_store_backed_requests_cover_operator_api_parity() {
    let store: Arc<dyn ControlPlaneStore> = Arc::new(FakeInstanceStore::default());
    let template = stateful_manifest_template_proto();

    let created_class: WorkloadClassVersion = grpc_store_unary(
        Arc::clone(&store),
        "CreateWorkloadClassVersion",
        CreateWorkloadClassVersionRequest {
            idempotency_key: "native-create-class".to_owned(),
            class_id: "class-native".to_owned(),
            version: 13,
            default_values: [("image".to_owned(), "example/app:native".to_owned())].into(),
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
            template_generation: 6,
            template: Some(template.clone()),
            sleep_policy: Some(sleep_policy_proto()),
            exclusivity_keys: vec![],
        },
    )
    .await;
    assert_eq!(created_class.template_generation, 6);

    let loaded_class: WorkloadClassVersion = grpc_store_unary(
        Arc::clone(&store),
        "GetWorkloadClassVersion",
        GetWorkloadClassVersionRequest {
            reference: Some(WorkloadClassVersionRef {
                class_id: "class-native".to_owned(),
                version: 13,
            }),
        },
    )
    .await;
    assert_eq!(loaded_class, created_class);

    let created_instance: Instance = grpc_store_unary(
        Arc::clone(&store),
        "CreateInstance",
        CreateInstanceRequest {
            idempotency_key: "native-create-instance".to_owned(),
            instance_id: "instance-native".to_owned(),
            workload_class: Some(WorkloadClassVersionRef {
                class_id: "class-native".to_owned(),
                version: 13,
            }),
            values: [("tenant".to_owned(), "acme".to_owned())].into(),
        },
    )
    .await;
    assert_eq!(created_instance.instance_id, "instance-native");
    assert_eq!(created_instance.state, InstanceState::Cold as i32);

    let loaded_instance: Instance = grpc_store_unary(
        Arc::clone(&store),
        "GetInstance",
        GetInstanceRequest {
            instance_id: "instance-native".to_owned(),
        },
    )
    .await;
    assert_eq!(loaded_instance, created_instance);

    let created_route: RouteBinding = grpc_store_unary(
        Arc::clone(&store),
        "CreateRouteBinding",
        CreateRouteBindingRequest {
            idempotency_key: "native-create-route".to_owned(),
            route_binding_id: "route-native".to_owned(),
            instance_id: "instance-native".to_owned(),
            identity: Some(http_identity("Native.Example.COM.", Some("/app"))),
            protocol: ProtocolRoute::Http as i32,
        },
    )
    .await;
    assert_eq!(created_route.route_binding_id, "route-native");

    let loaded_route: RouteBinding = grpc_store_unary(
        Arc::clone(&store),
        "GetRouteBinding",
        GetRouteBindingRequest {
            route_binding_id: "route-native".to_owned(),
        },
    )
    .await;
    assert_eq!(loaded_route, created_route);

    let challenge: Http01Challenge = grpc_store_unary(
        Arc::clone(&store),
        "PutHttp01Challenge",
        PutHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "Native-Acme.Example.COM.".to_owned(),
                token: "token-native".to_owned(),
            }),
            key_authorization: "key-auth-native".to_owned(),
            expires_at_unix_millis: 4_102_444_800_000,
        },
    )
    .await;
    assert_eq!(
        challenge.key.as_ref().expect("HTTP-01 key returned").host,
        "native-acme.example.com"
    );

    let resolved: ResolveHttp01ChallengeResponse = grpc_store_unary(
        Arc::clone(&store),
        "ResolveHttp01Challenge",
        ResolveHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "native-acme.example.com".to_owned(),
                token: "token-native".to_owned(),
            }),
        },
    )
    .await;
    assert_eq!(
        resolved
            .challenge
            .as_ref()
            .expect("HTTP-01 challenge resolves")
            .key_authorization,
        "key-auth-native"
    );

    let deleted_challenge: DeleteHttp01ChallengeResponse = grpc_store_unary(
        Arc::clone(&store),
        "DeleteHttp01Challenge",
        DeleteHttp01ChallengeRequest {
            key: Some(Http01ChallengeKey {
                host: "native-acme.example.com".to_owned(),
                token: "token-native".to_owned(),
            }),
        },
    )
    .await;
    assert!(deleted_challenge.deleted);

    let deleted_route: DeleteRouteBindingResponse = grpc_store_unary(
        Arc::clone(&store),
        "DeleteRouteBinding",
        DeleteRouteBindingRequest {
            route_binding_id: "route-native".to_owned(),
        },
    )
    .await;
    assert!(deleted_route.deleted);

    let deleted_instance: DeleteInstanceResponse = grpc_store_unary(
        store,
        "DeleteInstance",
        DeleteInstanceRequest {
            instance_id: "instance-native".to_owned(),
        },
    )
    .await;
    assert!(deleted_instance.deleted);
}

#[tokio::test]
async fn grpc_web_cors_preflight_allows_browser_operator_headers() {
    let response = operator_grpc_web_cors_layer()
        .layer(
            tonic_web::GrpcWebLayer::new().layer(store_operator_grpc_service(Arc::new(
                FakeInstanceStore::default(),
            ))),
        )
        .oneshot(grpc_web_preflight_request("CreateInstance"))
        .await
        .expect("CORS preflight should be handled by grpc-web router");

    assert!(response.status().is_success());
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .expect("allow-origin is returned"),
        "*"
    );
    let allow_methods = response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_METHODS)
        .expect("allow-methods is returned")
        .to_str()
        .expect("allow-methods is valid");
    assert!(allow_methods.contains("POST"));
    assert!(allow_methods.contains("OPTIONS"));

    let allow_headers = response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
        .expect("allow-headers is returned")
        .to_str()
        .expect("allow-headers is valid")
        .to_ascii_lowercase();
    assert!(allow_headers.contains("authorization"));
    assert!(allow_headers.contains("content-type"));
    assert!(allow_headers.contains("x-grpc-web"));
    assert!(allow_headers.contains("x-sleepypods-operator"));
}

#[tokio::test]
async fn grpc_web_preserves_operator_metadata_headers_at_api_boundary() {
    let captured_metadata = Arc::new(Mutex::new(Vec::new()));
    let service = tonic_web::GrpcWebLayer::new().layer(OperatorControlPlaneServer::new(
        MetadataCapturingOperatorApi::new(Arc::clone(&captured_metadata)),
    ));

    let response = service
        .oneshot(grpc_web_operator_request(
            "CreateInstance",
            CreateInstanceRequest {
                idempotency_key: "metadata-create-instance".to_owned(),
                instance_id: "metadata-instance".to_owned(),
                workload_class: Some(WorkloadClassVersionRef {
                    class_id: "metadata-class".to_owned(),
                    version: 1,
                }),
                values: Default::default(),
            },
        ))
        .await
        .expect("gRPC-Web request with metadata should reach service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("metadata response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");

    let captured = captured_metadata
        .lock()
        .expect("captured metadata lock is available");
    assert!(captured
        .iter()
        .any(|(name, value)| { name == "authorization" && value == "Bearer operator-token" }));
    assert!(captured
        .iter()
        .any(|(name, value)| { name == "x-sleepypods-operator" && value == "tenant-a" }));
}

#[tokio::test]
async fn grpc_web_store_backed_errors_return_structured_status() {
    let response = operator_grpc_web_cors_layer()
        .layer(
            tonic_web::GrpcWebLayer::new().layer(store_operator_grpc_service(Arc::new(
                FakeInstanceStore::default(),
            ))),
        )
        .oneshot(grpc_web_operator_request(
            "GetInstance",
            GetInstanceRequest {
                instance_id: "missing-instance".to_owned(),
            },
        ))
        .await
        .expect("gRPC-Web missing instance request should route to store-backed service");
    let headers = response.headers().clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("error response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();

    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "5");
    let message = grpc_header_value(&headers, trailers.as_ref(), body.as_ref(), "grpc-message")
        .expect("structured grpc-message is returned");
    assert!(message.contains("instance%20not%20found"));
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
    assert!(!OPERATOR_UNARY_METHODS.contains(&"WakeInstance"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"Subscribe"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"ResolveRoute"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"LookupRouteDependencies"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"RecordMaterialization"));
    assert!(!OPERATOR_UNARY_METHODS.contains(&"CompareAndSwapInstanceState"));
}

async fn grpc_web_store_unary<M, R>(
    store: Arc<dyn ControlPlaneStore>,
    method: &'static str,
    request: R,
) -> M
where
    M: Message + Default,
    R: Message,
{
    let response = operator_grpc_web_cors_layer()
        .layer(tonic_web::GrpcWebLayer::new().layer(store_operator_grpc_service(store)))
        .oneshot(grpc_web_operator_request(method, request))
        .await
        .expect("gRPC-Web request should route through store-backed service");
    let headers = response.headers().clone();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .expect("gRPC-Web content type")
        .clone();
    let collected = response
        .into_body()
        .collect()
        .await
        .expect("gRPC-Web response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();

    assert_eq!(content_type, "application/grpc-web+proto");
    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");

    decode_grpc_message(body.as_ref())
}

async fn grpc_store_unary<M, R>(
    store: Arc<dyn ControlPlaneStore>,
    method: &'static str,
    request: R,
) -> M
where
    M: Message + Default,
    R: Message,
{
    let response = store_operator_grpc_service(store)
        .oneshot(grpc_operator_unary_request(
            request,
            method,
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
        .expect("native gRPC response body should collect");
    let trailers = collected.trailers().cloned();
    let body = collected.to_bytes();

    assert_grpc_status(&headers, trailers.as_ref(), body.as_ref(), "0");

    decode_grpc_message(body.as_ref())
}

fn grpc_request(content_type: &'static str, version: Version) -> Request<Body> {
    grpc_create_instance_request(CreateInstanceRequest::default(), content_type, version)
}

fn grpc_create_instance_request(
    request: CreateInstanceRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_operator_unary_request(request, "CreateInstance", content_type, version)
}

fn grpc_create_route_binding_request(
    request: CreateRouteBindingRequest,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_operator_unary_request(request, "CreateRouteBinding", content_type, version)
}

fn grpc_web_operator_request<M: Message>(method: &'static str, request: M) -> Request<Body> {
    let mut request = grpc_operator_unary_request(
        request,
        method,
        "application/grpc-web+proto",
        Version::HTTP_11,
    );
    request.headers_mut().insert(
        header::ORIGIN,
        HeaderValue::from_static("https://operator.example"),
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-grpc-web"),
        HeaderValue::from_static("1"),
    );
    request.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer operator-token"),
    );
    request.headers_mut().insert(
        HeaderName::from_static("x-sleepypods-operator"),
        HeaderValue::from_static("tenant-a"),
    );
    request
}

fn grpc_web_preflight_request(method: &'static str) -> Request<Body> {
    Request::builder()
        .version(Version::HTTP_11)
        .method(Method::OPTIONS)
        .uri(operator_method_uri(method))
        .header(header::ORIGIN, "https://operator.example")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            "authorization,content-type,x-grpc-web,x-sleepypods-operator",
        )
        .body(Body::new(Full::new(Bytes::new())))
        .expect("preflight request builds")
}

fn grpc_operator_unary_request<M: Message>(
    request: M,
    method: &'static str,
    content_type: &'static str,
    version: Version,
) -> Request<Body> {
    grpc_unary_request(request, operator_method_uri(method), content_type, version)
}

fn grpc_unary_request<M: Message>(
    request: M,
    uri: String,
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

fn operator_method_uri(method: &'static str) -> String {
    format!("/{OPERATOR_SERVICE_NAME}/{method}")
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

fn assert_grpc_status(
    headers: &HeaderMap,
    trailers: Option<&HeaderMap>,
    body: &[u8],
    expected: &str,
) {
    let status =
        grpc_header_value(headers, trailers, body, "grpc-status").expect("gRPC status is returned");
    assert_eq!(status, expected);
}

fn grpc_header_value(
    headers: &HeaderMap,
    trailers: Option<&HeaderMap>,
    body: &[u8],
    name: &str,
) -> Option<String> {
    headers
        .get(name)
        .or_else(|| trailers.and_then(|trailers| trailers.get(name)))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .or_else(|| grpc_web_trailer_value(body, name))
}

fn grpc_web_trailer_value(bytes: &[u8], name: &str) -> Option<String> {
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let frame_type = bytes[offset];
        let length = u32::from_be_bytes(
            bytes[offset + 1..offset + 5]
                .try_into()
                .expect("gRPC-Web frame has a length prefix"),
        ) as usize;
        offset += 5;
        if offset + length > bytes.len() {
            return None;
        }

        if frame_type & 0x80 != 0 {
            let trailers = std::str::from_utf8(&bytes[offset..offset + length]).ok()?;
            for line in trailers.split("\r\n").filter(|line| !line.is_empty()) {
                let (key, value) = line.split_once(':')?;
                if key.eq_ignore_ascii_case(name) {
                    return Some(value.trim_start().to_owned());
                }
            }
        }
        offset += length;
    }

    None
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
            mode: None,
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

fn sleep_policy_proto() -> WorkloadSleepPolicy {
    WorkloadSleepPolicy {
        idle_timeout_ms: 300_000,
        idle_retry_backoff_ms: 5_000,
        drain_grace_timeout_ms: 30_000,
        idle_timeout_override: Some(IdleTimeoutOverridePolicy {
            value_field: "idle_ms".to_owned(),
            min_idle_timeout_ms: 60_000,
            max_idle_timeout_ms: 600_000,
        }),
    }
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

#[derive(Clone)]
struct MetadataCapturingOperatorApi {
    captured: Arc<Mutex<Vec<(String, String)>>>,
}

impl MetadataCapturingOperatorApi {
    fn new(captured: Arc<Mutex<Vec<(String, String)>>>) -> Self {
        Self { captured }
    }

    fn capture_metadata<T>(&self, request: &tonic::Request<T>) {
        let mut captured = self
            .captured
            .lock()
            .expect("captured metadata lock is available");
        for name in ["authorization", "x-sleepypods-operator"] {
            if let Some(value) = request
                .metadata()
                .get(name)
                .and_then(|value| value.to_str().ok())
            {
                captured.push((name.to_owned(), value.to_owned()));
            }
        }
    }
}

#[tonic::async_trait]
impl OperatorControlPlane for MetadataCapturingOperatorApi {
    async fn create_workload_class_version(
        &self,
        _request: tonic::Request<CreateWorkloadClassVersionRequest>,
    ) -> Result<Response<WorkloadClassVersion>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn get_workload_class_version(
        &self,
        _request: tonic::Request<GetWorkloadClassVersionRequest>,
    ) -> Result<Response<WorkloadClassVersion>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn create_instance(
        &self,
        request: tonic::Request<CreateInstanceRequest>,
    ) -> Result<Response<Instance>, Status> {
        self.capture_metadata(&request);

        Ok(Response::new(Instance::default()))
    }

    async fn get_instance(
        &self,
        _request: tonic::Request<GetInstanceRequest>,
    ) -> Result<Response<Instance>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn delete_instance(
        &self,
        _request: tonic::Request<DeleteInstanceRequest>,
    ) -> Result<Response<DeleteInstanceResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn create_route_binding(
        &self,
        _request: tonic::Request<CreateRouteBindingRequest>,
    ) -> Result<Response<RouteBinding>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn get_route_binding(
        &self,
        _request: tonic::Request<GetRouteBindingRequest>,
    ) -> Result<Response<RouteBinding>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn delete_route_binding(
        &self,
        _request: tonic::Request<DeleteRouteBindingRequest>,
    ) -> Result<Response<DeleteRouteBindingResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn put_http01_challenge(
        &self,
        _request: tonic::Request<PutHttp01ChallengeRequest>,
    ) -> Result<Response<Http01Challenge>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn resolve_http01_challenge(
        &self,
        _request: tonic::Request<ResolveHttp01ChallengeRequest>,
    ) -> Result<Response<ResolveHttp01ChallengeResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn delete_http01_challenge(
        &self,
        _request: tonic::Request<DeleteHttp01ChallengeRequest>,
    ) -> Result<Response<DeleteHttp01ChallengeResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }

    async fn expire_http01_challenges(
        &self,
        _request: tonic::Request<ExpireHttp01ChallengesRequest>,
    ) -> Result<Response<control_plane::api::pb::ExpireHttp01ChallengesResponse>, Status> {
        Err(Status::unimplemented(
            "metadata test only implements CreateInstance",
        ))
    }
}

#[derive(Default)]
struct FakeInstanceStore {
    instances: Mutex<BTreeMap<String, InstanceRecord>>,
    workload_classes: Mutex<BTreeMap<(String, u64), control_plane::WorkloadClassVersion>>,
    route_bindings: Mutex<BTreeMap<String, control_plane::RouteBindingRecord>>,
    materialization: Mutex<Option<MaterializationRecord>>,
    delete_requests: Mutex<Vec<control_plane::DeleteInstanceRequest>>,
    http01: Mutex<BTreeMap<(String, String), control_plane::Http01ChallengeRecord>>,
}

impl FakeInstanceStore {
    fn seed_instance(&self, instance: InstanceRecord) {
        self.instances
            .lock()
            .expect("fake store lock is available")
            .insert(instance.id.as_str().to_owned(), instance);
    }

    fn seed_materialization(&self, materialization: MaterializationRecord) {
        *self
            .materialization
            .lock()
            .expect("fake store lock is available") = Some(materialization);
    }

    fn instance_exists(&self, instance_id: &str) -> bool {
        self.instances
            .lock()
            .expect("fake store lock is available")
            .contains_key(instance_id)
    }

    fn delete_requests(&self) -> Vec<control_plane::DeleteInstanceRequest> {
        self.delete_requests
            .lock()
            .expect("fake store lock is available")
            .clone()
    }

    fn materialization(&self) -> Option<MaterializationRecord> {
        self.materialization
            .lock()
            .expect("fake store lock is available")
            .clone()
    }
}

#[derive(Clone, Debug, Default)]
struct FakeKubernetesClient {
    deleted: Arc<Mutex<Vec<RenderedObjectRef>>>,
    fail_delete: Arc<Mutex<Option<RenderedObjectRef>>>,
}

impl FakeKubernetesClient {
    fn fail_delete(&self, object: RenderedObjectRef) {
        *self
            .fail_delete
            .lock()
            .expect("fake client lock is available") = Some(object);
    }

    fn deleted(&self) -> Vec<RenderedObjectRef> {
        self.deleted
            .lock()
            .expect("fake client lock is available")
            .clone()
    }
}

impl KubernetesMaterializerClient for FakeKubernetesClient {
    fn apply_object<'a>(
        &'a self,
        _object: &'a control_plane::KubernetesObject,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.deleted
                .lock()
                .expect("fake client lock is available")
                .push(object.clone());
            if self
                .fail_delete
                .lock()
                .expect("fake client lock is available")
                .as_ref()
                == Some(object)
            {
                return Err(KubernetesClientError::new("delete failed"));
            }

            Ok(())
        })
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
            BackendEndpoint::new("http://example").map_err(|error| {
                KubernetesClientError::new(format!("invalid backend endpoint: {error}"))
            })
        })
    }
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
            self.delete_requests
                .lock()
                .expect("fake store lock is available")
                .push(request.clone());
            let deleted = self
                .instances
                .lock()
                .expect("fake store lock is available")
                .remove(request.instance_id.as_str())
                .is_some();
            if deleted {
                let mut materialization = self
                    .materialization
                    .lock()
                    .expect("fake store lock is available");
                if materialization.as_ref().is_some_and(|materialization| {
                    materialization.instance_id == request.instance_id
                }) {
                    *materialization = None;
                }
            }

            Ok(deleted)
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

    fn load_active_materialization<'a>(
        &'a self,
        request: control_plane::LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<control_plane::MaterializationRecord>>> {
        Box::pin(async move {
            Ok(self
                .materialization
                .lock()
                .expect("fake store lock is available")
                .as_ref()
                .filter(|materialization| {
                    materialization.instance_id == request.instance_id
                        && materialization.target == request.target
                        && materialization.state != MaterializationState::Deleted
                })
                .cloned())
        })
    }

    fn complete_wake<'a>(
        &'a self,
        _request: control_plane::CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<control_plane::CompleteWakeResult>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
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
