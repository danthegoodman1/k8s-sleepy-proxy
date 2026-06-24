use std::{
    collections::BTreeMap,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::materialization::{
    LoadActiveMaterializationRequest, LoadReadyMaterializationRequest,
};
use control_plane::{
    render_manifests, BackendEndpoint, BackendGeneration, BeginSleepRequest,
    CompareAndSwapInstanceStateRequest, CompleteWakeRequest, ContainerPortTemplate,
    ContainerTemplate, ControlPlaneStore, CreateInstanceRequest, CreateRouteBindingRequest,
    CreateWorkloadClassVersionRequest, DeleteInstanceRequest, DeleteRouteBindingRequest,
    EnvVarTemplate, ExpireHttp01ChallengesRequest, FinalizeSleepRequest, Generation,
    GetInstanceRequest, GetRouteBindingRequest, Http01ChallengeKey, IdempotencyKey,
    IdleTimeoutOverridePolicy, InstanceId, InstanceState, ManifestTemplate, MaterializationState,
    MaterializationTarget, PathPrefix, PostgresStore, PostgresStoreConfig, ProtocolRoute,
    PutHttp01ChallengeRequest, RecordMaterializationRequest, RenderManifestRequest,
    RenderedObjectRef, RouteBindingId, RouteBindingSpec, RouteDependencyLookup, RouteHost,
    RouteIdentity, RouteResolution, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
    StateTransitionReason, StoreError, TemplateText, TemplateTextPart, WorkloadClassId,
    WorkloadClassVersion, WorkloadClassVersionRef, WorkloadKind, WorkloadSleepPolicy,
    WorkloadTemplate, WorkloadValueFieldRule, WorkloadValueSchema,
};
use tokio_postgres::NoTls;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn postgres_provider_reports_invalid_connection_url() {
    let config = PostgresStoreConfig::new("http://example.com/not-postgres")
        .expect("non-empty URL reaches provider validation");
    let error = PostgresStore::connect(&config)
        .await
        .expect_err("provider rejects invalid Postgres URL");

    assert!(matches!(error, StoreError::InvalidArgument { .. }));
}

#[tokio::test]
async fn postgres_store_conformance_against_real_database() -> TestResult {
    let Ok(base_url) = std::env::var("SLEEPYPODS_POSTGRES_URL") else {
        eprintln!(
            "skipping Postgres store conformance; run with \
             SLEEPYPODS_POSTGRES_URL=postgres://user:pass@localhost/db \
             cargo test --package control-plane --test postgres_store -- --nocapture"
        );
        return Ok(());
    };

    let schema = unique_schema_name();
    let (admin, connection) = tokio_postgres::connect(&base_url, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("Postgres admin connection error: {error}");
        }
    });

    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await?;

    let store_url = connection_url_with_search_path(&base_url, &schema);
    let config = PostgresStoreConfig::new(store_url)?;
    let store = PostgresStore::connect(&config).await?;
    let result = run_conformance(&store).await;

    drop(store);
    let cleanup = admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    drop(admin);
    connection_task.abort();

    cleanup?;
    result.map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
}

async fn run_conformance(store: &PostgresStore) -> Result<(), StoreError> {
    store.run_migrations().await?;
    store.run_migrations().await?;

    let class = workload_class("class-a", 1);
    let created_class = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    assert_eq!(created_class, class);
    let loaded = store
        .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
            class.reference.clone(),
        ))
        .await?
        .expect("created workload class version loads");
    assert_eq!(loaded, class);
    let duplicate = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class.clone()))
        .await?;
    assert_eq!(duplicate, class);

    let mut changed_class = class.clone();
    changed_class.template_generation = Generation::new(2);
    let conflict = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(changed_class))
        .await
        .expect_err("same class/version with different contents is immutable");
    assert!(matches!(
        conflict,
        StoreError::AlreadyExists {
            resource: "workload class version"
        }
    ));
    let loaded_v1_after_conflict = store
        .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
            class.reference.clone(),
        ))
        .await?
        .expect("v1 still loads after conflicting create");
    assert_eq!(loaded_v1_after_conflict, class);

    let mut changed_template_class = class.clone();
    changed_template_class.template.workload.app_container.image =
        TemplateText::literal("example/app:changed");
    let template_conflict = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            changed_template_class,
        ))
        .await
        .expect_err("same class/version with a different template is immutable");
    assert!(matches!(
        template_conflict,
        StoreError::AlreadyExists {
            resource: "workload class version"
        }
    ));

    let mut changed_policy_class = class.clone();
    changed_policy_class.sleep_policy =
        WorkloadSleepPolicy::new(120_000, 5_000, 30_000).expect("valid sleep policy");
    let policy_conflict = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(changed_policy_class))
        .await
        .expect_err("same class/version with a different sleep policy is immutable");
    assert!(matches!(
        policy_conflict,
        StoreError::AlreadyExists {
            resource: "workload class version"
        }
    ));

    let class_v2 = workload_class("class-a", 2);
    let created_v2 = store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(class_v2.clone()))
        .await?;
    assert_eq!(created_v2, class_v2);
    let loaded_v1_after_v2 = store
        .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
            class.reference.clone(),
        ))
        .await?
        .expect("v1 still loads after v2 is created");
    assert_eq!(loaded_v1_after_v2, class);

    let missing_required = store
        .create_instance(CreateInstanceRequest::new(
            IdempotencyKey::new("idem-missing-required").expect("valid idempotency key"),
            InstanceId::new("instance-missing-required").expect("valid instance ID"),
            class.reference.clone(),
        ))
        .await
        .expect_err("missing required instance values are rejected");
    assert!(matches!(
        missing_required,
        StoreError::InvalidArgument { .. }
    ));

    let unknown_value = store
        .create_instance(
            create_instance_request(
                "idem-unknown-value",
                "instance-unknown-value",
                class.reference.clone(),
                vec![],
            )
            .with_values(BTreeMap::from([
                ("extra".to_owned(), "value".to_owned()),
                ("tenant".to_owned(), "instance-unknown-value".to_owned()),
            ])),
        )
        .await
        .expect_err("unknown instance values are rejected when schema disallows them");
    assert!(matches!(unknown_value, StoreError::InvalidArgument { .. }));

    let override_class = workload_class_with_idle_override("class-override", 1);
    store
        .create_workload_class_version(CreateWorkloadClassVersionRequest::new(
            override_class.clone(),
        ))
        .await?;
    let out_of_bounds_override = store
        .create_instance(
            create_instance_request(
                "idem-override-out-of-bounds",
                "instance-override-out-of-bounds",
                override_class.reference.clone(),
                vec![],
            )
            .with_values(BTreeMap::from([
                (
                    "tenant".to_owned(),
                    "instance-override-out-of-bounds".to_owned(),
                ),
                ("idle_ms".to_owned(), "50000".to_owned()),
            ])),
        )
        .await
        .expect_err("out-of-bounds idle override is rejected before persistence");
    assert!(matches!(
        out_of_bounds_override,
        StoreError::InvalidArgument { .. }
    ));

    let valid_override = store
        .create_instance(
            create_instance_request(
                "idem-override-valid",
                "instance-override-valid",
                override_class.reference.clone(),
                vec![],
            )
            .with_values(BTreeMap::from([
                ("tenant".to_owned(), "instance-override-valid".to_owned()),
                ("idle_ms".to_owned(), "120000".to_owned()),
            ])),
        )
        .await?;
    let resolved_override = override_class
        .sleep_policy
        .resolve(&valid_override.instance.values)
        .map_err(|error| StoreError::internal(error.to_string()))?;
    assert_eq!(resolved_override.idle_timeout_ms, 120_000);

    let create = create_instance_request(
        "idem-create-a",
        "instance-a",
        class.reference.clone(),
        vec![
            http_route("app.example.com", Some("/api")),
            sni_route("db.example.com"),
        ],
    );
    let created = store.create_instance(create.clone()).await?;
    assert_eq!(created.instance.id.as_str(), "instance-a");
    assert_eq!(created.instance.state, InstanceState::Cold);
    assert_eq!(created.instance.generation, Generation::new(0));
    assert_eq!(
        created.instance.values,
        BTreeMap::from([
            ("image".to_owned(), "example/app:1".to_owned()),
            ("tenant".to_owned(), "instance-a".to_owned()),
        ])
    );
    assert_eq!(created.route_bindings.len(), 2);
    assert!(!created.idempotency_replayed);
    let loaded_created = store
        .get_instance(GetInstanceRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
        ))
        .await?
        .expect("created instance loads through public store API");
    assert_eq!(loaded_created, created.instance);
    let rendered = render_manifests(RenderManifestRequest {
        template: &loaded.template,
        instance: &created.instance,
        sleep_policy: loaded
            .sleep_policy
            .resolve(&created.instance.values)
            .map_err(|error| StoreError::internal(error.to_string()))?,
        namespace: "apps",
        template_generation: Some(loaded.template_generation),
    })
    .map_err(|error| StoreError::internal(error.to_string()))?;
    assert!(
        rendered
            .objects
            .iter()
            .any(|object| object.object.name() == "app-instance-a"),
        "loaded workload class template should render durable instance manifests"
    );

    let replayed = store.create_instance(create.clone()).await?;
    assert!(replayed.idempotency_replayed);
    assert_eq!(replayed.instance, created.instance);
    assert_eq!(replayed.route_bindings, created.route_bindings);

    let explicit_default_replay = create.clone().with_values(BTreeMap::from([
        ("image".to_owned(), "example/app:1".to_owned()),
        ("tenant".to_owned(), "instance-a".to_owned()),
    ]));
    let replayed_with_explicit_default = store.create_instance(explicit_default_replay).await?;
    assert!(replayed_with_explicit_default.idempotency_replayed);
    assert_eq!(replayed_with_explicit_default.instance, created.instance);
    assert_eq!(
        replayed_with_explicit_default.route_bindings,
        created.route_bindings
    );

    let changed_values_conflict = store
        .create_instance(create.clone().with_values(BTreeMap::from([(
            "tenant".to_owned(),
            "different-tenant".to_owned(),
        )])))
        .await
        .expect_err("same idempotency key with different canonical values conflicts");
    assert!(matches!(
        changed_values_conflict,
        StoreError::IdempotencyConflict
    ));

    let conflict = store
        .create_instance(create_instance_request(
            "idem-create-a",
            "instance-conflict",
            class.reference.clone(),
            vec![http_route("conflict.example.com", None)],
        ))
        .await
        .expect_err("same idempotency key with a different payload conflicts");
    assert!(matches!(conflict, StoreError::IdempotencyConflict));

    let duplicate_route = store
        .create_instance(create_instance_request(
            "idem-rollback",
            "instance-rollback",
            class.reference.clone(),
            vec![http_route("app.example.com", Some("/api"))],
        ))
        .await
        .expect_err("duplicate route identity is rejected");
    assert!(matches!(
        duplicate_route,
        StoreError::AlreadyExists {
            resource: "route binding"
        }
    ));

    let recovered_after_rollback = store
        .create_instance(create_instance_request(
            "idem-rollback",
            "instance-rollback",
            class.reference.clone(),
            vec![http_route("rollback.example.com", None)],
        ))
        .await?;
    assert_eq!(
        recovered_after_rollback.instance.id.as_str(),
        "instance-rollback"
    );

    exercise_route_bindings(store, class.reference.clone()).await?;
    exercise_instance_lifecycle(store, class.reference.clone()).await?;
    exercise_complete_wake(store, class.reference.clone()).await?;
    exercise_rendered_object_ref_collision_rejection(store, class.reference.clone()).await?;

    let delete_target = store
        .create_instance(create_instance_request(
            "idem-delete",
            "instance-delete",
            class.reference.clone(),
            vec![],
        ))
        .await?;
    assert_eq!(delete_target.instance.generation, Generation::new(0));
    assert!(
        store
            .delete_instance(DeleteInstanceRequest::new(
                delete_target.instance.id.clone()
            ))
            .await?
    );
    assert!(store
        .get_instance(GetInstanceRequest::new(delete_target.instance.id.clone()))
        .await?
        .is_none());
    assert!(
        !store
            .delete_instance(DeleteInstanceRequest::new(delete_target.instance.id))
            .await?
    );

    let initial_resolution = store
        .resolve_route(RouteIdentity::Http {
            host: RouteHost::exact("APP.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        })
        .await?;
    let route_entry = match initial_resolution {
        RouteResolution::Resolved { entry, .. } => entry,
        RouteResolution::Miss { .. } => panic!("route should resolve"),
    };
    assert_eq!(route_entry.instance_id.as_str(), "instance-a");
    assert_eq!(route_entry.backend, None);

    let miss = store
        .resolve_route(RouteIdentity::Http {
            host: RouteHost::exact("missing.example.com").expect("valid host"),
            path: None,
        })
        .await?;
    assert!(matches!(miss, RouteResolution::Miss { .. }));

    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(waking.generation, Generation::new(1));
    assert_eq!(waking.state, InstanceState::Waking);

    let stale = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(0),
            InstanceState::Failed,
            StateTransitionReason::FailureReported("stale writer".to_owned()),
        ))
        .await
        .expect_err("stale generation is rejected");
    match stale {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(0));
            assert_eq!(actual, Generation::new(1));
        }
        other => panic!("expected generation conflict, got {other}"),
    }

    let target = MaterializationTarget::new("cluster-a", "default").expect("valid target");
    let pending = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(1),
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    let pending_record = store.record_materialization(pending).await?;
    assert_eq!(pending_record.state, MaterializationState::Pending);

    let running = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(1),
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await?;
    assert_eq!(running.generation, Generation::new(2));
    assert_eq!(running.state, InstanceState::Running);

    let mut ready = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(2),
        target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(2),
    );
    ready.backend = Some(BackendEndpoint::new("http://10.0.0.10:8080").expect("valid backend"));
    ready.rendered_objects = vec![RenderedObjectRef {
        api_version: "apps/v1".to_owned(),
        kind: "Deployment".to_owned(),
        namespace: "default".to_owned(),
        name: "instance-a".to_owned(),
    }];
    let ready_record = store.record_materialization(ready).await?;
    assert_eq!(ready_record.state, MaterializationState::Ready);
    assert_eq!(ready_record.backend_generation, BackendGeneration::new(2));
    assert_eq!(
        ready_record.backend.as_ref().map(BackendEndpoint::uri),
        Some("http://10.0.0.10:8080")
    );

    let dependencies = store
        .lookup_route_dependencies(RouteDependencyLookup::new(
            route_entry.route_binding_id.clone(),
        ))
        .await?
        .expect("route dependencies load");
    assert_eq!(dependencies.instance_id.as_str(), "instance-a");
    assert_eq!(
        dependencies.materialization_generation,
        Some(BackendGeneration::new(2))
    );

    let resolved_with_backend = store
        .resolve_route(RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        })
        .await?;
    match resolved_with_backend {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.10:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(2)));
        }
        RouteResolution::Miss { .. } => panic!("route should resolve after materialization"),
    }

    let mut rewind = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(2),
        target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(1),
    );
    rewind.backend = Some(BackendEndpoint::new("http://10.0.0.9:8080").expect("valid backend"));
    let rewind_error = store
        .record_materialization(rewind)
        .await
        .expect_err("lower backend generations must be rejected");
    match rewind_error {
        StoreError::InvalidArgument { message } => {
            assert!(message.contains("backend generation rewind"));
        }
        other => panic!("expected backend rewind invalid argument, got {other}"),
    }

    let resolved_after_rewind_rejection = store
        .resolve_route(RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        })
        .await?;
    match resolved_after_rewind_rejection {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.10:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(2)));
        }
        RouteResolution::Miss { .. } => panic!("route should still resolve"),
    }

    let draining = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(2),
            InstanceState::Draining,
            StateTransitionReason::SleepRequested,
        ))
        .await?;
    assert_eq!(draining.generation, Generation::new(3));

    let stale_sidecar_report = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(2),
            InstanceState::Draining,
            StateTransitionReason::IdleReported,
        ))
        .await
        .expect_err("stale sidecar idle reports are rejected");
    match stale_sidecar_report {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(2));
            assert_eq!(actual, Generation::new(3));
        }
        other => panic!("expected stale sidecar generation conflict, got {other}"),
    }

    let resolved_after_generation_advance = store
        .resolve_route(RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        })
        .await?;
    match resolved_after_generation_advance {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(entry.instance_generation, Generation::new(3));
            assert_eq!(entry.backend, None);
            assert_eq!(entry.backend_generation, None);
        }
        RouteResolution::Miss { .. } => panic!("route binding should still resolve"),
    }

    let dependencies_after_generation_advance = store
        .lookup_route_dependencies(RouteDependencyLookup::new(
            route_entry.route_binding_id.clone(),
        ))
        .await?
        .expect("route dependencies still load");
    assert_eq!(
        dependencies_after_generation_advance.materialization_generation,
        None
    );

    let mut stale_materialization = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(2),
        target,
        MaterializationState::Ready,
        BackendGeneration::new(3),
    );
    stale_materialization.backend =
        Some(BackendEndpoint::new("http://10.0.0.11:8080").expect("valid backend"));
    let stale_materialization_error = store
        .record_materialization(stale_materialization)
        .await
        .expect_err("stale instance generation materialization is rejected");
    match stale_materialization_error {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(2));
            assert_eq!(actual, Generation::new(3));
        }
        other => panic!("expected stale materialization generation conflict, got {other}"),
    }

    exercise_http01(store).await?;

    Ok(())
}

async fn exercise_http01(store: &PostgresStore) -> Result<(), StoreError> {
    let now = SystemTime::now();
    let active_key =
        Http01ChallengeKey::new("Acme.Example.COM.", "token-a").expect("valid challenge key");
    let active_put = PutHttp01ChallengeRequest::with_ttl(
        active_key.clone(),
        "key-auth-a",
        Duration::from_secs(60),
        now,
    )
    .expect("valid challenge");
    let active_record = store.put_http01_challenge(active_put).await?;
    assert_eq!(active_record.key().host().as_str(), "acme.example.com");

    let resolved = store
        .resolve_http01_challenge(active_key.clone())
        .await?
        .expect("active challenge resolves");
    assert_eq!(resolved.key_authorization(), "key-auth-a");

    let wrong_host =
        Http01ChallengeKey::new("wrong.example.com", "token-a").expect("valid challenge key");
    assert!(store.resolve_http01_challenge(wrong_host).await?.is_none());
    let wrong_token =
        Http01ChallengeKey::new("acme.example.com", "wrong-token").expect("valid challenge key");
    assert!(store.resolve_http01_challenge(wrong_token).await?.is_none());

    let repeated_put = PutHttp01ChallengeRequest::with_ttl(
        active_key.clone(),
        "key-auth-a",
        Duration::from_secs(60),
        now,
    )
    .expect("valid repeated challenge");
    let repeated_record = store.put_http01_challenge(repeated_put).await?;
    assert_eq!(repeated_record.key_authorization(), "key-auth-a");
    let repeated_resolved = store
        .resolve_http01_challenge(active_key.clone())
        .await?
        .expect("repeated challenge resolves");
    assert_eq!(repeated_resolved.key_authorization(), "key-auth-a");

    let overwrite_put = PutHttp01ChallengeRequest::with_ttl(
        active_key.clone(),
        "key-auth-overwritten",
        Duration::from_secs(120),
        now,
    )
    .expect("valid overwrite challenge");
    let overwritten_record = store.put_http01_challenge(overwrite_put).await?;
    assert_eq!(
        overwritten_record.key_authorization(),
        "key-auth-overwritten"
    );
    let overwritten_resolved = store
        .resolve_http01_challenge(active_key.clone())
        .await?
        .expect("overwritten challenge resolves");
    assert_eq!(
        overwritten_resolved.key_authorization(),
        "key-auth-overwritten"
    );

    assert!(
        store
            .delete_http01_challenge(control_plane::DeleteHttp01ChallengeRequest::new(
                active_key.clone()
            ))
            .await?
    );
    assert!(store.resolve_http01_challenge(active_key).await?.is_none());

    let expired_key = Http01ChallengeKey::new("expired.example.com", "token-b").expect("valid key");
    let expired_put = PutHttp01ChallengeRequest::new(
        expired_key.clone(),
        "key-auth-b",
        UNIX_EPOCH + Duration::from_secs(1),
        UNIX_EPOCH,
    )
    .expect("test can insert an already wall-clock-expired record");
    store.put_http01_challenge(expired_put).await?;
    assert!(store
        .resolve_http01_challenge(expired_key.clone())
        .await?
        .is_none());

    let expired = store
        .expire_http01_challenges(
            ExpireHttp01ChallengesRequest::new(SystemTime::now()).with_limit(1),
        )
        .await?;
    assert_eq!(expired, 1);
    assert!(store.resolve_http01_challenge(expired_key).await?.is_none());

    Ok(())
}

async fn exercise_instance_lifecycle(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let invalid_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-invalid",
        "instance-lifecycle-invalid",
    )
    .await?;
    assert_eq!(invalid_target.instance.generation, Generation::new(0));

    let invalid = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            invalid_target.instance.id.clone(),
            Generation::new(0),
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await
        .expect_err("cold instances cannot become running directly");
    assert!(matches!(invalid, StoreError::InvalidArgument { .. }));
    let after_invalid = store
        .get_instance(GetInstanceRequest::new(invalid_target.instance.id.clone()))
        .await?
        .expect("invalid transition target still exists");
    assert_eq!(after_invalid.state, InstanceState::Cold);
    assert_eq!(after_invalid.generation, Generation::new(0));

    let concurrent_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-concurrent",
        "instance-lifecycle-concurrent",
    )
    .await?;
    let wake_a = CompareAndSwapInstanceStateRequest::new(
        concurrent_target.instance.id.clone(),
        Generation::new(0),
        InstanceState::Waking,
        StateTransitionReason::WakeRequested,
    );
    let wake_b = wake_a.clone();
    let (first, second) = tokio::join!(
        store.compare_and_swap_instance_state(wake_a),
        store.compare_and_swap_instance_state(wake_b)
    );
    let mut successes = 0;
    let mut conflicts = 0;
    for result in [first, second] {
        match result {
            Ok(record) => {
                successes += 1;
                assert_eq!(record.state, InstanceState::Waking);
                assert_eq!(record.generation, Generation::new(1));
            }
            Err(StoreError::GenerationConflict { expected, actual }) => {
                conflicts += 1;
                assert_eq!(expected, Generation::new(0));
                assert_eq!(actual, Generation::new(1));
            }
            Err(other) => panic!("expected wake success or generation conflict, got {other}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(conflicts, 1);

    let sleep_while_waking = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-sleep-waking",
        "instance-lifecycle-sleep-waking",
    )
    .await?;
    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            sleep_while_waking.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(waking.generation, Generation::new(1));
    let sleep_during_wake = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            sleep_while_waking.instance.id.clone(),
            Generation::new(1),
            InstanceState::Draining,
            StateTransitionReason::SleepRequested,
        ))
        .await
        .expect_err("sleep while waking is deterministically rejected");
    assert!(matches!(
        sleep_during_wake,
        StoreError::InvalidArgument { .. }
    ));
    let still_waking = store
        .get_instance(GetInstanceRequest::new(
            sleep_while_waking.instance.id.clone(),
        ))
        .await?
        .expect("sleep while waking target still exists");
    assert_eq!(still_waking.state, InstanceState::Waking);
    assert_eq!(still_waking.generation, Generation::new(1));
    let deleting_from_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            sleep_while_waking.instance.id,
            Generation::new(1),
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    assert_eq!(deleting_from_waking.state, InstanceState::Deleting);
    assert_eq!(deleting_from_waking.generation, Generation::new(2));

    let drain_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-drain-complete",
        "instance-lifecycle-drain-complete",
    )
    .await?;
    let running = wake_to_running(store, drain_target.instance.id.clone()).await?;
    assert_eq!(running.generation, Generation::new(2));
    let draining = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            drain_target.instance.id.clone(),
            Generation::new(2),
            InstanceState::Draining,
            StateTransitionReason::IdleReported,
        ))
        .await?;
    assert_eq!(draining.generation, Generation::new(3));
    let cold = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            drain_target.instance.id,
            Generation::new(3),
            InstanceState::Cold,
            StateTransitionReason::DrainCompleted,
        ))
        .await?;
    assert_eq!(cold.state, InstanceState::Cold);
    assert_eq!(cold.generation, Generation::new(4));

    let delete_draining_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-delete-draining",
        "instance-lifecycle-delete-draining",
    )
    .await?;
    wake_to_running(store, delete_draining_target.instance.id.clone()).await?;
    let draining = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            delete_draining_target.instance.id.clone(),
            Generation::new(2),
            InstanceState::Draining,
            StateTransitionReason::SleepRequested,
        ))
        .await?;
    assert_eq!(draining.generation, Generation::new(3));
    let deleting_from_draining = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            delete_draining_target.instance.id,
            Generation::new(3),
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    assert_eq!(deleting_from_draining.state, InstanceState::Deleting);
    assert_eq!(deleting_from_draining.generation, Generation::new(4));

    let failed_target = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-lifecycle-failed-retry",
        "instance-lifecycle-failed-retry",
    )
    .await?;
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            failed_target.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let failed = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            failed_target.instance.id.clone(),
            Generation::new(1),
            InstanceState::Failed,
            StateTransitionReason::FailureReported("readiness timeout".to_owned()),
        ))
        .await?;
    assert_eq!(failed.state, InstanceState::Failed);
    assert_eq!(failed.generation, Generation::new(2));
    let retry = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            failed_target.instance.id,
            Generation::new(2),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(retry.state, InstanceState::Waking);
    assert_eq!(retry.generation, Generation::new(3));

    let terminal_target = create_lifecycle_instance(
        store,
        workload_class,
        "idem-lifecycle-terminal",
        "instance-lifecycle-terminal",
    )
    .await?;
    let deleting = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            terminal_target.instance.id.clone(),
            Generation::new(0),
            InstanceState::Deleting,
            StateTransitionReason::DeleteRequested,
        ))
        .await?;
    assert_eq!(deleting.generation, Generation::new(1));
    let deleted = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            terminal_target.instance.id.clone(),
            Generation::new(1),
            InstanceState::Deleted,
            StateTransitionReason::DeleteFinalized,
        ))
        .await?;
    assert_eq!(deleted.state, InstanceState::Deleted);
    assert_eq!(deleted.generation, Generation::new(2));
    let terminal_wake = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            terminal_target.instance.id.clone(),
            Generation::new(2),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await
        .expect_err("deleted instances are terminal");
    assert!(matches!(terminal_wake, StoreError::InvalidArgument { .. }));
    let still_deleted = store
        .get_instance(GetInstanceRequest::new(terminal_target.instance.id))
        .await?
        .expect("deleted terminal target still exists");
    assert_eq!(still_deleted.state, InstanceState::Deleted);
    assert_eq!(still_deleted.generation, Generation::new(2));

    Ok(())
}

async fn create_lifecycle_instance(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
    idempotency_key: &str,
    instance_id: &str,
) -> Result<control_plane::CreateInstanceResult, StoreError> {
    store
        .create_instance(create_instance_request(
            idempotency_key,
            instance_id,
            workload_class,
            vec![],
        ))
        .await
}

async fn exercise_rendered_object_ref_collision_rejection(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let owner = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-collision-service-owner",
        "instance-collision-service-owner",
    )
    .await?;
    let owner_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            owner.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let service_target =
        MaterializationTarget::new("cluster-collision", "apps").expect("valid target");
    let mut owner_materialization = RecordMaterializationRequest::new(
        owner.instance.id.clone(),
        owner_waking.generation,
        service_target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    owner_materialization.rendered_objects =
        vec![object_ref("v1", "Service", "apps", "shared-service")];
    store.record_materialization(owner_materialization).await?;

    let contender = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-collision-service-contender",
        "instance-collision-service-contender",
    )
    .await?;
    let contender_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            contender.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut contender_materialization = RecordMaterializationRequest::new(
        contender.instance.id.clone(),
        contender_waking.generation,
        service_target,
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    contender_materialization.rendered_objects =
        vec![object_ref("v1", "Service", "apps", "shared-service")];
    let service_collision = store
        .record_materialization(contender_materialization)
        .await
        .expect_err("namespaced rendered object ref collision is rejected");
    assert_collision_error(
        service_collision,
        "v1 Service apps/shared-service",
        "instance-collision-service-owner",
    );
    assert_eq!(
        store
            .get_instance(GetInstanceRequest::new(contender.instance.id.clone()))
            .await?
            .expect("contender instance remains")
            .state,
        InstanceState::Waking
    );

    let pv_owner = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-collision-pv-owner",
        "instance-collision-pv-owner",
    )
    .await?;
    let pv_owner_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            pv_owner.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut pv_owner_materialization = RecordMaterializationRequest::new(
        pv_owner.instance.id.clone(),
        pv_owner_waking.generation,
        MaterializationTarget::new("cluster-collision", "pv-owner").expect("valid target"),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    pv_owner_materialization.rendered_objects =
        vec![object_ref("v1", "PersistentVolume", "", "shared-pv")];
    store
        .record_materialization(pv_owner_materialization)
        .await?;

    let pv_contender = create_lifecycle_instance(
        store,
        workload_class,
        "idem-collision-pv-contender",
        "instance-collision-pv-contender",
    )
    .await?;
    let pv_contender_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            pv_contender.instance.id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let mut pv_contender_materialization = RecordMaterializationRequest::new(
        pv_contender.instance.id.clone(),
        pv_contender_waking.generation,
        MaterializationTarget::new("cluster-collision", "pv-contender").expect("valid target"),
        MaterializationState::Pending,
        BackendGeneration::new(1),
    );
    pv_contender_materialization.rendered_objects =
        vec![object_ref("v1", "PersistentVolume", "", "shared-pv")];
    let pv_collision = store
        .record_materialization(pv_contender_materialization)
        .await
        .expect_err("cluster-scoped PV rendered object ref collision is rejected");
    assert_collision_error(
        pv_collision,
        "v1 PersistentVolume /shared-pv",
        "instance-collision-pv-owner",
    );

    Ok(())
}

async fn wake_to_running(
    store: &PostgresStore,
    instance_id: InstanceId,
) -> Result<control_plane::InstanceRecord, StoreError> {
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            instance_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            instance_id,
            Generation::new(1),
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await
}

async fn exercise_complete_wake(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let success = store
        .create_instance(create_instance_request(
            "idem-complete-wake-success",
            "instance-complete-wake-success",
            workload_class.clone(),
            vec![http_route("complete-wake.example.com", None)],
        ))
        .await?;
    let instance_id = success.instance.id.clone();
    let route_binding_id = success.route_bindings[0].id.clone();
    let waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            instance_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(waking.state, InstanceState::Waking);
    assert_eq!(waking.generation, Generation::new(1));

    let target = MaterializationTarget::new("cluster-complete", "apps").expect("valid target");
    let pending = RecordMaterializationRequest::new(
        instance_id.clone(),
        waking.generation,
        target.clone(),
        MaterializationState::Pending,
        BackendGeneration::new(6),
    );
    let pending_record = store.record_materialization(pending).await?;
    assert_eq!(pending_record.instance_generation, Generation::new(1));
    assert_eq!(pending_record.backend_generation, BackendGeneration::new(6));

    let rendered_objects = vec![
        RenderedObjectRef {
            api_version: "apps/v1".to_owned(),
            kind: "Deployment".to_owned(),
            namespace: "apps".to_owned(),
            name: "instance-complete-wake-success".to_owned(),
        },
        RenderedObjectRef {
            api_version: "v1".to_owned(),
            kind: "Service".to_owned(),
            namespace: "apps".to_owned(),
            name: "instance-complete-wake-success".to_owned(),
        },
    ];
    let mut complete = CompleteWakeRequest::new(
        instance_id.clone(),
        waking.generation,
        target.clone(),
        BackendEndpoint::new("http://10.0.0.20:8080").expect("valid backend"),
        BackendGeneration::new(7),
    );
    complete.rendered_objects = rendered_objects.clone();
    let completed = store.complete_wake(complete).await?;
    assert_eq!(completed.instance.id, instance_id);
    assert_eq!(completed.instance.state, InstanceState::Running);
    assert_eq!(completed.instance.generation, Generation::new(2));
    assert_eq!(completed.materialization.instance_id, completed.instance.id);
    assert_eq!(
        completed.materialization.instance_generation,
        completed.instance.generation
    );
    assert_eq!(completed.materialization.target, target);
    assert_eq!(completed.materialization.state, MaterializationState::Ready);
    assert_eq!(
        completed.materialization.backend_generation,
        BackendGeneration::new(7)
    );
    assert_eq!(
        completed
            .materialization
            .backend
            .as_ref()
            .map(BackendEndpoint::uri),
        Some("http://10.0.0.20:8080")
    );
    assert_eq!(completed.materialization.rendered_objects, rendered_objects);
    let loaded_ready = store
        .load_ready_materialization(LoadReadyMaterializationRequest::new(
            completed.instance.id.clone(),
            completed.instance.generation,
            target.clone(),
        ))
        .await?
        .expect("ready materialization loads by exact target and generation");
    assert_eq!(loaded_ready, completed.materialization);
    assert!(
        store
            .load_ready_materialization(LoadReadyMaterializationRequest::new(
                completed.instance.id.clone(),
                Generation::new(1),
                target.clone(),
            ))
            .await?
            .is_none(),
        "wrong instance generation must not load ready materialization"
    );
    assert!(
        store
            .load_ready_materialization(LoadReadyMaterializationRequest::new(
                completed.instance.id.clone(),
                completed.instance.generation,
                MaterializationTarget::new("cluster-complete", "other").expect("valid target"),
            ))
            .await?
            .is_none(),
        "wrong target must not load ready materialization"
    );
    let non_ready_target =
        MaterializationTarget::new("cluster-complete", "pending").expect("valid target");
    store
        .record_materialization(RecordMaterializationRequest::new(
            completed.instance.id.clone(),
            completed.instance.generation,
            non_ready_target.clone(),
            MaterializationState::Pending,
            BackendGeneration::new(1),
        ))
        .await?;
    assert!(
        store
            .load_ready_materialization(LoadReadyMaterializationRequest::new(
                completed.instance.id.clone(),
                completed.instance.generation,
                non_ready_target,
            ))
            .await?
            .is_none(),
        "non-ready materialization must not load"
    );

    match store
        .resolve_route(http_identity("complete-wake.example.com", None))
        .await?
    {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(entry.route_binding_id, route_binding_id);
            assert_eq!(entry.instance_id, completed.instance.id);
            assert_eq!(entry.instance_state, InstanceState::Running);
            assert_eq!(entry.instance_generation, Generation::new(2));
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.20:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(7)));
        }
        RouteResolution::Miss { .. } => panic!("route should resolve after complete_wake"),
    }
    let dependencies = store
        .lookup_route_dependencies(RouteDependencyLookup::new(route_binding_id))
        .await?
        .expect("route dependencies load after complete_wake");
    assert_eq!(
        dependencies.materialization_generation,
        Some(BackendGeneration::new(7))
    );

    let sleep_started = store
        .begin_sleep(BeginSleepRequest::new(
            completed.instance.id.clone(),
            completed.instance.generation,
            target.clone(),
        ))
        .await?;
    assert_eq!(sleep_started.instance.state, InstanceState::Draining);
    assert_eq!(sleep_started.instance.generation, Generation::new(3));
    let deleting_materialization = sleep_started
        .materialization
        .expect("active materialization is marked deleting");
    assert_eq!(
        deleting_materialization.state,
        MaterializationState::Deleting
    );
    assert_eq!(deleting_materialization.backend, None);
    assert_eq!(deleting_materialization.rendered_objects, rendered_objects);
    let active = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            completed.instance.id.clone(),
            target.clone(),
        ))
        .await?
        .expect("deleting materialization remains active until cleanup finalizes");
    assert_eq!(active.state, MaterializationState::Deleting);

    let sleep_finalized = store
        .finalize_sleep(FinalizeSleepRequest::new(
            completed.instance.id.clone(),
            sleep_started.instance.generation,
            target.clone(),
        ))
        .await?;
    assert_eq!(sleep_finalized.instance.state, InstanceState::Cold);
    assert_eq!(sleep_finalized.instance.generation, Generation::new(4));
    let deleted_materialization = sleep_finalized
        .materialization
        .expect("materialization is marked deleted");
    assert_eq!(deleted_materialization.state, MaterializationState::Deleted);
    assert_eq!(
        deleted_materialization.instance_generation,
        Generation::new(4)
    );
    assert_eq!(deleted_materialization.backend, None);
    assert!(deleted_materialization.rendered_objects.is_empty());
    assert!(
        store
            .load_active_materialization(LoadActiveMaterializationRequest::new(
                completed.instance.id.clone(),
                target.clone(),
            ))
            .await?
            .is_none(),
        "deleted materialization is no longer active"
    );

    let stale_sleep = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-begin-sleep-stale-materialization",
        "instance-begin-sleep-stale-materialization",
    )
    .await?;
    let stale_sleep_id = stale_sleep.instance.id.clone();
    let stale_sleep_waking = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            stale_sleep_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let stale_sleep_target =
        MaterializationTarget::new("cluster-complete", "stale-sleep").expect("valid target");
    let stale_sleep_materialization = store
        .record_materialization(RecordMaterializationRequest::new(
            stale_sleep_id.clone(),
            stale_sleep_waking.generation,
            stale_sleep_target.clone(),
            MaterializationState::Ready,
            BackendGeneration::new(1),
        ))
        .await?;
    assert_eq!(
        stale_sleep_materialization.instance_generation,
        Generation::new(1)
    );
    let stale_sleep_running = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            stale_sleep_id.clone(),
            stale_sleep_waking.generation,
            InstanceState::Running,
            StateTransitionReason::MaterializationReady,
        ))
        .await?;
    assert_eq!(stale_sleep_running.generation, Generation::new(2));
    let stale_sleep_error = store
        .begin_sleep(BeginSleepRequest::new(
            stale_sleep_id.clone(),
            stale_sleep_running.generation,
            stale_sleep_target.clone(),
        ))
        .await
        .expect_err("begin_sleep rejects stale active materialization generation");
    match stale_sleep_error {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(2));
            assert_eq!(actual, Generation::new(1));
        }
        other => panic!("expected stale materialization generation conflict, got {other}"),
    }
    let active_after_rejected_sleep = store
        .load_active_materialization(LoadActiveMaterializationRequest::new(
            stale_sleep_id,
            stale_sleep_target,
        ))
        .await?
        .expect("stale materialization remains active after rejected sleep");
    assert_eq!(
        active_after_rejected_sleep.state,
        MaterializationState::Ready
    );
    assert_eq!(
        active_after_rejected_sleep.instance_generation,
        Generation::new(1)
    );

    let stale = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-complete-wake-stale",
        "instance-complete-wake-stale",
    )
    .await?;
    let stale_id = stale.instance.id.clone();
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            stale_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let stale_target =
        MaterializationTarget::new("cluster-complete", "stale").expect("valid target");
    let stale_error = store
        .complete_wake(CompleteWakeRequest::new(
            stale_id.clone(),
            Generation::new(0),
            stale_target.clone(),
            BackendEndpoint::new("http://10.0.0.30:8080").expect("valid backend"),
            BackendGeneration::new(5),
        ))
        .await
        .expect_err("stale complete_wake generation is rejected");
    match stale_error {
        StoreError::GenerationConflict { expected, actual } => {
            assert_eq!(expected, Generation::new(0));
            assert_eq!(actual, Generation::new(1));
        }
        other => panic!("expected stale complete_wake generation conflict, got {other}"),
    }
    let stale_after = store
        .get_instance(GetInstanceRequest::new(stale_id.clone()))
        .await?
        .expect("stale complete_wake target still exists");
    assert_eq!(stale_after.state, InstanceState::Waking);
    assert_eq!(stale_after.generation, Generation::new(1));
    let stale_probe = RecordMaterializationRequest::new(
        stale_id,
        Generation::new(1),
        stale_target,
        MaterializationState::Pending,
        BackendGeneration::new(4),
    );
    let stale_probe_record = store.record_materialization(stale_probe).await?;
    assert_eq!(
        stale_probe_record.backend_generation,
        BackendGeneration::new(4)
    );

    let cold = create_lifecycle_instance(
        store,
        workload_class.clone(),
        "idem-complete-wake-cold",
        "instance-complete-wake-cold",
    )
    .await?;
    let cold_id = cold.instance.id.clone();
    let cold_target = MaterializationTarget::new("cluster-complete", "cold").expect("valid target");
    let cold_error = store
        .complete_wake(CompleteWakeRequest::new(
            cold_id.clone(),
            Generation::new(0),
            cold_target.clone(),
            BackendEndpoint::new("http://10.0.0.31:8080").expect("valid backend"),
            BackendGeneration::new(3),
        ))
        .await
        .expect_err("cold instances cannot complete wake");
    assert!(matches!(cold_error, StoreError::InvalidArgument { .. }));
    let cold_after = store
        .get_instance(GetInstanceRequest::new(cold_id.clone()))
        .await?
        .expect("cold complete_wake target still exists");
    assert_eq!(cold_after.state, InstanceState::Cold);
    assert_eq!(cold_after.generation, Generation::new(0));
    let cold_probe = RecordMaterializationRequest::new(
        cold_id,
        Generation::new(0),
        cold_target,
        MaterializationState::Pending,
        BackendGeneration::new(2),
    );
    let cold_probe_record = store.record_materialization(cold_probe).await?;
    assert_eq!(
        cold_probe_record.backend_generation,
        BackendGeneration::new(2)
    );

    let rewind = store
        .create_instance(create_instance_request(
            "idem-complete-wake-rewind",
            "instance-complete-wake-rewind",
            workload_class,
            vec![http_route("complete-wake-rewind.example.com", None)],
        ))
        .await?;
    let rewind_id = rewind.instance.id.clone();
    let rewind_route_binding_id = rewind.route_bindings[0].id.clone();
    store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            rewind_id.clone(),
            Generation::new(0),
            InstanceState::Waking,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    let rewind_target =
        MaterializationTarget::new("cluster-complete", "rewind").expect("valid target");
    let mut old_ready = RecordMaterializationRequest::new(
        rewind_id.clone(),
        Generation::new(1),
        rewind_target.clone(),
        MaterializationState::Ready,
        BackendGeneration::new(9),
    );
    old_ready.backend = Some(BackendEndpoint::new("http://10.0.0.40:8080").expect("valid backend"));
    old_ready.rendered_objects = vec![RenderedObjectRef {
        api_version: "apps/v1".to_owned(),
        kind: "Deployment".to_owned(),
        namespace: "rewind".to_owned(),
        name: "old-ready".to_owned(),
    }];
    store.record_materialization(old_ready).await?;
    let rewind_error = store
        .complete_wake(CompleteWakeRequest::new(
            rewind_id.clone(),
            Generation::new(1),
            rewind_target.clone(),
            BackendEndpoint::new("http://10.0.0.41:8080").expect("valid backend"),
            BackendGeneration::new(8),
        ))
        .await
        .expect_err("complete_wake rejects backend generation rewinds");
    match rewind_error {
        StoreError::InvalidArgument { message } => {
            assert!(message.contains("backend generation rewind"));
        }
        other => panic!("expected backend rewind invalid argument, got {other}"),
    }
    let rewind_after = store
        .get_instance(GetInstanceRequest::new(rewind_id.clone()))
        .await?
        .expect("rewind complete_wake target still exists");
    assert_eq!(rewind_after.state, InstanceState::Waking);
    assert_eq!(rewind_after.generation, Generation::new(1));
    match store
        .resolve_route(http_identity("complete-wake-rewind.example.com", None))
        .await?
    {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(entry.route_binding_id, rewind_route_binding_id);
            assert_eq!(entry.instance_state, InstanceState::Waking);
            assert_eq!(entry.instance_generation, Generation::new(1));
            assert_eq!(
                entry.backend.as_ref().map(BackendEndpoint::uri),
                Some("http://10.0.0.40:8080")
            );
            assert_eq!(entry.backend_generation, Some(BackendGeneration::new(9)));
        }
        RouteResolution::Miss { .. } => {
            panic!("route should still resolve to the unchanged old materialization")
        }
    }
    let lower_backend_generation = RecordMaterializationRequest::new(
        rewind_id,
        Generation::new(1),
        rewind_target,
        MaterializationState::Ready,
        BackendGeneration::new(8),
    );
    let lower_error = store
        .record_materialization(lower_backend_generation)
        .await
        .expect_err("old materialization backend generation remains newer");
    assert!(matches!(lower_error, StoreError::InvalidArgument { .. }));

    Ok(())
}

async fn exercise_route_bindings(
    store: &PostgresStore,
    workload_class: WorkloadClassVersionRef,
) -> Result<(), StoreError> {
    let route_instance = store
        .create_instance(create_instance_request(
            "idem-route-instance",
            "instance-routes",
            workload_class,
            vec![],
        ))
        .await?;
    let loaded_before = store
        .get_instance(GetInstanceRequest::new(route_instance.instance.id.clone()))
        .await?
        .expect("route target instance loads");

    let exact_root = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-root",
            "route-exact-root",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::exact("App.Routes.Example.COM.").expect("valid host"),
                path: None,
            },
            ProtocolRoute::Http,
        ))
        .await?;
    assert_eq!(
        exact_root.identity,
        http_identity("app.routes.example.com", None)
    );
    let loaded_exact = store
        .get_route_binding(GetRouteBindingRequest::new(exact_root.id.clone()))
        .await?
        .expect("created route binding loads");
    assert_eq!(loaded_exact, exact_root);

    let replayed_exact = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-root",
            "route-exact-root",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::exact("app.routes.example.com").expect("valid host"),
                path: None,
            },
            ProtocolRoute::Http,
        ))
        .await?;
    assert_eq!(replayed_exact, exact_root);

    let idempotency_conflict = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-root",
            "route-conflicting-replay",
            "instance-routes",
            http_identity("conflict.routes.example.com", None),
            ProtocolRoute::Http,
        ))
        .await
        .expect_err("same idempotency key with different route payload conflicts");
    assert!(matches!(
        idempotency_conflict,
        StoreError::IdempotencyConflict
    ));

    let duplicate_identity = store
        .create_route_binding(create_route_binding_request(
            "idem-route-duplicate-identity",
            "route-duplicate-identity",
            "instance-routes",
            http_identity("APP.ROUTES.EXAMPLE.COM.", None),
            ProtocolRoute::Http,
        ))
        .await
        .expect_err("duplicate normalized route identity is rejected");
    assert!(matches!(
        duplicate_identity,
        StoreError::AlreadyExists {
            resource: "route binding"
        }
    ));

    let duplicate_id = store
        .create_route_binding(create_route_binding_request(
            "idem-route-duplicate-id",
            "route-exact-root",
            "instance-routes",
            http_identity("duplicate-id.routes.example.com", None),
            ProtocolRoute::Http,
        ))
        .await
        .expect_err("duplicate route binding ID is rejected");
    assert!(matches!(
        duplicate_id,
        StoreError::AlreadyExists {
            resource: "route binding"
        }
    ));

    let missing_instance = store
        .create_route_binding(create_route_binding_request(
            "idem-route-missing-instance",
            "route-missing-instance",
            "missing-instance",
            http_identity("missing.routes.example.com", None),
            ProtocolRoute::Http,
        ))
        .await
        .expect_err("route binding must point at an existing instance");
    assert!(matches!(
        missing_instance,
        StoreError::NotFound {
            resource: "referenced resource"
        }
    ));

    let protocol_mismatch = store
        .create_route_binding(create_route_binding_request(
            "idem-route-protocol-mismatch",
            "route-protocol-mismatch",
            "instance-routes",
            http_identity("mismatch.routes.example.com", None),
            ProtocolRoute::TlsSni,
        ))
        .await
        .expect_err("route identity and protocol must be compatible");
    assert!(matches!(
        protocol_mismatch,
        StoreError::InvalidArgument { .. }
    ));

    let exact_api = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-api",
            "route-exact-api",
            "instance-routes",
            http_identity("app.routes.example.com", Some("/api")),
            ProtocolRoute::Http,
        ))
        .await?;
    let exact_api_v1 = store
        .create_route_binding(create_route_binding_request(
            "idem-route-exact-api-v1",
            "route-exact-api-v1",
            "instance-routes",
            http_identity("app.routes.example.com", Some("/api/v1")),
            ProtocolRoute::Http,
        ))
        .await?;
    let wildcard_broad = store
        .create_route_binding(create_route_binding_request(
            "idem-route-wildcard-broad",
            "route-wildcard-broad",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix("routes.example.com").expect("valid host"),
                path: None,
            },
            ProtocolRoute::Http,
        ))
        .await?;
    let wildcard_specific = store
        .create_route_binding(create_route_binding_request(
            "idem-route-wildcard-specific",
            "route-wildcard-specific",
            "instance-routes",
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix("customer.routes.example.com")
                    .expect("valid host"),
                path: None,
            },
            ProtocolRoute::Http,
        ))
        .await?;

    assert_resolves_to(
        store,
        http_identity("app.routes.example.com", Some("/anything")),
        &exact_root.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("app.routes.example.com", Some("/api/v1/users")),
        &exact_api_v1.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("app.routes.example.com", Some("/apiary")),
        &exact_api.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("other.routes.example.com", None),
        &wildcard_broad.id,
    )
    .await?;
    assert_resolves_to(
        store,
        http_identity("db.customer.routes.example.com", None),
        &wildcard_specific.id,
    )
    .await?;

    let sni_exact = store
        .create_route_binding(create_route_binding_request(
            "idem-route-sni-exact",
            "route-sni-exact",
            "instance-routes",
            sni_identity("DB.Routes.Example.COM."),
            ProtocolRoute::TlsSni,
        ))
        .await?;
    let sni_wildcard = store
        .create_route_binding(create_route_binding_request(
            "idem-route-sni-wildcard",
            "route-sni-wildcard",
            "instance-routes",
            RouteIdentity::Sni {
                host: RouteHost::wildcard_suffix("routes.example.com").expect("valid host"),
            },
            ProtocolRoute::TlsSni,
        ))
        .await?;
    let duplicate_sni = store
        .create_route_binding(create_route_binding_request(
            "idem-route-sni-duplicate",
            "route-sni-duplicate",
            "instance-routes",
            sni_identity("db.routes.example.com"),
            ProtocolRoute::TlsSni,
        ))
        .await
        .expect_err("duplicate normalized SNI identity is rejected");
    assert!(matches!(
        duplicate_sni,
        StoreError::AlreadyExists {
            resource: "route binding"
        }
    ));
    assert_resolves_to(store, sni_identity("db.routes.example.com"), &sni_exact.id).await?;
    assert_resolves_to(
        store,
        sni_identity("tenant.routes.example.com"),
        &sni_wildcard.id,
    )
    .await?;

    let miss = store
        .resolve_route(http_identity("routes.example.com", None))
        .await?;
    match miss {
        RouteResolution::Miss { negative_cache } => {
            assert!(negative_cache.ttl() > Duration::from_secs(0));
        }
        RouteResolution::Resolved { entry, .. } => {
            panic!("base wildcard suffix should not match itself: {entry:?}")
        }
    }

    assert!(
        store
            .delete_route_binding(DeleteRouteBindingRequest::new(sni_wildcard.id.clone()))
            .await?
    );
    assert!(store
        .get_route_binding(GetRouteBindingRequest::new(sni_wildcard.id.clone()))
        .await?
        .is_none());
    assert!(
        !store
            .delete_route_binding(DeleteRouteBindingRequest::new(sni_wildcard.id))
            .await?
    );

    let loaded_after = store
        .get_instance(GetInstanceRequest::new(route_instance.instance.id))
        .await?
        .expect("route target instance still loads");
    assert_eq!(loaded_after, loaded_before);

    Ok(())
}

async fn assert_resolves_to(
    store: &PostgresStore,
    identity: RouteIdentity,
    expected_route_binding_id: &RouteBindingId,
) -> Result<(), StoreError> {
    match store.resolve_route(identity).await? {
        RouteResolution::Resolved { entry, .. } => {
            assert_eq!(&entry.route_binding_id, expected_route_binding_id);
            Ok(())
        }
        RouteResolution::Miss { .. } => panic!("route should resolve"),
    }
}

fn workload_class(class_id: &str, version: u64) -> WorkloadClassVersion {
    let image = format!("example/app:{version}");

    WorkloadClassVersion {
        reference: WorkloadClassVersionRef::new(
            WorkloadClassId::new(class_id).expect("valid workload class ID"),
            Generation::new(version),
        ),
        template_generation: Generation::new(1),
        template: workload_manifest_template(),
        default_values: BTreeMap::from([("image".to_owned(), image.clone())]),
        value_schema: WorkloadValueSchema::new(false)
            .with_field("tenant", WorkloadValueFieldRule::required())
            .with_field(
                "image",
                WorkloadValueFieldRule::optional_with_default(image),
            ),
        sleep_policy: default_sleep_policy(),
    }
}

fn workload_class_with_idle_override(class_id: &str, version: u64) -> WorkloadClassVersion {
    let mut workload_class = workload_class(class_id, version);
    workload_class.value_schema = workload_class
        .value_schema
        .with_field("idle_ms", WorkloadValueFieldRule::optional());
    workload_class.sleep_policy = default_sleep_policy()
        .with_idle_timeout_override(
            IdleTimeoutOverridePolicy::new("idle_ms", 60_000, 600_000)
                .expect("valid override policy"),
        )
        .expect("override policy attaches");
    workload_class
}

fn default_sleep_policy() -> WorkloadSleepPolicy {
    WorkloadSleepPolicy::new(300_000, 5_000, 30_000).expect("valid sleep policy")
}

fn workload_manifest_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::Deployment,
            name: composed_text("app-", "tenant"),
            replicas: None,
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::instance_value("image"),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: 8080,
                }],
                env: vec![EnvVarTemplate {
                    name: "TENANT".to_owned(),
                    value: TemplateText::instance_value("tenant"),
                }],
            },
        },
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
            mode: None,
        },
        service: Some(ServiceTemplate {
            name: composed_text("svc-", "tenant"),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 80,
                target_port: 8080,
            }],
        }),
        volumes: Vec::new(),
    }
}

fn composed_text(prefix: &str, field: &str) -> TemplateText {
    TemplateText::from_parts([
        TemplateTextPart::literal(prefix),
        TemplateTextPart::instance_value(field),
    ])
}

fn create_instance_request(
    idempotency_key: &str,
    instance_id: &str,
    workload_class: WorkloadClassVersionRef,
    route_bindings: Vec<RouteBindingSpec>,
) -> CreateInstanceRequest {
    CreateInstanceRequest::new(
        IdempotencyKey::new(idempotency_key).expect("valid idempotency key"),
        InstanceId::new(instance_id).expect("valid instance ID"),
        workload_class,
    )
    .with_values(BTreeMap::from([(
        "tenant".to_owned(),
        instance_id.to_owned(),
    )]))
    .with_route_bindings(route_bindings)
}

fn create_route_binding_request(
    idempotency_key: &str,
    route_binding_id: &str,
    instance_id: &str,
    identity: RouteIdentity,
    protocol: ProtocolRoute,
) -> CreateRouteBindingRequest {
    CreateRouteBindingRequest::new(
        IdempotencyKey::new(idempotency_key).expect("valid idempotency key"),
        RouteBindingId::new(route_binding_id).expect("valid route binding ID"),
        InstanceId::new(instance_id).expect("valid instance ID"),
        identity,
        protocol,
    )
}

fn http_identity(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("valid host"),
        path: path.map(|path| PathPrefix::new(path).expect("valid path prefix")),
    }
}

fn sni_identity(host: &str) -> RouteIdentity {
    RouteIdentity::Sni {
        host: RouteHost::exact(host).expect("valid host"),
    }
}

fn http_route(host: &str, path: Option<&str>) -> RouteBindingSpec {
    RouteBindingSpec::new(
        RouteIdentity::Http {
            host: RouteHost::exact(host).expect("valid host"),
            path: path.map(|path| PathPrefix::new(path).expect("valid path prefix")),
        },
        ProtocolRoute::Http,
    )
}

fn sni_route(host: &str) -> RouteBindingSpec {
    RouteBindingSpec::new(
        RouteIdentity::Sni {
            host: RouteHost::exact(host).expect("valid host"),
        },
        ProtocolRoute::TlsSni,
    )
}

fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    }
}

fn assert_collision_error(error: StoreError, object: &str, owner_instance_id: &str) {
    match error {
        StoreError::InvalidArgument { message } => {
            assert!(
                message.contains("rendered Kubernetes object ref collision"),
                "message {message:?} should identify a rendered object collision"
            );
            assert!(
                message.contains(object),
                "message {message:?} should include collided object {object:?}"
            );
            assert!(
                message.contains(owner_instance_id),
                "message {message:?} should include owner instance {owner_instance_id:?}"
            );
        }
        other => panic!("expected rendered object collision invalid argument, got {other}"),
    }
}

fn unique_schema_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after epoch")
        .as_nanos();

    format!("sleepypods_test_{}_{}", std::process::id(), nanos)
}

fn connection_url_with_search_path(base_url: &str, schema: &str) -> String {
    let separator = if base_url.contains('?') { '&' } else { '?' };

    format!("{base_url}{separator}options=-csearch_path%3D{schema}")
}
