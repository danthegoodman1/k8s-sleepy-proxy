use std::{
    collections::BTreeMap,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use control_plane::{
    BackendEndpoint, BackendGeneration, CompareAndSwapInstanceStateRequest, ControlPlaneStore,
    CreateInstanceRequest, ExpireHttp01ChallengesRequest, Generation, Http01ChallengeKey,
    IdempotencyKey, InstanceId, InstanceState, MaterializationState, MaterializationTarget,
    PathPrefix, PostgresStore, PostgresStoreConfig, ProtocolRoute, PutHttp01ChallengeRequest,
    RecordMaterializationRequest, RenderedObjectRef, RouteBindingSpec, RouteDependencyLookup,
    RouteHost, RouteIdentity, RouteResolution, StateTransitionReason, StoreError, WorkloadClassId,
    WorkloadClassVersion, WorkloadClassVersionRef,
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
    let seeded = store.seed_workload_class_version(class.clone()).await?;
    assert_eq!(seeded, class);
    let loaded = store
        .load_workload_class_version(control_plane::LoadWorkloadClassVersionRequest::new(
            class.reference.clone(),
        ))
        .await?
        .expect("seeded workload class version loads");
    assert_eq!(loaded, class);

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
    assert_eq!(created.route_bindings.len(), 2);
    assert!(!created.idempotency_replayed);

    let replayed = store.create_instance(create.clone()).await?;
    assert!(replayed.idempotency_replayed);
    assert_eq!(replayed.instance, created.instance);
    assert_eq!(replayed.route_bindings, created.route_bindings);

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

    let initial_resolution = store
        .resolve_route(RouteIdentity::Http {
            host: RouteHost::exact("APP.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        })
        .await?;
    let route_entry = match initial_resolution {
        RouteResolution::Resolved(entry) => entry,
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

    let running = store
        .compare_and_swap_instance_state(CompareAndSwapInstanceStateRequest::new(
            InstanceId::new("instance-a").expect("valid instance ID"),
            Generation::new(0),
            InstanceState::Running,
            StateTransitionReason::WakeRequested,
        ))
        .await?;
    assert_eq!(running.generation, Generation::new(1));
    assert_eq!(running.state, InstanceState::Running);

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

    let mut ready = RecordMaterializationRequest::new(
        InstanceId::new("instance-a").expect("valid instance ID"),
        Generation::new(1),
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
        RouteResolution::Resolved(entry) => {
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
        Generation::new(1),
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
        RouteResolution::Resolved(entry) => {
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
            Generation::new(1),
            InstanceState::Draining,
            StateTransitionReason::SleepRequested,
        ))
        .await?;
    assert_eq!(draining.generation, Generation::new(2));

    let resolved_after_generation_advance = store
        .resolve_route(RouteIdentity::Http {
            host: RouteHost::exact("app.example.com").expect("valid host"),
            path: Some(PathPrefix::new("/api/v1").expect("valid prefix")),
        })
        .await?;
    match resolved_after_generation_advance {
        RouteResolution::Resolved(entry) => {
            assert_eq!(entry.instance_generation, Generation::new(2));
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
        Generation::new(1),
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
            assert_eq!(expected, Generation::new(1));
            assert_eq!(actual, Generation::new(2));
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

fn workload_class(class_id: &str, version: u64) -> WorkloadClassVersion {
    WorkloadClassVersion {
        reference: WorkloadClassVersionRef::new(
            WorkloadClassId::new(class_id).expect("valid workload class ID"),
            Generation::new(version),
        ),
        template_generation: Generation::new(1),
        default_values: BTreeMap::from([("image".to_owned(), "example/app:1".to_owned())]),
    }
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
