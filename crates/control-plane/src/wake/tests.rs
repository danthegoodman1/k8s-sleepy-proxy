use std::sync::{Arc, Mutex};

use proxy_core::observability::{
    metrics::{RUNTIME_MATERIALIZATION_FAILURES_TOTAL_NAME, RUNTIME_WAKE_LATENCY_SECONDS_NAME},
    recorder::{
        InMemoryObservability, ObservabilityEvent, EVENT_MATERIALIZATION_FAILURE, EVENT_WAKE,
        FIELD_CLUSTER_ID, FIELD_ERROR_REASON, FIELD_INSTANCE_ID, FIELD_NAMESPACE,
    },
};

use crate::{
    http01::{
        DeleteHttp01ChallengeRequest, ExpireHttp01ChallengesRequest, Http01ChallengeKey,
        Http01ChallengeRecord, PutHttp01ChallengeRequest,
    },
    ids::{MaterializationId, WorkloadClassId},
    instance::{
        CreateInstanceRequest, CreateInstanceResult, DeleteInstanceRequest, InstanceValues,
        StateTransitionReason,
    },
    manifest::{
        ContainerPortTemplate, ContainerTemplate, EnvVarTemplate, KubernetesObject,
        ManifestTemplate, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
        WorkloadKind, WorkloadTemplate,
    },
    materialization::{
        BackendEndpoint, BeginSleepRequest, BeginSleepResult, FinalizeSleepRequest,
        FinalizeSleepResult, LoadActiveMaterializationRequest, LoadReadyMaterializationRequest,
        MaterializationRecord, MaterializationState, RecordMaterializationRequest,
    },
    materializer::{
        KubernetesClientError, KubernetesClientFuture, KubernetesClientResult,
        KubernetesMaterializer,
    },
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
        RouteBindingRecord, RouteDependencyLookup, RouteDependencySet, RouteIdentity,
        RouteResolution,
    },
    sleep_policy::WorkloadSleepPolicy,
    store::{StoreFuture, StoreResult},
    workload::{
        CreateWorkloadClassVersionRequest, WorkloadClassVersion, WorkloadClassVersionRef,
        WorkloadValueSchema,
    },
    KubernetesMaterializerClient,
};

use super::*;

#[derive(Clone, Debug)]
struct FakeStore {
    inner: Arc<Mutex<FakeStoreState>>,
}

#[derive(Clone, Debug)]
struct FakeStoreState {
    instance: Option<InstanceRecord>,
    workload_class: Option<WorkloadClassVersion>,
    materialization: Option<MaterializationRecord>,
    events: Vec<StoreEvent>,
    complete_conflict: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StoreEvent {
    Cas {
        expected: Generation,
        next_state: InstanceState,
        reason: StateTransitionReason,
    },
    Complete {
        expected_waking_generation: Generation,
        backend_generation: BackendGeneration,
        rendered_objects: Vec<RenderedObjectRef>,
    },
    FinalizeSleep {
        expected_draining_generation: Generation,
        target: MaterializationTarget,
    },
    LoadActiveMaterialization {
        instance_id: InstanceId,
        target: MaterializationTarget,
    },
    LoadReadyMaterialization {
        instance_id: InstanceId,
        instance_generation: Generation,
        target: MaterializationTarget,
    },
}

#[derive(Clone, Debug, Default)]
struct FakeKubernetesClient {
    inner: Arc<Mutex<FakeKubernetesState>>,
}

#[derive(Clone, Debug)]
struct FakeKubernetesState {
    applied_objects: Vec<KubernetesObject>,
    deleted_objects: Vec<RenderedObjectRef>,
    readiness_calls: Vec<Vec<RenderedObjectRef>>,
    backend: BackendEndpoint,
    fail_readiness: bool,
}

impl Default for FakeKubernetesState {
    fn default() -> Self {
        Self {
            applied_objects: Vec::new(),
            deleted_objects: Vec::new(),
            readiness_calls: Vec::new(),
            backend: backend("http://svc-acme.apps.svc.cluster.local:80"),
            fail_readiness: false,
        }
    }
}

#[tokio::test]
async fn successful_wake_cas_renders_applies_and_completes_with_waking_generation() {
    let instance = instance("instance-a", InstanceState::Cold, 1);
    let store = FakeStore::new(instance, Some(workload_class()));
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let result = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(1),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect("wake succeeds");

    let WakeInstanceResult::Completed { result } = result else {
        panic!("expected completed wake");
    };
    assert_eq!(result.instance.state, InstanceState::Running);
    assert_eq!(result.instance.generation, Generation::new(3));
    assert_eq!(
        result.materialization.instance_generation,
        Generation::new(3)
    );
    assert_eq!(
        result.materialization.backend,
        Some(backend("http://svc-acme.apps.svc.cluster.local:80"))
    );
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(1),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(2),
                backend_generation: BackendGeneration::new(2),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme"),
                ],
            },
        ]
    );
    assert_eq!(
        client.readiness_calls(),
        vec![vec![
            object_ref("v1", "Service", "apps", "svc-acme"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme"),
        ]]
    );

    let applied = client.applied_objects();
    assert_eq!(applied.len(), 2);
    let deployment = applied
        .iter()
        .find_map(|object| match object {
            KubernetesObject::Deployment(deployment) => Some(deployment),
            _ => None,
        })
        .expect("deployment was applied");
    assert_eq!(
        deployment
            .metadata
            .labels
            .get("sleepypods.io/instance-generation"),
        Some(&"2".to_owned())
    );
    let sidecar = deployment
        .spec
        .template
        .spec
        .containers
        .iter()
        .find(|container| container.name == "sleepypods-sidecar")
        .expect("sidecar container was rendered");
    assert_env(&sidecar.env, "SLEEPYPODS_IDLE_TIMEOUT_MS", "120000");
    assert_env(&sidecar.env, "SLEEPYPODS_IDLE_RETRY_BACKOFF_MS", "5000");
    assert_env(&sidecar.env, "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS", "30000");
}

#[tokio::test]
async fn stale_expected_generation_writes_nothing_and_does_not_apply() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 2),
        Some(workload_class()),
    );
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(1),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect_err("stale wake is rejected");

    assert_generation_conflict(error, Generation::new(1), Generation::new(2));
    assert!(store.events().is_empty());
    assert!(client.applied_objects().is_empty());
}

#[tokio::test]
async fn already_running_returns_matching_ready_materialization_without_apply() {
    let running = instance("instance-a", InstanceState::Running, 5);
    let store = FakeStore::new(running.clone(), Some(workload_class()));
    let materialization = ready_materialization(
        "instance-a",
        5,
        target("cluster-a", "apps"),
        "http://svc-acme.apps.svc.cluster.local:80",
    );
    store.set_materialization(materialization.clone());
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let result = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(5),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect("already running is stable");

    assert_eq!(
        result,
        WakeInstanceResult::AlreadyRunning {
            instance: running.clone(),
            materialization: materialization.clone()
        }
    );
    assert_eq!(result.rendered_objects(), materialization.rendered_objects);
    assert_eq!(
        store.events(),
        vec![StoreEvent::LoadReadyMaterialization {
            instance_id: instance_id("instance-a"),
            instance_generation: Generation::new(5),
            target: target("cluster-a", "apps"),
        }]
    );
    assert!(client.applied_objects().is_empty());
}

#[tokio::test]
async fn already_running_missing_or_filtered_materialization_errors_without_apply() {
    let cases = [
        ("missing ready materialization", None),
        (
            "wrong target",
            Some(materialization(
                "instance-a",
                5,
                target("cluster-a", "other"),
                MaterializationState::Ready,
                Some(backend("http://svc-acme.other.svc.cluster.local:80")),
            )),
        ),
        (
            "non-ready state",
            Some(materialization(
                "instance-a",
                5,
                target("cluster-a", "apps"),
                MaterializationState::Pending,
                Some(backend("http://svc-acme.apps.svc.cluster.local:80")),
            )),
        ),
    ];

    for (name, stored_materialization) in cases {
        let running = instance("instance-a", InstanceState::Running, 5);
        let store = FakeStore::new(running.clone(), Some(workload_class()));
        if let Some(stored_materialization) = stored_materialization {
            store.set_materialization(stored_materialization);
        }
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let expected_target = target("cluster-a", "apps");

        let error = wake_instance(
            &store,
            &materializer,
            WakeInstanceRequest::new(
                instance_id("instance-a"),
                Generation::new(5),
                expected_target.clone(),
            ),
        )
        .await
        .expect_err(name);

        assert!(matches!(
            error,
            WakeInstanceError::ReadyMaterializationNotFound {
                instance,
                target
            } if instance == running && target == expected_target
        ));
        assert_eq!(
            store.events(),
            vec![StoreEvent::LoadReadyMaterialization {
                instance_id: instance_id("instance-a"),
                instance_generation: Generation::new(5),
                target: target("cluster-a", "apps"),
            }]
        );
        assert!(client.applied_objects().is_empty());
    }
}

#[tokio::test]
async fn stale_running_expected_generation_does_not_load_materialization() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Running, 5),
        Some(workload_class()),
    );
    store.set_materialization(ready_materialization(
        "instance-a",
        5,
        target("cluster-a", "apps"),
        "http://svc-acme.apps.svc.cluster.local:80",
    ));
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(4),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect_err("stale wake is rejected before materialization lookup");

    assert_generation_conflict(error, Generation::new(4), Generation::new(5));
    assert!(store.events().is_empty());
    assert!(client.applied_objects().is_empty());
}

#[tokio::test]
async fn restart_during_wake_resumes_waking_generation_without_new_cas() {
    let waking = instance("instance-a", InstanceState::Waking, 5);
    let store = FakeStore::new(waking.clone(), Some(workload_class()));
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let result = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(5),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect("persisted waking generation resumes after restart");

    let WakeInstanceResult::Completed { result } = result else {
        panic!("expected wake replay to complete");
    };
    assert_eq!(result.instance.state, InstanceState::Running);
    assert_eq!(result.instance.generation, Generation::new(6));
    assert_eq!(
        result.materialization.instance_generation,
        Generation::new(6)
    );
    assert_eq!(
        store.events(),
        vec![StoreEvent::Complete {
            expected_waking_generation: Generation::new(5),
            backend_generation: BackendGeneration::new(5),
            rendered_objects: vec![
                object_ref("v1", "Service", "apps", "svc-acme"),
                object_ref("apps/v1", "Deployment", "apps", "app-acme"),
            ],
        }]
    );
    assert_eq!(client.applied_objects().len(), 2);
}

#[tokio::test]
async fn failed_and_draining_instances_can_start_wake() {
    for state in [InstanceState::Failed, InstanceState::Draining] {
        let store = FakeStore::new(instance("instance-a", state, 12), Some(workload_class()));
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());

        let result = wake_instance(
            &store,
            &materializer,
            WakeInstanceRequest::new(
                instance_id("instance-a"),
                Generation::new(12),
                target("cluster-a", "apps"),
            ),
        )
        .await
        .expect("wake succeeds");

        assert_eq!(result.instance().state, InstanceState::Running);
        assert_eq!(result.instance().generation, Generation::new(14));
        let mut expected_events = Vec::new();
        if state == InstanceState::Draining {
            expected_events.push(StoreEvent::LoadActiveMaterialization {
                instance_id: instance_id("instance-a"),
                target: target("cluster-a", "apps"),
            });
        }
        expected_events.push(StoreEvent::Cas {
            expected: Generation::new(12),
            next_state: InstanceState::Waking,
            reason: StateTransitionReason::WakeRequested,
        });
        expected_events.push(StoreEvent::Complete {
            expected_waking_generation: Generation::new(13),
            backend_generation: BackendGeneration::new(13),
            rendered_objects: vec![
                object_ref("v1", "Service", "apps", "svc-acme"),
                object_ref("apps/v1", "Deployment", "apps", "app-acme"),
            ],
        });
        assert_eq!(store.events(), expected_events);
        assert_eq!(client.applied_objects().len(), 2);
    }
}

#[tokio::test]
async fn render_failure_marks_waking_generation_failed_without_apply() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 4),
        Some(workload_class()),
    );
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(4),
            target("cluster-a", "Bad_Namespace"),
        ),
    )
    .await
    .expect_err("invalid namespace fails render");

    assert!(matches!(error, WakeInstanceError::Render { .. }));
    assert_eq!(store.instance().state, InstanceState::Failed);
    assert_eq!(store.instance().generation, Generation::new(6));
    assert_eq!(
            store.events(),
            vec![
                StoreEvent::Cas {
                    expected: Generation::new(4),
                    next_state: InstanceState::Waking,
                    reason: StateTransitionReason::WakeRequested,
                },
                StoreEvent::Cas {
                    expected: Generation::new(5),
                    next_state: InstanceState::Failed,
                    reason: StateTransitionReason::FailureReported(
                        "manifest render failed: namespace rendered invalid Kubernetes name \"Bad_Namespace\""
                            .to_owned()
                    ),
                },
            ]
        );
    assert!(client.applied_objects().is_empty());
}

#[tokio::test]
async fn materializer_failure_marks_waking_generation_failed() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 6),
        Some(workload_class()),
    );
    let client = FakeKubernetesClient::default();
    client.fail_readiness();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(6),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect_err("readiness failure fails wake");

    assert!(matches!(error, WakeInstanceError::Materializer { .. }));
    assert_eq!(store.instance().state, InstanceState::Failed);
    assert_eq!(store.instance().generation, Generation::new(8));
    assert_eq!(client.applied_objects().len(), 2);
    assert_eq!(
        store
            .events()
            .into_iter()
            .map(|event| match event {
                StoreEvent::Cas {
                    expected,
                    next_state,
                    ..
                } => (expected, next_state),
                StoreEvent::Complete { .. } => panic!("wake must not complete"),
                StoreEvent::LoadReadyMaterialization { .. } => {
                    panic!("cold wake failure must not load materialization")
                }
                StoreEvent::LoadActiveMaterialization { .. } => {
                    panic!("cold wake failure must not load active materialization")
                }
                StoreEvent::FinalizeSleep { .. } => {
                    panic!("cold wake failure must not finalize sleep")
                }
            })
            .collect::<Vec<_>>(),
        vec![
            (Generation::new(6), InstanceState::Waking),
            (Generation::new(7), InstanceState::Failed),
        ]
    );
}

#[tokio::test]
async fn materializer_failure_records_wake_and_failure_observability_fields() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 6),
        Some(workload_class()),
    );
    let client = FakeKubernetesClient::default();
    client.fail_readiness();
    let materializer = KubernetesMaterializer::new(client.clone());
    let sink = InMemoryObservability::default();

    let error = wake_instance_with_observability(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(6),
            target("cluster-a", "apps"),
        ),
        sink.recorder(),
    )
    .await
    .expect_err("readiness failure fails wake");

    assert!(matches!(error, WakeInstanceError::Materializer { .. }));
    let events = sink.events();
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_WAKE_LATENCY_SECONDS_NAME
                && metric.labels().iter().any(|label| label.value() == "error")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_MATERIALIZATION_FAILURES_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "materialize")
                && metric.labels().iter().any(|label| label.value() == "error")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Log(log)
            if log.name() == EVENT_WAKE
                && log.field_value(FIELD_INSTANCE_ID) == Some("instance-a")
                && log.field_value(FIELD_CLUSTER_ID) == Some("cluster-a")
                && log.field_value(FIELD_NAMESPACE) == Some("apps")
                && log.field_value(FIELD_ERROR_REASON) == Some("materializer")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Log(log)
            if log.name() == EVENT_MATERIALIZATION_FAILURE
                && log.field_value(FIELD_INSTANCE_ID) == Some("instance-a")
                && log.field_value(FIELD_ERROR_REASON).is_some()
    )));
}

#[tokio::test]
async fn complete_generation_conflict_after_apply_is_returned_without_retry() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 9),
        Some(workload_class()),
    );
    store.set_complete_conflict();
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(9),
            target("cluster-a", "apps"),
        )
        .with_backend_generation(BackendGeneration::new(44)),
    )
    .await
    .expect_err("complete conflict is surfaced");

    assert_generation_conflict(error, Generation::new(10), Generation::new(11));
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(9),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(10),
                backend_generation: BackendGeneration::new(44),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme"),
                ],
            },
        ]
    );
    assert_eq!(store.instance().state, InstanceState::Waking);
    assert_eq!(store.instance().generation, Generation::new(10));
    assert_eq!(client.applied_objects().len(), 2);
}

#[tokio::test]
async fn deleting_and_deleted_are_unavailable_and_do_not_apply() {
    for (state, reason) in [
        (InstanceState::Deleting, WakeUnavailableReason::Deleting),
        (InstanceState::Deleted, WakeUnavailableReason::Deleted),
    ] {
        let store = FakeStore::new(instance("instance-a", state, 3), Some(workload_class()));
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());

        let error = wake_instance(
            &store,
            &materializer,
            WakeInstanceRequest::new(
                instance_id("instance-a"),
                Generation::new(3),
                target("cluster-a", "apps"),
            ),
        )
        .await
        .expect_err("unavailable state");

        assert!(matches!(
            error,
            WakeInstanceError::Unavailable {
                reason: actual,
                ..
            } if actual == reason
        ));
        assert!(store.events().is_empty());
        assert!(client.applied_objects().is_empty());
    }
}

#[tokio::test]
async fn draining_with_deleting_materialization_waits_for_sleep_cleanup() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Draining, 13),
        Some(workload_class()),
    );
    let rendered_objects = vec![object_ref("apps/v1", "Deployment", "apps", "instance-a")];
    store.set_materialization(materialization(
        "instance-a",
        12,
        target("cluster-a", "apps"),
        MaterializationState::Deleting,
        None,
    ));
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let result = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(13),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect("deleting materialization sleep cleanup resumes before wake");

    assert_eq!(result.instance().state, InstanceState::Running);
    assert_eq!(result.instance().generation, Generation::new(16));
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::LoadActiveMaterialization {
                instance_id: instance_id("instance-a"),
                target: target("cluster-a", "apps"),
            },
            StoreEvent::FinalizeSleep {
                expected_draining_generation: Generation::new(13),
                target: target("cluster-a", "apps"),
            },
            StoreEvent::Cas {
                expected: Generation::new(14),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(15),
                backend_generation: BackendGeneration::new(15),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme"),
                ],
            },
        ]
    );
    assert_eq!(client.deleted_objects(), rendered_objects);
    assert_eq!(client.applied_objects().len(), 2);
}

impl FakeStore {
    fn new(instance: InstanceRecord, workload_class: Option<WorkloadClassVersion>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeStoreState {
                instance: Some(instance),
                workload_class,
                materialization: None,
                events: Vec::new(),
                complete_conflict: false,
            })),
        }
    }

    fn events(&self) -> Vec<StoreEvent> {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .events
            .clone()
    }

    fn instance(&self) -> InstanceRecord {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .instance
            .clone()
            .expect("fake instance")
    }

    fn set_complete_conflict(&self) {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .complete_conflict = true;
    }

    fn set_materialization(&self, materialization: MaterializationRecord) {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .materialization = Some(materialization);
    }
}

impl ControlPlaneStore for FakeStore {
    fn create_instance<'a>(
        &'a self,
        _request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async move {
            let inner = self.inner.lock().expect("fake store lock not poisoned");
            Ok(inner
                .instance
                .as_ref()
                .filter(|instance| instance.id == request.instance_id)
                .cloned())
        })
    }

    fn delete_instance<'a>(
        &'a self,
        _request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn create_workload_class_version<'a>(
        &'a self,
        _request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        Box::pin(async move {
            let inner = self.inner.lock().expect("fake store lock not poisoned");
            Ok(inner
                .workload_class
                .as_ref()
                .filter(|workload| workload.reference == request.reference)
                .cloned())
        })
    }

    fn create_route_binding<'a>(
        &'a self,
        _request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn get_route_binding<'a>(
        &'a self,
        _request: GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn delete_route_binding<'a>(
        &'a self,
        _request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn resolve_route<'a>(
        &'a self,
        _identity: RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("fake store lock not poisoned");
            let instance = inner.instance.as_mut().ok_or(StoreError::NotFound {
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
            let updated = instance.clone();
            inner.events.push(StoreEvent::Cas {
                expected: request.expected_generation,
                next_state: request.next_state,
                reason: request.reason,
            });
            Ok(updated)
        })
    }

    fn record_materialization<'a>(
        &'a self,
        _request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("fake store lock not poisoned");
            inner.events.push(StoreEvent::LoadReadyMaterialization {
                instance_id: request.instance_id.clone(),
                instance_generation: request.instance_generation,
                target: request.target.clone(),
            });
            Ok(inner
                .materialization
                .as_ref()
                .filter(|materialization| {
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
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("fake store lock not poisoned");
            inner.events.push(StoreEvent::LoadActiveMaterialization {
                instance_id: request.instance_id.clone(),
                target: request.target.clone(),
            });
            Ok(inner
                .materialization
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
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("fake store lock not poisoned");
            inner.events.push(StoreEvent::Complete {
                expected_waking_generation: request.expected_waking_generation,
                backend_generation: request.backend_generation,
                rendered_objects: request.rendered_objects.clone(),
            });
            if inner.complete_conflict {
                return Err(StoreError::GenerationConflict {
                    expected: request.expected_waking_generation,
                    actual: request.expected_waking_generation.next(),
                });
            }

            let instance = inner.instance.as_mut().ok_or(StoreError::NotFound {
                resource: "instance",
            })?;
            if instance.generation != request.expected_waking_generation {
                return Err(StoreError::GenerationConflict {
                    expected: request.expected_waking_generation,
                    actual: instance.generation,
                });
            }

            instance.state = InstanceState::Running;
            instance.generation = request.expected_waking_generation.next();
            let instance = instance.clone();
            let materialization = MaterializationRecord {
                id: MaterializationId::new(format!(
                    "{}:{}:{}",
                    request.instance_id.as_str(),
                    request.target.cluster_id(),
                    request.target.namespace()
                ))
                .expect("materialization ID"),
                instance_id: request.instance_id,
                instance_generation: instance.generation,
                target: request.target,
                state: MaterializationState::Ready,
                backend: Some(request.backend),
                backend_generation: request.backend_generation,
                rendered_objects: request.rendered_objects,
            };

            Ok(CompleteWakeResult {
                instance,
                materialization,
            })
        })
    }

    fn begin_sleep<'a>(
        &'a self,
        _request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("fake store lock not poisoned");
            inner.events.push(StoreEvent::FinalizeSleep {
                expected_draining_generation: request.expected_draining_generation,
                target: request.target.clone(),
            });
            let instance = inner.instance.as_mut().ok_or(StoreError::NotFound {
                resource: "instance",
            })?;
            if instance.generation != request.expected_draining_generation {
                return Err(StoreError::GenerationConflict {
                    expected: request.expected_draining_generation,
                    actual: instance.generation,
                });
            }

            instance.state = InstanceState::Cold;
            instance.generation = request.expected_draining_generation.next();
            let instance = instance.clone();
            let materialization = inner.materialization.as_mut().map(|materialization| {
                materialization.state = MaterializationState::Deleted;
                materialization.instance_generation = instance.generation;
                materialization.rendered_objects.clear();
                materialization.clone()
            });

            Ok(FinalizeSleepResult {
                instance,
                materialization,
            })
        })
    }

    fn lookup_route_dependencies<'a>(
        &'a self,
        _request: RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn put_http01_challenge<'a>(
        &'a self,
        _request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        _key: Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        _request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        _request: ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        Box::pin(async { Err(StoreError::internal("not implemented")) })
    }
}

impl FakeKubernetesClient {
    fn applied_objects(&self) -> Vec<KubernetesObject> {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .applied_objects
            .clone()
    }

    fn deleted_objects(&self) -> Vec<RenderedObjectRef> {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .deleted_objects
            .clone()
    }

    fn readiness_calls(&self) -> Vec<Vec<RenderedObjectRef>> {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .readiness_calls
            .clone()
    }

    fn fail_readiness(&self) {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .fail_readiness = true;
    }
}

impl KubernetesMaterializerClient for FakeKubernetesClient {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.inner
                .lock()
                .expect("fake kubernetes lock not poisoned")
                .applied_objects
                .push(object.clone());
            Ok(())
        })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            self.inner
                .lock()
                .expect("fake kubernetes lock not poisoned")
                .deleted_objects
                .push(object.clone());
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
        objects: &'a [RenderedObjectRef],
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<BackendEndpoint>> {
        Box::pin(async move {
            let mut inner = self
                .inner
                .lock()
                .expect("fake kubernetes lock not poisoned");
            inner.readiness_calls.push(objects.to_vec());
            if inner.fail_readiness {
                return Err(KubernetesClientError::new("not ready"));
            }
            Ok(inner.backend.clone())
        })
    }
}

fn assert_generation_conflict(error: WakeInstanceError, expected: Generation, actual: Generation) {
    assert!(matches!(
        error,
        WakeInstanceError::GenerationConflict {
            expected: found_expected,
            actual: found_actual,
        } if found_expected == expected && found_actual == actual
    ));
}

fn assert_env(env: &[crate::manifest::EnvVar], name: &str, expected: &str) {
    let value = env
        .iter()
        .find(|var| var.name == name)
        .unwrap_or_else(|| panic!("missing env var {name}"));

    assert_eq!(value.value, expected);
}

fn workload_class() -> WorkloadClassVersion {
    WorkloadClassVersion {
        reference: workload_ref(),
        template_generation: Generation::new(3),
        template: deployment_template(),
        default_values: InstanceValues::new(),
        value_schema: WorkloadValueSchema::new(true),
        sleep_policy: WorkloadSleepPolicy::new(120_000, 5_000, 30_000).expect("valid sleep policy"),
    }
}

fn deployment_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::Deployment,
            name: TemplateText::literal("app-acme"),
            replicas: None,
            app_container: ContainerTemplate {
                name: "app".to_owned(),
                image: TemplateText::literal("example/app:1"),
                ports: vec![ContainerPortTemplate {
                    name: Some("http".to_owned()),
                    container_port: 8080,
                }],
                env: vec![EnvVarTemplate {
                    name: "TENANT".to_owned(),
                    value: TemplateText::literal("acme"),
                }],
            },
        },
        service: Some(ServiceTemplate {
            name: TemplateText::literal("svc-acme"),
            ports: vec![ServicePortTemplate {
                name: Some("http".to_owned()),
                port: 80,
                target_port: 8080,
            }],
        }),
        sidecar: SidecarTemplate {
            name: "sleepypods-sidecar".to_owned(),
            image: TemplateText::literal("sleepypods/sidecar:test"),
            listen_port: 15000,
        },
        volumes: Vec::new(),
    }
}

fn instance(id: &str, state: InstanceState, generation: u64) -> InstanceRecord {
    InstanceRecord {
        id: instance_id(id),
        workload_class: workload_ref(),
        values: InstanceValues::new(),
        state,
        generation: Generation::new(generation),
    }
}

fn workload_ref() -> WorkloadClassVersionRef {
    WorkloadClassVersionRef::new(
        WorkloadClassId::new("web").expect("workload class ID"),
        Generation::new(1),
    )
}

fn target(cluster_id: &str, namespace: &str) -> MaterializationTarget {
    MaterializationTarget::new(cluster_id, namespace).expect("materialization target")
}

fn backend(uri: &str) -> BackendEndpoint {
    BackendEndpoint::new(uri).expect("backend endpoint")
}

fn object_ref(api_version: &str, kind: &str, namespace: &str, name: &str) -> RenderedObjectRef {
    RenderedObjectRef {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    }
}

fn ready_materialization(
    instance_id: &str,
    instance_generation: u64,
    target: MaterializationTarget,
    backend_uri: &str,
) -> MaterializationRecord {
    materialization(
        instance_id,
        instance_generation,
        target,
        MaterializationState::Ready,
        Some(backend(backend_uri)),
    )
}

fn materialization(
    instance_id: &str,
    instance_generation: u64,
    target: MaterializationTarget,
    state: MaterializationState,
    backend: Option<BackendEndpoint>,
) -> MaterializationRecord {
    MaterializationRecord {
        id: MaterializationId::new(format!(
            "{}:{}:{}",
            instance_id,
            target.cluster_id(),
            target.namespace()
        ))
        .expect("materialization ID"),
        instance_id: self::instance_id(instance_id),
        instance_generation: Generation::new(instance_generation),
        target: target.clone(),
        state,
        backend,
        backend_generation: BackendGeneration::new(instance_generation),
        rendered_objects: vec![object_ref(
            "apps/v1",
            "Deployment",
            target.namespace(),
            instance_id,
        )],
    }
}

fn instance_id(id: &str) -> InstanceId {
    InstanceId::new(id).expect("instance ID")
}
