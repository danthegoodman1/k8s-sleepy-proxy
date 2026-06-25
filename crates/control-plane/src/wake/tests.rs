use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use proxy_core::observability::{
    metrics::{RUNTIME_MATERIALIZATION_FAILURES_TOTAL_NAME, RUNTIME_WAKE_LATENCY_SECONDS_NAME},
    recorder::{
        InMemoryObservability, ObservabilityEvent, EVENT_MATERIALIZATION_FAILURE, EVENT_WAKE,
        FIELD_CLUSTER_ID, FIELD_ERROR_REASON, FIELD_EXCLUSIVITY_ACTION, FIELD_EXCLUSIVITY_KEY_NAME,
        FIELD_EXCLUSIVITY_OWNER_INSTANCE_ID, FIELD_INSTANCE_ID, FIELD_NAMESPACE,
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
        ManifestTemplate, PersistentVolumeAccessMode, PersistentVolumeReclaimPolicy,
        PersistentVolumeSourceTemplate, ServicePortTemplate, ServiceTemplate, SidecarTemplate,
        TemplateText, VolumeTemplate, WorkloadKind, WorkloadTemplate,
    },
    materialization::{
        BackendEndpoint, BeginSleepRequest, BeginSleepResult, FinalizeSleepRequest,
        FinalizeSleepResult, LoadActiveMaterializationRequest, LoadReadyMaterializationRequest,
        MaterializationRecord, MaterializationState, RecordMaterializationRequest,
        RenderedObjectRef,
    },
    materializer::{
        KubernetesClientError, KubernetesClientFuture, KubernetesClientResult,
        KubernetesMaterializer,
    },
    projection::{
        LiveObjectMetadata, ProjectionObjectInspection, ANNOTATION_MATERIALIZATION_ID,
        LABEL_MANAGED_BY, LABEL_MANAGED_BY_VALUE,
    },
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
        RouteBindingRecord, RouteDependencyLookup, RouteDependencySet, RouteIdentity,
        RouteResolution,
    },
    sleep_policy::WorkloadSleepPolicy,
    store::{StoreFuture, StoreResult},
    workload::{
        CreateWorkloadClassVersionRequest, RenderedExclusivityKey, WorkloadClassVersion,
        WorkloadClassVersionRef, WorkloadExclusivityKeyTemplate, WorkloadValueSchema,
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
    delete_before_record_materialization: bool,
    delete_after_record_materialization: bool,
    reject_record_materialization_collision: bool,
    reject_record_materialization_exclusivity_conflict: bool,
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
    RecordMaterialization {
        instance_generation: Generation,
        state: MaterializationState,
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
    observed_store: Option<FakeStore>,
}

#[derive(Clone, Debug)]
struct FakeKubernetesState {
    applied_objects: Vec<KubernetesObject>,
    deleted_objects: Vec<RenderedObjectRef>,
    pvc_bound_calls: Vec<(String, String)>,
    readiness_calls: Vec<Vec<RenderedObjectRef>>,
    backend: BackendEndpoint,
    fail_pvc_wait: Option<(String, String)>,
    fail_apply: Option<RenderedObjectRef>,
    fail_readiness: bool,
    first_apply_store_events: Option<Vec<StoreEvent>>,
    live_objects: BTreeMap<String, ProjectionObjectInspection>,
}

impl Default for FakeKubernetesState {
    fn default() -> Self {
        Self {
            applied_objects: Vec::new(),
            deleted_objects: Vec::new(),
            pvc_bound_calls: Vec::new(),
            readiness_calls: Vec::new(),
            backend: backend("http://svc-acme-instance.apps.svc.cluster.local:80"),
            fail_pvc_wait: None,
            fail_apply: None,
            fail_readiness: false,
            first_apply_store_events: None,
            live_objects: BTreeMap::new(),
        }
    }
}

#[tokio::test]
async fn successful_wake_cas_renders_applies_and_completes_with_waking_generation() {
    let instance = instance("instance-a", InstanceState::Cold, 1);
    let store = FakeStore::new(instance, Some(workload_class()));
    let client = FakeKubernetesClient::observing_store(store.clone());
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
        Some(backend(
            "http://svc-acme-instance.apps.svc.cluster.local:80"
        ))
    );
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(1),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(2),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(2),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(2),
                backend_generation: BackendGeneration::new(2),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
        ]
    );
    assert_eq!(
        client.readiness_calls(),
        vec![vec![
            object_ref("v1", "Service", "apps", "svc-acme-instance"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
        ]]
    );
    assert_eq!(
        client.first_apply_store_events(),
        Some(vec![
            StoreEvent::Cas {
                expected: Generation::new(1),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(2),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(2),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
        ])
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
async fn wake_records_rendered_exclusivity_keys_before_kubernetes_apply() {
    let mut instance = instance("instance-a", InstanceState::Cold, 1);
    instance
        .values
        .insert("volume_handle".to_owned(), "disk-a".to_owned());
    instance
        .values
        .insert("license_handle".to_owned(), "license-a".to_owned());
    let store = FakeStore::new(instance, Some(exclusive_workload_class()));
    let client = FakeKubernetesClient::observing_store(store.clone());
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
    let expected_keys = vec![
        RenderedExclusivityKey::new("disk", "disk-a"),
        RenderedExclusivityKey::new("license", "license-a"),
    ];
    assert_eq!(result.materialization.exclusivity_keys, expected_keys);
    assert_eq!(
        store
            .materialization()
            .expect("materialization is recorded")
            .exclusivity_keys,
        expected_keys
    );
    assert_eq!(
        client.first_apply_store_events(),
        Some(vec![
            StoreEvent::Cas {
                expected: Generation::new(1),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(2),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(2),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
        ])
    );
}

#[tokio::test]
async fn exclusivity_conflict_prevents_kubernetes_apply_and_materialization_record() {
    let mut instance = instance("instance-a", InstanceState::Cold, 1);
    instance
        .values
        .insert("volume_handle".to_owned(), "disk-a".to_owned());
    instance
        .values
        .insert("license_handle".to_owned(), "license-a".to_owned());
    let store = FakeStore::new(instance, Some(exclusive_workload_class()));
    store.reject_record_materialization_exclusivity_conflict();
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
    .expect_err("exclusivity conflict rejects wake before apply");

    assert!(matches!(
        error,
        WakeInstanceError::Store(StoreError::ExclusivityConflict {
            key_name,
            owner_instance_id: Some(owner),
            ..
        }) if key_name == "disk" && owner == "instance-b"
    ));
    assert_eq!(store.instance().state, InstanceState::Failed);
    assert_eq!(store.instance().generation, Generation::new(3));
    assert!(store.materialization().is_none());
    assert!(client.applied_objects().is_empty());
    assert!(client.deleted_objects().is_empty());
    assert!(client.readiness_calls().is_empty());
}

#[tokio::test]
async fn exclusivity_conflict_observability_uses_bounded_fields() {
    let mut instance = instance("instance-a", InstanceState::Cold, 1);
    instance
        .values
        .insert("volume_handle".to_owned(), "disk-secret-ish".to_owned());
    instance
        .values
        .insert("license_handle".to_owned(), "license-a".to_owned());
    let store = FakeStore::new(instance, Some(exclusive_workload_class()));
    store.reject_record_materialization_exclusivity_conflict();
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client);
    let sink = InMemoryObservability::default();

    let error = wake_instance_with_observability(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(1),
            target("cluster-a", "apps"),
        ),
        sink.recorder(),
    )
    .await
    .expect_err("exclusivity conflict rejects wake");

    assert!(matches!(
        error,
        WakeInstanceError::Store(StoreError::ExclusivityConflict { .. })
    ));
    let events = sink.events();
    let wake_log = events
        .iter()
        .find_map(|event| match event {
            ObservabilityEvent::Log(log) if log.name() == EVENT_WAKE => Some(log),
            _ => None,
        })
        .expect("wake log is recorded");
    assert_eq!(
        wake_log.field_value(FIELD_ERROR_REASON),
        Some("exclusivity_conflict")
    );
    assert_eq!(
        wake_log.field_value(FIELD_EXCLUSIVITY_ACTION),
        Some("conflict")
    );
    assert_eq!(
        wake_log.field_value(FIELD_EXCLUSIVITY_KEY_NAME),
        Some("disk")
    );
    assert_eq!(
        wake_log.field_value(FIELD_EXCLUSIVITY_OWNER_INSTANCE_ID),
        Some("instance-b")
    );
    assert!(
        wake_log
            .fields()
            .iter()
            .all(|field| field.value() != "disk-secret-ish"),
        "rendered opaque key values must not be logged"
    );
}

#[tokio::test]
async fn first_apply_failure_after_exclusivity_acquire_releases_pending_key() {
    let mut instance = instance("instance-a", InstanceState::Cold, 1);
    instance
        .values
        .insert("volume_handle".to_owned(), "disk-a".to_owned());
    instance
        .values
        .insert("license_handle".to_owned(), "license-a".to_owned());
    let store = FakeStore::new(instance, Some(exclusive_stateful_workload_class()));
    let client = FakeKubernetesClient::default();
    client.fail_apply(object_ref("v1", "PersistentVolume", "", "pv-acme-instance"));
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
    .expect_err("first Kubernetes apply failure fails wake");

    assert!(matches!(error, WakeInstanceError::Projection { .. }));
    assert!(client.applied_objects().is_empty());
    assert!(client.deleted_objects().is_empty());
    assert!(client.readiness_calls().is_empty());
    assert_eq!(store.instance().state, InstanceState::Failed);
    let materialization = store
        .materialization()
        .expect("failed no-object wake records deleted materialization");
    assert_eq!(materialization.state, MaterializationState::Deleted);
    assert!(materialization.rendered_objects.is_empty());
    assert!(materialization.exclusivity_keys.is_empty());
    assert!(
        store
            .load_active_materialization(LoadActiveMaterializationRequest::new(
                instance_id("instance-a"),
                target("cluster-a", "apps"),
            ))
            .await
            .expect("active materialization loads")
            .is_none(),
        "deleted no-object materialization must not keep the key held"
    );
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(1),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(2),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(2),
                rendered_objects: vec![
                    object_ref("v1", "PersistentVolume", "", "pv-acme-instance"),
                    object_ref(
                        "v1",
                        "PersistentVolumeClaim",
                        "apps",
                        "pvc-acme-instance"
                    ),
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "StatefulSet", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(2),
                state: MaterializationState::Deleted,
                backend_generation: BackendGeneration::new(2),
                rendered_objects: Vec::new(),
            },
            StoreEvent::Cas {
                expected: Generation::new(2),
                next_state: InstanceState::Failed,
                reason: StateTransitionReason::FailureReported(
                    "projection failed: projection apply rejected: failed to apply PersistentVolume /pv-acme-instance: apply failed".to_owned()
                ),
            },
            StoreEvent::LoadActiveMaterialization {
                instance_id: instance_id("instance-a"),
                target: target("cluster-a", "apps"),
            },
        ]
    );
}

#[tokio::test]
async fn unowned_live_ref_blocks_wake_before_apply_and_keeps_pending_materialization() {
    let instance = instance("instance-a", InstanceState::Cold, 1);
    let store = FakeStore::new(instance, Some(workload_class()));
    let client = FakeKubernetesClient::default();
    client.set_unowned_live_object(object_ref("v1", "Service", "apps", "svc-acme-instance"));
    let materializer = KubernetesMaterializer::new(client.clone());
    let sink = InMemoryObservability::default();

    let error = wake_instance_with_observability(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(1),
            target("cluster-a", "apps"),
        ),
        sink.recorder(),
    )
    .await
    .expect_err("unowned live object blocks wake");

    assert!(matches!(error, WakeInstanceError::Projection { .. }));
    assert!(client.applied_objects().is_empty());
    assert!(client.deleted_objects().is_empty());
    assert!(client.pvc_bound_calls().is_empty());
    assert!(client.readiness_calls().is_empty());

    let failed = store.instance();
    assert_eq!(failed.state, InstanceState::Failed);
    assert_eq!(failed.generation, Generation::new(3));

    let materialization = store
        .materialization()
        .expect("blocked wake keeps pending materialization for inspection");
    assert_eq!(materialization.state, MaterializationState::Pending);
    assert_eq!(
        materialization.rendered_objects,
        vec![
            object_ref("v1", "Service", "apps", "svc-acme-instance"),
            object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
        ]
    );
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(1),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(2),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(2),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::Cas {
                expected: Generation::new(2),
                next_state: InstanceState::Failed,
                reason: StateTransitionReason::FailureReported(
                    "projection failed: projection ownership conflict across 1 object(s)"
                        .to_owned(),
                ),
            },
        ]
    );

    let events = sink.events();
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Log(log)
            if log.name() == EVENT_WAKE
                && log.field_value(FIELD_INSTANCE_ID) == Some("instance-a")
                && log.field_value(FIELD_ERROR_REASON) == Some("projection")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObservabilityEvent::Log(log)
            if log.name() == EVENT_MATERIALIZATION_FAILURE
                && log.field_value(FIELD_INSTANCE_ID) == Some("instance-a")
                && log
                    .field_value(FIELD_ERROR_REASON)
                    .is_some_and(|reason| reason.contains("ownership conflict"))
    )));
}

#[tokio::test]
async fn apply_failure_after_objects_may_exist_keeps_exclusivity_key_held() {
    let mut instance = instance("instance-a", InstanceState::Cold, 1);
    instance
        .values
        .insert("volume_handle".to_owned(), "disk-a".to_owned());
    instance
        .values
        .insert("license_handle".to_owned(), "license-a".to_owned());
    let store = FakeStore::new(instance, Some(exclusive_stateful_workload_class()));
    let client = FakeKubernetesClient::default();
    client.fail_apply(object_ref("v1", "Service", "apps", "svc-acme-instance"));
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
    .expect_err("later Kubernetes apply failure fails wake");

    assert!(matches!(error, WakeInstanceError::Projection { .. }));
    assert_eq!(client.applied_objects().len(), 2);
    assert_eq!(
        client.deleted_objects(),
        vec![
            object_ref("v1", "PersistentVolumeClaim", "apps", "pvc-acme-instance"),
            object_ref("v1", "PersistentVolume", "", "pv-acme-instance"),
        ]
    );
    let materialization = store
        .materialization()
        .expect("failed partial apply keeps pending materialization");
    assert_eq!(materialization.state, MaterializationState::Pending);
    assert_eq!(
        materialization.exclusivity_keys,
        vec![
            RenderedExclusivityKey::new("disk", "disk-a"),
            RenderedExclusivityKey::new("license", "license-a"),
        ]
    );
    assert!(
        store
            .load_active_materialization(LoadActiveMaterializationRequest::new(
                instance_id("instance-a"),
                target("cluster-a", "apps"),
            ))
            .await
            .expect("active materialization loads")
            .is_some(),
        "partial apply failure must keep the key held until cleanup is finalized"
    );
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
        "http://svc-acme-instance.apps.svc.cluster.local:80",
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
                Some(backend(
                    "http://svc-acme-instance.other.svc.cluster.local:80",
                )),
            )),
        ),
        (
            "non-ready state",
            Some(materialization(
                "instance-a",
                5,
                target("cluster-a", "apps"),
                MaterializationState::Pending,
                Some(backend(
                    "http://svc-acme-instance.apps.svc.cluster.local:80",
                )),
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
        "http://svc-acme-instance.apps.svc.cluster.local:80",
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
        vec![
            StoreEvent::LoadActiveMaterialization {
                instance_id: instance_id("instance-a"),
                target: target("cluster-a", "apps"),
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(5),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(5),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(5),
                backend_generation: BackendGeneration::new(5),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
        ]
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
        if state == InstanceState::Failed {
            expected_events.push(StoreEvent::LoadActiveMaterialization {
                instance_id: instance_id("instance-a"),
                target: target("cluster-a", "apps"),
            });
        }
        expected_events.push(StoreEvent::RecordMaterialization {
            instance_generation: Generation::new(13),
            state: MaterializationState::Pending,
            backend_generation: BackendGeneration::new(13),
            rendered_objects: vec![
                object_ref("v1", "Service", "apps", "svc-acme-instance"),
                object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
            ],
        });
        expected_events.push(StoreEvent::Complete {
            expected_waking_generation: Generation::new(13),
            backend_generation: BackendGeneration::new(13),
            rendered_objects: vec![
                object_ref("v1", "Service", "apps", "svc-acme-instance"),
                object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
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
                } => Some((expected, next_state)),
                StoreEvent::RecordMaterialization {
                    instance_generation,
                    state,
                    backend_generation,
                    rendered_objects,
                } => {
                    assert_eq!(instance_generation, Generation::new(7));
                    assert_eq!(state, MaterializationState::Pending);
                    assert_eq!(backend_generation, BackendGeneration::new(7));
                    assert_eq!(
                        rendered_objects,
                        vec![
                            object_ref("v1", "Service", "apps", "svc-acme-instance"),
                            object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                        ]
                    );
                    None
                }
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
            .flatten()
            .collect::<Vec<_>>(),
        vec![
            (Generation::new(6), InstanceState::Waking),
            (Generation::new(7), InstanceState::Failed),
        ]
    );
}

#[tokio::test]
async fn retry_after_failed_high_backend_generation_does_not_rewind_pending_materialization() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 1),
        Some(workload_class()),
    );
    let first_client = FakeKubernetesClient::default();
    first_client.fail_readiness();
    let first_materializer = KubernetesMaterializer::new(first_client.clone());

    let first_error = wake_instance(
        &store,
        &first_materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(1),
            target("cluster-a", "apps"),
        )
        .with_backend_generation(BackendGeneration::new(100)),
    )
    .await
    .expect_err("first wake fails after recording high backend generation");

    assert!(matches!(
        first_error,
        WakeInstanceError::Materializer { .. }
    ));
    assert_eq!(store.instance().state, InstanceState::Failed);
    assert_eq!(store.instance().generation, Generation::new(3));
    assert_eq!(
        store.materialization().map(|record| (
            record.state,
            record.backend_generation,
            record.backend
        )),
        Some((
            MaterializationState::Pending,
            BackendGeneration::new(100),
            None
        ))
    );

    let retry_client = FakeKubernetesClient::default();
    let retry_materializer = KubernetesMaterializer::new(retry_client.clone());
    let retry_result = wake_instance(
        &store,
        &retry_materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(3),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect("retry reuses active backend generation instead of rewinding");

    let WakeInstanceResult::Completed { result } = retry_result else {
        panic!("retry should complete");
    };
    assert_eq!(result.instance.state, InstanceState::Running);
    assert_eq!(result.instance.generation, Generation::new(5));
    assert_eq!(
        result.materialization.backend_generation,
        BackendGeneration::new(100)
    );
    assert_eq!(
        result
            .materialization
            .backend
            .as_ref()
            .map(BackendEndpoint::uri),
        Some("http://svc-acme-instance.apps.svc.cluster.local:80")
    );
    assert_eq!(retry_client.applied_objects().len(), 2);
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(1),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(2),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(100),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::Cas {
                expected: Generation::new(2),
                next_state: InstanceState::Failed,
                reason: StateTransitionReason::FailureReported(
                    "materialization failed: failed waiting for readiness across 2 rendered Kubernetes objects: not ready"
                        .to_owned()
                ),
            },
            StoreEvent::Cas {
                expected: Generation::new(3),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::LoadActiveMaterialization {
                instance_id: instance_id("instance-a"),
                target: target("cluster-a", "apps"),
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(4),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(100),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(4),
                backend_generation: BackendGeneration::new(100),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
        ]
    );
}

#[tokio::test]
async fn pvc_bound_failure_keeps_pending_stateful_refs_for_delete_or_retry() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 6),
        Some(stateful_workload_class()),
    );
    let client = FakeKubernetesClient::observing_store(store.clone());
    client.fail_pvc_wait("apps", "pvc-acme-instance");
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
    .expect_err("pvc wait failure fails wake");

    assert!(matches!(error, WakeInstanceError::Projection { .. }));
    assert_eq!(store.instance().state, InstanceState::Failed);
    assert_eq!(store.instance().generation, Generation::new(8));
    let rendered_objects = vec![
        object_ref("v1", "PersistentVolume", "", "pv-acme-instance"),
        object_ref("v1", "PersistentVolumeClaim", "apps", "pvc-acme-instance"),
        object_ref("v1", "Service", "apps", "svc-acme-instance"),
        object_ref("apps/v1", "StatefulSet", "apps", "app-acme-instance"),
    ];
    assert_eq!(
        store.materialization().map(|record| {
            (
                record.state,
                record.backend,
                record.instance_generation,
                record.rendered_objects,
            )
        }),
        Some((
            MaterializationState::Pending,
            None,
            Generation::new(7),
            rendered_objects.clone()
        ))
    );
    assert_eq!(
        client.first_apply_store_events(),
        Some(vec![
            StoreEvent::Cas {
                expected: Generation::new(6),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(7),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(7),
                rendered_objects: rendered_objects.clone(),
            },
        ])
    );
    assert_eq!(
        client.pvc_bound_calls(),
        vec![("apps".to_owned(), "pvc-acme-instance".to_owned())]
    );
    assert!(client.readiness_calls().is_empty());
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
async fn record_materialization_failure_prevents_kubernetes_apply() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 8),
        Some(workload_class()),
    );
    store.delete_before_record_materialization();
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(8),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect_err("recording pending materialization should fail before apply");

    assert!(matches!(error, WakeInstanceError::NotFound));
    let rendered_objects = vec![
        object_ref("v1", "Service", "apps", "svc-acme-instance"),
        object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
    ];
    assert!(client.applied_objects().is_empty());
    assert!(client.deleted_objects().is_empty());
    assert!(client.readiness_calls().is_empty());
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(8),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(9),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(9),
                rendered_objects,
            },
        ]
    );
}

#[tokio::test]
async fn rendered_object_collision_failure_prevents_kubernetes_apply() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 8),
        Some(stateful_workload_class()),
    );
    store.reject_record_materialization_collision();
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(8),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect_err("rendered object collision should fail before apply");

    match error {
        WakeInstanceError::Store(StoreError::InvalidArgument { message }) => {
            assert!(message.contains("rendered Kubernetes object ref collision"));
        }
        other => panic!("expected store collision error, got {other:?}"),
    }
    assert!(client.applied_objects().is_empty());
    assert!(client.deleted_objects().is_empty());
    assert!(client.pvc_bound_calls().is_empty());
    assert!(client.readiness_calls().is_empty());
    assert_eq!(store.instance().state, InstanceState::Failed);
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(8),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(9),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(9),
                rendered_objects: vec![
                    object_ref("v1", "PersistentVolume", "", "pv-acme-instance"),
                    object_ref(
                        "v1",
                        "PersistentVolumeClaim",
                        "apps",
                        "pvc-acme-instance"
                    ),
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "StatefulSet", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::Cas {
                expected: Generation::new(9),
                next_state: InstanceState::Failed,
                reason: StateTransitionReason::FailureReported(
                    "store operation failed: invalid store argument: rendered Kubernetes object ref collision in cluster cluster-a: v1 Service apps/shared-service is already owned by active materialization for instance other-instance".to_owned()
                ),
            },
        ]
    );
}

#[tokio::test]
async fn delete_after_pending_before_apply_cleans_objects_applied_by_wake() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 8),
        Some(workload_class()),
    );
    store.delete_after_record_materialization();
    let client = FakeKubernetesClient::default();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(8),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect_err("complete fails after delete wins the post-pending race");

    assert!(matches!(error, WakeInstanceError::NotFound));
    let rendered_objects = vec![
        object_ref("v1", "Service", "apps", "svc-acme-instance"),
        object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
    ];
    assert_eq!(client.applied_objects().len(), 2);
    assert_eq!(
        client.deleted_objects(),
        vec![rendered_objects[0].clone(), rendered_objects[1].clone()]
    );
    assert_eq!(
        store.materialization().map(|record| {
            (
                record.state,
                record.backend,
                record.instance_generation,
                record.rendered_objects,
            )
        }),
        Some((
            MaterializationState::Pending,
            None,
            Generation::new(9),
            rendered_objects.clone()
        ))
    );
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(8),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(9),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(9),
                rendered_objects: rendered_objects.clone(),
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(9),
                backend_generation: BackendGeneration::new(9),
                rendered_objects,
            },
        ]
    );
}

#[tokio::test]
async fn delete_after_pending_before_readiness_failure_cleans_objects_applied_by_wake() {
    let store = FakeStore::new(
        instance("instance-a", InstanceState::Cold, 8),
        Some(workload_class()),
    );
    store.delete_after_record_materialization();
    let client = FakeKubernetesClient::default();
    client.fail_readiness();
    let materializer = KubernetesMaterializer::new(client.clone());

    let error = wake_instance(
        &store,
        &materializer,
        WakeInstanceRequest::new(
            instance_id("instance-a"),
            Generation::new(8),
            target("cluster-a", "apps"),
        ),
    )
    .await
    .expect_err("readiness fails after delete wins the post-pending race");

    assert!(matches!(error, WakeInstanceError::Materializer { .. }));
    let rendered_objects = vec![
        object_ref("v1", "Service", "apps", "svc-acme-instance"),
        object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
    ];
    assert_eq!(client.applied_objects().len(), 2);
    assert_eq!(
        client.deleted_objects(),
        vec![rendered_objects[0].clone(), rendered_objects[1].clone()]
    );
    assert_eq!(
        store.events(),
        vec![
            StoreEvent::Cas {
                expected: Generation::new(8),
                next_state: InstanceState::Waking,
                reason: StateTransitionReason::WakeRequested,
            },
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(9),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(9),
                rendered_objects,
            },
        ]
    );
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
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(10),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(44),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(10),
                backend_generation: BackendGeneration::new(44),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
        ]
    );
    assert_eq!(store.instance().state, InstanceState::Waking);
    assert_eq!(store.instance().generation, Generation::new(10));
    assert_eq!(client.applied_objects().len(), 2);
    assert!(client.deleted_objects().is_empty());
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
    let deleting_materialization = materialization(
        "instance-a",
        12,
        target("cluster-a", "apps"),
        MaterializationState::Deleting,
        None,
    );
    store.set_materialization(deleting_materialization.clone());
    let client = FakeKubernetesClient::default();
    client.set_owned_live_object(&deleting_materialization, rendered_objects[0].clone());
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
            StoreEvent::RecordMaterialization {
                instance_generation: Generation::new(15),
                state: MaterializationState::Pending,
                backend_generation: BackendGeneration::new(15),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
                ],
            },
            StoreEvent::Complete {
                expected_waking_generation: Generation::new(15),
                backend_generation: BackendGeneration::new(15),
                rendered_objects: vec![
                    object_ref("v1", "Service", "apps", "svc-acme-instance"),
                    object_ref("apps/v1", "Deployment", "apps", "app-acme-instance"),
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
                delete_before_record_materialization: false,
                delete_after_record_materialization: false,
                reject_record_materialization_collision: false,
                reject_record_materialization_exclusivity_conflict: false,
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

    fn materialization(&self) -> Option<MaterializationRecord> {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .materialization
            .clone()
    }

    fn set_complete_conflict(&self) {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .complete_conflict = true;
    }

    fn delete_before_record_materialization(&self) {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .delete_before_record_materialization = true;
    }

    fn delete_after_record_materialization(&self) {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .delete_after_record_materialization = true;
    }

    fn reject_record_materialization_collision(&self) {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .reject_record_materialization_collision = true;
    }

    fn reject_record_materialization_exclusivity_conflict(&self) {
        self.inner
            .lock()
            .expect("fake store lock not poisoned")
            .reject_record_materialization_exclusivity_conflict = true;
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
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().expect("fake store lock not poisoned");
            inner.events.push(StoreEvent::RecordMaterialization {
                instance_generation: request.instance_generation,
                state: request.state,
                backend_generation: request.backend_generation,
                rendered_objects: request.rendered_objects.clone(),
            });
            if inner.delete_before_record_materialization {
                inner.instance = None;
                return Err(StoreError::NotFound {
                    resource: "instance",
                });
            }
            let instance = inner.instance.as_ref().ok_or(StoreError::NotFound {
                resource: "instance",
            })?;
            if instance.generation != request.instance_generation {
                return Err(StoreError::GenerationConflict {
                    expected: request.instance_generation,
                    actual: instance.generation,
                });
            }
            if inner.reject_record_materialization_collision {
                return Err(StoreError::invalid_argument(
                    "rendered Kubernetes object ref collision in cluster cluster-a: v1 Service apps/shared-service is already owned by active materialization for instance other-instance",
                ));
            }
            if inner.reject_record_materialization_exclusivity_conflict {
                return Err(StoreError::ExclusivityConflict {
                    cluster_id: request.target.cluster_id().to_owned(),
                    namespace: request.target.namespace().to_owned(),
                    key_name: "disk".to_owned(),
                    owner_instance_id: Some("instance-b".to_owned()),
                    owner_generation: Some(Generation::new(3)),
                });
            }
            if inner.materialization.as_ref().is_some_and(|existing| {
                existing.instance_id == request.instance_id
                    && existing.target == request.target
                    && existing.backend_generation > request.backend_generation
            }) {
                return Err(StoreError::invalid_argument(
                    "materialization backend generation rewind rejected",
                ));
            }
            let record = MaterializationRecord {
                id: MaterializationId::new(format!(
                    "{}:{}:{}",
                    request.instance_id.as_str(),
                    request.target.cluster_id(),
                    request.target.namespace()
                ))
                .expect("valid materialization id"),
                instance_id: request.instance_id,
                instance_generation: request.instance_generation,
                target: request.target,
                state: request.state,
                backend: request.backend,
                backend_generation: request.backend_generation,
                rendered_objects: request.rendered_objects,
                exclusivity_keys: request.exclusivity_keys,
                reconciliation_lease: None,
            };
            inner.materialization = Some(record.clone());
            if inner.delete_after_record_materialization {
                inner.instance = None;
            }
            Ok(record)
        })
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
                exclusivity_keys: request.exclusivity_keys,
                reconciliation_lease: None,
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
    fn observing_store(store: FakeStore) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeKubernetesState::default())),
            observed_store: Some(store),
        }
    }

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

    fn pvc_bound_calls(&self) -> Vec<(String, String)> {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .pvc_bound_calls
            .clone()
    }

    fn readiness_calls(&self) -> Vec<Vec<RenderedObjectRef>> {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .readiness_calls
            .clone()
    }

    fn first_apply_store_events(&self) -> Option<Vec<StoreEvent>> {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .first_apply_store_events
            .clone()
    }

    fn fail_pvc_wait(&self, namespace: &str, name: &str) {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .fail_pvc_wait = Some((namespace.to_owned(), name.to_owned()));
    }

    fn fail_apply(&self, object: RenderedObjectRef) {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .fail_apply = Some(object);
    }

    fn fail_readiness(&self) {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .fail_readiness = true;
    }

    fn set_unowned_live_object(&self, object: RenderedObjectRef) {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .live_objects
            .insert(
                object_key(&object),
                ProjectionObjectInspection::Present(LiveObjectMetadata {
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                    deleting: false,
                    finalizers: Vec::new(),
                }),
            );
    }

    fn set_owned_live_object(
        &self,
        materialization: &MaterializationRecord,
        object: RenderedObjectRef,
    ) {
        self.inner
            .lock()
            .expect("fake kubernetes lock not poisoned")
            .live_objects
            .insert(
                object_key(&object),
                ProjectionObjectInspection::Present(LiveObjectMetadata {
                    labels: BTreeMap::from([
                        (
                            LABEL_MANAGED_BY.to_owned(),
                            LABEL_MANAGED_BY_VALUE.to_owned(),
                        ),
                        (
                            "sleepypods.io/instance-id".to_owned(),
                            materialization.instance_id.as_str().to_owned(),
                        ),
                        (
                            "sleepypods.io/instance-generation".to_owned(),
                            materialization.instance_generation.to_string(),
                        ),
                    ]),
                    annotations: BTreeMap::from([(
                        ANNOTATION_MATERIALIZATION_ID.to_owned(),
                        materialization.id.as_str().to_owned(),
                    )]),
                    deleting: false,
                    finalizers: Vec::new(),
                }),
            );
    }
}

impl KubernetesMaterializerClient for FakeKubernetesClient {
    fn apply_object<'a>(
        &'a self,
        object: &'a KubernetesObject,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let object_ref = crate::materializer::rendered_object_ref(object);
            let first_apply_store_events = self.observed_store.as_ref().and_then(|store| {
                let should_capture = self
                    .inner
                    .lock()
                    .expect("fake kubernetes lock not poisoned")
                    .first_apply_store_events
                    .is_none();
                should_capture.then(|| store.events())
            });
            let mut inner = self
                .inner
                .lock()
                .expect("fake kubernetes lock not poisoned");
            if inner.first_apply_store_events.is_none() {
                inner.first_apply_store_events = first_apply_store_events;
            }
            if inner
                .fail_apply
                .as_ref()
                .is_some_and(|failed| failed == &object_ref)
            {
                return Err(KubernetesClientError::new("apply failed"));
            }
            inner.applied_objects.push(object.clone());
            inner.live_objects.insert(
                object_key(&object_ref),
                ProjectionObjectInspection::Present(LiveObjectMetadata::from_rendered_object(
                    object,
                )),
            );
            Ok(())
        })
    }

    fn delete_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let mut inner = self
                .inner
                .lock()
                .expect("fake kubernetes lock not poisoned");
            inner.live_objects.remove(&object_key(object));
            inner.deleted_objects.push(object.clone());
            Ok(())
        })
    }

    fn inspect_object<'a>(
        &'a self,
        object: &'a RenderedObjectRef,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<ProjectionObjectInspection>> {
        Box::pin(async move {
            Ok(self
                .inner
                .lock()
                .expect("fake kubernetes lock not poisoned")
                .live_objects
                .get(&object_key(object))
                .cloned()
                .unwrap_or(ProjectionObjectInspection::Missing))
        })
    }

    fn wait_for_pvc_bound<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
        Box::pin(async move {
            let mut inner = self
                .inner
                .lock()
                .expect("fake kubernetes lock not poisoned");
            inner
                .pvc_bound_calls
                .push((namespace.to_owned(), name.to_owned()));
            if inner
                .fail_pvc_wait
                .as_ref()
                .is_some_and(|(fail_namespace, fail_name)| {
                    fail_namespace == namespace && fail_name == name
                })
            {
                return Err(KubernetesClientError::new("pvc did not bind"));
            }
            Ok(())
        })
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
        exclusivity_keys: vec![],
    }
}

fn stateful_workload_class() -> WorkloadClassVersion {
    WorkloadClassVersion {
        reference: workload_ref(),
        template_generation: Generation::new(3),
        template: stateful_template(),
        default_values: InstanceValues::new(),
        value_schema: WorkloadValueSchema::new(true),
        sleep_policy: WorkloadSleepPolicy::new(120_000, 5_000, 30_000).expect("valid sleep policy"),
        exclusivity_keys: vec![],
    }
}

fn exclusive_workload_class() -> WorkloadClassVersion {
    let mut workload_class = workload_class();
    add_exclusivity_keys(&mut workload_class);
    workload_class
}

fn exclusive_stateful_workload_class() -> WorkloadClassVersion {
    let mut workload_class = stateful_workload_class();
    add_exclusivity_keys(&mut workload_class);
    workload_class
}

fn add_exclusivity_keys(workload_class: &mut WorkloadClassVersion) {
    workload_class.exclusivity_keys = vec![
        WorkloadExclusivityKeyTemplate::new(
            "license",
            TemplateText::instance_value("license_handle"),
        ),
        WorkloadExclusivityKeyTemplate::new("disk", TemplateText::instance_value("volume_handle")),
    ];
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
            mode: None,
        },
        volumes: Vec::new(),
    }
}

fn stateful_template() -> ManifestTemplate {
    ManifestTemplate {
        workload: WorkloadTemplate {
            kind: WorkloadKind::StatefulSet,
            name: TemplateText::literal("app-acme"),
            replicas: Some(1),
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
            mode: None,
        },
        volumes: vec![VolumeTemplate {
            name: "data".to_owned(),
            mount_path: TemplateText::literal("/data"),
            pv_name: TemplateText::literal("pv-acme"),
            pvc_name: TemplateText::literal("pvc-acme"),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            capacity: TemplateText::literal("1Gi"),
            reclaim_policy: PersistentVolumeReclaimPolicy::Retain,
            storage_class_name: Some(TemplateText::literal("manual")),
            source: PersistentVolumeSourceTemplate::HostPath {
                path: TemplateText::literal("/tmp/sleepypods-test"),
                type_: None,
            },
        }],
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

fn object_key(object: &RenderedObjectRef) -> String {
    format!(
        "{}|{}|{}|{}",
        object.api_version, object.kind, object.namespace, object.name
    )
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
        exclusivity_keys: vec![],
        reconciliation_lease: None,
    }
}

fn instance_id(id: &str) -> InstanceId {
    InstanceId::new(id).expect("instance ID")
}
