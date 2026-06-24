use std::{
    cmp, fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use proxy_core::observability::{
    metrics::RUNTIME_MATERIALIZATION_FAILURES_TOTAL,
    recorder::{
        LifecycleLogEvent, LogField, MetricObservation, ObservabilityRecorder,
        EVENT_MATERIALIZATION_FAILURE,
    },
    Operation, Outcome,
};
use tokio::{sync::watch, task::JoinSet, time::sleep};

use crate::{
    ids::InstanceId,
    instance::{GetInstanceRequest, InstanceRecord, InstanceState},
    manifest::{render_manifests_with_options, RenderManifestOptions, RenderManifestRequest},
    materialization::{
        ClaimMaterializationReconciliationRequest, CompleteWakeReconciliationRequest,
        CompleteWakeRequest, DeleteMaterializationReconciliationRequest,
        FinalizeSleepReconciliationRequest, FinalizeSleepRequest,
        ListMaterializationReconciliationCandidatesRequest, MaterializationRecord,
        MaterializationState, ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    materializer::{KubernetesMaterializer, KubernetesMaterializerClient, MaterializerError},
    store::{ControlPlaneStore, StoreError},
    workload::LoadWorkloadClassVersionRequest,
};

pub const EVENT_MATERIALIZATION_RECONCILIATION: &str = "runtime.materialization.reconciliation";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationReconcilerConfig {
    pub owner: String,
    pub interval: Duration,
    pub lease_ttl: Duration,
    pub batch_size: usize,
    pub concurrency_limit: usize,
}

pub struct MaterializationReconciler<C> {
    store: Arc<dyn ControlPlaneStore>,
    materializer: KubernetesMaterializer<C>,
    config: MaterializationReconcilerConfig,
    observability: ObservabilityRecorder,
}

#[derive(Debug)]
pub enum MaterializationReconcileError {
    Store(StoreError),
    Materializer(MaterializerError),
    Render(String),
    StaleDesiredRefs,
    LeaseLost,
}

impl Default for MaterializationReconcilerConfig {
    fn default() -> Self {
        Self {
            owner: default_owner(),
            interval: Duration::from_secs(15),
            lease_ttl: Duration::from_secs(60),
            batch_size: 32,
            concurrency_limit: 4,
        }
    }
}

impl<C> MaterializationReconciler<C>
where
    C: KubernetesMaterializerClient + Clone + 'static,
{
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        materializer: KubernetesMaterializer<C>,
        config: MaterializationReconcilerConfig,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            store,
            materializer,
            config,
            observability,
        }
    }

    pub async fn run_until_shutdown(self, mut shutdown: watch::Receiver<bool>) {
        self.run_once().await;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
                _ = sleep(self.interval_with_jitter()) => {
                    self.run_once().await;
                }
            }
        }
    }

    pub async fn run_once(&self) {
        let now = SystemTime::now();
        let candidates = match self
            .store
            .list_materialization_reconciliation_candidates(
                ListMaterializationReconciliationCandidatesRequest::new(
                    now,
                    self.config.batch_size,
                ),
            )
            .await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                self.record_outcome("scan", Outcome::Error, Some(&error.to_string()));
                return;
            }
        };

        let concurrency = cmp::max(1, self.config.concurrency_limit);
        let mut join_set = JoinSet::new();
        for candidate in candidates {
            while join_set.len() >= concurrency {
                let _ = join_set.join_next().await;
            }
            let reconciler = self.clone();
            join_set.spawn(async move {
                reconciler.claim_and_reconcile(candidate).await;
            });
        }
        while join_set.join_next().await.is_some() {}
    }

    pub async fn reconcile_materialization(&self, materialization: MaterializationRecord) {
        self.claim_and_reconcile(materialization).await;
    }

    async fn claim_and_reconcile(&self, candidate: MaterializationRecord) {
        let lease_expires_at = SystemTime::now() + self.config.lease_ttl;
        let claimed = match self
            .store
            .claim_materialization_reconciliation(ClaimMaterializationReconciliationRequest::new(
                candidate.id.clone(),
                self.config.owner.clone(),
                SystemTime::now(),
                lease_expires_at,
            ))
            .await
        {
            Ok(Some(claimed)) => claimed,
            Ok(None) => {
                self.record_outcome("claim", Outcome::Rejected, None);
                return;
            }
            Err(error) => {
                self.record_outcome("claim", Outcome::Error, Some(&error.to_string()));
                return;
            }
        };
        self.record_outcome("claim", Outcome::Success, None);

        let result = match claimed.state {
            MaterializationState::Pending => self.reconcile_pending(claimed.clone()).await,
            MaterializationState::Deleting => self.reconcile_deleting(claimed.clone()).await,
            _ => Ok(()),
        };

        match result {
            Ok(()) => self.record_outcome("reconcile", Outcome::Success, None),
            Err(MaterializationReconcileError::LeaseLost) => {
                self.record_outcome("lease_lost", Outcome::Rejected, None)
            }
            Err(error) => {
                self.record_outcome("reconcile", Outcome::Error, Some(&error.to_string()));
                let _ = self
                    .store
                    .release_materialization_reconciliation_lease(
                        ReleaseMaterializationReconciliationLeaseRequest::new(
                            claimed.id,
                            self.config.owner.clone(),
                        ),
                    )
                    .await;
            }
        }
    }

    async fn reconcile_pending(
        &self,
        materialization: MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        let Some(instance) = self
            .load_instance(materialization.instance_id.clone())
            .await?
        else {
            return self.delete_refs_and_mark_deleted(materialization).await;
        };

        if instance.generation != materialization.instance_generation
            || matches!(
                instance.state,
                InstanceState::Deleting | InstanceState::Deleted
            )
        {
            return self.delete_refs_and_mark_deleted(materialization).await;
        }
        if instance.state != InstanceState::Waking {
            return Err(MaterializationReconcileError::Store(
                StoreError::invalid_argument(
                    "pending materialization is not attached to waking instance",
                ),
            ));
        }

        let manifest = self
            .render_current_manifest(&instance, &materialization)
            .await?;
        let desired_refs = self
            .materializer
            .rendered_object_refs(&manifest)
            .map_err(MaterializationReconcileError::Materializer)?;
        if desired_refs != materialization.rendered_objects {
            return Err(MaterializationReconcileError::StaleDesiredRefs);
        }

        self.materializer
            .apply_manifest(&manifest)
            .await
            .map_err(MaterializationReconcileError::Materializer)?;
        let backend = self
            .materializer
            .wait_for_readiness(&materialization.rendered_objects)
            .await
            .map_err(MaterializationReconcileError::Materializer)?;
        self.renew_or_lose(&materialization).await?;

        let mut complete = CompleteWakeRequest::new(
            materialization.instance_id.clone(),
            materialization.instance_generation,
            materialization.target.clone(),
            backend,
            materialization.backend_generation,
        );
        complete.rendered_objects = materialization.rendered_objects.clone();
        complete.exclusivity_keys = materialization.exclusivity_keys.clone();
        self.store
            .complete_wake_reconciliation(CompleteWakeReconciliationRequest::new(
                materialization.id,
                self.config.owner.clone(),
                complete,
            ))
            .await
            .map(|_| ())
            .map_err(MaterializationReconcileError::Store)
    }

    async fn reconcile_deleting(
        &self,
        materialization: MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        self.materializer
            .delete_rendered_objects(&materialization.rendered_objects)
            .await
            .map_err(MaterializationReconcileError::Materializer)?;
        self.renew_or_lose(&materialization).await?;

        let Some(instance) = self
            .load_instance(materialization.instance_id.clone())
            .await?
        else {
            return self.mark_deleted(materialization).await;
        };
        if instance.state != InstanceState::Draining
            || instance.generation != materialization.instance_generation
        {
            return self.mark_deleted(materialization).await;
        }

        self.store
            .finalize_sleep_reconciliation(FinalizeSleepReconciliationRequest::new(
                materialization.id,
                self.config.owner.clone(),
                FinalizeSleepRequest::new(instance.id, instance.generation, materialization.target),
            ))
            .await
            .map(|_| ())
            .map_err(MaterializationReconcileError::Store)
    }

    async fn delete_refs_and_mark_deleted(
        &self,
        materialization: MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        self.materializer
            .delete_rendered_objects(&materialization.rendered_objects)
            .await
            .map_err(MaterializationReconcileError::Materializer)?;
        self.renew_or_lose(&materialization).await?;
        self.mark_deleted(materialization).await
    }

    async fn mark_deleted(
        &self,
        materialization: MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        self.store
            .delete_materialization_reconciliation(DeleteMaterializationReconciliationRequest::new(
                materialization.id,
                self.config.owner.clone(),
                materialization.state,
                materialization.instance_id,
                materialization.instance_generation,
                materialization.target,
            ))
            .await
            .map(|_| ())
            .map_err(MaterializationReconcileError::Store)
    }

    async fn renew_or_lose(
        &self,
        materialization: &MaterializationRecord,
    ) -> Result<(), MaterializationReconcileError> {
        let renewed = self
            .store
            .renew_materialization_reconciliation_lease(
                RenewMaterializationReconciliationLeaseRequest::new(
                    materialization.id.clone(),
                    self.config.owner.clone(),
                    SystemTime::now() + self.config.lease_ttl,
                ),
            )
            .await
            .map_err(MaterializationReconcileError::Store)?;
        if renewed {
            Ok(())
        } else {
            Err(MaterializationReconcileError::LeaseLost)
        }
    }

    async fn load_instance(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<InstanceRecord>, MaterializationReconcileError> {
        self.store
            .get_instance(GetInstanceRequest::new(instance_id))
            .await
            .map_err(MaterializationReconcileError::Store)
    }

    async fn render_current_manifest(
        &self,
        instance: &InstanceRecord,
        materialization: &MaterializationRecord,
    ) -> Result<crate::manifest::RenderedManifest, MaterializationReconcileError> {
        let workload_class = self
            .store
            .load_workload_class_version(LoadWorkloadClassVersionRequest::new(
                instance.workload_class.clone(),
            ))
            .await
            .map_err(MaterializationReconcileError::Store)?
            .ok_or_else(|| {
                MaterializationReconcileError::Store(StoreError::NotFound {
                    resource: "workload class version",
                })
            })?;
        let sleep_policy = workload_class
            .sleep_policy
            .resolve(&instance.values)
            .map_err(|error| MaterializationReconcileError::Render(error.to_string()))?;
        render_manifests_with_options(
            RenderManifestRequest {
                template: &workload_class.template,
                instance,
                sleep_policy,
                namespace: materialization.target.namespace(),
                template_generation: Some(workload_class.template_generation),
            },
            RenderManifestOptions {
                sidecar_control_plane_token: self.materializer.sidecar_control_plane_token(),
            },
        )
        .map_err(|error| MaterializationReconcileError::Render(error.to_string()))
    }

    fn interval_with_jitter(&self) -> Duration {
        let interval = self.config.interval;
        let jitter_bound = interval / 5;
        if jitter_bound.is_zero() {
            return interval;
        }
        let jitter_millis = stable_owner_hash(&self.config.owner) % jitter_bound.as_millis() as u64;
        interval + Duration::from_millis(jitter_millis)
    }

    fn record_outcome(&self, operation: &'static str, outcome: Outcome, error: Option<&str>) {
        self.observability.record_log(LifecycleLogEvent::new(
            EVENT_MATERIALIZATION_RECONCILIATION,
            vec![
                LogField::new("operation", operation),
                LogField::new("outcome", outcome.as_str()),
            ],
        ));
        if let Some(error) = error {
            self.observability.record_metric(MetricObservation::new(
                RUNTIME_MATERIALIZATION_FAILURES_TOTAL,
                vec![
                    Operation::Materialize.metric_label(),
                    Outcome::Error.metric_label(),
                ],
                1.0,
            ));
            self.observability.record_log(LifecycleLogEvent::new(
                EVENT_MATERIALIZATION_FAILURE,
                vec![LogField::error_reason(error)],
            ));
        }
    }
}

impl<C> Clone for MaterializationReconciler<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            materializer: self.materializer.clone(),
            config: self.config.clone(),
            observability: self.observability.clone(),
        }
    }
}

impl MaterializationReconcileError {
    fn to_string(&self) -> String {
        format!("{self}")
    }
}

impl fmt::Display for MaterializationReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(f, "store: {error}"),
            Self::Materializer(error) => write!(f, "materializer: {error}"),
            Self::Render(error) => write!(f, "render: {error}"),
            Self::StaleDesiredRefs => {
                f.write_str("rendered object refs no longer match persisted refs")
            }
            Self::LeaseLost => f.write_str("reconciliation lease was lost"),
        }
    }
}

fn default_owner() -> String {
    format!(
        "pid-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default()
    )
}

fn stable_owner_hash(owner: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in owner.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use crate::{
        ids::{BackendGeneration, Generation, InstanceId, MaterializationId, WorkloadClassId},
        instance::{InstanceState, InstanceValues},
        manifest::{
            render_manifests, ContainerPortTemplate, ContainerTemplate, EnvVarTemplate,
            ManifestTemplate, ServicePortTemplate, ServiceTemplate, SidecarTemplate, TemplateText,
            WorkloadKind, WorkloadTemplate,
        },
        materialization::{
            BackendEndpoint, CompleteWakeResult, FinalizeSleepResult,
            MaterializationReconciliationLease, MaterializationTarget, RenderedObjectRef,
        },
        materializer::{KubernetesClientError, KubernetesClientFuture, KubernetesClientResult},
        store::{StoreFuture, StoreResult},
        workload::{
            RenderedExclusivityKey, WorkloadClassVersion, WorkloadClassVersionRef,
            WorkloadValueSchema,
        },
        WorkloadSleepPolicy,
    };

    use super::*;

    #[tokio::test]
    async fn deleting_reconciliation_deletes_refs_and_finalizes_with_current_lease() {
        let store = Arc::new(FakeReconcileStore::new(
            deleting_materialization("mat-delete-ok"),
            draining_instance("instance-reconcile"),
        ));
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 1);
        assert_eq!(store.release_calls(), 0);
    }

    #[tokio::test]
    async fn deleting_reconciliation_keeps_keys_when_cleanup_fails() {
        let store = Arc::new(FakeReconcileStore::new(
            deleting_materialization("mat-delete-fail"),
            draining_instance("instance-reconcile"),
        ));
        let materializer = KubernetesMaterializer::new(
            FakeKubernetesClient::default()
                .with_delete_error(KubernetesClientError::transient("api unavailable")),
        );
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(store.release_calls(), 1);
    }

    #[tokio::test]
    async fn deleting_reconciliation_does_not_finalize_after_lease_loss() {
        let store = Arc::new(
            FakeReconcileStore::new(
                deleting_materialization("mat-delete-lease-lost"),
                draining_instance("instance-reconcile"),
            )
            .with_renew_result(false),
        );
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(store.release_calls(), 0);
    }

    #[tokio::test]
    async fn pending_reconciliation_applies_waits_and_completes_current_wake() {
        let instance = waking_instance("instance-reconcile");
        let materialization = pending_materialization("mat-pending-complete", &instance);
        let store = Arc::new(FakeReconcileStore::new(materialization, instance));
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.complete_calls(), 1);
        assert_eq!(store.guarded_delete_calls(), 0);
        assert_eq!(store.materialization_state(), MaterializationState::Ready);
        assert_eq!(client.apply_calls(), 2);
        assert_eq!(client.wait_readiness_calls(), 1);
        assert_eq!(store.release_calls(), 0);
    }

    #[tokio::test]
    async fn pending_stale_generation_deletes_refs_and_marks_deleted_after_cleanup() {
        let materialization =
            pending_materialization("mat-pending-stale", &waking_instance("instance-reconcile"));
        let store = Arc::new(FakeReconcileStore::new(
            materialization,
            running_instance("instance-reconcile", 8),
        ));
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.complete_calls(), 0);
        assert_eq!(store.guarded_delete_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Deleted);
        assert_eq!(client.delete_calls(), 2);
        assert_eq!(store.release_calls(), 0);
    }

    #[tokio::test]
    async fn deleting_reconciliation_tolerates_missing_refs_and_finalizes() {
        let store = Arc::new(FakeReconcileStore::new(
            deleting_materialization("mat-delete-missing-refs"),
            draining_instance("instance-reconcile"),
        ));
        let client = FakeKubernetesClient::default();
        let materializer = KubernetesMaterializer::new(client.clone());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(client.delete_calls(), 2);
        assert_eq!(store.finalize_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Deleted);
    }

    #[tokio::test]
    async fn deleting_reconciliation_marks_deleted_after_generation_race_cleanup() {
        let store = Arc::new(FakeReconcileStore::new(
            deleting_materialization("mat-delete-race"),
            running_instance("instance-reconcile", 8),
        ));
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.finalize_calls(), 0);
        assert_eq!(store.guarded_delete_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Deleted);
    }

    #[tokio::test]
    async fn stale_cleanup_does_not_delete_newer_same_id_materialization() {
        let stale =
            pending_materialization("mat-stable-id", &waking_instance("instance-reconcile"));
        let mut newer =
            pending_materialization("mat-stable-id", &waking_instance("instance-reconcile"));
        newer.instance_generation = Generation::new(stale.instance_generation.get() + 1);
        newer.backend_generation = BackendGeneration::new(newer.instance_generation.get());
        newer.exclusivity_keys = vec![RenderedExclusivityKey::new("singleton", "class-a")];
        let store = Arc::new(
            FakeReconcileStore::new(stale, running_instance("instance-reconcile", 8))
                .with_replace_before_guarded_delete(newer.clone()),
        );
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let reconciler = reconciler(store.clone(), materializer);

        reconciler.run_once().await;

        assert_eq!(store.guarded_delete_calls(), 1);
        assert_eq!(store.materialization_state(), MaterializationState::Pending);
        assert_eq!(
            store.materialization_generation(),
            newer.instance_generation
        );
        assert_eq!(store.exclusivity_keys(), newer.exclusivity_keys);
    }

    fn reconciler(
        store: Arc<FakeReconcileStore>,
        materializer: KubernetesMaterializer<FakeKubernetesClient>,
    ) -> MaterializationReconciler<FakeKubernetesClient> {
        MaterializationReconciler::new(
            store,
            materializer,
            MaterializationReconcilerConfig {
                owner: "test-owner".to_owned(),
                interval: Duration::from_secs(60),
                lease_ttl: Duration::from_secs(30),
                batch_size: 10,
                concurrency_limit: 1,
            },
            ObservabilityRecorder::noop(),
        )
    }

    fn deleting_materialization(id: &str) -> MaterializationRecord {
        MaterializationRecord {
            id: MaterializationId::new(id).expect("valid materialization id"),
            instance_id: InstanceId::new("instance-reconcile").expect("valid instance id"),
            instance_generation: Generation::new(7),
            target: target(),
            state: MaterializationState::Deleting,
            backend: None,
            backend_generation: BackendGeneration::new(7),
            rendered_objects: vec![
                RenderedObjectRef {
                    api_version: "apps/v1".to_owned(),
                    kind: "Deployment".to_owned(),
                    namespace: "apps".to_owned(),
                    name: "instance-reconcile".to_owned(),
                },
                RenderedObjectRef {
                    api_version: "v1".to_owned(),
                    kind: "PersistentVolumeClaim".to_owned(),
                    namespace: "apps".to_owned(),
                    name: "instance-reconcile-data".to_owned(),
                },
            ],
            exclusivity_keys: vec![],
            reconciliation_lease: None,
        }
    }

    fn pending_materialization(id: &str, instance: &InstanceRecord) -> MaterializationRecord {
        let workload_class = workload_class();
        let manifest = render_manifests(RenderManifestRequest {
            template: &workload_class.template,
            instance,
            sleep_policy: workload_class
                .sleep_policy
                .resolve(&instance.values)
                .expect("sleep policy resolves"),
            namespace: "apps",
            template_generation: Some(workload_class.template_generation),
        })
        .expect("manifest renders");
        let materializer = KubernetesMaterializer::new(FakeKubernetesClient::default());
        let rendered_objects = materializer
            .rendered_object_refs(&manifest)
            .expect("rendered refs derive");
        MaterializationRecord {
            id: MaterializationId::new(id).expect("valid materialization id"),
            instance_id: instance.id.clone(),
            instance_generation: instance.generation,
            target: target(),
            state: MaterializationState::Pending,
            backend: None,
            backend_generation: BackendGeneration::new(instance.generation.get()),
            rendered_objects,
            exclusivity_keys: vec![],
            reconciliation_lease: None,
        }
    }

    fn draining_instance(instance_id: &str) -> InstanceRecord {
        InstanceRecord {
            id: InstanceId::new(instance_id).expect("valid instance id"),
            workload_class: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            values: Default::default(),
            state: InstanceState::Draining,
            generation: Generation::new(7),
        }
    }

    fn waking_instance(instance_id: &str) -> InstanceRecord {
        instance(instance_id, InstanceState::Waking, 7)
    }

    fn running_instance(instance_id: &str, generation: u64) -> InstanceRecord {
        instance(instance_id, InstanceState::Running, generation)
    }

    fn instance(instance_id: &str, state: InstanceState, generation: u64) -> InstanceRecord {
        InstanceRecord {
            id: InstanceId::new(instance_id).expect("valid instance id"),
            workload_class: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            values: InstanceValues::new(),
            state,
            generation: Generation::new(generation),
        }
    }

    fn workload_class() -> WorkloadClassVersion {
        WorkloadClassVersion {
            reference: WorkloadClassVersionRef::new(
                WorkloadClassId::new("class-a").expect("valid class id"),
                Generation::new(1),
            ),
            template_generation: Generation::new(3),
            template: ManifestTemplate {
                workload: WorkloadTemplate {
                    kind: WorkloadKind::Deployment,
                    name: TemplateText::literal("app"),
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
                    name: TemplateText::literal("svc"),
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
            },
            default_values: InstanceValues::new(),
            value_schema: WorkloadValueSchema::new(true),
            sleep_policy: WorkloadSleepPolicy::new(120_000, 5_000, 30_000)
                .expect("valid sleep policy"),
            exclusivity_keys: vec![],
        }
    }

    fn target() -> MaterializationTarget {
        MaterializationTarget::new("cluster-a", "apps").expect("valid target")
    }

    #[derive(Debug)]
    struct FakeReconcileStore {
        materialization: Mutex<MaterializationRecord>,
        instance: InstanceRecord,
        workload_class: WorkloadClassVersion,
        renew_result: Mutex<bool>,
        replace_before_guarded_delete: Mutex<Option<MaterializationRecord>>,
        finalize_calls: Mutex<usize>,
        complete_calls: Mutex<usize>,
        guarded_delete_calls: Mutex<usize>,
        release_calls: Mutex<usize>,
    }

    impl FakeReconcileStore {
        fn new(materialization: MaterializationRecord, instance: InstanceRecord) -> Self {
            Self {
                materialization: Mutex::new(materialization),
                instance,
                workload_class: workload_class(),
                renew_result: Mutex::new(true),
                replace_before_guarded_delete: Mutex::new(None),
                finalize_calls: Mutex::new(0),
                complete_calls: Mutex::new(0),
                guarded_delete_calls: Mutex::new(0),
                release_calls: Mutex::new(0),
            }
        }

        fn with_renew_result(self, renew_result: bool) -> Self {
            *self.renew_result.lock().expect("renew lock") = renew_result;
            self
        }

        fn with_replace_before_guarded_delete(
            self,
            materialization: MaterializationRecord,
        ) -> Self {
            *self
                .replace_before_guarded_delete
                .lock()
                .expect("replace before guarded delete lock") = Some(materialization);
            self
        }

        fn finalize_calls(&self) -> usize {
            *self.finalize_calls.lock().expect("finalize lock")
        }

        fn complete_calls(&self) -> usize {
            *self.complete_calls.lock().expect("complete lock")
        }

        fn guarded_delete_calls(&self) -> usize {
            *self
                .guarded_delete_calls
                .lock()
                .expect("guarded delete lock")
        }

        fn release_calls(&self) -> usize {
            *self.release_calls.lock().expect("release lock")
        }

        fn materialization_state(&self) -> MaterializationState {
            self.materialization
                .lock()
                .expect("materialization lock")
                .state
        }

        fn materialization_generation(&self) -> Generation {
            self.materialization
                .lock()
                .expect("materialization lock")
                .instance_generation
        }

        fn exclusivity_keys(&self) -> Vec<RenderedExclusivityKey> {
            self.materialization
                .lock()
                .expect("materialization lock")
                .exclusivity_keys
                .clone()
        }
    }

    impl ControlPlaneStore for FakeReconcileStore {
        fn get_instance<'a>(
            &'a self,
            _request: GetInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
            Box::pin(async move { Ok(Some(self.instance.clone())) })
        }

        fn load_workload_class_version<'a>(
            &'a self,
            _request: LoadWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
            Box::pin(async move { Ok(Some(self.workload_class.clone())) })
        }

        fn list_materialization_reconciliation_candidates<'a>(
            &'a self,
            _request: ListMaterializationReconciliationCandidatesRequest,
        ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>> {
            Box::pin(async move {
                Ok(vec![self
                    .materialization
                    .lock()
                    .expect("materialization lock")
                    .clone()])
            })
        }

        fn claim_materialization_reconciliation<'a>(
            &'a self,
            request: ClaimMaterializationReconciliationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            Box::pin(async move {
                let mut materialization =
                    self.materialization.lock().expect("materialization lock");
                if materialization.id != request.materialization_id {
                    return Ok(None);
                }
                materialization.reconciliation_lease = Some(MaterializationReconciliationLease {
                    owner: request.owner,
                    expires_at: request.lease_expires_at,
                    attempt: 1,
                });
                Ok(Some(materialization.clone()))
            })
        }

        fn renew_materialization_reconciliation_lease<'a>(
            &'a self,
            _request: RenewMaterializationReconciliationLeaseRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            Box::pin(async move { Ok(*self.renew_result.lock().expect("renew lock")) })
        }

        fn release_materialization_reconciliation_lease<'a>(
            &'a self,
            _request: ReleaseMaterializationReconciliationLeaseRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            Box::pin(async move {
                *self.release_calls.lock().expect("release lock") += 1;
                Ok(true)
            })
        }

        fn complete_wake_reconciliation<'a>(
            &'a self,
            _request: CompleteWakeReconciliationRequest,
        ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
            Box::pin(async move {
                *self.complete_calls.lock().expect("complete lock") += 1;
                let mut materialization =
                    self.materialization.lock().expect("materialization lock");
                materialization.state = MaterializationState::Ready;
                materialization.backend = Some(
                    BackendEndpoint::new("http://svc.apps.svc.cluster.local:80")
                        .expect("backend endpoint"),
                );
                materialization.reconciliation_lease = None;
                let mut instance = self.instance.clone();
                instance.state = InstanceState::Running;
                Ok(CompleteWakeResult {
                    instance,
                    materialization: materialization.clone(),
                })
            })
        }

        fn finalize_sleep_reconciliation<'a>(
            &'a self,
            _request: FinalizeSleepReconciliationRequest,
        ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
            Box::pin(async move {
                *self.finalize_calls.lock().expect("finalize lock") += 1;
                let mut materialization =
                    self.materialization.lock().expect("materialization lock");
                materialization.state = MaterializationState::Deleted;
                materialization.rendered_objects.clear();
                materialization.exclusivity_keys.clear();
                materialization.reconciliation_lease = None;
                let mut instance = self.instance.clone();
                instance.state = InstanceState::Cold;
                instance.generation = instance.generation.next();
                Ok(FinalizeSleepResult {
                    instance,
                    materialization: Some(materialization.clone()),
                })
            })
        }

        fn delete_materialization_reconciliation<'a>(
            &'a self,
            request: DeleteMaterializationReconciliationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            Box::pin(async move {
                *self
                    .guarded_delete_calls
                    .lock()
                    .expect("guarded delete lock") += 1;
                if let Some(replacement) = self
                    .replace_before_guarded_delete
                    .lock()
                    .expect("replace before guarded delete lock")
                    .take()
                {
                    *self.materialization.lock().expect("materialization lock") = replacement;
                }
                let mut materialization =
                    self.materialization.lock().expect("materialization lock");
                if materialization.id != request.materialization_id
                    || materialization.state != request.expected_state
                    || materialization.instance_id != request.instance_id
                    || materialization.instance_generation != request.instance_generation
                    || materialization.target != request.target
                    || materialization
                        .reconciliation_lease
                        .as_ref()
                        .map(|lease| lease.owner.as_str())
                        != Some(request.lease_owner.as_str())
                {
                    return Ok(None);
                }
                let previous = materialization.clone();
                materialization.state = MaterializationState::Deleted;
                materialization.rendered_objects.clear();
                materialization.exclusivity_keys.clear();
                materialization.reconciliation_lease = None;
                Ok(Some(previous))
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct FakeKubernetesClient {
        applied: Arc<Mutex<Vec<crate::manifest::KubernetesObject>>>,
        deleted: Arc<Mutex<Vec<RenderedObjectRef>>>,
        wait_readiness_calls: Arc<Mutex<usize>>,
        delete_errors: Arc<Mutex<VecDeque<KubernetesClientError>>>,
    }

    impl FakeKubernetesClient {
        fn with_delete_error(self, error: KubernetesClientError) -> Self {
            self.delete_errors
                .lock()
                .expect("delete errors lock")
                .push_back(error);
            self
        }

        fn apply_calls(&self) -> usize {
            self.applied.lock().expect("applied lock").len()
        }

        fn delete_calls(&self) -> usize {
            self.deleted.lock().expect("deleted lock").len()
        }

        fn wait_readiness_calls(&self) -> usize {
            *self
                .wait_readiness_calls
                .lock()
                .expect("wait readiness lock")
        }
    }

    impl KubernetesMaterializerClient for FakeKubernetesClient {
        fn apply_object<'a>(
            &'a self,
            object: &'a crate::manifest::KubernetesObject,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                self.applied
                    .lock()
                    .expect("applied lock")
                    .push(object.clone());
                Ok(())
            })
        }

        fn delete_object<'a>(
            &'a self,
            object: &'a RenderedObjectRef,
        ) -> KubernetesClientFuture<'a, KubernetesClientResult<()>> {
            Box::pin(async move {
                match self
                    .delete_errors
                    .lock()
                    .expect("delete errors lock")
                    .pop_front()
                {
                    Some(error) => Err(error),
                    None => {
                        self.deleted
                            .lock()
                            .expect("deleted lock")
                            .push(object.clone());
                        Ok(())
                    }
                }
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
            Box::pin(async move {
                *self
                    .wait_readiness_calls
                    .lock()
                    .expect("wait readiness lock") += 1;
                BackendEndpoint::new("http://svc.apps.svc.cluster.local:80")
                    .map_err(|error| KubernetesClientError::new(error.to_string()))
            })
        }
    }
}
