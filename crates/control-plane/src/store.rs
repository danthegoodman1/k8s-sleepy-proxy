use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use crate::{
    http01::{
        DeleteHttp01ChallengeRequest, ExpireHttp01ChallengesRequest, Http01ChallengeKey,
        Http01ChallengeRecord, PutHttp01ChallengeRequest,
    },
    instance::{
        CompareAndSwapInstanceStateRequest, CreateInstanceRequest, CreateInstanceResult,
        DeleteInstanceRequest, GetInstanceRequest, InstanceRecord,
    },
    materialization::{
        BeginSleepRequest, BeginSleepResult, ClaimMaterializationReconciliationRequest,
        CompleteWakeReconciliationRequest, CompleteWakeRequest, CompleteWakeResult,
        DeleteMaterializationReconciliationRequest, FinalizeSleepReconciliationRequest,
        FinalizeSleepRequest, FinalizeSleepResult, ForceDeleteMaterializationRequest,
        ForceReleaseExclusivityKeyRequest, ForceReleaseExclusivityKeyResult,
        ListMaterializationReconciliationCandidatesRequest, LoadActiveMaterializationRequest,
        LoadMaterializationOperationalMetricsRequest, LoadMaterializationRequest,
        LoadReadyMaterializationRequest, MaterializationOperationalMetrics, MaterializationRecord,
        RecordMaterializationRequest, ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
        ListRouteBindingsForInstanceRequest, RouteBindingRecord, RouteDependencyLookup,
        RouteDependencySet, RouteIdentity, RouteResolution,
    },
    workload::{
        CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, WorkloadClassVersion,
    },
    RetryPolicy,
};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type StoreResult<T> = Result<T, StoreError>;

pub trait ControlPlaneStore: Send + Sync {
    fn create_instance<'a>(
        &'a self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        let _ = request;
        unsupported_store_method("create_instance")
    }

    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        let _ = request;
        unsupported_store_method("get_instance")
    }

    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        let _ = request;
        unsupported_store_method("delete_instance")
    }

    fn create_workload_class_version<'a>(
        &'a self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
        let _ = request;
        unsupported_store_method("create_workload_class_version")
    }

    fn load_workload_class_version<'a>(
        &'a self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        let _ = request;
        unsupported_store_method("load_workload_class_version")
    }

    fn create_route_binding<'a>(
        &'a self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
        let _ = request;
        unsupported_store_method("create_route_binding")
    }

    fn get_route_binding<'a>(
        &'a self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
        let _ = request;
        unsupported_store_method("get_route_binding")
    }

    fn delete_route_binding<'a>(
        &'a self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        let _ = request;
        unsupported_store_method("delete_route_binding")
    }

    fn list_route_bindings_for_instance<'a>(
        &'a self,
        request: ListRouteBindingsForInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<RouteBindingRecord>>> {
        let _ = request;
        unsupported_store_method("list_route_bindings_for_instance")
    }

    fn resolve_route<'a>(
        &'a self,
        identity: RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
        let _ = identity;
        unsupported_store_method("resolve_route")
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        let _ = request;
        unsupported_store_method("compare_and_swap_instance_state")
    }

    fn record_materialization<'a>(
        &'a self,
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
        let _ = request;
        unsupported_store_method("record_materialization")
    }

    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        let _ = request;
        unsupported_store_method("load_ready_materialization")
    }

    fn load_active_materialization<'a>(
        &'a self,
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        let _ = request;
        unsupported_store_method("load_active_materialization")
    }

    fn load_materialization<'a>(
        &'a self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        let _ = request;
        unsupported_store_method("load_materialization")
    }

    fn complete_wake<'a>(
        &'a self,
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        let _ = request;
        unsupported_store_method("complete_wake")
    }

    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        let _ = request;
        unsupported_store_method("begin_sleep")
    }

    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        let _ = request;
        unsupported_store_method("finalize_sleep")
    }

    fn list_materialization_reconciliation_candidates<'a>(
        &'a self,
        request: ListMaterializationReconciliationCandidatesRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>> {
        let _ = request;
        unsupported_store_method("list_materialization_reconciliation_candidates")
    }

    fn load_materialization_operational_metrics<'a>(
        &'a self,
        request: LoadMaterializationOperationalMetricsRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationOperationalMetrics>> {
        let _ = request;
        unsupported_store_method("load_materialization_operational_metrics")
    }

    fn claim_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        let _ = request;
        unsupported_store_method("claim_materialization_reconciliation")
    }

    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        let _ = request;
        unsupported_store_method("renew_materialization_reconciliation_lease")
    }

    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        let _ = request;
        unsupported_store_method("release_materialization_reconciliation_lease")
    }

    fn complete_wake_reconciliation<'a>(
        &'a self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        let _ = request;
        unsupported_store_method("complete_wake_reconciliation")
    }

    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        let _ = request;
        unsupported_store_method("finalize_sleep_reconciliation")
    }

    fn delete_materialization_reconciliation<'a>(
        &'a self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        let _ = request;
        unsupported_store_method("delete_materialization_reconciliation")
    }

    fn force_delete_materialization<'a>(
        &'a self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        let _ = request;
        unsupported_store_method("force_delete_materialization")
    }

    fn force_release_exclusivity_key<'a>(
        &'a self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'a, StoreResult<ForceReleaseExclusivityKeyResult>> {
        let _ = request;
        unsupported_store_method("force_release_exclusivity_key")
    }

    fn lookup_route_dependencies<'a>(
        &'a self,
        request: RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
        let _ = request;
        unsupported_store_method("lookup_route_dependencies")
    }

    fn put_http01_challenge<'a>(
        &'a self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
        let _ = request;
        unsupported_store_method("put_http01_challenge")
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
        let _ = key;
        unsupported_store_method("resolve_http01_challenge")
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        let _ = request;
        unsupported_store_method("delete_http01_challenge")
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        request: ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        let _ = request;
        unsupported_store_method("expire_http01_challenges")
    }
}

#[derive(Clone)]
pub struct RetryingControlPlaneStore {
    inner: Arc<dyn ControlPlaneStore>,
    policy: RetryPolicy,
}

#[derive(Debug)]
pub enum StoreError {
    InvalidArgument {
        message: String,
    },
    NotFound {
        resource: &'static str,
    },
    AlreadyExists {
        resource: &'static str,
    },
    GenerationConflict {
        expected: crate::ids::Generation,
        actual: crate::ids::Generation,
    },
    ExclusivityConflict {
        cluster_id: String,
        namespace: String,
        key_name: String,
        owner_instance_id: Option<String>,
        owner_generation: Option<crate::ids::Generation>,
    },
    IdempotencyConflict,
    Unavailable {
        message: String,
    },
    Internal {
        message: String,
    },
}

impl StoreError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

impl RetryingControlPlaneStore {
    pub fn new(inner: Arc<dyn ControlPlaneStore>, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }

    pub fn with_default_policy(inner: Arc<dyn ControlPlaneStore>) -> Self {
        Self::new(inner, RetryPolicy::default())
    }

    pub fn inner(&self) -> &Arc<dyn ControlPlaneStore> {
        &self.inner
    }

    pub fn policy(&self) -> RetryPolicy {
        self.policy
    }
}

impl fmt::Debug for RetryingControlPlaneStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryingControlPlaneStore")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl ControlPlaneStore for RetryingControlPlaneStore {
    fn create_instance<'a>(
        &'a self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.create_instance(request.clone())
        })
    }

    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.get_instance(request.clone())
        })
    }

    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.delete_instance(request.clone())
        })
    }

    fn create_workload_class_version<'a>(
        &'a self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.create_workload_class_version(request.clone())
        })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_workload_class_version(request.clone())
        })
    }

    fn create_route_binding<'a>(
        &'a self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.create_route_binding(request.clone())
        })
    }

    fn get_route_binding<'a>(
        &'a self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.get_route_binding(request.clone())
        })
    }

    fn delete_route_binding<'a>(
        &'a self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.delete_route_binding(request.clone())
        })
    }

    fn list_route_bindings_for_instance<'a>(
        &'a self,
        request: ListRouteBindingsForInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<RouteBindingRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.list_route_bindings_for_instance(request.clone())
        })
    }

    fn resolve_route<'a>(
        &'a self,
        identity: RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.resolve_route(identity.clone())
        })
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.compare_and_swap_instance_state(request.clone())
        })
    }

    fn record_materialization<'a>(
        &'a self,
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.record_materialization(request.clone())
        })
    }

    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_ready_materialization(request.clone())
        })
    }

    fn load_active_materialization<'a>(
        &'a self,
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_active_materialization(request.clone())
        })
    }

    fn load_materialization<'a>(
        &'a self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_materialization(request.clone())
        })
    }

    fn complete_wake<'a>(
        &'a self,
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.complete_wake(request.clone())
        })
    }

    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.begin_sleep(request.clone())
        })
    }

    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.finalize_sleep(request.clone())
        })
    }

    fn list_materialization_reconciliation_candidates<'a>(
        &'a self,
        request: ListMaterializationReconciliationCandidatesRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.list_materialization_reconciliation_candidates(request.clone())
        })
    }

    fn load_materialization_operational_metrics<'a>(
        &'a self,
        request: LoadMaterializationOperationalMetricsRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationOperationalMetrics>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.load_materialization_operational_metrics(request.clone())
        })
    }

    fn claim_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.claim_materialization_reconciliation(request.clone())
        })
    }

    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.renew_materialization_reconciliation_lease(request.clone())
        })
    }

    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.release_materialization_reconciliation_lease(request.clone())
        })
    }

    fn complete_wake_reconciliation<'a>(
        &'a self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.complete_wake_reconciliation(request.clone())
        })
    }

    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.finalize_sleep_reconciliation(request.clone())
        })
    }

    fn delete_materialization_reconciliation<'a>(
        &'a self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.delete_materialization_reconciliation(request.clone())
        })
    }

    fn force_delete_materialization<'a>(
        &'a self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.force_delete_materialization(request.clone())
        })
    }

    fn force_release_exclusivity_key<'a>(
        &'a self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'a, StoreResult<ForceReleaseExclusivityKeyResult>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.force_release_exclusivity_key(request.clone())
        })
    }

    fn lookup_route_dependencies<'a>(
        &'a self,
        request: RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.lookup_route_dependencies(request.clone())
        })
    }

    fn put_http01_challenge<'a>(
        &'a self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.put_http01_challenge(request.clone())
        })
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.resolve_http01_challenge(key.clone())
        })
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.delete_http01_challenge(request.clone())
        })
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        request: ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        retry_store_operation(&self.inner, self.policy, move |store| {
            store.expire_http01_challenges(request.clone())
        })
    }
}

fn retry_store_operation<'a, T, O, Fut>(
    inner: &'a Arc<dyn ControlPlaneStore>,
    policy: RetryPolicy,
    mut operation: O,
) -> StoreFuture<'a, StoreResult<T>>
where
    T: Send + 'a,
    O: FnMut(&'a dyn ControlPlaneStore) -> Fut + Send + 'a,
    Fut: Future<Output = StoreResult<T>> + Send + 'a,
{
    Box::pin(async move {
        policy
            .retry_if(|| operation(inner.as_ref()), StoreError::is_retryable)
            .await
    })
}

fn unsupported_store_method<'a, T>(method: &'static str) -> StoreFuture<'a, StoreResult<T>>
where
    T: Send + 'a,
{
    Box::pin(async move {
        Err(StoreError::internal(format!(
            "control-plane store method {method} is not implemented"
        )))
    })
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { message } => write!(f, "invalid store argument: {message}"),
            Self::NotFound { resource } => write!(f, "{resource} not found"),
            Self::AlreadyExists { resource } => write!(f, "{resource} already exists"),
            Self::GenerationConflict { expected, actual } => {
                write!(
                    f,
                    "generation conflict: expected generation {expected}, found {actual}"
                )
            }
            Self::ExclusivityConflict {
                cluster_id,
                namespace,
                key_name,
                owner_instance_id,
                owner_generation,
            } => {
                write!(
                    f,
                    "exclusivity key {key_name:?} is already held for target {cluster_id}/{namespace}"
                )?;
                if let Some(owner_instance_id) = owner_instance_id {
                    write!(f, " by instance {owner_instance_id}")?;
                }
                if let Some(owner_generation) = owner_generation {
                    write!(f, " generation {owner_generation}")?;
                }
                Ok(())
            }
            Self::IdempotencyConflict => {
                f.write_str("idempotency key was already used for a different request")
            }
            Self::Unavailable { message } => write!(f, "store unavailable: {message}"),
            Self::Internal { message } => write!(f, "internal store error: {message}"),
        }
    }
}

impl Error for StoreError {}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };

    use crate::InstanceId;

    use super::*;

    fn assert_dyn_safe<T: ControlPlaneStore + ?Sized>() {}

    #[test]
    fn control_plane_store_trait_is_dyn_safe() {
        assert_dyn_safe::<dyn ControlPlaneStore>();
    }

    #[tokio::test]
    async fn retrying_control_plane_store_retries_unavailable_until_success() {
        let inner = Arc::new(FakeRetryStore::with_get_instance_errors(vec![
            StoreError::unavailable("database is starting"),
            StoreError::unavailable("database is still starting"),
        ]));
        let store = RetryingControlPlaneStore::new(inner.clone(), immediate_retry_policy());

        let result = store
            .get_instance(GetInstanceRequest {
                instance_id: InstanceId::new("retry-instance").expect("instance id"),
            })
            .await
            .expect("transient unavailable errors are retried");

        assert_eq!(result, None);
        assert_eq!(inner.get_instance_calls(), 3);
    }

    #[tokio::test]
    async fn retrying_control_plane_store_does_not_retry_permanent_errors() {
        let inner = Arc::new(FakeRetryStore::with_get_instance_errors(vec![
            StoreError::invalid_argument("bad instance id"),
        ]));
        let store = RetryingControlPlaneStore::new(inner.clone(), immediate_retry_policy());

        let error = store
            .get_instance(GetInstanceRequest {
                instance_id: InstanceId::new("retry-instance").expect("instance id"),
            })
            .await
            .expect_err("permanent errors are not retried");

        assert!(matches!(error, StoreError::InvalidArgument { .. }));
        assert_eq!(inner.get_instance_calls(), 1);
    }

    #[tokio::test]
    async fn retrying_control_plane_store_stops_after_max_attempts() {
        let inner = Arc::new(FakeRetryStore::with_get_instance_errors(vec![
            StoreError::unavailable("database is down"),
            StoreError::unavailable("database is still down"),
            StoreError::unavailable("database remains down"),
        ]));
        let store = RetryingControlPlaneStore::new(
            inner.clone(),
            RetryPolicy::new(2, Duration::ZERO, Duration::ZERO),
        );

        let error = store
            .get_instance(GetInstanceRequest {
                instance_id: InstanceId::new("retry-instance").expect("instance id"),
            })
            .await
            .expect_err("last transient error is returned after attempts are exhausted");

        assert!(matches!(error, StoreError::Unavailable { .. }));
        assert_eq!(inner.get_instance_calls(), 2);
    }

    fn immediate_retry_policy() -> RetryPolicy {
        RetryPolicy::new(4, Duration::ZERO, Duration::ZERO)
    }

    #[derive(Debug)]
    struct FakeRetryStore {
        get_instance_errors: Mutex<VecDeque<StoreError>>,
        get_instance_calls: AtomicUsize,
    }

    impl FakeRetryStore {
        fn with_get_instance_errors(errors: Vec<StoreError>) -> Self {
            Self {
                get_instance_errors: Mutex::new(VecDeque::from(errors)),
                get_instance_calls: AtomicUsize::new(0),
            }
        }

        fn get_instance_calls(&self) -> usize {
            self.get_instance_calls.load(Ordering::SeqCst)
        }
    }

    impl ControlPlaneStore for FakeRetryStore {
        fn create_instance<'a>(
            &'a self,
            _request: CreateInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
            not_implemented()
        }

        fn get_instance<'a>(
            &'a self,
            _request: GetInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
            self.get_instance_calls.fetch_add(1, Ordering::SeqCst);
            let error = self
                .get_instance_errors
                .lock()
                .expect("fake store lock")
                .pop_front();

            Box::pin(async move {
                match error {
                    Some(error) => Err(error),
                    None => Ok(None),
                }
            })
        }

        fn delete_instance<'a>(
            &'a self,
            _request: DeleteInstanceRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn create_workload_class_version<'a>(
            &'a self,
            _request: CreateWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
            not_implemented()
        }

        fn load_workload_class_version<'a>(
            &'a self,
            _request: LoadWorkloadClassVersionRequest,
        ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
            not_implemented()
        }

        fn create_route_binding<'a>(
            &'a self,
            _request: CreateRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
            not_implemented()
        }

        fn get_route_binding<'a>(
            &'a self,
            _request: GetRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
            not_implemented()
        }

        fn delete_route_binding<'a>(
            &'a self,
            _request: DeleteRouteBindingRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn resolve_route<'a>(
            &'a self,
            _identity: RouteIdentity,
        ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
            not_implemented()
        }

        fn compare_and_swap_instance_state<'a>(
            &'a self,
            _request: CompareAndSwapInstanceStateRequest,
        ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
            not_implemented()
        }

        fn record_materialization<'a>(
            &'a self,
            _request: RecordMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
            not_implemented()
        }

        fn load_ready_materialization<'a>(
            &'a self,
            _request: LoadReadyMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            not_implemented()
        }

        fn load_active_materialization<'a>(
            &'a self,
            _request: LoadActiveMaterializationRequest,
        ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
            not_implemented()
        }

        fn complete_wake<'a>(
            &'a self,
            _request: CompleteWakeRequest,
        ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
            not_implemented()
        }

        fn begin_sleep<'a>(
            &'a self,
            _request: BeginSleepRequest,
        ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
            not_implemented()
        }

        fn finalize_sleep<'a>(
            &'a self,
            _request: FinalizeSleepRequest,
        ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
            not_implemented()
        }

        fn lookup_route_dependencies<'a>(
            &'a self,
            _request: RouteDependencyLookup,
        ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
            not_implemented()
        }

        fn put_http01_challenge<'a>(
            &'a self,
            _request: PutHttp01ChallengeRequest,
        ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
            not_implemented()
        }

        fn resolve_http01_challenge<'a>(
            &'a self,
            _key: Http01ChallengeKey,
        ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
            not_implemented()
        }

        fn delete_http01_challenge<'a>(
            &'a self,
            _request: DeleteHttp01ChallengeRequest,
        ) -> StoreFuture<'a, StoreResult<bool>> {
            not_implemented()
        }

        fn expire_http01_challenges<'a>(
            &'a self,
            _request: ExpireHttp01ChallengesRequest,
        ) -> StoreFuture<'a, StoreResult<usize>> {
            not_implemented()
        }
    }

    fn not_implemented<'a, T>() -> StoreFuture<'a, StoreResult<T>> {
        Box::pin(async { Err(StoreError::internal("fake store method is not implemented")) })
    }
}
