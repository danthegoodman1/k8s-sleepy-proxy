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
        LoadMaterializationRequest, LoadReadyMaterializationRequest, MaterializationRecord,
        RecordMaterializationRequest, ReleaseMaterializationReconciliationLeaseRequest,
        RenewMaterializationReconciliationLeaseRequest,
    },
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
        RouteBindingRecord, RouteDependencyLookup, RouteDependencySet, RouteIdentity,
        RouteResolution,
    },
    store::{ControlPlaneStore, StoreFuture, StoreResult},
    workload::{
        CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, WorkloadClassVersion,
    },
};

use super::{connection::PostgresStore, http01_ops, instance_ops, materialization_ops, route_ops};

impl ControlPlaneStore for PostgresStore {
    fn create_instance<'a>(
        &'a self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>> {
        Box::pin(async move { instance_ops::create_instance(self, request).await })
    }

    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>> {
        Box::pin(async move { instance_ops::get_instance(self, request).await })
    }

    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move { instance_ops::delete_instance(self, request).await })
    }

    fn create_workload_class_version<'a>(
        &'a self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>> {
        Box::pin(async move { instance_ops::create_workload_class_version(self, request).await })
    }

    fn load_workload_class_version<'a>(
        &'a self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>> {
        Box::pin(async move { instance_ops::load_workload_class_version(self, request).await })
    }

    fn create_route_binding<'a>(
        &'a self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>> {
        Box::pin(async move { route_ops::create_route_binding(self, request).await })
    }

    fn get_route_binding<'a>(
        &'a self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>> {
        Box::pin(async move { route_ops::get_route_binding(self, request).await })
    }

    fn delete_route_binding<'a>(
        &'a self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move { route_ops::delete_route_binding(self, request).await })
    }

    fn resolve_route<'a>(
        &'a self,
        identity: RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<RouteResolution>> {
        Box::pin(async move { route_ops::resolve_route(self, identity).await })
    }

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>> {
        Box::pin(async move { instance_ops::compare_and_swap_instance_state(self, request).await })
    }

    fn record_materialization<'a>(
        &'a self,
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>> {
        Box::pin(async move { materialization_ops::record_materialization(self, request).await })
    }

    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(
            async move { materialization_ops::load_ready_materialization(self, request).await },
        )
    }

    fn load_active_materialization<'a>(
        &'a self,
        request: LoadActiveMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(
            async move { materialization_ops::load_active_materialization(self, request).await },
        )
    }

    fn load_materialization<'a>(
        &'a self,
        request: LoadMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move { materialization_ops::load_materialization(self, request).await })
    }

    fn complete_wake<'a>(
        &'a self,
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        Box::pin(async move { materialization_ops::complete_wake(self, request).await })
    }

    fn begin_sleep<'a>(
        &'a self,
        request: BeginSleepRequest,
    ) -> StoreFuture<'a, StoreResult<BeginSleepResult>> {
        Box::pin(async move { materialization_ops::begin_sleep(self, request).await })
    }

    fn finalize_sleep<'a>(
        &'a self,
        request: FinalizeSleepRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        Box::pin(async move { materialization_ops::finalize_sleep(self, request).await })
    }

    fn list_materialization_reconciliation_candidates<'a>(
        &'a self,
        request: ListMaterializationReconciliationCandidatesRequest,
    ) -> StoreFuture<'a, StoreResult<Vec<MaterializationRecord>>> {
        Box::pin(async move {
            materialization_ops::list_materialization_reconciliation_candidates(self, request).await
        })
    }

    fn claim_materialization_reconciliation<'a>(
        &'a self,
        request: ClaimMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            materialization_ops::claim_materialization_reconciliation(self, request).await
        })
    }

    fn renew_materialization_reconciliation_lease<'a>(
        &'a self,
        request: RenewMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            materialization_ops::renew_materialization_reconciliation_lease(self, request).await
        })
    }

    fn release_materialization_reconciliation_lease<'a>(
        &'a self,
        request: ReleaseMaterializationReconciliationLeaseRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move {
            materialization_ops::release_materialization_reconciliation_lease(self, request).await
        })
    }

    fn complete_wake_reconciliation<'a>(
        &'a self,
        request: CompleteWakeReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>> {
        Box::pin(
            async move { materialization_ops::complete_wake_reconciliation(self, request).await },
        )
    }

    fn finalize_sleep_reconciliation<'a>(
        &'a self,
        request: FinalizeSleepReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<FinalizeSleepResult>> {
        Box::pin(
            async move { materialization_ops::finalize_sleep_reconciliation(self, request).await },
        )
    }

    fn delete_materialization_reconciliation<'a>(
        &'a self,
        request: DeleteMaterializationReconciliationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(async move {
            materialization_ops::delete_materialization_reconciliation(self, request).await
        })
    }

    fn force_delete_materialization<'a>(
        &'a self,
        request: ForceDeleteMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>> {
        Box::pin(
            async move { materialization_ops::force_delete_materialization(self, request).await },
        )
    }

    fn force_release_exclusivity_key<'a>(
        &'a self,
        request: ForceReleaseExclusivityKeyRequest,
    ) -> StoreFuture<'a, StoreResult<ForceReleaseExclusivityKeyResult>> {
        Box::pin(
            async move { materialization_ops::force_release_exclusivity_key(self, request).await },
        )
    }

    fn lookup_route_dependencies<'a>(
        &'a self,
        request: RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>> {
        Box::pin(async move { route_ops::lookup_route_dependencies(self, request).await })
    }

    fn put_http01_challenge<'a>(
        &'a self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>> {
        Box::pin(async move { http01_ops::put_http01_challenge(self, request).await })
    }

    fn resolve_http01_challenge<'a>(
        &'a self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>> {
        Box::pin(async move { http01_ops::resolve_http01_challenge(self, key).await })
    }

    fn delete_http01_challenge<'a>(
        &'a self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>> {
        Box::pin(async move { http01_ops::delete_http01_challenge(self, request).await })
    }

    fn expire_http01_challenges<'a>(
        &'a self,
        request: ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>> {
        Box::pin(async move { http01_ops::expire_http01_challenges(self, request).await })
    }
}
