use crate::{
    http01::{
        DeleteHttp01ChallengeRequest, ExpireHttp01ChallengesRequest, Http01ChallengeKey,
        Http01ChallengeRecord, PutHttp01ChallengeRequest,
    },
    instance::{
        CompareAndSwapInstanceStateRequest, CreateInstanceRequest, CreateInstanceResult,
        InstanceRecord,
    },
    materialization::{MaterializationRecord, RecordMaterializationRequest},
    route::{RouteDependencyLookup, RouteDependencySet, RouteIdentity, RouteResolution},
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
