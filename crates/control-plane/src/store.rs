use std::{error::Error, fmt, future::Future, pin::Pin};

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
        CompleteWakeRequest, CompleteWakeResult, LoadReadyMaterializationRequest,
        MaterializationRecord, RecordMaterializationRequest,
    },
    route::{
        CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
        RouteBindingRecord, RouteDependencyLookup, RouteDependencySet, RouteIdentity,
        RouteResolution,
    },
    workload::{
        CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, WorkloadClassVersion,
    },
};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type StoreResult<T> = Result<T, StoreError>;

pub trait ControlPlaneStore: Send + Sync {
    fn create_instance<'a>(
        &'a self,
        request: CreateInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<CreateInstanceResult>>;

    fn get_instance<'a>(
        &'a self,
        request: GetInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<Option<InstanceRecord>>>;

    fn delete_instance<'a>(
        &'a self,
        request: DeleteInstanceRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn create_workload_class_version<'a>(
        &'a self,
        request: CreateWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<WorkloadClassVersion>>;

    fn load_workload_class_version<'a>(
        &'a self,
        request: LoadWorkloadClassVersionRequest,
    ) -> StoreFuture<'a, StoreResult<Option<WorkloadClassVersion>>>;

    fn create_route_binding<'a>(
        &'a self,
        request: CreateRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<RouteBindingRecord>>;

    fn get_route_binding<'a>(
        &'a self,
        request: GetRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<Option<RouteBindingRecord>>>;

    fn delete_route_binding<'a>(
        &'a self,
        request: DeleteRouteBindingRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn resolve_route<'a>(
        &'a self,
        identity: RouteIdentity,
    ) -> StoreFuture<'a, StoreResult<RouteResolution>>;

    fn compare_and_swap_instance_state<'a>(
        &'a self,
        request: CompareAndSwapInstanceStateRequest,
    ) -> StoreFuture<'a, StoreResult<InstanceRecord>>;

    fn record_materialization<'a>(
        &'a self,
        request: RecordMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<MaterializationRecord>>;

    fn load_ready_materialization<'a>(
        &'a self,
        request: LoadReadyMaterializationRequest,
    ) -> StoreFuture<'a, StoreResult<Option<MaterializationRecord>>>;

    fn complete_wake<'a>(
        &'a self,
        request: CompleteWakeRequest,
    ) -> StoreFuture<'a, StoreResult<CompleteWakeResult>>;

    fn lookup_route_dependencies<'a>(
        &'a self,
        request: RouteDependencyLookup,
    ) -> StoreFuture<'a, StoreResult<Option<RouteDependencySet>>>;

    fn put_http01_challenge<'a>(
        &'a self,
        request: PutHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<Http01ChallengeRecord>>;

    fn resolve_http01_challenge<'a>(
        &'a self,
        key: Http01ChallengeKey,
    ) -> StoreFuture<'a, StoreResult<Option<Http01ChallengeRecord>>>;

    fn delete_http01_challenge<'a>(
        &'a self,
        request: DeleteHttp01ChallengeRequest,
    ) -> StoreFuture<'a, StoreResult<bool>>;

    fn expire_http01_challenges<'a>(
        &'a self,
        request: ExpireHttp01ChallengesRequest,
    ) -> StoreFuture<'a, StoreResult<usize>>;
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
    use super::ControlPlaneStore;

    fn assert_dyn_safe<T: ControlPlaneStore + ?Sized>() {}

    #[test]
    fn control_plane_store_trait_is_dyn_safe() {
        assert_dyn_safe::<dyn ControlPlaneStore>();
    }
}
