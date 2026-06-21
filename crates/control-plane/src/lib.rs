//! Domain model and persistence contract for the SleepyPods control plane.

pub mod api;
pub mod config;
pub mod http01;
pub mod ids;
pub mod instance;
pub mod materialization;
pub mod postgres;
pub mod route;
pub mod store;
pub mod workload;

pub use config::{ControlPlaneConfig, PostgresStoreConfig, StoreProviderConfig, StoreProviderName};
pub use http01::{
    DeleteHttp01ChallengeRequest, ExpireHttp01ChallengesRequest, Http01ChallengeKey,
    Http01ChallengeRecord, PutHttp01ChallengeRequest,
};
pub use ids::{
    BackendGeneration, Generation, IdempotencyKey, InstanceId, MaterializationId, NonEmptyString,
    RouteBindingId, WorkloadClassId,
};
pub use instance::{
    CompareAndSwapInstanceStateRequest, CreateInstanceRequest, CreateInstanceResult,
    DeleteInstanceRequest, GetInstanceRequest, InstanceRecord, InstanceState, InstanceValues,
    StateTransitionReason,
};
pub use materialization::{
    BackendEndpoint, MaterializationRecord, MaterializationState, MaterializationTarget,
    RecordMaterializationRequest, RenderedObjectRef,
};
pub use postgres::PostgresStore;
pub use route::{
    CachePolicy, PathPrefix, ProtocolRoute, RouteBindingRecord, RouteBindingSpec,
    RouteDependencyLookup, RouteDependencySet, RouteEntry, RouteHost, RouteHostKind, RouteIdentity,
    RouteResolution,
};
pub use store::{ControlPlaneStore, StoreError, StoreFuture, StoreResult};
pub use workload::{
    CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, ValueSchemaError,
    WorkloadClassVersion, WorkloadClassVersionRef, WorkloadValueFieldRule, WorkloadValueSchema,
};
