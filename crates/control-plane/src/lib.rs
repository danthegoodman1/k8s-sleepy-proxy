//! Domain model and persistence contract for the SleepyPods control plane.

pub mod api;
pub mod config;
pub mod http01;
pub mod ids;
pub mod instance;
mod kube_materializer;
pub mod manifest;
pub mod materialization;
pub mod materializer;
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
pub use kube_materializer::{
    InvalidKubeMaterializerClientConfig, KubeMaterializerClient, KubeMaterializerClientConfig,
};
pub use manifest::{
    render_manifests, ApplyOrder, Container, ContainerPort, ContainerPortTemplate,
    ContainerTemplate, CsiPersistentVolumeSource, Deployment, DeploymentSpec, EnvVar,
    EnvVarTemplate, HostPathPersistentVolumeSource, KubernetesObject, LabelSelector,
    ManifestRenderError, ManifestTemplate, ObjectMeta, PersistentVolume,
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimRef,
    PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource, PersistentVolumeReclaimPolicy,
    PersistentVolumeSource, PersistentVolumeSourceTemplate, PersistentVolumeSpec, PodSpec,
    PodTemplateMetadata, PodTemplateSpec, PodVolume, RenderManifestRequest, RenderedManifest,
    RenderedManifestObject, Service, ServicePort, ServicePortTemplate, ServiceSpec,
    ServiceTemplate, SidecarTemplate, StatefulSet, StatefulSetSpec, TemplateText, TemplateTextPart,
    VolumeMount, VolumeResourceRequirements, VolumeTemplate, WorkloadKind, WorkloadTemplate,
};
pub use materialization::{
    BackendEndpoint, MaterializationRecord, MaterializationState, MaterializationTarget,
    RecordMaterializationRequest, RenderedObjectRef,
};
pub use materializer::{
    rendered_object_ref, AppliedMaterialization, KubernetesClientError, KubernetesClientFuture,
    KubernetesClientResult, KubernetesMaterializer, KubernetesMaterializerClient,
    MaterializerError,
};
pub use postgres::PostgresStore;
pub use route::{
    CachePolicy, CreateRouteBindingRequest, DeleteRouteBindingRequest, GetRouteBindingRequest,
    PathPrefix, ProtocolRoute, RouteBindingRecord, RouteBindingSpec, RouteDependencyLookup,
    RouteDependencySet, RouteEntry, RouteHost, RouteHostKind, RouteIdentity, RouteResolution,
};
pub use store::{ControlPlaneStore, StoreError, StoreFuture, StoreResult};
pub use workload::{
    CreateWorkloadClassVersionRequest, LoadWorkloadClassVersionRequest, ValueSchemaError,
    WorkloadClassVersion, WorkloadClassVersionRef, WorkloadValueFieldRule, WorkloadValueSchema,
};
