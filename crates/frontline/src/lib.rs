//! Frontline route-resolution primitives.
//!
//! This crate intentionally contains no listeners or real control-plane
//! transport. It models the pure local state and forwarding adapters future
//! frontline protocol handlers will use around lazy route subscription.

pub mod cache;
pub mod forward;
pub mod identity;
pub mod matcher;
pub mod subscription;
pub mod tls;
pub mod wake;

pub use cache::{
    CacheInsertResult, CacheLookup, CacheLookupHit, CacheLookupStatus, NegativeCacheEntry,
    PositiveCacheEntry, RouteCache,
};
pub use forward::{
    http_upstream_origin, websocket_upstream_url, BackendForwardError, FrontlineForwardError,
    FrontlineForwarder,
};
pub use identity::{RequestIdentityError, RouteRequestIdentity};
pub use matcher::{MatchedRoute, RouteMatcher, RouteRule};
pub use subscription::{
    ApplyControlPlaneMessageOutcome, ApplyUpdateOutcome, InvalidationReason, ProxySubscribeInput,
    RouteRequestId, SubscribeControlPlaneOutput, SubscriptionId, SubscriptionState,
    UnsubscribeOutcome,
};
pub use tls::{
    passthrough_backend_addr, FrontlineTlsAdapter, TerminatedTls, TlsCertificateError,
    TlsCertificateStore, TlsPassthrough, TlsPassthroughBackendError, TlsPassthroughError,
    TlsTerminationError,
};
pub use wake::{
    route_wake_decision, validate_route_update, validate_wake_response, ReadyBackend,
    RouteUpdateDisposition, RouteWakeDecision, StaleWakeObservation, WakeAdmission,
    WakeInstanceRequest, WakeInstanceResponse, WakeReason, WakeResponseDisposition, WakeTracker,
    WakeUnavailable, WakeUnavailableReason, WakeWait, WakeWaitReason,
};
