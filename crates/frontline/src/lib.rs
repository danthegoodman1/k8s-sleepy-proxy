//! Frontline route-resolution primitives.
//!
//! This crate intentionally contains no listeners, forwarding code, or real
//! control-plane transport. It models the pure local state future frontline
//! protocol handlers will use around lazy route subscription.

pub mod cache;
pub mod identity;
pub mod matcher;
pub mod subscription;
pub mod wake;

pub use cache::{
    CacheInsertResult, CacheLookup, CacheLookupHit, CacheLookupStatus, NegativeCacheEntry,
    PositiveCacheEntry, RouteCache,
};
pub use identity::{RequestIdentityError, RouteRequestIdentity};
pub use matcher::{MatchedRoute, RouteMatcher, RouteRule};
pub use subscription::{
    ApplyControlPlaneMessageOutcome, ApplyUpdateOutcome, InvalidationReason, ProxySubscribeInput,
    RouteRequestId, SubscribeControlPlaneOutput, SubscriptionId, SubscriptionState,
    UnsubscribeOutcome,
};
pub use wake::{
    route_wake_decision, validate_route_update, validate_wake_response, ReadyBackend,
    RouteUpdateDisposition, RouteWakeDecision, StaleWakeObservation, WakeAdmission,
    WakeInstanceRequest, WakeInstanceResponse, WakeReason, WakeResponseDisposition, WakeTracker,
    WakeUnavailable, WakeUnavailableReason, WakeWait, WakeWaitReason,
};
