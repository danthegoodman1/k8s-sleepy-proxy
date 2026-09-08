//! Shared wire, routing and client authentication contracts. No server implementation.
/// Initial activation protection: the default 130 s cold-route wait plus the
/// larger of the 10 s connect and 60 s initial HTTP-header setup budgets.
/// Frontline configurations must fit within this shared finite ceiling.
pub const INITIAL_ACTIVATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(190);

/// Bounded retry hint on an expected ReportIdle FailedPrecondition response.
pub const IDLE_RETRY_AFTER_METADATA: &str = "sleepypods-idle-retry-after-ms";

pub mod auth;
pub mod http01;
pub mod instance;
pub mod materialization;
pub mod route;
pub mod ids {
    pub use sleepypods_types::*;
}
pub mod pb {
    tonic::include_proto!("sleepypods.controlplane.v1");
}
pub use auth::{BearerToken, InvalidBearerToken, OptionalBearerTokenInterceptor};
pub use http01::*;
pub use instance::InstanceState;
pub use materialization::{BackendEndpoint, MaterializationTarget};
pub use route::*;
pub use sleepypods_types::*;
