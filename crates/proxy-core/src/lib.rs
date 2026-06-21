//! Shared proxy primitives for future data-plane binaries.

pub mod accounting;
pub mod drain;
pub mod tcp;

pub use accounting::{ActiveConnection, ActiveConnectionCounter};
pub use drain::{DrainError, DrainPermit, DrainTracker};
pub use tcp::{proxy_streams, TcpProxy, TcpProxyConfig, TcpProxyError, TcpProxyStats};
