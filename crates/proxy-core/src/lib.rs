//! Shared proxy primitives for future data-plane binaries.

pub mod accounting;
pub mod admission;
pub mod drain;
pub mod http;
pub mod shutdown;
pub mod tcp;
pub mod timeout;
pub mod websocket;

pub use accounting::{ActiveConnection, ActiveConnectionCounter};
pub use admission::{AdmissionError, AdmissionLimiter, AdmissionPermit};
pub use drain::{DrainError, DrainPermit, DrainTracker};
pub use http::{
    prepare_reverse_proxy_request, strip_hop_by_hop_headers, upstream_request_uri, HttpProxy,
    HttpProxyError, ReverseProxyRequestError, TrackedBody,
};
pub use shutdown::Shutdown;
pub use tcp::{proxy_streams, TcpProxy, TcpProxyConfig, TcpProxyError, TcpProxyStats};
pub use timeout::{with_timeout, TimeoutError};
pub use websocket::{
    proxy_websocket_streams, WebSocketProxy, WebSocketProxyError, WebSocketProxyStats,
};
