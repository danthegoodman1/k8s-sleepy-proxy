//! Shared proxy primitives for future data-plane binaries.

pub mod accounting;
pub mod admission;
mod connect;
mod delivery;
pub mod drain;
pub mod http;
pub mod observability;
pub mod resources;
pub mod shutdown;
pub mod tcp;
pub mod timeout;
pub mod tls;
pub mod websocket;

pub use accounting::{ActiveConnection, ActiveConnectionCounter};
pub use admission::{AdmissionError, AdmissionLimiter, AdmissionPermit};
pub use connect::connect_tcp;
pub use drain::{DrainError, DrainPermit, DrainTracker};
pub use http::{
    apply_forwarded_header_policy, forwarded_headers, prepare_reverse_proxy_request,
    serve_http_connection, serve_http_connection_admitted, strip_forwarded_headers,
    strip_hop_by_hop_headers, upstream_request_uri, HttpProxy, HttpProxyError,
    ReverseProxyRequestError, TrackedBody,
};
pub use resources::{AdmittedIo, InvalidProxyResourceConfig, ProxyAdmission, ProxyResourceConfig};
pub use shutdown::Shutdown;
pub use tcp::{
    configure_tcp_keepalive, proxy_streams, proxy_streams_with_idle_timeout,
    proxy_streams_with_timeouts, TcpProxy, TcpProxyConfig, TcpProxyError, TcpProxyStats,
};
pub use timeout::{with_timeout, TimeoutError};
pub use tls::{
    parse_tls_client_hello_sni, read_tls_client_hello_prefix,
    read_tls_client_hello_prefix_with_limit, TlsClientHelloError, TlsClientHelloPrefix,
    TlsClientHelloSni, MAX_TLS_CLIENT_HELLO_PREFIX_LEN,
};
pub use websocket::{
    is_websocket_upgrade, proxy_websocket_streams, proxy_websocket_streams_with_config,
    websocket_error_response, websocket_upgrade_response, AcceptedWebSocketUpstream,
    WebSocketProxy, WebSocketProxyConfig, WebSocketProxyError, WebSocketProxyStats,
    WebSocketUpgrade,
};
