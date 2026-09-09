//! Shared observability conventions and a tiny runtime recording boundary.
//!
//! This module defines stable names, low-cardinality labels, tracing field
//! conventions, and a backend-neutral recorder. Binaries may install the
//! process-wide stderr sink until a real metrics backend/exporter is wired.

use std::fmt;

pub mod metrics;
pub mod prometheus;
pub mod recorder;
pub mod trace;

/// Proxy protocol family for metrics and tracing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Protocol {
    Tcp,
    Http,
    WebSocket,
    Tls,
}

/// Direction for bytes, messages, and stream-level tracing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TrafficDirection {
    ClientToUpstream,
    UpstreamToClient,
}

/// Shared low-cardinality operation names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Operation {
    Accept,
    Admit,
    Connect,
    Forward,
    RewriteRequest,
    Drain,
    TlsClientHello,
    RouteCacheLookup,
    SubscribeRoute,
    Unsubscribe,
    SubscribeStream,
    WakeInstance,
    Materialize,
    Http01Resolve,
    ReportIdle,
    Apply,
    Delete,
    Readiness,
    CertificateFetch,
    CertificateRefresh,
    CertificateWatch,
    CertificateInstall,
}

/// Generic low-cardinality operation outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    Success,
    Error,
    Timeout,
    Rejected,
    Canceled,
    Hit,
    Miss,
    Started,
    Closed,
    Updated,
    Invalidated,
    AlreadyRunning,
    AlreadyWaking,
    AlreadyDraining,
}

/// Bounded proxy lifecycle states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProxyState {
    Accepting,
    Active,
    Draining,
    Idle,
}

/// TLS ClientHello/SNI parser outcomes suitable for metric labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TlsClientHelloOutcome {
    Sni,
    NoSni,
    Incomplete,
    NotTls,
    UnsupportedVersion,
    NotClientHello,
    RecordTooLarge,
    Malformed,
    InvalidHostname,
}

impl Protocol {
    pub const ALL: &'static [Self] = &[Self::Tcp, Self::Http, Self::WebSocket, Self::Tls];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Http => "http",
            Self::WebSocket => "websocket",
            Self::Tls => "tls",
        }
    }
}

impl TrafficDirection {
    pub const ALL: &'static [Self] = &[Self::ClientToUpstream, Self::UpstreamToClient];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClientToUpstream => "client_to_upstream",
            Self::UpstreamToClient => "upstream_to_client",
        }
    }
}

impl Operation {
    pub const ALL: &'static [Self] = &[
        Self::Accept,
        Self::Admit,
        Self::Connect,
        Self::Forward,
        Self::RewriteRequest,
        Self::Drain,
        Self::TlsClientHello,
        Self::RouteCacheLookup,
        Self::SubscribeRoute,
        Self::Unsubscribe,
        Self::SubscribeStream,
        Self::WakeInstance,
        Self::Materialize,
        Self::Http01Resolve,
        Self::ReportIdle,
        Self::Apply,
        Self::Delete,
        Self::Readiness,
        Self::CertificateFetch,
        Self::CertificateRefresh,
        Self::CertificateWatch,
        Self::CertificateInstall,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Admit => "admit",
            Self::Connect => "connect",
            Self::Forward => "forward",
            Self::RewriteRequest => "rewrite_request",
            Self::Drain => "drain",
            Self::TlsClientHello => "tls_client_hello",
            Self::RouteCacheLookup => "route_cache_lookup",
            Self::SubscribeRoute => "subscribe_route",
            Self::Unsubscribe => "unsubscribe",
            Self::SubscribeStream => "subscribe_stream",
            Self::WakeInstance => "wake_instance",
            Self::Materialize => "materialize",
            Self::Http01Resolve => "http01_resolve",
            Self::ReportIdle => "report_idle",
            Self::Apply => "apply",
            Self::Delete => "delete",
            Self::Readiness => "readiness",
            Self::CertificateFetch => "certificate_fetch",
            Self::CertificateRefresh => "certificate_refresh",
            Self::CertificateWatch => "certificate_watch",
            Self::CertificateInstall => "certificate_install",
        }
    }
}

impl Outcome {
    pub const ALL: &'static [Self] = &[
        Self::Success,
        Self::Error,
        Self::Timeout,
        Self::Rejected,
        Self::Canceled,
        Self::Hit,
        Self::Miss,
        Self::Started,
        Self::Closed,
        Self::Updated,
        Self::Invalidated,
        Self::AlreadyRunning,
        Self::AlreadyWaking,
        Self::AlreadyDraining,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Rejected => "rejected",
            Self::Canceled => "canceled",
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Started => "started",
            Self::Closed => "closed",
            Self::Updated => "updated",
            Self::Invalidated => "invalidated",
            Self::AlreadyRunning => "already_running",
            Self::AlreadyWaking => "already_waking",
            Self::AlreadyDraining => "already_draining",
        }
    }
}

impl ProxyState {
    pub const ALL: &'static [Self] = &[Self::Accepting, Self::Active, Self::Draining, Self::Idle];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepting => "accepting",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::Idle => "idle",
        }
    }
}

impl TlsClientHelloOutcome {
    pub const ALL: &'static [Self] = &[
        Self::Sni,
        Self::NoSni,
        Self::Incomplete,
        Self::NotTls,
        Self::UnsupportedVersion,
        Self::NotClientHello,
        Self::RecordTooLarge,
        Self::Malformed,
        Self::InvalidHostname,
    ];

    pub fn from_result<'a, T, E>(outcome: &'a Result<T, E>) -> Self
    where
        Self: From<&'a T> + From<&'a E>,
    {
        match outcome {
            Ok(value) => Self::from(value),
            Err(error) => Self::from(error),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sni => "sni",
            Self::NoSni => "no_sni",
            Self::Incomplete => "incomplete",
            Self::NotTls => "not_tls",
            Self::UnsupportedVersion => "unsupported_version",
            Self::NotClientHello => "not_client_hello",
            Self::RecordTooLarge => "record_too_large",
            Self::Malformed => "malformed",
            Self::InvalidHostname => "invalid_hostname",
        }
    }
}

macro_rules! impl_display {
    ($type:ty) => {
        impl fmt::Display for $type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

impl_display!(Protocol);
impl_display!(TrafficDirection);
impl_display!(Operation);
impl_display!(Outcome);
impl_display!(ProxyState);
impl_display!(TlsClientHelloOutcome);
