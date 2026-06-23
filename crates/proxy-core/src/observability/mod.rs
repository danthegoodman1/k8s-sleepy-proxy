//! Shared observability conventions and a tiny runtime recording boundary.
//!
//! This module defines stable names, low-cardinality labels, tracing field
//! conventions, and a backend-neutral recorder. Binaries may install the
//! process-wide stderr sink until a real metrics backend/exporter is wired.

use std::fmt;

use crate::{
    admission::AdmissionError,
    drain::DrainError,
    timeout::TimeoutError,
    tls::{TlsClientHelloError, TlsClientHelloSni},
};

pub mod metrics;
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

    pub fn from_result(
        outcome: &Result<TlsClientHelloSni, TlsClientHelloError>,
    ) -> TlsClientHelloOutcome {
        match outcome {
            Ok(sni) => Self::from(sni),
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

impl From<&AdmissionError> for Outcome {
    fn from(_error: &AdmissionError) -> Self {
        Self::Rejected
    }
}

impl From<&DrainError> for Outcome {
    fn from(error: &DrainError) -> Self {
        match error {
            DrainError::Draining => Self::Rejected,
            DrainError::GraceTimeout { .. } => Self::Timeout,
        }
    }
}

impl From<&TimeoutError> for Outcome {
    fn from(_error: &TimeoutError) -> Self {
        Self::Timeout
    }
}

impl From<&TlsClientHelloSni> for TlsClientHelloOutcome {
    fn from(sni: &TlsClientHelloSni) -> Self {
        match sni {
            TlsClientHelloSni::Sni { .. } => Self::Sni,
            TlsClientHelloSni::NoSni => Self::NoSni,
            TlsClientHelloSni::Incomplete { .. } => Self::Incomplete,
        }
    }
}

impl From<&TlsClientHelloError> for TlsClientHelloOutcome {
    fn from(error: &TlsClientHelloError) -> Self {
        match error {
            TlsClientHelloError::NotTls => Self::NotTls,
            TlsClientHelloError::UnsupportedVersion { .. } => Self::UnsupportedVersion,
            TlsClientHelloError::NotClientHello => Self::NotClientHello,
            TlsClientHelloError::RecordTooLarge { .. } => Self::RecordTooLarge,
            TlsClientHelloError::Malformed => Self::Malformed,
            TlsClientHelloError::InvalidHostname => Self::InvalidHostname,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        AdmissionError, DrainError, Operation, Outcome, Protocol, ProxyState, TimeoutError,
        TlsClientHelloError, TlsClientHelloOutcome, TlsClientHelloSni, TrafficDirection,
    };

    #[test]
    fn shared_label_values_are_stable() {
        assert_eq!(
            Protocol::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            ["tcp", "http", "websocket", "tls"]
        );
        assert_eq!(
            TrafficDirection::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            ["client_to_upstream", "upstream_to_client"]
        );
        assert_eq!(
            Operation::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            [
                "accept",
                "admit",
                "connect",
                "forward",
                "rewrite_request",
                "drain",
                "tls_client_hello",
                "route_cache_lookup",
                "subscribe_route",
                "unsubscribe",
                "subscribe_stream",
                "wake_instance",
                "materialize",
                "http01_resolve",
                "report_idle"
            ]
        );
        assert_eq!(
            Outcome::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            [
                "success",
                "error",
                "timeout",
                "rejected",
                "canceled",
                "hit",
                "miss",
                "started",
                "closed",
                "updated",
                "invalidated",
                "already_running",
                "already_waking",
                "already_draining"
            ]
        );
        assert_eq!(
            ProxyState::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            ["accepting", "active", "draining", "idle"]
        );
        assert_eq!(
            TlsClientHelloOutcome::ALL
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            [
                "sni",
                "no_sni",
                "incomplete",
                "not_tls",
                "unsupported_version",
                "not_client_hello",
                "record_too_large",
                "malformed",
                "invalid_hostname"
            ]
        );
    }

    #[test]
    fn existing_errors_map_to_bounded_outcomes() {
        assert_eq!(
            Outcome::from(&AdmissionError::Saturated { limit: 5000 }),
            Outcome::Rejected
        );
        assert_eq!(Outcome::from(&AdmissionError::Closed), Outcome::Rejected);
        assert_eq!(Outcome::from(&DrainError::Draining), Outcome::Rejected);
        assert_eq!(
            Outcome::from(&DrainError::GraceTimeout {
                timeout: Duration::from_secs(999),
                active: 42
            }),
            Outcome::Timeout
        );
        assert_eq!(
            Outcome::from(&TimeoutError::new(Duration::from_secs(123))),
            Outcome::Timeout
        );
    }

    #[test]
    fn tls_client_hello_outcomes_drop_dynamic_values() {
        assert_eq!(
            TlsClientHelloOutcome::from_result(&Ok(TlsClientHelloSni::Sni {
                hostname: "tenant-specific.example.test".to_owned()
            })),
            TlsClientHelloOutcome::Sni
        );
        assert_eq!(
            TlsClientHelloOutcome::from_result(&Ok(TlsClientHelloSni::Incomplete {
                needed: Some(4096)
            })),
            TlsClientHelloOutcome::Incomplete
        );
        assert_eq!(
            TlsClientHelloOutcome::from_result(&Err(TlsClientHelloError::UnsupportedVersion {
                major: 3,
                minor: 99
            })),
            TlsClientHelloOutcome::UnsupportedVersion
        );
        assert_eq!(
            TlsClientHelloOutcome::from_result(&Err(TlsClientHelloError::RecordTooLarge {
                len: 1_000_000,
                max: 16_384
            })),
            TlsClientHelloOutcome::RecordTooLarge
        );
    }
}
