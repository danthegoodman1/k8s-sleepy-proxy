use super::{Operation, Outcome, Protocol, ProxyState, TrafficDirection};

pub const FIELD_PROTOCOL: &str = "proxy.protocol";
pub const FIELD_DIRECTION: &str = "proxy.direction";
pub const FIELD_OPERATION: &str = "proxy.operation";
pub const FIELD_OUTCOME: &str = "proxy.outcome";
pub const FIELD_STATE: &str = "proxy.state";
pub const FIELD_ERROR_KIND: &str = "proxy.error_kind";
pub const FIELD_BYTES: &str = "proxy.bytes";
pub const FIELD_MESSAGES: &str = "proxy.messages";
pub const FIELD_ACTIVE: &str = "proxy.active";
pub const FIELD_TIMEOUT_MS: &str = "proxy.timeout_ms";

pub const SPAN_TCP_FORWARD: &str = "proxy.tcp.forward";
pub const SPAN_HTTP_FORWARD: &str = "proxy.http.forward";
pub const SPAN_WEBSOCKET_FORWARD: &str = "proxy.websocket.forward";
pub const SPAN_TLS_CLIENT_HELLO: &str = "proxy.tls.client_hello";
pub const SPAN_ADMISSION: &str = "proxy.admission";
pub const SPAN_DRAIN: &str = "proxy.drain";

pub const ALL_FIELDS: &[&str] = &[
    FIELD_PROTOCOL,
    FIELD_DIRECTION,
    FIELD_OPERATION,
    FIELD_OUTCOME,
    FIELD_STATE,
    FIELD_ERROR_KIND,
    FIELD_BYTES,
    FIELD_MESSAGES,
    FIELD_ACTIVE,
    FIELD_TIMEOUT_MS,
];

pub const ALL_SPANS: &[&str] = &[
    SPAN_TCP_FORWARD,
    SPAN_HTTP_FORWARD,
    SPAN_WEBSOCKET_FORWARD,
    SPAN_TLS_CLIENT_HELLO,
    SPAN_ADMISSION,
    SPAN_DRAIN,
];

/// A tracing field value built from bounded proxy-core conventions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TraceField {
    key: &'static str,
    value: &'static str,
}

impl TraceField {
    const fn new(key: &'static str, value: &'static str) -> Self {
        Self { key, value }
    }

    pub const fn key(self) -> &'static str {
        self.key
    }

    pub const fn value(self) -> &'static str {
        self.value
    }
}

impl Protocol {
    pub const fn trace_field(self) -> TraceField {
        TraceField::new(FIELD_PROTOCOL, self.as_str())
    }
}

impl TrafficDirection {
    pub const fn trace_field(self) -> TraceField {
        TraceField::new(FIELD_DIRECTION, self.as_str())
    }
}

impl Operation {
    pub const fn trace_field(self) -> TraceField {
        TraceField::new(FIELD_OPERATION, self.as_str())
    }
}

impl Outcome {
    pub const fn trace_field(self) -> TraceField {
        TraceField::new(FIELD_OUTCOME, self.as_str())
    }
}

impl ProxyState {
    pub const fn trace_field(self) -> TraceField {
        TraceField::new(FIELD_STATE, self.as_str())
    }
}

pub const fn forwarding_span(protocol: Protocol) -> Option<&'static str> {
    match protocol {
        Protocol::Tcp => Some(SPAN_TCP_FORWARD),
        Protocol::Http => Some(SPAN_HTTP_FORWARD),
        Protocol::WebSocket => Some(SPAN_WEBSOCKET_FORWARD),
        Protocol::Tls => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        forwarding_span, Operation, Outcome, Protocol, ProxyState, TraceField, TrafficDirection,
        ALL_FIELDS, ALL_SPANS, FIELD_OPERATION, FIELD_OUTCOME, FIELD_PROTOCOL,
    };

    #[test]
    fn trace_fields_and_spans_are_stable() {
        assert_eq!(
            ALL_FIELDS,
            [
                "proxy.protocol",
                "proxy.direction",
                "proxy.operation",
                "proxy.outcome",
                "proxy.state",
                "proxy.error_kind",
                "proxy.bytes",
                "proxy.messages",
                "proxy.active",
                "proxy.timeout_ms"
            ]
        );
        assert_eq!(
            ALL_SPANS,
            [
                "proxy.tcp.forward",
                "proxy.http.forward",
                "proxy.websocket.forward",
                "proxy.tls.client_hello",
                "proxy.admission",
                "proxy.drain"
            ]
        );
    }

    #[test]
    fn trace_fields_do_not_include_request_identity_by_default() {
        let forbidden = [
            "host",
            "hostname",
            "route",
            "route_id",
            "instance",
            "instance_id",
            "pod",
            "namespace",
            "path",
            "request",
        ];

        for field in ALL_FIELDS {
            for forbidden_fragment in forbidden {
                assert!(
                    !field.contains(forbidden_fragment),
                    "trace field {field} leaks request-specific identity by default"
                );
            }
        }
    }

    #[test]
    fn trace_field_names_are_unique() {
        let fields = ALL_FIELDS.iter().copied().collect::<HashSet<_>>();
        let spans = ALL_SPANS.iter().copied().collect::<HashSet<_>>();

        assert_eq!(fields.len(), ALL_FIELDS.len());
        assert_eq!(spans.len(), ALL_SPANS.len());
    }

    #[test]
    fn typed_trace_fields_use_expected_keys() {
        assert_eq!(
            Protocol::Tcp.trace_field(),
            TraceField::new(FIELD_PROTOCOL, "tcp")
        );
        assert_eq!(
            TrafficDirection::UpstreamToClient.trace_field().value(),
            "upstream_to_client"
        );
        assert_eq!(Operation::Drain.trace_field().key(), FIELD_OPERATION);
        assert_eq!(Outcome::Timeout.trace_field().key(), FIELD_OUTCOME);
        assert_eq!(ProxyState::Idle.trace_field().value(), "idle");
    }

    #[test]
    fn forwarding_span_only_applies_to_forwarding_protocols() {
        assert_eq!(forwarding_span(Protocol::Tcp), Some("proxy.tcp.forward"));
        assert_eq!(forwarding_span(Protocol::Http), Some("proxy.http.forward"));
        assert_eq!(
            forwarding_span(Protocol::WebSocket),
            Some("proxy.websocket.forward")
        );
        assert_eq!(forwarding_span(Protocol::Tls), None);
    }
}
