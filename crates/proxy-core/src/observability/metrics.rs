use super::{Operation, Outcome, Protocol, ProxyState, TlsClientHelloOutcome, TrafficDirection};

/// Metric label keys approved for proxy-core hot paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LabelKey {
    Protocol,
    Direction,
    Operation,
    Outcome,
    State,
}

/// Backend-neutral metric type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// Static metric metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricDescriptor {
    name: &'static str,
    kind: MetricKind,
    unit: Option<&'static str>,
    description: &'static str,
    label_keys: &'static [LabelKey],
}

/// A metric label value built from a bounded enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MetricLabel {
    key: LabelKey,
    value: &'static str,
}

pub const PROXY_ACTIVE_STREAMS_NAME: &str = "sleepypods_proxy_active_streams";
pub const PROXY_ADMISSION_DECISIONS_TOTAL_NAME: &str = "sleepypods_proxy_admission_decisions_total";
pub const PROXY_DRAIN_EVENTS_TOTAL_NAME: &str = "sleepypods_proxy_drain_events_total";
pub const PROXY_FORWARDED_BYTES_TOTAL_NAME: &str = "sleepypods_proxy_forwarded_bytes_total";
pub const PROXY_FORWARDED_MESSAGES_TOTAL_NAME: &str = "sleepypods_proxy_forwarded_messages_total";
pub const PROXY_OPERATION_DURATION_SECONDS_NAME: &str =
    "sleepypods_proxy_operation_duration_seconds";
pub const PROXY_TLS_CLIENT_HELLO_TOTAL_NAME: &str = "sleepypods_proxy_tls_client_hello_total";

const PROTOCOL_LABELS: &[LabelKey] = &[LabelKey::Protocol];
const PROTOCOL_OUTCOME_LABELS: &[LabelKey] = &[LabelKey::Protocol, LabelKey::Outcome];
const PROTOCOL_DIRECTION_LABELS: &[LabelKey] = &[LabelKey::Protocol, LabelKey::Direction];
const PROTOCOL_OPERATION_OUTCOME_LABELS: &[LabelKey] =
    &[LabelKey::Protocol, LabelKey::Operation, LabelKey::Outcome];
const STATE_OUTCOME_LABELS: &[LabelKey] = &[LabelKey::State, LabelKey::Outcome];

pub const PROXY_ACTIVE_STREAMS: MetricDescriptor = MetricDescriptor::new(
    PROXY_ACTIVE_STREAMS_NAME,
    MetricKind::Gauge,
    Some("streams"),
    "Active proxy streams, requests, or upgraded sessions currently holding work permits.",
    PROTOCOL_LABELS,
);

pub const PROXY_ADMISSION_DECISIONS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_ADMISSION_DECISIONS_TOTAL_NAME,
    MetricKind::Counter,
    Some("decisions"),
    "Admission limiter decisions for new proxy work.",
    PROTOCOL_OUTCOME_LABELS,
);

pub const PROXY_DRAIN_EVENTS_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_DRAIN_EVENTS_TOTAL_NAME,
    MetricKind::Counter,
    Some("events"),
    "Drain lifecycle events such as start, idle completion, rejection, and grace timeout.",
    STATE_OUTCOME_LABELS,
);

pub const PROXY_FORWARDED_BYTES_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_FORWARDED_BYTES_TOTAL_NAME,
    MetricKind::Counter,
    Some("bytes"),
    "Bytes forwarded by stream-oriented proxy primitives.",
    PROTOCOL_DIRECTION_LABELS,
);

pub const PROXY_FORWARDED_MESSAGES_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_FORWARDED_MESSAGES_TOTAL_NAME,
    MetricKind::Counter,
    Some("messages"),
    "Messages forwarded by message-oriented proxy primitives.",
    PROTOCOL_DIRECTION_LABELS,
);

pub const PROXY_OPERATION_DURATION_SECONDS: MetricDescriptor = MetricDescriptor::new(
    PROXY_OPERATION_DURATION_SECONDS_NAME,
    MetricKind::Histogram,
    Some("seconds"),
    "Duration of bounded proxy primitive operations.",
    PROTOCOL_OPERATION_OUTCOME_LABELS,
);

pub const PROXY_TLS_CLIENT_HELLO_TOTAL: MetricDescriptor = MetricDescriptor::new(
    PROXY_TLS_CLIENT_HELLO_TOTAL_NAME,
    MetricKind::Counter,
    Some("client_hellos"),
    "TLS ClientHello parser outcomes.",
    PROTOCOL_OUTCOME_LABELS,
);

pub const ALL_METRICS: &[MetricDescriptor] = &[
    PROXY_ACTIVE_STREAMS,
    PROXY_ADMISSION_DECISIONS_TOTAL,
    PROXY_DRAIN_EVENTS_TOTAL,
    PROXY_FORWARDED_BYTES_TOTAL,
    PROXY_FORWARDED_MESSAGES_TOTAL,
    PROXY_OPERATION_DURATION_SECONDS,
    PROXY_TLS_CLIENT_HELLO_TOTAL,
];

impl LabelKey {
    pub const ALL_LOW_CARDINALITY: &'static [Self] = &[
        Self::Protocol,
        Self::Direction,
        Self::Operation,
        Self::Outcome,
        Self::State,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Protocol => "protocol",
            Self::Direction => "direction",
            Self::Operation => "operation",
            Self::Outcome => "outcome",
            Self::State => "state",
        }
    }
}

impl MetricKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

impl MetricDescriptor {
    const fn new(
        name: &'static str,
        kind: MetricKind,
        unit: Option<&'static str>,
        description: &'static str,
        label_keys: &'static [LabelKey],
    ) -> Self {
        Self {
            name,
            kind,
            unit,
            description,
            label_keys,
        }
    }

    pub const fn name(self) -> &'static str {
        self.name
    }

    pub const fn kind(self) -> MetricKind {
        self.kind
    }

    pub const fn unit(self) -> Option<&'static str> {
        self.unit
    }

    pub const fn description(self) -> &'static str {
        self.description
    }

    pub const fn label_keys(self) -> &'static [LabelKey] {
        self.label_keys
    }
}

impl MetricLabel {
    const fn new(key: LabelKey, value: &'static str) -> Self {
        Self { key, value }
    }

    pub const fn key(self) -> LabelKey {
        self.key
    }

    pub const fn value(self) -> &'static str {
        self.value
    }
}

impl Protocol {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Protocol, self.as_str())
    }
}

impl TrafficDirection {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Direction, self.as_str())
    }
}

impl Operation {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Operation, self.as_str())
    }
}

impl Outcome {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Outcome, self.as_str())
    }
}

impl ProxyState {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::State, self.as_str())
    }
}

impl TlsClientHelloOutcome {
    pub const fn metric_label(self) -> MetricLabel {
        MetricLabel::new(LabelKey::Outcome, self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        LabelKey, MetricKind, Operation, Outcome, Protocol, ProxyState, TlsClientHelloOutcome,
        TrafficDirection, ALL_METRICS,
    };

    #[test]
    fn metric_descriptors_are_stable() {
        let expected: &[(&str, MetricKind, Option<&str>, &[LabelKey])] = &[
            (
                "sleepypods_proxy_active_streams",
                MetricKind::Gauge,
                Some("streams"),
                &[LabelKey::Protocol],
            ),
            (
                "sleepypods_proxy_admission_decisions_total",
                MetricKind::Counter,
                Some("decisions"),
                &[LabelKey::Protocol, LabelKey::Outcome],
            ),
            (
                "sleepypods_proxy_drain_events_total",
                MetricKind::Counter,
                Some("events"),
                &[LabelKey::State, LabelKey::Outcome],
            ),
            (
                "sleepypods_proxy_forwarded_bytes_total",
                MetricKind::Counter,
                Some("bytes"),
                &[LabelKey::Protocol, LabelKey::Direction],
            ),
            (
                "sleepypods_proxy_forwarded_messages_total",
                MetricKind::Counter,
                Some("messages"),
                &[LabelKey::Protocol, LabelKey::Direction],
            ),
            (
                "sleepypods_proxy_operation_duration_seconds",
                MetricKind::Histogram,
                Some("seconds"),
                &[LabelKey::Protocol, LabelKey::Operation, LabelKey::Outcome],
            ),
            (
                "sleepypods_proxy_tls_client_hello_total",
                MetricKind::Counter,
                Some("client_hellos"),
                &[LabelKey::Protocol, LabelKey::Outcome],
            ),
        ];

        assert_eq!(ALL_METRICS.len(), expected.len());
        for (descriptor, (name, kind, unit, labels)) in ALL_METRICS.iter().zip(expected) {
            assert_eq!(descriptor.name(), *name);
            assert_eq!(descriptor.kind(), *kind);
            assert_eq!(descriptor.unit(), *unit);
            assert_eq!(descriptor.label_keys(), *labels);
            assert!(!descriptor.description().is_empty());
        }
    }

    #[test]
    fn metric_names_are_unique() {
        let names = ALL_METRICS
            .iter()
            .map(|descriptor| descriptor.name())
            .collect::<HashSet<_>>();

        assert_eq!(names.len(), ALL_METRICS.len());
    }

    #[test]
    fn metric_descriptors_only_use_approved_low_cardinality_labels() {
        for descriptor in ALL_METRICS {
            for label_key in descriptor.label_keys() {
                assert!(
                    LabelKey::ALL_LOW_CARDINALITY.contains(label_key),
                    "{} uses unapproved metric label {}",
                    descriptor.name(),
                    label_key.as_str()
                );
            }
        }
    }

    #[test]
    fn metric_labels_do_not_include_request_identity_keys() {
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
            "raw_error",
        ];

        for descriptor in ALL_METRICS {
            for label_key in descriptor.label_keys() {
                for forbidden_fragment in forbidden {
                    assert!(
                        !label_key.as_str().contains(forbidden_fragment),
                        "{} uses high-cardinality metric label {}",
                        descriptor.name(),
                        label_key.as_str()
                    );
                }
            }
        }
    }

    #[test]
    fn typed_metric_labels_use_expected_keys() {
        assert_eq!(Protocol::Http.metric_label().key(), LabelKey::Protocol);
        assert_eq!(
            TrafficDirection::ClientToUpstream.metric_label().key(),
            LabelKey::Direction
        );
        assert_eq!(Operation::Forward.metric_label().key(), LabelKey::Operation);
        assert_eq!(Outcome::Success.metric_label().key(), LabelKey::Outcome);
        assert_eq!(ProxyState::Draining.metric_label().key(), LabelKey::State);
        assert_eq!(
            TlsClientHelloOutcome::Malformed.metric_label().key(),
            LabelKey::Outcome
        );
    }
}
