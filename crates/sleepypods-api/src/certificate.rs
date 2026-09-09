//! Certificate resources are independent of routes and application lifecycle.
//! V1 accepts leaf-first DER chains and an unencrypted PKCS#8 DER private key.
use std::{error::Error, fmt, net::IpAddr};
use zeroize::Zeroizing;

pub const MAX_CERTIFICATE_BUNDLE_BYTES: usize = 128 * 1024;
pub const MAX_CERTIFICATE_CHAIN_ENTRIES: usize = 16;
pub const MAX_CERTIFICATE_SANS: usize = 100;
pub const MAX_CERTIFICATE_BINDINGS: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidCertificateResource(pub &'static str);
impl fmt::Display for InvalidCertificateResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl Error for InvalidCertificateResource {}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CertificateId(String);
impl CertificateId {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidCertificateResource> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(InvalidCertificateResource("certificate ID must contain 1–128 ASCII letters, digits, dots, underscores or hyphens"));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for CertificateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Canonical exact ASCII DNS name. IDNs must already use their DNS A-label form;
/// this API does not convert Unicode names. A final DNS root dot is normalized.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TlsHostname(String);
impl TlsHostname {
    pub fn new(value: impl AsRef<str>) -> Result<Self, InvalidCertificateResource> {
        let value = value.as_ref().strip_suffix('.').unwrap_or(value.as_ref());
        if value.is_empty()
            || value.len() > 253
            || value.parse::<IpAddr>().is_ok()
            || value
                .rsplit('.')
                .next()
                .is_some_and(|label| label.bytes().all(|b| b.is_ascii_digit()))
            || !value.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && label.as_bytes()[0].is_ascii_alphanumeric()
                    && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
        {
            return Err(InvalidCertificateResource("TLS binding must be an exact ASCII DNS hostname without an IP address, port, URL or wildcard"));
        }
        Ok(Self(value.to_ascii_lowercase()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for TlsHostname {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated numeric fence; zero denotes a resource that has never existed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CertificateRevision(u64);
impl CertificateRevision {
    pub const ZERO: Self = Self(0);
    pub fn new(value: u64) -> Result<Self, InvalidCertificateResource> {
        if value > i64::MAX as u64 {
            return Err(InvalidCertificateResource(
                "certificate revision exceeds its durable range",
            ));
        }
        Ok(Self(value))
    }
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Secret-bearing payload. Debug never prints material; owned key bytes are
/// zeroized on drop. Metadata, bindings and snapshots never contain this type.
#[derive(Clone)]
pub struct CertificateBundle {
    chain_der: Vec<Vec<u8>>,
    private_key_pkcs8_der: Zeroizing<Vec<u8>>,
}
impl CertificateBundle {
    pub fn new(
        chain_der: Vec<Vec<u8>>,
        private_key_pkcs8_der: Vec<u8>,
    ) -> Result<Self, InvalidCertificateResource> {
        let private_key_pkcs8_der = Zeroizing::new(private_key_pkcs8_der);
        let total = chain_der
            .iter()
            .try_fold(private_key_pkcs8_der.len(), |n, c| n.checked_add(c.len()));
        if chain_der.is_empty()
            || chain_der.len() > MAX_CERTIFICATE_CHAIN_ENTRIES
            || chain_der.iter().any(Vec::is_empty)
            || private_key_pkcs8_der.is_empty()
            || total.is_none_or(|n| n > MAX_CERTIFICATE_BUNDLE_BYTES)
        {
            return Err(InvalidCertificateResource(
                "certificate bundle exceeds its nonempty 128KiB/16-entry bounds",
            ));
        }
        Ok(Self {
            chain_der,
            private_key_pkcs8_der,
        })
    }
    pub fn chain_der(&self) -> &[Vec<u8>] {
        &self.chain_der
    }
    pub fn private_key_pkcs8_der(&self) -> &[u8] {
        &self.private_key_pkcs8_der
    }
}
impl fmt::Debug for CertificateBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertificateBundle")
            .field("chain_entries", &self.chain_der.len())
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CertificateState {
    Active,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CertificateMetadata {
    pub id: CertificateId,
    pub version: CertificateRevision,
    pub state: CertificateState,
    /// Effective usable interval across the entire supplied certificate chain.
    pub not_before_unix_millis: i64,
    pub not_after_unix_millis: i64,
    pub dns_names: Vec<String>,
    pub leaf_sha256: Vec<u8>,
    pub sealing_key_id: Option<String>,
    pub sealing_revision: CertificateRevision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsBinding {
    pub hostname: TlsHostname,
    pub certificate_id: Option<CertificateId>,
    /// View/CAS revision also changes when the referenced certificate changes.
    /// The row and revision survive unbinding; never-bound names use zero.
    pub revision: CertificateRevision,
    pub last_invalidating_revision: CertificateRevision,
}

#[derive(Clone, Debug)]
pub struct PublishCertificateRequest {
    pub id: CertificateId,
    /// Zero creates a never-used ID. Deleted IDs cannot be republished.
    pub expected_version: CertificateRevision,
    pub bundle: CertificateBundle,
}
#[derive(Clone, Debug)]
pub struct RemoveCertificateRequest {
    pub id: CertificateId,
    pub expected_version: CertificateRevision,
}
#[derive(Clone, Debug)]
pub struct SetTlsBindingRequest {
    pub hostname: TlsHostname,
    pub expected_revision: CertificateRevision,
    /// None unbinds, retaining the hostname's revision tombstone.
    pub certificate_id: Option<CertificateId>,
}
#[derive(Clone, Debug)]
pub struct ReencryptCertificateRequest {
    pub id: CertificateId,
    pub expected_version: CertificateRevision,
    pub expected_sealing_revision: CertificateRevision,
}
#[derive(Clone, Debug)]
pub struct ResolveTlsCertificateRequest {
    pub hostname: TlsHostname,
    pub known_view_revision: Option<CertificateRevision>,
}
#[derive(Clone, Debug)]
pub enum TlsCertificateValue {
    Found {
        metadata: CertificateMetadata,
        bundle: CertificateBundle,
    },
    Unchanged {
        metadata: CertificateMetadata,
    },
    Missing,
}
#[derive(Clone, Debug)]
pub struct TlsCertificateResolution {
    pub hostname: TlsHostname,
    pub view_revision: CertificateRevision,
    pub observed_at_unix_millis: i64,
    pub value: TlsCertificateValue,
}

/// One metadata-only MVCC snapshot. The global revision only gates repeated reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsBindingSnapshot {
    pub revision: CertificateRevision,
    pub bindings: Vec<TlsBinding>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_dns_names_are_canonical_and_bounded() {
        assert_eq!(
            TlsHostname::new("WWW.Example.COM.").unwrap().as_str(),
            "www.example.com"
        );
        assert_eq!(
            TlsHostname::new("xn--bcher-kva.example").unwrap().as_str(),
            "xn--bcher-kva.example"
        );
        assert!(TlsHostname::new("123.middle.example").is_ok());
        assert!(TlsHostname::new("a.123.example").is_ok());
        for name in [
            "",
            ".",
            "a..b",
            "*.example.com",
            "https://example.com",
            "example.com:443",
            "127.0.0.1",
            "[::1]",
            "::1",
            "bücher.example",
            " a.example",
            "a_.example",
            "-a.example",
            "a-.example",
            "example.123",
            "123",
            "127.1",
        ] {
            assert!(TlsHostname::new(name).is_err(), "accepted {name:?}");
        }
        assert!(TlsHostname::new(format!("{}.example", "a".repeat(64))).is_err());
        assert!(TlsHostname::new(vec!["a".repeat(63); 4].join(".")).is_err());
    }
    #[test]
    fn material_limits_and_debug_do_not_expose_key_bytes() {
        let bundle = CertificateBundle::new(vec![vec![1]], b"PRIVATE-MARKER".to_vec()).unwrap();
        assert!(!format!("{bundle:?}").contains("PRIVATE-MARKER"));
        assert!(CertificateBundle::new(vec![vec![1]; 17], vec![2]).is_err());
        assert!(
            CertificateBundle::new(vec![vec![1; MAX_CERTIFICATE_BUNDLE_BYTES]], vec![2]).is_err()
        );
        assert!(CertificateBundle::new(vec![], vec![2]).is_err());
        assert!(CertificateRevision::new(i64::MAX as u64 + 1).is_err());
    }
}

#[cfg(test)]
mod watch_decode_tests {
    use crate::pb;
    use prost::Message;
    #[test]
    fn bounded_wire_can_expand_repeated_elements_and_valid_full_interests_fit() {
        let n = (512 * 1024 - 16) / 2;
        let wire = pb::WatchTlsCertificatesResponse {
            registration: 1,
            bindings: vec![pb::TlsBinding::default(); n],
        }
        .encode_to_vec();
        assert!(wire.len() <= 512 * 1024);
        let decoded = pb::WatchTlsCertificatesResponse::decode(wire.as_slice()).unwrap();
        let binding_bytes = decoded.bindings.capacity() * std::mem::size_of::<pb::TlsBinding>();
        // A moving realloc retains the previous half-capacity vector. Include
        // two wire buffers, a full wire-sized payload and 3MiB retained state.
        let watch_envelope = binding_bytes + binding_bytes / 2 + 3 * 512 * 1024 + 3 * 1024 * 1024;
        assert!(binding_bytes > 4 * 1024 * 1024);
        assert!(watch_envelope < 36 * 1024 * 1024);
        let input = pb::WatchTlsCertificatesRequest {
            registration: 1,
            hostnames: vec![String::new(); n],
        }
        .encode_to_vec();
        assert!(input.len() <= 512 * 1024);
        let decoded_input = pb::WatchTlsCertificatesRequest::decode(input.as_slice()).unwrap();
        let request_bytes = decoded_input.hostnames.capacity() * std::mem::size_of::<String>();
        // A moving Vec reallocation can retain its previous half-capacity.
        // Also allow two wire buffers and a full wire-sized string payload;
        // these conservative terms do not all maximize simultaneously.
        let request_envelope = request_bytes + request_bytes / 2 + 3 * 512 * 1024;
        assert!(request_envelope <= 12 * 1024 * 1024);
        let valid = pb::WatchTlsCertificatesRequest {
            registration: u64::MAX,
            hostnames: (0..1024)
                .map(|i| {
                    format!(
                        "{:063}.{}.{}.{}",
                        i,
                        "b".repeat(63),
                        "c".repeat(63),
                        "d".repeat(61)
                    )
                })
                .collect(),
        };
        assert!(valid
            .hostnames
            .iter()
            .all(|i| super::TlsHostname::new(i).is_ok()));
        assert!(valid.encoded_len() > 256 * 1024);
        assert!(valid.encoded_len() <= 512 * 1024);
        // Empty repeated DER and DNS fields likewise allocate before the
        // domain validator rejects them; charge unary decoding separately.
        let unary = pb::ResolveTlsCertificateResponse {
            value: Some(pb::resolve_tls_certificate_response::Value::Found(
                pb::FoundTlsCertificate {
                    metadata: Some(pb::CertificateMetadata {
                        dns_names: vec![String::new(); 65_537],
                        ..Default::default()
                    }),
                    bundle: Some(pb::CertificateBundle {
                        chain_der: vec![Vec::new(); 65_520],
                        private_key_pkcs8_der: Vec::new(),
                    }),
                },
            )),
            ..Default::default()
        }
        .encode_to_vec();
        assert!(unary.len() <= 256 * 1024);
        let decoded_unary = pb::ResolveTlsCertificateResponse::decode(unary.as_slice()).unwrap();
        let Some(pb::resolve_tls_certificate_response::Value::Found(decoded_unary)) =
            decoded_unary.value
        else {
            panic!()
        };
        let unary_bytes = decoded_unary.metadata.unwrap().dns_names.capacity()
            * std::mem::size_of::<String>()
            + decoded_unary.bundle.unwrap().chain_der.capacity() * std::mem::size_of::<Vec<u8>>();
        assert!(unary_bytes > 4 * 1024 * 1024);
        assert!(unary_bytes + 2 * 256 * 1024 + 1024 * 1024 < 8 * 1024 * 1024);
        let valid_response = pb::WatchTlsCertificatesResponse {
            registration: u64::MAX,
            bindings: valid
                .hostnames
                .iter()
                .map(|hostname| pb::TlsBinding {
                    hostname: hostname.clone(),
                    certificate_id: Some("c".repeat(128)),
                    revision: i64::MAX as u64,
                    last_invalidating_revision: i64::MAX as u64,
                })
                .collect(),
        };
        assert!(valid_response.encoded_len() <= 512 * 1024);
        println!("watch_snapshot wire_bytes={} binding_capacity_bytes={} envelope_bytes={} valid_snapshot_bytes={}", wire.len(), binding_bytes, watch_envelope, valid_response.encoded_len());
        // Exercise the corresponding unary Found -> Unchanged replacement.
        // Its two threshold-rounded DNS vectors and the growing vector's old
        // allocation must also fit the distinct 8MiB fetch charge.
        let old = pb::ResolveTlsCertificateResponse {
            value: Some(pb::resolve_tls_certificate_response::Value::Found(
                pb::FoundTlsCertificate {
                    metadata: Some(pb::CertificateMetadata {
                        dns_names: vec![String::new(); 32_769],
                        ..Default::default()
                    }),
                    bundle: Some(pb::CertificateBundle {
                        chain_der: vec![Vec::new(); 32_700],
                        private_key_pkcs8_der: Vec::new(),
                    }),
                },
            )),
            ..Default::default()
        }
        .encode_to_vec();
        let new = pb::ResolveTlsCertificateResponse {
            value: Some(pb::resolve_tls_certificate_response::Value::Unchanged(
                pb::CertificateMetadata {
                    dns_names: vec![String::new(); 65_537],
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
        .encode_to_vec();
        let old_decoded = pb::ResolveTlsCertificateResponse::decode(old.as_slice()).unwrap();
        let Some(pb::resolve_tls_certificate_response::Value::Found(old_decoded)) =
            old_decoded.value
        else {
            panic!()
        };
        let mut combined = old;
        combined.extend_from_slice(&new);
        assert!(combined.len() <= 256 * 1024);
        let new_decoded = pb::ResolveTlsCertificateResponse::decode(combined.as_slice()).unwrap();
        let Some(pb::resolve_tls_certificate_response::Value::Unchanged(new_decoded)) =
            new_decoded.value
        else {
            panic!()
        };
        let old_capacity = old_decoded.metadata.unwrap().dns_names.capacity();
        let old_chain_capacity = old_decoded.bundle.unwrap().chain_der.capacity();
        let new_capacity = new_decoded.dns_names.capacity();
        let unary_peak = (old_capacity + old_chain_capacity + new_capacity + new_capacity / 2)
            * std::mem::size_of::<String>();
        // Decode and validated-material processing are sequential. Malformed
        // Unchanged metadata is rejected before cryptographic validation. Do
        // not add valid-domain crypto scratch to the moving decode allocation.
        let decode_envelope = unary_peak + 2 * 256 * 1024 + 256 * 1024;
        let validation_envelope =
            4 * super::MAX_CERTIFICATE_BUNDLE_BYTES + 2 * 256 * 1024 + 1024 * 1024;
        assert!(decode_envelope.max(validation_envelope) < 8 * 1024 * 1024);
        println!("unary_oneof_replacement wire_bytes={} moving_growth_peak_bytes={} decode_envelope={} valid_domain_validation_envelope={}",combined.len(),unary_peak,decode_envelope,validation_envelope);
        println!("bounded_decode binding_struct={} binding_capacity_bytes={binding_bytes} request_struct={} request_capacity_bytes={request_bytes} request_envelope_bytes={request_envelope} unary_capacity_bytes={unary_bytes} valid_interests_wire_bytes={}",std::mem::size_of::<pb::TlsBinding>(),std::mem::size_of::<String>(),valid.encoded_len());
    }
}
