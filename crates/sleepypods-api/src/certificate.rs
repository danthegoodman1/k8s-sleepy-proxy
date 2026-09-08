//! Certificate resources are independent of routes and application lifecycle.
//! V1 accepts leaf-first DER chains and an unencrypted PKCS#8 DER private key.
use std::{error::Error, fmt, net::IpAddr};
use zeroize::Zeroizing;

pub const MAX_CERTIFICATE_BUNDLE_BYTES: usize = 128 * 1024;
pub const MAX_CERTIFICATE_CHAIN_ENTRIES: usize = 16;
pub const MAX_CERTIFICATE_SANS: usize = 100;
pub const MAX_CERTIFICATE_BINDINGS: usize = 1024;
pub const MAX_TLS_CHANGE_BATCH: u32 = 1024;

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
/// zeroized on drop. Metadata, bindings and change records never contain this type.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsCertificateChangeKind {
    Published,
    Bound,
    Unbound,
    Removed,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsCertificateChange {
    pub revision: CertificateRevision,
    pub hostname: Option<TlsHostname>,
    pub certificate_id: Option<CertificateId>,
    pub certificate_version: Option<CertificateRevision>,
    pub kind: TlsCertificateChangeKind,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableTlsCertificateChanges {
    pub cursor: CertificateRevision,
    pub reset: bool,
    pub events: Vec<TlsCertificateChange>,
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
