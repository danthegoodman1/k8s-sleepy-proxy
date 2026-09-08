use super::{CertificateBundle, InvalidCertificateResource, TlsHostname, MAX_CERTIFICATE_SANS};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
    sign::CertifiedKey,
};
use std::time::Duration;
use x509_parser::{extensions::GeneralName, parse_x509_certificate, time::ASN1Time};

#[derive(Clone, Debug)]
pub struct ValidatedCertificate {
    pub not_before_unix_millis: i64,
    pub not_after_unix_millis: i64,
    pub dns_names: Vec<String>,
    pub leaf_sha256: Vec<u8>,
}

/// Checks a leaf-first chain at the supplied authoritative time. The terminal
/// certificate is an explicitly supplied private trust anchor, not proof of
/// public trust. Every supplied certificate must be current; the leaf still
/// passes WebPKI server-auth/path validation, SAN validation and key matching.
/// Intermediate ordering/signatures are checked even when WebPKI can build a
/// shorter path. A self-issued terminal certificate must verify its signature.
pub fn validate_certificate(
    bundle: &CertificateBundle,
    now_unix_millis: i64,
) -> Result<ValidatedCertificate, InvalidCertificateResource> {
    let fail = |message| InvalidCertificateResource(message);
    let now =
        u64::try_from(now_unix_millis).map_err(|_| fail("invalid certificate validation time"))?;
    let time = ASN1Time::from_timestamp((now / 1000) as i64)
        .map_err(|_| fail("invalid certificate validation time"))?;
    let parsed = bundle
        .chain_der()
        .iter()
        .map(|der| {
            let (rest, cert) =
                parse_x509_certificate(der).map_err(|_| fail("invalid certificate DER"))?;
            if !rest.is_empty() {
                return Err(fail("trailing certificate DER data"));
            }
            if !cert.validity().is_valid_at(time)
                || now_unix_millis >= cert.validity().not_after.timestamp().saturating_mul(1000)
            {
                return Err(fail("certificate is outside its validity interval"));
            }
            Ok(cert)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for pair in parsed.windows(2) {
        if pair[0].issuer() != pair[1].subject() {
            return Err(fail("certificate chain is not leaf-first issuer order"));
        }
        pair[0]
            .verify_signature(Some(pair[1].public_key()))
            .map_err(|_| fail("invalid certificate chain signature"))?;
    }
    let terminal = parsed
        .last()
        .ok_or_else(|| fail("empty certificate chain"))?;
    if terminal.issuer() == terminal.subject() {
        terminal
            .verify_signature(None)
            .map_err(|_| fail("invalid terminal certificate signature"))?;
    }
    let leaf = &parsed[0];
    let san = leaf
        .subject_alternative_name()
        .map_err(|_| fail("invalid or duplicate subject alternative name extension"))?
        .ok_or_else(|| fail("certificate requires DNS subject alternative names"))?;
    if san.value.general_names.is_empty() || san.value.general_names.len() > MAX_CERTIFICATE_SANS {
        return Err(fail("certificate exceeds its 100-SAN bound"));
    }
    let mut dns_names = Vec::new();
    for name in &san.value.general_names {
        if let GeneralName::DNSName(name) = name {
            // Wildcards may appear in certificates, but bindings remain exact.
            let suffix = name.strip_prefix("*.").unwrap_or(name);
            let canonical = TlsHostname::new(suffix)?;
            if canonical.as_str() != suffix.to_ascii_lowercase() {
                return Err(fail("certificate DNS SAN must not contain a root dot"));
            }
            let canonical = if name.starts_with("*.") {
                format!("*.{canonical}")
            } else {
                canonical.to_string()
            };
            if !dns_names.contains(&canonical) {
                dns_names.push(canonical);
            }
        }
    }
    if dns_names.is_empty() {
        return Err(fail("certificate requires a DNS subject alternative name"));
    }
    let chain: Vec<_> = bundle
        .chain_der()
        .iter()
        .map(|der| CertificateDer::from(der.as_slice()))
        .collect();
    let end = webpki::EndEntityCert::try_from(&chain[0])
        .map_err(|_| fail("invalid server certificate"))?;
    let anchor = webpki::anchor_from_trusted_cert(chain.last().unwrap())
        .map_err(|_| fail("invalid terminal trust anchor"))?;
    let intermediates = if chain.len() > 1 {
        &chain[1..chain.len() - 1]
    } else {
        &[]
    };
    let provider = rustls::crypto::ring::default_provider();
    end.verify_for_usage(
        provider.signature_verification_algorithms.all,
        &[anchor],
        intermediates,
        UnixTime::since_unix_epoch(Duration::from_millis(now)),
        webpki::KeyUsage::server_auth(),
        None,
        None,
    )
    .map_err(|_| fail("certificate chain is not valid for TLS server authentication"))?;
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        bundle.private_key_pkcs8_der().to_vec(),
    ));
    let certified = CertifiedKey::from_der(
        chain.into_iter().map(CertificateDer::into_owned).collect(),
        key,
        &provider,
    )
    .map_err(|_| fail("unsupported private key or certificate/key mismatch"))?;
    certified
        .keys_match()
        .map_err(|_| fail("certificate public key does not match the private key"))?;
    Ok(ValidatedCertificate {
        not_before_unix_millis: parsed
            .iter()
            .map(|c| c.validity().not_before.timestamp())
            .max()
            .unwrap()
            .checked_mul(1000)
            .ok_or_else(|| fail("certificate validity overflows milliseconds"))?,
        not_after_unix_millis: parsed
            .iter()
            .map(|c| c.validity().not_after.timestamp())
            .min()
            .unwrap()
            .checked_mul(1000)
            .ok_or_else(|| fail("certificate validity overflows milliseconds"))?,
        dns_names,
        leaf_sha256: ring::digest::digest(&ring::digest::SHA256, &bundle.chain_der()[0])
            .as_ref()
            .to_vec(),
    })
}

/// Uses the same maintained hostname verifier used by rustls, including its
/// wildcard restrictions. Binding validation never implements matching itself.
pub fn validate_hostname(
    chain: &[Vec<u8>],
    hostname: &TlsHostname,
) -> Result<(), InvalidCertificateResource> {
    let leaf = chain
        .first()
        .ok_or(InvalidCertificateResource("empty certificate chain"))?;
    let leaf = CertificateDer::from(leaf.as_slice());
    let cert = webpki::EndEntityCert::try_from(&leaf)
        .map_err(|_| InvalidCertificateResource("invalid server certificate"))?;
    let name = ServerName::try_from(hostname.as_str())
        .map_err(|_| InvalidCertificateResource("invalid TLS hostname"))?;
    cert.verify_is_valid_for_subject_name(&name).map_err(|_| {
        InvalidCertificateResource("certificate does not cover the exact bound hostname")
    })
}
