//! Independently provisioned native control-plane transport. Tenant certificate
//! resolution never supplies this trust configuration.
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
pub const CONTROL_PLANE_TLS_CA_PEM_ENV: &str = "SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM";
pub const MAX_CONTROL_PLANE_CA_BYTES: usize = 64 * 1024;

/// HTTPS always verifies the endpoint hostname and certificate chain. An
/// optional private CA augments public roots. Plain HTTP cannot carry CA config.
pub fn native_endpoint(
    uri: String,
    ca_pem: Option<&str>,
) -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = Endpoint::from_shared(uri)?;
    match endpoint.uri().scheme_str() {
        Some("https") => {
            let mut tls = ClientTlsConfig::new().with_webpki_roots();
            if let Some(pem) = ca_pem {
                if pem.is_empty() || pem.len() > MAX_CONTROL_PLANE_CA_BYTES {
                    return Err("invalid control-plane CA size".into());
                }
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            Ok(endpoint.tls_config(tls)?)
        }
        Some("http") if ca_pem.is_none() => Ok(endpoint),
        _ => Err(
            "control-plane endpoint requires http or verified https; CA trust requires https"
                .into(),
        ),
    }
}
