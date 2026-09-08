use std::sync::Arc;
use control_plane::{ControlPlaneStore, PostgresStore, PostgresStoreConfig};
use control_plane::certificate::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = PostgresStoreConfig::new(std::env::var("SLEEPYPODS_POSTGRES_URL")?)?;
    let sealer = CertificateSealer::new("restart-test-key", vec![SealingKey::new("restart-test-key", [83; 32])?])?;
    let store = PostgresStore::connect(&config).await?.with_certificate_sealer(Arc::new(sealer));
    let id = CertificateId::new("restart-probe")?;
    let hostname = TlsHostname::new("restart.example.test")?;
    let mode = std::env::args().nth(1).expect("seed or verify");
    if mode == "seed" {
        let cert = rcgen::generate_simple_self_signed(vec![hostname.to_string()])?;
        let metadata = store.publish_certificate(PublishCertificateRequest {
            id: id.clone(), expected_version: CertificateRevision::ZERO,
            bundle: CertificateBundle::new(vec![cert.cert.der().to_vec()], cert.signing_key.serialize_der())?,
        }).await?;
        assert_eq!(metadata.version.get(), 1);
        store.set_tls_binding(SetTlsBindingRequest {
            hostname: hostname.clone(), expected_revision: CertificateRevision::ZERO, certificate_id: Some(id.clone()),
        }).await?;
    } else {
        assert_eq!(mode, "verify");
    }
    let resolved = store.resolve_tls_certificate(ResolveTlsCertificateRequest {
        hostname, known_view_revision: None,
    }).await?;
    let TlsCertificateValue::Found { metadata, bundle } = resolved.value else { panic!("published certificate missing") };
    let validated = validate_certificate(&bundle, resolved.observed_at_unix_millis)?;
    assert_eq!(validated.leaf_sha256, metadata.leaf_sha256);
    assert_eq!(metadata.id, id);
    assert_eq!(metadata.version.get(), 1);
    assert_eq!(resolved.view_revision.get(), 2);
    let fingerprint: String = metadata.leaf_sha256.iter().map(|b| format!("{b:02x}")).collect();
    if mode == "verify" {
        assert_eq!(fingerprint, std::env::args().nth(2).expect("expected fingerprint"));
    }
    println!("certificate_restart_probe mode={mode} fingerprint={fingerprint} version=1 view_revision=2 decrypted_and_validated=true");
    Ok(())
}
