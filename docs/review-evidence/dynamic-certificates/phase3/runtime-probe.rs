use sleepypods_api::pb::{self, operator_control_plane_client::OperatorControlPlaneClient};
use std::{
    error::Error,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tonic::{
    Request,
    transport::{Certificate, ClientTlsConfig, Endpoint},
};

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn request<T>(value: T) -> Request<T> {
    let mut request = Request::new(value);
    request.metadata_mut().insert(
        "authorization",
        "Bearer runtime-operator-token".parse().unwrap(),
    );
    request
}

#[tokio::main]
async fn main() -> Result {
    let args: Vec<_> = std::env::args().collect();
    let directory = PathBuf::from(&args[2]);
    let channel = Endpoint::from_shared(args[1].clone())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(fs::read(directory.join("ca.pem"))?))
                .domain_name("control-plane.example.test"),
        )?
        .connect()
        .await?;
    let mut operator = OperatorControlPlaneClient::new(channel);
    match args[3].as_str() {
        "http01" => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
            operator
                .put_http01_challenge(request(pb::PutHttp01ChallengeRequest {
                    key: Some(pb::Http01ChallengeKey {
                        host: "runtime.example.test".into(),
                        token: "runtime-probe-token".into(),
                    }),
                    key_authorization: "runtime-probe-token.authorization".into(),
                    expires_at_unix_millis: now + 120_000,
                }))
                .await?;
            println!("PASS HTTP-01 publication");
        }
        "publish" => {
            let metadata = operator
                .publish_certificate(request(pb::PublishCertificateRequest {
                    certificate_id: "runtime-probe-certificate".into(),
                    expected_version: Some(0),
                    bundle: Some(pb::CertificateBundle {
                        chain_der: vec![fs::read(directory.join("app-cert.der"))?],
                        private_key_pkcs8_der: fs::read(directory.join("app-key.der"))?,
                    }),
                }))
                .await?
                .into_inner();
            assert_eq!(metadata.version, 1);
            let binding = operator
                .set_tls_binding(request(pb::SetTlsBindingRequest {
                    hostname: "runtime.example.test".into(),
                    expected_revision: Some(0),
                    certificate_id: Some(metadata.certificate_id),
                }))
                .await?
                .into_inner();
            assert!(binding.revision > 0);
            println!(
                "PASS publication/binding; fingerprint={}",
                metadata
                    .leaf_sha256
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
        }
        "remove" => {
            let metadata = operator
                .remove_certificate(request(pb::RemoveCertificateRequest {
                    certificate_id: "runtime-probe-certificate".into(),
                    expected_version: Some(1),
                }))
                .await?
                .into_inner();
            assert!(metadata.deleted);
            assert_eq!(metadata.version, 2);
            println!("PASS certificate removal");
        }
        _ => return Err("expected http01, publish, or remove mode".into()),
    }
    Ok(())
}
