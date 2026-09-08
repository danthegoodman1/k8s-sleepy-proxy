use sleepypods_api::pb::{
    self, operator_control_plane_client::OperatorControlPlaneClient,
    proxy_control_plane_client::ProxyControlPlaneClient,
};
use std::{error::Error, fs, path::PathBuf, time::Duration};
use tonic::{
    Request,
    transport::{Certificate, ClientTlsConfig, Endpoint},
};

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn request<T>(value: T, role: &str) -> Request<T> {
    let mut request = Request::new(value);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer runtime-{role}-token").parse().unwrap(),
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
    let mut operator = OperatorControlPlaneClient::new(channel.clone());
    match args[3].as_str() {
        "publish" | "invalid" => {
            let id = args[4].clone();
            let prefix = &args[5];
            let expected: u64 = args[6].parse()?;
            let key_prefix = if args[3] == "invalid" { "a1" } else { prefix };
            let response = operator
                .publish_certificate(request(
                    pb::PublishCertificateRequest {
                        certificate_id: id,
                        expected_version: Some(expected),
                        bundle: Some(pb::CertificateBundle {
                            chain_der: vec![fs::read(directory.join(format!("{prefix}.der")))?],
                            private_key_pkcs8_der: fs::read(
                                directory.join(format!("{key_prefix}-key.der")),
                            )?,
                        }),
                    },
                    "operator",
                ))
                .await;
            if args[3] == "invalid" {
                assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
                println!("PASS invalid publication rejected");
            } else {
                let metadata = response?.into_inner();
                assert_eq!(metadata.version, expected + 1);
                assert!(!metadata.deleted);
                println!("PASS publication version={}", metadata.version);
            }
        }
        "bind" => {
            let host = args[4].clone();
            let previous = operator
                .get_tls_binding(request(
                    pb::GetTlsBindingRequest {
                        hostname: host.clone(),
                    },
                    "operator",
                ))
                .await?
                .into_inner();
            let id = (args[5] != "-").then(|| args[5].clone());
            let binding = operator
                .set_tls_binding(request(
                    pb::SetTlsBindingRequest {
                        hostname: host,
                        expected_revision: Some(previous.revision),
                        certificate_id: id.clone(),
                    },
                    "operator",
                ))
                .await?
                .into_inner();
            assert_eq!(binding.certificate_id, id);
            assert!(binding.revision > previous.revision);
            println!("PASS binding revision={}", binding.revision);
        }
        "remove" => {
            let id = args[4].clone();
            let previous = operator
                .get_certificate_metadata(request(
                    pb::GetCertificateMetadataRequest {
                        certificate_id: id.clone(),
                    },
                    "operator",
                ))
                .await?
                .into_inner();
            let metadata = operator
                .remove_certificate(request(
                    pb::RemoveCertificateRequest {
                        certificate_id: id,
                        expected_version: Some(previous.version),
                    },
                    "operator",
                ))
                .await?
                .into_inner();
            assert!(metadata.deleted);
            assert_eq!(metadata.version, previous.version + 1);
            println!("PASS removal version={}", metadata.version);
        }
        "resolve" => {
            let mut proxy = ProxyControlPlaneClient::new(channel);
            let response = proxy
                .resolve_tls_certificate(request(
                    pb::ResolveTlsCertificateRequest {
                        server_name: args[4].clone(),
                        known_view_revision: None,
                    },
                    "proxy",
                ))
                .await?
                .into_inner();
            let Some(pb::resolve_tls_certificate_response::Value::Found(found)) = response.value
            else {
                return Err("expected Found".into());
            };
            let metadata = found.metadata.ok_or("missing metadata")?;
            assert_eq!(metadata.certificate_id, args[5]);
            assert_eq!(metadata.version, args[6].parse::<u64>()?);
            println!(
                "PASS resolution version={} revision={}",
                metadata.version, response.view_revision
            );
        }
        _ => return Err("expected publish, invalid, bind, remove or resolve".into()),
    }
    Ok(())
}
