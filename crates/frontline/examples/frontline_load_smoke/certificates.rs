//! Native encrypted delivery for the comparative load fixture. Application keys
//! stay in this fake control plane; the production Frontline gets only CA trust.
use super::*;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::{Identity, ServerTlsConfig};

#[derive(Clone, Debug)]
pub(super) struct Delivery {
    host: String,
    metadata: pb::CertificateMetadata,
    bundle: pb::CertificateBundle,
}
impl Delivery {
    pub fn from_env(host: &str) -> Result<Self, BoxError> {
        let chain = rustls_pemfile::certs(
            &mut fs::read(required("SLEEPYPODS_LOAD_SMOKE_APP_CERT_FILE")?)?.as_slice(),
        )
        .map(|cert| cert.map(|v| v.to_vec()))
        .collect::<Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(
            &mut fs::read(required("SLEEPYPODS_LOAD_SMOKE_APP_KEY_FILE")?)?.as_slice(),
        )?
        .ok_or("fixture PKCS8 key missing")?;
        let domain = sleepypods_api::CertificateBundle::new(chain, key.secret_der().to_vec())?;
        Self::new(host, domain)
    }
    fn new(host: &str, domain: sleepypods_api::CertificateBundle) -> Result<Self, BoxError> {
        let validated = sleepypods_certificate::validate_certificate(&domain, now())?;
        sleepypods_certificate::validate_hostname(
            domain.chain_der(),
            &sleepypods_api::TlsHostname::new(host)?,
        )?;
        Ok(Self {
            host: host.to_owned(),
            metadata: pb::CertificateMetadata {
                certificate_id: "load-fixture".into(),
                version: 1,
                deleted: false,
                not_before_unix_millis: validated.not_before_unix_millis,
                not_after_unix_millis: validated.not_after_unix_millis,
                dns_names: validated
                    .dns_names
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                leaf_sha256: validated.leaf_sha256.to_vec(),
                sealing_key_id: Some("fixture".into()),
                sealing_revision: 1,
            },
            bundle: pb::CertificateBundle {
                chain_der: domain.chain_der().to_vec(),
                private_key_pkcs8_der: domain.private_key_pkcs8_der().to_vec(),
            },
        })
    }
    pub fn resolve(
        &self,
        request: pb::ResolveTlsCertificateRequest,
    ) -> pb::ResolveTlsCertificateResponse {
        use pb::resolve_tls_certificate_response::Value;
        let found = request.server_name == self.host && now() < self.metadata.not_after_unix_millis;
        pb::ResolveTlsCertificateResponse {
            server_name: request.server_name,
            view_revision: if found { 1 } else { 0 },
            observed_at_unix_millis: now(),
            authorization_ttl_millis: if found { 300_000 } else { 1000 },
            value: Some(if !found {
                Value::Missing(pb::MissingTlsCertificate {})
            } else if request.known_view_revision == Some(1) {
                Value::Unchanged(self.metadata.clone())
            } else {
                Value::Found(pb::FoundTlsCertificate {
                    metadata: Some(self.metadata.clone()),
                    bundle: Some(self.bundle.clone()),
                })
            }),
        }
    }
    pub fn watch(
        &self,
        requests: tonic::Streaming<pb::WatchTlsCertificatesRequest>,
    ) -> <FakeProxyControlPlane as ProxyControlPlane>::WatchTlsCertificatesStream {
        let host = self.host.clone();
        Box::pin(futures_util::stream::unfold(
            (requests, host),
            |(mut requests, host)| async move {
                let request = match timeout(Duration::from_secs(60), requests.message()).await {
                    Ok(Ok(Some(request))) => request,
                    Ok(Ok(None)) => return None,
                    Ok(Err(error)) => return Some((Err(error), (requests, host))),
                    Err(_) => return None,
                };
                let response = if request.hostnames.len() > 1024 {
                    Err(Status::resource_exhausted(
                        "fixture watch interests exceeded",
                    ))
                } else {
                    Ok(pb::WatchTlsCertificatesResponse {
                        registration: request.registration,
                        bindings: request
                            .hostnames
                            .into_iter()
                            .map(|hostname| {
                                let found = hostname == host;
                                pb::TlsBinding {
                                    hostname,
                                    revision: u64::from(found),
                                    last_invalidating_revision: u64::from(found),
                                    certificate_id: found.then(|| "load-fixture".into()),
                                }
                            })
                            .collect(),
                    })
                };
                Some((response, (requests, host)))
            },
        ))
    }
}
fn required(name: &'static str) -> Result<String, BoxError> {
    env::var(name).map_err(|_| format!("{name} is required").into())
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
pub(super) fn tls() -> Result<ServerTlsConfig, BoxError> {
    Ok(ServerTlsConfig::new().identity(Identity::from_pem(
        fs::read(required("SLEEPYPODS_LOAD_SMOKE_CP_CERT_FILE")?)?,
        fs::read(required("SLEEPYPODS_LOAD_SMOKE_CP_KEY_FILE")?)?,
    )))
}
pub(super) fn auth() -> Result<impl tonic::service::Interceptor + Clone, BoxError> {
    Ok(interceptor(required(
        "SLEEPYPODS_CONTROL_PLANE_PROXY_TOKEN",
    )?))
}
fn interceptor(token: String) -> impl tonic::service::Interceptor + Clone {
    let expected = format!("Bearer {token}");
    move |request: Request<()>| {
        if request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            == Some(expected.as_str())
        {
            Ok(request)
        } else {
            Err(Status::unauthenticated("proxy credentials required"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn native_fixture_verifies_platform_identity_and_delivers_app_only_through_rpc(
    ) -> Result<(), BoxError> {
        let app = generate_simple_self_signed(vec!["app.example.test".into()])?;
        let platform = generate_simple_self_signed(vec!["localhost".into()])?;
        let delivery = Delivery::new(
            "app.example.test",
            sleepypods_api::CertificateBundle::new(
                vec![app.cert.der().to_vec()],
                app.signing_key.serialize_der(),
            )?,
        )?;
        let stats = SmokeStats::default();
        let mut service = FakeProxyControlPlane::new(
            "app.example.test".into(),
            "/smoke".into(),
            "/cold".into(),
            "http://127.0.0.1:1".into(),
            "http://127.0.0.1:1".into(),
            stats.clone(),
        );
        service.certificates = Some(Arc::new(delivery));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let mut tasks = JoinSet::new();
        let identity =
            Identity::from_pem(platform.cert.pem(), platform.signing_key.serialize_pem());
        tasks.spawn(async move {
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(identity))
                .unwrap()
                .add_service(ProxyControlPlaneServer::with_interceptor(
                    service,
                    interceptor("proxy-only".into()),
                ))
                .serve_with_incoming(
                    tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(listener),
                )
                .await
                .unwrap();
        });
        let result = timeout(Duration::from_secs(5), async {
            let endpoint = sleepypods_api::transport::native_endpoint(
                format!("https://localhost:{}", addr.port()),
                Some(&platform.cert.pem()),
            )?;
            let mut client = pb::proxy_control_plane_client::ProxyControlPlaneClient::new(
                endpoint.connect().await?,
            );
            let request = pb::ResolveTlsCertificateRequest {
                server_name: "app.example.test".into(),
                known_view_revision: None,
            };
            assert_eq!(
                client
                    .resolve_tls_certificate(request.clone())
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::Unauthenticated
            );
            for (host, known) in [
                ("app.example.test", None),
                ("app.example.test", Some(1)),
                ("missing.example.test", None),
            ] {
                let mut request = Request::new(pb::ResolveTlsCertificateRequest {
                    server_name: host.into(),
                    known_view_revision: known,
                });
                request
                    .metadata_mut()
                    .insert("authorization", "Bearer proxy-only".parse()?);
                let value = client.resolve_tls_certificate(request).await?.into_inner();
                match (host, known, value.value.unwrap()) {
                    (
                        "app.example.test",
                        None,
                        pb::resolve_tls_certificate_response::Value::Found(found),
                    ) => {
                        let bundle = found.bundle.unwrap();
                        assert_eq!(bundle.chain_der, vec![app.cert.der().to_vec()]);
                        assert!(bundle.private_key_pkcs8_der == app.signing_key.serialize_der());
                        assert_eq!(found.metadata.unwrap().version, 1);
                    }
                    (
                        "app.example.test",
                        Some(1),
                        pb::resolve_tls_certificate_response::Value::Unchanged(meta),
                    ) => {
                        assert_eq!(meta.version, 1)
                    }
                    (
                        "missing.example.test",
                        None,
                        pb::resolve_tls_certificate_response::Value::Missing(_),
                    ) => {
                        assert_eq!(value.view_revision, 0)
                    }
                    _ => panic!("certificate response variant did not match requested view"),
                }
            }
            let mut request = Request::new(tonic::codegen::tokio_stream::iter([
                pb::WatchTlsCertificatesRequest {
                    registration: 1,
                    hostnames: vec!["app.example.test".into()],
                },
            ]));
            request
                .metadata_mut()
                .insert("authorization", "Bearer proxy-only".parse()?);
            let mut stream = client.watch_tls_certificates(request).await?.into_inner();
            let snapshot = stream.message().await?.ok_or("fixture watch closed")?;
            assert_eq!(snapshot.bindings[0].revision, 1);
            assert_eq!(stats.resolve_certificate_calls.load(Ordering::Relaxed), 3);
            assert_eq!(stats.snapshot().subscribe_route_calls, 0);
            assert_eq!(stats.snapshot().wake_instance_calls, 0);
            Ok::<_, BoxError>(())
        })
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result?
    }
}
