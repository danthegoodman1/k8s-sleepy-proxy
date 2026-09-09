use super::*;
use futures_util::{SinkExt, StreamExt};
use rustls::client::{
    ClientSessionMemoryCache, ClientSessionStore, Tls12ClientSessionValue, Tls13ClientSessionValue,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

pub(super) async fn echo(
    ws: &mut tokio_tungstenite::WebSocketStream<peers::Tls>,
    value: &str,
) -> TestResult<()> {
    timeout(Duration::from_secs(5), async {
        ws.send(tokio_tungstenite::tungstenite::Message::Text(value.into()))
            .await?;
        assert_eq!(
            ws.next().await.ok_or("WebSocket closed")??.into_text()?,
            format!("sleepypods-protocol-app\ninstance=dynamic-b\nprotocol=websocket\npath=/b/ws\ntext={value}\n")
        );
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await?
}
#[derive(Debug)]
struct ObservedSessions {
    inner: ClientSessionMemoryCache,
    offered: AtomicUsize,
}
impl ClientSessionStore for ObservedSessions {
    fn set_kx_hint(&self, n: ServerName<'static>, g: rustls::NamedGroup) {
        self.inner.set_kx_hint(n, g)
    }
    fn kx_hint(&self, n: &ServerName<'_>) -> Option<rustls::NamedGroup> {
        self.inner.kx_hint(n)
    }
    fn set_tls12_session(&self, n: ServerName<'static>, v: Tls12ClientSessionValue) {
        self.inner.set_tls12_session(n, v)
    }
    fn tls12_session(&self, n: &ServerName<'_>) -> Option<Tls12ClientSessionValue> {
        let v = self.inner.tls12_session(n);
        if v.is_some() {
            self.offered.fetch_add(1, Ordering::SeqCst);
        }
        v
    }
    fn remove_tls12_session(&self, n: &ServerName<'static>) {
        self.inner.remove_tls12_session(n)
    }
    fn insert_tls13_ticket(&self, n: ServerName<'static>, v: Tls13ClientSessionValue) {
        self.inner.insert_tls13_ticket(n, v)
    }
    fn take_tls13_ticket(&self, n: &ServerName<'static>) -> Option<Tls13ClientSessionValue> {
        let v = self.inner.take_tls13_ticket(n);
        if v.is_some() {
            self.offered.fetch_add(1, Ordering::SeqCst);
        }
        v
    }
}
pub(super) async fn offered(
    fronts: &[Peer],
    host: &str,
    certificate: &rcgen::CertifiedKey<rcgen::KeyPair>,
    roots: &[CertificateDer<'static>],
    deny: bool,
) -> TestResult<()> {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        for front in fronts {
            let mut seed = rustls::ServerConfig::builder_with_protocol_versions(&[version])
                .with_no_client_auth()
                .with_single_cert(
                    vec![certificate.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(
                        certificate.signing_key.serialize_der(),
                    )
                    .into(),
                )?;
            seed.alpn_protocols = vec![b"h2".to_vec()];
            seed.session_storage = rustls::server::ServerSessionMemoryCache::new(8);
            seed.send_tls13_tickets = 4;
            seed.ticketer = rustls::crypto::ring::Ticketer::new()?;
            let acceptor = TlsAcceptor::from(Arc::new(seed));
            let sessions = Arc::new(ObservedSessions {
                inner: ClientSessionMemoryCache::new(32),
                offered: AtomicUsize::new(0),
            });
            let mut config = client_config(roots, version, b"h2")?;
            config.resumption = rustls::client::Resumption::store(sessions.clone());
            let config = Arc::new(config);
            // Seed an actual prior session/ticket from a test-only server. The
            // token is then offered to the production Pod, never injected there.
            let (client, server) = tokio::io::duplex(64 * 1024);
            timeout(Duration::from_secs(5), async {
                let server = async {
                    let mut stream = acceptor.accept(server).await?;
                    stream.write_all(&[42]).await?;
                    stream.shutdown().await?;
                    Ok::<_, Box<dyn Error + Send + Sync>>(())
                };
                let client = async {
                    let mut stream = tokio_rustls::TlsConnector::from(config.clone())
                        .connect(ServerName::try_from(host.to_owned())?, client)
                        .await?;
                    assert_eq!(stream.read_u8().await?, 42);
                    let mut tail = Vec::new();
                    stream.read_to_end(&mut tail).await?;
                    Ok::<_, Box<dyn Error + Send + Sync>>(())
                };
                tokio::try_join!(server, client)?;
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            })
            .await??;
            let before = sessions.offered.load(Ordering::SeqCst);
            live_control(front).await?;
            let refused = if deny {
                Some(tls_refused(front.addr(8443), host, config.clone()).await?)
            } else {
                None
            };
            let result = if deny {
                None
            } else {
                Some(handshake(front.addr(8443), host, config).await?)
            };
            assert!(
                sessions.offered.load(Ordering::SeqCst) > before,
                "prior session was not offered"
            );
            if deny {
                assert_eq!(
                    refused,
                    Some(true),
                    "removed authorization accepted an offered session"
                );
            } else {
                assert_peer(&result.unwrap(), certificate.cert.der(), version, b"h2")?;
            }
            println!(
                "DYNAMIC_SESSION pod={} uid={} protocol={:?} offered=true denied={deny}",
                front.name(),
                front.uid(),
                version.version
            );
        }
    }
    Ok(())
}

// Only the expiry regression uses this verifier. It pins the exact public leaf
// and verifies handshake signatures while deliberately ignoring certificate time,
// so a client-side expiry failure cannot masquerade as server authorization denial.
#[derive(Debug)]
struct ExactLeafWithoutTime(Vec<CertificateDer<'static>>);
impl rustls::client::danger::ServerCertVerifier for ExactLeafWithoutTime {
    fn verify_server_cert(
        &self,
        end: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if !self.0.iter().any(|expected| expected == end) {
            return Err(rustls::Error::General("unexpected fixture leaf".into()));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            m,
            c,
            d,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            m,
            c,
            d,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
pub(super) async fn expiry(operator: &mut Operator, fronts: &[Peer]) -> TestResult<()> {
    let host = "expiry.dynamic.sleepypods.test";
    let mut params = rcgen::CertificateParams::new(vec![host.into()])?;
    params.not_before = (SystemTime::now() - Duration::from_secs(60)).into();
    params.not_after = (SystemTime::now() + Duration::from_secs(15)).into();
    let signing_key = rcgen::KeyPair::generate()?;
    let cert = params.self_signed(&signing_key)?;
    let certificate = rcgen::CertifiedKey { cert, signing_key };
    let metadata = publish(operator, "dynamic-expiry", 0, &certificate).await?;
    bind(operator, host, 0, Some("dynamic-expiry")).await?;
    converged(
        fronts,
        host,
        &[certificate.cert.der().clone()],
        certificate.cert.der(),
    )
    .await?;
    let mut client = client_config(&[], &rustls::version::TLS13, b"h2")?;
    client
        .dangerous()
        .set_certificate_verifier(Arc::new(ExactLeafWithoutTime(vec![certificate
            .cert
            .der()
            .clone()])));
    let client = Arc::new(client);
    // Prove the special verifier works before expiry on both exact Pods.
    for front in fronts {
        handshake(front.addr(8443), host, client.clone()).await?;
    }
    timeout(Duration::from_secs(20), async {
        while now() < metadata.not_after_unix_millis + 1 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    // A fixed monotonic lease cannot outlive the effective validity interval.
    for front in fronts {
        live_control(front).await?;
        assert!(tls_refused(front.addr(8443), host, client.clone()).await?);
    }
    println!(
        "DYNAMIC_EXPIRY server_denied=true client_time_check=false not_after={}",
        metadata.not_after_unix_millis
    );
    Ok(())
}

// Negative SNI probes must not count the client's name check as a server refusal.
// Trust only these generated public leaves but ignore the requested DNS name.
pub(super) fn refusal_config(roots: &[CertificateDer<'static>]) -> TestResult<Arc<ClientConfig>> {
    let mut config = client_config(roots, &rustls::version::TLS13, b"h2")?;
    config
        .dangerous()
        .set_certificate_verifier(Arc::new(ExactLeafWithoutTime(roots.to_vec())));
    Ok(Arc::new(config))
}
