//! Matched TLS delivery benchmark: a warm SNI, TLS 1.3, P-256, no resumption,
//! one connection at a time over a 64 KiB in-memory transport. The one-byte
//! response completes the handshake on both peers. Setup is outside timing.
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use criterion::{criterion_group, criterion_main, Criterion};
use frontline::{FrontlineTlsAdapter, TlsCertificateStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{
    rustls::{
        self, client::Resumption, pki_types::ServerName, ClientConfig, HandshakeKind, RootCertStore,
    },
    TlsConnector,
};

struct BenchmarkResolver {
    reply: sleepypods_api::pb::ResolveTlsCertificateResponse,
    fetches: Arc<AtomicUsize>,
}
impl frontline::certificates::CertificateResolver for BenchmarkResolver {
    fn resolve(
        &self,
        _: sleepypods_api::pb::ResolveTlsCertificateRequest,
    ) -> frontline::certificates::CertificateResolveFuture {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let reply = self.reply.clone();
        Box::pin(async move { Ok(reply) })
    }
}

fn warm_tls_handshake(c: &mut Criterion) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let certificate = rcgen::generate_simple_self_signed(vec!["bench.example.test".into()])
        .expect("P-256 certificate");
    let certificate_der = certificate.cert.der().clone();
    let bundle = sleepypods_api::CertificateBundle::new(
        vec![certificate_der.to_vec()],
        certificate.signing_key.serialize_der(),
    )
    .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let m = sleepypods_certificate::validate_certificate(&bundle, now).unwrap();
    let fetches = Arc::new(AtomicUsize::new(0));
    let reply = sleepypods_api::pb::ResolveTlsCertificateResponse {
        server_name: "bench.example.test".into(),
        view_revision: 1,
        observed_at_unix_millis: now,
        authorization_ttl_millis: 300_000,
        value: Some(
            sleepypods_api::pb::resolve_tls_certificate_response::Value::Found(
                sleepypods_api::pb::FoundTlsCertificate {
                    metadata: Some(sleepypods_api::pb::CertificateMetadata {
                        certificate_id: "bench".into(),
                        version: 1,
                        deleted: false,
                        not_before_unix_millis: m.not_before_unix_millis,
                        not_after_unix_millis: m.not_after_unix_millis,
                        dns_names: m.dns_names,
                        leaf_sha256: m.leaf_sha256,
                        sealing_key_id: Some("fixture".into()),
                        sealing_revision: 1,
                    }),
                    bundle: Some(sleepypods_api::pb::CertificateBundle {
                        chain_der: bundle.chain_der().to_vec(),
                        private_key_pkcs8_der: bundle.private_key_pkcs8_der().to_vec(),
                    }),
                },
            ),
        ),
    };
    let (store, worker) = TlsCertificateStore::new(Arc::new(BenchmarkResolver {
        reply,
        fetches: fetches.clone(),
    }));
    let shutdown = proxy_core::Shutdown::new();
    let worker = runtime.spawn(worker.run(shutdown.clone()));
    runtime
        .block_on(store.resolve("bench.example.test"))
        .unwrap()
        .unwrap();
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    let adapter = FrontlineTlsAdapter::new(store);
    let mut roots = RootCertStore::empty();
    roots.add(certificate_der.clone()).expect("trust fixture");
    let mut client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.resumption = Resumption::disabled();
    client.alpn_protocols = vec![b"h2".to_vec()];
    let connector = TlsConnector::from(Arc::new(client));

    c.bench_function("tls_handshake/warm_sni_tls13_full", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let (client_io, server_io) = tokio::io::duplex(64 * 1024);
                let server = async {
                    let mut terminated = adapter.terminate(server_io).await.expect("server TLS");
                    terminated.stream.write_all(&[42]).await.expect("response");
                    terminated.stream.flush().await.expect("flush response");
                };
                let client = async {
                    let mut stream = connector
                        .connect(
                            ServerName::try_from("bench.example.test").unwrap(),
                            client_io,
                        )
                        .await
                        .expect("verified client TLS");
                    assert_eq!(stream.read_u8().await.expect("response byte"), 42);
                    let connection = stream.get_ref().1;
                    assert_eq!(connection.handshake_kind(), Some(HandshakeKind::Full));
                    assert_eq!(connection.alpn_protocol(), Some(b"h2".as_slice()));
                    assert_eq!(connection.peer_certificates().unwrap()[0], certificate_der);
                };
                tokio::join!(server, client);
            });
        });
    });
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "all measured warm handshakes must use zero additional RPCs"
    );
    shutdown.shutdown();
    runtime.block_on(worker).unwrap();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .sample_size(100);
    targets = warm_tls_handshake
}
criterion_main!(benches);
