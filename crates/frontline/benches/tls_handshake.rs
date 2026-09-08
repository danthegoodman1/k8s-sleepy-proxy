//! Matched TLS delivery benchmark: a warm SNI, TLS 1.3, P-256, no resumption,
//! one connection at a time over a 64 KiB in-memory transport. The one-byte
//! response completes the handshake on both peers. Setup is outside timing.
use std::{sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use frontline::{FrontlineTlsAdapter, TlsCertificateStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{
    rustls::{
        self,
        client::Resumption,
        pki_types::{PrivatePkcs8KeyDer, ServerName},
        ClientConfig, HandshakeKind, RootCertStore,
    },
    TlsConnector,
};

fn warm_tls_handshake(c: &mut Criterion) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let certificate = rcgen::generate_simple_self_signed(vec!["bench.example.test".into()])
        .expect("P-256 certificate");
    let certificate_der = certificate.cert.der().clone();
    let store = TlsCertificateStore::new();
    store
        .upsert(
            "bench.example.test",
            vec![certificate_der.clone()],
            PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der()).into(),
        )
        .expect("warm certificate");
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
