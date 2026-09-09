//! Exact owned Pod forwarding and bounded, single-connection TLS clients.
use super::*;
use http_body_util::{BodyExt, Empty, Limited};
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::task::JoinSet;
use tokio_rustls::{client::TlsStream, TlsConnector};

static FORWARD_ID: AtomicUsize = AtomicUsize::new(0);
pub(super) struct Peer {
    child: Child,
    log: std::path::PathBuf,
    diagnostic: Option<std::path::PathBuf>,
    pub pod: Pod,
    ports: HashMap<u16, SocketAddr>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        let observed_at = now();
        // Observe before our cleanup sends a signal: a killed child is not
        // evidence that the forward had already failed during the workload.
        let observed = match self.child.try_wait() {
            Ok(Some(status)) => {
                serde_json::json!({"state":"exited","status":status.to_string(),"code":status.code()})
            }
            Ok(None) => serde_json::json!({"state":"running"}),
            Err(error) => serde_json::json!({"state":"unknown","error":error.to_string()}),
        };
        let child_pid = self.child.id();
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(path) = &self.diagnostic {
            let retained = (|| -> TestResult<()> {
                let mut file = fs::File::open(&self.log)?;
                let length = file.metadata()?.len();
                let offset = length.saturating_sub(64 * 1024);
                file.seek(SeekFrom::Start(offset))?;
                let mut tail = Vec::new();
                file.take(64 * 1024).read_to_end(&mut tail)?;
                let record = serde_json::json!({
                    "pod":self.name(),"uid":self.uid(),"child_pid":child_pid,
                    "observed_unix_millis":observed_at,"observed_before_cleanup":observed,"ports":self.ports,
                    "log_total_bytes":length,"log_tail_offset":offset,
                    "log_tail":String::from_utf8_lossy(&tail),
                });
                fs::write(path, serde_json::to_vec_pretty(&record)?)?;
                Ok(())
            })();
            if let Err(error) = retained {
                eprintln!(
                    "failed to retain exact Pod forward diagnostic for {}: {error}",
                    self.name()
                );
            }
        }
        let _ = fs::remove_file(&self.log);
    }
}
impl Peer {
    pub fn addr(&self, remote: u16) -> SocketAddr {
        self.ports[&remote]
    }
    pub fn name(&self) -> &str {
        self.pod.metadata.name.as_deref().unwrap()
    }
    pub fn uid(&self) -> &str {
        self.pod.metadata.uid.as_deref().unwrap()
    }
    pub async fn start(namespace: &str, pod: Pod, ports: &[u16]) -> TestResult<Self> {
        let name = pod.metadata.name.as_deref().ok_or("Pod name missing")?;
        let forward_id = FORWARD_ID.fetch_add(1, Ordering::Relaxed);
        let log = std::env::temp_dir().join(format!(
            "sleepypods-tls-pf-{}-{}",
            std::process::id(),
            forward_id
        ));
        let diagnostic = std::env::var_os("SLEEPYPODS_E2E_ARTIFACT_DIR").map(|directory| {
            std::path::PathBuf::from(directory).join(format!(
                "pod-forward-{name}-{}-{forward_id}.json",
                std::process::id()
            ))
        });
        let output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&log)?;
        let mut command = Command::new("kubectl");
        command.args([
            "--request-timeout=10s",
            "-n",
            namespace,
            "port-forward",
            "--address=127.0.0.1",
            &format!("pod/{name}"),
        ]);
        for port in ports {
            command.arg(format!(":{port}"));
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(output.try_clone()?)
            .stderr(output)
            .spawn()?;
        let mut peer = Self {
            child,
            log,
            diagnostic,
            pod,
            ports: HashMap::new(),
        };
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(status) = peer.child.try_wait()? {
                    return Err(format!(
                        "exact Pod forward exited {status}: {}",
                        fs::read_to_string(&peer.log)?
                    )
                    .into());
                }
                let log = fs::read_to_string(&peer.log)?;
                if log.len() > 16 * 1024 {
                    return Err("Pod forward startup output exceeds bound".into());
                }
                for line in log.lines() {
                    if let Some(pair) = line.strip_prefix("Forwarding from ") {
                        if let Some((local, remote)) = pair.split_once(" -> ") {
                            peer.ports.insert(remote.parse()?, local.parse()?);
                        }
                    }
                }
                if ports.iter().all(|p| peer.ports.contains_key(p)) {
                    return Ok::<_, Box<dyn Error + Send + Sync>>(());
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        Ok(peer)
    }
    pub fn identity(&self) -> serde_json::Value {
        serde_json::json!({"name":self.name(),"uid":self.uid(),"node":self.pod.spec.as_ref().and_then(|s|s.node_name.as_ref()),"containers":self.pod.status.as_ref().and_then(|s|s.container_statuses.as_ref()).map(|s|s.iter().map(|c|serde_json::json!({"name":c.name,"image":c.image,"image_id":c.image_id,"container_id":c.container_id})).collect::<Vec<_>>())})
    }
}
pub(super) async fn peers(
    kube: Client,
    namespace: &str,
    name: &str,
    count: usize,
    ports: &[u16],
) -> TestResult<Vec<Peer>> {
    let api = Api::<Pod>::namespaced(kube, namespace);
    let pods = tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            let mut pods = api
                .list(&ListParams::default().labels(&format!("app.kubernetes.io/name={name}")))
                .await?
                .items;
            pods.retain(|p| {
                p.metadata.deletion_timestamp.is_none()
                    && p.status
                        .as_ref()
                        .and_then(|s| s.conditions.as_ref())
                        .is_some_and(|c| c.iter().any(|v| v.type_ == "Ready" && v.status == "True"))
            });
            if pods.len() == count {
                pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
                return Ok::<_, Box<dyn Error + Send + Sync>>(pods);
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;
    let mut result = Vec::new();
    for pod in pods {
        result.push(Peer::start(namespace, pod, ports).await?);
    }
    Ok(result)
}
pub(super) fn client_config(
    certificates: &[CertificateDer<'static>],
    version: &'static rustls::SupportedProtocolVersion,
    alpn: &[u8],
) -> TestResult<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate.clone())?;
    }
    let mut config = ClientConfig::builder_with_protocol_versions(&[version])
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![alpn.to_vec()];
    config.resumption = rustls::client::Resumption::disabled();
    config.enable_early_data = false;
    Ok(config)
}
pub(super) type Tls = TlsStream<tokio::net::TcpStream>;
pub(super) async fn handshake(
    addr: SocketAddr,
    host: &str,
    config: Arc<ClientConfig>,
) -> TestResult<Tls> {
    tokio::time::timeout(Duration::from_secs(6), async {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(
            TlsConnector::from(config)
                .connect(ServerName::try_from(host.to_owned())?, stream)
                .await?,
        )
    })
    .await?
}
// Refusal proofs distinguish a server TLS rejection from an unavailable test
// path. Connect failures, TLS timeouts, and local certificate-validation errors
// are fatal. A successful handshake is returned as false for bounded convergence.
pub(super) async fn tls_refused(
    addr: SocketAddr,
    host: &str,
    config: Arc<ClientConfig>,
) -> TestResult<bool> {
    let stream = tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(addr))
        .await??;
    match tokio::time::timeout(
        Duration::from_secs(6),
        TlsConnector::from(config).connect(ServerName::try_from(host.to_owned())?, stream),
    )
    .await?
    {
        Ok(_) => Ok(false),
        Err(error) if is_tls_refusal(&error) => Ok(true),
        Err(error) => Err(error.into()),
    }
}
pub(super) fn is_tls_refusal(error: &std::io::Error) -> bool {
    if let Some(tls) = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<rustls::Error>())
    {
        return matches!(tls, rustls::Error::AlertReceived(_));
    }
    matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
    )
}

pub(super) async fn live_control(peer: &Peer) -> TestResult<()> {
    let response = http_once::get_once(
        peer.addr(9090),
        "localhost",
        "/metrics",
        Duration::from_secs(3),
    )
    .await?;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert!(response
        .body()
        .contains("sleepypods_runtime_certificate_entries"));
    Ok(())
}
pub(super) fn assert_peer(
    stream: &Tls,
    certificate: &CertificateDer<'_>,
    version: &'static rustls::SupportedProtocolVersion,
    alpn: &[u8],
) -> TestResult<()> {
    let connection = stream.get_ref().1;
    if connection.peer_certificates().and_then(|c| c.first()) != Some(certificate) {
        return Err("verified peer fingerprint differs from expected publication".into());
    }
    assert_eq!(connection.protocol_version(), Some(version.version));
    assert_eq!(connection.alpn_protocol(), Some(alpn));
    assert_eq!(
        connection.handshake_kind(),
        Some(rustls::HandshakeKind::Full)
    );
    Ok(())
}
pub(super) async fn http1(
    stream: Tls,
    host: &str,
    path: &str,
    budget: Duration,
) -> TestResult<String> {
    let mut tasks = JoinSet::new();
    let result = tokio::time::timeout(budget, async {
        let (mut sender, driver) = hyper::client::conn::http1::Builder::new()
            .max_buf_size(16 * 1024)
            .handshake(hyper_util::rt::TokioIo::new(stream))
            .await?;
        tasks.spawn(driver);
        let request = http::Request::builder()
            .uri(path)
            .header("host", host)
            .header("connection", "close")
            .body(Empty::<bytes::Bytes>::new())?;
        let response = sender.send_request(request).await?;
        let header_bytes: usize = response
            .headers()
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len() + 4)
            .sum();
        if header_bytes > 16 * 1024 {
            return Err("TLS HTTP fixture headers exceed16KiB".into());
        }
        assert_eq!(response.status(), http::StatusCode::OK);
        Ok::<_, Box<dyn Error + Send + Sync>>(String::from_utf8(
            Limited::new(response.into_body(), 64 * 1024)
                .collect()
                .await?
                .to_bytes()
                .to_vec(),
        )?)
    })
    .await;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result?
}
pub(super) struct H2 {
    sender: hyper::client::conn::http2::SendRequest<Empty<bytes::Bytes>>,
    tasks: JoinSet<Result<(), hyper::Error>>,
}
impl H2 {
    pub async fn connect<IO>(stream: IO) -> TestResult<Self>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (sender, driver) =
            hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .max_header_list_size(16 * 1024)
                .handshake(hyper_util::rt::TokioIo::new(stream))
                .await?;
        let mut tasks = JoinSet::new();
        tasks.spawn(driver);
        Ok(Self { sender, tasks })
    }
    pub async fn get(&mut self, host: &str, path: &str) -> TestResult<String> {
        tokio::time::timeout(Duration::from_secs(140), async {
            let response = self
                .sender
                .send_request(
                    http::Request::builder()
                        .uri(format!("https://{host}{path}"))
                        .body(Empty::<bytes::Bytes>::new())?,
                )
                .await?;
            let status = response.status();
            let version = response.version();
            let body = String::from_utf8(
                Limited::new(response.into_body(), 64 * 1024)
                    .collect()
                    .await?
                    .to_bytes()
                    .to_vec(),
            )?;
            if status != http::StatusCode::OK || version != http::Version::HTTP_2 {
                return Err(format!(
                    "H2 fixture response for {path}: status={status}, version={version:?}, body={body:?}"
                )
                .into());
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(body)
        })
        .await?
    }
    pub async fn close(mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}
pub(super) async fn converged(
    peers: &[Peer],
    host: &str,
    roots: &[CertificateDer<'static>],
    expected: &CertificateDer<'static>,
) -> TestResult<()> {
    let started = Instant::now();
    for peer in peers {
        tokio::time::timeout_at(started + Duration::from_secs(10), async {
            loop {
                if let Ok(stream) = handshake(
                    peer.addr(8443),
                    host,
                    Arc::new(client_config(roots, &rustls::version::TLS13, b"h2")?),
                )
                .await
                {
                    if stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(|c| c.first())
                        == Some(expected)
                    {
                        assert_peer(&stream, expected, &rustls::version::TLS13, b"h2")?;
                        break;
                    }
                }
                sleep(Duration::from_millis(50)).await;
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .await??;
        println!(
            "DYNAMIC_PEER pod={} uid={} fingerprint={} convergence_ms={}",
            peer.name(),
            peer.uid(),
            fingerprint(expected),
            started.elapsed().as_millis()
        );
    }
    Ok(())
}
pub(super) fn fingerprint(certificate: &CertificateDer<'_>) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(certificate)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::TlsAcceptor;

    #[test]
    fn forward_diagnostic_preserves_pre_cleanup_status_and_bounded_tail() -> TestResult<()> {
        for exited in [true, false] {
            let number = FORWARD_ID.fetch_add(1, Ordering::Relaxed);
            let stem = format!("sleepypods-forward-proof-{}-{number}", std::process::id());
            let log = std::env::temp_dir().join(&stem);
            let diagnostic = log.with_extension("json");
            let mut bytes = vec![b'x'; 128 * 1024];
            bytes.extend_from_slice(b"final-forward-diagnostic");
            fs::write(&log, &bytes)?;
            let mut child = if exited {
                Command::new("sh").args(["-c", "exit 17"]).spawn()?
            } else {
                Command::new("sleep").arg("30").spawn()?
            };
            if exited {
                assert_eq!(child.wait()?.code(), Some(17));
            }
            let peer = Peer {
                child,
                log: log.clone(),
                diagnostic: Some(diagnostic.clone()),
                pod: serde_json::from_value(
                    serde_json::json!({"metadata":{"name":"exact-peer","uid":"exact-uid"}}),
                )?,
                ports: HashMap::from([(8443, "127.0.0.1:23456".parse()?)]),
            };
            drop(peer);
            let record: serde_json::Value = serde_json::from_slice(&fs::read(&diagnostic)?)?;
            fs::remove_file(diagnostic)?;
            assert!(!log.exists());
            assert_eq!(record["pod"], "exact-peer");
            assert_eq!(record["uid"], "exact-uid");
            assert_eq!(record["ports"]["8443"], "127.0.0.1:23456");
            assert_eq!(record["log_total_bytes"], bytes.len());
            assert_eq!(record["log_tail"].as_str().unwrap().len(), 64 * 1024);
            assert!(record["log_tail"]
                .as_str()
                .unwrap()
                .ends_with("final-forward-diagnostic"));
            if exited {
                assert_eq!(record["observed_before_cleanup"]["state"], "exited");
                assert_eq!(record["observed_before_cleanup"]["code"], 17);
            } else {
                assert_eq!(record["observed_before_cleanup"]["state"], "running");
            }
        }
        Ok(())
    }

    async fn fixture() -> TestResult<(
        tokio::net::TcpListener,
        TlsAcceptor,
        CertificateDer<'static>,
    )> {
        control_plane::install_rustls_crypto_provider();
        let certificate = rcgen::generate_simple_self_signed(vec!["fixture.test".into()])?;
        let mut server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certificate.signing_key.serialize_der(),
                )
                .into(),
            )?;
        server.alpn_protocols = vec![b"http/1.1".to_vec(), b"h2".to_vec()];
        Ok((
            tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            TlsAcceptor::from(Arc::new(server)),
            certificate.cert.der().clone(),
        ))
    }
    async fn request(
        stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    ) -> TestResult<()> {
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(stream.read_u8().await?);
            assert!(headers.len() < 16 * 1024);
        }
        Ok(())
    }
    #[tokio::test]
    async fn tls_content_length_completes_before_eof_and_joins_socket() -> TestResult<()> {
        let (listener, acceptor, der) = fixture().await?;
        let addr = listener.local_addr()?;
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut stream = acceptor.accept(listener.accept().await?.0).await?;
            request(&mut stream).await?;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await?;
            stream.flush().await?;
            // The server never sends EOF. The client must finish its complete
            // body, then close its owned connection instead of waiting for EOF.
            let end = tokio::time::timeout(Duration::from_secs(2), stream.read_u8()).await?;
            assert!(end.is_err());
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        });
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let stream = handshake(
                addr,
                "fixture.test",
                Arc::new(client_config(
                    std::slice::from_ref(&der),
                    &rustls::version::TLS13,
                    b"http/1.1",
                )?),
            )
            .await?;
            assert_peer(&stream, &der, &rustls::version::TLS13, b"http/1.1")?;
            assert_eq!(
                http1(stream, "fixture.test", "/", Duration::from_secs(1)).await?,
                "ok"
            );
            tasks.join_next().await.ok_or("server task missing")???;
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        })
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result?
    }
    #[tokio::test]
    async fn tls_response_limit_and_external_cancellation_release_owned_socket() -> TestResult<()> {
        for cancel in [false, true] {
            let (listener, acceptor, der) = fixture().await?;
            let addr = listener.local_addr()?;
            let mut tasks = JoinSet::new();
            let (entered, request_seen) = tokio::sync::oneshot::channel();
            tasks.spawn(async move {
                let mut stream = acceptor.accept(listener.accept().await?.0).await?;
                request(&mut stream).await?;
                let _ = entered.send(());
                if !cancel {
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 65537\r\n\r\n")
                        .await?;
                    stream.write_all(&vec![b'x'; 65537]).await?;
                    stream.flush().await?;
                }
                let closed = tokio::time::timeout(Duration::from_secs(2), stream.read_u8()).await?;
                assert!(closed.is_err());
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            });
            let result = tokio::time::timeout(Duration::from_secs(5), async {
                let stream = handshake(
                    addr,
                    "fixture.test",
                    Arc::new(client_config(&[der], &rustls::version::TLS13, b"http/1.1")?),
                )
                .await?;
                let mut client = JoinSet::new();
                client.spawn(async move {
                    http1(stream, "fixture.test", "/", Duration::from_secs(2)).await
                });
                request_seen.await?;
                if cancel {
                    client.abort_all();
                    while client.join_next().await.is_some() {}
                } else {
                    assert!(client.join_next().await.ok_or("client missing")??.is_err());
                }
                tasks.join_next().await.ok_or("server missing")???;
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            })
            .await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            result??;
        }
        Ok(())
    }
    #[tokio::test]
    async fn refusal_requires_tls_close_not_connect_failure_timeout_or_bad_certificate(
    ) -> TestResult<()> {
        // Keep the non-listening address owned; a different listener cannot
        // turn the negative connect case into an unrelated TLS response.
        let reservation = tokio::net::TcpSocket::new_v4()?;
        reservation.bind("127.0.0.1:0".parse()?)?;
        let config = Arc::new(client_config(&[], &rustls::version::TLS13, b"http/1.1")?);
        assert!(
            tls_refused(reservation.local_addr()?, "fixture.test", config)
                .await
                .is_err()
        );
        for case in ["close", "timeout", "bad-name", "success"] {
            let (listener, acceptor, der) = fixture().await?;
            let addr = listener.local_addr()?;
            let config = Arc::new(client_config(&[der], &rustls::version::TLS13, b"http/1.1")?);
            let mut tasks = JoinSet::new();
            tasks.spawn(async move {
                let socket = listener.accept().await?.0;
                match case {
                    "close" => drop(socket),
                    "timeout" => {
                        let _owned = socket;
                        std::future::pending::<()>().await;
                    }
                    "bad-name" => {
                        assert!(acceptor.accept(socket).await.is_err());
                    }
                    "success" => {
                        let _stream = acceptor.accept(socket).await?;
                    }
                    _ => unreachable!(),
                }
                Ok::<_, Box<dyn Error + Send + Sync>>(())
            });
            let host = if case == "bad-name" {
                "wrong.test"
            } else {
                "fixture.test"
            };
            let result = tls_refused(addr, host, config).await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            match case {
                "close" => assert!(result?),
                "success" => assert!(!result?),
                "timeout" => assert!(result
                    .unwrap_err()
                    .downcast_ref::<tokio::time::error::Elapsed>()
                    .is_some()),
                "bad-name" => assert!(result.is_err()),
                _ => unreachable!(),
            }
        }
        Ok(())
    }
}
