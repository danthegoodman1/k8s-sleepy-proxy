use std::{
    cmp::min,
    convert::Infallible,
    env,
    error::Error,
    fmt, future, io,
    net::SocketAddr,
    process,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use control_plane::api::pb::{
    self,
    sidecar_control_plane_server::{SidecarControlPlane, SidecarControlPlaneServer},
};
use http::{Request as HttpRequest, Response as HttpResponse, StatusCode};
use http_body_util::Full;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::timeout,
};
use tonic::{transport::Server, Request, Response, Status};

type BoxError = Box<dyn Error + Send + Sync>;

const BACKEND_BODY: &[u8] = b"sidecar-load-smoke-ok\n";
const DEFAULT_TCP_CHUNK_SIZE: usize = 16 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const TCP_STREAM_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), BoxError> {
    let mut args = env::args();
    let _program = args.next();

    match args.next().as_deref() {
        None | Some("server") => run_server(ServerConfig::from_env()?).await,
        Some("client") => run_client(ClientConfig::from_args(args)?).await,
        Some("tcp-client") => run_tcp_client(TcpClientConfig::from_args(args)?).await,
        Some(command) => Err(Box::new(InvalidArgs(format!(
            "unknown command {command:?}; expected server, client, or tcp-client"
        )))),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ServerConfig {
    backend_addr: SocketAddr,
    tcp_backend_addr: Option<SocketAddr>,
    control_plane_addr: SocketAddr,
}

impl ServerConfig {
    fn from_env() -> Result<Self, InvalidArgs> {
        Ok(Self {
            backend_addr: socket_addr_from_env(
                "SLEEPYPODS_LOAD_SMOKE_BACKEND_ADDR",
                "0.0.0.0:18080",
            )?,
            tcp_backend_addr: optional_socket_addr_from_env(
                "SLEEPYPODS_LOAD_SMOKE_TCP_BACKEND_ADDR",
            )?,
            control_plane_addr: socket_addr_from_env(
                "SLEEPYPODS_LOAD_SMOKE_CONTROL_PLANE_ADDR",
                "0.0.0.0:19090",
            )?,
        })
    }
}

fn socket_addr_from_env(
    name: &'static str,
    default: &'static str,
) -> Result<SocketAddr, InvalidArgs> {
    env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .map_err(|error| InvalidArgs(format!("{name} must be a socket address: {error}")))
}

fn optional_socket_addr_from_env(name: &'static str) -> Result<Option<SocketAddr>, InvalidArgs> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|error| InvalidArgs(format!("{name} must be a socket address: {error}"))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(InvalidArgs(format!("{name} could not be read: {error}"))),
    }
}

async fn run_server(config: ServerConfig) -> Result<(), BoxError> {
    let backend_listener = TcpListener::bind(config.backend_addr).await?;
    let backend_addr = backend_listener.local_addr()?;
    let tcp_backend_listener = match config.tcp_backend_addr {
        Some(addr) => {
            let listener = TcpListener::bind(addr).await?;
            let tcp_backend_addr = listener.local_addr()?;
            eprintln!("load-smoke tcp echo backend listening on {tcp_backend_addr}");
            Some(listener)
        }
        None => None,
    };
    eprintln!("load-smoke http backend listening on {backend_addr}");
    eprintln!(
        "load-smoke sidecar control plane listening on {}",
        config.control_plane_addr
    );

    let backend = serve_http_backend(backend_listener);
    let tcp_backend = async move {
        match tcp_backend_listener {
            Some(listener) => serve_tcp_echo_backend(listener).await,
            None => future::pending::<Result<(), BoxError>>().await,
        }
    };
    let control_plane = Server::builder()
        .add_service(SidecarControlPlaneServer::new(FakeSidecarControlPlane))
        .serve(config.control_plane_addr);

    tokio::select! {
        result = backend => result,
        result = tcp_backend => result,
        result = control_plane => result.map_err(|error| Box::new(error) as BoxError),
    }
}

async fn serve_http_backend(listener: TcpListener) -> Result<(), BoxError> {
    loop {
        let (stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            let service =
                service_fn(|request| async move { Ok::<_, Infallible>(backend_response(request)) });

            if let Err(error) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                eprintln!("load-smoke backend connection failed: {error}");
            }
        });
    }
}

async fn serve_tcp_echo_backend(listener: TcpListener) -> Result<(), BoxError> {
    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buffer = vec![0_u8; DEFAULT_TCP_CHUNK_SIZE];

            loop {
                let read = match stream.read(&mut buffer).await {
                    Ok(0) => {
                        if let Err(error) = stream.shutdown().await {
                            eprintln!("load-smoke tcp echo shutdown failed: {error}");
                        }
                        break;
                    }
                    Ok(read) => read,
                    Err(error) => {
                        eprintln!("load-smoke tcp echo read failed: {error}");
                        break;
                    }
                };

                if let Err(error) = stream.write_all(&buffer[..read]).await {
                    eprintln!("load-smoke tcp echo write failed: {error}");
                    break;
                }
            }
        });
    }
}

fn backend_response(_request: HttpRequest<Incoming>) -> HttpResponse<Full<Bytes>> {
    HttpResponse::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain")
        .header("cache-control", "no-store")
        .body(Full::new(Bytes::from_static(BACKEND_BODY)))
        .expect("fixed load-smoke response builds")
}

#[derive(Clone, Debug, Default)]
struct FakeSidecarControlPlane;

#[tonic::async_trait]
impl SidecarControlPlane for FakeSidecarControlPlane {
    async fn report_idle(
        &self,
        request: Request<pb::SidecarReportIdleRequest>,
    ) -> Result<Response<pb::SidecarReportIdleResponse>, Status> {
        let request = request.into_inner();

        Ok(Response::new(pb::SidecarReportIdleResponse {
            outcome: Some(pb::sidecar_report_idle_response::Outcome::Accepted(
                pb::SidecarReportIdleAccepted {
                    instance_id: request.instance_id,
                    instance_generation: request.expected_generation,
                },
            )),
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ClientConfig {
    label: String,
    target: HttpTarget,
    requests: u64,
    concurrency: u64,
}

impl ClientConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, InvalidArgs> {
        let mut label = "smoke".to_owned();
        let mut url = None;
        let mut requests = 100;
        let mut concurrency = 4;
        let mut args = args.peekable();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--label" => label = next_arg(&mut args, "--label")?,
                "--url" => url = Some(next_arg(&mut args, "--url")?),
                "--requests" => {
                    requests =
                        parse_positive_u64("--requests", &next_arg(&mut args, "--requests")?)?
                }
                "--concurrency" => {
                    concurrency =
                        parse_positive_u64("--concurrency", &next_arg(&mut args, "--concurrency")?)?
                }
                _ => {
                    return Err(InvalidArgs(format!(
                        "unknown client argument {arg:?}; expected --label, --url, --requests, or --concurrency"
                    )));
                }
            }
        }

        let url = url.ok_or_else(|| InvalidArgs("--url is required".to_owned()))?;

        Ok(Self {
            label,
            target: HttpTarget::parse(&url)?,
            requests,
            concurrency,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TcpClientConfig {
    label: String,
    target: SocketAddr,
    streams: u64,
    concurrency: u64,
    bytes_per_stream: u64,
    chunk_size: usize,
}

impl TcpClientConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, InvalidArgs> {
        let mut label = "tcp-smoke".to_owned();
        let mut addr = None;
        let mut streams = 4;
        let mut concurrency = 2;
        let mut bytes_per_stream = 4 * 1024 * 1024;
        let mut chunk_size = DEFAULT_TCP_CHUNK_SIZE;
        let mut args = args.peekable();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--label" => label = next_arg(&mut args, "--label")?,
                "--addr" => {
                    let value = next_arg(&mut args, "--addr")?;
                    addr = Some(value.parse::<SocketAddr>().map_err(|error| {
                        InvalidArgs(format!("--addr must be a socket address: {error}"))
                    })?);
                }
                "--streams" => {
                    streams = parse_positive_u64("--streams", &next_arg(&mut args, "--streams")?)?
                }
                "--concurrency" => {
                    concurrency =
                        parse_positive_u64("--concurrency", &next_arg(&mut args, "--concurrency")?)?
                }
                "--bytes-per-stream" => {
                    bytes_per_stream = parse_positive_u64(
                        "--bytes-per-stream",
                        &next_arg(&mut args, "--bytes-per-stream")?,
                    )?
                }
                "--chunk-size" => {
                    chunk_size =
                        parse_positive_usize("--chunk-size", &next_arg(&mut args, "--chunk-size")?)?
                }
                _ => {
                    return Err(InvalidArgs(format!(
                        "unknown tcp-client argument {arg:?}; expected --label, --addr, --streams, --concurrency, --bytes-per-stream, or --chunk-size"
                    )));
                }
            }
        }

        let target = addr.ok_or_else(|| InvalidArgs("--addr is required".to_owned()))?;

        streams
            .checked_mul(bytes_per_stream)
            .ok_or_else(|| InvalidArgs("total TCP byte count overflows u64".to_owned()))?;

        Ok(Self {
            label,
            target,
            streams,
            concurrency,
            bytes_per_stream,
            chunk_size,
        })
    }
}

fn next_arg(
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    name: &'static str,
) -> Result<String, InvalidArgs> {
    args.next()
        .ok_or_else(|| InvalidArgs(format!("{name} requires a value")))
}

fn parse_positive_u64(name: &'static str, value: &str) -> Result<u64, InvalidArgs> {
    let value = value
        .parse::<u64>()
        .map_err(|error| InvalidArgs(format!("{name} must be a positive integer: {error}")))?;

    if value == 0 {
        return Err(InvalidArgs(format!("{name} must be greater than zero")));
    }

    Ok(value)
}

fn parse_positive_usize(name: &'static str, value: &str) -> Result<usize, InvalidArgs> {
    let value = parse_positive_u64(name, value)?;

    usize::try_from(value)
        .map_err(|_| InvalidArgs(format!("{name} is too large for this platform")))
}

async fn run_client(config: ClientConfig) -> Result<(), BoxError> {
    let next_request = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    let start = Instant::now();

    for _ in 0..config.concurrency {
        let next_request = Arc::clone(&next_request);
        let failures = Arc::clone(&failures);
        let target = config.target.clone();
        let requests = config.requests;

        tasks.spawn(async move {
            loop {
                let request_id = next_request.fetch_add(1, Ordering::Relaxed);

                if request_id >= requests {
                    break;
                }

                if let Err(error) = send_smoke_request(&target, request_id).await {
                    failures.fetch_add(1, Ordering::Relaxed);

                    if request_id < 5 {
                        eprintln!("request {request_id} failed: {error}");
                    }
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result?;
    }

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis().max(1);
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let failures = failures.load(Ordering::Relaxed);
    let rps = config.requests as f64 / elapsed_secs;

    println!(
        "{} requests={} failures={} elapsed_ms={} rps={:.1}",
        config.label, config.requests, failures, elapsed_ms, rps
    );

    if failures > 0 {
        return Err(Box::new(ClientFailures { failures }));
    }

    Ok(())
}

async fn run_tcp_client(config: TcpClientConfig) -> Result<(), BoxError> {
    let next_stream = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let completed_bytes = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    let start = Instant::now();
    let expected_bytes = config
        .streams
        .checked_mul(config.bytes_per_stream)
        .ok_or_else(|| InvalidArgs("total TCP byte count overflows u64".to_owned()))?;

    for _ in 0..config.concurrency {
        let next_stream = Arc::clone(&next_stream);
        let failures = Arc::clone(&failures);
        let completed_bytes = Arc::clone(&completed_bytes);
        let target = config.target;
        let streams = config.streams;
        let bytes_per_stream = config.bytes_per_stream;
        let chunk_size = config.chunk_size;

        tasks.spawn(async move {
            loop {
                let stream_id = next_stream.fetch_add(1, Ordering::Relaxed);

                if stream_id >= streams {
                    break;
                }

                match send_tcp_echo_stream(target, stream_id, bytes_per_stream, chunk_size).await {
                    Ok(bytes) => {
                        completed_bytes.fetch_add(bytes, Ordering::Relaxed);
                    }
                    Err(error) => {
                        failures.fetch_add(1, Ordering::Relaxed);

                        if stream_id < 5 {
                            eprintln!("tcp stream {stream_id} failed: {error}");
                        }
                    }
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result?;
    }

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis().max(1);
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let failures = failures.load(Ordering::Relaxed);
    let completed_bytes = completed_bytes.load(Ordering::Relaxed);
    let throughput_mib_s = completed_bytes as f64 / 1024.0 / 1024.0 / elapsed_secs;

    println!(
        "{} streams={} bytes={} expected_bytes={} failures={} elapsed_ms={} throughput_mib_s={:.2}",
        config.label,
        config.streams,
        completed_bytes,
        expected_bytes,
        failures,
        elapsed_ms,
        throughput_mib_s
    );

    if failures > 0 {
        return Err(Box::new(TcpClientFailures { failures }));
    }

    if completed_bytes != expected_bytes {
        return Err(Box::new(TcpByteCountMismatch {
            expected: expected_bytes,
            actual: completed_bytes,
        }));
    }

    Ok(())
}

async fn send_smoke_request(target: &HttpTarget, request_id: u64) -> Result<(), BoxError> {
    let mut stream = timeout(REQUEST_TIMEOUT, TcpStream::connect(target.authority()))
        .await
        .map_err(|_| timeout_error("connect"))??;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nUser-Agent: sidecar-load-smoke\r\n\r\n",
        target.request_path(request_id),
        target.authority()
    );

    timeout(REQUEST_TIMEOUT, stream.write_all(request.as_bytes()))
        .await
        .map_err(|_| timeout_error("write request"))??;

    let mut response = Vec::with_capacity(256);
    timeout(REQUEST_TIMEOUT, stream.read_to_end(&mut response))
        .await
        .map_err(|_| timeout_error("read response"))??;

    validate_response(&response)?;
    Ok(())
}

async fn send_tcp_echo_stream(
    target: SocketAddr,
    stream_id: u64,
    bytes_per_stream: u64,
    chunk_size: usize,
) -> Result<u64, BoxError> {
    let stream = timeout(REQUEST_TIMEOUT, TcpStream::connect(target))
        .await
        .map_err(|_| timeout_error("tcp connect"))??;

    timeout(TCP_STREAM_TIMEOUT, async move {
        let (mut reader, mut writer) = stream.into_split();
        let write = write_tcp_payload(&mut writer, stream_id, bytes_per_stream, chunk_size);
        let read = read_tcp_echo(&mut reader, stream_id, bytes_per_stream, chunk_size);

        tokio::try_join!(write, read)?;
        Ok::<(), BoxError>(())
    })
    .await
    .map_err(|_| timeout_error("tcp echo transfer"))??;

    Ok(bytes_per_stream)
}

async fn write_tcp_payload<W>(
    writer: &mut W,
    stream_id: u64,
    bytes_per_stream: u64,
    chunk_size: usize,
) -> Result<(), BoxError>
where
    W: AsyncWrite + Unpin,
{
    let mut offset = 0;
    let mut buffer = vec![0_u8; chunk_size];

    while offset < bytes_per_stream {
        let len = min(chunk_size as u64, bytes_per_stream - offset) as usize;
        fill_tcp_payload(&mut buffer[..len], stream_id, offset);
        writer.write_all(&buffer[..len]).await?;
        offset += len as u64;
    }

    writer.shutdown().await?;
    Ok(())
}

async fn read_tcp_echo<R>(
    reader: &mut R,
    stream_id: u64,
    bytes_per_stream: u64,
    chunk_size: usize,
) -> Result<(), BoxError>
where
    R: AsyncRead + Unpin,
{
    let mut offset = 0;
    let mut buffer = vec![0_u8; chunk_size];

    while offset < bytes_per_stream {
        let remaining = bytes_per_stream - offset;
        let max_read = min(chunk_size as u64, remaining) as usize;
        let read = reader.read(&mut buffer[..max_read]).await?;

        if read == 0 {
            return Err(Box::new(TcpEchoValidationError::EarlyClose {
                expected: bytes_per_stream,
                actual: offset,
            }));
        }

        validate_tcp_payload(&buffer[..read], stream_id, offset)?;
        offset += read as u64;
    }

    let read = reader.read(&mut buffer[..1]).await?;
    if read != 0 {
        return Err(Box::new(TcpEchoValidationError::ExtraBytes { len: read }));
    }

    Ok(())
}

fn fill_tcp_payload(buffer: &mut [u8], stream_id: u64, start_offset: u64) {
    for (index, byte) in buffer.iter_mut().enumerate() {
        *byte = tcp_payload_byte(stream_id, start_offset + index as u64);
    }
}

fn validate_tcp_payload(
    buffer: &[u8],
    stream_id: u64,
    start_offset: u64,
) -> Result<(), TcpEchoValidationError> {
    for (index, byte) in buffer.iter().copied().enumerate() {
        let offset = start_offset + index as u64;
        let expected = tcp_payload_byte(stream_id, offset);

        if byte != expected {
            return Err(TcpEchoValidationError::Mismatch {
                offset,
                expected,
                actual: byte,
            });
        }
    }

    Ok(())
}

fn tcp_payload_byte(stream_id: u64, offset: u64) -> u8 {
    let mixed = stream_id
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(offset);
    (mixed ^ (mixed >> 17) ^ (mixed >> 32)) as u8
}

fn timeout_error(stage: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, format!("timed out during {stage}"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HttpTarget {
    host: String,
    port: u16,
    path: String,
}

impl HttpTarget {
    fn parse(url: &str) -> Result<Self, InvalidArgs> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| InvalidArgs("only http:// URLs are supported".to_owned()))?;
        let (authority, path) = match rest.find('/') {
            Some(index) => (&rest[..index], &rest[index..]),
            None => (rest, "/"),
        };
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| InvalidArgs("URL must include host and port".to_owned()))?;

        if host.is_empty() {
            return Err(InvalidArgs("URL host is required".to_owned()));
        }

        let port = port
            .parse::<u16>()
            .map_err(|error| InvalidArgs(format!("URL port must be a u16: {error}")))?;

        if port == 0 {
            return Err(InvalidArgs("URL port must be greater than zero".to_owned()));
        }

        Ok(Self {
            host: host.to_owned(),
            port,
            path: path.to_owned(),
        })
    }

    fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn request_path(&self, request_id: u64) -> String {
        let separator = if self.path.contains('?') { '&' } else { '?' };
        format!("{}{}smoke_request={request_id}", self.path, separator)
    }
}

fn validate_response(bytes: &[u8]) -> Result<(), ResponseValidationError> {
    let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err(ResponseValidationError::MissingHeaders);
    };
    let headers = std::str::from_utf8(&bytes[..header_end])
        .map_err(|_| ResponseValidationError::NonUtf8Headers)?;
    let status_line = headers
        .lines()
        .next()
        .ok_or(ResponseValidationError::MissingStatus)?;

    if !status_line.starts_with("HTTP/1.") || !status_line.contains(" 200 ") {
        return Err(ResponseValidationError::UnexpectedStatus(
            status_line.to_owned(),
        ));
    }

    let body = &bytes[header_end + 4..];
    if body != BACKEND_BODY {
        return Err(ResponseValidationError::UnexpectedBody { len: body.len() });
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InvalidArgs(String);

impl fmt::Display for InvalidArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for InvalidArgs {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientFailures {
    failures: u64,
}

impl fmt::Display for ClientFailures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} request(s) failed", self.failures)
    }
}

impl Error for ClientFailures {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TcpClientFailures {
    failures: u64,
}

impl fmt::Display for TcpClientFailures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} TCP stream(s) failed", self.failures)
    }
}

impl Error for TcpClientFailures {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TcpByteCountMismatch {
    expected: u64,
    actual: u64,
}

impl fmt::Display for TcpByteCountMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "completed TCP bytes {} did not match expected {}",
            self.actual, self.expected
        )
    }
}

impl Error for TcpByteCountMismatch {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResponseValidationError {
    MissingHeaders,
    NonUtf8Headers,
    MissingStatus,
    UnexpectedStatus(String),
    UnexpectedBody { len: usize },
}

impl fmt::Display for ResponseValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHeaders => write!(f, "HTTP response headers are missing"),
            Self::NonUtf8Headers => write!(f, "HTTP response headers are not UTF-8"),
            Self::MissingStatus => write!(f, "HTTP response status line is missing"),
            Self::UnexpectedStatus(status) => write!(f, "unexpected HTTP status line {status:?}"),
            Self::UnexpectedBody { len } => {
                write!(f, "unexpected HTTP response body length {len}")
            }
        }
    }
}

impl Error for ResponseValidationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TcpEchoValidationError {
    EarlyClose {
        expected: u64,
        actual: u64,
    },
    ExtraBytes {
        len: usize,
    },
    Mismatch {
        offset: u64,
        expected: u8,
        actual: u8,
    },
}

impl fmt::Display for TcpEchoValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EarlyClose { expected, actual } => write!(
                f,
                "TCP echo closed after {actual} byte(s), expected {expected}"
            ),
            Self::ExtraBytes { len } => {
                write!(f, "TCP echo returned {len} unexpected extra byte(s)")
            }
            Self::Mismatch {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "TCP echo byte mismatch at offset {offset}: expected {expected}, got {actual}"
            ),
        }
    }
}

impl Error for TcpEchoValidationError {}

#[cfg(test)]
mod tests {
    use super::{
        fill_tcp_payload, validate_response, validate_tcp_payload, ClientConfig, HttpTarget,
        TcpClientConfig, TcpEchoValidationError, BACKEND_BODY,
    };

    #[test]
    fn parses_http_target_with_path_and_query() {
        assert_eq!(
            HttpTarget::parse("http://127.0.0.1:18080/smoke?ready=true").expect("target parses"),
            HttpTarget {
                host: "127.0.0.1".to_owned(),
                port: 18080,
                path: "/smoke?ready=true".to_owned(),
            }
        );
    }

    #[test]
    fn request_path_appends_smoke_request_parameter() {
        let target =
            HttpTarget::parse("http://127.0.0.1:18080/smoke?ready=true").expect("target parses");

        assert_eq!(target.request_path(7), "/smoke?ready=true&smoke_request=7");
    }

    #[test]
    fn client_config_rejects_zero_request_count() {
        let error = ClientConfig::from_args(
            ["--url", "http://127.0.0.1:18080/", "--requests", "0"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect_err("zero request count is rejected");

        assert_eq!(error.0, "--requests must be greater than zero");
    }

    #[test]
    fn response_validation_accepts_expected_http_response() {
        let mut response = b"HTTP/1.1 200 OK\r\ncontent-length: 22\r\n\r\n".to_vec();
        response.extend_from_slice(BACKEND_BODY);

        validate_response(&response).expect("response validates");
    }

    #[test]
    fn tcp_client_config_parses_smoke_load_options() {
        let config = TcpClientConfig::from_args(
            [
                "--label",
                "direct",
                "--addr",
                "127.0.0.1:18082",
                "--streams",
                "8",
                "--concurrency",
                "4",
                "--bytes-per-stream",
                "65536",
                "--chunk-size",
                "4096",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("tcp client config parses");

        assert_eq!(config.label, "direct");
        assert_eq!(config.streams, 8);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.bytes_per_stream, 65536);
        assert_eq!(config.chunk_size, 4096);
    }

    #[test]
    fn tcp_client_config_rejects_zero_bytes_per_stream() {
        let error = TcpClientConfig::from_args(
            ["--addr", "127.0.0.1:18082", "--bytes-per-stream", "0"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect_err("zero bytes per stream is rejected");

        assert_eq!(error.0, "--bytes-per-stream must be greater than zero");
    }

    #[test]
    fn tcp_payload_validation_rejects_mismatched_echo_byte() {
        let mut payload = vec![0_u8; 32];
        fill_tcp_payload(&mut payload, 3, 11);
        payload[5] ^= 0xff;

        let error = validate_tcp_payload(&payload, 3, 11).expect_err("mismatch is rejected");

        assert!(matches!(
            error,
            TcpEchoValidationError::Mismatch { offset: 16, .. }
        ));
    }
}
