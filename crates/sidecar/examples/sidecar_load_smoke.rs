use std::{
    convert::Infallible,
    env,
    error::Error,
    fmt, io,
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
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::timeout,
};
use tonic::{transport::Server, Request, Response, Status};

type BoxError = Box<dyn Error + Send + Sync>;

const BACKEND_BODY: &[u8] = b"sidecar-load-smoke-ok\n";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

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
        Some(command) => Err(Box::new(InvalidArgs(format!(
            "unknown command {command:?}; expected server or client"
        )))),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ServerConfig {
    backend_addr: SocketAddr,
    control_plane_addr: SocketAddr,
}

impl ServerConfig {
    fn from_env() -> Result<Self, InvalidArgs> {
        Ok(Self {
            backend_addr: socket_addr_from_env(
                "SLEEPYPODS_LOAD_SMOKE_BACKEND_ADDR",
                "0.0.0.0:18080",
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

async fn run_server(config: ServerConfig) -> Result<(), BoxError> {
    let backend_listener = TcpListener::bind(config.backend_addr).await?;
    let backend_addr = backend_listener.local_addr()?;
    eprintln!("load-smoke backend listening on {backend_addr}");
    eprintln!(
        "load-smoke sidecar control plane listening on {}",
        config.control_plane_addr
    );

    let backend = serve_backend(backend_listener);
    let control_plane = Server::builder()
        .add_service(SidecarControlPlaneServer::new(FakeSidecarControlPlane))
        .serve(config.control_plane_addr);

    tokio::select! {
        result = backend => result,
        result = control_plane => result.map_err(|error| Box::new(error) as BoxError),
    }
}

async fn serve_backend(listener: TcpListener) -> Result<(), BoxError> {
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

#[cfg(test)]
mod tests {
    use super::{validate_response, ClientConfig, HttpTarget, BACKEND_BODY};

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
}
