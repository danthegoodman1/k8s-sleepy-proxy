use std::{env, error::Error, process, time::Duration};

use control_plane::api::pb::sidecar_control_plane_client::SidecarControlPlaneClient;
use proxy_core::{observability::recorder::ObservabilityRecorder, Shutdown};
use sidecar::{
    runtime::{serve_http_with_idle, serve_tcp_with_idle, SidecarRuntimeConfig},
    GrpcSidecarControlPlaneClient, IdleReportConfig,
};
use sleepypods_types::{Generation, InstanceId};
use tonic::transport::Endpoint;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sidecar runtime failed: {error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    control_plane::install_rustls_crypto_provider();
    let _ = ObservabilityRecorder::install_stderr_global();
    let env = EnvConfig::from_env()?;
    let channel = Endpoint::from_shared(env.control_plane_endpoint.clone())?
        .connect()
        .await?;
    let client = GrpcSidecarControlPlaneClient::new(SidecarControlPlaneClient::new(channel));
    let shutdown = Shutdown::new();
    let _shutdown_task = spawn_shutdown_signal(shutdown.clone())?;

    match env.runtime_mode {
        SidecarRuntimeMode::Http => serve_http_with_idle(env.runtime, client, shutdown).await?,
        SidecarRuntimeMode::Tcp => serve_tcp_with_idle(env.runtime, client, shutdown).await?,
    }

    Ok(())
}

#[cfg(unix)]
fn spawn_shutdown_signal(
    shutdown: Shutdown,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error + Send + Sync>> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    Ok(tokio::spawn(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if result.is_ok() {
                    shutdown.shutdown();
                }
            },
            _ = terminate.recv() => {
                shutdown.shutdown();
            }
        }
    }))
}

#[cfg(not(unix))]
fn spawn_shutdown_signal(
    shutdown: Shutdown,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error + Send + Sync>> {
    Ok(tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            shutdown.shutdown();
        }
    }))
}

#[derive(Debug)]
struct EnvConfig {
    runtime: SidecarRuntimeConfig,
    runtime_mode: SidecarRuntimeMode,
    control_plane_endpoint: String,
}

impl EnvConfig {
    fn from_env() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let listen_addr = required_env("SLEEPYPODS_SIDECAR_LISTEN_ADDR")?.parse()?;
        let app_port = required_env("SLEEPYPODS_APP_PORT")?.parse()?;
        let instance_id = InstanceId::new(required_env("SLEEPYPODS_INSTANCE_ID")?)?;
        let generation = Generation::new(required_env("SLEEPYPODS_INSTANCE_GENERATION")?.parse()?);
        let control_plane_endpoint = required_env("SLEEPYPODS_CONTROL_PLANE_ENDPOINT")?;
        let idle_timeout = duration_from_env_ms("SLEEPYPODS_IDLE_TIMEOUT_MS", 300_000)?;
        let retry_backoff = duration_from_env_ms("SLEEPYPODS_IDLE_RETRY_BACKOFF_MS", 5_000)?;
        let drain_grace_timeout =
            duration_from_env_ms("SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS", 30_000)?;
        let runtime_mode = SidecarRuntimeMode::from_env()?;
        let idle_report = IdleReportConfig::new(idle_timeout, retry_backoff)?;
        let runtime = SidecarRuntimeConfig::new(
            listen_addr,
            app_port,
            instance_id,
            generation,
            idle_report,
            drain_grace_timeout,
        )?;

        Ok(Self {
            runtime,
            runtime_mode,
            control_plane_endpoint,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SidecarRuntimeMode {
    Http,
    Tcp,
}

impl SidecarRuntimeMode {
    fn from_env() -> Result<Self, InvalidSidecarRuntimeMode> {
        match env::var("SLEEPYPODS_SIDECAR_MODE") {
            Ok(value) => Self::parse(value),
            Err(_) => Ok(Self::Http),
        }
    }

    fn parse(value: String) -> Result<Self, InvalidSidecarRuntimeMode> {
        match value.as_str() {
            "http" => Ok(Self::Http),
            "tcp" => Ok(Self::Tcp),
            _ => Err(InvalidSidecarRuntimeMode(value)),
        }
    }
}

fn required_env(name: &'static str) -> Result<String, MissingEnvVar> {
    env::var(name).map_err(|_| MissingEnvVar(name))
}

fn duration_from_env_ms(
    name: &'static str,
    default_ms: u64,
) -> Result<Duration, Box<dyn Error + Send + Sync>> {
    let value = match env::var(name) {
        Ok(value) => value.parse()?,
        Err(_) => default_ms,
    };

    Ok(Duration::from_millis(value))
}

#[derive(Debug)]
struct MissingEnvVar(&'static str);

impl std::fmt::Display for MissingEnvVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} is required", self.0)
    }
}

impl Error for MissingEnvVar {}

#[derive(Debug)]
struct InvalidSidecarRuntimeMode(String);

impl std::fmt::Display for InvalidSidecarRuntimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SLEEPYPODS_SIDECAR_MODE must be either http or tcp, got {:?}",
            self.0
        )
    }
}

impl Error for InvalidSidecarRuntimeMode {}
