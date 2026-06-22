use std::{env, error::Error, process, time::Duration};

use control_plane::api::pb::sidecar_control_plane_client::SidecarControlPlaneClient;
use proxy_core::Shutdown;
use sidecar::{
    runtime::{serve_http_with_idle, SidecarRuntimeConfig},
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
    let env = EnvConfig::from_env()?;
    let channel = Endpoint::from_shared(env.control_plane_endpoint.clone())?
        .connect()
        .await?;
    let client = GrpcSidecarControlPlaneClient::new(SidecarControlPlaneClient::new(channel));
    let shutdown = Shutdown::new();

    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                shutdown.shutdown();
            }
        }
    });

    serve_http_with_idle(env.runtime, client, shutdown).await?;

    Ok(())
}

#[derive(Debug)]
struct EnvConfig {
    runtime: SidecarRuntimeConfig,
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
            control_plane_endpoint,
        })
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
