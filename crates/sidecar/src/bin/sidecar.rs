use std::{env, error::Error, net::SocketAddr, process, time::Duration};

use control_plane::{
    api::pb::sidecar_control_plane_client::SidecarControlPlaneClient, BearerToken,
    OptionalBearerTokenInterceptor,
};
use proxy_core::{
    observability::{
        prometheus::{serve_prometheus_metrics_with_collector, PrometheusMetricsSink},
        recorder::{CompositeObservabilitySink, ObservabilityRecorder, StderrObservabilitySink},
    },
    Shutdown,
};
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
    let env = EnvConfig::from_env()?;
    let prometheus = install_runtime_observability(env.metrics_listen_addr.is_some());
    let channel = Endpoint::from_shared(env.control_plane_endpoint.clone())?
        .connect()
        .await?;
    let interceptor =
        OptionalBearerTokenInterceptor::new(env.control_plane_sidecar_token.as_ref())?;
    let client = GrpcSidecarControlPlaneClient::new(SidecarControlPlaneClient::with_interceptor(
        channel,
        interceptor,
    ));
    let shutdown = Shutdown::new();
    let _shutdown_task = spawn_shutdown_signal(shutdown.clone())?;

    if let (Some(metrics_addr), Some(prometheus)) = (env.metrics_listen_addr, prometheus) {
        let metrics_shutdown = shutdown.clone();
        let runtime = env.runtime;
        let active_streams = runtime.active_streams_collector();
        let runtime_mode = env.runtime_mode;
        tokio::try_join!(
            async {
                match runtime_mode {
                    SidecarRuntimeMode::Http => serve_http_with_idle(runtime, client, shutdown)
                        .await
                        .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>),
                    SidecarRuntimeMode::Tcp => serve_tcp_with_idle(runtime, client, shutdown)
                        .await
                        .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>),
                }
            },
            async {
                serve_prometheus_metrics_with_collector(
                    metrics_addr,
                    prometheus,
                    metrics_shutdown.cancelled(),
                    move |sink| {
                        let active_streams = active_streams.clone();
                        async move {
                            active_streams.collect(sink);
                        }
                    },
                )
                .await
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
            },
        )?;
    } else {
        match env.runtime_mode {
            SidecarRuntimeMode::Http => serve_http_with_idle(env.runtime, client, shutdown).await?,
            SidecarRuntimeMode::Tcp => serve_tcp_with_idle(env.runtime, client, shutdown).await?,
        }
    }

    Ok(())
}

fn install_runtime_observability(metrics_enabled: bool) -> Option<PrometheusMetricsSink> {
    if !metrics_enabled {
        let _ = ObservabilityRecorder::install_stderr_global();
        return None;
    }

    let prometheus = PrometheusMetricsSink::new();
    let _ = ObservabilityRecorder::install_global(std::sync::Arc::new(
        CompositeObservabilitySink::new(vec![
            std::sync::Arc::new(StderrObservabilitySink),
            std::sync::Arc::new(prometheus.clone()),
        ]),
    ));
    Some(prometheus)
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
    control_plane_sidecar_token: Option<BearerToken>,
    metrics_listen_addr: Option<SocketAddr>,
}

impl EnvConfig {
    fn from_env() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let listen_addr = required_env("SLEEPYPODS_SIDECAR_LISTEN_ADDR")?.parse()?;
        let app_port = required_env("SLEEPYPODS_APP_PORT")?.parse()?;
        let instance_id = InstanceId::new(required_env("SLEEPYPODS_INSTANCE_ID")?)?;
        let generation = Generation::new(required_env("SLEEPYPODS_INSTANCE_GENERATION")?.parse()?);
        let control_plane_endpoint = required_env("SLEEPYPODS_CONTROL_PLANE_ENDPOINT")?;
        let control_plane_sidecar_token =
            optional_bearer_token("SLEEPYPODS_CONTROL_PLANE_SIDECAR_TOKEN")?;
        let idle_timeout = duration_from_env_ms("SLEEPYPODS_IDLE_TIMEOUT_MS", 300_000)?;
        let retry_backoff = duration_from_env_ms("SLEEPYPODS_IDLE_RETRY_BACKOFF_MS", 5_000)?;
        let drain_grace_timeout =
            duration_from_env_ms("SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS", 30_000)?;
        let metrics_listen_addr = optional_socket_addr("SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR")?;
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
            control_plane_sidecar_token,
            metrics_listen_addr,
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

fn optional_bearer_token(
    name: &'static str,
) -> Result<Option<BearerToken>, Box<dyn Error + Send + Sync>> {
    match env::var(name) {
        Ok(value) => Ok(Some(BearerToken::new(name, value)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Box::new(error)),
    }
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

fn optional_socket_addr(
    name: &'static str,
) -> Result<Option<SocketAddr>, Box<dyn Error + Send + Sync>> {
    match env::var(name) {
        Ok(value) => parse_optional_socket_addr_value(name, Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Box::new(error)),
    }
}

fn parse_optional_socket_addr_value(
    _name: &'static str,
    value: Option<String>,
) -> Result<Option<SocketAddr>, Box<dyn Error + Send + Sync>> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        })
        .transpose()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_listener_value_is_optional() {
        assert_eq!(
            parse_optional_socket_addr_value("SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR", None)
                .expect("missing value is valid"),
            None
        );
    }

    #[test]
    fn metrics_listener_value_parses_socket_addr() {
        assert_eq!(
            parse_optional_socket_addr_value(
                "SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR",
                Some("127.0.0.1:19092".to_owned()),
            )
            .expect("socket address parses"),
            Some("127.0.0.1:19092".parse().expect("socket address"))
        );
    }

    #[test]
    fn metrics_listener_value_rejects_invalid_socket_addr() {
        assert!(parse_optional_socket_addr_value(
            "SLEEPYPODS_SIDECAR_METRICS_LISTEN_ADDR",
            Some("localhost".to_owned()),
        )
        .is_err());
    }
}
