use std::{error::Error, process, time::Duration};

use control_plane::{
    api::pb::{
        operator_control_plane_client::OperatorControlPlaneClient,
        proxy_control_plane_client::ProxyControlPlaneClient,
    },
    OptionalBearerTokenInterceptor,
};
use frontline::{
    serve_frontline, FrontlineEnvConfig, FrontlineHttpRuntime, FrontlineRouteCoordinator,
    FrontlineRouteResolver, FrontlineTlsAdapter, GrpcOperatorHttp01Resolver,
    GrpcProxyControlPlaneClient, TlsCertificateStore, WakeTracker,
};
use proxy_core::{
    observability::{
        prometheus::{
            serve_prometheus_metrics_with_collector, PrometheusMetricsSink,
            RuntimeActiveStreamsCollector,
        },
        recorder::{
            CompositeObservabilitySink, FilteredStderrObservabilitySink, ObservabilityRecorder,
        },
    },
    DrainTracker, Shutdown,
};
use tokio::time::{sleep, Instant};
use tonic::transport::Endpoint;

const CONTROL_PLANE_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
const CONTROL_PLANE_CONNECT_RETRY_BACKOFF: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("frontline runtime failed: {error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    control_plane::install_rustls_crypto_provider();
    let env = FrontlineEnvConfig::from_env()?;
    let prometheus = install_runtime_observability(env.metrics_listen_addr().is_some());
    let observability = ObservabilityRecorder::global();
    let tls_certificates = env
        .load_tls_certificate_store()?
        .unwrap_or_else(TlsCertificateStore::new);
    let endpoint = Endpoint::from_shared(env.control_plane_endpoint().to_owned())?;
    let channel = connect_control_plane(endpoint, env.control_plane_endpoint()).await?;
    let proxy_interceptor = OptionalBearerTokenInterceptor::new(env.control_plane_proxy_token())?;
    let operator_interceptor =
        OptionalBearerTokenInterceptor::new(env.control_plane_operator_token())?;
    let route_client = GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::with_interceptor(
        channel.clone(),
        proxy_interceptor.clone(),
    ));
    let wake_client = GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::with_interceptor(
        channel.clone(),
        proxy_interceptor,
    ));
    let http01_resolver = GrpcOperatorHttp01Resolver::new(
        OperatorControlPlaneClient::with_interceptor(channel, operator_interceptor),
    );
    let resolver = FrontlineRouteResolver::with_observability(
        env.route_cache_capacity(),
        route_client,
        observability.clone(),
    );
    let coordinator = FrontlineRouteCoordinator::with_observability(
        resolver,
        WakeTracker::new(),
        wake_client,
        observability.clone(),
    )
    .with_wake_deadline(env.wake_instance_timeout());
    let drain = DrainTracker::with_observability(env.drain_grace_timeout(), observability.clone());
    let active_streams = RuntimeActiveStreamsCollector::default();
    active_streams.attach(drain.clone());
    let runtime = FrontlineHttpRuntime::with_http01_resolver_and_observability(
        coordinator,
        http01_resolver,
        drain,
        observability,
    );
    let shutdown = Shutdown::new();

    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                shutdown.shutdown();
            }
        }
    });

    if let (Some(metrics_addr), Some(prometheus)) = (env.metrics_listen_addr(), prometheus) {
        let metrics_shutdown = shutdown.clone();
        tokio::try_join!(
            async {
                serve_frontline(
                    env.listeners(),
                    runtime,
                    FrontlineTlsAdapter::new(tls_certificates),
                    shutdown,
                )
                .await
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
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
        serve_frontline(
            env.listeners(),
            runtime,
            FrontlineTlsAdapter::new(tls_certificates),
            shutdown,
        )
        .await?;
    }

    Ok(())
}

// The filtered stderr sink keeps lifecycle and error events visible while
// dropping metric samples and the per-request route-cache lookup event, so
// stderr observability performs no formatted writes on the forward path.
fn install_runtime_observability(metrics_enabled: bool) -> Option<PrometheusMetricsSink> {
    if !metrics_enabled {
        let _ = ObservabilityRecorder::install_global(std::sync::Arc::new(
            FilteredStderrObservabilitySink,
        ));
        return None;
    }

    let prometheus = PrometheusMetricsSink::new();
    let _ = ObservabilityRecorder::install_global(std::sync::Arc::new(
        CompositeObservabilitySink::new(vec![
            std::sync::Arc::new(FilteredStderrObservabilitySink),
            std::sync::Arc::new(prometheus.clone()),
        ]),
    ));
    Some(prometheus)
}

async fn connect_control_plane(
    endpoint: Endpoint,
    endpoint_label: &str,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    let deadline = Instant::now() + CONTROL_PLANE_CONNECT_TIMEOUT;
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return Ok(channel),
            Err(error) if Instant::now() < deadline => {
                eprintln!("frontline waiting for control-plane endpoint {endpoint_label}: {error}");
                sleep(CONTROL_PLANE_CONNECT_RETRY_BACKOFF).await;
            }
            Err(error) => return Err(error),
        }
    }
}
