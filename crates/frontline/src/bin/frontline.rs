use std::{error::Error, process};

use control_plane::api::pb::{
    operator_control_plane_client::OperatorControlPlaneClient,
    proxy_control_plane_client::ProxyControlPlaneClient,
};
use frontline::{
    serve_frontline, FrontlineEnvConfig, FrontlineHttpRuntime, FrontlineRouteCoordinator,
    FrontlineRouteResolver, FrontlineTlsAdapter, GrpcOperatorHttp01Resolver,
    GrpcProxyControlPlaneClient, TlsCertificateStore, WakeTracker,
};
use proxy_core::{observability::recorder::ObservabilityRecorder, DrainTracker, Shutdown};
use tonic::transport::Endpoint;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("frontline runtime failed: {error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let _ = ObservabilityRecorder::install_stderr_global();
    let observability = ObservabilityRecorder::global();
    let env = FrontlineEnvConfig::from_env()?;
    let tls_certificates = env
        .load_tls_certificate_store()?
        .unwrap_or_else(TlsCertificateStore::new);
    let channel = Endpoint::from_shared(env.control_plane_endpoint().to_owned())?
        .connect()
        .await?;
    let route_client =
        GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::new(channel.clone()));
    let wake_client =
        GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::new(channel.clone()));
    let http01_resolver = GrpcOperatorHttp01Resolver::new(OperatorControlPlaneClient::new(channel));
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
    );
    let drain = DrainTracker::with_observability(env.drain_grace_timeout(), observability.clone());
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

    serve_frontline(
        env.listeners(),
        runtime,
        FrontlineTlsAdapter::new(tls_certificates),
        shutdown,
    )
    .await?;

    Ok(())
}
