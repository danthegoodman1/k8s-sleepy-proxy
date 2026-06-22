use std::{error::Error, process};

use control_plane::api::pb::proxy_control_plane_client::ProxyControlPlaneClient;
use frontline::{
    serve_http, FrontlineEnvConfig, FrontlineHttpRuntime, FrontlineRouteCoordinator,
    FrontlineRouteResolver, GrpcProxyControlPlaneClient, WakeTracker,
};
use proxy_core::{DrainTracker, Shutdown};
use tonic::transport::Endpoint;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("frontline runtime failed: {error}");
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let env = FrontlineEnvConfig::from_env()?;
    let channel = Endpoint::from_shared(env.control_plane_endpoint().to_owned())?
        .connect()
        .await?;
    let route_client =
        GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::new(channel.clone()));
    let wake_client = GrpcProxyControlPlaneClient::new(ProxyControlPlaneClient::new(channel));
    let resolver = FrontlineRouteResolver::new(env.route_cache_capacity(), route_client);
    let coordinator = FrontlineRouteCoordinator::new(resolver, WakeTracker::new(), wake_client);
    let drain = DrainTracker::new(env.drain_grace_timeout());
    let runtime = FrontlineHttpRuntime::new(coordinator, drain);
    let shutdown = Shutdown::new();

    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                shutdown.shutdown();
            }
        }
    });

    serve_http(env.listener(), runtime, shutdown).await?;

    Ok(())
}
