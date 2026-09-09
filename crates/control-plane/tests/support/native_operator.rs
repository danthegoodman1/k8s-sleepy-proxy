//! Verified native operator bootstrap shared by the dynamic-only release fixtures.
use control_plane::{
    api::pb::operator_control_plane_client::OperatorControlPlaneClient, BearerToken,
    OptionalBearerTokenInterceptor,
};
use std::{env, error::Error, time::Duration};
use tonic::{service::interceptor::InterceptedService, transport::Channel};
pub type Operator =
    OperatorControlPlaneClient<InterceptedService<Channel, OptionalBearerTokenInterceptor>>;
pub async fn connect(endpoint: &str) -> Result<Operator, Box<dyn Error + Send + Sync>> {
    if !endpoint.starts_with("https://") {
        return Err("native TLS fixture requires HTTPS".into());
    }
    let ca = env::var("SLEEPYPODS_CONTROL_PLANE_TLS_CA_PEM")?;
    let token = BearerToken::new(
        "fixture_operator",
        env::var("SLEEPYPODS_E2E_OPERATOR_TOKEN")?,
    )?;
    let interceptor = OptionalBearerTokenInterceptor::new(Some(&token))?;
    let endpoint = sleepypods_api::transport::native_endpoint(endpoint.to_owned(), Some(&ca))?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(10));
    // Only bootstrap connections retry; no operator mutation is replayed.
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match endpoint.connect().await {
                Ok(channel) => {
                    return Ok(OperatorControlPlaneClient::with_interceptor(
                        channel,
                        interceptor,
                    ))
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
    })
    .await
    .map_err(|_| -> Box<dyn Error + Send + Sync> {
        "verified operator bootstrap deadline elapsed".into()
    })?
}
