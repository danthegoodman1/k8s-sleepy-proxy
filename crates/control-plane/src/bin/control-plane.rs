use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    control_plane::install_rustls_crypto_provider();
    control_plane::runtime::run_from_env().await
}
