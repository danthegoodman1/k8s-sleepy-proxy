use std::{error::Error, time::Duration};

const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    control_plane::install_rustls_crypto_provider();
    run_with_runtime(control_plane::runtime::run_from_env())
}

// run_from_env() completes the existing owned RPC/controller/drain cleanup.
// A blocking OS resolver can outlive its canceled future; bound only the final
// Tokio teardown, without shortening or changing that async cleanup policy.
fn run_with_runtime(
    future: impl std::future::Future<Output = Result<(), Box<dyn Error + Send + Sync>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(future);
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    result
}

#[cfg(test)]
#[path = "../../../../tests/support/runtime_shutdown.rs"]
mod runtime_shutdown_tests;
