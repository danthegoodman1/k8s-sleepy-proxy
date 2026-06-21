use std::str::FromStr;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::{Config as PgConfig, NoTls};

use crate::{
    config::PostgresStoreConfig,
    store::{StoreError, StoreResult},
};

use super::migrations;

#[derive(Clone, Debug)]
pub struct PostgresStore {
    pub(crate) pool: Pool,
}

impl PostgresStore {
    pub async fn connect(config: &PostgresStoreConfig) -> StoreResult<Self> {
        let pg_config = parse_connection_url(config.connection_url())?;
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(pg_config, NoTls, manager_config);
        let pool = Pool::builder(manager)
            .max_size(16)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|error| {
                StoreError::internal(format!("failed to build Postgres pool: {error}"))
            })?;
        let store = Self { pool };

        store.run_migrations().await?;

        Ok(store)
    }

    pub async fn run_migrations(&self) -> StoreResult<()> {
        let client = self.client().await?;

        migrations::run(&client).await
    }

    pub(crate) async fn client(&self) -> StoreResult<deadpool_postgres::Client> {
        self.pool.get().await.map_err(|error| {
            StoreError::unavailable(format!("failed to get Postgres connection: {error}"))
        })
    }
}

fn parse_connection_url(value: &str) -> StoreResult<PgConfig> {
    PgConfig::from_str(value).map_err(|error| {
        StoreError::invalid_argument(format!(
            "postgres.connection_url is not a valid Postgres connection URL: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::parse_connection_url;
    use crate::{config::PostgresStoreConfig, postgres::PostgresStore, store::StoreError};

    #[test]
    fn parse_connection_url_rejects_invalid_urls() {
        let error = parse_connection_url("http://example.com/not-postgres")
            .expect_err("non-Postgres URL is invalid");

        assert!(matches!(error, StoreError::InvalidArgument { .. }));
    }

    #[test]
    fn postgres_store_config_rejects_missing_url_before_provider_construction() {
        let error = PostgresStoreConfig::new(" ").expect_err("missing URL is invalid");

        assert_eq!(error.field(), "postgres.connection_url");
    }

    #[test]
    fn postgres_store_is_control_plane_store() {
        fn assert_store<T: crate::store::ControlPlaneStore>() {}

        assert_store::<PostgresStore>();
    }
}
