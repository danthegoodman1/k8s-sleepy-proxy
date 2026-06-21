use std::str::FromStr;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::{Config as PgConfig, NoTls};

use crate::{
    config::PostgresStoreConfig,
    store::{ControlPlaneStore, StoreError, StoreResult},
    workload::WorkloadClassVersion,
};

use super::{mapping, migrations};

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

    pub async fn seed_workload_class_version(
        &self,
        version: WorkloadClassVersion,
    ) -> StoreResult<WorkloadClassVersion> {
        let client = self.client().await?;
        let class_id = version.reference.class_id.as_str();
        let version_number = mapping::generation_to_i64(version.reference.version)?;
        let template_generation = mapping::generation_to_i64(version.template_generation)?;
        let default_values = mapping::values_to_json(&version.default_values)?;
        let inserted = client
            .execute(
                "
                INSERT INTO workload_class_versions (
                    class_id,
                    version,
                    template_generation,
                    default_values
                )
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (class_id, version) DO NOTHING
                ",
                &[
                    &class_id,
                    &version_number,
                    &template_generation,
                    &default_values,
                ],
            )
            .await
            .map_err(super::error::map_postgres_error)?;

        let stored = ControlPlaneStore::load_workload_class_version(
            self,
            crate::workload::LoadWorkloadClassVersionRequest::new(version.reference.clone()),
        )
        .await?
        .ok_or(StoreError::NotFound {
            resource: "workload class version",
        })?;

        if inserted == 0 && stored != version {
            return Err(StoreError::AlreadyExists {
                resource: "workload class version",
            });
        }

        Ok(stored)
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
