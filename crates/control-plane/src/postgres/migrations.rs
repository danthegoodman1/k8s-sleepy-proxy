use deadpool_postgres::GenericClient;

use crate::store::{StoreError, StoreResult};

pub(crate) struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "initial_control_plane_store",
    sql: include_str!("../../migrations/0001_initial_control_plane_store.sql"),
}];

pub(crate) async fn run(client: &impl GenericClient) -> StoreResult<()> {
    client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS control_plane_schema_migrations (
                version integer PRIMARY KEY,
                name text NOT NULL,
                applied_at_unix_millis bigint NOT NULL DEFAULT (
                    extract(epoch from clock_timestamp()) * 1000
                )::bigint
            );
            ",
        )
        .await
        .map_err(map_migration_error)?;

    for migration in MIGRATIONS {
        let transaction = "BEGIN";
        client
            .batch_execute(transaction)
            .await
            .map_err(map_migration_error)?;
        let applied = apply_one(client, migration).await;
        match applied {
            Ok(()) => {
                client
                    .batch_execute("COMMIT")
                    .await
                    .map_err(map_migration_error)?;
            }
            Err(error) => {
                let _ = client.batch_execute("ROLLBACK").await;
                return Err(error);
            }
        }
    }

    Ok(())
}

async fn apply_one(client: &impl GenericClient, migration: &Migration) -> StoreResult<()> {
    let existing = client
        .query_opt(
            "SELECT name FROM control_plane_schema_migrations WHERE version = $1",
            &[&migration.version],
        )
        .await
        .map_err(map_migration_error)?;

    if let Some(row) = existing {
        let name: String = row.get("name");
        if name == migration.name {
            return Ok(());
        }

        return Err(StoreError::internal(format!(
            "migration version {} was already applied as {:?}, expected {:?}",
            migration.version, name, migration.name
        )));
    }

    client
        .batch_execute(migration.sql)
        .await
        .map_err(map_migration_error)?;
    client
        .execute(
            "INSERT INTO control_plane_schema_migrations (version, name) VALUES ($1, $2)",
            &[&migration.version, &migration.name],
        )
        .await
        .map_err(map_migration_error)?;

    Ok(())
}

fn map_migration_error(error: tokio_postgres::Error) -> StoreError {
    StoreError::internal(format!("postgres migration failed: {error}"))
}
