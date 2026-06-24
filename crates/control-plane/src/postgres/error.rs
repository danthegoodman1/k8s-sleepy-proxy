use tokio_postgres::error::SqlState;

use crate::store::StoreError;

pub(crate) fn map_postgres_error(error: tokio_postgres::Error) -> StoreError {
    if let Some(db_error) = error.as_db_error() {
        return match *db_error.code() {
            SqlState::UNIQUE_VIOLATION => StoreError::AlreadyExists {
                resource: unique_violation_resource(db_error.constraint()),
            },
            SqlState::FOREIGN_KEY_VIOLATION => StoreError::NotFound {
                resource: "referenced resource",
            },
            SqlState::CHECK_VIOLATION => {
                StoreError::invalid_argument(format!("Postgres check constraint failed: {error}"))
            }
            _ => StoreError::internal(format!(
                "Postgres query failed: code={} message={}",
                db_error.code().code(),
                db_error.message()
            )),
        };
    }

    StoreError::internal(format!("Postgres query failed: {error}"))
}

fn unique_violation_resource(constraint: Option<&str>) -> &'static str {
    match constraint {
        Some("instances_pkey") => "instance",
        Some("route_bindings_pkey") | Some("route_bindings_identity_key_key") => "route binding",
        Some("workload_class_versions_pkey") => "workload class version",
        Some("http01_challenges_pkey") => "HTTP-01 challenge",
        Some("materializations_pkey")
        | Some("materializations_instance_id_cluster_id_namespace_key") => "materialization",
        Some("idempotency_records_pkey") => "idempotency record",
        _ => "Postgres record",
    }
}
