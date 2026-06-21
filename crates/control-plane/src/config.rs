use std::{error::Error, fmt, str::FromStr};

use crate::ids::{EmptyStringError, NonEmptyString};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlPlaneConfig {
    pub store: StoreProviderConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreProviderConfig {
    Postgres(PostgresStoreConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresStoreConfig {
    connection_url: NonEmptyString,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreProviderName {
    Postgres,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownStoreProvider {
    value: String,
}

impl ControlPlaneConfig {
    pub fn new(store: StoreProviderConfig) -> Self {
        Self { store }
    }
}

impl StoreProviderConfig {
    pub fn provider_name(&self) -> StoreProviderName {
        match self {
            Self::Postgres(_) => StoreProviderName::Postgres,
        }
    }
}

impl PostgresStoreConfig {
    pub fn new(connection_url: impl Into<String>) -> Result<Self, EmptyStringError> {
        Ok(Self {
            connection_url: NonEmptyString::new("postgres.connection_url", connection_url)?,
        })
    }

    pub fn connection_url(&self) -> &str {
        self.connection_url.as_str()
    }
}

impl StoreProviderName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
        }
    }
}

impl FromStr for StoreProviderName {
    type Err = UnknownStoreProvider;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "postgres" => Ok(Self::Postgres),
            _ => Err(UnknownStoreProvider {
                value: value.to_owned(),
            }),
        }
    }
}

impl fmt::Display for StoreProviderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl UnknownStoreProvider {
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for UnknownStoreProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown store provider {:?}", self.value)
    }
}

impl Error for UnknownStoreProvider {}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{PostgresStoreConfig, StoreProviderConfig, StoreProviderName};

    #[test]
    fn parses_postgres_provider_name_case_insensitively() {
        assert_eq!(
            StoreProviderName::from_str(" PostgreSQL ")
                .expect_err("only canonical name is accepted")
                .value(),
            " PostgreSQL "
        );
        assert_eq!(
            StoreProviderName::from_str("Postgres").expect("postgres provider is supported"),
            StoreProviderName::Postgres
        );
    }

    #[test]
    fn validates_postgres_connection_url_presence() {
        let error = PostgresStoreConfig::new("\t").expect_err("connection URL is required");

        assert_eq!(error.field(), "postgres.connection_url");
    }

    #[test]
    fn store_config_reports_selected_provider() {
        let config = StoreProviderConfig::Postgres(
            PostgresStoreConfig::new("postgres://example").expect("valid URL"),
        );

        assert_eq!(config.provider_name(), StoreProviderName::Postgres);
    }
}
