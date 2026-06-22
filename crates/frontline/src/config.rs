use std::{
    collections::HashMap,
    env,
    error::Error,
    fmt,
    net::{AddrParseError, SocketAddr},
    num::ParseIntError,
    time::Duration,
};

use crate::FrontlineHttpListenerConfig;

const DEFAULT_ROUTE_CACHE_CAPACITY: usize = 1024;
const DEFAULT_DRAIN_GRACE_TIMEOUT_MS: u64 = 30_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontlineEnvConfig {
    listener: FrontlineHttpListenerConfig,
    control_plane_endpoint: String,
    route_cache_capacity: usize,
    drain_grace_timeout: Duration,
}

#[derive(Debug)]
pub enum FrontlineEnvConfigError {
    Missing {
        name: &'static str,
    },
    InvalidSocketAddr {
        name: &'static str,
        value: String,
        source: AddrParseError,
    },
    InvalidUsize {
        name: &'static str,
        value: String,
        source: ParseIntError,
    },
    InvalidDurationMs {
        name: &'static str,
        value: String,
        source: ParseIntError,
    },
}

impl FrontlineEnvConfig {
    pub fn from_env() -> Result<Self, FrontlineEnvConfigError> {
        Self::from_vars(env::vars())
    }

    pub fn from_vars<I, K, V>(vars: I) -> Result<Self, FrontlineEnvConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let vars: HashMap<String, String> = vars
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect();

        let listen_addr = required(&vars, "SLEEPYPODS_FRONTLINE_LISTEN_ADDR")?;
        let listen_addr = listen_addr.parse::<SocketAddr>().map_err(|source| {
            FrontlineEnvConfigError::InvalidSocketAddr {
                name: "SLEEPYPODS_FRONTLINE_LISTEN_ADDR",
                value: listen_addr.clone(),
                source,
            }
        })?;
        let control_plane_endpoint = required(&vars, "SLEEPYPODS_CONTROL_PLANE_ENDPOINT")?;
        let route_cache_capacity = optional_usize(
            &vars,
            "SLEEPYPODS_ROUTE_CACHE_CAPACITY",
            DEFAULT_ROUTE_CACHE_CAPACITY,
        )?;
        let drain_grace_timeout = optional_duration_ms(
            &vars,
            "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS",
            DEFAULT_DRAIN_GRACE_TIMEOUT_MS,
        )?;

        Ok(Self {
            listener: FrontlineHttpListenerConfig::new(listen_addr),
            control_plane_endpoint,
            route_cache_capacity,
            drain_grace_timeout,
        })
    }

    pub fn listener(&self) -> FrontlineHttpListenerConfig {
        self.listener
    }

    pub fn control_plane_endpoint(&self) -> &str {
        &self.control_plane_endpoint
    }

    pub fn route_cache_capacity(&self) -> usize {
        self.route_cache_capacity
    }

    pub fn drain_grace_timeout(&self) -> Duration {
        self.drain_grace_timeout
    }
}

fn required(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<String, FrontlineEnvConfigError> {
    vars.get(name)
        .cloned()
        .ok_or(FrontlineEnvConfigError::Missing { name })
}

fn optional_usize(
    vars: &HashMap<String, String>,
    name: &'static str,
    default: usize,
) -> Result<usize, FrontlineEnvConfigError> {
    match vars.get(name) {
        Some(value) => value
            .parse()
            .map_err(|source| FrontlineEnvConfigError::InvalidUsize {
                name,
                value: value.clone(),
                source,
            }),
        None => Ok(default),
    }
}

fn optional_duration_ms(
    vars: &HashMap<String, String>,
    name: &'static str,
    default_ms: u64,
) -> Result<Duration, FrontlineEnvConfigError> {
    let value = match vars.get(name) {
        Some(value) => {
            value
                .parse()
                .map_err(|source| FrontlineEnvConfigError::InvalidDurationMs {
                    name,
                    value: value.clone(),
                    source,
                })?
        }
        None => default_ms,
    };

    Ok(Duration::from_millis(value))
}

impl fmt::Display for FrontlineEnvConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { name } => write!(f, "{name} is required"),
            Self::InvalidSocketAddr { name, value, .. } => {
                write!(f, "{name} must be a socket address, got {value:?}")
            }
            Self::InvalidUsize { name, value, .. } => {
                write!(f, "{name} must be an unsigned integer, got {value:?}")
            }
            Self::InvalidDurationMs { name, value, .. } => {
                write!(
                    f,
                    "{name} must be a duration in milliseconds, got {value:?}"
                )
            }
        }
    }
}

impl Error for FrontlineEnvConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidSocketAddr { source, .. } => Some(source),
            Self::InvalidUsize { source, .. } | Self::InvalidDurationMs { source, .. } => {
                Some(source)
            }
            Self::Missing { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_values_parse_with_defaults() {
        let config = FrontlineEnvConfig::from_vars([
            ("SLEEPYPODS_FRONTLINE_LISTEN_ADDR", "127.0.0.1:8080"),
            (
                "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
                "http://127.0.0.1:50051",
            ),
        ])
        .expect("config parses");

        assert_eq!(
            config.listener().listen_addr(),
            "127.0.0.1:8080".parse().unwrap()
        );
        assert_eq!(config.control_plane_endpoint(), "http://127.0.0.1:50051");
        assert_eq!(config.route_cache_capacity(), DEFAULT_ROUTE_CACHE_CAPACITY);
        assert_eq!(
            config.drain_grace_timeout(),
            Duration::from_millis(DEFAULT_DRAIN_GRACE_TIMEOUT_MS)
        );
    }

    #[test]
    fn optional_values_override_defaults() {
        let config = FrontlineEnvConfig::from_vars([
            ("SLEEPYPODS_FRONTLINE_LISTEN_ADDR", "127.0.0.1:8080"),
            (
                "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
                "http://127.0.0.1:50051",
            ),
            ("SLEEPYPODS_ROUTE_CACHE_CAPACITY", "17"),
            ("SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS", "250"),
        ])
        .expect("config parses");

        assert_eq!(config.route_cache_capacity(), 17);
        assert_eq!(config.drain_grace_timeout(), Duration::from_millis(250));
    }

    #[test]
    fn missing_required_value_is_reported() {
        let error = FrontlineEnvConfig::from_vars([(
            "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
            "http://127.0.0.1:50051",
        )])
        .expect_err("listen addr is required");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::Missing {
                name: "SLEEPYPODS_FRONTLINE_LISTEN_ADDR"
            }
        ));
    }

    #[test]
    fn invalid_optional_number_is_reported() {
        let error = FrontlineEnvConfig::from_vars([
            ("SLEEPYPODS_FRONTLINE_LISTEN_ADDR", "127.0.0.1:8080"),
            (
                "SLEEPYPODS_CONTROL_PLANE_ENDPOINT",
                "http://127.0.0.1:50051",
            ),
            ("SLEEPYPODS_ROUTE_CACHE_CAPACITY", "nope"),
        ])
        .expect_err("route cache capacity is invalid");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::InvalidUsize {
                name: "SLEEPYPODS_ROUTE_CACHE_CAPACITY",
                ..
            }
        ));
    }
}
