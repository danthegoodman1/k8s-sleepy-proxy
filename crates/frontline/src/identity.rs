use std::{error::Error, fmt};

use control_plane::{PathPrefix, RouteHost, RouteIdentity};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteRequestIdentity {
    identity: RouteIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestIdentityError {
    EmptyHost,
    InvalidHostPort,
    UnsupportedHostSyntax,
    InvalidRouteHost(String),
    InvalidPath(String),
}

impl RouteRequestIdentity {
    pub fn http(host: impl AsRef<str>, path: Option<&str>) -> Result<Self, RequestIdentityError> {
        let host = canonical_request_host(host.as_ref())?;
        let path = canonical_request_path(path)?;

        Ok(Self {
            identity: RouteIdentity::Http {
                host,
                path: Some(path),
            },
        })
    }

    pub fn sni(host: impl AsRef<str>) -> Result<Self, RequestIdentityError> {
        Ok(Self {
            identity: RouteIdentity::Sni {
                host: canonical_request_host(host.as_ref())?,
            },
        })
    }

    pub fn identity(&self) -> &RouteIdentity {
        &self.identity
    }

    pub fn into_identity(self) -> RouteIdentity {
        self.identity
    }
}

impl fmt::Display for RequestIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyHost => f.write_str("request host must not be empty"),
            Self::InvalidHostPort => f.write_str("request host port must be numeric"),
            Self::UnsupportedHostSyntax => f.write_str(
                "request host must not contain a colon except for an optional numeric port",
            ),
            Self::InvalidRouteHost(reason) => write!(f, "invalid request host: {reason}"),
            Self::InvalidPath(path) => write!(f, "request path {path:?} must start with /"),
        }
    }
}

impl Error for RequestIdentityError {}

fn canonical_request_host(value: &str) -> Result<RouteHost, RequestIdentityError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(RequestIdentityError::EmptyHost);
    }

    if trimmed.starts_with('[') {
        return Err(RequestIdentityError::UnsupportedHostSyntax);
    }

    let without_port = strip_optional_port(trimmed)?;
    if without_port.trim().is_empty() {
        return Err(RequestIdentityError::EmptyHost);
    }

    if without_port.contains(':') || without_port.contains('/') {
        return Err(RequestIdentityError::UnsupportedHostSyntax);
    }

    RouteHost::exact(without_port)
        .map_err(|error| RequestIdentityError::InvalidRouteHost(error.to_string()))
}

fn strip_optional_port(host: &str) -> Result<&str, RequestIdentityError> {
    match host.rsplit_once(':') {
        Some((name, port)) if !name.contains(':') => {
            if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(RequestIdentityError::InvalidHostPort);
            }

            Ok(name)
        }
        Some(_) => Err(RequestIdentityError::UnsupportedHostSyntax),
        None => Ok(host),
    }
}

fn canonical_request_path(path: Option<&str>) -> Result<PathPrefix, RequestIdentityError> {
    let path = path.filter(|value| !value.is_empty()).unwrap_or("/");
    PathPrefix::new(path.to_owned()).map_err(|_| RequestIdentityError::InvalidPath(path.to_owned()))
}

#[cfg(test)]
mod tests {
    use control_plane::RouteIdentity;

    use super::{RequestIdentityError, RouteRequestIdentity};

    #[test]
    fn http_host_normalization_handles_case_trailing_dot_and_port() {
        let identity = RouteRequestIdentity::http(" App.Example.COM.:443 ", Some("/api"))
            .expect("valid identity")
            .into_identity();

        match identity {
            RouteIdentity::Http { host, path } => {
                assert_eq!(host.as_str(), "app.example.com");
                assert_eq!(path.expect("path").as_str(), "/api");
            }
            RouteIdentity::Sni { .. } => panic!("expected HTTP identity"),
        }
    }

    #[test]
    fn sni_uses_same_host_canonicalization() {
        let identity = RouteRequestIdentity::sni(" Db.Example.COM. ")
            .expect("valid identity")
            .into_identity();

        match identity {
            RouteIdentity::Sni { host } => assert_eq!(host.as_str(), "db.example.com"),
            RouteIdentity::Http { .. } => panic!("expected SNI identity"),
        }
    }

    #[test]
    fn host_normalization_rejects_empty_and_invalid_hosts() {
        assert_eq!(
            RouteRequestIdentity::http("  ", None).expect_err("empty host"),
            RequestIdentityError::EmptyHost
        );
        assert_eq!(
            RouteRequestIdentity::http("app.example.com:http", None).expect_err("bad port"),
            RequestIdentityError::InvalidHostPort
        );
        assert!(matches!(
            RouteRequestIdentity::http("localhost", None).expect_err("invalid host"),
            RequestIdentityError::InvalidRouteHost(_)
        ));
    }

    #[test]
    fn http_identity_defaults_missing_or_empty_path_to_root() {
        for path in [None, Some("")] {
            let identity = RouteRequestIdentity::http("app.example.com", path)
                .expect("valid identity")
                .into_identity();

            match identity {
                RouteIdentity::Http { path, .. } => {
                    assert_eq!(path.expect("path").as_str(), "/");
                }
                RouteIdentity::Sni { .. } => panic!("expected HTTP identity"),
            }
        }
    }
}
