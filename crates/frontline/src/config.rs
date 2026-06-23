use std::{
    collections::HashMap,
    env,
    error::Error,
    fmt,
    fs::File,
    io::{self, BufReader},
    net::{AddrParseError, SocketAddr},
    num::ParseIntError,
    path::{Path, PathBuf},
    time::Duration,
};

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::{
    FrontlineHttpListenerConfig, FrontlineListenersConfig, FrontlineTlsPassthroughListenerConfig,
    FrontlineTlsTerminationListenerConfig, RequestIdentityError, TlsCertificateError,
    TlsCertificateStore,
};

const DEFAULT_ROUTE_CACHE_CAPACITY: usize = 1024;
const DEFAULT_DRAIN_GRACE_TIMEOUT_MS: u64 = 30_000;
const FRONTLINE_LISTEN_ADDR: &str = "SLEEPYPODS_FRONTLINE_LISTEN_ADDR";
const CONTROL_PLANE_ENDPOINT: &str = "SLEEPYPODS_CONTROL_PLANE_ENDPOINT";
const ROUTE_CACHE_CAPACITY: &str = "SLEEPYPODS_ROUTE_CACHE_CAPACITY";
const DRAIN_GRACE_TIMEOUT_MS: &str = "SLEEPYPODS_DRAIN_GRACE_TIMEOUT_MS";
const TLS_TERMINATION_LISTEN_ADDR: &str = "SLEEPYPODS_FRONTLINE_TLS_TERMINATION_LISTEN_ADDR";
const TLS_TERMINATION_CERTS: &str = "SLEEPYPODS_FRONTLINE_TLS_TERMINATION_CERTS";
const TLS_PASSTHROUGH_LISTEN_ADDR: &str = "SLEEPYPODS_FRONTLINE_TLS_PASSTHROUGH_LISTEN_ADDR";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontlineEnvConfig {
    listeners: FrontlineListenersConfig,
    tls_certificates: Vec<FrontlineTlsCertificateConfig>,
    control_plane_endpoint: String,
    route_cache_capacity: usize,
    drain_grace_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontlineTlsCertificateConfig {
    sni: String,
    certificate_path: PathBuf,
    private_key_path: PathBuf,
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
    InvalidTlsCertificates {
        name: &'static str,
        value: String,
        reason: String,
    },
}

#[derive(Debug)]
pub enum FrontlineTlsCertificateLoadError {
    OpenCertificate {
        path: PathBuf,
        source: io::Error,
    },
    ParseCertificate {
        path: PathBuf,
        source: io::Error,
    },
    MissingCertificate {
        path: PathBuf,
    },
    OpenPrivateKey {
        path: PathBuf,
        source: io::Error,
    },
    ParsePrivateKey {
        path: PathBuf,
        source: io::Error,
    },
    MissingPrivateKey {
        path: PathBuf,
    },
    Store {
        sni: String,
        source: TlsCertificateError,
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

        let listen_addr = required(&vars, FRONTLINE_LISTEN_ADDR)?;
        let listen_addr = listen_addr.parse::<SocketAddr>().map_err(|source| {
            FrontlineEnvConfigError::InvalidSocketAddr {
                name: FRONTLINE_LISTEN_ADDR,
                value: listen_addr.clone(),
                source,
            }
        })?;
        let control_plane_endpoint = required(&vars, CONTROL_PLANE_ENDPOINT)?;
        let route_cache_capacity =
            optional_usize(&vars, ROUTE_CACHE_CAPACITY, DEFAULT_ROUTE_CACHE_CAPACITY)?;
        let drain_grace_timeout = optional_duration_ms(
            &vars,
            DRAIN_GRACE_TIMEOUT_MS,
            DEFAULT_DRAIN_GRACE_TIMEOUT_MS,
        )?;
        let tls_termination =
            optional_tls_termination_listener_config(&vars, TLS_TERMINATION_LISTEN_ADDR)?;
        let tls_passthrough = optional_listener_config(&vars, TLS_PASSTHROUGH_LISTEN_ADDR)?
            .map(|listener| FrontlineTlsPassthroughListenerConfig::new(listener.listen_addr()));
        let tls_certificates = optional_tls_certificates(&vars, tls_termination.is_some())?;
        let listeners =
            FrontlineListenersConfig::new(FrontlineHttpListenerConfig::new(listen_addr))
                .with_tls_termination(tls_termination)
                .with_tls_passthrough(tls_passthrough);

        Ok(Self {
            listeners,
            tls_certificates,
            control_plane_endpoint,
            route_cache_capacity,
            drain_grace_timeout,
        })
    }

    pub fn listener(&self) -> FrontlineHttpListenerConfig {
        self.listeners.http()
    }

    pub fn listeners(&self) -> FrontlineListenersConfig {
        self.listeners
    }

    pub fn tls_termination_listener(&self) -> Option<FrontlineTlsTerminationListenerConfig> {
        self.listeners.tls_termination()
    }

    pub fn tls_passthrough_listener(&self) -> Option<FrontlineTlsPassthroughListenerConfig> {
        self.listeners.tls_passthrough()
    }

    pub fn tls_certificates(&self) -> &[FrontlineTlsCertificateConfig] {
        &self.tls_certificates
    }

    pub fn load_tls_certificate_store(
        &self,
    ) -> Result<Option<TlsCertificateStore>, FrontlineTlsCertificateLoadError> {
        if self.tls_certificates.is_empty() {
            return Ok(None);
        }

        let store = TlsCertificateStore::new();
        for certificate in &self.tls_certificates {
            store
                .upsert(
                    &certificate.sni,
                    load_certificate_chain(&certificate.certificate_path)?,
                    load_private_key(&certificate.private_key_path)?,
                )
                .map_err(|source| FrontlineTlsCertificateLoadError::Store {
                    sni: certificate.sni.clone(),
                    source,
                })?;
        }

        Ok(Some(store))
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

impl FrontlineTlsCertificateConfig {
    pub fn new(
        sni: impl Into<String>,
        certificate_path: impl Into<PathBuf>,
        private_key_path: impl Into<PathBuf>,
    ) -> Result<Self, RequestIdentityError> {
        let sni = sni.into();
        crate::RouteRequestIdentity::sni(&sni)?;

        Ok(Self {
            sni,
            certificate_path: certificate_path.into(),
            private_key_path: private_key_path.into(),
        })
    }

    pub fn sni(&self) -> &str {
        &self.sni
    }

    pub fn certificate_path(&self) -> &Path {
        &self.certificate_path
    }

    pub fn private_key_path(&self) -> &Path {
        &self.private_key_path
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

fn optional_listener_config(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<FrontlineHttpListenerConfig>, FrontlineEnvConfigError> {
    let Some(value) = vars.get(name) else {
        return Ok(None);
    };
    let listen_addr = value.parse::<SocketAddr>().map_err(|source| {
        FrontlineEnvConfigError::InvalidSocketAddr {
            name,
            value: value.clone(),
            source,
        }
    })?;

    Ok(Some(FrontlineHttpListenerConfig::new(listen_addr)))
}

fn optional_tls_termination_listener_config(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<FrontlineTlsTerminationListenerConfig>, FrontlineEnvConfigError> {
    optional_listener_config(vars, name).map(|listener| {
        listener.map(|listener| FrontlineTlsTerminationListenerConfig::new(listener.listen_addr()))
    })
}

fn optional_tls_certificates(
    vars: &HashMap<String, String>,
    tls_termination_enabled: bool,
) -> Result<Vec<FrontlineTlsCertificateConfig>, FrontlineEnvConfigError> {
    match (vars.get(TLS_TERMINATION_CERTS), tls_termination_enabled) {
        (None, false) => Ok(Vec::new()),
        (None, true) => Err(FrontlineEnvConfigError::Missing {
            name: TLS_TERMINATION_CERTS,
        }),
        (Some(_), false) => Err(FrontlineEnvConfigError::Missing {
            name: TLS_TERMINATION_LISTEN_ADDR,
        }),
        (Some(value), true) => parse_tls_certificates(value),
    }
}

fn parse_tls_certificates(
    value: &str,
) -> Result<Vec<FrontlineTlsCertificateConfig>, FrontlineEnvConfigError> {
    let mut certificates = Vec::new();
    for raw_entry in value.split(';') {
        let entry = raw_entry.trim();
        if entry.is_empty() {
            continue;
        }

        let parts: Vec<&str> = entry.split('|').map(str::trim).collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return Err(invalid_tls_certificates(
                value,
                "entries must use sni|certificate_path|private_key_path",
            ));
        }

        let certificate = FrontlineTlsCertificateConfig::new(parts[0], parts[1], parts[2])
            .map_err(|error| invalid_tls_certificates(value, error.to_string()))?;
        certificates.push(certificate);
    }

    if certificates.is_empty() {
        return Err(invalid_tls_certificates(
            value,
            "at least one TLS certificate entry is required",
        ));
    }

    Ok(certificates)
}

fn invalid_tls_certificates(value: &str, reason: impl Into<String>) -> FrontlineEnvConfigError {
    FrontlineEnvConfigError::InvalidTlsCertificates {
        name: TLS_TERMINATION_CERTS,
        value: value.to_owned(),
        reason: reason.into(),
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

fn load_certificate_chain(
    path: &Path,
) -> Result<Vec<CertificateDer<'static>>, FrontlineTlsCertificateLoadError> {
    let file =
        File::open(path).map_err(|source| FrontlineTlsCertificateLoadError::OpenCertificate {
            path: path.to_owned(),
            source,
        })?;
    let mut reader = BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(
            |source| FrontlineTlsCertificateLoadError::ParseCertificate {
                path: path.to_owned(),
                source,
            },
        )?;
    if certificates.is_empty() {
        return Err(FrontlineTlsCertificateLoadError::MissingCertificate {
            path: path.to_owned(),
        });
    }

    Ok(certificates)
}

fn load_private_key(
    path: &Path,
) -> Result<PrivateKeyDer<'static>, FrontlineTlsCertificateLoadError> {
    let file =
        File::open(path).map_err(|source| FrontlineTlsCertificateLoadError::OpenPrivateKey {
            path: path.to_owned(),
            source,
        })?;
    let mut reader = BufReader::new(file);

    rustls_pemfile::private_key(&mut reader)
        .map_err(|source| FrontlineTlsCertificateLoadError::ParsePrivateKey {
            path: path.to_owned(),
            source,
        })?
        .ok_or_else(|| FrontlineTlsCertificateLoadError::MissingPrivateKey {
            path: path.to_owned(),
        })
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
            Self::InvalidTlsCertificates {
                name,
                value,
                reason,
            } => {
                write!(
                    f,
                    "{name} must be TLS certificate entries, got {value:?}: {reason}"
                )
            }
        }
    }
}

impl fmt::Display for FrontlineTlsCertificateLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenCertificate { path, source } => {
                write!(f, "failed to open TLS certificate {:?}: {source}", path)
            }
            Self::ParseCertificate { path, source } => {
                write!(f, "failed to parse TLS certificate {:?}: {source}", path)
            }
            Self::MissingCertificate { path } => {
                write!(f, "TLS certificate file {:?} has no certificate", path)
            }
            Self::OpenPrivateKey { path, source } => {
                write!(f, "failed to open TLS private key {:?}: {source}", path)
            }
            Self::ParsePrivateKey { path, source } => {
                write!(f, "failed to parse TLS private key {:?}: {source}", path)
            }
            Self::MissingPrivateKey { path } => {
                write!(f, "TLS private key file {:?} has no private key", path)
            }
            Self::Store { sni, source } => {
                write!(f, "failed to store TLS certificate for {sni:?}: {source}")
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
            Self::Missing { .. } | Self::InvalidTlsCertificates { .. } => None,
        }
    }
}

impl Error for FrontlineTlsCertificateLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::OpenCertificate { source, .. }
            | Self::ParseCertificate { source, .. }
            | Self::OpenPrivateKey { source, .. }
            | Self::ParsePrivateKey { source, .. } => Some(source),
            Self::Store { source, .. } => Some(source),
            Self::MissingCertificate { .. } | Self::MissingPrivateKey { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    #[test]
    fn required_values_parse_with_defaults() {
        let config = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
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
        assert!(config.tls_termination_listener().is_none());
        assert!(config.tls_passthrough_listener().is_none());
        assert!(config.tls_certificates().is_empty());
        assert!(config
            .load_tls_certificate_store()
            .expect("disabled TLS certificate store loads")
            .is_none());
    }

    #[test]
    fn optional_values_override_defaults() {
        let config = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (ROUTE_CACHE_CAPACITY, "17"),
            (DRAIN_GRACE_TIMEOUT_MS, "250"),
        ])
        .expect("config parses");

        assert_eq!(config.route_cache_capacity(), 17);
        assert_eq!(config.drain_grace_timeout(), Duration::from_millis(250));
    }

    #[test]
    fn missing_required_value_is_reported() {
        let error =
            FrontlineEnvConfig::from_vars([(CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051")])
                .expect_err("listen addr is required");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::Missing {
                name: FRONTLINE_LISTEN_ADDR
            }
        ));
    }

    #[test]
    fn invalid_optional_number_is_reported() {
        let error = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (ROUTE_CACHE_CAPACITY, "nope"),
        ])
        .expect_err("route cache capacity is invalid");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::InvalidUsize {
                name: ROUTE_CACHE_CAPACITY,
                ..
            }
        ));
    }

    #[test]
    fn tls_listener_values_parse_when_enabled() {
        let config = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (TLS_TERMINATION_LISTEN_ADDR, "127.0.0.1:8443"),
            (
                TLS_TERMINATION_CERTS,
                " App.Example.COM. | /tmp/app-cert.pem | /tmp/app-key.pem ; db.example.com | /tmp/db-cert.pem | /tmp/db-key.pem ",
            ),
            (TLS_PASSTHROUGH_LISTEN_ADDR, "127.0.0.1:15432"),
        ])
        .expect("config parses");

        assert_eq!(
            config
                .tls_termination_listener()
                .expect("TLS termination listener")
                .listen_addr(),
            "127.0.0.1:8443".parse().unwrap()
        );
        assert_eq!(
            config
                .tls_passthrough_listener()
                .expect("TLS passthrough listener")
                .listen_addr(),
            "127.0.0.1:15432".parse().unwrap()
        );
        assert_eq!(config.tls_certificates().len(), 2);
        assert_eq!(config.tls_certificates()[0].sni(), "App.Example.COM.");
        assert_eq!(
            config.tls_certificates()[0].certificate_path(),
            Path::new("/tmp/app-cert.pem")
        );
        assert_eq!(
            config.tls_certificates()[0].private_key_path(),
            Path::new("/tmp/app-key.pem")
        );
    }

    #[test]
    fn tls_termination_listener_requires_certificate_entries() {
        let error = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (TLS_TERMINATION_LISTEN_ADDR, "127.0.0.1:8443"),
        ])
        .expect_err("TLS certs are required when TLS termination is enabled");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::Missing {
                name: TLS_TERMINATION_CERTS
            }
        ));
    }

    #[test]
    fn tls_certificate_entries_require_tls_termination_listener() {
        let error = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (
                TLS_TERMINATION_CERTS,
                "app.example.com|/tmp/app-cert.pem|/tmp/app-key.pem",
            ),
        ])
        .expect_err("TLS certs without listener are invalid");

        assert!(matches!(
            error,
            FrontlineEnvConfigError::Missing {
                name: TLS_TERMINATION_LISTEN_ADDR
            }
        ));
    }

    #[test]
    fn invalid_tls_values_are_reported() {
        let invalid_addr = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (TLS_PASSTHROUGH_LISTEN_ADDR, "not-an-addr"),
        ])
        .expect_err("TLS passthrough addr is invalid");
        assert!(matches!(
            invalid_addr,
            FrontlineEnvConfigError::InvalidSocketAddr {
                name: TLS_PASSTHROUGH_LISTEN_ADDR,
                ..
            }
        ));

        let invalid_certs = FrontlineEnvConfig::from_vars([
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080"),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051"),
            (TLS_TERMINATION_LISTEN_ADDR, "127.0.0.1:8443"),
            (TLS_TERMINATION_CERTS, "app.example.com|/tmp/app-cert.pem"),
        ])
        .expect_err("TLS certificate entry is invalid");
        assert!(matches!(
            invalid_certs,
            FrontlineEnvConfigError::InvalidTlsCertificates {
                name: TLS_TERMINATION_CERTS,
                ..
            }
        ));
    }

    #[test]
    fn tls_certificate_store_loads_from_pem_files() {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["app.example.com".to_owned()])
                .expect("cert generates");
        let cert_path = unique_temp_path("cert.pem");
        let key_path = unique_temp_path("key.pem");
        fs::write(&cert_path, cert.pem()).expect("cert file writes");
        fs::write(&key_path, signing_key.serialize_pem()).expect("key file writes");

        let certs = format!(
            "app.example.com|{}|{}",
            cert_path.display(),
            key_path.display()
        );
        let config = FrontlineEnvConfig::from_vars(vec![
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080".to_owned()),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051".to_owned()),
            (TLS_TERMINATION_LISTEN_ADDR, "127.0.0.1:8443".to_owned()),
            (TLS_TERMINATION_CERTS, certs),
        ])
        .expect("config parses");

        let store = config
            .load_tls_certificate_store()
            .expect("store loads")
            .expect("store is enabled");
        assert!(store
            .resolve("app.example.com")
            .expect("store lookup succeeds")
            .is_some());

        let _ = fs::remove_file(cert_path);
        let _ = fs::remove_file(key_path);
    }

    #[test]
    fn tls_certificate_store_reports_missing_files() {
        let cert_path = unique_temp_path("missing-cert.pem");
        let key_path = unique_temp_path("missing-key.pem");
        let certs = format!(
            "app.example.com|{}|{}",
            cert_path.display(),
            key_path.display()
        );
        let config = FrontlineEnvConfig::from_vars(vec![
            (FRONTLINE_LISTEN_ADDR, "127.0.0.1:8080".to_owned()),
            (CONTROL_PLANE_ENDPOINT, "http://127.0.0.1:50051".to_owned()),
            (TLS_TERMINATION_LISTEN_ADDR, "127.0.0.1:8443".to_owned()),
            (TLS_TERMINATION_CERTS, certs),
        ])
        .expect("config parses");

        let error = config
            .load_tls_certificate_store()
            .expect_err("missing cert file is reported");
        assert!(matches!(
            error,
            FrontlineTlsCertificateLoadError::OpenCertificate { path, .. }
                if path == cert_path
        ));
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        std::env::temp_dir().join(format!(
            "frontline-config-test-{}-{}-{name}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
