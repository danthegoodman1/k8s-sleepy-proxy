//! Platform transport identity and external sealing configuration. Neither is
//! resolved from application TLS bindings or persisted as workload data.
use crate::certificate::{CertificateSealer, SealingKey};
use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
use zeroize::Zeroizing;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeSecurityConfig {
    pub certificate_file: Option<PathBuf>,
    pub private_key_file: Option<PathBuf>,
    pub sealing_keys_file: Option<PathBuf>,
    pub public_endpoint: Option<String>,
    pub ca_pem: Option<String>,
}
impl RuntimeSecurityConfig {
    pub(crate) fn parse(
        values: &HashMap<String, String>,
        authenticated: bool,
    ) -> Result<Self, &'static str> {
        let get = |name: &str| values.get(name).filter(|v| !v.is_empty()).cloned();
        let config = Self {
            certificate_file: get("SLEEPYPODS_CONTROL_PLANE_TLS_CERT_FILE").map(PathBuf::from),
            private_key_file: get("SLEEPYPODS_CONTROL_PLANE_TLS_KEY_FILE").map(PathBuf::from),
            sealing_keys_file: get("SLEEPYPODS_CERTIFICATE_SEALING_KEYS_FILE").map(PathBuf::from),
            public_endpoint: get("SLEEPYPODS_CONTROL_PLANE_PUBLIC_ENDPOINT"),
            ca_pem: get(sleepypods_api::transport::CONTROL_PLANE_TLS_CA_PEM_ENV),
        };
        if config.certificate_file.is_some() != config.private_key_file.is_some() {
            return Err("native TLS requires both identity files");
        }
        if config.sealing_keys_file.is_some()
            && (!authenticated || config.certificate_file.is_none())
        {
            return Err("sealing keys require authenticated native TLS");
        }
        if config.certificate_file.is_some()
            && !config
                .public_endpoint
                .as_deref()
                .is_some_and(|v| v.starts_with("https://"))
        {
            return Err("native TLS requires a verified HTTPS public endpoint for sidecars");
        }
        if let Some(endpoint) = &config.public_endpoint {
            sleepypods_api::transport::native_endpoint(endpoint.clone(), config.ca_pem.as_deref())
                .map_err(|_| "invalid public endpoint or CA trust")?;
        } else if config.ca_pem.is_some() {
            return Err("public CA trust requires a public endpoint");
        }
        Ok(config)
    }
    pub(crate) fn tls(
        &self,
    ) -> Result<Option<tonic::transport::ServerTlsConfig>, Box<dyn std::error::Error + Send + Sync>>
    {
        match (&self.certificate_file, &self.private_key_file) {
            (None, None) => Ok(None),
            (Some(cert), Some(key)) => {
                let cert = read_bounded(cert, 64 * 1024)?;
                let key = read_bounded(key, 16 * 1024)?;
                Ok(Some(tonic::transport::ServerTlsConfig::new().identity(
                    tonic::transport::Identity::from_pem(cert.as_slice(), key.as_slice()),
                )))
            }
            _ => Err("native TLS requires both identity files".into()),
        }
    }
    pub(crate) fn sealer(
        &self,
    ) -> Result<Option<Arc<CertificateSealer>>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(path) = &self.sealing_keys_file else {
            return Ok(None);
        };
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Key {
            id: String,
            key_hex: Zeroizing<String>,
        }
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Keys {
            active_id: String,
            keys: Vec<Key>,
        }
        // Deserialize through a bounded buffer; never include serde's input
        // context or the file contents in diagnostics.
        let raw = read_bounded(path, 8192)?;
        let value: Keys = serde_json::from_slice(&raw).map_err(|_| "invalid sealing key file")?;
        let mut keys = Vec::new();
        for entry in value.keys {
            let key_hex = entry.key_hex;
            if key_hex.len() != 64 {
                return Err("sealing keys require 32-byte hexadecimal values".into());
            }
            let mut bytes = Zeroizing::new([0u8; 32]);
            for (i, pair) in key_hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
                let hex = |b: u8| match b {
                    b'0'..=b'9' => Some(b - b'0'),
                    b'a'..=b'f' => Some(b - b'a' + 10),
                    b'A'..=b'F' => Some(b - b'A' + 10),
                    _ => None,
                };
                bytes[i] = (hex(pair[0]).ok_or("invalid sealing key encoding")? << 4)
                    | hex(pair[1]).ok_or("invalid sealing key encoding")?;
            }
            keys.push(SealingKey::new(entry.id, *bytes)?);
        }
        Ok(Some(Arc::new(CertificateSealer::new(
            value.active_id,
            keys,
        )?)))
    }
}
fn read_bounded(
    path: &Path,
    limit: usize,
) -> Result<Zeroizing<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    let file = std::fs::File::open(path).map_err(|_| "cannot open platform security file")?;
    let mut bytes = Zeroizing::new(Vec::new());
    file.take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read platform security file")?;
    if bytes.is_empty() || bytes.len() > limit {
        return Err("platform security file exceeds size bound".into());
    }
    Ok(bytes)
}
