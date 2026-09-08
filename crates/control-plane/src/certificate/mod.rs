//! Certificate validation has no store or sealing dependency. It can be reused
//! by a receiving TLS adapter without giving that adapter persistence access.
mod seal;
mod validate;
pub(crate) use seal::SealedPrivateKey;
pub use seal::{CertificateSealer, SealingKey};
pub use sleepypods_api::certificate::*;
pub use validate::{validate_certificate, validate_hostname, ValidatedCertificate};

#[cfg(test)]
mod tests;

pub(crate) mod work;
