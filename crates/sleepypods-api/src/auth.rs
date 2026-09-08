use std::{error::Error, fmt};
use tonic::{metadata::AsciiMetadataValue, service::Interceptor, Request, Status};

#[derive(Clone, PartialEq, Eq)]
pub struct BearerToken {
    value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidBearerToken {
    Empty { field: &'static str },
    ContainsNonAscii { field: &'static str },
    ContainsWhitespace { field: &'static str },
    ContainsControl { field: &'static str },
}

#[derive(Clone, Debug)]
pub struct OptionalBearerTokenInterceptor {
    authorization: Option<AsciiMetadataValue>,
}

impl BearerToken {
    pub fn new(field: &'static str, value: impl Into<String>) -> Result<Self, InvalidBearerToken> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidBearerToken::Empty { field });
        }
        if !value.is_ascii() {
            return Err(InvalidBearerToken::ContainsNonAscii { field });
        }
        if value.chars().any(char::is_whitespace) {
            return Err(InvalidBearerToken::ContainsWhitespace { field });
        }
        if value.chars().any(char::is_control) {
            return Err(InvalidBearerToken::ContainsControl { field });
        }

        Ok(Self { value })
    }

    pub fn authorization_header_value(&self) -> Result<AsciiMetadataValue, InvalidBearerToken> {
        format!("Bearer {}", self.value)
            .parse()
            .map_err(|_| InvalidBearerToken::ContainsControl {
                field: "authorization",
            })
    }

    /// Explicit access for credential validation and secret projection. Debug stays redacted.
    pub fn as_secret_str(&self) -> &str {
        &self.value
    }
}

impl OptionalBearerTokenInterceptor {
    pub fn new(token: Option<&BearerToken>) -> Result<Self, InvalidBearerToken> {
        let authorization = token
            .map(BearerToken::authorization_header_value)
            .transpose()?;

        Ok(Self { authorization })
    }
}

impl Interceptor for OptionalBearerTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(authorization) = self.authorization.clone() {
            request
                .metadata_mut()
                .insert("authorization", authorization);
        }

        Ok(request)
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken([redacted])")
    }
}

impl fmt::Display for InvalidBearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(f, "{field} must not be empty"),
            Self::ContainsNonAscii { field } => write!(f, "{field} must contain only ASCII"),
            Self::ContainsWhitespace { field } => write!(f, "{field} must not contain whitespace"),
            Self::ContainsControl { field } => {
                write!(f, "{field} must not contain control characters")
            }
        }
    }
}

impl Error for InvalidBearerToken {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_token_validation_redaction_and_interceptor_are_shared() {
        for invalid in ["", "two words", "token\n", "café"] {
            assert!(BearerToken::new("client_token", invalid).is_err());
        }
        let token = BearerToken::new("client_token", "private-token").unwrap();
        assert_eq!(format!("{token:?}"), "BearerToken([redacted])");
        let mut interceptor = OptionalBearerTokenInterceptor::new(Some(&token)).unwrap();
        let request = interceptor.call(Request::new(())).unwrap();
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer private-token"
        );
        let mut anonymous = OptionalBearerTokenInterceptor::new(None).unwrap();
        assert!(!anonymous
            .call(Request::new(()))
            .unwrap()
            .metadata()
            .contains_key("authorization"));
    }
}
