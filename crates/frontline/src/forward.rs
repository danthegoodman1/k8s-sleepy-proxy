use std::{error::Error, fmt};

use bytes::Bytes;
use http::{
    uri::{InvalidUri, InvalidUriParts, PathAndQuery},
    Request, Response, Uri,
};
use http_body::Body;
use hyper::body::Incoming;
use proxy_core::{
    DrainTracker, HttpProxy, HttpProxyError, TrackedBody, WebSocketProxy, WebSocketProxyError,
    WebSocketProxyStats,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::wake::ReadyBackend;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug)]
pub struct FrontlineForwarder {
    http: HttpProxy,
    websocket: WebSocketProxy,
}

#[derive(Debug)]
pub enum FrontlineForwardError {
    Backend(BackendForwardError),
    Http(HttpProxyError),
    WebSocket(WebSocketProxyError),
}

#[derive(Debug)]
pub enum BackendForwardError {
    InvalidUri(InvalidUri),
    MissingScheme,
    MissingAuthority,
    UnsupportedHttpScheme(String),
    UnsupportedWebSocketScheme(String),
    InvalidWebSocketPath(InvalidUri),
    InvalidWebSocketUri(InvalidUriParts),
}

impl FrontlineForwarder {
    pub fn new(drain: DrainTracker) -> Self {
        Self {
            http: HttpProxy::new(drain.clone()),
            websocket: WebSocketProxy::new(drain),
        }
    }

    pub async fn forward_http<B>(
        &self,
        ready: &ReadyBackend,
        request: Request<B>,
    ) -> Result<Response<TrackedBody<Incoming>>, FrontlineForwardError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        let upstream_origin = http_upstream_origin(ready)?;
        self.http
            .proxy(request, &upstream_origin)
            .await
            .map_err(FrontlineForwardError::Http)
    }

    pub async fn forward_websocket<Client>(
        &self,
        ready: &ReadyBackend,
        client: Client,
        upstream_path_and_query: &str,
    ) -> Result<WebSocketProxyStats, FrontlineForwardError>
    where
        Client: AsyncRead + AsyncWrite + Unpin,
    {
        let upstream_url = websocket_upstream_url(ready, upstream_path_and_query)?;
        self.websocket
            .accept_and_proxy(client, &upstream_url)
            .await
            .map_err(FrontlineForwardError::WebSocket)
    }

    pub async fn forward_accepted_websocket<Client>(
        &self,
        ready: &ReadyBackend,
        client: Client,
        upstream_path_and_query: &str,
    ) -> Result<WebSocketProxyStats, FrontlineForwardError>
    where
        Client: AsyncRead + AsyncWrite + Unpin,
    {
        let upstream_url = websocket_upstream_url(ready, upstream_path_and_query)?;
        self.websocket
            .proxy_accepted_upgrade(client, &upstream_url)
            .await
            .map_err(FrontlineForwardError::WebSocket)
    }
}

pub fn http_upstream_origin(ready: &ReadyBackend) -> Result<Uri, BackendForwardError> {
    let uri = parse_backend_uri(ready)?;
    let scheme = uri.scheme_str().ok_or(BackendForwardError::MissingScheme)?;
    if scheme != "http" {
        return Err(BackendForwardError::UnsupportedHttpScheme(
            scheme.to_string(),
        ));
    }
    require_authority(&uri)?;
    Ok(uri)
}

pub fn websocket_upstream_url(
    ready: &ReadyBackend,
    upstream_path_and_query: &str,
) -> Result<String, BackendForwardError> {
    let uri = parse_backend_uri(ready)?;
    let scheme = match uri.scheme_str().ok_or(BackendForwardError::MissingScheme)? {
        "http" | "ws" => "ws",
        unsupported => {
            return Err(BackendForwardError::UnsupportedWebSocketScheme(
                unsupported.to_string(),
            ));
        }
    };
    let authority = require_authority(&uri)?.clone();
    let path_and_query = if upstream_path_and_query.is_empty() {
        PathAndQuery::from_static("/")
    } else {
        upstream_path_and_query
            .parse()
            .map_err(BackendForwardError::InvalidWebSocketPath)?
    };

    let mut parts = Uri::default().into_parts();
    parts.scheme = Some(scheme.parse().expect("static scheme is valid"));
    parts.authority = Some(authority);
    parts.path_and_query = Some(path_and_query);

    Uri::from_parts(parts)
        .map(|uri| uri.to_string())
        .map_err(BackendForwardError::InvalidWebSocketUri)
}

fn parse_backend_uri(ready: &ReadyBackend) -> Result<Uri, BackendForwardError> {
    ready
        .backend
        .uri()
        .parse()
        .map_err(BackendForwardError::InvalidUri)
}

fn require_authority(uri: &Uri) -> Result<&http::uri::Authority, BackendForwardError> {
    uri.authority().ok_or(BackendForwardError::MissingAuthority)
}

impl From<BackendForwardError> for FrontlineForwardError {
    fn from(error: BackendForwardError) -> Self {
        Self::Backend(error)
    }
}

impl fmt::Display for FrontlineForwardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "backend forwarding target is invalid: {error}"),
            Self::Http(error) => write!(f, "{error}"),
            Self::WebSocket(error) => write!(f, "{error}"),
        }
    }
}

impl Error for FrontlineForwardError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Backend(error) => Some(error),
            Self::Http(error) => Some(error),
            Self::WebSocket(error) => Some(error),
        }
    }
}

impl fmt::Display for BackendForwardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUri(error) => write!(f, "backend URI is invalid: {error}"),
            Self::MissingScheme => write!(f, "backend URI is missing a scheme"),
            Self::MissingAuthority => write!(f, "backend URI is missing an authority"),
            Self::UnsupportedHttpScheme(scheme) => {
                write!(f, "backend URI scheme {scheme:?} is not supported for HTTP")
            }
            Self::UnsupportedWebSocketScheme(scheme) => {
                write!(
                    f,
                    "backend URI scheme {scheme:?} is not supported for WebSocket"
                )
            }
            Self::InvalidWebSocketPath(error) => {
                write!(f, "websocket upstream path/query is invalid: {error}")
            }
            Self::InvalidWebSocketUri(error) => {
                write!(f, "websocket upstream URI is invalid: {error}")
            }
        }
    }
}

impl Error for BackendForwardError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUri(error) | Self::InvalidWebSocketPath(error) => Some(error),
            Self::InvalidWebSocketUri(error) => Some(error),
            Self::MissingScheme
            | Self::MissingAuthority
            | Self::UnsupportedHttpScheme(_)
            | Self::UnsupportedWebSocketScheme(_) => None,
        }
    }
}

#[cfg(test)]
mod tests;
