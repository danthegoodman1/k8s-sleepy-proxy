use std::{
    error::Error,
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use http::{
    header::CONNECTION, uri::InvalidUriParts, HeaderMap, HeaderName, Request, Response, Uri,
};
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{Client, Error as ClientError},
    rt::TokioExecutor,
};
use pin_project_lite::pin_project;

use crate::drain::{DrainError, DrainPermit, DrainTracker};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Debug)]
pub struct HttpProxy {
    drain: DrainTracker,
}

#[derive(Debug)]
pub enum HttpProxyError {
    Drain(DrainError),
    RequestRewrite(ReverseProxyRequestError),
    Client(ClientError),
}

#[derive(Debug)]
pub enum ReverseProxyRequestError {
    MissingUpstreamScheme,
    MissingUpstreamAuthority,
    InvalidUri(InvalidUriParts),
}

impl HttpProxy {
    pub fn new(drain: DrainTracker) -> Self {
        Self { drain }
    }

    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    pub async fn proxy<B>(
        &self,
        request: Request<B>,
        upstream_origin: &Uri,
    ) -> Result<Response<TrackedBody<Incoming>>, HttpProxyError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<BoxError>,
    {
        let permit = self.drain.try_acquire()?;
        let use_http2_upstream = request.version() == http::Version::HTTP_2;
        let request = prepare_reverse_proxy_request(request, upstream_origin)?;
        let mut builder = Client::builder(TokioExecutor::new());
        if use_http2_upstream {
            // h2c has no ALPN signal, so HTTP/2 inbound requests use prior
            // knowledge when dialing a cleartext upstream.
            builder.http2_only(true);
        }
        let client = builder.build_http();
        let mut response = client.request(request).await?;

        strip_hop_by_hop_headers(response.headers_mut());

        Ok(response.map(|body| TrackedBody::new(body, permit)))
    }
}

pub fn prepare_reverse_proxy_request<B>(
    mut request: Request<B>,
    upstream_origin: &Uri,
) -> Result<Request<B>, ReverseProxyRequestError> {
    *request.uri_mut() = upstream_request_uri(upstream_origin, request.uri())?;
    strip_hop_by_hop_headers(request.headers_mut());
    Ok(request)
}

pub fn upstream_request_uri(
    upstream_origin: &Uri,
    original_uri: &Uri,
) -> Result<Uri, ReverseProxyRequestError> {
    let Some(scheme) = upstream_origin.scheme().cloned() else {
        return Err(ReverseProxyRequestError::MissingUpstreamScheme);
    };
    let Some(authority) = upstream_origin.authority().cloned() else {
        return Err(ReverseProxyRequestError::MissingUpstreamAuthority);
    };

    let mut parts = Uri::default().into_parts();
    parts.scheme = Some(scheme);
    parts.authority = Some(authority);
    parts.path_and_query = original_uri
        .path_and_query()
        .cloned()
        .or_else(|| Some(http::uri::PathAndQuery::from_static("/")));

    Uri::from_parts(parts).map_err(ReverseProxyRequestError::InvalidUri)
}

pub fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    let connection_tokens = connection_header_tokens(headers);

    for name in connection_tokens {
        headers.remove(name);
    }

    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn connection_header_tokens(headers: &HeaderMap) -> Vec<HeaderName> {
    headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|token| {
            let token = token.trim();
            if token.is_empty() {
                return None;
            }

            HeaderName::from_bytes(token.as_bytes()).ok()
        })
        .collect()
}

pin_project! {
    #[derive(Debug)]
    pub struct TrackedBody<B> {
        #[pin]
        inner: B,
        permit: Option<DrainPermit>,
    }
}

impl<B> TrackedBody<B> {
    pub fn new(inner: B, permit: DrainPermit) -> Self {
        Self {
            inner,
            permit: Some(permit),
        }
    }
}

impl<B> Body for TrackedBody<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let poll = this.inner.poll_frame(cx);

        if matches!(poll, Poll::Ready(None)) {
            if let Some(mut permit) = this.permit.take() {
                permit.release();
            }
        }

        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl From<DrainError> for HttpProxyError {
    fn from(error: DrainError) -> Self {
        Self::Drain(error)
    }
}

impl From<ReverseProxyRequestError> for HttpProxyError {
    fn from(error: ReverseProxyRequestError) -> Self {
        Self::RequestRewrite(error)
    }
}

impl From<ClientError> for HttpProxyError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl fmt::Display for HttpProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drain(error) => write!(f, "{error}"),
            Self::RequestRewrite(error) => write!(f, "http request rewrite failed: {error}"),
            Self::Client(error) => write!(f, "http proxy request failed: {error}"),
        }
    }
}

impl Error for HttpProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Drain(error) => Some(error),
            Self::RequestRewrite(error) => Some(error),
            Self::Client(error) => Some(error),
        }
    }
}

impl fmt::Display for ReverseProxyRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingUpstreamScheme => write!(f, "upstream origin is missing a scheme"),
            Self::MissingUpstreamAuthority => {
                write!(f, "upstream origin is missing an authority")
            }
            Self::InvalidUri(error) => write!(f, "invalid upstream request URI: {error}"),
        }
    }
}

impl Error for ReverseProxyRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUri(error) => Some(error),
            Self::MissingUpstreamScheme | Self::MissingUpstreamAuthority => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use http::{Method, Request};

    use super::{
        prepare_reverse_proxy_request, strip_hop_by_hop_headers, upstream_request_uri,
        ReverseProxyRequestError,
    };

    #[test]
    fn request_rewrite_preserves_method_path_query_and_forwardable_headers() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/things?name=value")
            .header("host", "app.example.test")
            .header("x-forwarded-value", "kept")
            .header("connection", "x-remove, upgrade")
            .header("x-remove", "drop")
            .header("upgrade", "websocket")
            .body(())
            .expect("request builds");
        let upstream = "http://127.0.0.1:8080".parse().expect("valid upstream");

        let rewritten =
            prepare_reverse_proxy_request(request, &upstream).expect("request rewrites");

        assert_eq!(rewritten.method(), Method::POST);
        assert_eq!(
            rewritten.uri().to_string(),
            "http://127.0.0.1:8080/api/things?name=value"
        );
        assert_eq!(
            rewritten.headers().get("host").expect("host is preserved"),
            "app.example.test"
        );
        assert_eq!(
            rewritten
                .headers()
                .get("x-forwarded-value")
                .expect("forwardable header is preserved"),
            "kept"
        );
        assert!(rewritten.headers().get("connection").is_none());
        assert!(rewritten.headers().get("upgrade").is_none());
        assert!(rewritten.headers().get("x-remove").is_none());
    }

    #[test]
    fn request_rewrite_uses_root_path_when_original_uri_has_no_path() {
        let upstream = "http://127.0.0.1:8080".parse().expect("valid upstream");
        let original = "http://incoming.example.test"
            .parse()
            .expect("absolute URI without explicit path");

        let rewritten = upstream_request_uri(&upstream, &original).expect("uri rewrites");

        assert_eq!(rewritten.to_string(), "http://127.0.0.1:8080/");
    }

    #[test]
    fn request_rewrite_requires_absolute_upstream_origin() {
        let upstream = "/relative".parse().expect("relative URI parses");
        let original = "/".parse().expect("relative URI parses");

        assert!(matches!(
            upstream_request_uri(&upstream, &original).expect_err("scheme is required"),
            ReverseProxyRequestError::MissingUpstreamScheme
        ));
    }

    #[test]
    fn strip_removes_standard_and_connection_named_hop_by_hop_headers() {
        let mut request = Request::builder()
            .uri("/")
            .header("connection", "x-drop-one, x-drop-two")
            .header("x-drop-one", "no")
            .header("x-drop-two", "no")
            .header("transfer-encoding", "chunked")
            .header("x-keep", "yes")
            .body(())
            .expect("request builds");

        strip_hop_by_hop_headers(request.headers_mut());

        assert!(request.headers().get("connection").is_none());
        assert!(request.headers().get("x-drop-one").is_none());
        assert!(request.headers().get("x-drop-two").is_none());
        assert!(request.headers().get("transfer-encoding").is_none());
        assert_eq!(request.headers().get("x-keep").expect("kept"), "yes");
    }
}
