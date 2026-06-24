use std::{error::Error, fmt};

use futures_util::{SinkExt, StreamExt};
use http::{HeaderMap, Request as HttpRequest, Response};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{
    accept_hdr_async, connect_async,
    tungstenite::{
        client::IntoClientRequest,
        handshake::{
            client::Request as WsClientRequest,
            server::{create_response_with_body, Request as WsServerRequest},
        },
        protocol::Role,
    },
    tungstenite::{Error as TungsteniteError, Message},
    WebSocketStream,
};

use crate::{
    drain::{DrainError, DrainTracker},
    http::forwarded_headers,
};

#[derive(Clone, Debug)]
pub struct WebSocketProxy {
    drain: DrainTracker,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebSocketProxyStats {
    pub client_to_upstream_messages: u64,
    pub upstream_to_client_messages: u64,
    pub client_to_upstream_bytes: u64,
    pub upstream_to_client_bytes: u64,
}

#[derive(Debug)]
pub enum WebSocketProxyError {
    Drain(DrainError),
    ClientHandshake(TungsteniteError),
    UpstreamConnect(TungsteniteError),
    Proxy(TungsteniteError),
}

impl WebSocketProxy {
    pub fn new(drain: DrainTracker) -> Self {
        Self { drain }
    }

    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    pub async fn accept_and_proxy<Client>(
        &self,
        client: Client,
        upstream_url: &str,
    ) -> Result<WebSocketProxyStats, WebSocketProxyError>
    where
        Client: AsyncRead + AsyncWrite + Unpin,
    {
        let _permit = self.drain.try_acquire()?;
        let mut upstream_headers = HeaderMap::new();
        let client = accept_hdr_async(client, |request: &WsServerRequest, response| {
            upstream_headers = forwarded_headers(request.headers());
            Ok(response)
        })
        .await
        .map_err(WebSocketProxyError::ClientHandshake)?;
        let upstream_request = upstream_websocket_request(upstream_url, &upstream_headers)
            .map_err(WebSocketProxyError::UpstreamConnect)?;
        let (upstream, _) = connect_async(upstream_request)
            .await
            .map_err(WebSocketProxyError::UpstreamConnect)?;

        proxy_websocket_streams(client, upstream)
            .await
            .map_err(WebSocketProxyError::Proxy)
    }

    pub async fn proxy_accepted_upgrade<Client>(
        &self,
        client: Client,
        upstream_url: &str,
    ) -> Result<WebSocketProxyStats, WebSocketProxyError>
    where
        Client: AsyncRead + AsyncWrite + Unpin,
    {
        let _permit = self.drain.try_acquire()?;
        let client = WebSocketStream::from_raw_socket(client, Role::Server, None).await;
        let (upstream, _) = connect_async(upstream_url)
            .await
            .map_err(WebSocketProxyError::UpstreamConnect)?;

        proxy_websocket_streams(client, upstream)
            .await
            .map_err(WebSocketProxyError::Proxy)
    }

    pub async fn proxy_accepted_upgrade_with_upstream_headers<Client>(
        &self,
        client: Client,
        upstream_url: &str,
        upstream_headers: &HeaderMap,
    ) -> Result<WebSocketProxyStats, WebSocketProxyError>
    where
        Client: AsyncRead + AsyncWrite + Unpin,
    {
        let _permit = self.drain.try_acquire()?;
        let client = WebSocketStream::from_raw_socket(client, Role::Server, None).await;
        let upstream_request = upstream_websocket_request(upstream_url, upstream_headers)
            .map_err(WebSocketProxyError::UpstreamConnect)?;
        let (upstream, _) = connect_async(upstream_request)
            .await
            .map_err(WebSocketProxyError::UpstreamConnect)?;

        proxy_websocket_streams(client, upstream)
            .await
            .map_err(WebSocketProxyError::Proxy)
    }
}

pub fn websocket_upgrade_response<B, R>(
    request: &HttpRequest<B>,
    body: R,
) -> Result<Response<R>, WebSocketProxyError> {
    create_response_with_body(request, || body).map_err(WebSocketProxyError::ClientHandshake)
}

fn upstream_websocket_request(
    upstream_url: &str,
    upstream_headers: &HeaderMap,
) -> Result<WsClientRequest, TungsteniteError> {
    let mut request = upstream_url.into_client_request()?;
    for (name, value) in upstream_headers.iter() {
        request.headers_mut().append(name.clone(), value.clone());
    }
    Ok(request)
}

pub async fn proxy_websocket_streams<Client, Upstream>(
    mut client: WebSocketStream<Client>,
    mut upstream: WebSocketStream<Upstream>,
) -> Result<WebSocketProxyStats, TungsteniteError>
where
    Client: AsyncRead + AsyncWrite + Unpin,
    Upstream: AsyncRead + AsyncWrite + Unpin,
{
    let mut stats = WebSocketProxyStats::default();

    loop {
        tokio::select! {
            message = client.next() => {
                let Some(message) = message else {
                    let _ = upstream.close(None).await;
                    return Ok(stats);
                };

                let message = message?;
                let is_close = message.is_close();
                stats.client_to_upstream_messages += 1;
                stats.client_to_upstream_bytes += message_payload_len(&message) as u64;
                upstream.send(message).await?;

                if is_close {
                    flush_close_reply(&mut client).await?;
                    wait_for_close_response(&mut upstream).await?;
                    return Ok(stats);
                }
            }
            message = upstream.next() => {
                let Some(message) = message else {
                    let _ = client.close(None).await;
                    return Ok(stats);
                };

                let message = message?;
                let is_close = message.is_close();
                stats.upstream_to_client_messages += 1;
                stats.upstream_to_client_bytes += message_payload_len(&message) as u64;
                client.send(message).await?;

                if is_close {
                    flush_close_reply(&mut upstream).await?;
                    wait_for_close_response(&mut client).await?;
                    return Ok(stats);
                }
            }
        }
    }
}

async fn flush_close_reply<S>(websocket: &mut WebSocketStream<S>) -> Result<(), TungsteniteError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match websocket.flush().await {
        Ok(()) | Err(TungsteniteError::ConnectionClosed) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn wait_for_close_response<S>(
    websocket: &mut WebSocketStream<S>,
) -> Result<(), TungsteniteError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match websocket.next().await {
            Some(Ok(message)) if message.is_close() => return Ok(()),
            Some(Ok(_)) => continue,
            Some(Err(TungsteniteError::ConnectionClosed)) | None => return Ok(()),
            Some(Err(error)) => return Err(error),
        }
    }
}

fn message_payload_len(message: &Message) -> usize {
    match message {
        Message::Text(payload) => payload.len(),
        Message::Binary(payload) | Message::Ping(payload) | Message::Pong(payload) => payload.len(),
        Message::Close(close) => close
            .as_ref()
            .map(|frame| frame.reason.len())
            .unwrap_or_default(),
        Message::Frame(frame) => frame.payload().len(),
    }
}

impl From<DrainError> for WebSocketProxyError {
    fn from(error: DrainError) -> Self {
        Self::Drain(error)
    }
}

impl fmt::Display for WebSocketProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drain(error) => write!(f, "{error}"),
            Self::ClientHandshake(error) => write!(f, "websocket client handshake failed: {error}"),
            Self::UpstreamConnect(error) => write!(f, "websocket upstream connect failed: {error}"),
            Self::Proxy(error) => write!(f, "websocket proxy failed: {error}"),
        }
    }
}

impl Error for WebSocketProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Drain(error) => Some(error),
            Self::ClientHandshake(error) | Self::UpstreamConnect(error) | Self::Proxy(error) => {
                Some(error)
            }
        }
    }
}
