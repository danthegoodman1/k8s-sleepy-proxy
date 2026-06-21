use std::{error::Error, fmt, io, net::SocketAddr, time::Duration};

use tokio::{
    io::{self as tokio_io, AsyncRead, AsyncWrite},
    net::TcpStream,
};

use crate::drain::{DrainError, DrainTracker};
use crate::timeout::with_timeout;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpProxyConfig {
    pub connect_timeout: Duration,
}

impl Default for TcpProxyConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TcpProxy {
    drain: DrainTracker,
    config: TcpProxyConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpProxyStats {
    pub client_to_upstream: u64,
    pub upstream_to_client: u64,
}

#[derive(Debug)]
pub enum TcpProxyError {
    Drain(DrainError),
    ConnectTimeout { timeout: Duration },
    Connect(io::Error),
    Proxy(io::Error),
}

impl TcpProxy {
    pub fn new(drain: DrainTracker, config: TcpProxyConfig) -> Self {
        Self { drain, config }
    }

    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    pub fn config(&self) -> &TcpProxyConfig {
        &self.config
    }

    pub async fn proxy(
        &self,
        client: TcpStream,
        upstream_addr: SocketAddr,
    ) -> Result<TcpProxyStats, TcpProxyError> {
        let _permit = self.drain.try_acquire()?;
        let upstream = self.connect_upstream(upstream_addr).await?;

        proxy_streams(client, upstream)
            .await
            .map_err(TcpProxyError::Proxy)
    }

    async fn connect_upstream(
        &self,
        upstream_addr: SocketAddr,
    ) -> Result<TcpStream, TcpProxyError> {
        match with_timeout(
            self.config.connect_timeout,
            TcpStream::connect(upstream_addr),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(error)) => Err(TcpProxyError::Connect(error)),
            Err(_) => Err(TcpProxyError::ConnectTimeout {
                timeout: self.config.connect_timeout,
            }),
        }
    }
}

pub async fn proxy_streams<Client, Upstream>(
    mut client: Client,
    mut upstream: Upstream,
) -> io::Result<TcpProxyStats>
where
    Client: AsyncRead + AsyncWrite + Unpin,
    Upstream: AsyncRead + AsyncWrite + Unpin,
{
    let (client_to_upstream, upstream_to_client) =
        tokio_io::copy_bidirectional(&mut client, &mut upstream).await?;

    Ok(TcpProxyStats {
        client_to_upstream,
        upstream_to_client,
    })
}

impl From<DrainError> for TcpProxyError {
    fn from(error: DrainError) -> Self {
        Self::Drain(error)
    }
}

impl fmt::Display for TcpProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drain(error) => write!(f, "{error}"),
            Self::ConnectTimeout { timeout } => {
                write!(f, "upstream connect timed out after {timeout:?}")
            }
            Self::Connect(error) => write!(f, "upstream connect failed: {error}"),
            Self::Proxy(error) => write!(f, "tcp proxy failed: {error}"),
        }
    }
}

impl Error for TcpProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Drain(error) => Some(error),
            Self::ConnectTimeout { .. } => None,
            Self::Connect(error) | Self::Proxy(error) => Some(error),
        }
    }
}
