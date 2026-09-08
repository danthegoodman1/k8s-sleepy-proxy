use std::{
    error::Error,
    fmt, io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use socket2::{SockRef, TcpKeepalive};
use tokio::{
    io::{self as tokio_io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::{timeout, Instant},
};

use crate::drain::{DrainError, DrainTracker};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpProxyConfig {
    pub connect_timeout: Duration,
    pub stream_idle_timeout: Duration,
    pub write_idle_timeout: Duration,
    pub tcp_keepalive: Duration,
}

impl Default for TcpProxyConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            stream_idle_timeout: Duration::from_secs(60 * 60),
            write_idle_timeout: Duration::from_secs(60),
            tcp_keepalive: Duration::from_secs(60),
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
        configure_tcp_keepalive(&client, self.config.tcp_keepalive)
            .map_err(TcpProxyError::Proxy)?;
        let upstream = self.connect_upstream(upstream_addr).await?;

        proxy_streams_with_timeouts(
            client,
            upstream,
            self.config.stream_idle_timeout,
            self.config.write_idle_timeout,
        )
        .await
        .map_err(TcpProxyError::Proxy)
    }

    async fn connect_upstream(
        &self,
        upstream_addr: SocketAddr,
    ) -> Result<TcpStream, TcpProxyError> {
        let stream = crate::connect_tcp(upstream_addr, self.config.connect_timeout)
            .await
            .map_err(|error| {
                if error.kind() == io::ErrorKind::TimedOut {
                    TcpProxyError::ConnectTimeout {
                        timeout: self.config.connect_timeout,
                    }
                } else {
                    TcpProxyError::Connect(error)
                }
            })?;
        configure_tcp_keepalive(&stream, self.config.tcp_keepalive)
            .map_err(TcpProxyError::Connect)?;
        Ok(stream)
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

pub async fn proxy_streams_with_idle_timeout<Client, Upstream>(
    client: Client,
    upstream: Upstream,
    idle_timeout: Duration,
) -> io::Result<TcpProxyStats>
where
    Client: AsyncRead + AsyncWrite + Unpin,
    Upstream: AsyncRead + AsyncWrite + Unpin,
{
    proxy_streams_with_timeouts(client, upstream, idle_timeout, idle_timeout).await
}

/// Session idleness and a pending write have independent clocks. Progress in
/// the other direction cannot keep a blocked writer alive indefinitely.
pub async fn proxy_streams_with_timeouts<Client, Upstream>(
    client: Client,
    upstream: Upstream,
    idle_timeout: Duration,
    write_timeout: Duration,
) -> io::Result<TcpProxyStats>
where
    Client: AsyncRead + AsyncWrite + Unpin,
    Upstream: AsyncRead + AsyncWrite + Unpin,
{
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let (mut upstream_read, mut upstream_write) = tokio::io::split(upstream);
    let idle_clock = IdleClock::new();
    let (client_to_upstream, upstream_to_client) = tokio::try_join!(
        copy_direction(
            &mut client_read,
            &mut upstream_write,
            idle_timeout,
            write_timeout,
            idle_clock.clone()
        ),
        copy_direction(
            &mut upstream_read,
            &mut client_write,
            idle_timeout,
            write_timeout,
            idle_clock
        ),
    )?;

    Ok(TcpProxyStats {
        client_to_upstream,
        upstream_to_client,
    })
}

pub fn configure_tcp_keepalive(stream: &TcpStream, idle: Duration) -> io::Result<()> {
    let keepalive = TcpKeepalive::new().with_time(idle);
    SockRef::from(stream).set_tcp_keepalive(&keepalive)
}

async fn copy_direction<R, W>(
    reader: &mut R,
    writer: &mut W,
    idle_timeout: Duration,
    write_timeout: Duration,
    idle_clock: IdleClock,
) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut total = 0_u64;
    let mut buffer = [0_u8; 16 * 1024];

    loop {
        let read = wait_for_progress(reader.read(&mut buffer), idle_timeout, &idle_clock).await?;
        if read == 0 {
            timeout(
                write_timeout,
                wait_for_progress(writer.shutdown(), idle_timeout, &idle_clock),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TCP shutdown write stalled"))??;
            return Ok(total);
        }

        idle_clock.record_activity();
        let mut written = 0;
        while written < read {
            let count = timeout(
                write_timeout,
                wait_for_progress(
                    writer.write(&buffer[written..read]),
                    idle_timeout,
                    &idle_clock,
                ),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TCP write stalled"))??;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "stream stopped accepting bytes",
                ));
            }
            written += count;
            idle_clock.record_activity();
        }
        total += read as u64;
    }
}

async fn wait_for_progress<F, T>(
    future: F,
    idle_timeout: Duration,
    idle_clock: &IdleClock,
) -> io::Result<T>
where
    F: std::future::Future<Output = io::Result<T>>,
{
    tokio::pin!(future);
    loop {
        match timeout(idle_clock.remaining(idle_timeout), &mut future).await {
            Ok(result) => return result,
            Err(_) if idle_clock.is_idle_for(idle_timeout) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "stream idle timeout elapsed",
                ));
            }
            Err(_) => {}
        }
    }
}

#[derive(Clone, Debug)]
struct IdleClock {
    started_at: Instant,
    last_activity_nanos: Arc<AtomicU64>,
}

impl IdleClock {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            last_activity_nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record_activity(&self) {
        self.last_activity_nanos
            .store(self.elapsed_nanos(), Ordering::Relaxed);
    }

    fn remaining(&self, idle_timeout: Duration) -> Duration {
        idle_timeout
            .checked_sub(self.idle_for())
            .unwrap_or(Duration::ZERO)
    }

    fn is_idle_for(&self, idle_timeout: Duration) -> bool {
        self.idle_for() >= idle_timeout
    }

    fn idle_for(&self) -> Duration {
        Duration::from_nanos(
            self.elapsed_nanos()
                .saturating_sub(self.last_activity_nanos.load(Ordering::Relaxed)),
        )
    }

    fn elapsed_nanos(&self) -> u64 {
        self.started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }
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
