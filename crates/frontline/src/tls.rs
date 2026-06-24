use std::{
    collections::HashMap,
    error::Error,
    fmt, io,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use http::Uri;
use proxy_core::{
    proxy_streams, read_tls_client_hello_prefix, TcpProxyStats, TlsClientHelloError,
    TlsClientHelloSni,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::{
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer},
        ServerConfig,
    },
    LazyConfigAcceptor,
};

use crate::{ReadyBackend, RequestIdentityError, RouteRequestIdentity};

#[derive(Clone, Debug, Default)]
pub struct TlsCertificateStore {
    configs: Arc<RwLock<HashMap<String, Arc<ServerConfig>>>>,
}

#[derive(Clone, Debug)]
pub struct FrontlineTlsAdapter {
    certificates: TlsCertificateStore,
}

#[derive(Debug)]
pub struct TerminatedTls<IO> {
    pub identity: RouteRequestIdentity,
    pub stream: tokio_rustls::server::TlsStream<IO>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsPassthrough {
    pub identity: RouteRequestIdentity,
    pub stats: TcpProxyStats,
}

#[derive(Debug)]
pub struct TlsPassthroughClientHello {
    identity: RouteRequestIdentity,
    bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum TlsCertificateError {
    InvalidSni(RequestIdentityError),
    InvalidCertificate(rustls::Error),
    StorePoisoned,
}

#[derive(Debug)]
pub enum TlsTerminationError {
    ClientHello(io::Error),
    MissingSni,
    InvalidSni(RequestIdentityError),
    UnknownCertificate,
    Handshake(io::Error),
}

#[derive(Debug)]
pub enum TlsPassthroughError {
    Backend(TlsPassthroughBackendError),
    ClientHello(io::Error),
    NotTls(TlsClientHelloError),
    MissingSni,
    InvalidSni(RequestIdentityError),
    Connect(io::Error),
    Proxy(io::Error),
}

#[derive(Debug)]
pub enum TlsPassthroughBackendError {
    InvalidUri(http::uri::InvalidUri),
    MissingScheme,
    MissingAuthority,
    UnsupportedScheme(String),
    InvalidAuthority(String),
}

impl TlsCertificateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(
        &self,
        sni: impl AsRef<str>,
        cert_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> Result<(), TlsCertificateError> {
        let key = canonical_sni_key(sni.as_ref()).map_err(TlsCertificateError::InvalidSni)?;
        let mut config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(TlsCertificateError::InvalidCertificate)?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        self.configs
            .write()
            .map_err(|_| TlsCertificateError::StorePoisoned)?
            .insert(key, Arc::new(config));

        Ok(())
    }

    pub fn resolve(
        &self,
        sni: impl AsRef<str>,
    ) -> Result<Option<Arc<ServerConfig>>, TlsCertificateError> {
        let key = canonical_sni_key(sni.as_ref()).map_err(TlsCertificateError::InvalidSni)?;
        let config = self
            .configs
            .read()
            .map_err(|_| TlsCertificateError::StorePoisoned)?
            .get(&key)
            .cloned();

        Ok(config)
    }
}

impl FrontlineTlsAdapter {
    pub fn new(certificates: TlsCertificateStore) -> Self {
        Self { certificates }
    }

    pub fn certificates(&self) -> &TlsCertificateStore {
        &self.certificates
    }

    pub async fn terminate<IO>(&self, io: IO) -> Result<TerminatedTls<IO>, TlsTerminationError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let acceptor = LazyConfigAcceptor::new(rustls::server::Acceptor::default(), io);
        let start = acceptor.await.map_err(TlsTerminationError::ClientHello)?;
        let client_hello = start.client_hello();
        let sni = client_hello
            .server_name()
            .ok_or(TlsTerminationError::MissingSni)?;
        let identity = RouteRequestIdentity::sni(sni).map_err(TlsTerminationError::InvalidSni)?;
        let config = self
            .certificates
            .resolve(sni)
            .map_err(|error| match error {
                TlsCertificateError::InvalidSni(error) => TlsTerminationError::InvalidSni(error),
                TlsCertificateError::InvalidCertificate(error) => TlsTerminationError::Handshake(
                    io::Error::new(io::ErrorKind::InvalidData, error),
                ),
                TlsCertificateError::StorePoisoned => TlsTerminationError::Handshake(
                    io::Error::other("TLS certificate store lock is poisoned"),
                ),
            })?
            .ok_or(TlsTerminationError::UnknownCertificate)?;
        let stream = start
            .into_stream(config)
            .await
            .map_err(TlsTerminationError::Handshake)?;

        Ok(TerminatedTls { identity, stream })
    }

    pub async fn passthrough<Client>(
        &self,
        ready: &ReadyBackend,
        mut client: Client,
    ) -> Result<TlsPassthrough, TlsPassthroughError>
    where
        Client: AsyncRead + AsyncWrite + Unpin,
    {
        let client_hello = self.read_passthrough_client_hello(&mut client).await?;

        self.passthrough_prefixed(ready, client, client_hello).await
    }

    pub async fn read_passthrough_client_hello<Client>(
        &self,
        client: &mut Client,
    ) -> Result<TlsPassthroughClientHello, TlsPassthroughError>
    where
        Client: AsyncRead + Unpin,
    {
        let prefix = read_tls_client_hello_prefix(client)
            .await
            .map_err(TlsPassthroughError::ClientHello)?;
        let sni = match &prefix.outcome {
            Ok(TlsClientHelloSni::Sni { hostname }) => hostname.clone(),
            Ok(TlsClientHelloSni::NoSni) | Ok(TlsClientHelloSni::Incomplete { .. }) => {
                return Err(TlsPassthroughError::MissingSni);
            }
            Err(error) => return Err(TlsPassthroughError::NotTls(error.clone())),
        };
        let identity = RouteRequestIdentity::sni(&sni).map_err(TlsPassthroughError::InvalidSni)?;

        Ok(TlsPassthroughClientHello {
            identity,
            bytes: prefix.bytes,
        })
    }

    pub async fn passthrough_prefixed<Client>(
        &self,
        ready: &ReadyBackend,
        client: Client,
        client_hello: TlsPassthroughClientHello,
    ) -> Result<TlsPassthrough, TlsPassthroughError>
    where
        Client: AsyncRead + AsyncWrite + Unpin,
    {
        let upstream = passthrough_backend_addr(ready)?;
        let identity = client_hello.identity;

        let upstream = TcpStream::connect(upstream)
            .await
            .map_err(TlsPassthroughError::Connect)?;
        let client = PrefixedStream::new(client_hello.bytes, client);
        let stats = proxy_streams(client, upstream)
            .await
            .map_err(TlsPassthroughError::Proxy)?;

        Ok(TlsPassthrough { identity, stats })
    }
}

impl TlsPassthroughClientHello {
    pub fn identity(&self) -> &RouteRequestIdentity {
        &self.identity
    }
}

pub fn passthrough_backend_addr(
    ready: &ReadyBackend,
) -> Result<String, TlsPassthroughBackendError> {
    let uri: Uri = ready
        .backend
        .uri()
        .parse()
        .map_err(TlsPassthroughBackendError::InvalidUri)?;
    let scheme = uri
        .scheme_str()
        .ok_or(TlsPassthroughBackendError::MissingScheme)?;
    if scheme != "tcp" {
        return Err(TlsPassthroughBackendError::UnsupportedScheme(
            scheme.to_owned(),
        ));
    }
    let authority = uri
        .authority()
        .ok_or(TlsPassthroughBackendError::MissingAuthority)?
        .clone();
    if authority.port_u16().is_none() {
        return Err(TlsPassthroughBackendError::InvalidAuthority(
            authority.as_str().to_owned(),
        ));
    }

    Ok(authority.as_str().to_owned())
}

fn canonical_sni_key(sni: &str) -> Result<String, RequestIdentityError> {
    match RouteRequestIdentity::sni(sni)?.into_identity() {
        control_plane::RouteIdentity::Sni { host } => Ok(host.as_str().to_owned()),
        control_plane::RouteIdentity::Http { .. } => unreachable!("SNI constructor returns SNI"),
    }
}

struct PrefixedStream<IO> {
    prefix: Vec<u8>,
    prefix_pos: usize,
    inner: IO,
}

impl<IO> PrefixedStream<IO> {
    fn new(prefix: Vec<u8>, inner: IO) -> Self {
        Self {
            prefix,
            prefix_pos: 0,
            inner,
        }
    }
}

impl<IO> AsyncRead for PrefixedStream<IO>
where
    IO: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.prefix_pos < self.prefix.len() {
            let remaining = &self.prefix[self.prefix_pos..];
            let len = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..len]);
            self.prefix_pos += len;
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<IO> AsyncWrite for PrefixedStream<IO>
where
    IO: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl From<TlsPassthroughBackendError> for TlsPassthroughError {
    fn from(error: TlsPassthroughBackendError) -> Self {
        Self::Backend(error)
    }
}

impl fmt::Display for TlsCertificateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSni(error) => write!(f, "invalid certificate SNI: {error}"),
            Self::InvalidCertificate(error) => write!(f, "invalid TLS certificate: {error}"),
            Self::StorePoisoned => f.write_str("TLS certificate store lock is poisoned"),
        }
    }
}

impl Error for TlsCertificateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidSni(error) => Some(error),
            Self::InvalidCertificate(error) => Some(error),
            Self::StorePoisoned => None,
        }
    }
}

impl fmt::Display for TlsTerminationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientHello(error) => write!(f, "TLS ClientHello read failed: {error}"),
            Self::MissingSni => f.write_str("TLS ClientHello is missing SNI"),
            Self::InvalidSni(error) => write!(f, "TLS SNI is invalid: {error}"),
            Self::UnknownCertificate => f.write_str("no TLS certificate is configured for SNI"),
            Self::Handshake(error) => write!(f, "TLS handshake failed: {error}"),
        }
    }
}

impl Error for TlsTerminationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ClientHello(error) | Self::Handshake(error) => Some(error),
            Self::InvalidSni(error) => Some(error),
            Self::MissingSni | Self::UnknownCertificate => None,
        }
    }
}

impl fmt::Display for TlsPassthroughError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "TLS passthrough backend is invalid: {error}"),
            Self::ClientHello(error) => write!(f, "TLS ClientHello read failed: {error}"),
            Self::NotTls(error) => write!(f, "TLS ClientHello is invalid: {error}"),
            Self::MissingSni => f.write_str("TLS ClientHello is missing SNI"),
            Self::InvalidSni(error) => write!(f, "TLS SNI is invalid: {error}"),
            Self::Connect(error) => write!(f, "TLS passthrough connect failed: {error}"),
            Self::Proxy(error) => write!(f, "TLS passthrough proxy failed: {error}"),
        }
    }
}

impl Error for TlsPassthroughError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Backend(error) => Some(error),
            Self::ClientHello(error) | Self::Connect(error) | Self::Proxy(error) => Some(error),
            Self::NotTls(error) => Some(error),
            Self::InvalidSni(error) => Some(error),
            Self::MissingSni => None,
        }
    }
}

impl fmt::Display for TlsPassthroughBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUri(error) => write!(f, "backend URI is invalid: {error}"),
            Self::MissingScheme => write!(f, "backend URI is missing a scheme"),
            Self::MissingAuthority => write!(f, "backend URI is missing an authority"),
            Self::UnsupportedScheme(scheme) => {
                write!(
                    f,
                    "backend URI scheme {scheme:?} is not supported for TLS passthrough"
                )
            }
            Self::InvalidAuthority(authority) => {
                write!(f, "backend authority {authority:?} is not a socket address")
            }
        }
    }
}

impl Error for TlsPassthroughBackendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUri(error) => Some(error),
            Self::MissingScheme
            | Self::MissingAuthority
            | Self::UnsupportedScheme(_)
            | Self::InvalidAuthority(_) => None,
        }
    }
}

#[cfg(test)]
mod tests;
