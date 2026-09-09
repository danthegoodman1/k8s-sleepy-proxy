use super::*;
use sleepypods_api::{
    pb::proxy_control_plane_client::ProxyControlPlaneClient, OptionalBearerTokenInterceptor,
};
use tonic::{service::interceptor::InterceptedService, transport::Channel};

/// The channel is verified independently of application TLS. The runtime
/// constructs it from the HTTPS endpoint and independent platform trust.
#[derive(Clone)]
pub struct GrpcCertificateResolver {
    client: ProxyControlPlaneClient<InterceptedService<Channel, OptionalBearerTokenInterceptor>>,
}
impl GrpcCertificateResolver {
    pub fn new(channel: Channel, auth: OptionalBearerTokenInterceptor) -> Self {
        Self {
            client: ProxyControlPlaneClient::with_interceptor(channel, auth)
                .max_decoding_message_size(256 * 1024),
        }
    }
}
impl CertificateResolver for GrpcCertificateResolver {
    fn resolve(&self, request: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture {
        let mut client = self.client.clone();
        Box::pin(async move {
            client
                .resolve_tls_certificate(request)
                .await
                .map(|r| r.into_inner())
                .map_err(|_| CertificateLookupError::Unavailable)
        })
    }
}

impl CertificateWatcher for GrpcCertificateResolver {
    fn watch(
        &self,
        interests: mpsc::Receiver<pb::WatchTlsCertificatesRequest>,
    ) -> CertificateWatchFuture {
        use tonic::codegen::tokio_stream::StreamExt;
        let mut client = self.client.clone().max_decoding_message_size(512 * 1024);
        Box::pin(async move {
            let response = client
                .watch_tls_certificates(
                    tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(interests),
                )
                .await
                .map_err(|_| CertificateLookupError::Unavailable)?;
            Ok(Box::pin(
                response
                    .into_inner()
                    .map(|r| r.map_err(|_| CertificateLookupError::Unavailable)),
            ) as CertificateWatchStream)
        })
    }
}
