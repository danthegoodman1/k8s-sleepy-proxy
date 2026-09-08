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
