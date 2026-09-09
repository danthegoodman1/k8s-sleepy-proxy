//! Generated material belongs to the fake delivery API, never a proxy preload.
use super::*;
use proxy_core::Shutdown;
use tokio::task::JoinSet;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
#[derive(Default)]
pub(crate) struct FakeResolver {
    pub responses: Mutex<HashMap<String, pb::ResolveTlsCertificateResponse>>,
    pub requests: Mutex<Vec<pb::ResolveTlsCertificateRequest>>,
}
impl CertificateResolver for FakeResolver {
    fn resolve(&self, request: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture {
        self.requests.lock().unwrap().push(request.clone());
        let mut response = self
            .responses
            .lock()
            .unwrap()
            .get(&request.server_name)
            .cloned()
            .unwrap_or(pb::ResolveTlsCertificateResponse {
                server_name: request.server_name.clone(),
                view_revision: 0,
                observed_at_unix_millis: wall_now(),
                authorization_ttl_millis: 1000,
                value: Some(pb::resolve_tls_certificate_response::Value::Missing(
                    pb::MissingTlsCertificate {},
                )),
            });
        response.observed_at_unix_millis = wall_now();
        if request.known_view_revision == Some(response.view_revision) {
            if let Some(pb::resolve_tls_certificate_response::Value::Found(found)) = &response.value
            {
                response.value = Some(pb::resolve_tls_certificate_response::Value::Unchanged(
                    found.metadata.clone().unwrap(),
                ));
            }
        }
        Box::pin(async move { Ok(response) })
    }
}
pub(crate) struct CertificateFixture {
    pub cache: TlsCertificateStore,
    pub api: Arc<FakeResolver>,
    shutdown: Shutdown,
    tasks: JoinSet<()>,
}
impl CertificateFixture {
    pub fn new() -> Self {
        let api = Arc::new(FakeResolver::default());
        let (cache, worker) = TlsCertificateStore::new(api.clone());
        let shutdown = Shutdown::new();
        let mut tasks = JoinSet::new();
        tasks.spawn(worker.run(shutdown.clone()));
        Self {
            cache,
            api,
            shutdown,
            tasks,
        }
    }
    pub fn publish(
        &self,
        hostname: &str,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<(), CertificateLookupError> {
        let mut responses = self.api.responses.lock().unwrap();
        let revision = responses.get(hostname).map_or(1, |r| r.view_revision + 1);
        responses.insert(
            hostname.to_owned(),
            response(hostname, revision, chain, key),
        );
        Ok(())
    }
    pub async fn finish(mut self) {
        self.shutdown.shutdown();
        while let Some(result) = self.tasks.join_next().await {
            result.unwrap();
        }
    }
}
impl Drop for CertificateFixture {
    fn drop(&mut self) {
        self.shutdown.shutdown();
    }
}
pub(crate) fn response(
    hostname: &str,
    revision: u64,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> pb::ResolveTlsCertificateResponse {
    let bundle = sleepypods_api::CertificateBundle::new(
        chain.into_iter().map(|v| v.to_vec()).collect(),
        key.secret_der().to_vec(),
    )
    .unwrap();
    let m = sleepypods_certificate::validate_certificate(&bundle, wall_now()).unwrap();
    pb::ResolveTlsCertificateResponse {
        server_name: hostname.to_owned(),
        view_revision: revision,
        observed_at_unix_millis: wall_now(),
        authorization_ttl_millis: 300_000,
        value: Some(pb::resolve_tls_certificate_response::Value::Found(
            pb::FoundTlsCertificate {
                metadata: Some(pb::CertificateMetadata {
                    certificate_id: "fixture".into(),
                    version: revision,
                    deleted: false,
                    not_before_unix_millis: m.not_before_unix_millis,
                    not_after_unix_millis: m.not_after_unix_millis,
                    dns_names: m.dns_names,
                    leaf_sha256: m.leaf_sha256,
                    sealing_key_id: Some("test".into()),
                    sealing_revision: 1,
                }),
                bundle: Some(pb::CertificateBundle {
                    chain_der: bundle.chain_der().to_vec(),
                    private_key_pkcs8_der: bundle.private_key_pkcs8_der().to_vec(),
                }),
            },
        )),
    }
}
