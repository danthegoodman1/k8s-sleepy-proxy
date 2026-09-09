use super::*;
use crate::{
    certificates::test_support::{response, FakeResolver},
    FrontlineTlsAdapter,
};
use tokio::{sync::oneshot, task::JoinSet};
use tokio_rustls::{
    rustls::{
        self,
        pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
    },
    TlsConnector,
};

#[derive(Default)]
struct Resolver {
    api: FakeResolver,
    hold: Mutex<std::collections::VecDeque<oneshot::Receiver<()>>>,
}
impl CertificateResolver for Resolver {
    fn resolve(&self, request: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture {
        let response = self.api.resolve(request);
        let hold = self.hold.lock().unwrap().pop_front();
        Box::pin(async move {
            if let Some(hold) = hold {
                let _ = hold.await;
            }
            response.await
        })
    }
}
struct Opened {
    requests: mpsc::Receiver<pb::WatchTlsCertificatesRequest>,
    events: mpsc::Sender<Result<pb::WatchTlsCertificatesResponse, CertificateLookupError>>,
    registration: u64,
    bindings: std::collections::BTreeMap<String, pb::TlsBinding>,
}
struct Watch(mpsc::Sender<Opened>);
impl CertificateWatcher for Watch {
    fn watch(
        &self,
        requests: mpsc::Receiver<pb::WatchTlsCertificatesRequest>,
    ) -> CertificateWatchFuture {
        let opened = self.0.clone();
        Box::pin(async move {
            let (events, receiver) = mpsc::channel(2);
            opened
                .send(Opened {
                    requests,
                    events,
                    registration: 0,
                    bindings: Default::default(),
                })
                .await
                .map_err(|_| CertificateLookupError::Unavailable)?;
            Ok(
                Box::pin(tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(
                    receiver,
                )) as CertificateWatchStream,
            )
        })
    }
}
struct Rig {
    cache: TlsCertificateStore,
    api: Arc<Resolver>,
    opened: mpsc::Receiver<Opened>,
    shutdown: Shutdown,
    tasks: JoinSet<()>,
    metrics: proxy_core::observability::prometheus::PrometheusMetricsSink,
}
impl Rig {
    fn new() -> Self {
        let api = Arc::new(Resolver::default());
        let metrics = proxy_core::observability::prometheus::PrometheusMetricsSink::new();
        let (cache, worker) = TlsCertificateStore::with_observability(
            api.clone(),
            ObservabilityRecorder::new(Arc::new(metrics.clone())),
        );
        let (watch, opened) = mpsc::channel(2);
        let shutdown = Shutdown::new();
        let mut tasks = JoinSet::new();
        tasks.spawn(
            worker
                .with_watch(Arc::new(Watch(watch)))
                .unwrap()
                .run(shutdown.clone()),
        );
        Self {
            cache,
            api,
            opened,
            shutdown,
            tasks,
            metrics,
        }
    }
    fn publish(
        &self,
        host: &str,
        revision: u64,
        id: &str,
        version: u64,
    ) -> CertificateDer<'static> {
        let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
        let der = cert.cert.der().clone();
        let mut value = response(
            host,
            revision,
            vec![der.clone()],
            PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        );
        if let Some(pb::resolve_tls_certificate_response::Value::Found(found)) = &mut value.value {
            found.metadata.as_mut().unwrap().certificate_id = id.into();
            found.metadata.as_mut().unwrap().version = version;
        }
        self.api
            .api
            .responses
            .lock()
            .unwrap()
            .insert(host.into(), value);
        der
    }
    fn missing(&self, host: &str, revision: u64) {
        self.api.api.responses.lock().unwrap().insert(
            host.into(),
            pb::ResolveTlsCertificateResponse {
                server_name: host.into(),
                view_revision: revision,
                observed_at_unix_millis: wall_now(),
                authorization_ttl_millis: 1000,
                value: Some(pb::resolve_tls_certificate_response::Value::Missing(
                    pb::MissingTlsCertificate {},
                )),
            },
        );
    }
    fn hold(&self) -> oneshot::Sender<()> {
        let (send, recv) = oneshot::channel();
        self.api.hold.lock().unwrap().push_back(recv);
        send
    }
    async fn open(&mut self) -> Opened {
        tokio::time::timeout(Duration::from_secs(2), self.opened.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn finish(mut self) {
        self.shutdown.shutdown();
        while let Some(result) = self.tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(self.cache.usage().fetches, 0);
        assert!(self.cache.shared.state.lock().unwrap().entries.is_empty());
    }
}
impl Drop for Rig {
    fn drop(&mut self) {
        self.shutdown.shutdown();
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    let bound = std::time::Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(
            std::time::Instant::now() < bound,
            "notification barrier did not complete"
        );
        tokio::task::yield_now().await;
    }
}
fn revision(rig: &Rig, host: &str) -> Option<u64> {
    rig.cache
        .shared
        .state
        .lock()
        .unwrap()
        .entries
        .get(host)
        .and_then(|e| e.value.as_ref())
        .map(|v| v.revision)
}
async fn sync(open: &mut Opened, bindings: Vec<(&str, u64, Option<&str>)>) {
    let request = tokio::time::timeout(Duration::from_secs(2), open.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.hostnames.len(), bindings.len());
    open.registration = request.registration;
    open.bindings = bindings
        .into_iter()
        .map(|(host, revision, id)| {
            (
                host.into(),
                pb::TlsBinding {
                    hostname: host.into(),
                    revision,
                    certificate_id: id.map(str::to_owned),
                    last_invalidating_revision: revision,
                },
            )
        })
        .collect();
    send_snapshot(open).await;
}
async fn send_snapshot(open: &Opened) {
    open.events
        .send(Ok(pb::WatchTlsCertificatesResponse {
            registration: open.registration,
            bindings: open.bindings.values().cloned().collect(),
        }))
        .await
        .unwrap();
}
async fn push(open: &mut Opened, host: &str, revision: u64, id: Option<&str>, invalidate: bool) {
    let binding = open.bindings.get_mut(host).unwrap();
    binding.revision = revision;
    binding.certificate_id = id.map(str::to_owned);
    if invalidate {
        binding.last_invalidating_revision = revision;
    }
    send_snapshot(open).await;
}
async fn peer(cache: &TlsCertificateStore, host: &str, cert: &CertificateDer<'static>) {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let connector = TlsConnector::from(Arc::new(config));
    let adapter = FrontlineTlsAdapter::new(cache.clone());
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (client, server) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            connector.connect(ServerName::try_from(host.to_owned()).unwrap(), client),
            adapter.terminate(server)
        )
    })
    .await
    .unwrap();
    let client = client.unwrap();
    let server = server.unwrap();
    assert_eq!(client.get_ref().1.peer_certificates().unwrap()[0], *cert);
    assert_eq!(client.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
    assert_eq!(server.stream.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
}
#[tokio::test]
async fn cached_miss_then_registration_snapshot_closes_publish_race_with_real_peer() {
    let mut rig = Rig::new();
    assert!(rig.opened.try_recv().is_err(), "empty cache must not watch");
    assert!(rig.cache.resolve("app.example").await.unwrap().is_none());
    let mut watch = rig.open().await;
    let cert = rig.publish("app.example", 2, "A", 1);
    sync(&mut watch, vec![("app.example", 2, Some("A"))]).await;
    until(|| revision(&rig, "app.example") == Some(2)).await;
    peer(&rig.cache, "app.example", &cert).await;
    assert_eq!(rig.api.api.requests.lock().unwrap().len(), 2);
    rig.finish().await;
}
#[tokio::test]
async fn rotation_fences_held_unchanged_and_preserves_only_original_view_until_replacement() {
    let mut rig = Rig::new();
    let old = rig.publish("app.example", 1, "A", 1);
    rig.cache.resolve("app.example").await.unwrap();
    let mut watch = rig.open().await;
    sync(&mut watch, vec![("app.example", 1, Some("A"))]).await;
    let release = rig.hold();
    {
        let mut state = rig.cache.shared.state.lock().unwrap();
        rig.cache.queue_locked(&mut state, "app.example").unwrap();
    }
    until(|| rig.api.api.requests.lock().unwrap().len() == 2).await;
    peer(&rig.cache, "app.example", &old).await;
    let original_expiry = rig.cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;
    let new = rig.publish("app.example", 2, "A", 2);
    let replacement = rig.hold();
    push(&mut watch, "app.example", 2, Some("A"), false).await;
    until(|| rig.api.api.requests.lock().unwrap().len() == 3).await;
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"].floor,
        2
    );
    assert_eq!(revision(&rig, "app.example"), Some(1));
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        original_expiry
    );
    peer(&rig.cache, "app.example", &old).await;
    replacement.send(()).unwrap();
    until(|| revision(&rig, "app.example") == Some(2)).await;
    let expiry = rig.cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;
    release.send(()).unwrap();
    until(|| rig.cache.usage().fetches == 0).await;
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        expiry
    );
    peer(&rig.cache, "app.example", &new).await;
    let counters = rig.metrics.render();
    assert!(counters.contains("{operation=\"certificate_refresh\",outcome=\"hit\"} 1"));
    assert!(counters.contains("{operation=\"certificate_install\",outcome=\"canceled\"} 1"));
    rig.finish().await;
}
#[tokio::test]
async fn received_removal_fences_late_found_and_rebind_accepts_lower_certificate_version() {
    let mut rig = Rig::new();
    rig.publish("app.example", 9, "A", 9);
    let release = rig.hold();
    let mut callers = JoinSet::new();
    let cache = rig.cache.clone();
    callers.spawn(async move { cache.resolve("app.example").await });
    until(|| rig.api.api.requests.lock().unwrap().len() == 1).await;
    let mut watch = rig.open().await;
    // Initial snapshot still observes A; its authoritative fetch supersedes the
    // original cold fetch without letting that held response restore removal.
    sync(&mut watch, vec![("app.example", 9, Some("A"))]).await;
    until(|| revision(&rig, "app.example") == Some(9)).await;
    rig.missing("app.example", 10);
    push(&mut watch, "app.example", 10, None, true).await;
    until(|| revision(&rig, "app.example") == Some(10)).await;
    assert!(rig.cache.resolve("app.example").await.unwrap().is_none());
    release.send(()).unwrap();
    until(|| rig.cache.usage().fetches == 0).await;
    while callers.join_next().await.is_some() {}
    assert!(rig.cache.resolve("app.example").await.unwrap().is_none());
    let cert = rig.publish("app.example", 11, "B", 1);
    push(&mut watch, "app.example", 11, Some("B"), true).await;
    until(|| revision(&rig, "app.example") == Some(11)).await;
    peer(&rig.cache, "app.example", &cert).await;
    rig.finish().await;
}
#[tokio::test]
async fn independent_host_revisions_and_duplicate_snapshots_never_renew() {
    let mut rig = Rig::new();
    rig.publish("a.example", 1, "A", 1);
    rig.cache.resolve("a.example").await.unwrap();
    let mut watch = rig.open().await;
    // First registration precedes the second interest; acknowledge each exact set.
    sync(&mut watch, vec![("a.example", 1, Some("A"))]).await;
    rig.publish("b.example", 2, "B", 1);
    rig.cache.resolve("b.example").await.unwrap();
    sync(
        &mut watch,
        vec![("a.example", 1, Some("A")), ("b.example", 2, Some("B"))],
    )
    .await;
    rig.publish("b.example", 4, "B", 2);
    push(&mut watch, "b.example", 4, Some("B"), false).await;
    until(|| revision(&rig, "b.example") == Some(4)).await;
    rig.missing("a.example", 3);
    push(&mut watch, "a.example", 3, None, true).await;
    until(|| revision(&rig, "a.example") == Some(3)).await;
    assert!(rig.cache.resolve("a.example").await.unwrap().is_none());
    let expiry = rig.cache.shared.state.lock().unwrap().entries["b.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;
    send_snapshot(&watch).await;
    // The next distinct host event is a causal receive-order barrier; sending
    // a snapshot alone would not prove the duplicate had been processed.
    rig.missing("a.example", 5);
    push(&mut watch, "a.example", 5, None, true).await;
    until(|| revision(&rig, "a.example") == Some(5)).await;
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["b.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        expiry
    );
    rig.finish().await;
}
#[tokio::test]
async fn reconnect_snapshot_preserves_low_host_revision_and_original_lease() {
    let mut rig = Rig::new();
    let cert = rig.publish("app.example", 2, "A", 1);
    rig.cache.resolve("app.example").await.unwrap();
    let original = rig.cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;
    let mut watch = rig.open().await;
    sync(&mut watch, vec![("app.example", 2, Some("A"))]).await;
    drop(watch);
    let mut next = rig.open().await;
    sync(&mut next, vec![("app.example", 2, Some("A"))]).await;
    until(|| {
        rig.metrics
            .render()
            .contains("{operation=\"certificate_watch\",outcome=\"updated\"} 2")
    })
    .await;
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"].floor,
        2
    );
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        original
    );
    peer(&rig.cache, "app.example", &cert).await;
    rig.finish().await;
}

#[tokio::test]
async fn eviction_incarnation_fences_old_snapshot_and_releases_interest_stream() {
    let mut rig = Rig::new();
    assert!(rig.cache.resolve("app.example").await.unwrap().is_none());
    let mut open = rig.open().await;
    let old_request = open.requests.recv().await.unwrap();
    assert!(evict_one(&mut rig.cache.shared.state.lock().unwrap(), None));
    rig.publish("app.example", 1, "A", 1);
    rig.cache.resolve("app.example").await.unwrap();
    let incarnation = rig.cache.shared.state.lock().unwrap().entries["app.example"].incarnation;
    open.events
        .send(Ok(pb::WatchTlsCertificatesResponse {
            registration: old_request.registration,
            bindings: vec![pb::TlsBinding {
                hostname: "app.example".into(),
                revision: 2,
                certificate_id: Some("A".into()),
                last_invalidating_revision: 1,
            }],
        }))
        .await
        .unwrap();
    // A new registration is the processing barrier for the stale snapshot.
    let next = tokio::time::timeout(Duration::from_secs(2), open.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.hostnames, ["app.example"]);
    assert_eq!(revision(&rig, "app.example"), Some(1));
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"].incarnation,
        incarnation
    );
    let new = rig.publish("app.example", 2, "A", 2);
    open.events
        .send(Ok(pb::WatchTlsCertificatesResponse {
            registration: next.registration,
            bindings: vec![pb::TlsBinding {
                hostname: "app.example".into(),
                revision: 2,
                certificate_id: Some("A".into()),
                last_invalidating_revision: 1,
            }],
        }))
        .await
        .unwrap();
    until(|| revision(&rig, "app.example") == Some(2)).await;
    peer(&rig.cache, "app.example", &new).await;
    for cycle in 0..4 {
        while evict_one(&mut rig.cache.shared.state.lock().unwrap(), None) {}
        until(|| open.events.is_closed()).await;
        assert_eq!(rig.cache.usage().entries, 0);
        let host = format!("cycle{cycle}.example");
        assert!(rig.cache.resolve(&host).await.unwrap().is_none());
        open = rig.open().await;
        sync(&mut open, vec![(&host, 0, None)]).await;
        assert_eq!(rig.cache.usage().entries, 1);
        assert!(rig.cache.usage().accounted_bytes < CERTIFICATE_CACHE_BYTES);
    }
    rig.finish().await;
    assert!(open.events.is_closed());
}

#[tokio::test]
async fn invalid_replacement_and_watch_outage_cannot_extend_original_hard_lease() {
    let mut rig = Rig::new();
    let cert = rig.publish("app.example", 1, "A", 1);
    rig.api
        .api
        .responses
        .lock()
        .unwrap()
        .get_mut("app.example")
        .unwrap()
        .authorization_ttl_millis = 10_000;
    rig.cache.resolve("app.example").await.unwrap();
    let mut open = rig.open().await;
    sync(&mut open, vec![("app.example", 1, Some("A"))]).await;
    let original = rig.cache.shared.state.lock().unwrap().entries["app.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;
    rig.publish("app.example", 2, "A", 2);
    if let Some(pb::resolve_tls_certificate_response::Value::Found(found)) = &mut rig
        .api
        .api
        .responses
        .lock()
        .unwrap()
        .get_mut("app.example")
        .unwrap()
        .value
    {
        found.bundle.as_mut().unwrap().private_key_pkcs8_der = b"invalid-key-marker".to_vec();
    }
    push(&mut open, "app.example", 2, Some("A"), false).await;
    until(|| {
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .last_error
            .is_some()
    })
    .await;
    assert_eq!(revision(&rig, "app.example"), Some(1));
    peer(&rig.cache, "app.example", &cert).await;
    drop(open);
    tokio::time::pause();
    tokio::time::advance(original - Instant::now() + Duration::from_millis(1)).await;
    assert!(rig.cache.resolve("app.example").await.is_err());
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        original
    );
    rig.finish().await;
}

#[tokio::test]
async fn watch_decode_reserve_and_three_fetch_owners_survive_until_joined_shutdown() {
    let mut rig = Rig::new();
    assert_eq!(
        rig.cache.usage().accounted_bytes,
        STRUCTURE_BYTES + WATCH_BYTES
    );
    let mut callers = JoinSet::new();
    let mut releases = Vec::new();
    for i in 0..CERTIFICATE_FETCHES {
        releases.push(rig.hold());
        let cache = rig.cache.clone();
        callers.spawn(async move { cache.resolve(&format!("held{i}.example")).await });
        until(|| rig.api.api.requests.lock().unwrap().len() == i + 1).await;
    }
    assert_eq!(rig.cache.usage().fetches, CERTIFICATE_FETCHES);
    assert!(
        rig.cache.usage().accounted_bytes
            >= STRUCTURE_BYTES + WATCH_BYTES + CERTIFICATE_FETCHES * FETCH_BYTES
    );
    assert!(rig.cache.usage().accounted_bytes <= CERTIFICATE_CACHE_BYTES);
    assert!(matches!(
        rig.cache.resolve("excess.example").await,
        Err(CertificateLookupError::Capacity)
    ));
    let open = rig.open().await;
    let cache = rig.cache.clone();
    rig.finish().await;
    while callers.join_next().await.is_some() {}
    assert!(open.events.is_closed());
    assert_eq!(cache.usage().accounted_bytes, STRUCTURE_BYTES);
    assert!(releases.into_iter().all(|r| r.send(()).is_err()));
}

#[tokio::test]
async fn coalesced_destructive_watermark_invalidates_retained_view_and_both_late_responses() {
    let mut rig = Rig::new();
    rig.publish("app.example", 1, "A", 1);
    rig.cache.resolve("app.example").await.unwrap();
    let mut open = rig.open().await;
    sync(&mut open, vec![("app.example", 1, Some("A"))]).await;
    let unchanged = rig.hold();
    {
        let mut state = rig.cache.shared.state.lock().unwrap();
        rig.cache.queue_locked(&mut state, "app.example").unwrap();
    }
    until(|| rig.api.api.requests.lock().unwrap().len() == 2).await;
    rig.publish("app.example", 4, "A", 2);
    let stale_found = rig.hold();
    push(&mut open, "app.example", 4, Some("A"), false).await;
    until(|| rig.api.api.requests.lock().unwrap().len() == 3).await;
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"].floor,
        4
    );
    assert_eq!(revision(&rig, "app.example"), Some(1));
    // A -> unbind -> A -> rotate coalesces to the same certificate ID, with
    // invalidation5 distinct from latest view7 and retained material view1.
    let new = rig.publish("app.example", 7, "A", 3);
    let current = rig.hold();
    let binding = open.bindings.get_mut("app.example").unwrap();
    binding.revision = 7;
    binding.last_invalidating_revision = 5;
    send_snapshot(&open).await;
    until(|| rig.api.api.requests.lock().unwrap().len() == 4).await;
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["app.example"].floor,
        7
    );
    assert_eq!(revision(&rig, "app.example"), None);
    unchanged.send(()).unwrap();
    stale_found.send(()).unwrap();
    until(|| rig.cache.usage().fetches == 1).await;
    assert_eq!(revision(&rig, "app.example"), None);
    current.send(()).unwrap();
    until(|| revision(&rig, "app.example") == Some(7)).await;
    peer(&rig.cache, "app.example", &new).await;
    rig.finish().await;
}

#[tokio::test]
async fn malformed_snapshot_is_rejected_before_any_host_mutation() {
    let mut rig = Rig::new();
    rig.publish("a.example", 1, "A", 1);
    rig.cache.resolve("a.example").await.unwrap();
    let mut open = rig.open().await;
    sync(&mut open, vec![("a.example", 1, Some("A"))]).await;
    rig.publish("b.example", 2, "B", 1);
    rig.cache.resolve("b.example").await.unwrap();
    sync(
        &mut open,
        vec![("a.example", 1, Some("A")), ("b.example", 2, Some("B"))],
    )
    .await;
    until(|| {
        rig.metrics
            .render()
            .contains("{operation=\"certificate_watch\",outcome=\"updated\"} 2")
    })
    .await;
    let original = rig.cache.shared.state.lock().unwrap().entries["a.example"]
        .value
        .as_ref()
        .unwrap()
        .expires;
    let generation = rig.cache.shared.state.lock().unwrap().entries["a.example"].generation;
    open.bindings.get_mut("a.example").unwrap().revision = 3;
    open.bindings
        .get_mut("b.example")
        .unwrap()
        .last_invalidating_revision = 4;
    send_snapshot(&open).await;
    until(|| open.events.is_closed()).await;
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["a.example"].generation,
        generation
    );
    assert_eq!(
        rig.cache.shared.state.lock().unwrap().entries["a.example"]
            .value
            .as_ref()
            .unwrap()
            .expires,
        original
    );
    assert_eq!(revision(&rig, "a.example"), Some(1));
    assert_eq!(rig.api.api.requests.lock().unwrap().len(), 2);
    rig.finish().await;
}

#[tokio::test]
async fn acknowledged_registration_keeps_receiving_while_pending_ack_deadline_stays_fixed() {
    let mut rig = Rig::new();
    rig.publish("a.example", 1, "A", 1);
    rig.cache.resolve("a.example").await.unwrap();
    let mut open = rig.open().await;
    sync(&mut open, vec![("a.example", 1, Some("A"))]).await;
    rig.publish("b.example", 2, "B", 1);
    rig.cache.resolve("b.example").await.unwrap();
    let pending = tokio::time::timeout(Duration::from_secs(2), open.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.registration, 2);
    assert_eq!(pending.hostnames.len(), 2);
    tokio::time::pause();
    for revision in [3, 4] {
        tokio::time::advance(Duration::from_secs(1)).await;
        push(&mut open, "a.example", revision, Some("A"), false).await;
        until(|| rig.cache.shared.state.lock().unwrap().entries["a.example"].floor == revision)
            .await;
        assert!(!open.events.is_closed());
    }
    tokio::time::advance(Duration::from_millis(1001)).await;
    until(|| open.events.is_closed()).await;
    assert_eq!(revision(&rig, "a.example"), Some(1));
    rig.finish().await;
}
