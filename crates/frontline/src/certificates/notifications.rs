//! One lazy, owned watch. Full replacement registrations coalesce interests;
//! neither notification cursors nor reconnects grant authorization leases.
use super::*;
use proxy_core::Shutdown;
use sleepypods_api::{CertificateId, CertificateRevision};
use tonic::codegen::tokio_stream::{Stream, StreamExt};

pub type CertificateWatchStream = Pin<
    Box<dyn Stream<Item = Result<pb::WatchTlsCertificatesResponse, CertificateLookupError>> + Send>,
>;
pub type CertificateWatchFuture =
    Pin<Box<dyn Future<Output = Result<CertificateWatchStream, CertificateLookupError>> + Send>>;
pub trait CertificateWatcher: Send + Sync + 'static {
    fn watch(
        &self,
        interests: mpsc::Receiver<pb::WatchTlsCertificatesRequest>,
    ) -> CertificateWatchFuture;
}
const REGISTRATION_INTERVAL: Duration = Duration::from_millis(250);
const SESSION_LIMIT: Duration = Duration::from_secs(65);
struct Registration {
    id: u64,
    incarnations: HashMap<String, u64>,
    request: pb::WatchTlsCertificatesRequest,
}
fn registration(cache: &TlsCertificateStore, id: u64) -> Registration {
    let state = cache.shared.state.lock().unwrap();
    Registration {
        id,
        incarnations: state
            .entries
            .iter()
            .map(|(h, e)| (h.clone(), e.incarnation))
            .collect(),
        request: pb::WatchTlsCertificatesRequest {
            registration: id,
            interests: state
                .entries
                .iter()
                .map(|(h, e)| pb::TlsCertificateInterest {
                    hostname: h.clone(),
                    known_view_revision: e.floor,
                })
                .collect(),
        },
    }
}
pub(super) async fn run(
    cache: TlsCertificateStore,
    watcher: Arc<dyn CertificateWatcher>,
    shutdown: Shutdown,
) {
    let mut changed = cache
        .shared
        .state
        .lock()
        .unwrap()
        .interests_changed
        .subscribe();
    // Per-worker bounded jitter avoids synchronized reconnect waves across
    // replicas without retaining per-host retry records.
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos() as u64 % 501);
    let retry = Duration::from_millis(500 + jitter);
    loop {
        if shutdown.is_shutdown() {
            return;
        }
        if cache.shared.state.lock().unwrap().entries.is_empty() {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = changed.changed() => {},
            }
            continue;
        }
        // No spawned task or historical stream survives a session. The outer
        // fixed bound includes setup, registrations and a silent broken peer.
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::timeout(SESSION_LIMIT, session(&cache, watcher.as_ref(), &mut changed)) => {},
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(retry) => {},
        }
    }
}
async fn session(
    cache: &TlsCertificateStore,
    watcher: &dyn CertificateWatcher,
    changed: &mut watch::Receiver<()>,
) -> Result<(), CertificateLookupError> {
    let mut next_id = 1u64;
    changed.borrow_and_update();
    let first = registration(cache, next_id);
    if first.incarnations.is_empty() {
        return Ok(());
    }
    let (requests, receiver) = mpsc::channel(1);
    requests
        .try_send(first.request.clone())
        .map_err(|_| CertificateLookupError::Capacity)?;
    let mut pending = Some(first);
    let mut acknowledgement = Instant::now() + CERTIFICATE_LOOKUP_TIMEOUT;
    let mut stream = tokio::time::timeout_at(acknowledgement, watcher.watch(receiver))
        .await
        .map_err(|_| CertificateLookupError::Deadline)??;
    let mut registered = HashMap::new();
    let mut cursor = 0;
    let mut next_registration = Instant::now() + REGISTRATION_INTERVAL;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(acknowledgement), if pending.is_some() => return Err(CertificateLookupError::Deadline),
            _ = tokio::time::sleep_until(next_registration), if pending.is_none() => {
                next_registration = Instant::now() + REGISTRATION_INTERVAL;
                if changed.has_changed().unwrap_or(true) {
                    changed.borrow_and_update();
                    next_id = next_id.checked_add(1).ok_or(CertificateLookupError::Capacity)?;
                    let next = registration(cache, next_id);
                    if next.incarnations.is_empty() {
                        return Ok(());
                    }
                    requests.try_send(next.request.clone()).map_err(|_| CertificateLookupError::Capacity)?;
                    pending = Some(next);
                    acknowledgement = Instant::now() + CERTIFICATE_LOOKUP_TIMEOUT;
                }
            }
            message = stream.next() => {
                let message = message.ok_or(CertificateLookupError::Unavailable)??;
                match message.value.ok_or(CertificateLookupError::Invalid)? {
                    pb::watch_tls_certificates_response::Value::Snapshot(snapshot) => {
                        let sent = pending.take().ok_or(CertificateLookupError::Invalid)?;
                        if snapshot.registration != sent.id
                            || snapshot.bindings.len() != sent.incarnations.len()
                            || CertificateRevision::new(snapshot.cursor).is_err()
                        {
                            return Err(CertificateLookupError::Invalid);
                        }
                        let mut seen = std::collections::HashSet::new();
                        for b in &snapshot.bindings {
                            if !sent.incarnations.contains_key(&b.hostname)
                                || !seen.insert(&b.hostname)
                                || CertificateRevision::new(b.revision).is_err()
                                || b.revision > snapshot.cursor
                                || b.certificate_id
                                    .as_ref()
                                    .is_some_and(|id| CertificateId::new(id.clone()).is_err())
                            {
                                return Err(CertificateLookupError::Invalid);
                            }
                        }
                        // Validate the entire bounded message before applying it.
                        for binding in snapshot.bindings {
                            apply(
                                cache,
                                &binding.hostname,
                                sent.incarnations[&binding.hostname],
                                binding.revision,
                                binding.certificate_id.as_deref(),
                                true,
                            )?;
                        }
                        registered = sent.incarnations;
                        cursor = snapshot.cursor;
                    }
                    pb::watch_tls_certificates_response::Value::Changes(changes) => {
                        if CertificateRevision::new(changes.cursor).is_err() || changes.events.len() > 256 {
                            return Err(CertificateLookupError::Invalid);
                        }
                        for e in &changes.events {
                            if CertificateRevision::new(e.view_revision).is_err()
                                || e.view_revision > changes.cursor
                                || !registered.contains_key(&e.hostname)
                                || e.certificate_id
                                    .as_ref()
                                    .is_some_and(|id| CertificateId::new(id.clone()).is_err())
                            {
                                return Err(CertificateLookupError::Invalid);
                            }
                        }
                        for event in changes.events {
                            apply(
                                cache,
                                &event.hostname,
                                registered[&event.hostname],
                                event.view_revision,
                                event.certificate_id.as_deref(),
                                event.invalidate,
                            )?;
                        }
                        cursor = cursor.max(changes.cursor);
                    }
                    pb::watch_tls_certificates_response::Value::Reset(reset) => {
                        if CertificateRevision::new(reset.cursor).is_err() {
                            return Err(CertificateLookupError::Invalid);
                        }
                        reset_views(cache)?;
                        return Ok(());
                    }
                }
            }
        }
    }
}
fn apply(
    cache: &TlsCertificateStore,
    hostname: &str,
    incarnation: u64,
    revision: u64,
    id: Option<&str>,
    invalidate: bool,
) -> Result<(), CertificateLookupError> {
    let mut state = cache.shared.state.lock().unwrap();
    let Some(entry) = state
        .entries
        .get(hostname)
        .filter(|e| e.incarnation == incarnation)
    else {
        return Ok(());
    };
    if revision <= entry.floor {
        return Ok(());
    }
    let retain = !invalidate
        && id.is_some()
        && entry
            .value
            .as_ref()
            .and_then(|v| v.metadata.as_ref())
            .is_some_and(|m| Some(m.certificate_id.as_str()) == id);
    state.serial = state
        .serial
        .checked_add(1)
        .ok_or(CertificateLookupError::Capacity)?;
    let serial = state.serial;
    let entry = state.entries.get_mut(hostname).unwrap();
    entry.floor = revision;
    entry.generation = serial;
    entry.pending = false;
    entry.refresh_requested = true;
    if !retain {
        entry.value = None;
    }
    entry.changed.send_modify(|v| *v = v.wrapping_add(1));
    // Capacity pressure may postpone fetch, never postpone the invalidation.
    let _ = cache.queue_locked(&mut state, hostname);
    Ok(())
}
fn reset_views(cache: &TlsCertificateStore) -> Result<(), CertificateLookupError> {
    let mut state = cache.shared.state.lock().unwrap();
    let hosts: Vec<_> = state.entries.keys().cloned().collect();
    for hostname in hosts {
        state.serial = state
            .serial
            .checked_add(1)
            .ok_or(CertificateLookupError::Capacity)?;
        let serial = state.serial;
        let entry = state.entries.get_mut(&hostname).unwrap();
        entry.generation = serial;
        entry.pending = false;
        entry.value = None;
        entry.refresh_requested = true;
        entry.changed.send_modify(|v| *v = v.wrapping_add(1));
        // Keep only the per-host floor already observed. The reset's global
        // history cursor must never fence an unchanged low-revision hostname.
    }
    Ok(())
}

#[cfg(test)]
mod tests;
