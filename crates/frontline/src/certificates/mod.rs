//! Demand-loaded application certificates. The worker owns all asynchronous
//! work; the handle only accesses bounded memory and queues bounded fetches.
mod client;
mod metrics;
mod notifications;
mod worker;
pub use client::GrpcCertificateResolver;
pub use notifications::{CertificateWatchFuture, CertificateWatchStream, CertificateWatcher};
#[cfg(test)]
mod handshake_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

use proxy_core::observability::{recorder::ObservabilityRecorder, Operation, Outcome};
use sleepypods_api::{pb, TlsHostname};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_rustls::rustls::ServerConfig;
pub use worker::TlsCertificateWorker;

pub const CERTIFICATE_CACHE_ENTRIES: usize = 1024;
pub const CERTIFICATE_CACHE_BYTES: usize = 64 * 1024 * 1024;
pub const CERTIFICATE_FETCHES: usize = 3;
pub const CERTIFICATE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
pub const CERTIFICATE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const ENTRY_BYTES: usize = 1024;
// Retained hash-table buckets, bounded channels/task records and state remain
// allocated even when entries are evicted. Includes one 512KiB watch response,
// one queued/request snapshot, registration maps and coalesced interest state.
const STRUCTURE_BYTES: usize = 4 * 1024 * 1024;
// Includes bounded wire response, validation scratch, DER/key copies and the
// replacement config. On installation this is reduced to the retained charge.
const FETCH_BYTES: usize = 8 * 1024 * 1024;
// Malformed bounded Prost snapshots can retain old/new vector allocations
// during growth; retain the conservative existing notification envelope.
const WATCH_BYTES: usize = 32 * 1024 * 1024;
const CONFIG_OVERHEAD: usize = 128 * 1024;

pub type CertificateResolveFuture = Pin<
    Box<
        dyn Future<Output = Result<pb::ResolveTlsCertificateResponse, CertificateLookupError>>
            + Send,
    >,
>;
pub trait CertificateResolver: Send + Sync + 'static {
    fn resolve(&self, request: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CertificateLookupError {
    Invalid,
    Unavailable,
    Capacity,
    Deadline,
    Stopped,
}
impl fmt::Display for CertificateLookupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid certificate response",
            Self::Unavailable => "certificate resolution unavailable",
            Self::Capacity => "certificate cache capacity exhausted",
            Self::Deadline => "certificate lookup deadline elapsed",
            Self::Stopped => "certificate worker stopped",
        })
    }
}
impl std::error::Error for CertificateLookupError {}

#[derive(Clone)]
pub struct TlsCertificateStore {
    shared: Arc<Shared>,
    sender: mpsc::Sender<Fetch>,
}
impl fmt::Debug for TlsCertificateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsCertificateStore")
            .field("usage", &self.usage())
            .finish()
    }
}
struct Shared {
    _structures: OwnedSemaphorePermit,
    #[cfg(test)]
    validation_gate: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    task_high_water: std::sync::atomic::AtomicUsize,
    retained_tasks: std::sync::atomic::AtomicUsize,
    watches: std::sync::atomic::AtomicUsize,
    observability: ObservabilityRecorder,
    wall: Arc<dyn Fn() -> i64 + Send + Sync>,
    state: Mutex<State>,
    bytes: Arc<Semaphore>,
    fetches: Arc<Semaphore>,
}
struct State {
    interests_changed: watch::Sender<()>,
    entries: HashMap<String, Entry>,
    serial: u64,
    stopped: bool,
}
struct Entry {
    incarnation: u64,
    floor: u64,
    refresh_requested: bool,
    generation: u64,
    touched: u64,
    value: Option<View>,
    pending: bool,
    changed: watch::Sender<u64>,
    last_error: Option<CertificateLookupError>,
    _memory: OwnedSemaphorePermit,
}
#[derive(Clone)]
struct View {
    revision: u64,
    config: Option<Arc<ServerConfig>>,
    metadata: Option<pb::CertificateMetadata>,
    expires: Instant,
    refresh: Instant,
}
struct Fetch {
    hostname: String,
    generation: u64,
    prior: Option<View>,
    floor: u64,
    memory: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CertificateCacheUsage {
    pub entries: usize,
    pub accounted_bytes: usize,
    pub fetches: usize,
}

impl TlsCertificateStore {
    pub fn new(resolver: Arc<dyn CertificateResolver>) -> (Self, TlsCertificateWorker) {
        Self::with_observability(resolver, ObservabilityRecorder::default())
    }
    pub fn with_observability(
        resolver: Arc<dyn CertificateResolver>,
        observability: ObservabilityRecorder,
    ) -> (Self, TlsCertificateWorker) {
        Self::with_clock_and_observability(resolver, Arc::new(wall_now), observability)
    }
    #[cfg(test)]
    fn with_clock(
        resolver: Arc<dyn CertificateResolver>,
        wall: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> (Self, TlsCertificateWorker) {
        Self::with_clock_and_observability(resolver, wall, ObservabilityRecorder::default())
    }
    fn with_clock_and_observability(
        resolver: Arc<dyn CertificateResolver>,
        wall: Arc<dyn Fn() -> i64 + Send + Sync>,
        observability: ObservabilityRecorder,
    ) -> (Self, TlsCertificateWorker) {
        let bytes = Arc::new(Semaphore::new(CERTIFICATE_CACHE_BYTES));
        let structures = bytes
            .clone()
            .try_acquire_many_owned(STRUCTURE_BYTES as u32)
            .unwrap();
        let shared = Arc::new(Shared {
            _structures: structures,
            #[cfg(test)]
            validation_gate: Mutex::new(None),
            task_high_water: std::sync::atomic::AtomicUsize::new(0),
            retained_tasks: std::sync::atomic::AtomicUsize::new(0),
            watches: std::sync::atomic::AtomicUsize::new(0),
            observability,
            wall,
            state: Mutex::new(State {
                interests_changed: watch::channel(()).0,
                entries: HashMap::new(),
                serial: 0,
                stopped: false,
            }),
            bytes,
            fetches: Arc::new(Semaphore::new(CERTIFICATE_FETCHES)),
        });
        let (sender, receiver) = mpsc::channel(CERTIFICATE_FETCHES);
        let cache = Self { shared, sender };
        let worker = TlsCertificateWorker::new(cache.clone(), resolver, receiver);
        (cache, worker)
    }
    /// A passthrough-only adapter has no certificate delivery worker.
    pub fn disabled() -> Self {
        struct Disabled;
        impl CertificateResolver for Disabled {
            fn resolve(&self, _: pb::ResolveTlsCertificateRequest) -> CertificateResolveFuture {
                Box::pin(async { Err(CertificateLookupError::Stopped) })
            }
        }
        let (cache, worker) = Self::new(Arc::new(Disabled));
        drop(worker);
        cache
    }
    pub fn usage(&self) -> CertificateCacheUsage {
        CertificateCacheUsage {
            entries: self.shared.state.lock().unwrap().entries.len(),
            accounted_bytes: CERTIFICATE_CACHE_BYTES - self.shared.bytes.available_permits(),
            fetches: CERTIFICATE_FETCHES - self.shared.fetches.available_permits(),
        }
    }
    /// Each waiter remains owned by its admitted handshake. Canceling a waiter
    /// does not cancel a shared bounded fetch needed by another waiter.
    pub async fn resolve(
        &self,
        sni: &str,
    ) -> Result<Option<Arc<ServerConfig>>, CertificateLookupError> {
        let hostname = TlsHostname::new(sni)
            .map_err(|_| CertificateLookupError::Invalid)?
            .to_string();
        let deadline = Instant::now() + CERTIFICATE_LOOKUP_TIMEOUT;
        let mut receiver = {
            let mut state = self.shared.state.lock().unwrap();
            if state.stopped {
                return Err(CertificateLookupError::Stopped);
            }
            state.serial = state
                .serial
                .checked_add(1)
                .ok_or(CertificateLookupError::Capacity)?;
            let serial = state.serial;
            if let Some(entry) = state.entries.get_mut(&hostname) {
                entry.touched = serial;
                if let Some(view) = &entry.value {
                    if usable(view, Instant::now(), (self.shared.wall)()) {
                        return Ok(view.config.clone());
                    }
                }
            } else {
                if state.entries.len() == CERTIFICATE_CACHE_ENTRIES {
                    evict_one(&mut state, None);
                }
                let memory = self
                    .shared
                    .bytes
                    .clone()
                    .try_acquire_many_owned(ENTRY_BYTES as u32)
                    .map_err(|_| CertificateLookupError::Capacity)?;
                let (changed, _) = watch::channel(0);
                state.entries.insert(
                    hostname.clone(),
                    Entry {
                        incarnation: serial,
                        floor: 0,
                        refresh_requested: false,
                        generation: serial,
                        touched: serial,
                        value: None,
                        pending: false,
                        changed,
                        last_error: None,
                        _memory: memory,
                    },
                );
            }
            state.interests_changed.send_replace(());
            self.queue_locked(&mut state, &hostname)?;
            state.entries[&hostname].changed.subscribe()
        };
        loop {
            {
                let state = self.shared.state.lock().unwrap();
                let entry = state
                    .entries
                    .get(&hostname)
                    .ok_or(CertificateLookupError::Unavailable)?;
                if let Some(view) = &entry.value {
                    if usable(view, Instant::now(), (self.shared.wall)()) {
                        return Ok(view.config.clone());
                    }
                }
                if !entry.pending {
                    return Err(entry
                        .last_error
                        .unwrap_or(CertificateLookupError::Unavailable));
                }
            }
            tokio::time::timeout_at(deadline, receiver.changed())
                .await
                .map_err(|_| CertificateLookupError::Deadline)?
                .map_err(|_| CertificateLookupError::Unavailable)?;
        }
    }
    fn queue_locked(
        &self,
        state: &mut State,
        hostname: &str,
    ) -> Result<(), CertificateLookupError> {
        let Some(entry) = state.entries.get(hostname) else {
            return Err(CertificateLookupError::Unavailable);
        };
        if entry.pending {
            return Ok(());
        }
        let slot = self
            .shared
            .fetches
            .clone()
            .try_acquire_owned()
            .map_err(|_| CertificateLookupError::Capacity)?;
        let memory = loop {
            if let Ok(p) = self
                .shared
                .bytes
                .clone()
                .try_acquire_many_owned(FETCH_BYTES as u32)
            {
                break p;
            }
            if !evict_one(state, Some(hostname)) {
                return Err(CertificateLookupError::Capacity);
            }
        };
        let entry = state.entries.get_mut(hostname).unwrap();
        let fetch = Fetch {
            hostname: hostname.to_owned(),
            generation: entry.generation,
            prior: entry.value.clone(),
            floor: entry.floor,
            memory,
            _slot: slot,
        };
        self.sender
            .try_send(fetch)
            .map_err(|_| CertificateLookupError::Capacity)?;
        entry.pending = true;
        entry.refresh_requested = false;
        entry.last_error = None;
        Ok(())
    }
    fn stop(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.stopped = true;
        state.entries.clear();
        state.interests_changed.send_replace(());
    }
}
fn evict_one(state: &mut State, exclude: Option<&str>) -> bool {
    let victim = state
        .entries
        .iter()
        .filter(|(key, _)| Some(key.as_str()) != exclude)
        .min_by_key(|(_, v)| v.touched)
        .map(|(k, _)| k.clone());
    if let Some(key) = victim {
        state.entries.remove(&key);
        state.interests_changed.send_replace(());
        true
    } else {
        false
    }
}

fn wall_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
fn usable(view: &View, now: Instant, wall: i64) -> bool {
    now < view.expires
        && view
            .metadata
            .as_ref()
            .is_none_or(|m| wall >= m.not_before_unix_millis && wall < m.not_after_unix_millis)
}
