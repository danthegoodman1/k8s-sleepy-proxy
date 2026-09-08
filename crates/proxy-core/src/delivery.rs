//! Track bytes after a body frame has been produced, including HTTP/2 flow-control queues.
use crate::{AdmissionPermit, Shutdown};
use bytes::Buf;
use std::{
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

pub(crate) struct DeliveryState {
    _permit: Option<Arc<AdmissionPermit>>,
    _upgrade: Option<Arc<AdmissionPermit>>,
    _drain: Option<Arc<crate::DrainPermit>>,
    progress: Mutex<DeliveryProgress>,
    timeout: Duration,
    failed: DeliveryFailure,
    watchdog: OnceLock<tokio::task::JoinHandle<()>>,
}
pub(crate) enum DeliveryFailure {
    Downstream(Shutdown),
    Upstream(Option<hyper_util::client::legacy::connect::CaptureConnection>),
}
impl From<Shutdown> for DeliveryFailure {
    fn from(shutdown: Shutdown) -> Self {
        Self::Downstream(shutdown)
    }
}
impl DeliveryFailure {
    pub(crate) fn cancel(&self) {
        match self {
            Self::Downstream(shutdown) => shutdown.shutdown(),
            Self::Upstream(Some(captured)) => {
                if let Some(connected) = captured.connection_metadata().as_ref() {
                    connected.poison();
                    let mut extensions = http::Extensions::new();
                    connected.get_extras(&mut extensions);
                    if let Some(shutdown) = extensions.get::<crate::resources::UpstreamShutdown>() {
                        shutdown.0.shutdown();
                    }
                }
            }
            Self::Upstream(None) => {}
        }
    }
}

struct DeliveryProgress {
    pending: usize,
    last: tokio::time::Instant,
}
impl DeliveryState {
    pub(crate) fn new(
        permit: Option<Arc<AdmissionPermit>>,
        upgrade: Option<Arc<AdmissionPermit>>,
        drain: Option<Arc<crate::DrainPermit>>,
        timeout: Duration,
        failed: impl Into<DeliveryFailure>,
    ) -> Arc<Self> {
        Arc::new(Self {
            _permit: permit,
            _upgrade: upgrade,
            _drain: drain,
            progress: Mutex::new(DeliveryProgress {
                pending: 0,
                last: tokio::time::Instant::now(),
            }),
            timeout,
            failed: failed.into(),
            watchdog: OnceLock::new(),
        })
    }
    fn start(self: &Arc<Self>) {
        self.watchdog.get_or_init(|| {
            let weak = Arc::downgrade(self);
            // Retain admission until an aborted task is dropped too. Rapid short
            // responses cannot accumulate watchdogs outside the request limit.
            let permit = self._permit.clone();
            tokio::spawn(async move {
                let _permit = permit;
                loop {
                    let deadline = {
                        let Some(state) = weak.upgrade() else { return };
                        let progress = state.progress.lock().expect("delivery progress lock");
                        if progress.pending == 0 {
                            tokio::time::Instant::now() + state.timeout
                        } else {
                            progress.last + state.timeout
                        }
                    };
                    tokio::time::sleep_until(deadline).await;
                    let Some(state) = weak.upgrade() else { return };
                    let progress = state.progress.lock().expect("delivery progress lock");
                    if progress.pending > 0
                        && progress.last + state.timeout <= tokio::time::Instant::now()
                    {
                        // Hyper has no external per-stream reset handle; closing
                        // this connection releases its flow-controlled queues.
                        state.failed.cancel();
                        return;
                    }
                }
            })
        });
    }
}
impl Drop for DeliveryState {
    fn drop(&mut self) {
        if let Some(task) = self.watchdog.get() {
            task.abort();
        }
    }
}

pub(crate) struct DeliveryBuf<B: Buf> {
    inner: B,
    state: Arc<DeliveryState>,
}
impl<B: Buf> DeliveryBuf<B> {
    pub(crate) fn new(inner: B, state: Arc<DeliveryState>) -> Self {
        let bytes = inner.remaining();
        if bytes > 0 {
            {
                // Publish timestamp and zero-to-nonzero transition together, so
                // the watchdog cannot see fresh bytes with an old idle timestamp.
                let mut progress = state.progress.lock().expect("delivery progress lock");
                if progress.pending == 0 {
                    progress.last = tokio::time::Instant::now();
                }
                progress.pending += bytes;
            }
            state.start();
        }
        Self { inner, state }
    }
}
impl<B: Buf> Buf for DeliveryBuf<B> {
    fn remaining(&self) -> usize {
        self.inner.remaining()
    }
    fn chunk(&self) -> &[u8] {
        self.inner.chunk()
    }
    fn advance(&mut self, count: usize) {
        self.inner.advance(count);
        if count > 0 {
            let mut progress = self.state.progress.lock().expect("delivery progress lock");
            progress.pending -= count;
            progress.last = tokio::time::Instant::now();
        }
    }
    fn chunks_vectored<'a>(&'a self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        self.inner.chunks_vectored(dst)
    }
}
impl<B: Buf> Drop for DeliveryBuf<B> {
    fn drop(&mut self) {
        self.state
            .progress
            .lock()
            .expect("delivery progress lock")
            .pending -= self.inner.remaining();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    #[tokio::test(start_paused = true)]
    async fn fresh_frame_after_idle_publishes_progress_before_watchdog_can_expire() {
        let failed = Shutdown::new();
        let state = DeliveryState::new(None, None, None, Duration::from_secs(1), failed.clone());
        let mut first = DeliveryBuf::new(Bytes::from_static(b"a"), state.clone());
        first.advance(1);
        drop(first);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(!failed.is_shutdown());
        let mut next = DeliveryBuf::new(Bytes::from_static(b"b"), state.clone());
        tokio::task::yield_now().await;
        assert!(!failed.is_shutdown());
        next.advance(1);
        drop(next);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(!failed.is_shutdown());
    }
    #[tokio::test]
    async fn body_completion_aborts_owned_watchdog_and_returns_its_request_slot() {
        let admission = crate::AdmissionLimiter::new(1);
        let state = DeliveryState::new(
            Some(Arc::new(admission.try_acquire().unwrap())),
            None,
            None,
            Duration::from_secs(3600),
            Shutdown::new(),
        );
        let weak = Arc::downgrade(&state);
        let mut bytes = DeliveryBuf::new(Bytes::from_static(b"ok"), state.clone());
        drop(state);
        assert_eq!(admission.in_flight(), 1, "queued bytes retain capacity");
        bytes.advance(2);
        drop(bytes);
        assert!(
            weak.upgrade().is_none(),
            "watchdog does not retain delivery state"
        );
        tokio::time::timeout(Duration::from_secs(1), admission.wait_for_in_flight(0))
            .await
            .unwrap();
    }
}
