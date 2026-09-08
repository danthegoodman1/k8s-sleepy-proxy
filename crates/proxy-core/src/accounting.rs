use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use tokio::sync::watch;

/// Counts active proxy sessions through RAII guards.
#[derive(Clone, Debug)]
pub struct ActiveConnectionCounter {
    inner: Arc<Inner>,
}

/// Watches for any active-count update after an idle baseline is observed.
#[derive(Debug)]
pub struct IdleActivityWatch {
    counter: ActiveConnectionCounter,
    updates: watch::Receiver<()>,
    baseline_revision: u64,
}

#[derive(Debug)]
struct Inner {
    active: AtomicUsize,
    revision: AtomicU64,
    updates: watch::Sender<()>,
}

/// A counted active connection.
///
/// Dropping or releasing this guard decrements the active count exactly once.
#[derive(Debug)]
#[must_use = "dropping the guard releases the active connection count"]
pub struct ActiveConnection {
    inner: Option<Arc<Inner>>,
}

impl ActiveConnectionCounter {
    pub fn new() -> Self {
        let (updates, _) = watch::channel(());

        Self {
            inner: Arc::new(Inner {
                active: AtomicUsize::new(0),
                revision: AtomicU64::new(0),
                updates,
            }),
        }
    }

    pub fn active(&self) -> usize {
        self.inner.active.load(Ordering::Acquire)
    }

    pub fn track(&self) -> ActiveConnection {
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        self.publish();

        ActiveConnection {
            inner: Some(Arc::clone(&self.inner)),
        }
    }

    pub async fn wait_for_zero(&self) {
        self.wait_for_count(0).await;
    }

    pub async fn wait_for_count(&self, expected: usize) {
        let mut updates = self.inner.updates.subscribe();

        loop {
            if self.active() == expected {
                return;
            }

            if updates.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn watch_for_activity_after_idle(&self) -> Option<IdleActivityWatch> {
        let updates = self.inner.updates.subscribe();
        let baseline_revision = self.revision();

        if self.active() != 0 {
            return None;
        }

        Some(IdleActivityWatch {
            counter: self.clone(),
            updates,
            baseline_revision,
        })
    }

    fn revision(&self) -> u64 {
        self.inner.revision.load(Ordering::Acquire)
    }

    fn publish(&self) {
        self.inner.revision.fetch_add(1, Ordering::AcqRel);
        self.inner.updates.send_replace(());
    }
}

impl IdleActivityWatch {
    pub async fn wait_for_update(mut self) {
        loop {
            if self.counter.revision() > self.baseline_revision {
                return;
            }

            if self.updates.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for ActiveConnectionCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl ActiveConnection {
    pub fn is_active(&self) -> bool {
        self.inner.is_some()
    }

    pub fn release(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };

        let previous = inner.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(
            previous > 0,
            "active connection count underflowed while releasing guard"
        );
        inner.revision.fetch_add(1, Ordering::AcqRel);
        inner.updates.send_replace(());
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::ActiveConnectionCounter;

    #[tokio::test]
    async fn active_count_tracks_guard_lifetime() {
        let counter = ActiveConnectionCounter::new();

        let guard = counter.track();
        assert_eq!(counter.active(), 1);

        drop(guard);
        counter.wait_for_zero().await;
        assert_eq!(counter.active(), 0);
    }

    #[tokio::test]
    async fn release_decrements_once() {
        let counter = ActiveConnectionCounter::new();
        let mut guard = counter.track();

        guard.release();
        guard.release();
        drop(guard);

        counter.wait_for_zero().await;
        assert_eq!(counter.active(), 0);
    }

    #[tokio::test]
    async fn idle_activity_watch_is_absent_when_work_is_already_active() {
        let counter = ActiveConnectionCounter::new();
        let _guard = counter.track();

        assert!(counter.watch_for_activity_after_idle().is_none());
    }

    #[tokio::test]
    async fn idle_activity_watch_resolves_for_brief_active_burst() {
        let counter = ActiveConnectionCounter::new();
        let watch = counter
            .watch_for_activity_after_idle()
            .expect("counter is idle");

        let guard = counter.track();
        drop(guard);

        watch.wait_for_update().await;
        assert_eq!(counter.active(), 0);
    }
}
