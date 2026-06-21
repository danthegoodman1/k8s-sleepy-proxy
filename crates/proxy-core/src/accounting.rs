use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use tokio::sync::watch;

/// Counts active proxy sessions through RAII guards.
#[derive(Clone, Debug)]
pub struct ActiveConnectionCounter {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    active: AtomicUsize,
    updates: watch::Sender<usize>,
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
        let (updates, _) = watch::channel(0);

        Self {
            inner: Arc::new(Inner {
                active: AtomicUsize::new(0),
                updates,
            }),
        }
    }

    pub fn active(&self) -> usize {
        self.inner.active.load(Ordering::Acquire)
    }

    pub fn track(&self) -> ActiveConnection {
        let next = self.inner.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.inner.updates.send_replace(next);

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
}

impl Default for ActiveConnectionCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl ActiveConnection {
    pub fn release(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };

        let previous = inner.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(
            previous > 0,
            "active connection count underflowed while releasing guard"
        );
        inner.updates.send_replace(previous.saturating_sub(1));
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
}
