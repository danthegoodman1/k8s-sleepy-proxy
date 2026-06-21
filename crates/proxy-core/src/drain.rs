use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::accounting::{ActiveConnection, ActiveConnectionCounter};
use crate::timeout::with_timeout;

/// Tracks active sessions and rejects new work once drain starts.
#[derive(Clone, Debug)]
pub struct DrainTracker {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
    active: ActiveConnectionCounter,
    grace_timeout: Duration,
}

#[derive(Debug, Default)]
struct State {
    draining: bool,
}

/// Active work admitted before drain started.
#[derive(Debug)]
#[must_use = "dropping the permit releases the active connection count"]
pub struct DrainPermit {
    active: ActiveConnection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainError {
    Draining,
    GraceTimeout { timeout: Duration, active: usize },
}

impl DrainTracker {
    pub fn new(grace_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                active: ActiveConnectionCounter::new(),
                grace_timeout,
            }),
        }
    }

    pub fn grace_timeout(&self) -> Duration {
        self.inner.grace_timeout
    }

    pub fn active_count(&self) -> usize {
        self.inner.active.active()
    }

    pub fn is_draining(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("drain tracker lock poisoned")
            .draining
    }

    pub fn try_acquire(&self) -> Result<DrainPermit, DrainError> {
        let state = self
            .inner
            .state
            .lock()
            .expect("drain tracker lock poisoned");

        if state.draining {
            return Err(DrainError::Draining);
        }

        Ok(DrainPermit {
            active: self.inner.active.track(),
        })
    }

    pub fn start_drain(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("drain tracker lock poisoned");
        state.draining = true;
    }

    pub async fn wait_for_idle(&self) -> Result<(), DrainError> {
        if self.active_count() == 0 {
            return Ok(());
        }

        match with_timeout(self.inner.grace_timeout, self.inner.active.wait_for_zero()).await {
            Ok(()) => Ok(()),
            Err(_) if self.active_count() == 0 => Ok(()),
            Err(_) => Err(DrainError::GraceTimeout {
                timeout: self.inner.grace_timeout,
                active: self.active_count(),
            }),
        }
    }

    pub async fn drain(&self) -> Result<(), DrainError> {
        self.start_drain();
        self.wait_for_idle().await
    }

    pub async fn wait_for_active_count(&self, expected: usize) {
        self.inner.active.wait_for_count(expected).await;
    }
}

impl DrainPermit {
    pub fn release(&mut self) {
        self.active.release();
    }
}

impl fmt::Display for DrainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Draining => write!(f, "drain has started; new work is rejected"),
            Self::GraceTimeout { timeout, active } => write!(
                f,
                "drain grace timeout of {:?} elapsed with {} active connection(s)",
                timeout, active
            ),
        }
    }
}

impl Error for DrainError {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{DrainError, DrainTracker};

    #[tokio::test]
    async fn drain_rejects_new_work_and_waits_for_existing_work() {
        let tracker = DrainTracker::new(Duration::from_secs(5));
        let mut permit = tracker.try_acquire().expect("connection admitted");

        tracker.start_drain();

        assert_eq!(tracker.try_acquire().unwrap_err(), DrainError::Draining);
        assert!(tracker.is_draining());
        assert_eq!(tracker.active_count(), 1);

        let waiter = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.wait_for_idle().await }
        });

        permit.release();

        assert_eq!(waiter.await.expect("waiter task completed"), Ok(()));
        assert_eq!(tracker.active_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn drain_reports_grace_timeout_with_active_work() {
        let tracker = DrainTracker::new(Duration::from_secs(5));
        let _permit = tracker.try_acquire().expect("connection admitted");

        tracker.start_drain();

        let waiter = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.wait_for_idle().await }
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;

        assert_eq!(
            waiter
                .await
                .expect("waiter task completed")
                .expect_err("drain should time out"),
            DrainError::GraceTimeout {
                timeout: Duration::from_secs(5),
                active: 1,
            }
        );
    }
}
