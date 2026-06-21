use std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Bounded admission control for in-flight work.
#[derive(Clone, Debug)]
pub struct AdmissionLimiter {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    semaphore: Arc<Semaphore>,
    limit: usize,
    in_flight: AtomicUsize,
    updates: watch::Sender<usize>,
}

/// Capacity admitted by an [`AdmissionLimiter`].
///
/// Dropping or releasing this permit returns exactly one unit of capacity.
#[derive(Debug)]
#[must_use = "dropping the permit releases admission capacity"]
pub struct AdmissionPermit {
    inner: Option<Arc<Inner>>,
    permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionError {
    Saturated { limit: usize },
    Closed,
}

impl AdmissionLimiter {
    pub fn new(max_in_flight: usize) -> Self {
        let (updates, _) = watch::channel(0);

        Self {
            inner: Arc::new(Inner {
                semaphore: Arc::new(Semaphore::new(max_in_flight)),
                limit: max_in_flight,
                in_flight: AtomicUsize::new(0),
                updates,
            }),
        }
    }

    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    pub fn available(&self) -> usize {
        self.inner.semaphore.available_permits()
    }

    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    pub fn close(&self) {
        self.inner.semaphore.close();
    }

    pub fn try_acquire(&self) -> Result<AdmissionPermit, AdmissionError> {
        let permit = self
            .inner
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|error| self.acquire_error(error))?;

        Ok(self.admit(permit))
    }

    pub async fn acquire(&self) -> Result<AdmissionPermit, AdmissionError> {
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AdmissionError::Closed)?;

        Ok(self.admit(permit))
    }

    pub async fn wait_for_in_flight(&self, expected: usize) {
        let mut updates = self.inner.updates.subscribe();

        loop {
            if self.in_flight() == expected {
                return;
            }

            if updates.changed().await.is_err() {
                return;
            }
        }
    }

    fn admit(&self, permit: OwnedSemaphorePermit) -> AdmissionPermit {
        let next = self.inner.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.inner.updates.send_replace(next);

        AdmissionPermit {
            inner: Some(Arc::clone(&self.inner)),
            permit: Some(permit),
        }
    }

    fn acquire_error(&self, error: TryAcquireError) -> AdmissionError {
        match error {
            TryAcquireError::NoPermits => AdmissionError::Saturated {
                limit: self.inner.limit,
            },
            TryAcquireError::Closed => AdmissionError::Closed,
        }
    }
}

impl AdmissionPermit {
    pub fn release(&mut self) {
        let permit = self.permit.take();
        let inner = self.inner.take();

        let Some(inner) = inner else {
            return;
        };

        drop(permit);
        let previous = inner.in_flight.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(
            previous > 0,
            "admission in-flight count underflowed while releasing permit"
        );
        inner.updates.send_replace(previous.saturating_sub(1));
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.release();
    }
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Saturated { limit } => write!(
                f,
                "admission limit of {limit} in-flight operation(s) is saturated"
            ),
            Self::Closed => write!(f, "admission limiter is closed"),
        }
    }
}

impl Error for AdmissionError {}

#[cfg(test)]
mod tests {
    use super::{AdmissionError, AdmissionLimiter};

    #[tokio::test]
    async fn try_acquire_enforces_limit_and_releases_on_drop() {
        let limiter = AdmissionLimiter::new(1);
        let permit = limiter.try_acquire().expect("first operation admitted");

        assert_eq!(limiter.in_flight(), 1);
        assert_eq!(limiter.available(), 0);
        assert_eq!(
            limiter.try_acquire().expect_err("limit is saturated"),
            AdmissionError::Saturated { limit: 1 }
        );

        drop(permit);
        limiter.wait_for_in_flight(0).await;

        assert_eq!(limiter.in_flight(), 0);
        assert_eq!(limiter.available(), 1);
        let _permit = limiter
            .try_acquire()
            .expect("capacity is returned after drop");
    }

    #[tokio::test]
    async fn explicit_release_is_idempotent() {
        let limiter = AdmissionLimiter::new(1);
        let mut permit = limiter.try_acquire().expect("operation admitted");

        permit.release();
        permit.release();
        drop(permit);
        limiter.wait_for_in_flight(0).await;

        assert_eq!(limiter.in_flight(), 0);
        assert_eq!(limiter.available(), 1);
    }

    #[tokio::test]
    async fn acquire_waiter_observes_release() {
        let limiter = AdmissionLimiter::new(1);
        let permit = limiter.try_acquire().expect("first operation admitted");

        let waiter = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.acquire().await }
        });

        tokio::task::yield_now().await;
        assert_eq!(limiter.in_flight(), 1);
        assert_eq!(limiter.available(), 0);

        drop(permit);

        let permit = waiter
            .await
            .expect("waiter task completed")
            .expect("waiter admitted after release");
        assert_eq!(limiter.in_flight(), 1);

        drop(permit);
        limiter.wait_for_in_flight(0).await;
        assert_eq!(limiter.available(), 1);
    }

    #[tokio::test]
    async fn cancelling_pending_acquire_does_not_leak_capacity() {
        let limiter = AdmissionLimiter::new(1);
        let permit = limiter.try_acquire().expect("first operation admitted");

        let waiter = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.acquire().await }
        });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter
            .await
            .expect_err("waiter is cancelled")
            .is_cancelled());

        drop(permit);
        limiter.wait_for_in_flight(0).await;

        assert_eq!(limiter.in_flight(), 0);
        assert_eq!(limiter.available(), 1);
    }

    #[tokio::test]
    async fn closed_limiter_rejects_new_admission() {
        let limiter = AdmissionLimiter::new(1);

        limiter.close();

        assert_eq!(
            limiter.try_acquire().expect_err("closed limiter rejects"),
            AdmissionError::Closed
        );
    }
}
