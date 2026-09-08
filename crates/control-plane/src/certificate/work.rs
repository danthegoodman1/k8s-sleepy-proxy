//! Separate certificate work capacity, leaving at least one shared database slot
//! for route/lifecycle work. A detached blocking task retains both its operation
//! and crypto permits until it actually finishes.
use crate::store::{StoreError, StoreResult};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[derive(Clone, Debug)]
pub(crate) struct CertificateWorkLimits {
    operations: Arc<Semaphore>,
    crypto: Arc<Semaphore>,
}
impl CertificateWorkLimits {
    pub fn new(database_capacity: usize) -> Self {
        Self {
            operations: Arc::new(Semaphore::new(database_capacity.saturating_sub(1).min(4))),
            crypto: Arc::new(Semaphore::new(2)),
        }
    }
    pub fn acquire(&self) -> StoreResult<CertificateWork> {
        let permit = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| StoreError::unavailable("certificate work capacity exhausted"))?;
        Ok(CertificateWork {
            operation: Arc::new(permit),
            crypto: self.crypto.clone(),
        })
    }
}
pub(crate) struct CertificateWork {
    operation: Arc<OwnedSemaphorePermit>,
    crypto: Arc<Semaphore>,
}
impl CertificateWork {
    pub async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> StoreResult<T> + Send + 'static,
    ) -> StoreResult<T> {
        let crypto = self
            .crypto
            .clone()
            .try_acquire_owned()
            .map_err(|_| StoreError::unavailable("certificate crypto capacity exhausted"))?;
        let operation = self.operation.clone();
        tokio::task::spawn_blocking(move || {
            let (_operation, _crypto) = (operation, crypto);
            work()
        })
        .await
        .map_err(|_| StoreError::internal("certificate crypto worker failed"))?
    }
}

/// A checkout may have sent SQL even when its query future is dropped. Never
/// return that session to Fast recycling until its queued rollback and a final
/// protocol round trip have drained. At most one drain task exists per operation
/// slot. Timeout or task/runtime cancellation discards the session instead.
pub(crate) struct CertificateConnection {
    client: Option<deadpool_postgres::Client>,
    operation: Arc<OwnedSemaphorePermit>,
}
impl std::ops::Deref for CertificateConnection {
    type Target = deadpool_postgres::Client;
    fn deref(&self) -> &Self::Target {
        self.client.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for CertificateConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().unwrap()
    }
}
struct PendingDrain {
    client: Option<deadpool_postgres::Client>,
    _operation: Arc<OwnedSemaphorePermit>,
}
impl Drop for PendingDrain {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Remove from pool before ClientWrapper aborts its connection task.
            drop(deadpool_postgres::Client::take(client));
        }
    }
}
impl PendingDrain {
    async fn run(mut self) {
        let drained = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.client.as_ref().unwrap().simple_query(""),
        )
        .await;
        if matches!(drained, Ok(Ok(_))) {
            drop(self.client.take()); // Only this successful path recycles.
        }
    }
}
impl Drop for CertificateConnection {
    fn drop(&mut self) {
        let drain = PendingDrain {
            client: self.client.take(),
            _operation: self.operation.clone(),
        };
        // Holding the operation permit bounds queued/running drain tasks as
        // well as SQL. Dropping this task also discards its client safely.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(drain.run());
        } else {
            drop(drain); // Safe discard even outside an entered runtime.
        }
    }
}
impl CertificateWork {
    pub async fn client(
        &self,
        store: &crate::postgres::PostgresStore,
    ) -> StoreResult<CertificateConnection> {
        Ok(CertificateConnection {
            client: Some(store.client().await?),
            operation: self.operation.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancelled_crypto_keeps_both_permits_until_worker_really_returns() {
        let limits = CertificateWorkLimits::new(2);
        let work = limits.acquire().unwrap();
        let (entered_tx, entered) = tokio::sync::oneshot::channel();
        let (release_tx, release) = std::sync::mpsc::channel::<()>();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            work.blocking(move || {
                let _ = entered_tx.send(());
                let _ = release.recv();
                Ok(())
            })
            .await
        });
        entered.await.unwrap();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        assert_eq!(limits.operations.available_permits(), 0);
        assert_eq!(limits.crypto.available_permits(), 1);
        assert!(
            limits.acquire().is_err(),
            "cancelled RPC must not admit another operation while its worker runs"
        );
        drop(release_tx);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while limits.operations.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(limits.crypto.available_permits(), 2);
        assert!(limits.acquire().is_ok());
    }
    #[test]
    fn operation_capacity_always_reserves_ordinary_database_capacity() {
        for capacity in [1, 2, 4, 16, 1024] {
            let limits = CertificateWorkLimits::new(capacity);
            assert!(limits.operations.available_permits() < capacity);
            assert!(limits.operations.available_permits() <= 4);
        }
    }
}
