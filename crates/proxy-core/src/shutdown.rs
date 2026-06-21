use tokio_util::sync::CancellationToken;

/// Cooperative shutdown signal with support for structured child tokens.
#[derive(Clone, Debug)]
pub struct Shutdown {
    token: CancellationToken,
}

impl Shutdown {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    pub fn child_token(&self) -> Self {
        Self {
            token: self.token.child_token(),
        }
    }

    pub fn shutdown(&self) {
        self.token.cancel();
    }

    pub fn cancel(&self) {
        self.shutdown();
    }

    pub fn is_shutdown(&self) -> bool {
        self.token.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::Shutdown;
    use crate::timeout::with_timeout;

    #[tokio::test]
    async fn shutdown_wakes_existing_waiters() {
        let shutdown = Shutdown::new();
        let (waiting_tx, waiting_rx) = oneshot::channel();

        let waiter = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                waiting_tx.send(()).expect("test waits for waiter setup");
                shutdown.cancelled().await;
            }
        });

        waiting_rx.await.expect("waiter is ready");
        assert!(!shutdown.is_shutdown());

        shutdown.shutdown();

        waiter.await.expect("waiter task completed");
        assert!(shutdown.is_shutdown());
    }

    #[tokio::test(start_paused = true)]
    async fn new_waiters_observe_already_triggered_shutdown() {
        let shutdown = Shutdown::new();

        shutdown.shutdown();

        with_timeout(Duration::from_secs(1), shutdown.cancelled())
            .await
            .expect("already shutdown signal is observed immediately");
    }

    #[tokio::test]
    async fn parent_shutdown_wakes_child_waiters() {
        let parent = Shutdown::new();
        let child = parent.child_token();
        let (waiting_tx, waiting_rx) = oneshot::channel();

        let waiter = tokio::spawn({
            let child = child.clone();
            async move {
                waiting_tx
                    .send(())
                    .expect("test waits for child waiter setup");
                child.cancelled().await;
            }
        });

        waiting_rx.await.expect("child waiter is ready");
        parent.shutdown();

        waiter.await.expect("child waiter completed");
        assert!(parent.is_shutdown());
        assert!(child.is_shutdown());
    }

    #[test]
    fn child_shutdown_does_not_cancel_parent() {
        let parent = Shutdown::new();
        let child = parent.child_token();

        child.shutdown();

        assert!(child.is_shutdown());
        assert!(!parent.is_shutdown());
    }
}
