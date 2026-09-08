use super::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

fn error() -> tonic::transport::Error {
    Endpoint::from_shared("\n").unwrap_err()
}
fn channel() -> tonic::transport::Channel {
    Endpoint::from_static("http://127.0.0.1:1").connect_lazy()
}
fn assert_timeout(result: Result<Option<tonic::transport::Channel>, Box<dyn Error + Send + Sync>>) {
    let error = result.unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert_eq!(
        error.to_string(),
        "frontline control-plane startup exceeded 60 seconds"
    );
}
struct OnDrop(Arc<AtomicBool>);
impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn a_stalled_attempt_is_dropped_at_the_total_deadline() {
    let dropped = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let result = connect_control_plane_with("test", &Shutdown::new(), || {
        let guard = OnDrop(dropped.clone());
        async move {
            let _guard = guard;
            std::future::pending().await
        }
    })
    .await;
    assert_timeout(result);
    assert_eq!(started.elapsed(), CONTROL_PLANE_CONNECT_TIMEOUT);
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn quick_errors_never_start_an_attempt_or_sleep_past_the_deadline() {
    let calls = AtomicUsize::new(0);
    let started = Instant::now();
    let result = connect_control_plane_with("test", &Shutdown::new(), || {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Err(error()))
    })
    .await;
    assert_timeout(result);
    assert_eq!(started.elapsed(), CONTROL_PLANE_CONNECT_TIMEOUT);
    assert_eq!(calls.load(Ordering::SeqCst), 60);
}

#[tokio::test(start_paused = true)]
async fn success_on_the_deadline_is_rejected() {
    let result = connect_control_plane_with("test", &Shutdown::new(), || async {
        sleep(CONTROL_PLANE_CONNECT_TIMEOUT).await;
        Ok(channel())
    })
    .await;
    assert_timeout(result);
}

#[tokio::test(start_paused = true)]
async fn success_after_clock_advances_inside_attempt_is_rejected() {
    let result = connect_control_plane_with("test", &Shutdown::new(), || async {
        tokio::time::advance(CONTROL_PLANE_CONNECT_TIMEOUT + Duration::from_secs(1)).await;
        Ok(channel())
    })
    .await;
    assert_timeout(result);
}

#[tokio::test(start_paused = true)]
async fn success_before_deadline_preserves_retry_and_return_behavior() {
    let calls = AtomicUsize::new(0);
    let started = Instant::now();
    let result = connect_control_plane_with("test", &Shutdown::new(), || {
        std::future::ready(if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(error())
        } else {
            Ok(channel())
        })
    })
    .await
    .unwrap();
    assert!(result.is_some());
    assert_eq!(started.elapsed(), CONTROL_PLANE_CONNECT_RETRY_BACKOFF);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn startup_cancellation_drops_stalled_attempt_without_waiting_for_timeout() {
    let dropped = Arc::new(AtomicBool::new(false));
    let shutdown = Shutdown::new();
    let task_shutdown = shutdown.clone();
    let task_dropped = dropped.clone();
    let task = tokio::spawn(async move {
        connect_control_plane_with("test", &task_shutdown, || {
            let guard = OnDrop(task_dropped.clone());
            async move {
                let _guard = guard;
                std::future::pending().await
            }
        })
        .await
    });
    tokio::task::yield_now().await;
    let started = Instant::now();
    shutdown.shutdown();
    assert!(task.await.unwrap().unwrap().is_none());
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn cancellation_before_start_does_not_attempt_connection() {
    let shutdown = Shutdown::new();
    shutdown.shutdown();
    let calls = AtomicUsize::new(0);
    let result = connect_control_plane_with("test", &shutdown, || {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(channel()))
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(result.is_none());
}
