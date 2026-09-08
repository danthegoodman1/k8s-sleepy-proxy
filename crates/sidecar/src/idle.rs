use std::{error::Error, fmt, future::Future, time::Duration};

use proxy_core::{observability::recorder::ObservabilityRecorder, DrainTracker};
use sleepypods_types::{Generation, InstanceId};
use tokio::time::timeout;

mod control_plane;
pub use control_plane::ControlPlaneIdleReportOutcome;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdleReportConfig {
    idle_timeout: Duration,
    retry_backoff: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdleReportConfigError {
    ZeroIdleTimeout,
    ZeroRetryBackoff,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdleObservation {
    active_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportIdleRequest {
    instance_id: InstanceId,
    generation: Generation,
    observation: IdleObservation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdleReportOutcome {
    Reported(ReportIdleRequest),
    AlreadyReported,
}

#[derive(Clone, Debug)]
pub struct IdleDetector {
    instance_id: InstanceId,
    generation: Generation,
    config: IdleReportConfig,
    drain: DrainTracker,
    reported: bool,
    observability: ObservabilityRecorder,
}

impl IdleReportConfig {
    pub fn new(
        idle_timeout: Duration,
        retry_backoff: Duration,
    ) -> Result<Self, IdleReportConfigError> {
        if idle_timeout.is_zero() {
            return Err(IdleReportConfigError::ZeroIdleTimeout);
        }

        if retry_backoff.is_zero() {
            return Err(IdleReportConfigError::ZeroRetryBackoff);
        }

        Ok(Self {
            idle_timeout,
            retry_backoff,
        })
    }

    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    pub fn retry_backoff(&self) -> Duration {
        self.retry_backoff
    }
}

impl IdleObservation {
    pub fn zero_active() -> Self {
        Self { active_count: 0 }
    }

    pub fn active_count(&self) -> usize {
        self.active_count
    }

    pub fn observed_zero_active(&self) -> bool {
        self.active_count == 0
    }
}

impl ReportIdleRequest {
    pub fn new(
        instance_id: InstanceId,
        generation: Generation,
        observation: IdleObservation,
    ) -> Self {
        Self {
            instance_id,
            generation,
            observation,
        }
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn observation(&self) -> &IdleObservation {
        &self.observation
    }
}

impl IdleDetector {
    pub fn new(
        instance_id: InstanceId,
        generation: Generation,
        config: IdleReportConfig,
        drain: DrainTracker,
    ) -> Self {
        Self {
            instance_id,
            generation,
            config,
            drain,
            reported: false,
            observability: ObservabilityRecorder::default(),
        }
    }

    pub fn with_observability(
        instance_id: InstanceId,
        generation: Generation,
        config: IdleReportConfig,
        drain: DrainTracker,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            instance_id,
            generation,
            config,
            drain,
            reported: false,
            observability,
        }
    }

    pub fn config(&self) -> IdleReportConfig {
        self.config
    }

    pub fn drain_tracker(&self) -> &DrainTracker {
        &self.drain
    }

    pub fn has_reported(&self) -> bool {
        self.reported
    }

    pub async fn report_when_idle<Reporter, ReportFuture, ReportError>(
        &mut self,
        mut reporter: Reporter,
    ) -> IdleReportOutcome
    where
        Reporter: FnMut(ReportIdleRequest) -> ReportFuture,
        ReportFuture: Future<Output = Result<(), ReportError>>,
    {
        if self.reported {
            return IdleReportOutcome::AlreadyReported;
        }

        loop {
            self.wait_until_idle_timeout_elapsed().await;

            loop {
                let Some(activity) = self.drain.watch_for_activity_after_idle() else {
                    break;
                };
                let activity = activity.wait_for_update();
                tokio::pin!(activity);
                let request = self.report_request();
                let response = tokio::select! {
                    result = reporter(request.clone()) => result,
                    _ = &mut activity => break,
                };
                if response.is_ok() {
                    self.reported = true;
                    return IdleReportOutcome::Reported(request);
                }

                if timeout(self.config.retry_backoff(), &mut activity)
                    .await
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    async fn wait_until_idle_timeout_elapsed(&self) {
        loop {
            self.wait_until_zero_active().await;

            if !self
                .active_appeared_before(self.config.idle_timeout())
                .await
            {
                return;
            }
        }
    }

    async fn wait_until_zero_active(&self) {
        if self.drain.active_count() == 0 {
            return;
        }

        self.drain.wait_for_active_count(0).await;
    }

    async fn active_appeared_before(&self, duration: Duration) -> bool {
        let Some(activity) = self.drain.watch_for_activity_after_idle() else {
            return true;
        };

        timeout(duration, activity.wait_for_update()).await.is_ok()
    }

    fn report_request(&self) -> ReportIdleRequest {
        ReportIdleRequest::new(
            self.instance_id.clone(),
            self.generation,
            IdleObservation {
                active_count: self.drain.active_count(),
            },
        )
    }
}

impl fmt::Display for IdleReportConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroIdleTimeout => write!(f, "idle report timeout must be non-zero"),
            Self::ZeroRetryBackoff => write!(f, "idle report retry backoff must be non-zero"),
        }
    }
}

impl Error for IdleReportConfigError {}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    use super::{
        IdleDetector, IdleObservation, IdleReportConfig, IdleReportConfigError, IdleReportOutcome,
        ReportIdleRequest,
    };
    use proxy_core::DrainTracker;
    use sleepypods_types::{Generation, InstanceId};
    use tokio::{task::JoinHandle, time::Duration};

    const IDLE_TIMEOUT: Duration = Duration::from_secs(5);
    const RETRY_BACKOFF: Duration = Duration::from_secs(2);
    const ONE_MILLISECOND: Duration = Duration::from_millis(1);

    type RecordedRequests = Arc<Mutex<Vec<ReportIdleRequest>>>;

    #[derive(Debug, Clone, Copy)]
    struct TestReportError;

    #[test]
    fn config_rejects_zero_durations() {
        assert_eq!(
            IdleReportConfig::new(Duration::ZERO, RETRY_BACKOFF)
                .expect_err("zero idle timeout is invalid"),
            IdleReportConfigError::ZeroIdleTimeout
        );
        assert_eq!(
            IdleReportConfig::new(IDLE_TIMEOUT, Duration::ZERO)
                .expect_err("zero retry backoff is invalid"),
            IdleReportConfigError::ZeroRetryBackoff
        );
    }

    #[tokio::test(start_paused = true)]
    async fn report_fires_only_after_zero_active_for_full_idle_timeout() {
        let drain = drain_tracker();
        let calls = recorded_requests();
        let handle = spawn_successful_detector(detector_for(drain), Arc::clone(&calls));

        yield_now().await;
        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_recorded_count(&calls, 0);
        assert!(!handle.is_finished());

        advance(ONE_MILLISECOND).await;

        let (_detector, outcome) = handle.await.expect("detector task completed");
        assert_reported_zero_active(outcome);
        assert_recorded_count(&calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn active_work_prevents_reporting_until_it_closes() {
        let drain = drain_tracker();
        let permit = drain.try_acquire().expect("work admitted");
        let calls = recorded_requests();
        let handle = spawn_successful_detector(detector_for(drain.clone()), Arc::clone(&calls));

        yield_now().await;
        advance(IDLE_TIMEOUT * 2).await;
        assert_recorded_count(&calls, 0);
        assert!(!handle.is_finished());

        drop(permit);
        drain.wait_for_active_count(0).await;

        advance(IDLE_TIMEOUT).await;

        let (_detector, outcome) = handle.await.expect("detector task completed");
        assert_reported_zero_active(outcome);
        assert_recorded_count(&calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn new_active_work_during_idle_timeout_resets_timer() {
        let drain = drain_tracker();
        let calls = recorded_requests();
        let handle = spawn_successful_detector(detector_for(drain.clone()), Arc::clone(&calls));

        yield_now().await;
        advance(IDLE_TIMEOUT / 2).await;

        let permit = drain.try_acquire().expect("work admitted");
        drop(permit);
        drain.wait_for_active_count(0).await;
        yield_now().await;

        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_recorded_count(&calls, 0);
        assert!(!handle.is_finished());

        advance(ONE_MILLISECOND).await;

        let (_detector, outcome) = handle.await.expect("detector task completed");
        assert_reported_zero_active(outcome);
        assert_recorded_count(&calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn active_count_jump_to_two_during_idle_timeout_resets_timer() {
        let drain = drain_tracker();
        let calls = recorded_requests();
        let handle = spawn_successful_detector(detector_for(drain.clone()), Arc::clone(&calls));

        yield_now().await;
        advance(IDLE_TIMEOUT / 2).await;

        let first = drain.try_acquire().expect("first work item admitted");
        let second = drain.try_acquire().expect("second work item admitted");
        assert_eq!(drain.active_count(), 2);

        yield_now().await;
        drop(first);
        drop(second);
        drain.wait_for_active_count(0).await;

        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_recorded_count(&calls, 0);
        assert!(!handle.is_finished());

        advance(ONE_MILLISECOND).await;

        let (_detector, outcome) = handle.await.expect("detector task completed");
        assert_reported_zero_active(outcome);
        assert_recorded_count(&calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn successful_report_is_not_duplicated_by_same_detector() {
        let calls = recorded_requests();
        let handle = spawn_successful_detector(detector_for(drain_tracker()), Arc::clone(&calls));

        advance(IDLE_TIMEOUT).await;
        let (mut detector, outcome) = handle.await.expect("detector task completed");

        assert_reported_zero_active(outcome);
        assert!(detector.has_reported());
        assert_recorded_count(&calls, 1);

        let extra_calls = Arc::new(AtomicUsize::new(0));
        let second = detector
            .report_when_idle({
                let extra_calls = Arc::clone(&extra_calls);
                move |_request| {
                    let extra_calls = Arc::clone(&extra_calls);
                    async move {
                        extra_calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), TestReportError>(())
                    }
                }
            })
            .await;

        assert_eq!(second, IdleReportOutcome::AlreadyReported);
        assert_eq!(extra_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn report_error_retries_after_backoff_and_eventually_succeeds() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let handle = spawn_retrying_detector(detector_for(drain_tracker()), Arc::clone(&attempts));

        yield_now().await;
        advance(IDLE_TIMEOUT).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(!handle.is_finished());

        advance(RETRY_BACKOFF - ONE_MILLISECOND).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(!handle.is_finished());

        advance(ONE_MILLISECOND).await;

        let outcome = handle.await.expect("detector task completed");
        assert_reported_zero_active(outcome);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn active_work_during_retry_backoff_resets_to_full_idle_timeout() {
        let drain = drain_tracker();
        let attempts = Arc::new(AtomicUsize::new(0));
        let handle = spawn_retrying_detector(detector_for(drain.clone()), Arc::clone(&attempts));

        yield_now().await;
        advance(IDLE_TIMEOUT).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(!handle.is_finished());

        let permit = drain.try_acquire().expect("work admitted");
        yield_now().await;
        advance(RETRY_BACKOFF).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(!handle.is_finished());

        drop(permit);
        drain.wait_for_active_count(0).await;

        advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(!handle.is_finished());

        advance(ONE_MILLISECOND).await;

        let outcome = handle.await.expect("detector task completed");
        assert_reported_zero_active(outcome);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn new_detector_with_same_identity_can_report_after_idle_timeout() {
        let calls = recorded_requests();

        let first = spawn_successful_detector(detector_for(drain_tracker()), Arc::clone(&calls));
        advance(IDLE_TIMEOUT).await;
        let (_detector, first_outcome) = first.await.expect("first detector completed");
        assert_reported_zero_active(first_outcome);

        let second = spawn_successful_detector(detector_for(drain_tracker()), Arc::clone(&calls));
        advance(IDLE_TIMEOUT).await;
        let (_detector, second_outcome) = second.await.expect("second detector completed");
        assert_reported_zero_active(second_outcome);

        assert_recorded_count(&calls, 2);
    }

    fn spawn_successful_detector(
        mut detector: IdleDetector,
        requests: RecordedRequests,
    ) -> JoinHandle<(IdleDetector, IdleReportOutcome)> {
        tokio::spawn(async move {
            let outcome = detector
                .report_when_idle(move |request| {
                    let requests = Arc::clone(&requests);
                    async move {
                        requests
                            .lock()
                            .expect("recorded request lock not poisoned")
                            .push(request);
                        Ok::<(), TestReportError>(())
                    }
                })
                .await;
            (detector, outcome)
        })
    }

    fn spawn_retrying_detector(
        mut detector: IdleDetector,
        attempts: Arc<AtomicUsize>,
    ) -> JoinHandle<IdleReportOutcome> {
        tokio::spawn(async move {
            detector
                .report_when_idle(move |_request| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(TestReportError)
                        } else {
                            Ok(())
                        }
                    }
                })
                .await
        })
    }

    async fn advance(duration: Duration) {
        tokio::time::advance(duration).await;
        yield_now().await;
    }

    async fn yield_now() {
        tokio::task::yield_now().await;
    }

    fn assert_reported_zero_active(outcome: IdleReportOutcome) {
        let IdleReportOutcome::Reported(request) = outcome else {
            panic!("expected reported outcome");
        };

        assert_eq!(request.instance_id(), &instance_id());
        assert_eq!(request.generation(), generation());
        assert_eq!(
            request.observation(),
            &IdleObservation::zero_active(),
            "idle report should carry a zero-active observation"
        );
        assert!(request.observation().observed_zero_active());
    }

    fn assert_recorded_count(requests: &RecordedRequests, expected: usize) {
        assert_eq!(
            requests
                .lock()
                .expect("recorded request lock not poisoned")
                .len(),
            expected
        );
    }

    fn detector_for(drain: DrainTracker) -> IdleDetector {
        IdleDetector::new(instance_id(), generation(), config(), drain)
    }

    fn config() -> IdleReportConfig {
        IdleReportConfig::new(IDLE_TIMEOUT, RETRY_BACKOFF).expect("idle config builds")
    }

    fn drain_tracker() -> DrainTracker {
        DrainTracker::new(Duration::from_secs(30))
    }

    fn recorded_requests() -> RecordedRequests {
        Arc::new(Mutex::new(Vec::new()))
    }

    fn instance_id() -> InstanceId {
        InstanceId::new("instance-a").expect("instance id")
    }

    fn generation() -> Generation {
        Generation::new(7)
    }

    impl fmt::Display for TestReportError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("test report error")
        }
    }

    impl std::error::Error for TestReportError {}
}
