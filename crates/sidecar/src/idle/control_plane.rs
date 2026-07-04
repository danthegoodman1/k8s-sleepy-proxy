use proxy_core::observability::{
    metrics::RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
    recorder::{LifecycleLogEvent, LogField, MetricObservation, EVENT_IDLE_REPORT},
    Operation, Outcome,
};
use sleepypods_types::Generation;

use crate::control_plane_transport::{
    ReportIdleClient, ReportIdleResponse, ReportIdleUnavailableReason,
};

use super::{IdleDetector, ReportIdleRequest};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlPlaneIdleReportOutcome {
    Accepted(ReportIdleRequest),
    AlreadyDraining(ReportIdleRequest),
    GenerationConflict {
        request: ReportIdleRequest,
        expected_generation: Generation,
        actual_generation: Generation,
    },
    Unavailable {
        request: ReportIdleRequest,
        reason: ReportIdleUnavailableReason,
    },
    AlreadyReported,
}

impl IdleDetector {
    pub async fn report_to_control_plane_when_idle<Client>(
        &mut self,
        client: &mut Client,
    ) -> ControlPlaneIdleReportOutcome
    where
        Client: ReportIdleClient,
    {
        if self.reported {
            return ControlPlaneIdleReportOutcome::AlreadyReported;
        }

        loop {
            self.wait_until_idle_timeout_elapsed().await;

            loop {
                let Some(activity) = self.drain.watch_for_activity_after_idle() else {
                    break;
                };
                let request = self.report_request();

                let response = tokio::select! {
                    response = client.report_idle(request.clone()) => Some(response),
                    _ = activity.wait_for_update() => None,
                };

                match response {
                    None => break,
                    Some(Ok(response)) => {
                        let outcome = idle_report_outcome(&response);
                        self.record_idle_report(&request, outcome);
                        if idle_report_is_terminal(&response) {
                            self.reported = true;
                            return control_plane_idle_report_outcome(request, response);
                        }
                    }
                    Some(Err(_error)) => {
                        self.record_idle_report(&request, Outcome::Error);
                    }
                }

                if self
                    .active_appeared_before(self.config.retry_backoff())
                    .await
                {
                    break;
                }
            }
        }
    }
}

impl IdleDetector {
    fn record_idle_report(&self, request: &ReportIdleRequest, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
            vec![Operation::ReportIdle.metric_label(), outcome.metric_label()],
            1.0,
        ));
        self.observability.record_log(LifecycleLogEvent::new(
            EVENT_IDLE_REPORT,
            vec![
                LogField::instance_id(request.instance_id().as_str()),
                LogField::generation(request.generation().get()),
                LogField::active_count(request.observation().active_count()),
            ],
        ));
    }
}

fn idle_report_is_terminal(response: &ReportIdleResponse) -> bool {
    matches!(
        response,
        ReportIdleResponse::Accepted { .. } | ReportIdleResponse::AlreadyDraining { .. }
    )
}

fn idle_report_outcome(response: &ReportIdleResponse) -> Outcome {
    match response {
        ReportIdleResponse::Accepted { .. } => Outcome::Success,
        ReportIdleResponse::AlreadyDraining { .. } => Outcome::AlreadyDraining,
        ReportIdleResponse::GenerationConflict { .. } | ReportIdleResponse::Unavailable { .. } => {
            Outcome::Rejected
        }
    }
}

fn control_plane_idle_report_outcome(
    request: ReportIdleRequest,
    response: ReportIdleResponse,
) -> ControlPlaneIdleReportOutcome {
    match response {
        ReportIdleResponse::Accepted { .. } => ControlPlaneIdleReportOutcome::Accepted(request),
        ReportIdleResponse::AlreadyDraining { .. } => {
            ControlPlaneIdleReportOutcome::AlreadyDraining(request)
        }
        ReportIdleResponse::GenerationConflict {
            expected_generation,
            actual_generation,
            ..
        } => ControlPlaneIdleReportOutcome::GenerationConflict {
            request,
            expected_generation,
            actual_generation,
        },
        ReportIdleResponse::Unavailable { reason, .. } => {
            ControlPlaneIdleReportOutcome::Unavailable { request, reason }
        }
    }
}
#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        fmt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use proxy_core::{
        observability::{
            metrics::RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME,
            recorder::{
                InMemoryObservability, ObservabilityEvent, EVENT_IDLE_REPORT, FIELD_ACTIVE_COUNT,
                FIELD_INSTANCE_ID,
            },
        },
        DrainTracker,
    };
    use sleepypods_types::{Generation, InstanceId};

    use crate::{
        control_plane_transport::{
            ReportIdleClient, ReportIdleFuture, ReportIdleResponse, ReportIdleUnavailableReason,
        },
        idle::{IdleObservation, IdleReportConfig},
    };

    use super::{ControlPlaneIdleReportOutcome, IdleDetector, ReportIdleRequest};

    const IDLE_TIMEOUT: Duration = Duration::from_secs(5);
    const RETRY_BACKOFF: Duration = Duration::from_secs(2);
    const ONE_MILLISECOND: Duration = Duration::from_millis(1);

    type RecordedRequests = Arc<Mutex<Vec<ReportIdleRequest>>>;

    #[derive(Debug, Clone, Copy)]
    struct TestReportError;

    #[tokio::test(start_paused = true)]
    async fn accepted_marks_reported_and_is_idempotent() {
        let mut detector = detector_for(drain_tracker());
        let mut client = FakeReportIdleClient::new([Ok(accepted_response())]);
        let requests = client.requests();

        let (outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
            }
        );

        assert_eq!(
            outcome,
            ControlPlaneIdleReportOutcome::Accepted(expected_report_request())
        );
        assert!(detector.has_reported());
        assert_recorded_count(&requests, 1);

        let second = detector
            .report_to_control_plane_when_idle(&mut client)
            .await;

        assert_eq!(second, ControlPlaneIdleReportOutcome::AlreadyReported);
        assert_recorded_count(&requests, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn accepted_idle_report_records_metric_and_identity_fields() {
        let sink = InMemoryObservability::default();
        let mut detector = IdleDetector::with_observability(
            instance_id(),
            generation(),
            config(),
            drain_tracker(),
            sink.recorder(),
        );
        let mut client = FakeReportIdleClient::new([Ok(accepted_response())]);

        let (_outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
            }
        );

        let events = sink.events();
        assert!(events.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Metric(metric)
                if metric.name() == RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME
                    && metric.labels().iter().any(|label| label.value() == "report_idle")
                    && metric.labels().iter().any(|label| label.value() == "success")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Log(log)
                if log.name() == EVENT_IDLE_REPORT
                    && log.field_value(FIELD_INSTANCE_ID) == Some("instance-a")
                    && log.field_value(FIELD_ACTIVE_COUNT) == Some("0")
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn already_draining_marks_reported() {
        let mut detector = detector_for(drain_tracker());
        let mut client = FakeReportIdleClient::new([Ok(already_draining_response())]);
        let requests = client.requests();

        let (outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
            }
        );

        assert_eq!(
            outcome,
            ControlPlaneIdleReportOutcome::AlreadyDraining(expected_report_request())
        );
        assert!(detector.has_reported());
        assert_recorded_count(&requests, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn already_draining_records_distinct_idle_report_outcome() {
        let sink = InMemoryObservability::default();
        let mut detector = IdleDetector::with_observability(
            instance_id(),
            generation(),
            config(),
            drain_tracker(),
            sink.recorder(),
        );
        let mut client = FakeReportIdleClient::new([Ok(already_draining_response())]);

        let (_outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
            }
        );

        assert!(sink.events().iter().any(|event| matches!(
            event,
            ObservabilityEvent::Metric(metric)
                if metric.name() == RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME
                    && metric.labels().iter().any(|label| label.value() == "report_idle")
                    && metric.labels().iter().any(|label| label.value() == "already_draining")
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn generation_conflict_rearms_until_accepted() {
        let mut detector = detector_for(drain_tracker());
        let mut client = FakeReportIdleClient::new([
            Ok(generation_conflict_response()),
            Ok(accepted_response()),
        ]);
        let requests = client.requests();

        let (outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
                assert_recorded_count(&requests, 1);
                advance(RETRY_BACKOFF - ONE_MILLISECOND).await;
                assert_recorded_count(&requests, 1);
                advance(ONE_MILLISECOND).await;
            }
        );

        assert_eq!(
            outcome,
            ControlPlaneIdleReportOutcome::Accepted(expected_report_request())
        );
        assert!(detector.has_reported());
        assert_recorded_count(&requests, 2);

        let second = detector
            .report_to_control_plane_when_idle(&mut client)
            .await;

        assert_eq!(second, ControlPlaneIdleReportOutcome::AlreadyReported);
        assert_recorded_count(&requests, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_waking_rearms_until_accepted() {
        let mut detector = detector_for(drain_tracker());
        let mut client =
            FakeReportIdleClient::new([Ok(unavailable_response()), Ok(accepted_response())]);
        let requests = client.requests();

        let (outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
                assert_recorded_count(&requests, 1);
                advance(RETRY_BACKOFF - ONE_MILLISECOND).await;
                assert_recorded_count(&requests, 1);
                advance(ONE_MILLISECOND).await;
            }
        );

        assert_eq!(
            outcome,
            ControlPlaneIdleReportOutcome::Accepted(expected_report_request())
        );
        assert!(detector.has_reported());
        assert_recorded_count(&requests, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn transport_error_retries_after_backoff_and_eventually_succeeds() {
        let mut detector = detector_for(drain_tracker());
        let mut client = FakeReportIdleClient::new([Err(TestReportError), Ok(accepted_response())]);
        let requests = client.requests();

        let (outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
                assert_recorded_count(&requests, 1);

                advance(RETRY_BACKOFF - ONE_MILLISECOND).await;
                assert_recorded_count(&requests, 1);

                advance(ONE_MILLISECOND).await;
            }
        );

        assert_eq!(
            outcome,
            ControlPlaneIdleReportOutcome::Accepted(expected_report_request())
        );
        assert_recorded_count(&requests, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn active_work_during_retry_backoff_resets_to_idle_timeout() {
        let drain = drain_tracker();
        let mut detector = detector_for(drain.clone());
        let mut client = FakeReportIdleClient::new([Err(TestReportError), Ok(accepted_response())]);
        let requests = client.requests();

        let (outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
                assert_recorded_count(&requests, 1);

                let permit = drain.try_acquire().expect("work admitted");
                yield_now().await;
                advance(RETRY_BACKOFF).await;
                assert_recorded_count(&requests, 1);

                drop(permit);
                drain.wait_for_active_count(0).await;

                advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
                assert_recorded_count(&requests, 1);

                advance(ONE_MILLISECOND).await;
            }
        );

        assert_eq!(
            outcome,
            ControlPlaneIdleReportOutcome::Accepted(expected_report_request())
        );
        assert_recorded_count(&requests, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn active_work_during_report_round_trip_aborts_report_and_rearms() {
        let drain = drain_tracker();
        let mut detector = detector_for(drain.clone());
        let mut client =
            FakeReportIdleClient::new([Ok(accepted_response()), Ok(accepted_response())])
                .with_response_delay(RETRY_BACKOFF);
        let requests = client.requests();

        let (outcome, ()) = tokio::join!(
            detector.report_to_control_plane_when_idle(&mut client),
            async {
                yield_now().await;
                advance(IDLE_TIMEOUT).await;
                assert_recorded_count(&requests, 1);
                yield_now().await;

                let permit = drain.try_acquire().expect("work admitted during RTT");
                yield_now().await;
                drop(permit);
                drain.wait_for_active_count(0).await;
                yield_now().await;

                advance(IDLE_TIMEOUT - ONE_MILLISECOND).await;
                assert_recorded_count(&requests, 1);

                advance(ONE_MILLISECOND).await;
                assert_recorded_count(&requests, 2);
                advance(RETRY_BACKOFF).await;
            }
        );

        assert_eq!(
            outcome,
            ControlPlaneIdleReportOutcome::Accepted(expected_report_request())
        );
        assert_recorded_count(&requests, 2);
    }

    async fn advance(duration: Duration) {
        tokio::time::advance(duration).await;
        yield_now().await;
    }

    async fn yield_now() {
        tokio::task::yield_now().await;
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

    fn expected_report_request() -> ReportIdleRequest {
        ReportIdleRequest::new(instance_id(), generation(), IdleObservation::zero_active())
    }

    fn accepted_response() -> ReportIdleResponse {
        ReportIdleResponse::Accepted {
            instance_id: instance_id(),
            generation: generation(),
        }
    }

    fn already_draining_response() -> ReportIdleResponse {
        ReportIdleResponse::AlreadyDraining {
            instance_id: instance_id(),
            generation: generation(),
        }
    }

    fn generation_conflict_response() -> ReportIdleResponse {
        ReportIdleResponse::GenerationConflict {
            instance_id: instance_id(),
            expected_generation: generation(),
            actual_generation: Generation::new(8),
        }
    }

    fn unavailable_response() -> ReportIdleResponse {
        ReportIdleResponse::Unavailable {
            instance_id: instance_id(),
            generation: generation(),
            reason: ReportIdleUnavailableReason::Waking,
        }
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

    struct FakeReportIdleClient {
        responses: VecDeque<Result<ReportIdleResponse, TestReportError>>,
        requests: RecordedRequests,
        response_delay: Option<Duration>,
    }

    impl FakeReportIdleClient {
        fn new(
            responses: impl IntoIterator<Item = Result<ReportIdleResponse, TestReportError>>,
        ) -> Self {
            Self {
                responses: responses.into_iter().collect(),
                requests: recorded_requests(),
                response_delay: None,
            }
        }

        fn with_response_delay(mut self, response_delay: Duration) -> Self {
            self.response_delay = Some(response_delay);
            self
        }

        fn requests(&self) -> RecordedRequests {
            Arc::clone(&self.requests)
        }
    }

    impl ReportIdleClient for FakeReportIdleClient {
        type Error = TestReportError;

        fn report_idle(
            &mut self,
            request: ReportIdleRequest,
        ) -> ReportIdleFuture<'_, ReportIdleResponse, Self::Error> {
            self.requests
                .lock()
                .expect("recorded request lock not poisoned")
                .push(request);
            let response = self
                .responses
                .pop_front()
                .expect("fake report idle response queued");
            let response_delay = self.response_delay;

            Box::pin(async move {
                if let Some(response_delay) = response_delay {
                    tokio::time::sleep(response_delay).await;
                }
                response
            })
        }
    }
}
