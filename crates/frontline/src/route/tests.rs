use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use control_plane::{
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState,
    PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
};
use proxy_core::observability::{
    metrics::RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME,
    recorder::{InMemoryObservability, ObservabilityEvent},
};

use super::{
    FrontlineRouteCoordinator, FrontlineRouteCoordinatorError, FrontlineRouteOutcome, RouteFlight,
    SharedFrontlineRouteCoordinator, WakeClient, WakeClientFuture,
};
use crate::{
    ApplyUpdateOutcome, FrontlineRouteResolver, InvalidationReason, ReadyBackend, RouteRequestId,
    RouteSubscriptionClient, RouteSubscriptionFuture, SubscribeControlPlaneOutput, SubscriptionId,
    SubscriptionState, WakeInstanceRequest, WakeInstanceResponse, WakeResponseDisposition,
    WakeTracker, WakeUnavailableReason, WakeWaitReason,
};
use tokio::sync::{oneshot, Notify};

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestRouteClientError {
    SubscribeFailed,
}

impl fmt::Display for TestRouteClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubscribeFailed => f.write_str("subscribe failed"),
        }
    }
}

impl std::error::Error for TestRouteClientError {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestWakeClientError {
    WakeFailed,
}

impl fmt::Display for TestWakeClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WakeFailed => f.write_str("wake failed"),
        }
    }
}

impl std::error::Error for TestWakeClientError {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RouteClientCall {
    Subscribe {
        request_id: RouteRequestId,
        identity: RouteIdentity,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FakeRouteClient {
    calls: Vec<RouteClientCall>,
    subscribe_responses: VecDeque<Result<SubscribeControlPlaneOutput, TestRouteClientError>>,
}

impl FakeRouteClient {
    fn push_subscribe_response(&mut self, response: SubscribeControlPlaneOutput) {
        self.subscribe_responses.push_back(Ok(response));
    }

    fn push_subscribe_error(&mut self, error: TestRouteClientError) {
        self.subscribe_responses.push_back(Err(error));
    }
}

impl RouteSubscriptionClient for FakeRouteClient {
    type Error = TestRouteClientError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'_, SubscribeControlPlaneOutput, Self::Error> {
        self.calls.push(RouteClientCall::Subscribe {
            request_id,
            identity,
        });
        let response = self
            .subscribe_responses
            .pop_front()
            .expect("queued subscribe response");
        Box::pin(async move { response })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'_, (), Self::Error> {
        self.calls
            .push(RouteClientCall::Unsubscribe { subscription_id });
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug, Default)]
struct BlockingRouteClient {
    calls: Arc<Mutex<Vec<RouteClientCall>>>,
    subscribe_responses: BlockingRouteResponses,
}

type BlockingRouteResponses = Arc<
    Mutex<VecDeque<oneshot::Receiver<Result<SubscribeControlPlaneOutput, TestRouteClientError>>>>,
>;

impl BlockingRouteClient {
    fn push_subscribe_response_channel(
        &self,
    ) -> oneshot::Sender<Result<SubscribeControlPlaneOutput, TestRouteClientError>> {
        let (tx, rx) = oneshot::channel();
        self.subscribe_responses
            .lock()
            .expect("blocking route responses lock")
            .push_back(rx);
        tx
    }

    async fn wait_for_call_count(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if self.calls.lock().expect("blocking route calls lock").len() >= expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for route call count");
    }

    fn calls(&self) -> Vec<RouteClientCall> {
        self.calls
            .lock()
            .expect("blocking route calls lock")
            .clone()
    }
}

impl RouteSubscriptionClient for BlockingRouteClient {
    type Error = TestRouteClientError;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'_, SubscribeControlPlaneOutput, Self::Error> {
        self.calls
            .lock()
            .expect("blocking route calls lock")
            .push(RouteClientCall::Subscribe {
                request_id,
                identity,
            });
        let response = self
            .subscribe_responses
            .lock()
            .expect("blocking route responses lock")
            .pop_front()
            .expect("queued blocking route response");
        Box::pin(async move { response.await.expect("route response sent") })
    }

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'_, (), Self::Error> {
        self.calls
            .lock()
            .expect("blocking route calls lock")
            .push(RouteClientCall::Unsubscribe { subscription_id });
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FakeWakeClient {
    calls: Vec<WakeInstanceRequest>,
    responses: VecDeque<Result<WakeInstanceResponse, TestWakeClientError>>,
}

#[derive(Clone, Debug, Default)]
struct BlockingWakeClient {
    calls: Arc<Mutex<Vec<WakeInstanceRequest>>>,
    responses: BlockingWakeResponses,
    wake_started: Arc<Notify>,
}

type BlockingWakeResponses =
    Arc<Mutex<VecDeque<oneshot::Receiver<Result<WakeInstanceResponse, TestWakeClientError>>>>>;

impl FakeWakeClient {
    fn push_response(&mut self, response: WakeInstanceResponse) {
        self.responses.push_back(Ok(response));
    }

    fn push_error(&mut self, error: TestWakeClientError) {
        self.responses.push_back(Err(error));
    }
}

impl BlockingWakeClient {
    fn push_response_channel(
        &self,
    ) -> oneshot::Sender<Result<WakeInstanceResponse, TestWakeClientError>> {
        let (tx, rx) = oneshot::channel();
        self.responses
            .lock()
            .expect("blocking wake responses lock")
            .push_back(rx);
        tx
    }

    async fn wait_for_call_count(&self, expected: usize) {
        loop {
            if self.calls.lock().expect("blocking wake calls lock").len() >= expected {
                return;
            }
            self.wake_started.notified().await;
        }
    }

    fn calls(&self) -> Vec<WakeInstanceRequest> {
        self.calls.lock().expect("blocking wake calls lock").clone()
    }
}

impl WakeClient for BlockingWakeClient {
    type Error = TestWakeClientError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'_, WakeInstanceResponse, Self::Error> {
        self.calls
            .lock()
            .expect("blocking wake calls lock")
            .push(request);
        self.wake_started.notify_waiters();
        let response = self
            .responses
            .lock()
            .expect("blocking wake responses lock")
            .pop_front()
            .expect("queued blocking wake response");
        Box::pin(async move { response.await.expect("wake response sent") })
    }
}

impl WakeClient for FakeWakeClient {
    type Error = TestWakeClientError;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'_, WakeInstanceResponse, Self::Error> {
        self.calls.push(request);
        let response = self.responses.pop_front().expect("queued wake response");
        Box::pin(async move { response })
    }
}

fn now() -> Instant {
    Instant::now()
}

fn ttl(seconds: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_secs(seconds))
}

fn request_id(value: &str) -> RouteRequestId {
    RouteRequestId::new(value).expect("request ID")
}

fn generated_request_id(index: u64) -> RouteRequestId {
    request_id(&format!("req:{index}"))
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription")
}

fn instance_id(value: &str) -> InstanceId {
    InstanceId::new(value).expect("instance ID")
}

fn route_binding_id(value: &str) -> RouteBindingId {
    RouteBindingId::new(value).expect("route binding ID")
}

fn backend(generation: u64) -> BackendEndpoint {
    BackendEndpoint::new(format!("http://10.0.0.{generation}:8080")).expect("backend")
}

fn http_request(host: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("valid host"),
        path: Some(PathPrefix::new(path).expect("valid path")),
    }
}

fn http_rule(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(host).expect("valid host"),
        path: path.map(|path| PathPrefix::new(path).expect("valid path")),
    }
}

fn route_entry(
    state: InstanceState,
    instance_generation: u64,
    backend_generation: Option<u64>,
) -> RouteEntry {
    RouteEntry {
        route_binding_id: route_binding_id("route-a"),
        instance_id: instance_id("instance-a"),
        instance_state: state,
        instance_generation: Generation::new(instance_generation),
        backend: backend_generation.map(backend),
        backend_generation: backend_generation.map(BackendGeneration::new),
    }
}

fn resolved_response(
    request_id: RouteRequestId,
    subscription_id: SubscriptionId,
    matched_identity: RouteIdentity,
    entry: RouteEntry,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteResolved {
        request_id,
        subscription_id,
        matched_identity,
        entry,
        cache_policy: ttl(30),
    }
}

fn miss_response(
    request_id: RouteRequestId,
    identity: RouteIdentity,
) -> SubscribeControlPlaneOutput {
    SubscribeControlPlaneOutput::RouteMiss {
        request_id,
        request_identity: identity,
        negative_cache_policy: ttl(30),
    }
}

fn wake_request(generation: u64) -> WakeInstanceRequest {
    WakeInstanceRequest {
        instance_id: instance_id("instance-a"),
        expected_generation: Generation::new(generation),
    }
}

fn ready_wake_response(generation: u64, backend_generation: u64) -> WakeInstanceResponse {
    WakeInstanceResponse::AlreadyRunning {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(generation),
        backend: backend(backend_generation),
        backend_generation: Some(BackendGeneration::new(backend_generation)),
    }
}

fn coordinator_with_state(
    state: SubscriptionState,
    route_client: FakeRouteClient,
    wake_client: FakeWakeClient,
) -> FrontlineRouteCoordinator<FakeRouteClient, FakeWakeClient> {
    FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::from_parts(state, route_client),
        WakeTracker::new(),
        wake_client,
    )
}

fn coordinator_with_wake_client<Wake>(
    state: SubscriptionState,
    route_client: FakeRouteClient,
    wake_client: Wake,
) -> FrontlineRouteCoordinator<FakeRouteClient, Wake> {
    FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::from_parts(state, route_client),
        WakeTracker::new(),
        wake_client,
    )
}

async fn wait_for_shared_flight_ref_count(
    shared: &SharedFrontlineRouteCoordinator<BlockingRouteClient, FakeWakeClient>,
    identity: &RouteIdentity,
    expected: usize,
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let count = shared
                .flights
                .lock()
                .await
                .get(identity)
                .map(Arc::strong_count)
                .unwrap_or_default();
            if count >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for shared route waiters");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_flight_waiters_do_not_lose_completion_notification() {
    for _ in 0..10_000 {
        let flight = Arc::new(RouteFlight::<TestRouteClientError, TestWakeClientError>::new());
        let waiter = {
            let flight = flight.clone();
            tokio::spawn(async move { flight.wait().await })
        };
        tokio::task::yield_now().await;
        flight
            .complete(Err(FrontlineRouteCoordinatorError::RouteActorClosed))
            .await;

        // This hammers the waiter/completer interleaving that used to lose a
        // Notify wakeup between checking the result and registering interest.
        let result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("completed flight wait does not hang")
            .expect("waiter task joins");
        assert!(matches!(
            result,
            Err(FrontlineRouteCoordinatorError::RouteActorClosed)
        ));
    }
}

#[tokio::test]
async fn running_backend_cached_returns_ready_without_subscribe_or_wake() {
    let started_at = now();
    let request = http_request("app.example.com", "/api/users");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", Some("/api")),
            route_entry(InstanceState::Running, 7, Some(3)),
        ),
        started_at,
    );
    let mut coordinator =
        coordinator_with_state(state, FakeRouteClient::default(), FakeWakeClient::default());

    let outcome = coordinator
        .route(request, started_at)
        .await
        .expect("cached route is ready");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert!(coordinator.resolver().client().calls.is_empty());
    assert!(coordinator.wake_client().calls.is_empty());
}

#[tokio::test]
async fn cold_lazy_subscribe_wakes_updates_cache_and_next_route_is_hot() {
    let now = now();
    let request = http_request("app.example.com", "/api/users");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-1"),
        http_rule("example.com", Some("/api")),
        route_entry(InstanceState::Cold, 4, None),
    ));
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(4, 9));
    let mut coordinator = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client),
        WakeTracker::new(),
        wake_client,
    );

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("cold route wakes");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(
        coordinator.resolver().client().calls,
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request.clone(),
        }]
    );
    assert_eq!(coordinator.wake_client().calls, vec![wake_request(4)]);
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);

    let outcome = coordinator
        .route(request, now)
        .await
        .expect("ready wake was cached");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(coordinator.resolver().client().calls.len(), 1);
    assert_eq!(coordinator.wake_client().calls.len(), 1);
}

#[tokio::test]
async fn shared_route_hot_hits_do_not_wait_for_in_flight_cold_wake() {
    let started_at = now();
    let hot = http_request("hot.example.com", "/");
    let cold = http_request("cold.example.com", "/");
    let mut state = SubscriptionState::new(8);
    state.apply_control_plane_message(
        resolved_response(
            request_id("hot-initial"),
            subscription_id("sub-hot"),
            hot.clone(),
            route_entry(InstanceState::Running, 7, Some(3)),
        ),
        started_at,
    );
    state.apply_control_plane_message(
        resolved_response(
            request_id("cold-initial"),
            subscription_id("sub-cold"),
            cold.clone(),
            route_entry(InstanceState::Cold, 8, None),
        ),
        started_at,
    );
    let wake_client = BlockingWakeClient::default();
    let release_wake = wake_client.push_response_channel();
    let shared =
        coordinator_with_wake_client(state, FakeRouteClient::default(), wake_client.clone())
            .into_shared();

    // The cold route owns the actor while the hot route should resolve from the
    // shared read cache without waiting for the wake response.
    let cold_shared = shared.clone();
    let cold_task = tokio::spawn(async move { cold_shared.route(cold, now()).await });
    wake_client.wait_for_call_count(1).await;

    let mut hot_tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let shared = shared.clone();
        let hot = hot.clone();
        hot_tasks.spawn(async move { shared.route(hot, now()).await });
    }

    while let Some(result) = hot_tasks.join_next().await {
        let outcome = result
            .expect("hot route task joins")
            .expect("hot route resolves");
        assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    }
    assert_eq!(wake_client.calls(), vec![wake_request(8)]);

    release_wake
        .send(Ok(ready_wake_response(8, 10)))
        .expect("wake response is received");
    let cold_outcome = cold_task
        .await
        .expect("cold route task joins")
        .expect("cold route completes");
    assert!(matches!(cold_outcome, FrontlineRouteOutcome::Ready(_)));
}

#[tokio::test]
async fn shared_route_same_identity_uses_one_in_flight_subscribe_route() {
    let route_client = BlockingRouteClient::default();
    let release_subscribe = route_client.push_subscribe_response_channel();
    let shared = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client.clone()),
        WakeTracker::new(),
        FakeWakeClient::default(),
    )
    .into_shared();
    let identity = http_request("single-flight.example.com", "/same");

    let mut tasks = Vec::new();
    for _ in 0..3 {
        let shared = shared.clone();
        let identity = identity.clone();
        tasks.push(tokio::spawn(
            async move { shared.route(identity, now()).await },
        ));
    }
    route_client.wait_for_call_count(1).await;
    wait_for_shared_flight_ref_count(&shared, &identity, 5).await;

    // All same-identity waiters should attach to the explicit RouteFlight while
    // the first subscribe is still blocked, so only one control-plane request is issued.
    assert_eq!(
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: identity.clone(),
        }]
    );

    release_subscribe
        .send(Ok(resolved_response(
            generated_request_id(1),
            subscription_id("sub-single-flight"),
            identity.clone(),
            route_entry(InstanceState::Running, 7, Some(3)),
        )))
        .expect("subscribe response is received");

    for task in tasks {
        let outcome = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("shared route completes")
            .expect("shared route task joins")
            .expect("shared route succeeds");
        assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    }
    assert_eq!(
        route_client.calls(),
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity,
        }]
    );
}

#[tokio::test]
async fn shared_route_cancelled_mid_wake_does_not_strand_recovery() {
    let started_at = now();
    let cold = http_request("cancel-cold.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("cold-initial"),
            subscription_id("sub-cancel-cold"),
            cold.clone(),
            route_entry(InstanceState::Cold, 9, None),
        ),
        started_at,
    );
    let wake_client = BlockingWakeClient::default();
    let release_wake = wake_client.push_response_channel();
    let shared =
        coordinator_with_wake_client(state, FakeRouteClient::default(), wake_client.clone())
            .into_shared();

    // Dropping this waiter simulates the HTTP request going away. The actor-owned
    // wake must still complete and publish the ready route for later callers.
    let first_shared = shared.clone();
    let first = tokio::spawn(async move { first_shared.route(cold.clone(), now()).await });
    wake_client.wait_for_call_count(1).await;
    first.abort();

    release_wake
        .send(Ok(ready_wake_response(9, 11)))
        .expect("wake response is received");

    let recovered = tokio::time::timeout(
        Duration::from_secs(1),
        shared.route(http_request("cancel-cold.example.com", "/"), now()),
    )
    .await
    .expect("later route does not hang")
    .expect("later route resolves");

    assert!(matches!(recovered, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(wake_client.calls(), vec![wake_request(9)]);
}

#[tokio::test]
async fn cold_wake_records_control_plane_wake_call_metric() {
    let now = now();
    let sink = InMemoryObservability::default();
    let request = http_request("app.example.com", "/api/users");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-1"),
        http_rule("example.com", Some("/api")),
        route_entry(InstanceState::Cold, 4, None),
    ));
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(4, 9));
    let mut coordinator = FrontlineRouteCoordinator::with_observability(
        FrontlineRouteResolver::new(4, route_client),
        WakeTracker::new(),
        wake_client,
        sink.recorder(),
    );

    coordinator
        .route(request, now)
        .await
        .expect("cold route wakes");

    assert!(sink.events().iter().any(|event| matches!(
        event,
        ObservabilityEvent::Metric(metric)
            if metric.name() == RUNTIME_CONTROL_PLANE_CALLS_TOTAL_NAME
                && metric.labels().iter().any(|label| label.value() == "wake_instance")
                && metric.labels().iter().any(|label| label.value() == "success")
    )));
}

#[tokio::test]
async fn route_miss_returns_miss_without_wake() {
    let now = now();
    let request = http_request("missing.example.com", "/");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(miss_response(generated_request_id(1), request.clone()));
    let mut coordinator = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client),
        WakeTracker::new(),
        FakeWakeClient::default(),
    );

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("route miss");

    let FrontlineRouteOutcome::Miss(entry) = outcome else {
        panic!("expected miss");
    };
    assert_eq!(entry.request_identity, request);
    assert!(coordinator.wake_client().calls.is_empty());
}

#[tokio::test]
async fn running_route_without_backend_triggers_wake() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Running, 5, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(5, 1));
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let outcome = coordinator
        .route(request, now)
        .await
        .expect("missing backend wakes");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(coordinator.wake_client().calls, vec![wake_request(5)]);
}

#[tokio::test]
async fn waking_route_without_local_pending_wake_resumes_via_control_plane() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Waking, 6, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(7, 2));
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("waking route resumes wake");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(coordinator.wake_client().calls, vec![wake_request(6)]);
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);

    let cached = coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("ready wake was cached");
    assert_eq!(cached.entry.instance_state, InstanceState::Running);
    assert_eq!(cached.entry.instance_generation, Generation::new(7));
    assert_eq!(
        cached.entry.backend_generation,
        Some(BackendGeneration::new(2))
    );
}

#[tokio::test]
async fn waking_route_with_local_pending_wake_waits_without_second_wake_call() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Waking, 6, None),
        ),
        now,
    );
    let mut coordinator =
        coordinator_with_state(state, FakeRouteClient::default(), FakeWakeClient::default());
    coordinator.wake_tracker_mut().admit(wake_request(6));

    let outcome = coordinator.route(request, now).await.expect("waits");

    let FrontlineRouteOutcome::Waiting(wait) = outcome else {
        panic!("expected waiting outcome");
    };
    assert_eq!(wait.reason, WakeWaitReason::DuplicatePendingWake);
    assert!(coordinator.wake_client().calls.is_empty());
}

#[tokio::test]
async fn deleting_and_deleted_routes_are_unavailable_without_wake_call() {
    for (state, reason) in [
        (InstanceState::Deleting, WakeUnavailableReason::Deleting),
        (InstanceState::Deleted, WakeUnavailableReason::Deleted),
    ] {
        let now = now();
        let request = http_request("app.example.com", "/");
        let mut subscription_state = SubscriptionState::new(4);
        subscription_state.apply_control_plane_message(
            resolved_response(
                request_id("initial"),
                subscription_id("sub-1"),
                http_rule("example.com", None),
                route_entry(state, 6, None),
            ),
            now,
        );
        let mut coordinator = coordinator_with_state(
            subscription_state,
            FakeRouteClient::default(),
            FakeWakeClient::default(),
        );

        let outcome = coordinator.route(request, now).await.expect("unavailable");

        let FrontlineRouteOutcome::Unavailable(unavailable) = outcome else {
            panic!("expected unavailable outcome");
        };
        assert_eq!(unavailable.reason, reason);
        assert!(coordinator.wake_client().calls.is_empty());
    }
}

#[tokio::test]
async fn duplicate_pending_wake_waits_without_second_wake_call() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Cold, 8, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(8),
    });
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let first = coordinator
        .route(request.clone(), now)
        .await
        .expect("wake starts");
    assert!(matches!(first, FrontlineRouteOutcome::Waking { .. }));
    assert_eq!(coordinator.wake_tracker().pending_len(), 1);

    let second = coordinator
        .route(request, now)
        .await
        .expect("duplicate waits");

    let FrontlineRouteOutcome::Waiting(wait) = second else {
        panic!("expected waiting outcome");
    };
    assert_eq!(wait.reason, WakeWaitReason::DuplicatePendingWake);
    assert_eq!(coordinator.wake_client().calls.len(), 1);
}

#[tokio::test]
async fn stale_wake_response_is_rejected_clears_pending_and_does_not_update_cache() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Cold, 9, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(ready_wake_response(8, 1));
    wake_client.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(9),
    });
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("stale response rejected");

    assert!(matches!(
        outcome,
        FrontlineRouteOutcome::RejectedWakeObservation(_)
    ));
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    let cached = coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("cached route");
    assert_eq!(cached.entry.instance_state, InstanceState::Cold);
    assert!(cached.entry.backend.is_none());

    let retry = coordinator
        .route(request, now)
        .await
        .expect("pending cleared for retry");

    assert!(matches!(retry, FrontlineRouteOutcome::Waking { .. }));
    assert_eq!(coordinator.wake_client().calls.len(), 2);
}

#[tokio::test]
async fn stale_cold_wake_conflict_then_refreshed_waking_route_resumes_wake() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let matched_identity = http_rule("example.com", None);
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            matched_identity.clone(),
            route_entry(InstanceState::Cold, 5, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_response(WakeInstanceResponse::GenerationConflict {
        instance_id: instance_id("instance-a"),
        expected_generation: Generation::new(5),
        actual_generation: Generation::new(6),
    });
    wake_client.push_response(ready_wake_response(7, 2));
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let conflict = coordinator
        .route(request.clone(), now)
        .await
        .expect("stale cold wake returns conflict");

    let FrontlineRouteOutcome::GenerationConflict {
        expected_generation,
        actual_generation,
        ..
    } = conflict
    else {
        panic!("expected generation conflict");
    };
    assert_eq!(expected_generation, Generation::new(5));
    assert_eq!(actual_generation, Generation::new(6));
    assert_eq!(coordinator.wake_client().calls, vec![wake_request(5)]);
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);

    coordinator
        .resolver_mut()
        .apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteUpdated {
                subscription_id: subscription_id("sub-1"),
                matched_identity,
                entry: route_entry(InstanceState::Waking, 6, None),
                cache_policy: ttl(30),
            },
            now,
        )
        .await
        .expect("refreshed waking route applies");

    let ready = coordinator
        .route(request, now)
        .await
        .expect("refreshed waking route resumes wake");

    assert!(matches!(ready, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(
        coordinator.wake_client().calls,
        vec![wake_request(5), wake_request(6)]
    );
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    let cached = coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("ready route cached");
    assert_eq!(cached.entry.instance_state, InstanceState::Running);
    assert_eq!(cached.entry.instance_generation, Generation::new(7));
    assert_eq!(
        cached.entry.backend_generation,
        Some(BackendGeneration::new(2))
    );
}

#[tokio::test]
async fn wake_failed_unavailable_and_generation_conflict_map_to_typed_outcomes() {
    let cases = [
        (
            WakeInstanceResponse::Failed {
                instance_id: instance_id("instance-a"),
                generation: Generation::new(10),
                reason: "boom".to_owned(),
            },
            "failed",
        ),
        (
            WakeInstanceResponse::Unavailable {
                instance_id: instance_id("instance-a"),
                generation: Generation::new(10),
                reason: "gone".to_owned(),
            },
            "unavailable",
        ),
        (
            WakeInstanceResponse::GenerationConflict {
                instance_id: instance_id("instance-a"),
                expected_generation: Generation::new(10),
                actual_generation: Generation::new(11),
            },
            "conflict",
        ),
    ];

    for (response, expected) in cases {
        let now = now();
        let request = http_request("app.example.com", "/");
        let mut state = SubscriptionState::new(4);
        state.apply_control_plane_message(
            resolved_response(
                request_id("initial"),
                subscription_id("sub-1"),
                http_rule("example.com", None),
                route_entry(InstanceState::Cold, 10, None),
            ),
            now,
        );
        let mut wake_client = FakeWakeClient::default();
        wake_client.push_response(response);
        let mut coordinator =
            coordinator_with_state(state, FakeRouteClient::default(), wake_client);

        let outcome = coordinator.route(request, now).await.expect("wake outcome");

        match (expected, outcome) {
            ("failed", FrontlineRouteOutcome::WakeFailed { reason, .. }) => {
                assert_eq!(reason, "boom");
            }
            ("unavailable", FrontlineRouteOutcome::WakeUnavailable { reason, .. }) => {
                assert_eq!(reason, "gone");
            }
            (
                "conflict",
                FrontlineRouteOutcome::GenerationConflict {
                    expected_generation,
                    actual_generation,
                    ..
                },
            ) => {
                assert_eq!(expected_generation, Generation::new(10));
                assert_eq!(actual_generation, Generation::new(11));
            }
            _ => panic!("unexpected outcome"),
        }
        assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    }
}

#[tokio::test]
async fn wake_client_error_surfaces_and_clears_pending_for_retry() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Cold, 11, None),
        ),
        now,
    );
    let mut wake_client = FakeWakeClient::default();
    wake_client.push_error(TestWakeClientError::WakeFailed);
    wake_client.push_response(WakeInstanceResponse::WakeStarted {
        instance_id: instance_id("instance-a"),
        generation: Generation::new(11),
    });
    let mut coordinator = coordinator_with_state(state, FakeRouteClient::default(), wake_client);

    let error = coordinator
        .route(request.clone(), now)
        .await
        .expect_err("wake client error");

    assert!(matches!(error, FrontlineRouteCoordinatorError::Wake(_)));
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);

    let retry = coordinator
        .route(request, now)
        .await
        .expect("retry calls wake again");

    assert!(matches!(retry, FrontlineRouteOutcome::Waking { .. }));
    assert_eq!(coordinator.wake_client().calls.len(), 2);
}

#[tokio::test]
async fn invalidation_during_wake_rejects_ready_update_and_leaves_cache_empty() {
    let now = now();
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-1"),
            http_rule("example.com", None),
            route_entry(InstanceState::Cold, 12, None),
        ),
        now,
    );
    let mut coordinator =
        coordinator_with_state(state, FakeRouteClient::default(), FakeWakeClient::default());
    let observed_entry = coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-1"))
        .expect("observed cache entry before wake")
        .clone();

    coordinator.wake_tracker_mut().admit(wake_request(12));
    assert_eq!(coordinator.wake_tracker().pending_len(), 1);
    coordinator
        .resolver_mut()
        .apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("sub-1"),
                reason: InvalidationReason::StreamClosed,
            },
            now,
        )
        .await
        .expect("stream invalidation applies");

    let error = coordinator
        .handle_wake_response(
            observed_entry,
            WakeResponseDisposition::Ready(ReadyBackend {
                instance_id: instance_id("instance-a"),
                instance_generation: Generation::new(12),
                backend: backend(5),
                backend_generation: Some(BackendGeneration::new(5)),
            }),
            now,
        )
        .await
        .expect_err("ready observation cannot update invalidated subscription");

    assert_eq!(
        error,
        FrontlineRouteCoordinatorError::RejectedCacheUpdate(
            ApplyUpdateOutcome::MissingSubscription
        )
    );
    assert_eq!(coordinator.wake_tracker().pending_len(), 0);
    assert!(coordinator.resolver().state().cache().is_empty());
}

#[tokio::test]
async fn route_after_stream_loss_lazily_resubscribes_and_rebuilds_cache() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut state = SubscriptionState::new(4);
    state.apply_control_plane_message(
        resolved_response(
            request_id("initial"),
            subscription_id("sub-old"),
            http_rule("example.com", None),
            route_entry(InstanceState::Running, 13, Some(1)),
        ),
        now,
    );
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_response(resolved_response(
        generated_request_id(1),
        subscription_id("sub-new"),
        http_rule("example.com", None),
        route_entry(InstanceState::Running, 13, Some(2)),
    ));
    let mut coordinator = coordinator_with_state(state, route_client, FakeWakeClient::default());

    coordinator
        .resolver_mut()
        .apply_control_plane_message(
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id: subscription_id("sub-old"),
                reason: InvalidationReason::StreamClosed,
            },
            now,
        )
        .await
        .expect("stream invalidation removes active subscription");

    let outcome = coordinator
        .route(request.clone(), now)
        .await
        .expect("route lazily rebuilds after stream loss");

    assert!(matches!(outcome, FrontlineRouteOutcome::Ready(_)));
    assert_eq!(
        coordinator.resolver().client().calls,
        vec![RouteClientCall::Subscribe {
            request_id: generated_request_id(1),
            identity: request,
        }]
    );
    assert!(coordinator
        .resolver()
        .state()
        .cache()
        .positive_by_subscription(&subscription_id("sub-old"))
        .is_none());
    assert_eq!(
        coordinator
            .resolver()
            .state()
            .cache()
            .positive_by_subscription(&subscription_id("sub-new"))
            .expect("rebuilt subscription")
            .entry
            .backend_generation,
        Some(BackendGeneration::new(2))
    );
}

#[tokio::test]
async fn route_resolver_error_surfaces_without_wake_call() {
    let now = now();
    let request = http_request("app.example.com", "/");
    let mut route_client = FakeRouteClient::default();
    route_client.push_subscribe_error(TestRouteClientError::SubscribeFailed);
    let mut coordinator = FrontlineRouteCoordinator::new(
        FrontlineRouteResolver::new(4, route_client),
        WakeTracker::new(),
        FakeWakeClient::default(),
    );

    let error = coordinator
        .route(request, now)
        .await
        .expect_err("resolver error");

    assert!(matches!(error, FrontlineRouteCoordinatorError::Resolve(_)));
    assert!(coordinator.wake_client().calls.is_empty());
}
