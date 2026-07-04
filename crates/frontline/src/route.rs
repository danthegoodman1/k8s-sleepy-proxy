use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use control_plane::{CachePolicy, InstanceState, RouteEntry, RouteIdentity};
use proxy_core::observability::{
    metrics::RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
    recorder::{MetricObservation, ObservabilityRecorder},
    Operation, Outcome,
};

use crate::{
    route_wake_decision, validate_wake_response, ApplyUpdateOutcome, FrontlineRouteResolution,
    FrontlineRouteResolver, FrontlineRouteResolverError, NegativeCacheEntry, ReadyBackend,
    RouteSubscriptionClient, StaleWakeObservation, SubscribeControlPlaneOutput, SubscriptionState,
    WakeAdmission, WakeInstanceRequest, WakeInstanceResponse, WakeResponseDisposition, WakeTracker,
    WakeUnavailable, WakeWait,
};

pub type WakeClientFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;
const DEFAULT_WAKE_INSTANCE_DEADLINE: Duration = Duration::from_secs(5);
const ROUTE_ACTOR_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(100);

pub trait WakeClient {
    type Error: Send;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'_, WakeInstanceResponse, Self::Error>;
}

#[derive(Clone, Debug)]
pub struct FrontlineRouteCoordinator<RouteClient, Wake> {
    resolver: FrontlineRouteResolver<RouteClient>,
    wake_tracker: WakeTracker,
    wake_client: Wake,
    wake_deadline: Duration,
    observability: ObservabilityRecorder,
}

#[derive(Debug)]
pub struct SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    state: Arc<tokio::sync::RwLock<SubscriptionState>>,
    commands: tokio::sync::mpsc::Sender<RouteActorCommand<RouteClient::Error, Wake::Error>>,
    flights: RouteFlights<RouteClient::Error, Wake::Error>,
    _task: Arc<tokio::task::JoinHandle<()>>,
}

impl<RouteClient, Wake> Clone for SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            commands: self.commands.clone(),
            flights: self.flights.clone(),
            _task: self._task.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteOutcome {
    Ready(ReadyBackend),
    Miss(Arc<NegativeCacheEntry>),
    Waiting(WakeWait),
    Waking {
        instance_id: control_plane::InstanceId,
        generation: control_plane::Generation,
    },
    Unavailable(WakeUnavailable),
    WakeFailed {
        instance_id: control_plane::InstanceId,
        generation: control_plane::Generation,
        reason: String,
    },
    WakeUnavailable {
        instance_id: control_plane::InstanceId,
        generation: control_plane::Generation,
        reason: String,
    },
    GenerationConflict {
        instance_id: control_plane::InstanceId,
        expected_generation: control_plane::Generation,
        actual_generation: control_plane::Generation,
    },
    RejectedWakeObservation(StaleWakeObservation),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteCoordinatorError<RouteClientError, WakeClientError> {
    Resolve(FrontlineRouteResolverError<RouteClientError>),
    CacheUpdate(FrontlineRouteResolverError<RouteClientError>),
    Wake(WakeClientError),
    WakeDeadline(WakeInstanceRequest),
    RouteActorClosed,
    RejectedCacheUpdate(ApplyUpdateOutcome),
}

type RouteResult<RouteClientError, WakeClientError> = Result<
    FrontlineRouteOutcome,
    FrontlineRouteCoordinatorError<RouteClientError, WakeClientError>,
>;
type RouteFlights<RouteClientError, WakeClientError> = Arc<
    tokio::sync::Mutex<HashMap<RouteIdentity, Arc<RouteFlight<RouteClientError, WakeClientError>>>>,
>;

#[derive(Debug)]
struct RouteFlight<RouteClientError, WakeClientError> {
    result: tokio::sync::Mutex<Option<RouteResult<RouteClientError, WakeClientError>>>,
    notify: tokio::sync::Notify,
}

#[derive(Debug)]
enum RouteActorCommand<RouteClientError, WakeClientError> {
    Route {
        identity: RouteIdentity,
        now: Instant,
        response: tokio::sync::oneshot::Sender<RouteResult<RouteClientError, WakeClientError>>,
    },
}

impl<RouteClient, Wake> FrontlineRouteCoordinator<RouteClient, Wake> {
    pub fn new(
        resolver: FrontlineRouteResolver<RouteClient>,
        wake_tracker: WakeTracker,
        wake_client: Wake,
    ) -> Self {
        Self {
            resolver,
            wake_tracker,
            wake_client,
            wake_deadline: DEFAULT_WAKE_INSTANCE_DEADLINE,
            observability: ObservabilityRecorder::default(),
        }
    }

    pub fn with_observability(
        resolver: FrontlineRouteResolver<RouteClient>,
        wake_tracker: WakeTracker,
        wake_client: Wake,
        observability: ObservabilityRecorder,
    ) -> Self {
        Self {
            resolver,
            wake_tracker,
            wake_client,
            wake_deadline: DEFAULT_WAKE_INSTANCE_DEADLINE,
            observability,
        }
    }

    pub fn with_wake_deadline(mut self, wake_deadline: Duration) -> Self {
        self.wake_deadline = wake_deadline;
        self
    }

    pub fn resolver(&self) -> &FrontlineRouteResolver<RouteClient> {
        &self.resolver
    }

    pub fn resolver_mut(&mut self) -> &mut FrontlineRouteResolver<RouteClient> {
        &mut self.resolver
    }

    pub fn wake_tracker(&self) -> &WakeTracker {
        &self.wake_tracker
    }

    pub fn wake_tracker_mut(&mut self) -> &mut WakeTracker {
        &mut self.wake_tracker
    }

    pub fn wake_client(&self) -> &Wake {
        &self.wake_client
    }

    pub fn wake_client_mut(&mut self) -> &mut Wake {
        &mut self.wake_client
    }
}

impl<RouteClient, Wake> FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send + 'static,
{
    pub fn into_shared(self) -> SharedFrontlineRouteCoordinator<RouteClient, Wake> {
        SharedFrontlineRouteCoordinator::new(self)
    }
}

impl<RouteClient, Wake> SharedFrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Clone + Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Clone + Send + 'static,
{
    pub fn new(coordinator: FrontlineRouteCoordinator<RouteClient, Wake>) -> Self {
        let state = Arc::new(tokio::sync::RwLock::new(
            coordinator.resolver().state().clone(),
        ));
        let (commands, rx) = tokio::sync::mpsc::channel(64);
        let task_state = state.clone();
        let task = tokio::spawn(route_actor(coordinator, task_state, rx));

        Self {
            state,
            commands,
            flights: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            _task: Arc::new(task),
        }
    }

    pub async fn route(
        &self,
        identity: RouteIdentity,
        now: Instant,
    ) -> RouteResult<RouteClient::Error, Wake::Error> {
        if let Some(outcome) = self.local_route(&identity, now).await {
            return Ok(outcome);
        }

        let flight = self.flight_for(identity.clone(), now).await;
        flight.wait().await
    }

    async fn local_route(
        &self,
        identity: &RouteIdentity,
        now: Instant,
    ) -> Option<FrontlineRouteOutcome> {
        let state = self.state.read().await;
        match state.cache().lookup(identity, now) {
            crate::CacheLookup::Hit(crate::CacheLookupHit::Positive(entry)) => {
                match route_wake_decision(&entry.entry) {
                    crate::RouteWakeDecision::Ready(backend) => {
                        Some(FrontlineRouteOutcome::Ready(backend))
                    }
                    crate::RouteWakeDecision::Unavailable(unavailable) => {
                        Some(FrontlineRouteOutcome::Unavailable(unavailable))
                    }
                    crate::RouteWakeDecision::Wait(wait) => {
                        Some(FrontlineRouteOutcome::Waiting(wait))
                    }
                    crate::RouteWakeDecision::Wake { .. } => None,
                }
            }
            crate::CacheLookup::Hit(crate::CacheLookupHit::Negative(entry)) => {
                Some(FrontlineRouteOutcome::Miss(entry))
            }
            crate::CacheLookup::Expired | crate::CacheLookup::Absent => None,
        }
    }

    async fn flight_for(
        &self,
        identity: RouteIdentity,
        now: Instant,
    ) -> Arc<RouteFlight<RouteClient::Error, Wake::Error>> {
        let mut flights = self.flights.lock().await;
        if let Some(flight) = flights.get(&identity) {
            return flight.clone();
        }

        let flight = Arc::new(RouteFlight::new());
        flights.insert(identity.clone(), flight.clone());
        spawn_route_flight(
            identity,
            now,
            flight.clone(),
            self.commands.clone(),
            self.flights.clone(),
        );
        flight
    }
}

impl<RouteClientError, WakeClientError> RouteFlight<RouteClientError, WakeClientError>
where
    RouteClientError: Clone,
    WakeClientError: Clone,
{
    fn new() -> Self {
        Self {
            result: tokio::sync::Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    async fn complete(&self, result: RouteResult<RouteClientError, WakeClientError>) {
        *self.result.lock().await = Some(result);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> RouteResult<RouteClientError, WakeClientError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(result) = self.result.lock().await.clone() {
                return result;
            }
            notified.await;
        }
    }
}

fn spawn_route_flight<RouteClientError, WakeClientError>(
    identity: RouteIdentity,
    now: Instant,
    flight: Arc<RouteFlight<RouteClientError, WakeClientError>>,
    commands: tokio::sync::mpsc::Sender<RouteActorCommand<RouteClientError, WakeClientError>>,
    flights: RouteFlights<RouteClientError, WakeClientError>,
) where
    RouteClientError: Clone + Send + 'static,
    WakeClientError: Clone + Send + 'static,
{
    tokio::spawn(async move {
        let result = route_via_actor(commands, identity.clone(), now).await;
        flight.complete(result).await;
        flights.lock().await.remove(&identity);
    });
}

async fn route_via_actor<RouteClientError, WakeClientError>(
    commands: tokio::sync::mpsc::Sender<RouteActorCommand<RouteClientError, WakeClientError>>,
    identity: RouteIdentity,
    now: Instant,
) -> RouteResult<RouteClientError, WakeClientError>
where
    RouteClientError: Send + 'static,
    WakeClientError: Send + 'static,
{
    let (response, result) = tokio::sync::oneshot::channel();
    if commands
        .send(RouteActorCommand::Route {
            identity,
            now,
            response,
        })
        .await
        .is_err()
    {
        return Err(FrontlineRouteCoordinatorError::RouteActorClosed);
    }

    result
        .await
        .unwrap_or(Err(FrontlineRouteCoordinatorError::RouteActorClosed))
}

async fn route_actor<RouteClient, Wake>(
    mut coordinator: FrontlineRouteCoordinator<RouteClient, Wake>,
    state: Arc<tokio::sync::RwLock<SubscriptionState>>,
    mut rx: tokio::sync::mpsc::Receiver<RouteActorCommand<RouteClient::Error, Wake::Error>>,
) where
    RouteClient: RouteSubscriptionClient + Send + 'static,
    RouteClient::Error: Send + 'static,
    Wake: WakeClient + Send + 'static,
    Wake::Error: Send + 'static,
{
    let mut maintenance = tokio::time::interval(ROUTE_ACTOR_MAINTENANCE_INTERVAL);
    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else {
                    break;
                };
                match command {
                    RouteActorCommand::Route { identity, now, response } => {
                        let result = coordinator.route(identity, now).await;
                        sync_shared_state(&state, coordinator.resolver().state().clone()).await;
                        let _ = response.send(result);
                    }
                }
            }
            _ = maintenance.tick() => {
                let _ = coordinator.resolver_mut().maintain(Instant::now()).await;
                sync_shared_state(&state, coordinator.resolver().state().clone()).await;
            }
        }
    }
}

async fn sync_shared_state(
    state: &tokio::sync::RwLock<SubscriptionState>,
    snapshot: SubscriptionState,
) {
    *state.write().await = snapshot;
}

impl<RouteClient, Wake> FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: RouteSubscriptionClient,
    Wake: WakeClient,
{
    pub async fn route(
        &mut self,
        identity: RouteIdentity,
        now: Instant,
    ) -> Result<
        FrontlineRouteOutcome,
        FrontlineRouteCoordinatorError<RouteClient::Error, Wake::Error>,
    > {
        let resolution = self
            .resolver
            .resolve(identity, now)
            .await
            .map_err(FrontlineRouteCoordinatorError::Resolve)?;

        let cache_entry = match resolution {
            FrontlineRouteResolution::Resolved(entry) => entry,
            FrontlineRouteResolution::Miss(entry) => return Ok(FrontlineRouteOutcome::Miss(entry)),
        };

        match route_wake_decision(&cache_entry.entry) {
            crate::RouteWakeDecision::Ready(backend) => Ok(FrontlineRouteOutcome::Ready(backend)),
            crate::RouteWakeDecision::Wait(wait) => Ok(FrontlineRouteOutcome::Waiting(wait)),
            crate::RouteWakeDecision::Unavailable(unavailable) => {
                Ok(FrontlineRouteOutcome::Unavailable(unavailable))
            }
            crate::RouteWakeDecision::Wake { request, .. } => {
                match self.wake_tracker.admit(request) {
                    WakeAdmission::Wait(wait) => Ok(FrontlineRouteOutcome::Waiting(wait)),
                    WakeAdmission::Start(request) => {
                        let response = match tokio::time::timeout(
                            self.wake_deadline,
                            self.wake_client.wake_instance(request.clone()),
                        )
                        .await
                        {
                            Err(_elapsed) => {
                                self.record_wake_instance_call(Outcome::Error);
                                self.wake_tracker
                                    .complete(&request.instance_id, request.expected_generation);
                                return Err(FrontlineRouteCoordinatorError::WakeDeadline(request));
                            }
                            Ok(result) => match result {
                                Ok(response) => response,
                                Err(error) => {
                                    self.record_wake_instance_call(Outcome::Error);
                                    self.wake_tracker.complete(
                                        &request.instance_id,
                                        request.expected_generation,
                                    );
                                    return Err(FrontlineRouteCoordinatorError::Wake(error));
                                }
                            },
                        };

                        let disposition = validate_wake_response(&cache_entry.entry, response);
                        self.record_wake_instance_call(wake_response_outcome(&disposition));
                        self.handle_wake_response(cache_entry, disposition, now)
                            .await
                    }
                }
            }
        }
    }

    fn record_wake_instance_call(&self, outcome: Outcome) {
        self.observability.record_metric(MetricObservation::new(
            RUNTIME_CONTROL_PLANE_CALLS_TOTAL,
            vec![
                Operation::WakeInstance.metric_label(),
                outcome.metric_label(),
            ],
            1.0,
        ));
    }

    async fn handle_wake_response(
        &mut self,
        cache_entry: Arc<crate::PositiveCacheEntry>,
        disposition: WakeResponseDisposition,
        now: Instant,
    ) -> Result<
        FrontlineRouteOutcome,
        FrontlineRouteCoordinatorError<RouteClient::Error, Wake::Error>,
    > {
        let observed_instance_id = cache_entry.entry.instance_id.clone();
        let observed_generation = cache_entry.entry.instance_generation;

        match disposition {
            WakeResponseDisposition::WakeStarted {
                instance_id,
                generation,
            }
            | WakeResponseDisposition::StillWaking {
                instance_id,
                generation,
            } => Ok(FrontlineRouteOutcome::Waking {
                instance_id,
                generation,
            }),
            WakeResponseDisposition::Ready(backend) => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                self.apply_ready_wake(cache_entry, backend.clone(), now)
                    .await?;
                Ok(FrontlineRouteOutcome::Ready(backend))
            }
            WakeResponseDisposition::Failed {
                instance_id,
                generation,
                reason,
            } => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                Ok(FrontlineRouteOutcome::WakeFailed {
                    instance_id,
                    generation,
                    reason,
                })
            }
            WakeResponseDisposition::Unavailable {
                instance_id,
                generation,
                reason,
            } => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                Ok(FrontlineRouteOutcome::WakeUnavailable {
                    instance_id,
                    generation,
                    reason,
                })
            }
            WakeResponseDisposition::GenerationConflict {
                instance_id,
                expected_generation,
                actual_generation,
            } => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                Ok(FrontlineRouteOutcome::GenerationConflict {
                    instance_id,
                    expected_generation,
                    actual_generation,
                })
            }
            WakeResponseDisposition::Rejected(stale) => {
                self.wake_tracker
                    .complete(&observed_instance_id, observed_generation);
                Ok(FrontlineRouteOutcome::RejectedWakeObservation(stale))
            }
        }
    }

    async fn apply_ready_wake(
        &mut self,
        cache_entry: Arc<crate::PositiveCacheEntry>,
        backend: ReadyBackend,
        now: Instant,
    ) -> Result<(), FrontlineRouteCoordinatorError<RouteClient::Error, Wake::Error>> {
        let remaining_ttl = cache_entry.expires_at().saturating_duration_since(now);
        let update = SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: cache_entry.subscription_id.clone(),
            matched_identity: cache_entry.matched_identity.clone(),
            entry: ready_route_entry(&cache_entry.entry, backend),
            cache_policy: CachePolicy::new(remaining_ttl),
        };

        let outcome = self
            .resolver
            .apply_control_plane_message(update, now)
            .await
            .map_err(FrontlineRouteCoordinatorError::CacheUpdate)?;

        match outcome {
            crate::ApplyControlPlaneMessageOutcome::Updated(ApplyUpdateOutcome::Replaced(_)) => {
                Ok(())
            }
            crate::ApplyControlPlaneMessageOutcome::Updated(outcome) => {
                Err(FrontlineRouteCoordinatorError::RejectedCacheUpdate(outcome))
            }
            crate::ApplyControlPlaneMessageOutcome::Resolved(_)
            | crate::ApplyControlPlaneMessageOutcome::Miss(_)
            | crate::ApplyControlPlaneMessageOutcome::Invalidated { .. } => {
                unreachable!("RouteUpdated control-plane messages must produce an update outcome")
            }
        }
    }
}

fn ready_route_entry(observed: &RouteEntry, backend: ReadyBackend) -> RouteEntry {
    RouteEntry {
        route_binding_id: observed.route_binding_id.clone(),
        instance_id: observed.instance_id.clone(),
        instance_state: InstanceState::Running,
        instance_generation: backend.instance_generation,
        backend: Some(backend.backend),
        backend_generation: backend.backend_generation,
    }
}

fn wake_response_outcome(disposition: &WakeResponseDisposition) -> Outcome {
    match disposition {
        WakeResponseDisposition::Ready(_) => Outcome::Success,
        WakeResponseDisposition::WakeStarted { .. } => Outcome::Started,
        WakeResponseDisposition::StillWaking { .. } => Outcome::AlreadyWaking,
        WakeResponseDisposition::Failed { .. }
        | WakeResponseDisposition::Unavailable { .. }
        | WakeResponseDisposition::GenerationConflict { .. } => Outcome::Rejected,
        WakeResponseDisposition::Rejected(_) => Outcome::Error,
    }
}

impl<RouteClient, Wake> PartialEq for FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: PartialEq,
    Wake: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.resolver == other.resolver
            && self.wake_tracker == other.wake_tracker
            && self.wake_client == other.wake_client
    }
}

impl<RouteClient, Wake> Eq for FrontlineRouteCoordinator<RouteClient, Wake>
where
    RouteClient: Eq,
    Wake: Eq,
{
}

impl<RouteClientError, WakeClientError> fmt::Display
    for FrontlineRouteCoordinatorError<RouteClientError, WakeClientError>
where
    RouteClientError: fmt::Display,
    WakeClientError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolve(error) => write!(f, "route resolution failed: {error}"),
            Self::CacheUpdate(error) => write!(f, "route cache update failed: {error}"),
            Self::Wake(error) => write!(f, "wake request failed: {error}"),
            Self::WakeDeadline(request) => write!(
                f,
                "wake request timed out for instance {} generation {}",
                request.instance_id.as_str(),
                request.expected_generation.get()
            ),
            Self::RouteActorClosed => f.write_str("frontline route actor is closed"),
            Self::RejectedCacheUpdate(outcome) => {
                write!(f, "ready wake cache update was rejected: {outcome:?}")
            }
        }
    }
}

impl<RouteClientError, WakeClientError> std::error::Error
    for FrontlineRouteCoordinatorError<RouteClientError, WakeClientError>
where
    RouteClientError: fmt::Debug + fmt::Display,
    WakeClientError: fmt::Debug + fmt::Display,
{
}

#[cfg(test)]
mod tests;
