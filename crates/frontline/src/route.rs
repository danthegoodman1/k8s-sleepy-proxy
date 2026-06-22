use std::{fmt, future::Future, pin::Pin, time::Instant};

use control_plane::{CachePolicy, InstanceState, RouteEntry, RouteIdentity};

use crate::{
    route_wake_decision, validate_wake_response, ApplyUpdateOutcome, FrontlineRouteResolution,
    FrontlineRouteResolver, FrontlineRouteResolverError, NegativeCacheEntry, ReadyBackend,
    RouteSubscriptionClient, StaleWakeObservation, SubscribeControlPlaneOutput, WakeAdmission,
    WakeInstanceRequest, WakeInstanceResponse, WakeResponseDisposition, WakeTracker,
    WakeUnavailable, WakeWait,
};

pub type WakeClientFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>;

pub trait WakeClient {
    type Error;

    fn wake_instance(
        &mut self,
        request: WakeInstanceRequest,
    ) -> WakeClientFuture<'_, WakeInstanceResponse, Self::Error>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontlineRouteCoordinator<RouteClient, Wake> {
    resolver: FrontlineRouteResolver<RouteClient>,
    wake_tracker: WakeTracker,
    wake_client: Wake,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteOutcome {
    Ready(ReadyBackend),
    Miss(NegativeCacheEntry),
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
    RejectedCacheUpdate(ApplyUpdateOutcome),
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
        }
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
                        let response = match self.wake_client.wake_instance(request.clone()).await {
                            Ok(response) => response,
                            Err(error) => {
                                self.wake_tracker
                                    .complete(&request.instance_id, request.expected_generation);
                                return Err(FrontlineRouteCoordinatorError::Wake(error));
                            }
                        };

                        let disposition = validate_wake_response(&cache_entry.entry, response);
                        self.handle_wake_response(cache_entry, disposition, now)
                            .await
                    }
                }
            }
        }
    }

    async fn handle_wake_response(
        &mut self,
        cache_entry: crate::PositiveCacheEntry,
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
        cache_entry: crate::PositiveCacheEntry,
        backend: ReadyBackend,
        now: Instant,
    ) -> Result<(), FrontlineRouteCoordinatorError<RouteClient::Error, Wake::Error>> {
        let remaining_ttl = cache_entry.expires_at().saturating_duration_since(now);
        let update = SubscribeControlPlaneOutput::RouteUpdated {
            subscription_id: cache_entry.subscription_id,
            matched_identity: cache_entry.matched_identity,
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
