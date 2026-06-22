use std::{error::Error, fmt, future::Future, pin::Pin, time::Instant};

use control_plane::RouteIdentity;

use crate::{
    matcher::rank_match, ApplyControlPlaneMessageOutcome, CacheInsertResult, CacheLookup,
    CacheLookupHit, NegativeCacheEntry, PositiveCacheEntry, RouteRequestId,
    SubscribeControlPlaneOutput, SubscriptionId, SubscriptionState,
};

pub type RouteSubscriptionFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>;

pub trait RouteSubscriptionClient {
    type Error;

    fn subscribe_route(
        &mut self,
        request_id: RouteRequestId,
        identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'_, SubscribeControlPlaneOutput, Self::Error>;

    fn unsubscribe(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'_, (), Self::Error>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteResolution {
    Resolved(PositiveCacheEntry),
    Miss(NegativeCacheEntry),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontlineRouteResolverError<ClientError> {
    Subscribe(ClientError),
    Unsubscribe {
        subscription_id: SubscriptionId,
        source: ClientError,
    },
    Protocol(RouteResolverProtocolError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteResolverProtocolError {
    MismatchedRequestId {
        expected: RouteRequestId,
        actual: RouteRequestId,
    },
    MismatchedMissIdentity {
        expected: RouteIdentity,
        actual: RouteIdentity,
    },
    MismatchedResolvedIdentity {
        requested: RouteIdentity,
        matched: RouteIdentity,
    },
    UnexpectedSubscribeResponse {
        kind: UnexpectedSubscribeResponseKind,
    },
    ResponseDidNotInstallUsableCacheEntry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnexpectedSubscribeResponseKind {
    RouteUpdated,
    RouteInvalidated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontlineRouteResolver<Client> {
    state: SubscriptionState,
    client: Client,
    next_request_id: u64,
}

impl<Client> FrontlineRouteResolver<Client> {
    pub fn new(cache_capacity: usize, client: Client) -> Self {
        Self {
            state: SubscriptionState::new(cache_capacity),
            client,
            next_request_id: 0,
        }
    }

    pub fn from_parts(state: SubscriptionState, client: Client) -> Self {
        Self {
            state,
            client,
            next_request_id: 0,
        }
    }

    pub fn state(&self) -> &SubscriptionState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut SubscriptionState {
        &mut self.state
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    fn next_request_id(&mut self) -> RouteRequestId {
        self.next_request_id = self.next_request_id.saturating_add(1);
        RouteRequestId::new(format!("req:{}", self.next_request_id))
            .expect("resolver-generated request IDs are non-empty")
    }
}

impl<Client> FrontlineRouteResolver<Client>
where
    Client: RouteSubscriptionClient,
{
    pub async fn resolve(
        &mut self,
        identity: RouteIdentity,
        now: Instant,
    ) -> Result<FrontlineRouteResolution, FrontlineRouteResolverError<Client::Error>> {
        let expired = self.state.cache_mut().expire(now);
        self.unsubscribe_all(expired).await?;

        match self.state.cache().lookup(&identity, now) {
            CacheLookup::Hit(CacheLookupHit::Positive(entry)) => {
                return Ok(FrontlineRouteResolution::Resolved(entry));
            }
            CacheLookup::Hit(CacheLookupHit::Negative(entry)) => {
                return Ok(FrontlineRouteResolution::Miss(entry));
            }
            CacheLookup::Expired | CacheLookup::Absent => {}
        }

        let request_id = self.next_request_id();
        let message = self
            .client
            .subscribe_route(request_id.clone(), identity.clone())
            .await
            .map_err(FrontlineRouteResolverError::Subscribe)?;

        match &message {
            SubscribeControlPlaneOutput::RouteResolved {
                request_id: actual, ..
            }
            | SubscribeControlPlaneOutput::RouteMiss {
                request_id: actual, ..
            } => {
                if actual != &request_id {
                    return Err(FrontlineRouteResolverError::Protocol(
                        RouteResolverProtocolError::MismatchedRequestId {
                            expected: request_id,
                            actual: actual.clone(),
                        },
                    ));
                }
            }
            SubscribeControlPlaneOutput::RouteUpdated { .. } => {
                return Err(FrontlineRouteResolverError::Protocol(
                    RouteResolverProtocolError::UnexpectedSubscribeResponse {
                        kind: UnexpectedSubscribeResponseKind::RouteUpdated,
                    },
                ));
            }
            SubscribeControlPlaneOutput::RouteInvalidated { .. } => {
                return Err(FrontlineRouteResolverError::Protocol(
                    RouteResolverProtocolError::UnexpectedSubscribeResponse {
                        kind: UnexpectedSubscribeResponseKind::RouteInvalidated,
                    },
                ));
            }
        }

        match &message {
            SubscribeControlPlaneOutput::RouteResolved {
                matched_identity, ..
            } => {
                if rank_match(&identity, matched_identity).is_none() {
                    return Err(FrontlineRouteResolverError::Protocol(
                        RouteResolverProtocolError::MismatchedResolvedIdentity {
                            requested: identity,
                            matched: matched_identity.clone(),
                        },
                    ));
                }
            }
            SubscribeControlPlaneOutput::RouteMiss {
                request_identity, ..
            } => {
                if request_identity != &identity {
                    return Err(FrontlineRouteResolverError::Protocol(
                        RouteResolverProtocolError::MismatchedMissIdentity {
                            expected: identity,
                            actual: request_identity.clone(),
                        },
                    ));
                }
            }
            SubscribeControlPlaneOutput::RouteUpdated { .. }
            | SubscribeControlPlaneOutput::RouteInvalidated { .. } => {
                unreachable!("unexpected subscribe responses are rejected before validation")
            }
        }

        let outcome = self.state.apply_control_plane_message(message, now);
        self.unsubscribe_outcome(&outcome).await?;

        match self.state.cache().lookup(&identity, now) {
            CacheLookup::Hit(CacheLookupHit::Positive(entry)) => {
                Ok(FrontlineRouteResolution::Resolved(entry))
            }
            CacheLookup::Hit(CacheLookupHit::Negative(entry)) => {
                Ok(FrontlineRouteResolution::Miss(entry))
            }
            CacheLookup::Expired | CacheLookup::Absent => {
                Err(FrontlineRouteResolverError::Protocol(
                    RouteResolverProtocolError::ResponseDidNotInstallUsableCacheEntry,
                ))
            }
        }
    }

    pub async fn apply_control_plane_message(
        &mut self,
        message: SubscribeControlPlaneOutput,
        now: Instant,
    ) -> Result<ApplyControlPlaneMessageOutcome, FrontlineRouteResolverError<Client::Error>> {
        let outcome = self.state.apply_control_plane_message(message, now);
        self.unsubscribe_outcome(&outcome).await?;
        Ok(outcome)
    }

    async fn unsubscribe_outcome(
        &mut self,
        outcome: &ApplyControlPlaneMessageOutcome,
    ) -> Result<(), FrontlineRouteResolverError<Client::Error>> {
        match outcome {
            ApplyControlPlaneMessageOutcome::Resolved(result)
            | ApplyControlPlaneMessageOutcome::Miss(result) => {
                self.unsubscribe_all(result.clone()).await
            }
            ApplyControlPlaneMessageOutcome::Updated(crate::ApplyUpdateOutcome::Replaced(
                result,
            )) => self.unsubscribe_all(result.clone()).await,
            ApplyControlPlaneMessageOutcome::Updated(
                crate::ApplyUpdateOutcome::MissingSubscription
                | crate::ApplyUpdateOutcome::StaleInstanceGeneration { .. }
                | crate::ApplyUpdateOutcome::StaleBackendGeneration { .. },
            )
            | ApplyControlPlaneMessageOutcome::Invalidated { .. } => Ok(()),
        }
    }

    async fn unsubscribe_all(
        &mut self,
        result: CacheInsertResult,
    ) -> Result<(), FrontlineRouteResolverError<Client::Error>> {
        for subscription_id in result.subscriptions_to_unsubscribe {
            self.client
                .unsubscribe(subscription_id.clone())
                .await
                .map_err(|source| FrontlineRouteResolverError::Unsubscribe {
                    subscription_id,
                    source,
                })?;
        }

        Ok(())
    }
}

impl<ClientError> fmt::Display for FrontlineRouteResolverError<ClientError>
where
    ClientError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Subscribe(error) => write!(f, "subscribe route failed: {error}"),
            Self::Unsubscribe {
                subscription_id,
                source,
            } => write!(
                f,
                "unsubscribe {} failed: {source}",
                subscription_id.as_str()
            ),
            Self::Protocol(error) => write!(f, "{error}"),
        }
    }
}

impl<ClientError> Error for FrontlineRouteResolverError<ClientError> where
    ClientError: Error + 'static
{
}

impl fmt::Display for RouteResolverProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MismatchedRequestId { expected, actual } => write!(
                f,
                "subscribe response request_id {} did not match {}",
                actual.as_str(),
                expected.as_str()
            ),
            Self::MismatchedMissIdentity { .. } => {
                write!(
                    f,
                    "route miss response identity did not match request identity"
                )
            }
            Self::MismatchedResolvedIdentity { .. } => {
                write!(
                    f,
                    "route resolved response identity did not match request identity"
                )
            }
            Self::UnexpectedSubscribeResponse { kind } => {
                write!(f, "unexpected direct subscribe response: {kind:?}")
            }
            Self::ResponseDidNotInstallUsableCacheEntry => {
                write!(f, "subscribe response did not install a usable cache entry")
            }
        }
    }
}

impl Error for RouteResolverProtocolError {}

#[cfg(test)]
mod tests;
