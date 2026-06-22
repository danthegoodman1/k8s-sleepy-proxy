use std::{error::Error, fmt, time::Instant};

use control_plane::{BackendGeneration, CachePolicy, Generation, RouteEntry, RouteIdentity};

use crate::{
    cache::{stale_route_entry, CacheInsertResult, StaleRouteEntry},
    RouteCache,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RouteRequestId(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxySubscribeInput {
    SubscribeRoute {
        request_id: RouteRequestId,
        identity: RouteIdentity,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscribeControlPlaneOutput {
    RouteResolved {
        request_id: RouteRequestId,
        subscription_id: SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
    },
    RouteMiss {
        request_id: RouteRequestId,
        request_identity: RouteIdentity,
        negative_cache_policy: CachePolicy,
    },
    RouteUpdated {
        subscription_id: SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
    },
    RouteInvalidated {
        subscription_id: SubscriptionId,
        reason: InvalidationReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidationReason {
    RouteRemoved,
    RouteChanged,
    InstanceChanged,
    BackendChanged,
    StreamClosed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyControlPlaneMessageOutcome {
    Resolved(CacheInsertResult),
    Miss(CacheInsertResult),
    Updated(ApplyUpdateOutcome),
    Invalidated { removed: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyUpdateOutcome {
    Replaced(CacheInsertResult),
    MissingSubscription,
    StaleInstanceGeneration {
        current: Generation,
        incoming: Generation,
    },
    StaleBackendGeneration {
        current: BackendGeneration,
        incoming: BackendGeneration,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsubscribeOutcome {
    Removed,
    AlreadyAbsent,
    SubscribeRequestIgnored,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptySubscriptionField {
    field: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionState {
    cache: RouteCache,
}

impl SubscriptionId {
    pub fn new(value: impl Into<String>) -> Result<Self, EmptySubscriptionField> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(EmptySubscriptionField {
                field: "subscription_id",
            });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl RouteRequestId {
    pub fn new(value: impl Into<String>) -> Result<Self, EmptySubscriptionField> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(EmptySubscriptionField {
                field: "request_id",
            });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl EmptySubscriptionField {
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for EmptySubscriptionField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} must not be empty", self.field)
    }
}

impl Error for EmptySubscriptionField {}

impl SubscriptionState {
    pub fn new(cache_capacity: usize) -> Self {
        Self {
            cache: RouteCache::new(cache_capacity),
        }
    }

    pub fn cache(&self) -> &RouteCache {
        &self.cache
    }

    pub fn cache_mut(&mut self) -> &mut RouteCache {
        &mut self.cache
    }

    pub fn apply_proxy_input(&mut self, input: ProxySubscribeInput) -> UnsubscribeOutcome {
        match input {
            ProxySubscribeInput::SubscribeRoute { .. } => {
                UnsubscribeOutcome::SubscribeRequestIgnored
            }
            ProxySubscribeInput::Unsubscribe { subscription_id } => {
                if self.cache.invalidate_subscription(&subscription_id) {
                    UnsubscribeOutcome::Removed
                } else {
                    UnsubscribeOutcome::AlreadyAbsent
                }
            }
        }
    }

    pub fn apply_control_plane_message(
        &mut self,
        message: SubscribeControlPlaneOutput,
        now: Instant,
    ) -> ApplyControlPlaneMessageOutcome {
        match message {
            SubscribeControlPlaneOutput::RouteResolved {
                subscription_id,
                matched_identity,
                entry,
                cache_policy,
                ..
            } => ApplyControlPlaneMessageOutcome::Resolved(self.cache.insert_positive(
                subscription_id,
                matched_identity,
                entry,
                cache_policy,
                now,
            )),
            SubscribeControlPlaneOutput::RouteMiss {
                request_identity,
                negative_cache_policy,
                ..
            } => ApplyControlPlaneMessageOutcome::Miss(self.cache.insert_negative(
                request_identity,
                negative_cache_policy,
                now,
            )),
            SubscribeControlPlaneOutput::RouteUpdated {
                subscription_id,
                matched_identity,
                entry,
                cache_policy,
            } => ApplyControlPlaneMessageOutcome::Updated(self.apply_update(
                &subscription_id,
                matched_identity,
                entry,
                cache_policy,
                now,
            )),
            SubscribeControlPlaneOutput::RouteInvalidated {
                subscription_id, ..
            } => ApplyControlPlaneMessageOutcome::Invalidated {
                removed: self.cache.invalidate_subscription(&subscription_id),
            },
        }
    }

    fn apply_update(
        &mut self,
        subscription_id: &SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
        now: Instant,
    ) -> ApplyUpdateOutcome {
        let Some(current) = self.cache.positive_by_subscription(subscription_id) else {
            return ApplyUpdateOutcome::MissingSubscription;
        };

        if let Some(stale) = stale_route_entry(&current.entry, &entry) {
            return stale_update_outcome(stale);
        }

        if let Some(conflicting) = self.cache.positive_by_matched_identity(&matched_identity) {
            if &conflicting.subscription_id != subscription_id {
                if let Some(stale) = stale_route_entry(&conflicting.entry, &entry) {
                    return stale_update_outcome(stale);
                }
            }
        }

        let result = self.cache.replace_subscription(
            subscription_id,
            matched_identity,
            entry,
            cache_policy,
            now,
        );
        ApplyUpdateOutcome::Replaced(result)
    }
}

fn stale_update_outcome(stale: StaleRouteEntry) -> ApplyUpdateOutcome {
    match stale {
        StaleRouteEntry::InstanceGeneration { current, incoming } => {
            ApplyUpdateOutcome::StaleInstanceGeneration { current, incoming }
        }
        StaleRouteEntry::BackendGeneration { current, incoming } => {
            ApplyUpdateOutcome::StaleBackendGeneration { current, incoming }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
