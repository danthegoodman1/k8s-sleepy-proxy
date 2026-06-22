use std::{collections::VecDeque, time::Instant};

use control_plane::{BackendGeneration, CachePolicy, Generation, RouteEntry, RouteIdentity};

use crate::{matcher::rank_match, subscription::SubscriptionId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositiveCacheEntry {
    pub subscription_id: SubscriptionId,
    pub matched_identity: RouteIdentity,
    pub entry: RouteEntry,
    expires_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegativeCacheEntry {
    pub request_identity: RouteIdentity,
    expires_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheLookup {
    Hit(CacheLookupHit),
    Expired,
    Absent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheLookupHit {
    Positive(PositiveCacheEntry),
    Negative(NegativeCacheEntry),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheLookupStatus {
    PositiveHit,
    NegativeHit,
    Expired,
    Absent,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheInsertResult {
    pub subscriptions_to_unsubscribe: Vec<SubscriptionId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StaleRouteEntry {
    InstanceGeneration {
        current: Generation,
        incoming: Generation,
    },
    BackendGeneration {
        current: BackendGeneration,
        incoming: BackendGeneration,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteCache {
    capacity: usize,
    positives: Vec<PositiveCacheEntry>,
    negatives: Vec<NegativeCacheEntry>,
    order: VecDeque<CacheKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CacheKey {
    Positive(SubscriptionId),
    Negative(RouteIdentity),
}

impl PositiveCacheEntry {
    pub fn new(
        subscription_id: SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
        now: Instant,
    ) -> Self {
        Self {
            subscription_id,
            matched_identity,
            entry,
            expires_at: now + cache_policy.ttl(),
        }
    }

    pub fn expires_at(&self) -> Instant {
        self.expires_at
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

impl NegativeCacheEntry {
    pub fn new(request_identity: RouteIdentity, cache_policy: CachePolicy, now: Instant) -> Self {
        Self {
            request_identity,
            expires_at: now + cache_policy.ttl(),
        }
    }

    pub fn expires_at(&self) -> Instant {
        self.expires_at
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

impl CacheLookup {
    pub fn status(&self) -> CacheLookupStatus {
        match self {
            Self::Hit(CacheLookupHit::Positive(_)) => CacheLookupStatus::PositiveHit,
            Self::Hit(CacheLookupHit::Negative(_)) => CacheLookupStatus::NegativeHit,
            Self::Expired => CacheLookupStatus::Expired,
            Self::Absent => CacheLookupStatus::Absent,
        }
    }
}

impl RouteCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            positives: Vec::new(),
            negatives: Vec::new(),
            order: VecDeque::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.positives.len() + self.negatives.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn lookup(&self, request_identity: &RouteIdentity, now: Instant) -> CacheLookup {
        let mut matched_expired_positive = false;
        if let Some(entry) = self.best_positive_match(request_identity, now) {
            if !entry.is_expired(now) {
                return CacheLookup::Hit(CacheLookupHit::Positive(entry.clone()));
            }

            matched_expired_positive = true;
        }

        if let Some(entry) = self
            .negatives
            .iter()
            .find(|entry| &entry.request_identity == request_identity)
        {
            return if entry.is_expired(now) {
                CacheLookup::Expired
            } else {
                CacheLookup::Hit(CacheLookupHit::Negative(entry.clone()))
            };
        }

        if matched_expired_positive {
            CacheLookup::Expired
        } else {
            CacheLookup::Absent
        }
    }

    pub fn insert_positive(
        &mut self,
        subscription_id: SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
        now: Instant,
    ) -> CacheInsertResult {
        let mut result = self.expire(now);

        if let Some(existing) = self
            .positive_by_matched_identity(&matched_identity)
            .cloned()
        {
            if stale_route_entry(&existing.entry, &entry).is_some() {
                result.subscriptions_to_unsubscribe.push(subscription_id);
                return result;
            }

            if let Some(replaced) = self.remove_positive(&existing.subscription_id) {
                if replaced.subscription_id != subscription_id {
                    result
                        .subscriptions_to_unsubscribe
                        .push(replaced.subscription_id);
                }
            }
        }

        self.remove_positive(&subscription_id);
        self.positives.push(PositiveCacheEntry::new(
            subscription_id.clone(),
            matched_identity,
            entry,
            cache_policy,
            now,
        ));
        self.order.push_back(CacheKey::Positive(subscription_id));
        result.extend(self.evict_over_capacity());
        result
    }

    pub fn insert_negative(
        &mut self,
        request_identity: RouteIdentity,
        cache_policy: CachePolicy,
        now: Instant,
    ) -> CacheInsertResult {
        let mut result = self.expire(now);
        self.remove_negative(&request_identity);
        self.negatives.push(NegativeCacheEntry::new(
            request_identity.clone(),
            cache_policy,
            now,
        ));
        self.order.push_back(CacheKey::Negative(request_identity));
        result.extend(self.evict_over_capacity());
        result
    }

    pub fn expire(&mut self, now: Instant) -> CacheInsertResult {
        let mut result = CacheInsertResult::default();

        let expired_subscriptions = self
            .positives
            .iter()
            .filter(|entry| entry.is_expired(now))
            .map(|entry| entry.subscription_id.clone())
            .collect::<Vec<_>>();
        for subscription_id in expired_subscriptions {
            if self.remove_positive(&subscription_id).is_some() {
                result.subscriptions_to_unsubscribe.push(subscription_id);
            }
        }

        let expired_negatives = self
            .negatives
            .iter()
            .filter(|entry| entry.is_expired(now))
            .map(|entry| entry.request_identity.clone())
            .collect::<Vec<_>>();
        for request_identity in expired_negatives {
            self.remove_negative(&request_identity);
        }

        result
    }

    pub fn invalidate_subscription(&mut self, subscription_id: &SubscriptionId) -> bool {
        self.remove_positive(subscription_id).is_some()
    }

    pub fn replace_subscription(
        &mut self,
        subscription_id: &SubscriptionId,
        matched_identity: RouteIdentity,
        entry: RouteEntry,
        cache_policy: CachePolicy,
        now: Instant,
    ) -> CacheInsertResult {
        let mut result = CacheInsertResult::default();
        if self.positive_by_subscription(subscription_id).is_none() {
            return result;
        }

        if let Some(conflicting) = self
            .positive_by_matched_identity(&matched_identity)
            .cloned()
        {
            if &conflicting.subscription_id != subscription_id {
                if let Some(removed) = self.remove_positive(&conflicting.subscription_id) {
                    result
                        .subscriptions_to_unsubscribe
                        .push(removed.subscription_id);
                }
            }
        }

        if let Some(existing) = self
            .positives
            .iter_mut()
            .find(|entry| &entry.subscription_id == subscription_id)
        {
            existing.matched_identity = matched_identity;
            existing.entry = entry;
            existing.expires_at = now + cache_policy.ttl();
        }

        result
    }

    pub fn positive_by_subscription(
        &self,
        subscription_id: &SubscriptionId,
    ) -> Option<&PositiveCacheEntry> {
        self.positives
            .iter()
            .find(|entry| &entry.subscription_id == subscription_id)
    }

    pub fn positive_by_matched_identity(
        &self,
        matched_identity: &RouteIdentity,
    ) -> Option<&PositiveCacheEntry> {
        self.positives
            .iter()
            .find(|entry| &entry.matched_identity == matched_identity)
    }

    pub fn positives(&self) -> &[PositiveCacheEntry] {
        &self.positives
    }

    pub fn negatives(&self) -> &[NegativeCacheEntry] {
        &self.negatives
    }

    fn best_positive_match(
        &self,
        request_identity: &RouteIdentity,
        now: Instant,
    ) -> Option<&PositiveCacheEntry> {
        self.positives
            .iter()
            .filter_map(|entry| {
                rank_match(request_identity, &entry.matched_identity)
                    .map(|rank| (rank, entry.is_expired(now), entry))
            })
            .max_by(
                |(left_rank, left_expired, _), (right_rank, right_expired, _)| {
                    left_rank
                        .cmp(right_rank)
                        .then_with(|| right_expired.cmp(left_expired))
                },
            )
            .map(|(_, _, entry)| entry)
    }

    fn remove_positive(&mut self, subscription_id: &SubscriptionId) -> Option<PositiveCacheEntry> {
        let index = self
            .positives
            .iter()
            .position(|entry| &entry.subscription_id == subscription_id)?;
        self.order.retain(
            |key| !matches!(key, CacheKey::Positive(existing) if existing == subscription_id),
        );
        Some(self.positives.remove(index))
    }

    fn remove_negative(&mut self, request_identity: &RouteIdentity) -> Option<NegativeCacheEntry> {
        let index = self
            .negatives
            .iter()
            .position(|entry| &entry.request_identity == request_identity)?;
        self.order.retain(
            |key| !matches!(key, CacheKey::Negative(existing) if existing == request_identity),
        );
        Some(self.negatives.remove(index))
    }

    fn evict_over_capacity(&mut self) -> CacheInsertResult {
        let mut subscriptions_to_unsubscribe = Vec::new();

        while self.len() > self.capacity {
            let Some(key) = self.order.pop_front() else {
                break;
            };

            match key {
                CacheKey::Positive(subscription_id) => {
                    if self.remove_positive(&subscription_id).is_some() {
                        subscriptions_to_unsubscribe.push(subscription_id);
                    }
                }
                CacheKey::Negative(identity) => {
                    self.remove_negative(&identity);
                }
            }
        }

        CacheInsertResult {
            subscriptions_to_unsubscribe,
        }
    }
}

impl CacheInsertResult {
    fn extend(&mut self, other: Self) {
        self.subscriptions_to_unsubscribe
            .extend(other.subscriptions_to_unsubscribe);
    }
}

pub(crate) fn stale_route_entry(
    current: &RouteEntry,
    incoming: &RouteEntry,
) -> Option<StaleRouteEntry> {
    if current.instance_id != incoming.instance_id {
        return None;
    }

    // Instance and backend generations are only ordered within one instance
    // lineage. A route reassignment to a different instance can legitimately
    // restart generation numbers at a lower value.
    if incoming.instance_generation < current.instance_generation {
        return Some(StaleRouteEntry::InstanceGeneration {
            current: current.instance_generation,
            incoming: incoming.instance_generation,
        });
    }

    if let (Some(current), Some(incoming)) =
        (current.backend_generation, incoming.backend_generation)
    {
        if incoming < current {
            return Some(StaleRouteEntry::BackendGeneration { current, incoming });
        }
    }

    None
}

#[cfg(test)]
mod tests;
