use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use control_plane::{
    BackendGeneration, CachePolicy, Generation, PathPrefix, RouteEntry, RouteHost, RouteHostKind,
    RouteIdentity,
};

use crate::{
    matcher::{rank_match, MatchRank},
    subscription::SubscriptionId,
};

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
    Positive(Arc<PositiveCacheEntry>),
    Negative(Arc<NegativeCacheEntry>),
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
    negative_capacity: usize,
    positives: Vec<Arc<PositiveCacheEntry>>,
    positive_index: PositiveRouteIndex,
    negatives: HashMap<RouteIdentity, Arc<NegativeCacheEntry>>,
    positive_lru: VecDeque<SubscriptionId>,
    negative_order: VecDeque<RouteIdentity>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PositiveRouteIndex {
    by_subscription: HashMap<SubscriptionId, usize>,
    by_matched_identity: HashMap<RouteIdentity, usize>,
    http: HttpRouteIndex,
    sni: SniRouteIndex,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct HttpRouteIndex {
    exact_hosts: HashMap<String, HttpPathIndex>,
    wildcard_suffixes: HashMap<String, HttpPathIndex>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct HttpPathIndex {
    paths: HashMap<String, Vec<usize>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SniRouteIndex {
    exact_hosts: HashMap<String, Vec<usize>>,
    wildcard_suffixes: HashMap<String, Vec<usize>>,
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
            negative_capacity: capacity,
            positives: Vec::new(),
            positive_index: PositiveRouteIndex::default(),
            negatives: HashMap::new(),
            positive_lru: VecDeque::new(),
            negative_order: VecDeque::new(),
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

        if let Some(entry) = self.negatives.get(request_identity) {
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
                        .push(replaced.subscription_id.clone());
                }
            }
        }

        self.remove_positive(&subscription_id);
        let positive = Arc::new(PositiveCacheEntry::new(
            subscription_id.clone(),
            matched_identity,
            entry,
            cache_policy,
            now,
        ));
        let index = self.positives.len();
        self.positive_index.insert(index, &positive);
        self.positives.push(positive);
        self.touch_positive(&subscription_id);
        result.extend(self.evict_positives_over_capacity());
        result
    }

    pub fn insert_negative(
        &mut self,
        request_identity: RouteIdentity,
        cache_policy: CachePolicy,
        now: Instant,
    ) -> CacheInsertResult {
        let result = self.expire(now);
        self.remove_negative(&request_identity);
        self.negatives.insert(
            request_identity.clone(),
            Arc::new(NegativeCacheEntry::new(
                request_identity.clone(),
                cache_policy,
                now,
            )),
        );
        self.touch_negative(&request_identity);
        self.evict_negatives_over_capacity();
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
            .filter(|(_, entry)| entry.is_expired(now))
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        for request_identity in expired_negatives {
            self.remove_negative(&request_identity);
        }

        result
    }

    pub fn invalidate_subscription(&mut self, subscription_id: &SubscriptionId) -> bool {
        self.remove_positive(subscription_id).is_some()
    }

    pub fn active_subscription_ids(&self) -> Vec<SubscriptionId> {
        self.positives
            .iter()
            .map(|entry| entry.subscription_id.clone())
            .collect()
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
                        .push(removed.subscription_id.clone());
                }
            }
        }

        if let Some(index) = self
            .positive_index
            .by_subscription
            .get(subscription_id)
            .copied()
        {
            let previous = self.positives[index].clone();
            self.positive_index.remove(index, &previous);
            let existing = Arc::make_mut(&mut self.positives[index]);
            existing.matched_identity = matched_identity;
            existing.entry = entry;
            existing.expires_at = now + cache_policy.ttl();
            let updated = self.positives[index].clone();
            self.positive_index.insert(index, &updated);
            self.touch_positive(subscription_id);
        }

        result
    }

    pub fn positive_by_subscription(
        &self,
        subscription_id: &SubscriptionId,
    ) -> Option<&Arc<PositiveCacheEntry>> {
        self.positive_index
            .by_subscription
            .get(subscription_id)
            .and_then(|index| self.positives.get(*index))
    }

    pub fn positive_by_matched_identity(
        &self,
        matched_identity: &RouteIdentity,
    ) -> Option<&Arc<PositiveCacheEntry>> {
        self.positive_index
            .by_matched_identity
            .get(matched_identity)
            .and_then(|index| self.positives.get(*index))
    }

    pub fn positives(&self) -> &[Arc<PositiveCacheEntry>] {
        &self.positives
    }

    pub fn negatives(&self) -> Vec<Arc<NegativeCacheEntry>> {
        self.negatives.values().cloned().collect()
    }

    fn best_positive_match(
        &self,
        request_identity: &RouteIdentity,
        now: Instant,
    ) -> Option<&Arc<PositiveCacheEntry>> {
        let mut best = None;

        match request_identity {
            RouteIdentity::Http { host, path } => {
                self.consider_http_index(
                    request_identity,
                    now,
                    host,
                    path.as_ref().map(|path| path.as_str()).unwrap_or("/"),
                    &self.positive_index.http,
                    &mut best,
                );
            }
            RouteIdentity::Sni { host } => {
                self.consider_sni_index(
                    request_identity,
                    now,
                    host,
                    &self.positive_index.sni,
                    &mut best,
                );
            }
        }

        best.map(|(_, _, index)| &self.positives[index])
    }

    fn remove_positive(
        &mut self,
        subscription_id: &SubscriptionId,
    ) -> Option<Arc<PositiveCacheEntry>> {
        let index = self
            .positive_index
            .by_subscription
            .get(subscription_id)
            .copied()?;
        self.positive_lru
            .retain(|existing| existing != subscription_id);
        let removed = self.positives.swap_remove(index);
        self.positive_index.remove(index, &removed);
        if index < self.positives.len() {
            let moved = self.positives[index].clone();
            self.positive_index.remove(self.positives.len(), &moved);
            self.positive_index.insert(index, &moved);
        }
        Some(removed)
    }

    fn remove_negative(
        &mut self,
        request_identity: &RouteIdentity,
    ) -> Option<Arc<NegativeCacheEntry>> {
        self.negative_order
            .retain(|existing| existing != request_identity);
        self.negatives.remove(request_identity)
    }

    fn evict_positives_over_capacity(&mut self) -> CacheInsertResult {
        let mut subscriptions_to_unsubscribe = Vec::new();

        while self.positives.len() > self.capacity {
            let Some(subscription_id) = self.positive_lru.pop_front() else {
                break;
            };

            if self.remove_positive(&subscription_id).is_some() {
                subscriptions_to_unsubscribe.push(subscription_id);
            }
        }

        CacheInsertResult {
            subscriptions_to_unsubscribe,
        }
    }

    fn evict_negatives_over_capacity(&mut self) {
        while self.negatives.len() > self.negative_capacity {
            let Some(identity) = self.negative_order.pop_front() else {
                break;
            };
            self.negatives.remove(&identity);
        }
    }

    fn touch_positive(&mut self, subscription_id: &SubscriptionId) {
        self.positive_lru
            .retain(|existing| existing != subscription_id);
        self.positive_lru.push_back(subscription_id.clone());
    }

    fn touch_negative(&mut self, request_identity: &RouteIdentity) {
        self.negative_order
            .retain(|existing| existing != request_identity);
        self.negative_order.push_back(request_identity.clone());
    }

    fn consider_http_index(
        &self,
        request_identity: &RouteIdentity,
        now: Instant,
        host: &RouteHost,
        request_path: &str,
        index: &HttpRouteIndex,
        best: &mut Option<(MatchRank, bool, usize)>,
    ) {
        if let Some(paths) = index.exact_hosts.get(host.as_str()) {
            self.consider_http_path_candidates(request_identity, now, request_path, paths, best);
        }

        for suffix in wildcard_suffixes(host.as_str()) {
            if let Some(paths) = index.wildcard_suffixes.get(suffix) {
                self.consider_http_path_candidates(
                    request_identity,
                    now,
                    request_path,
                    paths,
                    best,
                );
            }
        }
    }

    fn consider_http_path_candidates(
        &self,
        request_identity: &RouteIdentity,
        now: Instant,
        request_path: &str,
        index: &HttpPathIndex,
        best: &mut Option<(MatchRank, bool, usize)>,
    ) {
        for_matching_path_prefix(request_path, |path| {
            if let Some(candidates) = index.paths.get(path) {
                self.consider_positive_candidates(request_identity, now, candidates, best);
            }
        });
    }

    fn consider_sni_index(
        &self,
        request_identity: &RouteIdentity,
        now: Instant,
        host: &RouteHost,
        index: &SniRouteIndex,
        best: &mut Option<(MatchRank, bool, usize)>,
    ) {
        if let Some(candidates) = index.exact_hosts.get(host.as_str()) {
            self.consider_positive_candidates(request_identity, now, candidates, best);
        }

        for suffix in wildcard_suffixes(host.as_str()) {
            if let Some(candidates) = index.wildcard_suffixes.get(suffix) {
                self.consider_positive_candidates(request_identity, now, candidates, best);
            }
        }
    }

    fn consider_positive_candidates(
        &self,
        request_identity: &RouteIdentity,
        now: Instant,
        candidates: &[usize],
        best: &mut Option<(MatchRank, bool, usize)>,
    ) {
        for &index in candidates {
            let Some(entry) = self.positives.get(index) else {
                debug_assert!(false, "positive route index points outside positive cache");
                continue;
            };
            let Some(rank) = rank_match(request_identity, &entry.matched_identity) else {
                continue;
            };
            let candidate = (rank, entry.is_expired(now), index);
            if match best.as_ref() {
                Some(current) => positive_candidate_order(candidate, *current).is_gt(),
                None => true,
            } {
                *best = Some(candidate);
            }
        }
    }
}

impl CacheInsertResult {
    fn extend(&mut self, other: Self) {
        self.subscriptions_to_unsubscribe
            .extend(other.subscriptions_to_unsubscribe);
    }
}

impl PositiveRouteIndex {
    fn insert(&mut self, index: usize, entry: &PositiveCacheEntry) {
        self.by_subscription
            .insert(entry.subscription_id.clone(), index);
        self.by_matched_identity
            .insert(entry.matched_identity.clone(), index);

        match &entry.matched_identity {
            RouteIdentity::Http { host, path } => self.http.insert(host, path, index),
            RouteIdentity::Sni { host } => self.sni.insert(host, index),
        }
    }

    fn remove(&mut self, index: usize, entry: &PositiveCacheEntry) {
        self.by_subscription.remove(&entry.subscription_id);
        self.by_matched_identity.remove(&entry.matched_identity);

        match &entry.matched_identity {
            RouteIdentity::Http { host, path } => self.http.remove(host, path, index),
            RouteIdentity::Sni { host } => self.sni.remove(host, index),
        }
    }
}

impl HttpRouteIndex {
    fn insert(&mut self, host: &RouteHost, path: &Option<PathPrefix>, index: usize) {
        let hosts = match host.kind() {
            RouteHostKind::Exact => &mut self.exact_hosts,
            RouteHostKind::WildcardSuffix => &mut self.wildcard_suffixes,
        };
        hosts.entry(host.as_str().to_owned()).or_default().insert(
            path.as_ref().map(|path| path.as_str()).unwrap_or("/"),
            index,
        );
    }

    fn remove(&mut self, host: &RouteHost, path: &Option<PathPrefix>, index: usize) {
        let hosts = match host.kind() {
            RouteHostKind::Exact => &mut self.exact_hosts,
            RouteHostKind::WildcardSuffix => &mut self.wildcard_suffixes,
        };
        let key = host.as_str();
        if let Some(paths) = hosts.get_mut(key) {
            paths.remove(
                path.as_ref().map(|path| path.as_str()).unwrap_or("/"),
                index,
            );
            if paths.paths.is_empty() {
                hosts.remove(key);
            }
        }
    }
}

impl HttpPathIndex {
    fn insert(&mut self, path: &str, index: usize) {
        self.paths.entry(path.to_owned()).or_default().push(index);
    }

    fn remove(&mut self, path: &str, index: usize) {
        if let Some(candidates) = self.paths.get_mut(path) {
            candidates.retain(|candidate| *candidate != index);
            if candidates.is_empty() {
                self.paths.remove(path);
            }
        }
    }
}

impl SniRouteIndex {
    fn insert(&mut self, host: &RouteHost, index: usize) {
        let hosts = match host.kind() {
            RouteHostKind::Exact => &mut self.exact_hosts,
            RouteHostKind::WildcardSuffix => &mut self.wildcard_suffixes,
        };
        hosts
            .entry(host.as_str().to_owned())
            .or_default()
            .push(index);
    }

    fn remove(&mut self, host: &RouteHost, index: usize) {
        let hosts = match host.kind() {
            RouteHostKind::Exact => &mut self.exact_hosts,
            RouteHostKind::WildcardSuffix => &mut self.wildcard_suffixes,
        };
        let key = host.as_str();
        if let Some(candidates) = hosts.get_mut(key) {
            candidates.retain(|candidate| *candidate != index);
            if candidates.is_empty() {
                hosts.remove(key);
            }
        }
    }
}

fn positive_candidate_order(
    candidate: (MatchRank, bool, usize),
    current: (MatchRank, bool, usize),
) -> Ordering {
    let (candidate_rank, candidate_expired, candidate_index) = candidate;
    let (current_rank, current_expired, current_index) = current;

    candidate_rank
        .cmp(&current_rank)
        .then_with(|| current_expired.cmp(&candidate_expired))
        .then_with(|| candidate_index.cmp(&current_index))
}

fn wildcard_suffixes(host: &str) -> impl Iterator<Item = &str> {
    host.match_indices('.').map(|(index, _)| &host[index + 1..])
}

fn for_matching_path_prefix(request_path: &str, mut visit: impl FnMut(&str)) {
    visit("/");
    if request_path == "/" {
        return;
    }

    for (index, _) in request_path.match_indices('/').skip(1) {
        visit(&request_path[..index]);
        let slash_terminated = &request_path[..=index];
        if slash_terminated != request_path {
            visit(slash_terminated);
        }
    }

    visit(request_path);
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
