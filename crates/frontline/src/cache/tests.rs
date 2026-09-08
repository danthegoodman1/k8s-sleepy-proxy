use super::{CacheLookup, CacheLookupHit, CacheLookupStatus, PositiveCacheEntry, RouteCache};
use crate::{subscription::tests::route_entry_for_instance, SubscriptionId};
use sleepypods_api::{CachePolicy, PathPrefix, RouteHost, RouteIdentity};
use std::time::{Duration, Instant};
fn request(host: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).unwrap(),
        path: Some(PathPrefix::new(path).unwrap()),
    }
}
fn sub(id: &str) -> SubscriptionId {
    SubscriptionId::new(id).unwrap()
}
fn positive(
    id: &str,
    matched: RouteIdentity,
    generation: u64,
    ttl: u64,
    now: Instant,
) -> PositiveCacheEntry {
    PositiveCacheEntry::new(
        sub(id),
        matched,
        route_entry_for_instance(id, "instance", generation, Some(generation)),
        CachePolicy::new(Duration::from_secs(ttl)),
        now,
    )
}
#[test]
fn wildcard_and_prefix_answers_authorize_only_the_queried_identity() {
    let now = Instant::now();
    let mut cache = RouteCache::new(4);
    let queried = request("public.example.com", "/api/users");
    let matched = RouteIdentity::Http {
        host: RouteHost::wildcard_suffix("example.com").unwrap(),
        path: Some(PathPrefix::new("/api").unwrap()),
    };
    cache.insert_resolved(
        queried.clone(),
        positive("wild", matched.clone(), 1, 30, now),
        now,
    );
    let CacheLookup::Hit(CacheLookupHit::Positive(entry)) = cache.lookup(&queried, now) else {
        panic!("queried route absent")
    };
    assert_eq!(entry.matched_identity, matched);
    for unseen in [
        request("private.example.com", "/api/users"),
        request("public.example.com", "/api/private"),
        request("public.example.com", "/api"),
    ] {
        assert_eq!(
            cache.lookup(&unseen, now).status(),
            CacheLookupStatus::Absent
        );
    }
}
#[test]
fn same_matched_rule_can_have_independent_subscriptions() {
    let now = Instant::now();
    let mut cache = RouteCache::new(2);
    let matched = request("app.example.com", "/");
    let a = request("app.example.com", "/a");
    let b = request("app.example.com", "/b");
    cache.insert_resolved(a.clone(), positive("a", matched.clone(), 1, 30, now), now);
    cache.insert_resolved(b.clone(), positive("b", matched, 1, 30, now), now);
    assert!(cache.invalidate_subscription(&sub("a")));
    assert_eq!(cache.lookup(&a, now).status(), CacheLookupStatus::Absent);
    assert_eq!(
        cache.lookup(&b, now).status(),
        CacheLookupStatus::PositiveHit
    );
}
#[test]
fn update_changes_metadata_without_expanding_authority() {
    let now = Instant::now();
    let mut cache = RouteCache::new(2);
    let key = request("app.example.com", "/a");
    cache.insert_resolved(
        key.clone(),
        positive("a", request("app.example.com", "/"), 1, 30, now),
        now,
    );
    cache.replace_subscription(
        &sub("a"),
        request("app.example.com", "/a"),
        route_entry_for_instance("new", "instance", 2, Some(2)),
        CachePolicy::new(Duration::from_secs(40)),
        now,
    );
    assert_eq!(
        cache.lookup(&key, now).status(),
        CacheLookupStatus::PositiveHit
    );
    assert_eq!(
        cache
            .lookup(&request("app.example.com", "/a/child"), now)
            .status(),
        CacheLookupStatus::Absent
    );
}
#[test]
fn positive_and_negative_fifo_budgets_are_independent() {
    let now = Instant::now();
    let mut cache = RouteCache::new(1);
    let key = request("hot.example.com", "/");
    cache.insert_resolved(key.clone(), positive("hot", key.clone(), 1, 30, now), now);
    for i in 0..100 {
        cache.insert_negative(
            request(&format!("scan{i}.example.com"), "/"),
            CachePolicy::new(Duration::from_secs(3)),
            now,
        );
    }
    assert_eq!(cache.len(), 2);
    assert_eq!(
        cache.lookup(&key, now).status(),
        CacheLookupStatus::PositiveHit
    );
    assert_eq!(cache.negative_order.len(), 1);
    assert_eq!(cache.negative_expiry.len(), 1);
}
#[test]
fn fifo_eviction_and_expiry_indices_remain_bounded_during_churn() {
    let now = Instant::now();
    let mut cache = RouteCache::new(8);
    for i in 0..1000 {
        let key = request("app.example.com", &format!("/{i}"));
        cache.insert_resolved(
            key.clone(),
            positive(&format!("sub{i}"), key, 1, 30, now),
            now,
        );
    }
    assert_eq!(cache.len(), 8);
    assert_eq!(cache.positive_order.len(), 8);
    assert_eq!(cache.positive_expiry.len(), 8);
    assert_eq!(
        cache
            .lookup(&request("app.example.com", "/0"), now)
            .status(),
        CacheLookupStatus::Absent
    );
    assert_eq!(
        cache
            .lookup(&request("app.example.com", "/999"), now)
            .status(),
        CacheLookupStatus::PositiveHit
    );
    assert_eq!(
        cache
            .expire(now + Duration::from_secs(30))
            .subscriptions_to_unsubscribe
            .len(),
        8
    );
    assert!(cache.is_empty());
    assert!(cache.by_request.is_empty());
    assert!(cache.positive_expiry.is_empty());
}
#[test]
fn stale_exact_answer_is_rejected_but_other_identities_do_not_conflict() {
    let now = Instant::now();
    let mut cache = RouteCache::new(4);
    let a = request("app.example.com", "/a");
    cache.insert_resolved(a.clone(), positive("new", a.clone(), 7, 30, now), now);
    let result = cache.insert_resolved(a.clone(), positive("old", a, 6, 30, now), now);
    assert_eq!(result.subscriptions_to_unsubscribe, vec![sub("old")]);
    assert!(cache.positive_by_subscription(&sub("new")).is_some());
}
#[test]
fn expired_positive_and_exact_negative_have_bounded_lifetimes() {
    let now = Instant::now();
    let mut cache = RouteCache::new(4);
    let key = request("app.example.com", "/");
    cache.insert_resolved(key.clone(), positive("a", key.clone(), 1, 1, now), now);
    assert_eq!(
        cache.lookup(&key, now + Duration::from_secs(1)).status(),
        CacheLookupStatus::Expired
    );
    let result = cache.insert_negative(
        key.clone(),
        CachePolicy::new(Duration::from_secs(2)),
        now + Duration::from_secs(1),
    );
    assert_eq!(result.subscriptions_to_unsubscribe, vec![sub("a")]);
    assert_eq!(
        cache.lookup(&key, now + Duration::from_secs(2)).status(),
        CacheLookupStatus::NegativeHit
    );
    cache.expire(now + Duration::from_secs(3));
    assert!(cache.is_empty());
}
#[test]
fn sni_wildcard_answer_does_not_authorize_an_unseen_exact_host() {
    let now = Instant::now();
    let mut cache = RouteCache::new(4);
    let key = RouteIdentity::Sni {
        host: RouteHost::exact("public.example.com").unwrap(),
    };
    let matched = RouteIdentity::Sni {
        host: RouteHost::wildcard_suffix("example.com").unwrap(),
    };
    cache.insert_resolved(key.clone(), positive("sni", matched, 1, 30, now), now);
    assert_eq!(
        cache.lookup(&key, now).status(),
        CacheLookupStatus::PositiveHit
    );
    assert_eq!(
        cache
            .lookup(
                &RouteIdentity::Sni {
                    host: RouteHost::exact("private.example.com").unwrap()
                },
                now
            )
            .status(),
        CacheLookupStatus::Absent
    );
}

#[test]
fn expiration_work_is_incremental_without_serving_expired_entries() {
    let now = Instant::now();
    let mut cache = RouteCache::new(512);
    for index in 0..512 {
        let key = request("app.example.com", &format!("/{index}"));
        cache.insert_resolved(
            key.clone(),
            positive(&format!("sub{index}"), key, 1, 1, now),
            now,
        );
    }
    let expired = now + Duration::from_secs(1);
    assert_eq!(
        cache
            .expire_limited(expired, 64)
            .subscriptions_to_unsubscribe
            .len(),
        64
    );
    assert_eq!(cache.len(), 448);
    assert_eq!(
        cache
            .lookup(&request("app.example.com", "/511"), expired)
            .status(),
        CacheLookupStatus::Expired
    );
}
