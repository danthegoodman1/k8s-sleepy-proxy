use std::time::{Duration, Instant};

use control_plane::{CachePolicy, PathPrefix, RouteHost, RouteIdentity};

use super::{CacheLookup, CacheLookupHit, CacheLookupStatus, RouteCache};
use crate::{subscription::tests::route_entry, SubscriptionId};

fn now() -> Instant {
    Instant::now()
}

fn http_request(host: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("valid host"),
        path: Some(PathPrefix::new(path).expect("valid path")),
    }
}

fn http_wildcard_rule(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(host).expect("valid host"),
        path: path.map(|path| PathPrefix::new(path).expect("valid path")),
    }
}

fn ttl(seconds: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_secs(seconds))
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription")
}

#[test]
fn positive_cache_hit_uses_matched_rule_identity() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("example.com", Some("/api")),
        route_entry("route-1", 1, None),
        ttl(10),
        now,
    );

    let lookup = cache.lookup(&http_request("app.example.com", "/api/users"), now);

    assert_eq!(lookup.status(), CacheLookupStatus::PositiveHit);
}

#[test]
fn positive_cache_misses_partial_path_segments() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("example.com", Some("/api")),
        route_entry("route-1", 1, None),
        ttl(10),
        now,
    );

    let lookup = cache.lookup(&http_request("app.example.com", "/apiary"), now);

    assert_eq!(lookup.status(), CacheLookupStatus::Absent);
}

#[test]
fn positive_expiry_does_not_route() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("example.com", None),
        route_entry("route-1", 1, None),
        ttl(1),
        now,
    );

    let lookup = cache.lookup(
        &http_request("app.example.com", "/"),
        now + Duration::from_secs(1),
    );

    assert_eq!(lookup.status(), CacheLookupStatus::Expired);
}

#[test]
fn expire_removes_positive_entries_and_surfaces_subscriptions() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("example.com", None),
        route_entry("route-1", 1, None),
        ttl(1),
        now,
    );

    let result = cache.expire(now + Duration::from_secs(1));

    assert_eq!(
        result.subscriptions_to_unsubscribe,
        vec![subscription_id("sub-1")]
    );
    assert_eq!(
        cache
            .lookup(
                &http_request("app.example.com", "/"),
                now + Duration::from_secs(1)
            )
            .status(),
        CacheLookupStatus::Absent
    );
}

#[test]
fn expired_positive_does_not_shadow_later_negative_miss() {
    let now = now();
    let mut cache = RouteCache::new(4);
    let request = http_request("app.example.com", "/");
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("example.com", None),
        route_entry("route-1", 1, None),
        ttl(1),
        now,
    );

    let result = cache.insert_negative(request.clone(), ttl(10), now + Duration::from_secs(1));

    assert_eq!(
        result.subscriptions_to_unsubscribe,
        vec![subscription_id("sub-1")]
    );
    assert_eq!(
        cache
            .lookup(&request, now + Duration::from_secs(1))
            .status(),
        CacheLookupStatus::NegativeHit
    );
}

#[test]
fn negative_cache_hit_and_expiry_are_distinct() {
    let now = now();
    let mut cache = RouteCache::new(4);
    let request = http_request("missing.example.com", "/");
    cache.insert_negative(request.clone(), ttl(1), now);

    assert_eq!(
        cache.lookup(&request, now).status(),
        CacheLookupStatus::NegativeHit
    );
    assert_eq!(
        cache
            .lookup(&request, now + Duration::from_secs(1))
            .status(),
        CacheLookupStatus::Expired
    );
}

#[test]
fn bounded_eviction_is_fifo_and_returns_positive_subscriptions() {
    let now = now();
    let mut cache = RouteCache::new(2);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("one.example.com", None),
        route_entry("route-1", 1, None),
        ttl(10),
        now,
    );
    cache.insert_negative(http_request("missing.example.com", "/"), ttl(10), now);
    let result = cache.insert_positive(
        subscription_id("sub-2"),
        http_wildcard_rule("two.example.com", None),
        route_entry("route-2", 1, None),
        ttl(10),
        now,
    );

    assert_eq!(
        result.subscriptions_to_unsubscribe,
        vec![subscription_id("sub-1")]
    );
    assert_eq!(cache.len(), 2);
    assert!(matches!(
        cache.lookup(&http_request("app.one.example.com", "/"), now),
        CacheLookup::Absent
    ));
    assert!(matches!(
        cache.lookup(&http_request("missing.example.com", "/"), now),
        CacheLookup::Hit(CacheLookupHit::Negative(_))
    ));
}
