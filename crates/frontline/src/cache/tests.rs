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

fn http_exact_rule(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("valid host"),
        path: path.map(|path| PathPrefix::new(path).expect("valid path")),
    }
}

fn sni_request(host: &str) -> RouteIdentity {
    RouteIdentity::Sni {
        host: RouteHost::exact(host).expect("valid host"),
    }
}

fn sni_exact_rule(host: &str) -> RouteIdentity {
    RouteIdentity::Sni {
        host: RouteHost::exact(host).expect("valid host"),
    }
}

fn sni_wildcard_rule(host: &str) -> RouteIdentity {
    RouteIdentity::Sni {
        host: RouteHost::wildcard_suffix(host).expect("valid host"),
    }
}

fn ttl(seconds: u64) -> CachePolicy {
    CachePolicy::new(Duration::from_secs(seconds))
}

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription")
}

fn assert_positive_route(lookup: CacheLookup, expected_route: &str) {
    let CacheLookup::Hit(CacheLookupHit::Positive(entry)) = lookup else {
        panic!("expected positive route {expected_route}, got {lookup:?}");
    };
    assert_eq!(entry.entry.route_binding_id.as_str(), expected_route);
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
fn exact_http_host_beats_wildcard_host() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-wildcard"),
        http_wildcard_rule("example.com", None),
        route_entry("route-wildcard", 1, None),
        ttl(10),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-exact"),
        http_exact_rule("app.example.com", None),
        route_entry("route-exact", 1, None),
        ttl(10),
        now,
    );

    assert_positive_route(
        cache.lookup(&http_request("app.example.com", "/"), now),
        "route-exact",
    );
}

#[test]
fn more_specific_http_wildcard_suffix_beats_broader_suffix() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-broad"),
        http_wildcard_rule("example.com", None),
        route_entry("route-broad", 1, None),
        ttl(10),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-specific"),
        http_wildcard_rule("customer.example.com", None),
        route_entry("route-specific", 1, None),
        ttl(10),
        now,
    );

    assert_positive_route(
        cache.lookup(&http_request("api.customer.example.com", "/"), now),
        "route-specific",
    );
}

#[test]
fn longest_http_path_prefix_wins_within_same_host_match() {
    let now = now();
    let mut cache = RouteCache::new(4);
    for (subscription, path, route) in [
        ("sub-root", "/", "route-root"),
        ("sub-api", "/api", "route-api"),
        ("sub-v1", "/api/v1", "route-v1"),
    ] {
        cache.insert_positive(
            subscription_id(subscription),
            http_exact_rule("app.example.com", Some(path)),
            route_entry(route, 1, None),
            ttl(10),
            now,
        );
    }

    assert_positive_route(
        cache.lookup(&http_request("app.example.com", "/api/v1/users"), now),
        "route-v1",
    );
}

#[test]
fn sni_exact_host_beats_wildcard_and_wildcard_matches_other_hosts() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-wildcard"),
        sni_wildcard_rule("example.com"),
        route_entry("route-wildcard", 1, None),
        ttl(10),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-exact"),
        sni_exact_rule("db.example.com"),
        route_entry("route-exact", 1, None),
        ttl(10),
        now,
    );

    assert_positive_route(
        cache.lookup(&sni_request("db.example.com"), now),
        "route-exact",
    );
    assert_positive_route(
        cache.lookup(&sni_request("other.example.com"), now),
        "route-wildcard",
    );
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
fn slash_terminated_path_prefix_uses_indexed_lookup() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_exact_rule("app.example.com", Some("/api/")),
        route_entry("route-1", 1, None),
        ttl(10),
        now,
    );

    assert_positive_route(
        cache.lookup(&http_request("app.example.com", "/api/users"), now),
        "route-1",
    );
    assert_eq!(
        cache
            .lookup(&http_request("app.example.com", "/api"), now)
            .status(),
        CacheLookupStatus::Absent
    );
}

#[test]
fn negative_cache_matches_exact_request_identity_only() {
    let now = now();
    let mut cache = RouteCache::new(4);
    let request = http_request("missing.example.com", "/missing");
    cache.insert_negative(request.clone(), ttl(10), now);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("example.com", Some("/api")),
        route_entry("route-1", 1, None),
        ttl(10),
        now,
    );

    assert_eq!(
        cache.lookup(&request, now).status(),
        CacheLookupStatus::NegativeHit
    );
    assert_eq!(
        cache
            .lookup(&http_request("missing.example.com", "/other"), now)
            .status(),
        CacheLookupStatus::Absent
    );
    assert_eq!(
        cache
            .lookup(&http_request("other.example.com", "/missing"), now)
            .status(),
        CacheLookupStatus::Absent
    );
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
fn expired_positive_match_reports_expired_unless_exact_negative_matches() {
    let now = now();
    let mut cache = RouteCache::new(4);
    let request = http_request("app.example.com", "/");
    cache.insert_negative(request.clone(), ttl(10), now);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("example.com", None),
        route_entry("route-1", 1, None),
        ttl(1),
        now,
    );

    assert_eq!(
        cache
            .lookup(&request, now + Duration::from_secs(1))
            .status(),
        CacheLookupStatus::NegativeHit
    );
    assert_eq!(
        cache
            .lookup(
                &http_request("other.example.com", "/"),
                now + Duration::from_secs(1)
            )
            .status(),
        CacheLookupStatus::Expired
    );
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

#[test]
fn replace_subscription_reindexes_new_matched_identity() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_exact_rule("old.example.com", None),
        route_entry("route-old", 1, None),
        ttl(10),
        now,
    );

    let result = cache.replace_subscription(
        &subscription_id("sub-1"),
        http_exact_rule("new.example.com", Some("/api")),
        route_entry("route-new", 2, None),
        ttl(10),
        now,
    );

    assert!(result.subscriptions_to_unsubscribe.is_empty());
    assert_eq!(
        cache
            .lookup(&http_request("old.example.com", "/"), now)
            .status(),
        CacheLookupStatus::Absent
    );
    assert_positive_route(
        cache.lookup(&http_request("new.example.com", "/api/users"), now),
        "route-new",
    );
}

#[test]
fn invalidating_subscription_removes_positive_from_lookup_index() {
    let now = now();
    let mut cache = RouteCache::new(4);
    cache.insert_positive(
        subscription_id("sub-1"),
        http_wildcard_rule("example.com", None),
        route_entry("route-1", 1, None),
        ttl(10),
        now,
    );

    assert!(cache.invalidate_subscription(&subscription_id("sub-1")));

    assert_eq!(
        cache
            .lookup(&http_request("app.example.com", "/"), now)
            .status(),
        CacheLookupStatus::Absent
    );
}
