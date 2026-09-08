use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use frontline::{
    FrontlineRouteCoordinator, FrontlineRouteResolver, RouteCache, RouteSubscriptionClient,
    RouteSubscriptionFuture, SubscriptionId, WakeClient, WakeClientFuture, WakeInstanceRequest,
    WakeInstanceResponse, WakeTracker,
};
use sleepypods_api::{
    BackendEndpoint, BackendGeneration, CachePolicy, Generation, InstanceId, InstanceState,
    PathPrefix, RouteBindingId, RouteEntry, RouteHost, RouteIdentity,
};

const UNRELATED_ROUTES: usize = 4096;

#[derive(Clone, Debug)]
struct BenchRouteClient;

#[derive(Clone, Debug)]
struct BenchWakeClient;

#[derive(Clone, Debug)]
struct BenchControlPlaneError;

fn http_route_lookup(c: &mut Criterion) {
    let now = Instant::now();
    let exact_cache = exact_http_cache(now);
    let exact_request = http_request("target.example.test", "/api/v1/users");

    c.bench_function("route_lookup/http_exact_host_many_unrelated", |b| {
        b.iter(|| {
            black_box(exact_cache.lookup(black_box(&exact_request), black_box(now)));
        })
    });

    let wildcard_cache = wildcard_http_cache(now);
    let wildcard_request = http_request("api.customer.example.test", "/api/v1/users");

    c.bench_function("route_lookup/http_wildcard_suffix_many_unrelated", |b| {
        b.iter(|| {
            black_box(wildcard_cache.lookup(black_box(&wildcard_request), black_box(now)));
        })
    });

    let path_heavy_cache = path_heavy_http_cache(now);
    let path_heavy_request = http_request("path-heavy.example.test", "/api/v1/users");

    c.bench_function("route_lookup/http_same_host_many_paths", |b| {
        b.iter(|| {
            black_box(path_heavy_cache.lookup(black_box(&path_heavy_request), black_box(now)));
        })
    });
}

fn sni_route_lookup(c: &mut Criterion) {
    let now = Instant::now();
    let cache = sni_cache(now);
    let request = sni_request("db.customer.example.test");

    c.bench_function("route_lookup/sni_exact_host_many_unrelated", |b| {
        b.iter(|| {
            black_box(cache.lookup(black_box(&request), black_box(now)));
        })
    });
}

fn full_resolve_lookup(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime builds");
    let now = Instant::now();
    let request = http_request("target.example.test", "/api/v1/users");
    let shared = runtime.block_on(async move {
        let resolver = FrontlineRouteResolver::from_parts(
            frontline::SubscriptionState::from_cache(exact_http_cache(now)),
            BenchRouteClient,
        );
        FrontlineRouteCoordinator::new(resolver, WakeTracker::new(), BenchWakeClient).into_shared()
    });

    c.bench_function("route_lookup/full_resolve_http_exact_host_hot", |b| {
        b.iter(|| {
            runtime.block_on(async {
                black_box(
                    shared
                        .route(black_box(request.clone()), black_box(now))
                        .await
                        .expect("hot route resolves"),
                );
            });
        })
    });
}

#[derive(Debug)]
struct BenchObservationSink;
impl proxy_core::observability::recorder::ObservabilitySink for BenchObservationSink {
    fn record(&self, event: proxy_core::observability::recorder::ObservabilityEvent) {
        black_box(event);
    }
}

fn full_resolve_with_observations(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime builds");
    let now = Instant::now();
    let request = http_request("target.example.test", "/api/v1/users");
    let shared = runtime.block_on(async {
        FrontlineRouteCoordinator::with_observability(
            FrontlineRouteResolver::from_parts(
                frontline::SubscriptionState::from_cache(exact_http_cache(now)),
                BenchRouteClient,
            ),
            WakeTracker::new(),
            BenchWakeClient,
            proxy_core::observability::recorder::ObservabilityRecorder::new(std::sync::Arc::new(
                BenchObservationSink,
            )),
        )
        .into_shared()
    });
    c.bench_function(
        "route_lookup/full_resolve_http_exact_host_with_observations",
        |b| {
            b.iter(|| {
                runtime.block_on(async {
                    black_box(
                        shared
                            .route(request.clone(), now)
                            .await
                            .expect("hot route resolves"),
                    )
                })
            });
        },
    );
}

impl RouteSubscriptionClient for BenchRouteClient {
    type Error = BenchControlPlaneError;

    fn subscribe_route(
        &mut self,
        _request_id: frontline::RouteRequestId,
        _identity: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, frontline::SubscribeControlPlaneOutput, Self::Error> {
        Box::pin(async { panic!("hot benchmark must not subscribe") })
    }

    fn unsubscribe(
        &mut self,
        _subscription_id: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }
}

impl WakeClient for BenchWakeClient {
    type Error = BenchControlPlaneError;

    fn wake_instance(
        &mut self,
        _request: WakeInstanceRequest,
    ) -> WakeClientFuture<'static, WakeInstanceResponse, Self::Error> {
        Box::pin(async { panic!("hot benchmark must not wake") })
    }
}

fn exact_http_cache(now: Instant) -> RouteCache {
    let mut cache = RouteCache::new(UNRELATED_ROUTES + 8);
    insert_unrelated_http_exact_routes(&mut cache, now);

    cache.insert_positive(
        subscription_id("sub-target-root"),
        http_exact_rule("target.example.test", Some("/")),
        route_entry("route-target-root", 1),
        cache_policy(),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-target-api"),
        http_exact_rule("target.example.test", Some("/api")),
        route_entry("route-target-api", 2),
        cache_policy(),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-target-v1"),
        http_exact_rule("target.example.test", Some("/api/v1/users")),
        route_entry("route-target-v1", 3),
        cache_policy(),
        now,
    );

    cache
}

fn wildcard_http_cache(now: Instant) -> RouteCache {
    let mut cache = RouteCache::new(UNRELATED_ROUTES + 8);
    insert_unrelated_http_exact_routes(&mut cache, now);

    cache.insert_positive(
        subscription_id("sub-broad-wildcard"),
        http_wildcard_rule("example.test", Some("/api")),
        route_entry("route-broad-wildcard", 1),
        cache_policy(),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-specific-wildcard"),
        http_wildcard_rule("customer.example.test", Some("/api/v1/users")),
        route_entry("route-specific-wildcard", 2),
        cache_policy(),
        now,
    );

    cache.insert_resolved(
        http_request("api.customer.example.test", "/api/v1/users"),
        frontline::PositiveCacheEntry::new(
            subscription_id("sub-authoritative-wildcard"),
            http_wildcard_rule("customer.example.test", Some("/api/v1")),
            route_entry("route-authoritative-wildcard", 2),
            cache_policy(),
            now,
        ),
        now,
    );
    cache
}

fn path_heavy_http_cache(now: Instant) -> RouteCache {
    let mut cache = RouteCache::new(UNRELATED_ROUTES + 8);
    for index in 0..UNRELATED_ROUTES {
        cache.insert_positive(
            subscription_id(&format!("sub-path-unrelated-{index}")),
            http_exact_rule(
                "path-heavy.example.test",
                Some(&format!("/unrelated-{index}")),
            ),
            route_entry(&format!("route-path-unrelated-{index}"), generation(index)),
            cache_policy(),
            now,
        );
    }

    cache.insert_positive(
        subscription_id("sub-path-root"),
        http_exact_rule("path-heavy.example.test", Some("/")),
        route_entry("route-path-root", 1),
        cache_policy(),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-path-api"),
        http_exact_rule("path-heavy.example.test", Some("/api")),
        route_entry("route-path-api", 2),
        cache_policy(),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-path-v1"),
        http_exact_rule("path-heavy.example.test", Some("/api/v1/users")),
        route_entry("route-path-v1", 3),
        cache_policy(),
        now,
    );

    cache
}

fn sni_cache(now: Instant) -> RouteCache {
    let mut cache = RouteCache::new(UNRELATED_ROUTES + 8);
    for index in 0..UNRELATED_ROUTES {
        cache.insert_positive(
            subscription_id(&format!("sub-sni-unrelated-{index}")),
            sni_exact_rule(&format!("unrelated-{index}.example.test")),
            route_entry(&format!("route-sni-unrelated-{index}"), generation(index)),
            cache_policy(),
            now,
        );
    }

    cache.insert_positive(
        subscription_id("sub-sni-wildcard"),
        sni_wildcard_rule("customer.example.test"),
        route_entry("route-sni-wildcard", 1),
        cache_policy(),
        now,
    );
    cache.insert_positive(
        subscription_id("sub-sni-exact"),
        sni_exact_rule("db.customer.example.test"),
        route_entry("route-sni-exact", 2),
        cache_policy(),
        now,
    );

    cache
}

fn insert_unrelated_http_exact_routes(cache: &mut RouteCache, now: Instant) {
    for index in 0..UNRELATED_ROUTES {
        cache.insert_positive(
            subscription_id(&format!("sub-http-unrelated-{index}")),
            http_exact_rule(&format!("unrelated-{index}.example.test"), Some("/")),
            route_entry(&format!("route-http-unrelated-{index}"), generation(index)),
            cache_policy(),
            now,
        );
    }
}

fn http_request(host: &str, path: &str) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("valid host"),
        path: Some(PathPrefix::new(path).expect("valid path")),
    }
}

fn http_exact_rule(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::exact(host).expect("valid host"),
        path: path.map(|path| PathPrefix::new(path).expect("valid path")),
    }
}

fn http_wildcard_rule(host: &str, path: Option<&str>) -> RouteIdentity {
    RouteIdentity::Http {
        host: RouteHost::wildcard_suffix(host).expect("valid host"),
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

fn subscription_id(value: &str) -> SubscriptionId {
    SubscriptionId::new(value).expect("subscription ID")
}

fn route_entry(route_binding_id: &str, generation: u64) -> RouteEntry {
    RouteEntry {
        route_binding_id: RouteBindingId::new(route_binding_id).expect("route binding ID"),
        instance_id: InstanceId::new(format!("instance-{route_binding_id}")).expect("instance ID"),
        instance_state: InstanceState::Running,
        instance_generation: Generation::new(generation),
        backend: Some(
            BackendEndpoint::new(format!("http://127.0.0.1:{}", 10_000 + generation))
                .expect("benchmark backend"),
        ),
        backend_generation: Some(BackendGeneration::new(generation)),
    }
}

fn cache_policy() -> CachePolicy {
    CachePolicy::new(Duration::from_secs(600))
}

fn generation(index: usize) -> u64 {
    u64::try_from(index + 1).expect("benchmark generation fits u64")
}

#[derive(Clone)]
struct UpdatingBenchClient {
    last_update: Instant,
}
impl RouteSubscriptionClient for UpdatingBenchClient {
    type Error = BenchControlPlaneError;
    fn subscribe_route(
        &mut self,
        _: frontline::RouteRequestId,
        _: RouteIdentity,
    ) -> RouteSubscriptionFuture<'static, frontline::SubscribeControlPlaneOutput, Self::Error> {
        Box::pin(async { panic!("hot benchmark must not subscribe") })
    }
    fn unsubscribe(
        &mut self,
        _: SubscriptionId,
    ) -> RouteSubscriptionFuture<'static, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }
    fn drain_subscription_events(
        &mut self,
    ) -> RouteSubscriptionFuture<'static, Vec<frontline::RouteSubscriptionEvent>, Self::Error> {
        let now = Instant::now();
        let events = if now.duration_since(self.last_update) >= Duration::from_millis(100) {
            self.last_update = now;
            (0..64)
                .map(|index| {
                    frontline::RouteSubscriptionEvent::Update(Box::new(
                        frontline::SubscribeControlPlaneOutput::RouteUpdated {
                            subscription_id: subscription_id(&format!(
                                "sub-http-unrelated-{index}"
                            )),
                            matched_identity: http_request(
                                &format!("unrelated-{index}.example.test"),
                                "/",
                            ),
                            entry: route_entry(
                                &format!("route-http-unrelated-{index}"),
                                generation(index),
                            ),
                            cache_policy: cache_policy(),
                        },
                    ))
                })
                .collect()
        } else {
            Vec::new()
        };
        Box::pin(async move { Ok(events) })
    }
}
fn hot_resolve_during_updates(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let now = Instant::now();
    let request = http_request("target.example.test", "/api/v1/users");
    let shared = runtime.block_on(async {
        FrontlineRouteCoordinator::new(
            FrontlineRouteResolver::from_parts(
                frontline::SubscriptionState::from_cache(exact_http_cache(now)),
                UpdatingBenchClient { last_update: now },
            ),
            WakeTracker::new(),
            BenchWakeClient,
        )
        .into_shared()
    });
    c.bench_function(
        "route_lookup/full_resolve_during_640_updates_per_second",
        |b| {
            b.iter(|| {
                runtime.block_on(async {
                    black_box(shared.route(request.clone(), now).await.unwrap())
                })
            });
        },
    );
}

fn cache_maintenance_and_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_cache_scale");
    for capacity in [1_000, 10_000, 100_000] {
        let now = Instant::now();
        let mut cache = RouteCache::new(capacity);
        for index in 0..capacity {
            cache.insert_positive(
                subscription_id(&format!("sub-{index}")),
                http_request("scale.example.test", &format!("/{index}")),
                route_entry("scale", 1),
                cache_policy(),
                now,
            );
        }
        group.bench_with_input(
            BenchmarkId::new("idle_expire", capacity),
            &capacity,
            |b, _| {
                b.iter(|| black_box(cache.expire(black_box(now))));
            },
        );
        let hot = http_request("scale.example.test", &format!("/{}", capacity - 1));
        group.bench_with_input(
            BenchmarkId::new("hot_lookup", capacity),
            &capacity,
            |b, _| {
                b.iter(|| black_box(cache.lookup(black_box(&hot), black_box(now))));
            },
        );
        let mut next = capacity;
        group.bench_with_input(
            BenchmarkId::new("insert_evict", capacity),
            &capacity,
            |b, _| {
                b.iter(|| {
                    next += 1;
                    black_box(cache.insert_positive(
                        subscription_id(&format!("sub-{next}")),
                        http_request("scale.example.test", &format!("/{next}")),
                        route_entry("scale", 1),
                        cache_policy(),
                        now,
                    ));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    http_route_lookup,
    sni_route_lookup,
    full_resolve_lookup,
    full_resolve_with_observations,
    cache_maintenance_and_churn,
    hot_resolve_during_updates
);
criterion_main!(benches);
