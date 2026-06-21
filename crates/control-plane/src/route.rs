use std::{error::Error, fmt, time::Duration};

use crate::{
    ids::{BackendGeneration, Generation, IdempotencyKey, InstanceId, RouteBindingId},
    instance::InstanceState,
    materialization::BackendEndpoint,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouteIdentity {
    Http {
        host: RouteHost,
        path: Option<PathPrefix>,
    },
    Sni {
        host: RouteHost,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteBindingSpec {
    pub identity: RouteIdentity,
    pub protocol: ProtocolRoute,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteBindingRecord {
    pub id: RouteBindingId,
    pub instance_id: InstanceId,
    pub identity: RouteIdentity,
    pub protocol: ProtocolRoute,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRouteBindingRequest {
    pub idempotency_key: IdempotencyKey,
    pub route_binding_id: RouteBindingId,
    pub instance_id: InstanceId,
    pub identity: RouteIdentity,
    pub protocol: ProtocolRoute,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetRouteBindingRequest {
    pub route_binding_id: RouteBindingId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteRouteBindingRequest {
    pub route_binding_id: RouteBindingId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteResolution {
    Resolved(RouteEntry),
    Miss { negative_cache: CachePolicy },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteEntry {
    pub route_binding_id: RouteBindingId,
    pub instance_id: InstanceId,
    pub instance_state: InstanceState,
    pub instance_generation: Generation,
    pub backend: Option<BackendEndpoint>,
    pub backend_generation: Option<BackendGeneration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteDependencyLookup {
    route_binding_id: RouteBindingId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteDependencySet {
    pub route_binding_id: RouteBindingId,
    pub instance_id: InstanceId,
    pub materialization_generation: Option<BackendGeneration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtocolRoute {
    Http,
    TlsSni,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouteHost {
    kind: RouteHostKind,
    host: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteHostKind {
    Exact,
    WildcardSuffix,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PathPrefix(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CachePolicy {
    ttl: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidRouteHost {
    reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidPathPrefix {
    value: String,
}

impl RouteBindingSpec {
    pub fn new(identity: RouteIdentity, protocol: ProtocolRoute) -> Self {
        Self { identity, protocol }
    }
}

impl CreateRouteBindingRequest {
    pub fn new(
        idempotency_key: IdempotencyKey,
        route_binding_id: RouteBindingId,
        instance_id: InstanceId,
        identity: RouteIdentity,
        protocol: ProtocolRoute,
    ) -> Self {
        Self {
            idempotency_key,
            route_binding_id,
            instance_id,
            identity,
            protocol,
        }
    }

    pub fn spec(&self) -> RouteBindingSpec {
        RouteBindingSpec::new(self.identity.clone(), self.protocol)
    }
}

impl GetRouteBindingRequest {
    pub fn new(route_binding_id: RouteBindingId) -> Self {
        Self { route_binding_id }
    }
}

impl DeleteRouteBindingRequest {
    pub fn new(route_binding_id: RouteBindingId) -> Self {
        Self { route_binding_id }
    }
}

impl RouteDependencyLookup {
    pub fn new(route_binding_id: RouteBindingId) -> Self {
        Self { route_binding_id }
    }

    pub fn from_route_entry(entry: &RouteEntry) -> Self {
        Self::new(entry.route_binding_id.clone())
    }

    pub fn route_binding_id(&self) -> &RouteBindingId {
        &self.route_binding_id
    }
}

impl RouteHost {
    pub fn exact(host: impl AsRef<str>) -> Result<Self, InvalidRouteHost> {
        Ok(Self {
            kind: RouteHostKind::Exact,
            host: normalize_host(host.as_ref(), false)?,
        })
    }

    pub fn wildcard_suffix(host: impl AsRef<str>) -> Result<Self, InvalidRouteHost> {
        Ok(Self {
            kind: RouteHostKind::WildcardSuffix,
            host: normalize_host(host.as_ref(), true)?,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.host
    }

    pub fn kind(&self) -> RouteHostKind {
        self.kind
    }

    pub fn is_wildcard(&self) -> bool {
        self.kind == RouteHostKind::WildcardSuffix
    }
}

impl PathPrefix {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidPathPrefix> {
        let value = value.into();
        if value.starts_with('/') {
            return Ok(Self(value));
        }

        Err(InvalidPathPrefix { value })
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl CachePolicy {
    pub fn new(ttl: Duration) -> Self {
        Self { ttl }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }
}

impl InvalidRouteHost {
    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

impl fmt::Display for InvalidRouteHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason)
    }
}

impl Error for InvalidRouteHost {}

impl InvalidPathPrefix {
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for InvalidPathPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "route path prefix {:?} must start with /", self.value)
    }
}

impl Error for InvalidPathPrefix {}

fn normalize_host(value: &str, wildcard: bool) -> Result<String, InvalidRouteHost> {
    let host = value.trim().trim_end_matches('.').to_ascii_lowercase();
    let host = if wildcard {
        host.strip_prefix("*.").unwrap_or(&host).to_owned()
    } else {
        host
    };

    if host.is_empty() {
        return Err(InvalidRouteHost {
            reason: "route host must not be empty",
        });
    }

    if host.contains('*') {
        return Err(InvalidRouteHost {
            reason: "route host wildcard is only allowed as a leading wildcard suffix",
        });
    }

    if !host.contains('.') {
        return Err(InvalidRouteHost {
            reason: "route host must contain at least one dot",
        });
    }

    Ok(host)
}

#[cfg(test)]
mod tests {
    use super::{PathPrefix, RouteDependencyLookup, RouteEntry, RouteHost, RouteHostKind};
    use crate::{
        ids::{Generation, InstanceId, RouteBindingId},
        instance::InstanceState,
    };

    #[test]
    fn route_host_normalization_is_stable() {
        let exact = RouteHost::exact(" App.Example.COM. ").expect("valid host");
        let wildcard =
            RouteHost::wildcard_suffix("*.Customer.Example.COM.").expect("valid wildcard");

        assert_eq!(exact.as_str(), "app.example.com");
        assert_eq!(exact.kind(), RouteHostKind::Exact);
        assert_eq!(wildcard.as_str(), "customer.example.com");
        assert_eq!(wildcard.kind(), RouteHostKind::WildcardSuffix);
        assert!(wildcard.is_wildcard());
    }

    #[test]
    fn route_host_rejects_embedded_wildcards() {
        let error = RouteHost::exact("app.*.example.com").expect_err("wildcard is invalid");

        assert_eq!(
            error.reason(),
            "route host wildcard is only allowed as a leading wildcard suffix"
        );
    }

    #[test]
    fn path_prefix_must_start_with_slash() {
        let error = PathPrefix::new("api").expect_err("relative prefixes are invalid");

        assert_eq!(error.value(), "api");
    }

    #[test]
    fn dependency_lookup_uses_matched_route_binding_id() {
        let entry = RouteEntry {
            route_binding_id: RouteBindingId::new("route-1").expect("valid route ID"),
            instance_id: InstanceId::new("instance-1").expect("valid instance ID"),
            instance_state: InstanceState::Running,
            instance_generation: Generation::new(7),
            backend: None,
            backend_generation: None,
        };

        let lookup = RouteDependencyLookup::from_route_entry(&entry);

        assert_eq!(lookup.route_binding_id(), &entry.route_binding_id);
    }
}
