use sleepypods_api::{RouteEntry, RouteHost, RouteHostKind, RouteIdentity};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteRule {
    pub matched_identity: RouteIdentity,
    pub entry: RouteEntry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchedRoute<'a> {
    pub matched_identity: &'a RouteIdentity,
    pub entry: &'a RouteEntry,
}

#[derive(Clone, Debug, Default)]
pub struct RouteMatcher {
    rules: Vec<RouteRule>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MatchRank {
    host_kind: u8,
    host_specificity: usize,
    path_specificity: usize,
}

impl RouteRule {
    pub fn new(matched_identity: RouteIdentity, entry: RouteEntry) -> Self {
        Self {
            matched_identity,
            entry,
        }
    }
}

impl RouteMatcher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_rules(rules: impl IntoIterator<Item = RouteRule>) -> Self {
        Self {
            rules: rules.into_iter().collect(),
        }
    }

    pub fn insert(&mut self, rule: RouteRule) {
        self.rules.push(rule);
    }

    pub fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(&RouteRule) -> bool,
    {
        self.rules.retain(|rule| keep(rule));
    }

    pub fn match_route(&self, request: &RouteIdentity) -> Option<MatchedRoute<'_>> {
        self.rules
            .iter()
            .filter_map(|rule| rank_match(request, &rule.matched_identity).map(|rank| (rank, rule)))
            .max_by(|(left_rank, _), (right_rank, _)| left_rank.cmp(right_rank))
            .map(|(_, rule)| MatchedRoute {
                matched_identity: &rule.matched_identity,
                entry: &rule.entry,
            })
    }

    pub fn rules(&self) -> &[RouteRule] {
        &self.rules
    }
}

pub(crate) fn rank_match(request: &RouteIdentity, rule: &RouteIdentity) -> Option<MatchRank> {
    match (request, rule) {
        (
            RouteIdentity::Http {
                host: request_host,
                path: request_path,
            },
            RouteIdentity::Http {
                host: rule_host,
                path: rule_path,
            },
        ) => {
            let host_rank = host_rank(request_host, rule_host)?;
            let request_path = request_path
                .as_ref()
                .map(|path| path.as_str())
                .unwrap_or("/");
            let rule_path = rule_path.as_ref().map(|path| path.as_str()).unwrap_or("/");
            if !path_prefix_matches(request_path, rule_path) {
                return None;
            }

            Some(MatchRank {
                host_kind: host_rank.0,
                host_specificity: host_rank.1,
                path_specificity: rule_path.len(),
            })
        }
        (RouteIdentity::Sni { host: request_host }, RouteIdentity::Sni { host: rule_host }) => {
            let host_rank = host_rank(request_host, rule_host)?;
            Some(MatchRank {
                host_kind: host_rank.0,
                host_specificity: host_rank.1,
                path_specificity: 0,
            })
        }
        _ => None,
    }
}

fn host_rank(request: &RouteHost, rule: &RouteHost) -> Option<(u8, usize)> {
    match rule.kind() {
        RouteHostKind::Exact => {
            if request.as_str() == rule.as_str() {
                Some((2, rule.as_str().len()))
            } else {
                None
            }
        }
        RouteHostKind::WildcardSuffix => {
            let request = request.as_str();
            let suffix = rule.as_str();
            if request != suffix && request.ends_with(suffix) {
                let boundary = request.len().checked_sub(suffix.len() + 1)?;
                if request.as_bytes().get(boundary) == Some(&b'.') {
                    return Some((1, suffix.len()));
                }
            }

            None
        }
    }
}

fn path_prefix_matches(request_path: &str, rule_path: &str) -> bool {
    if rule_path == "/" || request_path == rule_path {
        return true;
    }

    if rule_path.ends_with('/') {
        return request_path.starts_with(rule_path);
    }

    request_path
        .strip_prefix(rule_path)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use sleepypods_api::{PathPrefix, RouteHost, RouteIdentity};

    use super::{path_prefix_matches, RouteMatcher, RouteRule};
    use crate::subscription::tests::route_entry;

    fn http_exact(host: &str, path: Option<&str>, id: &str) -> RouteRule {
        RouteRule::new(
            RouteIdentity::Http {
                host: RouteHost::exact(host).expect("valid host"),
                path: path.map(|path| PathPrefix::new(path).expect("valid path")),
            },
            route_entry(id, 1, None),
        )
    }

    fn http_wildcard(host: &str, path: Option<&str>, id: &str) -> RouteRule {
        RouteRule::new(
            RouteIdentity::Http {
                host: RouteHost::wildcard_suffix(host).expect("valid host"),
                path: path.map(|path| PathPrefix::new(path).expect("valid path")),
            },
            route_entry(id, 1, None),
        )
    }

    fn sni_exact(host: &str, id: &str) -> RouteRule {
        RouteRule::new(
            RouteIdentity::Sni {
                host: RouteHost::exact(host).expect("valid host"),
            },
            route_entry(id, 1, None),
        )
    }

    fn sni_wildcard(host: &str, id: &str) -> RouteRule {
        RouteRule::new(
            RouteIdentity::Sni {
                host: RouteHost::wildcard_suffix(host).expect("valid host"),
            },
            route_entry(id, 1, None),
        )
    }

    fn http_request(host: &str, path: &str) -> RouteIdentity {
        RouteIdentity::Http {
            host: RouteHost::exact(host).expect("valid host"),
            path: Some(PathPrefix::new(path).expect("valid path")),
        }
    }

    fn sni_request(host: &str) -> RouteIdentity {
        RouteIdentity::Sni {
            host: RouteHost::exact(host).expect("valid host"),
        }
    }

    #[test]
    fn exact_http_host_beats_wildcard() {
        let matcher = RouteMatcher::from_rules([
            http_wildcard("example.com", None, "wildcard"),
            http_exact("app.example.com", None, "exact"),
        ]);

        let matched = matcher
            .match_route(&http_request("app.example.com", "/"))
            .expect("match");

        assert_eq!(matched.entry.route_binding_id.as_str(), "exact");
    }

    #[test]
    fn more_specific_wildcard_beats_broader_wildcard() {
        let matcher = RouteMatcher::from_rules([
            http_wildcard("example.com", None, "broad"),
            http_wildcard("customer.example.com", None, "specific"),
        ]);

        let matched = matcher
            .match_route(&http_request("api.customer.example.com", "/"))
            .expect("match");

        assert_eq!(matched.entry.route_binding_id.as_str(), "specific");
    }

    #[test]
    fn longest_path_prefix_wins_within_host_match() {
        let matcher = RouteMatcher::from_rules([
            http_exact("app.example.com", Some("/"), "root"),
            http_exact("app.example.com", Some("/api"), "api"),
            http_exact("app.example.com", Some("/api/v1"), "v1"),
        ]);

        let matched = matcher
            .match_route(&http_request("app.example.com", "/api/v1/users"))
            .expect("match");

        assert_eq!(matched.entry.route_binding_id.as_str(), "v1");
    }

    #[test]
    fn path_prefix_matches_at_segment_boundaries() {
        assert!(path_prefix_matches("/any/path", "/"));
        assert!(path_prefix_matches("/api", "/api"));
        assert!(path_prefix_matches("/api/users", "/api"));
        assert!(!path_prefix_matches("/apiary", "/api"));
    }

    #[test]
    fn slash_terminated_path_prefix_matches_under_that_prefix() {
        assert!(path_prefix_matches("/api/users", "/api/"));
        assert!(path_prefix_matches("/api/", "/api/"));
        assert!(!path_prefix_matches("/api", "/api/"));
        assert!(!path_prefix_matches("/apiary", "/api/"));
    }

    #[test]
    fn route_path_prefix_does_not_match_partial_segment() {
        let matcher =
            RouteMatcher::from_rules([http_exact("app.example.com", Some("/api"), "api")]);

        assert!(matcher
            .match_route(&http_request("app.example.com", "/apiary"))
            .is_none());
    }

    #[test]
    fn wildcard_miss_does_not_match_base_domain_or_unrelated_suffix() {
        let matcher = RouteMatcher::from_rules([http_wildcard("example.com", None, "wildcard")]);

        assert!(matcher
            .match_route(&http_request("example.com", "/"))
            .is_none());
        assert!(matcher
            .match_route(&http_request("badexample.com", "/"))
            .is_none());
    }

    #[test]
    fn sni_matches_exact_and_wildcard_hosts() {
        let matcher = RouteMatcher::from_rules([
            sni_wildcard("example.com", "wildcard"),
            sni_exact("db.example.com", "exact"),
        ]);

        let exact = matcher
            .match_route(&sni_request("db.example.com"))
            .expect("exact match");
        let wildcard = matcher
            .match_route(&sni_request("other.example.com"))
            .expect("wildcard match");

        assert_eq!(exact.entry.route_binding_id.as_str(), "exact");
        assert_eq!(wildcard.entry.route_binding_id.as_str(), "wildcard");
    }
}
