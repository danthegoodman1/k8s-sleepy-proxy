use std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use control_plane::{http01::InvalidHttp01Challenge, Http01ChallengeKey, Http01ChallengeRecord};
use http::{
    header::{CONTENT_TYPE, HOST},
    HeaderValue, Request, StatusCode,
};

use super::{
    http01_challenge_key, http01_challenge_token, intercept_http01_challenge,
    Http01InterceptDecision, Http01InterceptError, HTTP01_CONTENT_TYPE,
};

#[tokio::test]
async fn exact_challenge_path_resolves_with_canonical_host_and_serves_response() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let resolver_calls = Arc::clone(&calls);
    let request = challenge_request("Example.COM.:443", "token-a");

    let decision = intercept_http01_challenge(&request, move |key| {
        let resolver_calls = Arc::clone(&resolver_calls);
        async move {
            resolver_calls
                .lock()
                .expect("calls lock")
                .push((key.host().as_str().to_owned(), key.token().to_owned()));
            Ok::<_, TestResolveError>(Some(challenge_record(key, "token-a.key")))
        }
    })
    .await
    .expect("challenge intercept succeeds");

    assert_eq!(
        calls.lock().expect("calls lock").as_slice(),
        &[("example.com".to_owned(), "token-a".to_owned())]
    );

    let Http01InterceptDecision::Serve { key, response } = decision else {
        panic!("expected serve decision");
    };
    assert_eq!(key.host().as_str(), "example.com");
    assert_eq!(key.token(), "token-a");
    assert_eq!(response.key_authorization(), "token-a.key");

    let http_response = response.into_http_response();
    assert_eq!(http_response.status(), StatusCode::OK);
    assert_eq!(
        http_response
            .headers()
            .get(CONTENT_TYPE)
            .expect("content-type"),
        HTTP01_CONTENT_TYPE
    );
    assert_eq!(http_response.body(), &Bytes::from_static(b"token-a.key"));
}

#[tokio::test]
async fn non_challenge_paths_pass_through_without_resolver_call() {
    for path in [
        "/foo/.well-known/acme-challenge/token",
        "/.well-known/acme-challenge",
        "/.well-known/acme-challenge/",
        "/.well-known/acme-challenge/a/b",
        "/.well-known/acme-challenges/token",
    ] {
        let called = Arc::new(AtomicBool::new(false));
        let resolver_called = Arc::clone(&called);
        let request = request("app.example.com", path);

        let decision = intercept_http01_challenge(&request, move |key| {
            resolver_called.store(true, Ordering::SeqCst);
            async move { Ok::<_, TestResolveError>(Some(challenge_record(key, "unused"))) }
        })
        .await
        .expect("pass-through intercept succeeds");

        assert_eq!(decision, Http01InterceptDecision::PassThrough, "{path}");
        assert!(!called.load(Ordering::SeqCst), "{path}");
    }
}

#[tokio::test]
async fn invalid_host_and_token_inputs_return_typed_errors() {
    let missing_host = Request::builder()
        .uri("/.well-known/acme-challenge/token")
        .body(())
        .expect("request builds");
    let error =
        intercept_http01_challenge(&missing_host, |_| async { Ok::<_, TestResolveError>(None) })
            .await
            .expect_err("missing host rejects");
    assert!(matches!(error, Http01InterceptError::MissingHost));

    let mut invalid_header = Request::builder()
        .uri("/.well-known/acme-challenge/token")
        .body(())
        .expect("request builds");
    invalid_header.headers_mut().insert(
        HOST,
        HeaderValue::from_bytes(b"\xff").expect("opaque header"),
    );
    let error = intercept_http01_challenge(&invalid_header, |_| async {
        Ok::<_, TestResolveError>(None)
    })
    .await
    .expect_err("invalid host header rejects");
    assert!(matches!(error, Http01InterceptError::InvalidHostHeader));

    let invalid_host = request("localhost", "/.well-known/acme-challenge/token");
    let error =
        intercept_http01_challenge(&invalid_host, |_| async { Ok::<_, TestResolveError>(None) })
            .await
            .expect_err("invalid host rejects");
    assert!(matches!(error, Http01InterceptError::InvalidHost(_)));

    assert!(matches!(
        http01_challenge_key("app.example.com", ""),
        Err(Http01InterceptError::InvalidChallenge(
            InvalidHttp01Challenge::EmptyToken
        ))
    ));
    assert!(matches!(
        http01_challenge_key("app.example.com", "a/b"),
        Err(Http01InterceptError::InvalidTokenSegment)
    ));
}

#[tokio::test]
async fn resolver_miss_is_explicit_and_does_not_fabricate_response() {
    let request = challenge_request("app.example.com", "missing-token");

    let decision = intercept_http01_challenge(&request, |key| async move {
        assert_eq!(key.host().as_str(), "app.example.com");
        assert_eq!(key.token(), "missing-token");
        Ok::<_, TestResolveError>(None)
    })
    .await
    .expect("challenge intercept succeeds");

    let Http01InterceptDecision::Miss { key } = decision else {
        panic!("expected miss decision");
    };
    assert_eq!(key.host().as_str(), "app.example.com");
    assert_eq!(key.token(), "missing-token");
}

#[tokio::test]
async fn resolver_error_propagates_as_typed_error() {
    let request = challenge_request("app.example.com", "token-a");

    let error = intercept_http01_challenge(&request, |_key| async move {
        Err::<Option<Http01ChallengeRecord>, _>(TestResolveError("resolver down"))
    })
    .await
    .expect_err("resolver error propagates");

    assert_eq!(
        error,
        Http01InterceptError::Resolve(TestResolveError("resolver down"))
    );
}

#[tokio::test]
async fn challenge_path_precedes_normal_route_fallback() {
    let normal_route_called = Arc::new(AtomicBool::new(false));
    let request = challenge_request("app.example.com", "token-a");

    let decision = intercept_http01_challenge(&request, |key| async move {
        Ok::<_, TestResolveError>(Some(challenge_record(key, "token-a.key")))
    })
    .await
    .expect("challenge intercept succeeds");

    if matches!(decision, Http01InterceptDecision::PassThrough) {
        normal_route_called.store(true, Ordering::SeqCst);
    }

    assert!(!normal_route_called.load(Ordering::SeqCst));
    assert!(matches!(decision, Http01InterceptDecision::Serve { .. }));
}

#[test]
fn challenge_path_token_matching_is_exact() {
    assert_eq!(
        http01_challenge_token("/.well-known/acme-challenge/token"),
        Some("token")
    );
    assert_eq!(http01_challenge_token("/.well-known/acme-challenge"), None);
    assert_eq!(http01_challenge_token("/.well-known/acme-challenge/"), None);
    assert_eq!(
        http01_challenge_token("/.well-known/acme-challenge/a/b"),
        None
    );
    assert_eq!(http01_challenge_token("/prefix/acme-challenge/token"), None);
}

fn challenge_request(host: &str, token: &str) -> Request<()> {
    request(host, &format!("/.well-known/acme-challenge/{token}"))
}

fn request(host: &str, path: &str) -> Request<()> {
    Request::builder()
        .uri(path)
        .header(HOST, host)
        .body(())
        .expect("request builds")
}

fn challenge_record(key: Http01ChallengeKey, key_authorization: &str) -> Http01ChallengeRecord {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    Http01ChallengeRecord::new(key, key_authorization, now + Duration::from_secs(60), now)
        .expect("challenge record builds")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestResolveError(&'static str);

impl fmt::Display for TestResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for TestResolveError {}
