use std::{collections::VecDeque, convert::Infallible, sync::Arc};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use serde_json::{json, Value};
use std::sync::Mutex;
use tower::service_fn;

use super::*;

type Reply = (Method, StatusCode, Value, Option<Value>);

fn deployment(uid: &str, version: &str, replicas: i32) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {
            "name": "managed", "namespace": "apps", "uid": uid,
            "resourceVersion": version, "annotations": {"preserve": version}
        },
        "spec": {"replicas": replicas, "selector": {}, "template": {"metadata": {}}}
    })
}

fn status(code: u16) -> Value {
    json!({"apiVersion": "v1", "kind": "Status", "status": "Failure",
        "reason": "FixtureFailure", "code": code, "message": "controlled replica mutation"})
}

fn patch(version: &str, replicas: i32) -> Value {
    json!({"metadata": {"uid": "original", "resourceVersion": version},
        "spec": {"replicas": replicas}})
}

fn client(replies: Vec<Reply>) -> (Api<Deployment>, Arc<Mutex<VecDeque<Reply>>>) {
    let remaining = Arc::new(Mutex::new(VecDeque::from(replies)));
    let queued = remaining.clone();
    let service = service_fn(move |request: Request<kube::client::Body>| {
        let (method, status, body, expected_patch) = queued
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected Kubernetes request (or retry)");
        async move {
            assert_eq!(request.method(), method);
            assert_eq!(
                request.uri().path(),
                "/apis/apps/v1/namespaces/apps/deployments/managed"
            );
            if let Some(expected_patch) = expected_patch {
                assert_eq!(
                    request.headers()["content-type"],
                    "application/merge-patch+json"
                );
                let actual: Value = serde_json::from_slice(
                    &request.into_body().collect().await.unwrap().to_bytes(),
                )
                .unwrap();
                assert_eq!(actual, expected_patch);
            }
            Ok::<_, Infallible>(
                Response::builder()
                    .status(status)
                    .body(Full::new(Bytes::from(body.to_string())))
                    .unwrap(),
            )
        }
    });
    (
        Api::namespaced(Client::new(service, "apps"), "apps"),
        remaining,
    )
}

#[tokio::test]
async fn replica_fixture_conflict_rereads_version_for_scale_and_restore() {
    for replicas in [2, 1] {
        let (api, remaining) = client(vec![
            (
                Method::GET,
                StatusCode::OK,
                deployment("original", "10", 1),
                None,
            ),
            (
                Method::PATCH,
                StatusCode::CONFLICT,
                status(409),
                Some(patch("10", replicas)),
            ),
            // An unrelated annotation/status update changes resourceVersion. The
            // next request must use this fresh version and send neither field.
            (
                Method::GET,
                StatusCode::OK,
                deployment("original", "11", 1),
                None,
            ),
            (
                Method::PATCH,
                StatusCode::OK,
                deployment("original", "12", replicas),
                Some(patch("11", replicas)),
            ),
        ]);
        set_fixture_deployment_replicas(&api, "managed", "original", replicas)
            .await
            .unwrap();
        assert!(remaining.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn replica_fixture_never_patches_replaced_or_missing_uid_after_conflict() {
    for uid in ["replacement", ""] {
        let (api, remaining) = client(vec![
            (
                Method::GET,
                StatusCode::OK,
                deployment("original", "10", 1),
                None,
            ),
            (
                Method::PATCH,
                StatusCode::CONFLICT,
                status(409),
                Some(patch("10", 2)),
            ),
            (Method::GET, StatusCode::OK, deployment(uid, "11", 1), None),
        ]);
        let error = set_fixture_deployment_replicas(&api, "managed", "original", 2)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("UID changed or is missing"));
        assert!(remaining.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn replica_fixture_does_not_retry_other_api_errors() {
    for code in [403, 500] {
        let (api, remaining) = client(vec![
            (
                Method::GET,
                StatusCode::OK,
                deployment("original", "10", 2),
                None,
            ),
            (
                Method::PATCH,
                StatusCode::from_u16(code).unwrap(),
                status(code),
                Some(patch("10", 1)),
            ),
        ]);
        let error = set_fixture_deployment_replicas(&api, "managed", "original", 1)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("set replicas=1 failed"));
        assert!(remaining.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn replica_fixture_conflicts_stop_after_eight_guarded_patches() {
    let replies = (0..8)
        .flat_map(|version| {
            let version = version.to_string();
            [
                (
                    Method::GET,
                    StatusCode::OK,
                    deployment("original", &version, 1),
                    None,
                ),
                (
                    Method::PATCH,
                    StatusCode::CONFLICT,
                    status(409),
                    Some(patch(&version, 2)),
                ),
            ]
        })
        .collect();
    let (api, remaining) = client(replies);
    let error = set_fixture_deployment_replicas(&api, "managed", "original", 2)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("set replicas=2 failed"));
    assert!(error.to_string().contains("409"));
    assert!(remaining.lock().unwrap().is_empty());
}

#[tokio::test]
async fn replica_fixture_total_deadline_bounds_even_a_stalled_get() {
    let service = service_fn(|_: Request<kube::client::Body>| async {
        std::future::pending::<Result<Response<Full<Bytes>>, Infallible>>().await
    });
    let api = Api::namespaced(Client::new(service, "apps"), "apps");
    let error = set_fixture_deployment_replicas(&api, "managed", "original", 2)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("total5s fixture deadline"));
}
