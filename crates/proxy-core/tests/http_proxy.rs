use std::{
    convert::Infallible,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Empty, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use proxy_core::{DrainError, DrainTracker, HttpProxy, HttpProxyError};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

const REQUEST_BODY: &[u8] = b"preserve this http request body";

#[tokio::test]
async fn http_proxy_preserves_basic_request_response_bytes_and_tracks_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_origin: Uri = format!("http://{upstream_addr}")
        .parse()
        .expect("upstream URI parses");

    let (request_received_tx, request_received_rx) = oneshot::channel();
    let (release_response_tx, release_response_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let request_received_tx = Arc::new(Mutex::new(Some(request_received_tx)));
        let release_response_rx = Arc::new(Mutex::new(Some(release_response_rx)));

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |mut request: Request<Incoming>| {
                    let request_received_tx = Arc::clone(&request_received_tx);
                    let release_response_rx = Arc::clone(&release_response_rx);

                    async move {
                        let request_received_tx = request_received_tx
                            .lock()
                            .expect("request channel lock is not poisoned")
                            .take();
                        let release_response_rx = release_response_rx
                            .lock()
                            .expect("release channel lock is not poisoned")
                            .take();

                        assert_eq!(request.method(), "POST");
                        assert_eq!(
                            request
                                .uri()
                                .path_and_query()
                                .expect("path and query present")
                                .as_str(),
                            "/v1/items?preserve=true"
                        );
                        assert_eq!(
                            request.headers().get("host").expect("host forwarded"),
                            "public.example.test"
                        );
                        assert_eq!(
                            request
                                .headers()
                                .get("x-preserve")
                                .expect("header forwarded"),
                            "yes"
                        );
                        assert!(request.headers().get("connection").is_none());
                        assert!(request.headers().get("x-remove").is_none());

                        let body = request
                            .body_mut()
                            .collect()
                            .await
                            .expect("upstream reads request body")
                            .to_bytes();
                        assert_eq!(body, Bytes::from_static(REQUEST_BODY));

                        if let Some(tx) = request_received_tx {
                            tx.send(()).expect("test waits for request receipt");
                        }
                        if let Some(rx) = release_response_rx {
                            rx.await.expect("test releases upstream response");
                        }

                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::CREATED)
                                .header("x-upstream", "ok")
                                .header("connection", "x-response-remove")
                                .header("x-response-remove", "drop")
                                .body(Full::new(Bytes::from_static(REQUEST_BODY)))
                                .expect("response builds"),
                        )
                    }
                }),
            )
            .await
            .expect("upstream serves request");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = HttpProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                service_fn(move |request| {
                    let proxy = proxy.clone();
                    let upstream_origin = upstream_origin.clone();

                    async move { proxy.proxy(request, &upstream_origin).await }
                }),
            )
            .await
            .expect("proxy serves request");
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");
    client
        .write_all(
            format!(
                "POST /v1/items?preserve=true HTTP/1.1\r\n\
                 Host: public.example.test\r\n\
                 Content-Length: {}\r\n\
                 X-Preserve: yes\r\n\
                 Connection: close, x-remove\r\n\
                 X-Remove: no\r\n\
                 \r\n",
                REQUEST_BODY.len()
            )
            .as_bytes(),
        )
        .await
        .expect("client writes headers");
    client
        .write_all(REQUEST_BODY)
        .await
        .expect("client writes body");

    request_received_rx
        .await
        .expect("upstream received proxied request");
    assert_eq!(drain.active_count(), 1);

    release_response_tx
        .send(())
        .expect("upstream response released");

    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("client reads response");
    let (headers, body) = split_http_response(&response);
    assert!(headers.starts_with("HTTP/1.1 201 Created"));
    assert!(headers.contains("x-upstream: ok"));
    assert!(!headers.contains("x-response-remove"));
    assert_eq!(body, REQUEST_BODY);

    proxy_task.await.expect("proxy task completed");
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn http_proxy_rejects_new_request_after_drain_starts() {
    let drain = DrainTracker::new(Duration::from_secs(5));
    drain.start_drain();
    let proxy = HttpProxy::new(drain);
    let upstream_origin = "http://127.0.0.1:1".parse().expect("upstream URI parses");
    let request = Request::builder()
        .uri("/")
        .body(Empty::<Bytes>::new())
        .expect("request builds");

    let error = proxy
        .proxy(request, &upstream_origin)
        .await
        .expect_err("draining proxy rejects request");

    assert!(matches!(error, HttpProxyError::Drain(DrainError::Draining)));
}

fn split_http_response(response: &[u8]) -> (String, &[u8]) {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response has header terminator");
    let headers = String::from_utf8(response[..header_end].to_vec()).expect("headers are utf8");
    let body = &response[header_end + 4..];

    if headers
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        return (
            headers,
            decode_single_chunk(body).expect("response contains a single chunk"),
        );
    }

    (headers, body)
}

fn decode_single_chunk(body: &[u8]) -> Result<&[u8], Box<dyn Error + Send + Sync>> {
    let size_end = body
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or("chunk size terminator missing")?;
    let size = std::str::from_utf8(&body[..size_end])?;
    let size = usize::from_str_radix(size.trim(), 16)?;
    let chunk_start = size_end + 2;
    let chunk_end = chunk_start + size;

    if body.get(chunk_end..chunk_end + 5) != Some(b"\r\n0\r\n") {
        return Err("single chunk terminator missing".into());
    }

    Ok(&body[chunk_start..chunk_end])
}
