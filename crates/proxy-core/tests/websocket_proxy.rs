use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use proxy_core::{
    proxy_websocket_streams, DrainError, DrainTracker, WebSocketProxy, WebSocketProxyError,
};
use tokio::{
    io::{duplex, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::{
    accept_async, accept_hdr_async, connect_async,
    tungstenite::{
        client::IntoClientRequest,
        handshake::server::{
            Callback, ErrorResponse, Request as WsRequest, Response as WsResponse,
        },
        protocol::{CloseFrame, Role},
        Bytes as WsBytes, Error as TungsteniteError, Message,
    },
    WebSocketStream,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn websocket_proxy_preserves_bidirectional_messages_client_close_and_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_url = format!("ws://{upstream_addr}");

    let (first_message_tx, first_message_rx) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let mut websocket = accept_async(stream)
            .await
            .expect("upstream accepts websocket");

        let first = websocket
            .next()
            .await
            .expect("upstream receives first message")
            .expect("first message is valid");
        assert_eq!(first, Message::Text("client text".into()));
        first_message_tx
            .send(())
            .expect("test waits for first message");

        websocket
            .send(Message::Text("upstream text".into()))
            .await
            .expect("upstream sends text");

        let second = websocket
            .next()
            .await
            .expect("upstream receives binary message")
            .expect("binary message is valid");
        assert_eq!(
            second,
            Message::Binary(WsBytes::from_static(b"client bytes"))
        );

        websocket
            .send(Message::Binary(WsBytes::from_static(b"upstream bytes")))
            .await
            .expect("upstream sends binary");

        let close = websocket
            .next()
            .await
            .expect("upstream receives close")
            .expect("close message is valid");
        assert!(matches!(close, Message::Close(Some(_))));
        websocket
            .flush()
            .await
            .expect("upstream flushes close response");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = WebSocketProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.accept_and_proxy(stream, &upstream_url).await
    });

    let (mut client, _) = connect_async(format!("ws://{proxy_addr}"))
        .await
        .expect("client connects to proxy websocket");
    client
        .send(Message::Text("client text".into()))
        .await
        .expect("client sends text");

    first_message_rx
        .await
        .expect("upstream received first message");
    assert_eq!(drain.active_count(), 1);

    let from_upstream = client
        .next()
        .await
        .expect("client receives upstream text")
        .expect("upstream text is valid");
    assert_eq!(from_upstream, Message::Text("upstream text".into()));

    client
        .send(Message::Binary(WsBytes::from_static(b"client bytes")))
        .await
        .expect("client sends binary");

    let from_upstream = client
        .next()
        .await
        .expect("client receives upstream binary")
        .expect("upstream binary is valid");
    assert_eq!(
        from_upstream,
        Message::Binary(WsBytes::from_static(b"upstream bytes"))
    );

    client
        .send(Message::Close(Some(CloseFrame {
            code: 1000.into(),
            reason: "done".into(),
        })))
        .await
        .expect("client sends close");

    let close_response = client
        .next()
        .await
        .expect("client receives close response")
        .expect("close response is valid");
    assert!(matches!(close_response, Message::Close(Some(frame)) if frame.reason == "done"));

    let stats = proxy_task
        .await
        .expect("proxy task completed")
        .expect("proxy completed successfully");
    assert_eq!(stats.client_to_upstream_messages, 3);
    assert_eq!(stats.upstream_to_client_messages, 2);
    assert_eq!(
        stats.client_to_upstream_bytes,
        "client text".len() as u64 + b"client bytes".len() as u64 + "done".len() as u64
    );
    assert_eq!(
        stats.upstream_to_client_bytes,
        "upstream text".len() as u64 + b"upstream bytes".len() as u64
    );

    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn websocket_proxy_preserves_forwarded_headers_in_upstream_handshake() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_url = format!("ws://{upstream_addr}");

    let (headers_seen_tx, headers_seen_rx) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let mut websocket = accept_hdr_async(
            stream,
            AssertForwardedHeaders {
                headers_seen_tx: Some(headers_seen_tx),
            },
        )
        .await
        .expect("upstream accepts websocket");

        let close = websocket
            .next()
            .await
            .expect("upstream receives close")
            .expect("close frame valid");
        assert!(matches!(close, Message::Close(Some(_))));
        websocket.flush().await.expect("upstream flushes close");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let proxy = WebSocketProxy::new(DrainTracker::new(Duration::from_secs(5)));

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.accept_and_proxy(stream, &upstream_url).await
    });

    let mut request = format!("ws://{proxy_addr}")
        .into_client_request()
        .expect("client request builds");
    request.headers_mut().insert(
        "forwarded",
        "for=203.0.113.10;proto=https"
            .parse()
            .expect("forwarded header"),
    );
    request.headers_mut().insert(
        "x-forwarded-for",
        "203.0.113.11".parse().expect("x-forwarded-for header"),
    );
    request.headers_mut().insert(
        "x-forwarded-proto",
        "https".parse().expect("x-forwarded-proto header"),
    );
    request.headers_mut().insert(
        "x-forwarded-host",
        "edge.example.com".parse().expect("x-forwarded-host header"),
    );
    request.headers_mut().insert(
        "x-forwarded-prefix",
        "/edge".parse().expect("x-forwarded-prefix header"),
    );
    let (mut client, _) = connect_async(request)
        .await
        .expect("client connects to proxy websocket");

    headers_seen_rx.await.expect("upstream saw headers");
    client
        .send(Message::Close(Some(CloseFrame {
            code: 1000.into(),
            reason: "done".into(),
        })))
        .await
        .expect("client sends close");
    let close_response = client
        .next()
        .await
        .expect("client receives close response")
        .expect("close response is valid");
    assert!(matches!(close_response, Message::Close(Some(frame)) if frame.reason == "done"));

    proxy_task
        .await
        .expect("proxy task completed")
        .expect("proxy completed successfully");
    upstream_task.await.expect("upstream task completed");
}

#[tokio::test]
async fn websocket_proxy_forwards_upstream_initiated_close_and_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_url = format!("ws://{upstream_addr}");

    let (upstream_accepted_tx, upstream_accepted_rx) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let mut websocket = accept_async(stream)
            .await
            .expect("upstream accepts websocket");

        upstream_accepted_tx
            .send(())
            .expect("test waits for upstream websocket");
        websocket
            .send(Message::Close(Some(CloseFrame {
                code: 1000.into(),
                reason: "upstream done".into(),
            })))
            .await
            .expect("upstream sends close");

        let close_response = websocket
            .next()
            .await
            .expect("upstream receives close response")
            .expect("close response is valid");
        assert!(
            matches!(close_response, Message::Close(Some(frame)) if frame.reason == "upstream done")
        );
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = WebSocketProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.accept_and_proxy(stream, &upstream_url).await
    });

    let (mut client, _) = connect_async(format!("ws://{proxy_addr}"))
        .await
        .expect("client connects to proxy websocket");
    upstream_accepted_rx
        .await
        .expect("upstream accepted websocket");
    assert_eq!(drain.active_count(), 1);

    let close = client
        .next()
        .await
        .expect("client receives upstream close")
        .expect("upstream close is valid");
    assert!(matches!(close, Message::Close(Some(frame)) if frame.reason == "upstream done"));
    client.flush().await.expect("client flushes close response");

    let stats = proxy_task
        .await
        .expect("proxy task completed")
        .expect("proxy completed successfully");
    assert_eq!(stats.client_to_upstream_messages, 0);
    assert_eq!(stats.upstream_to_client_messages, 1);
    assert_eq!(stats.client_to_upstream_bytes, 0);
    assert_eq!(stats.upstream_to_client_bytes, "upstream done".len() as u64);

    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn websocket_proxy_surfaces_upstream_upgrade_failure_and_releases_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_url = format!("ws://{upstream_addr}");

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        stream
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\n\
                  Content-Length: 0\r\n\
                  Connection: close\r\n\
                  \r\n",
            )
            .await
            .expect("upstream writes failed upgrade response");
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = WebSocketProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.accept_and_proxy(stream, &upstream_url).await
    });

    let error = connect_async(format!("ws://{proxy_addr}"))
        .await
        .expect_err("upstream rejection is returned before 101");
    let TungsteniteError::Http(response) = error else {
        panic!("expected upstream HTTP rejection");
    };
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);

    let error = timeout(TEST_TIMEOUT, proxy_task)
        .await
        .expect("proxy reports upstream upgrade failure before timeout")
        .expect("proxy task completed")
        .expect_err("proxy reports upstream upgrade failure");
    assert!(matches!(error, WebSocketProxyError::UpstreamConnect(_)));

    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn websocket_proxy_releases_lifecycle_after_client_peer_disconnect() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_url = format!("ws://{upstream_addr}");
    let (upstream_accepted_tx, upstream_accepted_rx) = oneshot::channel();

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let mut websocket = accept_async(stream)
            .await
            .expect("upstream accepts websocket");
        upstream_accepted_tx
            .send(())
            .expect("test waits for upstream websocket");

        let disconnected = timeout(TEST_TIMEOUT, websocket.next())
            .await
            .expect("upstream observes client peer disconnect before timeout");
        assert!(
            matches!(disconnected, Some(Err(_))),
            "upstream should observe unclean client peer disconnect as a websocket error"
        );
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = WebSocketProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.accept_and_proxy(stream, &upstream_url).await
    });

    let (client, _) = connect_async(format!("ws://{proxy_addr}"))
        .await
        .expect("client connects to proxy websocket");
    upstream_accepted_rx
        .await
        .expect("upstream accepted websocket");
    assert_eq!(drain.active_count(), 1);
    drop(client);

    let error = timeout(TEST_TIMEOUT, proxy_task)
        .await
        .expect("proxy reports client peer disconnect before timeout")
        .expect("proxy task completed")
        .expect_err("unclean client peer disconnect is reported");
    assert!(matches!(error, WebSocketProxyError::Proxy(_)));
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn websocket_proxy_releases_lifecycle_after_upstream_peer_disconnect() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_url = format!("ws://{upstream_addr}");

    let (disconnect_tx, disconnect_rx) = oneshot::channel();
    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let websocket = accept_async(stream)
            .await
            .expect("upstream accepts websocket");
        disconnect_rx.await.unwrap();
        drop(websocket);
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = WebSocketProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.accept_and_proxy(stream, &upstream_url).await
    });

    let (mut client, _) = connect_async(format!("ws://{proxy_addr}"))
        .await
        .expect("client connects to proxy websocket");
    drain.wait_for_active_count(1).await;
    disconnect_tx.send(()).unwrap();

    let disconnected = timeout(TEST_TIMEOUT, client.next())
        .await
        .expect("client observes upstream peer disconnect before timeout");
    assert!(
        disconnected.is_none() || matches!(disconnected, Some(Err(_))),
        "client should observe EOF or a websocket error after upstream peer disconnect"
    );

    let error = timeout(TEST_TIMEOUT, proxy_task)
        .await
        .expect("proxy reports upstream peer disconnect before timeout")
        .expect("proxy task completed")
        .expect_err("unclean upstream peer disconnect is reported");
    assert!(matches!(error, WebSocketProxyError::Proxy(_)));
    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn websocket_proxy_backpressure_blocks_until_slow_downstream_peer_reads() {
    const MESSAGE_COUNT: usize = 32;

    let (downstream_peer, proxy_downstream) = duplex(1024);
    let (proxy_upstream, upstream_peer) = duplex(1024);
    let proxy_downstream =
        WebSocketStream::from_raw_socket(proxy_downstream, Role::Server, None).await;
    let proxy_upstream = WebSocketStream::from_raw_socket(proxy_upstream, Role::Client, None).await;
    let mut downstream =
        WebSocketStream::from_raw_socket(downstream_peer, Role::Client, None).await;
    let mut upstream = WebSocketStream::from_raw_socket(upstream_peer, Role::Server, None).await;
    let payload = WsBytes::from(vec![b's'; 16 * 1024]);

    let mut proxy_task = tokio::spawn(proxy_websocket_streams(proxy_downstream, proxy_upstream));
    let upstream_payload = payload.clone();
    let mut upstream_task = tokio::spawn(async move {
        for _ in 0..MESSAGE_COUNT {
            upstream
                .send(Message::Binary(upstream_payload.clone()))
                .await
                .expect("upstream eventually sends backpressured message");
        }
    });

    tokio::task::yield_now().await;
    assert!(
        timeout(Duration::from_millis(25), &mut proxy_task)
            .await
            .is_err(),
        "proxy should remain blocked while the downstream peer is not reading"
    );
    assert!(
        timeout(Duration::from_millis(25), &mut upstream_task)
            .await
            .is_err(),
        "bounded websocket path should apply backpressure to the upstream sender"
    );

    for _ in 0..MESSAGE_COUNT {
        let message = timeout(TEST_TIMEOUT, downstream.next())
            .await
            .expect("downstream receives message after resuming reads")
            .expect("downstream receives websocket message")
            .expect("websocket message is valid");
        assert_eq!(message, Message::Binary(payload.clone()));
    }

    upstream_task.await.expect("upstream task completed");
    let _ = timeout(TEST_TIMEOUT, downstream.next())
        .await
        .expect("downstream observes upstream completion after draining messages");
    let error = proxy_task
        .await
        .expect("proxy task completed")
        .expect_err("raw upstream disconnect is reported after slow downstream resumes");
    assert!(matches!(error, TungsteniteError::Protocol(_)));
}

#[tokio::test]
async fn websocket_proxy_forwards_large_binary_frame_backpressure_smoke() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");
    let upstream_url = format!("ws://{upstream_addr}");
    let payload = WsBytes::from(vec![b'z'; 256 * 1024]);

    let upstream_payload = payload.clone();
    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let mut websocket = accept_async(stream)
            .await
            .expect("upstream accepts websocket");

        let message = websocket
            .next()
            .await
            .expect("upstream receives large frame")
            .expect("large frame is valid");
        assert_eq!(message, Message::Binary(upstream_payload));

        websocket
            .send(Message::Close(Some(CloseFrame {
                code: 1000.into(),
                reason: "large done".into(),
            })))
            .await
            .expect("upstream sends close");
        let _ = websocket.next().await;
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener.local_addr().expect("proxy listener address");
    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = WebSocketProxy::new(drain.clone());

    let proxy_task = tokio::spawn(async move {
        let (stream, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.accept_and_proxy(stream, &upstream_url).await
    });

    let (mut client, _) = connect_async(format!("ws://{proxy_addr}"))
        .await
        .expect("client connects to proxy websocket");
    client
        .send(Message::Binary(payload.clone()))
        .await
        .expect("client sends large binary frame");

    let close = client
        .next()
        .await
        .expect("client receives close")
        .expect("close is valid");
    assert!(matches!(close, Message::Close(Some(frame)) if frame.reason == "large done"));
    client.flush().await.expect("client flushes close response");

    let stats = proxy_task
        .await
        .expect("proxy task completed")
        .expect("proxy completed successfully");
    assert_eq!(stats.client_to_upstream_messages, 1);
    assert_eq!(stats.client_to_upstream_bytes, payload.len() as u64);

    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}

#[tokio::test]
async fn websocket_proxy_rejects_new_session_after_drain_starts() {
    let drain = DrainTracker::new(Duration::from_secs(5));
    drain.start_drain();
    let proxy = WebSocketProxy::new(drain);
    let (client, _server) = tokio::io::duplex(64);

    let error = proxy
        .accept_and_proxy(client, "ws://127.0.0.1:1")
        .await
        .expect_err("draining proxy rejects websocket session");

    assert!(matches!(
        error,
        WebSocketProxyError::Drain(DrainError::Draining)
    ));
}

struct AssertForwardedHeaders {
    headers_seen_tx: Option<oneshot::Sender<()>>,
}

impl Callback for AssertForwardedHeaders {
    fn on_request(
        mut self,
        request: &WsRequest,
        response: WsResponse,
    ) -> Result<WsResponse, ErrorResponse> {
        assert_eq!(
            request
                .headers()
                .get("forwarded")
                .expect("forwarded header"),
            "for=203.0.113.10;proto=https"
        );
        assert_eq!(
            request
                .headers()
                .get("x-forwarded-for")
                .expect("x-forwarded-for header"),
            "203.0.113.11"
        );
        assert_eq!(
            request
                .headers()
                .get("x-forwarded-proto")
                .expect("x-forwarded-proto header"),
            "https"
        );
        assert_eq!(
            request
                .headers()
                .get("x-forwarded-host")
                .expect("x-forwarded-host header"),
            "edge.example.com"
        );
        assert_eq!(
            request
                .headers()
                .get("x-forwarded-prefix")
                .expect("x-forwarded-prefix header"),
            "/edge"
        );
        self.headers_seen_tx
            .take()
            .expect("headers signal unused")
            .send(())
            .expect("test waits for headers");
        Ok(response)
    }
}

#[tokio::test(start_paused = true)]
async fn websocket_blocked_frame_write_has_a_deadline_without_a_session_lifetime_limit() {
    let (client_io, proxy_client) = duplex(1024);
    let (upstream_io, proxy_upstream) = duplex(1024);
    let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;
    let proxy_client = WebSocketStream::from_raw_socket(proxy_client, Role::Server, None).await;
    let proxy_upstream = WebSocketStream::from_raw_socket(proxy_upstream, Role::Client, None).await;
    let config = proxy_core::WebSocketProxyConfig {
        write_timeout: Duration::from_secs(5),
        ..proxy_core::WebSocketProxyConfig::default()
    };
    let proxy = tokio::spawn(proxy_core::proxy_websocket_streams_with_config(
        proxy_client,
        proxy_upstream,
        config,
    ));
    // An inactive but established WebSocket remains valid beyond write timeout.
    tokio::time::advance(Duration::from_secs(30)).await;
    assert!(!proxy.is_finished());
    let writer = tokio::spawn(async move {
        client
            .send(Message::Binary(vec![7; 128 * 1024].into()))
            .await
    });
    writer.await.unwrap().unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(config.write_timeout).await;
    let result = timeout(Duration::from_secs(1), proxy)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(result, TungsteniteError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut)
    );
    drop(upstream);
}

#[tokio::test]
async fn websocket_upstream_handshake_timeout_returns_504_and_releases_permit() {
    let upstream = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let upstream_url = format!("ws://{}", upstream.local_addr().unwrap());
    let (accepted, accepted_rx) = oneshot::channel();
    let app_task = tokio::spawn(async move {
        let (stream, _) = upstream.accept().await.unwrap();
        accepted.send(()).unwrap();
        let _stream = stream;
        std::future::pending::<()>().await;
    });
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let drain = DrainTracker::new(Duration::from_secs(1));
    let proxy = WebSocketProxy::with_config(
        drain.clone(),
        proxy_core::WebSocketProxyConfig {
            handshake_timeout: Duration::from_millis(100),
            ..proxy_core::WebSocketProxyConfig::default()
        },
    );
    let proxy_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        proxy.accept_and_proxy(stream, &upstream_url).await
    });
    let client = tokio::spawn(connect_async(format!("ws://{addr}")));
    accepted_rx.await.unwrap();
    let error = timeout(TEST_TIMEOUT, client)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, TungsteniteError::Http(response) if response.status() == http::StatusCode::GATEWAY_TIMEOUT)
    );
    assert!(proxy_task.await.unwrap().is_err());
    assert_eq!(drain.active_count(), 0);
    app_task.abort();
}

#[tokio::test]
async fn websocket_cancelled_handshake_releases_upstream_connection_and_permit() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let drain = DrainTracker::new(Duration::from_secs(1));
    let proxy = WebSocketProxy::new(drain.clone());
    let task = tokio::spawn(async move {
        proxy
            .connect_accepted_upstream_with_headers(&url, &http::HeaderMap::new())
            .await
    });
    let (mut app, _) = listener.accept().await.unwrap();
    assert_eq!(drain.active_count(), 1);
    task.abort();
    let _ = task.await;
    assert_eq!(drain.active_count(), 0);
    let mut bytes = Vec::new();
    timeout(
        TEST_TIMEOUT,
        tokio::io::AsyncReadExt::read_to_end(&mut app, &mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn websocket_blocked_close_write_uses_close_deadline() {
    let (client_peer, proxy_client) = duplex(1);
    let (upstream_peer, proxy_upstream) = duplex(64);
    let proxy_client = WebSocketStream::from_raw_socket(proxy_client, Role::Server, None).await;
    let proxy_upstream = WebSocketStream::from_raw_socket(proxy_upstream, Role::Client, None).await;
    let mut upstream = WebSocketStream::from_raw_socket(upstream_peer, Role::Server, None).await;
    let config = proxy_core::WebSocketProxyConfig {
        close_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(60),
        ..proxy_core::WebSocketProxyConfig::default()
    };
    let proxy = tokio::spawn(proxy_core::proxy_websocket_streams_with_config(
        proxy_client,
        proxy_upstream,
        config,
    ));
    upstream.send(Message::Close(None)).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(config.close_timeout).await;
    let error = timeout(Duration::from_secs(1), proxy)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, TungsteniteError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut)
    );
    drop(client_peer);
}

#[tokio::test(start_paused = true)]
async fn websocket_close_reply_wait_is_bounded() {
    let (client_peer, proxy_client) = duplex(64);
    let (_upstream_peer, proxy_upstream) = duplex(64);
    let proxy_client = WebSocketStream::from_raw_socket(proxy_client, Role::Server, None).await;
    let proxy_upstream = WebSocketStream::from_raw_socket(proxy_upstream, Role::Client, None).await;
    let mut client = WebSocketStream::from_raw_socket(client_peer, Role::Client, None).await;
    let config = proxy_core::WebSocketProxyConfig {
        close_timeout: Duration::from_secs(5),
        ..proxy_core::WebSocketProxyConfig::default()
    };
    let proxy = tokio::spawn(proxy_core::proxy_websocket_streams_with_config(
        proxy_client,
        proxy_upstream,
        config,
    ));
    client.send(Message::Close(None)).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(config.close_timeout).await;
    assert!(timeout(Duration::from_secs(1), proxy)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
}

#[tokio::test]
async fn websocket_proxy_connects_to_an_ipv6_literal_upstream() {
    let app = TcpListener::bind(("::1", 0))
        .await
        .expect("IPv6 loopback listener");
    let url = format!("ws://{}/socket", app.local_addr().unwrap());
    let app_task = tokio::spawn(async move {
        let (stream, _) = app.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let message = socket.next().await.unwrap().unwrap();
        socket.send(message).await.unwrap();
        let close = socket.next().await.unwrap().unwrap();
        assert!(close.is_close());
        socket.flush().await.unwrap();
    });
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let drain = DrainTracker::new(Duration::from_secs(1));
    let proxy = WebSocketProxy::new(drain.clone());
    let proxy_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        proxy.accept_and_proxy(stream, &url).await
    });
    let (mut client, _) = connect_async(format!("ws://{addr}/socket")).await.unwrap();
    client
        .send(Message::Text("ipv6 echo".into()))
        .await
        .unwrap();
    assert_eq!(
        client.next().await.unwrap().unwrap(),
        Message::Text("ipv6 echo".into())
    );
    client.close(None).await.unwrap();
    let _ = client.next().await;
    proxy_task.await.unwrap().unwrap();
    app_task.await.unwrap();
    assert_eq!(drain.active_count(), 0);
}
