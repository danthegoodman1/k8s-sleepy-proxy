use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use proxy_core::{DrainError, DrainTracker, WebSocketProxy, WebSocketProxyError};
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::{protocol::CloseFrame, Bytes as WsBytes, Message},
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

    let (mut client, _) = connect_async(format!("ws://{proxy_addr}"))
        .await
        .expect("client websocket handshake with proxy succeeds");
    let disconnect = timeout(TEST_TIMEOUT, client.next())
        .await
        .expect("client observes failed upstream upgrade before timeout");
    assert!(
        disconnect.is_none() || matches!(disconnect, Some(Err(_))),
        "client should observe EOF or a websocket error after upstream upgrade failure"
    );

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

    let upstream_task = tokio::spawn(async move {
        let (stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");
        let websocket = accept_async(stream)
            .await
            .expect("upstream accepts websocket");
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
