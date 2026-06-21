use std::time::Duration;

use proxy_core::{DrainTracker, TcpProxy, TcpProxyConfig, TcpProxyStats};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const REQUEST: &[u8] = b"preserve these bytes across the proxy";

#[tokio::test]
async fn tcp_proxy_preserves_bytes_and_tracks_connection_lifecycle() {
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("upstream listener binds");
    let upstream_addr = upstream_listener
        .local_addr()
        .expect("upstream listener has address");

    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream_listener
            .accept()
            .await
            .expect("upstream accepts proxy connection");

        let mut received = vec![0; REQUEST.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("upstream reads proxied bytes");
        assert_eq!(received, REQUEST);

        stream
            .write_all(&received)
            .await
            .expect("upstream writes response bytes");

        let mut eof = [0; 1];
        assert_eq!(stream.read(&mut eof).await.expect("upstream reads eof"), 0);
    });

    let proxy_listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener binds");
    let proxy_addr = proxy_listener
        .local_addr()
        .expect("proxy listener has address");

    let drain = DrainTracker::new(Duration::from_secs(5));
    let proxy = TcpProxy::new(
        drain.clone(),
        TcpProxyConfig {
            connect_timeout: Duration::from_secs(1),
        },
    );

    let proxy_task = tokio::spawn(async move {
        let (client, _) = proxy_listener
            .accept()
            .await
            .expect("proxy accepts client connection");

        proxy.proxy(client, upstream_addr).await
    });

    let mut client = TcpStream::connect(proxy_addr)
        .await
        .expect("client connects to proxy");

    client
        .write_all(REQUEST)
        .await
        .expect("client writes request bytes");

    drain.wait_for_active_count(1).await;
    assert_eq!(drain.active_count(), 1);

    let mut echoed = vec![0; REQUEST.len()];
    client
        .read_exact(&mut echoed)
        .await
        .expect("client reads echoed bytes");
    assert_eq!(echoed, REQUEST);

    client.shutdown().await.expect("client half-closes");

    let stats = proxy_task
        .await
        .expect("proxy task completed")
        .expect("proxy completed successfully");

    assert_eq!(
        stats,
        TcpProxyStats {
            client_to_upstream: REQUEST.len() as u64,
            upstream_to_client: REQUEST.len() as u64,
        }
    );

    upstream_task.await.expect("upstream task completed");
    drain.wait_for_active_count(0).await;
    assert_eq!(drain.active_count(), 0);
}
