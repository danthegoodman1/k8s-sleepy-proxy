use super::*;
use std::{sync::Arc, time::Duration};

use http_body_util::{Empty, Limited};
use proxy_core::{serve_http_connection, DrainTracker, HttpProxy, Shutdown};
use tokio::{task::JoinSet, time::timeout};

async fn backend(tasks: &mut JoinSet<AppResult<()>>, connections: usize) -> AppResult<SocketAddr> {
    let listener = Arc::new(TcpListener::bind(("127.0.0.1", 0)).await?);
    let address = listener.local_addr()?;
    for _ in 0..connections {
        let listener = listener.clone();
        tasks.spawn(async move {
            let (stream, _) = listener.accept().await?;
            serve_connection(stream).await
        });
    }
    Ok(address)
}

async fn forwarder(
    tasks: &mut JoinSet<AppResult<()>>,
    upstream: SocketAddr,
) -> AppResult<SocketAddr> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let origin: http::Uri = format!("http://{upstream}").parse()?;
    let proxy = Arc::new(HttpProxy::new(DrainTracker::new(Duration::from_secs(1))));
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await?;
        serve_http_connection(stream, Shutdown::new(), move |request| {
            let proxy = proxy.clone();
            let origin = origin.clone();
            async move {
                proxy
                    .proxy(request, &origin)
                    .await
                    .map_err(|error| io::Error::other(format!("production proxy error: {error:?}")))
            }
        })
        .await;
        Ok(())
    });
    Ok(address)
}

async fn h2_requests(
    tasks: &mut JoinSet<AppResult<()>>,
    address: SocketAddr,
    expected_instance: &str,
) -> AppResult<()> {
    let stream = TcpStream::connect(address).await?;
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .max_header_list_size(16 * 1024)
        .handshake(TokioIo::new(stream))
        .await?;
    tasks.spawn(async move {
        connection.await?;
        Ok(())
    });
    // Two requests on this exact sender prove the selected backend handles H2
    // and does not require a request replay or protocol downgrade.
    for path in ["/a/before", "/a/after"] {
        let response = sender
            .send_request(
                Request::builder()
                    .uri(format!("http://fixture.test{path}"))
                    .body(Empty::<Bytes>::new())?,
            )
            .await?;
        if response.status() != StatusCode::OK || response.version() != Version::HTTP_2 {
            return Err("fixture did not return HTTP/2 200".into());
        }
        let body = Limited::new(response.into_body(), 64 * 1024)
            .collect()
            .await?
            .to_bytes();
        let body = std::str::from_utf8(&body)?;
        for marker in [
            "sleepypods-protocol-app".to_owned(),
            format!("instance={expected_instance}"),
            "request_version=HTTP/2".to_owned(),
            format!("path={path}"),
        ] {
            if !body.lines().any(|line| line == marker) {
                return Err(format!("response lacks exact marker {marker:?}").into());
            }
        }
    }
    Ok(())
}

async fn stop(tasks: &mut JoinSet<AppResult<()>>) -> AppResult<()> {
    tasks.abort_all();
    let mut failure = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Err(error) if error.is_cancelled() => {}
            Ok(Err(error)) => failure = Some(error),
            Err(error) => failure = Some(error.into()),
        }
    }
    failure.map_or(Ok(()), Err)
}

#[tokio::test]
async fn h2_backend_and_two_production_hops_preserve_version_instance_and_path() -> AppResult<()> {
    let mut tasks = JoinSet::new();
    let result = timeout(Duration::from_secs(5), async {
        let app = backend(&mut tasks, 2).await?;
        let expected_instance = instance();
        h2_requests(&mut tasks, app, &expected_instance).await?;
        let sidecar = forwarder(&mut tasks, app).await?;
        let frontline = forwarder(&mut tasks, sidecar).await?;
        h2_requests(&mut tasks, frontline, &expected_instance).await
    })
    .await;
    let cleanup = stop(&mut tasks).await;
    result??;
    cleanup
}

#[tokio::test]
async fn websocket_response_preserves_exact_instance_path_and_message() -> AppResult<()> {
    let mut tasks = JoinSet::new();
    let result = timeout(Duration::from_secs(5), async {
        let address = backend(&mut tasks, 1).await?;
        let stream = TcpStream::connect(address).await?;
        let (mut websocket, _) =
            tokio_tungstenite::client_async("ws://fixture.test/b/ws", stream).await?;
        for text in ["before-rotation", "after-rotation"] {
            websocket.send(Message::Text(text.into())).await?;
            let received = websocket.next().await.ok_or("WebSocket closed")??.into_text()?;
            let expected = format!(
                "sleepypods-protocol-app\ninstance={}\nprotocol=websocket\npath=/b/ws\ntext={text}\n",
                instance()
            );
            if received != expected {
                return Err("WebSocket fixture markers differ".into());
            }
        }
        websocket.close(None).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await;
    let cleanup = stop(&mut tasks).await;
    result??;
    cleanup
}
