use std::{error::Error, net::SocketAddr, time::Duration};

use http_body_util::{BodyExt, Empty, Limited};

// One HTTP/1 connection and one send: no reconnecting client or request replay.
// A complete framed response does not depend on port-forward delivering EOF.
// JoinSet abort-on-drop owns the socket even when an outer fixture timeout or
// caller cancellation drops this future; normal exits explicitly abort and join.
pub async fn get_once(
    addr: SocketAddr,
    host: &str,
    path: &str,
    timeout: Duration,
) -> Result<http::Response<String>, Box<dyn Error + Send + Sync>> {
    let mut connections = tokio::task::JoinSet::new();
    let result = tokio::time::timeout(timeout, async {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let (mut sender, driver) = hyper::client::conn::http1::Builder::new()
            .max_buf_size(16 * 1024)
            .handshake(hyper_util::rt::TokioIo::new(stream))
            .await?;
        connections.spawn(driver);
        let request = http::Request::builder()
            .uri(path)
            .header(http::header::HOST, host)
            .header(http::header::CONNECTION, "close")
            .body(Empty::<bytes::Bytes>::new())?;
        let response = sender.send_request(request).await?;
        // Hyper's read-buffer setting can accept a complete header block that
        // arrives in one oversized read. Also bound decoded header fields.
        let header_bytes: usize = response
            .headers()
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len() + 4)
            .sum();
        if header_bytes > 16 * 1024 {
            return Err("HTTP fixture response headers exceed 16 KiB".into());
        }
        let (parts, body) = response.into_parts();
        let body = Limited::new(body, 64 * 1024).collect().await?.to_bytes();
        Ok::<_, Box<dyn Error + Send + Sync>>(http::Response::from_parts(
            parts,
            String::from_utf8(body.to_vec())?,
        ))
    })
    .await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    result.map_err(|_| format!("single HTTP request to {addr} for {path} exceeded {timeout:?}"))?
}
