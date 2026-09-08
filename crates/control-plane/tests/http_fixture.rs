#[path = "support/http_once.rs"]
mod http_once;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_request(socket: &mut tokio::net::TcpStream) {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut buffer = [0; 1024];
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        request.extend_from_slice(&buffer[..count]);
        assert!(request.len() <= 1024);
    }
    assert!(request.starts_with(b"GET /proof HTTP/1.1\r\n"));
}

#[tokio::test]
async fn framing_rejects_truncated_oversized_body_and_oversized_headers_without_retry() {
    let cases = [
        b"HTTP/1.0 200 OK\r\nContent-Length: 8\r\n\r\nshort".to_vec(),
        format!(
            "HTTP/1.0 200 OK\r\nContent-Length: 65537\r\n\r\n{}",
            "x".repeat(65537)
        )
        .into_bytes(),
        format!(
            "HTTP/1.0 200 OK\r\nX-Large: {}\r\nContent-Length: 0\r\n\r\n",
            "x".repeat(17 * 1024)
        )
        .into_bytes(),
    ];
    for (case, response) in cases.into_iter().enumerate() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            let _ = socket.write_all(&response).await;
            socket.shutdown().await.unwrap();
            let mut remaining = Vec::new();
            let closed =
                tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut remaining))
                    .await
                    .expect("failed response must close the owned connection");
            assert!(
                closed.is_ok()
                    || closed
                        .is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionReset)
            );
            assert!(remaining.is_empty());
            assert!(
                tokio::time::timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err()
            );
        });
        assert!(
            http_once::get_once(address, "proof.test", "/proof", Duration::from_millis(500))
                .await
                .is_err(),
            "case {case} must reject invalid framing or size"
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn stalled_headers_and_body_close_at_total_deadline() {
    for partial_response in [
        b"".as_slice(),
        b"HTTP/1.0 200 OK\r\nContent-Length: 8\r\n\r\nshort".as_slice(),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket.write_all(partial_response).await.unwrap();
            let mut remaining = Vec::new();
            tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut remaining))
                .await
                .expect("deadline must close socket")
                .unwrap();
            assert!(remaining.is_empty());
        });
        let result =
            http_once::get_once(address, "proof.test", "/proof", Duration::from_millis(100)).await;
        assert!(result.unwrap_err().to_string().contains("exceeded"));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn external_cancellation_during_body_closes_owned_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket
            .write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 8\r\n\r\nshort")
            .await
            .unwrap();
        sent.send(()).unwrap();
        let mut remaining = Vec::new();
        let closed =
            tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut remaining))
                .await
                .expect("cancellation must close socket");
        assert!(
            closed.is_ok()
                || closed.is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionReset)
        );
        assert!(remaining.is_empty());
    });
    let request = tokio::spawn(http_once::get_once(
        address,
        "proof.test",
        "/proof",
        Duration::from_secs(130),
    ));
    tokio::time::timeout(Duration::from_secs(1), received)
        .await
        .unwrap()
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    server.await.unwrap();
}
