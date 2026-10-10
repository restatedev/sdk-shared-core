//! End-to-end test of the embedded relay-tunnel loopback engine.
//!
//! Compiled only under the `tunnel` feature (the whole file is gated, so a
//! default `cargo test` sees an empty test binary). Run with:
//! `cargo test --features tunnel --test relay_loopback`.
//!
//! The test stands up two fakes and the real engine between them:
//!   * a **fake relay** — an h2 *client* (role-flip) that the receiver dials,
//!     asks `/whoami`, then opens a forwarded `/http/host/port/tail` stream on;
//!   * a **fake local server** — an h2 *server* on `127.0.0.1:<port>` playing
//!     the SDK's own HTTP/2 server, which echoes the tail.
//!
//! It asserts the forwarded request round-trips relay → engine → loopback →
//! local server → back, with the tail-only `:path` preserved and the body
//! delivered.
#![cfg(feature = "tunnel")]

use bytes::Bytes;
use restate_sdk_shared_core::relay::{Config, Engine};
use std::sync::Arc;
use tokio::net::TcpListener;

/// A minimal fake local HTTP/2 (h2c) server: accept one connection, serve
/// streams by echoing `served <path>` and draining the request body. Returns
/// the (host, port, body) it observed on the first stream via `tx`.
async fn spawn_fake_local_server(
    listener: TcpListener,
    tx: tokio::sync::mpsc::UnboundedSender<(String, Vec<u8>)>,
) {
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("local server accept");
        let mut conn = h2::server::handshake(socket)
            .await
            .expect("local h2 handshake");
        while let Some(accepted) = conn.accept().await {
            let (request, mut respond) = match accepted {
                Ok(pair) => pair,
                Err(_) => break,
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                // Full path + query (the `:path` pseudo-header verbatim), so
                // the test can confirm the query survived the tail-only replay.
                let path = request
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.as_str().to_string())
                    .unwrap_or_else(|| request.uri().path().to_string());
                let mut body = request.into_body();
                let mut got = Vec::new();
                while let Some(chunk) = body.data().await {
                    let chunk = chunk.expect("local body chunk");
                    let _ = body.flow_control().release_capacity(chunk.len());
                    got.extend_from_slice(&chunk);
                }
                let _ = tx.send((path.clone(), got));

                let response = http::Response::builder()
                    .status(http::StatusCode::OK)
                    .body(())
                    .unwrap();
                let mut send = respond.send_response(response, false).unwrap();
                let _ = send.send_data(Bytes::from(format!("served {path}")), true);
            });
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_request_round_trips_through_loopback() {
    // ── Fake local server (the SDK's own h2c server) ──────────────────────
    let local_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_port = local_listener.local_addr().unwrap().port();
    let (obs_tx, mut obs_rx) = tokio::sync::mpsc::unbounded_channel();
    spawn_fake_local_server(local_listener, obs_tx).await;

    // ── Fake relay (an h2 client, per the role-flip) ──────────────────────
    let relay_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_listener.local_addr().unwrap();

    // ── Start the engine (its own runtime) pointed at both ────────────────
    let config: Config = serde_json::from_value(serde_json::json!({
        "relay_addr": relay_addr.to_string(),
        "env": "e1",
        "tunnel": "c1",
        "api_key": "api-key",
        "local_port": local_port,
        "reconnect_backoff_millis": 50,
    }))
    .unwrap();
    let handle = Arc::new(std::sync::Mutex::new(Some(
        Engine::start(config).expect("engine starts"),
    )));
    assert!(handle
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .status()
        .is_running());

    // The receiver dials us; accept and drive it as the relay (h2 client).
    let (socket, _) =
        tokio::time::timeout(std::time::Duration::from_secs(5), relay_listener.accept())
            .await
            .expect("receiver should dial the fake relay")
            .unwrap();
    let (send_request, connection) = h2::client::handshake(socket).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // /whoami first (the relay learns our identity).
    let mut sr = send_request.ready().await.unwrap();
    let (resp, _) = sr
        .send_request(
            http::Request::builder()
                .method(http::Method::GET)
                .uri("/whoami")
                .body(())
                .unwrap(),
            true,
        )
        .unwrap();
    let response = resp.await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    let mut wbody = response.into_body();
    let mut buf = Vec::new();
    while let Some(c) = wbody.data().await {
        let c = c.unwrap();
        wbody.flow_control().release_capacity(c.len()).unwrap();
        buf.extend_from_slice(&c);
    }
    let json: serde_json::Value = serde_json::from_slice(&buf).unwrap();
    assert_eq!(json["tunnel"], "c1");
    assert_eq!(json["env"], "e1");
    assert_eq!(json["api_key"], "api-key");

    // Forwarded invocation with a body — should be bridged to the local server.
    let mut sr = sr.ready().await.unwrap();
    let (resp2, mut req_body) = sr
        .send_request(
            http::Request::builder()
                .method(http::Method::POST)
                .uri("/http/127.0.0.1/9999/orders?x=1")
                .body(())
                .unwrap(),
            false,
        )
        .unwrap();
    req_body
        .send_data(Bytes::from_static(b"hello"), true)
        .unwrap();

    let response2 = resp2.await.unwrap();
    assert_eq!(response2.status(), http::StatusCode::OK);
    let mut rbody = response2.into_body();
    let mut buf2 = Vec::new();
    while let Some(c) = rbody.data().await {
        let c = c.unwrap();
        rbody.flow_control().release_capacity(c.len()).unwrap();
        buf2.extend_from_slice(&c);
    }
    // The local server echoed the tail-only path — proving `:path` was the tail,
    // not the full `/e1/c1/http/...` sender path.
    assert_eq!(buf2, b"served /orders?x=1");

    // The local server observed the forwarded tail + body.
    let (path, body) = tokio::time::timeout(std::time::Duration::from_secs(5), obs_rx.recv())
        .await
        .expect("local server should have been invoked")
        .expect("channel open");
    assert_eq!(path, "/orders?x=1");
    assert_eq!(body, b"hello");

    // Stop the engine from a blocking context (it drops a tokio runtime).
    tokio::task::spawn_blocking(move || {
        drop(handle.lock().unwrap().take());
    })
    .await
    .unwrap();
}
