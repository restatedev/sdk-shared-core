//! The loopback engine: run the embedded relay [`super::receiver`] on its own
//! tokio runtime and bridge each forwarded stream to a local HTTP/2 (h2c)
//! server over a loopback socket.
//!
//! This is the piece that lets an SDK act as a relay *receiver* without
//! reimplementing the protocol and without letting the relay runtime own the
//! SDK's threads. The FFI/native binding only ever drives the control plane —
//! [`Engine::start`], [`Handle::status_json`], [`Handle::stop`]; every
//! forwarded request rides the loopback socket, never the FFI boundary.
//!
//! # Shape
//!
//! ```text
//!  relay :8080                         host process
//! ┌────────┐  h2 (role-flip)  ┌──────────────────────────────────────────┐
//! │ relay  │◄────────────────►│  Receiver + LoopbackHandler (this crate)  │
//! └────────┘                  │     owns its OWN tokio runtime            │
//!                             │            │ h2c dial per stream          │
//!                             │            ▼                              │
//!                             │   HTTP/2 server on 127.0.0.1:<local_port> │
//!                             └──────────────────────────────────────────┘
//! ```
//!
//! The `LoopbackHandler` replays the sender's method + **tail-only `:path`**
//! (so the request-identity signature, which covers only the SDK-relative
//! path, stays valid) + headers verbatim, then bridges both bodies with the
//! in-crate [`super::bridge`].

use super::bridge::{self, BridgeConfig};
use super::protocol::{Env, EnvError, H2Tuning, Tunnel, TunnelError};
use super::receiver::{Invocation, InvokeHandler, Receiver, ReceiverConfig, TlsClient};
use bytes::Bytes;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tracing::{debug, warn};

/// Tunnel configuration, as it crosses a native binding (JSON) or a Rust
/// caller. Deliberately version-tolerant (extra fields ignored, most fields
/// optional) so a binding can add knobs without breaking the wire shape.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// `host:port` of the relay's receiver port (`:8080`) or the LB in front
    /// of it. A single name that resolves to many A-records fans out across
    /// nodes (multi-homing); see [`connections`](Config::connections).
    pub relay_addr: String,
    /// The env (routing namespace) this receiver registers under.
    pub env: String,
    /// The tunnel this receiver registers under (scoped by `env`).
    pub tunnel: String,
    /// The API key presented in the `/whoami` body.
    pub api_key: String,
    /// The local HTTP/2 (h2c) server port to bridge forwarded requests to —
    /// the SDK's own server, listening on `127.0.0.1:<local_port>`.
    pub local_port: u16,
    /// Optional client TLS to the relay (PEM material). Usually absent —
    /// receiver TLS is normally LB-terminated so the relay sees plain TCP.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// R4 multi-homing: how many concurrent connection slots to fan out across
    /// the relay nodes `relay_addr` resolves to. `None` = one per resolved node.
    #[serde(default)]
    pub connections: Option<usize>,
    /// R5 receiver-instance id, held across reconnects so the broker treats
    /// this receiver's connections as one logical receiver for affinity.
    /// `None` = auto-generate an ephemeral, process-unique id.
    #[serde(default)]
    pub instance_id: Option<String>,
    /// Delay between a connection ending and the next redial, in milliseconds.
    /// `None` = 1000ms.
    #[serde(default)]
    pub reconnect_backoff_millis: Option<u64>,
    /// tokio worker-thread count for the embedded runtime. Bridging is
    /// I/O-bound, so a small pool is plenty and avoids spawning one thread per
    /// core inside the host process. `None` = 2.
    #[serde(default)]
    pub worker_threads: Option<usize>,
}

/// Client-TLS material for the receiver→relay hop (PEM). See
/// [`TlsClient::from_pem`].
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    /// CA cert(s) (PEM) verifying the relay's server cert.
    pub ca_pem: String,
    /// DNS name to verify on the relay's cert (SNI).
    pub server_name: String,
    /// Optional client-cert PEM for mTLS (rare on `:8080`).
    #[serde(default)]
    pub client_cert_pem: Option<String>,
    /// Optional client-key PEM for mTLS. Required iff `client_cert_pem` is set.
    #[serde(default)]
    pub client_key_pem: Option<String>,
}

impl TlsConfig {
    fn build(&self) -> Result<TlsClient, ConfigError> {
        let identity = match (&self.client_cert_pem, &self.client_key_pem) {
            (Some(cert), Some(key)) => Some((cert.as_bytes(), key.as_bytes())),
            (None, None) => None,
            _ => {
                return Err(ConfigError::Tls(
                    "client_cert_pem and client_key_pem must be set together".into(),
                ))
            }
        };
        TlsClient::from_pem(self.ca_pem.as_bytes(), identity, &self.server_name)
            .map_err(|e| ConfigError::Tls(e.to_string()))
    }
}

/// Error building a running [`Handle`] from a [`Config`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid env: {0}")]
    Env(#[from] EnvError),
    #[error("invalid tunnel: {0}")]
    Tunnel(#[from] TunnelError),
    #[error("tls: {0}")]
    Tls(String),
    #[error("failed to build tokio runtime: {0}")]
    Runtime(String),
}

impl Config {
    /// Turn the wire config into a [`ReceiverConfig`], validating the env,
    /// tunnel, and TLS material.
    fn into_receiver_config(self) -> Result<ReceiverConfig, ConfigError> {
        let env = Env::new(self.env)?;
        let tunnel = Tunnel::new(self.tunnel)?;
        let tls = match &self.tls {
            Some(t) => Some(t.build()?),
            None => None,
        };
        let mut rc = ReceiverConfig::new(self.relay_addr, env, tunnel, self.api_key);
        rc.tls = tls;
        rc.connections = self.connections;
        if let Some(id) = self.instance_id {
            // An explicit id overrides the auto-generated one; an empty string
            // is treated as "opt out of instance affinity".
            rc.instance_id = if id.is_empty() { None } else { Some(id) };
        }
        if let Some(ms) = self.reconnect_backoff_millis {
            rc.reconnect_backoff = Duration::from_millis(ms);
        }
        Ok(rc)
    }
}

/// Live status of a running tunnel, readable at any time via
/// [`Handle::status`] / [`Handle::status_json`].
#[derive(Debug)]
pub struct Status {
    running: AtomicBool,
    last_error: Mutex<Option<String>>,
}

impl Status {
    fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            last_error: Mutex::new(None),
        }
    }

    fn set_running(&self, v: bool) {
        self.running.store(v, Ordering::SeqCst);
    }

    /// Whether the receiver task is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// The last error observed by the engine, if any.
    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .expect("status mutex poisoned")
            .clone()
    }

    /// Serialise to the JSON shape the native bindings return:
    /// `{"running": bool, "last_error": string|null}`.
    pub fn to_json(&self) -> String {
        serde_json::json!({
            "running": self.is_running(),
            "last_error": self.last_error(),
        })
        .to_string()
    }
}

/// The tunnel engine. Zero-sized; the running state lives in the [`Handle`]
/// that [`start`](Engine::start) returns.
pub struct Engine;

impl Engine {
    /// Build a tokio runtime, spawn the embedded [`Receiver`] driving a
    /// `LoopbackHandler`, and return a live [`Handle`]. Returns immediately;
    /// the receiver dials the relay in the background.
    pub fn start(config: Config) -> Result<Handle, ConfigError> {
        let local_port = config.local_port;
        let worker_threads = config.worker_threads.unwrap_or(2).max(1);
        let receiver_config = config.into_receiver_config()?;
        let h2_tuning = receiver_config.h2_tuning.clone();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .thread_name("relay-tunnel")
            .build()
            .map_err(|e| ConfigError::Runtime(e.to_string()))?;

        let status = Arc::new(Status::new());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let handler = Arc::new(LoopbackHandler {
            local_port,
            h2_tuning,
        });
        let status_for_task = status.clone();
        runtime.spawn(async move {
            Receiver::new(receiver_config, handler)
                .run(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
            status_for_task.set_running(false);
        });
        status.set_running(true);

        Ok(Handle {
            runtime: Some(runtime),
            shutdown: Some(shutdown_tx),
            status,
        })
    }
}

/// A running tunnel. Dropping it (or calling [`stop`](Handle::stop)) signals a
/// graceful shutdown and joins the runtime.
pub struct Handle {
    runtime: Option<tokio::runtime::Runtime>,
    shutdown: Option<oneshot::Sender<()>>,
    status: Arc<Status>,
}

impl Handle {
    /// Read the shared [`Status`].
    pub fn status(&self) -> &Arc<Status> {
        &self.status
    }

    /// Read the status as the JSON the native bindings expose.
    pub fn status_json(&self) -> String {
        self.status.to_json()
    }

    /// Signal a graceful shutdown and join the runtime (bounded). Idempotent.
    ///
    /// Must be called from a non-async context (it blocks while the runtime
    /// drains) — the native bindings call it from a host control thread. It is
    /// never called from within the tunnel's own runtime.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_timeout(Duration::from_secs(5));
        }
        self.status.set_running(false);
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The [`InvokeHandler`] that bridges each forwarded relay stream to the local
/// HTTP/2 server.
struct LoopbackHandler {
    local_port: u16,
    h2_tuning: H2Tuning,
}

#[async_trait::async_trait]
impl InvokeHandler for LoopbackHandler {
    async fn handle(&self, invocation: Invocation) {
        self.serve(invocation).await;
    }
}

impl LoopbackHandler {
    async fn serve(&self, invocation: Invocation) {
        let Invocation {
            target,
            method,
            headers,
            body,
            mut respond,
        } = invocation;

        // Dial the local h2c server. A failure here is ours to surface: reply
        // 502 so the sender sees a clean status rather than a bare stream reset.
        let tcp = match TcpStream::connect(("127.0.0.1", self.local_port)).await {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    port = self.local_port,
                    ?e,
                    "loopback: connect to local server failed"
                );
                reply_status(&mut respond, http::StatusCode::BAD_GATEWAY);
                return;
            }
        };
        let _ = tcp.set_nodelay(true);

        let mut builder = h2::client::Builder::new();
        self.h2_tuning.apply_client(&mut builder);
        let (h2, connection) = match builder.handshake::<_, Bytes>(tcp).await {
            Ok(pair) => pair,
            Err(e) => {
                warn!(?e, "loopback: h2 handshake to local server failed");
                reply_status(&mut respond, http::StatusCode::BAD_GATEWAY);
                return;
            }
        };
        // Drive the local connection in the background.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                debug!(?e, "loopback: local h2 connection ended");
            }
        });

        // Replay method + tail-only `:path` (preserves the request-identity
        // signature) + headers verbatim onto an absolute URI so h2 derives the
        // `:scheme` / `:authority` / `:path` pseudo-headers.
        let uri = format!("http://127.0.0.1:{}{}", self.local_port, target.tail);
        let mut req_builder = http::Request::builder().method(method).uri(uri);
        if let Some(hs) = req_builder.headers_mut() {
            *hs = headers;
        }
        let request = match req_builder.body(()) {
            Ok(r) => r,
            Err(e) => {
                warn!(?e, "loopback: building forwarded request failed");
                reply_status(&mut respond, http::StatusCode::BAD_GATEWAY);
                return;
            }
        };

        let mut h2 = match h2.ready().await {
            Ok(h) => h,
            Err(e) => {
                warn!(?e, "loopback: local h2 client not ready");
                reply_status(&mut respond, http::StatusCode::BAD_GATEWAY);
                return;
            }
        };
        // `end_of_stream = false`: the request body (if any) follows via the
        // send stream; the upstream pump closes it (END_STREAM / trailers).
        let (response_fut, send_stream) = match h2.send_request(request, false) {
            Ok(pair) => pair,
            Err(e) => {
                warn!(?e, "loopback: send_request to local server failed");
                reply_status(&mut respond, http::StatusCode::BAD_GATEWAY);
                return;
            }
        };

        // Bridge both directions until they terminate. The relay-facing stream
        // plays the bridge's "sender side" (we are the h2 server on the
        // reversed connection); the local dial plays the "receiver side".
        let cfg = BridgeConfig::default();
        let up = bridge::bridge_upstream(body, send_stream, &cfg);
        let down = bridge::bridge_downstream(response_fut, &mut respond, &cfg);
        let (up_res, down_res) = bridge::run_duplex(up, down, &cfg).await;
        if let Err(e) = up_res {
            debug!(?e, "loopback: upstream bridge ended with error");
        }
        if let Err(e) = down_res {
            debug!(?e, "loopback: downstream bridge ended with error");
        }
    }
}

/// Send an empty-body response with the given status back to the relay.
fn reply_status(respond: &mut h2::server::SendResponse<Bytes>, status: http::StatusCode) {
    let response = http::Response::builder()
        .status(status)
        .body(())
        .expect("status response builds");
    if let Ok(mut send) = respond.send_response(response, false) {
        let _ = send.send_data(Bytes::new(), true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parses_minimal_json() {
        let json = r#"{
            "relay_addr": "127.0.0.1:8080",
            "env": "e1",
            "tunnel": "c1",
            "api_key": "k",
            "local_port": 9080
        }"#;
        let cfg: Config = serde_json::from_str(json).expect("parses");
        assert_eq!(cfg.local_port, 9080);
        assert!(cfg.connections.is_none());
        let rc = cfg.into_receiver_config().expect("builds");
        assert_eq!(rc.env.as_str(), "e1");
        assert_eq!(rc.tunnel.as_str(), "c1");
        // An instance_id is auto-generated when not supplied.
        assert!(rc.instance_id.is_some());
    }

    #[test]
    fn config_rejects_bad_env() {
        let json = r#"{"relay_addr":"h:8080","env":"has-dash","tunnel":"c1","api_key":"k","local_port":1}"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert!(matches!(
            cfg.into_receiver_config(),
            Err(ConfigError::Env(_))
        ));
    }

    #[test]
    fn config_honours_explicit_and_empty_instance_id() {
        let with_id: Config = serde_json::from_str(
            r#"{"relay_addr":"h:8080","env":"e1","tunnel":"c1","api_key":"k","local_port":1,"instance_id":"fixed"}"#,
        )
        .unwrap();
        assert_eq!(
            with_id
                .into_receiver_config()
                .unwrap()
                .instance_id
                .as_deref(),
            Some("fixed")
        );

        let empty_id: Config = serde_json::from_str(
            r#"{"relay_addr":"h:8080","env":"e1","tunnel":"c1","api_key":"k","local_port":1,"instance_id":""}"#,
        )
        .unwrap();
        assert_eq!(empty_id.into_receiver_config().unwrap().instance_id, None);
    }

    #[test]
    fn status_json_shape() {
        let s = Status::new();
        let v: serde_json::Value = serde_json::from_str(&s.to_json()).unwrap();
        assert_eq!(v["running"], false);
        assert!(v["last_error"].is_null());

        s.set_running(true);
        let v: serde_json::Value = serde_json::from_str(&s.to_json()).unwrap();
        assert_eq!(v["running"], true);
    }
}
