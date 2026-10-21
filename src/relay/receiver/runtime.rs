//! The relay-facing receiver runtime: dial the relay, accept the h2
//! role-flip (we are the *server*), answer `/whoami`, and dispatch each
//! forwarded stream to the [`InvokeHandler`]. Redials on connection loss.

use super::handler::{Invocation, InvokeHandler};
use crate::relay::protocol::{Env, H2Tuning, Tunnel, WhoamiBody};
use bytes::Bytes;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

/// Client TLS for the receiver→relay (or receiver→LB) hop. Usually unset —
/// receiver TLS is typically terminated at the LB (design §10.2) so the
/// relay sees plain TCP — but supported for direct-to-relay deployments.
#[derive(Clone)]
pub struct TlsClient {
    pub connector: TlsConnector,
    pub server_name: rustls_pki_types::ServerName<'static>,
}

/// Error building a [`TlsClient`] from PEM material.
#[derive(Debug, thiserror::Error)]
#[error("TLS config: {0}")]
pub struct TlsConfigError(String);

impl TlsClient {
    /// Build receiver→relay client TLS from PEM bytes — the ergonomic
    /// constructor so a caller doesn't have to hand-assemble a
    /// `rustls::ClientConfig` + `TlsConnector`.
    ///
    /// * `ca_pem` — CA cert(s) verifying the relay's **server** cert.
    /// * `client_identity` — `Some((cert_pem, key_pem))` presents a client cert
    ///   (rare on `:8080` — receiver auth is the `/whoami` API key, design
    ///   §10.2); `None` = server-only TLS, the usual case.
    /// * `server_name` — DNS name to verify on the relay's cert.
    ///
    /// Receiver→relay TLS is typically terminated at the LB (so the relay sees
    /// plain TCP and no `TlsClient` is needed); this is for direct-to-relay
    /// deployments. PEM parsing uses `rustls-pki-types`' `PemObject`; the ring
    /// crypto provider is installed once (idempotent).
    pub fn from_pem(
        ca_pem: &[u8],
        client_identity: Option<(&[u8], &[u8])>,
        server_name: &str,
    ) -> Result<Self, TlsConfigError> {
        use rustls_pki_types::pem::PemObject;
        use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
        use std::sync::Arc;
        use tokio_rustls::rustls::{ClientConfig, RootCertStore};

        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

        let mut roots = RootCertStore::empty();
        let mut added = 0usize;
        for ca in CertificateDer::pem_slice_iter(ca_pem) {
            let ca = ca.map_err(|e| TlsConfigError(format!("CA PEM: {e}")))?;
            roots
                .add(ca)
                .map_err(|e| TlsConfigError(format!("add CA: {e}")))?;
            added += 1;
        }
        if added == 0 {
            return Err(TlsConfigError("no CERTIFICATE block in ca_pem".into()));
        }

        let builder = ClientConfig::builder().with_root_certificates(roots);
        let mut config = match client_identity {
            Some((cert_pem, key_pem)) => {
                let certs = CertificateDer::pem_slice_iter(cert_pem)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| TlsConfigError(format!("client cert PEM: {e}")))?;
                let key = PrivateKeyDer::from_pem_slice(key_pem)
                    .map_err(|e| TlsConfigError(format!("client key PEM: {e}")))?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| TlsConfigError(format!("client auth cert: {e}")))?
            }
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols = vec![b"h2".to_vec()];

        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|e| TlsConfigError(format!("server name {server_name:?}: {e}")))?;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }
}

/// Receiver configuration.
pub struct ReceiverConfig {
    /// `host:port` of the relay's receiver port (`:8080`) or the LB in front
    /// of it.
    pub relay_addr: String,
    /// The env (routing namespace) this receiver registers under. Reported in
    /// `/whoami`. A tunnel is only meaningful within its env, so the broker
    /// keys membership on the `(env, tunnel)` pair.
    pub env: Env,
    /// The tunnel this receiver registers under (scoped by `env`).
    pub tunnel: Tunnel,
    /// The API key presented in the `/whoami` body.
    pub api_key: String,
    /// Optional client TLS. `None` = plain TCP.
    pub tls: Option<TlsClient>,
    /// h2 tuning for our server side of the reversed connection.
    pub h2_tuning: H2Tuning,
    /// Delay between a connection ending and the next redial.
    pub reconnect_backoff: Duration,
    /// Ephemeral receiver-instance id sent in `/whoami` (see
    /// [`crate::relay::protocol::WhoamiBody::instance_id`]). Generated once per
    /// process by [`ReceiverConfig::new`] and held across reconnections so
    /// the broker treats this receiver's connections as one logical receiver
    /// for affinity. `None` opts out (legacy per-connection affinity).
    pub instance_id: Option<String>,
    /// How many concurrent connections to fan out across the relay nodes that
    /// `relay_addr` resolves to (multi-homing — R4 in the reconnection
    /// contract). `None` (the default) opens **one connection per resolved
    /// node** (K=N) — every relay node gets a local owner for this receiver,
    /// so affinity traffic is served locally wherever the sender lands. `Some(k)`
    /// pins exactly `k` connection slots; when `k` exceeds the number of
    /// distinct resolved nodes the surplus slots double up on nodes
    /// (best-effort spread). Always at least 1. All slots share the one
    /// [`instance_id`](ReceiverConfig::instance_id), so the broker collapses
    /// them into a single affinity bucket — matching the native Node
    /// `connect()` client.
    pub connections: Option<usize>,
}

/// Generate an ephemeral, process-unique receiver-instance id. Uses the
/// OS-seeded entropy behind `RandomState` (the same source `HashMap` uses for
/// its DoS-resistance seed) so we get ~128 bits of randomness with **no new
/// dependency** — deliberately avoiding a `uuid`/`rand` pull-in for one id.
/// Ephemeral by design: a fresh id per process means a restarted receiver is
/// a new instance and request affinity re-pins, which is the intended
/// behaviour (a restarted process is genuinely new).
pub fn generate_instance_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let chunk = || {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u8(0xA5);
        h.finish()
    };
    format!("{:016x}{:016x}", chunk(), chunk())
}

impl ReceiverConfig {
    /// Construct with sensible defaults (plain TCP, recommended h2 tuning,
    /// 1s reconnect backoff, a freshly-generated ephemeral `instance_id`).
    pub fn new(
        relay_addr: impl Into<String>,
        env: Env,
        tunnel: Tunnel,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            relay_addr: relay_addr.into(),
            env,
            tunnel,
            api_key: api_key.into(),
            tls: None,
            h2_tuning: H2Tuning::default(),
            reconnect_backoff: Duration::from_secs(1),
            instance_id: Some(generate_instance_id()),
            connections: None,
        }
    }
}

/// Errors that end a single relay connection (the run loop redials after).
#[derive(Debug, thiserror::Error)]
pub enum ConnError {
    #[error("connect failed: {0}")]
    Connect(#[from] std::io::Error),
    #[error("h2 server handshake / connection error: {0}")]
    H2(#[from] h2::Error),
}

/// A configured receiver: the runtime plus the handler it dispatches to.
pub struct Receiver {
    config: ReceiverConfig,
    handler: Arc<dyn InvokeHandler>,
}

impl Receiver {
    pub fn new(config: ReceiverConfig, handler: Arc<dyn InvokeHandler>) -> Self {
        Self { config, handler }
    }

    /// Run until `shutdown` resolves. Resolves `relay_addr` to its relay nodes
    /// and fans out **K connection slots** (multi-homing — R4): each slot
    /// repeatedly dials a node, serves the reversed connection until it ends,
    /// then redials after the backoff, re-resolving DNS each time so the slot
    /// follows node churn. `shutdown` firing cancels every in-flight
    /// connection and ends all slots.
    ///
    /// K is [`ReceiverConfig::connections`] when set, else the number of nodes
    /// `relay_addr` resolves to at startup (one connection per node). All slots
    /// share the one `/whoami` body — including the `instance_id` — so the
    /// broker treats them as a single logical receiver for affinity.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) {
        let Receiver { config, handler } = self;
        // Precompute the /whoami body once — it never changes for a given
        // receiver identity, and every slot presents the same one (shared
        // instance_id → one affinity bucket across all K connections).
        let whoami = Arc::new(
            WhoamiBody {
                env: config.env.clone(),
                api_key: config.api_key.clone(),
                tunnel: config.tunnel.clone(),
                instance_id: config.instance_id.clone(),
            }
            .to_json(),
        );
        let config = Arc::new(config);

        // Decide the slot count from an initial resolution. `connections`
        // overrides; otherwise one slot per resolved node (K=N). If resolution
        // fails right now we still start a single slot, which keeps retrying.
        let initial = resolve(&config.relay_addr).await;
        let slot_count = config.connections.unwrap_or(initial.len()).max(1);

        info!(
            relay = %config.relay_addr,
            tunnel = %config.tunnel,
            resolved = initial.len(),
            slots = slot_count,
            "receiver: starting"
        );

        // Shared record of which resolved addresses currently have a live
        // connection, so each slot can prefer a node no sibling already holds.
        let live = Arc::new(LiveTargets::default());

        // One watch channel broadcasts shutdown to every slot; the driver
        // (this task) owns the `shutdown` future and flips it.
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut slots = Vec::with_capacity(slot_count);
        for idx in 0..slot_count {
            let config = config.clone();
            let handler = handler.clone();
            let whoami = whoami.clone();
            let live = live.clone();
            let stop_rx = stop_rx.clone();
            slots.push(tokio::spawn(async move {
                slot_loop(idx, config, handler, whoami, live, stop_rx).await;
            }));
        }
        drop(stop_rx);

        shutdown.await;
        let _ = stop_tx.send(true);
        for slot in slots {
            let _ = slot.await;
        }
        info!("receiver: shutdown");
    }
}

/// One connection slot: pick a node, dial, serve until the connection ends,
/// then re-resolve and redial after the backoff — until `stop` flips.
async fn slot_loop(
    idx: usize,
    config: Arc<ReceiverConfig>,
    handler: Arc<dyn InvokeHandler>,
    whoami: Arc<Vec<u8>>,
    live: Arc<LiveTargets>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow_and_update() {
            break;
        }
        // Re-resolve every iteration so a slot follows DNS / node churn.
        let targets = resolve(&config.relay_addr).await;
        match live.pick(&targets, idx) {
            Some(addr) => {
                // Hold the address for the lifetime of this connection so
                // sibling slots steer toward other nodes.
                let _guard = live.hold(addr);
                let outcome = tokio::select! {
                    _ = stop.changed() => break,
                    r = dial_and_serve(addr, &config, &handler, &whoami) => r,
                };
                match outcome {
                    Ok(()) => debug!(slot = idx, %addr, "receiver: connection closed, redialing"),
                    Err(e) => {
                        warn!(slot = idx, %addr, ?e, "receiver: connection ended, redialing after backoff")
                    }
                }
            }
            None => {
                debug!(
                    slot = idx,
                    relay = %config.relay_addr,
                    "receiver: no relay address resolved, retrying after backoff"
                );
            }
        }
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(config.reconnect_backoff) => {}
        }
    }
}

/// Resolve `relay_addr` to its socket addresses (all A/AAAA records, via the
/// platform resolver — no new dependency). Returns empty on failure; the
/// caller treats that as "nothing to dial yet" and backs off.
async fn resolve(relay_addr: &str) -> Vec<SocketAddr> {
    match tokio::net::lookup_host(relay_addr).await {
        Ok(addrs) => addrs.collect(),
        Err(e) => {
            debug!(%relay_addr, ?e, "receiver: DNS resolution failed");
            Vec::new()
        }
    }
}

/// One connection lifecycle: dial the chosen node, role-flip into an h2
/// server, serve streams until the connection ends.
async fn dial_and_serve(
    addr: SocketAddr,
    config: &ReceiverConfig,
    handler: &Arc<dyn InvokeHandler>,
    whoami: &Arc<Vec<u8>>,
) -> Result<(), ConnError> {
    let socket = TcpStream::connect(addr).await?;
    let _ = socket.set_nodelay(true);
    debug!(%addr, "receiver: connected, awaiting role-flip");

    match &config.tls {
        Some(tls) => {
            let tls_socket = tls
                .connector
                .connect(tls.server_name.clone(), socket)
                .await?;
            serve_connection(tls_socket, config, handler, whoami).await
        }
        None => serve_connection(socket, config, handler, whoami).await,
    }
}

/// Shared record of which resolved relay addresses currently have a live
/// connection. Each slot consults this to **prefer a node no sibling already
/// holds**, turning "K connections" into "K *distinct* nodes" (best-effort —
/// when K exceeds the resolved-node count, surplus slots deterministically
/// double up by slot index). Mirrors the Node `connect()` client's
/// `pickTarget`. The held/pick window is racy by design: two slots starting at
/// once may briefly pick the same node; they re-spread on the next redial.
#[derive(Default)]
struct LiveTargets {
    held: Mutex<Vec<SocketAddr>>,
}

impl LiveTargets {
    /// Choose a target for `slot` from `targets`: the first not currently held
    /// by a live sibling, else a deterministic fallback by slot index (all
    /// nodes already held, i.e. K > distinct nodes). `None` iff `targets` is
    /// empty.
    fn pick(&self, targets: &[SocketAddr], slot: usize) -> Option<SocketAddr> {
        if targets.is_empty() {
            return None;
        }
        let held = self.held.lock().expect("LiveTargets mutex poisoned");
        if let Some(addr) = targets.iter().find(|a| !held.contains(a)) {
            return Some(*addr);
        }
        Some(targets[slot % targets.len()])
    }

    /// Record `addr` as held; the returned guard releases it on drop (when the
    /// connection ends), so the node becomes preferable to siblings again.
    fn hold(self: &Arc<Self>, addr: SocketAddr) -> HoldGuard {
        self.held
            .lock()
            .expect("LiveTargets mutex poisoned")
            .push(addr);
        HoldGuard {
            book: self.clone(),
            addr,
        }
    }
}

/// RAII release of a held address (see [`LiveTargets::hold`]).
struct HoldGuard {
    book: Arc<LiveTargets>,
    addr: SocketAddr,
}

impl Drop for HoldGuard {
    fn drop(&mut self) {
        let mut held = self.book.held.lock().expect("LiveTargets mutex poisoned");
        if let Some(pos) = held.iter().position(|a| *a == self.addr) {
            held.swap_remove(pos);
        }
    }
}

/// Drive the h2 server side of the reversed connection: the relay is the h2
/// client (role-flip), we accept its streams. Spawns a task per stream so
/// concurrent invocations run in parallel and `accept()` keeps the
/// connection (and its PING acks) flowing.
async fn serve_connection<IO>(
    socket: IO,
    config: &ReceiverConfig,
    handler: &Arc<dyn InvokeHandler>,
    whoami: &Arc<Vec<u8>>,
) -> Result<(), ConnError>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut builder = h2::server::Builder::new();
    config.h2_tuning.apply_server(&mut builder);
    let mut connection = builder.handshake::<_, Bytes>(socket).await?;
    debug!("receiver: role-flip complete, serving");

    while let Some(accepted) = connection.accept().await {
        let (request, respond) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                warn!(?e, "receiver: stream accept error");
                return Err(ConnError::H2(e));
            }
        };
        let handler = handler.clone();
        let whoami = whoami.clone();
        tokio::spawn(serve_stream(request, respond, handler, whoami));
    }
    Ok(())
}

/// Serve a single accepted stream: answer `/whoami`, otherwise parse the
/// forwarded target and dispatch to the handler.
async fn serve_stream(
    request: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    handler: Arc<dyn InvokeHandler>,
    whoami: Arc<Vec<u8>>,
) {
    let (parts, body) = request.into_parts();

    // /whoami: the relay opens this first to learn our identity.
    if parts.method == http::Method::GET && parts.uri.path() == "/whoami" {
        if let Err(e) = reply_whoami(&mut respond, &whoami) {
            warn!(?e, "receiver: failed to answer /whoami");
        }
        return;
    }

    // Everything else is a forwarded invocation. Parse the target from the
    // full path+query; a malformed target is the relay/sender's error → 400
    // (we never reach the handler).
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| parts.uri.path());
    match crate::relay::protocol::parse_forwarded(path_and_query) {
        Ok(target) => {
            handler
                .handle(Invocation {
                    target,
                    method: parts.method,
                    headers: parts.headers,
                    body,
                    respond,
                })
                .await;
        }
        Err(e) => {
            warn!(?e, path = %path_and_query, "receiver: unparseable forwarded path → 400");
            let _ = reply_status(&mut respond, http::StatusCode::BAD_REQUEST);
        }
    }
}

/// Send the `/whoami` JSON body.
fn reply_whoami(
    respond: &mut h2::server::SendResponse<Bytes>,
    whoami: &Arc<Vec<u8>>,
) -> Result<(), h2::Error> {
    let response = http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(())
        .expect("static /whoami response builds");
    let mut send = respond.send_response(response, false)?;
    send.send_data(Bytes::from(whoami.as_ref().clone()), true)?;
    Ok(())
}

/// Send an empty-body response with the given status.
fn reply_status(
    respond: &mut h2::server::SendResponse<Bytes>,
    status: http::StatusCode,
) -> Result<(), h2::Error> {
    let response = http::Response::builder()
        .status(status)
        .body(())
        .expect("status response builds");
    let mut send = respond.send_response(response, false)?;
    let _ = send.send_data(Bytes::new(), true);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{HoldGuard, LiveTargets, TlsClient};
    use std::net::SocketAddr;
    use std::sync::Arc;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn pick_prefers_an_unheld_node_then_spreads_by_slot() {
        let nodes = [addr("10.0.0.1:8080"), addr("10.0.0.2:8080")];
        let live = Arc::new(LiveTargets::default());

        // Slot 0 takes node 0 (first unheld).
        let a0 = live.pick(&nodes, 0).unwrap();
        assert_eq!(a0, nodes[0]);
        let g0 = live.hold(a0);

        // Slot 1: node 0 is held, so it must land on node 1.
        let a1 = live.pick(&nodes, 1).unwrap();
        assert_eq!(a1, nodes[1]);
        let g1 = live.hold(a1);

        // Slot 2 (K > nodes): everything held → deterministic by index.
        assert_eq!(live.pick(&nodes, 2).unwrap(), nodes[2 % nodes.len()]);
        assert_eq!(live.pick(&nodes, 3).unwrap(), nodes[3 % nodes.len()]);

        // Releasing a slot frees its node for the next picker.
        drop(g0);
        assert_eq!(live.pick(&nodes, 5).unwrap(), nodes[0]);

        drop(g1);
    }

    #[test]
    fn pick_returns_none_when_nothing_resolves() {
        let live = Arc::new(LiveTargets::default());
        assert!(live.pick(&[], 0).is_none());
    }

    #[test]
    fn hold_guard_is_droppable() {
        // Compile-time check that HoldGuard's Drop releases cleanly even when
        // the address was already swap-removed (idempotent position lookup).
        let live = Arc::new(LiveTargets::default());
        let g: HoldGuard = live.hold(addr("127.0.0.1:1"));
        live.held.lock().unwrap().clear();
        drop(g); // position() returns None → no panic
        assert!(live.held.lock().unwrap().is_empty());
    }

    #[test]
    fn from_pem_rejects_invalid_ca() {
        // No CERTIFICATE block in the CA input → error (not a panic).
        assert!(TlsClient::from_pem(b"not a pem", None, "relay.example").is_err());
        assert!(TlsClient::from_pem(b"", None, "relay.example").is_err());
        assert!(TlsClient::from_pem(
            b"-----BEGIN FOO-----\nAAAA\n-----END FOO-----\n",
            None,
            "relay.example"
        )
        .is_err());
    }
}
