//! Bidirectional HTTP/2 stream bridge.
//!
//! Given:
//!   * A sender-side h2 stream — the cluster received this as an HTTP/2
//!     *server* on :9080. We have a `RecvStream` for the request body and a
//!     `SendResponse` for writing the response.
//!   * A receiver-side h2 stream — the cluster opened this as an HTTP/2
//!     *client* on the reversed connection. We have a `SendStream` for
//!     writing the request body and a `ResponseFuture`/`RecvStream` for
//!     reading the response.
//!
//! The bridge pumps bytes in both directions until either:
//!   * Both streams terminate cleanly (success path), or
//!   * Either side errors (failure path — propagate per design §6).
//!
//! ## Coupling the two halves ([`run_duplex`])
//!
//! Bridging upstream (sender→receiver) and downstream (receiver→sender) are
//! concurrent, but they are NOT independent at termination. A plain
//! `tokio::join!` waits for both regardless of outcome, so a failure on one
//! side leaves the other lingering until its own I/O happens to error — or
//! forever, if it is parked on an idle read (`reader.data().await` has no
//! timeout). [`run_duplex`] couples them:
//!
//!   * **One half errors** → the other is dropped immediately. Dropping a
//!     pump future drops its h2 send/recv halves, which resets the stream,
//!     so the failure propagates in both directions at once.
//!   * **One half completes cleanly (EOS)** → the other is *not* cancelled.
//!     HTTP/2 half-close is legal and the surviving direction may keep
//!     streaming (e.g. a server-streaming response after the request body
//!     finished, or a long upload after the response headers). We keep
//!     pumping it.
//!
//! The remaining gap is a *clean* half-close where the surviving direction's
//! peer then goes silent without closing (a silently-dead or adversarial
//! sender holding a half-open stream). For receiver connections this is
//! already covered by the PING liveness loop (`src/liveness.rs`), which
//! tears the connection down and errors its streams. Sender connections
//! have no such probe yet — see `BridgeConfig::half_close_drain_timeout`
//! for an opt-in bound; sender-side PING liveness is the proper fix and a
//! known follow-up. A *default* drain/idle timeout is
//! deliberately avoided: it would cut off legitimate long or sparse
//! one-directional streaming.
//!
//! ## Flow control
//!
//! For each direction, the sending side has an h2 send-window. We must
//! reserve capacity before sending DATA. The receiving side has a window
//! that we acknowledge as we consume bytes. Both `h2::SendStream::reserve_capacity` /
//! `available_capacity` and `RecvStream::flow_control().release_capacity()`
//! are the primitives.
//!
//! Per design §8: respect both windows; don't ack upstream faster than
//! downstream drains; bound memory held inside the cluster.

use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tracing::{debug, warn};

/// Cap on bytes coalesced into a single `send_data` call. Picked at
/// `max_frame_size` (64 KiB) so the resulting DATA frame doesn't
/// straddle h2's wire-frame boundary — h2 would just re-fragment a
/// larger payload internally, which would erase the saving. Sources
/// of "many small chunks" worth coalescing (drip producers,
/// fragmented application-level frames) typically have total
/// in-flight bytes well under this cap anyway.
const COALESCE_CAP_BYTES: usize = 64 * 1024;

/// A pump flushes its locally-accumulated held-bytes to the shared
/// [`BufferMeter`] only when the value moves by at least this much. Healthy
/// flow (held ≈ 0 on the fast path) does zero shared writes; a backpressured
/// stream flushes a handful of times as its backlog climbs/drains.
///
/// It is also the **per-pump quantization bound**: [`BufferMeterGuard::observe`]
/// keeps `|held − published| < BUFFER_FLUSH_THRESHOLD` at all times, so each pump
/// under-reports its held bytes by strictly less than this. `pub` so node
/// admission can reserve headroom for that residual (`PUMPS_PER_BRIDGE ×
/// BUFFER_FLUSH_THRESHOLD` per bridged stream) as a true bound rather than a
/// heuristic.
pub const BUFFER_FLUSH_THRESHOLD: i64 = 64 * 1024;

/// Pumps per bridged stream (upstream + downstream), each with its own
/// [`BufferMeterGuard`]. The worst-case unreported buffered bytes for one
/// bridge is therefore `PUMPS_PER_BRIDGE × BUFFER_FLUSH_THRESHOLD`.
pub const PUMPS_PER_BRIDGE: usize = 2;

/// Cheap, cloneable handle to a node-global "buffered bytes" accumulator that
/// the bridge feeds so memory-aware admission can see transient stall-buffering
/// (`Σ used_capacity` across streams). See `relay::node_admission`.
///
/// **Contention:** the per-chunk hot path touches NO shared memory. Each pump
/// accumulates its stream's held bytes locally in a [`BufferMeterGuard`] and
/// writes this shared atomic only on coarse [`BUFFER_FLUSH_THRESHOLD`]
/// crossings — and only on the backpressured slow path (a healthy fast-path
/// stream releases each chunk the same iteration, so it never registers held
/// bytes and never flushes). So the shared atomic sees writes only from
/// genuinely-backpressured streams, at a low rate. `None` disables it entirely
/// (a non-broker embedder, and the broker when admission is off).
#[derive(Clone, Default)]
pub struct BufferMeter {
    counter: Option<Arc<AtomicI64>>,
}

impl BufferMeter {
    /// A meter backed by a shared node-global counter.
    pub fn new(counter: Arc<AtomicI64>) -> Self {
        Self {
            counter: Some(counter),
        }
    }

    /// A no-op meter: `add` does nothing, `is_enabled` is false.
    pub fn disabled() -> Self {
        Self { counter: None }
    }

    /// Whether this meter is backed by a real counter.
    pub fn is_enabled(&self) -> bool {
        self.counter.is_some()
    }

    /// Apply a signed delta to the shared counter. `Relaxed` is sufficient —
    /// the value is a soft admission signal read on a separate slow path, not a
    /// synchronization point. Public so feeders other than the bridge pump (and
    /// tests) can contribute; the pump goes through [`BufferMeterGuard`].
    #[inline]
    pub fn add(&self, delta: i64) {
        if let Some(c) = &self.counter {
            if delta != 0 {
                c.fetch_add(delta, Ordering::Relaxed);
            }
        }
    }
}

impl std::fmt::Debug for BufferMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BufferMeter({})",
            if self.counter.is_some() { "on" } else { "off" }
        )
    }
}

/// Per-pump local accumulator that feeds a [`BufferMeter`] with this stream's
/// held (received-but-not-released) bytes, flushing to the shared counter only
/// on coarse threshold crossings. `Drop` zeroes this pump's contribution on
/// ANY exit path (mirrors the owner's `StreamFinishGuard`), so a reset/cancel
/// can never leak the counter.
struct BufferMeterGuard<'a> {
    meter: &'a BufferMeter,
    /// The value we last pushed to the shared counter (our current contribution).
    published: i64,
}

impl<'a> BufferMeterGuard<'a> {
    fn new(meter: &'a BufferMeter) -> Self {
        Self {
            meter,
            published: 0,
        }
    }

    /// Reconcile against this stream's TRUE current `used_capacity`. Pushes the
    /// delta to the shared counter only when it has moved by at least
    /// [`BUFFER_FLUSH_THRESHOLD`] since the last push — so the shared write is
    /// rare. A no-op when the meter is disabled.
    #[inline]
    fn observe(&mut self, used_capacity: usize) {
        if !self.meter.is_enabled() {
            return;
        }
        let held = used_capacity as i64;
        if (held - self.published).abs() >= BUFFER_FLUSH_THRESHOLD {
            self.meter.add(held - self.published);
            self.published = held;
        }
    }
}

impl Drop for BufferMeterGuard<'_> {
    fn drop(&mut self) {
        // Remove exactly what we contributed (the adds telescope to `published`).
        if self.published != 0 {
            self.meter.add(-self.published);
        }
    }
}

/// Errors surfaced from the bridge. The handler converts these to sender-
/// visible HTTP status codes per design §6.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    /// h2 error reading or writing on the sender-side stream.
    #[error("sender-side h2 error: {0}")]
    Sender(h2::Error),
    /// h2 error reading or writing on the receiver-side stream.
    #[error("receiver-side h2 error: {0}")]
    Receiver(h2::Error),
    /// Sender capacity-reservation timed out (upstream stalled).
    #[error("flow control wait timed out")]
    FlowControlTimeout,
    /// This half was cancelled by [`run_duplex`] because the other
    /// direction ended first — either it errored (immediate teardown) or it
    /// closed cleanly and this half exceeded the half-close drain timeout.
    /// The *cause* (if any) is carried by the other half's result.
    #[error("bridge half cancelled (other direction ended first)")]
    Cancelled,
}

impl BridgeError {
    /// True if this is a **sender-side** error that means the sender peer
    /// closed its own stream on its own terms — a remote reset/GOAWAY (any
    /// reason), an explicit `CANCEL`, or the `InactiveStreamId` race where our
    /// write to the sender lost to the sender's concurrent close. Per design
    /// §6 these are normal client closes, not relay/receiver faults, so the
    /// handler counts them as `SenderClosed` (not `BridgeError`) and the
    /// bridge logs them at `debug`.
    ///
    /// Returns `false` for `Receiver`/`FlowControlTimeout`/`Cancelled` and for
    /// relay-*caused* `Sender` errors (library resets, `PayloadTooBig`, etc.)
    /// — those are genuine faults. See `sender_went_away` for the predicate.
    pub fn is_benign_sender_close(&self) -> bool {
        matches!(self, BridgeError::Sender(e) if sender_went_away(e))
    }

    /// A coarse, stable bucket for WHY this bridge half failed — the label for
    /// the `relay_bridge_reset_total{reason}` breakdown. Maps the h2 RST_STREAM
    /// code (or a transport/IO error) to a fixed string so operators can tell,
    /// e.g., a receiver `protocol_error` / `cancel` apart from a `broken_pipe`
    /// connection drop or a flow-control stall. The set of returned values is
    /// finite and mirrored by `metrics_defs::BRIDGE_RESET_REASONS`.
    pub fn reset_reason(&self) -> &'static str {
        match self {
            BridgeError::FlowControlTimeout => "flow_control_timeout",
            BridgeError::Cancelled => "cancelled",
            BridgeError::Sender(e) | BridgeError::Receiver(e) => h2_reset_reason(e),
        }
    }
}

/// Bucket an `h2::Error` by its RST_STREAM reason (or transport/IO nature).
/// Kept in sync with `metrics_defs::BRIDGE_RESET_REASONS`.
fn h2_reset_reason(e: &h2::Error) -> &'static str {
    match e.reason() {
        Some(r) if r == h2::Reason::NO_ERROR => "no_error",
        Some(r) if r == h2::Reason::PROTOCOL_ERROR => "protocol_error",
        Some(r) if r == h2::Reason::INTERNAL_ERROR => "internal_error",
        Some(r) if r == h2::Reason::FLOW_CONTROL_ERROR => "flow_control_error",
        Some(r) if r == h2::Reason::STREAM_CLOSED => "stream_closed",
        Some(r) if r == h2::Reason::REFUSED_STREAM => "refused_stream",
        Some(r) if r == h2::Reason::CANCEL => "cancel",
        Some(r) if r == h2::Reason::ENHANCE_YOUR_CALM => "enhance_your_calm",
        // Any other RST_STREAM code (FRAME_SIZE_ERROR, COMPRESSION_ERROR, …).
        Some(_) => "other_h2",
        // No RST_STREAM reason: a transport/IO error (e.g. broken pipe / reset
        // connection) or a library-level error the SDK never turned into a code.
        None if e.is_io() => "broken_pipe",
        None => "other",
    }
}

/// Response headers/trailers whose (lowercased) name starts with this reserved
/// prefix are **relay-internal control signals** (see the relay crate's
/// `cluster::InternalResult` — `x-relay-internal-version` / `-result`). When
/// [`BridgeConfig::strip_reserved_relay_headers`] is set, the bridge removes
/// every such field from a forwarded *application* response so a receiver can
/// never smuggle one to the origin and forge a retry (double-execute). The relay
/// crate references THIS constant when emitting the signals, so the two sides
/// cannot drift.
pub const RESERVED_RELAY_RESPONSE_HEADER_PREFIX: &str = "x-relay-internal-";

/// Remove every field whose name starts with [`RESERVED_RELAY_RESPONSE_HEADER_PREFIX`].
/// Used on both the response head and the trailers. Allocation-free in the
/// common case (no reserved fields → the `to_remove` collect yields an empty,
/// non-allocating `Vec`).
fn strip_reserved_relay_headers(headers: &mut http::HeaderMap) {
    let to_remove: Vec<http::HeaderName> = headers
        .keys()
        .filter(|n| {
            n.as_str()
                .starts_with(RESERVED_RELAY_RESPONSE_HEADER_PREFIX)
        })
        .cloned()
        .collect();
    for name in to_remove {
        headers.remove(&name);
    }
}

/// Configuration knobs for the bridge.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// Maximum time to wait for flow-control capacity on a single chunk.
    /// Prevents pathological wedge if a peer never opens its window.
    pub flow_control_timeout: Duration,
    /// Optional bound on how long the *surviving* bridge half may keep
    /// running after the other half has closed **cleanly** (a legal HTTP/2
    /// half-close). `None` (the default) leaves it unbounded — correct for
    /// full-duplex workloads where one direction can legitimately stream
    /// long after the other finishes (server-streaming, sparse event
    /// streams). Set to `Some(_)` to bound a half-open stream's lifetime
    /// when a peer goes silent without closing — at the cost of cutting off
    /// legitimately-long one-directional streaming past the bound. See
    /// [`run_duplex`]. This does NOT apply when a half *errors* — that
    /// always tears the other down immediately. The proper fix for
    /// silently-dead senders is sender-side PING liveness (TODO F2), not
    /// this knob.
    pub half_close_drain_timeout: Option<Duration>,
    /// Optional node-global buffered-bytes meter for memory-aware admission.
    /// When enabled, the pump feeds this stream's held (received-but-not-
    /// released) bytes into it so the admission gate can see transient
    /// stall-buffering on top of its per-stream footprint accounting. Fed
    /// contention-light (local accumulation, coarse flush, slow-path only —
    /// see [`BufferMeter`]). Default [`BufferMeter::disabled`] (no accounting;
    /// a non-broker embedder and the admission-off broker leave it here).
    pub buffer_meter: BufferMeter,
    /// When `true`, strip every response header **and trailer** whose name
    /// starts with [`RESERVED_RELAY_RESPONSE_HEADER_PREFIX`] before forwarding
    /// the response. The cross-node forward server sets this so a receiver
    /// *application* response can never carry a forged relay-internal control
    /// signal (`x-relay-internal-*`) to the origin, where it could drive a
    /// spurious retry (double-execute). Relay-*generated* internal replies
    /// bypass the bridge, so they are never stripped. Default `false` — a
    /// non-broker embedder forwards headers verbatim.
    pub strip_reserved_relay_headers: bool,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            flow_control_timeout: Duration::from_secs(60),
            half_close_drain_timeout: None,
            buffer_meter: BufferMeter::disabled(),
            strip_reserved_relay_headers: false,
        }
    }
}

/// Run the two bridge halves as a coupled duplex pair, replacing a bare
/// `tokio::join!` so termination propagates correctly (see the module-level
/// "Coupling the two halves" docs).
///
/// Returns `(upstream_result, downstream_result)` — the same tuple shape
/// `tokio::join!(up, down)` produced, so callers' per-direction metric
/// recording is unchanged. A half cancelled because the other ended first
/// is reported as [`BridgeError::Cancelled`]; the real cause (if any) is in
/// the other half's result.
///
/// Semantics:
///   * first half to finish **errors** → the other is dropped at once
///     (resetting its stream) and reported `Cancelled`;
///   * first half to finish is **Ok** (clean half-close) → the survivor is
///     awaited, bounded by [`BridgeConfig::half_close_drain_timeout`] if
///     set (else unbounded).
pub async fn run_duplex<U, D>(
    up: U,
    down: D,
    config: &BridgeConfig,
) -> (Result<(), BridgeError>, Result<(), BridgeError>)
where
    U: Future<Output = Result<(), BridgeError>>,
    D: Future<Output = Result<(), BridgeError>>,
{
    tokio::pin!(up);
    tokio::pin!(down);

    tokio::select! {
        up_res = &mut up => {
            if up_res.is_err() {
                // Upstream errored — don't let downstream linger on an idle
                // read. Dropping `down` at return resets its stream.
                (up_res, Err(BridgeError::Cancelled))
            } else {
                let down_res = drain_survivor(&mut down, config).await;
                (up_res, down_res)
            }
        }
        down_res = &mut down => {
            if down_res.is_err() {
                (Err(BridgeError::Cancelled), down_res)
            } else {
                let up_res = drain_survivor(&mut up, config).await;
                (up_res, down_res)
            }
        }
    }
}

/// Await the surviving bridge half after the other closed cleanly, bounded
/// by the optional half-close drain timeout. On timeout the survivor is
/// dropped by the caller (resetting its stream) and reported `Cancelled`.
async fn drain_survivor<F>(fut: F, config: &BridgeConfig) -> Result<(), BridgeError>
where
    F: Future<Output = Result<(), BridgeError>>,
{
    match config.half_close_drain_timeout {
        Some(t) => tokio::time::timeout(t, fut)
            .await
            .unwrap_or(Err(BridgeError::Cancelled)),
        None => fut.await,
    }
}

/// A type-erased, `Send` drop-guard the caller wants released **only after** the
/// upstream pump has taken over buffered-bytes accounting for this stream — the
/// "peek→bridge accounting handoff" (see the [`bridge_upstream_with_handoff`]
/// docs).
///
/// The bridge crate does not know what the guard *is* (in the broker it is a
/// node-admission `PeekReservation`); it only needs that dropping it runs the
/// concrete guard's `Drop`. The pump publishes whatever the pre-bridge window
/// buffered into the [`BufferMeter`] and *then* drops this guard, so the held
/// bytes are covered by the meter the instant the transient reservation goes
/// away — never a window where they are unaccounted. If the pump future is
/// dropped before it runs (e.g. the other bridge half errored first), the guard
/// is simply dropped too, releasing the reservation — exactly once either way.
pub type AccountingHandoff = Box<dyn Send>;

/// Pump request body bytes from the sender's `RecvStream` to the receiver's
/// `SendStream`. Returns when either side ends or errors.
#[tracing::instrument(level = "info", name = "bridge.upstream", skip_all)]
pub async fn bridge_upstream(
    sender_body: h2::RecvStream,
    receiver_send: h2::SendStream<Bytes>,
    config: &BridgeConfig,
) -> Result<(), BridgeError> {
    pump(
        sender_body,
        receiver_send,
        config,
        PumpDirection::Upstream,
        None,
    )
    .await
}

/// Like [`bridge_upstream`] but carries an [`AccountingHandoff`] guard that is
/// released only once this pump has taken over buffered-bytes accounting.
///
/// This closes the **peek→bridge handoff gap**. Before the bridge starts, the
/// sender's request body can buffer in the h2 `RecvStream` (up to one stream
/// flow-control window) while the broker peeks the receiver's response head; the
/// broker covers that transient with a separate node-admission *peek
/// reservation*. If that reservation were released the instant the peek
/// resolved — before this pump observed the still-buffered bytes — there would
/// be a scheduling window where those bytes are accounted by neither the peek
/// reservation nor the meter. Instead the broker hands the reservation guard
/// here: the pump publishes the currently-held bytes into the [`BufferMeter`]
/// and *then* drops the guard, so `used` never dips below the true held amount
/// during the transition (it is momentarily double-counted, which is safe).
///
/// The handoff is a single, once-per-stream meter reconcile at pump start — not
/// a per-chunk write — so the healthy fast path (peek resolved within an RTT,
/// ~0 buffered) publishes nothing and stays free of shared writes.
#[tracing::instrument(level = "info", name = "bridge.upstream", skip_all)]
pub async fn bridge_upstream_with_handoff(
    sender_body: h2::RecvStream,
    receiver_send: h2::SendStream<Bytes>,
    config: &BridgeConfig,
    handoff: Option<AccountingHandoff>,
) -> Result<(), BridgeError> {
    pump(
        sender_body,
        receiver_send,
        config,
        PumpDirection::Upstream,
        handoff,
    )
    .await
}

/// Pump response body bytes from the receiver's `RecvStream` to the sender's
/// `SendStream`. Sends the response head first.
///
/// Awaits the response future internally — callers that need to inspect
/// the response status before committing to the bridge (e.g. for
/// admission-driven retry) should use [`bridge_response_body`] instead,
/// which takes an already-resolved response.
#[tracing::instrument(level = "info", name = "bridge.downstream", skip_all)]
pub async fn bridge_downstream(
    receiver_response: h2::client::ResponseFuture,
    sender_respond: &mut h2::server::SendResponse<Bytes>,
    config: &BridgeConfig,
) -> Result<(), BridgeError> {
    let response = receiver_response.await.map_err(BridgeError::Receiver)?;
    bridge_response_body(response, sender_respond, config).await
}

/// Like [`bridge_downstream`] but takes the response *already resolved*.
/// Used by the sender handler's retry loop, which awaits the response
/// future itself so it can inspect status (and trigger a retry on 503
/// before any sender body has been pumped upstream).
///
/// The receiver-side `cluster::forward` also uses this on the branch where its
/// bounded response-head peek RESOLVES: it inspects the head to detect a
/// capable owner's negotiated client-drain refusal (`503 +
/// x-restate-tunnel-draining`, Gate A) and translate it into a relay-internal
/// `client-draining` result instead of bridging the sentinel back. It falls
/// back to [`bridge_downstream`] only on the deferred / RPC-timeout branch,
/// where the response head has not arrived within the peek window (so no drain
/// decision is possible and the stream simply proceeds).
#[tracing::instrument(level = "info", name = "bridge.response_body", skip_all)]
pub async fn bridge_response_body(
    response: http::Response<h2::RecvStream>,
    sender_respond: &mut h2::server::SendResponse<Bytes>,
    config: &BridgeConfig,
) -> Result<(), BridgeError> {
    let (parts, receiver_body) = response.into_parts();
    // Reconstruct an http::Response<()> from the parts to hand to the
    // sender side.
    let mut response_head = http::Response::new(());
    *response_head.status_mut() = parts.status;
    *response_head.headers_mut() = parts.headers;
    *response_head.version_mut() = parts.version;

    // Scrub the relay-internal namespace from an *application* response before
    // it leaves this node (cross-node forward sets this). A receiver could
    // otherwise stamp `x-relay-internal-result` on its own response and forge a
    // retry on the origin — the double-execute vector. Relay-generated internal
    // replies bypass the bridge and are unaffected.
    if config.strip_reserved_relay_headers {
        strip_reserved_relay_headers(response_head.headers_mut());
    }

    // Send the response head. `end_of_stream = false`: body follows.
    let sender_send = sender_respond
        .send_response(response_head, false)
        .map_err(BridgeError::Sender)?;

    pump(
        receiver_body,
        sender_send,
        config,
        PumpDirection::Downstream,
        None,
    )
    .await
}

/// Which direction a [`pump`] call is moving bytes in.
///
/// Both directions share the same `read → reserve → send → release` shape;
/// the direction only determines (a) which side a read-error vs a
/// write-error blames in [`BridgeError`] and (b) the log prefix.
#[derive(Copy, Clone)]
enum PumpDirection {
    /// Sender's request body → receiver's send stream.
    Upstream,
    /// Receiver's response body → sender's send stream.
    Downstream,
}

impl PumpDirection {
    /// Error variant to use when the *read* side fails.
    fn read_err(self, e: h2::Error) -> BridgeError {
        match self {
            Self::Upstream => BridgeError::Sender(e),
            Self::Downstream => BridgeError::Receiver(e),
        }
    }

    /// Error variant to use when the *write* side fails.
    fn write_err(self, e: h2::Error) -> BridgeError {
        match self {
            Self::Upstream => BridgeError::Receiver(e),
            Self::Downstream => BridgeError::Sender(e),
        }
    }

    /// Log prefix.
    fn label(self) -> &'static str {
        match self {
            Self::Upstream => "bridge upstream",
            Self::Downstream => "bridge downstream",
        }
    }

    /// RST_STREAM reason to send on the *writer* when the *reader* fails,
    /// per design §6. The two directions mean different things:
    ///   * Upstream read failure = the sender vanished mid-request. Per §6
    ///     we `CANCEL` the receiver stream ("we no longer need this"); the
    ///     reversed connection stays up.
    ///   * Downstream read failure = the receiver died mid-response. Per §6
    ///     we send `INTERNAL_ERROR` to the sender ("forwarding broke").
    fn read_error_reset_reason(self) -> h2::Reason {
        match self {
            Self::Upstream => h2::Reason::CANCEL,
            Self::Downstream => h2::Reason::INTERNAL_ERROR,
        }
    }
}

/// Shared pump loop: read DATA from `reader`, forward to `writer`,
/// release the reader's window. Returns when the reader ends or either
/// side errors.
///
/// ## Zero-copy on the happy path
///
/// `bytes::Bytes` is refcounted. `RecvStream::data()` returns a `Bytes`
/// that refs into h2's recv buffer; passing the same `Bytes` to
/// `SendStream::send_data` queues the refcount in h2's send buffer.
/// The bridge never copies the payload itself — the only copies are the
/// two irreducible kernel↔user transitions (recv into h2's buffer, send
/// from h2's buffer). True zero-copy across HTTP/2 would need a
/// buffer-owning h2 impl (doesn't exist in mainline Rust — see
/// `docs/research-2026-05-off-the-shelf.md`).
///
/// ## Flow control
///
/// Two strategies stacked on each other:
/// * **Fast path** (the typical case with tuned h2 windows): if
///   `writer.capacity()` already covers the incoming chunk, just call
///   `send_data` — no `reserve_capacity` / `poll_capacity` await dance.
///   That's the common case at small-to-medium chunk sizes with the
///   recommended `initial_window_size` (1 MiB+).
/// * **Slow path** (window has filled): explicitly `reserve_capacity` +
///   `poll_capacity`-with-timeout. Partial grants are handled correctly
///   via `Bytes::split_to(granted)` (refcounted slice — no allocation,
///   no copy, no tail loss); the leftover tail is carried into the next
///   iteration via `pending`.
///
/// Either way, `release_capacity(n)` on the reader's flow control
/// happens *after* the bytes have been handed to the writer, so a
/// stalled writer applies backpressure all the way back to the reader.
/// True if this h2 error means the **sender peer closed its own stream** —
/// not a relay/receiver fault. Three shapes, all "the sender went away":
///   * a remote-initiated reset/GOAWAY (`is_remote()`), any reason — the peer
///     explicitly tore the stream/connection down;
///   * an explicit `CANCEL` reason — benign regardless of initiator ("no
///     longer needed"); this is how a completed Restate bidi invocation ends;
///   * the `InactiveStreamId` user-error race — our write to the sender lost
///     to the sender's own concurrent close. This is the suspension/reconnect
///     case (finding 5.7): Restate suspends mid-invocation, drops its stream,
///     and our `send-end`/`send_data` then hits an already-inactive stream.
///
/// Relay-*caused* sender errors are deliberately EXCLUDED so they keep
/// warning + counting as bridge failures: library-initiated resets
/// (`is_library()` — a protocol error the relay's own h2 stack emitted) and
/// every other `UserError` (PayloadTooBig, ReleaseCapacityTooBig,
/// MalformedHeaders, PollResetAfterSendResponse, …) are real defects, not the
/// sender leaving.
///
/// `InactiveStreamId` is matched by its `Display` string because h2's
/// `UserError` enum is not public and the variant carries `reason() == None`,
/// `is_remote() == false`, `is_library() == false` — there is nothing else to
/// match on. `h2` is a `=`-pinned dependency so the string is stable; a dep
/// bump must re-confirm it. The guard test `non_cancel_reason_is_not_benign`
/// pins the inverse (a non-CANCEL `Reason` error must NOT be treated benign).
fn sender_went_away(e: &h2::Error) -> bool {
    e.is_remote()
        || e.reason() == Some(h2::Reason::CANCEL)
        || e.to_string() == "user error: inactive stream"
}

/// True if `e` is a **graceful remote end-of-stream** on the *response* reader:
/// a remote `RST_STREAM(NO_ERROR)`.
///
/// Some HTTP/2 servers finish a **unary** response by sending the full body and
/// then `RST_STREAM(NO_ERROR)` — *not* a bare END_STREAM. hyper does exactly
/// this (and the Restate Cloud tunnel client is built on hyper): when it has a
/// complete response but the peer's request half is still open, it answers,
/// then resets the stream with NO_ERROR to reclaim it. RFC 7540 §8.1 defines
/// NO_ERROR as "no error" and h2 surfaces queued DATA *before* the reset, so
/// everything already received is the complete message. The relay must deliver
/// what it has and close the sender stream cleanly, NOT propagate an
/// INTERNAL_ERROR (which truncates the body — the sender would see headers but
/// an empty/aborted body). Discovered running the unmodified
/// `restate-cloud-tunnel-client` against the relay; see
/// `docs/cloud-tunnel-compat.md`.
///
/// Scoped to the **downstream** (receiver→sender) direction. The upstream
/// (sender→receiver) reader already classifies a remote reset as a benign
/// sender close via [`sender_went_away`]; only the downstream path currently
/// treats a remote reset as a hard receiver fault, and only a NO_ERROR reason
/// is unambiguously graceful — INTERNAL_ERROR / CANCEL / etc. stay faults.
fn is_graceful_response_eos(dir: PumpDirection, e: &h2::Error) -> bool {
    matches!(dir, PumpDirection::Downstream)
        && e.is_remote()
        && e.reason() == Some(h2::Reason::NO_ERROR)
}

/// True if a *write* failure on the **upstream** (request → receiver) direction
/// means the receiver has gracefully closed its side of the stream — so the
/// request-forwarding direction is cleanly *done*, not faulted.
///
/// The Restate Cloud tunnel client proxies **unary** requests: it answers the
/// forwarded request, then resets the stream (NO_ERROR). The relay is often
/// still finishing the request half (e.g. sending END_STREAM for a bodyless
/// GET), and that write loses the race to the receiver's close — surfacing as
/// either a remote `RST_STREAM(NO_ERROR)`/`CANCEL` or the `InactiveStreamId`
/// user-error (our write hit an already-closed stream). All three mean "the
/// receiver took what it needed and closed", not "forwarding broke". Treating
/// them as a clean upstream completion lets [`run_duplex`] *drain* the response
/// half (deliver the body) instead of tearing it down — without this, the
/// upstream error wins the `run_duplex` race and the response is lost.
///
/// Scoped to upstream. A downstream (response → sender) write failure is a
/// sender close, classified separately by [`sender_went_away`] /
/// [`BridgeError::is_benign_sender_close`].
fn write_hit_graceful_peer_close(dir: PumpDirection, e: &h2::Error) -> bool {
    matches!(dir, PumpDirection::Upstream)
        && ((e.is_remote() && e.reason() == Some(h2::Reason::NO_ERROR))
            || e.reason() == Some(h2::Reason::CANCEL)
            || e.to_string() == "user error: inactive stream")
}

/// Map a write error to a pump result: `Ok(())` when the peer gracefully closed
/// (see [`write_hit_graceful_peer_close`]) so the caller returns clean; a
/// genuine `Err` otherwise (logged at the level its cause warrants).
fn finish_on_write_err(
    dir: PumpDirection,
    e: h2::Error,
    label: &str,
    ctx: &str,
) -> Result<(), BridgeError> {
    if write_hit_graceful_peer_close(dir, &e) {
        debug!("{label}: {ctx}, but the receiver had already closed the request stream — request forwarding complete");
        return Ok(());
    }
    let be = dir.write_err(e);
    log_bridge_error(label, ctx, &be);
    Err(be)
}

/// Close the writer with a bare END_STREAM frame after the reader signalled a
/// graceful reset (no trailers can follow a reset, so we don't probe for them).
fn finish_writer_after_graceful_reset(
    writer: &mut h2::SendStream<Bytes>,
    dir: PumpDirection,
) -> Result<(), BridgeError> {
    debug!(
        "{}: receiver ended the response with RST_STREAM(NO_ERROR) — \
         treating as clean end-of-stream",
        dir.label()
    );
    if let Err(e) = writer.send_data(Bytes::new(), true) {
        let be = dir.write_err(e);
        log_bridge_error(dir.label(), "send-end failed", &be);
        return Err(be);
    }
    Ok(())
}

/// Release `n` bytes on the reader's flow-control window after forwarding them.
/// Returns `Ok(true)` when the reader has *gracefully* reset (remote NO_ERROR):
/// the bytes are already queued to the writer, so the caller should close the
/// writer with a bare END_STREAM frame and finish. `Ok(false)` is a normal
/// release; `Err` is a genuine read fault. `try_peek_data` uses a no-op waker
/// and so may not have observed a just-arrived reset during coalescing — it can
/// instead surface here, on the post-send `release_capacity`, which is why this
/// site needs the same graceful-EOS treatment as the DATA read.
fn release_or_graceful_eos(
    reader: &mut h2::RecvStream,
    n: usize,
    dir: PumpDirection,
    label: &str,
) -> Result<bool, BridgeError> {
    match reader.flow_control().release_capacity(n) {
        Ok(()) => Ok(false),
        Err(e) if is_graceful_response_eos(dir, &e) => Ok(true),
        Err(e) => {
            let be = dir.read_err(e);
            log_bridge_error(label, "release_capacity failed", &be);
            Err(be)
        }
    }
}

/// Log a bridge error at the level its cause warrants. A benign sender close
/// (see [`BridgeError::is_benign_sender_close`]) drops to `debug!`: the
/// primary workload ends every invocation in a sender CANCEL, and each
/// suspension step ends in the `InactiveStreamId` race, so warning on these
/// would flood the log and pollute WARN-rate alerts one-line-per-request.
/// Everything else — receiver faults, flow-control stalls, and relay-caused
/// sender errors — stays a genuine `warn!`.
fn log_bridge_error(label: &str, ctx: &str, err: &BridgeError) {
    if err.is_benign_sender_close() {
        debug!("{label}: {ctx}: {err} (sender closed its stream — normal)");
    } else {
        warn!("{label}: {ctx}: {err}");
    }
}

async fn pump(
    mut reader: h2::RecvStream,
    mut writer: h2::SendStream<Bytes>,
    config: &BridgeConfig,
    dir: PumpDirection,
    handoff: Option<AccountingHandoff>,
) -> Result<(), BridgeError> {
    let label = dir.label();
    // Bytes carried over from a partial-grant slow-path iteration.
    let mut pending: Option<Bytes> = None;
    // Feeds this stream's held (received-but-not-released) bytes into the
    // node-global buffered-bytes meter for memory-aware admission. Only the
    // backpressured slow path observes (a healthy fast-path chunk is released
    // the same iteration and never held), so healthy flow costs nothing and
    // the shared counter sees writes only under real buffering. Drop zeroes
    // this stream's contribution on any exit.
    let mut bmeter = BufferMeterGuard::new(&config.buffer_meter);

    // Peek→bridge accounting handoff (upstream only; see
    // [`bridge_upstream_with_handoff`] and [`AccountingHandoff`]). Before this
    // pump ran, the caller held a transient reservation (the node-admission peek
    // reservation) covering whatever the sender body buffered in `reader`'s recv
    // window during the response-head peek. Reconcile that held amount into the
    // meter FIRST, then drop the guard — so the bytes are covered by the meter
    // the instant the reservation is released, never unaccounted. `observe`
    // respects the flush threshold, so a healthy ~0-buffered handoff writes
    // nothing to the shared counter (fast path stays free); a genuinely-buffered
    // handoff publishes the held bytes (already bounded by the residual reserve
    // for the sub-threshold remainder). Runs once, at pump start.
    if let Some(guard) = handoff {
        bmeter.observe(reader.flow_control().used_capacity());
        drop(guard);
    }
    loop {
        // Acquire the next chunk. If we have a leftover tail from a
        // partial grant, use that; otherwise pull from the reader.
        let mut chunk = if let Some(p) = pending.take() {
            p
        } else {
            match reader.data().await {
                Some(Ok(b)) => b,
                Some(Err(e)) => {
                    // A graceful remote NO_ERROR reset on the response reader is
                    // a clean end-of-stream (hyper's unary-response idiom): any
                    // DATA already forwarded is the complete body, so close the
                    // sender stream normally instead of resetting it.
                    if is_graceful_response_eos(dir, &e) {
                        return finish_writer_after_graceful_reset(&mut writer, dir);
                    }
                    let be = dir.read_err(e);
                    log_bridge_error(label, "read body error", &be);
                    writer.send_reset(dir.read_error_reset_reason());
                    return Err(be);
                }
                None => {
                    // Reader's DATA stream is done. Forward the reader's
                    // trailers (if any) or an empty end-of-stream frame.
                    // Design §4.2(e)/§4.3 require trailers — the HEADERS
                    // frame that can follow DATA — to round-trip verbatim.
                    // `data()` returning None only signals the end of DATA;
                    // trailers are retrieved separately via `trailers()`.
                    debug!("{label}: read body finished");
                    return finish_writer(&mut reader, &mut writer, dir, config).await;
                }
            }
        };

        // Coalesce any DATA frames that are *already buffered* on the
        // reader's recv queue into a single payload. This costs one
        // allocation + memcpy per accumulated chunk, in exchange for
        // collapsing many `send_data` calls into one. The win shows
        // up under "drip" producers (sender sends many small frames
        // back-to-back within a stream). For the fixed-large-body
        // common case, `try_peek_data` returns Pending on the second
        // call and we don't allocate at all.
        //
        // `eos_after_coalesce` carries the reader-side EOF discovered
        // during peek so we can emit a single `send_data(.., true)`
        // closing call instead of going around the outer loop once
        // more just to send a zero-length end-of-stream frame.
        let mut coalesce_end = CoalesceEnd::Open;
        if pending.is_none() && chunk.len() < COALESCE_CAP_BYTES {
            (chunk, coalesce_end) = coalesce_pending(&mut reader, chunk, dir)?;
        }

        // Graceful NO_ERROR reset surfaced during coalescing: the reader is
        // dead. Deliver everything buffered as a single closing frame and
        // finish — do NOT fall through to release_capacity / finish_writer,
        // both of which would error on the reset stream. The buffer is bounded
        // by COALESCE_CAP_BYTES (64 KiB), so a single `send_data` is safe (h2
        // buffers past the peer window; the slow-path backpressure dance is
        // only needed for unbounded streaming bodies).
        if coalesce_end == CoalesceEnd::GracefulResetEos {
            debug!(
                "{label}: receiver ended the response with RST_STREAM(NO_ERROR) \
                 mid-body — delivering {} buffered byte(s) as clean EOS",
                chunk.len()
            );
            if let Err(e) = writer.send_data(chunk, true) {
                let be = dir.write_err(e);
                log_bridge_error(label, "send-end failed", &be);
                return Err(be);
            }
            return Ok(());
        }
        let eos_after_coalesce = coalesce_end == CoalesceEnd::CleanEos;

        let len = chunk.len();

        if len == 0 {
            // Zero-length chunk: forward nothing, but still ack the
            // reader's window. (`poll_capacity` on a 0-byte reservation
            // can never resolve, so the slow path would deadlock; skip
            // the whole capacity dance for empty chunks.)
            if release_or_graceful_eos(&mut reader, 0, dir, label)? {
                return finish_writer_after_graceful_reset(&mut writer, dir);
            }
            continue;
        }

        // Fast path: writer already has enough granted capacity from the
        // peer's initial window (or a prior WINDOW_UPDATE). Send directly
        // — no `reserve_capacity` / `poll_capacity` await round-trip.
        // The common case with tuned `initial_window_size` (1 MiB+) is
        // that capacity is plenty for any single chunk.
        if writer.capacity() >= len {
            // Never merge end-of-stream into this DATA frame, even when the
            // coalesce peek saw EOF: trailers may follow, and once an
            // end-of-stream frame is sent the stream is closed and trailers
            // can no longer be forwarded. `finish_writer` checks for
            // trailers and emits them (or an empty EOS frame). The cost is
            // one extra zero-length DATA frame on the trailers-absent
            // coalesced path — negligible next to losing trailers.
            if let Err(e) = writer.send_data(chunk, false) {
                return finish_on_write_err(dir, e, label, "send_data failed");
            }
            if release_or_graceful_eos(&mut reader, len, dir, label)? {
                return finish_writer_after_graceful_reset(&mut writer, dir);
            }
            // Fast path = the writer kept up, so nothing is held. Only touch the
            // meter to flush DOWN if this stream had previously backpressured
            // (published != 0) and has now recovered — otherwise skip the
            // `used_capacity` read entirely, keeping the healthy path free.
            if bmeter.published != 0 {
                bmeter.observe(reader.flow_control().used_capacity());
            }
            if eos_after_coalesce {
                return finish_writer(&mut reader, &mut writer, dir, config).await;
            }
            continue;
        }

        // Slow path: ask h2 for capacity, wait (with timeout) for it to
        // be granted, then forward what was granted.
        writer.reserve_capacity(len);
        // Slow path = the writer is backpressured and we're about to block,
        // holding `chunk` (and possibly more h2-buffered). This is exactly the
        // buffering that pressures node memory, so record it before the block.
        bmeter.observe(reader.flow_control().used_capacity());
        let granted =
            match tokio::time::timeout(config.flow_control_timeout, poll_capacity(&mut writer))
                .await
            {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => {
                    return finish_on_write_err(dir, e, label, "write capacity error");
                }
                Err(_elapsed) => {
                    warn!("{label}: write capacity timed out");
                    writer.send_reset(h2::Reason::CANCEL);
                    return Err(BridgeError::FlowControlTimeout);
                }
            };

        // `granted` may be less than `len`. Split the chunk via
        // `Bytes::split_to` — refcounted slice, zero copy. The tail
        // rides the next iteration via `pending`.
        let to_send_len = granted.min(len);
        let (to_send, leftover) = if to_send_len >= len {
            (chunk, None)
        } else {
            let mut head = chunk;
            let tail = head.split_off(to_send_len);
            (head, Some(tail))
        };

        if let Err(e) = writer.send_data(to_send, false) {
            return finish_on_write_err(dir, e, label, "send_data failed");
        }

        // Release exactly what we just forwarded.
        if release_or_graceful_eos(&mut reader, to_send_len, dir, label)? {
            return finish_writer_after_graceful_reset(&mut writer, dir);
        }
        // Update the held-bytes meter after draining part of the backlog.
        bmeter.observe(reader.flow_control().used_capacity());

        pending = leftover;
        // Slow-path partial grant: don't try to attach the coalesced
        // EOS here. Carry on; the next outer iteration will pull the
        // EOF naturally via `reader.data().await -> None`.
        let _ = eos_after_coalesce;
    }
}

/// Terminate the writer side once the reader's DATA stream has ended
/// (`data()` returned `None`, or the coalesce peek observed EOF).
///
/// HTTP/2 carries trailers as a HEADERS frame *after* the DATA frames;
/// `data()` returning `None` means only that DATA is done, not that the
/// stream is over. Design §4.2(e)/§4.3 require trailers to be forwarded
/// verbatim, so we pull them via `RecvStream::trailers()` and, if present,
/// emit them with `SendStream::send_trailers` (which carries END_STREAM).
/// With no trailers we send an empty end-of-stream DATA frame, exactly as
/// before. A receiver that puts its status in trailers (e.g. gRPC's
/// `grpc-status`) depends on this.
async fn finish_writer(
    reader: &mut h2::RecvStream,
    writer: &mut h2::SendStream<Bytes>,
    dir: PumpDirection,
    config: &BridgeConfig,
) -> Result<(), BridgeError> {
    let label = dir.label();
    match reader.trailers().await {
        Ok(Some(mut trailers)) => {
            // Trailers are a second forgery/leak channel the head-strip misses:
            // scrub the relay-internal namespace here too. (The origin never
            // reads trailers as a control signal, so this is defense-in-depth,
            // not a retry vector — but it keeps the namespace off the wire.)
            if config.strip_reserved_relay_headers {
                strip_reserved_relay_headers(&mut trailers);
            }
            if trailers.is_empty() {
                // Every trailer field was in the reserved namespace (or there
                // were none left): send a bare END_STREAM instead of an empty
                // HEADERS frame, so we don't emit a meaningless trailers frame.
                if let Err(e) = writer.send_data(Bytes::new(), true) {
                    return finish_on_write_err(dir, e, label, "send-end failed");
                }
                return Ok(());
            }
            debug!("{label}: forwarding {} trailer field(s)", trailers.len());
            if let Err(e) = writer.send_trailers(trailers) {
                return finish_on_write_err(dir, e, label, "send_trailers failed");
            }
            Ok(())
        }
        Ok(None) => {
            if let Err(e) = writer.send_data(Bytes::new(), true) {
                return finish_on_write_err(dir, e, label, "send-end failed");
            }
            Ok(())
        }
        Err(e) => {
            // A graceful NO_ERROR reset surfaces here when the coalesce peek
            // already saw EOS via that reset: no trailers follow a reset, so
            // close the sender stream cleanly (the body was forwarded above).
            if is_graceful_response_eos(dir, &e) {
                return finish_writer_after_graceful_reset(writer, dir);
            }
            // The reader errored while delivering trailers. Reset the
            // writer and surface it as a read-side error, mirroring the
            // main loop's DATA read-error handling (direction-dependent
            // reason, design §6).
            let be = dir.read_err(e);
            log_bridge_error(label, "read trailers error", &be);
            writer.send_reset(dir.read_error_reset_reason());
            Err(be)
        }
    }
}

/// How a [`coalesce_pending`] peek left the reader.
#[derive(Copy, Clone, PartialEq)]
enum CoalesceEnd {
    /// More DATA may still arrive; keep pumping. The reader stays usable.
    Open,
    /// Clean end-of-stream (`data()` would return `None`) observed during the
    /// peek. The reader is still usable — `finish_writer` reads trailers next.
    CleanEos,
    /// The reader was gracefully reset (remote `RST_STREAM(NO_ERROR)`) during
    /// the peek — hyper's unary-response close. Everything buffered so far is
    /// the complete body, and the reader is **dead**: the caller must deliver
    /// the buffer with END_STREAM and touch the reader no further (a
    /// `release_capacity` or `trailers()` on a reset stream errors).
    GracefulResetEos,
}

/// Drain any DATA chunks the reader has *already buffered* and
/// concatenate them with `first` into a single `Bytes`, up to
/// [`COALESCE_CAP_BYTES`]. Returns the coalesced bytes plus how the reader
/// ended (see [`CoalesceEnd`]).
///
/// Errors observed during peek propagate as a read error, mirroring
/// the main loop's error handling — except a graceful NO_ERROR reset, which
/// is reported as [`CoalesceEnd::GracefulResetEos`] so the already-buffered
/// body is delivered rather than discarded.
///
/// Uses a no-op waker to peek `RecvStream::poll_data` without
/// suspending: only chunks that are *already* on h2's recv queue are
/// pulled in. The next `data().await` call re-registers a real waker
/// for future arrivals, so no deadlock.
fn coalesce_pending(
    reader: &mut h2::RecvStream,
    first: Bytes,
    dir: PumpDirection,
) -> Result<(Bytes, CoalesceEnd), BridgeError> {
    // Peek once before allocating. The common (no-coalesce) case
    // returns Pending here and we hand `first` back as-is — zero
    // allocation, zero memcpy.
    let next = match try_peek_data(reader) {
        PeekOutcome::Pending => return Ok((first, CoalesceEnd::Open)),
        PeekOutcome::Eof => return Ok((first, CoalesceEnd::CleanEos)),
        PeekOutcome::Chunk(b) => b,
        PeekOutcome::Error(e) => {
            // Graceful NO_ERROR reset while peeking: `first` is the complete
            // body (h2 hands us all queued DATA before the reset). Deliver it
            // rather than discarding the chunk we already hold.
            if is_graceful_response_eos(dir, &e) {
                return Ok((first, CoalesceEnd::GracefulResetEos));
            }
            let be = dir.read_err(e);
            log_bridge_error(dir.label(), "read body error during peek", &be);
            return Err(be);
        }
    };

    // At least two chunks present — build an accumulator. Capacity
    // hint is generous: this branch only runs when there are >=2
    // chunks already buffered, so we'll likely drain more.
    let mut buf = BytesMut::with_capacity(COALESCE_CAP_BYTES.min(first.len() + next.len() + 4096));
    buf.extend_from_slice(&first);
    buf.extend_from_slice(&next);

    while buf.len() < COALESCE_CAP_BYTES {
        match try_peek_data(reader) {
            PeekOutcome::Pending => return Ok((buf.freeze(), CoalesceEnd::Open)),
            PeekOutcome::Eof => return Ok((buf.freeze(), CoalesceEnd::CleanEos)),
            PeekOutcome::Chunk(more) => {
                buf.extend_from_slice(&more);
            }
            PeekOutcome::Error(e) => {
                // Graceful NO_ERROR reset mid-coalesce: everything buffered so
                // far is the complete body. Deliver it.
                if is_graceful_response_eos(dir, &e) {
                    return Ok((buf.freeze(), CoalesceEnd::GracefulResetEos));
                }
                let be = dir.read_err(e);
                log_bridge_error(dir.label(), "read body error during peek", &be);
                return Err(be);
            }
        }
    }
    Ok((buf.freeze(), CoalesceEnd::Open))
}

/// Outcome of a non-suspending `poll_data` peek.
enum PeekOutcome {
    /// A chunk was already buffered; consumed by the peek.
    Chunk(Bytes),
    /// Reader has signalled end-of-stream.
    Eof,
    /// No chunk currently buffered; would have to await.
    Pending,
    /// Peek surfaced a read error.
    Error(h2::Error),
}

/// Try to pull a chunk from the reader without yielding. Uses a
/// `Waker::noop()` so h2's internal "wake on new data" doesn't fire
/// anything — fine because the outer loop's next `data().await` will
/// re-register a real waker.
fn try_peek_data(reader: &mut h2::RecvStream) -> PeekOutcome {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match reader.poll_data(&mut cx) {
        Poll::Ready(Some(Ok(b))) => PeekOutcome::Chunk(b),
        Poll::Ready(Some(Err(e))) => PeekOutcome::Error(e),
        Poll::Ready(None) => PeekOutcome::Eof,
        Poll::Pending => PeekOutcome::Pending,
    }
}

/// Wait until the SendStream's `poll_capacity` reports any non-zero value.
/// Returns the granted capacity in bytes, or an h2 error if the stream
/// errors before any is granted.
async fn poll_capacity(stream: &mut h2::SendStream<Bytes>) -> Result<usize, h2::Error> {
    std::future::poll_fn(|cx| stream.poll_capacity(cx))
        .await
        .unwrap_or(Ok(0))
}

#[cfg(test)]
mod tests {
    //! Bridge tests use a full sender↔cluster↔receiver triangle. They are
    //! larger and slower than unit tests; their value is in catching real
    //! flow-control and ordering bugs that wouldn't show up in isolation.

    use super::*;
    use bytes::Bytes;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};

    // --- is_benign_sender_close (finding 5.6/5.7 classification) ----------
    //
    // Only the `reason()`-constructible and variant cases are unit-testable:
    // h2 exposes no public constructor for remote resets or `User` errors, so
    // the `is_remote()` and `InactiveStreamId` branches are exercised by the
    // live Restate e2e (suspension test). The guard test below pins the
    // load-bearing inverse: a *non-CANCEL* `Reason` error must NOT be benign,
    // so a relay-caused fault keeps warning + counting as bridge_error.

    #[test]
    fn strip_reserved_removes_only_reserved_prefix() {
        // The sanitizer used by bridge_response_body (head) and finish_writer
        // (trailers) must drop the whole x-relay-internal-* namespace while
        // leaving every ordinary header/trailer field intact.
        let mut h = http::HeaderMap::new();
        h.insert("content-type", "text/plain".parse().unwrap());
        h.insert("x-relay-internal-result", "at-capacity".parse().unwrap());
        h.insert("x-relay-internal-version", "1".parse().unwrap());
        h.insert("x-custom", "keep".parse().unwrap());
        h.insert("grpc-status", "0".parse().unwrap()); // a trailer field
        strip_reserved_relay_headers(&mut h);
        assert!(!h.contains_key("x-relay-internal-result"));
        assert!(!h.contains_key("x-relay-internal-version"));
        assert_eq!(h.get("content-type").unwrap(), "text/plain");
        assert_eq!(h.get("x-custom").unwrap(), "keep");
        assert_eq!(h.get("grpc-status").unwrap(), "0");
    }

    #[test]
    fn strip_reserved_is_noop_without_reserved_fields() {
        let mut h = http::HeaderMap::new();
        h.insert("content-type", "application/json".parse().unwrap());
        strip_reserved_relay_headers(&mut h);
        assert_eq!(h.len(), 1);
        assert_eq!(h.get("content-type").unwrap(), "application/json");
    }

    #[test]
    fn cancel_reason_is_benign_sender_close() {
        let e = BridgeError::Sender(h2::Error::from(h2::Reason::CANCEL));
        assert!(e.is_benign_sender_close());
    }

    #[test]
    fn non_cancel_reason_is_not_benign() {
        // A non-CANCEL, non-remote sender error is a genuine fault.
        for reason in [
            h2::Reason::INTERNAL_ERROR,
            h2::Reason::PROTOCOL_ERROR,
            h2::Reason::FRAME_SIZE_ERROR,
        ] {
            let e = BridgeError::Sender(h2::Error::from(reason));
            assert!(!e.is_benign_sender_close(), "{reason:?} must not be benign");
        }
    }

    #[test]
    fn non_sender_variants_are_not_benign() {
        // Even a CANCEL on the *receiver* side is a receiver fault, and
        // FlowControlTimeout / Cancelled are never sender closes.
        assert!(
            !BridgeError::Receiver(h2::Error::from(h2::Reason::CANCEL)).is_benign_sender_close()
        );
        assert!(!BridgeError::FlowControlTimeout.is_benign_sender_close());
        assert!(!BridgeError::Cancelled.is_benign_sender_close());
    }

    /// Set up:
    ///   * a "fake sender" — an h2 client that hits the cluster on a TCP listener
    ///   * a "fake receiver" — an h2 server that's dialed by the cluster on another TCP listener
    ///   * the bridge wired between them
    ///
    /// Returns the body the receiver captured and the response body the
    /// sender captured. Both ends pump h2 properly (server stays in
    /// accept-loop, client drains response fully) so flow control and
    /// flushing work end-to-end.
    async fn run_bridge_test(request_body: &[u8], response_body: &[u8]) -> (Vec<u8>, Vec<u8>) {
        run_bridge_test_chunked(request_body, response_body, 1, 1).await
    }

    /// Same as [`run_bridge_test`] but the sender splits its request
    /// body into `req_chunks` `send_data` calls and the receiver
    /// splits its response into `resp_chunks` calls. Used to exercise
    /// the bridge's DATA-frame coalescing path under "drip" producers.
    async fn run_bridge_test_chunked(
        request_body: &[u8],
        response_body: &[u8],
        req_chunks: usize,
        resp_chunks: usize,
    ) -> (Vec<u8>, Vec<u8>) {
        run_bridge_test_full(
            request_body,
            response_body,
            req_chunks,
            resp_chunks,
            ReceiverEnding::Clean,
        )
        .await
    }

    /// How the fake receiver terminates the response stream.
    #[derive(Copy, Clone)]
    enum ReceiverEnding {
        /// Last DATA frame carries END_STREAM — the normal HTTP/2 close.
        Clean,
        /// Body DATA frames carry `end_of_stream=false`, then the receiver
        /// sends `RST_STREAM(NO_ERROR)`. Models hyper's unary-response idiom
        /// (used by the Restate Cloud tunnel client): a complete response
        /// followed by a graceful reset to reclaim the still-open request half.
        ResetNoError,
    }

    async fn run_bridge_test_full(
        request_body: &[u8],
        response_body: &[u8],
        req_chunks: usize,
        resp_chunks: usize,
        ending: ReceiverEnding,
    ) -> (Vec<u8>, Vec<u8>) {
        let receiver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver_listener.local_addr().unwrap();
        let response_body_vec = response_body.to_vec();

        // Fake receiver: handle one request, send the response, then keep
        // accept-polling to flush + drive the connection until the cluster
        // closes its side.
        let (recv_body_tx, recv_body_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let recv_task = tokio::spawn(async move {
            let (socket, _) = receiver_listener.accept().await.unwrap();
            let mut server = h2::server::handshake(socket).await.unwrap();

            // Spawn the request handler so the main loop can keep polling.
            let mut recv_body_tx = Some(recv_body_tx);
            while let Some(req) = server.accept().await {
                let (request, mut respond) = req.unwrap();
                let response_body_vec = response_body_vec.clone();
                let tx = recv_body_tx.take();
                tokio::spawn(async move {
                    let (_parts, mut body) = request.into_parts();
                    let mut got_body = Vec::new();
                    while let Some(chunk) = body.data().await {
                        let chunk = chunk.unwrap();
                        body.flow_control().release_capacity(chunk.len()).unwrap();
                        got_body.extend_from_slice(&chunk);
                    }
                    if let Some(tx) = tx {
                        let _ = tx.send(got_body);
                    }

                    let response = http::Response::builder()
                        .status(http::StatusCode::OK)
                        .body(())
                        .unwrap();
                    let mut send = respond.send_response(response, false).unwrap();
                    match ending {
                        ReceiverEnding::Clean => {
                            // Send the response body either in one frame or in
                            // `resp_chunks` smaller ones — the latter exercises
                            // the bridge's coalescing path on the downstream
                            // direction. With `resp_chunks == 1` this is a
                            // single `send_data` call.
                            send_in_chunks(&mut send, &response_body_vec, resp_chunks);
                        }
                        ReceiverEnding::ResetNoError => {
                            // Body without END_STREAM, then a graceful reset —
                            // hyper's unary-response close. The bridge must
                            // still deliver the full body to the sender.
                            //
                            // Yield first so the response HEADERS frame is
                            // flushed and the relay's response future resolves
                            // `Ok(200)` *before* the body + reset arrive —
                            // matching the real networked ordering (the live
                            // run sees a clean 200, then the reset hits the body
                            // pump). Without the gap the in-process h2 races and
                            // the reset can pre-empt the headers future, which is
                            // a harness artifact, not the path under test.
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            if !response_body_vec.is_empty() {
                                send.send_data(Bytes::from(response_body_vec.clone()), false)
                                    .unwrap();
                                // Let the DATA frame flush to the relay before
                                // the reset — hyper resets only *after* the
                                // complete body is on the wire. Sending the
                                // reset immediately would drop the unflushed
                                // DATA frame (an artifact of the synchronous
                                // in-process send, not the behaviour under test).
                                tokio::time::sleep(Duration::from_millis(25)).await;
                            }
                            send.send_reset(h2::Reason::NO_ERROR);
                        }
                    }
                });
            }
        });

        // Cluster side: dial the fake receiver as h2 client.
        let cluster_recv_sock = TcpStream::connect(receiver_addr).await.unwrap();
        let (send_request, connection) = h2::client::handshake(cluster_recv_sock).await.unwrap();
        let recv_conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut send_request = send_request.ready().await.unwrap();

        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/invoke/test")
            .body(())
            .unwrap();
        let (receiver_response, receiver_send) = send_request.send_request(req, false).unwrap();

        // Fake sender + cluster's sender-side server.
        let sender_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_listener.local_addr().unwrap();
        let request_body_vec = request_body.to_vec();

        let sender_task = tokio::spawn(async move {
            let socket = TcpStream::connect(sender_addr).await.unwrap();
            let (h2, connection) = h2::client::handshake(socket).await.unwrap();
            let sender_conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri("/c1/invoke/test")
                .body(())
                .unwrap();
            let (resp, mut body) = h2.send_request(request, false).unwrap();
            send_in_chunks(&mut body, &request_body_vec, req_chunks);

            let response = resp.await.unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            let (_parts, mut response_body) = response.into_parts();
            let mut got_response_body = Vec::new();
            while let Some(chunk) = response_body.data().await {
                let chunk = chunk.unwrap();
                response_body
                    .flow_control()
                    .release_capacity(chunk.len())
                    .unwrap();
                got_response_body.extend_from_slice(&chunk);
            }
            sender_conn_task.abort();
            got_response_body
        });

        // Cluster's sender-facing h2 server: accept the sender's connection.
        let (cluster_sender_sock, _) = sender_listener.accept().await.unwrap();
        let mut sender_server = h2::server::handshake(cluster_sender_sock).await.unwrap();
        let (sender_request, mut sender_respond) = sender_server.accept().await.unwrap().unwrap();
        let (_sender_parts, sender_body) = sender_request.into_parts();

        // Run the bridge. The sender-server must continue to be polled
        // to drive the h2 connection on the sender side; spawn a tiny task
        // that keeps draining accept() (it'll return None once the sender
        // closes).
        let sender_server_drain =
            tokio::spawn(async move { while sender_server.accept().await.is_some() {} });

        let config = BridgeConfig::default();
        let up = bridge_upstream(sender_body, receiver_send, &config);
        let down = bridge_downstream(receiver_response, &mut sender_respond, &config);
        let (up_res, down_res) = tokio::join!(up, down);
        down_res.unwrap();
        match ending {
            ReceiverEnding::Clean => up_res.unwrap(),
            // Under a graceful full-stream reset, the request pump may race the
            // receiver's reset and see a benign `Receiver(Reset NO_ERROR)` write
            // error. In production `forward.rs` cancels the request pump the
            // moment the (unary) response completes, so this never surfaces; the
            // load-bearing contract here is only that DOWNSTREAM delivered the
            // body cleanly (asserted above + by the captured response body).
            ReceiverEnding::ResetNoError => {
                let _ = up_res;
            }
        }

        let received_response_body = tokio::time::timeout(Duration::from_secs(5), sender_task)
            .await
            .unwrap()
            .unwrap();
        let received_request_body = tokio::time::timeout(Duration::from_secs(5), recv_body_rx)
            .await
            .unwrap()
            .unwrap();

        // Clean up background tasks.
        sender_server_drain.abort();
        recv_conn_task.abort();
        recv_task.abort();

        (received_request_body, received_response_body)
    }

    /// Send `body` over `stream` in `chunks` near-equal `send_data`
    /// calls; only the last carries `end_of_stream=true`. With
    /// `chunks == 1` this is the original one-shot send.
    fn send_in_chunks(stream: &mut h2::SendStream<Bytes>, body: &[u8], chunks: usize) {
        if body.is_empty() {
            stream.send_data(Bytes::new(), true).unwrap();
            return;
        }
        let chunks = chunks.max(1);
        let chunk_size = body.len().div_ceil(chunks);
        let mut offset = 0;
        while offset < body.len() {
            let end = (offset + chunk_size).min(body.len());
            let eos = end == body.len();
            stream
                .send_data(Bytes::copy_from_slice(&body[offset..end]), eos)
                .unwrap();
            offset = end;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn round_trip_small_body() {
        let (req_seen_by_receiver, resp_seen_by_sender) =
            run_bridge_test(b"hello world", b"goodbye world").await;
        assert_eq!(req_seen_by_receiver, b"hello world");
        assert_eq!(resp_seen_by_sender, b"goodbye world");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn round_trip_empty_bodies() {
        let (req_seen_by_receiver, resp_seen_by_sender) = run_bridge_test(b"", b"").await;
        assert_eq!(req_seen_by_receiver, b"");
        assert_eq!(resp_seen_by_sender, b"");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn round_trip_medium_body() {
        let req: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let resp: Vec<u8> = (0..20_000).map(|i| ((i * 7) % 256) as u8).collect();
        let (req_seen, resp_seen) = run_bridge_test(&req, &resp).await;
        assert_eq!(req_seen, req);
        assert_eq!(resp_seen, resp);
    }

    /// A receiver that finishes a unary response with `RST_STREAM(NO_ERROR)`
    /// instead of END_STREAM (hyper's idiom — what the unmodified Restate Cloud
    /// tunnel client does) must still have its full body delivered to the
    /// sender. Regression for the body-truncation found running the real
    /// client against the relay: the graceful reset surfaced during the
    /// coalesce peek and discarded the already-received body. The harness's
    /// `down_res.unwrap()` asserts the downstream bridge returns `Ok`.
    #[test]
    fn upstream_graceful_peer_close_predicate() {
        use PumpDirection::{Downstream, Upstream};
        // Upstream: a receiver CANCEL while we're still writing the request is a
        // graceful close (it took what it needed) — request forwarding is done.
        assert!(write_hit_graceful_peer_close(
            Upstream,
            &h2::Error::from(h2::Reason::CANCEL)
        ));
        // Upstream: a genuine receiver fault is NOT a graceful close.
        assert!(!write_hit_graceful_peer_close(
            Upstream,
            &h2::Error::from(h2::Reason::INTERNAL_ERROR)
        ));
        assert!(!write_hit_graceful_peer_close(
            Upstream,
            &h2::Error::from(h2::Reason::PROTOCOL_ERROR)
        ));
        // Downstream (response → sender) is out of scope here — a sender close
        // is classified separately. Even a CANCEL must return false.
        assert!(!write_hit_graceful_peer_close(
            Downstream,
            &h2::Error::from(h2::Reason::CANCEL)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn downstream_no_error_reset_delivers_full_body() {
        let (_req_seen, resp_seen) = run_bridge_test_full(
            b"req",
            b"hello-from-target\n",
            1,
            1,
            ReceiverEnding::ResetNoError,
        )
        .await;
        assert_eq!(resp_seen, b"hello-from-target\n");
    }

    /// Same graceful-reset close, but with an empty response body: the reset
    /// surfaces on the very first `data().await` (no buffered DATA), exercising
    /// the main-pump-loop arm rather than the coalesce peek. The sender must
    /// see a clean, empty `200`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn downstream_no_error_reset_empty_body_is_clean_eos() {
        let (_req_seen, resp_seen) =
            run_bridge_test_full(b"req", b"", 1, 1, ReceiverEnding::ResetNoError).await;
        assert_eq!(resp_seen, b"");
    }

    // ── Peek→bridge accounting handoff (remediation Gate C, Item 4) ───────

    /// A test [`AccountingHandoff`] guard: on Drop it increments a shared
    /// counter (release count) and snapshots the meter's value at that instant,
    /// so a test can prove the pump published held bytes into the meter BEFORE
    /// releasing the guard (publish-then-release ordering) and released it
    /// exactly once.
    struct ReleaseSpy {
        released: Arc<AtomicI64>,
        meter_at_release: Arc<AtomicI64>,
        meter: Arc<AtomicI64>,
    }
    impl Drop for ReleaseSpy {
        fn drop(&mut self) {
            self.meter_at_release
                .store(self.meter.load(Ordering::SeqCst), Ordering::SeqCst);
            self.released.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The upstream pump, given an [`AccountingHandoff`], releases it exactly
    /// once while forwarding a normal request body, and the shared buffer meter
    /// returns to zero at the end (the handoff never leaks the meter). Proves the
    /// peek→bridge handoff runs and releases on the healthy path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn handoff_guard_released_exactly_once_by_upstream_pump() {
        let meter_counter = Arc::new(AtomicI64::new(0));
        let released = Arc::new(AtomicI64::new(0));
        let meter_at_release = Arc::new(AtomicI64::new(-1));

        // Wire a real sender↔relay↔receiver triangle, running the UPSTREAM pump
        // with a handoff guard and the DOWNSTREAM pump normally.
        let receiver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver_listener.local_addr().unwrap();
        let (recv_body_tx, recv_body_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let recv_task = tokio::spawn(async move {
            let (socket, _) = receiver_listener.accept().await.unwrap();
            let mut server = h2::server::handshake(socket).await.unwrap();
            let mut recv_body_tx = Some(recv_body_tx);
            while let Some(req) = server.accept().await {
                let (request, mut respond) = req.unwrap();
                let tx = recv_body_tx.take();
                tokio::spawn(async move {
                    let (_p, mut body) = request.into_parts();
                    let mut got = Vec::new();
                    while let Some(chunk) = body.data().await {
                        let chunk = chunk.unwrap();
                        body.flow_control().release_capacity(chunk.len()).unwrap();
                        got.extend_from_slice(&chunk);
                    }
                    if let Some(tx) = tx {
                        let _ = tx.send(got);
                    }
                    let response = http::Response::builder()
                        .status(http::StatusCode::OK)
                        .body(())
                        .unwrap();
                    let mut send = respond.send_response(response, false).unwrap();
                    send.send_data(Bytes::new(), true).unwrap();
                });
            }
        });

        let cluster_recv_sock = TcpStream::connect(receiver_addr).await.unwrap();
        let (send_request, connection) = h2::client::handshake(cluster_recv_sock).await.unwrap();
        let recv_conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut send_request = send_request.ready().await.unwrap();
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/invoke/test")
            .body(())
            .unwrap();
        let (receiver_response, receiver_send) = send_request.send_request(req, false).unwrap();

        let sender_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_listener.local_addr().unwrap();
        let body_bytes: Vec<u8> = (0..40_000).map(|i| (i % 251) as u8).collect();
        let body_for_sender = body_bytes.clone();
        let sender_task = tokio::spawn(async move {
            let socket = TcpStream::connect(sender_addr).await.unwrap();
            let (h2, connection) = h2::client::handshake(socket).await.unwrap();
            let sender_conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri("/c1/invoke/test")
                .body(())
                .unwrap();
            let (resp, mut body) = h2.send_request(request, false).unwrap();
            body.send_data(Bytes::from(body_for_sender), true).unwrap();
            let response = resp.await.unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            let (_p, mut rb) = response.into_parts();
            while let Some(chunk) = rb.data().await {
                let chunk = chunk.unwrap();
                rb.flow_control().release_capacity(chunk.len()).unwrap();
            }
            sender_conn_task.abort();
        });

        let (cluster_sender_sock, _) = sender_listener.accept().await.unwrap();
        let mut sender_server = h2::server::handshake(cluster_sender_sock).await.unwrap();
        let (sender_request, mut sender_respond) = sender_server.accept().await.unwrap().unwrap();
        let (_p, sender_body) = sender_request.into_parts();
        let sender_server_drain =
            tokio::spawn(async move { while sender_server.accept().await.is_some() {} });

        let config = BridgeConfig {
            buffer_meter: BufferMeter::new(meter_counter.clone()),
            ..BridgeConfig::default()
        };
        let spy = ReleaseSpy {
            released: released.clone(),
            meter_at_release: meter_at_release.clone(),
            meter: meter_counter.clone(),
        };
        let up = bridge_upstream_with_handoff(
            sender_body,
            receiver_send,
            &config,
            Some(Box::new(spy) as AccountingHandoff),
        );
        let down = bridge_downstream(receiver_response, &mut sender_respond, &config);
        let (up_res, down_res) = tokio::join!(up, down);
        up_res.unwrap();
        down_res.unwrap();

        let got_req = tokio::time::timeout(Duration::from_secs(5), recv_body_rx)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), sender_task)
            .await
            .unwrap()
            .unwrap();

        // The guard was released exactly once by the pump…
        assert_eq!(
            released.load(Ordering::SeqCst),
            1,
            "the handoff guard must be released exactly once"
        );
        // …the release read a non-negative meter (publish-then-release: the meter
        // is reconciled at the handoff before the guard drops — never negative)…
        assert!(
            meter_at_release.load(Ordering::SeqCst) >= 0,
            "meter must be reconciled (>=0) at the instant the guard is released"
        );
        // …the full body round-tripped…
        assert_eq!(
            got_req, body_bytes,
            "the request body must round-trip intact"
        );
        // …and the shared meter telescopes back to zero (no leak from handoff).
        assert_eq!(
            meter_counter.load(Ordering::SeqCst),
            0,
            "the buffer meter must return to zero once the bridge completes"
        );

        sender_server_drain.abort();
        recv_conn_task.abort();
        recv_task.abort();
    }

    /// If the upstream-pump future is dropped **before its first poll** (the
    /// cancellation-before-bridge case — e.g. `run_duplex` dropping it because
    /// the other half errored first), the handoff guard is still released
    /// exactly once. Proves the RAII fallback: no leak when the pump never runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn handoff_guard_released_when_pump_future_dropped_unpolled() {
        // Build a real upstream stream pair, then drop the pump future without
        // ever awaiting it.
        let receiver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver_listener.local_addr().unwrap();
        let recv_task = tokio::spawn(async move {
            let (socket, _) = receiver_listener.accept().await.unwrap();
            let mut server = h2::server::handshake(socket).await.unwrap();
            while server.accept().await.is_some() {}
        });
        let cluster_recv_sock = TcpStream::connect(receiver_addr).await.unwrap();
        let (send_request, connection) = h2::client::handshake(cluster_recv_sock).await.unwrap();
        let recv_conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut send_request = send_request.ready().await.unwrap();
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/invoke/test")
            .body(())
            .unwrap();
        let (_receiver_response, receiver_send) = send_request.send_request(req, false).unwrap();

        let sender_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_listener.local_addr().unwrap();
        let sender_task = tokio::spawn(async move {
            let socket = TcpStream::connect(sender_addr).await.unwrap();
            let (h2, connection) = h2::client::handshake(socket).await.unwrap();
            let drive = tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri("/c1/invoke/test")
                .body(())
                .unwrap();
            let (_resp, mut body) = h2.send_request(request, false).unwrap();
            let _ = body.send_data(Bytes::new(), true);
            // Keep the connection alive briefly.
            tokio::time::sleep(Duration::from_millis(50)).await;
            drive.abort();
        });
        let (cluster_sender_sock, _) = sender_listener.accept().await.unwrap();
        let mut sender_server = h2::server::handshake(cluster_sender_sock).await.unwrap();
        let (sender_request, _sender_respond) = sender_server.accept().await.unwrap().unwrap();
        let (_p, sender_body) = sender_request.into_parts();

        let released = Arc::new(AtomicI64::new(0));
        let spy = ReleaseSpy {
            released: released.clone(),
            meter_at_release: Arc::new(AtomicI64::new(0)),
            meter: Arc::new(AtomicI64::new(0)),
        };
        let config = BridgeConfig::default();
        let up = bridge_upstream_with_handoff(
            sender_body,
            receiver_send,
            &config,
            Some(Box::new(spy) as AccountingHandoff),
        );
        // Drop the future WITHOUT polling it — the guard it captured must drop.
        drop(up);
        assert_eq!(
            released.load(Ordering::SeqCst),
            1,
            "dropping the un-polled pump future must release the handoff guard exactly once"
        );

        recv_conn_task.abort();
        recv_task.abort();
        let _ = sender_task.await;
    }

    /// Coalescing correctness: sender drips the request body in many
    /// small DATA frames; bridge must deliver the full payload
    /// byte-identical to the receiver. Each chunk is sent in its own
    /// `send_data` call which lands as a separate DATA frame on the
    /// wire; the bridge's coalescing peek then concatenates whatever
    /// is already buffered into a single forwarded `send_data` call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesces_dripped_request_body_lossless() {
        // 32 chunks × 128 bytes = 4 KiB body. Distinct byte values
        // per chunk so a re-ordering bug would show up in the diff.
        let req: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let (req_seen, resp_seen) = run_bridge_test_chunked(
            &req, b"ok", /*req_chunks=*/ 32, /*resp_chunks=*/ 1,
        )
        .await;
        assert_eq!(
            req_seen, req,
            "dripped request body must round-trip byte-exact"
        );
        assert_eq!(resp_seen, b"ok");
    }

    /// Same but on the response side: receiver drips, bridge coalesces
    /// downstream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesces_dripped_response_body_lossless() {
        let resp: Vec<u8> = (0..8192).map(|i| ((i * 11) % 256) as u8).collect();
        let (req_seen, resp_seen) = run_bridge_test_chunked(
            b"ping", &resp, /*req_chunks=*/ 1, /*resp_chunks=*/ 64,
        )
        .await;
        assert_eq!(req_seen, b"ping");
        assert_eq!(
            resp_seen, resp,
            "dripped response body must round-trip byte-exact"
        );
    }

    /// Bidirectional drip — both ends fragment their bodies.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesces_both_directions_lossless() {
        let req: Vec<u8> = (0..16_384).map(|i| (i % 256) as u8).collect();
        let resp: Vec<u8> = (0..16_384).map(|i| ((i * 13) % 256) as u8).collect();
        let (req_seen, resp_seen) = run_bridge_test_chunked(&req, &resp, 16, 32).await;
        assert_eq!(req_seen, req);
        assert_eq!(resp_seen, resp);
    }

    /// Boundary: a body larger than the COALESCE_CAP_BYTES (64 KiB).
    /// Coalescing must stop at the cap and the remainder ride the
    /// next outer-loop iteration; no bytes are lost or duplicated.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesces_at_cap_lossless() {
        // 128 KiB total, dripped in 256 chunks of 512 bytes each.
        // After coalescing the first ~128 chunks (~64 KiB) we hit the
        // cap, send, then loop and pick up the rest.
        let req: Vec<u8> = (0..131_072).map(|i| ((i * 17) % 256) as u8).collect();
        let (req_seen, resp_seen) = run_bridge_test_chunked(&req, b"k", 256, 1).await;
        assert_eq!(req_seen, req);
        assert_eq!(resp_seen, b"k");
    }

    /// Trailers must round-trip verbatim in BOTH directions (design
    /// §4.2(e)/§4.3). The sender attaches a request trailer after its
    /// body; the receiver attaches a response trailer after its body.
    /// Each side must observe the other's trailer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn forwards_trailers_both_directions() {
        use http::HeaderMap;

        let receiver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver_listener.local_addr().unwrap();

        // Receiver: drain the request body, capture its trailers, then
        // respond with a body + a response trailer.
        let (req_tr_tx, req_tr_rx) = tokio::sync::oneshot::channel::<Option<HeaderMap>>();
        let recv_task = tokio::spawn(async move {
            let (socket, _) = receiver_listener.accept().await.unwrap();
            let mut server = h2::server::handshake(socket).await.unwrap();
            let mut req_tr_tx = Some(req_tr_tx);
            while let Some(req) = server.accept().await {
                let (request, mut respond) = req.unwrap();
                let tx = req_tr_tx.take();
                tokio::spawn(async move {
                    let (_p, mut body) = request.into_parts();
                    while let Some(c) = body.data().await {
                        let c = c.unwrap();
                        body.flow_control().release_capacity(c.len()).unwrap();
                    }
                    let trailers = body.trailers().await.unwrap();
                    if let Some(tx) = tx {
                        let _ = tx.send(trailers);
                    }
                    let response = http::Response::builder()
                        .status(http::StatusCode::OK)
                        .body(())
                        .unwrap();
                    let mut send = respond.send_response(response, false).unwrap();
                    send.send_data(Bytes::from_static(b"resp-body"), false)
                        .unwrap();
                    let mut tr = HeaderMap::new();
                    tr.insert("x-resp-trailer", "resp-val".parse().unwrap());
                    send.send_trailers(tr).unwrap();
                });
            }
        });

        let cluster_recv_sock = TcpStream::connect(receiver_addr).await.unwrap();
        let (send_request, connection) = h2::client::handshake(cluster_recv_sock).await.unwrap();
        let recv_conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut send_request = send_request.ready().await.unwrap();
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/invoke/h/9080/test")
            .body(())
            .unwrap();
        let (receiver_response, receiver_send) = send_request.send_request(req, false).unwrap();

        let sender_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_listener.local_addr().unwrap();

        // Sender: send a body + a request trailer; read the response body
        // and capture the response trailer.
        let (resp_tr_tx, resp_tr_rx) = tokio::sync::oneshot::channel::<Option<HeaderMap>>();
        let sender_task = tokio::spawn(async move {
            let socket = TcpStream::connect(sender_addr).await.unwrap();
            let (h2, connection) = h2::client::handshake(socket).await.unwrap();
            let sender_conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri("/invoke/c/h/9080/test")
                .body(())
                .unwrap();
            let (resp, mut body) = h2.send_request(request, false).unwrap();
            body.send_data(Bytes::from_static(b"req-body"), false)
                .unwrap();
            let mut tr = HeaderMap::new();
            tr.insert("x-req-trailer", "req-val".parse().unwrap());
            body.send_trailers(tr).unwrap();

            let response = resp.await.unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            let (_p, mut rb) = response.into_parts();
            while let Some(c) = rb.data().await {
                let c = c.unwrap();
                rb.flow_control().release_capacity(c.len()).unwrap();
            }
            let resp_trailers = rb.trailers().await.unwrap();
            let _ = resp_tr_tx.send(resp_trailers);
            sender_conn_task.abort();
        });

        let (cluster_sender_sock, _) = sender_listener.accept().await.unwrap();
        let mut sender_server = h2::server::handshake(cluster_sender_sock).await.unwrap();
        let (sender_request, mut sender_respond) = sender_server.accept().await.unwrap().unwrap();
        let (_p, sender_body) = sender_request.into_parts();
        let sender_server_drain =
            tokio::spawn(async move { while sender_server.accept().await.is_some() {} });

        let config = BridgeConfig::default();
        let up = bridge_upstream(sender_body, receiver_send, &config);
        let down = bridge_downstream(receiver_response, &mut sender_respond, &config);
        let (up_res, down_res) = tokio::join!(up, down);
        up_res.unwrap();
        down_res.unwrap();

        let req_trailers = tokio::time::timeout(Duration::from_secs(5), req_tr_rx)
            .await
            .unwrap()
            .unwrap()
            .expect("receiver should have seen request trailers");
        let resp_trailers = tokio::time::timeout(Duration::from_secs(5), resp_tr_rx)
            .await
            .unwrap()
            .unwrap()
            .expect("sender should have seen response trailers");

        sender_server_drain.abort();
        recv_conn_task.abort();
        recv_task.abort();
        let _ = sender_task.await;

        assert_eq!(req_trailers.get("x-req-trailer").unwrap(), "req-val");
        assert_eq!(resp_trailers.get("x-resp-trailer").unwrap(), "resp-val");
    }

    /// A bodyless response carrying only trailers (HEADERS → trailing
    /// HEADERS, no DATA) must still deliver the trailers — this exercises
    /// `finish_writer` when `data()` returns None immediately. gRPC's
    /// trailers-only responses have this shape.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn forwards_trailers_on_bodyless_response() {
        use http::HeaderMap;

        let receiver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver_listener.local_addr().unwrap();
        let recv_task = tokio::spawn(async move {
            let (socket, _) = receiver_listener.accept().await.unwrap();
            let mut server = h2::server::handshake(socket).await.unwrap();
            while let Some(req) = server.accept().await {
                let (request, mut respond) = req.unwrap();
                tokio::spawn(async move {
                    let (_p, mut body) = request.into_parts();
                    while let Some(c) = body.data().await {
                        let c = c.unwrap();
                        body.flow_control().release_capacity(c.len()).unwrap();
                    }
                    let _ = body.trailers().await;
                    // Response head, then trailers — no DATA frame at all.
                    let response = http::Response::builder()
                        .status(http::StatusCode::OK)
                        .body(())
                        .unwrap();
                    let mut send = respond.send_response(response, false).unwrap();
                    let mut tr = HeaderMap::new();
                    tr.insert("grpc-status", "0".parse().unwrap());
                    send.send_trailers(tr).unwrap();
                });
            }
        });

        let cluster_recv_sock = TcpStream::connect(receiver_addr).await.unwrap();
        let (send_request, connection) = h2::client::handshake(cluster_recv_sock).await.unwrap();
        let recv_conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut send_request = send_request.ready().await.unwrap();
        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/invoke/h/9080/test")
            .body(())
            .unwrap();
        // eos=false: the bridge owns closing this stream (it pumps the
        // sender's empty body through and emits the end-of-stream frame).
        let (receiver_response, receiver_send) = send_request.send_request(req, false).unwrap();

        let sender_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sender_addr = sender_listener.local_addr().unwrap();
        let (resp_tr_tx, resp_tr_rx) = tokio::sync::oneshot::channel::<Option<HeaderMap>>();
        let sender_task = tokio::spawn(async move {
            let socket = TcpStream::connect(sender_addr).await.unwrap();
            let (h2, connection) = h2::client::handshake(socket).await.unwrap();
            let sender_conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri("/invoke/c/h/9080/test")
                .body(())
                .unwrap();
            let (resp, mut send_body) = h2.send_request(request, false).unwrap();
            send_body.send_data(Bytes::new(), true).unwrap();
            let response = resp.await.unwrap();
            let (_p, mut rb) = response.into_parts();
            while let Some(c) = rb.data().await {
                let c = c.unwrap();
                rb.flow_control().release_capacity(c.len()).unwrap();
            }
            let resp_trailers = rb.trailers().await.unwrap();
            let _ = resp_tr_tx.send(resp_trailers);
            sender_conn_task.abort();
        });

        let (cluster_sender_sock, _) = sender_listener.accept().await.unwrap();
        let mut sender_server = h2::server::handshake(cluster_sender_sock).await.unwrap();
        let (sender_request, mut sender_respond) = sender_server.accept().await.unwrap().unwrap();
        let (_p, sender_body) = sender_request.into_parts();
        let sender_server_drain =
            tokio::spawn(async move { while sender_server.accept().await.is_some() {} });

        let config = BridgeConfig::default();
        let up = bridge_upstream(sender_body, receiver_send, &config);
        let down = bridge_downstream(receiver_response, &mut sender_respond, &config);
        let (up_res, down_res) = tokio::join!(up, down);
        up_res.unwrap();
        down_res.unwrap();

        let resp_trailers = tokio::time::timeout(Duration::from_secs(5), resp_tr_rx)
            .await
            .unwrap()
            .unwrap()
            .expect("sender should have seen response trailers on a bodyless response");

        sender_server_drain.abort();
        recv_conn_task.abort();
        recv_task.abort();
        let _ = sender_task.await;

        assert_eq!(resp_trailers.get("grpc-status").unwrap(), "0");
    }

    // --- run_duplex coupling (F2) ---

    /// When one half errors, the other is cancelled immediately — even with
    /// no drain timeout and even if the other would otherwise never finish.
    /// This is the property a bare `tokio::join!` lacked (it would hang on
    /// the pending half forever).
    #[tokio::test]
    async fn errored_half_cancels_the_other_immediately() {
        let cfg = BridgeConfig::default(); // half_close_drain_timeout = None
        let up = async { Err::<(), BridgeError>(BridgeError::FlowControlTimeout) };
        let down = std::future::pending::<Result<(), BridgeError>>();

        let res = tokio::time::timeout(Duration::from_secs(2), run_duplex(up, down, &cfg)).await;
        let (up_res, down_res) = res.expect("run_duplex must return promptly, not hang on `down`");
        assert!(matches!(up_res, Err(BridgeError::FlowControlTimeout)));
        assert!(matches!(down_res, Err(BridgeError::Cancelled)));
    }

    /// Symmetric: a downstream error cancels a never-finishing upstream.
    #[tokio::test]
    async fn errored_downstream_cancels_upstream_immediately() {
        let cfg = BridgeConfig::default();
        let up = std::future::pending::<Result<(), BridgeError>>();
        let down = async { Err::<(), BridgeError>(BridgeError::FlowControlTimeout) };

        let res = tokio::time::timeout(Duration::from_secs(2), run_duplex(up, down, &cfg)).await;
        let (up_res, down_res) = res.expect("run_duplex must return promptly");
        assert!(matches!(up_res, Err(BridgeError::Cancelled)));
        assert!(matches!(down_res, Err(BridgeError::FlowControlTimeout)));
    }

    /// A clean half-close with the drain timeout set bounds an otherwise
    /// idle survivor — the half-open stream can't pin resources forever.
    #[tokio::test]
    async fn clean_close_bounds_idle_survivor_when_drain_timeout_set() {
        let cfg = BridgeConfig {
            half_close_drain_timeout: Some(Duration::from_millis(50)),
            ..BridgeConfig::default()
        };
        let up = async { Ok::<(), BridgeError>(()) };
        let down = std::future::pending::<Result<(), BridgeError>>();

        let res = tokio::time::timeout(Duration::from_secs(2), run_duplex(up, down, &cfg)).await;
        let (up_res, down_res) = res.expect("run_duplex must return within the drain timeout");
        assert!(up_res.is_ok());
        assert!(matches!(down_res, Err(BridgeError::Cancelled)));
    }

    /// No-regression: with the default (no drain timeout), a clean half-close
    /// does NOT cancel the survivor — it keeps running to completion. This is
    /// the legitimate server-streaming / long-upload case.
    #[tokio::test]
    async fn clean_close_lets_survivor_finish_when_unbounded() {
        let cfg = BridgeConfig::default(); // None → unbounded survivor
        let up = async { Ok::<(), BridgeError>(()) };
        let down = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<(), BridgeError>(())
        };
        let (up_res, down_res) = run_duplex(up, down, &cfg).await;
        assert!(up_res.is_ok());
        assert!(down_res.is_ok());
    }

    /// Both halves completing cleanly returns `(Ok, Ok)`.
    #[tokio::test]
    async fn both_clean_returns_ok_ok() {
        let cfg = BridgeConfig::default();
        let up = async { Ok::<(), BridgeError>(()) };
        let down = async { Ok::<(), BridgeError>(()) };
        let (up_res, down_res) = run_duplex(up, down, &cfg).await;
        assert!(up_res.is_ok() && down_res.is_ok());
    }
}
