//! The `InvokeHandler` seam — the one thing an embedder customises: whether to
//! dial a target `host:port`, serve the invocation in-process, or anything else.
//! The runtime owns all the relay-facing machinery and calls a handler per
//! forwarded stream.

use crate::relay::protocol::ForwardedTarget;
use bytes::Bytes;

/// One forwarded invocation handed to an [`InvokeHandler`].
///
/// The runtime has already validated the path into `target` (host/port/tail)
/// and stripped the relay framing. The handler owns both halves of the h2
/// stream — `body` (the request bytes coming from the relay) and `respond`
/// (where it writes the response) — so it can stream in both directions
/// without buffering, matching the bidi model.
///
/// **Contract:** the handler MUST send a response on `respond` (a success
/// status or an error status). If it drops `respond` without sending, the
/// relay observes the stream reset and the sender gets a 500.
pub struct Invocation {
    /// The target the sender named: where to dial and what path to use.
    pub target: ForwardedTarget,
    /// The sender's method, forwarded verbatim.
    pub method: http::Method,
    /// The sender's headers, forwarded verbatim (relay framing already
    /// stripped). The handler is responsible for any hop-by-hop hygiene.
    pub headers: http::HeaderMap,
    /// Request body bytes streaming in from the relay.
    pub body: h2::RecvStream,
    /// Response sink back to the relay (and thence the sender).
    pub respond: h2::server::SendResponse<Bytes>,
}

/// Handles forwarded invocations. One handler instance is shared
/// (`Arc`-wrapped by the runtime) across all streams on all connections, so
/// implementations are `Send + Sync` and hold their own shared state.
#[async_trait::async_trait]
pub trait InvokeHandler: Send + Sync + 'static {
    /// Serve one invocation. See [`Invocation`] for the response contract.
    async fn handle(&self, invocation: Invocation);
}
