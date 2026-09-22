//! The relay-facing receiver runtime.
//!
//! A receiver dials *out* to the relay's `:8080`, lets the relay drive the
//! HTTP/2 role-flip (so the receiver is the h2 *server* on its own
//! outbound socket), answers `GET /whoami` with its `{api_key, tunnel}`,
//! and then serves the forwarded streams the relay opens
//! (`/invoke/<host>/<port>/<tail>`). See the relay `design.md` §4.1.
//!
//! Everything relay-facing — dial-in, role-flip, /whoami, stream
//! multiplexing, redial-on-drop — lives here. What to *do* with a
//! forwarded invocation is the one thing left open, via the
//! [`InvokeHandler`] trait:
//!
//! ```ignore
//! use restate_sdk_shared_core::relay::receiver::{Receiver, ReceiverConfig, InvokeHandler, Invocation};
//! use restate_sdk_shared_core::relay::protocol::{Env, Tunnel};
//! use std::sync::Arc;
//!
//! struct MyHandler;
//! #[async_trait::async_trait]
//! impl InvokeHandler for MyHandler {
//!     async fn handle(&self, inv: Invocation) {
//!         // dial inv.target.host:inv.target.port, forward inv.tail, …
//!     }
//! }
//!
//! let cfg = ReceiverConfig::new("relay.internal:8080", Env::new("e1")?, Tunnel::new("c1")?, "api-key");
//! Receiver::new(cfg, Arc::new(MyHandler)).run(shutdown_future).await;
//! ```
//!
//! An embedder chooses what the handler does: dial the named `host:port` and
//! bridge, serve the invocation in-process, or anything else. The embedded
//! [`crate::relay::loopback`] engine supplies a handler that bridges each
//! forwarded stream to a local HTTP/2 server over a loopback socket.

mod handler;
mod runtime;

pub use handler::{Invocation, InvokeHandler};
pub use runtime::{ConnError, Receiver, ReceiverConfig, TlsClient, TlsConfigError};

// Re-export the protocol types that appear in this module's public API so
// callers don't need to reach into `crate::relay::protocol` just to name them.
pub use super::protocol::{Env, ForwardedTarget, H2Tuning, Tunnel};
