//! Embedded relay-tunnel receiver — **compiled only under the `tunnel`
//! feature** (see `docs/relay-receiver-in-shared-core.md`).
//!
//! With the feature off (the default), none of this compiles and the crate
//! stays strictly sans-IO / WASM-compilable — no tokio, no networking in the
//! dependency graph. With it on, an SDK can act as a relay *receiver*: register
//! with the relay, accept the HTTP/2 role-flip, and bridge each forwarded
//! request to the SDK's own local HTTP/2 server over a loopback socket.
//!
//! The stack is a fork of the relay repo's `relay-protocol` / `relay-bridge` /
//! `relay-receiver` crates plus the loopback engine that ties them together:
//!
//! * [`protocol`] — the runtime-agnostic wire contract (env/tunnel, path
//!   grammar, `/whoami`, h2 tuning). Sans-IO.
//! * [`bridge`] — the bidirectional HTTP/2 stream bridge.
//! * [`receiver`] — the relay-facing receiver runtime (dial-out, role-flip,
//!   `/whoami`, liveness, redial, multi-homing) + the [`receiver::InvokeHandler`]
//!   seam.
//! * [`loopback`] — the engine: own a tokio runtime, run the receiver, bridge
//!   each forwarded stream to `127.0.0.1:<local_port>`. This is the API a
//!   native binding drives.

// `bridge` is a faithful fork of the relay repo's `relay-bridge`; its public
// docs reference the private `BufferMeterGuard` (harmless internal cross-refs).
// Allow it here rather than diverging the forked file.
#[allow(rustdoc::private_intra_doc_links)]
pub mod bridge;
pub mod loopback;
pub mod protocol;
pub mod receiver;

pub use loopback::{Config, ConfigError, Engine, Handle, Status, TlsConfig};
