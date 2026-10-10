//! `relay-protocol` — the runtime-agnostic wire contract of the M:N
//! HTTP/2 reverse-tunnel rendezvous system.
//!
//! Everything here is pure data + parsing: no tokio, no sockets. It's the
//! single source of truth for the pieces the broker, the receiver, and the
//! sender must all agree on:
//!
//!   * [`Tunnel`] — the rendezvous token (`[A-Za-z0-9._-]+`).
//!   * [`path`] — the URL grammar, both directions:
//!       - [`path::parse`] for the sender form `/invoke/<tunnel>/<host>/<port>/<tail>`
//!         (used by the broker),
//!       - [`path::parse_forwarded`] for the form the broker forwards to the
//!         receiver, `/invoke/<host>/<port>/<tail>` (used by the receiver).
//!   * [`whoami`] — the `/whoami` JSON body the receiver returns and the
//!     broker parses.
//!   * [`H2Tuning`] — frame-size / flow-control window knobs applied by
//!     every h2 endpoint in the system.
//!
//! See `design.md` §4 for the protocol these types encode.

pub mod env;
pub mod error;
pub mod h2_tuning;
pub mod path;
pub mod tunnel;
pub mod whoami;

pub use env::Env;
pub use error::{EnvError, PathError, TunnelError, WhoamiError};
pub use h2_tuning::H2Tuning;
pub use path::{parse, parse_forwarded, ForwardedTarget, ParsedPath};
pub use tunnel::Tunnel;
pub use whoami::WhoamiBody;
