//! Protocol error types shared across the workspace.
//!
//! These were originally in the broker's `error.rs`; the broker re-exports
//! them from here now so there's one definition. Broker-specific errors
//! (e.g. `AuthError`) stay in the broker.

use thiserror::Error;

/// Errors that can occur while validating a tunnel string.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TunnelError {
    #[error("tunnel is empty")]
    Empty,
    #[error("tunnel contains characters outside [A-Za-z0-9._-]")]
    InvalidChar,
    #[error("tunnel exceeds the maximum length ({0} bytes)")]
    TooLong(usize),
}

/// Errors that can occur while validating an env string.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnvError {
    #[error("env is empty")]
    Empty,
    #[error("env contains non-alphanumeric characters")]
    NonAlphanumeric,
    #[error("env exceeds the maximum length ({0} bytes)")]
    TooLong(usize),
}

/// Errors that can occur during path parsing.
///
/// Most variants arise from the sender-form parser ([`super::path::parse`],
/// grammar `/<env>/<tunnel>/<proto>/<host>/<port>/<tail>`); `InvalidHost` /
/// `InvalidPort` are specific to the forwarded-form parser
/// ([`super::path::parse_forwarded`], grammar `/<proto>/<host>/<port>/<tail>`),
/// which the receiver uses and which validates the target the relay treated
/// as opaque.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PathError {
    #[error("path is empty or does not start with '/'")]
    Malformed,
    #[error("env segment is missing or empty")]
    MissingEnv,
    #[error("env segment contains non-alphanumeric characters")]
    InvalidEnv,
    #[error("tunnel segment is missing or empty")]
    MissingTunnel,
    #[error("tunnel segment contains non-alphanumeric characters")]
    InvalidTunnel,
    #[error("proto segment is missing or empty")]
    MissingProto,
    /// The `<proto>` segment was present and non-empty but is not the literal
    /// `http` (the only implemented transport). Payload-free by design: the
    /// rejected value is attacker-controlled, so it is neither retained nor
    /// logged — carrying it as a `String` would let a caller stuff an arbitrary
    /// (up to `MAX_PATH_LEN`) value into an allocation and any error log. The
    /// segment is bounded by the path-length cap regardless; there is no
    /// diagnostic value in echoing it back.
    #[error("unsupported target protocol (only \"http\" is implemented)")]
    UnsupportedProto,
    #[error("path is missing the target host and/or port segments")]
    MissingTarget,
    #[error("target host segment is empty")]
    InvalidHost,
    #[error("target host segment exceeds the maximum length ({0} bytes)")]
    HostTooLong(usize),
    #[error("path exceeds the maximum length ({0} bytes)")]
    PathTooLong(usize),
    #[error("target port segment is not a valid port: {0:?}")]
    InvalidPort(String),
}

/// Errors from parsing a `/whoami` response body.
#[derive(Debug, Error)]
pub enum WhoamiError {
    #[error("response body is not valid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("response is missing required field: {0}")]
    MissingField(&'static str),
    #[error("tunnel field is invalid: {0}")]
    InvalidTunnel(#[from] TunnelError),
    #[error("env field is invalid: {0}")]
    InvalidEnv(#[from] EnvError),
    #[error("api_key field is empty")]
    EmptyApiKey,
    #[error("api_key field exceeds the maximum length ({0} bytes)")]
    ApiKeyTooLong(usize),
    #[error("instance_id field exceeds the maximum length ({0} bytes)")]
    InstanceIdTooLong(usize),
}
