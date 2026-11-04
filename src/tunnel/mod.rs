//! The in-process Restate Cloud tunnel engine.
//!
//! The engine dials the tunnel servers of a Restate Cloud environment over
//! outbound TCP+TLS, plays the HTTP/2 *server* on every connection, performs the
//! `GET /_/start-tunnel` handshake and hands every forwarded invocation, with
//! its identity verified and its URI rewritten, to the [`TunnelService`] the
//! SDK provides. It reconnects, follows DNS, honours server drains and drains
//! gracefully on [`TunnelEngine::shutdown`].
//!
//! Not here, by design: signal handling, the tokio runtime (the engine must be
//! started inside one owned by the SDK) and language bridges.
//!
//! Enabled with the `tunnel` feature together with a crypto backend
//! (`rust_crypto` or `aws_lc_rs`; with both, `rust_crypto` is selected).

mod connection;
mod draining;
mod options;
mod supervisor;
mod targets;

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper::service::Service;
use tokio::sync::{broadcast, watch};

pub use options::{
    parse_server_address, ConfigError, ConfigSource, ConnectOptions, Discovery, ReconnectPolicy,
    ServerAddress, Servers, Token, TokenError, Tuning, TunnelEngineOptions, AUTH_TOKEN_ENV,
    CONNECT_NAME_ENV, INPROC_AUTH_TOKEN_FILE_ENV, INPROC_CLOUD_REGION_ENV,
    INPROC_ENVIRONMENT_ID_ENV, INPROC_SIGNING_PUBLIC_KEY_ENV, INPROC_TUNNEL_NAME_ENV,
};
use supervisor::Engine;

/// Boxed error type of a [`TunnelBody`].
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The response body a [`TunnelService`] produces.
pub type TunnelBody = UnsyncBoxBody<Bytes, BoxError>;

/// The SDK's request handler.
///
/// The engine verifies the request identity and rewrites the forwarded URI
/// first, so the service sees `/discover`, `/invoke/<service>/<handler>` and
/// so on, exactly as it would from a local HTTP server. The service is cloned
/// into every connection and called from the engine's tokio runtime, so it
/// must be `Send + Sync` and its future `Send`.
pub trait TunnelService:
    Service<Request<Incoming>, Response = Response<TunnelBody>, Error = Infallible, Future: Send>
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<S> TunnelService for S where
    S: Service<
            Request<Incoming>,
            Response = Response<TunnelBody>,
            Error = Infallible,
            Future: Send,
        > + Clone
        + Send
        + Sync
        + 'static
{
}

/// The lifecycle of an engine, as state: always the latest value, no history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    /// No connection has completed its handshake yet.
    Starting,
    /// The first handshake succeeded. Later reconnections do not change the status.
    Ready(TunnelInfo),
    /// The engine stopped: `Ok` after a shutdown, `Err` after a fatal error.
    Terminated {
        result: Result<(), Error>,
        /// The first handshake's metadata, if there was one.
        info: Option<TunnelInfo>,
    },
}

/// Transient diagnostics, delivered only to subscribers present when they happen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A connection attempt to a tunnel server starts.
    Connecting { target: String },
    /// A connection completed its handshake and serves invocations.
    Connected { target: String, info: TunnelInfo },
    /// A serving connection ended; a reconnection follows.
    Disconnected {
        target: String,
        reason: Option<String>,
    },
    /// A connection attempt failed before serving; the next one starts after `retry_in`.
    AttemptFailed {
        target: String,
        reason: String,
        retry_in: Duration,
    },
    /// The server asked this connection to drain (a handover); a replacement is dialed at once.
    ServerDrain { target: String },
    /// The reconnect policy's bounds were hit for one server while others keep serving.
    GaveUp { target: String, reason: String },
    /// The engine stops because of an unrecoverable error.
    Fatal { reason: String },
}

/// Why the engine stopped or could not become ready.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// An unrecoverable error: rejected credentials, a rejected name, the reconnect policy
    /// exhausted on the last server, or an internal failure.
    #[error("tunnel: fatal connection error: {reason}")]
    Fatal { reason: String },
    /// The engine was shut down before the first successful handshake.
    #[error("tunnel: closed before the first successful handshake")]
    Closed,
}

/// Why the engine could not start.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("tunnel: must be started inside a tokio runtime")]
    NoRuntime,
    #[error("tunnel: invalid signing public key: {0}")]
    SigningKey(#[source] crate::KeyError),
    #[error("tunnel: cannot initialise the DNS resolver: {0}")]
    Resolver(String),
    #[error("tunnel: cannot initialise TLS: {0}")]
    Tls(String),
}

/// Metadata learned from a successful Cloud handshake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TunnelInfo {
    /// The deployment name confirmed by Restate Cloud.
    pub name: String,
    /// The proxy URL advertised by Restate Cloud.
    pub proxy_url: String,
    /// The tunnel URL advertised by Restate Cloud.
    pub tunnel_url: String,
    /// The deployment URL of this in-process endpoint, as Restate sees it.
    pub deployment_url: String,
}

impl TunnelInfo {
    pub(crate) fn from_handshake(
        name: String,
        proxy_url: String,
        tunnel_url: String,
    ) -> Result<Self, &'static str> {
        let uri: http::Uri = proxy_url.parse().map_err(|_| "invalid proxy-url")?;
        let scheme = uri.scheme_str().ok_or("proxy-url has no scheme")?;
        let authority = uri.authority().ok_or("proxy-url has no host")?;
        if authority.host().is_empty() {
            return Err("proxy-url has no host");
        }
        let path = uri.path().trim_end_matches('/');
        let deployment_base = if authority.port().is_some() {
            format!("{scheme}://{authority}{path}")
        } else {
            // A managed-Cloud proxy URL without a port means the 9080 ingress port.
            format!("{scheme}://{authority}:9080{path}")
        };
        let deployment_url = format!("{deployment_base}/http/in-process/9080/");
        Ok(Self {
            name,
            proxy_url,
            tunnel_url,
            deployment_url,
        })
    }
}

/// A running tunnel engine. Cloning shares the engine; dropping the last clone
/// cuts every connection at once (like `shutdown(Duration::ZERO)`).
#[derive(Clone)]
pub struct TunnelEngine {
    engine: Arc<Engine>,
}

impl fmt::Debug for TunnelEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelEngine")
            .field("status", &*self.engine.status().borrow())
            .finish()
    }
}

impl TunnelEngine {
    /// Start connecting. Must be called inside a tokio runtime, which the engine then
    /// uses for all its tasks. Returns as soon as the engine is spawned; use
    /// [`ready`](Self::ready) to wait for the first handshake.
    pub fn start<S: TunnelService>(
        service: S,
        options: TunnelEngineOptions,
    ) -> Result<Self, StartError> {
        Ok(Self {
            engine: Arc::new(Engine::start(service, options)?),
        })
    }

    /// The lifecycle as state: `Starting` → `Ready` → `Terminated`.
    pub fn status(&self) -> watch::Receiver<Status> {
        self.engine.status()
    }

    /// Transient diagnostics, from the subscription onwards. A slow subscriber sees
    /// [`broadcast::error::RecvError::Lagged`]; the engine never waits for one.
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.engine.events()
    }

    /// Wait for the first successful handshake. Fails if the engine stops first.
    pub async fn ready(&self) -> Result<TunnelInfo, Error> {
        self.engine.wait_ready().await
    }

    /// Wait for the engine to stop: `Ok` after a shutdown, `Err` after a fatal error.
    pub async fn terminated(&self) -> Result<(), Error> {
        self.engine.wait_terminal().await
    }

    /// Stop serving: refuse new invocations, tell the servers, let the in-flight ones
    /// finish, and cut whatever is still running once `grace` elapses.
    /// `Duration::ZERO` cuts everything at once.
    ///
    /// Teardown begins synchronously, before the returned future is polled. The
    /// future resolves with the engine's terminal result.
    pub fn shutdown(
        &self,
        grace: Duration,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        let engine = Arc::clone(&self.engine);
        if grace.is_zero() {
            engine.force_close();
        } else {
            engine.begin_shutdown();
        }
        // Anchor the bound to the method call, alongside the synchronous drain transition.
        let deadline = tokio::time::Instant::now().checked_add(grace);
        async move {
            let result = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, engine.wait_terminal()).await,
                // A duration too large to represent as an Instant is, for practical
                // purposes, unbounded.
                None => Ok(engine.wait_terminal().await),
            };
            let result = match result {
                Ok(result) => result,
                Err(_) => {
                    engine.force_close();
                    engine.wait_terminal().await
                }
            };
            engine.reap().await;
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_url_defaults_proxy_port() {
        let info = TunnelInfo::from_handshake(
            "greeter-v1".into(),
            "https://proxy.example/env/tunnel".into(),
            "https://tunnel.example".into(),
        )
        .unwrap();
        assert_eq!(
            info.deployment_url,
            "https://proxy.example:9080/env/tunnel/http/in-process/9080/"
        );
        assert_eq!(info.proxy_url, "https://proxy.example/env/tunnel");

        let explicit_default = TunnelInfo::from_handshake(
            "greeter-v1".into(),
            "https://proxy.example:443/env/tunnel/".into(),
            "https://tunnel.example".into(),
        )
        .unwrap();
        assert_eq!(
            explicit_default.deployment_url,
            "https://proxy.example:443/env/tunnel/http/in-process/9080/"
        );

        let bare = TunnelInfo::from_handshake(
            "greeter-v1".into(),
            "https://proxy.example".into(),
            "https://tunnel.example".into(),
        )
        .unwrap();
        assert_eq!(
            bare.deployment_url,
            "https://proxy.example:9080/http/in-process/9080/"
        );

        assert!(TunnelInfo::from_handshake("n".into(), "/relative".into(), "x".into()).is_err());
        assert!(TunnelInfo::from_handshake("n".into(), "not a url".into(), "x".into()).is_err());
    }
}
