//! Restate Cloud: keys (`rstcloudv1_…`) and the configuration a deployment needs to
//! connect to it, resolved from code and the process environment.
//!
//! Enabled with the `cloud` feature. The `tunnel` feature, which adds the engine that
//! actually connects, implies it.

mod key;
mod options;

pub use key::{
    ConnectKey, IngressKey, KeyError, CLOUD_KEY_PREFIX, CLOUD_KEY_SHAPE, CLOUD_UI_URL,
    CONNECT_KEY_ENV, CONNECT_KEY_URL, INGRESS_KEY_ENV, INGRESS_KEY_URL,
};
pub use options::{
    parse_server_address, ConfigError, ConfigSource, ConnectOptions, Discovery, ReconnectPolicy,
    ServerAddress, Servers, Token, TokenError, Tuning, TunnelEngineOptions, AUTH_TOKEN_ENV,
    CONNECT_NAME_ENV, INPROC_AUTH_TOKEN_FILE_ENV, INPROC_CLOUD_REGION_ENV,
    INPROC_ENVIRONMENT_ID_ENV, INPROC_SIGNING_PUBLIC_KEY_ENV, INPROC_TUNNEL_NAME_ENV,
};
