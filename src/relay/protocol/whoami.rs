//! The `/whoami` response body — produced by the receiver, parsed by the
//! broker.
//!
//! Per design §4.1, the receiver's response to the cluster's `GET /whoami`
//! has a JSON body of the shape:
//!
//! ```json
//! { "env": "...", "api_key": "...", "tunnel": "..." }
//! ```
//!
//! `env`, `api_key`, and `tunnel` are required and non-empty. The env is
//! alphanumeric and the tunnel is `[A-Za-z0-9._-]+` (validated by the
//! [`Env`] / [`Tunnel`] types); a tunnel is meaningful only within its env,
//! so the receiver reports both. Additional
//! fields are ignored (forward compatibility — `/whoami` carries `instance_id`,
//! and may grow `version`, etc., per design §3 / open questions).

use super::env::Env;
use super::error::WhoamiError;
use super::tunnel::Tunnel;
use serde::{Deserialize, Serialize};

/// Maximum accepted `/whoami` `api_key` length in bytes. This is the relay's own
/// API key (e.g. Restate Cloud `key_<id>.<secret>`), which is short — a few tens
/// of bytes — so 512 is generous. The `start_tunnel` bearer token (which can be
/// a multi-KB JWT) is a *separate* credential with its own, larger cap; do not
/// reuse this one for it.
pub const MAX_API_KEY_LEN: usize = 512;

/// Maximum accepted `/whoami` `instance_id` length in bytes. An ephemeral,
/// receiver-generated identifier; 128 is ample.
pub const MAX_INSTANCE_ID_LEN: usize = 128;

/// Maximum accepted `start_tunnel` `authorization: Bearer <token>` length in
/// bytes. This is a *different* credential from the `/whoami` [`MAX_API_KEY_LEN`]
/// key — a Restate Cloud tunnel client may present a multi-KB JWT — so it has
/// its own, larger cap. Still far below the h2 `max_header_list_size` (64 KiB)
/// that bounds the whole header block, so a max-length token is never rejected
/// by the enclosing header cap first.
pub const MAX_BEARER_TOKEN_LEN: usize = 8192;

/// Parsed contents of a valid `/whoami` response body.
///
/// `Serialize` is derived so the receiver can build the body it returns from
/// the same type the broker parses — one struct, both directions. (`Tunnel`
/// serialises as a plain string; see its impl in `tunnel.rs`.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WhoamiBody {
    /// The env this receiver registers under — the routing namespace that
    /// scopes the tunnel. Required: a tunnel is only meaningful within an env.
    pub env: Env,
    pub api_key: String,
    pub tunnel: Tunnel,
    /// Optional ephemeral receiver-instance id, generated once per receiver
    /// *process* and presented on every connection + reconnection + node
    /// (so the broker recognises the receiver's many connections — when it
    /// multi-homes for HA — as one logical receiver). Scoped under the
    /// tunnel. Drives request affinity: all of a receiver's connections
    /// share one affinity bucket `(tunnel, instance_id)`, so a key sticks to
    /// the *receiver* (served on whichever node the sender hits) rather than
    /// to one specific connection. Absent ⇒ the broker falls back to a
    /// per-connection affinity bucket (legacy behaviour). Not persisted: a
    /// restarted receiver is a new instance and affinity re-pins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
}

/// Internal raw deserialisation type. We keep it private so that all
/// callers go through [`parse`], which enforces non-emptiness and the
/// `Env` / `Tunnel` charsets.
#[derive(Debug, Deserialize)]
struct Raw {
    #[serde(default)]
    env: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    tunnel: Option<String>,
    #[serde(default)]
    instance_id: Option<String>,
}

/// Parse a `/whoami` response body.
pub fn parse(body: &[u8]) -> Result<WhoamiBody, WhoamiError> {
    let raw: Raw = serde_json::from_slice(body)?;

    let env_str = raw.env.ok_or(WhoamiError::MissingField("env"))?;
    let env = Env::new(env_str)?;

    let api_key = raw.api_key.ok_or(WhoamiError::MissingField("api_key"))?;
    if api_key.is_empty() {
        return Err(WhoamiError::EmptyApiKey);
    }
    if api_key.len() > MAX_API_KEY_LEN {
        return Err(WhoamiError::ApiKeyTooLong(MAX_API_KEY_LEN));
    }

    let tunnel_str = raw.tunnel.ok_or(WhoamiError::MissingField("tunnel"))?;
    let tunnel = Tunnel::new(tunnel_str)?;

    // An empty instance_id is treated as absent (no identity supplied).
    let instance_id = raw.instance_id.filter(|s| !s.is_empty());
    if let Some(id) = &instance_id {
        if id.len() > MAX_INSTANCE_ID_LEN {
            return Err(WhoamiError::InstanceIdTooLong(MAX_INSTANCE_ID_LEN));
        }
    }

    Ok(WhoamiBody {
        env,
        api_key,
        tunnel,
        instance_id,
    })
}

impl WhoamiBody {
    /// Serialise to the JSON wire body. Used by the receiver to answer
    /// `GET /whoami`. Infallible for our fixed shape.
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("WhoamiBody serialises")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_body() {
        let body = br#"{"env":"e1","api_key":"k1","tunnel":"abc123"}"#;
        let parsed = parse(body).unwrap();
        assert_eq!(parsed.env.as_str(), "e1");
        assert_eq!(parsed.api_key, "k1");
        assert_eq!(parsed.tunnel.as_str(), "abc123");
    }

    #[test]
    fn round_trips_through_json() {
        let b = WhoamiBody {
            env: Env::new("e1").unwrap(),
            api_key: "k1".to_string(),
            tunnel: Tunnel::new("abc123").unwrap(),
            instance_id: None,
        };
        let bytes = b.to_json();
        let back = parse(&bytes).unwrap();
        assert_eq!(back, b);
    }

    #[test]
    fn round_trips_with_instance_id() {
        let b = WhoamiBody {
            env: Env::new("e1").unwrap(),
            api_key: "k1".to_string(),
            tunnel: Tunnel::new("abc123").unwrap(),
            instance_id: Some("inst-7f3a".to_string()),
        };
        let bytes = b.to_json();
        // The id is on the wire and parses back.
        assert!(std::str::from_utf8(&bytes).unwrap().contains("instance_id"));
        let back = parse(&bytes).unwrap();
        assert_eq!(back, b);
        assert_eq!(back.instance_id.as_deref(), Some("inst-7f3a"));
    }

    #[test]
    fn empty_instance_id_parses_as_absent() {
        let body = br#"{"env":"e1","api_key":"k1","tunnel":"abc","instance_id":""}"#;
        assert_eq!(parse(body).unwrap().instance_id, None);
    }

    #[test]
    fn parses_with_whitespace_and_extra_fields() {
        let body = br#"
            {
                "env": "e1",
                "api_key": "k1",
                "tunnel": "abc",
                "instance_id": "i-future-field",
                "version": "1.2.3"
            }
        "#;
        // Extra fields are accepted (forward compat).
        let parsed = parse(body).unwrap();
        assert_eq!(parsed.env.as_str(), "e1");
        assert_eq!(parsed.api_key, "k1");
        assert_eq!(parsed.tunnel.as_str(), "abc");
    }

    #[test]
    fn rejects_missing_env() {
        let body = br#"{"api_key":"k1","tunnel":"abc"}"#;
        assert!(matches!(parse(body), Err(WhoamiError::MissingField("env"))));
    }

    #[test]
    fn rejects_invalid_env() {
        let body = br#"{"env":"has-dash","api_key":"k1","tunnel":"abc"}"#;
        assert!(matches!(parse(body), Err(WhoamiError::InvalidEnv(_))));
    }

    #[test]
    fn rejects_missing_api_key() {
        let body = br#"{"env":"e1","tunnel":"abc"}"#;
        assert!(matches!(
            parse(body),
            Err(WhoamiError::MissingField("api_key"))
        ));
    }

    #[test]
    fn rejects_missing_tunnel() {
        let body = br#"{"env":"e1","api_key":"k1"}"#;
        assert!(matches!(
            parse(body),
            Err(WhoamiError::MissingField("tunnel"))
        ));
    }

    #[test]
    fn rejects_empty_api_key() {
        let body = br#"{"env":"e1","api_key":"","tunnel":"abc"}"#;
        assert!(matches!(parse(body), Err(WhoamiError::EmptyApiKey)));
    }

    #[test]
    fn rejects_over_long_api_key() {
        let key = "k".repeat(MAX_API_KEY_LEN + 1);
        let body = format!(r#"{{"env":"e1","api_key":"{key}","tunnel":"abc"}}"#);
        assert!(matches!(
            parse(body.as_bytes()),
            Err(WhoamiError::ApiKeyTooLong(_))
        ));
        // Exactly at the cap is accepted.
        let key = "k".repeat(MAX_API_KEY_LEN);
        let body = format!(r#"{{"env":"e1","api_key":"{key}","tunnel":"abc"}}"#);
        assert!(parse(body.as_bytes()).is_ok());
    }

    #[test]
    fn rejects_over_long_instance_id() {
        let id = "i".repeat(MAX_INSTANCE_ID_LEN + 1);
        let body = format!(r#"{{"env":"e1","api_key":"k1","tunnel":"abc","instance_id":"{id}"}}"#);
        assert!(matches!(
            parse(body.as_bytes()),
            Err(WhoamiError::InstanceIdTooLong(_))
        ));
    }

    #[test]
    fn rejects_invalid_tunnel_via_tunnel_type() {
        // `~` is outside the relaxed tunnel charset `[A-Za-z0-9._-]`.
        let body = br#"{"env":"e1","api_key":"k1","tunnel":"has~tilde"}"#;
        assert!(matches!(parse(body), Err(WhoamiError::InvalidTunnel(_))));
    }

    #[test]
    fn rejects_empty_tunnel_via_tunnel_type() {
        let body = br#"{"env":"e1","api_key":"k1","tunnel":""}"#;
        assert!(matches!(parse(body), Err(WhoamiError::InvalidTunnel(_))));
    }

    #[test]
    fn rejects_invalid_json() {
        let body = b"not json";
        assert!(matches!(parse(body), Err(WhoamiError::InvalidJson(_))));
    }

    #[test]
    fn rejects_wrong_type_for_field() {
        // api_key is a number, not a string.
        let body = br#"{"env":"e1","api_key":42,"tunnel":"abc"}"#;
        assert!(matches!(parse(body), Err(WhoamiError::InvalidJson(_))));
    }

    #[test]
    fn rejects_empty_body() {
        assert!(matches!(parse(b""), Err(WhoamiError::InvalidJson(_))));
    }

    #[test]
    fn rejects_null_fields() {
        let body = br#"{"env":"e1","api_key":null,"tunnel":"abc"}"#;
        // serde sees null as None for Option<String>, which we treat as missing.
        assert!(matches!(
            parse(body),
            Err(WhoamiError::MissingField("api_key"))
        ));
    }
}
