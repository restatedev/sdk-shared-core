//! Restate Cloud keys.
//!
//! To users a key is opaque: docs and error messages only ever say "copy it
//! again from the Cloud UI". This module is the one place in the SDKs that
//! spells the format out.
//!
//! ```text
//! rstcloudv1_<base58>
//! ```
//!
//! The base58 part (Bitcoin alphabet, no padding) encodes a UTF-8 query string
//! (`application/x-www-form-urlencoded`). For a connect key:
//!
//! ```text
//! envid=env_201k…&apikey=key_01abc.5Kd…&region=eu.restate.cloud&signingkey=publickeyv1_…
//! ```
//!
//! and for an ingress key the same without `signingkey`. Every value is made
//! of unreserved characters, so nothing in it is percent-encoded. The caller
//! decides which parameters it knows; the only derivations are the tunnel SRV
//! name (`tunnel.<region>`) and the ingress URL
//! (`https://<envid minus env_>.env.<region>:8080`).

use std::fmt;

/// The prefix every Restate Cloud key starts with.
pub const CLOUD_KEY_PREFIX: &str = "rstcloudv1_";

/// The shape shown in errors.
pub const CLOUD_KEY_SHAPE: &str = "rstcloudv1_<base58>";

/// The Cloud UI, for the deep links below.
pub const CLOUD_UI_URL: &str = "https://cloud.restate.dev";

/// Where to get a connect key (Development → Configure SDK), linked in every error.
pub const CONNECT_KEY_URL: &str =
    "https://cloud.restate.dev/to/developers/integration#configure-sdk";

/// Where to get an ingress key (Development → Configure Client), linked in every error.
pub const INGRESS_KEY_URL: &str =
    "https://cloud.restate.dev/to/developers/integration#configure-client";

/// The environment variable holding a connect key.
pub const CONNECT_KEY_ENV: &str = "RESTATE_CONNECT_KEY";

/// The environment variable holding an ingress key.
pub const INGRESS_KEY_ENV: &str = "RESTATE_INGRESS_KEY";

const IDENTITY_V1_PREFIX: &str = "publickeyv1_";

/// Why a string is not a usable Restate Cloud key.
///
/// The texts are predicates of the key ("RESTATE_CONNECT_KEY {error}"): whoever
/// reports the error prepends what the key was read from and appends where to
/// get a fresh one. They never echo parts of the value (the whole thing is a
/// credential) and never describe the encoding: a user only needs to know whether
/// they pasted something else or lost part of the key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// The value does not start with [`CLOUD_KEY_PREFIX`].
    #[error("is not a Restate Cloud key (it does not start with {CLOUD_KEY_PREFIX}); expected {CLOUD_KEY_SHAPE}")]
    NotACloudKey,
    /// The encoded part is empty, is not base58, or does not decode to a query string.
    #[error(
        "is not a Restate Cloud key (it is truncated or corrupted); expected {CLOUD_KEY_SHAPE}"
    )]
    Corrupted,
    /// A required parameter is missing.
    #[error("is incomplete")]
    Incomplete,
    /// A parameter is present but does not have the expected shape.
    #[error("is malformed")]
    Malformed,
    /// A connect key was expected but the key carries no signing key (an ingress key, or an incomplete paste).
    #[error("carries no signing key (this is an ingress key, or an incomplete paste)")]
    NotAConnectKey,
    /// An ingress key was expected but the key carries no API key.
    #[error("carries no API key (this is not an ingress key)")]
    NotAnIngressKey,
}

/// The coordinates of a Restate Cloud environment for a deployment that
/// connects to it: a parsed connect key.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectKey {
    /// The environment id (`env_…`).
    pub environment_id: String,
    /// The API key used as bearer token towards the tunnel servers, when the key carries one.
    pub api_key: Option<String>,
    /// The region domain (for example `eu.restate.cloud`).
    pub region: String,
    /// The request-identity public key (`publickeyv1_…`) Restate signs forwarded requests with.
    pub signing_public_key: String,
}

impl fmt::Debug for ConnectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectKey")
            .field("environment_id", &self.environment_id)
            .field("api_key", &self.api_key.as_ref().map(|_| Redacted))
            .field("region", &self.region)
            .field("signing_public_key", &self.signing_public_key)
            .finish()
    }
}

impl ConnectKey {
    /// Parse a connect key.
    pub fn parse(value: &str) -> Result<Self, KeyError> {
        let params = parse_cloud_key(value)?;
        let environment_id = params.required("envid")?;
        validate_environment_id(&environment_id)?;
        let region = params.required("region")?;
        validate_region_domain(&region)?;
        let signing_public_key = params
            .optional("signingkey")
            .ok_or(KeyError::NotAConnectKey)?;
        validate_signing_public_key(&signing_public_key)?;
        let api_key = params.optional("apikey");
        if let Some(api_key) = &api_key {
            if !header_safe(api_key) {
                return Err(KeyError::Malformed);
            }
        }
        Ok(Self {
            environment_id,
            api_key,
            region,
            signing_public_key,
        })
    }

    /// Read and parse `RESTATE_CONNECT_KEY`. `Ok(None)` when it is unset or empty.
    pub fn from_env() -> Result<Option<Self>, KeyError> {
        Self::from_env_with(|name| std::env::var(name).ok())
    }

    pub(crate) fn from_env_with(
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<Self>, KeyError> {
        match env(CONNECT_KEY_ENV).filter(|value| !value.trim().is_empty()) {
            Some(value) => Self::parse(&value).map(Some),
            None => Ok(None),
        }
    }

    /// The DNS SRV name of the tunnel servers of this environment's region.
    pub fn tunnel_srv(&self) -> String {
        srv_name_for_region_domain(&self.region)
    }

    /// Encode the key back into its `rstcloudv1_<base58>` form.
    pub fn encode(&self) -> String {
        let mut query = format!("envid={}", self.environment_id);
        if let Some(api_key) = &self.api_key {
            query.push_str("&apikey=");
            query.push_str(api_key);
        }
        query.push_str("&region=");
        query.push_str(&self.region);
        query.push_str("&signingkey=");
        query.push_str(&self.signing_public_key);
        encode_cloud_key(&query)
    }
}

/// The coordinates for calling a Restate Cloud environment: a parsed ingress key.
#[derive(Clone, PartialEq, Eq)]
pub struct IngressKey {
    /// The environment id (`env_…`).
    pub environment_id: String,
    /// The API key used as bearer token towards the ingress.
    pub api_key: String,
    /// The region domain (for example `eu.restate.cloud`).
    pub region: String,
}

impl fmt::Debug for IngressKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngressKey")
            .field("environment_id", &self.environment_id)
            .field("api_key", &Redacted)
            .field("region", &self.region)
            .finish()
    }
}

impl IngressKey {
    /// Parse an ingress key. A connect key is accepted too, since it carries the same coordinates.
    pub fn parse(value: &str) -> Result<Self, KeyError> {
        let params = parse_cloud_key(value)?;
        let environment_id = params.required("envid")?;
        validate_environment_id(&environment_id)?;
        let region = params.required("region")?;
        validate_region_domain(&region)?;
        let api_key = params.optional("apikey").ok_or(KeyError::NotAnIngressKey)?;
        if !header_safe(&api_key) {
            return Err(KeyError::Malformed);
        }
        Ok(Self {
            environment_id,
            api_key,
            region,
        })
    }

    /// Read and parse `RESTATE_INGRESS_KEY`. `Ok(None)` when it is unset or empty.
    pub fn from_env() -> Result<Option<Self>, KeyError> {
        Self::from_env_with(|name| std::env::var(name).ok())
    }

    pub(crate) fn from_env_with(
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<Self>, KeyError> {
        match env(INGRESS_KEY_ENV).filter(|value| !value.trim().is_empty()) {
            Some(value) => Self::parse(&value).map(Some),
            None => Ok(None),
        }
    }

    /// The ingress URL of the environment: `https://<id>.env.<region>:8080`.
    pub fn ingress_url(&self) -> String {
        let id = self
            .environment_id
            .strip_prefix("env_")
            .unwrap_or(&self.environment_id);
        format!("https://{id}.env.{}:8080", self.region)
    }

    /// The `Authorization` header value for the ingress.
    pub fn bearer(&self) -> String {
        format!("Bearer {}", self.api_key)
    }

    /// Encode the key back into its `rstcloudv1_<base58>` form.
    pub fn encode(&self) -> String {
        encode_cloud_key(&format!(
            "envid={}&apikey={}&region={}",
            self.environment_id, self.api_key, self.region
        ))
    }
}

fn encode_cloud_key(query: &str) -> String {
    format!(
        "{CLOUD_KEY_PREFIX}{}",
        bs58::encode(query.as_bytes()).into_string()
    )
}

/// The DNS SRV name of the tunnel servers of a region domain.
pub(crate) fn srv_name_for_region_domain(region: &str) -> String {
    format!("tunnel.{region}")
}

struct Params(Vec<(String, String)>);

impl Params {
    fn optional(&self, name: &str) -> Option<String> {
        self.0
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
            .filter(|value| !value.is_empty())
    }

    fn required(&self, name: &str) -> Result<String, KeyError> {
        self.optional(name).ok_or(KeyError::Incomplete)
    }
}

/// Decode `rstcloudv1_<base58>` into its query parameters, in order.
fn parse_cloud_key(value: &str) -> Result<Params, KeyError> {
    let trimmed = value.trim();
    let encoded = trimmed
        .strip_prefix(CLOUD_KEY_PREFIX)
        .ok_or(KeyError::NotACloudKey)?;
    if encoded.is_empty() {
        return Err(KeyError::Corrupted);
    }
    let bytes = bs58::decode(encoded)
        .into_vec()
        .map_err(|_| KeyError::Corrupted)?;
    let query = String::from_utf8(bytes).map_err(|_| KeyError::Corrupted)?;
    // Demand the shape we produce: printable ASCII, non-empty, every entry with a name.
    if query.is_empty() || !query.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return Err(KeyError::Corrupted);
    }
    let mut params = Vec::new();
    for entry in query.split('&') {
        if entry.is_empty() {
            continue;
        }
        let (name, value) = match entry.find('=') {
            Some(index) => (&entry[..index], &entry[index + 1..]),
            None => (entry, ""),
        };
        if name.is_empty() {
            return Err(KeyError::Corrupted);
        }
        let name = form_decode(name).ok_or(KeyError::Corrupted)?;
        let value = form_decode(value).ok_or(KeyError::Corrupted)?;
        if params.iter().any(|(existing, _)| *existing == name) {
            return Err(KeyError::Malformed);
        }
        params.push((name, value));
    }
    Ok(Params(params))
}

/// `application/x-www-form-urlencoded` decoding of one name or value.
fn form_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = bytes.get(index + 1..index + 3)?;
                let hex = std::str::from_utf8(hex).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                index += 2;
            }
            byte => out.push(byte),
        }
        index += 1;
    }
    String::from_utf8(out).ok()
}

pub(crate) fn validate_environment_id(value: &str) -> Result<(), KeyError> {
    let valid = value
        .strip_prefix("env_")
        .is_some_and(|tail| !tail.is_empty() && tail.bytes().all(is_identifier_byte));
    if valid {
        Ok(())
    } else {
        Err(KeyError::Malformed)
    }
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

/// A region domain: lowercase labels of `[a-z0-9-]`, dot-separated, none empty.
pub(crate) fn validate_region_domain(region: &str) -> Result<(), KeyError> {
    let valid = !region.is_empty()
        && region.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(KeyError::Malformed)
    }
}

/// A request-identity public key: `publickeyv1_` followed by base58 of exactly 32 bytes.
pub(crate) fn validate_signing_public_key(key: &str) -> Result<(), KeyError> {
    let encoded = key
        .strip_prefix(IDENTITY_V1_PREFIX)
        .ok_or(KeyError::Malformed)?;
    let decoded = bs58::decode(encoded)
        .into_vec()
        .map_err(|_| KeyError::Malformed)?;
    if decoded.len() != 32 {
        return Err(KeyError::Malformed);
    }
    Ok(())
}

/// Printable ASCII without whitespace: safe to travel in an HTTP header.
pub(crate) fn header_safe(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

#[derive(Clone, Copy)]
pub(crate) struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const SIGNING_KEY: &str = "publickeyv1_ChjENKeMvCtRnqG2mrBK1HmPKufgFUc98K8B3ononQvp";

    pub(crate) fn encode_key(query: &str) -> String {
        format!(
            "{CLOUD_KEY_PREFIX}{}",
            bs58::encode(query.as_bytes()).into_string()
        )
    }

    pub(crate) fn connect_key() -> String {
        encode_key(&format!(
            "envid=env_201kxe0kdaynq7h1fsn2chhcbwb&apikey=key_01abc.secret&region=eu.restate.cloud&signingkey={SIGNING_KEY}"
        ))
    }

    #[test]
    fn parses_connect_key() {
        let key = ConnectKey::parse(&format!("  {}\n", connect_key())).unwrap();
        assert_eq!(key.environment_id, "env_201kxe0kdaynq7h1fsn2chhcbwb");
        assert_eq!(key.api_key.as_deref(), Some("key_01abc.secret"));
        assert_eq!(key.region, "eu.restate.cloud");
        assert_eq!(key.signing_public_key, SIGNING_KEY);
        assert_eq!(key.tunnel_srv(), "tunnel.eu.restate.cloud");
        assert!(!format!("{key:?}").contains("secret"));
    }

    #[test]
    fn connect_key_without_api_key_is_valid() {
        let key = ConnectKey::parse(&encode_key(&format!(
            "envid=env_abc&region=eu.restate.cloud&signingkey={SIGNING_KEY}"
        )))
        .unwrap();
        assert_eq!(key.api_key, None);
    }

    #[test]
    fn parses_ingress_key_and_derives_url() {
        let key = IngressKey::parse(&encode_key(
            "envid=env_201kxe0kdaynq7h1fsn2chhcbwb&apikey=key_01abc.secret&region=eu.restate.cloud",
        ))
        .unwrap();
        assert_eq!(
            key.ingress_url(),
            "https://201kxe0kdaynq7h1fsn2chhcbwb.env.eu.restate.cloud:8080"
        );
        assert_eq!(key.bearer(), "Bearer key_01abc.secret");
        assert!(!format!("{key:?}").contains("secret"));

        // A connect key carries the same coordinates.
        assert!(IngressKey::parse(&connect_key()).is_ok());
    }

    #[test]
    fn unknown_parameters_are_ignored() {
        let key = ConnectKey::parse(&encode_key(&format!(
            "future=1&envid=env_abc&region=eu.restate.cloud&signingkey={SIGNING_KEY}&other=x"
        )))
        .unwrap();
        assert_eq!(key.environment_id, "env_abc");
    }

    #[test]
    fn error_taxonomy() {
        assert_eq!(
            ConnectKey::parse("key_01abc").unwrap_err(),
            KeyError::NotACloudKey
        );
        assert_eq!(
            ConnectKey::parse("rstcloudv1_").unwrap_err(),
            KeyError::Corrupted
        );
        assert_eq!(
            ConnectKey::parse("rstcloudv1_0OIl").unwrap_err(),
            KeyError::Corrupted
        );
        assert_eq!(
            ConnectKey::parse(&encode_key("=x&envid=env_a")).unwrap_err(),
            KeyError::Corrupted
        );
        assert_eq!(
            ConnectKey::parse(&encode_key("envid=env_abc&region=eu.restate.cloud")).unwrap_err(),
            KeyError::NotAConnectKey
        );
        assert_eq!(
            IngressKey::parse(&encode_key("envid=env_abc&region=eu.restate.cloud")).unwrap_err(),
            KeyError::NotAnIngressKey
        );
        assert_eq!(
            ConnectKey::parse(&encode_key(&format!(
                "envid=env_abc&signingkey={SIGNING_KEY}"
            )))
            .unwrap_err(),
            KeyError::Incomplete
        );
        assert_eq!(
            ConnectKey::parse(&encode_key(&format!(
                "envid=abc&region=eu.restate.cloud&signingkey={SIGNING_KEY}"
            )))
            .unwrap_err(),
            KeyError::Malformed
        );
        assert_eq!(
            ConnectKey::parse(&encode_key(&format!(
                "envid=env_abc&region=EU.restate.cloud&signingkey={SIGNING_KEY}"
            )))
            .unwrap_err(),
            KeyError::Malformed
        );
        assert_eq!(
            ConnectKey::parse(&encode_key(
                "envid=env_abc&region=eu.restate.cloud&signingkey=publickeyv1_short"
            ))
            .unwrap_err(),
            KeyError::Malformed
        );
        assert_eq!(
            ConnectKey::parse(&encode_key(&format!(
                "envid=env_abc&envid=env_abc&region=eu.restate.cloud&signingkey={SIGNING_KEY}"
            )))
            .unwrap_err(),
            KeyError::Malformed
        );
        assert_eq!(
            KeyError::NotACloudKey.to_string(),
            "is not a Restate Cloud key (it does not start with rstcloudv1_); expected rstcloudv1_<base58>"
        );
    }

    #[test]
    fn from_env_treats_empty_as_unset() {
        assert_eq!(ConnectKey::from_env_with(|_| None).unwrap(), None);
        assert_eq!(
            ConnectKey::from_env_with(|_| Some(" ".into())).unwrap(),
            None
        );
        assert!(ConnectKey::from_env_with(|_| Some("nope".into())).is_err());
        let key = connect_key();
        assert!(
            ConnectKey::from_env_with(|name| (name == CONNECT_KEY_ENV).then(|| key.clone()))
                .unwrap()
                .is_some()
        );
        assert!(
            IngressKey::from_env_with(|name| (name == INGRESS_KEY_ENV).then(|| key.clone()))
                .unwrap()
                .is_some()
        );
    }
}
