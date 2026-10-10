//! The `Env` newtype: an opaque, alphanumeric environment id.
//!
//! An `Env` scopes tunnels: a tunnel is only meaningful *within* an env, so
//! the rendezvous routing identity is the `(env, tunnel)` pair, not the
//! tunnel alone. Two receivers under the same tunnel string but different
//! envs are distinct tunnels. Like [`super::tunnel::Tunnel`], an env is
//! validated at construction and otherwise treated as an opaque label.

use super::error::EnvError;
use std::fmt;
use std::str::FromStr;

/// Maximum accepted env length in bytes. An env is an externally-controlled
/// identifier (sender path segment + receiver `/whoami` field), so it is
/// length-bounded like every other ingress input. Restate Cloud env ids are
/// `env_<ver><ulid>` → ~28 chars after the `env_` strip, so 64 is generous.
pub const MAX_ENV_LEN: usize = 64;

/// A validated environment id. Always non-empty, alphanumeric
/// (`[A-Za-z0-9]+`) — the same charset as [`super::tunnel::Tunnel`] — and at
/// most [`MAX_ENV_LEN`] bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Env(String);

impl Env {
    /// Construct an `Env`, validating the input.
    pub fn new(s: impl Into<String>) -> Result<Self, EnvError> {
        let s = s.into();
        if s.is_empty() {
            return Err(EnvError::Empty);
        }
        if s.len() > MAX_ENV_LEN {
            return Err(EnvError::TooLong(MAX_ENV_LEN));
        }
        if !s.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(EnvError::NonAlphanumeric);
        }
        Ok(Self(s))
    }

    /// The env as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Env {
    type Err = EnvError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl AsRef<str> for Env {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// Serialises as a plain string so the `/whoami` wire body carries
// `"env": "..."`. No `Deserialize`: inbound envs go through `Env::new`.
impl serde::Serialize for Env {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_alphanumeric() {
        assert!(Env::new("201kqw76hvvdkhsvhdajh77zxek").is_ok());
        assert!(Env::new("E1").is_ok());
        assert!(Env::new("0").is_ok());
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(Env::new(""), Err(EnvError::Empty));
    }

    #[test]
    fn rejects_too_long() {
        // At the cap is fine; one over is rejected before the charset check.
        assert!(Env::new("a".repeat(MAX_ENV_LEN)).is_ok());
        assert_eq!(
            Env::new("a".repeat(MAX_ENV_LEN + 1)),
            Err(EnvError::TooLong(MAX_ENV_LEN))
        );
    }

    #[test]
    fn rejects_special_characters() {
        for bad in ["e-1", "e 1", "e_1", "e/1", "e.1"] {
            assert_eq!(
                Env::new(bad),
                Err(EnvError::NonAlphanumeric),
                "reject {bad:?}"
            );
        }
    }

    #[test]
    fn round_trips_via_display_and_from_str() {
        let e = Env::new("env123").unwrap();
        assert_eq!(e.to_string(), "env123");
        assert_eq!(e.as_str(), "env123");
        let p: Env = "env123".parse().unwrap();
        assert_eq!(p, e);
    }
}
