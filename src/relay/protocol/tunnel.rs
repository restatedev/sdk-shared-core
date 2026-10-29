//! The `Tunnel` newtype: an opaque rendezvous token, `[A-Za-z0-9._-]+`.
//!
//! Tunnels are validated at construction and are otherwise treated as
//! opaque routing labels. The system never interprets tunnel contents.
//!
//! The charset is alphanumerics plus `.`, `_`, and `-`. The dot/dash/
//! underscore allowance exists for **Restate Cloud tunnel compatibility**
//! (`docs/cloud-tunnel-compat.md`): a Cloud tunnel client may present a
//! self-chosen `tunnel-name` matching Cloud's `^[A-Za-z0-9._-]+$`, and the
//! relay routes on it as an opaque label. None of those characters collide
//! with the path delimiter (`/`), so the sender-path grammar is unaffected.
//! `Env`, by contrast, stays strictly alphanumeric (Cloud env ids are
//! `<ver><ulid>`, alnum after the `env_` strip).

use super::error::TunnelError;
use std::fmt;
use std::str::FromStr;

/// Maximum accepted tunnel length in bytes. A tunnel is externally controlled
/// (sender path segment + receiver `/whoami`/`start-tunnel` field), so it is
/// length-bounded. Cloud tunnel names are short; 128 is generous headroom.
pub const MAX_TUNNEL_LEN: usize = 128;

/// A validated tunnel value. Always non-empty, `[A-Za-z0-9._-]+`, and at most
/// [`MAX_TUNNEL_LEN`] bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tunnel(String);

impl Tunnel {
    /// Whether `c` is allowed in a tunnel: ASCII alphanumeric or one of
    /// `.`, `_`, `-`. Kept as a single predicate so the charset has exactly
    /// one definition.
    fn is_valid_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
    }

    /// Construct a `Tunnel`, validating the input.
    pub fn new(s: impl Into<String>) -> Result<Self, TunnelError> {
        let s = s.into();
        if s.is_empty() {
            return Err(TunnelError::Empty);
        }
        if s.len() > MAX_TUNNEL_LEN {
            return Err(TunnelError::TooLong(MAX_TUNNEL_LEN));
        }
        if !s.chars().all(Self::is_valid_char) {
            return Err(TunnelError::InvalidChar);
        }
        Ok(Self(s))
    }

    /// The tunnel as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Tunnel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Tunnel {
    type Err = TunnelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl AsRef<str> for Tunnel {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// Serialises as a plain string so the `/whoami` wire body is
// `{"api_key": "...", "tunnel": "..."}`. Used by `whoami::WhoamiBody`.
// (No `Deserialize`: inbound tunnels go through `Tunnel::new` validation.)
impl serde::Serialize for Tunnel {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_alphanumeric() {
        assert!(Tunnel::new("abc123").is_ok());
        assert!(Tunnel::new("ABC").is_ok());
        assert!(Tunnel::new("0").is_ok());
        assert!(Tunnel::new("a").is_ok());
    }

    #[test]
    fn accepts_dot_dash_underscore() {
        // The relaxed charset (Cloud tunnel-name compatibility,
        // `^[A-Za-z0-9._-]+$`). A self-chosen Cloud tunnel name like these
        // must round-trip as an opaque routing label.
        for ok in ["abc-123", "abc_123", "abc.123", "a.b-c_d", "-", ".", "_"] {
            assert!(Tunnel::new(ok).is_ok(), "should accept {ok:?}");
        }
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(Tunnel::new(""), Err(TunnelError::Empty));
    }

    #[test]
    fn rejects_too_long() {
        assert!(Tunnel::new("a".repeat(MAX_TUNNEL_LEN)).is_ok());
        assert_eq!(
            Tunnel::new("a".repeat(MAX_TUNNEL_LEN + 1)),
            Err(TunnelError::TooLong(MAX_TUNNEL_LEN))
        );
    }

    #[test]
    fn rejects_out_of_charset_characters() {
        // Everything outside `[A-Za-z0-9._-]` is rejected, including the
        // path delimiter `/`, whitespace, and other punctuation.
        for bad in ["abc 123", "abc/123", "abc!", "abc~", "a:b", "a@b", "a+b"] {
            assert_eq!(
                Tunnel::new(bad),
                Err(TunnelError::InvalidChar),
                "should reject {bad:?}"
            );
        }
    }

    #[test]
    fn rejects_non_ascii() {
        // Unicode letters are not ASCII alphanumeric.
        assert_eq!(Tunnel::new("café"), Err(TunnelError::InvalidChar));
        assert_eq!(Tunnel::new("日本"), Err(TunnelError::InvalidChar));
    }

    #[test]
    fn round_trips_via_display() {
        let c = Tunnel::new("hello123").unwrap();
        assert_eq!(c.to_string(), "hello123");
        assert_eq!(c.as_str(), "hello123");
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Tunnel::new("a").unwrap());
        assert!(set.contains(&Tunnel::new("a").unwrap()));
        assert!(!set.contains(&Tunnel::new("b").unwrap()));
    }

    #[test]
    fn from_str_works() {
        let c: Tunnel = "abc".parse().unwrap();
        assert_eq!(c.as_str(), "abc");
        assert!("".parse::<Tunnel>().is_err());
    }
}
