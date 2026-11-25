//! The values user code passes to an action that the runtime would refuse as malformed, checked
//! before the action is sent (D65): an invalid one fails that action alone, with a terminal error
//! user code can catch, rather than the whole attempt with a protocol violation.
//!
//! The rules mirror the runtime's (`restate/crates/invoker-impl/src/explicit_recovery/actions.rs`):
//! what it parses out of an action and refuses as a violation, where the value comes from user
//! code. What it refuses with a failure result instead (a call's target that cannot be invoked,
//! D22) needs no check here.

/// Restate's invocation ids (`restate/crates/types/src/identifiers.rs`, `id_util.rs`): the
/// resource type `inv`, `_`, the id scheme version `1`, then the partition key (a `u64`) and the
/// invocation's ULID (a `u128`), each in base62 (`0-9a-zA-Z`) padded with `0` to a fixed width.
pub(crate) mod invocation_id {
    const PREFIX: &str = "inv_1";
    /// Base62 digits of a `u64`, and of a `u128`: 6 bits each, rounded up.
    const PARTITION_KEY_DIGITS: usize = 11;
    const UUID_DIGITS: usize = 22;
    pub(crate) const LEN: usize = PREFIX.len() + PARTITION_KEY_DIGITS + UUID_DIGITS;

    #[cfg(any(test, feature = "__fake_runtime"))]
    const ALPHABET: &[u8; 62] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    fn digit(c: u8) -> Option<u128> {
        match c {
            b'0'..=b'9' => Some((c - b'0') as u128),
            b'a'..=b'z' => Some((c - b'a') as u128 + 10),
            b'A'..=b'Z' => Some((c - b'A') as u128 + 36),
            _ => None,
        }
    }

    /// `digits`, base62, as a number; `None` if a character is not a digit, or above `u128`.
    fn decode(digits: &[u8]) -> Option<u128> {
        digits
            .iter()
            .try_fold(0u128, |n, &c| n.checked_mul(62)?.checked_add(digit(c)?))
    }

    /// Whether the runtime parses `id` as an invocation id; if not, why.
    pub(crate) fn check(id: &str) -> Result<(), String> {
        let why = if !id.starts_with("inv_") {
            "it does not start with 'inv_'"
        } else if !id.starts_with(PREFIX) {
            "its version is not 1 (it does not start with 'inv_1')"
        } else if id.len() != LEN {
            "it is not 38 characters long ('inv_1' and 33 base62 digits)"
        } else {
            let bytes = id.as_bytes();
            let partition_key = decode(&bytes[PREFIX.len()..PREFIX.len() + PARTITION_KEY_DIGITS]);
            let uuid = decode(&bytes[PREFIX.len() + PARTITION_KEY_DIGITS..]);
            match (partition_key, uuid) {
                (Some(partition_key), Some(_)) if partition_key <= u64::MAX as u128 => {
                    return Ok(())
                }
                _ if !bytes[PREFIX.len()..].iter().all(u8::is_ascii_alphanumeric) => {
                    "it has a character that is not a base62 digit (0-9, a-z, A-Z)"
                }
                _ => "its digits are out of range",
            }
        };
        Err(format!("'{id}' is not an invocation id: {why}"))
    }

    #[cfg(any(test, feature = "__fake_runtime"))]
    fn encode(mut n: u128, width: usize, out: &mut String) {
        let mut digits = vec![b'0'; width];
        for slot in digits.iter_mut().rev() {
            *slot = ALPHABET[(n % 62) as usize];
            n /= 62;
        }
        debug_assert_eq!(n, 0, "the number fits its width");
        out.push_str(std::str::from_utf8(&digits).expect("ASCII"));
    }

    /// The id the runtime writes for the invocation of `partition_key` and `uuid`.
    #[cfg(any(test, feature = "__fake_runtime"))]
    pub(crate) fn format(partition_key: u64, uuid: u128) -> String {
        let mut id = String::with_capacity(LEN);
        id.push_str(PREFIX);
        // The runtime encodes the big-endian value, and decodes it back (`base62_util.rs`).
        encode(partition_key.to_be() as u128, PARTITION_KEY_DIGITS, &mut id);
        encode(uuid.to_be(), UUID_DIGITS, &mut id);
        id
    }
}

/// A scope, of a call's target or of an attach's (`restate_util_string::RestrictedValue`): 1 to
/// 36 characters of `[a-zA-Z0-9_.-]`.
pub(crate) fn check_scope(scope: &str) -> Result<(), String> {
    const MAX_LEN: usize = 36;
    let why = if scope.is_empty() {
        "it is empty"
    } else if scope.len() > MAX_LEN {
        "it is longer than 36 characters"
    } else if !scope
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
    {
        "it has a character outside [a-zA-Z0-9_.-]"
    } else {
        return Ok(());
    };
    Err(format!("'{scope}' is not a scope: {why}"))
}

#[cfg(test)]
mod tests {
    use super::check_scope;
    use super::invocation_id::{check, format, LEN};

    #[test]
    fn real_ids_pass() {
        // An id a Restate server minted.
        let id = "inv_1aGuEfH1fC0F1tDaHYyy2FfUD7DhkVvjSl";
        assert_eq!(id.len(), LEN);
        check(id).unwrap();
    }

    #[test]
    fn formatted_ids_pass_and_are_distinct() {
        let ids = [
            format(0, 0),
            format(0, 1),
            format(1, 0),
            format(u64::MAX, u128::MAX),
        ];
        for id in &ids {
            assert_eq!(id.len(), LEN, "{id}");
            check(id).unwrap();
        }
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            ids.len()
        );
        assert_eq!(format(0, 0), "inv_1000000000000000000000000000000000");
    }

    #[test]
    fn malformed_ids_fail() {
        // A partition key above u64 (11 digits hold up to 62^11 - 1), and a ULID above u128.
        let big_partition_key = format!("inv_1{}{}", "Z".repeat(11), "0".repeat(22));
        let big_uuid = format!("inv_1{}{}", "0".repeat(11), "Z".repeat(22));
        for id in [
            "",
            "inv",
            "inv_",
            "inv_garbage",
            "inv_2aGuEfH1fC0F1tDaHYyy2FfUD7DhkVvjSl",
            "prom_1aGuEfH1fC0F1tDaHYyy2FfUD7DhkVvjSl",
            "inv_1aGuEfH1fC0F1tDaHYyy2FfUD7DhkVvjS",
            "inv_1aGuEfH1fC0F1tDaHYyy2FfUD7DhkVvjSlx",
            "inv_1aGuEfH1fC0-1tDaHYyy2FfUD7DhkVvjSl",
            // The e2e campaigns': a ULID above u128 (and 2 characters too many), a short one.
            "inv_1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "inv_1fakeruntime0000000000000000",
            "inv_1fakecallee000000000000000001",
            &big_partition_key,
            &big_uuid,
            "inv_1aGuEfH1fC0F1tDaHYyy2FfUD7DhkVvjé",
        ] {
            assert!(check(id).is_err(), "{id:?} passed");
        }
    }

    #[test]
    fn scopes() {
        check_scope("tenant-1.eu_west").unwrap();
        check_scope(&"s".repeat(36)).unwrap();
        for scope in ["", "a/b", "with space", "é", &"s".repeat(37)] {
            assert!(check_scope(scope).is_err(), "{scope:?} passed");
        }
    }
}
