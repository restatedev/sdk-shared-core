//! Shared HTTP/2 protocol-level tuning knobs.
//!
//! Every h2 endpoint in the system accepts the same set of frame-size and
//! flow-control window settings via this struct: the broker's sender-facing
//! server (`:9080`) and receiver-facing client (`:8080` after role-flip),
//! and — once they exist — the receiver's relay-facing server and the
//! sender's client.
//!
//! For large-payload forwarded requests, raising
//! [`H2Tuning::initial_window_size`] reduces `WINDOW_UPDATE` round-trips
//! per stream; raising [`H2Tuning::max_frame_size`] lets each DATA frame
//! carry more bytes. The connection window is the binding constraint at
//! high stream-fan-in (see the field doc).

use serde::Deserialize;

/// Default `SETTINGS_MAX_HEADER_LIST_SIZE` (64 KiB) applied to **every** h2
/// endpoint when [`H2Tuning::max_header_list_size`] is unset. Load-bearing for
/// pre-auth DoS resistance: the h2 crate's own default is **16 MiB**, so a
/// listener that never sets this would accept a 16 MiB header block from an
/// unauthenticated peer. Because [`H2Tuning::apply_server`] /
/// [`apply_client`](H2Tuning::apply_client) fall back to this constant (not to
/// h2's default) when the field is `None`, the cap can never silently vanish —
/// not on a partial `[h2.*]` config section, not on the raw-defaults path.
///
/// NB: h2 enforces this *softly* up to 4×: a header block between `max` and
/// `4 × max` is a per-stream reject (RST), only `> 4 × max` aborts the whole
/// connection. So the real per-block ceiling is `4 × 64 KiB = 256 KiB`.
pub const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 65536;

/// HTTP/2 tuning. Frame-size / window fields are optional; `None` leaves the h2
/// crate's default in place. `max_header_list_size` is special — it is *always*
/// applied (falling back to [`DEFAULT_MAX_HEADER_LIST_SIZE`] when `None`) so the
/// pre-auth header cap can never silently vanish. Use [`H2Tuning::default`] for
/// the recommended relay tuning (see field docs) or [`H2Tuning::h2_raw_defaults`]
/// to fall back to the h2 crate's wire windows (still with the header cap).
#[derive(Debug, Clone, Deserialize)]
pub struct H2Tuning {
    /// Largest frame size we accept (and prefer to send), in bytes.
    /// HTTP/2 caps this at `2^24 - 1` (16 MiB - 1). h2 crate default is
    /// 16 KiB; relay default is 64 KiB — for large-payload forwarded
    /// requests, larger frames mean less framing overhead per byte.
    pub max_frame_size: Option<u32>,
    /// Initial flow-control window per stream, in bytes. h2 crate
    /// default is 64 KiB; relay default is 1 MiB. Raising this reduces
    /// `WINDOW_UPDATE` chatter on long-lived streams and lets a single
    /// stream push more bytes in flight before being window-blocked.
    pub initial_window_size: Option<u32>,
    /// Initial flow-control window for the whole connection, in bytes.
    /// h2 crate default is 64 KiB; relay default is 16 MiB. This is the
    /// **binding constraint at high stream-fan-in** — every stream on a
    /// connection shares it, so with hundreds-to-thousands of streams per
    /// connection the per-stream window (above) is unreachable and this
    /// window governs aggregate throughput. A high-fan-in bench (2000
    /// streams / 2 connections, 64 KiB frames) measured +28% request-body
    /// throughput going 4 MiB → 16 MiB and +79% at 64 MiB; 16 MiB is the
    /// balanced default (the window is a *ceiling* on un-acked buffered
    /// bytes per connection, not steady-state use, and connection count is
    /// O(senders+receivers), not O(streams)). Deployments with extreme
    /// fan-in and memory headroom can raise this to 64 MiB+ via `[h2]`.
    pub initial_connection_window_size: Option<u32>,
    /// `SETTINGS_MAX_HEADER_LIST_SIZE` — the largest request/response header
    /// block (sum of all header name+value+overhead bytes) this endpoint
    /// accepts, in bytes. Unlike the fields above, `None` here does **not** mean
    /// "h2 default" — [`apply_server`](Self::apply_server) /
    /// [`apply_client`](Self::apply_client) fall back to
    /// [`DEFAULT_MAX_HEADER_LIST_SIZE`] (64 KiB) so the pre-auth header cap is
    /// always in force (h2's own default is 16 MiB — unsafe for a public
    /// listener). Set explicitly to override; validated to `[1 KiB, 16 MiB]`.
    pub max_header_list_size: Option<u32>,
}

impl Default for H2Tuning {
    /// Relay's recommended tuning, applied by the broker's `AcceptorConfig`
    /// and `ServerConfig` defaults (and the receiver/sender):
    /// `max_frame_size = 64 KiB`,
    /// `initial_window_size = 1 MiB`,
    /// `initial_connection_window_size = 16 MiB`.
    fn default() -> Self {
        Self {
            max_frame_size: Some(65536),
            initial_window_size: Some(1024 * 1024),
            initial_connection_window_size: Some(16 * 1024 * 1024),
            max_header_list_size: Some(DEFAULT_MAX_HEADER_LIST_SIZE),
        }
    }
}

impl H2Tuning {
    /// All-fields-`None`: leave every setting at the h2 crate's wire
    /// default. Useful for tests that pin behaviour to the crate's
    /// defaults regardless of what we choose as recommended values.
    /// Frame-size / window fields all-`None` (h2 crate wire defaults), but the
    /// header cap still applies via [`DEFAULT_MAX_HEADER_LIST_SIZE`] — used by
    /// the cluster-forward raw path so it keeps h2's small windows yet still
    /// bounds the pre-auth header block. Pin-crate-default tests should note the
    /// header cap is intentionally present here.
    pub fn h2_raw_defaults() -> Self {
        Self {
            max_frame_size: None,
            initial_window_size: None,
            initial_connection_window_size: None,
            max_header_list_size: None,
        }
    }

    /// The effective header-list cap: the explicit value or
    /// [`DEFAULT_MAX_HEADER_LIST_SIZE`]. Never h2's 16 MiB default.
    fn effective_max_header_list_size(&self) -> u32 {
        self.max_header_list_size
            .unwrap_or(DEFAULT_MAX_HEADER_LIST_SIZE)
    }

    /// Validate the tuning knobs. `max_header_list_size`, when set, must be in
    /// `[1 KiB, 16 MiB]` (below 1 KiB would reject legitimate requests; above
    /// 16 MiB exceeds any sane header block and defeats the DoS bound).
    pub fn validate(&self) -> Result<(), &'static str> {
        if let Some(v) = self.max_header_list_size {
            if !(1024..=16 * 1024 * 1024).contains(&v) {
                return Err("max_header_list_size must be between 1024 and 16777216 bytes");
            }
        }
        Ok(())
    }

    /// Apply our settings to an `h2::server::Builder`. Frame-size / window fields
    /// left `None` stay at the builder's default; the header-list cap is
    /// **always** applied (explicit value or [`DEFAULT_MAX_HEADER_LIST_SIZE`]).
    pub fn apply_server(&self, builder: &mut h2::server::Builder) {
        if let Some(v) = self.max_frame_size {
            builder.max_frame_size(v);
        }
        if let Some(v) = self.initial_window_size {
            builder.initial_window_size(v);
        }
        if let Some(v) = self.initial_connection_window_size {
            builder.initial_connection_window_size(v);
        }
        builder.max_header_list_size(self.effective_max_header_list_size());
    }

    /// Apply our settings to an `h2::client::Builder`. Frame-size / window fields
    /// left `None` stay at the builder's default; the header-list cap is
    /// **always** applied (explicit value or [`DEFAULT_MAX_HEADER_LIST_SIZE`]).
    pub fn apply_client(&self, builder: &mut h2::client::Builder) {
        if let Some(v) = self.max_frame_size {
            builder.max_frame_size(v);
        }
        if let Some(v) = self.initial_window_size {
            builder.initial_window_size(v);
        }
        if let Some(v) = self.initial_connection_window_size {
            builder.initial_connection_window_size(v);
        }
        builder.max_header_list_size(self.effective_max_header_list_size());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_carries_the_header_cap() {
        assert_eq!(
            H2Tuning::default().max_header_list_size,
            Some(DEFAULT_MAX_HEADER_LIST_SIZE)
        );
    }

    #[test]
    fn raw_defaults_still_apply_the_header_cap() {
        // The field is None, but the *effective* cap is the const default, not
        // h2's 16 MiB — the whole point of the always-apply design.
        let raw = H2Tuning::h2_raw_defaults();
        assert_eq!(raw.max_header_list_size, None);
        assert_eq!(
            raw.effective_max_header_list_size(),
            DEFAULT_MAX_HEADER_LIST_SIZE
        );
    }

    #[test]
    fn validate_rejects_out_of_range_header_cap() {
        let with_cap = |v: Option<u32>| H2Tuning {
            max_header_list_size: v,
            ..Default::default()
        };
        assert!(with_cap(Some(512)).validate().is_err());
        assert!(with_cap(Some(32 * 1024 * 1024)).validate().is_err());
        assert!(with_cap(Some(65536)).validate().is_ok());
        assert!(with_cap(None).validate().is_ok());
    }
}
