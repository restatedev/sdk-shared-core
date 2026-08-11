//! The URL path grammar, both directions.
//!
//! * Sender → broker (`:9080`):
//!   `/<env>/<tunnel>/<proto>/<host>/<port>/<tail>`, parsed by [`parse`].
//!   The broker routes on the **`(env, tunnel)` pair** — a tunnel is only
//!   meaningful within an env — and forwards the rest.
//! * Broker → receiver: `/<proto>/<host>/<port>/<tail>` (the env + tunnel
//!   removed, the proto + target kept), parsed by [`parse_forwarded`]. The
//!   receiver uses `host`/`port` to dial its local target and forwards
//!   `tail` to it.
//!
//! `<proto>` must be the literal `http` — the only transport implemented. Both
//! parsers reject any other value ([`SUPPORTED_PROTO`]), so an unimplemented
//! transport (e.g. `https`) can never be accepted and silently dialed as
//! cleartext; the grammar still keeps the segment so a real transport can be
//! added by widening the gate. `<host>` and `<port>`
//! are **opaque** to the broker (it neither validates nor restricts them —
//! see `design.md` §4.3 / §10.1). The receiver is the first validator:
//! [`parse_forwarded`] is the receiver's tool, so it *does* reject an empty
//! host and a non-numeric/out-of-range port.
//!
//! No path normalisation is performed in either direction: `..`, `.`, `//`,
//! and percent-encoding pass through verbatim (design §4.4 rule 7).

use super::env::Env;
use super::error::{EnvError, PathError, TunnelError};
use super::tunnel::Tunnel;

/// The only `<proto>` the relay implements. Both parsers reject any other value
/// (including `https` and mixed case) so an unimplemented transport can never be
/// accepted and silently dialed as cleartext. Adding a transport means widening
/// this gate deliberately, not just carrying the string through.
pub const SUPPORTED_PROTO: &str = "http";

/// Maximum accepted `:path` length in bytes (both parsers). The h2
/// `max_header_list_size` cap is the primary bound on the request header block;
/// this is defense-in-depth so a single pseudo-header can't drive an outsized
/// allocation before that cap applies, and it bounds the `String`s the parser
/// builds. 8 KiB comfortably covers any legitimate `/<env>/<tunnel>/http/<host>/<port>/<tail>`.
pub const MAX_PATH_LEN: usize = 8192;

/// Maximum accepted target host length in bytes (forwarded form). 255 is the
/// DNS name limit; an IP literal is far shorter.
pub const MAX_HOST_LEN: usize = 255;

/// Result of parsing a sender request path.
#[derive(Debug, PartialEq, Eq)]
pub struct ParsedPath {
    /// The alphanumeric env from segment 0 — the routing namespace.
    pub env: Env,
    /// The tunnel from segment 1 (`[A-Za-z0-9._-]+`). Meaningful only within `env`.
    pub tunnel: Tunnel,
    /// The path to forward to the receiver, including any query string.
    /// Always begins with `/<proto>/<host>/<port>` (env + tunnel removed).
    pub forwarded_path: String,
}

/// Parse a sender `:path` of the form
/// `/<env>/<tunnel>/<proto>/<host>/<port>/<tail>`.
///
/// Returns the env + tunnel (the `(env, tunnel)` routing key) and the path to
/// forward to the receiver. The forwarded path is `/<proto>/<host>/<port>`
/// followed by any suffix segments joined by `/`, plus any query string from
/// the original path. The `<env>` and `<tunnel>` segments are dropped — the
/// receiver never sees them (design §4.3); the proto + target are kept.
pub fn parse(path: &str) -> Result<ParsedPath, PathError> {
    if path.len() > MAX_PATH_LEN {
        return Err(PathError::PathTooLong(MAX_PATH_LEN));
    }
    let (raw_path, query) = split_query(path);

    if !raw_path.starts_with('/') {
        return Err(PathError::Malformed);
    }

    let segments: Vec<&str> = raw_path[1..].split('/').collect();

    // Segment 0 is the env — the routing namespace. Validated alphanumeric.
    let Some(env_seg) = segments.first() else {
        return Err(PathError::MissingEnv);
    };
    if env_seg.is_empty() {
        return Err(PathError::MissingEnv);
    }
    let env = Env::new(*env_seg).map_err(|e| match e {
        EnvError::Empty => PathError::MissingEnv,
        // A too-long or out-of-charset env segment is an invalid env either way.
        EnvError::NonAlphanumeric | EnvError::TooLong(_) => PathError::InvalidEnv,
    })?;

    // Segment 1 is the tunnel — meaningful only within the env. Charset
    // [A-Za-z0-9._-] (validated by `Tunnel`).
    let Some(tunnel_seg) = segments.get(1) else {
        return Err(PathError::MissingTunnel);
    };
    if tunnel_seg.is_empty() {
        return Err(PathError::MissingTunnel);
    }
    let tunnel = Tunnel::new(*tunnel_seg).map_err(|e| match e {
        TunnelError::Empty => PathError::MissingTunnel,
        // A too-long or out-of-charset tunnel segment is an invalid tunnel.
        TunnelError::InvalidChar | TunnelError::TooLong(_) => PathError::InvalidTunnel,
    })?;

    // Segment 2 is the proto. It MUST be the literal `http` — the only
    // transport implemented. Reject anything else at this shared boundary
    // (e.g. `https`, mixed case) rather than carry it forward, so it can never
    // be silently dialed as cleartext downstream (remediation Phase 8.2).
    let Some(proto_seg) = segments.get(2) else {
        return Err(PathError::MissingProto);
    };
    if proto_seg.is_empty() {
        return Err(PathError::MissingProto);
    }
    if *proto_seg != SUPPORTED_PROTO {
        // Payload-free: do not retain or echo the attacker-controlled proto
        // value (see `PathError::UnsupportedProto`).
        return Err(PathError::UnsupportedProto);
    }

    // Segments 3 (host) and 4 (port) MUST be present structurally; content
    // is opaque here (the receiver validates).
    if segments.len() < 5 {
        return Err(PathError::MissingTarget);
    }

    // Forwarded path: drop env (0) + tunnel (1), keep
    // `/<proto>/<host>/<port>/<suffix...>` (segments 2..) + query.
    let mut forwarded = String::with_capacity(raw_path.len());
    forwarded.push('/');
    forwarded.push_str(&segments[2..].join("/"));
    if let Some(q) = query {
        forwarded.push_str(q);
    }

    Ok(ParsedPath {
        env,
        tunnel,
        forwarded_path: forwarded,
    })
}

/// The target a receiver should dial, parsed from the forwarded path
/// `/<proto>/<host>/<port>/<tail>`.
#[derive(Debug, PartialEq, Eq)]
pub struct ForwardedTarget {
    /// Transport the sender named. Always `http` — [`parse_forwarded`] rejects
    /// anything else ([`SUPPORTED_PROTO`]). Kept as a field so a future
    /// transport can branch on it once the gate is widened.
    pub proto: String,
    /// Target host (opaque string: IP or DNS name). Non-empty.
    pub host: String,
    /// Target port. Validated `1..=65535`.
    pub port: u16,
    /// The path to forward to the target, beginning with `/` and including
    /// any query string. `/http/h/9080` (no suffix) yields `tail = "/"`.
    pub tail: String,
}

/// Parse the forwarded path `/<proto>/<host>/<port>/<tail>` the broker sends
/// to a receiver. Unlike [`parse`], this validates the target the broker
/// treated as opaque: empty host → [`PathError::InvalidHost`], non-numeric
/// or out-of-range port → [`PathError::InvalidPort`].
pub fn parse_forwarded(path: &str) -> Result<ForwardedTarget, PathError> {
    if path.len() > MAX_PATH_LEN {
        return Err(PathError::PathTooLong(MAX_PATH_LEN));
    }
    let (raw_path, query) = split_query(path);

    if !raw_path.starts_with('/') {
        return Err(PathError::Malformed);
    }

    let segments: Vec<&str> = raw_path[1..].split('/').collect();

    // Segment 0 = proto (must be literal `http`), 1 = host, 2 = port, 3.. = tail.
    let proto = match segments.first() {
        Some(p) if !p.is_empty() => *p,
        _ => return Err(PathError::MissingProto),
    };
    if proto != SUPPORTED_PROTO {
        // Defense in depth: the broker only builds `http` forwarded paths, but a
        // receiver must never dial a non-`http` proto as cleartext either.
        // Payload-free (see `PathError::UnsupportedProto`).
        return Err(PathError::UnsupportedProto);
    }
    let host = match segments.get(1) {
        Some(h) => *h,
        None => return Err(PathError::MissingTarget),
    };
    let port_seg = match segments.get(2) {
        Some(p) => *p,
        None => return Err(PathError::MissingTarget),
    };
    if host.is_empty() {
        return Err(PathError::InvalidHost);
    }
    if host.len() > MAX_HOST_LEN {
        return Err(PathError::HostTooLong(MAX_HOST_LEN));
    }
    let port: u16 = port_seg
        .parse()
        .map_err(|_| PathError::InvalidPort(port_seg.to_string()))?;

    // Tail = "/" + segments[3..] joined, + query. No suffix → "/".
    let mut tail = String::with_capacity(raw_path.len());
    tail.push('/');
    if segments.len() > 3 {
        tail.push_str(&segments[3..].join("/"));
    }
    if let Some(q) = query {
        tail.push_str(q);
    }

    Ok(ForwardedTarget {
        proto: proto.to_string(),
        host: host.to_string(),
        port,
        tail,
    })
}

/// Split a path into `(path_without_query, Some("?..."))`.
fn split_query(path: &str) -> (&str, Option<&str>) {
    match path.find('?') {
        Some(idx) => (&path[..idx], Some(&path[idx..])),
        None => (path, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Sender form: parse ────────────────────────────────────────────

    fn ok(path: &str, env: &str, tunnel: &str, forwarded: &str) {
        let parsed = parse(path).unwrap_or_else(|e| panic!("expected ok for {path:?}, got {e:?}"));
        assert_eq!(parsed.env.as_str(), env, "env for {path:?}");
        assert_eq!(parsed.tunnel.as_str(), tunnel, "tunnel for {path:?}");
        assert_eq!(parsed.forwarded_path, forwarded, "forwarded for {path:?}");
    }

    fn err(path: &str, expected: PathError) {
        assert_eq!(parse(path), Err(expected), "for path {path:?}");
    }

    #[test]
    fn basic() {
        ok(
            "/e1/abc123/http/127.0.0.1/9080/foo",
            "e1",
            "abc123",
            "/http/127.0.0.1/9080/foo",
        );
    }

    #[test]
    fn example_from_spec() {
        // The real-world example: env / tunnel / http / host / port / (empty tail).
        ok(
            "/201kqw76hvvdkhsvhdajh77zxek/mytunnel/http/replit-agent.restate-apps/9080/",
            "201kqw76hvvdkhsvhdajh77zxek",
            "mytunnel",
            "/http/replit-agent.restate-apps/9080/",
        );
    }

    #[test]
    fn nested_path_with_query() {
        ok(
            "/e1/abc123/http/10.0.0.5/8080/users/42?fields=name",
            "e1",
            "abc123",
            "/http/10.0.0.5/8080/users/42?fields=name",
        );
    }

    #[test]
    fn trailing_slash_preserved() {
        ok("/e1/abc/http/h/9080/", "e1", "abc", "/http/h/9080/");
    }

    #[test]
    fn empty_suffix() {
        ok("/e1/abc/http/h/9080", "e1", "abc", "/http/h/9080");
    }

    #[test]
    fn deep_path() {
        ok(
            "/env/key/http/host/1/a/b/c/d/e",
            "env",
            "key",
            "/http/host/1/a/b/c/d/e",
        );
    }

    #[test]
    fn dns_host_with_dots_and_dashes() {
        ok(
            "/e1/c/http/my-svc.ns.svc.cluster.local/8080/x",
            "e1",
            "c",
            "/http/my-svc.ns.svc.cluster.local/8080/x",
        );
    }

    #[test]
    fn host_and_port_content_not_validated_by_sender_parse() {
        ok(
            "/e1/c/http/not_a_host/not_a_port/x",
            "e1",
            "c",
            "/http/not_a_host/not_a_port/x",
        );
    }

    #[test]
    fn no_path_normalization() {
        ok(
            "/e1/c/http/h/9080/../etc/passwd",
            "e1",
            "c",
            "/http/h/9080/../etc/passwd",
        );
        ok("/e1/c/http/h/9080//foo", "e1", "c", "/http/h/9080//foo");
    }

    #[test]
    fn percent_encoding_forwarded_verbatim() {
        ok(
            "/e1/c/http/h/9080/hello%20world",
            "e1",
            "c",
            "/http/h/9080/hello%20world",
        );
    }

    #[test]
    fn query_on_empty_suffix() {
        ok("/e1/abc/http/h/9080?q=1", "e1", "abc", "/http/h/9080?q=1");
    }

    #[test]
    fn missing_env() {
        err("/", PathError::MissingEnv);
        err("", PathError::Malformed);
    }

    #[test]
    fn invalid_env_rejected() {
        err("/e-1/c/http/h/9080/x", PathError::InvalidEnv);
    }

    #[test]
    fn missing_tunnel() {
        err("/e1", PathError::MissingTunnel);
        err("/e1//http/h/9080/x", PathError::MissingTunnel);
    }

    #[test]
    fn dashed_dotted_underscored_tunnel_accepted() {
        // The relaxed tunnel charset `[A-Za-z0-9._-]+` (Cloud tunnel-name
        // compatibility) accepts these; none collide with the `/` delimiter.
        ok(
            "/e1/abc-123/http/h/9080/foo",
            "e1",
            "abc-123",
            "/http/h/9080/foo",
        );
        ok(
            "/e1/a.b_c-d/http/h/9080/x",
            "e1",
            "a.b_c-d",
            "/http/h/9080/x",
        );
    }

    #[test]
    fn tunnel_with_out_of_charset_char_rejected() {
        // `~` is URL-safe (unreserved) but outside `[A-Za-z0-9._-]`.
        err("/e1/abc~123/http/h/9080/foo", PathError::InvalidTunnel);
    }

    #[test]
    fn missing_proto() {
        err("/e1/abc", PathError::MissingProto);
        err("/e1/abc//h/9080/x", PathError::MissingProto);
    }

    #[test]
    fn missing_host_or_port() {
        err("/e1/abc/http", PathError::MissingTarget);
        err("/e1/abc/http/h", PathError::MissingTarget);
    }

    #[test]
    fn missing_leading_slash() {
        err("e1/abc/http/h/9080/foo", PathError::Malformed);
    }

    #[test]
    fn rejects_over_long_path() {
        // A path longer than MAX_PATH_LEN is rejected up front (defense-in-depth
        // beyond the h2 header-list-size cap).
        let long_tail = "a".repeat(MAX_PATH_LEN);
        let path = format!("/e1/abc/http/h/9080/{long_tail}");
        assert!(path.len() > MAX_PATH_LEN);
        err(&path, PathError::PathTooLong(MAX_PATH_LEN));
    }

    #[test]
    fn parse_rejects_non_http_proto() {
        // Only literal `http` is accepted; anything else (incl. mixed case) is
        // rejected here so it can't be carried forward + dialed as cleartext.
        // The error is payload-free — it never echoes the rejected proto value.
        err("/e1/abc/https/h/9080/x", PathError::UnsupportedProto);
        err("/e1/abc/HTTP/h/9080/x", PathError::UnsupportedProto);
        err("/e1/abc/Http/h/9080/x", PathError::UnsupportedProto);
        err("/e1/abc/tcp/h/9080/x", PathError::UnsupportedProto);
        err("/e1/abc/ftp/h/9080/x", PathError::UnsupportedProto);
        // Empty proto is still MissingProto, not UnsupportedProto.
        err("/e1/abc//h/9080/x", PathError::MissingProto);
    }

    #[test]
    fn unsupported_proto_error_never_echoes_the_rejected_value() {
        // Payload-free by design: an attacker-controlled (up to MAX_PATH_LEN)
        // proto value must not be retained or rendered. Feed a large, distinctive
        // proto and assert the formatted error does not contain any of its bytes.
        let attacker = "SECRETPROTO".repeat(64);
        let path = format!("/e1/abc/{attacker}/h/9080/x");
        let e = parse(&path).unwrap_err();
        assert_eq!(e, PathError::UnsupportedProto);
        let rendered = e.to_string();
        assert!(
            !rendered.contains("SECRETPROTO"),
            "UnsupportedProto must not echo the rejected value, got: {rendered}"
        );
    }

    // ── Forwarded form: parse_forwarded ───────────────────────────────

    fn fwd_ok(path: &str, proto: &str, host: &str, port: u16, tail: &str) {
        let t =
            parse_forwarded(path).unwrap_or_else(|e| panic!("expected ok for {path:?}, got {e:?}"));
        assert_eq!(t.proto, proto, "proto for {path:?}");
        assert_eq!(t.host, host, "host for {path:?}");
        assert_eq!(t.port, port, "port for {path:?}");
        assert_eq!(t.tail, tail, "tail for {path:?}");
    }

    fn fwd_err(path: &str, expected: PathError) {
        assert_eq!(parse_forwarded(path), Err(expected), "for path {path:?}");
    }

    #[test]
    fn forwarded_rejects_non_http_proto() {
        // Defense in depth on the receiver side: never dial a non-http proto.
        // Payload-free error (no rejected value echoed).
        fwd_err("/https/h/9080/x", PathError::UnsupportedProto);
        fwd_err("/HTTP/h/9080/x", PathError::UnsupportedProto);
        fwd_err("/Http/h/9080/x", PathError::UnsupportedProto);
        fwd_err("//h/9080/x", PathError::MissingProto);
    }

    #[test]
    fn forwarded_basic() {
        fwd_ok(
            "/http/127.0.0.1/9080/orders",
            "http",
            "127.0.0.1",
            9080,
            "/orders",
        );
    }

    #[test]
    fn forwarded_with_query() {
        fwd_ok(
            "/http/127.0.0.1/9080/orders?x=1",
            "http",
            "127.0.0.1",
            9080,
            "/orders?x=1",
        );
    }

    #[test]
    fn forwarded_no_suffix_is_root() {
        fwd_ok("/http/h/9080", "http", "h", 9080, "/");
        fwd_ok("/http/h/9080?q=1", "http", "h", 9080, "/?q=1");
    }

    #[test]
    fn forwarded_trailing_slash() {
        fwd_ok("/http/h/9080/", "http", "h", 9080, "/");
    }

    #[test]
    fn forwarded_deep_tail_and_dns_host() {
        fwd_ok(
            "/http/svc.ns.svc.cluster.local/8080/a/b/c",
            "http",
            "svc.ns.svc.cluster.local",
            8080,
            "/a/b/c",
        );
    }

    #[test]
    fn forwarded_rejects_empty_host() {
        fwd_err("/http//9080/x", PathError::InvalidHost);
    }

    #[test]
    fn forwarded_rejects_over_long_host() {
        let host = "h".repeat(MAX_HOST_LEN + 1);
        let path = format!("/http/{host}/9080/x");
        fwd_err(&path, PathError::HostTooLong(MAX_HOST_LEN));
        // At the cap is fine.
        let host = "h".repeat(MAX_HOST_LEN);
        let path = format!("/http/{host}/9080/x");
        assert!(parse_forwarded(&path).is_ok());
    }

    #[test]
    fn forwarded_rejects_over_long_path() {
        let long_tail = "a".repeat(MAX_PATH_LEN);
        let path = format!("/http/h/9080/{long_tail}");
        assert!(path.len() > MAX_PATH_LEN);
        fwd_err(&path, PathError::PathTooLong(MAX_PATH_LEN));
    }

    #[test]
    fn forwarded_rejects_non_numeric_port() {
        fwd_err(
            "/http/h/notaport/x",
            PathError::InvalidPort("notaport".to_string()),
        );
    }

    #[test]
    fn forwarded_rejects_out_of_range_port() {
        fwd_err(
            "/http/h/70000/x",
            PathError::InvalidPort("70000".to_string()),
        );
    }

    #[test]
    fn forwarded_rejects_missing_port() {
        fwd_err("/http/h", PathError::MissingTarget);
    }

    #[test]
    fn forwarded_rejects_malformed() {
        fwd_err("http/h/9080", PathError::Malformed);
    }

    // --- Restate-as-sender contract (the e2e example rests on this) ---
    //
    // Restate registers a base URL
    // `http://relay:9080/<env>/<tunnel>/http/<host>/<port>` and appends
    // `/discover` and `/invoke/<svc>/<handler>`. The relay strips its
    // `/<env>/<tunnel>` routing prefix and forwards the (proto + target +
    // possibly `invoke`-containing) tail verbatim.

    #[test]
    fn restate_discovery_path() {
        ok(
            "/e1/demo/http/127.0.0.1/9080/discover",
            "e1",
            "demo",
            "/http/127.0.0.1/9080/discover",
        );
    }

    #[test]
    fn restate_invocation_path() {
        ok(
            "/e1/demo/http/127.0.0.1/9080/invoke/greeter/greet",
            "e1",
            "demo",
            "/http/127.0.0.1/9080/invoke/greeter/greet",
        );
    }

    #[test]
    fn restate_forwarded_tail_round_trips_at_receiver() {
        let t = parse_forwarded("/http/127.0.0.1/9080/invoke/greeter/greet").unwrap();
        assert_eq!(t.proto, "http");
        assert_eq!(t.host, "127.0.0.1");
        assert_eq!(t.port, 9080);
        assert_eq!(t.tail, "/invoke/greeter/greet");

        let d = parse_forwarded("/http/127.0.0.1/9080/discover").unwrap();
        assert_eq!(d.tail, "/discover");
    }
}
