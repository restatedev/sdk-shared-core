# Relay receiver in shared-core — plan

**Status:** **landed** (Phase 0–1). **Date:** 2026-08-11.
**Scope:** this repo (`sdk-shared-core`) absorbs the whole SDK-side relay Rust
stack — **`relay-protocol` + `relay-bridge` + `relay-receiver`** — plus a
**loopback engine**, as internal modules gated behind a `tunnel` cargo feature
so the crate still compiles to WASM (sans-IO) when the feature is off.

> **Implemented.** The stack lives under `src/relay/` (`protocol/`, `bridge.rs`,
> `receiver/`, `loopback.rs`) behind `#[cfg(feature = "tunnel")]`; the feature
> pulls in the optional `tokio`/`h2`/`http`/`tokio-rustls`/`rustls-pki-types`/
> `async-trait`/`serde_json` deps. Default build is unchanged & sans-IO
> (verified: no tokio in the default `cargo tree`); `--features tunnel` builds
> the engine, and `tests/relay_loopback.rs` proves a forwarded request round-
> trips relay → engine → loopback → local h2c server → back with the tail-only
> `:path` preserved. The native binding surface (`relay::{Config, Engine,
> Handle, Status}`) is what sdk-java/sdk-typescript wrap. Remaining: Phase 2
> (SDK bindings — sdk-java landed on its own branch) and Phase 3 (CI/release).

Sibling doc (consuming side, Java): `sdk-java/development/relay-receiver-plan.md`.

## What this repo is today

`restate-sdk-shared-core` is a **single, strictly sans-IO crate**: the
invocation VM (`CoreVM` / the `VM` trait) with **zero networking**. That's why
it ships as **WASM** (TS SDK + edge: Workers/Deno) and as a **no-runtime FFM
cdylib** (Java: `librestate_sdk_core.so`, driven by `FfmStateMachine`).
Bindings are vendored per-SDK (TS wasm-bindings; Java `sdk-core/src/main/rust`),
not in this repo.

## The approach: bring the relay stack in, behind a `tunnel` feature

Copy the three relay crates + the loopback engine into this crate as modules
gated by `#[cfg(feature = "tunnel")]`, with **no `relay-*` external deps** — only
third-party networking deps, made **optional** and pulled in by the feature:

```toml
[features]
default = []                      # sans-IO; WASM/edge use this
tunnel  = ["dep:tokio", "dep:h2", "dep:tokio-rustls", "dep:rustls-pki-types",
           "dep:async-trait"]

[dependencies]
# already present, non-optional: bytes, http (optional today), serde, thiserror, tracing
tokio            = { version = "1", optional = true, features = ["rt-multi-thread","net","io-util","sync","time","macros"] }
h2               = { version = "0.4", optional = true }
tokio-rustls     = { version = "0.26", optional = true, default-features = false, features = ["ring","tls12"] }
rustls-pki-types = { version = "1", optional = true }
async-trait      = { version = "0.1", optional = true }
```

Module layout (all `#[cfg(feature = "tunnel")]`):

```
src/relay/
├── mod.rs
├── protocol/     # was relay-protocol: Env/Tunnel, path grammar, whoami/start-tunnel, h2 tuning (sans-IO)
├── bridge.rs     # was relay-bridge: bidirectional h2 bridge
├── receiver/     # was relay-receiver: dial-out, role-flip, /whoami, liveness, redial, R4/R5, InvokeHandler
├── loopback.rs   # LoopbackHandler + Engine{start,stop,status} + Config
└── ffi.rs        # optional: control-plane facade (see Bindings)
```

- **Feature OFF (default):** no tokio in the dependency graph, no `src/relay`
  compiled → the crate is exactly as sans-IO as today → `wasm-pack` / edge /
  Lambda unaffected. This is the load-bearing invariant; the feature gate + the
  optional deps are all that enforce it.
- **Feature ON:** the relay stack + loopback engine compile in and the crate
  exposes the tunnel API. Native builds (Java cdylib, Node napi) enable it.

Payoff: the whole SDK-side relay stack is single-sourced here, the VM stays
WASM-able, and there is **no cross-repo coupling** — see the note below.

## No cross-repo coupling (the deliberate trade)

The three crates are **copied in**, not shared with `restatedev/relay`. The
relay repo keeps its own `relay-protocol` / `relay-bridge` / `relay-receiver`
untouched — the broker still builds, its `receiver_e2e` still uses the relay
repo's own copy, nobody publishes anything, no dependency inverts. The one
honest cost is that the code is **forked at move time**; keeping the two copies
in sync (if ever needed) is a manual/later concern — the same kind of workflow
the existing `translate-from-shared-core` skill already handles. Independence
was chosen over DRY, on purpose.

## Artifact × consumer matrix

| Consumer | Build | Contents |
|---|---|---|
| TS edge / Workers / Deno / Lambda | WASM, **default features** | VM only — sans-IO, unchanged |
| Java (all) | FFM cdylib, `--features tunnel` | one `librestate_sdk_core.so` exporting `vm_*` + `relay_tunnel_*`; tunnel **inert** unless started |
| TS Node **server** | napi addon, `--features tunnel` | tunnel engine (bridges to the loopback port where the WASM-VM node-http2 server listens) |

Java folds the tunnel into its existing cdylib (it already links the VM
natively). TS keeps VM-via-WASM (default) + tunnel-via-napi (feature on) as two
independent artifacts — the tunnel addon doesn't need the VM.

## The one-lib decision for Java (measured)

Enabling `tunnel` grows the Java cdylib, it doesn't add a second one. Measured
tunnel cdylib (relay workspace `lto=fat`, stripped, `ring`, **TLS linked**):

| | size |
|---|---|
| VM lib today | 1.60 MB |
| + tunnel, stripped | ~1.5–1.8 MB added |
| + tunnel, gzipped (jar-stored) | ~0.7–0.85 MB added |

An order of magnitude under the ~15–20 MB that would argue for a split. Lambda
ships ~0.7 MB gzipped of dead bytes (tokio spawns only on `relay_tunnel_start`).

## The engine (`src/relay/loopback.rs`)

- `LoopbackHandler: InvokeHandler` — per forwarded `Invocation`: h2c-dial
  `127.0.0.1:<localPort>`, replay method + **tail-only `:path`** (preserves
  request-identity, signed over the SDK-relative path) + headers verbatim,
  bridge both bodies via the in-crate `bridge`.
- `Engine::start(Config) -> Handle` builds a tokio runtime, spawns
  `receiver::Receiver::run(shutdown)`, holds the runtime + `Arc<Status>`; `stop`
  fires the shutdown future + joins; `status` reads the shared state.
- `Config { relay: Vec<String>, env, tunnel, api_key, local_port, tls,
  connections (R4), instance_id (R5), h2_tuning }`.

## Binding surfaces

Bindings stay vendored per-SDK and just enable `shared-core/tunnel`:

- **Java** — `sdk-core/src/main/rust` enables the feature and adds the
  `relay_tunnel_*` C ABI (`start`/`stop`/`status`, JSON config, `free_string`)
  next to the VM's `vm_*` → one cdylib. jextract's header covers both.
- **TS** — a new napi wrapper enables the feature and exports the same three
  control functions. No `ThreadsafeFunction`, no per-request marshalling — data
  rides the loopback socket.

Config crosses as a **UTF-8 JSON string**; `status()` reads an `Arc<Status>`;
**zero upcalls**. (Optionally a `src/relay/ffi.rs` facade could live here behind
the feature to single-source the C ABI; not required for v1.)

## Phases

- **Phase 0 — spike.** Prove the mechanism (can be done before the move, in the
  relay repo): loopback engine + a `relay_tunnel_*` C ABI round-tripping a
  relay-forwarded request → loopback → an HTTP/2 server → back, vs `cargo run
  --bin relay examples/relay.toml` (whoami mode). Retires (a) encapsulated-runtime
  start/stop over FFI, (b) prior-knowledge h2c bidi into the SDK server.
- **Phase 1 — bring the stack in.** Copy `relay-protocol` / `relay-bridge` /
  `relay-receiver` into `src/relay/` + add `loopback.rs`, all behind
  `#[cfg(feature = "tunnel")]`; make the networking deps optional. **Gate:**
  default build + WASM compile unchanged (sans-IO, no tokio in the default
  graph); `--features tunnel` builds the engine; existing VM tests green;
  add tunnel tests under the feature.
- **Phase 2 — SDK bindings.** sdk-java enables the feature + adds the
  `relay_tunnel_*` C ABI (one cdylib); sdk-typescript adds the napi package; its
  WASM/VM path is untouched.
- **Phase 3 — CI/release.** Build the Java cdylib + Node napi `--features
  tunnel` across the platform matrix; WASM keeps building default. crates.io
  source publish unchanged (name + semver + default API stable).

## Risks

1. **WASM contamination** (#1). A networking dep leaking into a non-optional
   path, or a `src/relay` module not gated, breaks the WASM/edge build. Enforce
   with a CI check: the **default-feature** dependency graph must contain no
   tokio.
2. **Published-crate stability.** `restate-sdk-shared-core` v7.x has real
   crates.io consumers; the **default** build + public API must stay identical.
   A new optional feature is additive and safe.
3. **napi ⇄ FFM parity.** One engine, two bindings — keep the control surface
   (start/stop/status + JSON config) identical so behaviour can't drift.

## Scope cuts for v1

- **`/whoami` mode** (target the relay; `/_/start-tunnel` / real Cloud is a
  later receiver extension).
- **Loopback TCP**, not UDS.
- **No drain** capability advertised.
- Java **FFM only** (23+); napi for Node servers.
