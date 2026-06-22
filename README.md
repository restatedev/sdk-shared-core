# SDK Shared core

Shared core to build SDKs in various languages. Currently used by:

* [Typescript SDK](https://github.com/restatedev/sdk-typescript)
* [Python SDK](https://github.com/restatedev/sdk-python)
* [Go SDK](https://github.com/restatedev/sdk-go)
* [Ruby SDK](https://github.com/restatedev/sdk-ruby)
* [Rust SDK](https://github.com/restatedev/sdk-rust)

## Documentation

* [Service invocation protocol](docs/service-invocation-protocol.md) — the wire protocol
  spoken between the Restate runtime and a service deployment.
* [SDK integration guide](docs/sdk-integration.md) — how to build a new SDK on top of this crate:
  driving the VM, the progress loop, and the invocation lifecycle.

## Versions

This library follows [Semantic Versioning](https://semver.org/).

The compatibility with Restate is described in the following table:

| Restate Server\sdk-shared-core | <= 0.2 | 0.3 - 0.5 | 0.6 - 0.10 | 7.0 |
|--------------------------------|--------|-----------|------------|-----|
| <= 1.2                         | ✅      | ❌         | ❌          | ❌   |
| 1.3 - 1.5                      | ✅      | ✅         | ✅          | ✅   |
| 1.6                            | ❌      | ✅         | ✅          | ✅   |
| 1.7                            | ❌      | ✅         | ✅          | ✅   |

## Development

You need the [Rust toolchain](https://rustup.rs/). To verify:

```
just verify
```

To release we use [cargo-release](https://github.com/crate-ci/cargo-release):

```
cargo release <VERSION>
```

After the dry-run succeeded, run with `--execute`:

```
cargo release <VERSION> --execute
```

Last but not least, push the newly created commit and tag and create a GitHub release.
