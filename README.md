# SDK Shared core

Shared core to build SDKs in various languages. Currently used by:

* [Typescript SDK](https://github.com/restatedev/sdk-typescript)
* [Python SDK](https://github.com/restatedev/sdk-python)
* [Rust SDK](https://github.com/restatedev/sdk-rust)

## Versions

This library follows [Semantic Versioning](https://semver.org/).

The compatibility with Restate is described in the following table:

| Restate Server\sdk-shared-core | 0.0.x | 0.1.x | 0.2.x | 0.3.x/0.4.x |
|--------------------------------|-------|-------|-------|-------------|
| 1.0                            | ✅     | ❌     | ❌     | ❌           |
| 1.1                            | ✅     | ✅     | ✅     | ❌           |
| 1.2                            | ✅     | ✅     | ✅     | ❌           |
| 1.3/1.4                        | ✅     | ✅     | ✅     | ✅           |

## Development

You need the [Rust toolchain](https://rustup.rs/). To verify:

```
just verify
```

To release we use [cargo-release](https://github.com/crate-ci/cargo-release):

```
cargo release <VERSION>
```

