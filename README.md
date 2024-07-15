# SDK Shared core

Shared core to build SDKs in various languages. Currently used by:

* [Python SDK](https://github.com/restatedev/sdk-python)

## Versions

This library follows [Semantic Versioning](https://semver.org/).

The compatibility with Restate is described in the following table:

| Restate Server\sdk-shared-core | 0.0/0.1 |
|--------------------------------|---------|
| 1.0                            | ✅       |

## Development

You need the [Rust toolchain](https://rustup.rs/). To verify:

```
just verify
```

To release we use [cargo-release](https://github.com/crate-ci/cargo-release):

```
cargo release <VERSION>
```

