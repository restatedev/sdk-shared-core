# SDK Shared core

Shared core to build SDKs in various languages. Currently used by:

* [Typescript SDK](https://github.com/restatedev/sdk-typescript)
* [Python SDK](https://github.com/restatedev/sdk-python)
* [Rust SDK](https://github.com/restatedev/sdk-rust)

## Versions

This library follows [Semantic Versioning](https://semver.org/).

The compatibility with Restate is described in the following table:

| Restate Server\sdk-shared-core | <= 0.2.x | 0.3.x - 0.5.x | 0.6.x - 0.9.x |
|--------------------------------|----------|---------------|---------------|
| <= 1.2                         | ✅        | ❌             | ❌             |
| 1.3 - 1.5                      | ✅        | ✅             | ✅             |
| 1.6                            | ❌        | ✅             | ✅             |

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
