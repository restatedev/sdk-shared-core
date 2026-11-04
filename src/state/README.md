# State

The storage of a session's database, over an asynchronous key/value store,
for namespaces with one owner at a time: the state of a Restate object
without Restate's journal (with explicit recovery). It keeps one paged
**file** (4 KiB pages, read and written at random), the database's, and
**records** committed with it (whole values by name, which the host keeps
beside the SQL, such as its outbox). The state is read; it changes only
through a **mutation**, an owned value that collects the pages written and
the records set, and a **flush** makes everything in the
mutation durable at once, in one round trip. It needs only `get`, `put`,
`delete` and `sync` from the backend, and a listing of its keys to collect
the garbage. It is a private module of the addon's crate, for the
[database](../sql/README.md) on it: see [the
layers](../../README.md#inside-the-layers).

## API

A host implements `kv::KvStore` over its store (the addon, over the
JavaScript one), then:

```rust,ignore
use crate::state::{PageCache, PageCacheConfig, State};

// Once per process: 256 MiB of pages in memory, 8 GiB in a temporary file.
let cache = PageCache::open(PageCacheConfig::new(256 << 20, 8 << 30)).await?;

// Per session, kept warm in the cache under the namespace's stable key.
let state = State::open(store).cached(&cache, "object-42").await?;
let mut mutation = state.mutation();
mutation.write(0, Some(page))?;
mutation.set_record("outbox", "sends".into())?;
mutation.flush().await?; // the page and the record, durable together
```

Nothing is durable before `Mutation::flush`, and then everything in that
mutation is, at once: its tail entry, then the head that names it, in one
round trip. A flush consumes the mutation; a mutation dropped instead
discards its writes. The state shows what was flushed (`state.record(name)`,
`state.file().read(page)`), never what a mutation holds. The host must
guarantee one owner per namespace. A failed or cancelled flush ends the
state (`is_usable()` returns false): its outcome is unknown, so settle
outstanding requests and open a new one. A flush the state refuses before
writing anything (too many records, or too large ones) fails alone, and the
state goes on; so does a flush whose checkpoint fails. A garbage collection
that fails after a flush landed ends the state, but the flush succeeds, its
report saying why.

With a page cache, a value can be parked for the namespace's next state
(`state.park(value, weight)`), which takes it back (`state.take_parked()`)
when nothing changed since: the database keeps its engine warm this way.

## How it works

A flush puts its mutation as one entry of a **tail** in Restate, then the
**head**, the one key whose write commits, and syncs once. Now and then a
flush first writes a **checkpoint**: a manifest in Restate that indexes the
whole state, and a pack of what changed since the checkpoint before, in
Restate, until the packs there hold a few MiB, then promoted to S3 if the
state has one. Nothing a head names is ever overwritten, so a
crash leaves nothing to repair. It relies on one owner at a time, which
Restate's virtual objects give, and on a crash keeping a prefix of the
writes since the last sync.

## Docs

- [Architecture](docs/ARCHITECTURE.md): start here. The data model, the
  head, tail, checkpoints, manifests and packs and how they change over
  time, L0 and L1, the numbers, and the layouts this one replaced
- [Design](docs/DESIGN.md): the rules the architecture leaves out: retries,
  readers, the SQL engine, the owner cache, costs and limits
- [Storage format](docs/FORMAT.md): exact encodings
- [SQL](../sql/README.md): the database on the state, with the storage
  tests and benchmarks that run SQL workloads

## Layout

| Path                                       | What                                                      |
| ------------------------------------------ | --------------------------------------------------------- |
| `mod.rs` and the files beside it, `tests/` | The module and its tests                                  |
| `simulation/`                              | In-memory stores, fault injection and a latency simulator |
| [`docs/`](docs)                            | Architecture, design, storage format                      |

## Build and test

From `packages/sessions/native`:

```sh
cargo test --locked --release state::     # the state's tests, with the in-memory store and fault injection
cargo clippy --locked --all-targets -- -D warnings
```

The `simulation` module, an in-memory KV store, fault injection and a
latency simulator, is always on in tests; the tests here and in
[`sql`](../sql) use it.
