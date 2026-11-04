# SQL

A session's database: SQLite-compatible SQL on the
[state](../state/README.md), for a Restate object without Restate's journal
(with explicit recovery), whose state the namespace is. It runs Turso's Rust
engine (`turso_core` 0.8.2) unmodified, over the state's one file, and commits
with the state: the rows and the host's records, durable together in one
flush.

A `Database` is one session of it. Its statements change a private view of
the file, and each call that leaves no SQL transaction open commits: what the
SQL committed, and the records of `commit(records)`, in one flush,
durable before the call returns. With a page cache, a session that ends
cleanly leaves its engine warm for the namespace's next one. It is the `sql`
module of `restate_sdk_shared_core`, on with the feature `sql`; the Node
addon of sdk-sessions is one host of it.

## API

`Database` is the module's entry point. A host implements `state::KvStore`
over its store (sdk-sessions' Node addon, over a JavaScript one), then:

```rust,ignore
use restate_sdk_shared_core::sql::Database;

// Kept warm in the process's page cache under the namespace's stable key.
let db = Database::open(store).cached(&cache, "object-42").await?;
db.execute("UPDATE stock SET n = n - 1 WHERE id = 7").await?; // durable now
db.begin().await?;                                  // one transaction, with a record
db.execute("INSERT INTO shipments VALUES(7)").await?;
db.commit([("outbox".into(), "ship 7".into())]).await?; // the row and the record, durable together
db.save().await?;                                   // ends the session, leaving its engine warm
```

`Database::open` returns a builder: `.options(StateOptions)` sets how the
state flushes and reads, `.checkpoints(blobs)` keeps the bulk of the
checkpoints in S3, and `.reader()` opens a reader beside the owner, as a
session object's view does. A host's `transaction(body)` (sdk-sessions'
`src/database.ts`, say) runs
`body` between `begin` and `commit`, or `rollback` when it fails; a statement
in the body that ends its transaction (`COMMIT`, `END`, `ROLLBACK`) ends the
session rather than commit the body halfway. `commit(records)` commits
records with the transaction, whole values by name kept beside the SQL, in
no table (the host's outbox, say); `record(name)` reads one as the session
holds it, empty if never set, and a reader's commits set its own, which are
never stored, as its SQL is not. Bind values with
`query_with`/`execute_with` rather than formatting them into SQL. A session
holds at most 256 MiB of changes not yet committed, and a statement returns
at most 64 MiB of rows: past either, the session ends.

An SQL error (syntax, a constraint, a missing table) undoes just that statement,
as in SQLite, and the session goes on. A storage error, a cancelled call or a
failed commit ends the session (`is_usable()` returns false): settle
outstanding requests and open a new one; a failed commit's outcome is unknown.

sdk-sessions' Node addon (its `src/node.rs`) exposes a `Database` to
JavaScript, over a key/value store implemented there.

## Layout

| Path                                  | What                                                                                   |
| ------------------------------------- | -------------------------------------------------------------------------------------- |
| `mod.rs`, `io.rs`                     | `Database` and its engine, and the adapter (`io`) from the engine's files to the state |
| `tests/`                              | SQL, and the state's storage, crash and cache campaigns under SQL workloads            |
| [`benches/`](../../benches/README.md) | Benchmarks of SQL workloads on the state                                               |

## Build and test

From the crate's root, with the feature `sql` (no Cargo.lock is committed):

```sh
cargo test --release --features sql sql::
cargo clippy --all-features --all-targets -- -D warnings
```

The tests run on the state's in-memory KV store, fault injection and latency
simulator (its `simulation` module, always on in tests; the feature `__bench`
turns it on for the benchmarks).

The randomized campaign (`tests/integration/campaign.rs`) runs a few seeds by
default; run more, or replay a failing one with a trace:

```sh
SQL_CAMPAIGN_SEEDS=1000 cargo test --release --features sql sql::tests::integration::campaign
SQL_CAMPAIGN_SEED=17 SQL_CAMPAIGN_TRACE=1 cargo test --features sql sql::tests::integration::campaign -- --nocapture
```
