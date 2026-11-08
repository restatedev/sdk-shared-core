# Design

Its host is an object without Restate's journal: a key, its own database,
and calls to other objects or services, with at-least-once delivery. The
namespace is all the object keeps, and the state is how it reaches the store:
the database (the `sql` module's `Database`) flushes a mutation, the pages
its SQL changed and the records set, in one put, at every commit, as SQLite
does. The sections on SQL describe the `sql` module, which keeps its
engine's state private until it commits.

This doc holds the rules [ARCHITECTURE.md](ARCHITECTURE.md) leaves out;
read that first, for the layout, flush, checkpoint, open and garbage
collection. The encodings are in [FORMAT.md](FORMAT.md).

## Constraints

- **The backend is a Restate object's state** ([`kv`](../kv.rs)):
  whole-value `get`, and `put` and `delete` of one key, atomic, queued in
  issue order; `sync` returns once every write before it is stored; `keys`
  lists every key (Restate's `getKeys()`), to collect garbage. No multi-key
  transactions or compare-and-swap.
- **A crash keeps a prefix.** Of the writes since the last sync, a crash (or
  a failed sync) keeps the first few and loses the rest: never a write
  without the ones before it. Values are never torn. Reads see every write
  this owner issued.
- **One owner at a time.** Restate runs one exclusive handler per object key,
  and fences a former attempt's writes. The state does not fence. Packs
  promoted to L1, in S3 ([`ObjectStoreBlobs`](../blobs.rs)), are
  not fenced: there every pack has a name no other put uses
  ([ARCHITECTURE.md](ARCHITECTURE.md#5-with-l1-in-s3)).
- **Round trips dominate.** A flush or open should cost one round trip in
  the steady state.

## Records and the one rule

**Records** are whole named values, which the host keeps beside the SQL
(the SDK, its outbox): in the manifest up to `inline_record_bytes` (1 KiB),
and a value of its own in a pack when larger, written by the first
checkpoint after it changes. The format names its records and files, as the
database's file `db`; a name, once set, stays, and an empty value reads as
one never set.

**One rule.** A tail entry or a pack is stored before the first head that
names it, and dropped only once no head that a state may still hold needs
it. The store keeps a prefix of the writes, so whatever a crash keeps, the
head stored names only what is stored. Nothing a head names is ever
overwritten, so there is nothing to repair after a crash.

## Compression and records

**Compression.** zstd does the work twice. A frame compresses its 4 pages
together, so a B-tree page compresses against its neighbours. And a page in the tail, or a patch, uses the page as
the checkpoint holds it as a reference prefix ("patch from"), so a flush
that changes a few bytes of a page writes about those bytes plus the
frame's overhead.

**Records.** A host may commit values of its own with a transaction, as
records (`Database::commit(records)`): set in the same mutation, they ride
in the same tail entry, so they are durable exactly when the transaction
is, and stay until a later commit replaces them. The Restate SDKs keep a
transaction's sends in the record `outbox`, and make them once it is
committed. A mutation that changes nothing writes nothing.

## When a flush happens

At every commit of the database, and only then: each call of a `Database`
that leaves no SQL transaction open (a statement on its own, or the
`COMMIT` of a transaction) moves the pages its SQL committed into a
mutation, as writes to the file `db`, with the records of
`commit(records)`, and flushes it, in one tail entry, before the
call returns. SQL statements in a transaction change only the engine's
private view. The pages stay private until the flush succeeds; a commit
that fails ends the session. `Database::transaction(body)` wraps
`begin`/`commit`, and rolls back when the body returns `Err`.

A continuing session moves its view forward locally instead of reopening:
the flush's entry joins the state's overlay. Each flush costs one round
trip. Flushes are sequential: the owner's calls hold its session one at a
time, and each builds on the head of the one before, once its sync
returned.

**Retries without a journal.** With at-least-once delivery, a handler that
fails after a flush runs again on top of it. Two patterns keep that correct:

- **Idempotent requests.** In the same SQL transaction as the request's
  writes, record its id, and stop if it was already there:
  a plain `INSERT` into a table keyed by the id fails on a duplicate, which
  rolls back `Database::transaction`, or `ON CONFLICT DO NOTHING RETURNING id`
  returns no row for one. The dedupe row and the writes become durable
  together, because they are one tail entry.
- **Outbox for calls.** Flush the intent to call another object or service
  with the transaction, as a record, then make the call, and drop it from a
  later flush once the call is stored. A retry finds it and calls again, so
  the callee must deduplicate too.

A flush that returned an error has an unknown outcome: reopen and look (the
dedupe row says whether it landed) rather than retrying blindly.

**Why a flush per commit.** It costs one round trip per write transaction
and makes progress durable as it happens. Without a journal, it is the only
choice that does not lose acknowledged work when a later step fails.

## Readers

A reader (`State::open(store).reader()`) reads a namespace that another owns,
alongside it, such as a virtual object's shared handler beside the exclusive
one. It flushes nothing: its SQL changes and the records it commits stay in
the session. It reads the head it opened, its
checkpoint and its tail, and packs never change, so it reads one consistent
state, however the owner flushes meanwhile: snapshot isolation, with no
check and no retry. The packs and the tail its head names stay until the
owner's second checkpoint after it, so a reader that outlives that fails
with "is gone", and its handler is retried. A reader may collect garbage
too. With the cache, a reader reads as an owner's state does (see
[The cache](#the-cache)): with what the head names kept, its open is one
get, the head.

## Engine integration

How the [`sql`](../../sql/README.md) module runs the SQL engine on the state.
The engine sees two virtual files, the database and its WAL file (SQLite's
own), served by `KvIo`:

- **Reads** of the database file go to the committed view: the overlay's
  page, else the page's patch or frame, decoded, from the state, the cache,
  or the store. The engine issues one dependent read at a time; adjacent
  queued reads are batched (up to 16) without crossing write, truncate or
  sync barriers. A get fetches a whole frame, which the state keeps decoded,
  so its neighbours read without another.
- **Writes, truncation and sync** stay in private memory. File sync is only an
  ordering barrier. Nothing reaches KV before a mutation's flush, and the engine's
  WAL file never does.
- **Execution.** The caller's task drives the engine's state machines and
  awaits KV I/O. Queries yield every 64 engine steps; one engine call cannot be
  preempted. The engine also reads synchronously for metadata. Open preloads the
  first page (every 4096-byte block of it, for larger page sizes) for those
  reads, and any other synchronous read that would need KV fails and ends the
  session.
- **Statements.** A string of several statements runs them in order, each
  under the same rules as on its own. `query_with`/`execute_with` bind
  parameters to one statement, so values never become SQL text.
- **Errors.** An SQL error (syntax, a constraint, a missing table, wrong
  parameters) undoes its statement, as in SQLite: the session stays usable and
  an open transaction continues. The engine undoes a failed statement itself,
  so this holds when no I/O failed and none is pending; the adapter records
  every failed request and synchronous read to tell. Any other error (storage,
  a limit, a cancelled call, an engine anomaly) ends the session, and an error
  from a commit leaves its outcome unknown.
  `Database::is_usable` says which happened.
- **Commit.** The engine's checkpoint moves its WAL into the private view of
  the database file, whose changed pages go into the mutation, as writes to
  the file `db`. They stay in the private view until the mutation's flush
  succeeds; a flush the state refused (before anything was written) ends
  the session, with nothing of the call stored. Between `Database::begin`
  and `commit`, a statement that ends the transaction (`COMMIT`, `END`,
  `ROLLBACK`) fails and ends the session before anything is durable, and so
  does dropping a call's future.

## The cache

One `PageCache` per process. A state opens in it under its namespace's key
(`State::open(store).cached(&cache, key)`), and keeps that namespace's part of
it (a `NamespaceCache`, inside the crate).

**Reads.** A state's stores are wrapped in the cache (`CachedKv` for
Restate, `CachedBlobs` for S3): every get but the head's is served from the
cache, or fetched and kept, and every put but the head's is kept too (a pack
in S3 by the ranges its checkpoint wrote, by which states read it). That
is the whole of it, and it holds because every key but the head's names one
value for ever: a tail entry carries the nonce of the writer that flushed
it, a manifest and a pack their own, and none is put twice. So there is
nothing to invalidate (no versions, no generations, no read marks), and
what a lost flush put is harmless: no head names it, and the next writer's
keys are others. The head is the one key every flush changes: every open
reads it from Restate, and every state, an owner's or a reader's, reads
what it names the same way. Values are kept by (namespace, key) in Restate,
and by (namespace, key, range) in S3, where a read takes a frame or a
value, not a run a compaction moves (kept up to 1 MiB); in memory, and on a
local disk file ([foyer](https://github.com/foyer-rs/foyer)), one budget for
all namespaces. A value larger than a shard of the memory tier (a large
tail entry, say) goes to disk alone. The namespace's part of the keys is a
per-cache counter, randomly seeded, drawn per namespace, and the disk tier
starts empty in every process. A memory hit is served without foyer's disk
hop.

**Before nonces.** The tail entries of a head written before entries had
nonces have keys a retry of a lost flush puts again: the cache reads them
from Restate at every open, as the head, until a checkpoint drops them.

**Requests.** Concurrent reads of one range in a state share one get, held
weakly, so a cancelled read cancels the get. Across states, nothing waits on
another's request: a request goes through one invocation's store (in
Restate, its journal), and completes only while that invocation runs, so a
state waiting on another's would wait forever once that one's attempt
ended. What one state fetched, the next finds kept.

**The writer.** States of one owner may overlap in a process, such as two
sessions of one invocation. The namespace knows the version of its last
durable flush: an owner's open records the head it read, unless a flush
started since it read it, and a flush is refused unless it builds on that
version, rather than overwrite the head another state wrote. It holds the
nonce of the writer's tail entries: one for all its flushes, so that the
head names them in one run (each run is about 10 bytes of a head every
flush rewrites); another after a flush whose outcome is unknown, and at an
open of a head older than its last durable flush (the object's state
cleared, and made again), so that it never puts a key twice. It knows the
flush that runs, and the flushes with an unknown outcome, so that the next
owner's open syncs first; and when the next checkpoint may be tried. A
flush whose outcome is unknown forgets the version to build on and what
was parked. Writers are kept for a bounded number of namespaces
(`PageCacheConfig::namespace_bytes`, least recently used first): a
namespace forgotten waits for an owner's open to read its head again before
it flushes, and one with an unknown outcome is kept until a sync settles
it.

**Engine reuse.** A session that ends cleanly (after a successful flush, or
read-only) parks its engine at its head, but only if that head is the
writer's latest durable flush (an overlapping state's engine may be older)
and its SQL changed no per-connection state (`PRAGMA`, `ATTACH`, `DETACH`, the temp schema).
The next owner's state at that head takes it; at another head, it goes.
Only the database is kept: the next session connects afresh, so connection
counters (`last_insert_rowid()`, `changes()`) and settings never carry over.
This keeps the parsed schema warm, which saves each session parsing it again
(the `cache_cpu` benchmark measures it). Parked engines share
a byte budget (`PageCacheConfig::parked_bytes`, 64 MiB), each weighed as 4 MiB
(turso's buffer pool) plus 16 bytes per byte of schema SQL. Namespace states
are kept by their bytes (above). Least recently used go first; dropping either only makes
a namespace cold.

**Threads.** foyer's disk I/O runs on the host's tokio runtime, or on a
one-thread runtime of its own. It is the only background work.

## Costs

| Operation        | Round trips                                                                                                                    | Writes                             |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------- |
| Flush, cached    | 1, the sync                                                                                                                    | A tail entry (its parts), the head |
| Flush, cold      | 1, plus 1 to get the frames (or patches) of the pages it writes, where the state lacks them                                    | The same                           |
| Checkpoint       | Up to 3 more, for what the state lacks: frames it rebuilds or patches, ranges it compacts; a promotion, 1 PUT before the flush | A pack, a manifest                 |
| Collection       | 1 to list the packs and manifests; if any is older than the previous checkpoint, 1 for both manifests, then the deletes        | The deletes                        |
| Open, with cache | 1, the head, when the cache holds what the head names (an owner's open or a reader's)                                          | None                               |
| Cold open        | Up to 3: the head; the manifest and the tail (what the cache lacks); the large values, and a small database whole              | None                               |
| Cold page read   | 1 ranged get of its frame (and its patch, together); for a page in the overlay, of its reference                               | None                               |

Measurements: [ARCHITECTURE.md](ARCHITECTURE.md#6-numbers).

## Limits and open questions

- **Size.** A paged file holds at most 10 GiB; the SQL database is capped
  there too (`PRAGMA max_page_count`), so a statement that would grow it
  past fails with SQLITE_FULL and the session goes on. A cold open reads
  the manifest whole, a frame location per frame: a few bytes per 16 KiB.
- **Bytes.** A page in the tail is compressed alone against its reference,
  and a checkpoint rewrites whole frames, so both write more bytes than a
  delta of what changed would; compaction at half live stores up to twice
  what is live.
- **Keys.** Every flush puts at least two keys (a tail entry and the head);
  a namespace holds a key per part of each tail entry kept, a key per 64
  KiB of each manifest kept (two checkpoints'), and a key per 64 KiB of
  each pack in L0: all of them without S3, the last few MiB with it. A collection lists every key:
  a round trip, and its deletes.
- **Fencing.** Restate provides it for the head and the tail: a former
  attempt's writes never land after a newer owner's. Packs beside Restate
  are not fenced; their unique names keep attempts apart.
- **Concurrency.** A state has at most 32 reads of packs (or of the
  pages a flush compresses against) in flight at once: an open of many
  large values, or a checkpoint of many frames, waits for some rather
  than asking the store for all of them together.
- **Memory.** A session's private changes (`max_private_bytes`, 256 MiB) and a
  statement's result rows (`max_result_bytes`, 64 MiB) are capped; exceeding
  either fails the statement as `Refused`, which a retry would repeat, and
  ends the session. A state keeps the tail since its checkpoint in
  memory, as its overlay, and at most 256 decoded frames and patches. Parked
  engines share a byte budget, by a measured estimate.
- **Integrity.** Restate provides it: a value reads back as written. Decoding
  checks structure and bounds versions, sizes, counts and names, and every
  zstd frame carries a checksum, which also catches a page decoded against
  the wrong reference.
- **Format.** Only v22, with no migration.
- **Encoding.** Hand-written (`format.rs`): LEB128 varints, offsets as
  differences from the location before, and zstd over the manifest's body;
  the pages are zstd frames of their own. A zero-copy format (FlatBuffers,
  Cap'n Proto, rkyv) would save the decode, but nothing here is read
  uncompressed: a manifest of 70,000 frames (1M rows) is 16–30 KB encoded
  and decodes in 2 ms, once per cold open (the cache keeps it decoded),
  where laid out flat it would be about 1 MB, rewritten in Restate at
  every checkpoint; a frame decompresses in 25 µs.
- **Validation.** A monitor checks every request of the simulator's
  campaigns against the one rule, decoding every head, tail entry and
  manifest it sees, and every page, frame and value they name: a head that
  names a tail entry or a checkpoint (its manifest and every range it
  names) not stored, or does not build on the head before; a tail entry
  put for any version but the next, or a pack's or manifest's part put
  twice; a delete of a tail entry the head's checkpoint does not cover, of
  a pack a head may need, or of a manifest of a checkpoint not older than
  the previous one. A seeded random campaign over the public API
  (`tests/campaign.rs`; `STATE_SEEDS=<n>` runs more seeds) mixes changes to
  records and files, random checkpoint thresholds, faults,
  cancellations, crashes, reopens, owners and readers, with packs in
  Restate (even seeds) or promoted at random sizes to an S3 that fails as
  S3 does (odd seeds: uploads lost, done with the response lost, or
  landing late), and
  compares every reopen and every read with a model; other tests crash at
  every write of a flush, with packs in Restate and in S3, drop a flush
  after every number of polls, and corrupt every stored value. The Restate
  crash tests kill processes; there are no power-loss tests.
