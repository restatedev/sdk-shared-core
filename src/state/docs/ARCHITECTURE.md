# Architecture

How the state works, from the top down: the data model the database sees, the state as a sequence of mutations, how that sequence is stored
(the head, the tail, checkpoints, manifests and packs), and how each of
those changes over time, with packs in two tiers: L0 in Restate and, once
a database outgrows it, L1 in S3. It ends with what changes when there is
an S3, with measured sizes and costs, and with the layouts this one
replaced.

Two docs go further: [DESIGN.md](DESIGN.md) has the rules this one leaves
out (retries, readers, the cache, the SQL engine, limits), and
[FORMAT.md](FORMAT.md) the byte encodings.

## 1. The data model

**A namespace** is the state of one Restate virtual object: one key, such
as `chat/42`. It has **one owner at a time**: the object's exclusive
handler, which Restate runs once per key and fences, so a former attempt's
writes never land after a newer one's. Shared handlers may open the same
namespace as **readers** beside the owner.

A namespace's state holds values of two kinds, named in the format:

| Kind          | What it is                                                          | Used for                                               |
| ------------- | ------------------------------------------------------------------- | ------------------------------------------------------ |
| **Records**   | Whole values by name, each up to 16 MiB, set and read whole         | What the host keeps beside the SQL, such as its outbox |
| **File** `db` | A paged file: 4 KiB pages, read and written at random, up to 10 GiB | The SQL database                                       |

**The state is read, never written in place.** A change goes into a
**mutation**: an owned value that collects the pages written, the file
resized and the records set, in memory. A **flush**
of the mutation makes all of it durable at once, atomically, in one round
trip to Restate, or none of it; a mutation dropped instead discards it.

```rust,ignore
let state = State::open(store).cached(&cache, "chat/42").await?;
let mut mutation = state.mutation();
mutation.set_record("outbox", sends)?;
mutation.write(7, Some(page))?;
mutation.flush().await?; // both durable together, or neither
```

What the state guarantees:

- **Atomic.** After a crash, a flush is either all there or not at all.
- **Durable on return.** When `flush()` returns, the mutation is stored.
  When it fails, its outcome is unknown: the state ends, and the next open
  reads what is there.
- **Ordered.** Flushes are sequential; each builds on the one before.
- **Snapshots for readers.** A reader sees the state as of the flush it
  opened on, however many flushes the owner makes meanwhile.

**SQL on top.** The [SQL module](../../sql/README.md)'s `Database` runs a
SQLite-compatible engine on the file `db`. Its statements change a private
view; at each commit, it moves the pages the SQL committed into a mutation,
with the records the host committed with them, and flushes it, so the
database and the records become durable together. It commits after every
call that leaves no
SQL transaction open.

## 2. The state over time: a sequence of mutations

The state is a sequence. Every flush that changes something makes the next
**version**: version 0 is empty, and

```
state(n) = state(n − 1) + mutation(n)
```

A chat agent's first turns, say:

| Version | Mutation (one flush)                                  | State after                           |
| ------- | ----------------------------------------------------- | ------------------------------------- |
| 1       | `CREATE TABLE messages`: pages 0–1 of `db`            | An empty table                        |
| 2       | Insert the user's message: page 1 of `db`             | 1 message                             |
| 3       | Insert the model's reply: page 1; set record `outbox` | 2 messages, and the sends of the turn |
| 4       | Insert the next user's message: page 1                | 3 messages                            |
| …       | …                                                     | …                                     |

Stored as is, that sequence is cheap to write (a flush puts only its own
mutation, often a few hundred bytes) but grows without end, and opening the
state would mean replaying all of it. Stored as whole states, opening is
cheap but every flush rewrites everything. So the state keeps both, in two
layers:

```
state(n) = checkpoint(c) + mutation(c + 1) + … + mutation(n)
           └ a snapshot ┘   └──────────── the tail ─────────┘
```

- **The tail**: the mutations since the last checkpoint, stored as they
  were. A flush appends one.
- **A checkpoint**: the whole state as of version `c`, written now and
  then by folding the tail into the checkpoint before it. Once a
  checkpoint is written, the tail before it can go. It describes the
  whole state but stores only what changed: its index is complete, its
  data is spread over its own pack and older ones (see
  [What a checkpoint holds](#what-a-checkpoint-holds)).

Everything below is how those two layers are laid out, written and read.

## 3. How it is stored

```
Restate (the object's state)                           S3, if the state has one
┌───────────────────────────────────────────────┐     ┌──────────────────────────────┐
│ v22/head          the commit point: version,  │     │ L1: l1/<v>-<nonce>           │
│                   checkpoints, tail entries   │     │   packs promoted from L0:    │
│ v22/tail/<v>/<p>  one per flush: its mutation │     │   frames, patches,           │
│ v22/manifest/...  one per checkpoint: where   │────▶│   large values               │
│                   every frame and value is    │     │                              │
│ v22/pack/l0/...   L0: the packs of the        │     └──────────────────────────────┘
│                   checkpoints since the last  │
│                   promotion; all of them      │
│                   without S3                  │
└───────────────────────────────────────────────┘
```

A paged file is stored as **frames** of 4 pages (16 KiB), each compressed
with zstd on its own: the unit a read fetches and a checkpoint rewrites.
The state in memory keeps the tail decoded as an **overlay**, the records
and pages changed since the checkpoint, and a read looks there first.
The rest of the terms are introduced below, each where its piece is.

## 4. The pieces, and how they change over time

### A flush

1. **If a checkpoint is due**, write it first (below), as of the version
   before this flush.
2. **Encode the mutation** as a tail entry. Each page written is
   compressed on its own, against the page as the state holds it before
   the flush: its **change**. A row update usually changes a few
   bytes of a page, so its page costs about 100–200 bytes, not 4 KiB; and a
   page written at every flush (a chat's last message page, say) costs each
   flush only what that flush changed.
3. **Put** the tail entry, then the head; **sync**. Restate keeps writes in
   order, and a crash keeps a prefix of them, so the head is never stored
   without the tail entry (and a checkpoint's pack in L0, and its
   manifest) put before it. That is one round trip, checkpoint or not.
4. **Apply** the entry to the overlay in memory. The state is now at the
   next version, without reading anything back.

### The head

About 80–180 bytes, uncompressed, overwritten by every flush:

- the database's random identity, drawn when it was made, and its frame size;
- the version;
- the checkpoint (its version and nonce, and its manifest's length), and
  the previous one, which readers that opened before the latest checkpoint
  may still read;
- the first tail entry kept, the tail's bytes since the checkpoint, the
  number of parts of each tail entry kept, and the nonces in their keys:
  one per run of entries a writer flushed.

It is the only key whose write commits anything, and the only one put
twice. Every other key is new and never changed: a tail entry carries its
writer's nonce, a pack and a manifest their own. So a crash midway leaves
only keys no head names, and a cache may keep any key but the head's for
as long as it likes.

### Tail entries

One per flush, for the version it makes, cut into parts of 1 MiB if large:

- the records set, with their values;
- per file changed, its new size and the pages written, each its change.

So the tail is a log: an entry's pages decode against the state the
entries before it left, from the checkpoint's pages on. A page's
**chain** is its changes since the checkpoint, in order; a hole, or a
shrink past it, starts it over from a hole. An entry is never changed; it
is deleted when the second checkpoint after it is adopted, until which
readers on the older checkpoint may need it. Its key has the nonce of the
writer that flushed it (`v22/tail/<version>-<nonce>/<part>`): an owner's
process, which keeps one for its flushes so that the head names them in
one run, or, without a cache, a session. A writer draws another after a
flush whose outcome is unknown, and at an open of a head older than its
last flush (the object's state cleared, and made again), so that it never
puts a key twice. A flush that is lost leaves its entry under a key no
head names, which garbage collection deletes, and the next entry at that
version has another key.

### Checkpoints

**When.** A flush checkpoints first when, since the last checkpoint:

- 128 flushes have been made (`checkpoint_entries`), or
- the tail holds 1 MiB (`checkpoint_bytes`), or, for a checkpoint into
  L0, as many bytes as L0 and the manifest hold, whichever is smaller,
  but at least a frame's (16 KiB),

or when the mutation asks for one (`mutation.checkpoint()`). The
relative rule is for checkpoints into L0, in Restate, which cost only
their bytes: a 20 KiB database checkpoints every few dozen flushes and
keeps a short tail, and a 100 MiB one every 128. A promotion (below)
costs an S3 PUT, so it waits for the absolute rule. The floor of a frame keeps a
state of a few small records from checkpointing at every flush, as a
checkpoint, and the collection after it, costs a round trip or two.

**What it writes.** For each frame the overlay changed, as the state
holds it now (the frame and its patches, with the chains over them):

- **Patch it** when only a few of its pages changed: each page that
  differs from the frame's is stored as a patch, compressed against the
  frame's page (a patch of a page unchanged since is kept as it is), while
  the frame's patches stay within half the frame's stored bytes. A one-row update in a
  large database touches a frame or two, so a checkpoint of 128 such
  flushes is about 28 KB of patches. Patches are per page so that a
  change rewrites only its page's: one patch per frame (the frame as it
  is now, against the frame as stored) keeps the manifest smaller, but
  rewrites every page the frame changed since it was written at each
  change, and on an aged database put 2,187 bytes a flush against 1,172
  (branch `claude/frame-patches`).
- **Rebuild it** otherwise, or when a page past the file's end changed:
  compress it again, without patches.

Then the records set since: inline in the
manifest if up to 1 KiB, else into the pack. And **compaction**: any older
pack the new manifest needs less than half of has its live ranges copied,
as they are, into the new pack, so that pack can go. Ranges of a pack
close together are fetched as one read, and one checkpoint moves at most
64 MiB, the cheapest packs first; the rest move at the checkpoints after
it.

All of that is one pack and one manifest. The flush's head then names the
new checkpoint, the one before becomes the previous checkpoint, and the
tail entries the previous checkpoint covered are deleted in the same sync.

A checkpoint that fails (an upload, say) fails nothing: the flush commits
its tail entry anyway, reports why (`FlushReport::checkpoint_failed`), and
the next flush tries again. Each failure in a row after that doubles the
flushes before the next try, up to 64, so a checkpoint that keeps failing
(its store down, say) costs one flush in 64 rather than each; one that
succeeds ends the wait. The process's cache keeps the wait for the
namespace's next states, and `Mutation::checkpoint` tries regardless.

### L0 and L1

Every pack is in one of two tiers, and its name, in the head and in every
manifest, says which:

- **L0**, in Restate: every checkpoint's pack goes there, as above, and so
  does compaction, of L0's packs mostly dead.
- **L1**, in S3, where the state has one (`State::open(store).checkpoints(s3)`):
  once L0, with the tail the next checkpoint folds in, holds `l0_bytes`
  (4 MiB), that checkpoint **promotes**: its pack goes to S3, with
  everything L0 still holds moved into it as it is, and L1's packs mostly
  dead too, within the 64 MiB a checkpoint moves. L0 is empty after it,
  and its packs are garbage.

A database under 4 MiB, compressed, never touches S3: it is stored and
read as without one. A larger one keeps its last few MiB of checkpoints
in Restate, where a cold read is a Restate round trip, and the rest in S3,
uploading a pack once in thousands of flushes. Without S3 there is no
promotion: every pack stays in L0.

Nothing else depends on the tier: a frame, a patch and a value read the
same from either, through the cache, and a manifest names packs of both.

### Manifests

One per checkpoint, zstd-compressed, always in Restate, in 64 KiB parts:

- the packs it uses, and the bytes each holds (for compaction);
- every record: inline, or where its value is;
- every file: its size, a location (pack, offset, length) per
  frame, or none for a hole, and its patches.

About 1.7 bytes per frame: 3 KiB for a 100k-row database, 30 KiB for 1M
rows. A checkpoint writes its manifest whole. Two are kept, the head's
checkpoint's and the previous one's.

### Packs

One per checkpoint that stores anything outside its manifest. Put once,
never changed, named with a random nonce, so a cache can keep any range of
any pack forever, with nothing to invalidate. A pack lives as long as some
manifest the head names points into it. A frame no flush touches stays in
the pack that first wrote it, across any number of checkpoints, until
compaction moves it.

### What a checkpoint holds

A checkpoint has two parts: a manifest that indexes the whole state, and a
pack of what changed since the checkpoint before.

- **The manifest** names every file and record, and gives a location
  (pack, offset, length) for every frame, patch and stored value. It
  stands on its own: an open reads this one manifest.
- **The pack** holds the frames the checkpoint rebuilt, the patches it
  added, the values set since, and what compaction moved. For everything
  else, the manifest points into the older packs that already hold it.

The state as of a checkpoint is its manifest and the packs it points
into. Say `db` has 6 frames (f0–f5, 4 pages each), with a checkpoint every
4 flushes:

| Checkpoint | What the tail since the one before changed | Its pack holds                                        | Compaction                                       |
| ---------- | ------------------------------------------ | ----------------------------------------------------- | ------------------------------------------------ |
| C4         | Every frame (the database was made)        | P4: f0–f5                                             | –                                                |
| C8         | Page 9 (in f2); all of f5                  | P8: f5, rebuilt; a patch for page 9 (p9)              | –                                                |
| C12        | Most of f0; f5 again                       | P12: f0 and f5, rebuilt; p9, moved from P8            | P8 is only p9 now, under half live: p9 moves     |
| C16        | Most of f1 and f2                          | P16: f1 and f2, rebuilt (f2 absorbs p9); f3, f4 moved | P4 is only f3 and f4, under half live: they move |

**Where each manifest points.** Every manifest names every frame, and
shares what did not change with the manifests before it:

```
          f0     f1     f2     f3     f4     f5     p9
M4        P4     P4     P4     P4     P4     P4     ·
M8        P4     P4     P4     P4     P4     P8     P8
M12       P12    P4     P4     P4     P4     P12    P12*
M16       P12    P16    P16    P16*   P16*   P12    ·

* moved there by compaction    · no patch
```

M8 shares five frames with M4; M16 still reads f0 and f5 from P12. A read
of page 9 under M8 or M12 takes the frame from P4 and the patch from P8
or P12; under M16, f2 is rebuilt with page 9 in it, and the patch is gone.

**What each pack still holds for the latest manifest.** A pack never
changes; what the latest manifest stops pointing into is dead in place:

```
after C8    P4   [ f0  f1  f2  f3  f4  ░░ ]    5 of 6 live: kept
            P8   [ f5  p9 ]

after C12   P4   [ ░░  f1  f2  f3  f4  ░░ ]    4 of 6 live: kept
            P8   [ ░░  p9 ]                    p9 alone, under half: p9 moves to P12
            P12  [ f0  f5  p9 ]

after C16   P4   [ ░░  ░░  ░░  f3  f4  ░░ ]    under half: f3 and f4 move to P16
            P12  [ f0  f5  ░░ ]                p9 dead, but small: kept
            P16  [ f1  f2  f3  f4 ]

░░ dead: the latest manifest no longer points into it
```

**How long each pack lives.** Garbage collection keeps what the head's two
manifests, its checkpoint's and the previous one's, point into, and
deletes older packs that neither names:

```
         C4     C8     C12    C16    C20
P4       ●━━━━━━━━━━━━━━━━━━━━━━━━━━━×      M12 points into it; M16 and M20 do not
P8              ●━━━━━━━━━━━━━×             M8 points into it; M12 and M16 do not
P12                    ●━━━━━━━━━━━━━━━━▶   M16 still reads f0 and f5 from it
P16                           ●━━━━━━━━━▶

● written    × deleted, two checkpoints after the last manifest that named it
```

- **Sharing:** a checkpoint writes what the tail changed; every frame it
  did not touch stays where an older checkpoint wrote it, however long
  ago.
- **Patches:** a page or two changed in a frame is a patch beside the
  frame (p9 in C8); a rebuilt frame absorbs its patches (f2 in C16), so a
  page reads from at most a frame and one patch.
- **Compaction:** once the latest manifest needs less than half of a
  pack's bytes, the checkpoint copies the live ranges, as they are, into
  its own pack (P8 at C12, P4 at C16), up to 64 MiB a checkpoint. Every
  pack a manifest names is soon at least half live, so what is stored
  stays within about twice the state.
- **Size:** a checkpoint's pack follows what the tail changed, plus what
  compaction moves; the manifest is the part that grows with the
  database, at about 1.7 bytes per frame.

### A timeline

With a checkpoint every 4 flushes (128 by default), `E5` the tail entry of
version 5, and `C4` the checkpoint of version 4 (its pack `P4` and manifest
`M4`):

| Flush makes | Checkpoint first | The head names   | Tail kept | Deleted in the same sync | Garbage collected after                      |
| ----------- | ---------------- | ---------------- | --------- | ------------------------ | -------------------------------------------- |
| v1 … v4     | –                | no checkpoint    | E1 … E4   | –                        | –                                            |
| v5          | C4 = P4 + M4     | C4               | E1 … E5   | –                        | –                                            |
| v6 … v8     | –                | C4               | E1 … E8   | –                        | –                                            |
| v9          | C8 = P8 + M8     | C8, previous C4  | E5 … E9   | E1 … E4                  | nothing older than C4                        |
| v10 … v12   | –                | C8, previous C4  | E5 … E12  | –                        | –                                            |
| v13         | C12 = P12 + M12  | C12, previous C8 | E9 … E13  | E5 … E8                  | M4; P4 unless M8 or M12 still points into it |

- **State v10** is `C8 + E9 + E10`, and E9 and E10 compress against C8's
  pages.
- **A reader** that opened on v6 reads `C4 + E5 + E6`. Those stay until
  v13, two checkpoints later; after that it fails with "is gone", and its
  handler is retried.
- **In Restate**, at any time: the head, the tail since the previous
  checkpoint (at most twice 128 entries), two manifests, and the packs
  those two manifests point into.

### Opening and reading

**Open**:

1. **Get the head**, from Restate, never from the cache. An owner without a
   cache, or with a flush running or whose outcome is unknown, syncs in the
   same round trip, so that what it reads is durable.
2. **Get the checkpoint's manifest and the tail past it**, together,
   through the cache, and apply the tail to the overlay in memory. With a
   warm cache, the cache holds them all: the open is one round trip, for an
   owner's state and a reader's alike, and an owner's takes back its parked
   SQL engine if it was parked at this head.
3. **Get large records**, and every frame and patch of a
   file of at most 16 pages, such as a small database
   whole, together.

That is at most 3 round trips, whatever the database's size.

**Read a page** from the first of:

1. the overlay (the tail changed it): its chain, each change decompressed
   in turn, from the checkpoint's page (read first) or a hole (about 3 µs
   a change);
2. its patch;
3. its frame: one ranged read of the pack (in Restate, the 64 KiB parts
   it is in, which the cache keeps for the frames beside it; in S3, a
   ranged GET), decompressed in about 25 µs;
4. a hole: zeros.

A SQL query reads a page per b-tree level it descends, so a cold point
query is a round trip per level the cache does not hold: 2 to 6.

### Garbage collection

After every flush that checkpoints (and in the tests, whenever one calls
`State::collect_garbage`, as a reader may too):

1. **List** Restate's keys, once (Restate's `getKeys()`): the manifests
   and L0. List S3 too only if the head's checkpoint or the previous one
   is a promotion, as only a promotion changes what L1 holds, or when
   asked (`State::collect_garbage`): a checkpoint into L0 makes no S3
   request. A store that cannot list keeps its garbage.
2. **Keep** the packs the head's two manifests point into, and the two
   manifests.
3. **Delete** listed packs and manifests older than the previous
   checkpoint that neither manifest needs, and tail entries at versions the
   checkpoint covers that the head does not name (a lost flush's): in
   Restate, queued and stored by one sync; in S3, one DELETE per pack, sent
   together.

Collections may overlap, crash or race the owner: a later head only ever
names packs its own manifests name, or packs newer than its previous
checkpoint, and the same tail entries as the collection's head at the
versions that head's checkpoint covers (a head that is lost, which a
reader may have read beside its flush, is one flush past a durable one,
and checkpoints a state before it), so whatever head a collection read, it
never deletes what a later head needs. A pack a crashed attempt put, which
no head names, goes once the previous checkpoint passes it; a lost flush's
tail entry, once a checkpoint covers its version.

### The cache

One `PageCache` per process ([foyer](https://github.com/foyer-rs/foyer),
in memory and spilling to a local disk file):

Every get but the head's goes through it: served from it, or fetched and
kept; and every put but the head's is kept too (a pack in S3 by the ranges
its checkpoint wrote, by which states read it). Each key names one value
for ever, so nothing is ever invalidated:

- **Tail entries, manifests and packs' parts** in Restate, by key: each key
  carries a nonce (a tail entry its writer's).
- **Ranges of packs in S3**: frames, patches and values, by (pack, offset,
  length).
- **Parked engines**: SQL parks its engine at the end of a
  session (64 MiB in all), so that the next owner's session at the same
  head skips parsing the schema.

The tail entries of heads written before entries had nonces are read from
Restate at every open, like the head: a retry may have put their keys
again.

With a warm cache, a handler invocation costs two round trips: get the
head, and the flush's sync.

### Failures

A crash anywhere keeps a prefix of the writes since the last sync, and
nothing needs repair:

| Interrupted                             | What is left                             | What happens next                         |
| --------------------------------------- | ---------------------------------------- | ----------------------------------------- |
| A flush                                 | Its tail entry without the head, or both | The entry is garbage, collected later     |
| A checkpoint                            | A pack and manifest no head names        | Garbage, collected later                  |
| A collection                            | Some deletes                             | The next collection deletes the rest      |
| A flush's tail deletes, before its head | The head before, with its tail           | The next checkpoint deletes the rest      |
| An unknown outcome                      | The flush may or may not be stored       | The state ends; the next open syncs first |

## 5. With L1 in S3

`State::open(store).checkpoints(s3)` gives the state an L1 in an object
store. Only the packs L0 outgrows move. The head, the tail, the manifests
and L0 stay in Restate, so an open never needs S3 to find anything, and a
database under `l0_bytes` never touches S3.

What changes, once a database outgrows L0:

- **A promotion uploads its pack** before the flush puts its head, so a
  promoting flush also waits for S3. The pack is what L0 held and what
  compaction moved with it, up to `l0_bytes` and 64 MiB more: up to 8 MiB
  goes in one PUT, a larger one in parts of 8 MiB, four at a time (a
  multipart upload, aborted if a part fails).
- **A read of a frame in L1 is a ranged GET**, about 20 ms against about
  2 ms for one in L0. The frames checkpoints rewrote since the last
  promotion are in L0.
- **No fencing in S3.** Restate fences the head and the tail; S3 fences
  nothing, so a former attempt may still be uploading. A pack's name has a
  random nonce, so no two uploads ever share a name, and a head only names
  a pack its own attempt uploaded before putting it: a zombie's packs are
  garbage, collected at a later promotion's collection.
- **Garbage collection** lists S3 only around a promotion, and deletes in
  one Restate sync and one S3 DELETE per pack, sent together.

## 6. Numbers

Bytes and request counts below come from the `flush_costs` bench
(`benches/`), which runs the real encoding over in-memory stores.
Latencies come from `read_latency`, a discrete-event simulation that
**assumes** 2 ms per Restate round trip and 20 ms per S3 request; they show
the shape, not production timings.

### Without S3 (the default)

Per flush, with a warm cache:

| Workload           | Database | Bytes put to Restate (tail + pack + manifest + head) | Stored in Restate | Keys  | Cold open and a point read |
| ------------------ | -------- | ---------------------------------------------------- | ----------------- | ----- | -------------------------- |
| Chat with outbox   | 180 KiB  | 1175 (851 + 210 + 15 + 99)                           | 161 KiB           | 121   | 5 round trips, 48 gets     |
| Small, update 1    | 20 KiB   | 326 (114 + 34 + 0 + 178)                             | 28 KiB            | 156   | 3 round trips, 26 gets     |
| 10k rows, update 1 | 2.8 MiB  | 631                                                  | 1.2 MiB           | 153   | 5 round trips, 6 gets      |
| 1M rows, update 1  | 274 MiB  | 859 (226 + 206 + 250 + 177)                          | 108 MiB           | 1,860 | 6 round trips, 8 gets      |
| 1M rows, update 10 | 274 MiB  | 1642                                                 | 108 MiB           | 1,862 | 6 round trips, 8 gets      |
| 100k rows, aged    | 27 MiB   | 1171 (225 + 470 + 235 + 241)                         | 14 MiB            | 379   | 5 round trips, 7 gets      |

The pack and manifest columns are spread over the flushes: a 1M-row
database's checkpoint, every 128 flushes, writes about 28 KB of patches
and a 30 KiB manifest.

A tail entry's page is its change since the flush before, so a page
written at every flush (a chat's last message page) costs each
flush only what that flush wrote: the chat's tail is 851 bytes a flush,
where compressing against the checkpoint's page put 1,593. Its tail then
fills more slowly, so it is checkpointed less often and runs longer: more
entries for a cold open to read (48 gets against 30), and a longer head,
which lists them. A cold read of a page a checkpoint patched long ago
fetches the patch beside its frame, a round trip more than a frame just
rebuilt: the 1M-row database's header page is such a page.

The other rows are young, a few hundred flushes old. An aged database
costs more: its patches pile up, one per page changed since its frame was
written, and its manifest, written whole, grows with them. After 16,384
one-row updates, a 100k-row database's manifest is about 7× its first
(235 bytes a flush against 32), and its packs carry more dead bytes
before compaction moves them.

Cold reads, at 2 ms a round trip:

| Database | Open | Point query (primary key) | Point query (index) | 100-row range |
| -------- | ---- | ------------------------- | ------------------- | ------------- |
| 100 rows | 6 ms | 0 (all in memory)         | 0                   | 0             |
| 10k rows | 6 ms | 4 ms                      | 6 ms                | 8 ms          |
| 1M rows  | 6 ms | 6 ms                      | 12 ms               | 10 ms         |

### With L1 in S3

Per flush, with a warm cache, and against v20's packs all in S3:

| Workload            | Bytes put to Restate per flush | v20, all in S3 | Stored in Restate | Stored in S3 | S3 Standard $ per million flushes, warm / cold | v20, warm |
| ------------------- | ------------------------------ | -------------- | ----------------- | ------------ | ---------------------------------------------- | --------- |
| Chat with outbox    | 1177                           | 1938           | 161 KiB           | none         | $0 / $0                                        | $0.07     |
| Small, update 1     | 327                            | 1734           | 28 KiB            | none         | $0 / $0                                        | $0.07     |
| 10k rows, update 1  | 633                            | 436            | 1.2 MiB           | none         | $0 / $0                                        | $0.08     |
| 100k rows, update 1 | 521                            | 426            | 81 KiB            | 10.7 MiB     | $0.02 / $2.45                                  | $0.08     |
| 1M rows, update 1   | 861                            | 654            | 145 KiB           | 107 MiB      | $0.03 / $4.39                                  | $0.08     |
| 1M rows, update 10  | 1372                           | 1080           | 277 KiB           | 107 MiB      | $0.03 / $24.97                                 | $0.08     |
| 100k rows, aged     | 1167                           | 702            | 3.6 MiB           | 10.7 MiB     | $0 / $2.65                                     | $0.08     |

A database under 4 MiB, compressed, is stored and costs as without S3:
nothing in S3, and its tail cut by the relative rule, so a small one puts
a fifth of the bytes v20's S3 mode did. A larger one checkpoints into L0
and pays its packs' bytes to Restate (about 220 bytes a flush for one-row
updates, v20's Restate mode's), and in exchange makes no S3 request
between promotions: none of the 256 flushes measured uploaded, where v20
uploaded a pack every 128. What remains is the listing after the load's
promotion. Warm, S3 is cheaper by four; cold, it is the GETs of the
frames a flush reads, as in v20. Storage is a rounding error: 108 MiB
costs $0.0024 a month.

Cold reads, at 2 ms a Restate round trip and 20 ms an S3 GET:

| Database | Open  | Point query (primary key) | Point query (index) | 100-row range |
| -------- | ----- | ------------------------- | ------------------- | ------------- |
| 100 rows | 4 ms  | 0 (all in the tail)       | 0                   | 0             |
| 10k rows | 6 ms  | 4 ms                      | 6 ms                | 8 ms          |
| 1M rows  | 24 ms | 60 ms                     | 120 ms              | 100 ms        |

The 10k-row database is all in L0: it reads as without S3 (v20 took 24
ms to open it and 40 to 80 ms a query). The 1M-row one reads from L1, as
v20 did from S3.

With a warm cache both modes cost the same: two Restate round trips per
invocation, and no S3 request.

### Choosing

- **Without S3**: one system, no unfenced writer, no credentials, and
  cold reads about 10× faster. The whole database, compressed (about
  2.5×), is in Restate, a key per 64 KiB, and every collection lists them
  all.
- **With S3**: the same for a database under `l0_bytes`; past it, Restate
  holds the last few MiB of checkpoints however large the database grows,
  S3 costs cents per million flushes, and each b-tree level a cold query
  reads from L1 costs an S3 GET.

## 7. Earlier layouts

Each layout below was built, measured and replaced by the next; their
reports are in git history (`core/state/docs/` at commit `e015181`).

| Layout                           | What it stored                                                                       | Why it went                                                                                  |
| -------------------------------- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| A log                            | Every flush appended to a log, checkpointed now and then into page keys              | Each change was written twice, to the log and then in place                                  |
| Copy-on-write blobs              | A root naming a radix map of chunks, each a base and a delta, all in Restate         | The whole database lived in Restate key by key: 33,000 keys for 100 MB of text               |
| Blobs in S3                      | The same blobs in an object store, the root in Restate, zombies kept apart by claims | Every flush was S3 PUTs: $15 per million one-row updates, over $100 for ten rows             |
| Tail and checkpoints (v18)       | A tail in Restate, and checkpoints of copy-on-write blobs, in S3 or Restate          | The chunk map, bases, deltas and rebase rules; a cold open of 1M rows took up to 1,331 gets  |
| Frame packs (v20)                | A tail of plain mutations; checkpoints of frames and patches, with a flat manifest   | Its packs were all in Restate or all in S3: a choice per deployment, not per database's size |
| Frame packs in tiers (v22, this) | The same, each pack in L0 (Restate) or, once L0 holds 4 MiB, L1 (S3)                 | –                                                                                            |

Against v18, frame packs halved the bytes a flush puts on large databases
(a tail page costs what changed since the checkpoint) and brought a cold
open to a manifest and a frame, in about 14% less code.
