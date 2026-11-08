//! The storage of an object's database, over an asynchronous key/value
//! store: the database's file, 4 KiB pages, and its records, changed
//! through a mutation in memory, which a flush makes durable at once.
//!
//! A [`State`] is one namespace in a KV store: the state of a Restate object
//! without Restate's journal, which keeps writes in issue order and makes
//! them durable at a sync (see [`kv`]). It needs no multi-key transaction or
//! compare-and-swap; only collecting garbage lists keys. It holds the
//! database's [`File`], pages read and written at random, and its records,
//! whole values by name, which the host keeps beside the SQL (such as the
//! outbox of the sends committed with the rows). The state is read; it
//! changes only through a [`Mutation`] ([`State::mutation`]): the pages
//! written and the records set there become durable together at
//! [`Mutation::flush`], and a mutation dropped discards them.
//! [`crate::sql::Database`] makes one after each SQL transaction.
//!
//! # A tail and checkpoints
//!
//! Where the state is stored is split by what each store is good at (see
//! the private `format` and `store` modules, and `docs/ARCHITECTURE.md`). A flush
//! puts its mutation as one entry of a *tail* in Restate, then the *head*,
//! and syncs once: one round trip, which Restate fences. Now and then
//! ([`StateOptions::checkpoint_entries`]), a flush first writes the state as
//! a *checkpoint*: the frames of pages the tail changed, and the records it
//! set, into one *pack*, and a manifest of where every frame and value is,
//! in Restate. Its head then names the checkpoint, and drops the tail it
//! covered. Packs are tiered: a checkpoint's pack goes to *L0*, in Restate's
//! state, until L0 holds [`StateOptions::l0_bytes`]; then one goes to *L1*,
//! a [`blobs::BlobStore`] (S3, through [`Open::checkpoints`]), with
//! everything L0 held. Without an L1, every pack stays in Restate. Pack
//! names are unique, and packs never change, so S3 needs no fencing, nor
//! conditional puts.
//!
//! | Layer | Representation | Survives? |
//! |---|---|---|
//! | A mutation | Pages written and records set in it, in memory | No: dropping it discards them |
//! | Head | `v22/head`: the version, the checkpoint and the one before, the tail's extent | Yes, after an acknowledged flush |
//! | Tail | `v22/tail/<version>-<nonce>/<part>`: the mutation a flush made: records, pages (each a zstd frame against the page before it), under its writer's nonce | Yes, until a checkpoint after the next one |
//! | Manifests | `v22/manifest/<version>-<nonce>/<part>`: where a checkpoint's frames and records are | Yes, while the head names its checkpoint |
//! | Packs | `v22/pack/l0/<version>-<nonce>/<part>` in Restate, `l1/<version>-<nonce>` in the blob store: frames of [`StateOptions::frame_pages`] pages, large records | Yes, until no checkpoint the head names needs it |
//!
//! # Ownership and concurrency
//!
//! The host grants one owner exclusive use of a namespace, including
//! settlement of cancelled requests before reopening or handing ownership to
//! another session. There is no distributed lock or fence here.
//!
//! Any number of states may read beside the owner ([`Open::reader`], or
//! states of the owner that overlap in a [`PageCache`]): each reads the
//! state it opened, a consistent snapshot, while the owner flushes, because
//! packs never change, and the tail and the packs a head names stay for one
//! checkpoint more. Only one flushes: a flush is refused if another state
//! of the owner flushed since this one opened.
//!
//! The [`PageCache`] is the process's: every get but the head's goes
//! through it, served from it or fetched and kept, and every put but the
//! head's is kept, in memory and on a local disk tier under one budget for
//! every namespace it opens. Every key but the head's names one value for
//! ever (a tail entry, a manifest and a pack each carry a nonce), so nothing
//! kept is invalidated, and a steady-state open, an owner's or a reader's,
//! and a flush are each one round trip. The SQL engine is parked there for
//! the next owner's state at the same head ([`State::park`]). The disk
//! tier's I/O runs on tokio.
//!
//! # Example
//!
//! ```ignore
//! # use std::sync::Arc;
//! # use crate::state::{State, kv::KvStore};
//! # async fn example(store: Arc<dyn KvStore>) -> crate::state::Result<()> {
//! let state = State::open(store).await?;
//! let mut mutation = state.mutation();
//! mutation.write(0, Some(vec![7; 4096].into()))?;
//! mutation.set_record("outbox", "sends".into())?;
//! mutation.flush().await?; // the page and the record, durable together
//! # Ok(())
//! # }
//! ```
//!
//! # Limits
//!
//! The file holds at most [`MAX_FILE_BYTES`], a record
//! [`MAX_RECORD_BYTES`], under a name of at most [`MAX_NAME_BYTES`]. A state
//! holds at most 65,535 records, and a name once set stays: setting it empty
//! frees nothing. The backend is assumed
//! to return what was stored; decoding checks structure and bounds, and
//! zstd frames carry a checksum. Use a fresh, dedicated namespace; no
//! migration or format discovery is implemented. The simulated stores
//! (`simulation`) are in tests, and with the crate's `__bench` feature.
pub(crate) mod blobs;
mod cache;
mod format;
pub(crate) mod kv;
#[cfg(any(test, feature = "__bench"))]
pub mod simulation;
mod store;
mod tail;
mod zstd;

pub use blobs::BlobStore;
#[cfg(feature = "object_store")]
pub use blobs::ObjectStoreBlobs;
pub use cache::parse_size;
pub use cache::{PageCache, PageCacheConfig};
pub(crate) use format::{MAX_FILE_BYTES, MAX_NAME_BYTES, MAX_RECORD_BYTES};
pub use kv::KvStore;
pub(crate) use store::FlushReport;
#[cfg(test)]
pub(crate) use {
    format::{HEAD_KEY, TAIL_PREFIX},
    store::Checkpointed,
};

use crate::state::cache::{same_state, Backoff, NamespaceCache, ParkedValue};
use crate::state::store::Snapshot;
use crate::state::tail::Tiered;
use bytes::Bytes;
use std::{
    any::Any,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

/// State and backend errors returned to the host task.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Bytes of a page of the [`File`].
pub(crate) const PAGE_SIZE: usize = 4096;

/// The name the format stores the database's file under: no record's.
const DB: &str = "db";

/// A page's 4096 bytes.
pub(crate) type Page = Bytes;

/// Default [`StateOptions::frame_pages`].
pub(crate) const FRAME_PAGES: u64 = 4;

/// How a state flushes and reads. Only [`Self::frame_pages`] is stored, when
/// the namespace is made; the others never change the stored format.
#[derive(Clone)]
pub struct StateOptions {
    /// Pages per frame of a checkpoint, the unit a read fetches and a
    /// checkpoint rewrites: larger frames compress better and make a
    /// smaller manifest, smaller ones cost a checkpoint less to rewrite. Set
    /// when the namespace is made; its head keeps it.
    pub frame_pages: u64,
    /// Bytes of a record a checkpoint's manifest holds; a larger one is a
    /// value of its own in the pack.
    pub inline_record_bytes: usize,
    /// Every frame and patch of a file of at most this many
    /// pages, is fetched at open, alongside the manifest's values.
    pub prefetch_pages: u64,
    /// A flush checkpoints first once the tail holds
    /// this many entries since the last checkpoint; 0 never does by count.
    pub checkpoint_entries: u64,
    /// ... or this many bytes; 0 never does by size. A checkpoint into L0
    /// (Restate) is due at most at the bytes L0 and the manifest hold, but at
    /// least a frame's: it costs only its bytes, and spares the tail's,
    /// though a round trip or two too. One into L1 (S3) is due at exactly
    /// these.
    pub checkpoint_bytes: u64,
    /// With packs in S3 ([`crate::sql::Open::checkpoints`]), a checkpoint's pack goes to
    /// L0, in Restate, until the packs there hold this many bytes; the
    /// checkpoint after that puts its pack in S3, and moves everything L0
    /// holds into it. 0 puts every checkpoint in S3; without S3, every
    /// pack stays in Restate.
    pub l0_bytes: u64,
}

impl Default for StateOptions {
    fn default() -> Self {
        Self {
            frame_pages: FRAME_PAGES,
            inline_record_bytes: 1024,
            prefetch_pages: 16,
            checkpoint_entries: 128,
            checkpoint_bytes: 1 << 20,
            l0_bytes: 4 << 20,
        }
    }
}

/// One namespace's state: the database's file and its records, as last
/// flushed. It is read here, and changed through a [`Mutation`]
/// ([`Self::mutation`]), which [`Mutation::flush`] makes durable at once. A
/// cheap handle; clones share it.
///
/// Acquire exclusive ownership of the namespace and settle the preceding
/// owner's requests before opening it; a reader ([`Open::reader`]) may open
/// beside the owner.
#[derive(Clone)]
pub(crate) struct State {
    shared: Arc<Shared>,
}

/// What a state's handles share.
struct Shared {
    /// The durable state: as opened, then as the mutations flushed it.
    snapshot: Mutex<Arc<Snapshot>>,
    /// Where the state is stored: the head, the tail, the packs.
    tail: Arc<Tiered>,
    cache: Option<NamespaceCache>,
    /// What was parked at the state this one opened on, until taken
    /// ([`State::take_parked`]).
    parked: Mutex<Option<ParkedValue>>,
    /// A reader's ([`Open::reader`]): it never flushes.
    reader: bool,
    /// A flush failed or was cancelled: its outcome is unknown.
    poisoned: AtomicBool,
    /// A flush is running: a second one is refused, as it would build on the
    /// same snapshot and lose the first one's writes.
    flushing: AtomicBool,
    /// When the next checkpoint may be tried, after some failed: the
    /// cache's, which the namespace's next states share, if it has one.
    backoff: Mutex<Backoff>,
}

impl Shared {
    fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.lock().unwrap().clone()
    }

    /// Change the backoff with `f`, here and in the cache.
    fn backoff(&self, f: impl FnOnce(&mut Backoff)) {
        let mut backoff = self.backoff.lock().unwrap();
        f(&mut backoff);
        if let Some(cache) = &self.cache {
            cache.set_backoff(*backoff);
        }
    }

    /// Delete the packs no checkpoint the head names needs, and the
    /// manifests of checkpoints older than the previous one. A failure, or
    /// dropping the future midway, ends the state: in Restate, what it
    /// deleted has an unknown outcome, which the next open syncs. With
    /// `all`, S3 is listed whatever the head; without, only where L1's
    /// packs may have changed ([`Tiered::collect_garbage`]).
    async fn collect_garbage(&self, all: bool) -> Result<usize> {
        let mut collecting = Collecting {
            shared: self,
            done: false,
        };
        let head = self.snapshot().head().clone();
        let deleted = self.tail.collect_garbage(&head, all).await?;
        collecting.done = true;
        Ok(deleted)
    }
}

/// A running garbage collection: see [`Shared::collect_garbage`].
struct Collecting<'a> {
    shared: &'a Shared,
    done: bool,
}

impl Drop for Collecting<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.shared.poisoned.store(true, Ordering::SeqCst);
        if let Some(cache) = (self.shared.cache.as_ref()).filter(|_| !self.shared.reader) {
            cache.forget();
        }
    }
}

/// A running flush. One that fails ends the state. One dropped midway has
/// an unknown outcome too, and the cache forgets what it knew of the
/// namespace (as a failed sync makes it), so that no other state flushes on
/// top of a state that may be gone.
struct Flushing<'a> {
    shared: &'a Shared,
    /// The flush returned, rather than being dropped.
    returned: bool,
    /// It succeeded.
    done: bool,
}

impl Flushing<'_> {
    /// Refuse the flush before it wrote anything: the state goes on.
    fn refuse<T>(&mut self, error: Box<dyn std::error::Error + Send + Sync>) -> Result<T> {
        (self.returned, self.done) = (true, true);
        Err(error)
    }
}

impl Drop for Flushing<'_> {
    fn drop(&mut self) {
        self.shared.flushing.store(false, Ordering::SeqCst);
        if !self.done {
            self.shared.poisoned.store(true, Ordering::SeqCst);
        }
        if let (false, Some(cache)) = (self.returned, &self.shared.cache) {
            cache.forget();
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // What was parked and not taken goes back, if nothing changed.
        let Some(cache) = self.cache.as_ref().filter(|_| !self.reader) else {
            return;
        };
        let snapshot = self.snapshot();
        if let Some(value) = self.parked.get_mut().unwrap().take() {
            if same_state(&value.head, snapshot.head())
                && cache.committed() == Some(snapshot.version())
            {
                cache.park(value);
            }
        }
    }
}

/// The error of a state that a failed flush ended.
const ENDED: &str = "the state ended after a failed or cancelled flush; open a new one";

impl State {
    /// Open the namespace over `store`, Restate's state: read its head, its
    /// checkpoint's manifest and its tail (and its large records and small
    /// files) before anything reads; a crash leaves nothing to recover. Await it as it is, or set
    /// how first:
    ///
    /// ```ignore
    /// # use std::sync::Arc;
    /// # use crate::state::{PageCache, PageCacheConfig, State, StateOptions, kv::KvStore};
    /// # async fn example(store: Arc<dyn KvStore>, cache: PageCache) -> crate::state::Result<()> {
    /// let state = State::open(store.clone()).await?;
    /// let state = State::open(store.clone()).cached(&cache, "object-42").await?;
    /// let state = State::open(store)
    ///     .options(StateOptions { checkpoint_entries: 64, ..StateOptions::default() })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub(crate) fn open(store: Arc<dyn KvStore>) -> Open {
        Open {
            store,
            packs: None,
            options: StateOptions::default(),
            cache: None,
            reader: false,
        }
    }

    async fn open_as(open: Open) -> Result<Self> {
        let Open {
            store,
            packs,
            options,
            cache,
            reader,
        } = open;
        let tail = Arc::new(Tiered::new(store, packs, cache.as_ref()));
        let snapshot = Snapshot::open(tail.clone(), options, cache.clone(), reader);
        let snapshot = snapshot.await?;
        let parked = match &cache {
            Some(cache) if snapshot.unchanged => cache.take_parked(snapshot.head()),
            _ => None,
        };
        let backoff = cache
            .as_ref()
            .map(NamespaceCache::backoff)
            .unwrap_or_default();
        Ok(Self {
            shared: Arc::new(Shared {
                snapshot: Mutex::new(Arc::new(snapshot)),
                tail,
                cache,
                parked: Mutex::new(parked),
                reader,
                poisoned: AtomicBool::new(false),
                flushing: AtomicBool::new(false),
                backoff: Mutex::new(backoff),
            }),
        })
    }

    /// The record `name`, as last flushed: empty if never set.
    pub(crate) fn record(&self, name: &str) -> Bytes {
        let snapshot = self.shared.snapshot();
        snapshot.records().get(name).cloned().unwrap_or_default()
    }

    /// The database's file: 4 KiB pages, read here and written in a
    /// mutation (see [`File`]).
    pub(crate) fn file(&self) -> File {
        File {
            shared: self.shared.clone(),
        }
    }

    /// What was parked ([`Self::park`]) at the end of the owner's previous
    /// state, if nothing changed since and it is a `T`; one of another type
    /// stays parked.
    pub(crate) fn take_parked<T: Any + Send>(&self) -> Option<T> {
        let mut parked = self.shared.parked.lock().unwrap();
        if !parked.as_ref()?.value.is::<T>() {
            return None;
        }
        let value = parked.take()?.value.downcast().ok()?;
        Some(*value)
    }

    /// Whether [`Self::park`] would keep a value now: the state has a
    /// [`PageCache`] and is at the owner's last durable flush.
    pub(crate) fn can_park(&self) -> bool {
        let Some(cache) = &self.shared.cache else {
            return false;
        };
        !self.is_reader() && self.is_usable() && cache.committed() == Some(self.version())
    }

    /// Leave `value`, which corresponds to the state as last flushed, for
    /// the namespace's next state ([`Self::take_parked`]), weighing `weight`
    /// bytes against [`PageCacheConfig::parked_bytes`]: the SQL engine,
    /// which the next state then need not open. `Err` gives it back if it
    /// cannot be kept (see [`Self::can_park`]). The cache drops it when
    /// anything changes, or under its budget; one heavier than the whole
    /// budget is not kept. `value` must hold no handle of this state (the
    /// [`File`], the [`State`]), which would keep it open meanwhile, on a
    /// state whose packs later flushes delete.
    pub(crate) fn park<T: Any + Send>(
        &self,
        value: T,
        weight: usize,
    ) -> std::result::Result<(), T> {
        if !self.can_park() {
            return Err(value);
        }
        let cache = self.shared.cache.as_ref().unwrap();
        let parked = ParkedValue {
            value: Box::new(value),
            head: self.shared.snapshot().head().clone(),
            weight,
        };
        cache.park(parked);
        Ok(())
    }

    /// A new, empty mutation on this state: the pages written and the
    /// records set in it ([`Mutation::write`], [`Mutation::set_record`])
    /// become durable together at [`Mutation::flush`]; dropping it discards
    /// them.
    pub(crate) fn mutation(&self) -> Mutation {
        Mutation {
            shared: self.shared.clone(),
            records: Default::default(),
            files: Default::default(),
            checkpoint: false,
        }
    }

    /// The namespace's version: the flushes ever made, as this state holds
    /// it (opened on, or flushed itself).
    pub(crate) fn version(&self) -> u64 {
        self.shared.snapshot().version()
    }

    /// Whether the state opened in a [`PageCache`] ([`Open::cached`]).
    pub(crate) fn is_cached(&self) -> bool {
        self.shared.cache.is_some()
    }

    /// Whether this is a reader ([`Open::reader`]): its mutations never flush.
    pub(crate) fn is_reader(&self) -> bool {
        self.shared.reader
    }

    /// Whether the state can flush: a failed or cancelled flush ends it.
    pub(crate) fn is_usable(&self) -> bool {
        !self.shared.poisoned.load(Ordering::SeqCst)
    }

    /// Delete the packs no checkpoint the head names needs (those older
    /// than the checkpoint before the head's, which neither names), and the
    /// manifests of checkpoints older than that one. How
    /// many. A flush that makes a checkpoint collects too.
    ///
    /// Any state may collect, an owner's or a reader's, beside the owner's
    /// flushes, or one Restate fenced: whatever head it read, a pack a later
    /// head needs is one its head names, or newer than its previous
    /// checkpoint. A failure, or dropping the future midway, ends the state,
    /// as a failed flush does.
    #[cfg(test)]
    pub(crate) async fn collect_garbage(&self) -> Result<usize> {
        if self.shared.poisoned.load(Ordering::SeqCst) {
            return Err(ENDED.into());
        }
        self.shared.collect_garbage(true).await
    }

    /// The packs the head's checkpoints need: those its checkpoint and the
    /// one before name. Others are garbage.
    #[cfg(test)]
    pub(crate) async fn needed_packs(&self) -> Result<std::collections::BTreeSet<String>> {
        let head = self.shared.snapshot().head().clone();
        let needed = self.shared.tail.needed(&head).await?;
        Ok(needed.iter().map(|pack| pack.key()).collect())
    }

    /// The manifests the head names, by name: its checkpoint's and the one
    /// before's. Others are garbage.
    #[cfg(test)]
    pub(crate) fn needed_manifests(&self) -> std::collections::BTreeSet<String> {
        let head = self.shared.snapshot().head().clone();
        (head.checkpoint.iter().chain(&head.previous))
            .map(|checkpoint| checkpoint.pack.name())
            .collect()
    }
}

/// How to open a [`State`] ([`State::open`]): await it to open.
#[must_use = "a state opens when awaited"]
pub(crate) struct Open {
    store: Arc<dyn KvStore>,
    packs: Option<Arc<dyn BlobStore>>,
    options: StateOptions,
    cache: Option<NamespaceCache>,
    reader: bool,
}

impl Open {
    /// Flush and read with `options` rather than the defaults.
    pub(crate) fn options(self, options: StateOptions) -> Self {
        Self { options, ..self }
    }

    /// Keep the bulk of the checkpoints in `packs`, S3 say
    /// (`ObjectStoreBlobs`), as L1: the latest few go to Restate
    /// first ([`StateOptions::l0_bytes`]). Every state of the namespace must.
    pub(crate) fn checkpoints(self, packs: Arc<dyn BlobStore>) -> Self {
        Self {
            packs: Some(packs),
            ..self
        }
    }

    /// Keep the state warm in the process's `cache`, as `namespace`: the
    /// key of what the store holds, which no other state of this cache
    /// uses for another (an object's key, say). With nothing changed since
    /// this process last flushed it, the next open reads no pages and gets
    /// back what was parked there ([`State::park`]).
    pub(crate) fn cached(self, cache: &PageCache, namespace: &str) -> Self {
        Self {
            cache: Some(cache.namespace(namespace)),
            ..self
        }
    }

    /// Open a reader of a namespace that another owns, alongside it: a
    /// state whose mutations refuse to flush. It reads the state as of open, whatever the owner flushes meanwhile,
    /// until the owner's second checkpoint after it. With a cache
    /// ([`Self::cached`]), it reads through it as an owner's state does: the
    /// state as of the head it read, if kept, and what else the cache keeps.
    pub(crate) fn reader(self) -> Self {
        Self {
            reader: true,
            ..self
        }
    }
}

impl IntoFuture for Open {
    type Output = Result<State>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<State>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(State::open_as(self))
    }
}

/// A change to a [`State`]: the pages written and the records set since
/// [`State::mutation`] made it, in memory, which [`Self::flush`] makes
/// durable together, atomically. There is no other way to change the state.
/// A mutation dropped discards its writes.
pub(crate) struct Mutation {
    shared: Arc<Shared>,
    /// The records set, by name.
    records: store::Records,
    /// The file's writes, under its name in the format.
    files: store::Writes,
    /// [`Self::checkpoint`]: checkpoint first, however short the tail.
    checkpoint: bool,
}

impl Mutation {
    /// Whether nothing was set or written.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty() && self.files.is_empty()
    }

    /// Have [`Self::flush`] checkpoint first, however short the tail, if it
    /// writes anything: the checkpoint holds every flush before this one.
    #[cfg(test)]
    pub(crate) fn checkpoint(&mut self) {
        self.checkpoint = true;
    }

    /// Replace the record `name` with `value` at this mutation's flush; an
    /// empty value reads as one never set. Refused, before the mutation
    /// changes, is a record [`check_record`] refuses.
    pub(crate) fn set_record(&mut self, name: &str, value: Bytes) -> Result<()> {
        check_record(name, &value)?;
        self.records.insert(name.to_owned(), value);
        Ok(())
    }

    /// Write `image` (4096 bytes) at the file's `page`, or a hole with `None`,
    /// with this mutation's flush, which grows the file to include it if it is
    /// shorter then. Refused, before the mutation changes, is a page past the
    /// largest file.
    pub(crate) fn write(&mut self, page: u64, image: Option<Page>) -> Result<()> {
        if image.as_ref().is_some_and(|image| image.len() != PAGE_SIZE) {
            return Err("a page is 4096 bytes".into());
        }
        let end = page.saturating_add(1).saturating_mul(PAGE);
        if end > MAX_FILE_BYTES {
            return Err("a page past the largest file".into());
        }
        self.change(|written| {
            written.size = written.size.max(end);
            written.pages.insert(page, image);
        });
        Ok(())
    }

    /// Make the file `len` bytes, a multiple of 512, with this mutation's
    /// flush, whatever its length is then: pages wholly past it become holes,
    /// so regrowing it reads zeros. Bytes of a last, partial page past `len`
    /// stay as written.
    pub(crate) fn set_len(&mut self, len: u64) -> Result<()> {
        if !len.is_multiple_of(512) || len > MAX_FILE_BYTES {
            return Err(format!("invalid file length {len}").into());
        }
        self.change(|written| {
            // Every page past it a hole: those written in the mutation too, as an
            // earlier shrink may have cut flushed pages there.
            let kept = len.div_ceil(PAGE);
            for (_, image) in written.pages.range_mut(kept..) {
                *image = None;
            }
            for page in kept..written.size.div_ceil(PAGE) {
                written.pages.insert(page, None);
            }
            written.size = len;
            written.sized = true;
        });
        Ok(())
    }

    /// Change the file's writes with `f`, starting from none at its length
    /// as last flushed.
    fn change(&mut self, f: impl FnOnce(&mut store::Written)) {
        let size = self.shared.snapshot().file_size(DB);
        let written = (self.files.entry(DB.to_owned())).or_insert_with(|| store::Written {
            size,
            pages: Default::default(),
            sized: false,
        });
        f(written);
    }

    /// Make every page written and record set durable: as a tail
    /// entry, then the head, then one sync, in one round trip. Nothing
    /// changed: nothing is written. Refused, for writes the state cannot
    /// store, a flush writes nothing, and the state goes on. `Ok` holds what
    /// it wrote; any other error (one refused because another state flushed
    /// since included), or dropping the future, ends the state: open it
    /// again. One flush of a state runs at a time: its owner's calls hold
    /// its session.
    ///
    /// Once the tail passes [`StateOptions::checkpoint_entries`] or
    /// [`StateOptions::checkpoint_bytes`], a flush first checkpoints the
    /// durable state: it puts a pack, which its head names; then it
    /// deletes the packs and manifests no checkpoint the head names needs. A checkpoint that
    /// fails changes nothing: the flush goes on, and a later one
    /// checkpoints. A collection that fails ends the state, but not the
    /// flush, which landed: the report says why
    /// ([`FlushReport::collection_failed`]).
    pub(crate) async fn flush(mut self) -> Result<FlushReport> {
        let shared = self.shared.clone();
        if shared.reader {
            return Err("a reader writes nothing".into());
        }
        if shared.flushing.swap(true, Ordering::SeqCst) {
            return Err("a flush of this state is running".into());
        }
        let mut running = Flushing {
            shared: &shared,
            returned: false,
            done: false,
        };
        if shared.poisoned.load(Ordering::SeqCst) {
            return running.refuse(ENDED.into());
        }
        let snapshot = shared.snapshot();
        // What the writes checked as they were made still holds, unless
        // another mutation flushed since: refused before anything is written.
        if let Err(error) = (snapshot).check(&self.files, &self.records).await {
            return running.refuse(error.into());
        }
        let mut files = std::mem::take(&mut self.files);
        let mut records = std::mem::take(&mut self.records);
        // Only a flush that writes a head adopts a checkpoint. A failed
        // checkpoint fails nothing: the report says why, and a later flush
        // tries again, the next one first, then fewer and fewer of them
        // ([`Backoff`]); one asked for is tried regardless.
        let version = snapshot.version();
        let allowed = shared.backoff.lock().unwrap().allows(version);
        let due = self.checkpoint || (snapshot.due() && allowed);
        let (staged, failed) = match snapshot.changes(&mut files, &mut records) && due {
            true => match snapshot.checkpoint().await {
                Ok(staged) => (Some(staged), None),
                Err(error) => {
                    shared.backoff(|backoff| backoff.failed(version));
                    (None, Some(error.to_string()))
                }
            },
            false => (None, None),
        };
        let result = snapshot.commit(files, records, staged).await;
        running.returned = true;
        let (mut report, after) = result?;
        if report.checkpoint.is_some() {
            shared.backoff(Backoff::succeeded);
        }
        report.checkpoint_failed = failed;
        *shared.snapshot.lock().unwrap() = Arc::new(after);
        running.done = true;
        if let Some(checkpoint) = &mut report.checkpoint {
            match shared.collect_garbage(false).await {
                Ok(deleted) => checkpoint.deleted = deleted,
                Err(error) => report.collection_failed = Some(error.to_string()),
            }
        }
        Ok(report)
    }
}

/// The database's file: 4 KiB pages, read at random from the state and
/// written at random in a mutation, made durable by its flush with the
/// records. An absent page, never written or past the end, reads as `None`
/// (zeros). A cheap handle.
///
/// A page flushed is in the tail, a zstd frame of its own, until a
/// checkpoint writes it into a frame of [`StateOptions::frame_pages`]
/// pages, which a read fetches whole and the [`PageCache`] keeps across
/// states.
///
/// Two handles are equal (and hash alike) when they are of the same state.
#[derive(Clone)]
pub(crate) struct File {
    shared: Arc<Shared>,
}

impl PartialEq for File {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl Eq for File {}

impl std::hash::Hash for File {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.shared).hash(state);
    }
}

impl File {
    /// Bytes in the file, as last flushed.
    pub(crate) fn len(&self) -> u64 {
        self.shared.snapshot().file_size(DB)
    }

    /// Page `page` as the state holds it; `None` for zeros (a hole, or past
    /// its end).
    pub(crate) async fn read(&self, page: u64) -> Result<Option<Page>> {
        let snapshot = self.shared.snapshot();
        if page.saturating_mul(PAGE) >= snapshot.file_size(DB) {
            return Ok(None);
        }
        Ok(snapshot.read(DB, page).await?)
    }
}

/// Refuse a record no flush could store: a name that is empty, past
/// [`MAX_NAME_BYTES`], or the file's; a value past [`MAX_RECORD_BYTES`].
pub(crate) fn check_record(name: &str, value: &[u8]) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name == DB {
        return Err(format!("{name:?} cannot name a record").into());
    }
    if value.len() > MAX_RECORD_BYTES {
        let (len, max) = (value.len(), MAX_RECORD_BYTES);
        return Err(format!("the record {name:?} of {len} bytes exceeds {max}").into());
    }
    Ok(())
}

/// Bytes of a page, as the `u64` that offsets and sizes are.
const PAGE: u64 = PAGE_SIZE as u64;

#[cfg(test)]
mod tests;

/// Return to the executor once, even when every awaited future is ready.
pub(crate) async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}
