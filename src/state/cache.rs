//! Process-wide cache of what states read, shared by every namespace this
//! process opens, and per-namespace knowledge of its writer.
//!
//! A [`PageCache`] is created once per process. Each namespace gets its own
//! [`NamespaceCache`] handle by key, so one namespace never reads another's
//! values.
//!
//! # Reads
//!
//! A state reads its stores through the cache ([`CachedKv`] for Restate,
//! [`CachedBlobs`] for S3): a value is served from the cache, or fetched and
//! kept, and what a state puts is kept too: a value in Restate as it is put,
//! a pack in S3 by the ranges its checkpoint wrote, by which states read it
//! ([`NamespaceCache::keep_ranges`]). Every key but the head's names one value
//! for ever (see [`crate::state::format`]): a tail entry, a manifest, a pack
//! all carry a nonce in their names, so nothing kept is ever invalidated, and
//! what a flush that is lost put is harmless, as no head names it. The head is
//! the one key every flush changes: it is never kept, and every open reads
//! it; so are the tail entries of heads written before entries had nonces,
//! whose keys a retry reuses. An owner's state and a reader's read alike.
//!
//! # The writer
//!
//! A namespace has one owner at a time, which may run overlapping states in
//! this process. The namespace knows its last durable flush, so that a state
//! can tell another flushed since it opened, and is refused rather than
//! overwrite it; the nonce of the writer's tail entries, one for all its
//! flushes, so that the head names them in one run; the flush that runs; the flushes with an unknown outcome,
//! so that the next open syncs first ([`NamespaceCache::unsettled`]); when
//! the next checkpoint may be tried; and the value parked at a flush (SQL's
//! engine, say), for the next owner's state at that head.
//!
//! Values are kept within a byte budget, in memory and on a local disk tier,
//! namespaces' writers for a bounded number, and parked values within a byte
//! budget, least recently used first; forgetting any only makes a namespace
//! cold. A namespace with a flush whose outcome is unknown is kept until a
//! sync settles it.
use crate::state::{
    blobs::BlobStore,
    format::{immutable, Head},
    kv::KvStore,
};
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, Code, DeviceBuilder, FileDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCacheProperties, Location, RecoverMode, Spawner,
};
use futures::future::BoxFuture;
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    io,
    ops::Range,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc, Mutex, Weak,
    },
};

/// Bookkeeping bytes charged per value kept.
const ENTRY_OVERHEAD: usize = 64;

/// The weight of a namespace's writer, for
/// [`PageCacheConfig::namespace_bytes`].
pub(crate) const NAMESPACE_WEIGHT: usize = 256;

/// The largest range of a blob kept: a frame, a patch, a value, not a run
/// of ranges a compaction moves.
const MAX_KEPT_RANGE: u64 = 1 << 20;

/// The shards of the values' memory tier, each a part of its budget.
const MEMORY_SHARDS: usize = 8;

/// Sizes and location of a [`PageCache`].
#[derive(Clone, Debug)]
pub struct PageCacheConfig {
    /// File of the disk tier, its previous contents discarded; `None` for a
    /// temporary one, which goes away with the process.
    pub file: Option<PathBuf>,
    /// Bytes of values kept in memory: tail entries, manifests, ranges of
    /// packs.
    pub memory: usize,
    /// Bytes of the disk tier.
    pub disk: usize,
    /// Bytes per disk block, the unit the disk tier writes and reclaims:
    /// 16 MiB, foyer's own default, unless the disk is small
    /// ([`Self::sized`]). A flush that fills its block waits for a clean one,
    /// which a reclaim makes by evicting a whole block, so a larger block
    /// gives the writers more room before they wait (and their buffers fill,
    /// and evicted values that do not fit are dropped).
    pub disk_block: usize,
    /// The disk tier's writers ([`BlockEngineConfig::with_flushers`]), each
    /// with one write in flight and its share of [`Self::disk_buffer`] (at
    /// least 4 MiB: fewer writers if the buffer is small). An evicted value
    /// goes to one of them by hash.
    pub disk_flushers: usize,
    /// Bytes the disk tier's writers fill while their writes are in flight,
    /// split between them. A writer takes every evicted value sent to it into
    /// its buffer at once, so this, not a queue, is how far writes may lag:
    /// a value that does not fit is not written (and a later read of it goes
    /// to the store). Each writer keeps two of its share (one filling, one
    /// written), and the values in both stay held until written, so this
    /// costs twice its size for good, and up to twice again while writes lag.
    /// A share under a value's size (up to 1 MiB) never takes that value.
    pub disk_buffer: usize,
    /// Clean blocks kept ready for the writers, and the reclaims that make
    /// them: a writer that fills its block waits for a clean one. Every block
    /// starts clean, so this matters once the disk tier has been filled.
    pub disk_clean_blocks: usize,
    /// Threads of the disk tier's own tokio runtime, on which its writers,
    /// reclaims and reads (its tasks, not their blocking I/O) run; 0 runs
    /// them on the caller's runtime, where they wait behind its other tasks.
    pub disk_threads: usize,
    /// Bytes of the namespaces' writers kept, each weighing 256: least
    /// recently used go first, and one forgotten only makes its next flush
    /// wait for an owner's open to read the head again.
    pub namespace_bytes: usize,
    /// Bytes of the values parked for the next session of their
    /// namespace, as they weigh it: SQL's engine, say. Least recently parked go first.
    pub parked_bytes: usize,
}

impl Default for PageCacheConfig {
    /// [`Self::MEMORY`] and [`Self::DISK`], in a temporary file.
    fn default() -> Self {
        Self::sized(Self::MEMORY, Self::DISK)
    }
}

impl PageCacheConfig {
    /// The default bytes of the memory tier.
    pub const MEMORY: usize = 256 << 20;

    /// The default bytes of the disk tier: the pages of some 5,000 sessions
    /// of 1.5 MB each. The file is sized, not filled: its blocks are taken
    /// as the tier writes them.
    pub const DISK: usize = 8 << 30;

    /// `memory` bytes of packs' ranges in memory, and `disk` in a temporary
    /// file.
    pub fn new(memory: usize, disk: usize) -> Self {
        Self {
            file: None,
            memory,
            disk,
            disk_block: 16 << 20,
            disk_flushers: 1,
            disk_buffer: 32 << 20,
            disk_clean_blocks: 2,
            disk_threads: 1,
            namespace_bytes: 256 << 20,
            parked_bytes: 64 << 20,
        }
    }

    /// As [`PageCacheConfig::new`], with the namespaces' writers and the parked
    /// values bounded by `memory` too, so that a small cache keeps little,
    /// disk blocks small enough that a small disk has 4 of them to reclaim,
    /// and a write buffer no larger than an eighth of the disk (at least
    /// 2 MiB), so that one batch never spans the blocks a wrap reclaims.
    pub fn sized(memory: usize, disk: usize) -> Self {
        let config = Self::new(memory, disk);
        Self {
            disk_block: config.disk_block.min((disk / 4).max(64 << 10)),
            disk_buffer: config.disk_buffer.min((disk / 8).max(2 << 20)),
            namespace_bytes: config.namespace_bytes.min(memory),
            parked_bytes: config.parked_bytes.min(memory),
            ..config
        }
    }
}

/// `value` in bytes: a number, or one with KiB, MiB or GiB after it.
pub fn parse_size(value: &str) -> Option<usize> {
    let value = value.trim();
    let (number, unit) = match value.find(|c: char| !c.is_ascii_digit()) {
        Some(at) => value.split_at(at),
        None => (value, ""),
    };
    let unit: usize = match unit.trim() {
        "" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        _ => return None,
    };
    number.parse::<usize>().ok()?.checked_mul(unit)
}

/// Process-wide cache of what states read; clones share it.
///
/// Parked values live here and may refer back to its namespace's handle, so
/// handles only reach it weakly: dropping the last `PageCache` drops it.
/// It is meant to live as long as the process: foyer's writer tasks keep
/// themselves (and the disk tier's runtime, its buffers and its file) alive
/// past a drop.
#[derive(Clone)]
pub struct PageCache {
    shared: Arc<Shared>,
    parked: Arc<Mutex<Parked>>,
}

struct Shared {
    values: HybridCache<ValueKey, Bytes>,
    namespaces: Cache<String, Arc<Mutex<Owner>>>,
    /// Every namespace state a handle still holds, so that one namespace never
    /// has two states while a session uses it, even after LRU eviction.
    live: Mutex<HashMap<String, Weak<Mutex<Owner>>>>,
    /// The namespace states with a flush whose outcome is unknown, kept until
    /// a sync settles it: an evicted one would forget that its next open
    /// must sync.
    unsettled: Mutex<Vec<Arc<Mutex<Owner>>>>,
    /// Source of namespace state identities.
    ids: AtomicU64,
    /// Keeps the disk tier's own runtime alive.
    _runtime: Option<Spawner>,
    /// foyer's counters ([`PageCache::stats`]).
    counters: Counters,
}

impl PageCache {
    /// Open the cache. Disk I/O runs on a runtime of its own of
    /// [`PageCacheConfig::disk_threads`] threads, or with none, on the
    /// caller's tokio runtime if there is one, otherwise on a one-thread
    /// runtime of its own.
    pub async fn open(config: PageCacheConfig) -> io::Result<Self> {
        let own = |threads: usize| -> io::Result<Spawner> {
            Ok(Spawner::from(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(threads)
                    .thread_name("page-cache")
                    .enable_all()
                    .build()?,
            ))
        };
        let spawner = match (config.disk_threads, tokio::runtime::Handle::try_current()) {
            (0, Ok(handle)) => Spawner::from(handle),
            (threads, _) => own(threads.max(1))?,
        };
        static TEMPORARY: AtomicU64 = AtomicU64::new(0);
        let file = config.file.clone().unwrap_or_else(|| {
            let n = TEMPORARY.fetch_add(1, Relaxed);
            std::env::temp_dir().join(format!("state-cache-{}-{n}", std::process::id()))
        });
        let device = FileDeviceBuilder::new(&file)
            .with_capacity(config.disk)
            .build()
            .map_err(io::Error::other)?;
        let counters = Counters::default();
        // Each writer's share of the buffer at least 4 MiB (and a page).
        let flushers = (config.disk_flushers)
            .min(config.disk_buffer / (4 << 20))
            .max(1);
        let engine = BlockEngineConfig::new(device)
            .with_block_size(config.disk_block)
            .with_flushers(flushers)
            .with_buffer_pool_size(config.disk_buffer.max(flushers * 4096))
            .with_reclaimers(config.disk_clean_blocks.max(1))
            .with_clean_block_threshold(config.disk_clean_blocks.max(1));
        let values = HybridCacheBuilder::new()
            .with_name("state-values")
            .with_metrics_registry(Box::new(counters.clone()))
            // The disk tier starts empty in every process, and a temporary
            // file is unlinked: what memory holds is not written out on close.
            .with_flush_on_close(false)
            .memory(config.memory)
            .with_shards(MEMORY_SHARDS)
            .with_weighter(|key: &ValueKey, value: &Bytes| {
                key.name.len() + value.len() + ENTRY_OVERHEAD
            })
            .storage()
            .with_engine_config(engine)
            // Namespaces' identities are not persisted, so earlier entries are unusable.
            .with_recover_mode(RecoverMode::None)
            .with_spawner(spawner.clone())
            .build()
            .await
            .map_err(io::Error::other)?;
        // The cache keeps the file open, and nothing else uses a temporary
        // one: unlinked now, it goes away with the process however it ends
        // (where unlinking an open file is refused, it stays).
        if config.file.is_none() {
            let _ = std::fs::remove_file(&file);
        }
        Ok(Self {
            shared: Arc::new(Shared {
                values,
                namespaces: CacheBuilder::new(config.namespace_bytes)
                    .with_weighter(|_, _: &Arc<Mutex<Owner>>| NAMESPACE_WEIGHT)
                    .build(),
                live: Mutex::default(),
                unsettled: Mutex::default(),
                // Distinct per instance, so two caches sharing a disk file by
                // mistake still never share keys.
                ids: AtomicU64::new(random() >> 1),
                _runtime: matches!(spawner, Spawner::Runtime(_)).then_some(spawner),
                counters,
            }),
            parked: Arc::new(Mutex::new(Parked::new(config.parked_bytes))),
        })
    }

    /// The handle for one namespace, such as one virtual object. Successive
    /// sessions of the namespace must use the same key.
    pub(crate) fn namespace(&self, key: &str) -> NamespaceCache {
        let mut live = self.shared.live.lock().unwrap();
        let owner = match self.shared.namespaces.get(key) {
            Some(entry) => entry.value().clone(),
            None => {
                let owner = live
                    .get(key)
                    .and_then(Weak::upgrade)
                    .unwrap_or_else(|| Arc::new(Mutex::new(Owner::new(&self.shared.ids))));
                self.shared.namespaces.insert(key.to_owned(), owner.clone());
                owner
            }
        };
        live.retain(|_, owner| owner.strong_count() > 0);
        live.insert(key.to_owned(), Arc::downgrade(&owner));
        drop(live);
        let id = owner.lock().unwrap().id;
        NamespaceCache {
            shared: self.shared.clone(),
            parked: Arc::downgrade(&self.parked),
            id,
            handle: self.shared.ids.fetch_add(1, Relaxed),
            owner,
        }
    }

    /// What the cache has done since it opened, as foyer counts it.
    pub fn stats(&self) -> CacheStats {
        let counter = |metric: &str, op: &str| self.shared.counters.get(metric, op);
        CacheStats {
            memory_hits: counter("foyer_memory_op_total", "hit"),
            memory_evictions: counter("foyer_memory_op_total", "evict"),
            disk_enqueued: counter("foyer_storage_op_total", "enqueue"),
            disk_hits: counter("foyer_storage_op_total", "hit"),
            disk_misses: counter("foyer_storage_op_total", "miss"),
            dropped_queue: counter("foyer_storage_inner_op_total", "channel_overflow"),
            dropped_buffer: counter("foyer_storage_inner_op_total", "buffer_overflow"),
            skipped_on_disk: counter("foyer_storage_block_engine_op_total", "enqueue_skip"),
            disk_writes: counter("foyer_storage_disk_io_total", "write"),
            disk_written_bytes: counter("foyer_storage_disk_io_bytes_total", "write"),
            disk_reads: counter("foyer_storage_disk_io_total", "read"),
            disk_read_bytes: counter("foyer_storage_disk_io_bytes_total", "read"),
        }
    }

    /// Bytes of values held in memory.
    pub fn memory_bytes(&self) -> usize {
        self.shared.values.memory().usage()
    }
}

/// One namespace's view of a [`PageCache`]; clones share it.
#[derive(Clone)]
pub(crate) struct NamespaceCache {
    shared: Arc<Shared>,
    parked: Weak<Mutex<Parked>>,
    /// The namespace's identity, which keys its values and parked values.
    id: u64,
    /// This handle's identity: the state whose flush holds the right to
    /// flush ([`Owner::flushing`]).
    handle: u64,
    owner: Arc<Mutex<Owner>>,
}

/// Per-namespace state: its writer's.
struct Owner {
    /// Identity of this namespace in the cache, which keys its values and
    /// parked values.
    id: u64,
    /// The version of the writer's last durable flush.
    committed: Option<u64>,
    /// The handle whose flush took the right to flush
    /// ([`NamespaceCache::claim`]) and has not ended: its writes may be
    /// visible, and not durable yet.
    flushing: Option<u64>,
    /// Counts the flushes that started, landed or failed: an owner's open
    /// records that a flush may build on the head it read only if none did
    /// since it read it ([`NamespaceCache::opened`]).
    epoch: u64,
    /// The nonce in the keys of the tail entries the writer's flushes put
    /// ([`NamespaceCache::nonce`]), so that no key is ever put twice.
    nonce: u64,
    /// Flushes with an unknown outcome so far, and how many of them a sync
    /// has settled since.
    unknown: u64,
    settled: u64,
    /// When the next checkpoint may be tried, after some failed.
    backoff: Backoff,
}

/// Checkpoints that failed in a row, and the version from which the next
/// may be tried: the first retry is at the next flush, and each failure
/// after it doubles the flushes to wait, up to 64. A checkpoint that fails
/// for good (its store down, say) then costs one flush in 64, not each.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Backoff {
    failures: u32,
    next: u64,
}

impl Backoff {
    /// Whether a checkpoint due before the flush of a state at `version`
    /// may be tried.
    pub(crate) fn allows(&self, version: u64) -> bool {
        version >= self.next
    }

    /// A checkpoint tried at `version` failed.
    pub(crate) fn failed(&mut self, version: u64) {
        self.failures += 1;
        self.next = version + (1 << (self.failures - 1).min(6));
    }

    /// A checkpoint succeeded.
    pub(crate) fn succeeded(&mut self) {
        *self = Self::default();
    }
}

impl NamespaceCache {
    fn owner(&self) -> std::sync::MutexGuard<'_, Owner> {
        self.owner.lock().unwrap()
    }

    fn key(&self, name: &str, range: Option<Range<u64>>) -> ValueKey {
        let (offset, len) = range.map_or((0, u64::MAX), |range| {
            (range.start, range.end - range.start)
        });
        ValueKey {
            namespace: self.id,
            name: name.to_owned(),
            offset,
            len,
        }
    }

    /// The value kept under `key`, from memory or disk. A disk error is a
    /// miss.
    async fn lookup(&self, key: &ValueKey) -> Option<Bytes> {
        // A hit in memory without a hop to the disk's threads.
        if let Some(entry) = self.shared.values.memory().get(key) {
            return Some(entry.value().clone());
        }
        match self.shared.values.get(key).await {
            Ok(entry) => entry.map(|entry| entry.value().clone()),
            Err(_) => None,
        }
    }

    /// The value of the key `name`, if kept.
    pub(crate) async fn value(&self, name: &str) -> Option<Bytes> {
        self.lookup(&self.key(name, None)).await
    }

    /// Keep `value`, the value of the key `name`.
    pub(crate) fn keep_value(&self, name: &str, value: &Bytes) {
        self.keep(self.key(name, None), value);
    }

    /// Keep `value` under `key`: in memory, or, larger than a shard of the
    /// memory tier holds (a large tail entry, say), on disk.
    fn keep(&self, key: ValueKey, value: &Bytes) {
        let shard = self.shared.values.memory().capacity() / MEMORY_SHARDS;
        let location = match value.len() > shard {
            true => Location::OnDisk,
            false => Location::Default,
        };
        let properties = HybridCacheProperties::default().with_location(location);
        (self.shared.values).insert_with_properties(key, owned(value), properties);
    }

    /// Keep bytes `ranges` of `body`, the blob `name` as put, by which
    /// states read it (a checkpoint's frames, patches and values), each up
    /// to the extent a read keeps ([`MAX_KEPT_RANGE`]).
    pub(crate) fn keep_ranges(&self, name: &str, body: &Bytes, ranges: &[Range<u64>]) {
        for range in ranges {
            if range.end - range.start <= MAX_KEPT_RANGE && range.end <= body.len() as u64 {
                let value = body.slice(range.start as usize..range.end as usize);
                self.keep_range(name, range.clone(), &value);
            }
        }
    }

    /// Bytes `range` of the blob `name`, if kept.
    pub(crate) async fn range(&self, name: &str, range: Range<u64>) -> Option<Bytes> {
        self.lookup(&self.key(name, Some(range))).await
    }

    /// Keep `value`, bytes `range` of the blob `name`.
    pub(crate) fn keep_range(&self, name: &str, range: Range<u64>, value: &Bytes) {
        self.keep(self.key(name, Some(range)), value);
    }

    /// What the writer has done so far, which an owner's open reads before
    /// the head: see [`Self::opened`].
    pub(crate) fn epoch(&self) -> u64 {
        self.owner().epoch
    }

    /// An owner's open read the durable head of `version` after the writer
    /// was at `epoch`: a flush may build on that version, unless one started
    /// since. A head older than the writer's last durable flush is another
    /// database's (the object's state was cleared, and made again), whose
    /// next flushes would put keys the writer put in the old one: the writer
    /// draws another nonce ([`Self::nonce`]).
    pub(crate) fn opened(&self, version: u64, epoch: u64) {
        let mut owner = self.owner();
        if owner.epoch == epoch && owner.flushing.is_none() {
            if owner.committed.is_some_and(|committed| version < committed) {
                owner.nonce = random().max(1);
            }
            owner.committed = Some(version);
        }
    }

    /// The nonce for the keys of the tail entries the writer's flushes put:
    /// one for all of them, so that the head names them in one run. Each
    /// flush builds on the writer's last durable one ([`Self::claim`]), so
    /// they put each version once; the writer draws another after a flush
    /// whose outcome is unknown ([`Self::forget`]), whose entry may be
    /// stored, and at an open of an older head ([`Self::opened`]). No two
    /// values are ever put at one key, which another process may have kept.
    pub(crate) fn nonce(&self) -> u64 {
        self.owner().nonce
    }

    /// The claimed flush (see [`Self::claim`]) made `version` durable.
    pub(crate) fn flushed(&self, version: u64) {
        let mut owner = self.owner();
        owner.committed = Some(version);
        owner.flushing = None;
        owner.epoch += 1;
    }

    /// A flush of this handle's failed or was dropped: its outcome is
    /// unknown. It no longer runs; no flush builds on the last durable one
    /// until an open reads the head again, after a sync; and what was parked
    /// goes; and the next flush's tail entry has another nonce, as this one's
    /// may be stored.
    pub(crate) fn forget(&self) {
        let mut owner = self.owner();
        if owner.flushing == Some(self.handle) {
            owner.flushing = None;
        }
        owner.unknown += 1;
        owner.committed = None;
        owner.epoch += 1;
        owner.nonce = random().max(1);
        let stale = self.take_any_parked();
        // Under the owner's lock, which a sync settling it takes too.
        let mut unsettled = self.shared.unsettled.lock().unwrap();
        if !unsettled
            .iter()
            .any(|owner| Arc::ptr_eq(owner, &self.owner))
        {
            unsettled.push(self.owner.clone());
        }
        drop(unsettled);
        drop(owner);
        // Parked values may hold state handles, whose drop takes the lock.
        drop(stale);
    }

    /// The flushes with an unknown outcome so far, if a sync has not settled
    /// them all, or a flush runs: their writes may be visible and yet not
    /// durable, so an owner's open syncs before it reads.
    pub(crate) fn unsettled(&self) -> Option<u64> {
        let owner = self.owner();
        (owner.unknown != owner.settled || owner.flushing.is_some()).then_some(owner.unknown)
    }

    /// A sync succeeded after the first `unknown` flushes with an unknown
    /// outcome (see [`Self::unsettled`]).
    pub(crate) fn settled(&self, unknown: u64) {
        let mut owner = self.owner();
        owner.settled = owner.settled.max(unknown);
        if owner.settled == owner.unknown {
            // Under the owner's lock: a flush failing now stays unsettled.
            let mut unsettled = self.shared.unsettled.lock().unwrap();
            unsettled.retain(|owner| !Arc::ptr_eq(owner, &self.owner));
        }
    }

    /// Take the right to flush on top of `version`, the writer's last
    /// durable flush: refused if another state flushed since, or is
    /// flushing. A flush that lands records its version
    /// ([`Self::flushed`]); one that fails leaves it unknown
    /// ([`Self::forget`]).
    pub(crate) fn claim(&self, version: u64) -> bool {
        let mut owner = self.owner();
        let claimed = owner.committed == Some(version);
        if claimed {
            owner.committed = None;
            owner.flushing = Some(self.handle);
            owner.epoch += 1;
        }
        claimed
    }

    /// When the namespace's next checkpoint may be tried.
    pub(crate) fn backoff(&self) -> Backoff {
        self.owner().backoff
    }

    /// Keep `backoff` for the namespace's next states.
    pub(crate) fn set_backoff(&self, backoff: Backoff) {
        self.owner().backoff = backoff;
    }

    /// The version of the writer's last durable flush, if still known.
    pub(crate) fn committed(&self) -> Option<u64> {
        self.owner().committed
    }

    /// Keep a parked value for the next owner's state at its head, in place
    /// of one parked before; it is dropped when a flush fails.
    pub(crate) fn park(&self, value: ParkedValue) {
        if let Some(parked) = self.parked.upgrade() {
            // Dropped once the lock is released.
            let evicted = parked.lock().unwrap().park(self.id, value);
            drop(evicted);
        }
    }

    /// The value parked for this namespace, if it was parked at `head`; one
    /// parked at another head is dropped.
    pub(crate) fn take_parked(&self, head: &Head) -> Option<ParkedValue> {
        let parked = self.take_any_parked()?;
        same_state(&parked.head, head).then_some(parked)
    }

    fn take_any_parked(&self) -> Option<ParkedValue> {
        self.parked.upgrade()?.lock().unwrap().take(self.id)
    }
}

/// A namespace's Restate store, read through its cache: the value of a key
/// that names one value for ever ([`immutable`]) is served from the cache,
/// or fetched and kept, and one put is kept too; any other (the head's)
/// goes to the store, as every write does.
pub(crate) struct CachedKv {
    store: Arc<dyn KvStore>,
    cache: NamespaceCache,
}

impl CachedKv {
    pub(crate) fn new(store: Arc<dyn KvStore>, cache: NamespaceCache) -> Self {
        Self { store, cache }
    }
}

impl KvStore for CachedKv {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        if !immutable(key) {
            // Issued now, as the store's contract wants.
            return self.store.get(key);
        }
        Box::pin(async move {
            if let Some(value) = self.cache.value(key).await {
                return Ok(Some(value));
            }
            let value = self.store.get(key).await?;
            if let Some(value) = &value {
                self.cache.keep_value(key, value);
            }
            Ok(value)
        })
    }

    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.store.put(key, value.clone())?;
        if immutable(key) {
            self.cache.keep_value(key, &value);
        }
        Ok(())
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        self.store.delete(key)
    }

    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        self.store.sync()
    }

    fn put_and_sync<'a>(&'a self, key: &'a str, value: Bytes) -> BoxFuture<'a, io::Result<()>> {
        if !immutable(key) {
            return self.store.put_and_sync(key, value);
        }
        let synced = self.store.put_and_sync(key, value.clone());
        Box::pin(async move {
            synced.await?;
            // Kept once stored, never if the put failed.
            self.cache.keep_value(key, &value);
            Ok(())
        })
    }

    fn keys(&self) -> BoxFuture<'_, io::Result<Vec<String>>> {
        self.store.keys()
    }
}

/// A namespace's pack store (S3), read through its cache: every blob is put
/// once, under a name never reused, so a range read is served from the
/// cache, or fetched and kept (up to [`MAX_KEPT_RANGE`]). A put keeps nothing
/// here, as states read a pack by ranges: the checkpoint that puts it keeps
/// those ([`NamespaceCache::keep_ranges`]).
pub(crate) struct CachedBlobs {
    store: Arc<dyn BlobStore>,
    cache: NamespaceCache,
}

impl CachedBlobs {
    pub(crate) fn new(store: Arc<dyn BlobStore>, cache: NamespaceCache) -> Self {
        Self { store, cache }
    }
}

impl BlobStore for CachedBlobs {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move {
            let kept = range.end - range.start <= MAX_KEPT_RANGE;
            if kept {
                if let Some(value) = self.cache.range(key, range.clone()).await {
                    return Ok(Some(value));
                }
            }
            let value = self.store.get_range(key, range.clone()).await?;
            if let (true, Some(value)) = (kept, &value) {
                self.cache.keep_range(key, range, value);
            }
            Ok(value)
        })
    }

    fn put<'a>(&'a self, key: &'a str, value: Bytes) -> BoxFuture<'a, io::Result<()>> {
        self.store.put(key, value)
    }

    fn delete<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, io::Result<()>> {
        self.store.delete(keys)
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<String>>> {
        self.store.list(prefix)
    }
}

impl Owner {
    fn new(ids: &AtomicU64) -> Self {
        Self {
            id: ids.fetch_add(1, Relaxed),
            committed: None,
            flushing: None,
            epoch: 0,
            nonce: random().max(1),
            unknown: 0,
            settled: 0,
            backoff: Backoff::default(),
        }
    }
}

/// Whether the heads `a` and `b` name the same state: they are equal, or
/// both absent (version 0), an empty database, whatever identity each open
/// drew for it.
pub(crate) fn same_state(a: &Head, b: &Head) -> bool {
    a == b || (a.version == 0 && b.version == 0)
}

/// `value` in an allocation of its own. A value read from the store can be
/// a slice of a larger receive buffer (eager state is slices of the
/// invocation's start message), which the cache would otherwise keep
/// whole, uncounted, for as long as it keeps the value.
fn owned(value: &Bytes) -> Bytes {
    Bytes::copy_from_slice(value)
}

/// A random number, for identities.
pub(crate) fn random() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |time| time.as_nanos()),
    );
    hasher.finish()
}

/// A value parked at a durable head, for its namespace's next owner's
/// session at that head (see [`crate::state::State::park`]).
pub(crate) struct ParkedValue {
    pub value: Box<dyn Any + Send>,
    /// The head of the state it corresponds to.
    pub head: Head,
    /// Its bytes, as whoever parked it weighs it.
    pub weight: usize,
}

/// Parked values within a byte budget, one per owner, least recently
/// parked dropped first.
struct Parked {
    budget: usize,
    used: usize,
    clock: u64,
    parked: HashMap<u64, (ParkedValue, u64)>,
    order: BTreeMap<u64, u64>,
}

impl Parked {
    fn new(budget: usize) -> Self {
        Self {
            budget,
            used: 0,
            clock: 0,
            parked: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    /// Park `value` for the owner `key`, evicting the least recently parked
    /// to stay in the budget; one heavier than the whole budget is refused
    /// rather than evict everything. What no longer fits is returned.
    #[must_use]
    fn park(&mut self, key: u64, value: ParkedValue) -> Vec<ParkedValue> {
        let mut out: Vec<ParkedValue> = self.take(key).into_iter().collect();
        if value.weight > self.budget {
            out.push(value);
            return out;
        }
        self.clock += 1;
        self.used += value.weight;
        self.parked.insert(key, (value, self.clock));
        self.order.insert(self.clock, key);
        while self.used > self.budget {
            let (_, oldest) = self.order.pop_first().expect("nonempty when over budget");
            let (value, _) = self
                .parked
                .remove(&oldest)
                .expect("ordered state is parked");
            self.used -= value.weight;
            out.push(value);
        }
        out
    }

    fn take(&mut self, key: u64) -> Option<ParkedValue> {
        let (value, parked) = self.parked.remove(&key)?;
        self.order.remove(&parked);
        self.used -= value.weight;
        Some(value)
    }
}

/// The key of a value kept: its namespace's identity, and its key in the
/// store (`name`), whole (`offset` 0, `len` `u64::MAX`), or a range of it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ValueKey {
    namespace: u64,
    name: String,
    offset: u64,
    len: u64,
}

impl Code for ValueKey {
    fn encode(&self, writer: &mut impl std::io::Write) -> foyer::Result<()> {
        self.namespace.encode(writer)?;
        (self.name.len() as u64).encode(writer)?;
        writer
            .write_all(self.name.as_bytes())
            .map_err(foyer::Error::io_error)?;
        self.offset.encode(writer)?;
        self.len.encode(writer)
    }

    fn decode(reader: &mut impl std::io::Read) -> foyer::Result<Self> {
        let namespace = u64::decode(reader)?;
        let len = u64::decode(reader)?;
        let mut name = vec![0; len as usize];
        reader
            .read_exact(&mut name)
            .map_err(foyer::Error::io_error)?;
        Ok(Self {
            namespace,
            name: String::from_utf8(name)
                .map_err(|error| foyer::Error::io_error(io::Error::other(error)))?,
            offset: u64::decode(reader)?,
            len: u64::decode(reader)?,
        })
    }

    fn estimated_size(&self) -> usize {
        32 + self.name.len()
    }
}

#[cfg(test)]
#[path = "tests/unit/cache.rs"]
mod tests;

/// What a [`PageCache`] has done since it opened ([`PageCache::stats`]), as
/// foyer counts it. An evicted value goes to the disk tier unless it is
/// dropped: `disk_enqueued` counts those offered, `skipped_on_disk` those
/// read from the disk and still there (not written again), `dropped_buffer`
/// those that did not fit a writer's buffer ([`PageCacheConfig::disk_buffer`]),
/// and `dropped_queue` those sent while a writer was not taking any.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Lookups the memory tier answered.
    pub memory_hits: u64,
    /// Values the memory tier evicted.
    pub memory_evictions: u64,
    /// Evicted values offered to the disk tier.
    pub disk_enqueued: u64,
    /// Lookups the disk tier answered from its file: not those of values
    /// evicted and not yet written, which it answers from memory.
    pub disk_hits: u64,
    /// Lookups neither tier answered.
    pub disk_misses: u64,
    /// Evicted values dropped: a writer's queue was full (it was not polled
    /// in time to take them into its buffer).
    pub dropped_queue: u64,
    /// Evicted values dropped: a writer's buffer had no room while its
    /// write was in flight.
    pub dropped_buffer: u64,
    /// Evicted values not written again: read from the disk, still there.
    pub skipped_on_disk: u64,
    /// Writes to the disk tier's file, and their bytes.
    pub disk_writes: u64,
    pub disk_written_bytes: u64,
    /// Reads of the disk tier's file, and their bytes.
    pub disk_reads: u64,
    pub disk_read_bytes: u64,
}

/// A metrics registry for foyer that keeps its counters, by metric and
/// labels, for [`PageCache::stats`]; its gauges and histograms are dropped.
#[derive(Clone, Debug, Default)]
struct Counters(Arc<Mutex<BTreeMap<String, Arc<AtomicU64>>>>);

impl Counters {
    /// The counter `metric` of the operation `op`, whatever the cache's name.
    fn get(&self, metric: &str, op: &str) -> u64 {
        let counters = self.0.lock().unwrap();
        (counters.iter())
            .filter(|(key, _)| key.starts_with(metric) && key.ends_with(&format!("/{op}")))
            .filter(|(key, _)| key.split('/').next() == Some(metric))
            .map(|(_, count)| count.load(Relaxed))
            .sum()
    }
}

impl mixtrics::metrics::RegistryOps for Counters {
    fn register_counter_vec(
        &self,
        name: std::borrow::Cow<'static, str>,
        _: std::borrow::Cow<'static, str>,
        _: &'static [&'static str],
    ) -> mixtrics::metrics::BoxedCounterVec {
        Box::new(CounterVec {
            name: name.into_owned(),
            counters: self.clone(),
        })
    }

    fn register_gauge_vec(
        &self,
        name: std::borrow::Cow<'static, str>,
        desc: std::borrow::Cow<'static, str>,
        labels: &'static [&'static str],
    ) -> mixtrics::metrics::BoxedGaugeVec {
        mixtrics::registry::noop::NoopMetricsRegistry.register_gauge_vec(name, desc, labels)
    }

    fn register_histogram_vec(
        &self,
        name: std::borrow::Cow<'static, str>,
        desc: std::borrow::Cow<'static, str>,
        labels: &'static [&'static str],
    ) -> mixtrics::metrics::BoxedHistogramVec {
        mixtrics::registry::noop::NoopMetricsRegistry.register_histogram_vec(name, desc, labels)
    }

    fn register_histogram_vec_with_buckets(
        &self,
        name: std::borrow::Cow<'static, str>,
        desc: std::borrow::Cow<'static, str>,
        labels: &'static [&'static str],
        buckets: Vec<f64>,
    ) -> mixtrics::metrics::BoxedHistogramVec {
        mixtrics::registry::noop::NoopMetricsRegistry
            .register_histogram_vec_with_buckets(name, desc, labels, buckets)
    }
}

/// One metric's counters, by labels.
#[derive(Debug)]
struct CounterVec {
    name: String,
    counters: Counters,
}

impl mixtrics::metrics::CounterVecOps for CounterVec {
    fn counter(
        &self,
        labels: &[std::borrow::Cow<'static, str>],
    ) -> mixtrics::metrics::BoxedCounter {
        let mut key = self.name.clone();
        for label in labels {
            key.push('/');
            key.push_str(label);
        }
        let count = self
            .counters
            .0
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .clone();
        Box::new(Counter(count))
    }
}

#[derive(Debug)]
struct Counter(Arc<AtomicU64>);

impl mixtrics::metrics::CounterOps for Counter {
    fn increase(&self, by: u64) {
        self.0.fetch_add(by, Relaxed);
    }
}
