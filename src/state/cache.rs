//! Process-wide owner state: ranges of packs in a memory and disk cache
//! ([foyer]), shared by every namespace this process opens, plus
//! per-namespace knowledge of what this owner made durable.
//!
//! A [`PageCache`] is created once per process. Each namespace gets its own
//! [`NamespaceCache`] handle by key, so one namespace never reads another's
//! packs.
//!
//! # Packs
//!
//! A pack is put once, under a name no other put uses, and never changed
//! (see [`crate::state::format`]), so a range of it (a frame, a value, a manifest)
//! is cached under its location, for as long as the cache keeps it: there
//! is nothing to invalidate. Keys are the namespace's identity, the pack's
//! name, the offset and the length; a namespace's identity comes from a per-cache
//! counter, randomly seeded, and the disk tier starts empty in every process.
//!
//! # Durability knowledge
//!
//! The namespace remembers the head it last read or wrote and the state as
//! of it (so that an open that reads the same head reads nothing more), its
//! last durable flush (so a state can tell another flushed since it
//! opened), and the values parked at that flush (SQL's engine, say), for the
//! next state. An open records what it read only if nothing changed while
//! it loaded: a flush that landed meanwhile made it stale. A flush
//! whose outcome is unknown leaves writes this owner can read but a crash
//! can still lose, so the next open syncs first
//! ([`NamespaceCache::unsettled`]): what it reads, and caches, is durable.
//!
//! Namespace states are kept for a bounded number of namespaces, and parked
//! values within a byte budget, least recently used first; forgetting either
//! only makes a namespace cold.
use crate::state::{
    format::{Location, PackId},
    store::Durable,
};
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, Code, DeviceBuilder, FileDeviceBuilder, HybridCache,
    HybridCacheBuilder, RecoverMode, Spawner,
};
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    io,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc, Mutex, Weak,
    },
};

/// Bookkeeping bytes charged per cached range.
const ENTRY_OVERHEAD: usize = 64;

/// Sizes and location of a [`PageCache`].
#[derive(Clone, Debug)]
pub struct PageCacheConfig {
    /// File of the disk tier, its previous contents discarded; `None` for a
    /// temporary one, which goes away with the process.
    pub file: Option<PathBuf>,
    /// Bytes of packs' ranges kept in memory.
    pub memory: usize,
    /// Bytes of the disk tier.
    pub disk: usize,
    /// Bytes per disk block, the unit the disk tier writes and reclaims.
    pub disk_block: usize,
    /// Bytes of the namespaces' states kept, so that a session that finds
    /// its head unchanged reads nothing more: each its head, its checkpoint's
    /// manifest and its tail applied, as estimated. Least recently used go
    /// first; the cache has 8 shards, so a state past an eighth of this is
    /// not kept for long.
    pub namespace_bytes: usize,
    /// Bytes of the values parked for the next session of their
    /// namespace, as they weigh it: SQL's engine, say. Least recently parked go first.
    pub parked_bytes: usize,
}

impl PageCacheConfig {
    /// `memory` bytes of packs' ranges in memory, and `disk` in a temporary
    /// file.
    pub fn new(memory: usize, disk: usize) -> Self {
        Self {
            file: None,
            memory,
            disk,
            disk_block: 4 << 20,
            namespace_bytes: 256 << 20,
            parked_bytes: 64 << 20,
        }
    }

    /// As [`PageCacheConfig::new`], with the namespaces' states and the parked
    /// values bounded by `memory` too, so that a small cache keeps little,
    /// and disk blocks small enough that a small disk has 4 of them to reclaim.
    pub fn sized(memory: usize, disk: usize) -> Self {
        let config = Self::new(memory, disk);
        Self {
            disk_block: config.disk_block.min((disk / 4).max(64 << 10)),
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

/// Process-wide cache of packs' ranges; clones share it.
///
/// Parked values live here and may refer back to its namespace's handle, so
/// handles only reach it weakly: dropping the last `PageCache` drops it.
#[derive(Clone)]
pub struct PageCache {
    shared: Arc<Shared>,
    parked: Arc<Mutex<Parked>>,
}

struct Shared {
    ranges: HybridCache<RangeKey, Bytes>,
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
    /// Keeps the disk tier's own runtime alive when the host has none.
    _runtime: Option<Spawner>,
}

impl PageCache {
    /// Open the cache. Disk I/O runs on the caller's tokio runtime if there is
    /// one, otherwise on a small runtime of its own.
    pub async fn open(config: PageCacheConfig) -> io::Result<Self> {
        let spawner = match tokio::runtime::Handle::try_current() {
            Ok(handle) => Spawner::from(handle),
            Err(_) => Spawner::from(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("page-cache")
                    .enable_all()
                    .build()?,
            ),
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
        let ranges = HybridCacheBuilder::new()
            .with_name("state-ranges")
            .memory(config.memory)
            .with_weighter(|_, range: &Bytes| range.len() + ENTRY_OVERHEAD)
            .storage()
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(config.disk_block))
            // Namespace state is not persisted, so earlier entries are unusable.
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
                ranges,
                namespaces: CacheBuilder::new(config.namespace_bytes)
                    .with_weighter(|_, owner: &Arc<Mutex<Owner>>| owner.lock().unwrap().weight())
                    .build(),
                live: Mutex::default(),
                unsettled: Mutex::default(),
                // Distinct per instance, so two caches sharing a disk file by
                // mistake still never share keys.
                ids: AtomicU64::new(random() >> 1),
                _runtime: matches!(spawner, Spawner::Runtime(_)).then_some(spawner),
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
            key: key.to_owned(),
            id,
            handle: self.shared.ids.fetch_add(1, Relaxed),
            owner,
        }
    }

    /// Bytes of ranges held in memory.
    pub fn memory_bytes(&self) -> usize {
        self.shared.ranges.memory().usage()
    }
}

/// One namespace's view of a [`PageCache`]; clones share it.
#[derive(Clone)]
pub(crate) struct NamespaceCache {
    shared: Arc<Shared>,
    parked: Weak<Mutex<Parked>>,
    key: String,
    /// The owner's identity, which keys its ranges and parked values.
    id: u64,
    /// This handle's identity: the state whose flush holds the right to
    /// flush ([`Owner::flushing`]).
    handle: u64,
    owner: Arc<Mutex<Owner>>,
}

/// What [`NamespaceCache::validate`] found, for the open's
/// [`NamespaceCache::record`].
pub(crate) struct Validated {
    /// The head the open read is the one the owner last saw.
    pub(crate) known: bool,
    generation: u64,
}

/// Per-namespace state.
struct Owner {
    /// Identity of this state, which keys its ranges and parked values.
    id: u64,
    /// The head as last read or written by this owner; `None` when unknown,
    /// which matches no head.
    head: Option<Bytes>,
    /// The state as of that head.
    durable: Option<Arc<Durable>>,
    /// The version of this owner's last durable flush.
    committed: Option<u64>,
    /// The handle whose flush took the right to flush
    /// ([`NamespaceCache::claim`]) and has not ended: its writes may be
    /// visible, and not durable yet.
    flushing: Option<u64>,
    /// Counts the changes above: an open records what it read only if
    /// nothing changed since it validated ([`NamespaceCache::record`]).
    generation: u64,
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

    /// Compare the head that open read (`bytes`) to what this owner last
    /// saw: whether it is the same, as of what the owner is now.
    pub(crate) fn validate(&self, bytes: Option<&[u8]>) -> Validated {
        let mut owner = self.owner();
        // An absent head is an empty namespace; a real head is never empty.
        if owner.head.as_deref() == Some(bytes.unwrap_or_default()) {
            return Validated {
                known: true,
                generation: owner.generation,
            };
        }
        let stale = self.forget_locked(&mut owner);
        let generation = owner.generation;
        // Parked values may hold state handles, whose drop takes this lock.
        drop(owner);
        drop(stale);
        Validated {
            known: false,
            generation,
        }
    }

    /// The state as of the head this owner last saw.
    pub(crate) fn durable(&self) -> Option<Arc<Durable>> {
        self.owner().durable.clone()
    }

    /// Record the durable head `bytes` an open read, and the state as of
    /// it, and, if `committed`, that a flush may build on it unless a flush
    /// runs (that one's outcome decides); nothing, if the owner changed
    /// since the open's [`Self::validate`]: another state flushed, or failed
    /// to, and what the open read may be stale.
    pub(crate) fn record(
        &self,
        validated: &Validated,
        bytes: Option<Bytes>,
        committed: Option<u64>,
        durable: Arc<Durable>,
    ) {
        let mut owner = self.owner();
        if owner.generation != validated.generation {
            return;
        }
        owner.generation += 1;
        // An absent head is a real state: an empty namespace.
        owner.head = Some(owned(&bytes.unwrap_or_default()));
        owner.durable = Some(durable);
        if committed.is_some() && owner.flushing.is_none() {
            owner.committed = committed;
        }
        drop(owner);
        self.reweigh();
    }

    /// The claimed flush (see [`Self::claim`]) made `bytes`, at `version`,
    /// durable: the state `durable`.
    pub(crate) fn flushed(&self, bytes: Bytes, version: u64, durable: Arc<Durable>) {
        let mut owner = self.owner();
        owner.head = Some(bytes);
        owner.durable = Some(durable);
        owner.committed = Some(version);
        owner.flushing = None;
        owner.generation += 1;
        drop(owner);
        self.reweigh();
    }

    /// Insert the owner again, so that the cache weighs the state it now
    /// keeps (the weigher locks it: the caller must not hold the lock).
    fn reweigh(&self) {
        (self.shared.namespaces).insert(self.key.clone(), self.owner.clone());
    }

    /// Forget the head after a flush whose outcome is unknown, this
    /// handle's: it no longer runs.
    pub(crate) fn forget(&self) {
        let mut owner = self.owner();
        if owner.flushing == Some(self.handle) {
            owner.flushing = None;
        }
        owner.unknown += 1;
        let stale = self.forget_locked(&mut owner);
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
        drop(stale);
    }

    /// The flushes with an unknown outcome so far, if a sync has not settled
    /// them all, or a flush runs: their writes may be visible and yet not
    /// durable, so an open syncs before it reads (and caches).
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

    /// Forget the head and what was parked at it, which the caller drops once
    /// it holds no lock.
    #[must_use]
    fn forget_locked(&self, owner: &mut Owner) -> Option<ParkedValue> {
        owner.head = None;
        owner.durable = None;
        owner.committed = None;
        owner.generation += 1;
        self.take_parked()
    }

    /// Take the right to flush on top of `version`, this owner's last
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
            owner.generation += 1;
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

    /// The version of this owner's last durable flush, if still known.
    pub(crate) fn committed(&self) -> Option<u64> {
        self.owner().committed
    }

    /// Keep a parked value for the next session, in place of one parked
    /// before; it is dropped on any reset.
    pub(crate) fn park(&self, value: ParkedValue) {
        if let Some(parked) = self.parked.upgrade() {
            // Dropped once the lock is released.
            let evicted = parked.lock().unwrap().park(self.id, value);
            drop(evicted);
        }
    }

    /// The value parked for this namespace, if any.
    pub(crate) fn take_parked(&self) -> Option<ParkedValue> {
        self.parked.upgrade()?.lock().unwrap().take(self.id)
    }

    fn key(&self, at: Location) -> RangeKey {
        RangeKey {
            namespace: self.id,
            version: at.pack.version,
            nonce: at.pack.nonce,
            offset: at.offset,
            len: at.len,
        }
    }

    /// The bytes at `at`, from memory or disk. A disk error is a miss.
    pub(crate) async fn get(&self, at: Location) -> Option<Bytes> {
        let key = self.key(at);
        // A hit in memory without a hop to the disk's threads.
        if let Some(entry) = self.shared.ranges.memory().get(&key) {
            return Some(entry.value().clone());
        }
        match self.shared.ranges.get(&key).await {
            Ok(entry) => entry.map(|entry| entry.value().clone()),
            Err(_) => None,
        }
    }

    /// Keep the bytes at `at`, in a pack put.
    pub(crate) fn insert(&self, at: Location, value: Bytes) {
        self.shared.ranges.insert(self.key(at), owned(&value));
    }

    /// The manifest of the checkpoint whose pack is `pack`, from memory or
    /// disk: it never changes either.
    pub(crate) async fn manifest(&self, pack: PackId) -> Option<Bytes> {
        self.get(manifest_at(pack)).await
    }

    /// Keep the manifest of the checkpoint whose pack is `pack`.
    pub(crate) fn insert_manifest(&self, pack: PackId, manifest: Bytes) {
        self.insert(manifest_at(pack), manifest);
    }
}

impl Owner {
    /// Its weight in the namespace cache: about the bytes it keeps.
    fn weight(&self) -> usize {
        256 + self
            .durable
            .as_ref()
            .map_or(0, |durable| durable.estimated_bytes())
    }

    fn new(ids: &AtomicU64) -> Self {
        Self {
            id: ids.fetch_add(1, Relaxed),
            head: None,
            durable: None,
            committed: None,
            flushing: None,
            generation: 0,
            unknown: 0,
            settled: 0,
            backoff: Backoff::default(),
        }
    }
}

/// `value` in an allocation of its own. A value read from the store can be
/// a slice of a larger receive buffer (eager state is slices of the
/// invocation's start message), which the cache would otherwise keep
/// whole, uncounted, for as long as it keeps the value.
fn owned(value: &Bytes) -> Bytes {
    Bytes::copy_from_slice(value)
}

/// Where the cache keeps the manifest named like `pack`: past the end of
/// the pack, where none of its ranges are.
fn manifest_at(pack: PackId) -> Location {
    Location {
        pack,
        offset: u64::MAX,
        len: 0,
    }
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

/// A value parked at a durable version, for its namespace's
/// next session (see [`crate::state::State::park`]).
pub(crate) struct ParkedValue {
    pub value: Box<dyn Any + Send>,
    /// The version it corresponds to.
    pub version: u64,
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

/// A range's key: its namespace's identity, its pack's name, its offset and
/// its length. A pack never changes, so these name its bytes exactly: a
/// range a checkpoint wrote and a block a read fetched (a part of a pack
/// kept in Restate) are both kept, each under its own extent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RangeKey {
    namespace: u64,
    version: u64,
    nonce: u64,
    offset: u64,
    len: u64,
}

impl Code for RangeKey {
    fn encode(&self, writer: &mut impl std::io::Write) -> foyer::Result<()> {
        self.namespace.encode(writer)?;
        self.version.encode(writer)?;
        self.nonce.encode(writer)?;
        self.offset.encode(writer)?;
        self.len.encode(writer)
    }

    fn decode(reader: &mut impl std::io::Read) -> foyer::Result<Self> {
        Ok(Self {
            namespace: u64::decode(reader)?,
            version: u64::decode(reader)?,
            nonce: u64::decode(reader)?,
            offset: u64::decode(reader)?,
            len: u64::decode(reader)?,
        })
    }

    fn estimated_size(&self) -> usize {
        40
    }
}

#[cfg(test)]
#[path = "tests/unit/cache.rs"]
mod tests;
