//! The state as stored: a checkpoint, and the tail of flushes since, applied
//! in memory (see [`crate::state::format`] for what is stored where).
//!
//! **Flush.** A flush encodes its mutation as one tail entry (each page it
//! wrote a zstd frame of its own), puts it and the head, and syncs: one round
//! trip. Its pages join the *overlay*: the pages written since the
//! checkpoint, kept as their frames.
//!
//! **Checkpoint.** Now and then, a flush first writes the durable state as a
//! checkpoint: each frame the overlay changed, rebuilt from its frame in the
//! last checkpoint and the overlay's pages, and the records set since, into
//! one new pack, and a manifest, in Restate,
//! that names those, and every other frame and value where the checkpoints
//! before put them. The live frames and values of packs mostly dead move into it too
//! (compaction). The flush's head then names it, and the overlay empties.
//!
//! **Open.** Get the head; then, together, the checkpoint's manifest and the
//! tail entries since it, which the open applies; then, together, the large
//! records the manifest stores, and small files' frames. An
//! owner whose cache holds the state of the head it read reads nothing more.
//!
//! **Read.** A page in the overlay is decoded from its frame; any other is
//! sliced from its frame in a pack, fetched with one ranged get, which the
//! cache keeps under its location: a pack never changes, and its name is
//! unique.
use crate::state::{
    cache::{self, NamespaceCache},
    format::{
        invalid, Checkpoint, Entry, FileChange, FileIndex, Head, Location, Manifest, PackId, Tier,
        Value, MAX_DECODED, MAX_FILE_BYTES, MAX_FRAME_PAGES, MAX_NAMES, MAX_NAME_BYTES,
        MAX_RECORD_BYTES,
    },
    tail::Tiered,
    zstd, Page, StateOptions, PAGE,
};
use bytes::Bytes;
use futures::{
    future::{try_join_all, BoxFuture, FutureExt, Shared, WeakShared},
    StreamExt, TryStreamExt,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    io,
    ops::Range,
    sync::{Arc, Mutex},
};

/// Decoded frames a state keeps before it drops them all.
const KEPT_FRAMES: usize = 256;
/// Reads of packs, or of pages' references, a state has in flight at once:
/// an open of many large values, or a checkpoint of many frames, waits
/// for some rather than asking the store for all of them together.
const CONCURRENT_READS: usize = 32;
/// Bytes a checkpoint's compaction moves at most, sparsest packs first
/// (one pack at least): the rest move at the checkpoints after it.
const MAX_MOVED_BYTES: u64 = 64 << 20;
/// Ranges a compaction moves from one pack are fetched together while the
/// dead bytes between them are at most this many...
const RUN_GAP: u64 = 64 << 10;
/// ... and the whole fetch is at most this many.
const MAX_RUN: u64 = 8 << 20;

/// The outputs of `futures`, in order, with at most [`CONCURRENT_READS`]
/// of them running at once; the first error. They are collected first, so
/// that the future returned holds them and not the iterator that made
/// them (whose closures would keep it from being `Send`).
fn bounded<T, F: Future<Output = io::Result<T>>>(
    futures: impl IntoIterator<Item = F>,
) -> impl Future<Output = io::Result<Vec<T>>> {
    let futures: Vec<F> = futures.into_iter().collect();
    (futures::stream::iter(futures))
        .buffered(CONCURRENT_READS)
        .try_collect()
}

fn refuse<T>(message: String) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidInput, message))
}

/// The records' values, by name.
pub(crate) type Records = BTreeMap<String, Bytes>;

/// Logical page replacements of one file, by page number; `None` is a hole.
pub(crate) type Changes = BTreeMap<u64, Option<Page>>;

/// What a mutation writes to one paged file: the pages it changed, and its size:
/// set ([`crate::state::Mutation::set_len`]), or else only what the pages written need
/// (the file's length when the mutation first wrote it, grown by its writes),
/// which a flush keeps a longer file past.
#[derive(Clone, Debug, Default)]
pub(crate) struct Written {
    pub size: u64,
    pub pages: Changes,
    /// `size` was set, rather than grown by writes.
    pub sized: bool,
}

impl Written {
    /// The file's size after the flush, the file being `current` bytes now.
    pub(super) fn size_after(&self, current: u64) -> u64 {
        match self.sized {
            true => self.size,
            false => self.size.max(current),
        }
    }
}

/// What a flush writes to paged files, by name.
pub(crate) type Writes = BTreeMap<String, Written>;

/// What a name is: one kind, for good.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Is {
    Record,
    Paged,
}

impl Is {
    /// Refuse the name `name`, held as `self`, as `wanted`.
    pub(crate) fn refuse(self, name: &str, wanted: Is) -> String {
        format!("{name:?} is {}, not {}", self.article(), wanted.article())
    }

    fn article(self) -> &'static str {
        match self {
            Is::Record => "a record",
            Is::Paged => "a paged file",
        }
    }
}

/// What a flush wrote.
#[derive(Clone, Debug, Default)]
pub(crate) struct FlushReport {
    /// Bytes of its tail entry: the records and pages it wrote.
    #[cfg(test)]
    pub entry_bytes: usize,
    /// What the checkpoint its head names did, if it named a new one
    /// ([`crate::state::StateOptions::checkpoint_entries`]).
    pub checkpoint: Option<Checkpointed>,
    /// Why the checkpoint that was due failed: the flush went on without
    /// it, the tail grows, and the next flush tries again; after more
    /// failures in a row, fewer and fewer flushes do (one in 64 at most).
    pub checkpoint_failed: Option<String>,
    /// Why the garbage collection after the checkpoint failed: the flush
    /// landed, but the state ended, as what the collection deleted has an
    /// unknown outcome; the next open collects again.
    pub collection_failed: Option<String>,
}

/// What a checkpoint did ([`FlushReport::checkpoint`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Checkpointed {
    /// The version it holds the state of.
    pub version: u64,
    /// Frames it wrote, rebuilt from the overlay.
    pub frames: usize,
    /// Frames it patched instead: each of their pages that differs from
    /// their frame's, written alone.
    pub patched: usize,
    /// Large records it wrote.
    pub values: usize,
    /// Bytes of its pack: none if it stored nothing outside its manifest.
    pub pack_bytes: usize,
    /// Bytes of its manifest, in Restate.
    pub manifest_bytes: usize,
    /// Packs mostly dead, whose live frames and values it moved into its own.
    pub compacted: usize,
    /// Packs the garbage collection after it deleted.
    pub deleted: usize,
    /// Its pack went to L1 (S3), and everything L0 held with it.
    pub promoted: bool,
}

/// A checkpoint put, for the next head to name.
pub(crate) struct Staged {
    pub checkpoint: Checkpoint,
    pub manifest: Arc<Manifest>,
    pub report: Checkpointed,
}

/// A paged file as of a head: its size, and the pages written
/// since the checkpoint (its overlay), each the chain of its changes.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FileState {
    pub size: u64,
    pub pages: BTreeMap<u64, Chain>,
    /// The fewest pages the file had since the checkpoint, if it shrank:
    /// the checkpoint's pages from there on are cut, holes.
    pub cut: Option<u64>,
}

impl FileState {
    /// Whether the checkpoint's page `page` is cut.
    pub(crate) fn cut(&self, page: u64) -> bool {
        self.cut.is_some_and(|cut| page >= cut)
    }

    /// Whether page `page` builds on the page as the checkpoint holds it:
    /// neither cut nor made a hole since.
    pub(crate) fn on_checkpoint(&self, page: u64) -> bool {
        !self.cut(page) && !self.pages.get(&page).is_some_and(Chain::starts_over)
    }

    /// Page `page` as the file holds it: `checkpointed`, the page as its
    /// checkpoint holds it (asked for only [`Self::on_checkpoint`]), else
    /// `hole`, with the tail's changes to it applied in order.
    pub(crate) fn image(&self, page: u64, checkpointed: Page, hole: &Page) -> io::Result<Page> {
        let before = match self.cut(page) {
            true => hole.clone(),
            false => checkpointed,
        };
        match self.pages.get(&page) {
            Some(chain) => chain.fold(before, hole),
            None => Ok(before),
        }
    }

    /// Apply `change`, a tail entry's to this file: a shrink, then its
    /// pages, each appended to its chain.
    pub(crate) fn apply(&mut self, change: &FileChange) {
        if change.size < self.size {
            let end = change.size.div_ceil(PAGE);
            self.pages.retain(|&page, _| page < end);
            self.cut = Some(self.cut.map_or(end, |cut| cut.min(end)));
        }
        self.size = change.size;
        for (page, frame) in &change.pages {
            self.pages.entry(*page).or_default().push(frame.clone());
        }
    }
}

/// A page's changes since the checkpoint, in order: each a zstd frame
/// against the page before it, or `None`, a hole, which starts it over.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Chain(Vec<Option<Bytes>>);

impl Chain {
    pub(crate) fn push(&mut self, change: Option<Bytes>) {
        if change.is_none() {
            self.0.clear();
        }
        self.0.push(change);
    }

    /// Whether it starts over, from a hole, needing no page before it.
    pub(crate) fn starts_over(&self) -> bool {
        self.0.first().is_some_and(Option::is_none)
    }

    /// The page after its changes, from `before`, the page before them; a
    /// hole is `hole`.
    pub(crate) fn fold(&self, before: Page, hole: &Page) -> io::Result<Page> {
        let mut image = before;
        for change in &self.0 {
            image = match change {
                Some(frame) => decode_page(frame, &image)?,
                None => hole.clone(),
            };
        }
        Ok(image)
    }
}

/// The state as of a head: its checkpoint's manifest, and its tail entries,
/// applied.
#[derive(Clone, Debug)]
pub(crate) struct Durable {
    pub head: Head,
    pub manifest: Arc<Manifest>,
    pub files: BTreeMap<String, FileState>,
    pub records: Records,
    /// Records set since the checkpoint.
    pub dirty: BTreeSet<String>,
}

impl Durable {
    /// The state of `head` with nothing in it.
    pub(crate) fn empty(head: Head) -> Self {
        Self {
            head,
            manifest: Arc::default(),
            files: BTreeMap::new(),
            records: Records::new(),
            dirty: BTreeSet::new(),
        }
    }

    /// The state of `head`'s checkpoint, whose manifest is `manifest`, with
    /// the values it stores in `values`.
    fn checkpointed(
        head: Head,
        manifest: Arc<Manifest>,
        values: &HashMap<Location, Bytes>,
    ) -> io::Result<Self> {
        let value = |value: &Value| match value {
            Value::Inline(bytes) => Ok(bytes.clone()),
            Value::Stored(at) => decode_value(&values[at]),
        };
        let records = (manifest.records.iter())
            .map(|(name, record)| Ok((name.clone(), value(record)?)))
            .collect::<io::Result<_>>()?;
        let frame_bytes = head.frame_bytes();
        let mut files = BTreeMap::new();
        for (name, file) in &manifest.files {
            if file.frames.len() as u64 != file.size.div_ceil(frame_bytes) {
                return Err(invalid("manifest"));
            }
            let state = FileState {
                size: file.size,
                ..FileState::default()
            };
            files.insert(name.clone(), state);
        }
        let durable = Self {
            head,
            manifest,
            files,
            records,
            dirty: BTreeSet::new(),
        };
        let names = durable.files.len() + durable.records.len();
        match names == durable.names().len() {
            true => Ok(durable),
            false => Err(invalid("manifest: a name of two kinds")),
        }
    }

    /// Every name, and what it is.
    fn names(&self) -> HashMap<&str, Is> {
        (self.records.keys().map(|name| (name.as_str(), Is::Record)))
            .chain(self.files.keys().map(|name| (name.as_str(), Is::Paged)))
            .collect()
    }

    pub(crate) fn is(&self, name: &str) -> Option<Is> {
        if self.records.contains_key(name) {
            Some(Is::Record)
        } else {
            self.files.contains_key(name).then_some(Is::Paged)
        }
    }

    /// Apply the tail entry `entry`, which a flush made of this state.
    fn apply(&mut self, entry: &Entry) -> io::Result<()> {
        let kind = |this: &Self, name: &str, wanted: Is| match this.is(name) {
            Some(held) if held != wanted => Err(invalid("tail entry: a name of two kinds")),
            _ => Ok(()),
        };
        for (name, value) in &entry.records {
            kind(self, name, Is::Record)?;
            self.records.insert(name.clone(), value.clone());
            self.dirty.insert(name.clone());
        }
        for (name, change) in &entry.files {
            kind(self, name, Is::Paged)?;
            self.files.entry(name.clone()).or_default().apply(change);
        }
        match self.files.len() + self.records.len() <= MAX_NAMES {
            true => Ok(()),
            false => Err(invalid("tail entry: too many names")),
        }
    }

    /// This state, once the checkpoint `staged` of it is named: the same
    /// files and records, and an empty overlay.
    fn rebased(&self, staged: &Staged) -> Self {
        let mut files = self.files.clone();
        for file in files.values_mut() {
            file.pages.clear();
            file.cut = None;
        }
        Self {
            head: self.head.clone(),
            manifest: staged.manifest.clone(),
            files,
            records: self.records.clone(),
            dirty: BTreeSet::new(),
        }
    }
}

/// A record's value as a pack stores it: a zstd frame.
pub(crate) fn decode_value(frame: &[u8]) -> io::Result<Bytes> {
    zstd::decompress_sized(frame, &[], MAX_RECORD_BYTES).map(Bytes::from)
}

/// What a hole reads as: zeros.
pub(crate) fn hole_page() -> Page {
    static ZEROS: [u8; PAGE as usize] = [0; PAGE as usize];
    Page::from_static(&ZEROS)
}

/// A page, as a tail entry or a patch keeps it: a zstd frame, against
/// `reference`, the page before it.
fn encode_page(image: &[u8], reference: &[u8]) -> io::Result<Bytes> {
    zstd::compress(image, reference, Bytes::copy_from_slice)
}

pub(crate) fn decode_page(frame: &[u8], reference: &[u8]) -> io::Result<Page> {
    let image = zstd::decompress_sized(frame, reference, PAGE as usize)?;
    match image.len() == PAGE as usize {
        true => Ok(image.into()),
        false => Err(invalid("page")),
    }
}

/// A pack range being fetched, which concurrent reads of it share.
type Fetch = Shared<BoxFuture<'static, Result<Bytes, Arc<io::Error>>>>;
/// A fetch held only by its readers: when all of them are dropped, so is
/// the request.
type Fetching = HashMap<Location, WeakShared<BoxFuture<'static, Result<Bytes, Arc<io::Error>>>>>;

/// The committed view a session reads and extends.
pub(crate) struct Snapshot {
    tiered: Arc<Tiered>,
    options: StateOptions,
    /// The namespace in the process's [`crate::state::PageCache`].
    cache: Option<NamespaceCache>,
    /// A reader's: it never flushes, and reads as an owner's state does.
    reader: bool,
    pub(crate) durable: Arc<Durable>,
    /// An owner's, at the writer's last durable flush: no other state
    /// flushed since.
    pub(crate) unchanged: bool,
    /// Frames decoded, by location: packs never change, so every view of
    /// the state shares them.
    frames: Arc<Mutex<HashMap<Location, Arc<Bytes>>>>,
    fetching: Arc<Mutex<Fetching>>,
    /// The nonce in the keys of this session's tail entries without a cache,
    /// drawn at open: no other session's flush, lost or not, puts one of
    /// them. With a cache, the writer's ([`NamespaceCache::nonce`]).
    nonce: u64,
}

impl Snapshot {
    /// Open the namespace: see the module documentation. An owner without a
    /// cache, or with a flush running or unsettled, syncs alongside, so that
    /// what it reads is durable; a `reader` reads what is stored. Either
    /// reads the head from the store, and what it names through the cache.
    pub(super) async fn open(
        tiered: Arc<Tiered>,
        options: StateOptions,
        cache: Option<NamespaceCache>,
        reader: bool,
    ) -> io::Result<Self> {
        if !(1..=MAX_FRAME_PAGES).contains(&options.frame_pages) {
            return refuse(format!("frame_pages {} is invalid", options.frame_pages));
        }
        let owner = cache.as_ref().filter(|_| !reader);
        let epoch = owner.map(NamespaceCache::epoch);
        let unsettled = owner.and_then(NamespaceCache::unsettled);
        let settle = !reader && (owner.is_none() || unsettled.is_some());
        let settle = async {
            match settle {
                true => tiered.restate.sync().await,
                false => Ok(()),
            }
        };
        let (head_bytes, ()) = futures::try_join!(tiered.head(), settle)?;
        if let (Some(owner), Some(unknown)) = (owner, unsettled) {
            owner.settled(unknown);
        }
        let head = match &head_bytes {
            Some(bytes) => Head::decode(bytes)?,
            None => Head::new(cache::random(), options.frame_pages),
        };
        let mut snapshot = Self {
            tiered,
            options,
            cache: cache.clone(),
            reader,
            durable: Arc::new(Durable::empty(head.clone())),
            unchanged: false,
            frames: Arc::default(),
            fetching: Arc::default(),
            nonce: cache::random().max(1),
        };
        snapshot.durable = Arc::new(snapshot.load(head).await?);
        if let (Some(owner), Some(epoch)) = (owner, epoch) {
            let version = snapshot.version();
            owner.opened(version, epoch);
            snapshot.unchanged = owner.committed() == Some(version);
        }
        Ok(snapshot)
    }

    /// The state of `head`: its checkpoint's manifest and its tail together,
    /// then the values the manifest stores and small files' frames together.
    async fn load(&self, head: Head) -> io::Result<Durable> {
        let manifest = async {
            match &head.checkpoint {
                Some(checkpoint) => self.manifest(checkpoint).await,
                None => Ok(Manifest::default()),
            }
        };
        let tail = self.tiered.tail(&head, head.checkpointed() + 1);
        let (manifest, entries) = futures::try_join!(manifest, tail)?;
        let values: Vec<Location> = (manifest.records.values())
            .filter_map(|value| match value {
                Value::Stored(at) => Some(*at),
                Value::Inline(_) => None,
            })
            .collect();
        let small: Vec<&FileIndex> = (manifest.files.values())
            .filter(|file| file.size.div_ceil(PAGE) <= self.options.prefetch_pages)
            .collect();
        let wanted: Vec<Location> = (values.iter().copied())
            .chain(
                small
                    .iter()
                    .flat_map(|file| file.frames.iter().flatten().copied()),
            )
            .chain(small.iter().flat_map(|file| file.patches.values().copied()))
            .collect();
        let fetched = bounded(wanted.iter().copied().map(|at| self.raw(at))).await?;
        let fetched: HashMap<Location, Bytes> = wanted.into_iter().zip(fetched).collect();
        for file in &small {
            let frames: Vec<Option<Arc<Bytes>>> = (file.frames.iter())
                .map(|at| at.map(|at| self.decode_frame(&fetched[&at], at)))
                .map(Option::transpose)
                .collect::<io::Result<_>>()?;
            for (&page, &at) in &file.patches {
                let frame = frames
                    .get((page / head.frame_pages) as usize)
                    .cloned()
                    .flatten();
                let offset = ((page % head.frame_pages) * PAGE) as usize;
                let reference = frame.map_or(Bytes::new(), |frame| {
                    frame.slice(offset..offset + PAGE as usize)
                });
                self.keep(at, Arc::new(decode_page(&fetched[&at], &reference)?));
            }
        }
        let mut durable = Durable::checkpointed(head, Arc::new(manifest), &fetched)?;
        for entry in &entries {
            durable.apply(entry)?;
        }
        Ok(durable)
    }

    pub(super) fn version(&self) -> u64 {
        self.durable.head.version
    }

    pub(super) fn head(&self) -> &Head {
        &self.durable.head
    }

    pub(super) fn records(&self) -> &Records {
        &self.durable.records
    }

    pub(super) fn file_size(&self, name: &str) -> u64 {
        self.durable.files.get(name).map_or(0, |file| file.size)
    }

    /// What the name `name` is, if the state holds it.
    pub(crate) fn is(&self, name: &str) -> Option<Is> {
        self.durable.is(name)
    }

    /// Page `page` of the file `name`; `None` for zeros (a hole, or past its
    /// end).
    pub(super) async fn read(&self, name: &str, page: u64) -> io::Result<Option<Page>> {
        if !self.durable.files.contains_key(name) {
            return Ok(None);
        }
        let image = self.image(name, page).await?;
        Ok(image.iter().any(|&byte| byte != 0).then_some(image))
    }

    /// Page `page` of the file `name` as the state holds it
    /// ([`FileState::image`]): what a read returns, and what a flush
    /// compresses the page it writes against. A hole reads as zeros.
    async fn image(&self, name: &str, page: u64) -> io::Result<Page> {
        let hole = hole_page();
        let Some(file) = self.durable.files.get(name) else {
            return Ok(hole);
        };
        let checkpointed = match file.on_checkpoint(page) {
            true => (self.checkpointed(name, page).await?).unwrap_or_else(|| hole.clone()),
            false => hole.clone(),
        };
        file.image(page, checkpointed, &hole)
    }

    /// The frame stored at `at`: decoded, or fetched and decoded (and kept).
    async fn frame(&self, at: Location) -> io::Result<Arc<Bytes>> {
        if let Some(frame) = self.frames.lock().unwrap().get(&at) {
            return Ok(frame.clone());
        }
        let raw = self.raw(at).await?;
        self.decode_frame(&raw, at)
    }

    /// Page `page` of the file `name`, as its patch at `at` holds it:
    /// decoded, or fetched and decoded (and kept). A patch is compressed
    /// against the page its frame holds.
    async fn patch(&self, name: &str, page: u64, at: Location) -> io::Result<Page> {
        if let Some(image) = self.frames.lock().unwrap().get(&at) {
            return Ok((**image).clone());
        }
        let framed = self.framed(name, page);
        let (raw, framed) = futures::try_join!(self.raw(at), framed)?;
        let framed = framed.ok_or_else(|| invalid("manifest: a patch of a frame not stored"))?;
        let image = decode_page(&raw, &framed)?;
        self.keep(at, Arc::new(image.clone()));
        Ok(image)
    }

    /// Page `page` of the file `name` as the checkpoint holds it: its
    /// patch, else its frame's page; `None` if its frame is not stored.
    async fn checkpointed(&self, name: &str, page: u64) -> io::Result<Option<Page>> {
        let patched = (self.durable.manifest.files.get(name))
            .and_then(|file| file.patches.get(&page).copied());
        match patched {
            Some(at) => self.patch(name, page, at).await.map(Some),
            None => self.framed(name, page).await,
        }
    }

    /// Page `page` of the file `name` as its frame holds it; `None` if the
    /// frame is not stored.
    async fn framed(&self, name: &str, page: u64) -> io::Result<Option<Page>> {
        let frame_pages = self.durable.head.frame_pages;
        let index = page / frame_pages;
        let stored = (self.durable.manifest.files.get(name))
            .and_then(|file| file.frames.get(index as usize).copied().flatten());
        let Some(at) = stored else {
            return Ok(None);
        };
        let frame = self.frame(at).await?;
        let at = ((page % frame_pages) * PAGE) as usize;
        Ok(Some(frame.slice(at..at + PAGE as usize)))
    }

    /// Decode a frame, as stored at `at`, and keep it.
    fn decode_frame(&self, raw: &[u8], at: Location) -> io::Result<Arc<Bytes>> {
        let size = (self.durable.head.frame_pages * PAGE) as usize;
        let mut image = Vec::with_capacity(size);
        zstd::decompress(&mut image, raw, &[])?;
        if image.len() != size {
            return Err(invalid("frame"));
        }
        let image = Arc::new(Bytes::from(image));
        self.keep(at, image.clone());
        Ok(image)
    }

    /// Keep a decoded frame, dropping them all first once there are
    /// [`KEPT_FRAMES`].
    fn keep(&self, at: Location, image: Arc<Bytes>) {
        let mut frames = self.frames.lock().unwrap();
        if frames.len() >= KEPT_FRAMES {
            frames.clear();
        }
        frames.insert(at, image);
    }

    /// The manifest of `checkpoint`.
    async fn manifest(&self, checkpoint: &Checkpoint) -> io::Result<Manifest> {
        self.tiered.manifest(checkpoint).await
    }

    /// The bytes at `at`, in a pack. Where the store reads in blocks (the
    /// parts of a pack kept in Restate), it fetches the blocks `at`
    /// overlaps, which the cache keeps, so that its neighbours in them are
    /// read from the cache too.
    async fn raw(&self, at: Location) -> io::Result<Bytes> {
        let Some(block) = self.tiered.block(at.pack) else {
            return self.fetch(at, false).await;
        };
        let blocks = at.offset / block..at.range().end.div_ceil(block);
        let first = blocks.start;
        let got = try_join_all(blocks.map(|index| {
            let at = Location {
                pack: at.pack,
                offset: index * block,
                len: block,
            };
            self.fetch(at, true)
        }))
        .await?;
        let start = (at.offset - first * block) as usize;
        let end = start + at.len as usize;
        let joined = match got.as_slice() {
            [one] => one.clone(),
            _ => Bytes::from(got.concat()),
        };
        if end > joined.len() {
            return Err(io::Error::other(format!(
                "{} is shorter than {:?}",
                at.pack.key(),
                at.range()
            )));
        }
        Ok(joined.slice(start..end))
    }

    /// The bytes at each of `ats`, sorted by pack and offset, for a
    /// compaction: ranges of a pack close together are fetched as one run.
    /// Each range is kept where it moves to, as its new pack is put.
    async fn raw_runs(&self, ats: &[Location]) -> io::Result<Vec<Bytes>> {
        let mut runs: Vec<(Location, Vec<usize>)> = Vec::new();
        for (i, at) in ats.iter().enumerate() {
            match runs.last_mut() {
                Some((run, members))
                    if run.pack == at.pack
                        && at.offset <= run.range().end + RUN_GAP
                        && at.range().end - run.offset <= MAX_RUN =>
                {
                    run.len = run.len.max(at.range().end - run.offset);
                    members.push(i);
                }
                _ => runs.push((*at, vec![i])),
            }
        }
        let fetched = bounded(runs.iter().map(|&(run, _)| async move {
            match self.tiered.block(run.pack) {
                // In parts, which the cache keeps: a range of them.
                Some(_) => self.raw(run).await,
                None => self.tiered.get(run.pack, run.range()).await,
            }
        }))
        .await?;
        let mut out = vec![Bytes::new(); ats.len()];
        for ((run, members), bytes) in runs.iter().zip(fetched) {
            for &i in members {
                let start = (ats[i].offset - run.offset) as usize;
                out[i] = bytes.slice(start..start + ats[i].len as usize);
            }
        }
        Ok(out)
    }

    /// The range, or with `block` the block, `at` of a pack: fetched (through
    /// the cache), once for every read of this state that awaits it.
    async fn fetch(&self, at: Location, block: bool) -> io::Result<Bytes> {
        let fetch = {
            let mut fetching = self.fetching.lock().unwrap();
            if let Some(fetch) = fetching.get(&at).and_then(WeakShared::upgrade) {
                fetch
            } else {
                let tiered = self.tiered.clone();
                let fetch: Fetch = async move {
                    let value = match block {
                        true => tiered.get_block(at.pack, at.offset / at.len).await,
                        false => tiered.get(at.pack, at.range()).await,
                    };
                    value.map_err(Arc::new)
                }
                .boxed()
                .shared();
                fetching.insert(at, fetch.downgrade().expect("not polled yet"));
                fetch
            }
        };
        let value = fetch.await;
        // Gone once no read awaits it, unless a newer fetch took its place.
        let mut fetching = self.fetching.lock().unwrap();
        if (fetching.get(&at)).is_some_and(|fetch| fetch.upgrade().is_none()) {
            fetching.remove(&at);
        }
        drop(fetching);
        value.map_err(|error| io::Error::new(error.kind(), error.to_string()))
    }

    /// Refuse what no flush could store, before anything is written.
    pub(crate) async fn check(&self, writes: &Writes, records: &Records) -> io::Result<()> {
        // Each name is one kind: as stored, and in the mutation.
        let names = (records.keys().map(|name| (name, Is::Record)))
            .chain(writes.keys().map(|name| (name, Is::Paged)));
        let mut seen: HashMap<&str, Is> = HashMap::new();
        for (name, wanted) in names {
            if name.len() > MAX_NAME_BYTES {
                return refuse(format!("name {name:?} exceeds {MAX_NAME_BYTES} bytes"));
            }
            let held = seen.insert(name, wanted).or(self.is(name));
            if let Some(held) = held.filter(|&held| held != wanted) {
                return refuse(held.refuse(name, wanted));
            }
        }
        for (name, value) in records {
            if value.len() > MAX_RECORD_BYTES {
                let (len, max) = (value.len(), MAX_RECORD_BYTES);
                return refuse(format!("{name:?}: a value of {len} bytes exceeds {max}"));
            }
        }
        for (name, written) in writes {
            let size = written.size_after(self.file_size(name));
            if !size.is_multiple_of(512) || size > MAX_FILE_BYTES {
                return refuse(format!("{name:?}: invalid size {size}"));
            }
            if (written.pages.keys()).any(|&page| page >= MAX_FILE_BYTES / PAGE) {
                return refuse(format!("{name:?}: a page past the largest file"));
            }
        }
        // Its tail entry must decode: refused here, before anything is
        // written, is a mutation whose entry could pass the limit (a page
        // compresses to at most its bytes and 128 more).
        let named = |name: &String| name.len() + 64;
        let bound: usize =
            (records.iter())
                .map(|(name, value)| named(name) + value.len())
                .chain((writes.iter()).map(|(name, written)| {
                    named(name) + written.pages.len() * (PAGE as usize + 144)
                }))
                .sum();
        if bound > MAX_DECODED {
            return refuse(format!(
                "a mutation of up to {bound} bytes exceeds the {MAX_DECODED} a flush stores"
            ));
        }
        let new = (records.keys().chain(writes.keys()))
            .filter(|name| self.is(name).is_none())
            .count();
        if self.durable.names().len() + new > MAX_NAMES {
            return refuse(format!(
                "a state holds at most {MAX_NAMES} files and records"
            ));
        }
        Ok(())
    }

    /// Drop from a mutation what changes nothing; whether anything is left.
    pub(crate) fn changes(&self, writes: &mut Writes, records: &mut Records) -> bool {
        records.retain(|name, value| {
            (self.records().get(name)).map_or(!value.is_empty(), |held| held != value)
        });
        writes.retain(|name, written| {
            let current = self.file_size(name);
            !written.pages.is_empty() || written.size_after(current) != current
        });
        !(writes.is_empty() && records.is_empty())
    }

    /// Whether the tail is long enough to checkpoint before the next flush.
    pub(crate) fn due(&self) -> bool {
        let head = self.head();
        let since = head.version - head.checkpointed();
        let (entries, bytes) = (
            self.options.checkpoint_entries,
            self.options.checkpoint_bytes,
        );
        // A checkpoint into L0 costs only its bytes, in Restate: one is due
        // once the tail passes what L0 and the manifest hold, as it costs
        // more to read, and to write against its pages, than a checkpoint.
        // A tail under a frame's bytes is never worth one: a checkpoint, and
        // the collection after it, costs a round trip or two. A checkpoint
        // into L1 costs an S3 put: it is due by the thresholds as they are.
        let manifest = head.checkpoint.map_or(0, |checkpoint| checkpoint.manifest);
        let held = self.l0_bytes() + manifest;
        let frame = head.frame_pages * PAGE;
        let limit = match self.promotes() {
            false => bytes.min(held.max(frame)),
            true => bytes,
        };
        head.version > 0
            && ((entries > 0 && since >= entries) || (bytes > 0 && head.tail_bytes >= limit))
    }

    /// Bytes L0's packs hold.
    fn l0_bytes(&self) -> u64 {
        (self.durable.manifest.sizes.iter())
            .filter(|(pack, _)| pack.tier == Tier::L0)
            .map(|(_, &bytes)| bytes)
            .sum()
    }

    /// Whether the next checkpoint goes to L1, with all of L0: there is an
    /// L1, and L0 would hold its share ([`StateOptions::l0_bytes`]) with the
    /// tail the checkpoint folds in, about what it writes: one large
    /// checkpoint never takes L0 far past it.
    fn promotes(&self) -> bool {
        let l0 = self.l0_bytes() + self.durable.head.tail_bytes;
        self.tiered.has_l1() && l0 >= self.options.l0_bytes
    }

    /// Flush `writes` and `records`, which [`Self::check`] passed,
    /// with a head that names `staged`, a checkpoint of this state, if
    /// given: see the module documentation.
    pub(crate) async fn commit(
        &self,
        mut writes: Writes,
        mut records: Records,
        staged: Option<Staged>,
    ) -> io::Result<(FlushReport, Self)> {
        if !self.changes(&mut writes, &mut records) {
            return Ok((FlushReport::default(), self.continued(self.durable.clone())));
        }
        let mut entry = Entry::default();
        for (name, written) in writes {
            let held = self.durable.files.get(&name);
            let size = written.size_after(held.map_or(0, |file| file.size));
            // A page past the end is a hole.
            let written: Vec<(u64, Option<Page>)> = (written.pages.into_iter())
                .map(|(page, image)| (page, image.filter(|_| page * PAGE < size)))
                .collect();
            // Each page against the page as the state holds it now: its
            // change, as the next in its chain.
            let befores = (written.iter())
                .filter(|(_, image)| image.is_some())
                .map(|(page, _)| self.image(&name, *page));
            let mut befores = bounded(befores).await?.into_iter();
            let mut pages = Vec::with_capacity(written.len());
            for (page, image) in written {
                let frame = match image {
                    Some(image) => {
                        let before = befores.next().expect("one a page");
                        Some(encode_page(&image, &before)?)
                    }
                    None => None,
                };
                pages.push((page, frame));
            }
            entry.files.push((name, FileChange { size, pages }));
        }
        entry.records = records.into_iter().collect();
        let parts = entry.encode()?;
        let entry_bytes: usize = parts.iter().map(Bytes::len).sum();
        let mut next = match &staged {
            Some(staged) => self.durable.rebased(staged),
            None => (*self.durable).clone(),
        };
        next.apply(&entry)?;
        let held = &self.durable.head;
        let mut head = Head {
            version: held.version + 1,
            tail_bytes: held.tail_bytes + entry_bytes as u64,
            ..held.clone()
        };
        head.parts.push(parts.len() as u32);
        let nonce = (self.cache.as_ref()).map_or(self.nonce, NamespaceCache::nonce);
        head.written_by(nonce);
        let mut deletes = Vec::new();
        if let Some(staged) = &staged {
            // The tail the checkpoint before covered goes; the tail this one
            // covers stays, for readers that opened on it.
            if let Some(before) = held.checkpoint {
                for version in held.tail_first..=before.version() {
                    deletes.extend(held.entry_keys(version));
                }
                head.parts
                    .drain(..(before.version() + 1 - held.tail_first) as usize);
                head.tail_first = before.version() + 1;
                head.trim_runs();
            }
            head.previous = held.checkpoint;
            head.checkpoint = Some(staged.checkpoint);
            head.tail_bytes = entry_bytes as u64;
        }
        next.head = head;
        let cache = self.cache.as_ref();
        if cache.is_some_and(|cache| !cache.claim(self.version())) {
            return refuse("another state flushed since this one opened; reopen".into());
        }
        if let Err(error) = self.tiered.write(&next.head, parts, &deletes).await {
            return Err(self.failed(error));
        }
        let next = Arc::new(next);
        if let Some(cache) = cache {
            cache.flushed(next.head.version);
        }
        let report = FlushReport {
            #[cfg(test)]
            entry_bytes,
            checkpoint: staged.map(|staged| staged.report),
            checkpoint_failed: None,
            collection_failed: None,
        };
        Ok((report, self.continued(next)))
    }

    /// Write this state, the head's, as a checkpoint: each frame the overlay
    /// changed, the records set since the last one, and the live frames and values of packs mostly dead, into one
    /// new pack, put, for the next head to name.
    pub(crate) async fn checkpoint(&self) -> io::Result<Staged> {
        let durable = &self.durable;
        let head = &durable.head;
        let (frame_pages, frame_bytes) = (head.frame_pages, head.frame_bytes());
        let promoted = self.promotes();
        let pack = PackId {
            version: head.version,
            nonce: cache::random(),
            tier: match promoted {
                true => Tier::L1,
                false => Tier::L0,
            },
        };
        let old = &durable.manifest;
        let mut manifest = Manifest {
            files: old.files.clone(),
            records: old.records.clone(),
            sizes: BTreeMap::new(),
        };
        let mut body = Vec::new();
        let put = |body: &mut Vec<u8>, bytes: &[u8]| {
            let at = Location {
                pack,
                offset: body.len() as u64,
                len: bytes.len() as u64,
            };
            body.extend_from_slice(bytes);
            at
        };
        // The frames the overlay changed, as the state holds them now: as
        // the checkpoint held them (a frame and its patches, fetched
        // together), with the overlay over them. A stored one is patched
        // while its patches stay within half its size: each page that
        // differs from the frame's, compressed against it, the patches of
        // pages unchanged since kept as they are. The others are rebuilt.
        let mut written = Vec::new();
        let mut changed = Vec::new();
        let (mut frames, mut patched) = (0, 0);
        for (name, file) in &durable.files {
            let count = file.size.div_ceil(frame_bytes);
            let held = old.files.get(name);
            let mut dirty: BTreeSet<u64> = (file.pages.keys())
                .map(|page| page / frame_pages)
                .filter(|&index| index < count)
                .collect();
            // The frames it held past a cut.
            if let (Some(cut), Some(held)) = (file.cut, held) {
                let stored = (held.frames.iter().enumerate())
                    .filter(|&(index, at)| at.is_some() && (index as u64 + 1) * frame_pages > cut)
                    .map(|(index, _)| index as u64);
                dirty.extend(stored.filter(|&index| index < count));
            }
            let indexed = manifest.files.entry(name.clone()).or_default();
            indexed.size = file.size;
            indexed.frames.resize(count as usize, None);
            indexed
                .patches
                .retain(|&page, _| page < count * frame_pages);
            for index in dirty {
                let range = index * frame_pages..(index + 1) * frame_pages;
                let patches: Vec<(u64, Location)> = (indexed.patches.range(range))
                    .map(|(&page, &at)| (page, at))
                    .collect();
                changed.push((name, file, index, indexed.frames[index as usize], patches));
            }
        }
        let helds = bounded(
            changed
                .iter()
                .map(|(name, _, _, stored, patches)| async move {
                    let frame = match stored {
                        Some(at) => Some(self.frame(*at).await?),
                        None => None,
                    };
                    let patches = patches.iter().map(|&(page, at)| async move {
                        Ok::<_, io::Error>((page, self.patch(name, page, at).await?))
                    });
                    Ok::<_, io::Error>((frame, try_join_all(patches).await?))
                }),
        )
        .await?;
        for ((name, file, index, stored, kept), (frame, patches)) in changed.into_iter().zip(helds)
        {
            let hole = vec![0; frame_bytes as usize];
            let first = index * frame_pages;
            let at =
                |page: u64| ((page - first) * PAGE) as usize..((page - first + 1) * PAGE) as usize;
            let mut image = frame
                .as_ref()
                .map_or_else(|| hole.clone(), |frame| frame.to_vec());
            for (page, patch) in &patches {
                image[at(*page)].copy_from_slice(patch);
            }
            let end = file.size.div_ceil(PAGE);
            for page in first..first + frame_pages {
                let at = at(page);
                let hole = Page::copy_from_slice(&hole[at.clone()]);
                let now = match page < end {
                    true => file.image(page, Page::copy_from_slice(&image[at.clone()]), &hole)?,
                    false => hole,
                };
                image[at].copy_from_slice(&now);
            }
            let indexed = manifest.files.get_mut(name).expect("indexed");
            let range = first..first + frame_pages;
            indexed.patches.retain(|page, _| !range.contains(page));
            if image == hole {
                indexed.frames[index as usize] = None;
                continue;
            }
            // A page past the end that changed takes a rebuild: no patch is
            // past a file's end.
            let differs = |frame: &[u8], page: u64| image[at(page)] != frame[at(page)];
            let patchable = frame
                .as_ref()
                .is_some_and(|frame| (end.max(first)..range.end).all(|page| !differs(frame, page)));
            if let (Some(stored), Some(frame), true) = (stored, &frame, patchable) {
                let mut patches = Vec::new();
                let mut bytes = 0;
                for page in range {
                    let at = at(page);
                    if !differs(frame, page) {
                        continue;
                    }
                    let unchanged = !file.cut(page) && !file.pages.contains_key(&page);
                    let held = kept.iter().find(|(held, _)| *held == page);
                    let patch = match held {
                        Some(&(_, location)) if unchanged => Ok(location),
                        _ => Err(encode_page(&image[at.clone()], &frame[at])?),
                    };
                    bytes += patch
                        .as_ref()
                        .map_or_else(|patch| patch.len() as u64, |at| at.len);
                    patches.push((page, patch));
                }
                if bytes * 2 <= stored.len {
                    for (page, patch) in patches {
                        let location = match patch {
                            Ok(location) => location,
                            Err(patch) => {
                                let location = put(&mut body, &patch);
                                let page = Bytes::copy_from_slice(&image[at(page)]);
                                written.push((location, Some(Arc::new(page))));
                                location
                            }
                        };
                        indexed.patches.insert(page, location);
                    }
                    patched += 1;
                    continue;
                }
            }
            let whole = zstd::compress(&image, &[], Bytes::copy_from_slice)?;
            let location = put(&mut body, &whole);
            written.push((location, Some(Arc::new(Bytes::from(image)))));
            indexed.frames[index as usize] = Some(location);
            frames += 1;
        }
        let before = written.len();
        // Values: inline if small, else a frame of their own.
        let inline = self.options.inline_record_bytes;
        let mut store = |body: &mut Vec<u8>, value: &Bytes| -> io::Result<Value> {
            if value.len() <= inline {
                return Ok(Value::Inline(value.clone()));
            }
            let frame = zstd::compress(value, &[], Bytes::copy_from_slice)?;
            let at = put(body, &frame);
            written.push((at, None));
            Ok(Value::Stored(at))
        };
        for (name, value) in &durable.records {
            if durable.dirty.contains(name) || !manifest.records.contains_key(name) {
                let value = store(&mut body, value)?;
                manifest.records.insert(name.clone(), value);
            }
        }
        let values = written.len() - before;
        // Compaction: what packs of this tier mostly dead still hold moves
        // into this one; into L1, all of L0 does.
        let mut live: HashMap<PackId, u64> = HashMap::new();
        for at in manifest.locations().filter(|at| at.pack != pack) {
            *live.entry(at.pack).or_default() += at.len;
        }
        let mut sparse: Vec<(u64, PackId)> = (live.iter())
            .filter(|&(held, _)| held.tier == pack.tier)
            .filter(|&(held, &live)| live * 2 < old.sizes.get(held).copied().unwrap_or(0))
            .map(|(&held, &live)| (live, held))
            .collect();
        // The cheapest to move first, up to a budget, so that no checkpoint
        // copies a large pack's worth at once.
        sparse.sort();
        let mut budget = 0;
        let mut sparse: BTreeSet<PackId> = (sparse.into_iter())
            .take_while(|&(live, _)| {
                let first = budget == 0;
                budget += live;
                first || budget <= MAX_MOVED_BYTES
            })
            .map(|(_, held)| held)
            .collect();
        if promoted {
            sparse.extend(live.keys().filter(|held| held.tier == Tier::L0));
        }
        let moving: BTreeSet<Location> = (manifest.locations())
            .filter(|at| sparse.contains(&at.pack))
            .copied()
            .collect();
        let moving: Vec<Location> = moving.into_iter().collect();
        let fetched = self.raw_runs(&moving).await?;
        let mut moved = HashMap::new();
        for (at, raw) in moving.into_iter().zip(fetched) {
            moved.insert(at, put(&mut body, &raw));
            written.push((moved[&at], None));
        }
        manifest.relocate(&moved);
        for held in manifest.packs() {
            if held != pack {
                manifest
                    .sizes
                    .insert(held, old.sizes.get(&held).copied().unwrap_or(0));
            }
        }
        // A checkpoint that stores nothing outside its manifest (the values
        // it set are inline) puts no pack.
        if !body.is_empty() {
            manifest.sizes.insert(pack, body.len() as u64);
        }
        let encoded = manifest.encode()?;
        let checkpoint = Checkpoint {
            pack,
            manifest: encoded.len() as u64,
        };
        let report = Checkpointed {
            version: head.version,
            frames,
            patched,
            values,
            pack_bytes: body.len(),
            manifest_bytes: encoded.len(),
            compacted: sparse.len(),
            deleted: 0,
            promoted,
        };
        if !body.is_empty() {
            let ranges: Vec<Range<u64>> = written.iter().map(|(at, _)| at.range()).collect();
            self.tiered.put_pack(pack, body.into(), &ranges).await?;
        }
        self.tiered.put_manifest(pack, encoded).await?;
        for (at, image) in written {
            if let Some(image) = image {
                self.keep(at, image);
            }
        }
        Ok(Staged {
            checkpoint,
            manifest: Arc::new(manifest),
            report,
        })
    }

    fn failed(&self, error: io::Error) -> io::Error {
        if let Some(cache) = &self.cache {
            cache.forget();
        }
        error
    }

    fn continued(&self, durable: Arc<Durable>) -> Self {
        Self {
            tiered: self.tiered.clone(),
            options: self.options.clone(),
            cache: self.cache.clone(),
            reader: self.reader,
            durable,
            unchanged: false,
            frames: self.frames.clone(),
            fetching: self.fetching.clone(),
            nonce: self.nonce,
        }
    }
}

#[cfg(test)]
#[path = "tests/unit/store.rs"]
mod tests;
