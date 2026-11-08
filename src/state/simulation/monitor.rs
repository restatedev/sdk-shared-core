//! A protocol monitor: checks every request the code issues to Restate's
//! state against the rules of [`crate::state::store`] and [`crate::state::tail`]: a tail
//! entry or a pack is stored before the first head that names it, and
//! dropped only once no head a state may hold needs it.
//!
//! The store keeps writes in issue order, and a crash keeps a prefix of them,
//! so a write may rely on every write issued before it. The monitor wraps a
//! store and keeps the state those writes make: each write applied as it is
//! issued, and values it read (after a crash, a read learns what was kept).
//! A write that fails makes its key unknown until a read learns it; a read
//! of the head makes the tail entries past it unknown, which a crash may
//! have lost. Each write is checked when it is issued:
//!
//! 1. **Name only what is stored**: a head's tail entries past its
//!    checkpoint are stored in full (those before, kept for readers that
//!    opened on the checkpoint before, a crashed flush may have deleted),
//!    and so is a new checkpoint: its manifest, and every range the
//!    manifest names (where the monitor sees the packs: in the store, or
//!    given [`MonitorKv::packs`]).
//! 2. **Build on the head**: a head is the next version, with the same
//!    identity and frame size, and a tail that starts no earlier; a new
//!    checkpoint is newer than the one held, which becomes its previous.
//! 3. **Never overwrite**: a tail entry is put only for the version after
//!    the head's, and a part of a pack or of a manifest only once.
//! 4. **Drop only what no head needs**: a tail entry is deleted only once
//!    the head's checkpoint covers it, a pack in the store only if older
//!    than the head's previous checkpoint, and neither checkpoint names it,
//!    and a manifest only if older than the previous checkpoint.
//! 5. **Decodable**: every head, tail entry and manifest decodes, and so
//!    does every page, frame and value they name.
use crate::state::{
    blobs::{KV_PREFIX, MANIFEST_PREFIX},
    format::{
        parse_tail_key, Checkpoint, Entry, Head, Location, Manifest, PackId, Tier, Value, HEAD_KEY,
    },
    kv::KvStore,
    simulation::MemoryBlobs,
    store::{decode_page, decode_value, hole_page, FileState},
    zstd, PAGE,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io,
    ops::{Deref, Range},
    sync::Mutex,
};

/// A store wrapper that checks the protocol invariants on every request.
pub struct MonitorKv<S> {
    pub inner: S,
    known: Mutex<Known>,
}

impl<S> Deref for MonitorKv<S> {
    type Target = S;
    fn deref(&self) -> &S {
        &self.inner
    }
}

/// The state the writes issued so far make.
#[derive(Default)]
struct Known {
    /// The parts of tail entries, by key.
    tail: BTreeMap<String, Bytes>,
    /// Tail parts a failed write, or a crash, may or may not have changed.
    unknown: BTreeSet<String>,
    /// The keys with a nonce of every tail part ever put: each is put once.
    tail_put: BTreeSet<String>,
    /// The head, unless a failed write left it unknown.
    head: Option<Head>,
    head_unknown: bool,
    /// Packs in the store: their parts, by key.
    packs: BTreeMap<String, BTreeMap<u64, Bytes>>,
    /// Manifests: their parts, by name.
    manifest_parts: BTreeMap<String, BTreeMap<u64, Bytes>>,
    /// Packs elsewhere ([`MonitorKv::packs`]).
    external: Option<MemoryBlobs>,
    manifests: HashMap<PackId, Option<Manifest>>,
    /// Ranges of packs already decoded.
    decoded: HashSet<Location>,
    /// Frames decoded, by location: `None` if they do not decode.
    frames: HashMap<Location, Option<Bytes>>,
    violations: Vec<String>,
    checked: usize,
}

/// A key of the store, as the monitor tells them apart.
enum Key {
    Head,
    /// A tail entry's: its version, and whether its key has a nonce.
    Tail(u64, bool),
    Pack(String, u64),
    Manifest(String, u64),
    Other,
}

fn key(key: &str) -> Key {
    if key == HEAD_KEY {
        return Key::Head;
    }
    if let Some(tail) = parse_tail_key(key) {
        return Key::Tail(tail.version, tail.nonce != 0);
    }
    let part = |prefix: &str| {
        let (name, part) = key.strip_prefix(prefix)?.rsplit_once('/')?;
        Some((name.to_owned(), part.parse().ok()?))
    };
    if let Some((pack, part)) = part(KV_PREFIX) {
        return Key::Pack(pack, part);
    }
    match part(MANIFEST_PREFIX) {
        Some((name, part)) => Key::Manifest(name, part),
        None => Key::Other,
    }
}

/// `parts` whole, if none is missing.
fn whole(parts: &BTreeMap<u64, Bytes>) -> Option<Vec<u8>> {
    let mut whole = Vec::new();
    for (expected, (&part, bytes)) in parts.iter().enumerate() {
        if part != expected as u64 {
            return None;
        }
        whole.extend_from_slice(bytes);
    }
    Some(whole)
}

/// Learn that part `part` of the blob `name` among `blobs` is `value`.
fn learn_part(
    blobs: &mut BTreeMap<String, BTreeMap<u64, Bytes>>,
    name: String,
    part: u64,
    value: Option<Bytes>,
) {
    let parts = blobs.entry(name.clone()).or_default();
    match value {
        Some(value) => {
            parts.insert(part, value);
        }
        None => {
            parts.remove(&part);
            if parts.is_empty() {
                blobs.remove(&name);
            }
        }
    }
}

impl Known {
    fn violation(&mut self, message: String) {
        self.violations.push(message);
    }

    fn learn(&mut self, at: &str, value: Option<Bytes>) {
        match key(at) {
            Key::Head => {
                let head = value.and_then(|bytes| Head::decode(&bytes).ok());
                let version = head.as_ref().map_or(0, |head| head.version);
                self.head = head;
                self.head_unknown = false;
                let past =
                    |key: &String| parse_tail_key(key).is_some_and(|tail| tail.version > version);
                let lost: Vec<String> = self.tail.keys().filter(|key| past(key)).cloned().collect();
                for key in lost {
                    self.tail.remove(&key);
                    self.unknown.insert(key);
                }
            }
            Key::Tail(..) => {
                self.unknown.remove(at);
                match value {
                    Some(value) => self.tail.insert(at.to_owned(), value),
                    None => self.tail.remove(at),
                };
            }
            Key::Pack(name, part) => learn_part(&mut self.packs, name, part, value),
            Key::Manifest(name, part) => learn_part(&mut self.manifest_parts, name, part, value),
            Key::Other => {}
        }
    }

    fn forget(&mut self, at: &str) {
        match key(at) {
            Key::Head => self.head_unknown = true,
            Key::Tail(..) => {
                self.tail.remove(at);
                self.unknown.insert(at.to_owned());
            }
            Key::Pack(..) | Key::Manifest(..) | Key::Other => {}
        }
    }

    /// The head, unless a failed write left it unknown.
    fn held(&self) -> Option<&Head> {
        self.head.as_ref().filter(|_| !self.head_unknown)
    }

    /// The bytes `range` of the pack `pack`, if the monitor sees it.
    fn pack_range(&self, pack: PackId, range: Range<u64>) -> Option<Bytes> {
        let key = pack.key();
        if pack.tier == Tier::L1 {
            return self.external.as_ref()?.peek(&key, range);
        }
        let whole = whole(self.packs.get(&key)?)?;
        let range = range.start as usize..range.end as usize;
        whole.get(range).map(Bytes::copy_from_slice)
    }

    /// The manifest of `checkpoint`, if the monitor sees it.
    fn manifest(&mut self, checkpoint: &Checkpoint) -> Option<&Manifest> {
        if !self.manifests.contains_key(&checkpoint.pack) {
            let parts = self.manifest_parts.get(&checkpoint.pack.name());
            let manifest = (parts.and_then(whole))
                .filter(|bytes| bytes.len() as u64 == checkpoint.manifest)
                .and_then(|bytes| Manifest::decode(&bytes).ok());
            self.manifests.insert(checkpoint.pack, manifest);
        }
        self.manifests[&checkpoint.pack].as_ref()
    }

    fn check_put(&mut self, at: &str, value: &Bytes) {
        self.checked += 1;
        match key(at) {
            Key::Head => self.check_head(value),
            Key::Tail(version, nonce) => {
                // A key with a nonce names one value for ever: a lost flush's
                // entry is left for garbage collection, never put over.
                if nonce && !self.tail_put.insert(at.to_owned()) {
                    self.violation(format!("tail entry {at} put again"));
                }
                let Some(head) = self.held() else {
                    return;
                };
                let expected = head.version + 1;
                if version != expected {
                    self.violation(format!("tail entry {version} put; the next is {expected}"));
                }
            }
            Key::Pack(pack, part) => {
                if self
                    .packs
                    .get(&pack)
                    .is_some_and(|parts| parts.contains_key(&part))
                {
                    self.violation(format!("part {part} of pack {pack} put again"));
                }
            }
            Key::Manifest(name, part) => {
                if (self.manifest_parts.get(&name)).is_some_and(|parts| parts.contains_key(&part)) {
                    self.violation(format!("part {part} of manifest {name} put again"));
                }
            }
            Key::Other => self.violation(format!("a put of {at:?}")),
        }
    }

    fn check_head(&mut self, value: &Bytes) {
        let head = match Head::decode(value) {
            Ok(head) => head,
            Err(error) => return self.violation(format!("a head that does not decode: {error}")),
        };
        let v = head.version;
        let held = self.held().cloned();
        if let Some(held) = &held {
            if head.version != held.version + 1
                || head.id != held.id
                || head.frame_pages != held.frame_pages
                || head.tail_first < held.tail_first
            {
                let held = held.version;
                self.violation(format!("head {v} does not build on head {held}"));
            }
            let new = head.checkpoint != held.checkpoint;
            if new
                && (head.previous != held.checkpoint || head.checkpointed() <= held.checkpointed())
            {
                self.violation(format!(
                    "head {v}: a new checkpoint, not after the one held"
                ));
            }
        }
        self.check_tail(&head);
        let new = held.is_none_or(|held| head.checkpoint != held.checkpoint);
        if let Some(checkpoint) = head.checkpoint.filter(|_| new) {
            self.check_checkpoint(&head, &checkpoint);
        }
    }

    /// The tail entries of `head` past its checkpoint are stored and decode,
    /// in order: each page against the page as the entries before it left it.
    fn check_tail(&mut self, head: &Head) {
        let v = head.version;
        let mut files: BTreeMap<String, FileState> = BTreeMap::new();
        for version in head.checkpointed() + 1..=head.version {
            let mut parts = Vec::new();
            for key in head.entry_keys(version) {
                match self.tail.get(&key) {
                    Some(part) => parts.push(part.clone()),
                    None if self.unknown.contains(&key) => return,
                    None => return self.violation(format!("head {v} names {key}, not stored")),
                }
            }
            let entry = match Entry::decode(&parts) {
                Ok(entry) => entry,
                Err(error) => return self.violation(format!("tail entry {version}: {error}")),
            };
            for (name, change) in &entry.files {
                let mut file = match files.remove(name) {
                    Some(file) => file,
                    None => {
                        let Some(file) = self.checkpointed_file(head, name) else {
                            return;
                        };
                        file
                    }
                };
                for (page, frame) in &change.pages {
                    let Some(frame) = frame else {
                        continue;
                    };
                    let hole = hole_page();
                    let checkpointed = match file.on_checkpoint(*page) {
                        true => match self.checkpointed(head, name, *page) {
                            Some(page) => page,
                            None => return,
                        },
                        false => hole.clone(),
                    };
                    let decoded = (file.image(*page, checkpointed, &hole))
                        .and_then(|before| decode_page(frame, &before));
                    if let Err(error) = decoded {
                        let error = format!("tail entry {version}: page {page} of {name}: {error}");
                        return self.violation(error);
                    }
                }
                file.apply(change);
                files.insert(name.clone(), file);
            }
        }
    }

    /// The file `name` as `head`'s checkpoint holds it, with no tail over
    /// it; `None` where the monitor does not see the checkpoint.
    fn checkpointed_file(&mut self, head: &Head, name: &str) -> Option<FileState> {
        let size = match head.checkpoint {
            Some(checkpoint) => {
                (self.manifest(&checkpoint)?.files.get(name)).map_or(0, |file| file.size)
            }
            None => 0,
        };
        Some(FileState {
            size,
            ..FileState::default()
        })
    }

    /// Page `page` of the file `name` as `head`'s checkpoint holds it (its
    /// patch, else its frame's page), else what a hole reads; `None` where
    /// the monitor does not see the checkpoint.
    fn checkpointed(&mut self, head: &Head, name: &str, page: u64) -> Option<Bytes> {
        let hole = hole_page();
        let Some(checkpoint) = head.checkpoint else {
            return Some(hole);
        };
        let index = page / head.frame_pages;
        let file = self.manifest(&checkpoint)?.files.get(name);
        let patch = file.and_then(|file| file.patches.get(&page)).copied();
        let stored = file.and_then(|file| file.frames.get(index as usize).copied().flatten());
        let Some(stored) = stored else {
            return Some(hole);
        };
        let frame = self.frame(head.frame_pages, stored)?;
        let at = ((page % head.frame_pages) * PAGE) as usize;
        let framed = frame.slice(at..at + PAGE as usize);
        match patch {
            Some(at) => decode_page(&self.pack_range(at.pack, at.range())?, &framed).ok(),
            None => Some(framed),
        }
    }

    /// The frame stored at `at`, decoded; `None` if it is not stored or
    /// does not decode.
    fn frame(&mut self, frame_pages: u64, at: Location) -> Option<Bytes> {
        if let Some(frame) = self.frames.get(&at) {
            return frame.clone();
        }
        let size = (frame_pages * PAGE) as usize;
        let frame = self.pack_range(at.pack, at.range()).and_then(|raw| {
            let mut image = Vec::with_capacity(size);
            zstd::decompress(&mut image, &raw, &[]).ok()?;
            (image.len() == size).then(|| Bytes::from(image))
        });
        self.frames.insert(at, frame.clone());
        frame
    }

    /// A new checkpoint's manifest is stored and decodes, and so does every
    /// range it names.
    fn check_checkpoint(&mut self, head: &Head, checkpoint: &Checkpoint) {
        let (v, frame_pages) = (head.version, head.frame_pages);
        let Some(manifest) = self.manifest(checkpoint).cloned() else {
            return self.violation(format!("head {v}: its checkpoint's manifest is unreadable"));
        };
        for (name, file) in &manifest.files {
            for (index, at) in file.frames.iter().enumerate() {
                let what = format!("frame {index} of {name}");
                let Some(at) = at else {
                    continue;
                };
                if self.stored(v, *at, &what).is_none() {
                    continue;
                }
                if self.frame(frame_pages, *at).is_none() {
                    self.violation(format!("head {v}: {what} does not decode"));
                }
            }
            for (&page, &at) in &file.patches {
                let what = format!("patch of page {page} of {name}");
                let Some(raw) = self.stored(v, at, &what) else {
                    continue;
                };
                let index = page / frame_pages;
                let frame = (file.frames.get(index as usize).copied().flatten())
                    .map(|frame| self.frame(frame_pages, frame));
                let reference = match frame {
                    Some(Some(frame)) => {
                        let at = ((page % frame_pages) * PAGE) as usize;
                        frame.slice(at..at + PAGE as usize)
                    }
                    Some(None) => continue,
                    None => Bytes::new(),
                };
                if decode_page(&raw, &reference).is_err() {
                    self.violation(format!("head {v}: {what} does not decode"));
                }
            }
        }
        for (name, value) in &manifest.records {
            let what = format!("record {name}");
            let Value::Stored(at) = value else {
                continue;
            };
            if let Some(raw) = self.stored(v, *at, &what) {
                if let Err(error) = decode_value(&raw) {
                    self.violation(format!("head {v}: {what}: {error}"));
                }
            }
        }
    }

    /// The bytes at `at`, the first time a head names them; a violation if
    /// they are not stored.
    fn stored(&mut self, v: u64, at: Location, what: &str) -> Option<Bytes> {
        if !self.decoded.insert(at) {
            return None;
        }
        let raw = self.pack_range(at.pack, at.range());
        if raw.is_none() {
            let pack = at.pack.key();
            self.violation(format!("head {v}: {what} is in {pack}, not stored"));
        }
        raw
    }

    fn check_delete(&mut self, at: &str) {
        self.checked += 1;
        let Some(head) = self.held().cloned() else {
            return;
        };
        match key(at) {
            Key::Tail(version, _) => {
                // Whatever head a state read, it deletes no entry past its
                // checkpoint, which a later head may name: a lost flush's
                // entry waits for a checkpoint too.
                if version > head.checkpointed() {
                    self.violation(format!("tail entry {version} deleted, not checkpointed"));
                }
            }
            Key::Pack(pack, _) => {
                let Some(id) = PackId::parse(&pack) else {
                    return self.violation(format!("a delete of {at:?}"));
                };
                let mut needed = BTreeSet::new();
                for checkpoint in [head.checkpoint, head.previous].into_iter().flatten() {
                    needed.insert(checkpoint.pack);
                    if let Some(manifest) = self.manifest(&checkpoint) {
                        needed.extend(manifest.packs());
                    }
                }
                let older = head.previous.is_some_and(|p| id.version < p.version());
                if !older || needed.contains(&id) {
                    self.violation(format!("pack {pack} deleted, still needed"));
                }
            }
            Key::Manifest(name, _) => {
                let Some(version) = PackId::manifest_version(&name) else {
                    return self.violation(format!("a delete of {at:?}"));
                };
                if head.previous.is_none_or(|p| version >= p.version()) {
                    self.violation(format!("manifest {name} deleted, still needed"));
                }
            }
            Key::Head | Key::Other => self.violation(format!("a delete of {at:?}")),
        }
    }
}

impl<S: KvStore> MonitorKv<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            known: Mutex::default(),
        }
    }

    /// A monitor that knows `values` are durable, such as a store's contents
    /// before a campaign starts.
    pub fn seeded(inner: S, values: impl IntoIterator<Item = (String, Bytes)>) -> Self {
        let monitor = Self::new(inner);
        {
            let mut known = monitor.known.lock().unwrap();
            let mut values: Vec<(String, Bytes)> = values.into_iter().collect();
            // The head last: it forgets the tail entries past it.
            values.sort_by_key(|(key, _)| key == HEAD_KEY);
            for (key, value) in values {
                known.learn(&key, Some(value));
            }
        }
        monitor
    }

    /// Read checkpoints' packs from `packs`, where the state keeps them
    /// ([`crate::sql::Open::checkpoints`]).
    pub fn packs(self, packs: &MemoryBlobs) -> Self {
        self.known.lock().unwrap().external = Some(packs.clone());
        self
    }

    /// Invariant violations seen so far.
    pub fn violations(&self) -> Vec<String> {
        self.known.lock().unwrap().violations.clone()
    }

    /// Requests checked so far.
    pub fn checked(&self) -> usize {
        self.known.lock().unwrap().checked
    }
}

impl<S: KvStore> KvStore for MonitorKv<S> {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move {
            let value = self.inner.get(key).await?;
            self.known.lock().unwrap().learn(key, value.clone());
            Ok(value)
        })
    }

    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        let mut known = self.known.lock().unwrap();
        known.check_put(key, &value);
        let result = self.inner.put(key, value.clone());
        match &result {
            Ok(()) => known.learn(key, Some(value)),
            Err(_) => known.forget(key),
        }
        result
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        let mut known = self.known.lock().unwrap();
        known.check_delete(key);
        let result = self.inner.delete(key);
        match &result {
            Ok(()) => known.learn(key, None),
            Err(_) => known.forget(key),
        }
        result
    }

    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        self.inner.sync()
    }

    fn keys(&self) -> BoxFuture<'_, io::Result<Vec<String>>> {
        self.inner.keys()
    }
}
