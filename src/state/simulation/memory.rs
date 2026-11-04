//! In-memory model of a Restate object's state, as [`crate::state::kv`] describes
//! it, with exact logical traffic counters.
use crate::state::yield_now;
use crate::state::{
    blobs::{KV_PART, KV_PREFIX, MANIFEST_PREFIX},
    format::{parse_tail_key, Head, Manifest, PackId, HEAD_KEY, TAIL_PREFIX},
    kv::KvStore,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{Arc, Mutex},
};

/// Simulator traffic for one object class. Bytes are exact logical key/value
/// lengths; deletes carry only the key. `bytes_deleted` is reclaimed value size,
/// not request traffic. This does not model wire framing, tombstones or replicas.
#[derive(Clone, Debug, Default)]
pub struct Traffic {
    pub gets: u64,
    pub misses: u64,
    pub puts: u64,
    pub deletes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bytes_deleted: u64,
    pub get_key_bytes: u64,
    pub put_key_bytes: u64,
    pub delete_key_bytes: u64,
}

/// Class of a key, used to split traffic counters: `head`, `tail`, `pack`
/// (a part of a checkpoint's pack, kept in the store), `manifest` (a part of
/// a checkpoint's manifest) or `other`.
pub fn object_kind(key: &str) -> &'static str {
    if key == HEAD_KEY {
        "head"
    } else if key.starts_with(TAIL_PREFIX) {
        "tail"
    } else if key.starts_with(KV_PREFIX) {
        "pack"
    } else if key.starts_with(MANIFEST_PREFIX) {
        "manifest"
    } else {
        "other"
    }
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub gets: u64,
    pub puts: u64,
    pub deletes: u64,
    pub syncs: u64,
    /// Listings of the keys, and the pages an object store would answer
    /// them in (1,000 keys a page, as S3's `ListObjectsV2`).
    pub lists: u64,
    pub list_pages: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub read_keys: BTreeSet<String>,
    pub write_keys: BTreeSet<String>,
    pub delete_keys: BTreeSet<String>,
    pub by_kind: BTreeMap<&'static str, Traffic>,
}

impl Stats {
    /// Traffic of one [`object_kind`] class, zero when absent.
    pub fn kind(&self, kind: &str) -> Traffic {
        self.by_kind.get(kind).cloned().unwrap_or_default()
    }
}

#[derive(Default)]
struct Inner {
    /// Every write issued, applied: what reads see.
    values: BTreeMap<String, Bytes>,
    /// What a crash keeps: the writes synced.
    durable: BTreeMap<String, Bytes>,
    /// The writes issued since the last sync, in order.
    unsynced: Vec<(String, Option<Bytes>)>,
    stats: Stats,
}

/// Memory simulator of a store that keeps writes in issue order: reads see
/// every write issued, a sync makes them durable, and a crash keeps a prefix
/// of the writes since the last sync. Process memory, not disk storage.
#[derive(Clone, Default)]
pub struct MemoryKv {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryKv {
    /// Replace the value durably at `key`, as a fault of the disk would.
    pub fn corrupt(&self, key: &str, value: Bytes) {
        let mut inner = self.inner.lock().unwrap();
        inner.values.insert(key.to_owned(), value.clone());
        inner.durable.insert(key.to_owned(), value);
    }

    /// Independent byte-for-byte copy for deterministic crash simulations.
    pub fn fork(&self) -> Self {
        let inner = self.inner.lock().unwrap();
        Self {
            inner: Arc::new(Mutex::new(Inner {
                values: inner.values.clone(),
                durable: inner.durable.clone(),
                unsynced: inner.unsynced.clone(),
                stats: Stats::default(),
            })),
        }
    }
    /// Simulate a crash that loses every write since the last sync.
    pub fn crash(&self) {
        self.crash_keeping(0);
    }
    /// Simulate a crash that keeps the first `kept` writes since the last
    /// sync, and loses the others.
    pub fn crash_keeping(&self, kept: usize) {
        let mut inner = self.inner.lock().unwrap();
        let unsynced = std::mem::take(&mut inner.unsynced);
        assert!(
            kept <= unsynced.len(),
            "only {} writes since the sync",
            unsynced.len()
        );
        for (key, value) in unsynced.into_iter().take(kept) {
            inner.apply_durable(key, value);
        }
        inner.values = inner.durable.clone();
    }
    /// The writes issued since the last sync, in order: what a crash may lose.
    pub fn unsynced(&self) -> Vec<(String, Option<Bytes>)> {
        self.inner.lock().unwrap().unsynced.clone()
    }
    pub fn stats(&self) -> Stats {
        self.inner.lock().unwrap().stats.clone()
    }
    pub fn reset_stats(&self) {
        self.inner.lock().unwrap().stats = Stats::default();
    }
    /// Every key and its value, as reads see them: for seeding a
    /// [`super::MonitorKv`].
    pub fn values(&self) -> Vec<(String, Bytes)> {
        let inner = self.inner.lock().unwrap();
        (inner.values.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    /// Every key and its value as two stores that hold the same state hold
    /// them: the head's random database identity zeroed, and each pack and
    /// manifest named by its version, without its random nonce, here and in
    /// the head and manifests that name it.
    pub fn contents(&self) -> BTreeMap<String, Bytes> {
        let canonical = |pack: PackId| PackId { nonce: 0, ..pack };
        let mut contents = BTreeMap::new();
        // Packs and manifests, whole, by prefix and name.
        let mut blobs: BTreeMap<(&str, String), BTreeMap<u64, Bytes>> = BTreeMap::new();
        for (key, value) in self.values() {
            let blob = [KV_PREFIX, MANIFEST_PREFIX].into_iter().find_map(|prefix| {
                let (name, part) = key.strip_prefix(prefix)?.rsplit_once('/')?;
                Some((prefix, name.to_owned(), part.parse().ok()?))
            });
            match blob {
                Some((prefix, name, part)) => {
                    blobs.entry((prefix, name)).or_default().insert(part, value);
                }
                None => drop(contents.insert(key, value)),
            }
        }
        // The manifests' lengths, by version.
        let mut manifests = BTreeMap::new();
        for ((prefix, name), parts) in blobs {
            let mut bytes: Vec<u8> = parts.into_values().flatten().collect();
            let (version, name) = match prefix == MANIFEST_PREFIX {
                true => {
                    let version =
                        PackId::manifest_version(&name).expect("a manifest's name parses");
                    let mut manifest = Manifest::decode(&bytes).expect("a stored manifest decodes");
                    for at in manifest.locations_mut() {
                        at.pack = canonical(at.pack);
                    }
                    let sizes = std::mem::take(&mut manifest.sizes);
                    manifest.sizes = (sizes.into_iter())
                        .map(|(pack, size)| (canonical(pack), size))
                        .collect();
                    bytes = manifest.encode().expect("a manifest decoded").to_vec();
                    manifests.insert(version, bytes.len() as u64);
                    (version, format!("{version:016x}-{:016x}", 0))
                }
                false => {
                    let id = PackId::parse(&name).expect("a stored pack's key parses");
                    (id.version, canonical(id).key())
                }
            };
            for (part, bytes) in bytes.chunks(KV_PART as usize).enumerate() {
                let kept = contents.insert(
                    format!("{prefix}{name}/{part}"),
                    Bytes::copy_from_slice(bytes),
                );
                assert!(kept.is_none(), "two checkpoints of version {version}");
            }
        }
        if let Some(bytes) = contents.get_mut(HEAD_KEY) {
            let mut head = Head::decode(bytes).expect("a stored head decodes");
            head.id = 0;
            for checkpoint in [&mut head.checkpoint, &mut head.previous]
                .into_iter()
                .flatten()
            {
                if let Some(&len) = manifests.get(&checkpoint.pack.version) {
                    checkpoint.manifest = len;
                }
                checkpoint.pack = canonical(checkpoint.pack);
            }
            *bytes = head.encode();
        }
        contents
    }
    pub fn keys(&self) -> BTreeSet<String> {
        self.inner.lock().unwrap().values.keys().cloned().collect()
    }
    pub fn stored_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .values
            .values()
            .map(Bytes::len)
            .sum()
    }
    pub fn stored_key_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .values
            .keys()
            .map(String::len)
            .sum()
    }
    /// The checkpoints' packs stored here (rather than in a blob store),
    /// by name.
    pub fn packs(&self) -> BTreeSet<String> {
        self.blobs(KV_PREFIX)
    }

    /// The names (`<version>-<nonce>`) of the checkpoints' manifests stored.
    pub fn manifests(&self) -> BTreeSet<String> {
        self.blobs(MANIFEST_PREFIX)
    }

    fn blobs(&self, prefix: &str) -> BTreeSet<String> {
        let inner = self.inner.lock().unwrap();
        (inner.values.keys())
            .filter_map(|key| Some(key.strip_prefix(prefix)?.rsplit_once('/')?.0.to_owned()))
            .collect()
    }

    /// The versions of the tail entries stored.
    pub fn tail_entries(&self) -> BTreeSet<u64> {
        let inner = self.inner.lock().unwrap();
        (inner.values.keys())
            .filter_map(|key| Some(parse_tail_key(key)?.0))
            .collect()
    }

    /// Number of keys of the kind `kind` ([`object_kind`]).
    pub fn count(&self, kind: &str) -> usize {
        self.inner
            .lock()
            .unwrap()
            .values
            .keys()
            .filter(|key| object_kind(key) == kind)
            .count()
    }
}

impl Inner {
    fn apply_durable(&mut self, key: String, value: Option<Bytes>) {
        match value {
            Some(value) => self.durable.insert(key, value),
            None => self.durable.remove(&key),
        };
    }
}

impl KvStore for MemoryKv {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move {
            yield_now().await;
            let mut inner = self.inner.lock().unwrap();
            // A shared buffer, like a network client's receive buffer: no copy.
            let value = inner.values.get(key).cloned();
            inner.stats.gets += 1;
            inner.stats.bytes_read += value.as_ref().map_or(0, |v| v.len() as u64);
            inner.stats.read_keys.insert(key.to_owned());
            let traffic = inner.stats.by_kind.entry(object_kind(key)).or_default();
            traffic.gets += 1;
            traffic.misses += u64::from(value.is_none());
            traffic.bytes_read += value.as_ref().map_or(0, |v| v.len() as u64);
            traffic.get_key_bytes += key.len() as u64;
            Ok(value)
        })
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let bytes = value.len() as u64;
        inner.stats.puts += 1;
        inner.stats.bytes_written += bytes;
        inner.stats.write_keys.insert(key.to_owned());
        let traffic = inner.stats.by_kind.entry(object_kind(key)).or_default();
        traffic.puts += 1;
        traffic.bytes_written += bytes;
        traffic.put_key_bytes += key.len() as u64;
        inner.values.insert(key.to_owned(), value.clone());
        inner.unsynced.push((key.to_owned(), Some(value)));
        Ok(())
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.stats.deletes += 1;
        let removed = inner.values.remove(key);
        inner.stats.delete_keys.insert(key.to_owned());
        let traffic = inner.stats.by_kind.entry(object_kind(key)).or_default();
        traffic.deletes += 1;
        traffic.bytes_deleted += removed.map_or(0, |v| v.len() as u64);
        traffic.delete_key_bytes += key.len() as u64;
        inner.unsynced.push((key.to_owned(), None));
        Ok(())
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            yield_now().await;
            let mut inner = self.inner.lock().unwrap();
            inner.stats.syncs += 1;
            for (key, value) in std::mem::take(&mut inner.unsynced) {
                inner.apply_durable(key, value);
            }
            Ok(())
        })
    }
    fn keys(&self) -> BoxFuture<'_, io::Result<Vec<String>>> {
        Box::pin(async move {
            yield_now().await;
            let mut inner = self.inner.lock().unwrap();
            let keys: Vec<String> = inner.values.keys().cloned().collect();
            inner.stats.lists += 1;
            inner.stats.list_pages += keys.len().div_ceil(1000).max(1) as u64;
            Ok(keys)
        })
    }
}

#[cfg(test)]
#[path = "../tests/unit/memory.rs"]
mod tests;
