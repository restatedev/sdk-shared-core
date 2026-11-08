//! The requests a state makes: the head, the tail and the checkpoints'
//! manifests in Restate, and the packs in two tiers: L0 in Restate
//! ([`KvBlobs`], by part), L1 in S3 (a [`BlobStore`], if there is one), all
//! through the process's cache, if the state has one ([`CachedKv`],
//! [`CachedBlobs`]): every get but the head's is served from it, or fetched
//! and kept.
//!
//! **A flush** puts its tail entry and the head, then syncs: one round trip,
//! which Restate fences. A flush that adopts a checkpoint deletes, in the
//! same sync, the tail entries that the checkpoint before covered (those
//! the new one covers stay, for readers that opened before it).
//!
//! **Zombies.** A pack's name is unique (its version and a random nonce), so
//! an attempt that Restate fenced may still put packs, but never one that a
//! head names, and never over another's: they are garbage, as are a crashed
//! attempt's. So is a lost flush's tail entry, whose key has its writer's
//! nonce: the next entry at that version has another, as a writer draws
//! another nonce after a flush whose outcome is unknown.
//!
//! **Garbage** ([`Tiered::collect_garbage`]): a pack older than the head's
//! previous checkpoint that neither of its checkpoints names is deleted, and
//! so is the manifest of a checkpoint older than the previous one, and any
//! tail entry at a version the head's checkpoint covers that the head does
//! not name. Any state may collect, an owner's or a reader's, and so may a
//! zombie: whatever head it holds, every pack a later head names is either
//! named by its head's checkpoints or newer than the previous one, and every
//! tail entry a later head names at a version its checkpoint covers is one
//! it names too.
use crate::state::{
    blobs::{BlobStore, KvBlobs, KV_PART},
    cache::{CachedBlobs, CachedKv, NamespaceCache},
    format::{
        parse_tail_key, Checkpoint, Entry, Head, Manifest, PackId, Tier, HEAD_KEY, L0_PREFIX,
        PACK_PREFIX,
    },
    kv::KvStore,
};
use bytes::Bytes;
use futures::future::try_join_all;
use std::{collections::BTreeSet, io, ops::Range, sync::Arc};

/// The stores of a state: see the module documentation.
pub(crate) struct Tiered {
    pub restate: Arc<dyn KvStore>,
    /// L0: the packs in Restate, read by part.
    l0: KvBlobs,
    /// L1: the packs in S3, where there is one.
    l1: Option<Arc<dyn BlobStore>>,
    manifests: KvBlobs,
    /// The process's cache, if the state has one, which keeps the ranges a
    /// checkpoint puts in L1 ([`Self::put_pack`]).
    cache: Option<NamespaceCache>,
}

impl Tiered {
    /// The stores of a state, read through `cache`, if any (see
    /// [`CachedKv`]).
    pub(super) fn new(
        restate: Arc<dyn KvStore>,
        l1: Option<Arc<dyn BlobStore>>,
        cache: Option<&NamespaceCache>,
    ) -> Self {
        let (restate, l1) = match cache {
            Some(cache) => (
                Arc::new(CachedKv::new(restate, cache.clone())) as Arc<dyn KvStore>,
                l1.map(|l1| Arc::new(CachedBlobs::new(l1, cache.clone())) as Arc<dyn BlobStore>),
            ),
            None => (restate, l1),
        };
        Self {
            l0: KvBlobs::new(restate.clone()),
            l1,
            manifests: KvBlobs::manifests(restate.clone()),
            restate,
            cache: cache.cloned(),
        }
    }

    /// Whether there is an L1 for packs to move to.
    pub(super) fn has_l1(&self) -> bool {
        self.l1.is_some()
    }

    fn store(&self, tier: Tier) -> io::Result<&dyn BlobStore> {
        match tier {
            Tier::L0 => Ok(&self.l0),
            Tier::L1 => (self.l1.as_deref())
                .ok_or_else(|| io::Error::other("a pack in L1, which this state has no store for")),
        }
    }

    pub(super) async fn head(&self) -> io::Result<Option<Bytes>> {
        self.restate.get(HEAD_KEY).await
    }

    /// The tail entries of `head` from `first` on, fetched together.
    pub(super) async fn tail(&self, head: &Head, first: u64) -> io::Result<Vec<Entry>> {
        let first = first.max(head.tail_first);
        let gets = (first..=head.version).map(|version| async move {
            let keys = head.entry_keys(version);
            let parts = try_join_all(keys.into_iter().map(|key| async move {
                (self.restate.get(&key).await?)
                    .ok_or_else(|| io::Error::other(format!("{key} is gone")))
            }))
            .await?;
            Entry::decode(&parts)
        });
        try_join_all(gets).await
    }

    /// Bytes `range` of `pack`.
    pub(super) async fn get(&self, pack: PackId, range: Range<u64>) -> io::Result<Bytes> {
        let key = pack.key();
        (self.store(pack.tier)?.get_range(&key, range).await?).ok_or_else(|| {
            io::Error::other(format!(
                "{key} is gone: the owner collected it since this state opened"
            ))
        })
    }

    /// Bytes of the unit a read of `pack` fetches whole, where there is
    /// one: a part, for a pack in L0.
    pub(super) fn block(&self, pack: PackId) -> Option<u64> {
        (pack.tier == Tier::L0).then_some(KV_PART)
    }

    /// Block `index` of `pack`, in L0 (see [`Self::block`]): shorter at the
    /// pack's end.
    pub(super) async fn get_block(&self, pack: PackId, index: u64) -> io::Result<Bytes> {
        let key = pack.key();
        (self.l0.part(&key, index).await?).ok_or_else(|| {
            io::Error::other(format!(
                "{key} is gone: the owner collected it since this state opened"
            ))
        })
    }

    pub(super) async fn manifest(&self, checkpoint: &Checkpoint) -> io::Result<Manifest> {
        Manifest::decode(&self.encoded_manifest(checkpoint).await?)
    }

    /// The manifest of `checkpoint`, as stored.
    pub(super) async fn encoded_manifest(&self, checkpoint: &Checkpoint) -> io::Result<Bytes> {
        let name = checkpoint.pack.name();
        let bytes = self
            .manifests
            .get_range(&name, 0..checkpoint.manifest)
            .await?;
        bytes.ok_or_else(|| {
            io::Error::other(format!(
                "manifest {name} is gone: the owner collected it since this state opened"
            ))
        })
    }

    /// Queue the manifest of the checkpoint whose pack is `pack`: like the
    /// tail entry, it is durable with the head that names it.
    pub(super) async fn put_manifest(&self, pack: PackId, manifest: Bytes) -> io::Result<()> {
        self.manifests.put(&pack.name(), manifest).await
    }

    /// Put `pack`, whose `body` holds, at `ranges`, what states read of it:
    /// a checkpoint's frames, patches and values. L0's parts are kept as they
    /// are put ([`CachedKv`]); L1 is read by range, so the cache keeps those
    /// ranges, as reads of them would.
    pub(super) async fn put_pack(
        &self,
        pack: PackId,
        body: Bytes,
        ranges: &[Range<u64>],
    ) -> io::Result<()> {
        let key = pack.key();
        self.store(pack.tier)?.put(&key, body.clone()).await?;
        if let (Tier::L1, Some(cache)) = (pack.tier, &self.cache) {
            cache.keep_ranges(&key, &body, ranges);
        }
        Ok(())
    }

    /// Put a flush's tail entry (`parts`) and its head, and delete the tail
    /// entries `deletes`; then sync.
    pub(super) async fn write(
        &self,
        head: &Head,
        parts: Vec<Bytes>,
        deletes: &[String],
    ) -> io::Result<()> {
        for (key, part) in head.entry_keys(head.version).iter().zip(parts) {
            self.restate.put(key, part)?;
        }
        for key in deletes {
            self.restate.delete(key)?;
        }
        self.restate.put(HEAD_KEY, head.encode())?;
        self.restate.sync().await
    }

    /// The packs `head`'s checkpoints need: those their manifests name.
    pub(super) async fn needed(&self, head: &Head) -> io::Result<BTreeSet<PackId>> {
        let checkpoints: Vec<Checkpoint> = [head.checkpoint, head.previous]
            .into_iter()
            .flatten()
            .collect();
        let manifests = try_join_all(
            checkpoints
                .iter()
                .copied()
                .map(|c| async move { self.manifest(&c).await }),
        )
        .await?;
        // A checkpoint's own pack, if it put one, holds what its manifest
        // names.
        Ok(manifests.iter().flat_map(Manifest::packs).collect())
    }

    /// Delete the packs no checkpoint `head` names needs, and the manifests
    /// of checkpoints older than its previous one: see the module
    /// documentation. How many packs.
    ///
    /// L1's packs change only at a promotion, so S3 is listed (a request
    /// billed) only when the head's checkpoint or the previous one is in L1,
    /// or with `all`: a checkpoint into L0 lists Restate alone. What L0's
    /// checkpoints leave dead in L1 waits for the next promotion.
    pub(super) async fn collect_garbage(&self, head: &Head, all: bool) -> io::Result<usize> {
        let Some(previous) = head.previous else {
            return Ok(0);
        };
        let promoted = [head.checkpoint, head.previous]
            .into_iter()
            .flatten()
            .any(|checkpoint| checkpoint.pack.tier == Tier::L1);
        // One listing of Restate's keys finds the manifests and L0's packs;
        // S3's, L1's. A store that cannot list keeps its garbage.
        let in_l1 = async {
            match &self.l1 {
                Some(l1) if all || promoted => unlisted(l1.list(PACK_PREFIX).await),
                _ => Ok(Vec::new()),
            }
        };
        let (keys, in_l1) =
            futures::try_join!(async { unlisted(self.restate.keys().await) }, in_l1)?;
        let in_l0 = self.l0.listed(&keys, L0_PREFIX);
        let older = |id: &PackId| id.version < previous.version();
        let manifests: BTreeSet<String> = (self.manifests.listed(&keys, "").into_iter())
            .filter(|name| PackId::manifest_version(name).is_some_and(|v| v < previous.version()))
            .collect();
        let older: Vec<(PackId, String)> = (in_l0.into_iter().chain(in_l1))
            .filter_map(|key| Some((PackId::parse(&key)?, key)))
            .filter(|(id, _)| older(id))
            .collect();
        let garbage: Vec<(PackId, String)> = match older.is_empty() {
            true => Vec::new(),
            false => {
                let keep = self.needed(head).await?;
                (older.into_iter())
                    .filter(|(id, _)| !keep.contains(id))
                    .collect()
            }
        };
        // Restate's deletes are queued, and stored by one sync.
        let mut queued = (self.manifests).queue_delete(&keys, |name| manifests.contains(name))?;
        // Tail entries at versions the checkpoint covers that the head does
        // not name: a lost flush's, whose nonce is not the head's, and, for
        // entries written before nonces, which a retry overwrote, a crashed
        // flush's parts past those its retry put. Only there: a head that is
        // lost (a reader's, opened beside the flush) has a last entry that
        // the next writer's at that version replaces, but its checkpoint
        // holds a state every later head descends from.
        for key in &keys {
            let Some(tail) = parse_tail_key(key) else {
                continue;
            };
            let named = head.nonce_of(tail.version) == Some(tail.nonce)
                && tail.part < head.parts_of(tail.version);
            if tail.version <= head.checkpointed() && !named {
                self.restate.delete(key)?;
                queued = true;
            }
        }
        let doomed: BTreeSet<&str> = (garbage.iter())
            .filter(|(id, _)| id.tier == Tier::L0)
            .map(|(_, key)| key.as_str())
            .collect();
        queued |= self.l0.queue_delete(&keys, |key| doomed.contains(key))?;
        let in_l1: Vec<String> = (garbage.iter())
            .filter(|(id, _)| id.tier == Tier::L1)
            .map(|(_, key)| key.clone())
            .collect();
        let delete_in_l1 = async {
            match &self.l1 {
                Some(l1) if !in_l1.is_empty() => l1.delete(&in_l1).await,
                _ => Ok(()),
            }
        };
        let sync = async {
            match queued {
                true => self.restate.sync().await,
                false => Ok(()),
            }
        };
        futures::try_join!(sync, delete_in_l1)?;
        Ok(garbage.len())
    }
}

/// A listing; nothing, from a store that cannot list.
fn unlisted(listed: io::Result<Vec<String>>) -> io::Result<Vec<String>> {
    match listed {
        Err(error) if error.kind() == io::ErrorKind::Unsupported => Ok(Vec::new()),
        listed => listed,
    }
}
