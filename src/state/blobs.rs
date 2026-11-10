//! Where checkpoints' packs are: L0 in Restate's state itself ([`KvBlobs`]),
//! and L1 in a [`BlobStore`], S3 through `ObjectStoreBlobs` (the feature
//! `object_store`), if the state has one.
//!
//! ```rust,ignore
//! let l1 = ObjectStoreL1::from_env()?; // the feature `object_store`, once per process
//! let packs = l1.blobs("objects/42"); // or ObjectStoreBlobs::new(store, prefix)
//! let state = State::open(restate).checkpoints(Arc::new(packs)).await?;
//! ```
//!
//! A pack is put once, under a name no other put uses, read in ranges, and
//! deleted once no checkpoint needs it (by a later checkpoint's flush):
//! a store needs no conditional put, and no fencing.
use crate::state::kv::KvStore;
use bytes::{Bytes, BytesMut};
use futures::future::{try_join_all, BoxFuture};
use std::{collections::BTreeSet, io, ops::Range, sync::Arc};

/// A store of blobs: S3, say, through the feature `object_store`
/// (`ObjectStoreBlobs`). A blob is durable once its put returns.
pub trait BlobStore: Send + Sync {
    /// Bytes `range` of the value of `key`; `None` if absent.
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> BoxFuture<'a, io::Result<Option<Bytes>>>;
    /// Store `value` at `key`.
    fn put<'a>(&'a self, key: &'a str, value: Bytes) -> BoxFuture<'a, io::Result<()>>;
    /// Remove `keys`; a missing key succeeds.
    fn delete<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, io::Result<()>>;
    /// Every key under `prefix`.
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<String>>>;
}

/// Where [`KvBlobs::new`] keeps its blobs' parts: L0's packs, in Restate.
pub(crate) const KV_PREFIX: &str = "v22/pack/";
/// Where checkpoints' manifests are kept, in Restate wherever the packs
/// are, in parts of 64 KiB.
pub(crate) const MANIFEST_PREFIX: &str = "v22/manifest/";
/// Bytes of a part of a blob in [`KvBlobs`]: a range read gets the parts it
/// overlaps.
pub(crate) const KV_PART: u64 = 64 << 10;

/// Blobs in a [`KvStore`] (Restate's state, beside the head and the tail),
/// each in parts of 64 KiB: `<prefix><key>/<part>`.
///
/// A put only queues its parts: they are durable with the store's next sync.
/// That is enough for a checkpoint, as a [`KvStore`] keeps its writes in
/// order: the head naming the pack, put after it, is never durable without it.
pub(crate) struct KvBlobs {
    store: Arc<dyn KvStore>,
    prefix: &'static str,
}

impl KvBlobs {
    /// L0's packs, under [`KV_PREFIX`].
    pub(crate) fn new(store: Arc<dyn KvStore>) -> Self {
        Self {
            store,
            prefix: KV_PREFIX,
        }
    }

    /// Manifests, under [`MANIFEST_PREFIX`].
    pub(crate) fn manifests(store: Arc<dyn KvStore>) -> Self {
        Self {
            store,
            prefix: MANIFEST_PREFIX,
        }
    }

    fn part_key(&self, key: &str, part: u64) -> String {
        format!("{}{key}/{part}", self.prefix)
    }

    /// Part `part` of the blob `key`: its bytes `part × 64 KiB` onward, up
    /// to 64 KiB; `None` if absent.
    pub(crate) async fn part(&self, key: &str, part: u64) -> io::Result<Option<Bytes>> {
        self.store.get(&self.part_key(key, part)).await
    }

    /// The blob's key, of a part's key.
    fn blob<'k>(&self, part: &'k str) -> Option<&'k str> {
        Some(part.strip_prefix(self.prefix)?.rsplit_once('/')?.0)
    }

    /// The keys of the blobs under `prefix` whose parts are among `keys`, a
    /// listing of the store.
    pub(crate) fn listed(&self, keys: &[String], prefix: &str) -> BTreeSet<String> {
        (keys.iter())
            .filter_map(|part| self.blob(part))
            .filter(|blob| blob.starts_with(prefix))
            .map(str::to_owned)
            .collect()
    }

    /// Queue the deletes of the parts among `keys`, a listing of the store,
    /// of the blobs `doomed` picks: durable with the store's next sync.
    /// Whether it queued any.
    pub(crate) fn queue_delete(
        &self,
        keys: &[String],
        doomed: impl Fn(&str) -> bool,
    ) -> io::Result<bool> {
        let mut queued = false;
        for part in keys {
            if self.blob(part).is_some_and(&doomed) {
                self.store.delete(part)?;
                queued = true;
            }
        }
        Ok(queued)
    }
}

impl BlobStore for KvBlobs {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move {
            if range.is_empty() {
                return Ok(Some(Bytes::new()));
            }
            let parts = range.start / KV_PART..range.end.div_ceil(KV_PART);
            let first = parts.start;
            let got = try_join_all(parts.map(|part| {
                let key = self.part_key(key, part);
                async move { self.store.get(&key).await }
            }))
            .await?;
            let mut out = BytesMut::new();
            for part in got {
                match part {
                    Some(part) => out.extend_from_slice(&part),
                    None => return Ok(None),
                }
            }
            let start = (range.start - first * KV_PART) as usize;
            let end = start + (range.end - range.start) as usize;
            if end > out.len() {
                return Err(io::Error::other(format!("{key} is shorter than {range:?}")));
            }
            Ok(Some(out.freeze().slice(start..end)))
        })
    }

    fn put<'a>(&'a self, key: &'a str, value: Bytes) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            for (part, at) in (0..value.len() as u64)
                .step_by(KV_PART as usize)
                .enumerate()
            {
                let end = (at + KV_PART).min(value.len() as u64);
                let slice = value.slice(at as usize..end as usize);
                self.store.put(&self.part_key(key, part as u64), slice)?;
            }
            Ok(())
        })
    }

    /// One listing and one sync for all of `keys`.
    fn delete<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(());
            }
            let blobs: BTreeSet<&str> = keys.iter().map(String::as_str).collect();
            self.queue_delete(&self.store.keys().await?, |blob| blobs.contains(blob))?;
            self.store.sync().await
        })
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<String>>> {
        Box::pin(async move {
            let keys = self.store.keys().await?;
            Ok(self.listed(&keys, prefix).into_iter().collect())
        })
    }
}

#[cfg(feature = "object_store")]
pub use object::ObjectStoreBlobs;

#[cfg(feature = "object_store")]
mod object {
    use super::BlobStore;
    use bytes::Bytes;
    use futures::{
        future::{try_join_all, BoxFuture},
        StreamExt, TryStreamExt,
    };
    use object_store::{path::Path, Error, ObjectStore, ObjectStoreExt};
    use std::{io, ops::Range, sync::Arc};

    /// Bytes of a part of an upload. A blob larger than this, a promotion's
    /// pack carrying what it compacted, say, goes up in parts of this size
    /// (S3 takes parts of 5 MiB to 5 GiB), a few at a time, rather than in
    /// one long request.
    const UPLOAD_PART: usize = 8 << 20;
    /// Parts of one upload in flight at once.
    const PARTS_IN_FLIGHT: usize = 4;

    /// A [`BlobStore`] in an object store, under a prefix of its own (one per
    /// namespace: the object's key, say): S3 through the feature
    /// `object_store_aws`. `ObjectStoreL1::blobs` gives each namespace its
    /// own prefix; one shared would let a namespace's garbage collection
    /// delete another's packs.
    pub struct ObjectStoreBlobs {
        store: Arc<dyn ObjectStore>,
        prefix: String,
    }

    impl ObjectStoreBlobs {
        /// The blobs of `store` under `prefix`: `objects/42`, say.
        pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
            Self {
                store,
                prefix: prefix.into(),
            }
        }

        fn path(&self, key: &str) -> Path {
            Path::from(format!("{}/{key}", self.prefix))
        }

        async fn delete_one(&self, key: &str) -> io::Result<()> {
            match self.store.delete(&self.path(key)).await {
                Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
                Err(error) => Err(error.into()),
            }
        }
    }

    impl BlobStore for ObjectStoreBlobs {
        fn get_range<'a>(
            &'a self,
            key: &'a str,
            range: Range<u64>,
        ) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
            Box::pin(async move {
                match self.store.get_range(&self.path(key), range).await {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(Error::NotFound { .. }) => Ok(None),
                    Err(error) => Err(error.into()),
                }
            })
        }

        /// One request up to `UPLOAD_PART` bytes; a multipart upload past it.
        fn put<'a>(&'a self, key: &'a str, value: Bytes) -> BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                let path = self.path(key);
                if value.len() <= UPLOAD_PART {
                    self.store.put(&path, value.into()).await?;
                    return Ok(());
                }
                let mut upload = self.store.put_multipart(&path).await?;
                let parts: Vec<_> = (0..value.len())
                    .step_by(UPLOAD_PART)
                    .map(|at| value.slice(at..(at + UPLOAD_PART).min(value.len())))
                    .map(|part| upload.put_part(part.into()))
                    .collect();
                let uploaded = futures::stream::iter(parts)
                    .buffered(PARTS_IN_FLIGHT)
                    .try_collect::<Vec<()>>()
                    .await;
                let completed = match uploaded {
                    Ok(_) => upload.complete().await.map(drop),
                    Err(error) => Err(error),
                };
                if let Err(error) = completed {
                    // Parts uploaded are billed until aborted; an abort that
                    // fails leaves them to the bucket's lifecycle rule.
                    let _ = upload.abort().await;
                    return Err(error.into());
                }
                Ok(())
            })
        }

        fn delete<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                try_join_all(keys.iter().map(|key| self.delete_one(key))).await?;
                Ok(())
            })
        }

        fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<String>>> {
            Box::pin(async move {
                let ours = format!("{}/", self.prefix);
                let listed: Vec<_> = (self.store.list(Some(&self.path(prefix))))
                    .map_ok(|meta| meta.location.as_ref().to_owned())
                    .try_collect()
                    .await?;
                Ok(listed
                    .into_iter()
                    .filter_map(|path| Some(path.strip_prefix(&ours)?.to_owned()))
                    .filter(|key| key.starts_with(prefix))
                    .collect())
            })
        }
    }
}

#[cfg(all(test, feature = "object_store"))]
mod tests {
    use super::{BlobStore, ObjectStoreBlobs};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    /// A blob past a part's size goes up in parts, and reads back whole and
    /// in ranges across their boundaries, as one that fits in a request.
    #[test]
    fn a_large_blob_is_uploaded_in_parts_and_reads_back() {
        futures::executor::block_on(async {
            let blobs = ObjectStoreBlobs::new(Arc::new(InMemory::new()), "objects/42");
            for (key, len) in [("small", 1000usize), ("large", (20 << 20) + 7)] {
                let value: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
                blobs.put(key, Bytes::from(value.clone())).await.unwrap();
                let whole = blobs.get_range(key, 0..len as u64).await.unwrap();
                assert_eq!(whole.as_deref(), Some(&value[..]), "{key}");
                // Across the first part's end, where there is one.
                let range = match len > 8 << 20 {
                    true => (8 << 20) - 5..(8 << 20) + 5,
                    false => 10..20,
                };
                let read = (blobs.get_range(key, range.start as u64..range.end as u64))
                    .await
                    .unwrap();
                assert_eq!(read.as_deref(), Some(&value[range]), "{key}");
            }
            assert_eq!(blobs.list("").await.unwrap().len(), 2);
        })
    }
}
