use super::*;
use crate::state::{
    format::{tail_key, Head, HEAD_KEY},
    simulation::{fresh_namespace_cache, shared_cache, MemoryBlobs, MemoryKv},
};

fn value(n: usize) -> Bytes {
    Bytes::from(vec![n as u8; n])
}

/// The head of version `version` of the database 1.
fn head(version: u64) -> Head {
    let mut head = Head::new(1, 16);
    head.version = version;
    head
}

fn parked(head: &Head) -> ParkedValue {
    ParkedValue {
        value: Box::new(()),
        head: head.clone(),
        weight: 1,
    }
}

#[test]
fn namespaces_never_share_values() {
    futures::executor::block_on(async {
        let (a, b) = (fresh_namespace_cache(), fresh_namespace_cache());
        a.keep_value("key", &value(4));
        assert_eq!(a.value("key").await, Some(value(4)));
        assert_eq!(b.value("key").await, None, "same key, other namespace");
        assert_eq!(a.value("other").await, None);
        // A range is a value of its own, by its extent.
        a.keep_range("blob", 0..4, &value(4));
        assert_eq!(a.range("blob", 0..4).await, Some(value(4)));
        assert_eq!(a.range("blob", 0..5).await, None);
        assert_eq!(a.value("blob").await, None, "the range, not the blob");
        // A handle by the same key is the same namespace.
        shared_cache()
            .namespace("by-key")
            .keep_value("key", &value(7));
        assert_eq!(
            shared_cache().namespace("by-key").value("key").await,
            Some(value(7))
        );
    })
}

/// The cached store reads the head from the store at every get, and the
/// value of every other key once: from the store, or as put.
#[test]
fn the_cached_store_reads_the_head_every_time_and_the_rest_once() {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let store = CachedKv::new(Arc::new(memory.clone()), fresh_namespace_cache());
        let entry = tail_key(3, 0xabc, 0);
        let manifest = "v22/manifest/0000000000000002-00000000000000aa/0";
        store.put(HEAD_KEY, value(1))?;
        store.put(&entry, value(2))?;
        // Put beside the cache, as another process would.
        memory.put(manifest, value(3))?;
        memory.reset_stats();
        for _ in 0..2 {
            assert_eq!(store.get(&entry).await?, Some(value(2)));
            assert_eq!(store.get(manifest).await?, Some(value(3)));
            assert_eq!(store.get(HEAD_KEY).await?, Some(value(1)));
        }
        let stats = memory.stats();
        assert_eq!(stats.kind("head").gets, 2, "the head, at every get");
        assert_eq!(stats.kind("tail").gets, 0, "the entry, as put");
        assert_eq!(stats.kind("manifest").gets, 1, "the manifest, once");
        // Another flush changes the head: the next get reads it.
        memory.put(HEAD_KEY, value(9))?;
        assert_eq!(store.get(HEAD_KEY).await?, Some(value(9)));
        // A key with no value is asked for again.
        memory.reset_stats();
        assert_eq!(store.get(&tail_key(4, 0xabc, 0)).await?, None);
        assert_eq!(store.get(&tail_key(4, 0xabc, 0)).await?, None);
        assert_eq!(memory.stats().kind("tail").gets, 2);
        Ok::<_, io::Error>(())
    })
    .unwrap()
}

/// A tail entry written before entries had nonces has a key a retry may put
/// again: it is read from the store at every get, as the head is.
#[test]
fn the_cached_store_reads_a_tail_entry_without_a_nonce_every_time() {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let store = CachedKv::new(Arc::new(memory.clone()), fresh_namespace_cache());
        let old = tail_key(3, 0, 0);
        store.put(&old, value(2))?;
        memory.put(&old, value(5))?;
        assert_eq!(store.get(&old).await?, Some(value(5)), "never kept");
        memory.reset_stats();
        store.get(&old).await?;
        assert_eq!(memory.stats().kind("tail").gets, 1);
        Ok::<_, io::Error>(())
    })
    .unwrap()
}

/// A put the store refuses keeps nothing.
#[test]
fn a_put_the_store_refuses_keeps_nothing() {
    struct Refusing;
    impl KvStore for Refusing {
        fn get<'a>(&'a self, _: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
            Box::pin(async { Ok(None) })
        }
        fn put(&self, _: &str, _: Bytes) -> io::Result<()> {
            Err(io::Error::other("refused"))
        }
        fn delete(&self, _: &str) -> io::Result<()> {
            Ok(())
        }
        fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }
    futures::executor::block_on(async {
        let cache = fresh_namespace_cache();
        let store = CachedKv::new(Arc::new(Refusing), cache.clone());
        let entry = tail_key(3, 0xabc, 0);
        assert!(store.put(&entry, value(2)).is_err());
        assert_eq!(cache.value(&entry).await, None);
    })
}

/// The cached blob store reads a range once, up to the largest kept; a run
/// a compaction moves is read from the store.
#[test]
fn the_cached_blobs_read_a_range_once() {
    futures::executor::block_on(async {
        let memory = MemoryBlobs::default();
        let blobs = CachedBlobs::new(Arc::new(memory.clone()), fresh_namespace_cache());
        let body = Bytes::from(vec![7; (MAX_KEPT_RANGE + 10) as usize]);
        memory.put("l1/pack", body.clone()).await?;
        let gets = memory.gets();
        for _ in 0..2 {
            let range = blobs.get_range("l1/pack", 10..20).await?;
            assert_eq!(range, Some(body.slice(10..20)));
        }
        assert_eq!(memory.gets() - gets, 1);
        let whole = 0..MAX_KEPT_RANGE + 10;
        for _ in 0..2 {
            let range = blobs.get_range("l1/pack", whole.clone()).await?;
            assert_eq!(range, Some(body.clone()));
        }
        assert_eq!(memory.gets() - gets, 3, "too large to keep");
        Ok::<_, io::Error>(())
    })
    .unwrap()
}

/// A flush builds only on the writer's last durable flush: one an owner's
/// open recorded, or one that landed.
#[test]
fn a_flush_builds_only_on_the_last_durable_flush() {
    let cache = fresh_namespace_cache();
    assert!(!cache.claim(0), "nothing recorded yet");
    cache.opened(1, cache.epoch());
    assert!(cache.claim(1));
    assert!(!cache.claim(1), "another runs");
    assert_eq!(cache.unsettled(), Some(0), "an open syncs while it runs");
    cache.flushed(2);
    assert_eq!(cache.unsettled(), None);
    assert!(!cache.claim(1), "another state flushed since");
    assert!(cache.claim(2));
}

/// An open records that a flush may build on the head it read, unless a
/// flush started since it read it: one built on a stale head would
/// overwrite the flush that landed.
#[test]
fn an_open_records_its_head_unless_a_flush_started_since() {
    let cache = fresh_namespace_cache();
    // A slow open reads the head of version 1, and loads.
    let slow = cache.epoch();
    // Meanwhile another state opens on it, and flushes version 2.
    cache.opened(1, cache.epoch());
    assert!(cache.claim(1));
    cache.flushed(2);
    // The slow open's load ends.
    cache.opened(1, slow);
    assert_eq!(cache.committed(), Some(2));
    assert!(!cache.claim(1), "no flush on the stale head");
}

/// A flush with an unknown outcome drops the version to build on and what
/// was parked, and makes the next owner's open sync.
#[test]
fn a_flush_with_an_unknown_outcome_drops_what_it_may_have_changed() {
    let cache = fresh_namespace_cache();
    cache.opened(1, cache.epoch());
    cache.park(parked(&head(1)));
    assert!(cache.claim(1));
    cache.forget();
    assert_eq!(cache.committed(), None);
    assert_eq!(cache.unsettled(), Some(1));
    assert!(cache.take_parked(&head(1)).is_none());
    cache.settled(1);
    assert_eq!(cache.unsettled(), None);
}

/// The writer names its flushes' tail entries with one nonce, so that the
/// head names them in one run; and draws another after a flush whose
/// outcome is unknown, whose entry may be stored, and at an open of a head
/// older than its last durable flush: another database's, at versions whose
/// keys it put in the old one.
#[test]
fn the_writer_draws_another_nonce_where_a_key_could_come_again() {
    let cache = fresh_namespace_cache();
    let first = cache.nonce();
    assert_ne!(first, 0, "nonce 0 names the entries of before nonces");
    cache.opened(1, cache.epoch());
    assert!(cache.claim(1));
    cache.flushed(2);
    cache.opened(2, cache.epoch());
    assert_eq!(cache.nonce(), first, "its own head");
    cache.opened(5, cache.epoch());
    assert_eq!(cache.nonce(), first, "a newer head: another writer flushed");
    assert!(cache.claim(5));
    cache.forget();
    let second = cache.nonce();
    assert_ne!(second, first, "the lost flush's entry may be stored");
    cache.settled(1);
    cache.opened(5, cache.epoch());
    assert_eq!(cache.nonce(), second);
    cache.opened(0, cache.epoch());
    assert_ne!(
        cache.nonce(),
        second,
        "the state was cleared, and made again"
    );
}

/// A value parked at a head is taken only at that head; at another, it
/// goes. An absent head (an empty database) is the same state whatever
/// identity each open drew for it.
#[test]
fn a_value_parked_at_another_head_is_dropped() {
    let cache = fresh_namespace_cache();
    cache.park(parked(&head(1)));
    assert!(cache.take_parked(&head(1)).is_some());
    cache.park(parked(&head(1)));
    assert!(cache.take_parked(&head(2)).is_none());
    assert!(cache.take_parked(&head(1)).is_none(), "dropped");
    cache.park(parked(&Head::new(5, 16)));
    assert!(cache.take_parked(&Head::new(6, 16)).is_some());
}

/// A flush dropped before it claimed leaves another state's flush running.
#[test]
fn a_flush_that_never_claimed_leaves_the_running_one() {
    let cache = crate::state::simulation::open_cache(crate::state::simulation::cache_config(
        1 << 20,
        4 << 20,
    ));
    let (flushing, dropped) = (cache.namespace("a"), cache.namespace("a"));
    flushing.opened(1, flushing.epoch());
    assert!(flushing.claim(1));
    dropped.forget();
    dropped.settled(1);
    assert_eq!(flushing.unsettled(), Some(1), "an open still syncs");
    flushing.forget();
    flushing.settled(2);
    assert_eq!(flushing.unsettled(), None);
}

/// Idle state that refers back to its namespace's handle (as a parked SQL
/// engine does); dropping the last `PageCache` still frees everything.
#[test]
fn dropping_the_cache_frees_parked_values() -> crate::state::Result<()> {
    use crate::state::simulation::*;
    futures::executor::block_on(async {
        let cache = open_cache(cache_config(1 << 20, 16 << 20));
        let store: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let namespace = cache.namespace("parked");
        let state = crate::state::State::open(store)
            .cached(&cache, "parked")
            .await?;
        let freed = Arc::new(());
        let value = (namespace, freed.clone());
        assert!(state.park(value, 1).is_ok());
        assert_eq!(cache.parked.lock().unwrap().parked.len(), 1);
        drop(state);
        let shared = Arc::downgrade(&cache.shared);
        drop(cache);
        assert!(shared.upgrade().is_none());
        assert_eq!(Arc::strong_count(&freed), 1);
        Ok(())
    })
}

#[test]
fn values_read_as_slices_of_a_larger_buffer_are_kept_alone() {
    futures::executor::block_on(async {
        let cache = fresh_namespace_cache();
        // A receive buffer holding much more than the one value.
        let buffer = Bytes::from(vec![9; 1 << 20]);
        let inside = |value: &Bytes| buffer.as_ptr_range().contains(&value.as_ptr());
        let slice = buffer.slice(4096..8192);
        cache.keep_value("key", &slice);
        let kept = cache.value("key").await.expect("kept");
        assert_eq!(kept, slice);
        assert!(!inside(&kept));
        cache.keep_range("blob", 0..4096, &slice);
        assert!(!inside(&cache.range("blob", 0..4096).await.expect("kept")));
    })
}

/// A namespace evicted with a flush whose outcome is unknown would forget
/// that its next open must sync: it stays until a sync settles it.
#[test]
fn an_unsettled_namespace_is_never_evicted() {
    let cache = crate::state::simulation::open_cache(PageCacheConfig {
        namespace_bytes: 4 * NAMESPACE_WEIGHT,
        ..crate::state::simulation::cache_config(1 << 20, 4 << 20)
    });
    cache.namespace("failed").forget();
    let others = |cache: &PageCache| (0..64).for_each(|n| drop(cache.namespace(&n.to_string())));
    others(&cache);
    let failed = cache.namespace("failed");
    assert_eq!(failed.unsettled(), Some(1));
    failed.settled(1);
    drop(failed);
    others(&cache);
    assert_eq!(cache.namespace("failed").unsettled(), None);
}

#[test]
fn sizes_are_bytes_or_binary_units() {
    use crate::state::cache::parse_size;
    assert_eq!(parse_size("4096"), Some(4096));
    assert_eq!(parse_size("64KiB"), Some(64 << 10));
    assert_eq!(parse_size("1 MiB"), Some(1 << 20));
    assert_eq!(parse_size("2GiB"), Some(2 << 30));
    assert_eq!(parse_size("1MB"), None);
    assert_eq!(parse_size("lots"), None);
    assert_eq!(parse_size(""), None);
}
