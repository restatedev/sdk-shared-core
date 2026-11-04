use super::*;
use crate::state::{
    format::{Head, PackId, Tier},
    simulation::{fresh_namespace_cache, shared_cache},
};

fn value(n: usize) -> Bytes {
    Bytes::from(vec![n as u8; n])
}

/// The range at `offset` of the pack of version 1 of the database `id`.
fn at(id: u64, offset: u64) -> Location {
    let pack = PackId {
        version: 1,
        nonce: id,
        tier: Tier::L0,
    };
    Location {
        pack,
        offset,
        len: 4,
    }
}

fn durable() -> Arc<Durable> {
    Arc::new(Durable::empty(Head::new(1, 16)))
}

#[test]
fn namespaces_never_share_ranges() {
    futures::executor::block_on(async {
        let (a, b) = (fresh_namespace_cache(), fresh_namespace_cache());
        a.insert(at(0, 1), value(4));
        assert_eq!(a.get(at(0, 1)).await, Some(value(4)));
        assert_eq!(b.get(at(0, 1)).await, None, "same range, other namespace");
        assert_eq!(a.get(at(9, 1)).await, None, "same offset, other pack");
        // A handle by the same key is the same namespace.
        shared_cache()
            .namespace("by-key")
            .insert(at(0, 7), value(7));
        assert_eq!(
            shared_cache().namespace("by-key").get(at(0, 7)).await,
            Some(value(7))
        );
    })
}

/// What an open does: validate the head it read, then record it, and, if
/// it was not known, that a flush may build on `version`. Whether it was
/// known.
fn open(cache: &NamespaceCache, head: Option<Bytes>, version: u64) -> bool {
    let validated = cache.validate(head.as_deref());
    let committed = (!validated.known).then_some(version);
    cache.record(&validated, head, committed, durable());
    validated.known
}

/// The same head is known; another one forgets this owner's flush, and
/// keeps the ranges, which never change.
#[test]
fn another_head_forgets_the_flush_not_the_ranges() {
    futures::executor::block_on(async {
        let head = |n: u8| Bytes::from(vec![b'S', b'H', b'2', b'0', n]);
        let cache = fresh_namespace_cache();
        assert!(!open(&cache, None, 0), "a fresh owner knows nothing");
        assert!(open(&cache, None, 0), "an absent head, as recorded");
        assert!(!open(&cache, Some(head(1)), 3), "not after another");
        cache.insert(at(0, 1), value(4));
        assert!(cache.validate(Some(&head(1))).known);
        assert_eq!(cache.committed(), Some(3));

        // Another writer flushed: the ranges stay.
        assert!(!cache.validate(Some(&head(2))).known);
        assert_eq!(cache.committed(), None);
        assert_eq!(cache.get(at(0, 1)).await, Some(value(4)));

        // A flush with an unknown outcome forgets the head, and makes the
        // next open sync.
        open(&cache, Some(head(3)), 5);
        cache.forget();
        assert!(!cache.validate(Some(&head(3))).known);
        assert_eq!(cache.unsettled(), Some(1));
        cache.settled(1);
        assert_eq!(cache.unsettled(), None);

        // A flush runs: an open syncs, and does not let another flush build
        // on the head it reads, until the flush ends.
        open(&cache, Some(head(4)), 6);
        assert!(cache.claim(6));
        assert_eq!(cache.unsettled(), Some(1));
        open(&cache, Some(head(5)), 7);
        assert!(!cache.claim(7));
        cache.flushed(head(5), 7, durable());
        assert_eq!(cache.unsettled(), None);
        assert!(cache.claim(7));
    })
}

/// An open whose load overlapped another state's flush records nothing:
/// the head it read is stale, and a flush on top of it would overwrite the
/// one that landed.
#[test]
fn a_stale_open_records_nothing() {
    let head = |n: u8| Bytes::from(vec![b'S', b'H', b'2', b'0', n]);
    let cache = fresh_namespace_cache();
    // A slow open reads the head of version 1, and loads.
    let slow = cache.validate(Some(&head(1)));
    // Meanwhile another state opens on it, and flushes version 2.
    open(&cache, Some(head(1)), 1);
    assert!(cache.claim(1));
    cache.flushed(head(2), 2, durable());
    // The slow open's load ends.
    cache.record(&slow, Some(head(1)), Some(1), durable());
    assert_eq!(cache.committed(), Some(2));
    assert!(!cache.claim(1), "no flush on the stale head");
    assert!(cache.validate(Some(&head(2))).known);
}

/// A flush dropped before it claimed leaves another state's flush running.
#[test]
fn a_flush_that_never_claimed_leaves_the_running_one() {
    let cache = crate::state::simulation::open_cache(crate::state::simulation::cache_config(
        1 << 20,
        4 << 20,
    ));
    let (flushing, dropped) = (cache.namespace("a"), cache.namespace("a"));
    open(&flushing, None, 1);
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
    use crate::state::{kv::KvStore, simulation::*};
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
        cache.insert(at(0, 1), slice.clone());
        let cached = cache.get(at(0, 1)).await.expect("cached");
        assert_eq!(cached, slice);
        assert!(!inside(&cached));
        cache.record(&cache.validate(None), Some(slice.clone()), None, durable());
        assert!(!inside(cache.owner().head.as_ref().unwrap()));
    })
}

/// A namespace evicted with a flush whose outcome is unknown would forget
/// that its next open must sync: it stays until a sync settles it.
#[test]
fn an_unsettled_namespace_is_never_evicted() {
    let cache = crate::state::simulation::open_cache(PageCacheConfig {
        namespace_bytes: 1024,
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

/// Namespaces are kept by the bytes of their states: of many, a budget
/// of a few keeps few.
#[test]
fn namespaces_are_kept_by_the_bytes_of_their_states() {
    let cache = crate::state::simulation::open_cache(PageCacheConfig {
        namespace_bytes: 512 << 10,
        ..crate::state::simulation::cache_config(1 << 20, 4 << 20)
    });
    for n in 0..64 {
        let mut state = Durable::empty(Head::new(n, 16));
        state
            .records
            .insert("value".into(), Bytes::from(vec![0; 16 << 10]));
        let namespace = cache.namespace(&n.to_string());
        let validated = namespace.validate(None);
        namespace.record(&validated, None, Some(1), Arc::new(state));
    }
    let kept = (0..64)
        .filter(|n| cache.namespace(&n.to_string()).durable().is_some())
        .count();
    assert!((8..48).contains(&kept), "{kept} of 64 kept");
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
