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

/// The state of the database `id` at `version`, with one tail entry per
/// version, and its head's bytes.
fn state(id: u64, version: u64) -> (Bytes, Arc<Durable>) {
    let mut head = Head::new(id, 16);
    head.version = version;
    head.parts = vec![1; version as usize];
    (head.encode(), Arc::new(Durable::empty(head)))
}

fn parked(head: &Head) -> ParkedValue {
    ParkedValue {
        value: Box::new(()),
        head: head.clone(),
        weight: 1,
    }
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

/// A state is found by its head's bytes, by any state of the namespace;
/// looking it up changes nothing the writer knows.
#[test]
fn a_state_is_found_by_its_head_and_finding_it_changes_nothing() {
    let cache = fresh_namespace_cache();
    let (one, first) = state(1, 1);
    let (two, _) = state(1, 2);
    assert!(cache.state(&one).is_none());
    cache.keep(&one, first.clone());
    let before = cache.before_read();
    assert!(Arc::ptr_eq(&cache.state(&one).unwrap(), &first));
    assert!(cache.state(&two).is_none(), "another head");
    assert!(cache.state(b"").is_none(), "an absent head is never kept");
    assert_eq!(cache.committed(), None);
    assert!(cache.read_durably(before, false), "lookups start no flush");
}

/// A namespace keeps the newest states of its database: an older one goes,
/// and a database made again drops the old database's.
#[test]
fn a_namespace_keeps_the_newest_states_of_its_database() {
    let cache = fresh_namespace_cache();
    let states: Vec<(Bytes, Arc<Durable>)> = (1..=3).map(|version| state(1, version)).collect();
    for (bytes, durable) in &states {
        cache.keep(bytes, durable.clone());
    }
    let kept = |cache: &NamespaceCache| {
        (states.iter())
            .map(|(bytes, _)| cache.state(bytes).is_some())
            .collect::<Vec<_>>()
    };
    assert_eq!(kept(&cache), [false, true, true]);
    // An older one, kept late (a reader on an old head), does not push out
    // the newer.
    cache.keep(&states[0].0, states[0].1.clone());
    assert_eq!(kept(&cache), [false, true, true]);
    // The namespace is emptied and made again: another database.
    let (again, made) = state(2, 1);
    cache.keep(&again, made);
    assert_eq!(kept(&cache), [false, false, false]);
    assert!(cache.state(&again).is_some());
}

/// What an open reads is durable if it synced alongside, or if no flush ran
/// or was unsettled from just before its read to just after it.
#[test]
fn a_read_is_durable_if_no_flush_ran_or_was_unsettled_meanwhile() {
    let (one, first) = state(1, 1);
    let cache = fresh_namespace_cache();
    let quiet = cache.before_read();
    assert!(cache.read_durably(quiet, false));
    cache.opened(1, quiet);
    // A flush starts during a read: it may have read the flush's head.
    let during = cache.before_read();
    assert!(cache.claim(1));
    assert!(!cache.read_durably(during, false));
    assert!(cache.read_durably(during, true), "unless it synced");
    // While the flush runs.
    let running = cache.before_read();
    assert!(!cache.read_durably(running, false));
    cache.flushed(one, 1, first);
    assert!(cache.read_durably(cache.before_read(), false));
    // A flush with an unknown outcome, until a sync settles it.
    cache.forget();
    let unsettled = cache.before_read();
    assert!(!cache.read_durably(unsettled, false));
    cache.settled(1);
    assert!(cache.read_durably(cache.before_read(), false));
}

/// An open records that a flush may build on the head it read, unless a
/// flush started since: one built on a stale head would overwrite the flush
/// that landed. The state it read is durable all the same, and kept.
#[test]
fn an_open_lets_a_flush_build_on_its_head_unless_one_started_since() {
    let cache = fresh_namespace_cache();
    let (one, first) = state(1, 1);
    let (two, second) = state(1, 2);
    // A slow open reads the head of version 1, durably, and loads.
    let slow = cache.before_read();
    assert!(cache.read_durably(slow, false));
    // Meanwhile another state opens on it, and flushes version 2.
    let fast = cache.before_read();
    cache.opened(1, fast);
    assert!(cache.claim(1));
    cache.flushed(two.clone(), 2, second.clone());
    // The slow open's load ends: its state is durable, its head stale.
    cache.keep(&one, first);
    cache.opened(1, slow);
    assert_eq!(cache.committed(), Some(2));
    assert!(!cache.claim(1), "no flush on the stale head");
    assert!(cache.state(&one).is_some() && cache.state(&two).is_some());
    assert!(cache.claim(2));
}

/// A flush with an unknown outcome keeps the states, which are all as of
/// durable heads, and drops what was parked and the version to build on.
#[test]
fn a_flush_with_an_unknown_outcome_keeps_the_states_and_drops_the_rest() {
    let cache = fresh_namespace_cache();
    let (one, first) = state(1, 1);
    cache.keep(&one, first.clone());
    cache.opened(1, cache.before_read());
    cache.park(parked(&first.head));
    assert!(cache.claim(1));
    cache.forget();
    assert_eq!(cache.committed(), None);
    assert_eq!(cache.unsettled(), Some(1));
    assert!(cache.state(&one).is_some());
    assert!(cache.take_parked(&first.head).is_none());
}

/// A value parked at a head is taken only at that head; at another, it goes.
#[test]
fn a_value_parked_at_another_head_is_dropped() {
    let cache = fresh_namespace_cache();
    let (_, first) = state(1, 1);
    let (_, second) = state(1, 2);
    cache.park(parked(&first.head));
    assert!(cache.take_parked(&first.head).is_some());
    cache.park(parked(&first.head));
    assert!(cache.take_parked(&second.head).is_none());
    assert!(cache.take_parked(&first.head).is_none(), "dropped");
}

/// A tail entry's parts are kept by the database, the version and the
/// number of parts: another database's entry at that version is another.
#[test]
fn tail_entries_are_kept_by_database_and_version() {
    futures::executor::block_on(async {
        let (a, b) = (fresh_namespace_cache(), fresh_namespace_cache());
        let parts = [value(5), value(6)];
        a.keep_entry(1, 3, &parts);
        assert_eq!(a.entry(1, 3, 2).await, Some(parts.to_vec()));
        assert_eq!(a.entry(2, 3, 2).await, None, "another database");
        assert_eq!(a.entry(1, 4, 2).await, None, "another version");
        assert_eq!(a.entry(1, 3, 3).await, None, "another entry");
        assert_eq!(b.entry(1, 3, 2).await, None, "another namespace");
        // Not a range of a pack, however alike its numbers.
        assert_eq!(a.get(at(1, 0)).await, None);
    })
}

/// A flush dropped before it claimed leaves another state's flush running.
#[test]
fn a_flush_that_never_claimed_leaves_the_running_one() {
    let cache = crate::state::simulation::open_cache(crate::state::simulation::cache_config(
        1 << 20,
        4 << 20,
    ));
    let (flushing, dropped) = (cache.namespace("a"), cache.namespace("a"));
    flushing.opened(1, flushing.before_read());
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
        cache.keep_entry(1, 1, std::slice::from_ref(&slice));
        let entry = cache.entry(1, 1, 1).await.expect("kept");
        assert!(!inside(&entry[0]));
        let (_, durable) = state(1, 1);
        cache.keep(&slice, durable);
        assert!(!inside(&cache.owner().states[0].0));
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
    let head = |n: u64| state(n, 1).0;
    for n in 0..64 {
        let (bytes, durable) = state(n, 1);
        let mut durable = Durable::clone(&durable);
        durable
            .records
            .insert("value".into(), Bytes::from(vec![0; 16 << 10]));
        cache
            .namespace(&n.to_string())
            .keep(&bytes, Arc::new(durable));
    }
    let kept = (0..64)
        .filter(|&n| cache.namespace(&n.to_string()).state(&head(n)).is_some())
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
