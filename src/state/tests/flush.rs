//! Mutations and their flushes: a mutation is read once flushed, and
//! discarded when dropped; a write refuses, before the mutation changes, what
//! no flush could store; a flush is one at a time, and, cancelled midway,
//! leaves an unknown outcome that no other state of the owner flushes on top
//! of.
use crate::state::{kv::KvStore, simulation::*, Page, Result, State, MAX_RECORD_BYTES};
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use std::{io, sync::Arc};

fn page(n: u8) -> Page {
    vec![n; 4096].into()
}

/// A store whose sync never completes: a flush waits on it forever.
struct Stalled(Arc<MemoryKv>);

impl KvStore for Stalled {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        self.0.get(key)
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.0.put(key, value)
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.0.delete(key)
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(futures::future::pending())
    }
}

/// A store that cannot list its keys (the default [`KvStore::keys`]).
struct Unlisted(Arc<MemoryKv>);

impl KvStore for Unlisted {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        self.0.get(key)
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.0.put(key, value)
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.0.delete(key)
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        self.0.sync()
    }
}

/// The state reads as last flushed, whatever a mutation holds, until that mutation
/// flushes; a second mutation flushes on top of the first.
#[test]
fn a_mutation_is_read_once_flushed() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        let db = state.file();
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "1".into())?;
        mutation.write(0, Some(page(1)))?;
        assert!(
            state.record("outbox").is_empty() && db.len() == 0,
            "not flushed yet"
        );
        assert_eq!(db.read(0).await?, None);
        mutation.flush().await?;
        assert_eq!(&state.record("outbox")[..], b"1");
        assert_eq!(db.read(0).await?, Some(page(1)));
        // A second mutation, on top.
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "2".into())?;
        mutation.write(1, Some(page(2)))?;
        assert_eq!((&state.record("outbox")[..], db.len()), (&b"1"[..], 4096));
        mutation.flush().await?;
        assert_eq!((&state.record("outbox")[..], db.len()), (&b"2"[..], 8192));
        assert_eq!(db.read(0).await?, Some(page(1)));
        assert_eq!(db.read(1).await?, Some(page(2)));
        assert_eq!(state.version(), 2);
        let reopened = State::open(store).await?;
        assert_eq!(&reopened.record("outbox")[..], b"2");
        assert_eq!(reopened.file().read(1).await?, Some(page(2)));
        Ok(())
    })
}

/// A mutation dropped discards its writes: nothing is written, the state reads
/// as last flushed, and the next mutation flushes.
#[test]
fn a_mutation_dropped_discards_its_writes() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "1".into())?;
        mutation.flush().await?;
        let keys = store.keys();
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "2".into())?;
        mutation.write(0, Some(page(1)))?;
        assert!(!mutation.is_empty());
        drop(mutation);
        assert_eq!(&state.record("outbox")[..], b"1");
        assert_eq!(state.file().len(), 0);
        assert_eq!(state.version(), 1);
        assert_eq!(store.keys(), keys, "nothing written");
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "3".into())?;
        mutation.flush().await?;
        let reopened = State::open(store).await?;
        assert_eq!(&reopened.record("outbox")[..], b"3");
        assert_eq!(reopened.file().len(), 0);
        Ok(())
    })
}

/// A write refuses what no flush could store (a record too large or badly
/// named, a bad length, a page past the largest file or not 4096 bytes)
/// before the mutation changes: it holds what was accepted, and flushes.
#[test]
fn a_write_refuses_what_no_flush_could_store_and_the_mutation_goes_on() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "yes".into())?;
        mutation.write(0, Some(page(1)))?;
        let large = vec![0; MAX_RECORD_BYTES + 1].into();
        let error = mutation.set_record("outbox", large).unwrap_err();
        assert!(error.to_string().contains("exceed"), "{error}");
        for name in ["", "db", &"n".repeat(256)] {
            assert!(mutation.set_record(name, "no".into()).is_err(), "{name:?}");
        }
        assert!(mutation.set_len(1000).is_err());
        assert!(mutation.write(u64::MAX, None).is_err());
        assert!(mutation.write(1, Some(vec![0; 100].into())).is_err());
        // It flushes what was accepted, and nothing of the rest.
        mutation.flush().await?;
        let reopened = State::open(store).await?;
        assert_eq!(&reopened.record("outbox")[..], b"yes");
        assert_eq!(reopened.file().len(), 4096);
        assert_eq!(reopened.file().read(0).await?, Some(page(1)));
        Ok(())
    })
}

/// A mutation kept while another flushes still flushes afterwards: its writes
/// land on top, and a file the other flush grew keeps its length and pages.
#[test]
fn a_mutation_kept_across_another_flush_flushes_on_top() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        let mut kept = state.mutation();
        kept.set_record("outbox", "kept".into())?;
        kept.write(0, Some(page(1)))?;
        let mut other = state.mutation();
        other.set_record("outbox", "other".into())?;
        other.write(3, Some(page(2)))?;
        other.flush().await?;
        assert_eq!(&state.record("outbox")[..], b"other");
        kept.flush().await?;
        assert_eq!(state.version(), 2);
        let reopened = State::open(store).await?;
        assert_eq!(&reopened.record("outbox")[..], b"kept");
        let db = reopened.file();
        assert_eq!(db.len(), 4 * 4096);
        assert_eq!(db.read(0).await?, Some(page(1)));
        assert_eq!(db.read(3).await?, Some(page(2)));
        Ok(())
    })
}

/// A length set in a mutation holds at its flush, whatever another flush grew
/// the file to meanwhile: the pages past it are gone, and regrowing the file
/// reads zeros there.
#[test]
fn a_length_set_in_a_kept_mutation_cuts_what_another_flush_grew() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        let mut kept = state.mutation();
        kept.write(0, Some(page(1)))?;
        kept.set_len(4096)?;
        let mut other = state.mutation();
        other.write(3, Some(page(2)))?;
        other.flush().await?;
        kept.flush().await?;
        assert_eq!(state.file().len(), 4096);
        let mut regrown = state.mutation();
        regrown.write(5, Some(page(3)))?;
        regrown.flush().await?;
        let reopened = State::open(store).await?;
        let db = reopened.file();
        assert_eq!(db.read(0).await?, Some(page(1)));
        assert_eq!(db.read(3).await?, None);
        assert_eq!(db.read(5).await?, Some(page(3)));
        Ok(())
    })
}

#[test]
fn a_cancelled_flush_ends_the_state_and_no_other_flushes_on_top() -> Result<()> {
    futures::executor::block_on(async {
        let memory = Arc::new(MemoryKv::default());
        let namespace = fresh_namespace();
        let open =
            |store: Arc<dyn KvStore>| State::open(store).cached(&namespace.cache, &namespace.key);
        let state = open(Arc::new(Stalled(memory.clone()))).await?;
        // Another state of the owner, opened on the same flush.
        let other = open(memory.clone()).await?;
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "1".into())?;
        let mut flush = Box::pin(mutation.flush());
        assert!((&mut flush).now_or_never().is_none());
        // Dropped midway: its record may or may not be stored.
        drop(flush);
        assert!(!state.is_usable());
        let error = state.mutation().flush().await.unwrap_err().to_string();
        assert!(error.contains("ended"), "{error}");
        let mut mutation = other.mutation();
        mutation.set_record("outbox", "2".into())?;
        let error = mutation.flush().await.unwrap_err().to_string();
        assert!(error.contains("reopen"), "{error}");
        Ok(())
    })
}

/// A second flush while one runs is refused: it would build on the same
/// snapshot, and one of the two would lose its writes.
#[test]
fn a_flush_while_another_runs_is_refused() -> Result<()> {
    futures::executor::block_on(async {
        // Cached, so that the open itself does not sync.
        let namespace = fresh_namespace();
        let state = State::open(Arc::new(Stalled(Default::default())))
            .cached(&namespace.cache, &namespace.key)
            .await?;
        let mut first = state.mutation();
        first.set_record("outbox", "1".into())?;
        let mut first = Box::pin(first.flush());
        assert!((&mut first).now_or_never().is_none());
        let mut second = state.mutation();
        second.set_record("outbox", "2".into())?;
        let error = second.flush().await.unwrap_err().to_string();
        assert!(error.contains("is running"), "{error}");
        Ok(())
    })
}

#[test]
fn a_parked_value_heavier_than_the_budget_is_not_kept_and_evicts_nothing() -> Result<()> {
    futures::executor::block_on(async {
        let cache = open_cache(crate::state::PageCacheConfig {
            parked_bytes: 100,
            ..cache_config(1 << 20, 4 << 20)
        });
        let stores: Vec<Arc<dyn KvStore>> =
            vec![Arc::new(MemoryKv::default()), Arc::new(MemoryKv::default())];
        for ((namespace, value, weight), store) in [("light", 1u32, 10), ("heavy", 2, 101)]
            .into_iter()
            .zip(&stores)
        {
            let state = State::open(store.clone()).cached(&cache, namespace).await?;
            let mut mutation = state.mutation();
            mutation.set_record("outbox", "1".into())?;
            mutation.flush().await?;
            state.park(value, weight).unwrap();
        }
        // The light one is still parked; the heavy one was never kept.
        for ((namespace, expected), store) in [("light", Some(1u32)), ("heavy", None)]
            .into_iter()
            .zip(&stores)
        {
            let state = State::open(store.clone()).cached(&cache, namespace).await?;
            assert_eq!(state.take_parked::<u32>(), expected, "{namespace}");
        }
        Ok(())
    })
}

/// What a host spawns on a multi-threaded runtime is `Send`: opening,
/// flushing (checkpoints included), and collecting garbage.
#[test]
fn opening_flushing_and_collecting_are_send() -> Result<()> {
    fn send<T: Send>(value: T) -> T {
        value
    }
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let options = crate::state::StateOptions {
            checkpoint_entries: 1,
            ..Default::default()
        };
        for n in 0..3 {
            let state = send(State::open(store.clone()).options(options.clone())).await?;
            let mut mutation = state.mutation();
            mutation.write(n, Some(page(1)))?;
            send(mutation.flush()).await?;
            send(state.collect_garbage()).await?;
        }
        Ok(())
    })
}

/// A store that cannot list its keys keeps its garbage: its checkpoints
/// collect nothing, and its flushes go on.
#[test]
fn a_store_that_cannot_list_keeps_its_packs() -> Result<()> {
    futures::executor::block_on(async {
        let memory = Arc::new(MemoryKv::default());
        let options = crate::state::StateOptions {
            checkpoint_entries: 1,
            ..Default::default()
        };
        let state = State::open(Arc::new(Unlisted(memory.clone())))
            .options(options)
            .await?;
        let file = state.file();
        for n in 0..6 {
            let mut mutation = state.mutation();
            mutation.write(0, Some(page(n)))?;
            let report = mutation.flush().await?;
            assert_eq!(report.checkpoint.map_or(0, |c| c.deleted), 0);
        }
        assert_eq!(file.read(0).await?, Some(page(5)));
        assert!(memory.packs().len() > state.needed_packs().await?.len());
        Ok(())
    })
}

/// Where Restate cannot list its keys, packs in S3 are still collected: only
/// the manifests, in Restate, stay.
#[test]
fn packs_in_s3_are_collected_where_restate_cannot_list() -> Result<()> {
    futures::executor::block_on(async {
        let memory = Arc::new(MemoryKv::default());
        let packs = MemoryBlobs::default();
        let options = crate::state::StateOptions {
            checkpoint_entries: 1,
            l0_bytes: 0,
            ..Default::default()
        };
        let state = State::open(Arc::new(Unlisted(memory.clone())))
            .checkpoints(Arc::new(packs.clone()))
            .options(options)
            .await?;
        let mut deleted = 0;
        for n in 0..6 {
            let mut mutation = state.mutation();
            mutation.write(0, Some(page(n)))?;
            let report = mutation.flush().await?;
            deleted += report.checkpoint.map_or(0, |c| c.deleted);
        }
        assert!(deleted > 0);
        assert_eq!(packs.len(), state.needed_packs().await?.len());
        assert!(memory.manifests().len() > state.needed_manifests().len());
        Ok(())
    })
}

/// A checkpoint that fails fails nothing: the flush goes on and says why,
/// and the next flush checkpoints.
#[test]
fn a_checkpoint_that_fails_is_reported_and_the_flush_goes_on() -> Result<()> {
    futures::executor::block_on(async {
        let packs = MemoryBlobs::default();
        let options = crate::state::StateOptions {
            checkpoint_entries: 1,
            l0_bytes: 0,
            ..Default::default()
        };
        let state = State::open(Arc::new(MemoryKv::default()))
            .checkpoints(Arc::new(packs.clone()))
            .options(options)
            .await?;
        let file = state.file();
        let flush = async |n: u8| {
            let mut mutation = state.mutation();
            mutation.write(0, Some(page(n)))?;
            mutation.flush().await
        };
        flush(1).await?;
        packs.fail_after(Some(0));
        let report = flush(2).await?;
        assert!(report.checkpoint.is_none());
        let failed = report.checkpoint_failed.expect("the checkpoint failed");
        assert!(!failed.is_empty());
        packs.fail_after(None);
        let report = flush(3).await?;
        assert!(report.checkpoint.is_some() && report.checkpoint_failed.is_none());
        assert_eq!(file.read(0).await?, Some(page(3)));
        Ok(())
    })
}

/// A checkpoint that keeps failing (its store down) is tried at the next
/// flush, then at fewer and fewer of them, not at every one; the states
/// the namespace opens next in the same cache wait as well. One that
/// succeeds again ends the wait.
#[test]
fn a_checkpoint_that_keeps_failing_is_tried_less_and_less() -> Result<()> {
    futures::executor::block_on(async {
        let packs = MemoryBlobs::default();
        let store = Arc::new(MemoryKv::default());
        let cache = open_cache(cache_config(4 << 20, 4 << 20));
        let options = crate::state::StateOptions {
            checkpoint_entries: 1,
            l0_bytes: 0,
            ..Default::default()
        };
        let open = || {
            State::open(store.clone())
                .checkpoints(Arc::new(packs.clone()))
                .options(options.clone())
                .cached(&cache, "db")
        };
        // Whether a flush tried a checkpoint, and whether it made one. Never
        // a page of zeros, which a checkpoint stores nothing of (no put).
        let flush = async |state: &State, n: u8| -> Result<(bool, bool)> {
            let mut mutation = state.mutation();
            mutation.write(0, Some(page(n % 250 + 1)))?;
            let report = mutation.flush().await?;
            let made = report.checkpoint.is_some();
            Ok((made || report.checkpoint_failed.is_some(), made))
        };
        let state = open().await?;
        flush(&state, 1).await?;
        packs.fail_after(Some(0));
        let mut tried = Vec::new();
        for n in 0..40 {
            if flush(&state, n).await?.0 {
                tried.push(n);
            }
        }
        // At 0, then 1, 3, 7, 15 and 31 flushes after it: twice the wait each time.
        assert_eq!(tried, [0, 1, 3, 7, 15, 31], "the flushes that tried");

        // A state opened next in the cache waits out the same backoff.
        drop(state);
        let state = open().await?;
        assert!(!flush(&state, 40).await?.0, "still waiting");

        packs.fail_after(None);
        let mut made = None;
        for n in 41..120 {
            if flush(&state, n).await?.1 {
                made = Some(n);
                break;
            }
        }
        assert!(made.is_some(), "a checkpoint once the store is back");
        assert!(flush(&state, 0).await?.1, "and at every flush again");
        Ok(())
    })
}

/// With packs in Restate, a collection lists the keys once, and stores its
/// deletes, of packs and manifests together, with one sync.
#[test]
fn a_collection_lists_once_and_syncs_once() -> Result<()> {
    futures::executor::block_on(async {
        let memory = Arc::new(MemoryKv::default());
        let options = crate::state::StateOptions {
            checkpoint_entries: 1,
            ..Default::default()
        };
        let state = State::open(memory.clone()).options(options).await?;
        let mut collected = 0;
        for n in 0..6 {
            memory.reset_stats();
            let mut mutation = state.mutation();
            mutation.write(0, Some(page(n)))?;
            mutation.flush().await?;
            // The flush's sync; then, collecting, a listing and a sync.
            let stats = memory.stats();
            assert!(stats.lists <= 1 && stats.syncs <= 2, "{stats:?}");
            if stats.kind("manifest").deletes > 0 {
                assert_eq!((stats.lists, stats.syncs), (1, 2));
                collected += 1;
            }
        }
        assert!(collected > 0);
        assert_eq!(memory.manifests(), state.needed_manifests());
        assert_eq!(memory.packs(), state.needed_packs().await?);
        Ok(())
    })
}

/// With packs in Restate, a state of small records does not checkpoint at
/// every flush, though its checkpoint holds less than one flush puts: the
/// tail holds a frame's bytes (16 KiB) first.
#[test]
fn small_records_checkpoint_once_the_tail_holds_a_frame() -> Result<()> {
    futures::executor::block_on(async {
        let state = State::open(Arc::new(MemoryKv::default())).await?;
        let mut checkpoints = Vec::new();
        for n in 0..120u8 {
            let mut mutation = state.mutation();
            mutation.set_record("outbox", vec![n; 200].into())?;
            if mutation.flush().await?.checkpoint.is_some() {
                checkpoints.push(n);
            }
        }
        // About 210 bytes a flush: the tail passes 16 KiB at the 79th.
        assert_eq!(checkpoints.len(), 1, "{checkpoints:?}");
        Ok(())
    })
}

/// A mutation whose tail entry would pass the 1 GiB a decode takes is
/// refused before anything is written: the state stays usable, and its
/// next open reads it.
#[test]
fn a_mutation_past_a_gib_is_refused() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        // A page compresses to at most its bytes and 144 more.
        let pages = (1 << 30) / (4096 + 144) + 1;
        let mut mutation = state.mutation();
        for n in 0..pages {
            mutation.write(n, Some(page(7)))?;
        }
        let error = mutation.flush().await.unwrap_err().to_string();
        assert!(error.contains("exceeds"), "{error}");
        assert!(state.is_usable());
        let mut mutation = state.mutation();
        mutation.write(0, Some(page(7)))?;
        mutation.flush().await?;
        assert_eq!(
            State::open(store).await?.file().read(0).await?,
            Some(page(7))
        );
        Ok(())
    })
}

/// A part of a tail entry no head names, which a crashed flush left past
/// the parts its retry put, goes with a collection once a checkpoint
/// covers its version.
#[test]
fn a_crashed_flushs_extra_tail_parts_are_collected() -> Result<()> {
    futures::executor::block_on(async {
        let memory = Arc::new(MemoryKv::default());
        // The first flush's crashed attempt put more parts than it does.
        memory.put("v22/tail/1/3", Bytes::from_static(b"stray"))?;
        memory.sync().await?;
        let state = State::open(memory.clone()).await?;
        for n in 0..3u8 {
            let mut mutation = state.mutation();
            mutation.set_record("outbox", vec![n; 8].into())?;
            mutation.checkpoint();
            mutation.flush().await?;
        }
        let keys = memory.keys();
        assert!(!keys.contains("v22/tail/1/3"), "{keys:?}");
        Ok(())
    })
}

/// A lost flush leaves its tail entry, under a key with its writer's nonce
/// that no head names and no other flush puts: a collection deletes it once
/// a checkpoint covers its version, and keeps the entries the head names.
#[test]
fn a_lost_flushs_tail_entry_is_collected() -> Result<()> {
    futures::executor::block_on(async {
        let memory = Arc::new(MemoryKv::default());
        let state = State::open(memory.clone()).await?;
        let mut mutation = state.mutation();
        mutation.set_record("outbox", vec![0; 8].into())?;
        mutation.flush().await?;
        // Another writer's flush of version 2, lost: its entry stays.
        let lost = crate::state::format::tail_key(2, 0xdead, 0);
        memory.put(&lost, Bytes::from_static(b"lost"))?;
        memory.sync().await?;
        for n in 1..4u8 {
            let mut mutation = state.mutation();
            mutation.set_record("outbox", vec![n; 8].into())?;
            mutation.checkpoint();
            mutation.flush().await?;
        }
        let keys = memory.keys();
        assert!(!keys.contains(&lost), "{keys:?}");
        let reopened = State::open(memory.clone()).await?;
        assert_eq!(reopened.record("outbox"), Bytes::from(vec![3; 8]));
        Ok(())
    })
}
