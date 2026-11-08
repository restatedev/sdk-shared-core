//! The owner cache across owners, crashes and readers: no session reads a
//! page of a flush that was lost, and a reader reads the state it opened
//! while the owner flushes.

use super::support::*;
use crate::state::{kv::KvStore, simulation::*, Result, State, StateOptions};
use futures::future::Either;
use std::sync::Arc;

/// Small frames, large records in packs, and a checkpoint
/// every other flush.
fn options(cache: Option<&Namespace>) -> Setup {
    Setup {
        options: StateOptions {
            frame_pages: 2,
            inline_record_bytes: 256,
            checkpoint_entries: 2,
            ..StateOptions::default()
        },
        cache: cache.cloned(),
        packs: None,
    }
}

fn failing_sync() -> Failpoint {
    Failpoint {
        operation: Operation::Sync,
        key_prefix: String::new(),
        nth: 1,
        effect: Effect::None,
        cancel: false,
    }
}

/// Flush `ops` in a new session; the model follows.
async fn flush(
    store: &Arc<dyn KvStore>,
    cache: Option<&Namespace>,
    model: &mut Model,
    ops: &[Op],
) -> Result<()> {
    let session = Session::open(store.clone(), options(cache)).await?;
    assert_eq!(model.differs(&session).await?, None);
    session.flush(ops).await?;
    *model = model.applied(ops);
    Ok(())
}

/// Owner A's flush fails at its sync; A reopens and reads what the failed
/// flush wrote (visible, but synced by that open). A crash loses whatever is
/// not durable. Another process, B, takes over and flushes, at versions the
/// lost flush may have used. A, back with its cache, must read B's pages,
/// not the ones it cached.
#[test]
fn a_cache_that_read_a_lost_flush_never_serves_it() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let fault = Arc::new(FaultKv::new(memory.clone(), 0));
        let store: Arc<dyn KvStore> = fault.clone();
        let (a, b) = (fresh_namespace(), fresh_namespace());
        let mut model = Model::default();
        flush(&store, Some(&a), &mut model, &[Op::Write(0, Some(page(1)))]).await?;

        // A's flush fails at its sync: its writes are visible.
        let session = Session::open(store.clone(), options(Some(&a))).await?;
        let _gate = fault.arm(failing_sync());
        let write = [Op::Write(0, Some(page(2)))];
        assert!(session.flush(&write).await.is_err());
        fault.disarm();
        drop(session);
        // A reopens on them, and reads (and caches) the page.
        let failed = model.applied(&write);
        let session = Session::open(store.clone(), options(Some(&a))).await?;
        assert_eq!(failed.differs(&session).await?, None);
        drop(session);
        // The backend loses every write since the last sync, if any remain.
        memory.crash();
        let session = Session::open(store.clone(), options(None)).await?;
        if failed.matches(&session).await? {
            model = failed;
        }
        drop(session);

        // B, another process, flushes another page.
        flush(&store, Some(&b), &mut model, &[Op::Write(0, Some(page(3)))]).await?;
        // A reads B's page.
        let session = Session::open(store.clone(), options(Some(&a))).await?;
        assert_eq!(model.differs(&session).await?, None);
        Ok(())
    })
}

/// State A's flush waits on its sync when a second owner state, B, opens:
/// the store shows A's head, not yet durable. B must not cache the state it
/// names: A's flush is lost, and another process flushes at those versions.
#[test]
fn a_state_opened_beside_a_running_flush_caches_nothing_it_may_lose() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let fault = Arc::new(FaultKv::new(memory.clone(), 0));
        let store: Arc<dyn KvStore> = fault.clone();
        let (a, b) = (fresh_namespace(), fresh_namespace());
        let mut model = Model::default();
        flush(&store, Some(&a), &mut model, &[Op::Write(0, Some(page(1)))]).await?;

        let first = Session::open(store.clone(), options(Some(&a))).await?;
        let stuck = fault.arm(Failpoint {
            cancel: true,
            ..failing_sync()
        });
        let write = [Op::Write(0, Some(page(2)))];
        let running = Box::pin(first.flush(&write));
        let Either::Right((_, running)) = futures::future::select(running, stuck).await else {
            panic!("the flush did not wait on its sync");
        };
        // B opens, and reads (and may cache) what A's flush wrote.
        let second = Session::open(store.clone(), options(Some(&a))).await?;
        drop(second);
        drop(running);
        fault.disarm();
        memory.crash();
        let failed = model.applied(&write);
        let session = Session::open(store.clone(), options(None)).await?;
        if failed.matches(&session).await? {
            model = failed;
        }
        drop(session);

        // Another process flushes, at the versions the lost flush used.
        flush(&store, Some(&b), &mut model, &[Op::Write(0, Some(page(3)))]).await?;
        let session = Session::open(store.clone(), options(Some(&a))).await?;
        assert_eq!(model.differs(&session).await?, None);
        Ok(())
    })
}

/// The object's state is cleared, and another process makes a new database
/// in the same namespace, numbering its versions from 0 again. The owner,
/// back with its cache, must read the new database's pages, not the ones it
/// cached of the old one at the same versions.
#[test]
fn a_namespace_made_again_never_reads_the_old_databases_pages() -> Result<()> {
    futures::executor::block_on(async {
        let a = fresh_namespace();
        let old: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let mut model = Model::default();
        flush(&old, Some(&a), &mut model, &[Op::Write(0, Some(page(1)))]).await?;
        let session = Session::open(old, options(Some(&a))).await?;
        assert_eq!(model.differs(&session).await?, None);
        drop(session);

        let cleared: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let mut model = Model::default();
        flush(&cleared, None, &mut model, &[Op::Write(0, Some(page(2)))]).await?;
        let session = Session::open(cleared, options(Some(&a))).await?;
        assert_eq!(model.differs(&session).await?, None);
        Ok(())
    })
}

/// The object's state is cleared, and the owner, still in this process,
/// makes a new database in the same namespace, numbering its versions from 0
/// again. A reader in another process, back with its cache, must read the
/// new database's pages, not the ones it cached of the old one at the same
/// versions: the owner names the new database's entries with another nonce.
#[test]
fn a_namespace_its_owner_makes_again_never_reads_the_old_databases_pages() -> Result<()> {
    futures::executor::block_on(async {
        let (owner, reader) = (fresh_namespace(), fresh_namespace());
        let old: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let mut model = Model::default();
        flush(
            &old,
            Some(&owner),
            &mut model,
            &[Op::Write(0, Some(page(1)))],
        )
        .await?;
        let session = Session::reader(old, options(Some(&reader))).await?;
        assert_eq!(model.differs(&session).await?, None);
        drop(session);

        let cleared: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let mut model = Model::default();
        flush(
            &cleared,
            Some(&owner),
            &mut model,
            &[Op::Write(0, Some(page(2)))],
        )
        .await?;
        let session = Session::reader(cleared, options(Some(&reader))).await?;
        assert_eq!(model.differs(&session).await?, None);
        Ok(())
    })
}

/// A reader beside the owner, which flushes and checkpoints meanwhile, and a
/// worker that collects garbage after each flush: the reader reads the state
/// it opened, and never writes to the store; once the owner's second
/// checkpoint since dropped a pack it needs, a read of it fails, never reads
/// another state.
#[test]
fn a_reader_beside_a_flushing_owner_reads_what_it_opened() -> Result<()> {
    futures::executor::block_on(async {
        let mut rng = Rng::new(0x7ead);
        let memory = MemoryKv::default();
        let store: Arc<dyn KvStore> = Arc::new(memory.clone());
        let owner = fresh_namespace();
        let mut model = Model::default();
        // The owner's second checkpoint after the reader opened may come at
        // its third flush.
        let grace = 3;
        let (mut read, mut gone) = (0, 0);
        for round in 0..60 {
            let ops: Vec<Op> = (0..1 + rng.below(6))
                .map(|_| Op::random(&mut rng))
                .collect();
            flush(&store, Some(&owner), &mut model, &ops).await?;
            let lazy = Setup {
                options: StateOptions {
                    prefetch_pages: rng.below(2) * 16,
                    ..StateOptions::default()
                },
                cache: rng.percent(50).then(|| owner.clone()),
                packs: None,
            };
            let reader = Session::reader(store.clone(), lazy).await?;
            let opened = model.clone();
            // Past the grace sometimes: then packs it reads may be gone.
            let flushes = rng.pick(&[0, 1, grace - 1, grace, grace + 4]);
            for _ in 0..flushes {
                // Pages the reader reads often, and anything else.
                let hot = Op::Write(rng.below(4), Some(varied(&mut rng)));
                let ops = [hot, Op::random(&mut rng)];
                flush(&store, Some(&owner), &mut model, &ops).await?;
                // A worker collects garbage beside them.
                let worker = State::open(store.clone()).reader().await?;
                worker.collect_garbage().await?;
            }
            for _ in 0..16 {
                let index = rng.below(opened.len().div_ceil(4096) + 1);
                match reader.file().read(index).await {
                    Ok(page) => {
                        assert_eq!(page, opened.page(index), "round {round}");
                        read += 1;
                    }
                    Err(error) if flushes >= grace => {
                        assert!(error.to_string().contains("is gone"), "{error}");
                        gone += 1;
                    }
                    Err(error) => panic!("round {round}: {error}"),
                }
            }
            for name in RECORDS {
                assert_eq!(reader.state.record(name), opened.record(name));
            }
            // Its mutations never flush.
            let keys = memory.keys();
            let error = reader.flush(&[Op::Record("outbox", "reader".into())]).await;
            let error = error.unwrap_err().to_string();
            assert!(error.contains("a reader writes nothing"), "{error}");
            assert_eq!(memory.keys(), keys);
        }
        assert!(read > 100 && gone > 0, "{read} read, {gone} gone");
        Ok(())
    })
}

/// Two sessions of one owner overlap: the open one reads the state it opened
/// on while the other flushes; then it may not flush on top.
#[test]
fn an_open_session_reads_its_state_while_another_flushes() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let store: Arc<dyn KvStore> = Arc::new(memory.clone());
        let cache = fresh_namespace();
        let mut model = Model::default();
        let mut rng = Rng::new(0x0e7);
        for _ in 0..3 {
            let ops: Vec<Op> = (0..6).map(|_| Op::random(&mut rng)).collect();
            flush(&store, Some(&cache), &mut model, &ops).await?;
        }
        let open = Session::open(store.clone(), options(Some(&cache))).await?;
        let opened = model.clone();
        let writer = Session::open(store.clone(), options(Some(&cache))).await?;
        for n in 0..5 {
            let ops = [Op::Write(n, Some(page(100 + n))), Op::random(&mut rng)];
            writer.flush(&ops).await?;
            model = model.applied(&ops);
            assert_eq!(opened.differs(&open).await?, None);
        }
        let late = [Op::Record("outbox", "late".into())];
        let error = open.flush(&late).await.unwrap_err().to_string();
        assert!(error.contains("another state flushed"), "{error}");
        drop(open);
        let ops = [Op::Write(0, Some(page(7)))];
        writer.flush(&ops).await?;
        model = model.applied(&ops);
        drop(writer);
        let session = Session::open(store, options(Some(&cache))).await?;
        assert_eq!(model.differs(&session).await?, None);
        Ok(())
    })
}

/// Every state reads the head from the store, and what it names through the
/// cache, an owner's or a reader's alike: a reader at the head the owner
/// flushed opens with one get, the head, where one without the cache gets
/// the manifest and every tail entry too. A reader of a namespace the cache
/// has not seen reads them, and the next reads only the head; so does the
/// owner.
#[test]
fn every_state_reads_the_head_from_the_store_and_the_rest_through_the_cache() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let store: Arc<dyn KvStore> = Arc::new(memory.clone());
        let setup = |cache: Option<&Namespace>| Setup {
            options: StateOptions::default(),
            cache: cache.cloned(),
            packs: None,
        };
        // The gets that load a state: the head and the tail entries.
        let loading = || {
            let stats = memory.stats();
            stats.kind("head").gets + stats.kind("tail").gets
        };
        let owner = fresh_namespace();
        let mut model = Model::default();
        let mut rng = Rng::new(0x4ead);
        let session = Session::open(store.clone(), setup(Some(&owner))).await?;
        for _ in 0..40 {
            let ops: Vec<Op> = (0..3).map(|_| Op::random(&mut rng)).collect();
            session.flush(&ops).await?;
            model = model.applied(&ops);
        }
        drop(session);

        memory.reset_stats();
        let warm = Session::reader(store.clone(), setup(Some(&owner))).await?;
        let stats = memory.stats();
        assert_eq!((stats.gets, stats.kind("head").gets), (1, 1), "{stats:?}");
        assert_eq!(model.differs(&warm).await?, None);

        memory.reset_stats();
        let cold = Session::reader(store.clone(), setup(None)).await?;
        let loaded = loading();
        assert!(memory.stats().kind("tail").gets > 1, "{:?}", memory.stats());
        assert_eq!(model.differs(&cold).await?, None);

        let other = fresh_namespace();
        memory.reset_stats();
        let first = Session::reader(store.clone(), setup(Some(&other))).await?;
        assert_eq!(loading(), loaded);
        assert_eq!(model.differs(&first).await?, None);
        memory.reset_stats();
        let next = Session::reader(store.clone(), setup(Some(&other))).await?;
        assert_eq!(memory.stats().gets, 1, "{:?}", memory.stats());
        assert_eq!(model.differs(&next).await?, None);

        memory.reset_stats();
        let reopened = Session::open(store.clone(), setup(Some(&owner))).await?;
        assert_eq!(memory.stats().gets, 1, "{:?}", memory.stats());
        assert_eq!(model.differs(&reopened).await?, None);
        Ok(())
    })
}

/// A state at a head the cache lacks reads only the tail entries the cache
/// lacks: those this process flushed, or read, are kept. Here another
/// process flushed the last three.
#[test]
fn a_state_reads_only_the_tail_entries_the_cache_lacks() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let store: Arc<dyn KvStore> = Arc::new(memory.clone());
        // Small entries, far from any checkpoint.
        let setup = |cache: Option<&Namespace>| Setup {
            options: StateOptions {
                checkpoint_entries: 1000,
                ..StateOptions::default()
            },
            cache: cache.cloned(),
            packs: None,
        };
        let here = fresh_namespace();
        let mut model = Model::default();
        let session = Session::open(store.clone(), setup(Some(&here))).await?;
        for n in 0..20 {
            let ops = [Op::Write(n % 4, Some(page(n)))];
            session.flush(&ops).await?;
            model = model.applied(&ops);
        }
        drop(session);
        let elsewhere = Session::open(store.clone(), setup(None)).await?;
        for n in 20..23 {
            let ops = [Op::Write(n % 4, Some(page(n)))];
            elsewhere.flush(&ops).await?;
            model = model.applied(&ops);
        }
        drop(elsewhere);

        memory.reset_stats();
        let reader = Session::reader(store.clone(), setup(Some(&here))).await?;
        let stats = memory.stats();
        assert_eq!(stats.kind("tail").gets, 3, "{stats:?}");
        assert_eq!(stats.kind("manifest").gets, 0, "{stats:?}");
        assert_eq!(model.differs(&reader).await?, None);

        memory.reset_stats();
        let cold = Session::reader(store.clone(), setup(None)).await?;
        assert_eq!(memory.stats().kind("tail").gets, 23);
        assert_eq!(model.differs(&cold).await?, None);
        Ok(())
    })
}

/// A reader opens beside a running flush: the store shows the flush's head,
/// not yet durable, and the reader reads, and keeps, its tail entry. The
/// flush is lost, and another process flushes at its versions: under keys
/// of its own, as every flush's entries have its writer's nonce, so the
/// namespace's states read what that one flushed.
#[test]
fn a_lost_flushs_entries_kept_are_never_read_for_the_next_writers() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let fault = Arc::new(FaultKv::new(memory.clone(), 0));
        let store: Arc<dyn KvStore> = fault.clone();
        let (a, b) = (fresh_namespace(), fresh_namespace());
        let mut model = Model::default();
        flush(&store, Some(&a), &mut model, &[Op::Write(0, Some(page(1)))]).await?;

        let first = Session::open(store.clone(), options(Some(&a))).await?;
        let stuck = fault.arm(Failpoint {
            cancel: true,
            ..failing_sync()
        });
        let write = [Op::Write(0, Some(page(2)))];
        let running = Box::pin(first.flush(&write));
        let Either::Right((_, running)) = futures::future::select(running, stuck).await else {
            panic!("the flush did not wait on its sync");
        };
        // The reader reads what the running flush wrote.
        let failed = model.applied(&write);
        let reader = Session::reader(store.clone(), options(Some(&a))).await?;
        assert_eq!(failed.differs(&reader).await?, None);
        drop(reader);
        drop(running);
        fault.disarm();
        // Nothing synced the flush's writes: they are lost.
        memory.crash();
        let session = Session::open(store.clone(), options(None)).await?;
        assert_eq!(model.differs(&session).await?, None, "the flush is lost");
        drop(session);

        // Another process flushes, at the versions the lost flush used.
        flush(&store, Some(&b), &mut model, &[Op::Write(0, Some(page(3)))]).await?;
        let reader = Session::reader(store.clone(), options(Some(&a))).await?;
        assert_eq!(model.differs(&reader).await?, None);
        let session = Session::open(store.clone(), options(Some(&a))).await?;
        assert_eq!(model.differs(&session).await?, None);
        Ok(())
    })
}

/// A reader opens beside a running flush and reads its head, not yet
/// durable. The flush is lost, and another process flushes at its version,
/// under another nonce. The reader, holding the lost head, collects
/// garbage: whatever head a state read, it never deletes an entry a later
/// head names, so the database reads on.
#[test]
fn a_collection_on_a_lost_head_keeps_the_entries_a_later_head_names() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let fault = Arc::new(FaultKv::new(memory.clone(), 0));
        let store: Arc<dyn KvStore> = fault.clone();
        let (a, b) = (fresh_namespace(), fresh_namespace());
        let mut model = Model::default();
        // Checkpoints, so that the heads have a previous one, which a
        // collection needs.
        for n in 0..6 {
            let ops = [Op::Write(n % 3, Some(page(n + 1)))];
            flush(&store, Some(&a), &mut model, &ops).await?;
        }
        let first = Session::open(store.clone(), options(Some(&a))).await?;
        let stuck = fault.arm(Failpoint {
            cancel: true,
            ..failing_sync()
        });
        let write = [Op::Write(0, Some(page(9)))];
        let running = Box::pin(first.flush(&write));
        let Either::Right((_, running)) = futures::future::select(running, stuck).await else {
            panic!("the flush did not wait on its sync");
        };
        let reader = Session::reader(store.clone(), options(None)).await?;
        drop(running);
        fault.disarm();
        memory.crash();

        // Another process flushes, at the version the lost flush used.
        flush(
            &store,
            Some(&b),
            &mut model,
            &[Op::Write(1, Some(page(10)))],
        )
        .await?;
        reader.state.collect_garbage().await?;
        let session = Session::open(store.clone(), options(None)).await?;
        assert_eq!(model.differs(&session).await?, None);
        Ok(())
    })
}

/// A database written before tail entries had nonces: its head (`SH22`)
/// names entries under keys a retry overwrites, which every open reads from
/// the store, as the head; a flush now writes its entry under a key with
/// its writer's nonce, which is read once. Every state reads what it holds.
#[test]
fn a_database_written_before_nonces_reads_on_and_keeps_what_it_writes() -> Result<()> {
    use crate::state::format::{parse_tail_key, tail_key, Head, HEAD_KEY};
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let store: Arc<dyn KvStore> = Arc::new(memory.clone());
        let setup = |cache: Option<&Namespace>| Setup {
            options: StateOptions {
                checkpoint_entries: 1000,
                ..StateOptions::default()
            },
            cache: cache.cloned(),
            packs: None,
        };
        let mut model = Model::default();
        let session = Session::open(store.clone(), setup(None)).await?;
        for n in 0..3 {
            let ops = [Op::Write(n, Some(page(n + 1)))];
            session.flush(&ops).await?;
            model = model.applied(&ops);
        }
        drop(session);
        // As then: the entries' keys without their nonces, the head without
        // its runs, under the magic of then.
        for (key, value) in memory.contents() {
            if let Some(tail) = parse_tail_key(&key) {
                memory.delete(&key)?;
                memory.put(&tail_key(tail.version, 0, tail.part), value)?;
            }
        }
        let head = Head::decode(&memory.contents()[HEAD_KEY])?;
        let encoded = Head {
            runs: Vec::new(),
            ..head
        }
        .encode();
        let legacy = [b"SH22".as_slice(), &encoded[4..encoded.len() - 1]].concat();
        memory.put(HEAD_KEY, legacy.into())?;
        memory.sync().await?;

        let owner = fresh_namespace();
        for _ in 0..2 {
            memory.reset_stats();
            let reader = Session::reader(store.clone(), setup(Some(&owner))).await?;
            assert_eq!(memory.stats().kind("tail").gets, 3, "read every time");
            assert_eq!(model.differs(&reader).await?, None);
        }
        let session = Session::open(store.clone(), setup(Some(&owner))).await?;
        let ops = [Op::Write(3, Some(page(4)))];
        session.flush(&ops).await?;
        model = model.applied(&ops);
        drop(session);
        memory.reset_stats();
        let reader = Session::reader(store.clone(), setup(Some(&owner))).await?;
        assert_eq!(memory.stats().kind("tail").gets, 3, "the old entries only");
        assert_eq!(model.differs(&reader).await?, None);
        Ok(())
    })
}
