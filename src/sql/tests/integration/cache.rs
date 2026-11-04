use super::host::Db;
use super::host::StateSetup;
use crate::state::simulation::Namespace;
use crate::state::{
    kv::KvStore,
    simulation::{fresh_namespace, LatencyKv, MemoryKv},
    Result, StateOptions,
};
use std::sync::Arc;

fn cached(cache: &Namespace) -> StateSetup {
    StateSetup {
        options: StateOptions::default(),
        cache: Some(cache.clone()),
        packs: None,
    }
}

async fn create(store: Arc<dyn KvStore>, options: StateSetup) -> Result<()> {
    let db = Db::open(store, options).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")
        .await?;
    // Incompressible: a large save, of more frames than a small cache holds,
    // checkpointed.
    db.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<400) INSERT INTO t SELECT x,randomblob(900) FROM n").await?;
    db.save_checkpointed().await?;
    Ok(())
}

async fn payload_sum(store: Arc<dyn KvStore>, options: StateSetup) -> Result<i64> {
    let db = Db::open(store, options).await?;
    let rows = db.query("SELECT sum(length(payload) + id) FROM t").await?;
    Ok(rows[0][0].as_int().unwrap())
}

#[test]
fn an_owner_cache_serves_reopened_sessions_in_one_round_trip() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(LatencyKv::new(MemoryKv::default()));
        let cache = fresh_namespace();
        create(store.clone(), cached(&cache)).await?;
        let expected = 400 * 900 + 400 * 401 / 2;
        // The first session confirms nothing it wrote; it reads the full table.
        store.reset();
        store.inner.reset_stats();
        assert_eq!(payload_sum(store.clone(), cached(&cache)).await?, expected);
        let stats = store.inner.stats();
        assert_eq!(store.round_trips(), 1, "meta and probe together: {stats:?}");
        assert_eq!((stats.kind("pack").gets, stats.puts), (0, 0));

        store.inner.reset_stats();
        assert_eq!(
            payload_sum(store.clone(), StateOptions::default().into()).await?,
            expected
        );
        assert!(store.inner.stats().kind("pack").gets > 0);
        Ok(())
    })
}

/// Writers without the cache, checkpointing each flush or rarely, never
/// leave the owner's cache stale: it keeps ranges of packs, which never
/// change.
#[test]
fn other_writers_never_leave_the_cache_stale() -> Result<()> {
    futures::executor::block_on(async {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let cache = fresh_namespace();
        create(store.clone(), cached(&cache)).await?;
        let mut expected = 400 * 900 + 400 * 401 / 2;
        assert_eq!(payload_sum(store.clone(), cached(&cache)).await?, expected);
        for (round, checkpoint_entries) in [(0, 1), (1, 16), (2, 16), (3, 1)] {
            let other = StateOptions {
                checkpoint_entries,
                ..StateOptions::default()
            };
            let db = Db::open(store.clone(), other).await?;
            db.execute(&format!(
                "UPDATE t SET payload = printf('%0800d', id) WHERE id % 4 = {round}"
            ))
            .await?;
            db.save().await?;
            expected -= 100 * 100;
            assert_eq!(payload_sum(store.clone(), cached(&cache)).await?, expected);
        }
        Ok(())
    })
}

fn owner(cache: &Namespace) -> StateSetup {
    StateSetup {
        options: StateOptions::default(),
        cache: Some(cache.clone()),
        packs: None,
    }
}
async fn count(db: &Db) -> Result<i64> {
    Ok(db.query("SELECT count(*) FROM t").await?[0][0]
        .as_int()
        .unwrap())
}

/// A clean save or read-only session leaves its engine to the next session; a
/// session left in a transaction, an error, or a write by anyone else does not.
#[test]
fn warm_engines_continue_only_from_an_unchanged_flushed_state() -> Result<()> {
    use crate::state::simulation::{Effect, Failpoint, FaultKv, Operation};
    futures::executor::block_on(async {
        let store = Arc::new(FaultKv::new(MemoryKv::default(), 0));
        let cache = fresh_namespace();
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(!db.reused_engine());
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").await?;
        db.save().await?;

        // After a save, and after a read-only session, the engine continues.
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(db.reused_engine(), "after a save");
        db.execute("INSERT INTO t VALUES (1)").await?;
        db.save().await?;
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(db.reused_engine());
        assert_eq!(count(&db).await?, 1);
        drop(db);
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(db.reused_engine(), "after a read-only session");

        // A transaction left open discards the engine with it.
        db.execute("BEGIN; INSERT INTO t VALUES (2)").await?;
        drop(db);
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(!db.reused_engine(), "a transaction left open");
        assert_eq!(count(&db).await?, 1);
        drop(db);

        // An SQL error leaves the session, and so its engine, usable.
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(db.reused_engine());
        assert!(db.execute("INSERT INTO missing VALUES (1)").await.is_err());
        assert!(db.is_usable());
        drop(db);
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(db.reused_engine(), "after an SQL error");
        assert_eq!(count(&db).await?, 1);
        drop(db);

        // Another writer's flush forces a fresh engine, whether it
        // checkpoints or not.
        for (expected, checkpoint_entries) in [(2, 16), (3, 1)] {
            let other = StateOptions {
                checkpoint_entries,
                ..StateOptions::default()
            };
            let db = Db::open(store.clone(), other).await?;
            db.execute("INSERT INTO t SELECT max(id) + 1 FROM t")
                .await?;
            db.save().await?;
            let db = Db::open(store.clone(), owner(&cache)).await?;
            assert!(!db.reused_engine(), "another writer");
            assert_eq!(count(&db).await?, expected);
            drop(db);
            let db = Db::open(store.clone(), owner(&cache)).await?;
            assert!(db.reused_engine(), "warm again once unchanged");
        }

        // A commit that fails leaves nothing to reuse.
        let db = Db::open(store.clone(), owner(&cache)).await?;
        let _gate = store.arm(Failpoint {
            operation: Operation::Put,
            key_prefix: crate::state::HEAD_KEY.into(),
            nth: 1,
            effect: Effect::None,
            cancel: false,
        });
        assert!(db.execute("INSERT INTO t VALUES (100)").await.is_err());
        store.disarm();
        assert!(db.save().await.is_err());
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(!db.reused_engine(), "failed commit");
        assert_eq!(count(&db).await?, 3);
        Ok(())
    })
}

/// Another owner's saves cost this owner's cache only what they wrote: the
/// tail, and the packs of any checkpoint they made. Every other pack is
/// unchanged, so still cached.
#[test]
fn another_owner_costs_the_cache_only_what_it_wrote() -> Result<()> {
    futures::executor::block_on(async {
        for saves in [1, 6] {
            let store = Arc::new(LatencyKv::new(MemoryKv::default()));
            let cache = fresh_namespace();
            create(store.clone(), cached(&cache)).await?;
            let other = fresh_namespace();
            store.inner.reset_stats();
            for n in 0..saves {
                let db = Db::open(store.clone(), cached(&other)).await?;
                db.execute(&format!(
                    "UPDATE t SET payload = printf('%0800d', id) WHERE id = {}",
                    n * 31 + 1
                ))
                .await?;
                db.save().await?;
            }
            let written = store.inner.stats();
            store.inner.reset_stats();
            let expected = 400 * 900 + 400 * 401 / 2 - 100 * saves;
            assert_eq!(payload_sum(store.clone(), cached(&cache)).await?, expected);
            let fetched = store.inner.stats();
            // The tail since the checkpoint: what the other owner wrote,
            // and the checkpointing flush's own entry, which its checkpoint
            // does not hold.
            let tail = (fetched.kind("tail").gets, written.kind("tail").puts);
            assert!(
                tail.0 <= tail.1 + 1,
                "{saves} saves: {tail:?} tail parts fetched, written"
            );
            let packs = (fetched.kind("pack").gets, written.kind("pack").puts);
            assert!(
                packs.0 <= packs.1,
                "{saves} saves: {packs:?} pack parts fetched, written"
            );
        }
        Ok(())
    })
}

/// Namespaces with byte-identical `meta` share one process cache without ever
/// reading each other's pages, even with sessions running concurrently.
#[test]
fn namespaces_share_one_cache_without_sharing_pages() -> Result<()> {
    futures::executor::block_on(async {
        let cache = crate::state::simulation::shared_cache();
        let stores: Vec<Arc<dyn KvStore>> = (0..2)
            .map(|_| Arc::new(MemoryKv::default()) as Arc<dyn KvStore>)
            .collect();
        let options = |n: usize| StateSetup {
            options: StateOptions {
                checkpoint_entries: 1,
                ..StateOptions::default()
            },
            cache: Some(Namespace::new(cache, &format!("shared-{n}"))),
            packs: None,
        };
        // The same history shape, so the same meta, with different values.
        for (n, store) in stores.iter().enumerate() {
            let db = Db::open(store.clone(), options(n)).await?;
            db.execute("CREATE TABLE t(v TEXT)").await?;
            db.save().await?;
            let db = Db::open(store.clone(), options(n)).await?;
            db.execute(&format!("INSERT INTO t VALUES ('from {n}')"))
                .await?;
            db.save().await?;
        }
        let read = |n: usize| {
            let store = stores[n].clone();
            async move {
                let db = Db::open(store, options(n)).await?;
                let rows = db.query("SELECT v FROM t").await?;
                Result::Ok(rows[0][0].to_string())
            }
        };
        for _ in 0..3 {
            let (a, b) = futures::try_join!(read(0), read(1))?;
            assert_eq!((a.as_str(), b.as_str()), ("from 0", "from 1"));
        }
        Ok(())
    })
}

/// Frames evicted from memory are served from disk.
#[test]
fn evicted_frames_are_read_from_disk() -> Result<()> {
    use crate::state::simulation::{cache_config, open_cache};
    futures::executor::block_on(async {
        // Room for a few frames per shard, not the table.
        let cache = open_cache(cache_config(256 << 10, 64 << 20));
        let store = Arc::new(LatencyKv::new(MemoryKv::default()));
        let namespace = Namespace::new(&cache, "spilled");
        create(store.clone(), cached(&namespace)).await?;
        assert!(
            cache.memory_bytes() <= 256 << 10,
            "{}",
            cache.memory_bytes()
        );
        store.inner.reset_stats();
        let expected = 400 * 900 + 400 * 401 / 2;
        assert_eq!(
            payload_sum(store.clone(), cached(&namespace)).await?,
            expected
        );
        assert_eq!(store.inner.stats().kind("pack").gets, 0);
        Ok(())
    })
}

/// Parked engines are bounded by memory across namespaces.
#[test]
fn parked_engines_are_bounded() -> Result<()> {
    use crate::state::{
        simulation::{cache_config, open_cache},
        PageCacheConfig,
    };
    futures::executor::block_on(async {
        let cache = open_cache(PageCacheConfig {
            parked_bytes: (4 << 20) + (12 << 10), // one engine with an empty schema
            ..cache_config(1 << 20, 16 << 20)
        });
        let store: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let other: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let a = || owner(&Namespace::new(&cache, "a"));
        drop(Db::open(store.clone(), a()).await?);
        assert!(Db::open(store.clone(), a()).await?.reused_engine());
        drop(Db::open(other, owner(&Namespace::new(&cache, "b"))).await?);
        let db = Db::open(store, a()).await?;
        assert!(!db.reused_engine(), "evicted by b's engine");
        Ok(())
    })
}

/// A read-only session that overlaps a save must not park its older engine
/// under the newer flush: the next session would read and save stale state.
#[test]
fn an_overlapping_session_never_parks_a_stale_engine() -> Result<()> {
    futures::executor::block_on(async {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let cache = fresh_namespace();
        let db = Db::open(store.clone(), owner(&cache)).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").await?;
        db.save().await?;

        let reader = Db::open(store.clone(), owner(&cache)).await?;
        assert_eq!(count(&reader).await?, 0);
        let writer = Db::open(store.clone(), owner(&cache)).await?;
        writer.execute("INSERT INTO t VALUES (1)").await?;
        writer.save().await?;
        drop(reader);

        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert_eq!(count(&db).await?, 1);
        db.execute("INSERT INTO t VALUES (2)").await?;
        db.save().await?;
        let cold = Db::open(store, StateOptions::default()).await?;
        assert_eq!(count(&cold).await?, 2, "no flush lost");
        Ok(())
    })
}

/// Settings, attachments and the temp schema belong to a connection; a session
/// that changes them does not hand its engine to the next one.
#[test]
fn connection_state_never_reaches_the_next_session() -> Result<()> {
    futures::executor::block_on(async {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let cache = fresh_namespace();
        let db = Db::open(store.clone(), owner(&cache)).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").await?;
        db.save().await?;

        let db = Db::open(store.clone(), owner(&cache)).await?;
        db.execute("PRAGMA query_only = 1").await?;
        drop(db);
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(!db.reused_engine());
        db.execute("INSERT INTO t VALUES (1)").await?;
        db.execute("CREATE TEMP TABLE scratch(x)").await?;
        db.save().await?;

        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert!(!db.reused_engine());
        assert!(db.query("SELECT * FROM scratch").await.is_err());
        drop(db);
        // Ordinary SQL keeps the engine warm.
        let db = Db::open(store.clone(), owner(&cache)).await?;
        assert_eq!(count(&db).await?, 1);
        drop(db);
        let db = Db::open(store, owner(&cache)).await?;
        assert!(db.reused_engine());
        Ok(())
    })
}

/// A reused engine starts with fresh connection counters, exactly like a cold
/// one, so the same SQL gives the same answer either way.
#[test]
fn a_reused_engine_has_fresh_connection_counters() -> crate::state::Result<()> {
    futures::executor::block_on(async {
        let store = std::sync::Arc::new(crate::state::simulation::MemoryKv::default());
        let options = StateSetup {
            options: crate::state::StateOptions::default(),
            cache: Some(crate::state::simulation::fresh_namespace()),
            packs: None,
        };
        let db = Db::open(store.clone(), options.clone()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").await?;
        db.execute("INSERT INTO t VALUES(42)").await?;
        db.save().await?;
        let counters = "SELECT last_insert_rowid(), changes(), total_changes()";
        let warm = Db::open(store.clone(), options).await?;
        assert!(warm.reused_engine());
        let cold = Db::open(store, StateOptions::default()).await?;
        assert_eq!(warm.query(counters).await?, cold.query(counters).await?);
        assert_eq!(warm.query(counters).await?[0][0].as_int(), Some(0));
        Ok(())
    })
}

/// Evicting a namespace's state from the LRU while a session still holds its
/// handle must not give the next session a second, independent state: the two
/// would not see each other's flushes and could both write one log position.
#[test]
fn an_evicted_namespace_keeps_one_state_while_in_use() -> Result<()> {
    use crate::state::simulation::{cache_config, open_cache};
    futures::executor::block_on(async {
        let cache = open_cache(crate::state::PageCacheConfig {
            namespace_bytes: 1,
            ..cache_config(1 << 20, 64 << 20)
        });
        let store = Arc::new(MemoryKv::default());
        let options = |namespace: Namespace| StateSetup {
            options: StateOptions::default(),
            cache: Some(namespace),
            packs: None,
        };
        let first = Db::open(store.clone(), options(Namespace::new(&cache, "a"))).await?;
        first
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            .await?;
        for other in 0..1000 {
            Namespace::new(&cache, &format!("other-{other}")); // evicts "a"
        }
        let second = Db::open(store.clone(), options(Namespace::new(&cache, "a"))).await?;
        second.execute("INSERT INTO t VALUES(1)").await?;
        assert!(first.execute("INSERT INTO t VALUES(2)").await.is_err());
        drop((first, second));
        let db = Db::open(store, StateOptions::default()).await?;
        assert_eq!(db.query("SELECT id FROM t").await?.len(), 1);
        Ok(())
    })
}

/// A parked engine is weighed by its schema: one with a large schema takes the
/// room of several small ones.
#[test]
fn parked_engines_are_weighed_by_schema() -> Result<()> {
    use crate::state::{
        simulation::{cache_config, open_cache},
        PageCacheConfig,
    };
    futures::executor::block_on(async {
        let cache = open_cache(PageCacheConfig {
            parked_bytes: (16 << 20) + (100 << 10), // four engines with empty schemas
            ..cache_config(1 << 20, 16 << 20)
        });
        let open = |name: &str, store: &Arc<dyn KvStore>| {
            Db::open(store.clone(), owner(&Namespace::new(&cache, name)))
        };
        let small: Vec<Arc<dyn KvStore>> = (0..4)
            .map(|_| Arc::new(MemoryKv::default()) as Arc<dyn KvStore>)
            .collect();
        for (i, store) in small.iter().enumerate() {
            drop(open(&format!("small-{i}"), store).await?);
        }
        for (i, store) in small.iter().enumerate() {
            let db = open(&format!("small-{i}"), store).await?;
            assert!(db.reused_engine(), "four small engines fit");
        }

        let big: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let db = open("big", &big).await?;
        for t in 0..200 {
            db.execute(&format!(
                "CREATE TABLE t{t}(id INTEGER PRIMARY KEY, a TEXT, b TEXT); CREATE INDEX i{t} ON t{t}(a, b)"
            ))
            .await?;
        }
        db.save().await?; // parks, weighed by its schema
        let mut kept = 0;
        for (i, store) in small.iter().enumerate() {
            kept += usize::from(open(&format!("small-{i}"), store).await?.reused_engine());
        }
        assert!(kept < 4, "the big engine displaced small ones: {kept} kept");
        Ok(())
    })
}

/// A small cache, as `PageCacheConfig::sized` makes one (as the addon does
/// for `RESTATE_SQL_CACHE_MEMORY` and `RESTATE_SQL_CACHE_DISK`), opens with
/// its small disk blocks, and what it cannot hold is read from the store,
/// right. (Its memory is not a strict bound at this size: a frame may be
/// larger than a shard's share of it.)
#[test]
fn a_small_cache_reads_right() -> Result<()> {
    use crate::state::{simulation::open_cache, PageCacheConfig};
    futures::executor::block_on(async {
        let cache = open_cache(PageCacheConfig::sized(64 << 10, 256 << 10));
        let store = Arc::new(LatencyKv::new(MemoryKv::default()));
        let namespace = Namespace::new(&cache, "small");
        create(store.clone(), cached(&namespace)).await?;
        let expected = 400 * 900 + 400 * 401 / 2;
        for _ in 0..3 {
            assert_eq!(
                payload_sum(store.clone(), cached(&namespace)).await?,
                expected
            );
        }
        Ok(())
    })
}

/// The cache's disk file, lost or damaged under a running process, never
/// makes a read wrong: what the cache cannot serve is read from the store.
#[test]
fn a_damaged_cache_file_never_makes_a_read_wrong() -> Result<()> {
    use crate::state::{simulation::open_cache, PageCacheConfig};
    use std::io::{Seek, Write};
    type Damage = (&'static str, fn(&std::path::Path));
    let damages: [Damage; 3] = [
        ("truncated", |file| {
            std::fs::OpenOptions::new()
                .write(true)
                .open(file)
                .unwrap()
                .set_len(0)
                .unwrap();
        }),
        ("overwritten", |file| {
            let mut f = std::fs::OpenOptions::new().write(true).open(file).unwrap();
            let len = f.metadata().unwrap().len();
            f.seek(std::io::SeekFrom::Start(0)).unwrap();
            // Not zeros: bytes that look like data.
            let garbage: Vec<u8> = (0..len)
                .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
                .collect();
            f.write_all(&garbage).unwrap();
            f.sync_all().unwrap();
        }),
        ("deleted", |file| std::fs::remove_file(file).unwrap()),
    ];
    for (how, damage) in damages {
        futures::executor::block_on(async {
            let dir =
                std::env::temp_dir().join(format!("cache-damage-{how}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let file = dir.join("cache");
            // Memory for a few frames: the table goes to the disk tier.
            let cache = open_cache(PageCacheConfig {
                file: Some(file.clone()),
                ..crate::state::simulation::cache_config(64 << 10, 64 << 20)
            });
            let store = Arc::new(LatencyKv::new(MemoryKv::default()));
            let namespace = Namespace::new(&cache, &format!("damaged-{how}"));
            create(store.clone(), cached(&namespace)).await?;
            let expected = 400 * 900 + 400 * 401 / 2;
            // Served by the cache, its disk tier included: the table is larger than its memory.
            store.inner.reset_stats();
            assert_eq!(
                payload_sum(store.clone(), cached(&namespace)).await?,
                expected,
                "{how}: before"
            );
            assert_eq!(
                store.inner.stats().kind("pack").gets,
                0,
                "{how}: the cache serves the table before the damage"
            );
            damage(&file);
            for round in 0..2 {
                assert_eq!(
                    payload_sum(store.clone(), cached(&namespace)).await?,
                    expected,
                    "{how}: read {round} after the damage"
                );
            }
            drop(cache);
            let _ = std::fs::remove_dir_all(&dir);
            Result::Ok(())
        })?;
    }
    Ok(())
}
