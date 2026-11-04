//! Every SQL commit is durable when its statement returns.
use super::host::{Db, StateSetup};
use crate::state::simulation::Namespace;
use crate::state::{
    simulation::{
        fresh_namespace, Effect, Failpoint, FaultKv, LatencyKv, MemoryKv, MonitorKv, Operation,
    },
    Result, StateOptions,
};
use bytes::Bytes;
use std::sync::Arc;

fn options(cache: Option<Namespace>) -> StateSetup {
    StateSetup {
        options: StateOptions::default(),
        cache,
        packs: None,
    }
}

async fn count(db: &Db) -> Result<i64> {
    let rows = db
        .query("SELECT count(*), coalesce(sum(n), 0) FROM t")
        .await?;
    let count = rows[0][0].as_int().unwrap();
    assert_eq!(rows[0][1].as_int(), Some(0), "pairs commit together");
    Ok(count)
}

#[test]
fn statements_are_durable_without_save() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), options(None)).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        assert_eq!(
            store.stats().kind("head").puts,
            1,
            "one flush per statement"
        );
        db.execute("INSERT INTO t VALUES(1, 0)").await?;
        assert_eq!(store.stats().kind("head").puts, 2);
        db.query("SELECT * FROM t").await?;
        assert_eq!(store.stats().kind("head").puts, 2, "reads flush nothing");
        drop(db);
        store.crash();

        let db = Db::open(store.clone(), options(None)).await?;
        assert_eq!(count(&db).await?, 1);
        Ok(())
    })
}

#[test]
fn a_transaction_commits_as_a_whole_or_not_at_all() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), options(None)).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        let before = store.stats().kind("head").puts;
        let id = db
            .transaction(async |db| {
                db.execute("INSERT INTO t VALUES(1, 5)").await?;
                db.execute("INSERT INTO t VALUES(2, -5)").await?;
                Ok(2)
            })
            .await?;
        assert_eq!(id, 2);
        assert_eq!(store.stats().kind("head").puts, before + 1, "one flush");

        let failed: Result<()> = db
            .transaction(async |db| {
                db.execute("INSERT INTO t VALUES(3, 7)").await?;
                Err("application error".into())
            })
            .await;
        assert!(failed.is_err());
        assert_eq!(store.stats().kind("head").puts, before + 1, "rolled back");
        assert_eq!(count(&db).await?, 2, "the session continues");

        // An open transaction is all that dropping a session loses.
        db.execute("BEGIN").await?;
        db.execute("INSERT INTO t VALUES(4, 1)").await?;
        drop(db);
        store.crash();
        let db = Db::open(store.clone(), options(None)).await?;
        assert_eq!(count(&db).await?, 2);
        Ok(())
    })
}

/// A long session crosses a checkpoint every eight transactions and
/// collects garbage every five, reading its own writes throughout.
#[test]
fn a_long_session_collects_garbage_and_reads_its_writes() -> Result<()> {
    futures::executor::block_on(async {
        for cache in [None, Some(fresh_namespace())] {
            let store = Arc::new(MemoryKv::default());
            let options = || {
                options(cache.clone()).with(|o| StateOptions {
                    checkpoint_entries: 8,
                    ..o
                })
            };
            let db = Db::open(store.clone(), options()).await?;
            db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .await?;
            for i in 0..60i64 {
                db.transaction(async |db| {
                    db.execute(&format!(
                        "INSERT INTO t VALUES({}, {i}, randomblob(900))",
                        2 * i
                    ))
                    .await?;
                    db.execute(&format!("INSERT INTO t VALUES({}, {}, 'x')", 2 * i + 1, -i))
                        .await
                })
                .await?;
                assert_eq!(count(&db).await?, 2 * (i + 1));
                if i % 5 == 4 {
                    db.state().collect_garbage().await?;
                }
            }
            let (packs, needed) = (store.packs(), db.state().needed_packs().await?);
            assert!(
                store.stats().kind("pack").deletes > 0 && packs == needed,
                "garbage is collected as the session goes: {packs:?}, {needed:?}"
            );
            drop(db);
            store.crash();

            let db = Db::open(store.clone(), options()).await?;
            assert_eq!(count(&db).await?, 120);
            assert_eq!(
                db.query("PRAGMA integrity_check").await?[0][0].to_string(),
                "ok"
            );
        }
        Ok(())
    })
}

/// With an owner cache, a write transaction costs one round trip: its sync.
/// (One that checkpoints lists the keys too, to collect garbage: off here.)
#[test]
fn a_flush_is_one_round_trip_with_a_cache() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(LatencyKv::new(MemoryKv::default()));
        let cache = fresh_namespace();
        let options = options(Some(cache)).with(|o| StateOptions {
            checkpoint_entries: 0,
            checkpoint_bytes: 0,
            inline_record_bytes: usize::MAX,
            ..o
        });
        let db = Db::open(store.clone(), options).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        for i in 0..20 {
            store.reset();
            db.execute(&format!("INSERT INTO t VALUES({i}, 0)")).await?;
            assert_eq!(store.round_trips(), 1, "flush {i}");
        }
        store.reset();
        assert_eq!(count(&db).await?, 20);
        assert_eq!(store.round_trips(), 0, "reads are served locally");
        Ok(())
    })
}

/// Crash a flush-per-transaction run at every sync, keeping each prefix of
/// the writes the sync would have stored. Every transaction acknowledged
/// before the crash survives, the one it interrupted may or may not, and
/// nothing else appears.
#[test]
fn every_acknowledged_commit_survives_a_crash_at_any_write() -> Result<()> {
    futures::executor::block_on(async {
        let mut cases = 0;
        for cached in [false, true] {
            for nth in 1.. {
                let Some(prefixes) = run_case(cached, nth).await? else {
                    break;
                };
                cases += prefixes;
            }
        }
        println!("commit campaign: {cases} cases");
        assert!(cases > 100, "{cases} cases");
        Ok(())
    })
}

/// Crash at the `nth` sync, keeping each prefix of its writes in turn: the
/// number of prefixes, or `None` if the run made fewer syncs.
async fn run_case(cached: bool, nth: usize) -> Result<Option<usize>> {
    let cache = cached.then(fresh_namespace);
    // Checkpoints, and records stored beside the manifest, happen within the
    // run.
    let options = |cache| {
        options(cache).with(|o| StateOptions {
            checkpoint_entries: 8,
            inline_record_bytes: 512,
            ..o
        })
    };
    let store = Arc::new(MonitorKv::new(FaultKv::new(
        MemoryKv::default(),
        nth as u64,
    )));
    let db = Db::open(store.clone(), options(cache.clone())).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
        .await?;
    let _signal = store.arm(Failpoint {
        operation: Operation::Sync,
        key_prefix: String::new(),
        nth,
        effect: Effect::None,
        cancel: false,
    });
    let mut acknowledged = 0;
    for i in 0..30i64 {
        let result = db
            .transaction(async |db| {
                db.execute(&format!(
                    "INSERT INTO t VALUES({}, {i}, zeroblob(700))",
                    2 * i
                ))
                .await?;
                db.execute(&format!("INSERT INTO t VALUES({}, {}, 'x')", 2 * i + 1, -i))
                    .await
            })
            .await;
        match result {
            Ok(()) => acknowledged += 2,
            Err(_) => break,
        }
    }
    drop(db);
    store.disarm();
    if !store.events().iter().any(|event| event.faulted) {
        return Ok(None);
    }
    assert_eq!(store.violations(), Vec::<String>::new(), "sync #{nth}");
    let writes = store.inner.inner.unsynced().len();
    for kept in 0..=writes {
        let memory = store.inner.inner.fork();
        memory.crash_keeping(kept);
        let store = Arc::new(MonitorKv::seeded(memory.clone(), memory.values()));
        // Reopen through the same owner cache, as a restarted handler would:
        // only to read, as the cache follows one history, and the forks are
        // each another.
        let db = Db::open(store.clone(), options(cache.clone())).await?;
        let survived = count(&db).await?;
        assert!(
            survived == acknowledged || survived == acknowledged + 2,
            "sync #{nth}, {kept} of {writes} writes kept, cached={cached}: \
             {survived} rows, {acknowledged} acknowledged"
        );
        assert_eq!(
            db.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
        drop(db);
        // The namespace stays writable.
        let db = Db::open(store.clone(), options(None)).await?;
        db.execute("INSERT INTO t VALUES(1000, 0, 'y')").await?;
        drop(db);
        let db = Db::open(store.clone(), options(None)).await?;
        assert_eq!(count(&db).await?, survived + 1);
        assert_eq!(
            store.violations(),
            Vec::<String>::new(),
            "sync #{nth}, {kept} kept"
        );
    }
    Ok(Some(writes + 1))
}

/// A string of several statements runs all of them, each under the same rules
/// as on its own, and returns the rows of the last.
#[test]
fn every_statement_of_a_string_runs() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let options = || options(None);
        let db = Db::open(store.clone(), options()).await?;
        let rows = db
            .query(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER);
                 INSERT INTO t VALUES(1, 0); INSERT INTO t VALUES(2, 0);
                 SELECT count(*) FROM t; -- trailing comment",
            )
            .await?;
        assert_eq!(rows[0][0].as_int(), Some(2));
        db.execute("BEGIN; INSERT INTO t VALUES(3, 0); COMMIT")
            .await?;
        assert!(db.execute("  -- nothing\n ").await.is_err());
        drop(db);
        store.crash();
        let db = Db::open(store, options()).await?;
        assert_eq!(count(&db).await?, 3);
        Ok(())
    })
}

/// A transaction body may not end its transaction: the statement fails, the
/// session is poisoned, and nothing of the body becomes durable.
#[test]
fn a_transaction_body_cannot_commit_itself() -> Result<()> {
    futures::executor::block_on(async {
        for end in ["COMMIT", "END", "ROLLBACK", "COMMIT; BEGIN"] {
            let store = Arc::new(MemoryKv::default());
            let db = Db::open(store.clone(), options(None)).await?;
            db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
                .await?;
            let result: Result<()> = db
                .transaction(async |db| {
                    db.execute("INSERT INTO t VALUES(1, 0)").await?;
                    db.execute(end).await?;
                    Ok(())
                })
                .await;
            assert!(result.is_err(), "{end}");
            assert!(db.execute("SELECT 1").await.is_err(), "{end}: poisoned");
            drop(db);
            store.crash();
            let db = Db::open(store.clone(), options(None)).await?;
            assert_eq!(count(&db).await?, 0, "{end}");
        }

        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store, options(None)).await?;
        let nested: Result<()> = db
            .transaction(async |db| super::host::transaction(db, async |_| Ok(())).await)
            .await;
        assert!(nested.unwrap_err().to_string().contains("do not nest"));
        Ok(())
    })
}

/// A session that only reads, open while another flushes, keeps reading the
/// state it opened at: the packs its head names stay until a collection
/// after the next checkpoint but one.
#[test]
fn an_overlapping_reader_keeps_its_view() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let namespace = fresh_namespace();
        let opts = || {
            options(Some(namespace.clone())).with(|o| StateOptions {
                checkpoint_entries: 16,
                prefetch_pages: 0,
                ..o
            })
        };
        let db = Db::open(store.clone(), opts()).await?;
        db.execute(
            "CREATE TABLE a(x); CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, pad BLOB)",
        )
        .await?;
        db.transaction(async |db| {
            for i in 0..200 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, 1, zeroblob(100))"))
                    .await?;
            }
            Ok(())
        })
        .await?;
        for i in 0..8 {
            db.execute(&format!("INSERT INTO a VALUES ({i})")).await?;
        }
        drop(db);

        let reader = Db::open(store.clone(), opts()).await?;
        reader.query("SELECT count(*) FROM a").await?;
        let writer = Db::open(store.clone(), opts()).await?;
        for _ in 0..10 {
            writer.execute("UPDATE t SET n = n + 1").await?;
        }
        let rows = reader.query("SELECT count(*), sum(n) FROM t").await?;
        assert_eq!(
            (rows[0][0].as_int(), rows[0][1].as_int()),
            (Some(200), Some(200))
        );
        drop(reader);
        let rows = writer.query("SELECT sum(n) FROM t").await?;
        assert_eq!(rows[0][0].as_int(), Some(200 * 11));
        Ok(())
    })
}

/// Two sessions of one owner that both write: the second flush would replace
/// the head the first wrote, so it is refused, and so are the records set
/// with it.
#[test]
fn a_second_concurrent_writer_is_refused() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let namespace = fresh_namespace();
        let opts = || options(Some(namespace.clone()));
        let db = Db::open(store.clone(), opts()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        db.save().await?;
        let first = Db::open(store.clone(), opts()).await?;
        let second = Db::open(store.clone(), opts()).await?;
        first.execute("INSERT INTO t VALUES(1, 0)").await?;
        second.begin().await?;
        second.execute("INSERT INTO t VALUES(2, 0)").await?;
        let refused = second.commit_with("sends".into()).await;
        assert!(refused.unwrap_err().to_string().contains("another state"));
        let db = Db::open(store.clone(), opts()).await?;
        assert_eq!(count(&db).await?, 1);
        assert_eq!(db.outbox(), "");
        Ok(())
    })
}

#[test]
fn records_commit_with_their_transaction() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), options(None)).await?;
        db.execute("PRAGMA foreign_keys=ON").await?;
        db.execute(
            "CREATE TABLE p(id INTEGER PRIMARY KEY);
             CREATE TABLE c(pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        )
        .await?;
        assert!(db.outbox().is_empty());
        let v0 = db.version();
        // A record commits even with a transaction that changes nothing...
        db.execute("BEGIN").await?;
        db.query("SELECT * FROM p").await?;
        db.commit_with(Bytes::from_static(b"sends 1")).await?;
        assert_eq!(db.outbox(), &b"sends 1"[..]);
        assert_eq!(db.version(), v0 + 1);
        // ...but the same record again, without changes, flushes nothing.
        db.execute("BEGIN").await?;
        db.commit_with(Bytes::from_static(b"sends 1")).await?;
        assert_eq!(db.version(), v0 + 1);
        // A COMMIT that fails commits neither.
        db.execute("BEGIN").await?;
        db.execute("INSERT INTO c VALUES(7)").await?;
        assert!(db
            .commit_with(Bytes::from_static(b"sends 2"))
            .await
            .is_err());
        db.execute("ROLLBACK").await?;
        assert_eq!(db.outbox(), &b"sends 1"[..]);
        // Later flushes without records keep it.
        for i in 0..10 {
            db.execute(&format!("INSERT INTO p VALUES({i})")).await?;
        }
        assert!(db.is_usable());
        drop(db);
        store.crash();

        let db = Db::open(store.clone(), options(None)).await?;
        assert_eq!(db.outbox(), &b"sends 1"[..]);
        assert_eq!(db.version(), v0 + 11);
        assert_eq!(
            db.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
        // An empty value clears it.
        db.execute("BEGIN").await?;
        db.commit_with(Bytes::new()).await?;
        drop(db);
        let db = Db::open(store.clone(), options(None)).await?;
        assert!(db.outbox().is_empty());
        Ok(())
    })
}

/// A commit whose flush the state refuses (more names than a state holds)
/// writes nothing, and ends the session: its records read as stored, not as
/// refused, and a save does not claim the dropped changes stored. The error
/// says a retry would repeat it.
#[test]
fn a_refused_flush_ends_the_session_with_nothing_stored() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), options(None)).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        db.begin().await?;
        db.execute("INSERT INTO t VALUES(1, 0)").await?;
        // A state holds at most 2^16 names.
        let records = (0..=1 << 16)
            .map(|i| (format!("r{i}"), Bytes::from_static(b"x")))
            .chain([("outbox".to_owned(), Bytes::from_static(b"sends"))]);
        let refused = db.commit(records.collect::<Vec<_>>()).await.unwrap_err();
        assert!(refused.is::<crate::sql::Refused>(), "{refused}");
        assert!(refused.to_string().contains("files and records"));
        assert!(!db.is_usable());
        assert_eq!(db.record("outbox").await?, "");
        assert!(db.save().await.is_err());

        let db = Db::open(store.clone(), options(None)).await?;
        assert_eq!(count(&db).await?, 0);
        assert_eq!(db.outbox(), "");
        Ok(())
    })
}
