use super::*;
use crate::state::simulation::MemoryKv;
use futures::{
    future::{select, Either},
    FutureExt,
};

/// A session of the database in `store`, without the page cache.
async fn open(store: Arc<MemoryKv>) -> Result<Database> {
    Database::new(crate::state::State::open(store).await?).await
}

async fn baseline() -> Result<MemoryKv> {
    let store = MemoryKv::default();
    let db = open(Arc::new(store.clone())).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, payload TEXT)")
        .await?;
    db.execute("INSERT INTO t VALUES(1,10,printf('%09000d',1))")
        .await?;
    db.save().await?;
    store.crash();
    Ok(store)
}

const UPDATE: &str = "UPDATE t SET n=20, payload=printf('%09000d',2)";

/// A session in a transaction that changed the row.
async fn changed(store: MemoryKv) -> Result<Database> {
    let db = open(Arc::new(store)).await?;
    db.begin().await?;
    db.execute(UPDATE).await?;
    Ok(db)
}

/// A commit that fails or is cancelled at any local I/O (the engine's
/// `COMMIT`, its checkpoint, the move of its pages) publishes nothing.
#[test]
fn interrupted_checkpoint_never_publishes() -> Result<()> {
    futures::executor::block_on(async {
        let base = baseline().await?;
        let probe = changed(base.fork()).await?;
        let io = probe.io.clone();
        let _unused = io.arm_fault(usize::MAX, false, false);
        probe.commit([]).await?;
        let count = io.fault_ops();
        assert!(count > 0);
        for at in 1..=count {
            for after in [false, true] {
                for cancel in [false, true] {
                    let store = base.fork();
                    let db = changed(store.clone()).await?;
                    store.reset_stats();
                    let gate = db.io.arm_fault(at, after, cancel);
                    if cancel {
                        match select(db.commit([]).boxed(), gate).await {
                            Either::Left(_) => panic!("checkpoint missed cancellation point"),
                            Either::Right((signal, pending)) => {
                                signal?;
                                drop(pending);
                            }
                        }
                    } else {
                        assert!(db
                            .commit([])
                            .await
                            .unwrap_err()
                            .to_string()
                            .contains("injected checkpoint"));
                    }
                    assert_eq!(store.stats().puts, 0);
                    store.crash();
                    let db = open(Arc::new(store)).await?;
                    assert_eq!(db.query("SELECT n FROM t").await?[0][0].to_string(), "10");
                    assert_eq!(
                        db.query("PRAGMA integrity_check").await?[0][0].to_string(),
                        "ok"
                    );
                }
            }
        }
        println!(
            "checkpoint: {} interruption cases across {count} local I/O operations",
            count * 4
        );
        Ok(())
    })
}

/// The adapter moves pages into a mutation only once the engine WAL is
/// checkpointed; refused, it leaves the mutation empty.
#[test]
fn a_flush_into_a_mutation_rejects_an_engine_wal_not_checkpointed() -> Result<()> {
    futures::executor::block_on(async {
        let store = baseline().await?;
        let db = open(Arc::new(store.clone())).await?;
        // Committed in the engine alone, as no call of the session leaves it.
        let mut session = db.shared.session.lock().await;
        let engine = session.engine()?;
        engine.execute(UPDATE).await?;
        assert!(engine.io.wal_len() > 0);
        store.reset_stats();
        let mut mutation = db.state().mutation();
        let error = engine.io.flush_into(&mut mutation).unwrap_err().to_string();
        assert!(
            error.contains("the engine WAL is not checkpointed"),
            "{error}"
        );
        assert!(mutation.is_empty());
        assert_eq!(store.stats().puts, 0);
        Ok(())
    })
}

#[test]
fn cached_queries_yield_to_host_and_cancellation_poisons_the_view() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = open(store.clone()).await?;
        db.execute("CREATE TABLE t(n INTEGER)").await?;
        db.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200) INSERT INTO t SELECT x FROM n").await?;
        let expected = db.query("SELECT n FROM t ORDER BY n").await?;
        store.reset_stats();
        let mut query = Box::pin(db.query("SELECT n FROM t ORDER BY n"));
        assert!(
            futures::poll!(query.as_mut()).is_pending(),
            "cached rows must still give the host a scheduling point"
        );
        assert_eq!(store.stats().gets, 0);
        assert_eq!(query.await?, expected);
        let mut query = Box::pin(db.query("SELECT n FROM t ORDER BY n"));
        assert!(futures::poll!(query.as_mut()).is_pending());
        drop(query);
        assert_eq!(db.io.pending_count(), 0);
        assert!(db
            .query("SELECT 1")
            .await
            .unwrap_err()
            .to_string()
            .contains("the session ended"));
        assert_eq!(store.stats().puts, 0);
        Ok(())
    })
}

#[test]
fn host_driver_handles_cold_schema_stats_and_cache_eviction() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = open(store.clone()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, payload TEXT)")
            .await?;
        db.execute("CREATE INDEX by_n ON t(n)").await?;
        db.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100) INSERT INTO t SELECT x,x,printf('%0500d',x) FROM n").await?;
        for n in 0..48 {
            db.execute(&format!("CREATE TABLE schema_{n}(a INTEGER, b TEXT, c TEXT DEFAULT 'some schema text to span multiple pages')")).await?;
        }
        db.execute("ANALYZE").await?;
        db.save().await?;
        store.crash();
        let db = open(store.clone()).await?;
        assert!(!db.query("SELECT * FROM sqlite_stat1").await?.is_empty());
        db.execute("PRAGMA cache_size=5").await?;
        db.execute("PRAGMA shrink_memory").await?;
        assert_eq!(
            db.query("PRAGMA encoding").await?[0][0].to_string(),
            "UTF-8"
        );
        assert_eq!(
            db.query("PRAGMA page_size").await?[0][0].as_int(),
            Some(4096)
        );
        assert_eq!(
            db.query("SELECT id FROM t WHERE n=75").await?[0][0].as_int(),
            Some(75)
        );
        db.execute("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 11")
            .await?;
        db.execute("UPDATE t SET extra=12 WHERE id=75").await?;
        db.execute("DROP INDEX by_n").await?;
        db.checkpoint().await?;
        db.save().await?;
        let db = open(store).await?;
        assert_eq!(
            db.query("SELECT extra FROM t WHERE id=75").await?[0][0].as_int(),
            Some(12)
        );
        assert_eq!(
            db.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
        Ok(())
    })
}

#[test]
fn mentions_are_whole_words_in_any_case() {
    let none = Mentions::of("SELECT created, dropped, templates, rollbacks FROM t WHERE a = 'x'");
    assert!(!none.connection_state && !none.schema && !none.rollback);
    let all = Mentions::of("pragma x; Attach d; DETACH e; TEMP t; TeMporary u");
    assert!(all.connection_state && !all.schema && !all.rollback);
    for (sql, schema) in [
        ("CREATE TABLE t(a)", true),
        ("drop table t", true),
        ("ALTER TABLE t", true),
        ("SELECT 1", false),
    ] {
        assert_eq!(Mentions::of(sql).schema, schema, "{sql}");
    }
    assert!(Mentions::of("ROLLBACK TO s").rollback);
    // A word is a run of letters, digits and underscores: `temp_x` is not `temp`.
    assert!(!Mentions::of("SELECT temp_x, my_drop FROM t").connection_state);
    assert!(Mentions::of("SELECT * FROM temp.t").connection_state);
}

mod timing {
    use super::*;
    use std::task::Poll;

    /// What a statement's run is measured by: the longest single poll of it,
    /// which the time it waits between polls does not add to.
    #[test]
    fn timed_gives_the_longest_poll() {
        let stall = |millis, polls| {
            let mut polls = polls;
            std::future::poll_fn(move |cx| {
                std::thread::sleep(Duration::from_millis(millis));
                match polls {
                    0 => Poll::Ready(()),
                    _ => {
                        polls -= 1;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            })
        };
        let ((), longest) = futures::executor::block_on(timed(stall(30, 2)));
        // Three polls of 30 ms are 30 ms, not 90.
        assert!(
            (30..80).contains(&(longest.as_millis() as u64)),
            "{longest:?}"
        );
        let ((), longest) = futures::executor::block_on(timed(stall(1, 0)));
        assert!(longest < Duration::from_millis(30), "{longest:?}");
    }
}

mod quick {
    use super::*;
    use crate::sql::quick_texts::{Record, RELEARN};

    const QUICK: Duration = Duration::from_micros(10);
    const SLOW: Duration = QUICK_IN_PLACE;

    #[test]
    fn a_text_is_known_after_one_quick_run() {
        let mut record = Record::default();
        assert!(!record.known("SELECT 1"));
        record.observe("SELECT 1", QUICK);
        assert!(record.known("SELECT 1"));
        assert!(!record.known("SELECT 2"));
    }

    /// The bound is on the longest poll: a run that reaches it is slow.
    #[test]
    fn a_text_whose_first_run_was_slow_is_not_known() {
        let mut record = Record::default();
        record.observe("SELECT 1", SLOW);
        record.observe("SELECT 2", QUICK_IN_PLACE - Duration::from_nanos(1));
        assert!(!record.known("SELECT 1"));
        assert!(record.known("SELECT 2"));
    }

    #[test]
    fn a_slow_run_forgets_a_known_text_until_it_runs_quickly_again() {
        let mut record = Record::default();
        record.observe("SELECT 1", QUICK);
        record.observe("SELECT 1", SLOW);
        assert!(!record.known("SELECT 1"));
        for _ in 1..RELEARN {
            record.observe("SELECT 1", QUICK);
            assert!(!record.known("SELECT 1"));
        }
        record.observe("SELECT 1", QUICK);
        assert!(record.known("SELECT 1"));
        // Known, a quick run changes nothing.
        record.observe("SELECT 1", QUICK);
        assert!(record.known("SELECT 1"));
    }

    #[test]
    fn the_quick_runs_that_make_a_text_known_again_are_consecutive() {
        let mut record = Record::default();
        record.observe("SELECT 1", QUICK);
        record.observe("SELECT 1", SLOW);
        for _ in 1..RELEARN {
            record.observe("SELECT 1", QUICK);
        }
        record.observe("SELECT 1", SLOW);
        for _ in 1..RELEARN {
            record.observe("SELECT 1", QUICK);
        }
        assert!(!record.known("SELECT 1"), "the count started over");
        record.observe("SELECT 1", QUICK);
        assert!(record.known("SELECT 1"));
    }

    #[test]
    fn a_full_record_is_cleared_and_learns_again() {
        let mut record = Record::default();
        for n in 0..8192 {
            record.observe(&format!("SELECT {n}"), QUICK);
        }
        assert_eq!(record.len(), 8192);
        assert!(record.known("SELECT 0"));
        record.observe("SELECT new", QUICK);
        assert!(record.known("SELECT new"));
        assert_eq!(record.len(), 1);
        assert!(!record.known("SELECT 0"));
        // And the texts it forgot learn again, one quick run each.
        record.observe("SELECT 0", QUICK);
        assert!(record.known("SELECT 0"));
    }

    #[test]
    fn a_text_too_long_to_record_is_never_known() {
        let mut record = Record::default();
        let long = format!("SELECT '{}'", "x".repeat(600));
        record.observe(&long, QUICK);
        assert!(!record.known(&long));
        assert_eq!(record.len(), 0);
    }
}
