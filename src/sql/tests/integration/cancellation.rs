//! Cancellation at every point: a call dropped after any number of polls
//! ends its session, and the store holds the state before it or after it.
use super::{
    fault::poll_at_most,
    host::{Db, StateSetup},
    reference::{dump, integrity},
};
use crate::sql::Database;
use crate::state::simulation::Namespace;
use crate::state::{
    kv::KvStore,
    simulation::{fresh_namespace, MemoryKv},
    Result, StateOptions,
};
use bytes::Bytes;
use std::sync::Arc;

/// Pages read from the store as queries need them; garbage deleted soon.
fn options(cache: Option<Namespace>) -> StateSetup {
    StateSetup {
        options: StateOptions {
            checkpoint_entries: 2,
            prefetch_pages: 0,
            ..StateOptions::default()
        },
        cache,
        packs: None,
    }
}

/// A store with a table of 20 rows, some past a page, after a few flushes.
async fn baseline() -> Result<MemoryKv> {
    let memory = MemoryKv::default();
    let db = Db::open(Arc::new(memory.clone()), options(None)).await?;
    db.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT); CREATE INDEX t_n ON t(n)",
    )
    .await?;
    db.execute("WITH RECURSIVE x(i) AS (VALUES(0) UNION ALL SELECT i + 1 FROM x WHERE i < 19) INSERT INTO t SELECT i, i, substr(printf('%07000d', i), 1, 100 + (i % 5) * 1500) FROM x")
        .await?;
    db.begin().await?;
    for k in 0..3 {
        db.execute(&format!("CREATE TABLE a{k}(x)")).await?;
    }
    db.commit_with(Bytes::from("before")).await?;
    // Small flushes: tail entries, and checkpoints that leave garbage.
    for k in 0..6 {
        db.execute(&format!("UPDATE t SET n = {k} WHERE id = 1"))
            .await?;
    }
    Ok(memory)
}

/// The state of a store and its outbox, through a plain session; its
/// database checks out.
async fn observe(store: MemoryKv) -> Result<(String, Bytes)> {
    let db = Database::new(options(None).open(Arc::new(store)).await?).await?;
    assert_eq!(integrity(&db).await?, "ok");
    Ok((dump(&db).await?, db.record("outbox").await?))
}

/// A call to cancel, on a session it opens and prepares.
#[derive(Clone, Copy, Debug)]
enum Call {
    /// A write, which commits before it returns.
    Write,
    /// A read.
    Read,
    /// The `COMMIT` of a transaction, with a record; a change in it leaves the
    /// first page alone, and the read after it reads that page again, from
    /// the store.
    Commit,
}

const UPDATE: &str = "UPDATE t SET n = n + 1000, body = printf('%07000d', id) WHERE id % 7 = 0";

impl Call {
    async fn open(self, store: MemoryKv, cache: Option<Namespace>) -> Result<Db> {
        let store: Arc<dyn KvStore> = Arc::new(store);
        let db = Db::open(store, options(cache)).await?;
        if let Call::Commit = self {
            db.begin().await?;
            db.execute("INSERT INTO a0 VALUES(1)").await?;
            db.execute("SELECT sum(length(body)) FROM t").await?;
            db.execute(UPDATE).await?;
        }
        Ok(db)
    }

    async fn run(self, db: &Db) -> Result<()> {
        match self {
            Call::Write => db.execute(UPDATE).await,
            Call::Read => db.execute("SELECT sum(length(body)), sum(n) FROM t").await,
            Call::Commit => db.commit_with(Bytes::from("after")).await,
        }
    }
}

/// Drop each call after one poll, two, ... until it finishes: the session
/// ends (later calls fail), and the store holds the state before the call or,
/// if it wrote, the one after it, through a crash keeping any prefix of the
/// writes, and through the owner's cache.
#[test]
fn a_call_cancelled_after_any_number_of_polls_ends_its_session() -> Result<()> {
    futures::executor::block_on(async {
        let base = baseline().await?;
        let before = observe(base.fork()).await?;
        let mut cases = 0;
        for call in [Call::Write, Call::Read, Call::Commit] {
            for cached in [false, true] {
                let after = {
                    let memory = base.fork();
                    let db = call.open(memory.clone(), None).await?;
                    call.run(&db).await?;
                    drop(db);
                    observe(memory).await?
                };
                for polls in 1.. {
                    let case = format!("{call:?} dropped after {polls} polls, cached={cached}");
                    let memory = base.fork();
                    let cache = cached.then(fresh_namespace);
                    let db = call.open(memory.clone(), cache.clone()).await?;
                    let writes = memory.stats().puts + memory.stats().deletes;
                    if let Some(result) = poll_at_most(call.run(&db), polls) {
                        result?;
                        assert!(db.is_usable(), "{case}");
                        assert!(polls > 3, "{case}: too few points to cancel at");
                        break;
                    }
                    let wrote = memory.stats().puts + memory.stats().deletes > writes;
                    assert!(!db.is_usable(), "{case}: still usable");
                    assert!(db.execute("SELECT 1").await.is_err(), "{case}: ran SQL");
                    drop(db);
                    // What reads see, and what each crash keeps.
                    let unsynced = memory.unsynced().len();
                    let current = observe(memory.fork()).await?;
                    let mut seen = vec![current.clone()];
                    for kept in 0..unsynced {
                        let crashed = memory.fork();
                        crashed.crash_keeping(kept);
                        seen.push(observe(crashed).await?);
                    }
                    for state in seen {
                        assert!(
                            state == before || (wrote && state == after),
                            "{case}: neither before nor after:\n{}",
                            state.0
                        );
                    }
                    // The process goes on: the next session of the owner,
                    // through its cache, sees what the store holds.
                    if cached {
                        let db = Db::open(Arc::new(memory), options(cache)).await?;
                        assert_eq!(dump(&db).await?, current.0, "{case}: through the cache");
                    }
                    cases += 1;
                }
            }
        }
        println!("cancellation sweep: {cases} cases");
        Ok(())
    })
}
