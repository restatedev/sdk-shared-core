//! Engine reuse: a session that ends badly (a fault, a cancellation, an open
//! transaction) never leaves the next session a dirty engine; one that ends
//! cleanly leaves it, and the next session sees exactly the flushed state.
use super::host::StateSetup;
use super::{
    fault::{failpoint, poll_at_most, under},
    host::Db,
    reference::{dump, integrity},
};
use crate::sql::Database;
use crate::state::simulation::Namespace;
use crate::state::{
    kv::KvStore,
    simulation::{fresh_namespace, Effect, FaultKv, MemoryKv, Operation},
    Result, State, StateOptions,
};
use bytes::Bytes;
use std::future::Future;
use std::sync::Arc;

fn owner(cache: &Namespace) -> StateSetup {
    StateSetup {
        options: StateOptions {
            checkpoint_entries: 4,
            ..StateOptions::default()
        },
        cache: Some(cache.clone()),
        packs: None,
    }
}

/// The flushed state, through a session without the cache.
async fn flushed(store: Arc<dyn KvStore>) -> Result<String> {
    let db = Database::new(State::open(store).await?).await?;
    assert_eq!(integrity(&db).await?, "ok");
    dump(&db).await
}

/// A store with a table of 40 rows, some past a page, and an owner whose
/// engine is parked in its cache.
async fn parked() -> Result<(Arc<FaultKv>, Namespace)> {
    let store = Arc::new(FaultKv::new(MemoryKv::default(), 0));
    let cache = fresh_namespace();
    let db = Db::open(store.clone(), owner(&cache)).await?;
    db.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT); CREATE INDEX t_n ON t(n)",
    )
    .await?;
    db.execute("WITH RECURSIVE x(i) AS (VALUES(0) UNION ALL SELECT i + 1 FROM x WHERE i < 39) INSERT INTO t SELECT i, i, substr(printf('%06000d', i), 1, 100 + (i % 4) * 2000) FROM x")
        .await?;
    drop(db);
    assert!(Db::open(store.clone(), owner(&cache))
        .await?
        .reused_engine());
    Ok((store, cache))
}

/// Many rows, from cached pages: the statement yields to the host every 64.
const LONG_READ: &str = "SELECT a.id, b.id FROM t a, t b";
const LONG_WRITE: &str = "INSERT INTO t SELECT 1000 + a.id * 40 + b.id, a.n, iif(b.id = 0, a.body, 'w') FROM t a, t b WHERE b.id < 8 RETURNING id";

/// How a session that continued a parked engine ends.
#[derive(Clone, Copy, Debug)]
enum Ending {
    /// Dropped after this many polls of a long read.
    CancelledRead(usize),
    /// Dropped after this many polls of a long write.
    CancelledWrite(usize),
    /// Dropped after this many polls of a long write in a transaction.
    CancelledInTransaction(usize),
    /// A flush fails at its first request of the kind, with the effect.
    FailedFlush(Operation, Effect),
    /// A flush is dropped at its sync.
    CancelledFlush,
    /// Dropped with its transaction open.
    OpenTransaction,
    /// A table created, filled and altered, then rolled back.
    RolledBackSchema,
    /// Rows inserted and rolled back to a savepoint, which is released.
    RolledBackSavepoint,
    /// A string of statements whose second fails.
    ErrorMidString,
    /// An insert of rows past a page, failing at its last row.
    FailedInsert,
}

impl Ending {
    /// Whether it leaves its engine to the next session: a call dropped at
    /// its first poll ran nothing, as it yields before a statement it has
    /// not run before.
    fn parks(self) -> bool {
        matches!(
            self,
            Ending::RolledBackSchema
                | Ending::RolledBackSavepoint
                | Ending::ErrorMidString
                | Ending::FailedInsert
                | Ending::CancelledRead(1)
                | Ending::CancelledWrite(1)
        )
    }

    async fn run(self, db: &mut Db, store: &FaultKv) -> Result<()> {
        match self {
            Ending::CancelledRead(polls) => cancel(db.query(LONG_READ), polls),
            Ending::CancelledWrite(polls) => cancel(db.execute(LONG_WRITE), polls),
            Ending::CancelledInTransaction(polls) => {
                db.execute("BEGIN; INSERT INTO t VALUES(100, 0, 'x')")
                    .await?;
                cancel(db.execute(LONG_WRITE), polls);
            }
            Ending::FailedFlush(operation, effect) => {
                // The flush's own put: a failed put of a checkpoint's pack
                // fails only the checkpoint, and the flush goes on.
                let prefix = match operation {
                    Operation::Put => crate::state::TAIL_PREFIX,
                    _ => "",
                };
                let plan = failpoint(operation, prefix, 1, effect, false);
                let (hit, ended) = under(store, plan, db.execute(LONG_WRITE)).await;
                assert!(hit && ended.failed(), "{self:?}");
            }
            Ending::CancelledFlush => {
                let plan = failpoint(Operation::Sync, "", 1, Effect::None, true);
                let (hit, ended) = under(store, plan, db.execute(LONG_WRITE)).await;
                assert!(hit && ended.failed(), "{self:?}");
            }
            Ending::OpenTransaction => {
                db.execute("BEGIN; DELETE FROM t WHERE id < 10").await?;
            }
            Ending::RolledBackSchema => {
                db.execute("BEGIN; CREATE TABLE u(x); INSERT INTO u VALUES(1); CREATE INDEX u_x ON u(x); ALTER TABLE t ADD COLUMN extra; UPDATE t SET extra = 1; ROLLBACK")
                    .await?;
            }
            Ending::RolledBackSavepoint => {
                db.execute("SAVEPOINT s; INSERT INTO t VALUES(100, 0, printf('%09000d', 1)); DELETE FROM t WHERE id < 5; ROLLBACK TO s; RELEASE s")
                    .await?;
            }
            Ending::ErrorMidString => {
                let sql = "INSERT INTO t VALUES(100, 0, 'kept'); INSERT INTO missing VALUES(1); INSERT INTO t VALUES(101, 0, 'never')";
                assert!(db.execute(sql).await.is_err());
            }
            Ending::FailedInsert => {
                let sql = "INSERT INTO t VALUES(100, 0, printf('%09000d', 1)), (101, 0, printf('%09000d', 2)), (0, 0, 'taken')";
                assert!(db.execute(sql).await.is_err());
            }
        }
        // A cancelled call ends the session, even one that ran nothing.
        let cancelled = matches!(self, Ending::CancelledRead(_) | Ending::CancelledWrite(_));
        assert_eq!(
            db.is_usable(),
            !cancelled && (self.parks() || matches!(self, Ending::OpenTransaction)),
            "{self:?}"
        );
        Ok(())
    }
}

/// Drop `call` after `polls` polls; it must not finish by then.
fn cancel<T>(call: impl Future<Output = Result<T>>, polls: usize) {
    assert!(
        poll_at_most(call, polls).is_none(),
        "finished within {polls} polls"
    );
}

/// Each way a session can end, after continuing a parked engine: the next
/// session continues the engine only after a clean ending, and sees exactly
/// the flushed state, which it goes on from.
#[test]
fn the_next_session_continues_only_a_clean_engine() -> Result<()> {
    futures::executor::block_on(async {
        let mut endings = vec![
            Ending::FailedFlush(Operation::Put, Effect::None),
            Ending::FailedFlush(Operation::Put, Effect::After),
            Ending::FailedFlush(Operation::Sync, Effect::None),
            Ending::FailedFlush(Operation::Sync, Effect::After),
            Ending::CancelledFlush,
            Ending::OpenTransaction,
            Ending::RolledBackSchema,
            Ending::RolledBackSavepoint,
            Ending::ErrorMidString,
            Ending::FailedInsert,
        ];
        for polls in [1, 2, 3, 5, 8] {
            endings.push(Ending::CancelledRead(polls));
            endings.push(Ending::CancelledWrite(polls));
            endings.push(Ending::CancelledInTransaction(polls));
        }
        for ending in endings {
            let (store, cache) = parked().await?;
            let mut db = Db::open(store.clone(), owner(&cache)).await?;
            assert!(db.reused_engine(), "{ending:?}");
            ending.run(&mut db, &store).await?;
            drop(db);
            let expected = flushed(store.clone()).await?;
            let db = Db::open(store.clone(), owner(&cache)).await?;
            assert_eq!(db.reused_engine(), ending.parks(), "{ending:?}");
            assert_eq!(dump(&db).await?, expected, "{ending:?}");
            assert_eq!(integrity(&db).await?, "ok", "{ending:?}");
            // It goes on: what it flushes on top is what the store holds.
            db.execute("UPDATE t SET n = n * 2, body = body || 'z' WHERE id % 3 = 0; INSERT INTO t VALUES(200, 0, 'after'); CREATE TABLE u(x)")
                .await?;
            let after = dump(&db).await?;
            drop(db);
            assert_eq!(flushed(store.clone()).await?, after, "{ending:?}");
        }
        Ok(())
    })
}

/// The same through the addon's calls: a cancelled call, a failed
/// flush, a transaction left open, `save` after a failure, and a clean
/// `save` hand the next session exactly the flushed state.
#[test]
fn a_session_hands_on_only_the_flushed_state() -> Result<()> {
    futures::executor::block_on(async {
        for case in 0..6 {
            let (store, cache) = parked().await?;
            let open = || async { Database::new(owner(&cache).open(store.clone()).await?).await };
            let session = open().await?;
            match case {
                0 => assert!(poll_at_most(session.execute(LONG_WRITE), 3).is_none()),
                1 => {
                    let plan = failpoint(Operation::Sync, "", 1, Effect::None, false);
                    let (hit, _) = under(&store, plan, session.execute(LONG_WRITE)).await;
                    assert!(hit);
                    assert!(session.save().await.is_err());
                }
                2 => {
                    session.begin().await?;
                    session.execute("DELETE FROM t WHERE id < 10").await?;
                    assert!(session.save().await.is_err(), "a transaction left open");
                }
                3 => {
                    session.begin().await?;
                    session.execute(LONG_WRITE).await?;
                    let plan = failpoint(
                        Operation::Put,
                        crate::state::HEAD_KEY,
                        1,
                        Effect::After,
                        false,
                    );
                    let (hit, _) = under(
                        &store,
                        plan,
                        session.commit([("outbox".into(), Bytes::from("e"))]),
                    )
                    .await;
                    assert!(hit);
                    session.close().await;
                }
                4 => {
                    session.begin().await?;
                    session.execute("INSERT INTO t VALUES(100, 0, 'x')").await?;
                    session.rollback().await?;
                    session.save().await?;
                }
                _ => {
                    session.begin().await?;
                    assert!(poll_at_most(session.execute(LONG_WRITE), 2).is_none());
                    assert!(!session.is_usable());
                    assert!(session.rollback().await.is_err());
                }
            }
            drop(session);
            let expected = flushed(store.clone()).await?;
            let session = open().await?;
            let mut seen = dump(&session).await?;
            assert_eq!(seen, expected, "case {case}");
            session
                .execute(
                    "UPDATE t SET n = -n WHERE id % 4 = 1; INSERT INTO t VALUES(200, 0, 'after')",
                )
                .await?;
            seen = dump(&session).await?;
            session.save().await?;
            assert_eq!(flushed(store.clone()).await?, seen, "case {case}");
        }
        Ok(())
    })
}
