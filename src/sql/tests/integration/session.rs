//! The database's session (`Database`) under storage faults: what a
//! failed call leaves, records and their transaction under a crash, and a
//! reader beside its owner.
use super::fault::{every_failure, failpoint, probe, under, Ended};
use super::host::StateSetup;
use crate::sql::Database;
use crate::state::simulation::Namespace;
use crate::state::{
    kv::KvStore,
    simulation::{fresh_namespace, Effect, FaultKv, MemoryKv, MonitorKv, Operation},
    Result, StateOptions,
};
use bytes::Bytes;
use std::sync::Arc;

/// Pages read from the store as queries need them; garbage deleted soon.
fn options(cache: Option<Namespace>) -> StateSetup {
    StateSetup {
        options: StateOptions {
            checkpoint_entries: 3,
            prefetch_pages: 0,
            ..StateOptions::default()
        },
        cache,
        packs: None,
    }
}

async fn open(store: Arc<dyn KvStore>, cache: Option<Namespace>) -> Result<Database> {
    async { Database::new(options(cache).open(store).await?).await }.await
}

/// Insert pair `i`: two rows whose `n` cancel out, one with a body past a page.
async fn insert_pair(session: &Database, i: i64) -> Result<()> {
    let sql = format!(
        "INSERT INTO t VALUES({}, {i}, printf('%05000d', {i})), ({}, {}, 'x')",
        2 * i,
        2 * i + 1,
        -i
    );
    session.query_columns(&sql, None).await.map(drop)
}

/// What a commit of `pairs` pairs sets as its record `outbox`.
fn outbox(pairs: i64) -> Bytes {
    Bytes::from(format!("pairs {pairs}"))
}

/// The pairs a store holds and its outbox, through a new session; its
/// database checks out.
async fn observe(store: Arc<dyn KvStore>) -> Result<(i64, Bytes)> {
    let session = open(store, None).await?;
    let (_, rows) =
        (session.query_columns("SELECT count(*), coalesce(sum(n), 0) FROM t", None)).await?;
    assert_eq!(rows[0][1].as_int(), Some(0), "pairs commit whole");
    let (_, check) = session
        .query_columns("PRAGMA integrity_check", None)
        .await?;
    assert_eq!(check[0][0].to_string(), "ok");
    let outbox = session.record("outbox").await?;
    session.close().await;
    Ok((rows[0][0].as_int().unwrap() / 2, outbox))
}

/// A store with 20 pairs, committed with their outbox.
async fn baseline() -> Result<MemoryKv> {
    let memory = MemoryKv::default();
    let session = open(Arc::new(memory.clone()), None).await?;
    let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)";
    session.query_columns(schema, None).await?;
    session.begin().await?;
    for i in 0..20 {
        insert_pair(&session, i).await?;
    }
    session.commit([("outbox".into(), outbox(20))]).await?;
    // More flushes: tail entries over the checkpoint, read from the store.
    session.query_columns("CREATE TABLE a(x)", None).await?;
    for k in 0..4 {
        session
            .query_columns(&format!("INSERT INTO a VALUES({k})"), None)
            .await?;
    }
    session.save().await?;
    Ok(memory)
}

/// A call of the session, after what prepares it.
#[derive(Clone, Copy, Debug)]
enum Call {
    Read,
    Write,
    Begin,
    CommitWithRecord,
    CommitRecordAlone,
    Rollback,
}

impl Call {
    async fn prepare(self, session: &Database) -> Result<()> {
        match self {
            Call::CommitWithRecord | Call::Rollback => {
                session.begin().await?;
                insert_pair(session, 20).await
            }
            Call::CommitRecordAlone => session.begin().await,
            _ => Ok(()),
        }
    }

    async fn run(self, session: &Database) -> Result<()> {
        match self {
            Call::Read => {
                let sql = "SELECT sum(length(body)) FROM t";
                session.query_columns(sql, None).await.map(drop)
            }
            Call::Write => insert_pair(session, 20).await,
            Call::Begin => session.begin().await,
            Call::CommitWithRecord => session.commit([("outbox".into(), outbox(21))]).await,
            Call::CommitRecordAlone => {
                session
                    .commit([("outbox".into(), Bytes::from("sent"))])
                    .await
            }
            Call::Rollback => session.rollback().await,
        }
    }

    /// The pairs and the outbox once the call succeeded.
    fn new_state(self) -> (i64, Bytes) {
        match self {
            Call::Write => (21, outbox(20)),
            Call::CommitWithRecord => (21, outbox(21)),
            Call::CommitRecordAlone => (20, Bytes::from("sent")),
            _ => (20, outbox(20)),
        }
    }
}

/// A storage error or a cancellation at any request of any call ends the
/// session: it is no longer usable, its outbox stays that of its last
/// commit, later calls fail (a rollback has nothing to do), and `save` says
/// so. The store holds the state before the call or, if the call wrote, the
/// one after it, whatever a crash keeps, with the outbox of that state.
#[test]
fn a_storage_fault_in_any_call_ends_the_session() -> Result<()> {
    futures::executor::block_on(async {
        let base = baseline().await?;
        let old = (20, outbox(20));
        let mut cases = 0;
        for call in [
            Call::Read,
            Call::Write,
            Call::Begin,
            Call::CommitWithRecord,
            Call::CommitRecordAlone,
            Call::Rollback,
        ] {
            let events = {
                let store = Arc::new(FaultKv::new(base.fork(), 0));
                let session = open(store.clone(), None).await?;
                call.prepare(&session).await?;
                let (events, result) = probe(&store, call.run(&session)).await;
                result?;
                events
            };
            for plan in every_failure(&events) {
                let case = format!("{call:?} at {plan:?}");
                let memory = base.fork();
                let store = Arc::new(FaultKv::new(memory.clone(), 0));
                let session = open(store.clone(), None).await?;
                call.prepare(&session).await?;
                let writes = memory.stats().puts + memory.stats().deletes;
                let (hit, ended) = under(&store, plan, call.run(&session)).await;
                let wrote = memory.stats().puts + memory.stats().deletes > writes;
                assert!(hit, "{case}: not reached");
                assert!(ended.failed(), "{case}: succeeded");
                if matches!(ended, Ended::Cancelled) {
                    assert_eq!(store.active(), 0, "{case}: requests settle");
                }
                assert!(!session.is_usable(), "{case}: still usable");
                match ended {
                    // A cancelled call ends the session outright.
                    Ended::Cancelled => assert!(session.record("outbox").await.is_err(), "{case}"),
                    Ended::Returned(_) => {
                        assert_eq!(session.record("outbox").await?, old.1, "{case}: outbox")
                    }
                }
                assert!(
                    session.query_columns("SELECT 1", None).await.is_err(),
                    "{case}"
                );
                assert!(session.begin().await.is_err(), "{case}");
                assert!(
                    session
                        .commit([("outbox".into(), Bytes::from("late"))])
                        .await
                        .is_err(),
                    "{case}: commits a record after the failure"
                );
                let cancelled = matches!(ended, Ended::Cancelled);
                assert_eq!(
                    session.rollback().await.is_err(),
                    cancelled,
                    "{case}: rollback"
                );
                let saved = session.save().await.unwrap_err().to_string();
                assert!(saved.contains("the session ended"), "{case}: {saved}");
                assert!(session.record("outbox").await.is_err(), "{case}: closed");

                let mut seen = vec![observe(Arc::new(memory.fork())).await?];
                for kept in 0..=memory.unsynced().len() {
                    let crashed = memory.fork();
                    crashed.crash_keeping(kept);
                    seen.push(observe(Arc::new(crashed)).await?);
                }
                for state in seen {
                    assert!(
                        state == old || (wrote && state == call.new_state()),
                        "{case}: {state:?}"
                    );
                }
                cases += 1;
            }
        }
        println!("session faults: {cases} cases");
        assert!(cases > 50, "{cases} cases");
        Ok(())
    })
}

/// Crash a run of transactions, each committed with its outbox, at every
/// sync, keeping each prefix of the writes the sync would have stored: the
/// outbox is always that of the rows beside it.
#[test]
fn records_commit_with_their_transaction_under_a_crash_at_any_write() -> Result<()> {
    futures::executor::block_on(async {
        let mut cases = 0;
        for cached in [false, true] {
            for nth in 1.. {
                let Some(prefixes) = crash_at_sync(cached, nth).await? else {
                    break;
                };
                cases += prefixes;
            }
        }
        println!("session records campaign: {cases} cases");
        assert!(cases > 100, "{cases} cases");
        Ok(())
    })
}

/// Crash at the `nth` sync, keeping each prefix of its writes in turn: the
/// number of prefixes, or `None` if the run made fewer syncs.
async fn crash_at_sync(cached: bool, nth: usize) -> Result<Option<usize>> {
    let cache = cached.then(fresh_namespace);
    let memory = MemoryKv::default();
    let store = Arc::new(MonitorKv::new(FaultKv::new(memory.clone(), nth as u64)));
    let session = open(store.clone(), cache.clone()).await?;
    let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)";
    session.query_columns(schema, None).await?;
    let (mut acknowledged, plan) = (0, failpoint(Operation::Sync, "", nth, Effect::None, false));
    let (hit, _) = under(&store, plan, async {
        for i in 0..24 {
            session.begin().await?;
            insert_pair(&session, i).await?;
            if i % 5 == 4 {
                session
                    .query_columns("UPDATE t SET body = 'y' WHERE n < 0", None)
                    .await?;
            }
            session.commit([("outbox".into(), outbox(i + 1))]).await?;
            acknowledged = i + 1;
        }
        Result::Ok(())
    })
    .await;
    if !hit {
        return Ok(None);
    }
    assert!(!session.is_usable());
    drop(session);
    assert_eq!(store.violations(), Vec::<String>::new(), "sync #{nth}");
    let writes = memory.unsynced().len();
    for kept in 0..=writes {
        let crashed = memory.fork();
        crashed.crash_keeping(kept);
        let case = format!("sync #{nth}, {kept} of {writes} writes kept, cached={cached}");
        let (pairs, seen) = observe(Arc::new(crashed.clone())).await?;
        assert!(
            pairs == acknowledged || pairs == acknowledged + 1,
            "{case}: {pairs} pairs, {acknowledged} acknowledged"
        );
        let expected = match pairs {
            0 => Bytes::new(),
            pairs => outbox(pairs),
        };
        assert_eq!(seen, expected, "{case}");
        // It goes on. One of the crashes goes on through the owner's cache,
        // as a handler in the same process would: the cache follows one
        // history, and each crash is another.
        let store = Arc::new(MonitorKv::seeded(crashed.clone(), crashed.values()));
        let through_cache = cache.clone().filter(|_| kept == nth % (writes + 1));
        let session = open(store.clone(), through_cache).await?;
        session.begin().await?;
        insert_pair(&session, pairs).await?;
        session
            .commit([("outbox".into(), outbox(pairs + 1))])
            .await?;
        session.save().await?;
        assert_eq!(
            observe(store.clone()).await?,
            (pairs + 1, outbox(pairs + 1))
        );
        assert_eq!(store.violations(), Vec::<String>::new(), "{case}");
    }
    Ok(Some(writes + 1))
}

/// The result of `sql` on a store, through a plain session.
async fn expected(store: MemoryKv, sql: &str) -> Result<String> {
    let state = options(None).open(Arc::new(store)).await?;
    let session = Database::new(state).await?;
    Ok(format!("{:?}", session.query(sql).await?))
}

/// A reader beside its owner, who flushes meanwhile, some flushes failing,
/// while some of the reader's reads fail too. What a reader reads is the
/// state it opened on, however the owner flushes: the tail and the packs
/// its head names stay for a checkpoint more. Every other reader reads
/// through the owner's cache, which it changes nothing in.
#[test]
fn a_reader_sees_one_flushed_state_beside_its_owner() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let owner_store = Arc::new(FaultKv::new(memory.clone(), 0));
        let cache = fresh_namespace();
        let mut owner = open(owner_store.clone(), Some(cache.clone())).await?;
        let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)";
        owner.query_columns(schema, None).await?;
        owner.begin().await?;
        for i in 0..60 {
            insert_pair(&owner, i).await?;
        }
        owner.commit([]).await?;
        owner.query_columns("CREATE TABLE a(x)", None).await?;
        for k in 0..4 {
            owner
                .query_columns(&format!("INSERT INTO a VALUES({k})"), None)
                .await?;
        }
        let queries = [
            "SELECT count(*), sum(n), sum(length(body)) FROM t",
            "SELECT id, n, substr(body, -3) FROM t WHERE id BETWEEN 30 AND 50 ORDER BY id",
            "SELECT id, n FROM t WHERE id >= 100 ORDER BY id",
        ];
        let (mut before, mut after, mut faults) = (0, 0, 0);
        for round in 0..24u64 {
            let snapshot = memory.fork();
            let reader_store = Arc::new(FaultKv::new(memory.clone(), round));
            let shared = (round % 2 == 1 && round % 5 != 0).then(|| cache.clone());
            let reader =
                Database::new(options(shared).open(reader_store.clone()).reader().await?).await?;
            // Now and then its first read fails, which ends it.
            if round % 5 == 0 {
                let plan = failpoint(
                    Operation::Get,
                    crate::state::blobs::KV_PREFIX,
                    1 + round as usize % 3,
                    Effect::None,
                    round % 2 == 0,
                );
                let (hit, ended) =
                    under(&reader_store, plan, reader.query_columns(queries[0], None)).await;
                assert!(
                    hit && ended.failed(),
                    "round {round}: {hit} {:?}",
                    matches!(ended, Ended::Returned(Ok(_)))
                );
                assert!(!reader.is_usable());
                assert!(reader.query_columns(queries[0], None).await.is_err());
                faults += 1;
                continue;
            }
            let first = reader.query_columns(queries[0], None).await?;
            assert_eq!(
                format!("{:?}", first.1),
                expected(snapshot.fork(), queries[0]).await?
            );
            // The owner flushes `round % 4` times, one flush failing.
            let mut flushed = false;
            for k in 0..round % 4 {
                let sql = format!(
                    "UPDATE t SET n = n + 1, body = printf('%05000d', {round}) WHERE id % 7 = {k} AND n >= 0; UPDATE t SET n = n - 1 WHERE id % 7 = {k} AND n < 0"
                );
                if k == 1 {
                    let plan = failpoint(
                        Operation::Put,
                        crate::state::HEAD_KEY,
                        1,
                        [Effect::None, Effect::After][round as usize % 2],
                        false,
                    );
                    let (_, ended) =
                        under(&owner_store, plan, owner.query_columns(&sql, None)).await;
                    assert!(ended.failed());
                    assert!(!owner.is_usable());
                    owner.close().await;
                    owner = open(owner_store.clone(), Some(cache.clone())).await?;
                } else {
                    owner.query_columns(&sql, None).await?;
                    flushed = true;
                }
            }
            // The reader reads on, the state it opened on.
            for sql in queries {
                let (_, rows) = reader.query_columns(sql, None).await?;
                let opened = expected(snapshot.fork(), sql).await?;
                assert_eq!(format!("{rows:?}"), opened, "round {round}, {sql}");
                match flushed {
                    true => after += 1,
                    false => before += 1,
                }
            }
            reader.close().await;
        }
        println!("reader: {before} reads, {after} after the owner flushed, {faults} faults");
        assert!(before > 0 && after > 0 && faults > 0);
        Ok(())
    })
}

/// A storage fault inside a `transaction()` body is reported as itself, never
/// as the body ending its transaction (as it would be should the engine roll
/// back on the fault), and ends the session.
#[test]
fn a_storage_fault_in_a_transaction_body_is_reported_as_itself() -> Result<()> {
    futures::executor::block_on(async {
        let memory = baseline().await?;
        let statements = [
            "SELECT sum(length(body)) FROM t",
            "UPDATE t SET body = printf('%06000d', id) WHERE id % 3 = 0",
        ];
        let mut cases = 0;
        for statement in statements {
            let store = Arc::new(FaultKv::new(memory.fork(), 0));
            let session = open(store.clone(), None).await?;
            session.begin().await?;
            let (events, result) = probe(&store, session.query_columns(statement, None)).await;
            result?;
            for plan in every_failure(&events).into_iter().filter(|p| !p.cancel) {
                let store = Arc::new(FaultKv::new(memory.fork(), 0));
                let session = open(store.clone(), None).await?;
                session.begin().await?;
                let (hit, ended) =
                    under(&store, plan, session.query_columns(statement, None)).await;
                let Ended::Returned(Err(error)) = ended else {
                    continue;
                };
                assert!(hit, "{statement}: failed without its fault: {error}");
                cases += 1;
                let message = error.to_string();
                assert!(
                    !message.contains("ended the transaction"),
                    "{statement}: a storage fault reported as {message:?}"
                );
                assert!(!session.is_usable(), "{statement}: the session went on");
                assert!(session.query_columns("SELECT 1", None).await.is_err());
            }
        }
        assert!(cases > 0, "no fault failed a statement");
        Ok(())
    })
}
