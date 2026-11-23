//! A call polled outside every task of a multi-thread runtime, as the Node
//! addon polls one once, on JavaScript's thread, runs where it is polled
//! until it must wait or reaches work that must not run there; the host then
//! polls it again, or spawns it on the runtime (the module documentation of
//! `sql`, "Calls polled in place").
use crate::sql::{Database, Engine, Value};
use crate::state::{kv::KvStore, Result};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{
    collections::{BTreeMap, HashSet},
    future::{Future, IntoFuture},
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    thread::{self, ThreadId},
    time::Duration,
};

/// An immediately ready store that notes the threads that read or write it,
/// and counts its puts. Once `armed`, a read waits for a permit of `gate`.
struct Threads {
    values: Mutex<BTreeMap<String, Bytes>>,
    seen: Mutex<HashSet<ThreadId>>,
    puts: AtomicUsize,
    armed: AtomicBool,
    gate: tokio::sync::Semaphore,
}

impl Default for Threads {
    fn default() -> Self {
        Self {
            values: Mutex::default(),
            seen: Mutex::default(),
            puts: AtomicUsize::new(0),
            armed: AtomicBool::new(false),
            gate: tokio::sync::Semaphore::new(0),
        }
    }
}

impl Threads {
    fn note(&self) {
        self.seen.lock().unwrap().insert(thread::current().id());
    }

    fn seen(&self) -> HashSet<ThreadId> {
        std::mem::take(&mut self.seen.lock().unwrap())
    }

    fn puts(&self) -> usize {
        self.puts.load(Ordering::SeqCst)
    }

    /// Make reads wait until [`Self::release`].
    fn hold_reads(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn release(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.gate.add_permits(1 << 20);
    }
}

impl KvStore for Threads {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        self.note();
        Box::pin(async move {
            if self.armed.load(Ordering::SeqCst) {
                self.gate.acquire().await.expect("open").forget();
            }
            Ok(self.values.lock().unwrap().get(key).cloned())
        })
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.note();
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.values.lock().unwrap().insert(key.into(), value);
        Ok(())
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.values.lock().unwrap().remove(key);
        Ok(())
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Open `store` with a frame of a page and nothing fetched ahead, so that a
/// session reads a page from the store when it needs it.
fn open(store: &Arc<Threads>) -> crate::sql::Open {
    use crate::state::StateOptions;
    Database::open(store.clone()).options(StateOptions {
        frame_pages: 1,
        prefetch_pages: 0,
        ..StateOptions::default()
    })
}

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
}

/// A call of a session, owning what it needs, so that a host can spawn it.
type Call<T> = Pin<Box<dyn Future<Output = T> + Send>>;

fn show(db: &Arc<Database>, sql: &str, params: Vec<Value>) -> Call<String> {
    let (db, sql) = (db.clone(), sql.to_owned());
    Box::pin(async move {
        let params = (!params.is_empty()).then_some(params);
        match db.query_columns(&sql, params).await {
            Ok((columns, rows)) => format!("{columns:?} {rows:?}"),
            Err(error) => format!("error: {error}"),
        }
    })
}

fn execute(db: &Arc<Database>, sql: &str) -> Call<Result<()>> {
    let (db, sql) = (db.clone(), sql.to_owned());
    Box::pin(async move { db.execute(&sql).await })
}

fn query(db: &Arc<Database>, sql: &str) -> Call<Result<crate::sql::Rows>> {
    let (db, sql) = (db.clone(), sql.to_owned());
    Box::pin(async move { db.query(&sql).await })
}

/// `call` polled once here, outside every task.
fn first_poll<T>(runtime: &tokio::runtime::Runtime, call: &mut Call<T>) -> Poll<T> {
    let _entered = runtime.enter();
    call.as_mut().poll(&mut Context::from_waker(Waker::noop()))
}

/// `call` as a task of the runtime, to its end.
fn in_a_task<T: Send + 'static>(runtime: &tokio::runtime::Runtime, call: Call<T>) -> T {
    runtime.block_on(runtime.spawn(call)).expect("the task ran")
}

/// Run `f` on the session's engine.
fn engine<T>(
    runtime: &tokio::runtime::Runtime,
    db: &Database,
    f: impl FnOnce(&mut Engine) -> T,
) -> T {
    runtime.block_on(async {
        let mut session = db.shared.lock().await;
        f(session.engine().expect("an open session"))
    })
}

/// A session with the table `t`, which the engine has read: its pages are in
/// memory. Its calls are all tasks of the runtime.
fn session(runtime: &tokio::runtime::Runtime, store: &Arc<Threads>) -> Result<Arc<Database>> {
    let db = Arc::new(runtime.block_on(open(store).into_future())?);
    in_a_task(
        runtime,
        execute(
            &db,
            "CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v TEXT)",
        ),
    )?;
    in_a_task(
        runtime,
        execute(&db, "INSERT OR IGNORE INTO t VALUES (1, 'one'), (2, 'two')"),
    )?;
    in_a_task(runtime, query(&db, "SELECT id, v FROM t"))?;
    Ok(db)
}

/// Make `sql`, a statement of the session, quick: a run's own timing says
/// nothing of the quick runs of a test, which a build without optimizations
/// makes slow.
fn make_quick(runtime: &tokio::runtime::Runtime, db: &Database, sql: &str) {
    engine(runtime, db, |engine| {
        if let Some(prepared) = engine.statements.get_mut(sql) {
            prepared.longest = Duration::ZERO;
        }
    });
}

#[test]
fn a_quick_call_finishes_in_the_first_poll_on_the_polling_thread() -> Result<()> {
    let runtime = runtime()?;
    let store = Arc::new(Threads::default());
    let here = thread::current().id();
    let db = session(&runtime, &store)?;

    // A statement the session kept, whose last run was quick; and the calls
    // of a host's transaction, the same.
    let kept = "SELECT v FROM t WHERE id = 1 /*kept*/";
    for sql in [kept, "BEGIN", "ROLLBACK"] {
        in_a_task(&runtime, query(&db, sql))?;
        make_quick(&runtime, &db, sql);
    }
    store.seen();
    for (sql, expected) in [(kept, Some("one")), ("BEGIN", None), ("ROLLBACK", None)] {
        let mut call = query(&db, sql);
        let Poll::Ready(rows) = first_poll(&runtime, &mut call) else {
            panic!("not finished in the first poll: {sql}");
        };
        assert_eq!(
            rows?.first().map(|row| row[0].to_string()).as_deref(),
            expected
        );
    }
    assert!(!store.seen().contains(&here), "the store was used here");
    Ok(())
}

#[test]
fn work_that_may_run_long_never_runs_in_the_first_poll() -> Result<()> {
    let runtime = runtime()?;
    let store = Arc::new(Threads::default());
    let here = thread::current().id();
    let db = session(&runtime, &store)?;
    store.seen();

    // A text no one has seen run: it runs on the runtime.
    let mut call = query(&db, "SELECT v FROM t WHERE id = 1 /*never seen*/");
    assert!(first_poll(&runtime, &mut call).is_pending());
    assert_eq!(in_a_task(&runtime, call)?[0][0].to_string(), "one");

    // A kept statement whose last run held a thread too long.
    let slow = "SELECT v FROM t WHERE id = 2 /*slow*/";
    in_a_task(&runtime, query(&db, slow))?;
    engine(&runtime, &db, |engine| {
        engine.statements.get_mut(slow).expect("kept").longest = Duration::from_millis(5);
    });
    let mut call = query(&db, slow);
    assert!(first_poll(&runtime, &mut call).is_pending());
    assert_eq!(in_a_task(&runtime, call)?[0][0].to_string(), "two");

    // A commit of changed pages: the statement is quick, runs here and
    // changes the pages, and the commit, which compresses and stores them,
    // does not.
    let insert = "INSERT INTO t VALUES (?1, 'x') /*commit*/";
    let write = |id: i64| -> Call<Result<()>> {
        let db = db.clone();
        Box::pin(async move { db.execute_with(insert, [Value::from_i64(id)]).await })
    };
    in_a_task(&runtime, write(10))?;
    make_quick(&runtime, &db, insert);
    let puts = store.puts();
    let mut call = write(11);
    assert!(first_poll(&runtime, &mut call).is_pending());
    assert_eq!(store.puts(), puts, "stored in the first poll");
    in_a_task(&runtime, call)?;
    assert!(store.puts() > puts);
    assert_eq!(
        in_a_task(&runtime, query(&db, "SELECT count(*) FROM t"))?[0][0].as_int(),
        Some(4)
    );
    assert!(!store.seen().contains(&here), "the store was used here");
    Ok(())
}

/// A session with the table `big`, of many pages, of which it has read the
/// first only: its pages are the store's, in a checkpoint's pack.
fn cold_session(runtime: &tokio::runtime::Runtime, store: &Arc<Threads>) -> Result<Arc<Database>> {
    let first = session(runtime, store)?;
    in_a_task(
        runtime,
        execute(&first, "CREATE TABLE big(id INTEGER PRIMARY KEY, v TEXT)"),
    )?;
    in_a_task(
        runtime,
        execute(
            &first,
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 400) \
             INSERT INTO big SELECT x, printf('%0300d', x) FROM c",
        ),
    )?;
    runtime.block_on(first.checkpoint())?;
    runtime.block_on(first.save())?;
    let db = Arc::new(runtime.block_on(open(store).into_future())?);
    in_a_task(runtime, at(&db, 1))?;
    make_quick(runtime, &db, AT);
    Ok(db)
}

const AT: &str = "SELECT length(v) FROM big WHERE id = ?1 /*cold*/";

/// `AT` of row `id`.
fn at(db: &Arc<Database>, id: i64) -> Call<Result<crate::sql::Rows>> {
    let db = db.clone();
    Box::pin(async move { db.query_with(AT, [Value::from_i64(id)]).await })
}

/// A statement that needs a page the engine does not hold waits for it,
/// and finishes when the runtime runs it, whether the page arrived before it
/// was spawned or after.
#[test]
fn a_page_that_is_not_in_memory_makes_the_call_pending() -> Result<()> {
    let runtime = runtime()?;
    for released_before_spawn in [false, true] {
        let store = Arc::new(Threads::default());
        let db = cold_session(&runtime, &store)?;
        let here = thread::current().id();
        // Quick, and read in place while its page is in memory.
        let mut call = at(&db, 1);
        assert!(first_poll(&runtime, &mut call).is_ready());
        store.hold_reads();
        store.seen();
        let mut call = at(&db, 400);
        assert!(first_poll(&runtime, &mut call).is_pending());
        assert!(!store.seen().contains(&here), "the store was read here");
        if released_before_spawn {
            store.release();
        }
        let task = runtime.spawn(call);
        if !released_before_spawn {
            thread::sleep(Duration::from_millis(100));
            assert!(!task.is_finished(), "finished without its page");
            store.release();
        }
        let rows = runtime.block_on(task).expect("the task ran")?;
        assert_eq!(rows[0][0].as_int(), Some(300));
    }
    Ok(())
}

/// A call dropped after a first poll that was pending, whether it waited
/// for a page or was to compile, leaves the session unusable.
#[test]
fn a_call_dropped_after_a_pending_first_poll_leaves_the_session_unusable() -> Result<()> {
    let runtime = runtime()?;
    for cold in [false, true] {
        let store = Arc::new(Threads::default());
        let db = cold_session(&runtime, &store)?;
        let mut call = match cold {
            true => {
                store.hold_reads();
                at(&db, 400)
            }
            false => query(&db, "SELECT 1 /*dropped unseen*/"),
        };
        assert!(first_poll(&runtime, &mut call).is_pending());
        drop(call);
        store.release();
        assert!(!db.is_usable(), "usable after a dropped call: {cold}");
        assert!(runtime.block_on(db.query("SELECT 1")).is_err());
    }
    Ok(())
}

/// A store whose next put, once armed, says it started and blocks until
/// released: a call's task that is in it cannot be aborted until it returns.
#[derive(Default)]
struct BlockingPut {
    values: Mutex<BTreeMap<String, Bytes>>,
    armed: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    /// Its syncs never finish.
    hang_sync: AtomicBool,
}

impl KvStore for BlockingPut {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move { Ok(self.values.lock().unwrap().get(key).cloned()) })
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        let armed = self.armed.lock().unwrap().take();
        if let Some((started, release)) = armed {
            let _ = started.send(());
            let _ = release.recv();
        }
        self.values.lock().unwrap().insert(key.into(), value);
        Ok(())
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.values.lock().unwrap().remove(key);
        Ok(())
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        match self.hang_sync.load(Ordering::SeqCst) {
            true => Box::pin(std::future::pending()),
            false => Box::pin(async { Ok(()) }),
        }
    }
}

/// A call that was spawned and is inside a blocking put, which cannot be
/// stopped, then aborted, leaves the session unusable, whatever the put does
/// after it: the call is dropped, whether or not it would have finished.
#[test]
fn a_spawned_call_aborted_inside_a_blocking_put_leaves_the_session_unusable() -> Result<()> {
    use std::sync::mpsc;
    let runtime = runtime()?;
    let store = Arc::new(BlockingPut::default());
    let db = Arc::new(runtime.block_on(Database::open(store.clone()).into_future())?);
    runtime.block_on(db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)"))?;
    let (started, put_started) = mpsc::channel();
    let (release, released) = mpsc::channel();
    *store.armed.lock().unwrap() = Some((started, released));
    // After the put the call waits for ever: it cannot finish by itself.
    store.hang_sync.store(true, Ordering::SeqCst);
    let mut call = execute(&db, "INSERT INTO t VALUES (1)");
    assert!(first_poll(&runtime, &mut call).is_pending());
    let task = runtime.spawn(call);
    put_started
        .recv_timeout(Duration::from_secs(10))
        .expect("the call's task reached its put");
    task.abort();
    release.send(()).unwrap();
    assert!(runtime.block_on(task).expect_err("aborted").is_cancelled());
    assert!(!db.is_usable(), "usable after an aborted call");
    assert!(runtime.block_on(db.query("SELECT 1")).is_err());
    // The store holds the state before the call or after it.
    store.hang_sync.store(false, Ordering::SeqCst);
    let db = runtime.block_on(Database::open(store).into_future())?;
    let rows = runtime.block_on(db.query("SELECT count(*) FROM t"))?;
    assert!(matches!(rows[0][0].to_string().as_str(), "0" | "1"));
    assert_eq!(
        runtime.block_on(db.query("PRAGMA integrity_check"))?[0][0].to_string(),
        "ok"
    );
    Ok(())
}

/// Statements of every kind the engine keeps: DDL, inserts, queries with
/// parameters and errors, a rolled back transaction, savepoints, a pragma.
/// `tag` makes the texts this test's.
fn script(tag: &str) -> Vec<(String, Vec<Value>)> {
    let mut steps: Vec<(String, Vec<Value>)> = Vec::new();
    let mut add = |sql: String, params: Vec<Value>| steps.push((sql, params));
    add(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)".into(),
        vec![],
    );
    add("CREATE INDEX t_n ON t(n)".into(), vec![]);
    for i in 0..6 {
        add(
            format!("INSERT INTO t VALUES(?1, ?2, ?3) /*{tag}*/"),
            vec![
                Value::from_i64(i),
                Value::from_i64(i % 3),
                Value::from_text(format!("row {i}")),
            ],
        );
    }
    for _ in 0..2 {
        add(
            format!("SELECT id, n, body FROM t WHERE n = ?1 ORDER BY id /*{tag}*/"),
            vec![Value::from_i64(1)],
        );
        add(format!("SELECT count(*), sum(n) FROM t /*{tag}*/"), vec![]);
        add(
            format!("UPDATE t SET n = n + 1 WHERE id = ?1 RETURNING id, n /*{tag}*/"),
            vec![Value::from_i64(2)],
        );
        add(format!("SELECT * FROM missing /*{tag}*/"), vec![]);
        add(
            format!("INSERT INTO t VALUES(0, 0, 'dup') /*{tag}*/"),
            vec![],
        );
        add(format!("SELECT ?1 /*{tag}*/"), vec![]);
    }
    // A transaction whose statements are partly quick, and a DDL rolled back.
    add("BEGIN".into(), vec![]);
    add(
        format!("INSERT INTO t VALUES(20, 2, 'in a transaction') /*{tag}*/"),
        vec![],
    );
    add("SAVEPOINT s".into(), vec![]);
    add("CREATE TABLE gone(x)".into(), vec![]);
    add("INSERT INTO gone VALUES(1)".into(), vec![]);
    add("ALTER TABLE t ADD COLUMN extra TEXT".into(), vec![]);
    add(
        format!("SELECT id, extra FROM t ORDER BY id /*{tag}*/"),
        vec![],
    );
    add("ROLLBACK TO s".into(), vec![]);
    add(format!("SELECT count(*) FROM t /*{tag}*/"), vec![]);
    add(
        format!("SELECT id, extra FROM t ORDER BY id /*{tag}*/"),
        vec![],
    ); // gone again
    add("RELEASE s".into(), vec![]);
    add("SAVEPOINT s2".into(), vec![]);
    add(
        format!("DELETE FROM t WHERE id > ?1 /*{tag}*/"),
        vec![Value::from_i64(2)],
    );
    add("ROLLBACK TO s2".into(), vec![]);
    add("RELEASE s2".into(), vec![]);
    add("ROLLBACK".into(), vec![]);
    add(format!("SELECT count(*) FROM t /*{tag}*/"), vec![]);
    add(
        "CREATE TABLE after_ddl(x INTEGER PRIMARY KEY, y)".into(),
        vec![],
    );
    add(
        format!("INSERT INTO after_ddl VALUES(?1, 'y') /*{tag}*/"),
        vec![Value::from_i64(7)],
    );
    add("DROP INDEX t_n".into(), vec![]);
    add(format!("SELECT * FROM after_ddl /*{tag}*/"), vec![]);
    add("PRAGMA cache_size = 123".into(), vec![]);
    add(format!("SELECT count(*) FROM t /*{tag}*/"), vec![]);
    add(
        "SELECT type, name, rootpage, sql FROM sqlite_schema ORDER BY rowid".into(),
        vec![],
    );
    add("PRAGMA integrity_check".into(), vec![]);
    steps
}

/// How a script's calls run.
#[derive(Clone, Copy, PartialEq)]
enum Host {
    /// Each as a task of the runtime from the start.
    Task,
    /// Each polled once in place, with every other statement the session
    /// kept quick, and spawned if pending.
    InPlace,
}

/// The outcomes of the script's calls in a new session on an empty database,
/// and how many finished in their first poll.
fn run_script(
    runtime: &tokio::runtime::Runtime,
    host: Host,
    tag: &str,
) -> Result<(Vec<String>, usize)> {
    let store = Arc::new(Threads::default());
    let db = Arc::new(runtime.block_on(open(&store).into_future())?);
    let (mut outcomes, mut in_first_poll) = (Vec::new(), 0);
    for (n, (sql, params)) in script(tag).into_iter().enumerate() {
        let outcome = match host {
            Host::Task => in_a_task(runtime, show(&db, &sql, params)),
            Host::InPlace => {
                if n % 2 == 0 {
                    make_quick(runtime, &db, &sql);
                }
                let mut call = show(&db, &sql, params);
                match first_poll(runtime, &mut call) {
                    Poll::Ready(outcome) => {
                        in_first_poll += 1;
                        outcome
                    }
                    Poll::Pending => in_a_task(runtime, call),
                }
            }
        };
        outcomes.push(outcome);
    }
    Ok((outcomes, in_first_poll))
}

/// A session gives the same answers, and ends as the same database, whether
/// its calls run in a task from the start or are polled in place first: a
/// transaction, a savepoint and a rollback among statements some of which
/// ran in the first poll and some on the runtime.
#[test]
fn a_session_polled_in_place_is_the_session_run_in_a_task() -> Result<()> {
    let runtime = runtime()?;
    let (in_a_task, _) = run_script(&runtime, Host::Task, "equivalence")?;
    let (in_place, ready) = run_script(&runtime, Host::InPlace, "equivalence")?;
    assert_eq!(in_place, in_a_task);
    assert!(ready > 0, "no call finished in its first poll");
    assert!(
        ready < in_a_task.len(),
        "every call finished in its first poll"
    );
    assert!(in_a_task.last().unwrap().contains("ok"));
    Ok(())
}

/// An error raised in the first poll is the one the call raises in a task:
/// of two sessions in one state, one fails each statement in its first poll,
/// the other in a task.
#[test]
fn an_error_in_the_first_poll_is_the_error_of_a_call_in_a_task() -> Result<()> {
    let runtime = runtime()?;
    let mut errors = Vec::new();
    for in_place in [true, false] {
        let store = Arc::new(Threads::default());
        let db = session(&runtime, &store)?;
        in_a_task(&runtime, execute(&db, "CREATE TABLE u(x)"))?;
        let insert = "INSERT INTO t VALUES (?1, 'x') /*error*/";
        let select = "SELECT * FROM u /*error*/";
        let insert_with = |id: i64| -> Call<Result<()>> {
            let db = db.clone();
            Box::pin(async move { db.execute_with(insert, [Value::from_i64(id)]).await })
        };
        in_a_task(&runtime, insert_with(100))?;
        in_a_task(&runtime, execute(&db, select))?;
        // The statements are kept, and fail when run: one has no table now,
        // the other a row of its key.
        in_a_task(&runtime, execute(&db, "DROP TABLE u"))?;
        make_quick(&runtime, &db, insert);
        make_quick(&runtime, &db, select);
        let mut failed = Vec::new();
        for call in [insert_with(1), execute(&db, select)] {
            let mut call = call;
            let outcome = match in_place {
                true => match first_poll(&runtime, &mut call) {
                    Poll::Ready(outcome) => outcome,
                    Poll::Pending => in_a_task(&runtime, call),
                },
                false => in_a_task(&runtime, call),
            };
            failed.push(outcome.expect_err("the statement fails").to_string());
            assert!(db.is_usable());
        }
        errors.push(failed);
    }
    assert_eq!(errors[0], errors[1]);
    Ok(())
}

/// Weighing a schema that changed reads all of it, which takes as long as
/// the schema is large: not in the first poll, though the statement is quick
/// and nothing else is left to make durable.
#[test]
fn weighing_a_changed_schema_is_not_in_the_first_poll() -> Result<()> {
    let runtime = runtime()?;
    let store = Arc::new(Threads::default());
    let cache = runtime.block_on(crate::state::PageCache::open(
        crate::state::PageCacheConfig::new(64 << 20, 0),
    ))?;
    let db = Arc::new(runtime.block_on(open(&store).cached(&cache, "ns").into_future())?);
    in_a_task(
        &runtime,
        execute(&db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"),
    )?;
    // Mentions CREATE, so the engine weighs the schema again, though it changes nothing.
    let sql = "SELECT 'create' /*weigh*/";
    in_a_task(&runtime, execute(&db, sql))?;
    make_quick(&runtime, &db, sql);
    let before = db.state().snapshot();
    let mut call = execute(&db, sql);
    assert!(first_poll(&runtime, &mut call).is_pending());
    in_a_task(&runtime, call)?;
    // The statement changed nothing to make durable.
    assert!(Arc::ptr_eq(&before, &db.state().snapshot()));
    Ok(())
}

/// A future that counts its polls.
struct Counted<F>(Pin<Box<F>>, Arc<AtomicUsize>);

impl<F: Future> Future for Counted<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.0.as_mut().poll(cx)
    }
}

/// The polls of `sql` run as a task: one more than the times it yielded.
fn polls(runtime: &tokio::runtime::Runtime, db: &Arc<Database>, sql: &str) -> usize {
    let count = Arc::new(AtomicUsize::new(0));
    let call = Counted(Box::pin(execute(db, sql)), count.clone());
    runtime.block_on(runtime.spawn(call)).unwrap().unwrap();
    count.load(Ordering::SeqCst)
}

/// In a task a call costs no yield that the engine did not make before this
/// change: the polls of each kind of call are at most those of the engine at
/// 27a8927, counted there with the same calls on a store that answers at once
/// (the tests' engine I/O yields before each request, which these include).
#[test]
fn a_call_in_a_task_polls_no_more_than_before() -> Result<()> {
    let runtime = runtime()?;
    let store = Arc::new(Threads::default());
    let db = Arc::new(runtime.block_on(Database::open(store).into_future())?);
    polls(
        &runtime,
        &db,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
    );
    polls(&runtime, &db, "INSERT INTO t VALUES (1, 'one')");
    let select = "SELECT v FROM t WHERE id = 1";
    let upsert = "INSERT INTO t VALUES (2, 'two') ON CONFLICT DO NOTHING";
    let exists = "CREATE TABLE IF NOT EXISTS d(x)";
    let counted = [
        ("a read of a new text", polls(&runtime, &db, select), 4),
        ("a read, kept", polls(&runtime, &db, select), 1),
        (
            "a write of a new text",
            polls(&runtime, &db, "INSERT INTO t VALUES (2, 'two')"),
            13,
        ),
        ("a write, kept", polls(&runtime, &db, upsert), 11),
        ("a write, kept again", polls(&runtime, &db, upsert), 10),
        ("a DDL", polls(&runtime, &db, "CREATE TABLE d(x)"), 15),
        ("a DDL, kept", polls(&runtime, &db, exists), 10),
        ("a DDL, kept again", polls(&runtime, &db, exists), 9),
        ("BEGIN", polls(&runtime, &db, "BEGIN"), 2),
        ("ROLLBACK", polls(&runtime, &db, "ROLLBACK"), 2),
    ];
    for (call, now, before) in counted {
        assert!(now <= before, "{call}: {now} polls, {before} before");
    }
    // A kept read, with nothing to make durable, is one poll.
    assert_eq!(counted[1].1, 1);
    Ok(())
}
