//! On a multi-thread runtime, a call polled outside its tasks (as the Node
//! addon polls one once in place, on the JavaScript thread, before it spawns
//! it) runs on the runtime's threads, never on the thread that polls it.
use crate::sql::Database;
use crate::state::{kv::KvStore, Result};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{
    collections::{BTreeMap, HashSet},
    future::{Future, IntoFuture},
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    thread::{self, ThreadId},
};

/// An immediately ready store that notes the threads that read or write it.
#[derive(Default)]
struct Threads {
    values: Mutex<BTreeMap<String, Bytes>>,
    seen: Mutex<HashSet<ThreadId>>,
}

impl Threads {
    fn note(&self) {
        self.seen.lock().unwrap().insert(thread::current().id());
    }

    fn seen(&self) -> HashSet<ThreadId> {
        std::mem::take(&mut self.seen.lock().unwrap())
    }
}

impl KvStore for Threads {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        self.note();
        Box::pin(async move { Ok(self.values.lock().unwrap().get(key).cloned()) })
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.note();
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

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
}

/// `call` polled once here, inside the runtime's context but in no task of
/// it, then, unless that finished it, run to its end by the runtime.
fn in_place_then_spawned<F: Future>(runtime: &tokio::runtime::Runtime, call: F) -> F::Output {
    let mut call = Box::pin(call);
    let polled = {
        let _entered = runtime.enter();
        call.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    };
    match polled {
        Poll::Ready(output) => output,
        Poll::Pending => runtime.block_on(call),
    }
}

#[test]
fn a_call_polled_outside_a_task_runs_on_the_runtime() -> Result<()> {
    let runtime = runtime()?;
    let store = Arc::new(Threads::default());
    let here = thread::current().id();

    let db = in_place_then_spawned(&runtime, Database::open(store.clone()).into_future())?;
    in_place_then_spawned(
        &runtime,
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)"),
    )?;
    in_place_then_spawned(&runtime, db.execute("INSERT INTO t VALUES (1, 'one')"))?;
    in_place_then_spawned(&runtime, db.begin())?;
    in_place_then_spawned(&runtime, db.execute("INSERT INTO t VALUES (2, 'two')"))?;
    in_place_then_spawned(&runtime, db.commit([("outbox".into(), Bytes::from("x"))]))?;
    in_place_then_spawned(&runtime, db.save())?;
    let seen = store.seen();
    assert!(!seen.is_empty(), "the store was never used");
    assert!(!seen.contains(&here), "ran where it was polled");

    let db = in_place_then_spawned(&runtime, Database::open(store.clone()).into_future())?;
    let rows = in_place_then_spawned(&runtime, db.query("SELECT v FROM t ORDER BY id"))?;
    assert_eq!(rows.len(), 2);
    assert!(!store.seen().contains(&here), "opened where it was polled");
    Ok(())
}

/// A call dropped once it is on the runtime ends its session, as one dropped
/// in place does, whether or not its task had finished; the store holds the
/// state before it or after it.
#[test]
fn a_call_dropped_on_the_runtime_ends_its_session() -> Result<()> {
    use std::sync::mpsc;
    let runtime = runtime()?;
    let store = Arc::new(BlockingPut::default());
    let db = runtime.block_on(Database::open(store.clone()).into_future())?;
    runtime.block_on(db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)"))?;
    // Its put held until the call is dropped, so the call cannot finish
    // before it is: an immediately ready store let a fast worker finish it
    // within the poll in place, and a call that returned is not dropped.
    let (started, _put_started) = mpsc::channel();
    let (release, released) = mpsc::channel();
    *store.armed.lock().unwrap() = Some((started, released));
    {
        let mut call = Box::pin(db.execute("INSERT INTO t VALUES (1)"));
        let _entered = runtime.enter();
        let polled = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        assert!(polled.is_pending(), "the call finished in place");
    }
    release.send(()).unwrap();
    assert!(!db.is_usable());
    let ended = runtime.block_on(db.query("SELECT count(*) FROM t"));
    assert!(ended.is_err(), "the session went on after a dropped call");

    let db = runtime.block_on(Database::open(store).into_future())?;
    let rows = runtime.block_on(db.query("SELECT count(*) FROM t"))?;
    assert!(matches!(rows[0][0].to_string().as_str(), "0" | "1"));
    assert_eq!(
        runtime.block_on(db.query("PRAGMA integrity_check"))?[0][0].to_string(),
        "ok"
    );
    Ok(())
}

/// A store whose next put, once armed, says it started and blocks until
/// released: a call's task that is in it cannot be aborted until it returns.
#[derive(Default)]
struct BlockingPut {
    values: Mutex<BTreeMap<String, Bytes>>,
    armed: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
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
        Box::pin(async { Ok(()) })
    }
}

/// A call dropped while its task is inside a blocking put, which then
/// finishes without yielding, still leaves the session unusable.
#[test]
fn a_call_dropped_while_its_task_cannot_stop_leaves_the_session_unusable() -> Result<()> {
    use std::{sync::mpsc, time::Duration};
    let runtime = runtime()?;
    let store = Arc::new(BlockingPut::default());
    let db = runtime.block_on(Database::open(store.clone()).into_future())?;
    runtime.block_on(db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)"))?;
    let (started, put_started) = mpsc::channel();
    let (release, released) = mpsc::channel();
    *store.armed.lock().unwrap() = Some((started, released));
    {
        let mut call = Box::pin(db.execute("INSERT INTO t VALUES (1)"));
        let _entered = runtime.enter();
        let _ = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        put_started
            .recv_timeout(Duration::from_secs(10))
            .expect("the call's task reached its put");
        // Dropped here, while its task is blocked in the put.
    }
    release.send(()).unwrap();
    // The task now finishes its call; give it time to.
    std::thread::sleep(Duration::from_millis(200));
    assert!(!db.is_usable(), "usable after a dropped call");
    assert!(runtime.block_on(db.query("SELECT 1")).is_err());
    Ok(())
}
