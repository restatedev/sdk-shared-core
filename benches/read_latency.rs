//! How long uncached reads wait on storage: each sample opens the database
//! in a new state, without a page cache, and runs one query, then another
//! in the same session. Restate and S3 run on one virtual clock, each
//! request taking its store's time; requests issued together overlap.
//!
//! ```sh
//! cargo bench --features sql,__bench --bench read_latency
//! S3=1 cargo bench --features sql,__bench --bench read_latency
//! ```
//!
//! Without `S3`, the packs are kept in Restate's state too; with `S3=1`,
//! past L0 (`L0_BYTES`, 0 for all of them), in S3. `RESTATE_MS` (2) and `S3_MS` (20) set a request's time; CPU time,
//! bandwidth and queueing are not modeled. `WORKLOAD` runs only the
//! databases it names part of. Prints Markdown rows: milliseconds at the
//! median, the 90th percentile and the worst sample, and the mean round
//! trips each store adds to the critical path, and requests.
use futures::{
    future::BoxFuture,
    task::{waker, ArcWake},
};
use restate_sdk_shared_core::sql::{Database, Value};
use restate_sdk_shared_core::state::simulation::{MemoryBlobs, MemoryKv};
use restate_sdk_shared_core::state::{BlobStore, KvStore, Result, StateOptions};
use std::future::Future;
use std::{
    io,
    ops::Range,
    pin::pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    thread::{self, Thread},
};

/// Lanes of the clock: microseconds, then the round trips to Restate and to
/// S3, each the critical path as if only that lane's requests took time.
const LANES: usize = 3;
type Lanes = [u64; LANES];

/// A virtual clock: a request issued at `t` completes at `t + cost`, lane
/// by lane. Time stands still while any task can run, so requests issued
/// together overlap however their futures are polled; [`Clock::block_on`]
/// moves it to the next completion once every task waits.
#[derive(Clone, Default)]
struct Clock(Arc<Mutex<Timeline>>);

#[derive(Default)]
struct Timeline {
    now: Lanes,
    pending: Vec<Timer>,
}

struct Timer {
    end: Lanes,
    done: Arc<AtomicBool>,
    waker: Waker,
}

impl Clock {
    fn now(&self) -> Lanes {
        self.0.lock().unwrap().now
    }

    /// `request`, done once `cost` has passed since it was first polled.
    fn timed<'a, T: Send + 'a>(
        &'a self,
        cost: Lanes,
        request: BoxFuture<'a, io::Result<T>>,
    ) -> BoxFuture<'a, io::Result<T>> {
        Box::pin(async move {
            let done = Arc::new(AtomicBool::new(false));
            let mut issued = false;
            futures::future::poll_fn(|cx| {
                if done.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                if !std::mem::replace(&mut issued, true) {
                    let mut timeline = self.0.lock().unwrap();
                    let end = std::array::from_fn(|lane| timeline.now[lane] + cost[lane]);
                    timeline.pending.push(Timer {
                        end,
                        done: done.clone(),
                        waker: cx.waker().clone(),
                    });
                }
                Poll::Pending
            })
            .await;
            request.await
        })
    }

    /// Complete the requests that end first; false if none is pending.
    fn advance(&self) -> bool {
        let mut timeline = self.0.lock().unwrap();
        let Some(next) = timeline.pending.iter().map(|timer| timer.end[0]).min() else {
            return false;
        };
        let (due, pending) = std::mem::take(&mut timeline.pending)
            .into_iter()
            .partition(|timer| timer.end[0] == next);
        timeline.pending = pending;
        for timer in due {
            for lane in 0..LANES {
                timeline.now[lane] = timeline.now[lane].max(timer.end[lane]);
            }
            timer.done.store(true, Ordering::Release);
            timer.waker.wake();
        }
        true
    }

    /// Run `future` to completion, moving the clock whenever it waits on
    /// requests only.
    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        struct Wake(AtomicBool, Thread);
        impl ArcWake for Wake {
            fn wake_by_ref(this: &Arc<Self>) {
                this.0.store(true, Ordering::Release);
                this.1.unpark();
            }
        }
        let woken = Arc::new(Wake(AtomicBool::new(false), thread::current()));
        let waker = waker(woken.clone());
        let mut future = pin!(future);
        loop {
            if let Poll::Ready(out) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                return out;
            }
            if !woken.0.swap(false, Ordering::AcqRel) && !self.advance() {
                thread::park();
            }
        }
    }
}

/// A store whose requests take `cost` on `clock`; Restate's writes are
/// queued, and take none.
struct Timed<S> {
    inner: S,
    clock: Clock,
    cost: Lanes,
}

impl KvStore for Timed<MemoryKv> {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<bytes::Bytes>>> {
        self.clock.timed(self.cost, self.inner.get(key))
    }
    fn put(&self, key: &str, value: bytes::Bytes) -> io::Result<()> {
        self.inner.put(key, value)
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.inner.delete(key)
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        self.clock.timed(self.cost, self.inner.sync())
    }
    fn keys(&self) -> BoxFuture<'_, io::Result<Vec<String>>> {
        self.clock.timed(self.cost, KvStore::keys(&self.inner))
    }
}

impl BlobStore for Timed<MemoryBlobs> {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> BoxFuture<'a, io::Result<Option<bytes::Bytes>>> {
        self.clock
            .timed(self.cost, self.inner.get_range(key, range))
    }
    fn put<'a>(&'a self, key: &'a str, value: bytes::Bytes) -> BoxFuture<'a, io::Result<()>> {
        self.clock.timed(self.cost, self.inner.put(key, value))
    }
    fn delete<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, io::Result<()>> {
        self.clock.timed(self.cost, self.inner.delete(keys))
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<String>>> {
        self.clock.timed(self.cost, self.inner.list(prefix))
    }
}

struct Stores {
    clock: Clock,
    memory: MemoryKv,
    restate: Arc<Timed<MemoryKv>>,
    s3: Option<(MemoryBlobs, Arc<Timed<MemoryBlobs>>)>,
}

impl Stores {
    fn new(clock: &Clock) -> Self {
        let ms = |name: &str, default: f64| {
            let ms: f64 = std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default);
            (ms * 1000.0) as u64
        };
        let clock = clock.clone();
        let memory = MemoryKv::default();
        let restate = Arc::new(Timed {
            inner: memory.clone(),
            clock: clock.clone(),
            cost: [ms("RESTATE_MS", 2.0), 1, 0],
        });
        let s3 = std::env::var_os("S3").map(|_| {
            let blobs = MemoryBlobs::default();
            let timed = Arc::new(Timed {
                inner: blobs.clone(),
                clock: clock.clone(),
                cost: [ms("S3_MS", 20.0), 0, 1],
            });
            (blobs, timed)
        });
        Self {
            clock,
            memory,
            restate,
            s3,
        }
    }

    async fn open(&self) -> Result<Database> {
        let mut open = Database::open(self.restate.clone());
        if let Ok(bytes) = std::env::var("L0_BYTES") {
            open = open.options(StateOptions {
                l0_bytes: bytes.parse()?,
                ..Default::default()
            });
        }
        if let Some((_, s3)) = &self.s3 {
            open = open.checkpoints(s3.clone());
        }
        open.await
    }

    /// Requests so far: Restate's gets, S3's GETs.
    fn requests(&self) -> [u64; 2] {
        let gets = self.s3.as_ref().map_or(0, |(blobs, _)| blobs.gets());
        [self.memory.stats().gets, gets]
    }
}

/// A table of `rows` rows of 256 bytes, indexed on `a`, then `aged` flushes
/// of a row each, so that the tail holds what a live database's does.
async fn load(stores: &Stores, rows: i64, aged: i64) -> Result<()> {
    let db = stores.open().await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, payload TEXT)")
        .await?;
    db.execute("CREATE INDEX by_a ON t(a)").await?;
    db.save().await?;
    let payload = |id: i64| Value::build_text(text(id));
    for batch in (1..=rows).collect::<Vec<_>>().chunks(100_000) {
        let db = stores.open().await?;
        db.execute("BEGIN").await?;
        for &id in batch {
            db.execute_with(
                "INSERT INTO t VALUES(?, ?, ?)",
                [Value::from_i64(id), Value::from_i64(id), payload(id)],
            )
            .await?;
        }
        db.execute("COMMIT").await?;
        db.save().await?;
    }
    for i in 0..aged {
        let db = stores.open().await?;
        db.execute_with(
            "UPDATE t SET payload = ? WHERE id = ?",
            [payload(-i), Value::from_i64(key(rows, i))],
        )
        .await?;
        db.save().await?;
    }
    Ok(())
}

/// 256 bytes of hex, as random as a hash of `id`: text that compresses about
/// as well as prose.
fn text(id: i64) -> String {
    let mut x = id as u64 ^ 0x9e37_79b9_7f4a_7c15;
    (0..16)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            format!("{x:016x}")
        })
        .collect()
}

/// The `i`th sample's row: spread over the table.
fn key(rows: i64, i: i64) -> i64 {
    (i * 7919 + 104_729) % rows + 1
}

struct Query {
    name: &'static str,
    sql: &'static str,
}

const QUERIES: [Query; 3] = [
    Query {
        name: "point (primary key)",
        sql: "SELECT payload FROM t WHERE id = ?",
    },
    Query {
        name: "point (index)",
        sql: "SELECT payload FROM t WHERE a = ?",
    },
    Query {
        name: "range of 100 rows",
        sql: "SELECT sum(length(payload)) FROM t WHERE id BETWEEN ?1 AND ?1 + 99",
    },
];

/// What one step of a sample waited: its lanes and requests.
#[derive(Default)]
struct Step {
    micros: Vec<u64>,
    restate: u64,
    s3: u64,
    requests: [u64; 2],
}

impl Step {
    fn add(&mut self, before: (Lanes, [u64; 2]), after: (Lanes, [u64; 2])) {
        self.micros.push(after.0[0] - before.0[0]);
        self.restate += after.0[1] - before.0[1];
        self.s3 += after.0[2] - before.0[2];
        self.requests[0] += after.1[0] - before.1[0];
        self.requests[1] += after.1[1] - before.1[1];
    }

    fn row(&mut self, database: &str, query: &str, step: &str, s3: bool) {
        let n = self.micros.len() as f64;
        self.micros.sort_unstable();
        let at = |q: f64| self.micros[((n - 1.0) * q).round() as usize] as f64 / 1000.0;
        let mut row = format!(
            "| {database} | {query} | {step} | {:.0} | {:.0} | {:.0} | {:.1} | {:.1}",
            at(0.5),
            at(0.9),
            at(1.0),
            self.restate as f64 / n,
            self.requests[0] as f64 / n,
        );
        if s3 {
            row += &format!(
                " | {:.1} | {:.1}",
                self.s3 as f64 / n,
                self.requests[1] as f64 / n
            );
        }
        println!("{row} |");
    }
}

const SAMPLES: i64 = 64;

async fn run(clock: &Clock, database: &str, rows: i64) -> Result<()> {
    let stores = Stores::new(clock);
    load(&stores, rows, 64).await?;
    let now = || (stores.clock.now(), stores.requests());
    for query in &QUERIES {
        let (mut open, mut first, mut second) = (Step::default(), Step::default(), Step::default());
        for i in 0..SAMPLES {
            let k = Value::from_i64(key(rows, i));
            let before = now();
            let db = stores.open().await?;
            let opened = now();
            open.add(before, opened);
            db.query_with(query.sql, [k]).await?;
            let queried = now();
            first.add(opened, queried);
            let k = Value::from_i64(key(rows, i + SAMPLES));
            db.query_with(query.sql, [k]).await?;
            second.add(queried, now());
        }
        let s3 = stores.s3.is_some();
        if query.name.starts_with("point (primary") {
            open.row(database, "", "open", s3);
        }
        first.row(database, query.name, "first query", s3);
        second.row(database, query.name, "second query", s3);
    }
    Ok(())
}

fn main() -> Result<()> {
    let s3 = std::env::var_os("S3").is_some();
    let mut header = String::from(
        "| Database | Query | Step | p50 ms | p90 ms | max ms | Restate RT | Restate gets",
    );
    let mut rule = String::from("|---|---|---|---|---|---|---|---");
    if s3 {
        header += " | S3 RT | S3 GETs";
        rule += "|---|---";
    }
    println!("{header} |\n{rule}|");
    let only = std::env::var("WORKLOAD").ok();
    let clock = Clock::default();
    clock.block_on(async {
        for (database, rows) in [
            ("100 rows", 100),
            ("10k rows", 10_000),
            ("100k rows", 100_000),
            ("1M rows", 1_000_000),
        ] {
            if only
                .as_ref()
                .is_some_and(|only| !database.contains(only.as_str()))
            {
                continue;
            }
            run(&clock, database, rows).await?;
        }
        Ok(())
    })
}
