//! CPU time, allocations and cache misses of a long-lived owner, by cache size.
//!
//! A 10,000-row table with JSON payloads (half random hex) is bulk loaded.
//! Each measured session opens, runs four skewed point lookups (90% within a
//! hot tenth of the rows) and a 50-row range scan; every tenth session also
//! updates a row, which commits. All sessions share one `PageCache`, whose memory is
//! sized as a fraction of the logical database; the rest spills to a 1 GiB disk
//! tier. The backend is strong, so nothing is
//! confirmed. Output is CSV; traffic counters are deterministic, time is the
//! median of the repeats.
use restate_sdk_shared_core::sql::Database;
use restate_sdk_shared_core::state::simulation::{cache_config, open_cache, LatencyKv, MemoryKv};
use restate_sdk_shared_core::state::{KvStore, PageCache, Result};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc,
    },
    time::Instant,
};

struct Counting;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Relaxed);
        ALLOCATED.fetch_add(layout.size() as u64, Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Relaxed);
        ALLOCATED.fetch_add(size as u64, Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static GLOBAL: Counting = Counting;

const ROWS: u64 = 10_000;
const WARMUP: u64 = 50;
const SESSIONS: u64 = 300;
const FRACTIONS: [f64; 5] = [0.05, 0.1, 0.25, 0.5, 1.0];

type Store = Arc<LatencyKv<MemoryKv>>;

async fn baseline() -> Result<(MemoryKv, u64)> {
    let store = MemoryKv::default();
    let kv: Arc<dyn KvStore> = Arc::new(store.clone());
    let db = Database::open(kv.clone()).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, doc TEXT)")
        .await?;
    db.execute("CREATE INDEX by_n ON t(n)").await?;
    db.execute(&format!(
        "WITH RECURSIVE x(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM x WHERE i<{ROWS}) \
         INSERT INTO t SELECT i, i % 97, json_object('id', i, 'user', 'user-' || (i % 500), \
         'tags', json_array('alpha', 'beta', i % 7), 'note', hex(randomblob(40 + i % 30))) FROM x"
    ))
    .await?;
    db.save().await?;
    let pages = Database::open(kv).await?.io_stats().database_len / 4096;
    Ok((store, pages))
}

/// Deterministic skewed row IDs: 90% from the first tenth of the table.
fn row(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let r = *state >> 33;
    if r % 10 < 9 {
        r % (ROWS / 10) + 1
    } else {
        r % ROWS + 1
    }
}

async fn session(store: &Store, cache: &PageCache, n: u64, state: &mut u64) -> Result<()> {
    let kv: Arc<dyn KvStore> = store.clone();
    let db = Database::open(kv).cached(cache, "bench").await?;
    for _ in 0..4 {
        let id = row(state);
        let rows = db
            .query(&format!("SELECT doc FROM t WHERE id = {id}"))
            .await?;
        assert_eq!(rows.len(), 1);
    }
    let start = row(state).min(ROWS - 50);
    let rows = db
        .query(&format!(
            "SELECT count(*) FROM t WHERE id BETWEEN {start} AND {}",
            start + 49
        ))
        .await?;
    assert_eq!(rows[0][0].as_int(), Some(50));
    if n.is_multiple_of(10) {
        let id = row(state);
        db.execute(&format!("UPDATE t SET n = n + 1 WHERE id = {id}"))
            .await?;
        db.save().await?;
    }
    Ok(())
}

struct Sample {
    pack_gets: u64,
    round_trips: u64,
    allocations: u64,
    allocated: u64,
    cache_bytes: usize,
    micros: u128,
}

async fn run(base: &MemoryKv, capacity: usize) -> Result<Sample> {
    let store: Store = Arc::new(LatencyKv::new(base.fork()));
    let cache = open_cache(cache_config(capacity, 1 << 30));
    let mut state = 7;
    for n in 0..WARMUP {
        session(&store, &cache, n + 1, &mut state).await?;
    }
    store.reset();
    store.inner.reset_stats();
    let (allocations, allocated) = (ALLOCATIONS.load(Relaxed), ALLOCATED.load(Relaxed));
    let started = Instant::now();
    for n in 0..SESSIONS {
        session(&store, &cache, n, &mut state).await?;
    }
    let micros = started.elapsed().as_micros();
    Ok(Sample {
        pack_gets: store.inner.stats().kind("pack").gets,
        round_trips: store.round_trips(),
        allocations: ALLOCATIONS.load(Relaxed) - allocations,
        allocated: ALLOCATED.load(Relaxed) - allocated,
        cache_bytes: cache.memory_bytes(),
        micros,
    })
}

fn main() -> Result<()> {
    let repeats: usize = std::env::args()
        .skip(1)
        .find(|arg| !arg.starts_with('-'))
        .map_or(Ok(3), |arg| arg.parse())?;
    futures::executor::block_on(async {
        let (base, pages) = baseline().await?;
        let logical = pages as usize * 4096;
        println!(
            "capacity_fraction,capacity_bytes,db_pages,sessions,pack_gets,round_trips,allocations,allocated_bytes,cache_bytes,median_micros"
        );
        for fraction in FRACTIONS {
            let capacity = (logical as f64 * fraction) as usize;
            let mut samples = Vec::new();
            for _ in 0..repeats {
                samples.push(run(&base, capacity).await?);
            }
            let first = &samples[0];
            for s in &samples[1..] {
                // Memory use depends on the disk tier's timing; traffic does not.
                assert_eq!(
                    (s.pack_gets, s.round_trips),
                    (first.pack_gets, first.round_trips),
                    "counters must not vary"
                );
            }
            let mut micros: Vec<_> = samples.iter().map(|s| s.micros).collect();
            micros.sort();
            println!(
                "{fraction},{capacity},{pages},{SESSIONS},{},{},{},{},{},{}",
                first.pack_gets,
                first.round_trips,
                first.allocations,
                first.allocated,
                first.cache_bytes,
                micros[micros.len() / 2]
            );
        }
        Ok(())
    })
}
