//! A tail in Restate and checkpoints in packs, on SQL workloads, one flush
//! per session: the requests, bytes and round trips a flush costs (its share
//! of the checkpoints and garbage collections included), what is stored, and
//! a cold read.
//!
//! ```sh
//! cargo bench --features sql,__bench --bench flush_costs
//! S3=1 cargo bench --features sql,__bench --bench flush_costs
//! ```
//!
//! `FRAME_PAGES`, `INLINE_BYTES`, `CHECKPOINT_ENTRIES`, `CHECKPOINT_BYTES`
//! and `L0_BYTES` set the state's options; `WORKLOAD` runs only the
//! workloads it names part of, `FLUSHES` that many flushes each (an aged
//! database); `LABEL` names the run in its rows. Prints Markdown rows.
//!
//! Without `S3`, the packs are kept in Restate's state too. With `S3=1`,
//! the packs past L0 (`L0_BYTES`, 0 for all of them) are in an object
//! store, and the rows are what S3 would bill:
//! requests and bytes per flush, and dollars per million flushes and per
//! month stored, at S3 Standard's and S3 Express One Zone's prices.
use restate_sdk_shared_core::sql::{Database, Value};
use restate_sdk_shared_core::state::simulation::{
    cache_config, open_cache, LatencyKv, MemoryBlobs, MemoryKv, Namespace,
};
use restate_sdk_shared_core::state::{KvStore, Result, StateOptions};
use std::sync::Arc;

type Store = Arc<LatencyKv<MemoryKv>>;

/// Deterministic text: words from a vocabulary of made-up words.
struct Words {
    seed: u64,
    vocabulary: Vec<String>,
}

impl Words {
    fn new() -> Self {
        let mut words = Self {
            seed: 0x9e37_79b9_7f4a_7c15,
            vocabulary: Vec::new(),
        };
        const SYLLABLES: [&str; 24] = [
            "ka", "ro", "mi", "tel", "an", "us", "ver", "do", "sta", "ne", "pri", "lo", "qua",
            "en", "tor", "ia", "be", "sol", "ux", "re", "mon", "ti", "gra", "el",
        ];
        for _ in 0..2000 {
            let n = 1 + words.next() % 3;
            let word: String = (0..n)
                .map(|_| SYLLABLES[(words.next() % 24) as usize])
                .collect();
            words.vocabulary.push(word);
        }
        words
    }

    fn next(&mut self) -> u64 {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        self.seed
    }

    /// About `len` bytes of words.
    fn text(&mut self, len: usize) -> String {
        let mut text = String::new();
        while text.len() < len {
            // Zipf-ish: common words more often.
            let r = self.next() % 2000;
            let index = (r * r / 2000) as usize;
            text.push_str(&self.vocabulary[index]);
            text.push(' ');
        }
        text.truncate(len);
        text
    }
}

fn options() -> StateOptions {
    fn var<T: std::str::FromStr>(name: &str) -> Option<T> {
        std::env::var(name).ok().and_then(|v| v.parse().ok())
    }
    let default = StateOptions::default();
    StateOptions {
        frame_pages: var("FRAME_PAGES").unwrap_or(default.frame_pages),
        inline_record_bytes: var("INLINE_BYTES").unwrap_or(default.inline_record_bytes),
        checkpoint_entries: var("CHECKPOINT_ENTRIES").unwrap_or(default.checkpoint_entries),
        checkpoint_bytes: var("CHECKPOINT_BYTES").unwrap_or(default.checkpoint_bytes),
        l0_bytes: var("L0_BYTES").unwrap_or(default.l0_bytes),
        ..default
    }
}

/// Where a database is: Restate's state, and S3 with `S3=1`.
struct Stores {
    restate: Store,
    s3: Option<MemoryBlobs>,
}

async fn open(stores: &Stores, cache: &Option<Namespace>) -> Result<Database> {
    let kv: Arc<dyn KvStore> = stores.restate.clone();
    let mut open = Database::open(kv).options(options());
    if let Some(s3) = &stores.s3 {
        open = open.checkpoints(Arc::new(s3.clone()));
    }
    if let Some(ns) = cache {
        open = open.cached(&ns.cache, &ns.key);
    }
    open.await
}

#[derive(Clone, Copy)]
struct Workload {
    name: &'static str,
    rows: u64,
    payload: usize,
    flushes: u64,
}

const WORKLOADS: [Workload; 11] = [
    // A @restatedev/sessions object: a message row, and its sends in the outbox.
    Workload {
        name: "chat+outbox",
        rows: 0,
        payload: 400,
        flushes: 384,
    },
    Workload {
        name: "small: update 1",
        rows: 100,
        payload: 64,
        flushes: 384,
    },
    Workload {
        name: "10k: update 1",
        rows: 10_000,
        payload: 256,
        flushes: 384,
    },
    Workload {
        name: "10k: update 10",
        rows: 10_000,
        payload: 256,
        flushes: 384,
    },
    Workload {
        name: "10k: insert 1",
        rows: 10_000,
        payload: 256,
        flushes: 384,
    },
    Workload {
        name: "100k: update 1",
        rows: 100_000,
        payload: 256,
        flushes: 256,
    },
    // Aged: most of its pages patched, a manifest about 7× as large.
    Workload {
        name: "100k aged: update 1",
        rows: 100_000,
        payload: 256,
        flushes: 16_384,
    },
    // 56 MB: a manifest of about 6 KiB.
    Workload {
        name: "200k: update 1",
        rows: 200_000,
        payload: 256,
        flushes: 256,
    },
    Workload {
        name: "200k: update 10",
        rows: 200_000,
        payload: 256,
        flushes: 256,
    },
    // 280 MB: about 30 KiB.
    Workload {
        name: "1M: update 1",
        rows: 1_000_000,
        payload: 256,
        flushes: 256,
    },
    Workload {
        name: "1M: update 10",
        rows: 1_000_000,
        payload: 256,
        flushes: 256,
    },
];

async fn load(stores: &Stores, w: Workload, words: &mut Words) -> Result<()> {
    let mut db = open(stores, &None).await?;
    if w.name.starts_with("chat") {
        db.execute("CREATE TABLE messages(id INTEGER PRIMARY KEY, role TEXT, text TEXT)")
            .await?;
        return db.save().await;
    }
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, payload TEXT)")
        .await?;
    db.execute("CREATE INDEX by_a ON t(a)").await?;
    // A session of 100k rows at a time, within its private changes' limit.
    for batch in (1..=w.rows).collect::<Vec<_>>().chunks(100_000) {
        db.execute("BEGIN").await?;
        for &id in batch {
            db.execute_with(
                "INSERT INTO t VALUES(?, ?, ?)",
                [
                    Value::from_i64(id as i64),
                    Value::from_i64(id as i64),
                    Value::build_text(words.text(w.payload)),
                ],
            )
            .await?;
        }
        db.execute("COMMIT").await?;
        db.save().await?;
        db = open(stores, &None).await?;
    }
    db.save().await
}

fn row(w: Workload, i: u64, n: u64) -> i64 {
    ((i * 7919 + n * 104_729) % w.rows + 1) as i64
}

/// A flush of `w`: one commit.
async fn step(db: &Database, w: Workload, i: u64, words: &mut Words) -> Result<()> {
    match w.name {
        "chat+outbox" => {
            let text = words.text(w.payload);
            db.begin().await?;
            db.execute_with(
                "INSERT INTO messages(role, text) VALUES(?, ?)",
                [
                    Value::build_text(if i.is_multiple_of(2) {
                        "user"
                    } else {
                        "assistant"
                    }),
                    Value::build_text(text.clone()),
                ],
            )
            .await?;
            let outbox = format!(
                r#"{{"sends":[{{"service":"Notifier","handler":"message","key":"session-17","idempotencyKey":"inv_1{i:015x}-{i}","input":{{"id":{i},"text":"{text}"}}}}]}}"#
            );
            db.commit([("outbox".into(), outbox.into())]).await
        }
        name if name.ends_with("update 1") => {
            db.execute_with(
                "UPDATE t SET a = a + 1, payload = ? WHERE id = ?",
                [
                    Value::build_text(words.text(w.payload)),
                    Value::from_i64(row(w, i, 0)),
                ],
            )
            .await
        }
        name if name.ends_with("update 10") => {
            db.execute("BEGIN").await?;
            for n in 0..10 {
                db.execute_with(
                    "UPDATE t SET a = a + 1 WHERE id = ?",
                    [Value::from_i64(row(w, i, n))],
                )
                .await?;
            }
            db.execute("COMMIT").await
        }
        "10k: insert 1" => {
            let id = (w.rows + i + 1) as i64;
            db.execute_with(
                "INSERT INTO t VALUES(?, ?, ?)",
                [
                    Value::from_i64(id),
                    Value::from_i64(id),
                    Value::build_text(words.text(w.payload)),
                ],
            )
            .await
        }
        _ => unreachable!(),
    }
}

/// Prices in US East, in dollars: S3 Standard, then S3 Express One Zone.
/// A LIST is billed as a PUT; deletes are free.
const PUT: [f64; 2] = [0.005 / 1000.0, 0.00113 / 1000.0];
const GET: [f64; 2] = [0.0004 / 1000.0, 0.00003 / 1000.0];
const UPLOAD_GB: [f64; 2] = [0.0, 0.0032];
const STORED_GB_MONTH: [f64; 2] = [0.023, 0.11];
const GB: f64 = (1u64 << 30) as f64;

fn s3() -> bool {
    std::env::var_os("S3").is_some()
}

async fn run(w: Workload, cached: bool) -> Result<()> {
    let memory = MemoryKv::default();
    let stores = Stores {
        restate: Arc::new(LatencyKv::new(memory.clone())),
        s3: s3().then(MemoryBlobs::default),
    };
    let mut words = Words::new();
    load(&stores, w, &mut words).await?;
    let cache =
        cached.then(|| Namespace::new(&open_cache(cache_config(256 << 20, 1 << 30)), "bench"));
    // Warm the owner, and its cache with every frame, and checkpoint the load,
    // outside the measurement.
    let db = open(&stores, &cache).await?;
    if !w.name.starts_with("chat") {
        db.query("SELECT count(*), sum(length(payload)) FROM t")
            .await?;
        db.query("SELECT count(*) FROM t INDEXED BY by_a WHERE a > 0")
            .await?;
    }
    db.begin().await?;
    db.commit([("outbox".into(), "warm".into())]).await?;
    db.save().await?;
    stores.restate.reset();
    memory.reset_stats();
    let s3_before = stores
        .s3
        .as_ref()
        .map(|s3| (s3.puts(), s3.gets(), s3.lists(), s3.uploaded()));
    for i in 0..w.flushes {
        let db = open(&stores, &cache).await?;
        step(&db, w, i, &mut words).await?;
        db.save().await?;
    }
    let s = memory.stats();
    let rts = stores.restate.round_trips();
    let n = w.flushes as f64;
    let restate_stored = memory.stored_bytes() + memory.stored_key_bytes();
    let keys = memory.keys().len();
    // A cold open by a new owner, and a point read.
    stores.restate.reset();
    memory.reset_stats();
    let s3_cold = stores.s3.as_ref().map(|s3| s3.gets());
    let db = open(&stores, &None).await?;
    let probe = match w.name {
        "chat+outbox" => "SELECT text FROM messages WHERE id = 7",
        _ => "SELECT payload FROM t WHERE id = 4242",
    };
    db.query(probe).await?;
    let cold = memory.stats();
    let cold_rts = stores.restate.round_trips();
    let db_pages = db.query("PRAGMA page_count").await?[0][0]
        .as_int()
        .unwrap_or(0);
    drop(db);
    let label = std::env::var("LABEL").unwrap_or_else(|_| "tail".into());
    let restate_written = (s.bytes_written as f64) / n;
    if let (Some(s3), Some((puts, gets, lists, uploaded)), Some(cold_before)) =
        (&stores.s3, s3_before, s3_cold)
    {
        let puts = (s3.puts() - puts) as f64 / n;
        let gets = (s3.gets() - gets) as f64 / n;
        let lists = (s3.lists() - lists) as f64 / n;
        let upload = (s3.uploaded() - uploaded) as f64 / n;
        let stored = s3.stored_bytes() as f64;
        let per_million = |tier: usize| {
            1e6 * ((puts + lists) * PUT[tier] + gets * GET[tier] + upload / GB * UPLOAD_GB[tier])
        };
        println!(
            "| {} | {label} | {} | {:.2} | {:.0} | {:.0} | {:.4} | {:.3} | {:.4} | {:.0} | {:.2} | {} | {:.2} | {:.2} | {:.5} | {:.5} |",
            w.name,
            if cached { "yes" } else { "no" },
            rts as f64 / n,
            restate_written,
            restate_stored as f64 / 1024.0,
            puts,
            gets,
            lists,
            upload,
            stored / 1024.0 / 1024.0,
            s3.gets() - cold_before,
            per_million(0),
            per_million(1),
            stored / GB * STORED_GB_MONTH[0],
            stored / GB * STORED_GB_MONTH[1],
        );
        return Ok(());
    }
    println!(
        "| {} | {label} | {} | {:.2} | {:.2} | {:.0} | {:.0} | {:.0} | {:.0} | {:.2} | {:.1} | {:.0} | {keys} | {} | {} | {} | {:.0} |",
        w.name,
        if cached { "yes" } else { "no" },
        rts as f64 / n,
        s.puts as f64 / n,
        restate_written,
        s.kind("tail").bytes_written as f64 / n,
        s.kind("pack").bytes_written as f64 / n,
        s.kind("manifest").bytes_written as f64 / n,
        s.deletes as f64 / n,
        s.gets as f64 / n,
        restate_stored as f64 / 1024.0,
        db_pages * 4,
        cold_rts,
        cold.gets,
        cold.bytes_read as f64 / 1024.0,
    );
    Ok(())
}

fn main() -> Result<()> {
    if s3() {
        println!(
            "| Workload | Run | Cache | RT/flush | Restate B/flush | Restate stored KiB | S3 PUTs/flush | S3 GETs/flush | S3 LISTs/flush | S3 upload B/flush | S3 stored MiB | Cold S3 GETs | Standard $/M flushes | Express $/M flushes | Standard $/month stored | Express $/month stored |"
        );
        println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    } else {
        println!(
            "| Workload | Run | Cache | RT/flush | Puts/flush | Put B/flush | Tail B | Pack B | Manifest B | Deletes/flush | Gets/flush | Stored KiB | Keys | DB KiB | Cold RT | Cold gets | Cold read KiB |"
        );
        println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    }
    let only = std::env::var("WORKLOAD").ok();
    let flushes = std::env::var("FLUSHES").ok().and_then(|v| v.parse().ok());
    futures::executor::block_on(async {
        for w in WORKLOADS {
            if only.as_deref().is_some_and(|only| !w.name.contains(only)) {
                continue;
            }
            let w = Workload {
                flushes: flushes.unwrap_or(w.flushes),
                ..w
            };
            for cached in [false, true] {
                run(w, cached).await?;
            }
        }
        Ok(())
    })
}
