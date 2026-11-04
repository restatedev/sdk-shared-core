//! Operation/byte benchmark. MemoryKv is a simulator, not a remote durability benchmark.
use restate_sdk_shared_core::sql::Database;
use restate_sdk_shared_core::state::simulation::{MemoryKv, Stats};
use restate_sdk_shared_core::state::{KvStore, Result};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// A session of the database in `store`.
async fn open(store: Arc<impl KvStore + 'static>) -> Result<Database> {
    Database::open(store).await
}

#[derive(Clone, Copy)]
struct Case {
    rows: usize,
    payload: usize,
    indexes: usize,
}
async fn baseline(case: Case) -> Result<MemoryKv> {
    let store = MemoryKv::default();
    let db = open(Arc::new(store.clone())).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, note INTEGER, payload TEXT)").await?;
    for column in ["a", "b", "c"].into_iter().take(case.indexes) {
        db.execute(&format!("CREATE INDEX by_{column} ON t({column})"))
            .await?;
    }
    db.execute(&format!("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<{}) INSERT INTO t SELECT x, x, x, x, 0, printf('%0{}d',x) FROM n", case.rows, case.payload)).await?;
    db.save().await?;
    store.crash();
    Ok(store)
}
fn print_row(
    case: Case,

    op: &str,
    run: usize,
    sizes: (u64, usize, usize),
    elapsed: Duration,
    s: &Stats,
) {
    let (db_bytes, before, after) = sizes;
    println!(
        "{},{},{},{op},{run},{db_bytes},{before},{after},{},{},{},{},{},{},{},{}",
        case.rows,
        case.payload,
        case.indexes,
        elapsed.as_micros(),
        s.gets,
        s.puts,
        s.deletes,
        s.bytes_read,
        s.bytes_written,
        s.kind("tail").bytes_written,
        s.kind("head").bytes_written
    );
}
async fn run(case: Case, repeats: usize) -> Result<()> {
    let base = baseline(case).await?;
    let db_bytes = open(Arc::new(base.fork())).await?.io_stats().database_len;
    let before = base.stored_bytes();
    let id = case.rows / 2;
    let query = format!("SELECT id, length(payload) FROM t WHERE id={id}");
    for repeat in 0..repeats {
        for op in [
            "cold_pk",
            "warm_pk",
            "secondary_lookup",
            "update_note",
            "update_a",
            "insert",
            "delete",
            "ten_updates_one_commit",
            "ten_updates_ten_commits",
        ] {
            if op == "secondary_lookup" && case.indexes == 0 {
                continue;
            }
            let store = Arc::new(base.fork());
            let mut warm = if op == "warm_pk" {
                let db = open(store.clone()).await?;
                db.query(&query).await?;
                Some(db)
            } else {
                None
            };
            store.reset_stats();
            let start = Instant::now();
            let db = match warm.take() {
                Some(db) => db,
                None => open(store.clone()).await?,
            };
            match op {
                "cold_pk" | "warm_pk" | "secondary_lookup" => {
                    let sql = if op == "secondary_lookup" {
                        format!("SELECT id, length(payload) FROM t WHERE a={id}")
                    } else {
                        query.clone()
                    };
                    let rows = db.query(&sql).await?;
                    assert_eq!(rows[0][0].to_string(), id.to_string());
                    assert_eq!(rows[0][1].to_string(), case.payload.to_string());
                    let elapsed = start.elapsed();
                    print_row(
                        case,
                        op,
                        repeat,
                        (db_bytes, before, store.stored_bytes()),
                        elapsed,
                        &store.stats(),
                    );
                    // Drop happens outside the read timing.
                    drop(db);
                    continue;
                }
                "update_note" => {
                    db.execute(&format!("UPDATE t SET note=note+1 WHERE id={id}"))
                        .await?
                }
                "update_a" => {
                    db.execute(&format!("UPDATE t SET a=a+1000000 WHERE id={id}"))
                        .await?
                }
                "insert" => {
                    db.execute(&format!(
                        "INSERT INTO t VALUES({},999999,999999,999999,0,printf('%0{}d',1))",
                        case.rows + 1,
                        case.payload
                    ))
                    .await?
                }
                "delete" => db.execute(&format!("DELETE FROM t WHERE id={id}")).await?,
                "ten_updates_one_commit" | "ten_updates_ten_commits" => {
                    // Each statement commits, unless in a transaction.
                    if op == "ten_updates_one_commit" {
                        db.begin().await?;
                    }
                    for n in 1..=10 {
                        db.execute(&format!("UPDATE t SET note={n} WHERE id={id}"))
                            .await?;
                    }
                    if op == "ten_updates_one_commit" {
                        db.commit([]).await?;
                    }
                }
                _ => unreachable!(),
            }
            db.save().await?;
            let elapsed = start.elapsed();
            let stats = store.stats();
            print_row(
                case,
                op,
                repeat,
                (db_bytes, before, store.stored_bytes()),
                elapsed,
                &stats,
            );
            // Validate results after timing/count capture, with simulated loss of
            // all unacknowledged state and a fresh engine.
            store.crash();
            let reopened = open(store.clone()).await?;
            let count = reopened.query("SELECT count(*) FROM t").await?[0][0].to_string();
            let expected_count =
                case.rows + usize::from(op == "insert") - usize::from(op == "delete");
            assert_eq!(count, expected_count.to_string());
            if op == "update_note" || op.starts_with("ten_updates") {
                let n = reopened
                    .query(&format!("SELECT note FROM t WHERE id={id}"))
                    .await?[0][0]
                    .to_string();
                assert_eq!(n, if op == "update_note" { "1" } else { "10" });
            }
            if op == "update_a" {
                assert_eq!(
                    reopened
                        .query(&format!("SELECT a FROM t WHERE id={id}"))
                        .await?[0][0]
                        .to_string(),
                    (id + 1000000).to_string()
                );
            }
            assert_eq!(
                reopened.query("PRAGMA integrity_check").await?[0][0].to_string(),
                "ok"
            );
        }
    }
    Ok(())
}
fn main() -> Result<()> {
    let repeats = std::env::args()
        .filter(|arg| arg != "--bench")
        .nth(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(3);
    if repeats == 0 {
        return Err("repeats must be positive".into());
    }
    println!(
        "rows,payload_bytes,indexes,operation,run,db_bytes,stored_before,stored_after,elapsed_us,gets,puts,deletes,bytes_read,bytes_written,tail_bytes,head_bytes"
    );
    futures::executor::block_on(async {
        for rows in [10, 100, 300] {
            for payload in [64, 1024] {
                for indexes in [0, 1, 3] {
                    run(
                        Case {
                            rows,
                            payload,
                            indexes,
                        },
                        repeats,
                    )
                    .await?;
                }
            }
        }
        run(
            Case {
                rows: 10000,
                payload: 256,
                indexes: 1,
            },
            repeats,
        )
        .await?;
        Ok(())
    })
}
