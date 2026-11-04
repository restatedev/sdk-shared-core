//! Phase-separated logical traffic benchmark. No network/replication model.
//! A write runs in a transaction, measured as its SQL, then its commit (the
//! engine's checkpoint and the flush), apart.
use restate_sdk_shared_core::sql::Database;
use restate_sdk_shared_core::state::simulation::{
    object_kind, Effect, Failpoint, FaultKv, MemoryKv, Operation, Stats, Traffic,
};
use restate_sdk_shared_core::state::{KvStore, Result};
use std::sync::Arc;

/// A session of the database in `store`.
async fn open(store: Arc<impl KvStore + 'static>) -> Result<Database> {
    Database::open(store).await
}

#[derive(Clone, Copy)]
struct Case {
    rows: usize,
    payload: usize,
}
struct Sample {
    case: Case,

    operation: &'static str,
    cache: &'static str,
    run: usize,
    logical_read: u64,
    logical_write: u64,
    logical_delete: u64,
    db_pages: usize,
}
impl Sample {
    fn emit(&self, phase: &str, store: &MemoryKv, local_db: u64, local_wal: u64) {
        let s = store.stats();
        for kind in ["all", "head", "tail", "pack", "other"] {
            let t = if kind == "all" {
                sum(&s)
            } else {
                s.by_kind.get(kind).cloned().unwrap_or_default()
            };
            let unique = |keys: &std::collections::BTreeSet<String>| {
                keys.iter()
                    .filter(|k| kind == "all" || object_kind(k) == kind)
                    .count()
            };
            println!(
                "{},{},{},{},{},{phase},{kind},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                self.case.rows,
                self.case.payload,
                self.operation,
                self.cache,
                self.run,
                t.gets,
                t.misses,
                unique(&s.read_keys),
                t.puts,
                unique(&s.write_keys),
                t.deletes,
                unique(&s.delete_keys),
                t.bytes_read,
                t.bytes_written,
                t.bytes_deleted,
                t.get_key_bytes,
                t.put_key_bytes,
                t.delete_key_bytes,
                self.logical_read,
                self.logical_write,
                self.logical_delete,
                self.db_pages,
                store.stored_bytes(),
                store.stored_key_bytes(),
                local_db,
                local_wal
            );
        }
        store.reset_stats();
    }
}
fn sum(s: &Stats) -> Traffic {
    let mut out = Traffic::default();
    for t in s.by_kind.values() {
        out.gets += t.gets;
        out.misses += t.misses;
        out.puts += t.puts;
        out.deletes += t.deletes;
        out.bytes_read += t.bytes_read;
        out.bytes_written += t.bytes_written;
        out.bytes_deleted += t.bytes_deleted;
        out.get_key_bytes += t.get_key_bytes;
        out.put_key_bytes += t.put_key_bytes;
        out.delete_key_bytes += t.delete_key_bytes;
    }
    assert_eq!(
        (
            out.gets,
            out.puts,
            out.deletes,
            out.bytes_read,
            out.bytes_written
        ),
        (s.gets, s.puts, s.deletes, s.bytes_read, s.bytes_written)
    );
    out
}
async fn baseline(case: Case) -> Result<MemoryKv> {
    let store = MemoryKv::default();
    let db = open(Arc::new(store.clone())).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)")
        .await?;
    db.execute("CREATE INDEX by_bucket ON t(bucket)").await?;
    db.execute(&format!("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<{}) INSERT INTO t SELECT x, x%10, printf('%0{}d',x) FROM n", case.rows, case.payload)).await?;
    db.save().await?;
    store.crash();
    Ok(store)
}

/// The database's pages in `base`.
async fn db_pages(base: &MemoryKv) -> Result<usize> {
    let db = open(Arc::new(base.fork())).await?;
    Ok(db.query("PRAGMA page_count").await?[0][0]
        .as_int()
        .unwrap_or(0) as usize)
}

fn read_query(op: &str, case: Case) -> String {
    let id = case.rows / 2;
    match op {
        "pk_hit" => format!("SELECT payload FROM t WHERE id={id}"),
        "pk_miss" => format!("SELECT payload FROM t WHERE id={}", case.rows + 1),
        "range_10" => format!(
            "SELECT payload FROM t WHERE id BETWEEN {id} AND {} ORDER BY id",
            id + 9
        ),
        "covering_index" => {
            "SELECT id FROM t INDEXED BY by_bucket WHERE bucket=3 ORDER BY id".into()
        }
        "secondary_rows" => {
            "SELECT payload FROM t INDEXED BY by_bucket WHERE bucket=3 ORDER BY id".into()
        }
        "unindexed_filter" => format!(
            "SELECT payload FROM t NOT INDEXED WHERE payload='{:0width$}'",
            id,
            width = case.payload
        ),
        "full_scan" => "SELECT payload FROM t NOT INDEXED ORDER BY id".into(),
        _ => unreachable!(),
    }
}
async fn reads(base: &MemoryKv, case: Case, run: usize) -> Result<()> {
    for op in [
        "pk_hit",
        "pk_miss",
        "range_10",
        "covering_index",
        "secondary_rows",
        "unindexed_filter",
        "full_scan",
    ] {
        let store = Arc::new(base.fork());
        let mut sample = Sample {
            case,
            operation: op,
            cache: "cold",
            run,
            logical_read: 0,
            logical_write: 0,
            logical_delete: 0,
            db_pages: db_pages(base).await?,
        };
        let db = open(store.clone()).await?;
        sample.emit("open", &store, 0, 0);
        for cache in ["cold", "warm"] {
            sample.cache = cache;
            let rows = db.query(&read_query(op, case)).await?;
            let expected: Vec<usize> = match op {
                "pk_hit" | "unindexed_filter" => vec![case.rows / 2],
                "pk_miss" => vec![],
                "range_10" => (case.rows / 2..=(case.rows / 2 + 9).min(case.rows)).collect(),
                "covering_index" | "secondary_rows" => {
                    (1..=case.rows).filter(|n| n % 10 == 3).collect()
                }
                "full_scan" => (1..=case.rows).collect(),
                _ => unreachable!(),
            };
            assert_eq!(rows.len(), expected.len(), "{op}");
            if op == "covering_index" {
                let ids: Vec<_> = rows
                    .iter()
                    .map(|r| r[0].as_int().unwrap() as usize)
                    .collect();
                assert_eq!(ids, expected);
                sample.logical_read = (rows.len() * 8) as u64;
            } else {
                for (row, id) in rows.iter().zip(expected) {
                    assert_eq!(
                        row[0].to_string(),
                        format!("{id:0width$}", width = case.payload)
                    );
                }
                sample.logical_read = (rows.len() * case.payload) as u64;
            }
            sample.emit("query", &store, 0, 0);
        }
    }
    Ok(())
}

async fn writes(base: &MemoryKv, case: Case, run: usize) -> Result<()> {
    for op in [
        "read_only_save",
        "rollback",
        "replace_payload",
        "update_index",
        "insert",
        "delete",
        "ten_adjacent",
        "ten_scattered",
    ] {
        let store = Arc::new(base.fork());
        let mut sample = Sample {
            case,
            operation: op,
            cache: "cold",
            run,
            logical_read: 0,
            logical_write: 0,
            logical_delete: 0,
            db_pages: db_pages(base).await?,
        };
        let db = open(store.clone()).await?;
        sample.emit("open", &store, 0, 0);
        let id = case.rows / 2;
        let ids: Vec<_> = if op == "ten_scattered" {
            (0..10).map(|n| 1 + n * (case.rows - 1) / 9).collect()
        } else if op == "ten_adjacent" {
            (1..=10.min(case.rows)).collect()
        } else {
            vec![id]
        };
        let before = db.io_stats();
        db.begin().await?;
        match op {
            "read_only_save" => {
                db.query("SELECT count(*) FROM t").await?;
            }
            "rollback" => {
                db.execute(&format!(
                    "UPDATE t SET payload=printf('%0{}d',0) WHERE id={id}",
                    case.payload
                ))
                .await?;
                db.rollback().await?;
            }
            "replace_payload" | "ten_adjacent" | "ten_scattered" => {
                for id in &ids {
                    db.execute(&format!(
                        "UPDATE t SET payload=printf('%0{}d',0) WHERE id={id}",
                        case.payload
                    ))
                    .await?;
                }
                sample.logical_write = (ids.len() * case.payload) as u64;
            }
            "update_index" => {
                db.execute(&format!("UPDATE t SET bucket=42 WHERE id={id}"))
                    .await?;
                sample.logical_write = 8;
            }
            "insert" => {
                db.execute(&format!(
                    "INSERT INTO t VALUES({},42,printf('%0{}d',0))",
                    case.rows + 1,
                    case.payload
                ))
                .await?;
                sample.logical_write = (16 + case.payload) as u64;
            }
            "delete" => {
                db.execute(&format!("DELETE FROM t WHERE id={id}")).await?;
                sample.logical_delete = (16 + case.payload) as u64;
            }
            _ => unreachable!(),
        }
        let after = db.io_stats();
        sample.emit(
            "sql",
            &store,
            after.database_bytes_written - before.database_bytes_written,
            after.private_wal_bytes_written - before.private_wal_bytes_written,
        );
        db.commit([]).await?;
        let committed = db.io_stats();
        sample.db_pages = (committed.database_len / 4096) as usize;
        sample.emit(
            "commit",
            &store,
            committed.database_bytes_written - after.database_bytes_written,
            committed.private_wal_bytes_written - after.private_wal_bytes_written,
        );
        db.save().await?;
        store.crash();
        let db = open(store.clone()).await?;
        let count = db.query("SELECT count(*) FROM t").await?[0][0]
            .as_int()
            .unwrap() as usize;
        assert_eq!(
            count,
            case.rows + usize::from(op == "insert") - usize::from(op == "delete")
        );
        if matches!(op, "replace_payload" | "ten_adjacent" | "ten_scattered") {
            for id in ids {
                assert_eq!(
                    db.query(&format!("SELECT payload FROM t WHERE id={id}"))
                        .await?[0][0]
                        .to_string(),
                    "0".repeat(case.payload)
                );
            }
        }
        if op == "rollback" {
            assert_eq!(
                db.query(&format!("SELECT payload FROM t WHERE id={id}"))
                    .await?[0][0]
                    .to_string(),
                format!("{id:0width$}", width = case.payload)
            );
        }
        if op == "update_index" {
            assert_eq!(
                db.query("SELECT id FROM t WHERE bucket=42").await?[0][0].as_int(),
                Some(id as i64)
            );
        }
        assert_eq!(
            db.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
        if matches!(op, "read_only_save" | "rollback") {
            assert_eq!(store.keys(), base.keys());
            assert_eq!(store.stored_bytes(), base.stored_bytes());
        }
    }
    Ok(())
}

/// Crash a commit, then measure the open that follows. A commit puts its
/// tail entry, then its head, in one sync.
async fn recovery(base: &MemoryKv, case: Case, run: usize) -> Result<()> {
    // (operation, effect of the failed sync, writes lost, committed)
    for (op, effect, lost, committed) in [
        // Every write of the commit.
        ("commit_lost", Effect::None, usize::MAX, false),
        // The head: the tail entry is stored, but nothing names it.
        ("head_lost", Effect::None, 1, false),
        // Nothing: stored, then the sync failed.
        ("sync_ack_lost", Effect::After, 0, true),
    ] {
        let store = Arc::new(FaultKv::new(base.fork(), 7));
        let db = open(store.clone()).await?;
        db.begin().await?;
        db.execute(&format!(
            "UPDATE t SET payload=printf('%0{}d',0) WHERE id={}",
            case.payload,
            case.rows / 2
        ))
        .await?;
        let _gate = store.arm(Failpoint {
            operation: Operation::Sync,
            key_prefix: String::new(),
            nth: 1,
            effect,
            cancel: false,
        });
        assert!(db.commit([]).await.is_err());
        assert!(
            store.events().iter().any(|e| e.faulted),
            "the sync failpoint was not reached"
        );
        store.disarm();
        let writes = store.inner.unsynced().len();
        store.inner.crash_keeping(writes.saturating_sub(lost));
        store.inner.reset_stats();
        let sample = Sample {
            case,
            operation: op,
            cache: "cold",
            run,
            logical_read: 0,
            logical_write: 0,
            logical_delete: 0,
            db_pages: db_pages(base).await?,
        };
        let db = open(store.clone()).await?;
        sample.emit("recovery_open", &store.inner, 0, 0);
        let value = db
            .query(&format!("SELECT payload FROM t WHERE id={}", case.rows / 2))
            .await?[0][0]
            .to_string();
        assert_eq!(
            value,
            if committed {
                "0".repeat(case.payload)
            } else {
                format!("{:0width$}", case.rows / 2, width = case.payload)
            }
        );
        assert_eq!(
            db.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
    }
    Ok(())
}
fn main() -> Result<()> {
    let repeats: usize = std::env::args()
        .filter(|arg| arg != "--bench")
        .nth(1)
        .map(|n| n.parse())
        .transpose()?
        .unwrap_or(3);
    if repeats == 0 {
        return Err("repeats must be positive".into());
    }
    println!(
        "rows,payload_bytes,operation,cache,run,phase,kind,gets,misses,unique_read_keys,puts,unique_write_keys,deletes,unique_delete_keys,read_value_bytes,write_value_bytes,reclaimed_value_bytes,get_key_bytes,put_key_bytes,delete_key_bytes,logical_read_bytes,logical_write_bytes,logical_delete_bytes,db_pages,stored_value_bytes,stored_key_bytes,local_db_write_bytes,local_wal_write_bytes"
    );
    futures::executor::block_on(async {
        for case in [
            Case {
                rows: 100,
                payload: 64,
            },
            Case {
                rows: 300,
                payload: 1024,
            },
            Case {
                rows: 100,
                payload: 9000,
            },
            Case {
                rows: 10000,
                payload: 256,
            },
        ] {
            let base = baseline(case).await?;
            for run in 0..repeats {
                reads(&base, case, run).await?;
                writes(&base, case, run).await?;
                recovery(&base, case, run).await?;
            }
        }
        Ok(())
    })
}
