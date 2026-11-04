use super::host::Db;
use crate::state::{simulation::MemoryKv, Result, StateOptions};
use std::sync::Arc;

/// Every commit is durable when its call returns: one tail entry and the
/// head. The engine's WAL never reaches the store.
#[test]
fn every_commit_flushes_and_reopens_database_only() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
            .await?;
        assert_eq!(
            store.stats().puts,
            2,
            "a commit is a tail entry and the head"
        );
        db.execute("INSERT INTO items VALUES (1, 'one'), (2, 'two')")
            .await?;
        assert!(db.io_stats().private_wal_bytes_written > 0);
        assert_eq!(store.stats().puts, 4);
        db.save().await?;
        assert_eq!(store.stats().puts, 4, "a save flushes nothing more");
        store.crash(); // every acknowledged put survives

        let reopened = Db::open(store.clone(), StateOptions::default()).await?;
        assert_eq!(reopened.io_stats().private_wal_len, 0);
        let rows = reopened
            .query("SELECT value FROM items ORDER BY id")
            .await?;
        assert_eq!(
            rows.iter().map(|r| r[0].to_string()).collect::<Vec<_>>(),
            ["one", "two"]
        );
        assert_eq!(
            reopened.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
        Ok(())
    })
}

/// A session dropped in a transaction leaves the flushed state; a rolled back
/// transaction changes nothing.
#[test]
fn a_dropped_transaction_and_a_rollback_leave_the_flushed_state() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        db.execute("INSERT INTO t VALUES(1, 10)").await?;
        db.save().await?;

        let discarded = Db::open(store.clone(), StateOptions::default()).await?;
        discarded.execute("BEGIN; UPDATE t SET n=99").await?;
        drop(discarded);
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        assert_eq!(db.query("SELECT n FROM t").await?[0][0].to_string(), "10");
        db.execute("BEGIN").await?;
        db.execute("UPDATE t SET n=50").await?;
        db.execute("ROLLBACK").await?;
        db.save().await?;
        let db = Db::open(store, StateOptions::default()).await?;
        assert_eq!(db.query("SELECT n FROM t").await?[0][0].to_string(), "10");
        Ok(())
    })
}

/// A transaction left open when the session is saved is lost: `save` says
/// so, and the next session does not see it.
#[test]
fn a_transaction_left_open_is_lost() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        db.execute("BEGIN").await?;
        db.execute("CREATE TABLE t(n INTEGER)").await?;
        assert!(db.save().await.is_err());
        let db = Db::open(store, StateOptions::default()).await?;
        assert!(db.query("SELECT count(*) FROM t").await.is_err());
        Ok(())
    })
}

#[test]
fn cold_indexed_lookup_is_lazy() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT, payload TEXT)")
            .await?;
        db.execute("CREATE INDEX t_label ON t(label)").await?;
        // Incompressible payloads make this a large save, checkpointed in
        // frames of pages.
        db.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4000) INSERT INTO t SELECT x, printf('label-%06d',x), randomblob(400) FROM n").await?;
        db.save_checkpointed().await?;
        let total = store.stored_bytes();
        store.reset_stats();
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        let sql = "SELECT id FROM t WHERE label='label-002341'";
        let rows = db.query(sql).await?;
        assert_eq!(rows[0][0].to_string(), "2341");
        let stats = store.stats();
        let parts = (store.keys().iter())
            .filter(|key| key.starts_with(crate::state::blobs::KV_PREFIX))
            .count();
        assert!(
            stats.by_kind["pack"].gets < parts as u64 / 4,
            "{parts} parts: {stats:?}"
        );
        assert!(stats.bytes_read < total as u64 / 10, "{stats:?}");
        assert!(stats.gets > 0);
        store.reset_stats();
        db.query(sql).await?;
        assert_eq!(
            store.stats().gets,
            0,
            "warm page cache should avoid backend reads"
        );
        Ok(())
    })
}

/// A transaction of many statements, savepoints and all, flushes once at
/// its `COMMIT`: only its final pages.
#[test]
fn a_transaction_flushes_only_its_final_pages() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        db.execute("INSERT INTO t VALUES(1,0)").await?;
        let before = store.stats();
        db.execute("BEGIN").await?;
        for n in 1..=100 {
            db.execute(&format!("UPDATE t SET n={n}")).await?;
        }
        db.execute("SAVEPOINT s").await?;
        db.execute("UPDATE t SET n=999").await?;
        db.execute("ROLLBACK TO s").await?;
        db.execute("RELEASE s").await?;
        assert_eq!(store.stats().puts, before.puts, "nothing flushed in it");
        db.execute("COMMIT").await?;
        // A tail entry of the final pages, then the head.
        let stats = store.stats();
        assert_eq!(stats.puts - before.puts, 2);
        assert!(db.io_stats().database_len.div_ceil(4096) >= 2);
        let written = stats.bytes_written - before.bytes_written;
        assert!(written < 4096, "{written}");
        db.save().await?;
        store.crash();
        let db = Db::open(store, StateOptions::default()).await?;
        assert_eq!(db.query("SELECT n FROM t").await?[0][0].to_string(), "100");
        assert_eq!(
            db.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
        println!("100 updates in a transaction: {written} backend bytes in one flush");
        Ok(())
    })
}

/// Pages larger than a 4096-byte block survive a save and a cold reopen.
#[test]
fn large_page_sizes_reopen_after_a_save() -> Result<()> {
    futures::executor::block_on(async {
        for page_size in [8192, 65536] {
            let store: Arc<dyn crate::state::kv::KvStore> =
                Arc::new(crate::state::simulation::MemoryKv::default());
            let db = Db::open(store.clone(), StateOptions::default()).await?;
            db.execute(&format!("PRAGMA page_size = {page_size}"))
                .await?;
            db.execute("CREATE TABLE t(v TEXT)").await?;
            db.execute("INSERT INTO t VALUES ('kept')").await?;
            db.save().await?;
            let db = Db::open(store.clone(), StateOptions::default()).await?;
            let rows = db.query("SELECT v FROM t").await?;
            assert_eq!(rows[0][0].to_string(), "kept", "page size {page_size}");
            let rows = db.query("PRAGMA page_size").await?;
            assert_eq!(rows[0][0].as_int(), Some(page_size));
        }
        Ok(())
    })
}

/// Parameters bind values without SQL text: quotes need no escaping, and a
/// mismatched count or a second statement is refused before anything runs.
#[test]
fn parameters_bind_values_not_text() -> Result<()> {
    use crate::sql::Value;
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store, StateOptions::default()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)")
            .await?;
        let hostile = "x'); DROP TABLE t; --";
        db.execute_with(
            "INSERT INTO t VALUES(?, ?)",
            [Value::from_i64(1), Value::from_text(hostile.to_string())],
        )
        .await?;
        let rows = db
            .query_with("SELECT name FROM t WHERE id = ?1", [Value::from_i64(1)])
            .await?;
        assert_eq!(rows[0][0].to_string(), hostile);
        assert!(db.execute_with("SELECT ?", []).await.is_err());
        assert!(db
            .execute_with("SELECT ?; SELECT 1", [Value::from_i64(1)])
            .await
            .is_err());
        // A syntax error in the text after it is an SQL error too.
        assert!(db
            .execute_with("SELECT ?; SELEC", [Value::from_i64(1)])
            .await
            .is_err());
        assert!(db.is_usable());
        assert_eq!(
            db.query("SELECT count(*) FROM t").await?[0][0].as_int(),
            Some(1)
        );
        Ok(())
    })
}

#[test]
fn query_columns_names_the_last_statement_columns() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store, StateOptions::default()).await?;
        db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")
            .await?;
        let (columns, rows) = db
            .query_columns(
                "INSERT INTO items VALUES (1, 'one'); SELECT id, value AS v, id * 2 FROM items",
                None,
            )
            .await?;
        assert_eq!(columns, ["id", "v", "id * 2"]);
        assert_eq!(rows.len(), 1);
        let (columns, rows) = db
            .query_columns(
                "SELECT value FROM items WHERE id = ?",
                Some(vec![crate::sql::Value::from_i64(1)]),
            )
            .await?;
        assert_eq!(columns, ["value"]);
        assert_eq!(rows[0][0].to_string(), "one");
        Ok(())
    })
}

/// The engine refuses `auto_vacuum`, so no page is a pointer map, whose
/// first entry's type may read as a b-tree page's: were it allowed, the
/// SQL crate leaves such a database's pages as the engine wrote them
/// (`io::tidied`), and this test should check that they survive a reopen.
#[test]
fn auto_vacuum_is_refused() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store, StateOptions::default()).await?;
        let refused = db.execute("PRAGMA auto_vacuum = FULL").await;
        assert!(refused.is_err(), "auto_vacuum is allowed: {refused:?}");
        Ok(())
    })
}

/// A statement run again is prepared once and kept, and reads the schema as
/// it is now: a column added since shows, named.
#[test]
fn a_statement_run_again_reads_the_schema_as_it_is_now() -> Result<()> {
    use crate::sql::Value;
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store, StateOptions::default()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").await?;
        db.execute("INSERT INTO t VALUES (1)").await?;
        let select = "SELECT * FROM t WHERE id = ?";
        let (columns, _) = db
            .query_columns(select, Some(vec![Value::from_i64(1)]))
            .await?;
        assert_eq!(columns, ["id"]);
        db.execute("ALTER TABLE t ADD COLUMN name TEXT DEFAULT 'one'")
            .await?;
        let (columns, rows) = db
            .query_columns(select, Some(vec![Value::from_i64(1)]))
            .await?;
        assert_eq!(columns, ["id", "name"]);
        assert_eq!(rows[0][1].to_string(), "one");
        Ok(())
    })
}

/// Statements kept for their next run hold no read: commits after them
/// checkpoint the engine's WAL fully, and the database reopens with them.
#[test]
fn statements_kept_for_their_next_run_hold_no_read() -> Result<()> {
    use crate::sql::Value;
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        for i in 0..20 {
            db.execute_with("INSERT INTO t VALUES (?, 0)", [Value::from_i64(i)])
                .await?;
            db.query_with("SELECT n FROM t WHERE id = ?", [Value::from_i64(i)])
                .await?;
            db.query("SELECT count(*) FROM t").await?;
        }
        db.save().await?;
        let db = Db::open(store, StateOptions::default()).await?;
        assert_eq!(
            db.query("SELECT count(*) FROM t").await?[0][0].as_int(),
            Some(20)
        );
        Ok(())
    })
}

/// A statement run again that fails is an SQL error, as the first run's:
/// the session goes on, and the statement runs again after it. Values of
/// the wrong count are refused before it runs.
#[test]
fn a_statement_run_again_fails_as_its_first_run() -> Result<()> {
    use crate::sql::Value;
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store, StateOptions::default()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").await?;
        let insert = "INSERT INTO t VALUES (?)";
        db.execute_with(insert, [Value::from_i64(1)]).await?;
        assert!(db.execute_with(insert, [Value::from_i64(1)]).await.is_err());
        assert!(db.execute_with(insert, []).await.is_err());
        assert!(db.is_usable());
        db.execute_with(insert, [Value::from_i64(2)]).await?;
        assert_eq!(
            db.query("SELECT count(*) FROM t").await?[0][0].as_int(),
            Some(2)
        );
        Ok(())
    })
}
