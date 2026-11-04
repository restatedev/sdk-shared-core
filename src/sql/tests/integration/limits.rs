//! Memory limits: private changes and result rows.
use super::host::Db;
use crate::sql::Limits;
use crate::state::{simulation::MemoryKv, Result, StateOptions};
use std::sync::Arc;

#[test]
fn private_changes_are_bounded() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let options = StateOptions::default;
        let limits = || Limits {
            max_private_bytes: 1 << 20,
            ..Limits::default()
        };
        let db = Db::open_with(store.clone(), options(), limits()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB)")
            .await?;
        db.execute("INSERT INTO t VALUES(1, randomblob(100000))")
            .await?;
        db.save().await?;
        let db = Db::open_with(store.clone(), options(), limits()).await?;
        let error = db
            .execute("INSERT INTO t SELECT value, randomblob(100000) FROM (WITH RECURSIVE n(value) AS (VALUES(2) UNION ALL SELECT value + 1 FROM n WHERE value < 40) SELECT value FROM n)")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("max_private_bytes"), "{error}");
        // A retry would go over it again: refused, not a storage error.
        assert!(error.is::<crate::sql::Refused>(), "{error:?}");
        assert!(db.execute("SELECT 1").await.is_err(), "poisoned");
        drop(db);
        let db = Db::open_with(store, options(), limits()).await?;
        assert_eq!(
            db.query("SELECT count(*) FROM t").await?[0][0].as_int(),
            Some(1)
        );
        Ok(())
    })
}

#[test]
fn result_rows_are_bounded() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let limits = Limits {
            max_result_bytes: 64 << 10,
            ..Limits::default()
        };
        let db = Db::open_with(store, StateOptions::default(), limits).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB)")
            .await?;
        for i in 0..20 {
            db.execute(&format!("INSERT INTO t VALUES({i}, zeroblob(10000))"))
                .await?;
        }
        assert_eq!(db.query("SELECT id FROM t").await?.len(), 20);
        let error = db.query("SELECT body FROM t").await.unwrap_err();
        assert!(error.to_string().contains("max_result_bytes"), "{error}");
        Ok(())
    })
}

/// The limit is on private changes, not on moving committed ones into a
/// mutation: a flush, which copies the engine WAL into the database's pages, passes.
#[test]
fn a_transaction_within_the_limit_flushes() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let limits = Limits {
            max_private_bytes: 1 << 20,
            ..Limits::default()
        };
        let db = Db::open_with(store.clone(), StateOptions::default(), limits).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB)")
            .await?;
        // About 600 KiB: over half the limit, which the mutation and the
        // checkpoint's pages together would exceed.
        db.execute("INSERT INTO t SELECT value, randomblob(100000) FROM (WITH RECURSIVE n(value) AS (VALUES(1) UNION ALL SELECT value + 1 FROM n WHERE value < 6) SELECT value FROM n)")
            .await?;
        db.save().await?;
        let db = Db::open_with(store, StateOptions::default(), Limits::default()).await?;
        assert_eq!(
            db.query("SELECT count(*) FROM t").await?[0][0].as_int(),
            Some(6)
        );
        Ok(())
    })
}

/// A database is capped at the state's largest file: a statement that would
/// grow it past the cap fails with SQLITE_FULL, and the session goes on. A
/// lower cap stands in for 10 GiB here.
#[test]
fn a_statement_past_the_largest_file_fails_alone() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let largest = Some((crate::state::MAX_FILE_BYTES / 4096) as i64);
        let db = Db::open(store.clone(), StateOptions::default()).await?;
        let pages = db.query("PRAGMA max_page_count").await?;
        assert_eq!(pages[0][0].as_int(), largest);
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB)")
            .await?;
        db.execute("PRAGMA max_page_count = 10").await?;
        let error = db
            .execute("INSERT INTO t VALUES(1, zeroblob(100000))")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("full"), "{error}");
        db.execute("INSERT INTO t VALUES(2, zeroblob(1000))")
            .await?;
        db.save().await?;
        let db = Db::open(store, StateOptions::default()).await?;
        let pages = db.query("PRAGMA max_page_count").await?;
        assert_eq!(pages[0][0].as_int(), largest, "a session's own cap");
        assert_eq!(db.query("SELECT id FROM t").await?.len(), 1);
        Ok(())
    })
}
