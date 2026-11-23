//! What a session keeps between its calls: the statements it prepared, with
//! what their texts mention and their column names, and the commit's
//! checkpoint statement.
use crate::sql::{Columns, Database, Engine};
use crate::state::{simulation::MemoryKv, Result, State};
use std::sync::Arc;

async fn open() -> Result<Database> {
    Database::new(State::open(Arc::new(MemoryKv::default())).await?).await
}

/// `f` on the session's engine.
async fn engine<T>(db: &Database, f: impl FnOnce(&mut Engine) -> T) -> T {
    let mut session = db.shared.lock().await;
    f(session.engine().expect("an open session"))
}

async fn columns(db: &Database, sql: &str) -> Result<Columns> {
    Ok(db.query_columns_owned(sql.into(), None).await?.0)
}

/// A statement kept for its next call applies what its text mentions without
/// the text scanned again: a `CREATE` of a kept text, run in a transaction,
/// is a schema change that a rollback must drop the kept statements for.
#[test]
fn a_kept_statement_carries_what_its_text_mentions() -> Result<()> {
    futures::executor::block_on(async {
        let db = open().await?;
        db.execute("CREATE TABLE IF NOT EXISTS a(x)").await?; // prepared, kept
        db.execute("SELECT 1").await?;
        assert_eq!(engine(&db, |e| e.statements.len()).await, 2);
        db.begin().await?;
        db.execute("CREATE TABLE IF NOT EXISTS a(x)").await?; // from the kept one
        assert!(engine(&db, |e| e.schema_changed_in_transaction).await);
        db.rollback().await?;
        assert!(engine(&db, |e| e.statements.is_empty()).await);
        assert!(engine(&db, |e| !e.schema_changed_in_transaction).await);
        Ok(())
    })
}

/// A session prepares the commit's checkpoint once, whatever it commits
/// after, and drops it with its statements when a transaction that changed
/// the schema ends.
#[test]
fn the_checkpoint_statement_is_prepared_once_per_session() -> Result<()> {
    futures::executor::block_on(async {
        let db = open().await?;
        db.execute("CREATE TABLE t(x)").await?; // its commit prepares the checkpoint
        assert!(engine(&db, |e| e.checkpoint_statement.is_some()).await);
        let compiled = engine(&db, |e| e.compiled).await;
        db.execute("INSERT INTO t VALUES (1)").await?; // prepared, with its commit
        assert_eq!(engine(&db, |e| e.compiled).await, compiled + 1);
        db.execute("INSERT INTO t VALUES (1)").await?; // both kept
        db.execute("INSERT INTO t VALUES (1)").await?;
        assert_eq!(engine(&db, |e| e.compiled).await, compiled + 1);
        assert_eq!(
            db.query("SELECT count(*) FROM t").await?[0][0].as_int(),
            Some(3)
        );

        db.begin().await?;
        db.execute("ALTER TABLE t ADD COLUMN y").await?;
        db.rollback().await?;
        assert!(engine(&db, |e| e.checkpoint_statement.is_none()).await);
        db.execute("INSERT INTO t VALUES (2)").await?; // commits, preparing it again
        assert!(engine(&db, |e| e.checkpoint_statement.is_some()).await);
        assert_eq!(
            db.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
        Ok(())
    })
}

/// Results of one statement share its column names, until a schema change
/// renames them.
#[test]
fn a_statements_runs_share_its_column_names() -> Result<()> {
    futures::executor::block_on(async {
        let db = open().await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
            .await?;
        let select = "SELECT * FROM t";
        let first = columns(&db, select).await?;
        assert_eq!(first.to_vec(), ["id", "x"]);
        assert!(Arc::ptr_eq(&first, &columns(&db, select).await?));
        // The same statement, kept, prepared again by the engine.
        db.execute("ALTER TABLE t RENAME COLUMN x TO y").await?;
        let renamed = columns(&db, select).await?;
        assert_eq!(renamed.to_vec(), ["id", "y"]);
        assert!(!Arc::ptr_eq(&first, &renamed));
        assert!(Arc::ptr_eq(&renamed, &columns(&db, select).await?));
        assert_eq!(
            first.to_vec(),
            ["id", "x"],
            "an earlier result keeps its own"
        );
        Ok(())
    })
}

/// A call that leaves nothing to make durable flushes nothing: the state is
/// the one it was, though a call that changes a row, or sets a record, flushes.
#[test]
fn a_call_that_changes_nothing_flushes_nothing() -> Result<()> {
    futures::executor::block_on(async {
        let db = open().await?;
        db.execute("CREATE TABLE t(x)").await?;
        db.execute("INSERT INTO t VALUES (1)").await?;
        let before = db.state().snapshot();
        db.query("SELECT * FROM t").await?;
        db.begin().await?;
        db.query("SELECT * FROM t").await?;
        db.rollback().await?;
        db.begin().await?;
        db.commit([]).await?;
        assert!(Arc::ptr_eq(&before, &db.state().snapshot()));

        db.execute("INSERT INTO t VALUES (2)").await?;
        let before = db.state().snapshot();
        db.begin().await?;
        db.commit([("outbox".into(), "x".into())]).await?;
        assert!(
            !Arc::ptr_eq(&before, &db.state().snapshot()),
            "a record is a change"
        );
        assert_eq!(db.record("outbox").await?, "x");
        Ok(())
    })
}
