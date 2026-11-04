//! A reference engine on plain memory, outside this crate's storage, and the
//! state of a database as text, to compare a session's with it.
use crate::sql::{Rows, Value};
use crate::state::Result;
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use turso_core::{Connection, MemoryIO, SqliteDialect, StepResult, IO};

/// A new, empty reference engine.
pub(super) fn oracle() -> Arc<Connection> {
    // The engine keeps a process-wide registry of databases by path.
    static DATABASES: AtomicU64 = AtomicU64::new(0);
    let path = format!("oracle-{}.db", DATABASES.fetch_add(1, Ordering::Relaxed));
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    turso_core::Database::open_file(io, &path, Arc::new(SqliteDialect))
        .unwrap()
        .connect()
        .unwrap()
}

/// Run `sql` (one or more statements) on the reference engine, as
/// [`crate::sql::Database::query`] does: the rows of its last statement, or the first error,
/// after which no statement runs.
pub(super) fn run(connection: &Arc<Connection>, sql: &str) -> std::result::Result<Rows, String> {
    let mut remaining = sql;
    let mut rows = Vec::new();
    loop {
        let (mut statement, end) = match connection.consume_stmt(remaining) {
            Ok(Some(prepared)) => prepared,
            Ok(None) => return Ok(rows),
            Err(error) => return Err(error.to_string()),
        };
        remaining = &remaining[end..];
        rows.clear();
        loop {
            match statement.step().map_err(|error| error.to_string())? {
                StepResult::Row => {
                    rows.push(statement.row().unwrap().get_values().cloned().collect())
                }
                StepResult::Done => break,
                StepResult::IO => statement._io().step().map_err(|error| error.to_string())?,
                StepResult::Yield => {}
                other => return Err(format!("{other:?}")),
            }
        }
    }
}

/// A reference engine that ran `statements` in order, errors and all.
pub(super) fn replay(statements: &[String]) -> Arc<Connection> {
    let connection = oracle();
    for sql in statements {
        let _ = run(&connection, sql);
    }
    connection
}

/// Rows as text, to compare and print: a long value as its start, length
/// and hash.
pub(super) fn text(rows: &Rows) -> Vec<String> {
    let value = |value: &Value| {
        let text = format!("{value:?}");
        if text.len() <= 60 {
            return text;
        }
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let start: String = text.chars().take(40).collect();
        format!("{start}...({} chars, #{:x})", text.len(), hasher.finish())
    };
    (rows.iter())
        .map(|row| row.iter().map(value).collect::<Vec<_>>().join(", "))
        .collect()
}

const SCHEMA: &str = "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name";

/// Each table's rows, in rowid order.
fn tables(schema: &Rows) -> Vec<String> {
    (schema.iter())
        .filter(|row| row[0].to_string() == "table")
        .map(|row| row[1].to_string())
        .filter(|name| !name.starts_with("sqlite_"))
        .map(|name| format!("SELECT * FROM \"{name}\" ORDER BY rowid"))
        .collect()
}

/// The schema and every table's rows of the database `query` runs SQL on,
/// as text.
pub(super) async fn dump_of(mut query: impl AsyncFnMut(&str) -> Result<Rows>) -> Result<String> {
    let schema = query(SCHEMA).await?;
    let mut state = text(&schema);
    for table in tables(&schema) {
        let rows = query(&table).await?;
        state.push(table);
        state.extend(text(&rows));
    }
    Ok(state.join("\n"))
}

/// [`dump_of`] a session's database.
pub(super) async fn dump(db: &crate::sql::Database) -> Result<String> {
    dump_of(async |query| db.query(query).await).await
}

/// [`dump`] of the reference engine.
pub(super) fn reference_dump(connection: &Arc<Connection>) -> String {
    let schema = run(connection, SCHEMA).unwrap();
    let mut state = text(&schema);
    for query in tables(&schema) {
        state.push(query.clone());
        state.extend(text(&run(connection, &query).unwrap()));
    }
    state.join("\n")
}

/// `PRAGMA integrity_check` of a session's database: "ok" when it is.
pub(super) async fn integrity(db: &crate::sql::Database) -> Result<String> {
    Ok(db.query("PRAGMA integrity_check").await?[0][0].to_string())
}
