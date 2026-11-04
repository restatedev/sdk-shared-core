//! An object's database: SQLite-compatible SQL, Turso's Rust engine
//! (`turso_core`, unmodified), over the [`State`]'s file, committed with
//! the host's records in the state's flush.
//!
//! For a Restate object without Restate's journal (`journalReplay: false`),
//! whose state is the namespace. A [`Database`] is one session of it: its
//! statements change a private view of the file, and each call that leaves
//! no SQL transaction open commits: it moves what the SQL committed into a
//! [`Mutation`](crate::state::Mutation), with the records of
//! [`Database::commit`], and flushes it, durable before the call returns.
//!
//! | Layer | Representation | Survives? |
//! |---|---|---|
//! | SQL working state | Dirty 4096-byte pages, the private engine WAL, records committed | No |
//! | A commit | The pages and records in one mutation, flushing | No: one that fails ends the session |
//! | The state | Its head, tail and packs (see [`state`](crate::state)) | Yes, after an acknowledged flush |
//!
//! An SQL error (syntax, a constraint, a missing table) undoes just that
//! statement, as in SQLite, and the session goes on. A storage error, a
//! cancelled call or a failed flush ends the session ([`Database::is_usable`]
//! returns false): settle outstanding requests and open a new one.
//!
//! # Engine reuse
//!
//! With a [`PageCache`], a session that ends cleanly leaves its idle engine
//! there ([`State::park`]); the next session of the namespace reuses it when
//! nothing changed, keeping the parsed schema and the engine's page cache.
//!
//! # Ownership
//!
//! A session belongs to one invocation of the object's handler, which opens
//! it, calls it and saves or closes it before it ends; only what the
//! [`PageCache`] keeps (pages, the owner's last flush, a parked engine)
//! outlives it. Its calls take `&self` and run one at a time: each holds
//! the session's lock for the whole call, in the order the host issues them
//! (the host also keeps other calls out of a transaction's body, whose own
//! calls may overlap). Under that lock nothing is contended:
//!
//! - The engine, its adapter (`io`) and the state are used only by the call
//!   holding the session, on its task. No thread, channel or background task
//!   is created; the executor may move the task between its threads, so all
//!   of it is `Send`.
//! - Their own locks and atomics are there because turso's `IO` and `File`
//!   take `&self` and must be `Send + Sync`, and the engine's files share
//!   the state's handles: one call at a time takes them, never waiting. A
//!   second flush of a state, or a second driver of the engine's I/O, is
//!   not queued: it cannot happen.
//! - `usable` is an atomic so that [`Database::is_usable`] answers while a
//!   call runs.
//!
//! What invocations share, and so the one place with real concurrency, is
//! the process's [`PageCache`].
//!
//! # Driving the engine
//!
//! The caller's task drives nonblocking engine open/connect state machines and
//! statement steps, awaits queued state reads, then resumes the engine.
//! Completions run on that task. No worker thread, command channel or
//! background task is created. Cached queries yield to the executor after 64
//! steps; an individual engine call is not preemptible. Calls on a session
//! run one at a time.
//!
//! The pinned engine also has synchronous metadata helpers. Open asynchronously
//! retains page zero for these reads; dirty overlay bytes always take priority.
//! `IO::step` serves only resident reads and local sync barriers. A synchronous
//! nonresident read or file mutation fails explicitly, without starting I/O.
//! Such an engine path cannot be suspended by this adapter; the session must be
//! discarded. Async query/checkpoint paths continue through the host I/O pump.
//!
//! The engine holds an owned schema lock across open-time I/O. The `parking_lot`
//! `send_guard` feature keeps the open future `Send` when the executor moves it
//! between threads. No unsafe future wrapper or engine-source patch is used.
//!
//! # Example
//!
//! ```ignore
//! # use restate_sessions::Database;
//! # async fn example(store: Arc<dyn KvStore>, cache: &PageCache) -> restate_sessions::Result<()> {
//! let db = Database::open(store).cached(cache, "chat/42").await?;
//! db.execute("CREATE TABLE messages(body TEXT)").await?; // durable now
//! db.begin().await?;
//! db.execute("INSERT INTO messages VALUES('hello')").await?;
//! db.commit([("outbox".into(), "sends".into())]).await?; // the row and the record, durable together
//! db.save().await?; // ends the session, leaving its engine to the next one
//! # Ok(())
//! # }
//! ```
//!
//! # Limits
//!
//! A session holds at most 256 MiB of changes not yet committed, and a
//! statement returns at most 64 MiB of rows ([`Limits`]): going past either
//! ends the session.
//!
//! # Features
//!
//! In tests, and with the crate's `__bench` feature: the state's in-memory
//! store, fault injection and latency simulator, and the I/O stats here.
mod io;

/// SQL values, as returned in rows and bound as parameters.
pub use turso_core::{Numeric, Value};

use crate::sql::io::{KvIo, MountedIo, DB_PATH};
use crate::state::yield_now;
use crate::state::{blobs::BlobStore, kv::KvStore, PageCache, Result, State, StateOptions};
use bytes::Bytes;
use futures::lock::{Mutex, MutexGuard};
use std::{
    collections::{BTreeMap, HashMap},
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use turso_core::storage::database::DatabaseFile;
use turso_core::{
    ConnectAsyncState, Connection, IOResult, OpenFlags, OpenOptions, SqliteDialect, StepResult,
    TempStore, IO,
};

/// Per-session SQL limits.
#[derive(Clone, Debug)]
pub(crate) struct Limits {
    /// Most bytes of changes not yet flushed and private engine WAL a session
    /// may hold; a write beyond it ends the session.
    pub max_private_bytes: usize,
    /// Most bytes of result rows one statement may collect; beyond it the
    /// session ends.
    pub max_result_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_private_bytes: 256 << 20,
            max_result_bytes: 64 << 20,
        }
    }
}

/// Local engine traffic, distinct from durable backend traffic.
#[cfg(any(test, feature = "__bench"))]
#[derive(Clone, Copy, Debug)]
pub struct IoStats {
    /// Bytes the engine wrote to its view of the database file.
    pub database_bytes_written: u64,
    /// Bytes the engine wrote to its private WAL.
    pub private_wal_bytes_written: u64,
    /// Bytes in the engine's view of the database file.
    pub database_len: u64,
    /// Bytes in the private WAL.
    pub private_wal_len: u64,
}
/// Result rows, each a value per column.
pub type Rows = Vec<Vec<Value>>;

/// The SQL engine over a state's file: one private SQL view, driven
/// entirely by the caller's async task, which a [`Database`] holds.
///
/// The host advances engine state machines, awaits queued KV I/O and resumes the
/// engine on the same task. No worker thread or background task is created.
/// An SQL error leaves the engine usable; a storage error, a cancelled call
/// or a failed flush ends it (see [`Self::is_usable`]). Its changes become
/// durable only with [`Self::commit`]; dropping it leaves the engine to the
/// next session.
struct Engine {
    state: State,
    database: Option<Arc<turso_core::Database>>,
    connection: Option<Arc<Connection>>,
    io: Arc<KvIo>,
    /// See [`Limits::max_result_bytes`].
    max_result_bytes: usize,
    poisoned: bool,
    /// Inside a `transaction()` body, from `begin` to its `commit` or
    /// `rollback`: a statement that ends the transaction fails, and ends the
    /// engine, rather than commit the body halfway.
    in_body: bool,
    /// No SQL changed per-connection state (settings, attachments, the temp
    /// schema), which would otherwise carry over to the next session.
    reusable: bool,
    /// The engine continued from a previous session.
    reused: bool,
    /// Bytes of the schema's SQL as of the last commit, to weigh a parked
    /// engine; `None` when unknown, which prevents parking.
    schema_bytes: Option<usize>,
    /// SQL since the last commit may have changed the schema.
    schema_changed: bool,
    /// The records [`Database::commit`] set since the last flush, which
    /// the next one makes durable with the SQL; a reader keeps them, as it
    /// keeps its SQL.
    records: BTreeMap<String, Bytes>,
    /// The statements of this session's SQL, by its text, each reset and run
    /// again: preparing one costs more than running it. See [`Prepared`].
    statements: HashMap<String, Prepared>,
}

/// A statement prepared from a call's SQL, a single statement, and whether
/// it ran quickly last time. A call yields before it runs a statement that
/// did not, or that it prepares, which may run long: so a host that polls a
/// call once on a thread of its own (the Node.js addon, on JavaScript's)
/// runs only quick statements there, and the others in the runtime.
struct Prepared {
    statement: turso_core::Statement,
    quick: bool,
}

/// How long a statement may run to count as quick.
const QUICK: std::time::Duration = std::time::Duration::from_millis(1);

/// The most statements a session keeps prepared: it prepares others each
/// time it runs them.
const MAX_STATEMENTS: usize = 256;

/// An idle engine and its adapter at a flushed state, kept in the owner's
/// [`crate::state::PageCache`] so the next session can skip opening an engine. Only the
/// database is kept: each session connects afresh, so no connection state
/// (counters such as `last_insert_rowid()`, settings, attachments, the temp
/// schema) carries over.
struct WarmEngine {
    database: Arc<turso_core::Database>,
    io: Arc<KvIo>,
    /// Bytes of its schema's SQL, which sizes its parsed schema.
    schema_bytes: usize,
}

/// Memory of a parked engine besides its schema: its database's buffer pool,
/// which turso maps outside the allocator (a 3 MiB arena), and ~48 KiB more.
/// Measured as resident memory: ~4 MiB per engine parked.
const ENGINE_BYTES: usize = 4 << 20;

impl WarmEngine {
    /// Estimated memory: the buffer pool, and the parsed schema.
    fn weight(&self) -> usize {
        ENGINE_BYTES + 16 * self.schema_bytes
    }
}

/// One session of a namespace's database, owned by one invocation of its
/// object's handler: every SQL commit is durable when its call returns.
///
/// A call that leaves no SQL transaction open commits before it returns: what
/// the SQL committed, and the records [`Self::commit`] set, in one flush of
/// the state. The host's `transaction()` runs its body between
/// [`Self::begin`] and [`Self::commit`] or [`Self::rollback`]; meanwhile a
/// statement that ends the transaction ends the session, rather than commit
/// the body halfway.
///
/// Calls hold the session one at a time, each for the whole call; the host
/// orders them, and keeps other calls out of a transaction. A session ends
/// with [`Self::save`], which leaves a clean engine to the next session of
/// the namespace, [`Self::close`], or a failure; later calls fail.
pub struct Database {
    session: Mutex<Session>,
    /// Whether the session can still run SQL, as of the last call that ended:
    /// read without the lock, which a running call holds.
    usable: AtomicBool,
    /// The engine's adapter, for its I/O stats.
    #[cfg(any(test, feature = "__bench"))]
    io: Arc<KvIo>,
    /// The state the database is kept in.
    #[cfg(test)]
    state: State,
    /// The engine continued from a previous session.
    #[cfg(test)]
    reused: bool,
}

/// A session's engine, or why it has none.
enum Session {
    Open(Engine),
    /// Ended by [`Database::save`] or [`Database::close`].
    Closed,
    /// Ended by a storage error, a cancelled call or a failed commit.
    Ended,
}

impl Session {
    fn engine(&mut self) -> Result<&mut Engine> {
        match self {
            Session::Open(engine) => Ok(engine),
            Session::Closed => Err(CLOSED.into()),
            Session::Ended => Err(ENDED.into()),
        }
    }
}

/// The error of a call after the session was saved or closed.
const CLOSED: &str = "the database is closed";

/// The error of a call after a failure ended the session.
const ENDED: &str =
    "the session ended after a storage error, a cancelled call or a failed commit; open a new one";

/// The error of a statement that ended the transaction of a `transaction()`
/// body.
const BODY_ENDED: &str = "a statement ended the transaction of transaction(), whose body must not: the session has ended";

/// A failure that a retry would repeat, whether or not it ended the session:
/// it comes of what the host gave (its SQL, its records, a transaction it left
/// open), not of storage. An SQL error the engine undid is one too, but the
/// session goes on after it ([`Database::is_usable`]); one of these ends it.
#[derive(Debug)]
pub struct Refused(String);

impl Refused {
    fn boxed(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(Refused(message.into()))
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

/// How to open a [`Database`] ([`Database::open`]): await it to open.
#[must_use = "a database opens when awaited"]
pub struct Open(crate::state::Open);

impl Open {
    /// Flush and read with `options` rather than the defaults.
    pub fn options(self, options: StateOptions) -> Self {
        Self(self.0.options(options))
    }

    /// Keep the bulk of the checkpoints in `packs`, S3 say, as L1: the
    /// latest few go to Restate first ([`StateOptions::l0_bytes`]). Every
    /// session of the database must.
    pub fn checkpoints(self, packs: Arc<dyn BlobStore>) -> Self {
        Self(self.0.checkpoints(packs))
    }

    /// Keep the database warm in the process's `cache`, as `namespace`: the
    /// key of what the store holds, which no other database of this cache
    /// uses (an object's key, say). With nothing changed since this process
    /// last committed it, the next session reads no pages, and continues
    /// the engine the last one left.
    pub fn cached(self, cache: &PageCache, namespace: &str) -> Self {
        Self(self.0.cached(cache, namespace))
    }

    /// Open a reader of a database that another session owns, beside it, as
    /// a shared handler does: it commits nothing, and reads the database as
    /// of open, whatever the owner commits meanwhile, until the owner's
    /// second checkpoint after it. Cached, it reads the pages the owner
    /// cached.
    pub fn reader(self) -> Self {
        Self(self.0.reader())
    }
}

impl IntoFuture for Open {
    type Output = Result<Database>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<Database>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { Database::new(self.0.await?).await })
    }
}

impl Database {
    /// Open the database kept in `store`, one object's Restate state. Await
    /// it as it is, or say how first:
    ///
    /// ```ignore
    /// let db = Database::open(store).cached(&cache, "object-42").await?;
    /// let db = Database::open(store).checkpoints(s3).reader().await?;
    /// ```
    pub fn open(store: Arc<dyn KvStore>) -> Open {
        Open(State::open(store))
    }

    /// A session of the database in `state`, however it was opened.
    pub(crate) async fn new(state: State) -> Result<Self> {
        Self::with_limits(state, Limits::default()).await
    }

    /// [`Self::new`] with other [`Limits`].
    pub(crate) async fn with_limits(state: State, limits: Limits) -> Result<Self> {
        let engine = Engine::open(state, limits).await?;
        Ok(Self {
            #[cfg(any(test, feature = "__bench"))]
            io: engine.io.clone(),
            #[cfg(test)]
            state: engine.state.clone(),
            #[cfg(test)]
            reused: engine.reused,
            session: Mutex::new(Session::Open(engine)),
            usable: AtomicBool::new(true),
        })
    }

    /// Run SQL (one or more statements; `params` bind to a single one) and
    /// return the column names and rows of its last statement. A string of
    /// several statements runs them in order, each as if executed on its
    /// own; an SQL error undoes just its statement. Values never become SQL
    /// text, so they need no quoting.
    pub async fn query_columns(
        &self,
        sql: &str,
        params: Option<Vec<Value>>,
    ) -> Result<(Vec<String>, Rows)> {
        self.call(async |engine| engine.query(sql, params).await)
            .await
    }

    /// Run SQL and return the rows of its last statement.
    pub async fn query(&self, sql: &str) -> Result<Rows> {
        Ok(self.query_columns(sql, None).await?.1)
    }

    /// [`Self::query`] for one statement with its parameters (`?`, `?N`,
    /// `:name`, ...) bound in order.
    pub async fn query_with(
        &self,
        sql: &str,
        params: impl IntoIterator<Item = Value>,
    ) -> Result<Rows> {
        let params = params.into_iter().collect();
        Ok(self.query_columns(sql, Some(params)).await?.1)
    }

    /// Run SQL, discarding the rows.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.query_columns(sql, None).await.map(drop)
    }

    /// [`Self::query_with`], discarding the rows.
    pub async fn execute_with(
        &self,
        sql: &str,
        params: impl IntoIterator<Item = Value>,
    ) -> Result<()> {
        self.query_with(sql, params).await.map(drop)
    }

    /// `BEGIN` the transaction of the host's `transaction()` body: until its
    /// `commit` or `rollback`, a statement that ends it ends the session.
    pub async fn begin(&self) -> Result<()> {
        self.call(async |engine| {
            if engine.in_body {
                return Err("transactions do not nest; use SAVEPOINT".into());
            }
            engine.execute("BEGIN").await?;
            engine.in_body = true;
            Ok(())
        })
        .await
    }

    /// `COMMIT` the transaction of a `transaction()` body, with `records`:
    /// whole values by name, kept beside the SQL (such as the outbox of the
    /// sends the transaction made), which commit with it in the same flush,
    /// even if it changed no row. A `COMMIT` that fails sets no record and
    /// leaves the transaction open, for `rollback`. See [`Self::record`].
    pub async fn commit(&self, records: impl IntoIterator<Item = (String, Bytes)>) -> Result<()> {
        let records: Vec<(String, Bytes)> = records.into_iter().collect();
        self.call(async |engine| {
            engine.in_body = false;
            // Nothing commits on a session a failure ended: the records would
            // stay pending, and the call would resolve as if they committed.
            if !engine.is_usable() {
                return Err(ENDED.into());
            }
            for (name, value) in &records {
                crate::state::check_record(name, value)?;
            }
            if engine.in_transaction() {
                engine.execute("COMMIT").await?;
            }
            engine.records.extend(records);
            Ok(())
        })
        .await
    }

    /// `ROLLBACK` the transaction of a `transaction()` body, if it is still
    /// open and no failure ended the session.
    pub async fn rollback(&self) -> Result<()> {
        self.call(async |engine| {
            engine.in_body = false;
            match engine.is_usable() && engine.in_transaction() {
                true => engine.execute("ROLLBACK").await,
                false => Ok(()),
            }
        })
        .await
    }

    /// Whether the session can still run SQL: an SQL error leaves it usable,
    /// a storage error or a cancelled call ends it. A call that is running
    /// does not change the answer until it ends.
    pub fn is_usable(&self) -> bool {
        self.usable.load(Ordering::Acquire)
    }

    /// The record `name` as this session holds it: as the last commit that
    /// set it left it, empty if none did. A reader's own commits set its
    /// records, as they change its SQL, though they are never stored. Once a
    /// failure ended the session, it is the record as stored; once a
    /// cancelled call dropped the engine, it fails with the session's end.
    pub async fn record(&self, name: &str) -> Result<Bytes> {
        let mut session = self.session.lock().await;
        let engine = session.engine()?;
        Ok(match engine.records.get(name) {
            Some(value) => value.clone(),
            None => engine.state.record(name),
        })
    }

    /// End the session, leaving its engine to the next one. It fails if a
    /// transaction was left open, whose changes are lost, or a failure ended
    /// the session.
    pub async fn save(&self) -> Result<()> {
        let mut session = self.session.lock().await;
        self.usable.store(false, Ordering::Release);
        let engine = session.engine()?;
        let saved = match (engine.is_usable(), engine.in_transaction()) {
            (false, _) => Err(ENDED.into()),
            (true, true) => Err(Refused::boxed(
                "a transaction was left open: its changes are lost",
            )),
            (true, false) => Ok(()),
        };
        // Dropped, a clean engine parks for the next session.
        *session = Session::Closed;
        saved
    }

    /// End the session, whatever state it is in.
    pub async fn close(&self) {
        *self.session.lock().await = Session::Closed;
        self.usable.store(false, Ordering::Release);
    }

    /// The state the database is kept in.
    #[cfg(test)]
    pub(crate) fn state(&self) -> &State {
        &self.state
    }

    /// Whether this session continued a previous session's engine.
    #[cfg(test)]
    pub(crate) fn reused_engine(&self) -> bool {
        self.reused
    }

    /// Local engine I/O only, useful for separating WAL traffic from backend writes.
    #[cfg(any(test, feature = "__bench"))]
    pub fn io_stats(&self) -> IoStats {
        self.io.stats()
    }

    /// Checkpoint the state in a flush of its own, which rewrites the
    /// database's first page as it is: its pages are read from the
    /// checkpoint's pack after it. Nothing with an empty database.
    #[cfg(test)]
    pub(crate) async fn checkpoint(&self) -> Result<crate::state::FlushReport> {
        self.call(async |engine| {
            let mut mutation = engine.state.mutation();
            if let Some(first) = engine.state.file().read(0).await? {
                mutation.write(0, Some(first))?;
            }
            mutation.checkpoint();
            mutation.flush().await
        })
        .await
    }

    /// Run `call` on the open session, then settle it. A statement that
    /// ended the transaction of a `transaction()` body ends the session.
    /// Otherwise a call that leaves no SQL transaction open commits, even
    /// after an SQL error (the statements before it stand): what the SQL
    /// committed, with the records set, in one flush, durable before the call
    /// returns. A reader commits nothing, and a session that a failure ended
    /// cannot. A call dropped before it returns ends the session.
    async fn call<T>(&self, call: impl AsyncFnOnce(&mut Engine) -> Result<T>) -> Result<T> {
        let mut running = Call {
            session: self.session.lock().await,
            usable: &self.usable,
            returned: false,
        };
        let engine = running.session.engine()?;
        let result = call(engine).await;
        if engine.in_body && !engine.in_transaction() {
            // `running` ends the session as it drops. A storage error that
            // ended the transaction too is the error to report.
            return match result {
                Err(error) if !engine.is_usable() => Err(error),
                _ => Err(Refused::boxed(BODY_ENDED)),
            };
        }
        let committed =
            match !engine.state.is_reader() && engine.is_usable() && !engine.in_transaction() {
                true => engine.commit().await,
                false => Ok(()),
            };
        if !engine.is_usable() {
            // Ended, the session commits nothing more: it holds what was
            // stored.
            engine.records.clear();
        }
        self.usable.store(engine.is_usable(), Ordering::Release);
        running.returned = true;
        committed.and(result)
    }
}

/// A call holding the session: unless it returns, its state is unknown (a
/// cancelled statement, a commit whose outcome is unknown), and dropping
/// this ends the session.
struct Call<'a> {
    session: MutexGuard<'a, Session>,
    usable: &'a AtomicBool,
    returned: bool,
}

impl Drop for Call<'_> {
    fn drop(&mut self) {
        if !self.returned && matches!(*self.session, Session::Open(_)) {
            self.usable.store(false, Ordering::Release);
            *self.session = Session::Ended;
        }
    }
}

impl Engine {
    /// Open the engine over `state`'s file: one parked by this owner's
    /// previous session if the state found one, or a new one.
    async fn open(state: State, limits: Limits) -> Result<Self> {
        let file = state.file();
        let warm = state.take_parked::<WarmEngine>();
        let io = match &warm {
            Some(warm) => {
                warm.io.resume(file, limits.max_private_bytes).await?;
                warm.io.clone()
            }
            None => KvIo::new(file, limits.max_private_bytes),
        };
        let mut engine = Self {
            state,
            database: None,
            connection: None,
            io,
            max_result_bytes: limits.max_result_bytes,
            poisoned: true,
            in_body: false,
            reused: warm.is_some(),
            reusable: true,
            schema_bytes: warm.as_ref().map(|warm| warm.schema_bytes),
            schema_changed: false,
            records: BTreeMap::new(),
            statements: HashMap::new(),
        };
        let database = match warm {
            Some(warm) => warm.database,
            None => {
                engine.io.prepare_engine().await?;
                open_engine(&engine.io).await?
            }
        };
        engine.connection = Some(connect(&engine.io, &database).await?);
        engine.database = Some(database);
        engine.poisoned = false;
        // A statement that would grow the database past the state's file cap
        // fails with SQLITE_FULL, and the session goes on.
        let page_size = engine.connection.as_ref().unwrap().get_page_size().get();
        let max_pages = crate::state::MAX_FILE_BYTES / u64::from(page_size);
        engine
            .internal(&format!("PRAGMA max_page_count = {max_pages}"))
            .await?;
        if !engine.reused && engine.state.is_cached() {
            engine.schema_bytes = Some(engine.measure_schema().await?);
        }
        Ok(engine)
    }

    /// Close the connection, with the statements, which hold it.
    fn disconnect(&mut self) {
        self.statements.clear();
        drop(self.connection.take());
    }

    /// Leave an idle engine at a flushed state to the owner cache, and
    /// whether it did. Only an engine at the owner's latest acknowledged
    /// flush qualifies: one that an overlapping session left behind holds an
    /// older state.
    fn park(&mut self) -> bool {
        if !self.state.can_park() || !self.reusable || self.schema_changed {
            return false;
        }
        let Some(schema_bytes) = self.schema_bytes else {
            return false;
        };
        if !self.is_usable() || self.in_transaction() || !self.io.is_clean() {
            return false;
        }
        self.disconnect();
        // Closing the connection must leave nothing behind.
        if !self.io.is_clean() {
            return false;
        }
        let Some(database) = self.database.take() else {
            return false;
        };
        // An idle engine reads nothing until the next open gives it the next
        // state's file.
        self.io.release();
        let warm = WarmEngine {
            database,
            io: self.io.clone(),
            schema_bytes,
        };
        let weight = warm.weight();
        self.state.park(warm, weight).is_ok()
    }

    /// Run SQL (one or more statements; `params` bind to a single one) and
    /// return the column names and rows of its last statement, collected in
    /// memory, at most [`Limits::max_result_bytes`] of them. A string of
    /// several statements runs them in order, each as if executed on its own.
    ///
    /// An SQL error (syntax, a constraint, a missing table) undoes that
    /// statement and leaves the engine usable; earlier statements of `sql`,
    /// and an open transaction, stand. Any other error or a cancellation ends
    /// it; see [`Self::is_usable`]. Cached execution yields every 64 steps;
    /// one engine call cannot be preempted. An engine helper requiring
    /// synchronous nonresident I/O returns an error.
    async fn query(
        &mut self,
        sql: &str,
        params: Option<Vec<Value>>,
    ) -> Result<(Vec<String>, Rows)> {
        self.screen(sql);
        self.run_statements(sql, params).await
    }

    async fn execute(&mut self, sql: &str) -> Result<()> {
        self.query(sql, None).await.map(drop)
    }

    /// Make what the SQL committed durable, and the records set since the
    /// last flush, in one flush of the state. No SQL transaction may be
    /// open. The engine WAL goes into the private view of the file, whose
    /// pages move into a mutation; SQL goes on reading them from its view
    /// until the flush settles them: the state holds them once it flushed.
    /// A flush the state refused (for writes it cannot store) wrote nothing,
    /// but the engine holds what it committed: the engine ends, with a
    /// [`Refused`] error. Any other failure ends the state, and the engine
    /// with it.
    async fn commit(&mut self) -> Result<()> {
        if self.io.has_changes() || !self.records.is_empty() {
            // Compressing the pages and storing them may take long: see
            // [`Prepared`].
            yield_now().await;
        }
        let mut mutation = self.state.mutation();
        for (name, value) in &self.records {
            mutation.set_record(name, value.clone())?;
        }
        if self.io.has_changes() {
            self.checkpoint().await?;
            // Failing, the move leaves the mutation and the engine apart:
            // the engine ends.
            self.poisoned = true;
            self.io.flush_into(&mut mutation)?;
            self.poisoned = false;
        }
        self.measure_changed_schema().await?;
        match mutation.flush().await {
            Ok(_) => {
                self.io.settle_flushed();
                self.records.clear();
                Ok(())
            }
            // Refused, the flush wrote nothing and the state goes on, but the
            // engine committed what it refused: the session ends, with
            // nothing of the call stored.
            Err(error) if self.state.is_usable() => {
                self.poisoned = true;
                Err(Refused::boxed(error.to_string()))
            }
            Err(error) => Err(error),
        }
    }

    /// Whether an SQL transaction is open.
    fn in_transaction(&self) -> bool {
        (self.connection.as_ref()).is_some_and(|connection| !connection.get_auto_commit())
    }

    /// Note what `sql` may change besides the database: connection state, which
    /// rules out reuse, and the schema, which a parked engine is weighed by.
    fn screen(&mut self, sql: &str) {
        if changes_connection_state(sql) {
            self.reusable = false;
        }
        if mentions(sql, &["create", "drop", "alter"]) {
            self.schema_changed = true;
        }
    }

    /// Bytes of the schema's SQL.
    async fn measure_schema(&mut self) -> Result<usize> {
        let rows = self
            .internal("SELECT coalesce(sum(length(sql)), 0) FROM sqlite_schema")
            .await?;
        let bytes = rows
            .first()
            .and_then(|row| row.first())
            .and_then(Value::as_int);
        Ok(bytes.unwrap_or(0).max(0) as usize)
    }

    /// Weigh the schema again after a statement that may have changed it,
    /// for parking the engine; even one rolled back, which moved no pages.
    async fn measure_changed_schema(&mut self) -> Result<()> {
        if self.schema_changed && self.state.is_cached() {
            // Without a size the engine is just not parked.
            self.schema_bytes = self.measure_schema().await.ok();
            self.schema_changed = false;
        }
        Ok(())
    }

    /// Whether the engine can run more SQL: see [`Database::is_usable`].
    fn is_usable(&self) -> bool {
        !self.poisoned && self.state.is_usable()
    }

    /// Run each statement of `sql`. With `params`, `sql` must be one
    /// statement.
    async fn run_statements(
        &mut self,
        sql: &str,
        mut params: Option<Vec<Value>>,
    ) -> Result<(Vec<String>, Rows)> {
        if let Some(prepared) = self.statements.remove(sql) {
            if !prepared.quick {
                yield_now().await;
            }
            return self.run_prepared(sql, prepared.statement, params).await;
        }
        yield_now().await;
        let mut remaining = sql;
        let mut rows = None;
        loop {
            if !self.is_usable() {
                return Err(ENDED.into());
            }
            self.poisoned = true; // cancellation/error requires discarding the session
            let connection = Arc::clone(self.connection.as_ref().unwrap());
            let prepared = match connection.consume_stmt(remaining) {
                Ok(prepared) => prepared,
                // A parse or name error: nothing ran.
                Err(error) if !self.io.failed() => {
                    self.poisoned = false;
                    return Err(error.into());
                }
                Err(error) => return Err(error.into()),
            };
            let Some((mut statement, end)) = prepared else {
                self.poisoned = false;
                break;
            };
            remaining = &remaining[end..];
            let only = rows.is_none()
                && (params.is_some()
                    || remaining
                        .trim_start_matches(|c: char| c.is_whitespace() || c == ';')
                        .is_empty());
            if let Some(values) = params.take() {
                // Refused before anything runs, so the session stays usable.
                match connection.consume_stmt(remaining) {
                    Ok(None) => {}
                    Ok(Some(_)) => {
                        self.poisoned = false;
                        return Err("parameters bind to a single statement".into());
                    }
                    // A parse or name error: nothing ran.
                    Err(error) if !self.io.failed() => {
                        self.poisoned = false;
                        return Err(error.into());
                    }
                    Err(error) => return Err(error.into()),
                }
                if values.len() != statement.parameters_count() {
                    self.poisoned = false;
                    return Err(format!(
                        "the statement has {} parameters, {} values given",
                        statement.parameters_count(),
                        values.len()
                    )
                    .into());
                }
                for (index, value) in (1..).zip(values) {
                    if let Err(error) =
                        statement.bind_at(std::num::NonZero::new(index).unwrap(), value)
                    {
                        self.poisoned = false;
                        return Err(error.into());
                    }
                }
            }
            let limit = self.max_result_bytes;
            let columns = (0..statement.num_columns())
                .map(|i| statement.get_column_name(i).into_owned())
                .collect();
            let started = std::time::Instant::now();
            let ran = query_engine(&self.io, &mut statement, limit).await;
            let quick = started.elapsed() < QUICK;
            let failure = match ran {
                Ok(result) => {
                    rows = Some((columns, result));
                    None
                }
                Err(Failure::Sql(error)) => Some(error),
                Err(Failure::Fatal(error)) => return Err(error),
            };
            // Before the next statement of `sql`, which would run outside it.
            if self.in_body && connection.get_auto_commit() {
                return Err(Refused::boxed(BODY_ENDED));
            }
            self.poisoned = false;
            // The statement had no effect; earlier statements of `sql` stand.
            if let Some(error) = failure {
                return Err(error.into());
            }
            if only {
                self.keep(sql, statement, quick);
            }
        }
        rows.ok_or_else(|| "the SQL contains no statements".into())
    }

    /// Run `statement`, prepared from `sql` before, again with `params`.
    async fn run_prepared(
        &mut self,
        sql: &str,
        mut statement: turso_core::Statement,
        params: Option<Vec<Value>>,
    ) -> Result<(Vec<String>, Rows)> {
        if !self.is_usable() {
            return Err(ENDED.into());
        }
        statement.clear_bindings();
        if let Some(values) = params {
            if values.len() != statement.parameters_count() {
                return Err(format!(
                    "the statement has {} parameters, {} values given",
                    statement.parameters_count(),
                    values.len()
                )
                .into());
            }
            for (index, value) in (1..).zip(values) {
                statement.bind_at(std::num::NonZero::new(index).unwrap(), value)?;
            }
        }
        self.poisoned = true;
        let started = std::time::Instant::now();
        let ran = query_engine(&self.io, &mut statement, self.max_result_bytes).await;
        let quick = started.elapsed() < QUICK;
        // Named as it ran: a schema changed since its last run prepares it anew.
        let columns = (0..statement.num_columns())
            .map(|i| statement.get_column_name(i).into_owned())
            .collect();
        if self.in_body && self.connection.as_ref().unwrap().get_auto_commit() {
            return Err(Refused::boxed(BODY_ENDED));
        }
        let rows = match ran {
            Ok(rows) => rows,
            Err(Failure::Sql(error)) => {
                self.poisoned = false;
                return Err(error.into());
            }
            Err(Failure::Fatal(error)) => return Err(error),
        };
        self.poisoned = false;
        self.keep(sql, statement, quick);
        Ok((columns, rows))
    }

    /// Keep `statement`, of `sql`, which ran to its end, for the next call of
    /// it, reset: a statement left mid-way holds the version it read, which
    /// a checkpoint would wait for.
    fn keep(&mut self, sql: &str, mut statement: turso_core::Statement, quick: bool) {
        if self.statements.len() < MAX_STATEMENTS && statement.reset().is_ok() {
            (self.statements).insert(sql.to_owned(), Prepared { statement, quick });
        }
    }

    /// Run one statement of this crate's own, outside the transaction rules.
    async fn internal(&mut self, sql: &str) -> Result<Rows> {
        if !self.is_usable() {
            return Err(ENDED.into());
        }
        self.poisoned = true;
        let mut statement = self.connection.as_ref().unwrap().prepare(sql)?;
        let rows = match query_engine(&self.io, &mut statement, usize::MAX).await {
            Ok(rows) => rows,
            Err(Failure::Sql(error)) => return Err(error.into()),
            Err(Failure::Fatal(error)) => return Err(error),
        };
        self.poisoned = false;
        Ok(rows)
    }

    /// Merge the private engine WAL into the private database overlay and truncate it.
    ///
    /// This is a local barrier, **not** backend durability. Failure poisons the
    /// engine. A successful checkpoint may still be discarded without saving.
    async fn checkpoint(&mut self) -> Result<()> {
        let rows = self.internal("PRAGMA wal_checkpoint(TRUNCATE)").await?;
        if rows.len() != 1
            || rows[0].len() != 3
            || rows[0][0].as_int() != Some(0)
            || rows[0][1] != rows[0][2]
        {
            self.poisoned = true;
            return Err("checkpoint did not complete fully".into());
        }
        if self.io.wal_len() != 0 {
            self.poisoned = true;
            return Err("checkpoint did not truncate the private WAL".into());
        }
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // An engine that ends clean (every change flushed by the state)
        // leaves itself for the next session.
        if !self.park() {
            self.io.shutdown();
            self.disconnect();
        }
    }
}

/// Whether SQL may change per-connection state rather than the database:
/// PRAGMA settings, ATTACH/DETACH or the temp schema. Conservative: a false
/// positive only costs reusing the engine.
fn changes_connection_state(sql: &str) -> bool {
    mentions(sql, &["pragma", "attach", "detach", "temp", "temporary"])
}

/// Whether `sql` contains one of `keywords` as a whole word, in any case.
fn mentions(sql: &str, keywords: &[&str]) -> bool {
    sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .any(|word| {
            keywords
                .iter()
                .any(|keyword| word.eq_ignore_ascii_case(keyword))
        })
}

/// Resume an engine completion only after the host has driven all submitted I/O.
async fn drive_completion(io: &KvIo, completion: turso_core::types::IOCompletions) -> Result<()> {
    if completion.is_explicit_yield() {
        yield_now().await;
    } else {
        io.drive().await?;
        if !completion.finished() {
            return Err("engine completion has no queued I/O".into());
        }
        if let Some(error) = completion.get_error() {
            return Err(turso_core::LimboError::from(error).into());
        }
    }
    Ok(())
}

// TODO: open the engine without transparent huge pages, once a turso release
// lets its buffer arena do without `MADV_HUGEPAGE`: a cold open's first read
// may wait for a huge page.
async fn open_engine(io: &Arc<KvIo>) -> Result<Arc<turso_core::Database>> {
    let mounted: Arc<dyn IO> = Arc::new(MountedIo(io.clone()));
    let file = mounted.open_file(DB_PATH, OpenFlags::Create, false)?;
    let options =
        OpenOptions::new(Arc::new(SqliteDialect)).storage(Arc::new(DatabaseFile::new(file)));
    // Bypass the global registry: reopening constructs a cold engine.
    let mut state = turso_core::OpenDbAsyncState::new();
    let db = loop {
        match turso_core::Database::do_open_async(&mut state, mounted.clone(), DB_PATH, &options)? {
            IOResult::Done(db) => break db,
            IOResult::IO(completion) => drive_completion(io, completion).await?,
        }
    };
    Ok(db)
}

/// A new connection with this crate's settings.
async fn connect(io: &KvIo, db: &Arc<turso_core::Database>) -> Result<Arc<Connection>> {
    let mut state = ConnectAsyncState::default();
    let connection = loop {
        match db.connect_async(&mut state)? {
            IOResult::Done(connection) => break connection,
            IOResult::IO(completion) => drive_completion(io, completion).await?,
        }
    };
    connection.wal_auto_actions_disable();
    connection.set_temp_store(TempStore::Memory);
    Ok(connection)
}

/// Bytes a value takes in collected rows, approximately.
fn value_bytes(value: &Value) -> usize {
    16 + match value {
        Value::Text(text) => text.as_str().len(),
        Value::Blob(blob) => blob.len(),
        _ => 0,
    }
}

/// Why a statement failed.
enum Failure {
    /// The engine rejected the SQL (a constraint, a type error, ...) and undid
    /// the statement; no I/O failed and none is pending, so the session can go on.
    Sql(turso_core::LimboError),
    /// Anything else: storage, limits, anomalies. The session's state is unknown.
    Fatal(Box<dyn std::error::Error + Send + Sync>),
}

impl<E: Into<Box<dyn std::error::Error + Send + Sync>>> From<E> for Failure {
    fn from(error: E) -> Self {
        Failure::Fatal(error.into())
    }
}

/// Abort queued requests before a suspended statement is destroyed. This guard
/// is created after the statement so cancellation retires completions first.
struct Running<'a> {
    io: &'a KvIo,
    complete: bool,
}
impl Drop for Running<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.io.shutdown();
        }
    }
}

/// Step `statement` to its end, driving its I/O and collecting its rows, at
/// most `max_bytes` of them. A failure of a write over `max_private_bytes` is
/// [`Refused`]: the same statement goes over it again on every retry.
async fn query_engine(
    io: &KvIo,
    statement: &mut turso_core::Statement,
    max_bytes: usize,
) -> std::result::Result<Rows, Failure> {
    match step_to_end(io, statement, max_bytes).await {
        Err(Failure::Fatal(_)) if io.over_limit() => Err(Failure::Fatal(Refused::boxed(
            "private changes exceed Limits::max_private_bytes",
        ))),
        ran => ran,
    }
}

/// See [`query_engine`].
async fn step_to_end(
    io: &KvIo,
    statement: &mut turso_core::Statement,
    max_bytes: usize,
) -> std::result::Result<Rows, Failure> {
    let mut running = Running {
        io,
        complete: false,
    };
    let mut rows = Vec::new();
    let mut bytes = 0;
    let mut steps = 0;
    loop {
        // Bound work between host yields even when all pages are already cached.
        // A single engine step is not preemptible by this adapter.
        if steps == 64 {
            yield_now().await;
            steps = 0;
        }
        steps += 1;
        let step = match statement.step() {
            Ok(step) => step,
            Err(error) if !io.failed() && io.pending_count() == 0 => {
                running.complete = true;
                return Err(Failure::Sql(error));
            }
            Err(error) => return Err(error.into()),
        };
        match step {
            StepResult::Row => {
                let row: Vec<Value> = statement
                    .row()
                    .ok_or("missing row")?
                    .get_values()
                    .cloned()
                    .collect();
                bytes += row.iter().map(value_bytes).sum::<usize>();
                if bytes > max_bytes {
                    return Err(Failure::Fatal(Refused::boxed(
                        "result rows exceed Limits::max_result_bytes",
                    )));
                }
                rows.push(row);
            }
            StepResult::Done => {
                running.complete = true;
                return Ok(rows);
            }
            StepResult::IO => {
                let completion = statement
                    .take_io_completions()
                    .ok_or("engine yielded I/O without a completion")?;
                drive_completion(io, completion).await?;
            }
            StepResult::Yield => yield_now().await,
            StepResult::Busy | StepResult::Sleep { .. } => {
                return Err("unexpected contention in single-owner session".into());
            }
            StepResult::Interrupt => return Err("statement interrupted".into()),
        }
    }
}

#[cfg(test)]
#[path = "tests/unit/session.rs"]
mod checkpoint_tests;

#[cfg(test)]
mod tests;
