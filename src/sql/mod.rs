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
//! # Shared statements
//!
//! The statements a session compiles are shared with the other sessions of
//! the process, by the schema they were compiled for and their exact text, so
//! that a session of a schema that others ran compiles nothing: see the
//! `statements` module's documentation for what keeps one schema's program
//! from another's, and its bounds. [`statement_stats`] counts what it does.
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
//!   holding the session, on its task, or on the thread that polls it (see
//!   below). No thread or channel is created; the executor may move the task
//!   between its threads, so all of it is `Send`.
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
//! # Calls polled in place
//!
//! A host may poll a call once itself, on a thread of its own and outside
//! any task, in the context of a tokio runtime (the Node addon does, on
//! JavaScript's thread), take the answer if the call is ready, and otherwise
//! hand the same future to the runtime. The call runs where it is polled
//! until it must wait or reaches work that must not run there, and goes on
//! wherever it is polled next.
//!
//! The host promises that a call that returned `Pending` is polled again,
//! promptly, in a task of the runtime, or dropped (which ends the session, as
//! any dropped call does). The engine promises that in a poll outside a task
//! a call never:
//!
//! - opens the database, or compiles a statement, or runs one it has not run
//!   in this session, or runs a kept statement whose last run held a thread
//!   for 250 µs or more;
//! - makes durable the changes to the database (compresses and stores
//!   their pages), or weighs a schema that changed;
//! - runs more than 64 steps of a statement, or a step that needs a page
//!   that is not in memory: neither in the session's view of the database
//!   nor in the memory tier of the process's page cache. A page that is in
//!   the process's page cache is read, and its changes decoded, in the poll.
//!
//! It yields before them, which returns `Pending`; in a task nothing of this
//! costs a yield that the engine did not make already (a program another
//! session compiled costs none at all, as nothing is compiled for it). One
//! exception to the first item: a program another session compiled, of a text
//! the process knows to run quickly, runs where it is polled, as a kept
//! statement does. Nothing is ever compiled in such a poll, whatever is known
//! of the text: the record says that a text's runs are quick, not that its
//! compile is.
//!
//! How long a statement is quick is the longest single poll of its run
//! (preparing it not included), not the time it took in all, which waiting
//! for a page adds to. A session's kept statement holds that of its last run;
//! the process's record of texts is taught by the first run of each text in a
//! session, and by a slow run of a kept statement. What remains: one step of
//! a statement cannot be preempted, so a statement that was quick and has
//! since grown slow (a scan of a table that has grown) holds the thread for
//! one run, and is then not run in place until runs are quick again; and the
//! poll also takes the session's lock, binds the call's parameters and
//! collects its rows, which grow with the call, not with the database.
//! The polling thread also waits for the locks of the record and of the
//! cache of programs, which are held for a lookup or an insert, never for a
//! scan of a schema's image or a compile.
//!
//! # Driving the engine
//!
//! The call's task drives nonblocking engine open/connect state machines and
//! statement steps, awaits queued state reads, then resumes the engine.
//! Completions run on that task. No worker thread or command channel is
//! created. Cached queries yield to the executor after 64
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
//! # use restate_sdk_shared_core::sql::Database;
//! # use restate_sdk_shared_core::state::{KvStore, PageCache};
//! # async fn example(store: Arc<dyn KvStore>, cache: &PageCache) -> restate_sdk_shared_core::state::Result<()> {
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
mod statements;

pub use statements::{statement_stats, StatementStats};
use statements::{Cache, SchemaId};

/// SQL values, as returned in rows and bound as parameters.
pub use turso_core::{Numeric, Value};

use crate::sql::io::{KvIo, MountedIo, DB_PATH};
use crate::state::{blobs::BlobStore, kv::KvStore, PageCache, Result, State, StateOptions};
use crate::state::{outside_task, yield_now, yield_outside_task};
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
    time::{Duration, Instant},
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
/// A statement's column names, shared by the statement a session keeps and
/// the results of its runs.
pub type Columns = Arc<[String]>;

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
    /// SQL in the open transaction may have changed the schema, since
    /// [`Self::statements`] were last dropped.
    schema_changed_in_transaction: bool,
    /// The records [`Database::commit`] set since the last flush, which
    /// the next one makes durable with the SQL; a reader keeps them, as it
    /// keeps its SQL.
    records: BTreeMap<String, Bytes>,
    /// The statements of this session's SQL, by its text, each reset and run
    /// again: preparing one costs more than running it. See [`Prepared`].
    statements: HashMap<String, Prepared>,
    /// The commit's `PRAGMA wal_checkpoint(TRUNCATE)`, prepared once and reset
    /// after each run, and dropped wherever [`Self::statements`] are. Boxed:
    /// a statement is large, and an engine is held by value in a [`Session`].
    checkpoint_statement: Option<Box<turso_core::Statement>>,
    /// The programs the session shares with the others ([`statements`]): the
    /// process's cache, unless a test gave it another or none.
    cache: Option<Arc<Cache>>,
    /// The schema's id in [`Self::cache`], the key of the shared programs;
    /// `None` while the engine's SQL may have changed the schema or the
    /// connection, which keeps it from sharing.
    schema: Option<SchemaId>,
    /// The engine read the schema's image since SQL last may have changed
    /// the schema or the connection, whether or not the cache could take it
    /// (an image too large for it is not read again).
    schema_read: bool,
    /// What this engine compiled and shared, for tests.
    #[cfg(test)]
    tally: Tally,
}

/// An engine's own count of its statements' compiles and shared hits.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tally {
    pub compiled: u64,
    pub shared_hits: u64,
    pub reprepares: u64,
}

/// A statement prepared from a call's SQL, a single statement, and how long
/// its last run held a thread. A call yields before it runs a statement that
/// held it long, or that it prepares, which may run long: so a host that polls a call once on a thread
/// of its own (the Node.js addon, on JavaScript's) runs only quick
/// statements there, and the others in the runtime (see "Calls polled in
/// place" above).
struct Prepared {
    statement: turso_core::Statement,
    /// The longest one poll of its last run held a thread (not preparing it).
    longest: Duration,
    /// What its text mentions, which the text alone decides: kept, a call of
    /// it does not scan the text again.
    mentions: Mentions,
    /// Its column names, as of its last run: shared with each result.
    columns: Columns,
}

/// The keywords of a call's SQL that the engine acts on.
#[derive(Clone, Copy)]
struct Mentions {
    /// PRAGMA, ATTACH, DETACH or the temp schema: connection state, which
    /// rules out reusing the engine.
    connection_state: bool,
    /// CREATE, DROP or ALTER: the schema may change.
    schema: bool,
    /// ROLLBACK.
    rollback: bool,
    /// ANALYZE, VACUUM, REINDEX or EXPLAIN: not shared, and the schema's
    /// rows may change ([`statements`]).
    unshared: bool,
}

impl Mentions {
    /// Whether a statement of this text may share its program with other
    /// sessions: no DDL, nothing that changes the connection.
    fn shareable(self) -> bool {
        !(self.connection_state || self.schema || self.unshared)
    }

    /// What `sql` mentions as a whole word, in any case, in one pass over it.
    /// Conservative: a false positive only costs reusing the engine, or
    /// preparing statements again.
    fn of(sql: &str) -> Self {
        let mut found = Mentions {
            connection_state: false,
            schema: false,
            rollback: false,
            unshared: false,
        };
        let words = sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'));
        for word in words {
            let is = |keyword: &str| word.eq_ignore_ascii_case(keyword);
            match word.len() {
                4 => {
                    found.connection_state |= is("temp");
                    found.schema |= is("drop");
                }
                5 => found.schema |= is("alter"),
                6 => {
                    found.connection_state |= is("pragma") || is("attach") || is("detach");
                    found.schema |= is("create");
                    found.unshared |= is("vacuum");
                }
                7 => found.unshared |= is("analyze") || is("reindex") || is("explain"),
                8 => found.rollback |= is("rollback"),
                9 => found.connection_state |= is("temporary"),
                _ => {}
            }
        }
        found
    }
}

/// How long a poll of a statement may hold a thread for the statement to
/// count as quick: its call does not yield before it.
const QUICK: Duration = Duration::from_millis(1);

/// [`QUICK`] in a poll outside a task, where the thread is a host's own: also
/// the bound of the process's record of quick texts ([`quick_texts`]).
const QUICK_IN_PLACE: Duration = Duration::from_micros(250);

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
    /// What a call takes with it ([`Self::call_session`]).
    shared: Arc<Shared>,
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

/// A session, as its calls hold it: on the runtime, as tasks of their own.
struct Shared {
    session: Mutex<Session>,
    /// Fired by [`Database::close`]: the call running then is dropped (as a
    /// cancelled call is), rather than run to its end, so that what it waits
    /// for (a checkpoint's upload, say) stops with the attempt that closed it.
    closing: tokio_util::sync::CancellationToken,
    /// Whether the session can still run SQL, as of the last call that ended:
    /// read without the lock, which a running call holds.
    usable: AtomicBool,
    /// A call was dropped before it returned. Its task may have finished
    /// regardless, so the session ends as the next to hold it sees this
    /// ([`Self::lock`]): a dropped call ends it, whenever its task stopped.
    abandoned: AtomicBool,
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
        // Opening reads pages, and runs the engine: not in a poll of a host's.
        Box::pin(async move {
            yield_outside_task().await;
            Database::new(self.0.await?).await
        })
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
            shared: Arc::new(Shared {
                session: Mutex::new(Session::Open(engine)),
                closing: tokio_util::sync::CancellationToken::new(),
                usable: AtomicBool::new(true),
                abandoned: AtomicBool::new(false),
            }),
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
        let (columns, rows) = self.query_columns_owned(sql.to_owned(), params).await?;
        Ok((columns.to_vec(), rows))
    }

    /// [`Self::query_columns`] for SQL the caller owns: the call runs on the
    /// runtime and needs it there, so a host that has a `String` hands it
    /// over rather than have it copied. The column names are shared with
    /// the statement the session keeps, and with the next run's results.
    pub async fn query_columns_owned(
        &self,
        sql: String,
        params: Option<Vec<Value>>,
    ) -> Result<(Columns, Rows)> {
        self.call_session(move |db| async move {
            db.call(async |engine| engine.query(&sql, params).await)
                .await
        })
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
        self.call_session(|db| async move {
            db.call(async |engine| {
                if engine.in_body {
                    return Err("transactions do not nest; use SAVEPOINT".into());
                }
                engine.execute("BEGIN").await?;
                engine.in_body = true;
                Ok(())
            })
            .await
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
        self.call_session(move |db| async move {
            db.call(async |engine| {
                engine.in_body = false;
                // Nothing commits on a session a failure ended: the records
                // would stay pending, and the call would resolve as if they
                // committed.
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
        })
        .await
    }

    /// `ROLLBACK` the transaction of a `transaction()` body, if it is still
    /// open and no failure ended the session.
    pub async fn rollback(&self) -> Result<()> {
        self.call_session(|db| async move {
            db.call(async |engine| {
                engine.in_body = false;
                match engine.is_usable() && engine.in_transaction() {
                    true => engine.execute("ROLLBACK").await,
                    false => Ok(()),
                }
            })
            .await
        })
        .await
    }

    /// Whether the session can still run SQL: an SQL error leaves it usable,
    /// a storage error or a cancelled call ends it. A call that is running
    /// does not change the answer until it ends.
    pub fn is_usable(&self) -> bool {
        // `abandoned` only ever turns true, so a dropped call ends the
        // answer here even if its task, not yet aborted, stores `usable`
        // after the drop did.
        self.shared.usable.load(Ordering::Acquire) && !self.shared.abandoned.load(Ordering::Acquire)
    }

    /// The record `name` as this session holds it: as the last commit that
    /// set it left it, empty if none did. A reader's own commits set its
    /// records, as they change its SQL, though they are never stored. Once a
    /// failure ended the session, it is the record as stored; once a
    /// cancelled call dropped the engine, it fails with the session's end.
    pub async fn record(&self, name: &str) -> Result<Bytes> {
        let mut session = self.shared.lock().await;
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
        let mut session = self.shared.lock().await;
        self.shared.usable.store(false, Ordering::Release);
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

    /// End the session, whatever state it is in. A call running is
    /// cancelled, as if its future were dropped: its outcome is unknown, and
    /// the state it ran on is not reused ([`Call`]).
    pub async fn close(&self) {
        self.shared.closing.cancel();
        *self.shared.session.lock().await = Session::Closed;
        self.shared.usable.store(false, Ordering::Release);
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
        self.call_session(|db| async move {
            db.call(async |engine| {
                let mut mutation = engine.state.mutation();
                if let Some(first) = engine.state.file().read(0).await? {
                    mutation.write(0, Some(first))?;
                }
                mutation.checkpoint();
                mutation.flush().await
            })
            .await
        })
        .await
    }

    /// The call `call` makes of the session, where it is polled ([the module
    /// documentation](self#calls-polled-in-place)): dropped before it
    /// returns, it ends the session.
    async fn call_session<T, F>(&self, call: impl FnOnce(Arc<Shared>) -> F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        /// Marks the session abandoned unless the call returned.
        struct Abandon<'a>(&'a Shared, bool);
        impl Drop for Abandon<'_> {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.usable.store(false, Ordering::Release);
                    self.0.abandoned.store(true, Ordering::Release);
                }
            }
        }
        let mut returned = Abandon(&self.shared, false);
        // Boxed: a call's future would otherwise nest in its caller's, on its
        // stack.
        let result = Box::pin(call(self.shared.clone())).await;
        returned.1 = true;
        result
    }
}

impl Shared {
    /// The session, held: ended first if a call was abandoned.
    async fn lock(&self) -> MutexGuard<'_, Session> {
        let mut session = self.session.lock().await;
        if self.abandoned.load(Ordering::Acquire) && matches!(*session, Session::Open(_)) {
            *session = Session::Ended;
        }
        session
    }

    /// Run `call` on the open session, then settle it. A statement that
    /// ended the transaction of a `transaction()` body ends the session.
    /// Otherwise a call that leaves no SQL transaction open commits, even
    /// after an SQL error (the statements before it stand): what the SQL
    /// committed, with the records set, in one flush, durable before the call
    /// returns. A reader commits nothing, and a session that a failure ended
    /// cannot. A call dropped before it returns ends the session.
    async fn call<T>(&self, call: impl AsyncFnOnce(&mut Engine) -> Result<T>) -> Result<T> {
        // Closing drops the call where it waits, which ends the session
        // ([`Call`]); a call that returned as the database closed returns
        // its result.
        let run = std::pin::pin!(self.run(call));
        let closed = std::pin::pin!(self.closing.cancelled());
        match futures::future::select(run, closed).await {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right(((), _)) => Err(CLOSED.into()),
        }
    }

    /// [`Self::call`], until the database closes.
    async fn run<T>(&self, call: impl AsyncFnOnce(&mut Engine) -> Result<T>) -> Result<T> {
        let mut running = Call {
            session: self.lock().await,
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
            schema_changed_in_transaction: false,
            records: BTreeMap::new(),
            statements: HashMap::new(),
            checkpoint_statement: None,
            cache: Some(statements::process()),
            schema: None,
            schema_read: false,
            #[cfg(test)]
            tally: Tally::default(),
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
        engine.share_schema().await?;
        Ok(engine)
    }

    /// Close the connection, with the statements, which hold it.
    fn disconnect(&mut self) {
        self.statements.clear();
        self.checkpoint_statement = None;
        drop(self.connection.take());
    }

    /// Leave an idle engine at a flushed state to the cache, and
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
    async fn query(&mut self, sql: &str, params: Option<Vec<Value>>) -> Result<(Columns, Rows)> {
        // Taken out, to be put back when it has run: with its text, which the
        // call then does not copy, and what the text mentions.
        let kept = self.statements.remove_entry(sql);
        let mentions = match &kept {
            Some((_, prepared)) => prepared.mentions,
            None => Mentions::of(sql),
        };
        self.screen(mentions);
        let ran = self.run_statements(sql, params, kept, mentions).await;
        self.expire_statements(mentions);
        ran
    }

    /// Drop the statements kept for their next run when a transaction whose
    /// SQL may have changed the schema ends, however it ends. The engine
    /// checks a kept statement against the schema cookie alone, and a
    /// rollback takes the cookie back, to a value the next schema change
    /// reaches again: a statement prepared between the undone change and the
    /// rollback would then pass the check and run on the b-trees of the
    /// schema the rollback undid, such as an INSERT into a table that no
    /// longer exists, whose root page a new table took. SQLite expires its
    /// statements on such a rollback; the engine does not. (`ROLLBACK TO` a
    /// savepoint it handles.)
    ///
    /// The transaction is open again after a call that rolled back and began
    /// the next, such as `ROLLBACK; BEGIN`, so its `ROLLBACK` counts too. No
    /// other end comes before more SQL of the call: a statement that fails
    /// ends the call, and `COMMIT` only moves the cookie forward. A
    /// `ROLLBACK TO` costs preparing the statements again.
    fn expire_statements(&mut self, sql: Mentions) {
        if self.schema_changed_in_transaction && (!self.in_transaction() || sql.rollback) {
            self.statements.clear();
            self.checkpoint_statement = None;
            self.schema_changed_in_transaction = false;
        }
        if self.in_transaction() && sql.schema {
            self.schema_changed_in_transaction = true;
        }
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
        if !self.io.has_changes() && self.records.is_empty() {
            // Nothing to make durable: a flush of nothing writes nothing and
            // leaves the state as it is, so there is none.
            return self.measure_changed_schema().await;
        }
        // Compressing the pages and storing them may take long: see
        // [`Prepared`].
        yield_now().await;
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
    fn screen(&mut self, sql: Mentions) {
        if sql.connection_state {
            self.reusable = false;
        }
        if sql.schema {
            self.schema_changed = true;
        }
        // Programs compiled from now on are not those of the schema's id or
        // settings, until a commit settles them again.
        if !sql.shareable() {
            self.schema = None;
            self.schema_read = false;
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
        let weigh = self.schema_changed && self.state.is_cached();
        // Whatever SQL took the schema's id away has ended: the schema is as
        // committed (even a rolled back change leaves it as it was).
        let share = self.cache.is_some() && self.reusable && !self.schema_read;
        if weigh || share {
            // Reading the schema takes as long as the schema is large.
            yield_outside_task().await;
        }
        if weigh {
            // Without a size the engine is just not parked.
            self.schema_bytes = self.measure_schema().await.ok();
            self.schema_changed = false;
        }
        if share {
            self.share_schema().await?;
        }
        Ok(())
    }

    /// How long a kept statement's last run may have held a thread for the
    /// call to run it in this poll: less in a poll outside a task.
    fn quick_bound() -> Duration {
        match outside_task() {
            true => QUICK_IN_PLACE,
            false => QUICK,
        }
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
        kept: Option<(String, Prepared)>,
        mentions: Mentions,
    ) -> Result<(Columns, Rows)> {
        if let Some((key, prepared)) = kept {
            if prepared.longest >= Self::quick_bound() {
                yield_now().await;
            }
            return self.run_prepared(key, prepared, params, false).await;
        }
        // A program another session compiled needs no preparing: it runs where
        // it is polled if its text is known to run quickly, else a task runs
        // it. Nothing is ever compiled in a poll outside a task: it takes as
        // long as the text is long and the schema large. As the engine always
        // did, a text that is not shared yields first.
        if let Some(statement) = self.shared_statement(sql, mentions) {
            if outside_task() && !quick_texts::known(sql) {
                yield_now().await;
            }
            let prepared = Prepared {
                statement,
                longest: Duration::ZERO,
                mentions,
                columns: Columns::from([]),
            };
            return self
                .run_prepared(sql.to_owned(), prepared, params, true)
                .await;
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
            let compiling = Instant::now();
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
            self.compiled(compiling.elapsed());
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
            let columns: Columns = (0..statement.num_columns())
                .map(|i| statement.get_column_name(i).into_owned())
                .collect();
            let (ran, longest) = timed(query_engine(&self.io, &mut statement, limit)).await;
            self.reprepared(&mut statement);
            if only {
                quick_texts::observe(sql, longest);
            }
            let failure = match ran {
                Ok(result) => {
                    rows = Some((columns.clone(), result));
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
                self.share(sql, &statement, mentions);
                self.keep(sql.into(), statement, longest, mentions, columns);
            }
        }
        rows.ok_or_else(|| "the SQL contains no statements".into())
    }

    /// Run `statement`, prepared from `sql` before, again with `params`.
    async fn run_prepared(
        &mut self,
        key: String,
        prepared: Prepared,
        params: Option<Vec<Value>>,
        first_run: bool,
    ) -> Result<(Columns, Rows)> {
        let Prepared {
            mut statement,
            mentions,
            columns: kept,
            ..
        } = prepared;
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
        let (ran, longest) = timed(query_engine(
            &self.io,
            &mut statement,
            self.max_result_bytes,
        ))
        .await;
        self.reprepared(&mut statement);
        // The first run of a text in a session tells the record, hit or
        // compiled; a kept statement tells it only when it was slow, so that
        // the others need not wait for the lock of the record.
        if first_run || longest >= QUICK_IN_PLACE {
            quick_texts::observe(&key, longest);
        }
        // Named as it ran: a schema changed since its last run prepares it anew.
        let columns = columns_of(&statement, kept);
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
        self.keep(key.into(), statement, longest, mentions, columns.clone());
        Ok((columns, rows))
    }

    /// Keep `statement`, of `sql`, which ran to its end, for the next call of
    /// it, reset: a statement left mid-way holds the version it read, which
    /// a checkpoint would wait for.
    fn keep(
        &mut self,
        sql: std::borrow::Cow<'_, str>,
        mut statement: turso_core::Statement,
        longest: Duration,
        mentions: Mentions,
        columns: Columns,
    ) {
        if self.statements.len() < MAX_STATEMENTS && statement.reset().is_ok() {
            // A key that came out of the map goes back in; only a new one is a copy.
            let prepared = Prepared {
                statement,
                longest,
                mentions,
                columns,
            };
            (self.statements).insert(sql.into_owned(), prepared);
        }
    }

    /// Run one statement of this crate's own, outside the transaction rules.
    async fn internal(&mut self, sql: &str) -> Result<Rows> {
        if !self.is_usable() {
            return Err(ENDED.into());
        }
        self.poisoned = true;
        let compiling = Instant::now();
        let mut statement = self.connection.as_ref().unwrap().prepare(sql)?;
        self.compiled(compiling.elapsed());
        let ran = query_engine(&self.io, &mut statement, usize::MAX).await;
        self.reprepared(&mut statement);
        let rows = match ran {
            Ok(rows) => rows,
            Err(Failure::Sql(error)) => return Err(error.into()),
            Err(Failure::Fatal(error)) => return Err(error),
        };
        self.poisoned = false;
        Ok(rows)
    }

    /// [`Self::internal`] for the commit's checkpoint, whose statement stays
    /// prepared for the next commit. A run that fails drops it.
    async fn run_checkpoint(&mut self) -> Result<Rows> {
        if !self.is_usable() {
            return Err(ENDED.into());
        }
        self.poisoned = true;
        let mut statement = match self.checkpoint_statement.take() {
            Some(statement) => statement,
            None => {
                let compiling = Instant::now();
                let statement = (self.connection.as_ref().unwrap())
                    .prepare("PRAGMA wal_checkpoint(TRUNCATE)")?;
                self.compiled(compiling.elapsed());
                Box::new(statement)
            }
        };
        let ran = query_engine(&self.io, &mut statement, usize::MAX).await;
        self.reprepared(&mut statement);
        let rows = match ran {
            Ok(rows) => rows,
            Err(Failure::Sql(error)) => return Err(error.into()),
            Err(Failure::Fatal(error)) => return Err(error),
        };
        self.poisoned = false;
        // Reset, as a kept statement is: one left mid-way holds the version it read.
        if statement.reset().is_ok() {
            self.checkpoint_statement = Some(statement);
        }
        Ok(rows)
    }

    /// Note a statement compiled, which took `elapsed`.
    fn compiled(&mut self, elapsed: Duration) {
        statements::count_compile(elapsed.as_nanos() as u64);
        #[cfg(test)]
        {
            self.tally.compiled += 1;
        }
    }

    /// Note the reprepares turso did for `statement`.
    fn reprepared(&mut self, statement: &mut turso_core::Statement) {
        let _n = statements::take_reprepares(statement);
        #[cfg(test)]
        {
            self.tally.reprepares += _n;
        }
    }

    /// The schema's id in the cache, once the engine knows its image: a read
    /// of every row of `sqlite_schema`, which takes as long as the schema is
    /// large.
    async fn share_schema(&mut self) -> Result<()> {
        let Some(cache) = self.cache.clone() else {
            return Ok(());
        };
        let rows = self
            .internal(
                "SELECT type, name, tbl_name, rootpage, sql FROM sqlite_schema ORDER BY rowid",
            )
            .await?;
        let cookie = self.internal("PRAGMA schema_version").await?;
        let page_size = self.connection.as_ref().unwrap().get_page_size().get();
        self.schema = cache.intern(&statements::image(&rows, &cookie, page_size));
        self.schema_read = true;
        Ok(())
    }

    /// A statement of `sql` (mentioning `mentions`) from the shared programs,
    /// if the engine may share and there is one.
    fn shared_statement(&mut self, sql: &str, mentions: Mentions) -> Option<turso_core::Statement> {
        let (cache, schema) = (self.cache.as_ref()?, self.schema?);
        if !mentions.shareable() {
            return None;
        }
        let found = cache.lookup(schema, sql, self.connection.as_ref()?);
        #[cfg(test)]
        if found.is_some() {
            self.tally.shared_hits += 1;
        }
        found
    }

    /// Share `statement`, compiled from `sql` and run once, with the other
    /// sessions.
    fn share(&self, sql: &str, statement: &turso_core::Statement, mentions: Mentions) {
        if let (Some(cache), Some(schema)) = (&self.cache, self.schema) {
            if mentions.shareable() {
                cache.publish(schema, sql, statement);
            }
        }
    }

    /// Merge the private engine WAL into the private database overlay and truncate it.
    ///
    /// This is a local barrier, **not** backend durability. Failure poisons the
    /// engine. A successful checkpoint may still be discarded without saving.
    async fn checkpoint(&mut self) -> Result<()> {
        let rows = self.run_checkpoint().await?;
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

/// What `future` returns, and the longest one poll of it held a thread.
async fn timed<F: Future>(future: F) -> (F::Output, Duration) {
    let mut future = std::pin::pin!(future);
    let mut longest = Duration::ZERO;
    let output = std::future::poll_fn(|cx| {
        let started = Instant::now();
        let polled = future.as_mut().poll(cx);
        longest = longest.max(started.elapsed());
        polled
    })
    .await;
    (output, longest)
}

/// `statement`'s column names: `kept`, shared, when they are the ones it was
/// kept with; else new ones (a schema changed since its last run, which a
/// statement prepares again for).
fn columns_of(statement: &turso_core::Statement, kept: Columns) -> Columns {
    let count = statement.num_columns();
    if count == kept.len() && (0..count).all(|i| *statement.get_column_name(i) == *kept[i]) {
        return kept;
    }
    (0..count)
        .map(|i| statement.get_column_name(i).into_owned())
        .collect()
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

/// The process's record of the statement texts whose runs are quick, which
/// says nothing of compiling them: a program that another session compiled,
/// for a known text, runs where it is polled if that is a host's thread
/// ([`Engine::run_statements`]); a text that must be compiled never does.
///
/// A text is known after one run whose longest poll held a thread, preparing
/// it not included, for less than [`QUICK_IN_PLACE`]. A run that held it for
/// that or longer makes a known text unknown, which it becomes again after [`RELEARN`] consecutive
/// quick runs that were the first of a session (the runs of a kept statement
/// tell the record only when slow). A record at its limit is cleared, not
/// frozen; a text longer than [`LONGEST_TEXT`] is never known.
mod quick_texts {
    use super::{Duration, QUICK_IN_PLACE};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// The most texts recorded.
    const MOST: usize = 8192;
    /// The longest text recorded, in bytes: the record holds at most
    /// `MOST * LONGEST_TEXT` bytes of texts.
    const LONGEST_TEXT: usize = 512;
    /// The quick runs a forgotten text needs to be known again.
    pub(super) const RELEARN: u8 = 8;

    /// For each text, the quick runs it still needs to be known: none for a
    /// known text.
    #[derive(Default)]
    pub(super) struct Record(HashMap<Box<str>, u8>);

    impl Record {
        pub(super) fn known(&self, sql: &str) -> bool {
            self.0.get(sql) == Some(&0)
        }

        /// A run of `sql` whose longest poll held a thread for `longest`.
        pub(super) fn observe(&mut self, sql: &str, longest: Duration) {
            if sql.len() > LONGEST_TEXT {
                return;
            }
            if longest >= QUICK_IN_PLACE {
                if let Some(needs) = self.0.get_mut(sql) {
                    *needs = RELEARN;
                }
            } else if let Some(needs) = self.0.get_mut(sql) {
                *needs = needs.saturating_sub(1);
            } else {
                if self.0.len() >= MOST {
                    self.0.clear();
                }
                self.0.insert(sql.into(), 0);
            }
        }

        #[cfg(test)]
        pub(super) fn len(&self) -> usize {
            self.0.len()
        }
    }

    static RECORD: Mutex<Option<Record>> = Mutex::new(None);

    fn record() -> std::sync::MutexGuard<'static, Option<Record>> {
        RECORD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn known(sql: &str) -> bool {
        record().as_ref().is_some_and(|record| record.known(sql))
    }

    pub(super) fn observe(sql: &str, longest: Duration) {
        record()
            .get_or_insert_with(Record::default)
            .observe(sql, longest);
    }
}
