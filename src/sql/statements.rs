//! A process-wide cache of compiled SQL statements.
//!
//! A session keeps the statements it compiled ([`super::Engine::statements`])
//! only until it ends, so each visit to an object compiled the same few dozen
//! texts again. The objects of a service have one schema, so the compiled
//! programs are shared here, by the schema and the exact SQL text.
//!
//! # Safety of sharing
//!
//! A compiled program (`turso_core::PreparedProgram`) holds no connection and
//! no pager, but it has the root page of every b-tree it opens baked into its
//! instructions, and the schema cookie it was compiled at. Two databases with
//! the same schema text and different root pages (their tables created in
//! another order) must never share a program. A schema is therefore the
//! *image* of its database: every row of `sqlite_schema` (type, name,
//! tbl_name, rootpage, sql), the schema cookie and the page size, encoded
//! without ambiguity. The cache gives each distinct image an id, comparing
//! images byte for byte, and keys programs by that id and the exact text: no
//! hash decides that two schemas are one. An id is never reused, so a session
//! that holds the id of an image the cache forgot finds nothing, and
//! publishes nothing.
//!
//! A session has an id after it opened, and after any DDL has committed;
//! while its SQL may have changed the schema, or a setting of the connection
//! (a pragma, an attached database, the temp schema), it has none, and
//! neither looks up nor publishes.
//!
//! A program also remembers the database and the connection settings it was
//! compiled for, and recompiles itself silently when run on another. A hit
//! therefore stamps a copy of the program with the new connection's context
//! ([`PrepareContext::from_connection`], public) before binding it.
//!
//! # Bounds
//!
//! The cache keeps at most [`Limits::programs`] programs, of an estimated
//! [`Limits::bytes`] together, and the images of at most [`Limits::schemas`]
//! schemas, of at most [`Limits::image`] bytes each and [`Limits::image_bytes`]
//! together; the least recently used goes first, and a schema takes its
//! programs with it. The size of a program is estimated as the size of its
//! instructions (each of a fixed size) and of its text, and a fixed
//! [`OVERHEAD`] for the rest; one of more than [`Limits::program`] is not kept,
//! nor one whose text is longer than [`Limits::text`]. A schema whose image is
//! larger than one allows is not shared. A session that holds a statement of a
//! program the cache dropped keeps running it: the statement holds the program.
//!
//! # What waits for what
//!
//! One mutex guards the cache, held only for a lookup in a map, an insert, and
//! the eviction of the least recently used (a scan of at most the bound's
//! programs): the thread of a call polled in place may wait for those, not for
//! hashing or comparing a schema's image, or compiling, which happen outside
//! it.

use std::{
    collections::HashMap,
    hash::BuildHasher,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        Arc, Mutex, MutexGuard, OnceLock,
    },
};
use turso_core::{
    Connection, Numeric, PrepareContext, PreparedProgram, Program, QueryMode, Statement,
    StatementStatusCounter, Value,
};

/// What a cache may hold.
#[derive(Clone, Copy, Debug)]
pub(super) struct Limits {
    /// Programs, in all.
    pub programs: usize,
    /// The estimated bytes of all the programs.
    pub bytes: usize,
    /// The largest program kept, estimated in bytes.
    pub program: usize,
    /// The longest text of a program kept, in bytes.
    pub text: usize,
    /// Schemas.
    pub schemas: usize,
    /// The largest image of a schema shared, in bytes.
    pub image: usize,
    /// Bytes of the images of all the schemas.
    pub image_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            programs: 1024,
            bytes: 16 << 20,
            program: 256 << 10,
            text: 4096,
            schemas: 256,
            image: 1 << 20,
            image_bytes: 8 << 20,
        }
    }
}

/// What identifies a schema in a [`Cache`]: that of one image.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct SchemaId(u64);

struct Entry {
    program: Arc<PreparedProgram>,
    /// Its estimated size ([`estimate`]).
    size: usize,
    used: u64,
}

/// Bytes a program takes besides its instructions and text, estimated: its
/// table references, result columns and the rest.
pub(super) const OVERHEAD: usize = 4096;

/// The estimated size of `program`, compiled from `sql`.
fn estimate(program: &PreparedProgram, sql: &str) -> usize {
    std::mem::size_of_val(&program.insns[..]) + sql.len() + OVERHEAD
}

struct Schema {
    image: Arc<[u8]>,
    /// The hash of the image, its key in [`State::by_hash`].
    hash: u64,
    used: u64,
    programs: HashMap<String, Entry>,
}

#[derive(Default)]
struct State {
    /// The ids of the images held, by a hash of the image: equal images have
    /// equal hashes, and the images are compared to tell.
    by_hash: HashMap<u64, Vec<SchemaId>>,
    schemas: HashMap<SchemaId, Schema>,
    /// The id of the next image: ids are never reused.
    next_id: u64,
    image_bytes: usize,
    programs: usize,
    /// The estimated bytes of the programs.
    bytes: usize,
    clock: u64,
}

/// A cache of programs by schema and text.
pub(super) struct Cache {
    limits: Limits,
    /// Hashes images, outside the lock; a key for each cache.
    hasher: std::hash::RandomState,
    state: Mutex<State>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl Default for Cache {
    fn default() -> Self {
        Self::with_limits(Limits::default())
    }
}

impl Cache {
    pub(super) fn with_limits(limits: Limits) -> Self {
        Self {
            limits,
            hasher: std::hash::RandomState::new(),
            state: Mutex::default(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The id of the schema whose image is `image`, a new one if the cache
    /// holds none equal to it; `None` if it is too large to share. Hashing
    /// the image and comparing it with those of the same hash take no lock
    /// of the cache.
    pub(super) fn intern(&self, image: &[u8]) -> Option<SchemaId> {
        if image.len() > self.limits.image {
            return None;
        }
        let hash = self.hasher.hash_one(image);
        let image: Arc<[u8]> = image.into();
        loop {
            let candidates: Vec<(SchemaId, Arc<[u8]>)> = {
                let state = self.state();
                let ids = state.by_hash.get(&hash).map(Vec::as_slice).unwrap_or(&[]);
                let held = |&id| Some((id, state.schemas.get(&id)?.image.clone()));
                ids.iter().filter_map(held).collect()
            };
            let equal = candidates.iter().find(|(_, held)| **held == *image);
            let mut state = self.state();
            state.clock += 1;
            let clock = state.clock;
            if let Some(&(id, _)) = equal {
                // Still there, or forgotten since it was found: look again.
                match state.schemas.get_mut(&id) {
                    Some(schema) => {
                        schema.used = clock;
                        return Some(id);
                    }
                    None => continue,
                }
            }
            // Another session may have added one of this hash since: look
            // again, as it may be equal.
            let now = state.by_hash.get(&hash).map_or(0, Vec::len);
            if now != candidates.len() {
                continue;
            }
            state.next_id += 1;
            let id = SchemaId(state.next_id);
            state.by_hash.entry(hash).or_default().push(id);
            state.image_bytes += image.len();
            let schema = Schema {
                image,
                hash,
                used: clock,
                programs: HashMap::new(),
            };
            state.schemas.insert(id, schema);
            // The least recently used goes, never the one just made: it is the
            // most recent.
            while state.schemas.len() > self.limits.schemas
                || state.image_bytes > self.limits.image_bytes
            {
                let Some(oldest) = (state.schemas.iter())
                    .min_by_key(|(_, schema)| schema.used)
                    .map(|(&id, _)| id)
                else {
                    break;
                };
                self.forget(&mut state, oldest);
            }
            return Some(id);
        }
    }

    /// Drop a schema with its programs.
    fn forget(&self, state: &mut State, id: SchemaId) {
        if let Some(schema) = state.schemas.remove(&id) {
            if let Some(ids) = state.by_hash.get_mut(&schema.hash) {
                ids.retain(|&held| held != id);
                if ids.is_empty() {
                    state.by_hash.remove(&schema.hash);
                }
            }
            state.image_bytes -= schema.image.len();
            state.programs -= schema.programs.len();
            state.bytes -= schema
                .programs
                .values()
                .map(|entry| entry.size)
                .sum::<usize>();
            self.evictions
                .fetch_add(schema.programs.len() as u64, Relaxed);
        }
    }

    /// A statement of `sql` for `connection`, built from the program of the
    /// schema `id`, if there is one.
    pub(super) fn lookup(
        &self,
        id: SchemaId,
        sql: &str,
        connection: &Arc<Connection>,
    ) -> Option<Statement> {
        let found = {
            let mut state = self.state();
            state.clock += 1;
            let clock = state.clock;
            let schema = state.schemas.get_mut(&id);
            let entry = schema.and_then(|schema| schema.programs.get_mut(sql));
            entry.map(|entry| {
                entry.used = clock;
                entry.program.clone()
            })
        };
        let Some(program) = found else {
            self.misses.fetch_add(1, Relaxed);
            return None;
        };
        self.hits.fetch_add(1, Relaxed);
        Some(instantiate(&program, connection))
    }

    /// Publish the program `statement` runs, of `sql`, under the schema `id`,
    /// if the cache still holds it.
    pub(super) fn publish(&self, id: SchemaId, sql: &str, statement: &Statement) {
        if statement.get_query_mode() != QueryMode::Normal || sql.len() > self.limits.text {
            return;
        }
        let program = statement.get_program().prepared().clone();
        let size = estimate(&program, sql);
        if size > self.limits.program {
            return;
        }
        let mut state = self.state();
        state.clock += 1;
        let clock = state.clock;
        let Some(schema) = state.schemas.get_mut(&id) else {
            return;
        };
        if let Some(entry) = schema.programs.get_mut(sql) {
            entry.used = clock;
            return;
        }
        let entry = Entry {
            program,
            size,
            used: clock,
        };
        schema.programs.insert(sql.to_owned(), entry);
        state.programs += 1;
        state.bytes += size;
        while state.programs > self.limits.programs || state.bytes > self.limits.bytes {
            let oldest = (state.schemas.iter())
                .flat_map(|(&id, schema)| {
                    (schema.programs.iter()).map(move |(sql, entry)| (entry.used, id, sql))
                })
                .min_by_key(|(used, ..)| *used)
                .map(|(_, id, sql)| (id, sql.clone()));
            let Some((id, sql)) = oldest else { break };
            let removed =
                (state.schemas.get_mut(&id)).and_then(|schema| schema.programs.remove(&sql));
            state.bytes -= removed.map_or(0, |entry| entry.size);
            state.programs -= 1;
            self.evictions.fetch_add(1, Relaxed);
        }
    }

    /// What the cache holds and has done.
    fn stats(&self) -> CacheStats {
        let state = self.state();
        CacheStats {
            hits: self.hits.load(Relaxed),
            misses: self.misses.load(Relaxed),
            evictions: self.evictions.load(Relaxed),
            programs: state.programs as u64,
            schemas: state.schemas.len() as u64,
        }
    }

    /// Drop the schema `id`, as the bounds do the least recently used.
    #[cfg(test)]
    pub(super) fn evict(&self, id: SchemaId) {
        self.forget(&mut self.state(), id);
    }

    /// The program of `sql` under `id`.
    #[cfg(test)]
    pub(super) fn program(&self, id: SchemaId, sql: &str) -> Option<Arc<PreparedProgram>> {
        let state = self.state();
        Some(state.schemas.get(&id)?.programs.get(sql)?.program.clone())
    }

    #[cfg(test)]
    pub(super) fn schemas(&self) -> usize {
        self.state().schemas.len()
    }

    #[cfg(test)]
    pub(super) fn programs(&self) -> usize {
        self.state().programs
    }

    #[cfg(test)]
    pub(super) fn program_bytes(&self) -> usize {
        self.state().bytes
    }

    #[cfg(test)]
    pub(super) fn image_bytes(&self) -> usize {
        self.state().image_bytes
    }
}

/// The cache every session of the process shares.
pub(super) fn process() -> Arc<Cache> {
    static CACHE: OnceLock<Arc<Cache>> = OnceLock::new();
    CACHE.get_or_init(Default::default).clone()
}

/// The image of a schema: its rows of `sqlite_schema`, its cookie (a row of
/// one value) and its page size. Each value is a tag, its length and its
/// bytes, so that no two different schemas have one image.
pub(super) fn image(rows: &[Vec<Value>], cookie: &[Vec<Value>], page_size: u32) -> Vec<u8> {
    fn field(image: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
        image.push(tag);
        image.extend((bytes.len() as u64).to_le_bytes());
        image.extend(bytes);
    }
    let mut image = Vec::new();
    for rows in [rows, cookie] {
        image.extend((rows.len() as u64).to_le_bytes());
        for row in rows {
            image.extend((row.len() as u64).to_le_bytes());
            for value in row {
                match value {
                    Value::Null => field(&mut image, 0, &[]),
                    Value::Numeric(Numeric::Integer(n)) => field(&mut image, 1, &n.to_le_bytes()),
                    Value::Numeric(Numeric::Float(_)) => {
                        field(&mut image, 2, &value.as_float().to_bits().to_le_bytes())
                    }
                    Value::Text(text) => field(&mut image, 3, text.as_str().as_bytes()),
                    Value::Blob(_) => field(&mut image, 4, value.to_blob().unwrap_or_default()),
                }
            }
        }
    }
    image.extend(page_size.to_le_bytes());
    image
}

static COMPILED: AtomicU64 = AtomicU64::new(0);
static COMPILE_NS: AtomicU64 = AtomicU64::new(0);
static REPREPARES: AtomicU64 = AtomicU64::new(0);

pub(super) fn count_compile(nanos: u64) {
    COMPILED.fetch_add(1, Relaxed);
    COMPILE_NS.fetch_add(nanos, Relaxed);
}

/// Count the reprepares turso did for `statement` since the last call, and
/// return them.
pub(super) fn take_reprepares(statement: &mut Statement) -> u64 {
    let n = statement.stmt_status(StatementStatusCounter::Reprepare);
    if n > 0 {
        statement.reset_stmt_status(StatementStatusCounter::Reprepare);
        REPREPARES.fetch_add(n, Relaxed);
    }
    n
}

/// What one cache holds and has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CacheStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    programs: u64,
    schemas: u64,
}

/// Cumulative, process-wide counters of the statements the sessions compile
/// and of the cache they share ([`statement_stats`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatementStats {
    /// Statements compiled from SQL text (parse and translate), by every
    /// session, whether or not they could be shared.
    pub compiled: u64,
    /// Nanoseconds spent compiling them.
    pub compile_ns: u64,
    /// Statements built from a shared program, with no compile.
    pub shared_hits: u64,
    /// Lookups the cache could not answer (the statement was then compiled
    /// and published).
    pub shared_misses: u64,
    /// Times turso recompiled a statement of its own accord (a program run
    /// against another database or settings, or a schema cookie that moved).
    pub reprepares: u64,
    /// Programs evicted from the cache to stay within its bounds.
    pub shared_evictions: u64,
    /// Programs in the cache now.
    pub shared_entries: u64,
}

/// The process-wide statement counters, now (differences give an interval).
pub fn statement_stats() -> StatementStats {
    let cache = process().stats();
    StatementStats {
        compiled: COMPILED.load(Relaxed),
        compile_ns: COMPILE_NS.load(Relaxed),
        shared_hits: cache.hits,
        shared_misses: cache.misses,
        reprepares: REPREPARES.load(Relaxed),
        shared_evictions: cache.evictions,
        shared_entries: cache.programs,
    }
}

/// Bind `shared` to `connection` as a statement that turso takes as prepared
/// for it.
pub(super) fn instantiate(
    shared: &Arc<PreparedProgram>,
    connection: &Arc<Connection>,
) -> Statement {
    let program = if shared.is_compatible_with(connection) {
        shared.clone()
    } else {
        let mut own = PreparedProgram::clone(shared);
        own.prepare_context = PrepareContext::from_connection(connection);
        // Not shared between connections: a flag of the copy.
        own.needs_stmt_subtransactions = Arc::new(AtomicBool::new(
            shared.needs_stmt_subtransactions.load(Relaxed),
        ));
        Arc::new(own)
    };
    Statement::new(
        Program::from_prepared(program, connection.clone()),
        connection.get_pager(),
        QueryMode::Normal,
        0,
    )
}
