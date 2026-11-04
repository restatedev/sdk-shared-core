//! Bridge between engine byte-addressed files and the state's file `db`.
//!
//! The engine sees two virtual files: [`DB_PATH`] and its private engine WAL.
//! The database reads through the state's file; its dirty pages and the
//! entire engine WAL live in `Private` until `KvIo::flush_into` copies the
//! pages into a [`Mutation`] of the state, where they stay, read ahead of the
//! file, until the state flushes that mutation. Full-page writes avoid a backing
//! read; partial writes merge with the visible page. Truncation records
//! explicit holes and clears a partial tail so regrowing a file never
//! exposes bytes from before.
//!
//! File methods enqueue work without calling KV. [`KvIo::drive`] polls backend
//! futures on the host executor. Adjacent reads overlap in bounded batches; writes,
//! truncation and sync serialize the queue into ordered phases. Within a read batch,
//! completion order may differ from submission order. A single multi-page file read
//! fetches its pages sequentially. This layer does not prefetch: the state does
//! (a small database whole at open; a frame of pages at a time), and
//! cross-session reuse comes from the optional
//! [`crate::state::PageCache`].
//!
//! Read buffers are staged privately until the read succeeds. `Completing` retires
//! in-flight accounting and aborts incomplete engine callbacks even if a dequeued
//! future is never polled. Ordinary read errors drain their batch; cancellation
//! aborts the dequeued batch. The engine treats either as fatal: its session
//! ends, with its private view.
//!
//! The host task runs both engine steps and completion callbacks, separated by
//! async backend waits. There is no cross-thread completion queue. `pending_count`
//! includes queued and executing requests, so a commit cannot race them.
//!
//! Opening the engine preloads the database header page asynchronously. The engine's
//! synchronous metadata helpers may then read resident header/overlay/WAL bytes
//! through `IO::step`. That method never polls KV futures: a nonresident read,
//! file mutation or wait with no queued work returns an error instead of blocking
//! or spinning. Normal engine I/O is driven by the asynchronous pump.
//!
//! File sync is only a private-overlay ordering barrier. After a complete
//! private checkpoint, `flush_into` copies the database's pages into a mutation,
//! which the state's flush makes durable. The engine WAL never becomes a
//! durable KV object.
use crate::state::{File as StateFile, Mutation, Page};
use bytes::BytesMut;
use futures::future::join_all;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    io,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use turso_core::io::{clock::DefaultClock, FileId, FileSyncType};
use turso_core::{
    Buffer, Clock, Completion, CompletionError, File, LimboError, MonotonicInstant, OpenFlags,
    WallClockInstant, IO,
};

/// Logical database-page and overlay-block size; encoded KV values may be smaller.
pub(super) const BLOCK_SIZE: usize = crate::state::PAGE_SIZE;
/// Engine-visible virtual database name, not a filesystem or KV key.
pub(super) const DB_PATH: &str = "object.db";
/// Engine-visible private WAL name; this file is never fetched from KV.
const WAL_PATH: &str = "object.db-wal";
/// The error of a second driver of the queue.
const DRIVING: &str = "the engine's I/O is being driven";
/// Maximum number of engine reads driven concurrently.
const MAX_READS_IN_FLIGHT: usize = 16;
/// The virtual files, by number: the index of [`DB`] and [`WAL`] here.
const FILES: [&str; 2] = [DB_PATH, WAL_PATH];
/// The database's file number.
const DB: usize = 0;
/// The private WAL's file number.
const WAL: usize = 1;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
/// One shared zero-filled page for holes and the unwritten WAL.
fn zero() -> Page {
    static ZERO: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];
    Page::from_static(&ZERO)
}
/// A mutable page: the same buffer when nothing else shares it, else a copy.
fn writable(page: Page) -> BytesMut {
    page.try_into_mut()
        .unwrap_or_else(|shared| BytesMut::from(&shared[..]))
}
/// Blocks of the first database page `first` starts: as many as its page
/// size takes.
fn header_blocks(first: &Page) -> u64 {
    page_size(first).div_ceil(BLOCK_SIZE).max(1) as u64
}
/// The database's page size, from its first page.
fn page_size(first: &Page) -> usize {
    match u16::from_be_bytes([first[16], first[17]]) {
        1 => 65536,
        size => usize::from(size),
    }
}
/// `page` (of [`BLOCK_SIZE`] bytes; the first page if `first`) with the
/// bytes no b-tree reads zeroed: its unallocated space, between the cell
/// pointers and the cells, and its freeblocks' bodies. The engine leaves there
/// whatever its buffer held before (a recycled buffer's old page), which
/// compresses badly; SQLite's `secure_delete` would zero them too. `None` for
/// a page that is not a b-tree page, or whose header does not add up.
fn tidied(page: &Page, first: bool) -> Option<Page> {
    let u16_at = |at: usize| usize::from(u16::from_be_bytes([page[at], page[at + 1]]));
    let at = if first { 100 } else { 0 };
    let header = match page[at] {
        2 | 5 => 12,
        10 | 13 => 8,
        _ => return None,
    };
    let content = match u16_at(at + 5) {
        0 => 65536,
        content => content,
    };
    let pointers = at + header + 2 * u16_at(at + 3);
    if pointers > content || content > page.len() {
        return None;
    }
    let mut zeroed = Vec::new();
    zeroed.push(pointers..content);
    // Freeblocks: ascending, within the cells, each at least 4 bytes.
    let mut next = u16_at(at + 1);
    while next != 0 {
        if next < content || next + 4 > page.len() {
            return None;
        }
        let size = u16_at(next + 2);
        if size < 4 || next + size > page.len() {
            return None;
        }
        zeroed.push(next + 4..next + size);
        let after = u16_at(next);
        if after != 0 && after < next + size {
            return None;
        }
        next = after;
    }
    if zeroed
        .iter()
        .all(|range| page[range.clone()].iter().all(|&b| b == 0))
    {
        return Some(page.clone());
    }
    let mut tidy = page.to_vec();
    for range in zeroed {
        tidy[range].fill(0);
    }
    Some(tidy.into())
}
/// Split `len` bytes at `pos` into per-block pieces:
/// `(block index, offset in block, offset in buffer, piece length)`.
fn blocks(pos: u64, len: usize) -> impl Iterator<Item = (u64, usize, usize, usize)> {
    let mut done = 0;
    std::iter::from_fn(move || {
        if done == len {
            return None;
        }
        let offset = pos + done as u64;
        let start = (offset % BLOCK_SIZE as u64) as usize;
        let piece = (BLOCK_SIZE - start).min(len - done);
        let item = (offset / BLOCK_SIZE as u64, start, done, piece);
        done += piece;
        Some(item)
    })
}

/// A piece of a block a read returns: the block, the piece's offset in it,
/// its offset in the engine's buffer, and its length.
type Piece = (Page, usize, usize, usize);

/// Copy `pieces` into the engine's `output`, zeros past them: the bytes read.
fn fill(output: &mut [u8], pieces: &[Piece]) -> usize {
    let mut count = 0;
    for (block, start, done, len) in pieces {
        output[*done..done + len].copy_from_slice(&block[*start..start + len]);
        count = done + len;
    }
    output[count..].fill(0);
    count
}

/// The session's private view: the engine's changes, indexed by `(file
/// number, logical block number)` ([`DB`] or [`WAL`]). A missing entry falls
/// through to the state's file for the database, and reads zeros for the
/// engine WAL; `Some(None)` is an explicit zero-filled hole that masks old
/// data after truncation. Sizes track the private view.
#[derive(Default)]
struct Private {
    sizes: [u64; 2],
    /// The blocks of the first database page as the state holds it, retained
    /// for synchronous engine metadata reads. Dirty and flushed entries,
    /// including explicit holes, always override these bytes.
    header: Vec<Page>,
    /// Pages are shared with readers; a partial write updates a page in place
    /// when nothing else holds it.
    dirty: BTreeMap<(usize, u64), Option<Page>>,
    /// Database pages copied into a mutation ([`KvIo::flush_into`]) that the state
    /// has not flushed yet: read after the dirty ones, ahead of the file,
    /// which does not hold them until then.
    flushed: BTreeMap<u64, Option<Page>>,
    #[cfg(any(test, feature = "__bench"))]
    written: [u64; 2],
}

/// One queued engine file operation; mutations are barriers between read batches.
enum Operation {
    Read {
        file: usize,
        pos: u64,
    },
    /// The engine's buffers, held as it holds them until the write
    /// completes, as an asynchronous kernel write would.
    Write {
        file: usize,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
    },
    Truncate {
        file: usize,
        len: u64,
    },
    Sync,
}
/// Owned operation and engine completion, retained until execution or shutdown.
struct Pending {
    op: Operation,
    completion: Completion,
}

/// Lifecycle flag and count of queued/executing completions.
#[derive(Default)]
struct Progress {
    inflight: usize,
    stopped: bool,
}

/// Cancellation guard for a dequeued request, created before its future is polled.
/// Retires accounting and aborts incomplete callbacks when the host cancels work.
struct Completing<'a> {
    io: &'a KvIo,
    completion: Completion,
}
impl Drop for Completing<'_> {
    fn drop(&mut self) {
        if !self.completion.finished() {
            self.completion.abort();
        }
        let mut progress = self.io.progress.lock().unwrap();
        progress.inflight -= 1;
    }
}

/// The engine's virtual files, over the state's file: its `IO`, and the
/// shared part of its `File`s ([`MountedIo`]).
///
/// turso's `IO` and `File` take `&self` and must be `Send + Sync`, so the
/// adapter keeps its state behind locks and atomics. None is contended: the
/// call holding the session is the one using the engine (see the ownership
/// model in [`crate::sql`]).
pub(super) struct KvIo {
    /// The state's file `db`; replaced when a warm engine continues on a
    /// new state, and `None` while the engine is parked.
    file: Mutex<Option<StateFile>>,
    /// See [`crate::sql::Limits::max_private_bytes`].
    max_private_bytes: AtomicUsize,
    private: Mutex<Private>,
    queue: Mutex<VecDeque<Pending>>,
    /// Held while the queue is drained ([`Self::drive`], [`IO::step`]),
    /// across the awaits of a drive, so its guard must be `Send`.
    driver: futures::lock::Mutex<()>,
    progress: Mutex<Progress>,
    /// A request or synchronous engine read failed: the engine's state after
    /// the error is unknown.
    failed: AtomicBool,
    /// A write went over `max_private_bytes`: the statement's failure is
    /// one a retry would repeat ([`Self::over_limit`]).
    over_limit: AtomicBool,
    /// The state the tests' `mount` opened, which their `publish` flushes.
    #[cfg(test)]
    state: Mutex<Option<crate::state::State>>,
    #[cfg(test)]
    fault: Mutex<Option<test_support::Fault>>,
}

impl KvIo {
    /// An adapter over `file`, the state's file `db`.
    pub(super) fn new(file: StateFile, max_private_bytes: usize) -> Arc<Self> {
        let private = Private {
            sizes: [file.len(), 0],
            ..Private::default()
        };
        Arc::new(Self {
            file: Mutex::new(Some(file)),
            max_private_bytes: AtomicUsize::new(max_private_bytes),
            private: Mutex::new(private),
            queue: Mutex::default(),
            driver: futures::lock::Mutex::new(()),
            progress: Mutex::default(),
            failed: AtomicBool::default(),
            over_limit: AtomicBool::default(),
            #[cfg(test)]
            state: Mutex::new(None),
            #[cfg(test)]
            fault: Mutex::new(None),
        })
    }

    /// Let go of the state while the engine is parked: it reads nothing
    /// until [`Self::resume`] gives it the next one.
    pub(crate) fn release(&self) {
        *self.file.lock().unwrap() = None;
    }

    fn file(&self) -> io::Result<StateFile> {
        let file = self.file.lock().unwrap().clone();
        file.ok_or_else(|| invalid("the engine is parked"))
    }

    /// No private changes, none in a mutation the state has not flushed, no
    /// engine WAL and no outstanding I/O: the engine's view equals the state
    /// it was opened or last flushed at.
    pub(crate) fn is_clean(&self) -> bool {
        if self.has_changes() || self.failed() {
            return false;
        }
        if !self.private.lock().unwrap().flushed.is_empty() {
            return false;
        }
        let progress = self.progress.lock().unwrap();
        progress.inflight == 0 && !progress.stopped
    }

    /// Continue a clean adapter on `file` of a newly opened state at the
    /// same flushed state, keeping the engine that uses it warm.
    pub(crate) async fn resume(&self, file: StateFile, max_private_bytes: usize) -> io::Result<()> {
        if !self.is_clean() {
            return Err(invalid("cannot resume an adapter with private changes"));
        }
        self.max_private_bytes
            .store(max_private_bytes, Ordering::SeqCst);
        *self.private.lock().unwrap() = Private {
            sizes: [file.len(), 0],
            ..Private::default()
        };
        *self.file.lock().unwrap() = Some(file);
        self.prepare_engine().await
    }

    /// Keep the database header page resident for the engine's synchronous
    /// metadata reads during connection setup and SQL preparation. Fetching it
    /// happens asynchronously before entering the engine; dirty pages take priority.
    pub(crate) async fn prepare_engine(&self) -> io::Result<()> {
        let first = self.block(DB, 0).await?;
        // The first page spans several blocks when the page size is larger.
        let blocks = header_blocks(&first);
        let rest = futures::future::try_join_all((1..blocks).map(|n| self.block(DB, n))).await?;
        let mut header = vec![first];
        header.extend(rest);
        self.private.lock().unwrap().header = header;
        Ok(())
    }

    /// Snapshot local database/WAL traffic counters; these are not durable KV bytes.
    #[cfg(any(test, feature = "__bench"))]
    pub(crate) fn stats(&self) -> crate::sql::IoStats {
        let private = self.private.lock().unwrap();
        crate::sql::IoStats {
            database_bytes_written: private.written[DB],
            private_wal_bytes_written: private.written[WAL],
            database_len: private.sizes[DB],
            private_wal_len: private.sizes[WAL],
        }
    }

    pub(crate) fn wal_len(&self) -> u64 {
        self.private.lock().unwrap().sizes[WAL]
    }

    fn enqueue(&self, op: Operation, completion: Completion) -> turso_core::Result<Completion> {
        let mut progress = self.progress.lock().unwrap();
        if progress.stopped {
            return Err(self.engine_error("adapter stopped"));
        }
        progress.inflight += 1;
        self.queue.lock().unwrap().push_back(Pending {
            op,
            completion: completion.clone(),
        });
        Ok(completion)
    }

    /// Whether any request or synchronous engine read failed.
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    fn fail(&self) {
        self.failed.store(true, Ordering::SeqCst);
    }

    /// Whether a write went over `max_private_bytes`: a statement whose I/O
    /// failed for it would fail so again, however often it is retried.
    pub(crate) fn over_limit(&self) -> bool {
        self.over_limit.load(Ordering::SeqCst)
    }

    /// An error for the engine, recorded as a failure of this adapter.
    fn engine_error(&self, message: impl Into<String>) -> LimboError {
        self.fail();
        LimboError::InternalError(message.into())
    }

    pub(crate) fn shutdown(&self) {
        {
            let mut progress = self.progress.lock().unwrap();
            progress.stopped = true;
        }
        let pending: Vec<_> = self.queue.lock().unwrap().drain(..).collect();
        for request in pending {
            drop(Completing {
                io: self,
                completion: request.completion,
            });
        }
    }

    /// Count unfinished queued and executing engine requests.
    pub(super) fn pending_count(&self) -> usize {
        self.progress.lock().unwrap().inflight
    }

    fn resident_block(&self, file: usize, index: u64) -> Option<Page> {
        let private = self.private.lock().unwrap();
        if let Some(page) = private.dirty.get(&(file, index)) {
            return Some(page.clone().unwrap_or_else(zero));
        }
        if file == WAL {
            return Some(zero());
        }
        if let Some(page) = private.flushed.get(&index) {
            return Some(page.clone().unwrap_or_else(zero));
        }
        private.header.get(index as usize).cloned()
    }

    async fn block(&self, file: usize, index: u64) -> io::Result<Page> {
        if let Some(page) = self.resident_block(file, index) {
            return Ok(page);
        }
        match self.file()?.read(index).await.map_err(io::Error::other)? {
            Some(page) if page.len() == BLOCK_SIZE => Ok(page),
            Some(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid block length",
            )),
            None => Ok(zero()), // sparse-file hole
        }
    }

    /// Synchronous engine helpers may only consume data already in this view.
    /// A cache miss never starts or polls a backend request from `IO::step`.
    fn read_resident(&self, file: usize, pos: u64, len: usize) -> Option<Vec<Piece>> {
        let count = self.readable(file, pos, len);
        (blocks(pos, count))
            .map(|(index, start, done, len)| {
                Some((self.resident_block(file, index)?, start, done, len))
            })
            .collect()
    }

    /// The pieces of blocks a read of `len` bytes at `pos` returns, fetched
    /// before the engine's buffer is written: a failed or cancelled read
    /// leaves it untouched.
    async fn read(&self, file: usize, pos: u64, len: usize) -> io::Result<Vec<Piece>> {
        let count = self.readable(file, pos, len);
        let mut pieces = Vec::with_capacity(count.div_ceil(BLOCK_SIZE) + 1);
        for (index, start, done, len) in blocks(pos, count) {
            pieces.push((self.block(file, index).await?, start, done, len));
        }
        Ok(pieces)
    }

    /// How many of `len` bytes at `pos` lie inside the file.
    fn readable(&self, file: usize, pos: u64, len: usize) -> usize {
        let size = self.private.lock().unwrap().sizes[file];
        size.saturating_sub(pos).min(len as u64) as usize
    }

    /// Write the engine's `buffers`, one after another, at `pos`.
    async fn write(&self, file: usize, pos: u64, buffers: &[Arc<Buffer>]) -> io::Result<usize> {
        let total: usize = buffers.iter().map(|buffer| buffer.len()).sum();
        // Count the blocks this write may add: one write can be a large batch.
        // Only the WAL's writes count: a checkpoint copies committed WAL
        // pages into the database's, which must not fail a flush.
        let limit = self.max_private_bytes.load(Ordering::SeqCst);
        let held = {
            let private = self.private.lock().unwrap();
            (private.dirty.len() + private.flushed.len()) * BLOCK_SIZE
        };
        if file == WAL && held + total + 2 * BLOCK_SIZE > limit {
            self.over_limit.store(true, Ordering::SeqCst);
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "private changes exceed Limits::max_private_bytes",
            ));
        }
        let end = pos
            .checked_add(total as u64)
            .ok_or_else(|| invalid("offset overflow"))?;
        let mut at = pos;
        for buffer in buffers {
            self.write_blocks(file, at, buffer.as_slice()).await?;
            at += buffer.len() as u64;
        }
        if total != 0 {
            let mut private = self.private.lock().unwrap();
            private.sizes[file] = private.sizes[file].max(end);
            #[cfg(any(test, feature = "__bench"))]
            {
                private.written[file] += total as u64;
            }
        }
        Ok(total)
    }

    /// Write `bytes` at `pos` into the blocks they cover.
    async fn write_blocks(&self, file: usize, pos: u64, bytes: &[u8]) -> io::Result<()> {
        for (index, start, done, len) in blocks(pos, bytes.len()) {
            let piece = &bytes[done..done + len];
            if len == BLOCK_SIZE {
                // A full block needs no read before overwrite.
                let page = Page::copy_from_slice(piece);
                self.private
                    .lock()
                    .unwrap()
                    .dirty
                    .insert((file, index), Some(page));
                continue;
            }
            // A partial write to a dirty page (most WAL frames) updates it in place
            // when nothing else shares the buffer.
            if let Some(slot @ Some(_)) = self.private.lock().unwrap().dirty.get_mut(&(file, index))
            {
                let mut page = writable(slot.take().unwrap());
                page[start..start + len].copy_from_slice(piece);
                *slot = Some(page.freeze());
                continue;
            }
            let mut page = writable(self.block(file, index).await?);
            page[start..start + len].copy_from_slice(piece);
            self.private
                .lock()
                .unwrap()
                .dirty
                .insert((file, index), Some(page.freeze()));
        }
        Ok(())
    }

    async fn truncate_file(&self, file: usize, len: u64) -> io::Result<()> {
        let old_size = self.private.lock().unwrap().sizes[file];
        if len < old_size {
            let tail = (len % BLOCK_SIZE as u64) as usize;
            if tail != 0 {
                let index = len / BLOCK_SIZE as u64;
                let mut page = writable(self.block(file, index).await?);
                page[tail..].fill(0);
                self.private
                    .lock()
                    .unwrap()
                    .dirty
                    .insert((file, index), Some(page.freeze()));
            }
            let first = len.div_ceil(BLOCK_SIZE as u64);
            let last = old_size.div_ceil(BLOCK_SIZE as u64);
            let mut private = self.private.lock().unwrap();
            for index in first..last {
                private.dirty.insert((file, index), None);
            }
        }
        self.private.lock().unwrap().sizes[file] = len;
        Ok(())
    }

    /// Drain queued engine I/O on the caller's async executor.
    ///
    /// Adjacent reads overlap in batches of at most 16 requests. Writes,
    /// truncation and sync are barriers: all preceding reads finish before a
    /// mutation, and subsequent reads observe its result. These are private
    /// file mutations, not KV puts. One call drives at a time, the one
    /// holding the session: a second driver is refused, not queued.
    ///
    /// A read error drains the already submitted batch before returning. Cancelling
    /// the pump aborts every dequeued completion; the session must be discarded.
    pub(super) async fn drive(&self) -> io::Result<usize> {
        let _driver = (self.driver.try_lock()).ok_or_else(|| io::Error::other(DRIVING))?;
        let mut completed = 0;
        loop {
            let pending = self.queue.lock().unwrap().pop_front();
            let Some(pending) = pending else {
                break;
            };
            if matches!(pending.op, Operation::Read { .. }) {
                let mut reads = vec![self.perform(pending)];
                {
                    let mut queue = self.queue.lock().unwrap();
                    while reads.len() < MAX_READS_IN_FLIGHT
                        && queue
                            .front()
                            .is_some_and(|p| matches!(p.op, Operation::Read { .. }))
                    {
                        reads.push(self.perform(queue.pop_front().unwrap()));
                    }
                }
                // `join_all` polls every read in each pass, so all of them are
                // issued at once; the whole batch is drained even after an error.
                let mut error = None;
                for result in join_all(reads).await {
                    match result {
                        Ok(n) => completed += n,
                        Err(e) => {
                            error.get_or_insert(e);
                        }
                    }
                }
                if let Some(error) = error {
                    return Err(error);
                }
            } else {
                completed += self.perform(pending).await?;
            }
        }
        Ok(completed)
    }

    // Construct the completion guard before constructing the future: even a
    // dequeued request whose future was never polled must retire on cancellation.
    fn perform(&self, pending: Pending) -> impl Future<Output = io::Result<usize>> + '_ {
        let Pending { op, completion } = pending;
        let finishing = Completing {
            io: self,
            completion: completion.clone(),
        };
        async move {
            let _finishing = finishing;
            if completion.finished() {
                return Ok(0);
            }
            #[cfg(any(test, feature = "__bench"))]
            crate::state::yield_now().await;
            #[cfg(test)]
            let fault_operation = self.fault_operation();
            #[cfg(test)]
            self.fault_point(fault_operation, false).await?;
            let result = match op {
                Operation::Read { file, pos } => {
                    let buffer = completion.as_read().buf();
                    let pieces = self.read(file, pos, buffer.len()).await;
                    pieces.map(|pieces| fill(buffer.as_mut_slice(), &pieces))
                }
                Operation::Write { file, pos, buffers } => self.write(file, pos, &buffers).await,
                Operation::Truncate { file, len } => self.truncate_file(file, len).await.map(|_| 0),
                Operation::Sync => Ok(0),
            };
            #[cfg(test)]
            self.fault_point(fault_operation, true).await?;
            match result {
                Ok(n) => {
                    let n = i32::try_from(n).map_err(|_| invalid("I/O too large"))?;
                    completion.complete(n);
                }
                Err(e) => {
                    self.fail();
                    completion.error(CompletionError::IOError(e.kind(), "KV I/O"));
                    return Err(e);
                }
            }
            Ok(1)
        }
    }

    /// Whether the engine changed the database since the last flush into a
    /// mutation, or holds engine WAL a checkpoint has not moved into it.
    pub(crate) fn has_changes(&self) -> bool {
        let private = self.private.lock().unwrap();
        private.sizes[WAL] != 0 || private.dirty.keys().any(|(file, _)| *file == DB)
    }

    /// Copy the database's private pages into the state's file `db`, in
    /// `mutation`, once no SQL transaction remains and the engine WAL is
    /// truncated. They stay in the private view, read ahead of the file,
    /// until the state flushes the mutation ([`Self::settle_flushed`]): the
    /// file does not hold them before. A flush that fails ends the engine,
    /// which keeps them. After an error the mutation may hold part of the
    /// copy: drop it.
    pub(crate) fn flush_into(&self, mutation: &mut Mutation) -> io::Result<()> {
        if self.pending_count() != 0 {
            return Err(invalid("cannot flush pages with pending I/O"));
        }
        self.file()?;
        let mut private = self.private.lock().unwrap();
        if private.sizes[WAL] != 0 {
            return Err(invalid("the engine WAL is not checkpointed"));
        }
        let (database, rest): (BTreeMap<_, _>, BTreeMap<_, _>) =
            (std::mem::take(&mut private.dirty).into_iter())
                .partition(|((file, _), _)| *file == DB);
        private.dirty = rest;
        // Pages a block each (the default page size): tidied, unless the
        // database has pointer-map pages (`auto_vacuum`, whose header names
        // its largest root page), which `tidied` may take for b-tree pages.
        // The first page as this mutation leaves it: changed now, or by a
        // pending mutation, or as the file holds it.
        let first = (database.get(&(DB, 0)).cloned().flatten())
            .or_else(|| private.flushed.get(&0).cloned().flatten())
            .or_else(|| private.header.first().cloned());
        let tidy =
            first.is_some_and(|first| page_size(&first) == BLOCK_SIZE && first[52..56] == [0; 4]);
        for ((_, block), page) in database {
            let page = match page {
                Some(page) if tidy => Some(tidied(&page, block == 0).unwrap_or(page)),
                page => page,
            };
            private.flushed.insert(block, page);
        }
        for (block, page) in &private.flushed {
            mutation
                .write(*block, page.clone())
                .map_err(io::Error::other)?;
        }
        mutation
            .set_len(private.sizes[DB])
            .map_err(io::Error::other)
    }

    /// The state flushed the mutation the pages were copied into: the file holds
    /// them, and the engine reads them from it.
    pub(crate) fn settle_flushed(&self) {
        let mut private = self.private.lock().unwrap();
        let flushed = std::mem::take(&mut private.flushed);
        // A new first page: its blocks are the header the engine reads
        // synchronously, as many as its page size takes.
        if let Some(first) = flushed.get(&0) {
            let first = first.clone().unwrap_or_else(zero);
            let old = std::mem::take(&mut private.header);
            private.header = (0..header_blocks(&first))
                .map(|index| match flushed.get(&index) {
                    Some(block) => block.clone().unwrap_or_else(zero),
                    None => old.get(index as usize).cloned().unwrap_or_else(zero),
                })
                .collect();
        }
    }
}

impl Clock for KvIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for KvIo {
    fn open_file(&self, _: &str, _: OpenFlags, _: bool) -> turso_core::Result<Arc<dyn File>> {
        Err(self.engine_error("use MountedIo: file handles need the shared adapter"))
    }
    fn remove_file(&self, _: &str) -> turso_core::Result<()> {
        Err(self.engine_error("dynamic files are out of scope"))
    }
    fn step(&self) -> turso_core::Result<()> {
        // Synchronous engine helpers may read resident metadata, but they must
        // never wait for async KV. Unknown blocking paths fail explicitly.
        let _driver = (self.driver.try_lock()).ok_or_else(|| self.engine_error(DRIVING))?;
        let mut progressed = false;
        loop {
            let pending = self.queue.lock().unwrap().pop_front();
            let Some(Pending { op, completion }) = pending else {
                return if progressed {
                    Ok(())
                } else {
                    Err(self.engine_error("engine attempted a synchronous wait without queued I/O"))
                };
            };
            let _finishing = Completing {
                io: self,
                completion: completion.clone(),
            };
            progressed = true;
            if completion.finished() {
                continue;
            }
            match op {
                Operation::Read { file, pos } => {
                    let buffer = completion.as_read().buf();
                    let Some(pieces) = self.read_resident(file, pos, buffer.len()) else {
                        return Err(self.engine_error(
                            "engine attempted synchronous I/O for a nonresident page",
                        ));
                    };
                    let n = fill(buffer.as_mut_slice(), &pieces);
                    completion.complete(
                        i32::try_from(n).map_err(|_| self.engine_error("I/O too large"))?,
                    );
                }
                Operation::Sync => completion.complete(0),
                _ => return Err(self.engine_error("engine attempted a synchronous file mutation")),
            }
        }
    }
    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        Ok(FileId::from_path_hash(path))
    }
}

/// The engine's `IO`: opens [`DB_PATH`] and the private WAL as handles on the
/// one shared [`KvIo`], which owns queue ordering, private bytes and file
/// access. A separate type, since a handle needs the adapter's `Arc`.
pub(super) struct MountedIo(pub Arc<KvIo>);
impl Clock for MountedIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.0.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.0.current_time_wall_clock()
    }
}
impl IO for MountedIo {
    fn open_file(&self, path: &str, _: OpenFlags, _: bool) -> turso_core::Result<Arc<dyn File>> {
        let file = FILES
            .iter()
            .position(|name| *name == path)
            .ok_or_else(|| self.0.engine_error(format!("unsupported file: {path}")))?;
        Ok(Arc::new(KvFile {
            io: self.0.clone(),
            file,
        }))
    }
    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.0.remove_file(path)
    }
    fn step(&self) -> turso_core::Result<()> {
        self.0.step()
    }
    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        self.0.file_id(path)
    }
}

/// A handle on a virtual file ([`DB`] or [`WAL`]) of the shared adapter.
struct KvFile {
    io: Arc<KvIo>,
    file: usize,
}
impl File for KvFile {
    // One owner/connection per mounted store is a host-enforced precondition.
    fn lock_file(&self, _: bool) -> turso_core::Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        Ok(())
    }
    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.io.enqueue(
            Operation::Read {
                file: self.file,
                pos,
            },
            c,
        )
    }
    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.io.enqueue(
            Operation::Write {
                file: self.file,
                pos,
                buffers: vec![buffer],
            },
            c,
        )
    }
    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        // One queued operation/completion: do not use the upstream default's
        // child-completion aggregation, which complicates error propagation.
        self.io.enqueue(
            Operation::Write {
                file: self.file,
                pos,
                buffers,
            },
            c,
        )
    }
    fn sync(&self, c: Completion, _: FileSyncType) -> turso_core::Result<Completion> {
        self.io.enqueue(Operation::Sync, c)
    }
    fn size(&self) -> turso_core::Result<u64> {
        Ok(self.io.private.lock().unwrap().sizes[self.file])
    }
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.io.enqueue(
            Operation::Truncate {
                file: self.file,
                len,
            },
            c,
        )
    }
}

/// `bytes` as one engine buffer, for tests that write as the engine does.
#[cfg(test)]
fn buffers(bytes: &[u8]) -> Vec<Arc<Buffer>> {
    vec![Arc::new(Buffer::new(bytes.to_vec()))]
}

#[cfg(test)]
#[path = "tests/unit/io.rs"]
mod completion_tests;

#[cfg(test)]
#[path = "tests/unit/io_support.rs"]
mod test_support;

#[cfg(test)]
#[path = "tests/unit/parallel_reads.rs"]
mod parallel_reads;
