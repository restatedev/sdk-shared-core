//! A seeded random campaign over the public API: mutations of changes to several
//! files and records, flushed with small frames and
//! random checkpoint thresholds so that checkpoints, patches, compactions and
//! garbage collection all happen, and random
//! faults, cancellations, crashes and reopens by owners with and without a
//! cache, every request checked by the protocol monitor. Even seeds keep
//! checkpoints in the store; odd ones in an S3 that fails as S3 does, its
//! writes lost, done without a response, or landing late. After each reopen
//! the state reads as the model: every acknowledged flush, and an
//! unacknowledged one wholly or not at all. Readers opened beside the owner
//! read the state they opened while it flushes, and at the end only what
//! the head needs is stored.
//!
//! ```sh
//! cargo test --release state::tests::campaign   # a few seeds
//! STATE_SEEDS=1000 cargo test --release state::tests::campaign
//! STATE_SEED=17 cargo test state::tests::campaign  # one, as a failure prints it
//! ```

use super::support::*;
use crate::state::{
    blobs::{KV_PREFIX, MANIFEST_PREFIX},
    kv::KvStore,
    simulation::*,
    FlushReport, Mutation, PageCache, Result, StateOptions, HEAD_KEY, TAIL_PREFIX,
};
use futures::task::noop_waker_ref;
use std::future::Future;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc, OnceLock,
    },
    task::{Context, Poll},
};

/// Seeds `cargo test` runs without `STATE_SEEDS`.
const SEEDS: u64 = 12;
/// Flushes per seed.
const STEPS: usize = 40;

type Store = MonitorKv<FaultKv>;

/// The seeds to run: `STATE_SEED`, or the first `STATE_SEEDS`.
fn seeds() -> Vec<u64> {
    let var = |name| {
        std::env::var(name)
            .ok()
            .map(|v: String| v.parse().expect(name))
    };
    match (var("STATE_SEED"), var("STATE_SEEDS")) {
        (Some(seed), _) => vec![seed],
        (None, count) => (0..count.unwrap_or(SEEDS)).collect(),
    }
}

#[test]
fn random_faults_never_lose_an_acknowledged_flush_or_split_one() {
    let mut totals = Totals::default();
    for seed in seeds() {
        let run = std::panic::catch_unwind(|| futures::executor::block_on(run(seed)));
        let rerun = format!("STATE_SEED={seed} cargo test state::tests::campaign -- --nocapture");
        match run {
            Ok(Ok(run)) => totals.add(&run),
            Ok(Err(error)) => panic!("seed {seed}: {error}\nrerun: {rerun}"),
            Err(panic) => {
                eprintln!("seed {seed} panicked; rerun: {rerun}");
                std::panic::resume_unwind(panic)
            }
        }
    }
    println!("{totals:?}");
    if seeds().len() as u64 >= SEEDS {
        let t = &totals;
        assert!(
            t.tail_entries > 0
                && t.checkpoints > 0
                && t.compacted > 0
                && t.collected > 0
                && t.frames > 0
                && t.patched > 0
                && t.reads_beside > 0,
            "{t:?}"
        );
        assert!(
            t.s3_faults > 0 && t.landed_late > 0 && t.promoted > 0,
            "{t:?}"
        );
        assert!(
            t.landed > 0 && t.lost > 0 && t.crashes > 0 && t.cancelled > 0,
            "{t:?}"
        );
    }
}

/// What a run did, to check that the campaign covers what it should.
#[derive(Debug, Default)]
struct Totals {
    flushes: usize,
    /// Unacknowledged flushes found whole after a reopen, and found absent.
    landed: usize,
    lost: usize,
    crashes: usize,
    cancelled: usize,
    handovers: usize,
    requests_checked: usize,
    /// Tail entries put; checkpoints adopted, packs they compacted, and
    /// packs deleted as garbage; frames checkpoints rewrote, and patched.
    tail_entries: u64,
    checkpoints: usize,
    compacted: usize,
    /// Checkpoints that moved L0 into L1.
    promoted: usize,
    collected: usize,
    frames: usize,
    patched: usize,
    /// Reads by readers beside the owner, after it flushed.
    reads_beside: usize,
    /// Garbage collections by the owner or a worker beside it, faulty ones
    /// included.
    collections: usize,
    /// S3 requests failed, and writes that landed late.
    s3_faults: u64,
    landed_late: u64,
}

impl Totals {
    fn add(&mut self, run: &Totals) {
        self.flushes += run.flushes;
        self.landed += run.landed;
        self.lost += run.lost;
        self.crashes += run.crashes;
        self.cancelled += run.cancelled;
        self.handovers += run.handovers;
        self.requests_checked += run.requests_checked;
        self.tail_entries += run.tail_entries;
        self.checkpoints += run.checkpoints;
        self.compacted += run.compacted;
        self.promoted += run.promoted;
        self.collected += run.collected;
        self.frames += run.frames;
        self.patched += run.patched;
        self.reads_beside += run.reads_beside;
        self.collections += run.collections;
        self.s3_faults += run.s3_faults;
        self.landed_late += run.landed_late;
    }
}

/// A cache too small to keep much, for some owners.
fn small_cache() -> &'static PageCache {
    static CACHE: OnceLock<PageCache> = OnceLock::new();
    CACHE.get_or_init(|| open_cache(cache_config(64 << 10, 1 << 20)))
}

/// A new owner process's cache: its first look at the namespace.
fn new_owner(rng: &mut Rng) -> Namespace {
    static KEYS: AtomicU64 = AtomicU64::new(0);
    match rng.percent(30) {
        true => Namespace::new(
            small_cache(),
            &format!("campaign-{}", KEYS.fetch_add(1, Relaxed)),
        ),
        false => fresh_namespace(),
    }
}

/// One seed's world: the store, the owners, and what the state may hold.
struct World {
    rng: Rng,
    store: Arc<Store>,
    memory: MemoryKv,
    /// Two owner processes, each with its own cache.
    caches: [Namespace; 2],
    /// The owner of the namespace: a process, or one without a cache.
    owner: Option<usize>,
    /// The state as of the last acknowledged flush or reopen.
    model: Model,
    /// The states a crash may go back to: the durable one, then each one
    /// read or flushed since the last sync.
    since_sync: Vec<Model>,
    /// Where checkpoints are, if not in the store: an S3, with faults.
    packs: Option<MemoryBlobs>,
    faults: Option<BlobFaults>,
    /// Flushes that may have named a new checkpoint, so far.
    adopted: usize,
    /// Readers beside the owner, what they opened, and `adopted` then.
    readers: Vec<(Session, Model, usize)>,
    totals: Totals,
}

impl World {
    /// Random options: they never change the stored format.
    fn options(&mut self) -> Setup {
        let cache = self.owner.map(|owner| self.caches[owner].clone());
        let rng = &mut self.rng;
        Setup {
            options: StateOptions {
                // Kept by the namespace's first head.
                frame_pages: rng.pick(&[1, 2, 4, 16]),
                inline_record_bytes: rng.pick(&[0, 200, 1024, 8192]),
                prefetch_pages: rng.pick(&[0, 4, 16]),
                checkpoint_entries: rng.pick(&[0, 1, 2, 3, 8]),
                checkpoint_bytes: rng.pick(&[0, 0, 2048, 1 << 20]),
                // With S3: every pack there, promotions at every few
                // checkpoints, now and then, or never.
                l0_bytes: rng.pick(&[0, 0, 2048, 64 << 10, u64::MAX]),
            },
            cache,
            packs: self.packs.clone(),
        }
    }

    /// S3 fails at random while the owner flushes.
    fn s3_faults(&self, on: bool) {
        if let Some(packs) = &self.packs {
            packs.faults(self.faults.filter(|_| on));
        }
    }

    fn violations(&self) -> std::result::Result<(), String> {
        match self.store.violations() {
            violations if violations.is_empty() => Ok(()),
            violations => Err(format!("protocol violations: {violations:#?}")),
        }
    }

    /// The writes since the last sync become durable, or a crash keeps a
    /// prefix of them: an owner's requests settle before another owns the
    /// namespace.
    async fn settle(&mut self) -> Result<()> {
        if self.rng.percent(50) {
            self.crash();
        } else {
            self.memory.sync().await?;
        }
        Ok(())
    }

    /// A crash keeps a random prefix of the writes since the last sync.
    fn crash(&mut self) {
        let writes = self.memory.unsynced().len() as u64;
        let kept = self.rng.below(writes + 1) as usize;
        trace(|| format!("crash keeping {kept} of {writes} writes"));
        self.memory.crash_keeping(kept);
        self.totals.crashes += 1;
    }

    /// Open a new session, sometimes by another owner or a restarted one,
    /// sometimes with a fault while it reads, and find which of `candidates`
    /// it holds.
    async fn reopen(&mut self, candidates: &[Model]) -> std::result::Result<Session, String> {
        let handover = self.rng.percent(25);
        if handover {
            text(self.settle().await)?;
            self.totals.handovers += 1;
            self.owner = self.rng.pick(&[None, Some(0), Some(1)]);
            if let Some(owner) = self.owner {
                if self.rng.percent(30) {
                    self.caches[owner] = new_owner(&mut self.rng);
                }
            }
        }
        let mut faulty = self.rng.percent(20);
        loop {
            if faulty {
                let _signal = self.store.arm(Failpoint {
                    operation: Operation::Get,
                    key_prefix: String::new(),
                    nth: 1 + self.rng.below(8) as usize,
                    effect: Effect::None,
                    cancel: false,
                });
            }
            let options = self.options();
            trace(|| {
                let o = &options.options;
                format!(
                    "open by {:?}{}: frames of {}, inline {}, prefetch {}",
                    self.owner,
                    if faulty { ", faulty" } else { "" },
                    o.frame_pages,
                    o.inline_record_bytes,
                    o.prefetch_pages,
                )
            });
            let found = async {
                let session = Session::open(self.store.clone(), options).await?;
                for (n, candidate) in candidates.iter().enumerate() {
                    if candidate.matches(&session).await? {
                        return Ok((session, n));
                    }
                }
                let differs = self.model.differs(&session).await?;
                Err(format!(
                    "none of {} states; from the last: {differs:?}",
                    candidates.len()
                )
                .into())
            };
            let found: Result<_> = found.await;
            self.store.disarm();
            match found {
                Ok((session, n)) => {
                    trace(|| format!("reopened on state {n} of {}", candidates.len()));
                    // What an owner reads is durable: it syncs at open unless
                    // its cache knows every flush it made settled.
                    if !self.memory.unsynced().is_empty() {
                        return Err("a reopen read writes no sync made durable".into());
                    }
                    self.observed(candidates[n].clone());
                    return Ok(session);
                }
                // The fault it was given: again, without.
                Err(error) if faulty && error.to_string().contains("injected") => faulty = false,
                Err(error) => return Err(format!("reopen: {error}")),
            }
        }
    }

    /// A reopen read `state`, or a flush made it.
    fn observed(&mut self, state: Model) {
        if self.memory.unsynced().is_empty() {
            self.since_sync.clear();
        }
        self.since_sync.push(state.clone());
        self.model = state;
    }

    /// One flush of a mutation of random changes, perhaps with a fault, a
    /// cancellation, a crash or a reopen. Returns the session that goes on.
    async fn step(&mut self, mut session: Session) -> std::result::Result<Session, String> {
        // The backend may crash at any time, and with it the owner's session:
        // what a reopen read but no sync made durable may go.
        if !self.memory.unsynced().is_empty() && self.rng.percent(30) {
            drop(session);
            self.crash();
            let candidates = self.since_sync.clone();
            session = self.reopen(&candidates).await?;
        }
        let mut mutation = session.state.mutation();
        let mut pending = self.model.clone();
        for _ in 0..1 + self.rng.below(8) {
            let op = Op::random(&mut self.rng);
            trace(|| format!("  {}", describe(&op)));
            text(session.apply(&mut mutation, &op))?;
            pending.apply(&op);
            if self.rng.percent(30) {
                // The state reads as last flushed, whatever the mutation holds.
                let index = self.rng.below(self.model.len().div_ceil(4096) + 2);
                let read = text(session.file().read(index).await)?;
                if read != self.model.page(index) {
                    return Err(format!("page {index} not as flushed"));
                }
            }
        }
        if self.rng.percent(5) {
            trace(|| "discard".into());
            mutation = session.state.mutation();
            pending = self.model.clone();
        }
        if let (true, Some(packs)) = (self.rng.percent(20), &self.packs) {
            packs.land_late();
        }
        self.s3_faults(true);
        let outcome = match self.rng.below(10) {
            0..=5 => mutation.flush().await,
            6..=7 => {
                let failpoint = self.failpoint();
                let _signal = self.store.arm(failpoint);
                let outcome = mutation.flush().await;
                self.store.disarm();
                outcome
            }
            _ => match self.cancelled(mutation) {
                Some(outcome) => outcome,
                // Never polled: nothing happened, and the mutation went with the
                // future.
                None => {
                    self.s3_faults(false);
                    if !session.state.is_usable() {
                        return Err("a flush never polled ended the state".into());
                    }
                    return Ok(session);
                }
            },
        };
        self.s3_faults(false);
        self.totals.flushes += 1;
        trace(|| {
            let outcome = if outcome.is_ok() {
                "acknowledged"
            } else {
                "unacknowledged"
            };
            format!("flush: {outcome}")
        });
        match &outcome {
            Ok(report) => {
                if let Some(checkpoint) = &report.checkpoint {
                    self.adopted += 1;
                    self.totals.checkpoints += 1;
                    self.totals.frames += checkpoint.frames;
                    self.totals.patched += checkpoint.patched;
                    self.totals.compacted += checkpoint.compacted;
                    self.totals.promoted += checkpoint.promoted as usize;
                    self.totals.collected += checkpoint.deleted;
                }
            }
            // It may have named one.
            Err(_) => self.adopted += 1,
        }
        // Its collection failed: the flush landed, but the state ended, with
        // the collection's deletes unsynced, as after a failed collection.
        if let Ok(FlushReport {
            collection_failed: Some(_),
            ..
        }) = &outcome
        {
            if self
                .memory
                .unsynced()
                .iter()
                .any(|(_, value)| value.is_some())
            {
                return Err("a failed collection left writes unsynced".into());
            }
            if session.state.is_usable() {
                return Err("a failed collection left the state usable".into());
            }
            self.observed(pending);
            drop(session);
            if self.rng.percent(50) {
                self.crash();
            }
            let candidates = self.since_sync.clone();
            return self.reopen(&candidates).await;
        }
        if outcome.is_ok() {
            // One that wrote syncs; an unchanged one writes nothing, and
            // needs no sync: its session's open synced what it read.
            if !self.memory.unsynced().is_empty() {
                return Err("an acknowledged flush left writes unsynced".into());
            }
            self.observed(pending);
            text(self.read_beside().await)?;
            if self.rng.percent(25) {
                session = self.collect(session).await?;
            }
            // The session goes on, or a new one (after a crash, perhaps)
            // reads exactly what was acknowledged.
            if self.rng.percent(60) {
                if self.rng.percent(20) {
                    self.check(&session).await?;
                }
                return Ok(session);
            }
            drop(session);
            if self.rng.percent(30) {
                self.crash();
            }
            let candidates = self.since_sync.clone();
            return self.reopen(&candidates).await;
        }
        // The outcome is unknown: the state ended.
        if session.state.is_usable() || session.state.mutation().flush().await.is_ok() {
            return Err("a failed flush left the state usable".into());
        }
        drop(session);
        if self.rng.percent(50) {
            self.crash();
        }
        let before = self.model.clone();
        let mut candidates = self.since_sync.clone();
        candidates.push(pending.clone());
        let session = self.reopen(&candidates).await?;
        match (self.model == pending, self.model == before) {
            (true, false) => self.totals.landed += 1,
            (false, _) => self.totals.lost += 1,
            _ => {}
        }
        Ok(session)
    }

    /// After a flush by the owner: each reader beside it reads what it
    /// opened, until the owner's second checkpoint since may have dropped
    /// what it reads. Sometimes a new reader opens, on what is durable: a
    /// reader sees only what the store holds.
    async fn read_beside(&mut self) -> Result<()> {
        let mut kept = Vec::new();
        for (reader, model, opened) in std::mem::take(&mut self.readers) {
            let may_be_gone = self.adopted >= opened + 2;
            match model.differs(&reader).await {
                Ok(None) => self.totals.reads_beside += 1,
                Ok(Some(differs)) => {
                    return Err(format!("a reader beside the owner: {differs}").into());
                }
                Err(error) if may_be_gone && error.to_string().contains("is gone") => continue,
                Err(error) => return Err(format!("a reader beside the owner: {error}").into()),
            }
            if self.adopted < opened + 4 {
                kept.push((reader, model, opened));
            }
        }
        self.readers = kept;
        if self.readers.len() < 3 && self.rng.percent(30) && self.memory.unsynced().is_empty() {
            let cache = self.rng.pick(&[None, Some(0), Some(1)]);
            let setup = Setup {
                options: StateOptions::default(),
                cache: cache.map(|owner| self.caches[owner].clone()),
                packs: self.packs.clone(),
            };
            let reader = Session::reader(self.store.clone(), setup).await?;
            self.readers
                .push((reader, self.model.clone(), self.adopted));
        }
        Ok(())
    }

    /// A garbage collection after an acknowledged flush, by the owner's
    /// state or by a worker beside it, sometimes with a fault. A failed one
    /// ends the state that ran it: the owner's reopens. Returns the session
    /// that goes on.
    async fn collect(&mut self, session: Session) -> std::result::Result<Session, String> {
        let worker = match self.rng.percent(50) {
            true => None,
            false => {
                let setup = Setup {
                    options: StateOptions::default(),
                    cache: None,
                    packs: self.packs.clone(),
                };
                Some(text(Session::reader(self.store.clone(), setup).await)?)
            }
        };
        let state = worker
            .as_ref()
            .map_or(&session.state, |worker| &worker.state);
        // Whole, with a fault at any of its requests, or dropped at a poll.
        let (faulty, collected) = match self.rng.below(10) {
            0..=5 => (false, state.collect_garbage().await),
            6..=8 => {
                let failpoint = self.failpoint();
                let _signal = self.store.arm(failpoint);
                self.s3_faults(true);
                let collected = state.collect_garbage().await;
                self.s3_faults(false);
                self.store.disarm();
                (true, collected)
            }
            _ => {
                let polls = self.rng.below(8);
                let mut collecting = Box::pin(state.collect_garbage());
                let mut cx = Context::from_waker(noop_waker_ref());
                let mut collected = Err("cancelled".into());
                for _ in 0..polls {
                    if let Poll::Ready(outcome) = collecting.as_mut().poll(&mut cx) {
                        collected = outcome;
                        break;
                    }
                }
                drop(collecting);
                self.totals.cancelled += 1;
                match polls {
                    // Never polled: nothing happened.
                    0 => return Ok(session),
                    _ => (true, collected),
                }
            }
        };
        self.totals.collections += 1;
        trace(|| {
            let by = if worker.is_some() {
                "a worker"
            } else {
                "the owner"
            };
            format!("collection by {by}: {collected:?}")
        });
        let error = match collected {
            Ok(deleted) => {
                self.totals.collected += deleted;
                return Ok(session);
            }
            Err(error)
                if faulty
                    && ["injected", "cancelled"]
                        .iter()
                        .any(|e| error.to_string().contains(e)) =>
            {
                error
            }
            Err(error) => return Err(format!("collection: {error}")),
        };
        if state.is_usable() {
            return Err(format!(
                "a failed collection left the state usable: {error}"
            ));
        }
        if worker.is_some() {
            // The worker's requests settle, in its own process.
            text(self.settle().await)?;
            return Ok(session);
        }
        drop(session);
        if self.rng.percent(50) {
            self.crash();
        }
        let candidates = self.since_sync.clone();
        self.reopen(&candidates).await
    }

    /// Collect all garbage, as a fresh owner that checkpoints at every
    /// flush and then collects, once every write sent to S3 landed: then only the packs the
    /// head needs are stored, and the manifests it names.
    async fn collect_all(&mut self) -> std::result::Result<(), String> {
        if let Some(packs) = &self.packs {
            packs.land_late();
        }
        let setup = Setup {
            options: StateOptions {
                checkpoint_entries: 1,
                ..StateOptions::default()
            },
            cache: None,
            packs: self.packs.clone(),
        };
        let session = text(Session::open(self.store.clone(), setup).await)?;
        for tick in 0..3 {
            let ops = [Op::Record("outbox", format!("tick {tick}").into())];
            text(session.flush(&ops).await)?;
        }
        // The flushes' collections list S3 only around a promotion: one
        // asked for lists it whatever the head.
        text(session.state.collect_garbage().await)?;
        let needed = text(session.state.needed_packs().await)?;
        // L0's packs, in the store, and L1's, in S3.
        let mut stored = self.memory.packs();
        if let Some(packs) = &self.packs {
            stored.extend(packs.keys());
        }
        let manifests = self.memory.manifests();
        let named = session.state.needed_manifests();
        if manifests != named {
            return Err(format!("manifests left: {manifests:#?}, named {named:#?}"));
        }
        if stored != needed {
            return Err(format!("garbage left: {stored:#?}, needed {needed:#?}"));
        }
        Ok(())
    }

    /// A random failpoint, at any request to the store a flush or a garbage
    /// collection makes: of the head, the tail, the manifests, the packs kept
    /// there, or the listing. It may match nothing.
    fn failpoint(&mut self) -> Failpoint {
        let rng = &mut self.rng;
        let (operation, key_prefix) = rng.pick(&[
            (Operation::Put, ""),
            (Operation::Put, TAIL_PREFIX),
            (Operation::Put, HEAD_KEY),
            (Operation::Put, KV_PREFIX),
            (Operation::Put, MANIFEST_PREFIX),
            (Operation::Delete, ""),
            (Operation::Delete, TAIL_PREFIX),
            (Operation::Delete, KV_PREFIX),
            (Operation::Delete, MANIFEST_PREFIX),
            (Operation::Get, ""),
            (Operation::Get, TAIL_PREFIX),
            (Operation::Get, HEAD_KEY),
            (Operation::Get, KV_PREFIX),
            (Operation::Get, MANIFEST_PREFIX),
            (Operation::Sync, ""),
            (Operation::Sync, ""),
            (Operation::Keys, ""),
        ]);
        Failpoint {
            operation,
            key_prefix: key_prefix.into(),
            nth: 1 + rng.below(4) as usize,
            effect: rng.pick(&[Effect::None, Effect::After]),
            cancel: false,
        }
    }

    /// Poll the flush of `mutation` a random number of times, then drop it:
    /// `None` if it was never polled.
    fn cancelled(&mut self, mutation: Mutation) -> Option<Result<FlushReport>> {
        let polls = self.rng.below(12);
        let mut flush = Box::pin(mutation.flush());
        let mut cx = Context::from_waker(noop_waker_ref());
        for _ in 0..polls {
            if let Poll::Ready(outcome) = flush.as_mut().poll(&mut cx) {
                return Some(outcome);
            }
        }
        self.totals.cancelled += usize::from(polls > 0);
        (polls > 0).then(|| Err("cancelled".into()))
    }

    /// The session reads as the model.
    async fn check(&self, session: &Session) -> std::result::Result<(), String> {
        match self.model.differs(session).await {
            Ok(None) => Ok(()),
            Ok(Some(differs)) => Err(format!("the session differs: {differs}")),
            Err(error) => Err(format!("the session fails to read: {error}")),
        }
    }
}

/// With `STATE_TRACE` set, print what a run does.
fn trace(line: impl FnOnce() -> String) {
    static ON: OnceLock<bool> = OnceLock::new();
    if *ON.get_or_init(|| std::env::var_os("STATE_TRACE").is_some()) {
        eprintln!("{}", line());
    }
}

fn describe(op: &Op) -> String {
    match op {
        Op::Record(name, value) => format!("set {name:?} to {} bytes", value.len()),
        Op::Write(index, Some(image)) => format!("write page {index}: {:02x?}", &image[..4]),
        Op::Write(index, None) => format!("hole page {index}"),
        Op::SetLen(len) => format!("set_len {len}"),
    }
}

/// A result whose error is text, for a failing seed to print.
fn text<T>(result: Result<T>) -> std::result::Result<T, String> {
    result.map_err(|error| error.to_string())
}

async fn run(seed: u64) -> std::result::Result<Totals, String> {
    let mut rng = Rng::new(seed);
    // Odd seeds keep checkpoints in S3, which fails one request in eight.
    let packs = (seed % 2 == 1).then(MemoryBlobs::default);
    let faults = BlobFaults {
        one_in: 8,
        seed: rng.below(u64::MAX),
    };
    let memory = MemoryKv::default();
    let fault = FaultKv::new(memory.clone(), rng.below(3));
    let mut monitor = MonitorKv::new(fault);
    if let Some(packs) = &packs {
        monitor = monitor.packs(packs);
    }
    let store = Arc::new(monitor);
    let caches = [new_owner(&mut rng), new_owner(&mut rng)];
    let mut world = World {
        rng,
        store,
        memory,
        caches,
        owner: Some(0),
        model: Model::default(),
        since_sync: vec![Model::default()],
        packs,
        faults: Some(faults),
        adopted: 0,
        readers: Vec::new(),
        totals: Totals::default(),
    };
    let mut session = world.reopen(&[Model::default()]).await?;
    for step in 0..STEPS {
        session = (world.step(session).await).map_err(|e| format!("step {step}: {e}"))?;
        world
            .violations()
            .map_err(|e| format!("step {step}: {e}"))?;
    }
    // Whatever happened, a crash and a cold open read the last state.
    drop(session);
    text(world.settle().await)?;
    world.owner = None;
    let candidates = world.since_sync.clone();
    let session = world.reopen(&candidates).await?;
    world.check(&session).await?;
    drop(session);
    world.collect_all().await?;
    world.violations()?;
    let stats = world.memory.stats();
    world.totals.requests_checked = world.store.checked();
    world.totals.tail_entries = stats.kind("tail").puts;
    if let Some(packs) = &world.packs {
        world.totals.s3_faults = packs.failed();
        world.totals.landed_late = packs.landed_late();
    }
    Ok(world.totals)
}
