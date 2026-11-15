//! Futures dropped after every number of polls: a flush cancelled midway
//! ends its state and lands wholly or not at all, and the state reopens
//! and goes on; one never polled did nothing, and its mutation went with it; a
//! cancelled open leaves the owner's cache usable.

use super::support::*;
use crate::state::{
    blobs::BlobStore, kv::KvStore, simulation::*, Result, State, StateOptions, HEAD_KEY, PAGE_SIZE,
};
use bytes::Bytes;
use futures::{
    channel::oneshot,
    future::{select, BoxFuture, Either},
    task::noop_waker_ref,
};
use std::future::Future;
use std::{
    io,
    ops::Range,
    pin::pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread::{self, ThreadId},
    time::Duration,
};

/// Small frames, a checkpoint every other flush, and large records in
/// packs.
fn options(cache: Option<Namespace>) -> Setup {
    Setup {
        options: StateOptions {
            frame_pages: 2,
            checkpoint_entries: 2,
            inline_record_bytes: 200,
            ..StateOptions::default()
        },
        cache,
        packs: None,
    }
}

/// Poll `future` up to `polls` times: its output, if it completed.
fn poll_times<F: Future>(future: F, polls: usize) -> Option<F::Output> {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(noop_waker_ref());
    (0..polls).find_map(|_| match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    })
}

/// Random changes to one record, and to the file. With another record, one
/// left in an older pack moves at a checkpoint: a fetch that waits on the
/// cache's disk threads, not on polls.
fn random_ops(rng: &mut Rng, count: usize) -> Vec<Op> {
    let one = |op| match op {
        Op::Record(_, value) => Op::Record("outbox", value),
        op => op,
    };
    (0..count).map(|_| one(Op::random(rng))).collect()
}

/// A store after a few flushes, and what it holds.
async fn prepared() -> Result<(MemoryKv, Model)> {
    let memory = MemoryKv::default();
    let mut model = Model::default();
    let mut rng = Rng::new(0xca9ce1);
    for _ in 0..4 {
        let session = Session::open(Arc::new(memory.clone()), options(None)).await?;
        let ops = random_ops(&mut rng, 8);
        session.flush(&ops).await?;
        model = model.applied(&ops);
    }
    Ok((memory, model))
}

#[test]
fn a_flush_cancelled_after_any_number_of_polls_lands_wholly_or_not_at_all() -> Result<()> {
    futures::executor::block_on(async {
        let (prepared, old) = prepared().await?;
        let mut rng = Rng::new(0xd20);
        let ops = random_ops(&mut rng, 12);
        let new = old.applied(&ops);
        // Reopened by the owner, or after a crash.
        for (cached, crash) in [(false, false), (false, true), (true, false), (true, true)] {
            let mut landed = [0; 2];
            for polls in 0.. {
                // Slow requests: a flush takes several polls.
                let memory = prepared.fork();
                let fault = FaultKv::new(memory.clone(), 5);
                let store = Arc::new(MonitorKv::seeded(fault, memory.values()));
                let cache = cached.then(fresh_namespace);
                let session = Session::open(store.clone(), options(cache.clone())).await?;
                if cached {
                    // Read in full, so the flush fetches nothing: a miss
                    // waits on the cache's disk threads, not on polls.
                    assert_eq!(old.differs(&session).await?, None);
                }
                let mut mutation = session.state.mutation();
                for op in &ops {
                    session.apply(&mut mutation, op)?;
                }
                let state = &session.state;
                let version = state.version();
                match poll_times(mutation.flush(), polls) {
                    Some(outcome) => {
                        outcome?;
                        assert!(polls > 2, "a flush takes several polls");
                        break;
                    }
                    // Never polled: nothing happened, and the mutation went with
                    // the future.
                    None if polls == 0 => {
                        assert!(state.is_usable());
                        assert_eq!(state.version(), version);
                        assert_eq!(memory.unsynced(), vec![]);
                        assert_eq!(old.differs(&session).await?, None);
                        continue;
                    }
                    None => {}
                }
                let case = format!("cached {cached}, crash {crash}, dropped after {polls} polls");
                assert!(!state.is_usable(), "{case}");
                if state.version() > version {
                    // Dropped while it collected garbage, after it landed.
                    drop(session);
                    let reopened = Session::open(store.clone(), options(cache)).await?;
                    assert_eq!(new.differs(&reopened).await?, None, "{case}");
                    assert_eq!(store.violations(), Vec::<String>::new(), "{case}");
                    continue;
                }
                let error = state.mutation().flush().await.unwrap_err().to_string();
                assert!(error.contains("ended"), "{case}: {error}");
                drop(session);
                // The flush lands with its head; a crash takes it back, as
                // no sync completed.
                let head_queued = memory.unsynced().iter().any(|(key, _)| key == HEAD_KEY);
                if crash {
                    memory.crash();
                }
                let reopened = Session::open(store.clone(), options(cache)).await?;
                let whole = new.matches(&reopened).await?;
                assert!(whole || old.matches(&reopened).await?, "{case}");
                assert_eq!(whole, head_queued && !crash, "{case}");
                landed[usize::from(whole)] += 1;
                // It goes on, durably.
                let after = [Op::Record("outbox", "after".into())];
                reopened.flush(&after).await?;
                drop(reopened);
                memory.crash();
                let reopened = Session::open(store.clone(), options(None)).await?;
                let expected = if whole { &new } else { &old };
                assert_eq!(
                    expected.applied(&after).differs(&reopened).await?,
                    None,
                    "{case}"
                );
                assert_eq!(store.violations(), Vec::<String>::new(), "{case}");
            }
            // Some drops came after the head went out.
            if !crash {
                assert!(landed[1] > 0, "cached {cached}: {landed:?}");
            }
        }
        Ok(())
    })
}

/// An owner's open dropped midway, at each poll: the next open reads the
/// state, and flushes.
#[test]
fn an_open_cancelled_at_any_poll_leaves_the_cache_usable() -> Result<()> {
    futures::executor::block_on(async {
        let (memory, mut model) = prepared().await?;
        let seed = memory.values();
        let fault = FaultKv::new(memory, 3);
        let store: Arc<dyn KvStore> = Arc::new(MonitorKv::seeded(fault, seed));
        let cache = fresh_namespace();
        let mut rng = Rng::new(0x09e2);
        for polls in 0.. {
            let open = Session::open(store.clone(), options(Some(cache.clone())));
            if poll_times(open, polls).is_some() {
                assert!(polls > 2, "an open takes several polls");
                break;
            }
            let session = Session::open(store.clone(), options(Some(cache.clone()))).await?;
            assert_eq!(model.differs(&session).await?, None, "after {polls} polls");
            let ops = [Op::random(&mut rng)];
            session.flush(&ops).await?;
            model = model.applied(&ops);
        }
        Ok(())
    })
}

/// Packs in memory, until armed: then the next put says on which thread it
/// runs, and never returns, holding a sender that says when it is dropped.
#[derive(Default)]
struct StuckPut {
    inner: MemoryBlobs,
    armed: Mutex<Option<(oneshot::Sender<ThreadId>, oneshot::Sender<()>)>>,
}

impl BlobStore for StuckPut {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        self.inner.get_range(key, range)
    }
    fn put<'a>(&'a self, key: &'a str, value: Bytes) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let Some((started, _held)) = self.armed.lock().unwrap().take() else {
                return self.inner.put(key, value).await;
            };
            let _ = started.send(thread::current().id());
            futures::future::pending().await
        })
    }
    fn delete<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, io::Result<()>> {
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<String>>> {
        self.inner.list(prefix)
    }
}

/// On a multi-thread runtime, a checkpoint's upload runs as a task of it,
/// not on the thread that polls the flush, and dropping the flush aborts it.
#[test]
fn a_checkpoint_uploads_on_the_runtime_and_a_dropped_flush_aborts_it() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let packs = Arc::new(StuckPut::default());
        // Every checkpoint goes to L1, the packs.
        let options = StateOptions {
            l0_bytes: 0,
            ..StateOptions::default()
        };
        let state = State::open(Arc::new(MemoryKv::default()))
            .options(options)
            .checkpoints(packs.clone())
            .await?;
        let mut mutation = state.mutation();
        mutation.set_len(2 * PAGE_SIZE as u64)?;
        mutation.write(0, Some(page(1)))?;
        mutation.flush().await?;

        let (started, uploading) = oneshot::channel();
        let (held, dropped) = oneshot::channel();
        *packs.armed.lock().unwrap() = Some((started, held));
        let mut mutation = state.mutation();
        mutation.write(1, Some(page(2)))?;
        mutation.checkpoint();
        let uploader = match select(Box::pin(mutation.flush()), uploading).await {
            Either::Left(_) => panic!("the flush returned without its upload"),
            Either::Right((uploader, flush)) => {
                drop(flush);
                uploader.unwrap()
            }
        };
        assert_ne!(
            uploader,
            thread::current().id(),
            "uploaded where the flush is polled"
        );
        let aborted = tokio::time::timeout(Duration::from_secs(10), dropped).await;
        assert!(
            matches!(aborted, Ok(Err(oneshot::Canceled))),
            "the upload outlived its flush"
        );
        assert!(!state.is_usable());
        Ok(())
    })
}
