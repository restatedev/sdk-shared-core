//! A crash at every write of every flush of a sequence that sets records
//! (inline and in packs), writes the file, shrinks it, and checkpoints, with
//! checkpoints in the store and in S3: each flush is atomic, durable once its head is, and the state
//! goes on; a checkpoint, or a garbage collection, that fails takes nothing
//! a head needs.

use super::support::*;
use crate::state::{kv::KvStore, simulation::*, Result, StateOptions, HEAD_KEY};
use futures::future::Either;
use std::sync::Arc;

/// Flushes in the sequence.
const STEPS: u64 = 16;

/// Checkpoints every other flush, in the store, or in `packs`.
fn options(cache: Option<Namespace>, packs: Option<&MemoryBlobs>) -> Setup {
    Setup {
        options: StateOptions {
            frame_pages: 2,
            inline_record_bytes: 150,
            prefetch_pages: 0,
            checkpoint_entries: 2,
            checkpoint_bytes: 0,
            l0_bytes: 0,
        },
        cache,
        packs: packs.cloned(),
    }
}

/// The changes of flush `step`: pages a few bytes apart and pages of random
/// bytes, records over the inline budget, holes, and a shrink.
fn changes(step: u64) -> Vec<Op> {
    let mut rng = Rng::new(step);
    let image = |rng: &mut Rng| match step % 3 {
        0 => varied(rng),
        _ => page(step),
    };
    let mut ops = vec![
        Op::Record("outbox", format!("step {step}").into_bytes().into()),
        Op::Write(step % 4, Some(image(&mut rng))),
        Op::Write(4 + step % 3, Some(image(&mut rng))),
        Op::Write(8 + step % 6, Some(image(&mut rng))),
    ];
    if step % 2 == 1 {
        ops.push(Op::Record("outbox", rng.bytes(600).into()));
    }
    if step % 3 == 2 {
        ops.push(Op::SetLen(512 * 9));
    }
    if step % 4 == 3 {
        ops.push(Op::Write(1, None));
    }
    ops
}

/// Open a session and read a few pages, so that a flush finds their frames
/// decoded.
async fn session(
    store: Arc<dyn KvStore>,
    cache: Option<Namespace>,
    packs: Option<&MemoryBlobs>,
) -> Result<Session> {
    let session = Session::open(store, options(cache, packs)).await?;
    session.file().read(0).await?;
    Ok(session)
}

#[test]
fn a_flush_is_atomic_at_every_write_of_a_sequence() -> Result<()> {
    let kinds = futures::executor::block_on(crash_every_flush(None))?;
    let kinds: Vec<&str> = kinds.iter().map(String::as_str).collect();
    assert_eq!(
        kinds,
        [
            "head put",
            "manifest delete",
            "manifest put",
            "pack delete",
            "pack put",
            "tail delete",
            "tail put"
        ]
    );
    Ok(())
}

/// With checkpoints in S3 (the packs a crash cannot take back), the same.
#[test]
fn a_flush_with_checkpoints_in_s3_is_atomic_at_every_write() -> Result<()> {
    let packs = MemoryBlobs::default();
    let kinds = futures::executor::block_on(crash_every_flush(Some(packs)))?;
    assert_eq!(
        kinds,
        [
            "head put",
            "manifest delete",
            "manifest put",
            "tail delete",
            "tail put"
        ]
    );
    Ok(())
}

/// The syncs `run` makes on a copy of `durable`, once it opened a session.
async fn syncs<F: AsyncFnOnce(&Session) -> Result<()>>(
    durable: &MemoryKv,
    packs: Option<&MemoryBlobs>,
    run: F,
) -> Result<usize> {
    let dry = durable.fork();
    let packs = packs.map(MemoryBlobs::fork);
    let opened = session(Arc::new(dry.clone()), None, packs.as_ref()).await?;
    dry.reset_stats();
    run(&opened).await?;
    Ok(dry.stats().syncs as usize)
}

/// Crash each flush of the sequence at its every sync (its checkpoint's
/// and its garbage collection's too), keeping every prefix of the writes
/// queued: the kinds of writes it crashed at.
async fn crash_every_flush(packs: Option<MemoryBlobs>) -> Result<Vec<String>> {
    let durable = MemoryKv::default();
    let mut model = Model::default();
    let (mut cases, mut kinds) = (0, std::collections::BTreeSet::new());
    for step in 0..STEPS {
        let ops = changes(step);
        let new = model.applied(&ops);
        let syncs = syncs(&durable, packs.as_ref(), async |session| {
            session.flush(&ops).await.map(drop)
        })
        .await?;
        for (cached, nth) in [false, true]
            .into_iter()
            .flat_map(|c| (1..=syncs).map(move |n| (c, n)))
        {
            // The process dies at the flush's `nth` sync, the writes since
            // the last one queued.
            let fault = Arc::new(FaultKv::new(durable.fork(), 0));
            let packs = packs.as_ref().map(MemoryBlobs::fork);
            let cache = cached.then(fresh_namespace);
            let failing = session(fault.clone(), cache.clone(), packs.as_ref()).await?;
            let signal = fault.arm(Failpoint {
                operation: Operation::Sync,
                key_prefix: String::new(),
                nth,
                effect: Effect::None,
                cancel: true,
            });
            {
                let flush = std::pin::pin!(failing.flush(&ops));
                if let Either::Left((done, _)) = futures::future::select(flush, signal).await {
                    panic!("step {step}: the flush ended before its sync {nth}: {done:?}");
                }
            }
            fault.disarm();
            drop(failing);
            let writes = fault.inner.unsynced();
            // Its head is in the writes queued, or was synced already
            // (a collection's sync), or not written yet (a checkpoint's).
            let record = match writes.iter().position(|(key, _)| key == HEAD_KEY) {
                Some(at) => at + 1,
                None => {
                    let synced = fault.inner.fork();
                    synced.crash();
                    let opened = Session::open(Arc::new(synced), options(None, packs.as_ref()));
                    match new.matches(&opened.await?).await? {
                        true => 0,
                        false => writes.len() + 1,
                    }
                }
            };
            for (key, value) in &writes {
                let kind = object_kind(key);
                kinds.insert(format!(
                    "{kind} {}",
                    if value.is_some() { "put" } else { "delete" }
                ));
            }
            for kept in 0..=writes.len() {
                cases += 1;
                let memory = fault.inner.fork();
                memory.crash_keeping(kept);
                let packs = packs.as_ref().map(MemoryBlobs::fork);
                let mut monitor = MonitorKv::seeded(memory.clone(), memory.values());
                if let Some(packs) = &packs {
                    monitor = monitor.packs(packs);
                }
                let store = Arc::new(monitor);
                // The owner's cache saw the failure; reading, it follows
                // this crash's history as well as any.
                let reopened = Session::open(store.clone(), options(cache.clone(), packs.as_ref()));
                let reopened = reopened.await?;
                let expected = if kept >= record { &new } else { &model };
                let case = format!(
                    "step {step}, cached {cached}, sync {nth}: {kept} of {} writes",
                    writes.len()
                );
                assert_eq!(expected.differs(&reopened).await?, None, "{case}");
                drop(reopened);
                // A next flush, by a new owner, survives a crash.
                let next = [
                    Op::Record("outbox", "next".into()),
                    Op::Write(9, Some(page(99))),
                ];
                let owner = cached.then(fresh_namespace);
                let after = session(store.clone(), owner, packs.as_ref()).await?;
                after.flush(&next).await?;
                drop(after);
                memory.crash();
                let reopened = Session::open(store.clone(), options(None, packs.as_ref())).await?;
                let next = expected.applied(&next);
                assert_eq!(
                    next.differs(&reopened).await?,
                    None,
                    "{case}, then another flush"
                );
                assert_eq!(store.violations(), Vec::<String>::new(), "{case}");
            }
        }
        // The sequence goes on from the flush acknowledged.
        let store: Arc<dyn KvStore> = Arc::new(durable.clone());
        session(store, None, packs.as_ref())
            .await?
            .flush(&ops)
            .await?;
        model = new;
    }
    println!("crashed flushes: {cases} cases");
    Ok(kinds.into_iter().collect())
}
