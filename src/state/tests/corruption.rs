//! Corrupted stored values: truncated, bit-flipped or garbage, at the head,
//! a tail entry or a part of a pack. Open and reads fail or read what was
//! flushed, never panic; and garbage collected after them never takes what
//! the head needs.

use super::support::*;
use crate::state::{kv::KvStore, simulation::*, Result, StateOptions};
use futures::FutureExt;
use std::{panic::AssertUnwindSafe, sync::Arc};

fn options() -> StateOptions {
    StateOptions {
        frame_pages: 2,
        inline_record_bytes: 200,
        prefetch_pages: 2,
        checkpoint_entries: 2,
        ..StateOptions::default()
    }
}

/// A namespace with every kind of key: the head, tail entries and packs,
/// which hold frames, values and manifests.
async fn flushed() -> Result<(MemoryKv, Model)> {
    let memory = MemoryKv::default();
    let mut model = Model::default();
    let mut rng = Rng::new(0xbad);
    for _ in 0..5 {
        let session = Session::open(Arc::new(memory.clone()), options()).await?;
        session.file().read(1).await?;
        let ops: Vec<Op> = (0..10).map(|_| Op::random(&mut rng)).collect();
        session.flush(&ops).await?;
        model = model.applied(&ops);
    }
    let session = Session::open(Arc::new(memory.clone()), options()).await?;
    session.state.collect_garbage().await?;
    Ok((memory, model))
}

/// Ways to corrupt a value: each one's name, and what it makes of it.
fn corruptions(value: &[u8], rng: &mut Rng) -> Vec<(String, Vec<u8>)> {
    let len = value.len();
    let mut out = vec![("empty".to_owned(), Vec::new())];
    for cut in [1, 4, len / 2, len.saturating_sub(1)] {
        if cut < len {
            out.push((format!("cut to {cut}"), value[..cut].to_vec()));
        }
    }
    let mut longer = value.to_vec();
    longer.extend(rng.bytes(3));
    out.push(("extended".into(), longer));
    let garbage = 1 + rng.below(2 * len as u64 + 8) as usize;
    let garbage = rng.bytes(garbage);
    out.push(("garbage".into(), garbage));
    // A short value has a bit flipped in every byte.
    let flips: Vec<usize> = match len <= 16 {
        true => (0..len).collect(),
        false => (0..12).map(|_| rng.below(len as u64) as usize).collect(),
    };
    for at in flips {
        let mut flipped = value.to_vec();
        flipped[at] ^= 1 << rng.below(8);
        out.push((format!("bit flipped at {at}"), flipped));
    }
    out
}

/// What reading a corrupted namespace did.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Read {
    Failed,
    AsFlushed,
    Otherwise,
}

/// Open the namespace and read everything, without a cache or with one;
/// then collect garbage, and read again as a new owner: what it left.
async fn read(store: Arc<dyn KvStore>, model: &Model, cached: bool) -> (Read, Read) {
    let options = |cached: bool| Setup {
        options: options(),
        cache: cached.then(fresh_namespace),
        packs: None,
    };
    let read = async |collected: bool| {
        let session = Session::open(store.clone(), options(cached && !collected)).await?;
        if collected {
            // Its failure is a read's to find.
            let _ = session.state.collect_garbage().await;
            let session = Session::open(store.clone(), options(false)).await?;
            return model.matches(&session).await;
        }
        model.matches(&session).await
    };
    let outcome = |read: Result<bool>| match read {
        Err(_) => Read::Failed,
        Ok(true) => Read::AsFlushed,
        Ok(false) => Read::Otherwise,
    };
    (outcome(read(false).await), outcome(read(true).await))
}

#[test]
fn corrupted_values_fail_reads_or_read_as_flushed_and_never_panic() -> Result<()> {
    futures::executor::block_on(async {
        let (memory, model) = flushed().await?;
        let keys = memory.keys();
        for prefix in ["v22/head", "v22/tail/", "v22/manifest/", "v22/pack/"] {
            assert!(
                keys.iter().any(|key| key.starts_with(prefix)),
                "{prefix}: {keys:?}"
            );
        }
        let mut rng = Rng::new(0xf11b);
        let mut outcomes = std::collections::BTreeMap::<_, usize>::new();
        for key in &keys {
            let value = memory.get(key).await?.unwrap();
            for (corruption, bytes) in corruptions(&value, &mut rng) {
                let corrupted = memory.fork();
                corrupted.corrupt(key, bytes.into());
                for cached in [false, true] {
                    let case = format!("{key} {corruption}, cached {cached}");
                    let store: Arc<dyn KvStore> = Arc::new(corrupted.clone());
                    let read = AssertUnwindSafe(read(store, &model, cached)).catch_unwind();
                    let Ok((read, collected)) = read.await else {
                        panic!("{case}: panicked");
                    };
                    // Garbage is only what the head does not need.
                    if read == Read::AsFlushed && collected != Read::AsFlushed {
                        panic!("{case}: read as flushed, then after collecting {collected:?}");
                    }
                    // A bit flip may stay undetected: frames have a
                    // checksum, but not their headers.
                    let flipped = corruption.starts_with("bit");
                    let kind = key.split('/').nth(1).unwrap();
                    match &read {
                        Read::AsFlushed | Read::Failed => {}
                        Read::Otherwise if flipped => {}
                        _ => panic!("{case}: {read:?}"),
                    }
                    *outcomes.entry((kind, flipped, read)).or_default() += 1;
                }
            }
        }
        println!("corrupted reads: {outcomes:?}");
        Ok(())
    })
}
