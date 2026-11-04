//! What a cold read of a checkpoint's frames fetches from the store.
use crate::state::{simulation::*, Result, State};
use std::sync::Arc;

/// A page that compresses to about its size: a few frames share a part.
fn noise(seed: u64) -> crate::state::Page {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..4096)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect::<Vec<u8>>()
        .into()
}

/// With packs kept in Restate, a cold read fetches the 64 KiB part its frame
/// is in, and the cache keeps the part: reading every page of a file costs
/// a get per part of the pack, not one per frame.
#[test]
fn a_part_of_a_pack_is_fetched_once_for_every_frame_in_it() -> Result<()> {
    futures::executor::block_on(async {
        const PAGES: u64 = 64;
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        let mut mutation = state.mutation();
        for n in 0..PAGES {
            mutation.write(n, Some(noise(n)))?;
        }
        mutation.flush().await?;
        // Checkpoint the tail into a pack, with a flush that writes a record.
        let mut mutation = state.mutation();
        mutation.checkpoint();
        mutation.set_record("outbox", "x".into())?;
        let report = mutation.flush().await?;
        let pack_bytes = report.checkpoint.expect("a checkpoint").pack_bytes;
        let parts = pack_bytes.div_ceil(64 << 10) as u64;
        let frames = PAGES / 4;
        assert!(parts < frames, "{parts} parts for {frames} frames");

        // A cold cache: another process's.
        let cache = open_cache(cache_config(16 << 20, 16 << 20));
        let reader = State::open(store.clone()).cached(&cache, "db").await?;
        store.reset_stats();
        let file = reader.file();
        for n in 0..PAGES {
            assert_eq!(file.read(n).await?, Some(noise(n)), "page {n}");
        }
        let gets = store.stats().kind("pack").gets;
        assert_eq!(gets, parts, "a get per part, {frames} frames");
        Ok(())
    })
}

/// Write `frames` frames of noise, from frame `from`, seeded by `seed`, and
/// checkpoint them; the checkpoint's report.
async fn checkpointed(
    state: &State,
    from: u64,
    frames: u64,
    seed: u64,
) -> Result<crate::state::Checkpointed> {
    let mut mutation = state.mutation();
    for n in from * 4..(from + frames) * 4 {
        mutation.write(n, Some(noise(seed + n)))?;
    }
    mutation.flush().await?;
    let mut mutation = state.mutation();
    mutation.checkpoint();
    mutation.set_record("outbox", seed.to_string().into())?;
    Ok(mutation.flush().await?.checkpoint.expect("a checkpoint"))
}

/// With packs in S3, a compaction fetches the live frames of a pack in one
/// GET when they lie together, rather than one GET a frame.
#[test]
fn a_compaction_fetches_neighbouring_frames_in_one_get() -> Result<()> {
    futures::executor::block_on(async {
        let packs = MemoryBlobs::default();
        let store = Arc::new(MemoryKv::default());
        let options = crate::state::StateOptions {
            l0_bytes: 0,
            ..Default::default()
        };
        let state = State::open(store)
            .checkpoints(Arc::new(packs.clone()))
            .options(options)
            .await?;
        checkpointed(&state, 0, 32, 0).await?;
        // 18 of the 32 frames again: the first pack is under half live, and
        // its last 14 frames, side by side, move.
        let gets = packs.gets();
        let second = checkpointed(&state, 0, 18, 1000).await?;
        assert_eq!(second.compacted, 1, "{second:?}");
        assert_eq!(packs.gets() - gets, 1, "the 14 live frames in one get");
        let file = state.file();
        for n in 0..32 * 4 {
            let seed = if n < 18 * 4 { 1000 } else { 0 };
            assert_eq!(file.read(n).await?, Some(noise(seed + n)), "page {n}");
        }
        Ok(())
    })
}
