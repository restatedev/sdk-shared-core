//! Packs in two tiers: a checkpoint's pack goes to L0, in Restate, until L0
//! holds `l0_bytes`; the next goes to L1, in S3, with everything L0 held.
use crate::state::{simulation::*, Result, State, StateOptions};
use std::sync::Arc;

/// A page that compresses to about its size.
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

/// A checkpoint at every flush, L0 promoted past 64 KiB.
fn options() -> StateOptions {
    StateOptions {
        checkpoint_entries: 1,
        l0_bytes: 64 << 10,
        ..Default::default()
    }
}

/// Write pages `pages` of `db`, seeded by `seed`, in one flush: its report.
async fn write(
    state: &State,
    pages: std::ops::Range<u64>,
    seed: u64,
) -> Result<crate::state::FlushReport> {
    let mut mutation = state.mutation();
    for n in pages {
        mutation.write(n, Some(noise(seed + n)))?;
    }
    mutation.flush().await
}

/// A state far below `l0_bytes` keeps every pack in Restate: S3 is never
/// written, read or listed, not even by the collections after its
/// checkpoints.
#[test]
fn a_small_state_never_reaches_s3() -> Result<()> {
    futures::executor::block_on(async {
        let (store, s3) = (Arc::new(MemoryKv::default()), MemoryBlobs::default());
        let state = State::open(store.clone())
            .checkpoints(Arc::new(s3.clone()))
            .options(options())
            .await?;
        for n in 0..20 {
            write(&state, n % 4..n % 4 + 1, n * 100).await?;
        }
        assert_eq!(
            (s3.puts(), s3.gets(), s3.lists()),
            (0, 0, 0),
            "no request to S3"
        );
        assert!(!store.packs().is_empty());
        Ok(())
    })
}

/// Once L0 holds `l0_bytes`, one checkpoint puts its pack in S3, with what
/// L0 held; the collection after it empties L0; every page still reads, in a
/// cold open too.
#[test]
fn a_full_l0_is_promoted_to_s3_in_one_put() -> Result<()> {
    futures::executor::block_on(async {
        let (store, s3) = (Arc::new(MemoryKv::default()), MemoryBlobs::default());
        let state = State::open(store.clone())
            .checkpoints(Arc::new(s3.clone()))
            .options(options())
            .await?;
        // 8 pages (about 32 KiB) a flush: the third checkpoint finds L0 full.
        let mut promoted = None;
        for n in 0..6 {
            let report = write(&state, n * 8..(n + 1) * 8, 0).await?;
            if let Some(c) = report.checkpoint.filter(|c| c.promoted) {
                if promoted.is_none() {
                    promoted = Some((n, c.version, s3.puts()));
                }
            }
        }
        let (at, version, puts) = promoted.expect("a promotion");
        assert!(at >= 2, "promoted at flush {at}");
        assert_eq!(puts, 1, "the promotion is one put");
        // Two more checkpoints: the promotion's is now the previous, and the
        // packs of L0 it moved are garbage.
        write(&state, 0..1, 7).await?;
        write(&state, 1..2, 7).await?;
        let needed = state.needed_packs().await?;
        assert!(
            needed.iter().any(|pack| pack.starts_with("l1/")),
            "{needed:?}"
        );
        // L0 holds only packs written after the promotion.
        let written = |pack: &str| u64::from_str_radix(&pack[3..19], 16).unwrap();
        for pack in store.packs() {
            assert!(written(&pack) > version, "{pack} left in L0");
        }
        let cold = State::open(store.clone())
            .checkpoints(Arc::new(s3.clone()))
            .options(options())
            .await?;
        let file = cold.file();
        for n in 0..48 {
            let seed = if n < 2 { 7 } else { 0 };
            assert_eq!(file.read(n).await?, Some(noise(seed + n)), "page {n}");
        }
        Ok(())
    })
}

/// A checkpoint that would take L0 past `l0_bytes` on its own goes to L1:
/// one large write never lands in Restate whole.
#[test]
fn a_checkpoint_larger_than_l0_goes_to_s3() -> Result<()> {
    futures::executor::block_on(async {
        let (store, s3) = (Arc::new(MemoryKv::default()), MemoryBlobs::default());
        let state = State::open(store.clone())
            .checkpoints(Arc::new(s3.clone()))
            .options(options())
            .await?;
        // About 128 KiB in one flush; the next one checkpoints it.
        write(&state, 0..32, 0).await?;
        let report = write(&state, 32..33, 0).await?;
        let checkpoint = report.checkpoint.expect("a checkpoint");
        assert!(checkpoint.promoted, "{checkpoint:?}");
        assert!(store.packs().is_empty(), "{:?}", store.packs());
        Ok(())
    })
}

/// A cold read of a frame in L1 is one S3 GET; of a frame in L0, gets of
/// its parts in Restate, and none of S3.
#[test]
fn a_cold_read_goes_to_the_tier_its_frame_is_in() -> Result<()> {
    futures::executor::block_on(async {
        let (store, s3) = (Arc::new(MemoryKv::default()), MemoryBlobs::default());
        let state = State::open(store.clone())
            .checkpoints(Arc::new(s3.clone()))
            .options(options())
            .await?;
        // Frames 0-15 reach L1; frame 16 is written after, into L0.
        for n in 0..4 {
            write(&state, n * 16..(n + 1) * 16, 0).await?;
        }
        write(&state, 64..68, 0).await?;
        write(&state, 68..69, 0).await?;
        let cold = State::open(store.clone())
            .checkpoints(Arc::new(s3.clone()))
            .options(StateOptions {
                prefetch_pages: 0,
                ..options()
            })
            .await?;
        let file = cold.file();
        let gets = s3.gets();
        store.reset_stats();
        assert_eq!(file.read(64).await?, Some(noise(64)));
        assert_eq!(s3.gets() - gets, 0, "frame 16 is in L0");
        assert!(store.stats().kind("pack").gets > 0);
        store.reset_stats();
        assert_eq!(file.read(1).await?, Some(noise(1)));
        assert_eq!(s3.gets() - gets, 1, "frame 0 is in L1");
        assert_eq!(store.stats().kind("pack").gets, 0);
        Ok(())
    })
}

/// A promotion that fails (S3 down) fails nothing: L0 keeps its packs, the
/// flushes go on, and once S3 is back a promotion goes through.
#[test]
fn a_failed_promotion_leaves_l0_as_it_was() -> Result<()> {
    futures::executor::block_on(async {
        let (store, s3) = (Arc::new(MemoryKv::default()), MemoryBlobs::default());
        let state = State::open(store.clone())
            .checkpoints(Arc::new(s3.clone()))
            .options(options())
            .await?;
        s3.fail_after(Some(0));
        let mut failed = 0;
        for n in 0..6 {
            let report = write(&state, n * 8..(n + 1) * 8, 0).await?;
            failed += report.checkpoint_failed.is_some() as usize;
        }
        assert!(failed > 0);
        assert!(!store.packs().is_empty());
        s3.fail_after(None);
        let mut promoted = false;
        for n in 6..80 {
            let report = write(&state, n % 48..n % 48 + 1, 0).await?;
            promoted |= report.checkpoint.is_some_and(|c| c.promoted);
        }
        assert!(promoted, "a promotion once S3 is back");
        let file = state.file();
        for n in 0..48 {
            assert_eq!(file.read(n).await?, Some(noise(n)), "page {n}");
        }
        Ok(())
    })
}

/// A state whose packs reached L1, opened without S3, says so.
#[test]
fn an_open_without_s3_of_a_state_in_l1_fails_clearly() -> Result<()> {
    futures::executor::block_on(async {
        let (store, s3) = (Arc::new(MemoryKv::default()), MemoryBlobs::default());
        let state = State::open(store.clone())
            .checkpoints(Arc::new(s3.clone()))
            .options(StateOptions {
                l0_bytes: 0,
                ..options()
            })
            .await?;
        write(&state, 0..32, 0).await?;
        write(&state, 0..1, 1).await?;
        let error = match State::open(store.clone()).await {
            Ok(without) => without.file().read(5).await.err(),
            Err(error) => Some(error),
        };
        let error = error.expect("an error").to_string();
        assert!(error.contains("L1"), "{error}");
        Ok(())
    })
}
