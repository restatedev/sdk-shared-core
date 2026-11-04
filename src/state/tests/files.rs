//! Paged files: lengths and holes, across flushes.
use crate::state::{simulation::*, Result, State};
use std::sync::Arc;

fn page(n: u8) -> crate::state::Page {
    vec![n; 4096].into()
}

/// The byte a page of [`page`] repeats, for readable assertions.
async fn byte(file: &crate::state::File, n: u64) -> Result<Option<u8>> {
    Ok(file.read(n).await?.map(|page| page[0]))
}

/// A file shrunk twice in one mutation, then grown back: once flushed, every
/// page past the shorter length reads as zeros, also those only the first
/// shrink cut.
#[test]
fn shrinking_twice_in_a_mutation_leaves_no_page_behind() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        let file = state.file();
        let mut mutation = state.mutation();
        for n in 0..8 {
            mutation.write(n, Some(page(n as u8 + 1)))?;
        }
        mutation.flush().await?;
        let mut mutation = state.mutation();
        mutation.set_len(6 * 4096)?;
        mutation.set_len(2 * 4096)?;
        mutation.set_len(8 * 4096)?;
        assert_eq!(
            byte(&file, 7).await?,
            Some(8),
            "as flushed, until the mutation is"
        );
        mutation.flush().await?;
        for n in 2..8 {
            assert_eq!(byte(&file, n).await?, None, "page {n}");
        }
        let reopened = State::open(store).await?;
        let file = reopened.file();
        for n in 2..8 {
            assert_eq!(byte(&file, n).await?, None, "page {n}, reopened");
        }
        assert_eq!(byte(&file, 1).await?, Some(2));
        Ok(())
    })
}

/// A cold sequential read of 1500 pages (as many frames), all in the tail:
/// every page reads as written, decoded from the tail the open fetched,
/// without a round trip per page.
#[test]
fn a_long_sequential_read_returns_every_page() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let options = || crate::state::StateOptions {
            frame_pages: 1,
            ..crate::state::StateOptions::default()
        };
        let state = State::open(Arc::new(memory.clone()))
            .options(options())
            .await?;
        let mut mutation = state.mutation();
        for n in 0..1500 {
            mutation.write(n, Some(page(n as u8 | 1)))?;
        }
        mutation.flush().await?;
        let store = Arc::new(LatencyKv::new(memory));
        let state = State::open(store.clone()).options(options()).await?;
        let file = state.file();
        for n in 0..1500 {
            assert_eq!(byte(&file, n).await?, Some(n as u8 | 1), "page {n}");
        }
        // The open fetched the tail, which holds every page.
        assert!(store.round_trips() < 1500 / 12, "{}", store.round_trips());
        Ok(())
    })
}

/// A flush without a cache gets nothing: the open before it got the head,
/// then the manifest and the whole tail (400 pages), together, in
/// one round trip; the flush is one more.
#[test]
fn a_cold_flush_gets_nothing_the_open_fetched_the_tail() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let state = State::open(Arc::new(memory.clone())).await?;
        let mut mutation = state.mutation();
        for n in 0..400 {
            mutation.write(n, Some(page(n as u8 | 1)))?;
        }
        mutation.flush().await?;
        let store = Arc::new(LatencyKv::new(memory));
        let state = State::open(store.clone()).await?;
        assert_eq!(store.round_trips(), 2);
        let file = state.file();
        store.reset();
        let mut mutation = state.mutation();
        for n in (0..400).step_by(37) {
            mutation.write(n, Some(page(2)))?;
        }
        mutation.flush().await?;
        assert_eq!(store.round_trips(), 1);
        assert_eq!(byte(&file, 37).await?, Some(2));
        Ok(())
    })
}

/// Pages that do not compress, made from `seed`.
fn noise(seed: u64, pages: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..pages * 4096)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// Small changes to a page, a checkpoint each flush, patch its frame rather
/// than rewrite it, and the tail compresses the next change against the
/// patched page; a frame rewritten whole is rebuilt, dropping its patches.
/// The file reads the same in the session and reopened, small enough to be
/// fetched whole on open or not.
#[test]
fn checkpoints_patch_frames_a_few_pages_change() -> Result<()> {
    futures::executor::block_on(async {
        for pages in [8, 64] {
            let store = Arc::new(MemoryKv::default());
            let options = crate::state::StateOptions {
                checkpoint_entries: 1,
                ..Default::default()
            };
            let state = State::open(store.clone()).options(options.clone()).await?;
            let file = state.file();
            let mut image = noise(1, pages);
            let mut reports = Vec::new();
            for round in 0..12 {
                let at = 5 * 4096 + round * 8;
                image[at..at + 8].copy_from_slice(&[round as u8; 8]);
                if round == 6 {
                    image[4 * 4096..8 * 4096].copy_from_slice(&noise(2, 4));
                }
                let mut mutation = state.mutation();
                for page in 0..pages as u64 {
                    let range = page as usize * 4096..(page as usize + 1) * 4096;
                    if round == 0 || (4..8).contains(&page) && (round == 6 || page == 5) {
                        mutation.write(page, Some(image[range].to_vec().into()))?;
                    }
                }
                reports.push(mutation.flush().await?);
                let reopened = State::open(store.clone()).options(options.clone()).await?;
                for file in [&file, &reopened.file()] {
                    for page in 0..pages as u64 {
                        let range = page as usize * 4096..(page as usize + 1) * 4096;
                        let read = file.read(page).await?.expect("a page");
                        assert!(
                            read[..] == image[range],
                            "{pages} pages, round {round}, page {page}"
                        );
                    }
                }
            }
            let shape: Vec<Option<(usize, usize)>> = (reports.iter())
                .map(|report| (report.checkpoint.as_ref()).map(|c| (c.frames, c.patched)))
                .collect();
            // A checkpoint holds the flushes before its own: the one at
            // round 1 the file, the one at round 7 the frame round 6 rewrote.
            let mut expected = vec![None, Some((pages / 4, 0))];
            expected.extend([Some((0, 1)); 5]);
            expected.push(Some((1, 0)));
            expected.extend([Some((0, 1)); 4]);
            assert_eq!(shape, expected, "{pages} pages");
            for (round, report) in reports.iter().enumerate().skip(2) {
                if round != 6 {
                    assert!(
                        report.entry_bytes < 200,
                        "{pages} pages, round {round}: {report:?}"
                    );
                }
            }
        }
        Ok(())
    })
}

/// Every page of `file` up to `pages`, zeros for holes.
async fn contents(file: &crate::state::File, pages: u64) -> Result<Vec<u8>> {
    let mut contents = Vec::new();
    for n in 0..pages {
        match file.read(n).await? {
            Some(page) => contents.extend_from_slice(&page),
            None => contents.extend_from_slice(&[0; 4096]),
        }
    }
    Ok(contents)
}

/// Write `image`'s pages that differ from `before` in one flush.
async fn flush_changes(
    state: &State,
    image: &[u8],
    before: &[u8],
) -> Result<crate::state::FlushReport> {
    let mut mutation = state.mutation();
    for (n, (now, then)) in image.chunks(4096).zip(before.chunks(4096)).enumerate() {
        if now != then {
            mutation.write(n as u64, Some(now.to_vec().into()))?;
        }
    }
    mutation.flush().await
}

/// No checkpoint by count or size: only the tail.
fn tail_only() -> crate::state::StateOptions {
    crate::state::StateOptions {
        checkpoint_entries: 0,
        checkpoint_bytes: 0,
        ..Default::default()
    }
}

/// A page changed a little at every flush costs a flush its change alone,
/// not all it changed since the checkpoint: the tail is the chain of its
/// changes, each against the page before it.
#[test]
fn a_page_rewritten_flush_after_flush_costs_only_its_change() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).options(tail_only()).await?;
        let file = state.file();
        let mut image = noise(1, 1);
        flush_changes(&state, &image, &[0; 4096]).await?;
        for round in 0..64 {
            let before = image.clone();
            let at = round * 61;
            image[at..at + 8].copy_from_slice(&noise(round as u64 + 2, 1)[..8]);
            let report = flush_changes(&state, &image, &before).await?;
            assert!(report.checkpoint.is_none());
            assert!(report.entry_bytes < 200, "round {round}: {report:?}");
        }
        assert_eq!(contents(&file, 1).await?, image);
        let reopened = State::open(store).options(tail_only()).await?;
        assert_eq!(contents(&reopened.file(), 1).await?, image);
        Ok(())
    })
}

/// A page cut by a shrink, or written as a hole, and written again after,
/// builds on a hole, not on what the checkpoint held there.
#[test]
fn a_page_rewritten_after_a_shrink_or_a_hole_starts_from_a_hole() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).options(tail_only()).await?;
        let file = state.file();
        let mut image = noise(1, 8);
        flush_changes(&state, &image, &[0; 8 * 4096]).await?;
        let mut mutation = state.mutation();
        mutation.checkpoint();
        mutation.write(0, Some(page(1)))?;
        assert!(mutation.flush().await?.checkpoint.is_some());
        image[..4096].copy_from_slice(&page(1));
        // Page 6 cut, page 2 a hole.
        let mut mutation = state.mutation();
        mutation.set_len(4 * 4096)?;
        mutation.write(2, None)?;
        mutation.flush().await?;
        image[4 * 4096..].fill(0);
        image[2 * 4096..3 * 4096].fill(0);
        for round in 0..2u8 {
            let mut mutation = state.mutation();
            for n in [2, 6] {
                let at = n * 4096 + 100 * round as usize;
                image[at..at + 8].fill(round + 7);
                let range = n * 4096..(n + 1) * 4096;
                mutation.write(n as u64, Some(image[range].to_vec().into()))?;
            }
            mutation.flush().await?;
            assert_eq!(contents(&file, 8).await?, image, "round {round}");
            let reopened = State::open(store.clone()).options(tail_only()).await?;
            assert_eq!(
                contents(&reopened.file(), 8).await?,
                image,
                "round {round}, reopened"
            );
        }
        Ok(())
    })
}

/// A page rewritten across checkpoints: each checkpoint holds the page as
/// the chain left it, and the chain starts over on it.
#[test]
fn the_chain_starts_over_at_a_checkpoint() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let options = crate::state::StateOptions {
            checkpoint_entries: 4,
            checkpoint_bytes: 0,
            ..Default::default()
        };
        let state = State::open(store.clone()).options(options.clone()).await?;
        let file = state.file();
        let mut image = noise(1, 6);
        flush_changes(&state, &image, &[0; 6 * 4096]).await?;
        let mut checkpoints = 0;
        for round in 0..12 {
            let before = image.clone();
            let at = 4096 + round * 97;
            image[at..at + 8].copy_from_slice(&noise(round as u64 + 2, 1)[..8]);
            let report = flush_changes(&state, &image, &before).await?;
            checkpoints += report.checkpoint.is_some() as usize;
            let reopened = State::open(store.clone()).options(options.clone()).await?;
            assert_eq!(contents(&reopened.file(), 6).await?, image, "round {round}");
        }
        assert_eq!(checkpoints, 3);
        assert_eq!(contents(&file, 6).await?, image);
        Ok(())
    })
}
