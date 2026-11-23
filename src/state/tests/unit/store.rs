use super::*;
use crate::state::{simulation::MemoryKv, StateOptions};
use std::cell::Cell;

thread_local! {
    /// Pages decoded by this thread: a test counts what a read costs.
    pub(super) static DECODES: Cell<usize> = const { Cell::new(0) };
}

/// Pages decoded by this thread so far.
fn decodes() -> usize {
    DECODES.with(Cell::get)
}

/// A chain's count of kept images, for tests that use one chain.
fn counter() -> AtomicUsize {
    AtomicUsize::new(0)
}

fn page(byte: u8) -> Page {
    vec![byte; PAGE as usize].into()
}

/// `image`, as the change after `before`.
fn change(image: &Page, before: &Page) -> Option<Bytes> {
    Some(encode_page(image, before).unwrap())
}

#[test]
fn a_chain_applies_its_changes_in_order() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let (mut chain, memoed) = (Chain::default(), counter());
    assert_eq!(
        chain.fold(checkpointed.clone(), &hole, &memoed)?,
        checkpointed
    );
    let mut before = checkpointed.clone();
    for byte in 2..6 {
        let mut image = before.to_vec();
        image[byte as usize * 100] = byte;
        let image = Page::from(image);
        chain.push(change(&image, &before), &memoed);
        assert_eq!(chain.fold(checkpointed.clone(), &hole, &memoed)?, image);
        before = image;
    }
    assert!(!chain.starts_over());
    Ok(())
}

#[test]
fn a_hole_starts_a_chain_over() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let (mut chain, memoed) = (Chain::default(), counter());
    chain.push(change(&page(2), &checkpointed), &memoed);
    chain.push(None, &memoed);
    assert!(chain.starts_over());
    assert_eq!(
        chain.deltas.len(),
        1,
        "what came before the hole is dropped"
    );
    assert_eq!(chain.fold(checkpointed.clone(), &hole, &memoed)?, hole);
    chain.push(change(&page(3), &hole), &memoed);
    assert_eq!(chain.fold(checkpointed, &hole, &memoed)?, page(3));
    Ok(())
}

/// A shrink drops the chains past it and cuts the checkpoint's pages there;
/// a page written after it builds on a hole, not the checkpoint's page.
#[test]
fn a_file_cut_builds_on_holes_past_the_cut() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let mut file = FileState {
        size: 8 * PAGE,
        ..FileState::default()
    };
    let changed = |size, pages: Vec<(u64, Option<Bytes>)>| FileChange { size, pages };
    file.apply(&changed(
        8 * PAGE,
        vec![(6, change(&page(6), &checkpointed))],
    ));
    assert!(file.on_checkpoint(6));
    assert_eq!(file.image(6, checkpointed.clone(), &hole)?, page(6));
    file.apply(&changed(4 * PAGE, vec![]));
    assert!(!file.pages.contains_key(&6));
    assert!(!file.on_checkpoint(6) && file.on_checkpoint(3));
    assert_eq!(file.image(6, checkpointed.clone(), &hole)?, hole);
    file.apply(&changed(8 * PAGE, vec![(6, change(&page(7), &hole))]));
    assert_eq!(file.image(6, checkpointed.clone(), &hole)?, page(7));
    assert_eq!(file.image(3, checkpointed.clone(), &hole)?, checkpointed);
    Ok(())
}

/// A page that differs from [`page`]s and from other seeds.
fn varied(seed: u8) -> Page {
    (0..PAGE as usize)
        .map(|at| (at as u8).wrapping_mul(seed | 1) ^ seed)
        .collect::<Vec<u8>>()
        .into()
}

/// What `chain` folds to with no image kept: a fold of its own.
fn fresh(chain: &Chain, before: &Page, hole: &Page) -> io::Result<Page> {
    let copy = Chain {
        deltas: chain.deltas.clone(),
        memo: OnceLock::new(),
    };
    copy.fold(before.clone(), hole, &counter())
}

#[test]
fn the_kept_image_equals_a_fresh_fold_after_each_kind_of_change() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let (mut chain, memoed) = (Chain::default(), counter());
    let mut before = checkpointed.clone();
    // A delta, a hole, a delta after the hole, another delta.
    for change_to in [Some(varied(2)), None, Some(varied(3)), Some(varied(4))] {
        let image = change_to.clone().unwrap_or_else(|| hole.clone());
        chain.push(
            change_to.map(|_| encode_page(&image, &before).unwrap()),
            &memoed,
        );
        assert_eq!(chain.fold(checkpointed.clone(), &hole, &memoed)?, image);
        assert!(chain.kept());
        assert_eq!(chain.fold(checkpointed.clone(), &hole, &memoed)?, image);
        assert_eq!(fresh(&chain, &checkpointed, &hole)?, image);
        before = image;
    }
    assert_eq!(memoed.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn a_push_drops_the_kept_image() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let (mut chain, memoed) = (Chain::default(), counter());
    chain.push(change(&varied(2), &checkpointed), &memoed);
    assert_eq!(chain.fold(checkpointed.clone(), &hole, &memoed)?, varied(2));
    assert_eq!(memoed.load(Ordering::Relaxed), 1);
    chain.push(change(&varied(3), &varied(2)), &memoed);
    assert!(!chain.kept());
    assert_eq!(memoed.load(Ordering::Relaxed), 0);
    assert_eq!(chain.fold(checkpointed.clone(), &hole, &memoed)?, varied(3));
    chain.push(None, &memoed);
    assert_eq!(memoed.load(Ordering::Relaxed), 0);
    assert_eq!(chain.fold(checkpointed, &hole, &memoed)?, hole);
    Ok(())
}

#[test]
fn a_chain_without_changes_keeps_no_image() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let (chain, memoed) = (Chain::default(), counter());
    assert_eq!(
        chain.fold(checkpointed.clone(), &hole, &memoed)?,
        checkpointed
    );
    assert!(!chain.kept());
    assert_eq!(memoed.load(Ordering::Relaxed), 0);
    Ok(())
}

/// A page's file, of `pages` pages, each with one change on the checkpoint's
/// page `checkpointed`.
fn file_of(pages: u64, checkpointed: &Page) -> FileState {
    let mut file = FileState {
        size: pages * PAGE,
        ..FileState::default()
    };
    let pages = (0..pages)
        .map(|at| (at, change(&varied(at as u8 ^ 7), checkpointed)))
        .collect();
    file.apply(&FileChange {
        size: file.size,
        pages,
    });
    file
}

#[test]
fn a_clone_keeps_its_own_image() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let file = file_of(2, &checkpointed);
    assert_eq!(file.image(0, checkpointed.clone(), &hole)?, varied(7));
    let mut clone = file.clone();
    assert_eq!(clone.memoed(), 1, "the clone carries the count");
    clone.apply(&FileChange {
        size: 2 * PAGE,
        pages: vec![(0, change(&varied(9), &varied(7)))],
    });
    assert_eq!(clone.memoed(), 0);
    assert_eq!(file.memoed(), 1);
    assert_eq!(clone.image(0, checkpointed.clone(), &hole)?, varied(9));
    assert_eq!(file.image(0, checkpointed.clone(), &hole)?, varied(7));
    assert_eq!(file.recounted(), file.memoed());
    assert_eq!(clone.recounted(), clone.memoed());
    Ok(())
}

/// The changes of a page a flush did not touch are shared with the state before
/// it; one it did touch is copied for the new state alone, and a hole there
/// starts only the new state's chain over.
#[test]
fn a_clone_shares_the_changes_of_the_pages_it_does_not_change() {
    let (hole, checkpointed) = (page(0), page(1));
    let file = file_of(3, &checkpointed);
    let mut clone = file.clone();
    let shared = |a: &FileState, b: &FileState, page: u64| {
        Arc::ptr_eq(&a.pages[&page].deltas, &b.pages[&page].deltas)
    };
    assert!((0..3).all(|page| shared(&file, &clone, page)));
    clone.apply(&FileChange {
        size: 3 * PAGE,
        pages: vec![(0, change(&varied(9), &varied(7))), (1, None)],
    });
    assert!(!shared(&file, &clone, 0) && !shared(&file, &clone, 1));
    assert!(shared(&file, &clone, 2));
    assert_eq!(file.pages[&0].deltas.len(), 1);
    assert_eq!(clone.pages[&0].deltas.len(), 2);
    assert!(!file.pages[&1].starts_over() && clone.pages[&1].starts_over());
    assert_eq!(file.pages[&1].deltas.len(), 1);
    assert_eq!(clone.pages[&1].deltas.len(), 1);
    assert_eq!(
        file.image(1, checkpointed.clone(), &hole).unwrap(),
        varied(1 ^ 7)
    );
    assert_eq!(clone.image(1, checkpointed, &hole).unwrap(), hole);
}

#[test]
fn a_kept_image_survives_a_shrink_and_a_growth_only_where_it_is_right() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let mut file = file_of(6, &checkpointed);
    for at in 0..6 {
        assert_eq!(
            file.image(at, checkpointed.clone(), &hole)?,
            varied(at as u8 ^ 7)
        );
    }
    assert_eq!(file.memoed(), 6);
    let apply = |file: &mut FileState, size, pages| file.apply(&FileChange { size, pages });
    apply(&mut file, 3 * PAGE, vec![]);
    assert_eq!((file.memoed(), file.recounted()), (3, 3));
    apply(&mut file, 6 * PAGE, vec![(4, change(&varied(1), &hole))]);
    assert_eq!(file.image(4, checkpointed.clone(), &hole)?, varied(1));
    assert_eq!(file.image(5, checkpointed.clone(), &hole)?, hole);
    assert_eq!(file.image(2, checkpointed.clone(), &hole)?, varied(2 ^ 7));
    assert_eq!(file.memoed(), file.recounted());
    Ok(())
}

#[test]
fn a_second_fold_decodes_nothing() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let file = file_of(1, &checkpointed);
    let before = decodes();
    assert_eq!(file.image(0, checkpointed.clone(), &hole)?, varied(7));
    assert_eq!(decodes() - before, 1);
    assert_eq!(file.image(0, checkpointed, &hole)?, varied(7));
    assert_eq!(decodes() - before, 1, "the second fold decoded nothing");
    Ok(())
}

#[test]
fn a_file_keeps_the_image_of_at_most_a_cap_of_pages() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let pages = MEMO_PAGES as u64 + 40;
    let file = file_of(pages, &checkpointed);
    for round in 0..2 {
        for at in 0..pages {
            assert_eq!(
                file.image(at, checkpointed.clone(), &hole)?,
                varied(at as u8 ^ 7),
                "page {at}, round {round}"
            );
            assert!(file.memoed() <= MEMO_PAGES);
        }
    }
    assert_eq!(file.memoed(), MEMO_PAGES);
    assert_eq!(file.recounted(), MEMO_PAGES);
    // A page that lost its image makes room for another.
    let mut file = file;
    file.apply(&FileChange {
        size: pages * PAGE,
        pages: vec![(0, change(&varied(1), &varied(7)))],
    });
    assert_eq!(file.memoed(), MEMO_PAGES - 1);
    assert_eq!(file.image(0, checkpointed.clone(), &hole)?, varied(1));
    assert_eq!(
        file.image(pages - 1, checkpointed, &hole)?,
        varied((pages as u8 - 1) ^ 7)
    );
    assert_eq!((file.memoed(), file.recounted()), (MEMO_PAGES, MEMO_PAGES));
    Ok(())
}

/// Write `pages` of `name` in one commit: the state after.
async fn commit_pages(
    snapshot: &Snapshot,
    name: &str,
    size: u64,
    pages: Vec<(u64, Option<Page>)>,
    staged: Option<Staged>,
) -> io::Result<Snapshot> {
    let written = Written {
        size,
        pages: pages.into_iter().collect(),
        sized: true,
    };
    let writes = Writes::from([(name.to_string(), written)]);
    Ok(snapshot.commit(writes, Records::new(), staged).await?.1)
}

async fn open_snapshot() -> io::Result<Snapshot> {
    let tiered = Tiered::new(Arc::new(MemoryKv::default()), None, None);
    Snapshot::open(Arc::new(tiered), StateOptions::default(), None, false).await
}

fn memos(snapshot: &Snapshot, name: &str) -> (usize, usize) {
    let file = &snapshot.durable.files[name];
    (file.memoed(), file.recounted())
}

#[test]
fn a_read_right_after_a_commit_decodes_nothing() -> io::Result<()> {
    futures::executor::block_on(async {
        let snapshot = open_snapshot().await?;
        let before = decodes();
        let snapshot = commit_pages(
            &snapshot,
            "db",
            4 * PAGE,
            vec![(0, Some(varied(1))), (1, Some(varied(2))), (2, None)],
            None,
        )
        .await?;
        assert_eq!(snapshot.read("db", 0).await?, Some(varied(1)));
        assert_eq!(snapshot.read("db", 1).await?, Some(varied(2)));
        assert_eq!(memos(&snapshot, "db"), (2, 2), "the commit keeps no hole");
        assert_eq!(snapshot.read("db", 2).await?, None);
        assert_eq!(decodes() - before, 0);
        assert_eq!(memos(&snapshot, "db"), (3, 3), "a read of it does");
        // A page written again: its change is against the kept image, and
        // the image it was written as is kept in turn.
        let snapshot =
            commit_pages(&snapshot, "db", 4 * PAGE, vec![(0, Some(varied(3)))], None).await?;
        assert_eq!(snapshot.read("db", 0).await?, Some(varied(3)));
        assert_eq!(decodes() - before, 0);
        assert_eq!(memos(&snapshot, "db"), (3, 3));
        Ok(())
    })
}

#[test]
fn a_failed_commit_leaves_the_state_as_it_was() -> io::Result<()> {
    futures::executor::block_on(async {
        let snapshot = open_snapshot().await?;
        let snapshot =
            commit_pages(&snapshot, "db", 2 * PAGE, vec![(0, Some(varied(1)))], None).await?;
        let before = (snapshot.durable.head.version, memos(&snapshot, "db"));
        // A name of another kind: the commit refuses after reading.
        let written = Written {
            size: 2 * PAGE,
            pages: Changes::from([(0, Some(varied(2)))]),
            sized: true,
        };
        let writes = Writes::from([("db".to_string(), written)]);
        let records = Records::from([("db".to_string(), Bytes::from_static(b"x"))]);
        assert!(snapshot.commit(writes, records, None).await.is_err());
        assert_eq!(
            (snapshot.durable.head.version, memos(&snapshot, "db")),
            before
        );
        assert_eq!(snapshot.read("db", 0).await?, Some(varied(1)));
        Ok(())
    })
}

#[test]
fn the_count_matches_a_recount_across_pushes_clones_commits_and_a_checkpoint() -> io::Result<()> {
    futures::executor::block_on(async {
        let pages = MEMO_PAGES as u64 + 30;
        let image = |at: u64, round: u8| varied((at as u8).wrapping_add(round));
        let first = (0..pages).map(|at| (at, Some(image(at, 0)))).collect();
        let snapshot = open_snapshot().await?;
        let snapshot = commit_pages(&snapshot, "db", pages * PAGE, first, None).await?;
        let (count, recount) = memos(&snapshot, "db");
        assert_eq!((count, recount), (MEMO_PAGES, MEMO_PAGES), "the cap");
        // Reads keep nothing past the cap, and are right.
        for at in 0..pages {
            assert_eq!(
                snapshot.read("db", at).await?,
                Some(image(at, 0)),
                "page {at}"
            );
        }
        assert_eq!(memos(&snapshot, "db"), (MEMO_PAGES, MEMO_PAGES));
        // A commit of pages with and without an image.
        let some = (0..10).map(|at| (at, Some(image(at, 1)))).collect();
        let snapshot = commit_pages(&snapshot, "db", pages * PAGE, some, None).await?;
        let (count, recount) = memos(&snapshot, "db");
        assert_eq!(count, recount);
        assert!(count <= MEMO_PAGES);
        // A clone, and a commit on it, leave the first as it was.
        let held = memos(&snapshot, "db");
        let again = (0..3).map(|at| (at, Some(image(at, 2)))).collect();
        let next = commit_pages(&snapshot, "db", pages * PAGE, again, None).await?;
        assert_eq!(memos(&snapshot, "db"), held);
        assert_eq!(memos(&next, "db").0, memos(&next, "db").1);
        // A shrink drops chains and their images.
        let next = commit_pages(&next, "db", 5 * PAGE, vec![], None).await?;
        assert_eq!(memos(&next, "db").0, memos(&next, "db").1);
        assert!(memos(&next, "db").0 <= 5);
        // A checkpoint empties the chains.
        let staged = next.checkpoint().await?;
        let rebased = commit_pages(
            &next,
            "db",
            5 * PAGE,
            vec![(1, Some(image(1, 3)))],
            Some(staged),
        )
        .await?;
        assert_eq!(memos(&rebased, "db"), (1, 1), "the commit's page alone");
        for at in 0..5 {
            let expected = match at {
                1 => image(1, 3),
                _ => image(
                    at,
                    if at < 3 {
                        2
                    } else if at < 10 {
                        1
                    } else {
                        0
                    },
                ),
            };
            assert_eq!(rebased.read("db", at).await?, Some(expected), "page {at}");
        }
        Ok(())
    })
}
