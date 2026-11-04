use super::*;
use crate::state::simulation::MemoryKv;

#[test]
fn cached_snapshot_page_never_masks_overlay_wal_or_truncation() -> crate::sql::Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let initial = KvIo::mount(store.clone()).await?;
        initial
            .write(0, 0, &buffers(&vec![7; 2 * BLOCK_SIZE]))
            .await?;
        initial.publish().await?;
        let io = KvIo::mount(store.clone()).await?;
        io.prepare_engine().await?;
        assert_eq!(*io.block(0, 0).await?, vec![7; BLOCK_SIZE]);
        io.write(0, 10, &buffers(&[9, 8])).await?;
        assert_eq!(&io.block(0, 0).await?[10..12], &[9, 8]);
        assert_eq!(*io.block(1, 0).await?, vec![0; BLOCK_SIZE]);
        io.write(1, 0, &buffers(&[3])).await?;
        assert_eq!(io.block(1, 0).await?[0], 3);
        assert_eq!(io.block(0, 0).await?[0], 7);
        assert_eq!(*io.block(0, 1).await?, vec![7; BLOCK_SIZE]);
        io.truncate_file(0, 100).await?;
        io.truncate_file(0, (2 * BLOCK_SIZE) as u64).await?;
        let page = io.block(0, 0).await?;
        assert_eq!(&page[10..12], &[9, 8]);
        assert!(page[100..].iter().all(|&b| b == 0));
        assert_eq!(*io.block(0, 1).await?, vec![0; BLOCK_SIZE]);
        io.truncate_file(1, 0).await?;
        io.publish().await?;
        let reopened = KvIo::mount(store).await?;
        assert_eq!(reopened.block(0, 0).await?, page);
        assert_eq!(*reopened.block(0, 1).await?, vec![0; BLOCK_SIZE]);
        Ok(())
    })
}

#[test]
fn callbacks_run_on_host_and_shutdown_aborts_queued_requests() -> crate::sql::Result<()> {
    futures::executor::block_on(async {
        for abort in [false, true] {
            let io = KvIo::mount(Arc::new(MemoryKv::default())).await?;
            let file = MountedIo(io.clone()).open_file(DB_PATH, OpenFlags::Create, false)?;
            let called_on = Arc::new(Mutex::new(None));
            let captured = called_on.clone();
            let c = file.pwrite(
                0,
                Arc::new(Buffer::new_temporary(4096)),
                Completion::new_write(move |result| {
                    *captured.lock().unwrap() = Some((std::thread::current().id(), result.is_ok()));
                }),
            )?;
            assert!(!c.finished());
            assert_eq!(io.pending_count(), 1);
            if abort {
                io.shutdown();
                assert!(c.failed());
                assert!(!called_on.lock().unwrap().unwrap().1);
            } else {
                io.drive().await?;
                assert_eq!(
                    *called_on.lock().unwrap(),
                    Some((std::thread::current().id(), true))
                );
                assert!(c.succeeded());
            }
            assert_eq!(io.pending_count(), 0);
        }
        Ok(())
    })
}

#[test]
fn synchronous_engine_waits_use_only_resident_data_or_fail() -> crate::sql::Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let initial = KvIo::mount(store.clone()).await?;
        initial
            .write(0, 0, &buffers(&vec![7; 2 * BLOCK_SIZE]))
            .await?;
        initial.publish().await?;
        let io = KvIo::mount(store.clone()).await?;
        io.prepare_engine().await?;
        let file = MountedIo(io.clone()).open_file(DB_PATH, OpenFlags::Create, false)?;
        store.reset_stats();
        let out = Arc::new(Buffer::new_temporary(BLOCK_SIZE));
        let header = file.pread(0, Completion::new_read(out.clone(), |_| None))?;
        io.step()?;
        assert!(header.succeeded());
        assert!(!io.failed(), "a resident read is no failure");
        assert!(out.as_slice().iter().all(|&b| b == 7));
        assert_eq!(store.stats().gets, 0);
        out.as_mut_slice().fill(9);
        let cold = file.pread(
            BLOCK_SIZE as u64,
            Completion::new_read(out.clone(), |_| None),
        )?;
        assert!(io.step().unwrap_err().to_string().contains("nonresident"));
        assert!(cold.failed());
        // The session must not take the engine's error for an SQL error.
        assert!(io.failed(), "a failed synchronous read is recorded");
        assert!(out.as_slice().iter().all(|&b| b == 9));
        assert_eq!(
            store.stats().gets,
            0,
            "synchronous paths must not start KV requests"
        );
        let write = file.pwrite(0, out, Completion::new_write(|_| {}))?;
        assert!(io
            .step()
            .unwrap_err()
            .to_string()
            .contains("synchronous file mutation"));
        assert!(write.failed());
        assert_eq!(io.pending_count(), 0);
        assert!(io.step().is_err(), "an unserviceable wait must not spin");
        Ok(())
    })
}

/// A leaf table page: `cells` cells of 8 bytes at the end, a freeblock of 16
/// bytes before them, and junk everywhere else.
fn leaf_page(at: usize, cells: usize) -> Vec<u8> {
    let mut page = vec![0xAB; BLOCK_SIZE];
    let content = BLOCK_SIZE - 8 * cells - 16;
    page[at] = 13;
    page[at + 1..at + 3].copy_from_slice(&(content as u16).to_be_bytes()); // freeblock
    page[at + 3..at + 5].copy_from_slice(&(cells as u16).to_be_bytes());
    page[at + 5..at + 7].copy_from_slice(&(content as u16).to_be_bytes());
    page[at + 7] = 0;
    page[content..content + 2].copy_from_slice(&0u16.to_be_bytes()); // last freeblock
    page[content + 2..content + 4].copy_from_slice(&16u16.to_be_bytes());
    for cell in 0..cells {
        let offset = (content + 16 + 8 * cell) as u16;
        page[at + 8 + 2 * cell..at + 10 + 2 * cell].copy_from_slice(&offset.to_be_bytes());
        page[offset as usize..offset as usize + 8].fill(cell as u8 + 1);
    }
    page
}

#[test]
fn a_btree_page_is_written_without_the_bytes_no_btree_reads() {
    for (first, at) in [(false, 0), (true, 100)] {
        let page = leaf_page(at, 3);
        let tidy = tidied(&Page::from(page.clone()), first).expect("a b-tree page");
        let content = BLOCK_SIZE - 3 * 8 - 16;
        // The header and cell pointers, the freeblock's header and the cells
        // stay; the space between and the freeblock's body are zeros.
        assert_eq!(tidy[..at + 8 + 6], page[..at + 8 + 6]);
        assert!(tidy[at + 8 + 6..content].iter().all(|&b| b == 0));
        assert_eq!(tidy[content..content + 4], page[content..content + 4]);
        assert!(tidy[content + 4..content + 16].iter().all(|&b| b == 0));
        assert_eq!(tidy[content + 16..], page[content + 16..]);
        assert!(tidied(&tidy, first) == Some(tidy.clone()), "tidy already");
    }
}

#[test]
fn a_page_that_is_not_a_btree_page_or_does_not_add_up_is_left_as_is() {
    // An overflow or freelist page: it starts with a page number.
    let mut overflow = vec![0xAB; BLOCK_SIZE];
    overflow[..4].copy_from_slice(&7u32.to_be_bytes());
    assert!(tidied(&Page::from(overflow), false).is_none());
    // More cell pointers than fit before the cells.
    let mut crowded = leaf_page(0, 3);
    crowded[3..5].copy_from_slice(&2100u16.to_be_bytes());
    assert!(tidied(&Page::from(crowded), false).is_none());
    // A freeblock past the page's end.
    let mut runaway = leaf_page(0, 3);
    let content = BLOCK_SIZE - 3 * 8 - 16;
    runaway[content + 2..content + 4].copy_from_slice(&4000u16.to_be_bytes());
    assert!(tidied(&Page::from(runaway), false).is_none());
}
