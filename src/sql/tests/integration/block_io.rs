use crate::sql::io::{KvIo, MountedIo, BLOCK_SIZE, DB_PATH};
use crate::state::{simulation::MemoryKv, Result};
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc,
};
use turso_core::{Buffer, Completion, OpenFlags, IO};

#[test]
fn one_driver_at_a_time_in_submission_order() -> Result<()> {
    futures::executor::block_on(async {
        let io = KvIo::mount(Arc::new(MemoryKv::default())).await?;
        let file = MountedIo(io.clone()).open_file(DB_PATH, OpenFlags::Create, false)?;
        let mut writes = Vec::new();
        for byte in [1, 2] {
            let buffer = Arc::new(Buffer::new_temporary(1));
            buffer.as_mut_slice()[0] = byte;
            writes.push(file.pwrite(0, buffer, Completion::new_write(|_| {}))?);
        }
        let mut first = Box::pin(io.drive());
        assert!(futures::poll!(first.as_mut()).is_pending());
        // Only the call holding the session drives: a second is refused.
        assert!(io.drive().await.is_err());
        first.await?;
        assert!(writes.iter().all(Completion::succeeded));
        let output = Arc::new(Buffer::new_temporary(1));
        let read = file.pread(0, Completion::new_read(output.clone(), |_| None))?;
        io.drive().await?;
        assert!(read.succeeded());
        assert_eq!(output.as_slice(), &[2], "later submitted write must win");
        Ok(())
    })
}

#[test]
fn pending_count_includes_request_suspended_in_driver() -> Result<()> {
    futures::executor::block_on(async {
        let io = KvIo::mount(Arc::new(MemoryKv::default())).await?;
        let file = MountedIo(io.clone()).open_file(DB_PATH, OpenFlags::Create, false)?;
        let buffer = Arc::new(Buffer::new_temporary(1));
        let write = file.pwrite(0, buffer, Completion::new_write(|_| {}))?;
        let mut driver = Box::pin(io.drive());
        assert!(futures::poll!(driver.as_mut()).is_pending());
        assert_eq!(io.pending_count(), 1, "dequeued I/O is still in flight");
        driver.await?;
        assert!(write.succeeded());
        assert_eq!(io.pending_count(), 0);
        Ok(())
    })
}

#[test]
fn deferred_unaligned_writes_short_reads_and_truncate_regrow() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let io = KvIo::mount(store).await?;
        let file = MountedIo(io.clone()).open_file(DB_PATH, OpenFlags::Create, false)?;
        let bytes = Arc::new(Buffer::new_temporary(20));
        bytes.as_mut_slice().copy_from_slice(&[7; 20]);
        let written = file.pwrite(
            (BLOCK_SIZE - 6) as u64,
            bytes,
            Completion::new_write(|_| {}),
        )?;
        assert!(!written.finished(), "submission must not complete inline");
        io.drive().await?;
        assert!(written.finished());

        let output = Arc::new(Buffer::new_temporary(BLOCK_SIZE + 30));
        output.as_mut_slice().fill(99);
        let count = Arc::new(AtomicI32::new(-1));
        let captured = count.clone();
        let read = file.pread(
            0,
            Completion::new_read(output.clone(), move |r| {
                if let Ok((_, n)) = r {
                    captured.store(n, Ordering::SeqCst);
                }
                None
            }),
        )?;
        assert!(!read.finished());
        io.drive().await?;
        assert_eq!(count.load(Ordering::SeqCst), (BLOCK_SIZE + 14) as i32);
        assert!(output.as_slice()[..BLOCK_SIZE - 6].iter().all(|&b| b == 0));
        assert_eq!(
            &output.as_slice()[BLOCK_SIZE - 6..BLOCK_SIZE + 14],
            &[7; 20]
        );
        assert!(output.as_slice()[BLOCK_SIZE + 14..].iter().all(|&b| b == 0));

        let truncated = file.truncate((BLOCK_SIZE - 3) as u64, Completion::new_trunc(|_| {}))?;
        io.drive().await?;
        assert!(truncated.finished());
        let grown = file.truncate((BLOCK_SIZE + 30) as u64, Completion::new_trunc(|_| {}))?;
        io.drive().await?;
        assert!(grown.succeeded());
        let reread = file.pread(0, Completion::new_read(output.clone(), |_| None))?;
        io.drive().await?;
        assert!(reread.succeeded());
        assert_eq!(&output.as_slice()[BLOCK_SIZE - 6..BLOCK_SIZE - 3], &[7; 3]);
        assert!(output.as_slice()[BLOCK_SIZE - 3..].iter().all(|&b| b == 0));
        Ok(())
    })
}

#[test]
fn vectored_write_retains_data_and_completion() -> Result<()> {
    futures::executor::block_on(async {
        let io = KvIo::mount(Arc::new(MemoryKv::default())).await?;
        let file = MountedIo(io.clone()).open_file(DB_PATH, OpenFlags::Create, false)?;
        let a = Arc::new(Buffer::new_temporary(3));
        a.as_mut_slice().copy_from_slice(b"abc");
        let b = Arc::new(Buffer::new_temporary(3));
        b.as_mut_slice().copy_from_slice(b"def");
        let c = file.pwritev(4094, vec![a, b], Completion::new_write(|_| {}))?;
        assert!(!c.finished());
        io.drive().await?;
        assert!(c.succeeded());
        let out = Arc::new(Buffer::new_temporary(6));
        let read = file.pread(4094, Completion::new_read(out.clone(), |_| None))?;
        io.drive().await?;
        assert!(read.succeeded());
        assert_eq!(out.as_slice(), b"abcdef");
        Ok(())
    })
}
