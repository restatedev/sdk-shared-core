use super::*;
use crate::state::{kv::KvStore, simulation::MemoryKv};
use bytes::Bytes;
use futures::{channel::oneshot, future::BoxFuture};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Pack reads held until their gate opens, in the order they come.
struct GatedReads {
    inner: MemoryKv,
    gates: Mutex<VecDeque<oneshot::Receiver<()>>>,
    gated: AtomicUsize,
    active: AtomicUsize,
    maximum: AtomicUsize,
    fail_first: bool,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl KvStore for GatedReads {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move {
            let pack = key.starts_with(crate::state::blobs::KV_PREFIX);
            let gate = pack
                .then(|| self.gates.lock().unwrap().pop_front())
                .flatten();
            if let Some(gate) = gate {
                let first = self.gated.fetch_add(1, Ordering::SeqCst) == 0;
                let n = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                let _active = Active(&self.active);
                self.maximum.fetch_max(n, Ordering::SeqCst);
                gate.await.map_err(io::Error::other)?;
                if self.fail_first && first {
                    return Err(io::Error::other("injected read error"));
                }
                return self.inner.get(key).await;
            }
            self.inner.get(key).await
        })
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.inner.put(key, value)
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.inner.delete(key)
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        self.inner.sync()
    }
}

/// The page read second, far enough from page 0 to be in another part.
const FAR: u64 = 18;

/// `len` bytes that compress to about as many.
fn noise(len: usize) -> Vec<u8> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

#[test]
fn parallel_reads_preserve_barriers_and_retire_on_error_or_cancel() -> crate::sql::Result<()> {
    futures::executor::block_on(async {
        for mode in ["success", "error", "cancel"] {
            let base = Arc::new(MemoryKv::default());
            // A page in a frame each, checkpointed by every publish, and no
            // prefetch at open: the reads under test reach the pack. Pages
            // 0 and `FAR` hold 7s; the noise between puts `FAR`'s frame in
            // another 64 KiB part of the pack than page 0's, so that the
            // two reads are two gets.
            let all = crate::state::StateOptions {
                frame_pages: 1,
                prefetch_pages: 0,
                checkpoint_bytes: 1,
                ..Default::default()
            };
            let initial = KvIo::mount_with_options(base.clone(), all.clone()).await?;
            let mut pages = vec![7; BLOCK_SIZE];
            pages.extend(noise((FAR as usize - 1) * BLOCK_SIZE));
            pages.extend(vec![7; BLOCK_SIZE]);
            initial.write(0, 0, &buffers(&pages)).await?;
            initial.publish().await?;
            let after = (FAR + 1) * BLOCK_SIZE as u64;
            initial
                .write(0, after, &buffers(&vec![7; BLOCK_SIZE]))
                .await?;
            initial.publish().await?;
            let (send_a, recv_a) = oneshot::channel();
            let (send_b, recv_b) = oneshot::channel();
            let store = Arc::new(GatedReads {
                inner: base.fork(),
                gates: Mutex::new(VecDeque::new()),
                gated: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                maximum: AtomicUsize::new(0),
                fail_first: mode == "error",
            });
            let io = KvIo::mount_with_options(store.clone(), all).await?;
            // Mounted (its manifest read), the next two pack reads wait.
            store.gates.lock().unwrap().extend([recv_a, recv_b]);
            let file = MountedIo(io.clone()).open_file(DB_PATH, OpenFlags::Create, false)?;
            let outputs: Vec<_> = (0..4)
                .map(|_| Arc::new(Buffer::new_temporary(BLOCK_SIZE)))
                .collect();
            let first = file.pread(0, Completion::new_read(outputs[0].clone(), |_| None))?;
            let second = file.pread(
                FAR * BLOCK_SIZE as u64,
                Completion::new_read(outputs[1].clone(), |_| None),
            )?;
            let changed = Arc::new(Buffer::new_temporary(BLOCK_SIZE));
            changed.as_mut_slice().fill(9);
            let write = file.pwrite(0, changed, Completion::new_write(|_| {}))?;
            let third = file.pread(0, Completion::new_read(outputs[2].clone(), |_| None))?;
            let truncate = file.truncate(0, Completion::new_trunc(|_| {}))?;
            let fourth = file.pread(0, Completion::new_read(outputs[3].clone(), |_| None))?;
            let mut driver = Box::pin(io.drive());
            for _ in 0..8 {
                assert!(futures::poll!(driver.as_mut()).is_pending());
                if store.active.load(Ordering::SeqCst) == 2 {
                    break;
                }
            }
            assert_eq!(store.active.load(Ordering::SeqCst), 2, "reads must overlap");
            assert_eq!(io.pending_count(), 6);
            assert!(!write.finished(), "write must wait for prior reads");
            if mode == "cancel" {
                drop(driver);
                assert!(first.failed() && second.failed());
            } else {
                send_a.send(()).unwrap();
                for _ in 0..8 {
                    assert!(futures::poll!(driver.as_mut()).is_pending());
                    if first.finished() {
                        break;
                    }
                }
                assert!(first.finished());
                assert!(
                    !write.finished(),
                    "a completed/failed first read cannot release the barrier"
                );
                send_b.send(()).unwrap();
                let result = driver.await;
                if mode == "error" {
                    assert!(result.is_err());
                    assert!(first.failed() && second.succeeded());
                    assert!(!write.finished());
                } else {
                    assert_eq!(result?, 6);
                    assert!([&first, &second, &write, &third, &truncate, &fourth]
                        .into_iter()
                        .all(Completion::succeeded));
                    for (output, byte) in outputs.iter().zip([7, 7, 9, 0]) {
                        assert!(output.as_slice().iter().all(|&b| b == byte));
                    }
                }
            }
            assert_eq!(store.active.load(Ordering::SeqCst), 0);
            assert_eq!(store.maximum.load(Ordering::SeqCst), 2);
            io.shutdown();
            assert_eq!(io.pending_count(), 0);
        }
        Ok(())
    })
}

#[test]
fn never_polled_dequeued_read_retires_its_completion() -> crate::sql::Result<()> {
    futures::executor::block_on(async {
        let io = KvIo::mount(Arc::new(MemoryKv::default())).await?;
        let file = MountedIo(io.clone()).open_file(DB_PATH, OpenFlags::Create, false)?;
        let completion = file.pread(
            0,
            Completion::new_read(Arc::new(Buffer::new_temporary(1)), |_| None),
        )?;
        let pending = io.queue.lock().unwrap().pop_front().unwrap();
        let future = io.perform(pending);
        assert!(!completion.finished());
        drop(future);
        assert!(completion.failed());
        assert_eq!(io.pending_count(), 0);
        Ok(())
    })
}
