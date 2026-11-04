//! An in-memory [`BlobStore`]: S3, as tests need it, failures included.
//!
//! A request to S3 may fail before it does anything, do its work and lose
//! the response, or be in flight when its caller gives up on it, and land
//! later: after the attempt that sent it failed, while a retry, a zombie or
//! a garbage collection runs. [`MemoryBlobs::faults`] makes requests fail in each of
//! these ways; a write whose future is dropped once sent lands later too.
//! What lands later waits for [`MemoryBlobs::land_late`].
use crate::state::blobs::BlobStore;
use crate::state::yield_now;
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{
    collections::BTreeMap,
    io,
    ops::Range,
    sync::{Arc, Mutex},
};

/// Blobs in memory, each durable once its request returns; clones share
/// them.
#[derive(Clone, Default)]
pub struct MemoryBlobs {
    inner: Arc<Mutex<Inner>>,
}

/// Random faults ([`MemoryBlobs::faults`]): one request in `one_in` fails.
/// A failed get or list just fails; a failed put or delete is lost, done
/// before it fails, or left to land later, in turn at random.
#[derive(Clone, Copy, Debug)]
pub struct BlobFaults {
    pub one_in: u64,
    pub seed: u64,
}

/// How a faulty write ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// It fails, and stores nothing.
    Lost,
    /// It stores, and its response is lost.
    Done,
    /// It fails now, and lands later.
    Late,
}

/// A write: a put, or a delete when the value is `None`.
type Write = (String, Option<Bytes>);

#[derive(Default)]
struct Inner {
    values: BTreeMap<String, Bytes>,
    puts: u64,
    /// Bytes the puts stored.
    uploaded: u64,
    gets: u64,
    lists: u64,
    /// Puts of a blob key that held a value.
    /// Puts left to accept before the store fails every put.
    puts_before_failing: Option<u64>,
    faults: Option<BlobFaults>,
    /// Requests failed, by the faults.
    failed: u64,
    /// Writes sent that have not landed yet.
    late: Vec<Write>,
    /// Writes that landed late.
    landed_late: u64,
}

impl Inner {
    /// The fault of the next request, if it fails.
    fn fault(&mut self) -> Option<Fault> {
        let faults = self.faults.as_mut()?;
        faults.seed = faults
            .seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let draw = faults.seed >> 33;
        if draw % faults.one_in.max(1) != 0 {
            return None;
        }
        self.failed += 1;
        Some([Fault::Lost, Fault::Done, Fault::Late][(draw / faults.one_in.max(1) % 3) as usize])
    }

    fn apply(&mut self, (key, value): Write) {
        match value {
            Some(value) => {
                self.puts += 1;
                self.uploaded += value.len() as u64;
                self.values.insert(key, value);
            }
            None => {
                self.values.remove(&key);
            }
        }
    }
}

/// A write sent and not yet done: dropped, it lands later.
struct Sent {
    inner: Arc<Mutex<Inner>>,
    write: Option<Write>,
}

impl Drop for Sent {
    fn drop(&mut self) {
        if let Some(write) = self.write.take() {
            self.inner.lock().unwrap().late.push(write);
        }
    }
}

fn injected(what: &str, key: &str) -> io::Error {
    io::Error::other(format!("injected blob {what} failure at {key}"))
}

impl MemoryBlobs {
    /// An independent copy of what is stored, without the faults or what is
    /// in flight: for crash simulations.
    pub fn fork(&self) -> Self {
        let values = self.inner.lock().unwrap().values.clone();
        let fork = Self::default();
        fork.inner.lock().unwrap().values = values;
        fork
    }

    pub fn keys(&self) -> Vec<String> {
        self.inner.lock().unwrap().values.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes `range` of `key`, as stored: no request, so no fault.
    pub fn peek(&self, key: &str, range: Range<u64>) -> Option<Bytes> {
        let inner = self.inner.lock().unwrap();
        let value = inner.values.get(key)?;
        let range = range.start as usize..range.end as usize;
        (range.end <= value.len()).then(|| value.slice(range))
    }

    /// Puts that stored a value, late ones included.
    pub fn puts(&self) -> u64 {
        self.inner.lock().unwrap().puts
    }

    /// Bytes the puts stored.
    pub fn uploaded(&self) -> u64 {
        self.inner.lock().unwrap().uploaded
    }

    /// Each key stored, and its size.
    pub fn sizes(&self) -> Vec<(String, usize)> {
        let inner = self.inner.lock().unwrap();
        inner
            .values
            .iter()
            .map(|(key, value)| (key.clone(), value.len()))
            .collect()
    }

    /// Bytes stored now.
    pub fn stored_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .values
            .values()
            .map(Bytes::len)
            .sum()
    }

    /// Gets (of a range) asked, failed ones included.
    pub fn gets(&self) -> u64 {
        self.inner.lock().unwrap().gets
    }

    /// Listings asked, failed ones included.
    pub fn lists(&self) -> u64 {
        self.inner.lock().unwrap().lists
    }

    /// Accept `puts` more puts, then fail every put; `None` accepts them all.
    pub fn fail_after(&self, puts: Option<u64>) {
        self.inner.lock().unwrap().puts_before_failing = puts;
    }

    /// Fail requests at random from now on; `None` stops.
    pub fn faults(&self, faults: Option<BlobFaults>) {
        self.inner.lock().unwrap().faults = faults;
    }

    /// Requests the faults failed.
    pub fn failed(&self) -> u64 {
        self.inner.lock().unwrap().failed
    }

    /// Writes that landed late.
    pub fn landed_late(&self) -> u64 {
        self.inner.lock().unwrap().landed_late
    }

    /// Land every write still in flight, in the order sent.
    pub fn land_late(&self) {
        let mut inner = self.inner.lock().unwrap();
        for write in std::mem::take(&mut inner.late) {
            inner.landed_late += 1;
            inner.apply(write);
        }
    }

    /// A put or a delete: sent at its first poll, done at its second, unless
    /// it fails; dropped in between, it lands later.
    fn write(&self, write: Write) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            let key = write.0.clone();
            let mut sent = Sent {
                inner: self.inner.clone(),
                write: Some(write),
            };
            yield_now().await;
            let write = sent.write.take().unwrap();
            let mut inner = self.inner.lock().unwrap();
            let what = if write.1.is_some() { "put" } else { "delete" };
            if let (true, Some(left)) = (write.1.is_some(), &mut inner.puts_before_failing) {
                if *left == 0 {
                    return Err(io::Error::other("the blob store is down"));
                }
                *left -= 1;
            }
            match inner.fault() {
                None => {
                    inner.apply(write);
                    Ok(())
                }
                Some(Fault::Lost) => Err(injected(what, &key)),
                Some(Fault::Done) => {
                    inner.apply(write);
                    Err(injected(what, &key))
                }
                Some(Fault::Late) => {
                    inner.late.push(write);
                    Err(injected(what, &key))
                }
            }
        })
    }
}

impl BlobStore for MemoryBlobs {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move {
            yield_now().await;
            let mut inner = self.inner.lock().unwrap();
            inner.gets += 1;
            if inner.fault().is_some() {
                return Err(injected("get", key));
            }
            let Some(value) = inner.values.get(key) else {
                return Ok(None);
            };
            let range = range.start as usize..range.end as usize;
            match range.end <= value.len() {
                true => Ok(Some(value.slice(range))),
                false => Err(io::Error::other(format!("{key} is shorter than {range:?}"))),
            }
        })
    }

    fn put<'a>(&'a self, key: &'a str, value: Bytes) -> BoxFuture<'a, io::Result<()>> {
        self.write((key.to_owned(), Some(value)))
    }

    fn delete<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            for key in keys {
                self.write((key.clone(), None)).await?;
            }
            Ok(())
        })
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<String>>> {
        Box::pin(async move {
            yield_now().await;
            let mut inner = self.inner.lock().unwrap();
            inner.lists += 1;
            if inner.fault().is_some() {
                return Err(injected("list", prefix));
            }
            Ok((inner.values.keys())
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        })
    }
}
