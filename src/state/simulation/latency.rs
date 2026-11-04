//! Critical-path latency in round trips, measured on a virtual clock.
//!
//! A get, a list or a sync takes exactly one round trip; a write is queued, and costs
//! none. A request issued at virtual time `t` completes at `t + 1`, and the
//! clock advances to the latest completion observed. Requests issued together
//! overlap: a batch of reads costs one round trip, while dependent requests
//! cost one each. Multiply by a backend's RTT to
//! estimate wall time; CPU time, bandwidth and queueing are not modeled.
use crate::state::kv::KvStore;
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{io, sync::Mutex};

pub struct LatencyKv<S> {
    pub inner: S,
    now: Mutex<u64>,
}

impl<S: KvStore> LatencyKv<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            now: Mutex::new(0),
        }
    }

    /// Round trips on the critical path since creation or the last reset.
    pub fn round_trips(&self) -> u64 {
        *self.now.lock().unwrap()
    }

    pub fn reset(&self) {
        *self.now.lock().unwrap() = 0;
    }

    fn timed<'a, T: Send + 'a>(
        &'a self,
        request: BoxFuture<'a, io::Result<T>>,
    ) -> BoxFuture<'a, io::Result<T>> {
        let start = self.round_trips();
        Box::pin(async move {
            let result = request.await;
            let mut now = self.now.lock().unwrap();
            *now = (*now).max(start + 1);
            result
        })
    }
}

impl<S: KvStore> KvStore for LatencyKv<S> {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        self.timed(self.inner.get(key))
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.inner.put(key, value)
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.inner.delete(key)
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        self.timed(self.inner.sync())
    }
    fn keys(&self) -> BoxFuture<'_, io::Result<Vec<String>>> {
        self.timed(KvStore::keys(&self.inner))
    }
}

#[cfg(test)]
#[path = "../tests/unit/latency.rs"]
mod tests;
