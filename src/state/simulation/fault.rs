//! Deterministic simulator failpoints. Not a production backend or remote fencing.
use super::MemoryKv;
use crate::state::kv::KvStore;
use crate::state::yield_now;
use bytes::Bytes;
use futures::{channel::oneshot, future::BoxFuture};
use std::future::Future;
use std::{io, sync::Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Get,
    Put,
    Delete,
    Sync,
    /// A listing of the keys ([`KvStore::keys`]); its key is "".
    Keys,
}

#[derive(Clone, Copy, Debug)]
pub enum Effect {
    /// Fail before touching the underlying store: a write is not queued, a
    /// sync stores nothing.
    None,
    /// Complete the underlying operation, then report a failure.
    After,
}

#[derive(Clone, Debug)]
pub struct Failpoint {
    pub operation: Operation,
    pub key_prefix: String,
    /// One-based ordinal among matching operations since arm().
    pub nth: usize,
    pub effect: Effect,
    /// Signal the caller and stay pending until it drops the future (a get or
    /// a sync: a write returns at once).
    pub cancel: bool,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub operation: Operation,
    pub key: String,
    pub faulted: bool,
}

#[derive(Default)]
struct Control {
    plan: Option<Failpoint>,
    seen: usize,
    signal: Option<oneshot::Sender<()>>,
    events: Vec<Event>,
    active: usize,
}

/// Each delayed request remains a local future: dropping it settles it. Real
/// transports may outlive their futures and still require external host fencing.
pub struct FaultKv {
    pub inner: MemoryKv,
    seed: u64,
    control: Mutex<Control>,
}

struct Active<'a>(&'a FaultKv);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.control.lock().unwrap().active -= 1;
    }
}

impl FaultKv {
    pub fn new(inner: MemoryKv, seed: u64) -> Self {
        Self {
            inner,
            seed,
            control: Mutex::new(Control::default()),
        }
    }
    pub fn arm(&self, plan: Failpoint) -> oneshot::Receiver<()> {
        assert!(plan.nth > 0);
        let mut c = self.control.lock().unwrap();
        assert_eq!(c.active, 0, "arm only after requests settle");
        let (tx, rx) = oneshot::channel();
        c.plan = Some(plan);
        c.seen = 0;
        c.signal = Some(tx);
        c.events.clear();
        rx
    }
    pub fn disarm(&self) {
        let mut c = self.control.lock().unwrap();
        assert_eq!(c.active, 0, "requests must settle before recovery");
        c.plan = None;
        c.signal = None;
    }
    pub fn events(&self) -> Vec<Event> {
        self.control.lock().unwrap().events.clone()
    }
    pub fn active(&self) -> usize {
        self.control.lock().unwrap().active
    }

    /// Count `operation` on `key`, and return the failpoint it hits, if any.
    fn hit(&self, operation: Operation, key: &str) -> (Option<Failpoint>, u64) {
        let mut c = self.control.lock().unwrap();
        let matches = c
            .plan
            .as_ref()
            .is_some_and(|p| p.operation == operation && key.starts_with(&p.key_prefix));
        let plan = if matches {
            c.seen += 1;
            c.plan.as_ref().filter(|p| p.nth == c.seen).cloned()
        } else {
            None
        };
        c.events.push(Event {
            operation,
            key: key.into(),
            faulted: plan.is_some(),
        });
        (plan, c.events.len() as u64)
    }

    fn signal(&self) {
        if let Some(tx) = self.control.lock().unwrap().signal.take() {
            let _ = tx.send(());
        }
    }

    fn injected(operation: Operation, key: &str) -> io::Error {
        io::Error::other(format!("injected {operation:?} failure at {key}"))
    }

    /// A get, a sync or a listing: may be delayed, fail, or stay pending
    /// until dropped.
    async fn run<T>(
        &self,
        operation: Operation,
        key: &str,
        request: impl Future<Output = io::Result<T>>,
    ) -> io::Result<T> {
        let (plan, ordinal) = self.hit(operation, key);
        self.control.lock().unwrap().active += 1;
        let _active = Active(self);
        // Stable differing delays perturb concurrent completion order without a
        // wall clock, a runtime dependency, or sleeps.
        let hash = key.bytes().fold(self.seed.wrapping_add(ordinal), |n, b| {
            n.wrapping_mul(31).wrapping_add(b as u64)
        });
        for _ in 0..if self.seed == 0 { 0 } else { 1 + hash % 11 } {
            yield_now().await;
        }
        let Some(plan) = plan else {
            return request.await;
        };
        if let Effect::After = plan.effect {
            request.await?;
        }
        self.signal();
        if plan.cancel {
            futures::future::pending::<()>().await;
        }
        Err(Self::injected(operation, key))
    }

    /// A write: queued at once, unless it hits a failpoint.
    fn write(&self, operation: Operation, key: &str, value: Option<Bytes>) -> io::Result<()> {
        let (plan, _) = self.hit(operation, key);
        let queue = || match value {
            Some(value) => self.inner.put(key, value),
            None => self.inner.delete(key),
        };
        let Some(plan) = plan else {
            return queue();
        };
        if let Effect::After = plan.effect {
            queue()?;
        }
        self.signal();
        Err(Self::injected(operation, key))
    }
}

impl KvStore for FaultKv {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(self.run(Operation::Get, key, self.inner.get(key)))
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.write(Operation::Put, key, Some(value))
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.write(Operation::Delete, key, None)
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(self.run(Operation::Sync, "", self.inner.sync()))
    }
    fn keys(&self) -> BoxFuture<'_, io::Result<Vec<String>>> {
        Box::pin(self.run(Operation::Keys, "", KvStore::keys(&self.inner)))
    }
}
