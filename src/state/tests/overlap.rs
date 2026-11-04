//! Two states of one owner, flushing at once.
use crate::state::{kv::KvStore, simulation::*, Result, State};
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use std::{
    io,
    sync::{Arc, Mutex},
};

/// A store whose syncs wait until released.
struct Gated {
    inner: MemoryKv,
    gate: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
}

impl KvStore for Gated {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.inner.put(key, value)
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.inner.delete(key)
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        let gate = self.gate.lock().unwrap().take();
        async move {
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            self.inner.sync().await
        }
        .boxed()
    }
}

/// A flush waits on its sync while another state of the owner, opened on
/// the same version, flushes: the second is refused, as it would replace
/// the first's record.
#[test]
fn a_flush_on_top_of_a_running_one_is_refused() -> Result<()> {
    futures::executor::block_on(async {
        let gated = Arc::new(Gated {
            inner: MemoryKv::default(),
            gate: Mutex::new(None),
        });
        let store: Arc<dyn KvStore> = gated.clone();
        let cache = fresh_namespace();
        let open = || State::open(store.clone()).cached(&cache.cache, &cache.key);
        let first = open().await?;
        let second = open().await?;
        let (release, gate) = futures::channel::oneshot::channel();
        *gated.gate.lock().unwrap() = Some(gate);
        let mut mutation = first.mutation();
        mutation.set_record("outbox", "first".into())?;
        let mut running = Box::pin(mutation.flush());
        assert!((&mut running).now_or_never().is_none(), "waits on its sync");
        let mut mutation = second.mutation();
        mutation.set_record("outbox", "second".into())?;
        let refused = mutation.flush().await;
        release.send(()).unwrap();
        running.await?;
        assert!(
            refused.is_err(),
            "the second flush replaced the first's record"
        );
        Ok(())
    })
}
