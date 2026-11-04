use super::host::Db;
use crate::state::{blobs::KV_PREFIX, kv::KvStore, simulation::MemoryKv, Result, StateOptions};
use bytes::Bytes;
use futures::{
    channel::oneshot,
    future::{select, BoxFuture, Either},
    FutureExt,
};
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

struct ControlledKv {
    inner: MemoryKv,
    fail_reads: AtomicBool,
    gate: Mutex<Option<oneshot::Sender<()>>>,
}

impl KvStore for ControlledKv {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move {
            if key.starts_with(KV_PREFIX) {
                if self.fail_reads.load(Ordering::SeqCst) {
                    return Err(io::Error::other("injected pack-read failure"));
                }
                let signal = self.gate.lock().unwrap().take();
                if let Some(signal) = signal {
                    let _ = signal.send(());
                    futures::future::pending::<()>().await;
                }
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

async fn populated() -> Result<Arc<ControlledKv>> {
    let store = Arc::new(ControlledKv {
        inner: MemoryKv::default(),
        fail_reads: AtomicBool::new(false),
        gate: Mutex::new(None),
    });
    // A page per frame, so reading `t` reads a frame of its own.
    let options = StateOptions {
        frame_pages: 1,
        ..StateOptions::default()
    };
    let db = Db::open(store.clone(), options).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
        .await?;
    db.execute("INSERT INTO t VALUES(1,10)").await?;
    db.checkpoint().await?;
    db.save_checkpointed().await?;
    Ok(store)
}

/// Pack reads happen during the query, not at open.
fn no_prefetch() -> StateOptions {
    StateOptions {
        prefetch_pages: 0,
        ..StateOptions::default()
    }
}

#[test]
fn backend_read_error_reaches_caller_and_saved_state_survives() -> Result<()> {
    futures::executor::block_on(async {
        let store = populated().await?;
        // Open reads the schema page; the query reads the table's.
        let db = Db::open(store.clone(), no_prefetch()).await?;
        store.fail_reads.store(true, Ordering::SeqCst);
        match db.query("SELECT n FROM t").await {
            Ok(_) => panic!("expected injected failure"),
            Err(e) => assert!(e.to_string().contains("injected pack-read failure")),
        }
        drop(db);
        store.fail_reads.store(false, Ordering::SeqCst);
        let db = Db::open(store, StateOptions::default()).await?;
        assert_eq!(db.query("SELECT n FROM t").await?[0][0].to_string(), "10");
        Ok(())
    })
}

#[test]
fn cancellation_during_pending_kv_read_poison_session_without_publishing() -> Result<()> {
    futures::executor::block_on(async {
        let store = populated().await?;
        let db = Db::open(store.clone(), no_prefetch()).await?;
        let (started, waiting) = oneshot::channel();
        *store.gate.lock().unwrap() = Some(started);
        store.inner.reset_stats();
        // The host can poll another future while the engine waits for a page.
        // Cancel precisely after the backend has entered a pending read.
        match select(db.execute("UPDATE t SET n=99").boxed(), waiting).await {
            Either::Left(_) => panic!("query unexpectedly finished before its gated page read"),
            Either::Right((signal, pending_query)) => {
                signal?;
                drop(pending_query);
            }
        }
        assert!(db
            .query("SELECT 1")
            .await
            .unwrap_err()
            .to_string()
            .contains("the session ended"));
        drop(db);
        assert_eq!(store.inner.stats().puts, 0);
        let db = Db::open(store, StateOptions::default()).await?;
        assert_eq!(db.query("SELECT n FROM t").await?[0][0].to_string(), "10");
        Ok(())
    })
}
