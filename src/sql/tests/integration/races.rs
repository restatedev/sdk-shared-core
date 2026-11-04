//! Races between sessions of one owner that share its cache.
use super::host::Db;
use super::host::StateSetup;
use crate::state::simulation::Namespace;
use crate::state::{
    kv::KvStore,
    simulation::{fresh_namespace, MemoryKv},
    Result, StateOptions,
};
use bytes::Bytes;
use futures::{
    channel::oneshot,
    future::{BoxFuture, FutureExt, Shared},
};
use std::{
    io,
    sync::{Arc, Mutex},
};

fn opts(cache: &Namespace) -> StateSetup {
    StateSetup {
        options: StateOptions::default(),
        cache: Some(cache.clone()),
        packs: None,
    }
}

/// Reads a range of a pack, then holds it until released.
struct GateKv {
    inner: MemoryKv,
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<Shared<oneshot::Receiver<()>>>>,
}
impl KvStore for GateKv {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move {
            let value = self.inner.get(key).await?;
            let gate = match key.starts_with(crate::state::blobs::KV_PREFIX) {
                true => self.release.lock().unwrap().clone(),
                false => None,
            };
            if let Some(gate) = gate {
                if let Some(tx) = self.started.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                let _ = gate.await;
            }
            Ok(value)
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

async fn value(db: &mut Db, id: i64) -> Result<String> {
    Ok(db
        .query(&format!("SELECT v FROM t WHERE id = {id}"))
        .await?[0][0]
        .to_string())
}

/// A pack's get issued before another session's flushes replaced its pages
/// must not serve the old image to later sessions, which would then save on
/// top of it: the cache keeps it under its pack's name, which a later head
/// names only where the page is unchanged.
#[test]
fn an_overlapping_read_never_caches_a_replaced_page() -> Result<()> {
    futures::executor::block_on(async {
        let memory = MemoryKv::default();
        let gate = Arc::new(GateKv {
            inner: memory.clone(),
            started: Mutex::new(None),
            release: Mutex::new(None),
        });
        let plain: Arc<dyn KvStore> = Arc::new(memory.clone());
        // Setup without a cache: one large save, checkpointed, so the
        // reader reads its pages from a pack.
        let db = Db::open(plain.clone(), StateOptions::default()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .await?;
        db.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<600) INSERT INTO t SELECT x, 'old' || x || '|' || hex(randomblob(700)) FROM n").await?;
        db.save_checkpointed().await?;

        let ns = fresh_namespace();
        let mut reader = Db::open(gate.clone(), opts(&ns)).await?;
        assert!(value(&mut reader, 1).await?.starts_with("old1"));
        let (tx, started) = oneshot::channel();
        *gate.started.lock().unwrap() = Some(tx);
        let (release_tx, release_rx) = oneshot::channel::<()>();
        *gate.release.lock().unwrap() = Some(release_rx.shared());

        let mut writer = Db::open(
            plain.clone(),
            opts(&ns).with(|o| StateOptions {
                checkpoint_entries: 1,
                ..o
            }),
        )
        .await?;
        let read = value(&mut reader, 250);
        let write = async {
            started.await.unwrap();
            writer
                .execute("UPDATE t SET v = 'new250' WHERE id = 250")
                .await?;
            // A second flush, which deletes what the first left as garbage.
            writer
                .execute("UPDATE t SET v = 'new1' WHERE id = 1")
                .await?;
            assert_eq!(value(&mut writer, 250).await?, "new250");
            writer.execute("PRAGMA user_version").await?; // no engine reuse
            release_tx.send(()).unwrap();
            Result::Ok(())
        };
        let (read, write) = futures::join!(read, write);
        write?;
        read?;
        drop(reader);
        drop(writer);
        *gate.release.lock().unwrap() = None;

        let mut db = Db::open(plain.clone(), opts(&ns)).await?;
        assert!(!db.reused_engine());
        let seen = value(&mut db, 250).await?;
        let seen = seen[..seen.len().min(6)].to_owned();
        // A neighbour on the same leaf, flushed on top of what the cache served.
        db.execute("UPDATE t SET v = substr(v, 1, 6) || '!' WHERE id = 250")
            .await?;
        drop(db);
        let mut cold = Db::open(plain, StateOptions::default()).await?;
        let durable = value(&mut cold, 250).await?;
        assert_eq!(
            (seen.as_str(), durable.as_str()),
            ("new250", "new250!"),
            "cached / durable value of an acknowledged flush"
        );
        Ok(())
    })
}
