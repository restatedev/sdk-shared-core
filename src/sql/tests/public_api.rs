//! Exercises the normal library with an immediately-ready backend: no simulator,
//! instrumentation, forced yields or checkpoint fault hooks are required.
use crate::sql::Database;
use crate::state::{kv::KvStore, Result};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Store(Mutex<BTreeMap<String, Bytes>>);

impl KvStore for Store {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>> {
        Box::pin(async move { Ok(self.0.lock().unwrap().get(key).cloned()) })
    }
    fn put(&self, key: &str, value: Bytes) -> io::Result<()> {
        self.0.lock().unwrap().insert(key.into(), value);
        Ok(())
    }
    fn delete(&self, key: &str) -> io::Result<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
    fn sync(&self) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn save_and_reopen_with_an_immediately_ready_backend() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(Store::default());
        fn require_send<T: Send>(value: T) -> T {
            value
        }
        let db = require_send(Database::open(store.clone())).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
            .await?;
        db.execute("INSERT INTO t VALUES (1, 'hello')").await?;
        require_send(db.save()).await?;
        let db = Database::open(store.clone()).await?;
        assert_eq!(
            db.query("SELECT value FROM t").await?[0][0].to_string(),
            "hello"
        );
        db.begin().await?;
        db.execute("UPDATE t SET value='changed'").await?;
        drop(db);
        let db = Database::open(store).await?;
        assert_eq!(
            db.query("SELECT value FROM t").await?[0][0].to_string(),
            "hello"
        );
        assert_eq!(
            db.query("PRAGMA integrity_check").await?[0][0].to_string(),
            "ok"
        );
        Ok(())
    })
}

/// Records commit with their transaction, durable together at its commit; a
/// reader's are its own, as its SQL is, and never stored.
#[test]
fn records_commit_with_their_transaction_with_an_immediately_ready_backend() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(Store::default());
        fn require_send<T: Send>(value: T) -> T {
            value
        }
        let db = Database::open(store.clone()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
            .await?;
        db.execute("INSERT INTO t VALUES (1, 'hello')").await?;
        require_send(db.begin()).await?;
        db.execute("INSERT INTO t VALUES (2, 'world')").await?;
        let records = [
            ("outbox".into(), Bytes::from_static(b"two rows")),
            ("schema".into(), Bytes::from_static(b"1")),
        ];
        require_send(db.commit(records)).await?;
        // A name no record may have is refused, and the transaction stays
        // open, for its rollback.
        db.begin().await?;
        db.execute("INSERT INTO t VALUES (3, 'lost')").await?;
        assert!(db.commit([("db".into(), Bytes::new())]).await.is_err());
        db.rollback().await?;
        drop(db);

        let reader = Database::open(store.clone()).reader().await?;
        reader.begin().await?;
        reader.execute("INSERT INTO t VALUES (3, 'mine')").await?;
        reader
            .commit([("schema".into(), Bytes::from_static(b"2"))])
            .await?;
        assert_eq!(reader.record("schema").await?, &b"2"[..]);
        assert_eq!(reader.record("outbox").await?, &b"two rows"[..]);
        drop(reader);

        let db = Database::open(store).await?;
        assert_eq!(db.record("outbox").await?, &b"two rows"[..]);
        assert_eq!(db.record("schema").await?, &b"1"[..]);
        assert!(db.record("never set").await?.is_empty());
        assert_eq!(
            db.query("SELECT count(*) FROM t").await?[0][0].as_int(),
            Some(2)
        );
        Ok(())
    })
}
