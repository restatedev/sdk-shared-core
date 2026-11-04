use super::host::Db;
use crate::state::{
    kv::KvStore,
    simulation::{Effect, Failpoint, FaultKv, MemoryKv, Operation},
    Result, StateOptions,
};
use futures::{
    future::{select, Either},
    FutureExt,
};
use std::sync::Arc;

async fn baseline() -> Result<MemoryKv> {
    let store = MemoryKv::default();
    // A page per frame: each query reads frames of its own.
    let options = StateOptions {
        frame_pages: 1,
        ..StateOptions::default()
    };
    let db = Db::open(Arc::new(store.clone()), options).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, payload TEXT)")
        .await?;
    db.execute("CREATE INDEX by_n ON t(n)").await?;
    db.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<30) INSERT INTO t SELECT x,x,printf('%09000d',x) FROM n").await?;
    db.save_checkpointed().await?;
    Ok(store)
}
fn collecting() -> StateOptions {
    StateOptions {
        checkpoint_entries: 1,
        ..StateOptions::default()
    }
}
fn plan(operation: Operation, prefix: &str, nth: usize, effect: Effect, cancel: bool) -> Failpoint {
    Failpoint {
        operation,
        key_prefix: prefix.into(),
        nth,
        effect,
        cancel,
    }
}
async fn check(store: Arc<dyn KvStore>, changed: bool) -> Result<()> {
    let db = Db::open(store, StateOptions::default()).await?;
    let rows = db
        .query("SELECT id,n,length(payload),substr(payload,-2) FROM t ORDER BY id")
        .await?;
    assert_eq!(rows.len(), 30);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row[0].as_int(), Some(i as i64 + 1));
        assert_eq!(
            row[1].as_int(),
            Some(if changed { 99 } else { i as i64 + 1 })
        );
        assert_eq!(row[2].as_int(), Some(9000));
        assert_eq!(
            row[3].to_string(),
            if changed {
                "00".into()
            } else {
                format!("{:02}", i + 1)
            }
        );
    }
    assert_eq!(
        db.query("PRAGMA integrity_check").await?[0][0].to_string(),
        "ok"
    );
    Ok(())
}

#[test]
fn every_read_can_error_or_cancel_before_or_after_delivery() -> Result<()> {
    futures::executor::block_on(async {
        let mut cases = 0;
        let base = baseline().await?;
        for sql in [
            None,
            Some("SELECT payload FROM t WHERE id=15"),
            Some("SELECT payload FROM t WHERE id BETWEEN 13 AND 17"),
            Some("SELECT id FROM t INDEXED BY by_n WHERE n BETWEEN 10 AND 20"),
            Some("SELECT payload FROM t NOT INDEXED ORDER BY id"),
            Some("UPDATE t SET n=99 WHERE id=15"),
        ] {
            let probe = Arc::new(FaultKv::new(base.fork(), 7));
            if let Some(sql) = sql {
                let db = Db::open(probe.clone(), StateOptions::default()).await?;
                let _gate = probe.arm(plan(Operation::Get, "", usize::MAX, Effect::None, false));
                db.query(sql).await?;
            } else {
                let _gate = probe.arm(plan(Operation::Get, "", usize::MAX, Effect::None, false));
                drop(Db::open(probe.clone(), StateOptions::default()).await?);
            }
            let count = probe
                .events()
                .iter()
                .filter(|e| e.operation == Operation::Get)
                .count();
            assert!(count > 0);
            for nth in 1..=count {
                for effect in [Effect::None, Effect::After] {
                    for cancel in [false, true] {
                        let store = Arc::new(FaultKv::new(base.fork(), nth as u64));
                        let mut session = if sql.is_some() {
                            Some(Db::open(store.clone(), StateOptions::default()).await?)
                        } else {
                            None
                        };
                        store.inner.reset_stats();
                        let gate = store.arm(plan(Operation::Get, "", nth, effect, cancel));
                        let future = async {
                            if let Some(sql) = sql {
                                session.as_mut().unwrap().query(sql).await.map(|_| ())
                            } else {
                                Db::open(store.clone(), StateOptions::default())
                                    .await
                                    .map(drop)
                            }
                        }
                        .boxed();
                        if cancel {
                            match select(future, gate).await {
                                Either::Left(_) => panic!("read failpoint not reached"),
                                Either::Right((signal, pending)) => {
                                    signal?;
                                    drop(pending);
                                }
                            }
                        } else {
                            assert!(future.await.is_err());
                        }
                        if let Some(db) = session {
                            assert!(db
                                .query("SELECT 1")
                                .await
                                .unwrap_err()
                                .to_string()
                                .contains("the session ended"));
                            drop(db);
                        }
                        assert_eq!(store.active(), 0);
                        assert_eq!(store.events().iter().filter(|e| e.faulted).count(), 1);
                        assert_eq!(
                            (store.inner.stats().puts, store.inner.stats().deletes),
                            (0, 0)
                        );
                        store.disarm();
                        store.inner.crash();
                        assert_eq!(store.inner.keys(), base.keys());
                        assert_eq!(store.inner.stored_bytes(), base.stored_bytes());
                        check(store, false).await?;
                        cases += 1;
                    }
                }
            }
        }
        println!("read failpoints: {cases} cases (all observed gets; 6 workloads)");
        Ok(())
    })
}

/// A commit that fails, at the put of its tail entry or at its sync, has an
/// unknown outcome: after a crash it is there only if the sync stored it.
#[test]
fn a_failed_flush_is_there_only_if_its_sync_stored_it() -> Result<()> {
    futures::executor::block_on(async {
        let mut cases = 0;
        let base = baseline().await?;
        for (operation, prefix) in [(Operation::Put, "v22/tail/"), (Operation::Sync, "")] {
            for effect in [Effect::None, Effect::After] {
                let store = Arc::new(FaultKv::new(base.fork(), 7));
                // The commit puts its tail entry, then its head.
                let db = Db::open(store.clone(), collecting()).await?;
                let _gate = store.arm(plan(operation, prefix, 1, effect, false));
                let update = db.execute("UPDATE t SET n=99,payload=printf('%09000d',0)");
                assert!(update.await.is_err());
                assert_eq!(store.active(), 0, "the failed sync must settle");
                store.disarm();
                store.inner.crash();
                let synced = operation == Operation::Sync && matches!(effect, Effect::After);
                check(store.clone(), synced).await?;
                cases += 1;
            }
        }
        println!("flush failures: {cases} cases");
        Ok(())
    })
}

/// Open only reads: read failures mutate nothing, and open can be retried.
#[test]
fn open_read_failures_are_retryable() -> Result<()> {
    futures::executor::block_on(async {
        let mut read_cases = 0;
        let base = baseline().await?;
        let reads = {
            let probe = Arc::new(FaultKv::new(base.fork(), 0));
            let _gate = probe.arm(plan(Operation::Get, "", usize::MAX, Effect::None, false));
            drop(Db::open(probe.clone(), StateOptions::default()).await?);
            probe.disarm();
            probe
                .events()
                .iter()
                .filter(|e| e.operation == Operation::Get)
                .count()
        };
        assert!(reads >= 3, "the head, the tail, the manifest, then frames");
        for nth in 1..=reads {
            for effect in [Effect::None, Effect::After] {
                for cancel in [false, true] {
                    let store = Arc::new(FaultKv::new(base.fork(), 31));
                    let gate = store.arm(plan(Operation::Get, "", nth, effect, cancel));
                    let future = Db::open(store.clone(), StateOptions::default()).boxed();
                    if cancel {
                        match select(future, gate).await {
                            Either::Left(_) => panic!("open read failpoint not reached"),
                            Either::Right((signal, pending)) => {
                                signal?;
                                drop(pending);
                            }
                        }
                    } else {
                        assert!(future.await.is_err());
                    }
                    assert_eq!(store.active(), 0);
                    assert_eq!(
                        (store.inner.stats().puts, store.inner.stats().deletes),
                        (0, 0)
                    );
                    store.disarm();
                    check(store, false).await?;
                    read_cases += 1;
                }
            }
        }
        println!("open read failpoints: {read_cases} cases");
        Ok(())
    })
}

#[test]
fn counters_distinguish_request_bytes_reclamation_and_missing_keys() -> Result<()> {
    futures::executor::block_on(async {
        let store = MemoryKv::default();
        let key = &format!("{}0-1/0", crate::state::blobs::KV_PREFIX);
        assert!(store.get(key).await?.is_none());
        store.put(key, vec![1; 4096].into())?;
        store.get(key).await?;
        store.get(key).await?;
        store.delete(key)?;
        store.delete(key)?;
        let s = store.stats();
        let t = &s.by_kind["pack"];
        assert_eq!((t.gets, t.misses, t.puts, t.deletes), (3, 1, 1, 2));
        assert_eq!(
            (t.bytes_read, t.bytes_written, t.bytes_deleted),
            (8192, 4096, 4096)
        );
        assert_eq!(
            (t.get_key_bytes, t.put_key_bytes, t.delete_key_bytes),
            (3 * key.len() as u64, key.len() as u64, 2 * key.len() as u64)
        );
        assert_eq!(
            (s.read_keys.len(), s.write_keys.len(), s.delete_keys.len()),
            (1, 1, 1)
        );
        assert_eq!((store.stored_bytes(), store.stored_key_bytes()), (0, 0));
        store.reset_stats();
        assert!(store.stats().by_kind.is_empty());
        Ok(())
    })
}
