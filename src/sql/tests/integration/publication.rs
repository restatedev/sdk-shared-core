//! End-to-end SQL campaign: every backend mutation of a save and the garbage
//! collection after it fails in every outcome, with and without a crash and an owner
//! cache. The save is atomic, the database stays consistent and the next
//! save works.
use super::host::Db;
use super::host::StateSetup;
use crate::state::simulation::Namespace;
use crate::state::{
    simulation::{fresh_namespace, Effect, Failpoint, FaultKv, MemoryKv, Operation},
    Result, StateOptions,
};
use std::sync::Arc;

fn options(cache: Option<&Namespace>) -> StateSetup {
    StateSetup {
        options: StateOptions {
            checkpoint_entries: 1,
            ..StateOptions::default()
        },
        cache: cache.cloned(),
        packs: None,
    }
}

/// Two sessions, a checkpoint each flush, and a collection: the next save
/// deletes the tail its checkpoint covers, and the collection after it the
/// packs no checkpoint needs.
async fn baseline(cache: Option<&Namespace>) -> Result<MemoryKv> {
    let store = MemoryKv::default();
    let kv = Arc::new(store.clone());
    let db = Db::open(kv.clone(), options(cache)).await?;
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, payload TEXT)")
        .await?;
    db.execute("CREATE INDEX by_n ON t(n)").await?;
    db.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<40) INSERT INTO t SELECT x,x,printf('%0300d',x) FROM n").await?;
    db.save().await?;
    let db = Db::open(kv, options(cache)).await?;
    db.execute("UPDATE t SET n = n + 100 WHERE id % 3 = 0")
        .await?;
    db.state().collect_garbage().await?;
    Ok(store)
}

/// Returns whether the update `n = 7000` is visible; it must be all or nothing.
async fn changed(store: Arc<FaultKv>, options: StateSetup) -> Result<bool> {
    let db = Db::open(store, options).await?;
    let rows = db
        .query("SELECT count(*), sum(n = 7000), sum(length(payload)) FROM t")
        .await?;
    let (count, sevens) = (rows[0][0].as_int().unwrap(), rows[0][1].as_int().unwrap());
    assert_eq!(count, 40);
    assert_eq!(rows[0][2].as_int(), Some(40 * 300));
    assert!(sevens == 0 || sevens == 40, "torn save: {sevens} rows");
    assert_eq!(
        db.query("PRAGMA integrity_check").await?[0][0].to_string(),
        "ok"
    );
    Ok(sevens == 40)
}

#[test]
fn every_save_mutation_fails_atomically() -> Result<()> {
    futures::executor::block_on(async {
        let mut cases = 0;
        for cached in [false, true] {
            for operation in [Operation::Put, Operation::Delete] {
                'ordinal: for nth in 1.. {
                    for effect in [Effect::None, Effect::After] {
                        for crash in [false, true] {
                            let cache = cached.then(fresh_namespace);
                            let base = baseline(cache.as_ref()).await?;
                            let store = Arc::new(FaultKv::new(base, nth as u64));
                            let db = Db::open(store.clone(), options(cache.as_ref())).await?;
                            let _gate = store.arm(Failpoint {
                                operation,
                                key_prefix: String::new(),
                                nth,
                                effect,
                                cancel: false,
                            });
                            let (flushed, result) = match db.execute("UPDATE t SET n = 7000").await
                            {
                                Ok(_) => (true, db.state().collect_garbage().await.map(drop)),
                                Err(error) => (false, Err(error)),
                            };
                            drop(db);
                            let faulted = store.events().iter().any(|e| e.faulted);
                            store.disarm();
                            if !faulted {
                                result?;
                                break 'ordinal;
                            }
                            cases += 1;
                            let mut cache = cache;
                            if crash {
                                store.inner.crash();
                                cache = cached.then(fresh_namespace);
                            }
                            let durable = changed(store.clone(), options(cache.as_ref())).await?;
                            if flushed {
                                assert!(durable, "acknowledged save was lost");
                            }
                            // The next save works and survives a crash.
                            let db = Db::open(store.clone(), options(cache.as_ref())).await?;
                            db.execute("UPDATE t SET n = 7000").await?;
                            db.save().await?;
                            store.inner.crash();
                            assert!(changed(store.clone(), options(None)).await?);
                        }
                    }
                }
            }
        }
        // Pack, tail and head puts, and tail and pack deletes, cached or not.
        assert!(cases >= 30, "{cases}");
        println!("SQL save failpoints: {cases} cases");
        Ok(())
    })
}
