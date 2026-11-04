use super::host::Db;
use crate::state::{simulation::MemoryKv, Result, StateOptions};
use std::sync::Arc;

/// A checkpoint every `every` flushes.
fn options(every: u64) -> StateOptions {
    StateOptions {
        checkpoint_entries: every,
        ..StateOptions::default()
    }
}

async fn summary(db: &mut Db) -> Result<(i64, i64)> {
    let rows = db
        .query("SELECT count(*), coalesce(sum(id * length(payload)), 0) FROM t")
        .await?;
    Ok((rows[0][0].as_int().unwrap(), rows[0][1].as_int().unwrap()))
}

/// Many SQL sessions with inserts, deletes, a schema change and checkpoints
/// at changing intervals keep the same results as an in-memory model, while
/// a collection a round leaves only the packs the head needs, and those
/// hold at most a few times the database.
#[test]
fn sql_sessions_match_a_model_across_checkpoint_intervals() -> Result<()> {
    futures::executor::block_on(async {
        let store = MemoryKv::default();
        let db = Db::open(Arc::new(store.clone()), options(3)).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")
            .await?;
        db.save().await?;

        let mut model = std::collections::BTreeMap::new();
        for round in 0..24i64 {
            let every = [3, 3, 1, 8][(round / 6) as usize];
            let db = Db::open(Arc::new(store.clone()), options(every)).await?;
            let start = round * 25;
            db.execute(&format!(
                "WITH RECURSIVE n(x) AS (VALUES({start}) UNION ALL SELECT x+1 FROM n WHERE x<{}) \
                 INSERT INTO t SELECT x, printf('%0{}d', x) FROM n",
                start + 24,
                50 + round
            ))
            .await?;
            for id in start..start + 25 {
                model.insert(id, 50 + round);
            }
            if round % 4 == 3 {
                db.execute(&format!("DELETE FROM t WHERE id % 7 = {}", round % 7))
                    .await?;
                model.retain(|id, _| id % 7 != round % 7);
            }
            if round == 10 {
                db.execute("CREATE INDEX by_payload ON t(payload)").await?;
            }
            db.state().collect_garbage().await?;
            assert_eq!(
                store.packs(),
                db.state().needed_packs().await?,
                "round {round}"
            );

            let expected = (
                model.len() as i64,
                model.iter().map(|(id, len)| id * len).sum::<i64>(),
            );
            let mut db = Db::open(Arc::new(store.clone()), options(every)).await?;
            assert_eq!(summary(&mut db).await?, expected, "round {round}");
            let pages = db.query("PRAGMA page_count").await?[0][0].as_int().unwrap();
            let stored = store.stored_bytes();
            assert!(
                stored < 3 * pages as usize * 4096 + (64 << 10),
                "round {round}: {stored} bytes stored for {pages} pages"
            );
        }
        let db = Db::open(Arc::new(store.clone()), StateOptions::default()).await?;
        let rows = db
            .query("SELECT count(*) FROM t INDEXED BY by_payload WHERE payload > ''")
            .await?;
        assert_eq!(rows[0][0].as_int(), Some(model.len() as i64));
        Ok(())
    })
}
