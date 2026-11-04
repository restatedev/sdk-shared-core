//! Errors: SQL errors leave a session usable; anything else ends it.
use super::{
    host::{Db, StateSetup},
    reference::{oracle, run, text},
};
use crate::sql::Value;
use crate::state::{
    simulation::{Effect, Failpoint, FaultKv, MemoryKv, Operation},
    Result, StateOptions,
};
use std::sync::Arc;

fn options() -> StateSetup {
    StateOptions::default().into()
}

async fn rows(db: &mut Db) -> Result<Vec<(i64, i64)>> {
    let rows = db.query("SELECT id, n FROM t ORDER BY id").await?;
    Ok(rows
        .iter()
        .map(|row| (row[0].as_int().unwrap(), row[1].as_int().unwrap()))
        .collect())
}

/// Every kind of SQL error: the statement has no effect, the session goes on,
/// and an open transaction continues.
#[test]
fn sql_errors_leave_the_session_usable() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let mut db = Db::open(store.clone(), options()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER CHECK (n >= 0))")
            .await?;
        db.execute("INSERT INTO t VALUES (1, 1)").await?;
        for bad in [
            "SELEC 1",                              // syntax
            "INSERT INTO missing VALUES (1)",       // no such table
            "INSERT INTO t VALUES (2, 2), (1, 9)",  // unique, on the second row
            "UPDATE t SET n = n - 5",               // check
            "INSERT INTO t VALUES (3, 3); SELEC 1", // the first statement stands
        ] {
            assert!(db.execute(bad).await.is_err(), "{bad}");
            assert!(db.is_usable(), "{bad}");
        }
        assert!(db.execute_with("SELECT ?", []).await.is_err());
        assert_eq!(rows(&mut db).await?, [(1, 1), (3, 3)]);

        db.execute("BEGIN").await?;
        db.execute("INSERT INTO t VALUES (4, 4)").await?;
        assert!(db
            .execute("INSERT INTO t VALUES (5, 5), (6, -1)")
            .await
            .is_err());
        assert!(db.is_usable());
        db.execute("COMMIT").await?;
        assert_eq!(rows(&mut db).await?, [(1, 1), (3, 3), (4, 4)]);

        drop(db);
        store.crash();
        let mut db = Db::open(store, options()).await?;
        assert_eq!(rows(&mut db).await?, [(1, 1), (3, 3), (4, 4)]);
        Ok(())
    })
}

/// The dedupe pattern with a plain INSERT: the duplicate fails, the body's
/// error rolls the transaction back, and the session handles the next request.
#[test]
fn a_duplicate_request_rolls_back_and_the_session_goes_on() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), options()).await?;
        db.execute("CREATE TABLE requests(id TEXT PRIMARY KEY); CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        for (request, id) in [("a", 1), ("b", 2), ("a", 3), ("c", 4)] {
            let result = db
                .transaction(async |db| {
                    db.execute_with(
                        "INSERT INTO requests VALUES (?)",
                        [Value::from_text(request.to_string())],
                    )
                    .await?;
                    db.execute_with("INSERT INTO t VALUES (?, 0)", [Value::from_i64(id)])
                        .await
                })
                .await;
            assert_eq!(result.is_ok(), id != 3, "request {request}");
            assert!(db.is_usable());
        }
        drop(db);
        let mut db = Db::open(store, options()).await?;
        assert_eq!(rows(&mut db).await?, [(1, 0), (2, 0), (4, 0)]);
        Ok(())
    })
}

/// Storage failures still end the session: its state is unknown.
#[test]
fn storage_errors_end_the_session() -> Result<()> {
    futures::executor::block_on(async {
        for (operation, prefix) in [
            (Operation::Get, crate::state::blobs::KV_PREFIX),
            (Operation::Put, "v22/head"),
        ] {
            let store = Arc::new(FaultKv::new(MemoryKv::default(), 0));
            let db = Db::open(store.clone(), options()).await?;
            db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, pad BLOB)")
                .await?;
            for i in 0..300 {
                db.execute_with(
                    "INSERT INTO t VALUES (?, 0, zeroblob(500))",
                    [Value::from_i64(i)],
                )
                .await?;
            }
            drop(db);
            let options = options().with(|o| StateOptions {
                prefetch_pages: 0,
                ..o
            });
            let db = Db::open(store.clone(), options).await?;
            let _signal = store.arm(Failpoint {
                operation,
                key_prefix: prefix.into(),
                nth: 1,
                effect: Effect::None,
                cancel: false,
            });
            let result = match operation {
                Operation::Get => db.query("SELECT sum(n) FROM t").await.map(|_| ()),
                _ => db.execute("INSERT INTO t VALUES (1000, 0, NULL)").await,
            };
            assert!(
                store.events().iter().any(|event| event.faulted),
                "{operation:?}"
            );
            assert!(result.is_err(), "{operation:?}");
            assert!(!db.is_usable(), "{operation:?}");
            store.disarm();
        }
        Ok(())
    })
}

/// Random statements, about a third of them failing, run on a session and on
/// a reference engine: every outcome and every state agree, including after a
/// reopen from storage.
#[test]
fn sql_errors_match_a_reference_engine() -> Result<()> {
    futures::executor::block_on(async {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store.clone(), options()).await?;
        let reference = oracle();
        let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER CHECK (n >= 0))";
        db.execute(schema).await?;
        run(&reference, schema).unwrap();
        let mut open = false;
        for step in 0..400 {
            let sql = match next(10) {
                0 | 1 => format!("INSERT INTO t VALUES ({}, {})", next(40), next(5)),
                2 => format!("INSERT INTO t VALUES ({}, 1), ({}, 2)", next(40), next(40)),
                3 => format!("UPDATE t SET n = n - {} WHERE id < {}", next(4), next(40)),
                4 => format!("DELETE FROM t WHERE id = {}", next(40)),
                5 if !open => "BEGIN".to_owned(),
                5 | 6 if open => ["COMMIT", "ROLLBACK"][next(2) as usize].to_owned(),
                7 => "SELEC broken".to_owned(),
                8 => format!(
                    "INSERT INTO t VALUES ({}, 1); INSERT INTO nope VALUES (1)",
                    next(40)
                ),
                _ => "SELECT id, n FROM t ORDER BY id".to_owned(),
            };
            let expected = run(&reference, &sql).map(|rows| text(&rows));
            let got = db.query(&sql).await;
            assert_eq!(
                expected.is_ok(),
                got.is_ok(),
                "step {step}: {sql}: {expected:?} / {got:?}"
            );
            assert!(db.is_usable(), "step {step}: {sql}");
            if let (Ok(expected), Ok(got)) = (&expected, &got) {
                assert_eq!(&text(got), expected, "step {step}: {sql}");
            }
            open = !reference.get_auto_commit();
        }
        if open {
            db.execute("COMMIT").await?;
            run(&reference, "COMMIT").unwrap();
        }
        let expected = text(&run(&reference, "SELECT id, n FROM t ORDER BY id").unwrap());
        drop(db);
        store.crash();
        let db = Db::open(store, options()).await?;
        let got = text(&db.query("SELECT id, n FROM t ORDER BY id").await?);
        assert_eq!(got, expected, "after reopen");
        Ok(())
    })
}

/// A `transaction` whose `BEGIN` fails with an SQL error, inside a transaction
/// the caller began: the error is returned, and the session goes on.
#[test]
fn a_transaction_whose_begin_fails_leaves_the_session_usable() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let mut db = Db::open(store, options()).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
            .await?;
        db.execute("BEGIN").await?;
        let error = db
            .transaction(async |db| db.execute("INSERT INTO t VALUES(1, 1)").await)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("transaction"), "{error}");
        assert!(db.is_usable());
        // The caller's transaction is still open, and commits.
        db.execute("INSERT INTO t VALUES(2, 2)").await?;
        db.execute("COMMIT").await?;
        assert_eq!(rows(&mut db).await?, vec![(2, 2)]);
        Ok(())
    })
}

/// A `transaction` whose `COMMIT` fails (a deferred constraint) rolls back:
/// the error is returned, nothing of it stays, and the session goes on.
#[test]
fn a_transaction_whose_commit_fails_rolls_back() -> Result<()> {
    futures::executor::block_on(async {
        let store = Arc::new(MemoryKv::default());
        let db = Db::open(store, options()).await?;
        db.execute("PRAGMA foreign_keys=ON").await?;
        db.execute(
            "CREATE TABLE p(id INTEGER PRIMARY KEY);
             CREATE TABLE c(pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        )
        .await?;
        let error = db
            .transaction(async |db| db.execute("INSERT INTO c VALUES(7)").await)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("FOREIGN KEY"), "{error}");
        assert!(db.is_usable());
        assert!(db.query("SELECT * FROM c").await?.is_empty());
        db.transaction(async |db| db.execute("INSERT INTO p VALUES(7)").await)
            .await?;
        db.save().await?;
        Ok(())
    })
}
