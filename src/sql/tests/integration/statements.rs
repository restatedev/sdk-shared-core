//! The cache of compiled statements (`sql/statements.rs`): sharing changes
//! nothing a session sees, never crosses schemas whose images differ, and a
//! hit compiles nothing. Each test gives its sessions a cache of its own
//! (`open`), so what the others of the process do cannot evict what it counts.
use super::super::script::script;
use crate::sql::statements::{self, Cache, Limits, SchemaId};
use crate::sql::{statement_stats, Database, Tally, Value};
use crate::state::simulation::MemoryKv;
use crate::state::{Result, State};
use std::sync::Arc;
use turso_core::Value as Turso;

impl Database {
    /// What this session's engine compiled and shared so far.
    pub(in crate::sql::tests) async fn tally(&self) -> Tally {
        let mut session = self.shared.lock().await;
        session
            .engine()
            .map(|engine| engine.tally)
            .unwrap_or_default()
    }

    /// The id the session's engine shares programs under, if it has one.
    pub(in crate::sql::tests) async fn schema(&self) -> Option<SchemaId> {
        let mut session = self.shared.lock().await;
        session.engine().ok().and_then(|engine| engine.schema)
    }

    /// Share through `cache`, or not at all, whatever the process does.
    pub(in crate::sql::tests) async fn use_cache(&self, cache: Option<Arc<Cache>>) -> Result<()> {
        let mut session = self.shared.lock().await;
        let engine = session.engine()?;
        engine.cache = cache;
        engine.schema = None;
        engine.schema_read = false;
        engine.share_schema().await
    }

    /// Whether the shared program of `sql` is compatible with this session's
    /// connection as published, and as a hit binds it: a program that is not
    /// recompiles itself, which is what the restamp is for.
    pub(in crate::sql::tests) async fn program_compatibility(
        &self,
        sql: &str,
    ) -> Option<(bool, bool)> {
        let mut session = self.shared.lock().await;
        let engine = session.engine().ok()?;
        let connection = engine.connection.clone()?;
        let program = engine.cache.as_ref()?.program(engine.schema?, sql)?;
        let published = program.is_compatible_with(&connection);
        let bound = statements::instantiate(&program, &connection);
        let stamped = bound
            .get_program()
            .prepared()
            .is_compatible_with(&connection);
        Some((published, stamped))
    }
}

/// A session of a database of its own, sharing through `cache`.
async fn open(cache: &Arc<Cache>) -> Result<Database> {
    let db = Database::new(State::open(Arc::new(MemoryKv::default())).await?).await?;
    db.use_cache(Some(cache.clone())).await?;
    Ok(db)
}

/// The outcome of `sql`, as text.
async fn show(db: &Database, sql: &str, params: Vec<Value>) -> String {
    let params = (!params.is_empty()).then_some(params);
    match db.query_columns(sql, params).await {
        Ok((columns, rows)) => format!("{columns:?} {rows:?}"),
        Err(error) => format!("error: {error}"),
    }
}

async fn run_script(db: &Database, tag: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (sql, params) in script(tag) {
        out.push(show(db, &sql, params).await);
    }
    out
}

/// Results are the same with sharing on and off, whatever the session does:
/// DDL, a rollback of it, errors, savepoints, a pragma.
#[test]
fn same_results_with_and_without_sharing() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::default());
        let seed = open(&cache).await?;
        let seeded = run_script(&seed, "same_results").await;
        let off = open(&cache).await?;
        off.use_cache(None).await?;
        let unshared = run_script(&off, "same_results").await;
        let on = open(&cache).await?;
        let shared = run_script(&on, "same_results").await;
        assert_eq!(unshared, seeded);
        assert_eq!(unshared, shared);
        // The script's statements were not all compiled again.
        assert!(on.tally().await.shared_hits > 0);
        assert_eq!(off.tally().await.shared_hits, 0);
        // And what each session holds is the same.
        let dump = "SELECT type, name, rootpage, sql FROM sqlite_schema ORDER BY rowid";
        assert_eq!(
            show(&on, dump, vec![]).await,
            show(&off, dump, vec![]).await
        );
        Ok(())
    })
}

/// A DDL, and its commit or rollback, give the session the id of the schema
/// it has, and none while it is unsettled.
#[test]
fn the_schema_id_follows_the_schema() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::default());
        let db = open(&cache).await?;
        let empty = db.schema().await.expect("an id at open");
        db.execute("CREATE TABLE a(x)").await?;
        let with_a = db.schema().await.expect("settled by the commit");
        assert_ne!(empty, with_a);
        db.begin().await?;
        db.execute("CREATE TABLE b(x)").await?;
        assert_eq!(db.schema().await, None, "none inside a DDL transaction");
        db.rollback().await?;
        assert_eq!(
            db.schema().await,
            Some(with_a),
            "a rolled back DDL leaves the schema"
        );
        db.execute("PRAGMA cache_size = 100").await?;
        assert_eq!(
            db.schema().await,
            None,
            "a setting of the connection ends sharing"
        );
        db.execute("CREATE TABLE c(x)").await?;
        assert_eq!(db.schema().await, None);
        Ok(())
    })
}

/// The same tables created in a different order have other root pages: other
/// ids, no program shared, each database its own rows.
#[test]
fn different_root_pages_never_share() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::default());
        let (x, y, z) = (
            open(&cache).await?,
            open(&cache).await?,
            open(&cache).await?,
        );
        for (db, order) in [(&x, ["a", "b"]), (&y, ["b", "a"]), (&z, ["a", "b"])] {
            for table in order {
                db.execute(&format!(
                    "CREATE TABLE {table}(id INTEGER PRIMARY KEY, v TEXT)"
                ))
                .await?;
            }
            for table in ["a", "b"] {
                let sql = format!("INSERT INTO {table} VALUES(1, '{table}-in-{}')", order[0]);
                db.execute(&sql).await?;
            }
        }
        let z_rows = z.query("SELECT v FROM a").await?;
        assert_eq!(z_rows[0][0], Value::from_text("a-in-a".to_string()));
        assert_ne!(x.schema().await, y.schema().await);
        assert_eq!(x.schema().await, z.schema().await);

        let reads = [
            "SELECT v FROM a WHERE id = ?1 /*roots*/",
            "SELECT v FROM b WHERE id = ?1 /*roots*/",
        ];
        let write = "INSERT INTO a VALUES(?1, 'written') /*roots*/";
        let one = || vec![Value::from_i64(1)];
        // x publishes, y must not use it, z must.
        assert!(show(&x, reads[0], one()).await.contains("a-in-a"));
        let before = y.tally().await;
        let y_a = show(&y, reads[0], one()).await;
        let y_b = show(&y, reads[1], one()).await;
        assert_eq!(y.tally().await.shared_hits, before.shared_hits);
        assert!(
            y_a.contains("a-in-b") && y_b.contains("b-in-b"),
            "{y_a} {y_b}"
        );
        assert!(show(&x, reads[1], one()).await.contains("b-in-a"));
        let z_before = z.tally().await;
        let z_a = show(&z, reads[0], one()).await;
        assert!(z_a.contains("a-in-a"), "{z_a}");
        assert_eq!(z.tally().await.shared_hits, z_before.shared_hits + 1);
        // A write through each: into its own table a.
        for db in [&x, &y, &z] {
            show(db, write, vec![Value::from_i64(2)]).await;
            assert_eq!(
                db.query("SELECT count(*) FROM a").await?[0][0].as_int(),
                Some(2)
            );
            assert_eq!(
                db.query("SELECT count(*) FROM b").await?[0][0].as_int(),
                Some(1)
            );
        }
        assert_eq!(
            y.tally().await.shared_hits,
            before.shared_hits,
            "y never shared"
        );
        Ok(())
    })
}

/// Databases with one history share programs, and answer with their own rows;
/// a hit compiles nothing and turso never recompiles it.
#[test]
fn identical_schemas_share_without_compiling() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::default());
        let sql = "SELECT id, body FROM t WHERE id >= ?1 ORDER BY id /*identical*/";
        let insert = "INSERT INTO t VALUES(?1, ?2) /*identical*/";
        let schema =
            "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT); CREATE INDEX t_body ON t(body)";
        let mut dbs = Vec::new();
        for n in 0..4i64 {
            let db = open(&cache).await?;
            db.execute(schema).await?;
            dbs.push((n, db));
        }
        let id = dbs[0].1.schema().await;
        assert!(id.is_some());
        for (n, db) in &dbs {
            assert_eq!(db.schema().await, id);
            // The first publishes the programs; the others reuse them.
            for i in 0..3 {
                let body = Value::from_text(format!("db{n}-{i}"));
                db.execute_with(insert, [Value::from_i64(i), body]).await?;
            }
        }
        assert!(dbs[0].1.tally().await.compiled >= 2);
        for (_, db) in &dbs[1..] {
            assert!(
                db.tally().await.shared_hits >= 1,
                "the INSERT came from the first"
            );
        }
        for (n, db) in &dbs[1..] {
            let rows = db.query_with(sql, [Value::from_i64(1)]).await?;
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][1], Value::from_text(format!("db{n}-1")));
            assert_eq!(rows[1][1], Value::from_text(format!("db{n}-2")));
            assert_eq!(db.tally().await.reprepares, 0);
        }
        // Now every statement of the text is shared: a fresh session of the
        // schema compiles nothing to run both, and turso reprepares nothing.
        let fresh = open(&cache).await?;
        fresh.execute(schema).await?;
        let before = fresh.tally().await;
        fresh
            .execute_with(
                insert,
                [Value::from_i64(5), Value::from_text("fresh".to_string())],
            )
            .await?;
        let rows = fresh.query_with(sql, [Value::from_i64(0)]).await?;
        assert_eq!(
            rows,
            vec![vec![
                Value::from_i64(5),
                Value::from_text("fresh".to_string())
            ]]
        );
        let after = fresh.tally().await;
        assert_eq!(after.compiled, before.compiled, "a hit compiles nothing");
        assert_eq!(after.shared_hits, before.shared_hits + 2);
        assert_eq!(after.reprepares, 0, "turso reprepared a shared program");
        Ok(())
    })
}

/// A shared program, after the schema's cookie moved in another session of
/// the same history, is not run on a schema it was not compiled for.
#[test]
fn a_changed_schema_has_its_own_programs() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::default());
        let (a, b) = (open(&cache).await?, open(&cache).await?);
        for db in [&a, &b] {
            db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n)")
                .await?;
            db.execute("INSERT INTO t VALUES(1, 10)").await?;
        }
        let sql = "SELECT * FROM t /*changed*/";
        assert_eq!(a.query(sql).await?.len(), 1);
        // b alters t: its programs are compiled for its schema now.
        b.execute("ALTER TABLE t ADD COLUMN m DEFAULT 5").await?;
        assert_ne!(a.schema().await, b.schema().await);
        let rows = b.query(sql).await?;
        assert_eq!(rows[0].len(), 3, "{rows:?}");
        assert_eq!(a.query(sql).await?[0].len(), 2);
        Ok(())
    })
}

/// Many sessions on several threads at once, each with its own rows and its
/// own schema among a few, using the one cache.
#[test]
fn concurrent_sessions() -> Result<()> {
    let cache = Arc::new(Cache::default());
    let threads: Vec<_> = (0..8i64)
        .map(|t| {
            let cache = cache.clone();
            std::thread::spawn(move || -> Result<()> {
                futures::executor::block_on(async {
                    for round in 0..12i64 {
                        let db = open(&cache).await?;
                        // Three schema shapes, so threads share some and not others.
                        let shape = (t + round) % 3;
                        let order: Vec<&str> = match shape {
                            0 => vec!["a", "b"],
                            1 => vec!["b", "a"],
                            _ => vec!["a"],
                        };
                        for table in &order {
                            let sql =
                                format!("CREATE TABLE {table}(id INTEGER PRIMARY KEY, v TEXT)");
                            db.execute(&sql).await?;
                        }
                        for i in 0..5i64 {
                            let v = format!("t{t}r{round}i{i}");
                            db.execute_with(
                                "INSERT INTO a VALUES(?1, ?2) /*concurrent*/",
                                [Value::from_i64(i), Value::from_text(v)],
                            )
                            .await?;
                            let rows = db
                                .query_with(
                                    "SELECT v FROM a WHERE id = ?1 /*concurrent*/",
                                    [Value::from_i64(i)],
                                )
                                .await?;
                            assert_eq!(
                                rows,
                                vec![vec![Value::from_text(format!("t{t}r{round}i{i}"))]]
                            );
                        }
                        let rows = db
                            .query("SELECT count(*), min(id), max(id) FROM a /*concurrent*/")
                            .await?;
                        assert_eq!(rows[0][0].as_int(), Some(5));
                        if shape == 0 {
                            // A DDL mid-session: the session stops sharing and settles again.
                            db.execute("CREATE TABLE extra(x)").await?;
                            let rows = db.query("SELECT count(*) FROM a /*concurrent*/").await?;
                            assert_eq!(rows[0][0].as_int(), Some(5));
                        }
                        assert_eq!(db.tally().await.reprepares, 0);
                    }
                    Ok(())
                })
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("thread")?;
    }
    Ok(())
}

/// Why a hit stamps the program: as published, it belongs to the database
/// that compiled it, and turso would compile it again on another.
#[test]
fn a_shared_program_is_stamped_for_its_new_connection() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::default());
        let (a, b) = (open(&cache).await?, open(&cache).await?);
        for db in [&a, &b] {
            db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").await?;
        }
        let sql = "SELECT id FROM t /*stamp*/";
        a.query(sql).await?;
        assert_eq!(a.program_compatibility(sql).await, Some((true, true)));
        assert_eq!(b.program_compatibility(sql).await, Some((false, true)));
        Ok(())
    })
}

/// A session's program stays with its statement when the cache drops it, and
/// the session, and one that holds the id of a schema the cache dropped,
/// run on, without sharing; a later session of the schema has a new id.
#[test]
fn eviction_while_a_session_holds_a_statement_is_safe() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::default());
        let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)";
        let (a, b) = (open(&cache).await?, open(&cache).await?);
        for db in [&a, &b] {
            db.execute(schema).await?;
            db.execute("INSERT INTO t VALUES (1, 'one')").await?;
        }
        let sql = "SELECT v FROM t WHERE id = 1 /*evicted*/";
        assert_eq!(
            a.query(sql).await?[0][0],
            Value::from_text("one".to_string())
        );
        let old = a.schema().await.expect("an id");
        assert!(cache.program(old, sql).is_some());
        cache.evict(old);
        assert!(cache.program(old, sql).is_none());
        assert_eq!(cache.programs(), 0);
        // The kept statement runs on, and so does a call that finds no program.
        assert_eq!(
            a.query(sql).await?[0][0],
            Value::from_text("one".to_string())
        );
        let before = b.tally().await;
        assert_eq!(
            b.query(sql).await?[0][0],
            Value::from_text("one".to_string())
        );
        assert_eq!(b.tally().await.shared_hits, before.shared_hits);
        assert_eq!(
            cache.programs(),
            0,
            "nothing published under an id the cache dropped"
        );
        // A new session of the schema is a new schema to the cache.
        let c = open(&cache).await?;
        c.execute(schema).await?;
        assert_ne!(c.schema().await, Some(old));
        assert_eq!(
            c.query("SELECT count(*) FROM t").await?[0][0].as_int(),
            Some(0)
        );
        Ok(())
    })
}

/// The programs a cache holds stay within its bound, least recently used
/// first, on a session's texts.
#[test]
fn the_programs_are_bounded() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::with_limits(Limits {
            programs: 8,
            ..Limits::default()
        }));
        let db = open(&cache).await?;
        for i in 0..30 {
            db.query(&format!("SELECT {i} + 1 /*bounded*/")).await?;
            assert!(cache.programs() <= 8);
        }
        assert_eq!(cache.programs(), 8);
        // The newest are there, the oldest are not.
        let id = db.schema().await.expect("an id");
        assert!(cache.program(id, "SELECT 29 + 1 /*bounded*/").is_some());
        assert!(cache.program(id, "SELECT 0 + 1 /*bounded*/").is_none());
        Ok(())
    })
}

/// A program of a text past the bound is not kept.
#[test]
fn a_text_that_is_too_long_is_not_shared() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::with_limits(Limits {
            text: 64,
            ..Limits::default()
        }));
        let db = open(&cache).await?;
        db.query("SELECT 1 /*short*/").await?;
        db.query(&format!("SELECT 1 /*{}*/", "x".repeat(100)))
            .await?;
        assert_eq!(cache.programs(), 1);
        Ok(())
    })
}

/// A schema whose image is larger than the cache takes is not shared, and
/// the session does not read it again with every call.
#[test]
fn a_schema_too_large_to_share_is_not_shared() -> Result<()> {
    futures::executor::block_on(async {
        let cache = Arc::new(Cache::with_limits(Limits {
            image: 100,
            ..Limits::default()
        }));
        let db = open(&cache).await?;
        db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT)")
            .await?;
        assert_eq!(db.schema().await, None);
        db.execute("INSERT INTO t VALUES (1, 'a', 'b', 'c', 'd')")
            .await?;
        let compiled = db.tally().await.compiled;
        for _ in 0..3 {
            db.query("SELECT a FROM t").await?;
            db.execute("INSERT OR REPLACE INTO t VALUES (1, 'a', 'b', 'c', 'd')")
                .await?;
        }
        // The two texts, once each: no read of the schema in between.
        assert_eq!(db.tally().await.compiled, compiled + 2);
        assert_eq!(cache.programs(), 0);
        Ok(())
    })
}

#[test]
fn an_image_has_one_id_until_the_cache_forgets_it_and_never_again() {
    let cache = Cache::default();
    let a = cache.intern(b"image a").expect("an id");
    let b = cache.intern(b"image b").expect("an id");
    assert_ne!(a, b);
    assert_eq!(cache.intern(b"image a"), Some(a));
    assert_eq!(cache.intern(b"image b"), Some(b));
    // One byte apart, or a prefix of another: other schemas.
    assert_ne!(cache.intern(b"image c"), Some(a));
    assert!(![Some(a), Some(b)].contains(&cache.intern(b"image")));
    assert_eq!(cache.schemas(), 4);
    cache.evict(a);
    let again = cache.intern(b"image a").expect("an id");
    assert_ne!(again, a, "an id is not reused");
    assert_ne!(again, b);
}

#[test]
fn the_images_are_bounded_in_number_and_bytes() {
    let cache = Cache::with_limits(Limits {
        schemas: 3,
        image: 10,
        image_bytes: 18,
        ..Limits::default()
    });
    assert_eq!(cache.intern(b"0123456789 too large"), None);
    let first = cache.intern(b"aaaaaa").expect("an id");
    for image in [b"bbbbbb", b"cccccc"] {
        cache.intern(image).expect("an id");
    }
    assert_eq!((cache.schemas(), cache.image_bytes()), (3, 18));
    // The least recently used goes: touched, `first` is not it.
    assert_eq!(cache.intern(b"aaaaaa"), Some(first));
    cache.intern(b"dddddd").expect("an id");
    assert_eq!(cache.schemas(), 3);
    assert_eq!(
        cache.intern(b"aaaaaa"),
        Some(first),
        "kept as the most recent"
    );
    assert_ne!(cache.intern(b"bbbbbb"), None);
    // By bytes: one of 10 pushes out what the 18 cannot hold.
    cache.intern(b"0123456789").expect("an id");
    assert!(cache.image_bytes() <= 18, "{}", cache.image_bytes());
}

/// Two different rows never give one image, however their fields' bytes run
/// together; the same rows, one.
#[test]
fn an_image_is_unambiguous() {
    let text = |t: &str| Turso::from_text(t.to_string());
    let image = |rows: Vec<Vec<Turso>>| statements::image(&rows, &[vec![Turso::from_i64(1)]], 4096);
    let images = [
        image(vec![vec![text("ab"), text("c")]]),
        image(vec![vec![text("a"), text("bc")]]),
        image(vec![vec![text("abc"), text("")]]),
        image(vec![vec![text("ab")], vec![text("c")]]),
        image(vec![vec![Turso::Null, text("")]]),
        image(vec![vec![text(""), Turso::Null]]),
        image(vec![vec![Turso::from_i64(1)]]),
        image(vec![vec![text("1")]]),
        image(vec![vec![Turso::from_blob(b"1".to_vec())]]),
        image(vec![]),
    ];
    for (i, a) in images.iter().enumerate() {
        for b in &images[i + 1..] {
            assert_ne!(a, b);
        }
    }
    assert_eq!(images[0], image(vec![vec![text("ab"), text("c")]]));
    let cookie = |n| statements::image(&[], &[vec![Turso::from_i64(n)]], 4096);
    assert_ne!(cookie(1), cookie(2));
    assert_ne!(
        cookie(1),
        statements::image(&[], &[vec![Turso::from_i64(1)]], 8192)
    );
}

/// The process's counters move with the sessions that use its cache.
#[test]
fn the_statement_stats_count_the_process_cache() -> Result<()> {
    futures::executor::block_on(async {
        let first = Database::new(State::open(Arc::new(MemoryKv::default())).await?).await?;
        let second = Database::new(State::open(Arc::new(MemoryKv::default())).await?).await?;
        let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY)";
        let sql = "SELECT id FROM t /*stats*/";
        first.execute(schema).await?;
        second.execute(schema).await?;
        let before = statement_stats();
        first.query(sql).await?;
        second.query(sql).await?;
        let after = statement_stats();
        // Other tests' sessions add to these, none takes from them.
        assert!(after.compiled > before.compiled);
        assert!(after.compile_ns >= before.compile_ns);
        assert!(after.shared_misses > before.shared_misses);
        assert!(after.shared_hits > before.shared_hits);
        assert!(after.shared_entries > 0);
        assert!(after.shared_evictions >= before.shared_evictions);
        Ok(())
    })
}

/// A program past the size the cache keeps, as estimated from its
/// instructions and text, is not kept, and the programs held stay within an
/// estimated total.
#[test]
fn the_programs_are_bounded_in_estimated_bytes() -> Result<()> {
    futures::executor::block_on(async {
        let (tiny, big) = (
            "SELECT 1 /*tiny*/",
            "SELECT a.x, b.x, c.x FROM (SELECT 1 x) a, (SELECT 2 x) b, (SELECT 3 x) c /*big*/",
        );
        // What each is estimated at, in a cache that keeps both.
        let sizes = Arc::new(Cache::default());
        let db = open(&sizes).await?;
        db.query(tiny).await?;
        let tiny_size = sizes.program_bytes();
        db.query(big).await?;
        let big_size = sizes.program_bytes() - tiny_size;
        assert!(tiny_size < big_size, "{tiny_size} {big_size}");

        let small = Arc::new(Cache::with_limits(Limits {
            program: tiny_size,
            ..Limits::default()
        }));
        let db = open(&small).await?;
        db.query(tiny).await?;
        db.query(big).await?;
        assert_eq!(
            small.programs(),
            1,
            "only the program under the size is kept"
        );

        let cache = Arc::new(Cache::with_limits(Limits {
            bytes: 4 * statements::OVERHEAD,
            ..Limits::default()
        }));
        let db = open(&cache).await?;
        for i in 0..20 {
            db.query(&format!("SELECT {i} /*estimated*/")).await?;
            assert!(cache.program_bytes() <= 4 * statements::OVERHEAD);
        }
        assert!((1..4).contains(&cache.programs()), "{}", cache.programs());
        Ok(())
    })
}
