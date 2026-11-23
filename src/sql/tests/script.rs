//! A script of calls shared by the tests that run one two ways.
use crate::sql::Value;

/// Statements of every kind the engine keeps: DDL, inserts, queries with
/// parameters and errors, a rolled back transaction, savepoints, a pragma.
/// `tag` makes the texts this test's.
pub(super) fn script(tag: &str) -> Vec<(String, Vec<Value>)> {
    let mut steps: Vec<(String, Vec<Value>)> = Vec::new();
    let mut add = |sql: String, params: Vec<Value>| steps.push((sql, params));
    add(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)".into(),
        vec![],
    );
    add("CREATE INDEX t_n ON t(n)".into(), vec![]);
    for i in 0..6 {
        add(
            format!("INSERT INTO t VALUES(?1, ?2, ?3) /*{tag}*/"),
            vec![
                Value::from_i64(i),
                Value::from_i64(i % 3),
                Value::from_text(format!("row {i}")),
            ],
        );
    }
    for _ in 0..2 {
        add(
            format!("SELECT id, n, body FROM t WHERE n = ?1 ORDER BY id /*{tag}*/"),
            vec![Value::from_i64(1)],
        );
        add(format!("SELECT count(*), sum(n) FROM t /*{tag}*/"), vec![]);
        add(
            format!("UPDATE t SET n = n + 1 WHERE id = ?1 RETURNING id, n /*{tag}*/"),
            vec![Value::from_i64(2)],
        );
        add(format!("SELECT * FROM missing /*{tag}*/"), vec![]);
        add(
            format!("INSERT INTO t VALUES(0, 0, 'dup') /*{tag}*/"),
            vec![],
        );
        add(format!("SELECT ?1 /*{tag}*/"), vec![]);
    }
    // A transaction whose statements are partly quick, and a DDL rolled back.
    add("BEGIN".into(), vec![]);
    add(
        format!("INSERT INTO t VALUES(20, 2, 'in a transaction') /*{tag}*/"),
        vec![],
    );
    add("SAVEPOINT s".into(), vec![]);
    add("CREATE TABLE gone(x)".into(), vec![]);
    add("INSERT INTO gone VALUES(1)".into(), vec![]);
    add("ALTER TABLE t ADD COLUMN extra TEXT".into(), vec![]);
    add(
        format!("SELECT id, extra FROM t ORDER BY id /*{tag}*/"),
        vec![],
    );
    add("ROLLBACK TO s".into(), vec![]);
    add(format!("SELECT count(*) FROM t /*{tag}*/"), vec![]);
    add(
        format!("SELECT id, extra FROM t ORDER BY id /*{tag}*/"),
        vec![],
    ); // gone again
    add("RELEASE s".into(), vec![]);
    add("SAVEPOINT s2".into(), vec![]);
    add(
        format!("DELETE FROM t WHERE id > ?1 /*{tag}*/"),
        vec![Value::from_i64(2)],
    );
    add("ROLLBACK TO s2".into(), vec![]);
    add("RELEASE s2".into(), vec![]);
    add("ROLLBACK".into(), vec![]);
    add(format!("SELECT count(*) FROM t /*{tag}*/"), vec![]);
    add(
        "CREATE TABLE after_ddl(x INTEGER PRIMARY KEY, y)".into(),
        vec![],
    );
    add(
        format!("INSERT INTO after_ddl VALUES(?1, 'y') /*{tag}*/"),
        vec![Value::from_i64(7)],
    );
    add("DROP INDEX t_n".into(), vec![]);
    add(format!("SELECT * FROM after_ddl /*{tag}*/"), vec![]);
    add("PRAGMA cache_size = 123".into(), vec![]);
    add(format!("SELECT count(*) FROM t /*{tag}*/"), vec![]);
    add(
        "SELECT type, name, rootpage, sql FROM sqlite_schema ORDER BY rowid".into(),
        vec![],
    );
    add("PRAGMA integrity_check".into(), vec![]);
    steps
}
