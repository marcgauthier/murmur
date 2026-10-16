use sqlite_session::{Database, Migration, OpenMode, Options, Result};

#[test]
fn test_index_creation_and_chunked_migration_with_many_rows() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("test_chunks.db");

    let opts = Options {
        mode: OpenMode::CreateNew,
        // Set a small transaction_bytes budget (256 KiB)
        transaction_bytes: 256 * 1024,
        ..Options::default()
    };

    let key = [42u8; 32];
    let db = Database::open(&db_path, key, opts.clone())?;

    // Create table with 2 columns
    eprintln!("Step 0: create table");
    db.migrate(Migration::new(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, counter INTEGER);",
    ))?;

    // Insert 5000 rows in batches of 500 so normal transaction session capture stays under budget
    eprintln!("Step 0.5: insert items");
    for batch in 0..10 {
        db.transaction(move |sql| {
            for i in (batch * 500 + 1)..=((batch + 1) * 500) {
                sql.execute(
                    "INSERT INTO items (id, name, counter) VALUES (?, ?, ?);",
                    rusqlite::params![i, format!("item_{}", i), i * 10],
                )?;
            }
            Ok(())
        })?;
        db.flush()?;
    }

    db.flush()?;

    // 1. Index-only migration: must NOT fail even with 5000 rows
    eprintln!("Step 1: create index");
    db.migrate(Migration::new(
        "CREATE INDEX idx_items_counter ON items(counter);",
    ))?;

    db.flush()?;

    // Verify index works
    let count: i64 = db.read(|sql| {
        let c: i64 = sql.query_row("SELECT count(*) FROM items;", [], |r| r.get(0))?;
        Ok(c)
    })?;
    assert_eq!(count, 5000);

    // 2. Table-altering migration: add a column to 5000 rows across multiple chunks
    // All 5000 rows encoded would exceed transaction_bytes (256 KiB), but chunks stay within budget!
    eprintln!("Step 2: alter table");
    db.migrate(Migration::new(
        "ALTER TABLE items ADD COLUMN extra_col TEXT;",
    ))?;

    db.flush()?;

    // Verify new column exists and is queryable
    let has_extra: bool = db.read(|sql| {
        let val: Option<String> = sql.query_row("SELECT extra_col FROM items WHERE id = 100;", [], |r| r.get(0))?;
        Ok(val.is_none())
    })?;
    assert!(has_extra);

    // 3. Close and reopen to verify recovery of chunked migration
    db.close()?;

    let reopen_opts = Options {
        mode: OpenMode::Existing,
        transaction_bytes: 256 * 1024,
        ..Options::default()
    };
    let reopened = Database::open(&db_path, key, reopen_opts)?;

    let reopened_count: i64 = reopened.read(|sql| {
        let c: i64 = sql.query_row("SELECT count(*) FROM items WHERE counter = 1000;", [], |r| r.get(0))?;
        Ok(c)
    })?;
    assert_eq!(reopened_count, 1);

    reopened.close()?;
    Ok(())
}
