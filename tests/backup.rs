use sqlite_session::*;
use std::time::Duration;

fn options() -> Options {
    Options {
        mode: OpenMode::CreateNew,
        capture_interval: Duration::from_millis(30),
        ..Options::default()
    }
}

fn create(path: &std::path::Path) -> Result<Database> {
    let db = Database::open(path, [7; 32], options())?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
    ))?;
    Ok(db)
}

fn count(db: &Database) -> Result<i64> {
    db.read(|c| Ok(c.query_row("SELECT count(*) FROM items", [], |r| r.get(0))?))
}

#[test]
fn online_backup_and_restore_round_trip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let live = tmp.path().join("db");
    let backup = tmp.path().join("backup");
    let restored = tmp.path().join("restored");
    let db = create(&live)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(1,'one')", [])?;
        Ok(())
    })?;
    // Backup flushes; no explicit flush needed.
    db.backup(&backup)?;
    Database::verify_backup(&backup, [7; 32])?;
    let expected = db.export_state()?;
    Database::restore(&backup, &restored, [7; 32])?;
    let db2 = Database::open(&restored, [7; 32], Options::default())?;
    assert_eq!(db2.export_state()?, expected);
    assert_eq!(count(&db2)?, 1);
    assert_eq!(db2.database_id(), db.database_id());
    // Restored copy stays writable.
    db2.transaction(|c| {
        c.execute("INSERT INTO items VALUES(2,'two')", [])?;
        Ok(())
    })?;
    assert_eq!(count(&db2)?, 2);
    db.close()?;
    db2.close()
}

#[test]
fn backup_is_point_in_time() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let live = tmp.path().join("db");
    let backup = tmp.path().join("backup");
    let restored = tmp.path().join("restored");
    let db = create(&live)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(1,'before')", [])?;
        Ok(())
    })?;
    db.backup(&backup)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(2,'after')", [])?;
        Ok(())
    })?;
    db.flush()?;
    assert_eq!(count(&db)?, 2);
    Database::restore(&backup, &restored, [7; 32])?;
    let db2 = Database::open(&restored, [7; 32], Options::default())?;
    assert_eq!(count(&db2)?, 1);
    let value: String =
        db2.read(|c| Ok(c.query_row("SELECT value FROM items WHERE id=1", [], |r| r.get(0))?))?;
    assert_eq!(value, "before");
    db.close()?;
    db2.close()
}

#[test]
fn restore_with_fresh_identity_clones() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let live = tmp.path().join("db");
    let backup = tmp.path().join("backup");
    let clone = tmp.path().join("clone");
    let db = create(&live)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(1,'one')", [])?;
        Ok(())
    })?;
    db.backup(&backup)?;
    let expected = db.export_state()?;
    Database::restore_with_options(
        &backup,
        &clone,
        [7; 32],
        RestoreOptions {
            new_database_id: None,
            new_node_id: Some([9; 16]),
        },
    )?;
    let db2 = Database::open(&clone, [7; 32], Options::default())?;
    assert_eq!(db2.database_id(), db.database_id());
    assert_eq!(db2.node_id(), [9; 16]);
    assert_eq!(db2.export_state()?.rows, expected.rows);
    // Clone exchanges state as a distinct replica.
    db2.merge_state(db.export_state()?)?;
    db.merge_state(db2.export_state()?)?;
    assert_eq!(db.export_state()?.rows, db2.export_state()?.rows);
    db.close()?;
    db2.close()
}

#[test]
fn restore_with_new_database_id_is_fresh() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let live = tmp.path().join("db");
    let backup = tmp.path().join("backup");
    let fresh = tmp.path().join("fresh");
    let db = create(&live)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(1,'one')", [])?;
        Ok(())
    })?;
    db.backup(&backup)?;
    Database::restore_with_options(
        &backup,
        &fresh,
        [7; 32],
        RestoreOptions {
            new_database_id: Some([5; 16]),
            new_node_id: Some([6; 16]),
        },
    )?;
    let db2 = Database::open(&fresh, [7; 32], Options::default())?;
    assert_eq!(db2.database_id(), [5; 16]);
    assert_eq!(db2.node_id(), [6; 16]);
    assert_eq!(count(&db2)?, 1);
    db.close()?;
    db2.close()
}

#[test]
fn backup_and_restore_reject_bad_destinations_and_keys() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let live = tmp.path().join("db");
    let backup = tmp.path().join("backup");
    let db = create(&live)?;
    db.transaction(|c| {
        c.execute("INSERT INTO items VALUES(1,'one')", [])?;
        Ok(())
    })?;
    db.backup(&backup)?;
    assert!(matches!(db.backup(&backup), Err(Error::Invalid(_))));
    assert!(matches!(
        Database::restore(&backup, &backup, [7; 32]),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        Database::restore(&backup, &tmp.path().join("x"), [8; 32]),
        Err(Error::WrongKey)
    ));
    assert!(matches!(
        Database::verify_backup(&backup, [8; 32]),
        Err(Error::WrongKey)
    ));
    assert!(matches!(
        Database::restore(&tmp.path().join("missing"), &tmp.path().join("y"), [7; 32]),
        Err(Error::Invalid(_))
    ));
    // Wrong-key restore leaves no destination behind.
    assert!(!tmp.path().join("x").exists());
    db.close()
}
