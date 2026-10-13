use sqlite_session::*;

#[test]
fn disabled_table_persists_locally_and_setting_survives_mapped_migrations() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = Database::open(
        &path,
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new("CREATE TABLE local(id INTEGER PRIMARY KEY,n INTEGER); CREATE TABLE shared(id INTEGER PRIMARY KEY)"))?;
    assert!(db.replication_enabled("local")?);
    assert!(db.replication_enabled("shared")?);
    db.set_replication("local", false)?;
    db.transaction(|s| {
        s.execute("INSERT INTO local VALUES(1,42)", [])?;
        Ok(())
    })?;
    let id = db
        .export_state()?
        .schema
        .tables
        .iter()
        .find(|t| t.name == "local")
        .unwrap()
        .id
        .clone();
    db.migrate(Migration {
        sql: "ALTER TABLE local RENAME TO renamed; ALTER TABLE renamed ADD COLUMN extra TEXT"
            .into(),
        mappings: vec![TableMapping {
            old_name: "local".into(),
            new_name: "renamed".into(),
            columns: vec![],
        }],
    })?;
    assert!(!db.replication_enabled("renamed")?);
    assert!(db.replication_enabled("shared")?);
    assert_eq!(
        db.export_state()?
            .schema
            .tables
            .iter()
            .find(|t| t.name == "renamed")
            .unwrap()
            .id,
        id
    );
    db.flush()?;
    db.close()?;
    let db = Database::open(&path, [7; 32], Options::default())?;
    assert!(!db.replication_enabled("RENAMED")?);
    assert!(db.replication_enabled("shared")?);
    assert_eq!(
        db.read(
            |s| Ok(s.query_row("SELECT n FROM renamed WHERE id=1", [], |r| r
                .get::<_, i64>(0))?)
        )?,
        42
    );
    db.SetReplication("renamed", true)?;
    db.close()?;
    let db = Database::open(&path, [7; 32], Options::default())?;
    assert!(db.replication_enabled("renamed")?);
    db.close()
}

#[test]
fn unknown_table_setting_fails_without_changing_existing_policy() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = Database::open(
        tmp.path().join("db"),
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY)"))?;
    assert!(db.set_replication("missing", false).is_err());
    assert!(db.replication_enabled("items")?);
    db.set_replication("items", false)?;
    db.set_replication("items", false)?;
    assert!(!db.replication_enabled("items")?);
    db.close()
}
