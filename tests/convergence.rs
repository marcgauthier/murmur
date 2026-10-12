use sqlite_session::*;
fn create(path: &std::path::Path, node: u8) -> Result<Database> {
    let db = Database::open(
        path,
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            database_id: Some([1; 16]),
            node_id: Some([node; 16]),
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new("CREATE TABLE devices(id TEXT PRIMARY KEY NOT NULL, hostname TEXT NOT NULL, status TEXT NOT NULL)"))?;
    Ok(db)
}
#[test]
fn cells_deletions_reinsert_and_duplicate_delivery() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = create(&tmp.path().join("a"), 1)?;
    let b = create(&tmp.path().join("b"), 2)?;
    a.transaction(|c| {
        c.execute("INSERT INTO devices VALUES('one','initial','offline')", [])?;
        Ok(())
    })?;
    b.merge_state(a.export_state()?)?;
    a.transaction(|c| {
        c.execute("UPDATE devices SET hostname='router' WHERE id='one'", [])?;
        Ok(())
    })?;
    b.transaction(|c| {
        c.execute("UPDATE devices SET status='online' WHERE id='one'", [])?;
        Ok(())
    })?;
    let ax = a.export_state()?;
    let bx = b.export_state()?;
    a.merge_state(bx.clone())?;
    b.merge_state(ax.clone())?;
    a.merge_state(bx)?;
    b.merge_state(ax)?;
    assert_eq!(a.export_state()?.rows, b.export_state()?.rows);
    let baseline = a.export_state()?;
    a.transaction(|c| {
        c.execute("DELETE FROM devices WHERE id='one'", [])?;
        Ok(())
    })?;
    b.transaction(|c| {
        c.execute(
            "UPDATE devices SET status='newer-update' WHERE id='one'",
            [],
        )?;
        Ok(())
    })?;
    let deleted = a.export_state()?;
    let updated = b.export_state()?;
    a.merge_state(updated)?;
    b.merge_state(deleted)?;
    assert!(a.export_state()?.rows.is_empty());
    assert!(b.export_state()?.rows.is_empty());
    b.transaction(|c| {
        c.execute("INSERT INTO devices VALUES('one','reborn','online')", [])?;
        Ok(())
    })?;
    a.merge_state(b.export_state()?)?;
    a.merge_state(baseline)?;
    let aa = a.export_state()?;
    let bb = b.export_state()?;
    assert_eq!(aa.rows, bb.rows);
    assert_eq!(aa.tombstones, bb.tombstones);
    a.close()?;
    b.close()
}
#[test]
fn schema_and_identity_mismatch_rejected() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = create(&tmp.path().join("a"), 1)?;
    let b = create(&tmp.path().join("b"), 2)?;
    let mut state = b.export_state()?;
    state.database_id = [9; 16];
    assert!(matches!(a.merge_state(state), Err(Error::SchemaMismatch)));
    b.migrate(Migration::new("ALTER TABLE devices ADD COLUMN extra TEXT"))?;
    assert!(matches!(
        a.merge_state(b.export_state()?),
        Err(Error::SchemaMismatch)
    ));
    a.close()?;
    b.close()
}
#[test]
fn three_replicas_converge_under_permutations() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut replicas = Vec::new();
    for n in 0..3 {
        replicas.push(create(&tmp.path().join(format!("n{n}")), n + 1)?);
    }
    replicas[0].transaction(|c| {
        for i in 0..8 {
            c.execute(
                "INSERT INTO devices VALUES(?, 'initial', 'offline')",
                [format!("d{i}")],
            )?;
        }
        Ok(())
    })?;
    let baseline = replicas[0].export_state()?;
    for db in &replicas[1..] {
        db.merge_state(baseline.clone())?;
    }
    for (n, db) in replicas.iter().enumerate() {
        db.transaction(move |c| {
            for i in 0..8 {
                if (i + n) % 4 == 0 {
                    c.execute("DELETE FROM devices WHERE id=?", [format!("d{i}")])?;
                } else if i % 2 == 0 {
                    c.execute(
                        "UPDATE devices SET hostname=? WHERE id=?",
                        rusqlite::params![format!("n{n}"), format!("d{i}")],
                    )?;
                } else {
                    c.execute(
                        "UPDATE devices SET status=? WHERE id=?",
                        rusqlite::params![format!("s{n}"), format!("d{i}")],
                    )?;
                }
            }
            c.execute(
                "INSERT INTO devices VALUES('concurrent', ?, 'new')",
                [format!("n{n}")],
            )?;
            Ok(())
        })?;
    }
    let states = replicas
        .iter()
        .map(Database::export_state)
        .collect::<Result<Vec<_>>>()?;
    for (n, db) in replicas.iter().enumerate() {
        for index in [n, (n + 2) % 3, (n + 1) % 3, n, (n + 1) % 3] {
            db.merge_state(states[index].clone())?;
        }
    }
    let result = replicas[0].export_state()?;
    for db in &replicas {
        assert_eq!(result, db.export_state()?);
        db.close()?;
    }
    Ok(())
}
#[test]
fn altered_payload_with_same_version_is_rejected() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db = create(&tmp.path().join("a"), 1)?;
    db.transaction(|c| {
        c.execute("INSERT INTO devices VALUES('one','original','online')", [])?;
        Ok(())
    })?;
    let baseline = db.export_state()?;
    let mut invalid = baseline.clone();
    invalid.rows[0].cells[1].value = Value::Text("tampered".into());
    assert!(matches!(db.merge_state(invalid), Err(Error::Corrupt(_))));
    assert_eq!(db.export_state()?, baseline);
    db.close()
}
