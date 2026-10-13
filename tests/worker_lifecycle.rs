use sqlite_session::*;
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[test]
fn close_drains_uncaptured_application_writes() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let db = Database::open(
        &path,
        [7; 32],
        Options {
            mode: OpenMode::CreateNew,
            capture_interval: Duration::from_secs(3600),
            capture_bytes: 1 << 20,
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER)",
    ))?;
    for id in 0..40 {
        db.transaction(move |s| {
            s.execute(
                "INSERT INTO items VALUES(?,?)",
                rusqlite::params![id, id * 2],
            )?;
            Ok(())
        })?;
    }
    db.close()?;
    assert!(matches!(db.transaction(|_| Ok(())), Err(Error::Closed)));
    let recovered = Database::open(&path, [7; 32], Options::default())?;
    assert_eq!(
        recovered.read(
            |s| Ok(
                s.query_row("SELECT count(*),sum(n) FROM items", [], |r| Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?
                )))?
            )
        )?,
        (40, 1560)
    );
    recovered.close()
}

#[test]
fn concurrent_close_preserves_exactly_the_successful_transactions() -> Result<()> {
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
    db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY)"))?;
    let barrier = Arc::new(Barrier::new(5));
    let mut accepted = std::thread::scope(|s| {
        let mut workers = Vec::new();
        for worker in 0..4 {
            let db = db.clone();
            let barrier = barrier.clone();
            workers.push(s.spawn(move || {
                let mut accepted = Vec::new();
                let first = worker * 100;
                db.transaction(move |sql| {
                    sql.execute("INSERT INTO items VALUES(?)", [first])?;
                    Ok(())
                })
                .unwrap();
                accepted.push(first);
                barrier.wait();
                for n in 1..30 {
                    let id = first + n;
                    match db.transaction(move |sql| {
                        sql.execute("INSERT INTO items VALUES(?)", [id])?;
                        Ok(())
                    }) {
                        Ok(()) => accepted.push(id),
                        Err(Error::Closed) => break,
                        Err(e) => panic!("unexpected transaction failure: {e}"),
                    }
                }
                accepted
            }));
        }
        barrier.wait();
        db.close().unwrap();
        workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect::<Vec<_>>()
    });
    accepted.sort();
    let recovered = Database::open(&path, [7; 32], Options::default())?;
    let actual = recovered.read(|sql| {
        let mut statement = sql.prepare("SELECT id FROM items ORDER BY id")?;
        Ok(statement
            .query_map([], |r| r.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })?;
    assert_eq!(actual, accepted);
    recovered.close()
}
