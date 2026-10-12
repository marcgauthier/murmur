//! Raw in-memory speed: SQLite write/read path with persistence deferred
//! (no replication, no checkpoint during the timed section).
//!
//! Usage: rawbench [secs] (default 5)
//! Cases: raw rusqlite+session inserts, Database single-row inserts,
//! Database batched inserts, Database point selects, full scans.

use rusqlite::{Connection, session::Session};
use sqlite_session::{Database, Migration, OpenMode, Options};
use std::time::{Duration, Instant};

const VALUE_BYTES: usize = 200;

fn payload(id: i64) -> String {
    let head = format!("item-{id:08} ");
    let mut s = String::with_capacity(VALUE_BYTES);
    s.push_str(&head);
    while s.len() + 12 < VALUE_BYTES {
        s.push_str("lorem ipsum ");
    }
    while s.len() < VALUE_BYTES {
        s.push('.');
    }
    s.truncate(VALUE_BYTES);
    s
}

/// Simple deterministic PRNG (no deps).
struct Rng(u64);
impl Rng {
    fn next(&mut self, bound: i64) -> i64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((self.0 >> 33) as i64).rem_euclid(bound)
    }
}

fn deferred_options() -> Options {
    Options {
        mode: OpenMode::CreateNew,
        capture_interval: Duration::from_secs(3600),
        capture_bytes: 128 << 20,
        queue_bytes: 128 << 20,
        transaction_bytes: 128 << 20,
        ..Options::default()
    }
}

fn raw_inserts(secs: u64) -> Result<(), Box<dyn std::error::Error>> {
    let c = Connection::open_in_memory()?;
    c.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL, updated INTEGER NOT NULL)")?;
    let mut session = Session::new(&c)?;
    session.attach(Some("items"))?;
    let budget = Duration::from_secs(secs);
    let start = Instant::now();
    let mut n = 0i64;
    {
        let mut q = c.prepare("INSERT INTO items VALUES(?, ?, 0)")?;
        while start.elapsed() < budget {
            n += 1;
            q.execute((n, payload(n)))?;
        }
    }
    let el = start.elapsed().as_secs_f64();
    println!(
        "raw rusqlite+session, 1 row/txn: {n} inserts in {el:.1}s = {:.0}/s",
        n as f64 / el
    );
    Ok(())
}

fn db_inserts(path: &std::path::Path, secs: u64, batch: i64) -> Result<Database, Box<dyn std::error::Error>> {
    let db = Database::open(path, [7; 32], deferred_options())?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL, updated INTEGER NOT NULL)",
    ))?;
    let budget = Duration::from_secs(secs);
    let start = Instant::now();
    let mut n = 0i64;
    let mut capped = false;
    while start.elapsed() < budget {
        let first = n + 1;
        let result = db.transaction(move |s| {
            for id in first..first + batch {
                let v = payload(id);
                s.execute("INSERT INTO items VALUES(?, ?, 0)", (id, &v))?;
            }
            Ok(())
        });
        match result {
            Ok(()) => n += batch,
            Err(sqlite_session::Error::Limit(_)) => {
                capped = true;
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }
    let el = start.elapsed().as_secs_f64();
    println!(
        "Database deferred, {batch} row(s)/txn: {n} inserts in {el:.1}s = {:.0}/s{}",
        n as f64 / el,
        if capped { " (hit 128 MiB session budget)" } else { "" },
    );
    Ok(db)
}

fn point_selects(db: &Database, rows: i64, secs: u64) -> Result<(), Box<dyn std::error::Error>> {
    let budget = Duration::from_secs(secs);
    let start = Instant::now();
    let mut rng = Rng(0x1234);
    let mut n = 0u64;
    while start.elapsed() < budget {
        let id = rng.next(rows) + 1;
        db.read(move |s| {
            Ok(s.query_row("SELECT value FROM items WHERE id=?", [id], |r| {
                r.get::<_, String>(0)
            })?)
        })?;
        n += 1;
    }
    let el = start.elapsed().as_secs_f64();
    println!(
        "Database point select by PK ({rows} rows): {n} selects in {el:.1}s = {:.0}/s ({:.1}us avg)",
        n as f64 / el,
        el * 1e6 / n as f64,
    );
    Ok(())
}

fn scans(db: &Database, secs: u64) -> Result<(), Box<dyn std::error::Error>> {
    let budget = Duration::from_secs(secs);
    let start = Instant::now();
    let mut n = 0u64;
    let mut rows_seen = 0i64;
    while start.elapsed() < budget {
        let count: i64 = db.read(|s| {
            Ok(s.query_row("SELECT COUNT(*), SUM(LENGTH(value)) FROM items", [], |r| {
                r.get::<_, i64>(0)
            })?)
        })?;
        rows_seen += count;
        n += 1;
    }
    let el = start.elapsed().as_secs_f64();
    println!(
        "Database full scan: {n} scans in {el:.1}s ({:.1}/s, {:.0} rows scanned/s)",
        n as f64 / el,
        rows_seen as f64 / el,
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secs: u64 = std::env::args()
        .nth(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(5);
    if secs == 0 {
        return Err("duration must be positive".into());
    }
    println!("{secs}s per case, 200-byte text values");
    raw_inserts(secs)?;
    let temp = tempfile::tempdir()?;
    let db = db_inserts(&temp.path().join("single"), secs, 1)?;
    db.close()?;
    let db = db_inserts(&temp.path().join("batch"), secs, 20)?;
    let rows: i64 = db.read(|s| {
        Ok(s.query_row("SELECT COUNT(*) FROM items", [], |r| r.get::<_, i64>(0))?)
    })?;
    point_selects(&db, rows, secs)?;
    scans(&db, secs)?;
    db.close()?;
    Ok(())
}
