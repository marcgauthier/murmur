use rusqlite::{Connection, session::Session};
use sqlite_session::{Database, Migration, OpenMode, Options};
use std::time::{Duration, Instant};
const ROWS: i64 = 1000;
const BATCH: i64 = 100;
fn setup(c: &Connection) -> rusqlite::Result<()> {
    c.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")?;
    c.execute_batch("BEGIN")?;
    {
        let mut q = c.prepare("INSERT INTO items VALUES(?,0)")?;
        for n in 0..ROWS {
            q.execute([n])?;
        }
    }
    c.execute_batch("COMMIT")?;
    Ok(())
}
fn raw(duration: Duration, session: bool) -> Result<(), Box<dyn std::error::Error>> {
    let c = Connection::open_in_memory()?;
    setup(&c)?;
    let _session = if session {
        let mut s = Session::new(&c)?;
        s.attach(Some("items"))?;
        Some(s)
    } else {
        None
    };
    let start = Instant::now();
    let mut count = 0i64;
    let mut timings = Vec::new();
    while start.elapsed() < duration {
        let tx = Instant::now();
        c.execute_batch("BEGIN")?;
        {
            let mut q = c.prepare("UPDATE items SET value=? WHERE id=?")?;
            for n in count..count + BATCH {
                q.execute([n, n % ROWS])?;
            }
        }
        c.execute_batch("COMMIT")?;
        count += BATCH;
        timings.push(tx.elapsed().as_micros());
    }
    report(
        if session {
            "session only"
        } else {
            "plain SQLite"
        },
        count,
        start.elapsed(),
        timings,
    );
    Ok(())
}
fn report(label: &str, count: i64, elapsed: Duration, mut timings: Vec<u128>) {
    timings.sort_unstable();
    let p99 = timings[timings.len() * 99 / 100];
    println!(
        "{label}: {:.0} updates/s; transaction p99={}us (100 updates)",
        count as f64 / elapsed.as_secs_f64(),
        p99
    );
}
fn persistent(duration: Duration, background: bool) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("db");
    let mut opts = Options {
        mode: OpenMode::CreateNew,
        ..Default::default()
    };
    if !background {
        opts.capture_interval = Duration::from_secs(3600);
        opts.capture_bytes = 128 << 20;
        opts.queue_bytes = 128 << 20;
        opts.transaction_bytes = 128 << 20;
    } else {
        opts.snapshot_deltas = 1;
    }
    let db = Database::open(&path, [7; 32], opts)?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
    ))?;
    db.transaction(|c| {
        let mut q = c.prepare("INSERT INTO items VALUES(?,0)")?;
        for n in 0..ROWS {
            q.execute([n])?;
        }
        Ok(())
    })?;
    db.flush()?;
    let start = Instant::now();
    let mut count = 0i64;
    let mut timings = Vec::new();
    while start.elapsed() < duration {
        let tx = Instant::now();
        let begin = count;
        loop {
            match db.transaction(move |c| {
                let mut q = c.prepare("UPDATE items SET value=? WHERE id=?")?;
                for n in begin..begin + BATCH {
                    q.execute([n, n % ROWS])?;
                }
                Ok(())
            }) {
                Err(sqlite_session::Error::Busy) => std::thread::yield_now(),
                result => {
                    result?;
                    break;
                }
            }
        }
        count += BATCH;
        timings.push(tx.elapsed().as_micros());
    }
    report(
        if background {
            "session + background persistence"
        } else {
            "session (capture deferred)"
        },
        count,
        start.elapsed(),
        timings,
    );
    let s = db.status();
    println!(
        "  last extraction pause={:.3}ms, queued={} bytes, commit lag={}",
        s.checkpoint_pause.as_secs_f64() * 1000.,
        s.pending_bytes,
        s.committed - s.durable
    );
    let flush = Instant::now();
    db.flush()?;
    println!(
        "  final flush={:.3}ms",
        flush.elapsed().as_secs_f64() * 1000.
    );
    db.close()?;
    let recovery = Instant::now();
    let db = Database::open(path, [7; 32], Options::default())?;
    println!(
        "  recovery={:.3}ms",
        recovery.elapsed().as_secs_f64() * 1000.
    );
    db.close()?;
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secs = std::env::args()
        .nth(1)
        .map(|s| s.parse::<u64>())
        .transpose()?
        .unwrap_or(8);
    if secs == 0 {
        return Err("duration must be positive".into());
    }
    let duration = Duration::from_secs(secs);
    println!(
        "Repeated updates over {ROWS} rows, {BATCH} updates per transaction, {secs}s per case"
    );
    raw(duration, false)?;
    raw(duration, true)?;
    persistent(duration, false)?;
    persistent(duration, true)?;
    Ok(())
}
