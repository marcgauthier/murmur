use rusqlite::{Connection, OpenFlags, Result as SqlResult, session::Session};
use sqlite_session::{open_in_memory, start_session};
use std::{env, error::Error, path::PathBuf, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut duration_secs = 30u64;
    let mut extension = PathBuf::from("./libcrsqlite");
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--duration" => {
                duration_secs = args.next().ok_or("missing --duration value")?.parse()?
            }
            "--extension" => {
                extension = PathBuf::from(args.next().ok_or("missing --extension value")?)
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    if duration_secs == 0 {
        return Err("duration must be greater than zero".into());
    }

    let plain = open_named_memory("rust-plain")?;
    run_case("sqlite", &plain, None, duration_secs)?;

    let crsql = open_named_memory("rust-crr")?;
    unsafe { crsql.load_extension(&extension, None::<&str>)? };
    crsql.query_row("SELECT crsql_as_crr('items')", [], |_| Ok(()))?;
    run_case("cr-sqlite CRR", &crsql, None, duration_secs)?;

    let session_db = open_in_memory()?;
    create_items(&session_db)?;
    let session = start_session(&session_db)?;
    run_case("sqlite session", &session_db, Some(session), duration_secs)?;
    Ok(())
}

fn open_named_memory(name: &str) -> SqlResult<Connection> {
    let uri = format!("file:{name}?mode=memory&cache=shared");
    let connection = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_SHARED_CACHE,
    )?;
    create_items(&connection)?;
    Ok(connection)
}

fn create_items(connection: &Connection) -> SqlResult<()> {
    connection.execute_batch(
        "CREATE TABLE items (
            id INTEGER NOT NULL PRIMARY KEY DEFAULT 0,
            value INTEGER NOT NULL DEFAULT 0
        );",
    )
}

fn run_case<'conn>(
    label: &str,
    connection: &'conn Connection,
    session: Option<Session<'conn>>,
    duration_secs: u64,
) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let deadline = start + std::time::Duration::from_secs(duration_secs);
    let mut count = 0u64;
    while Instant::now() < deadline {
        // Reparse a fresh SQL string for every row, matching the original
        // ExecTransient benchmark path rather than reusing a prepared statement.
        connection.execute_batch(&format!("INSERT INTO items(value) VALUES ({count});"))?;
        count += 1;
    }
    let elapsed = start.elapsed();
    drop(session);
    println!(
        "{label}: {count} inserts in {:.3}s = {:.0} inserts/s",
        elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}
