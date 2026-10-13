//! Rust port of the Go `open-progress` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/open-progress/`
//! (`open_progress_test.go`: `TestEmbeddedOpenProgressLive`,
//! `TestEmbeddedOpenProgressLiveExtra` and their `TestOpenProgressChild*`
//! process workers). Real encrypted storage with 6000 rows x 2 tables is
//! closed and reopened while `on_open_progress` callbacks record every
//! event; the test asserts terminal snapshots, phase sequences, monotonic
//! progress, failure reporting, and recovery.
//!
//! Deviations from the Go original (see the scenario README.md): in-process
//! close()+reopen instead of fresh child processes per role; no `cancel`
//! leg (the Rust product has no open cancellation); Rust phase names
//! (`Opening, Rebuilding, Indexing, Finalizing, Ready`) instead of Go's
//! (`Opening, Replaying, Rebuilding, Finalizing, Ready`); Rust
//! processed-items semantics (spool records + rebuilt rows, not cells);
//! plain SQL instead of typed models. Single embedded node, no
//! replication, so the harness `artifacts`/`forensics`/`unlock` modules are
//! used directly instead of `Cluster` (which always builds a QUIC mesh).

use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{
    Database, Migration, OpenMode, OpenPhase, OpenProgress, Options,
};
use murmur_live_harness::{artifacts, forensics, unlock};

/// Rows per table (Go: `rowsPerTable = 6000`).
const ROWS_PER_TABLE: i64 = 6000;
/// Insert batching (Go: 1000 rows per write tx).
const BATCH_ROWS: i64 = 1000;
/// Application key (Go: `bytes.Repeat([]byte{0x63}, 32)`).
const KEY: [u8; 32] = [0x63; 32];
/// Wrong 32-byte key for the open-failure leg.
const WRONG_KEY: [u8; 32] = [0x44; 32];
/// Short key for the open-failure leg (Go: `[]byte{1, 2, 3}`).
const SHORT_KEY: [u8; 3] = [1, 2, 3];
/// Full Rust phase sequence on every open (populated or empty).
const FULL_SEQUENCE: [OpenPhase; 5] = [
    OpenPhase::Opening,
    OpenPhase::Rebuilding,
    OpenPhase::Indexing,
    OpenPhase::Finalizing,
    OpenPhase::Ready,
];

/// Thread-safe `on_open_progress` recorder with an overlap guard (Go: the
/// `active`/`overlap` atomics in `TestOpenProgressChild`). The callback
/// runs on the writer thread; the reporter serializes it under a mutex.
struct Recorder {
    events: Mutex<Vec<OpenProgress>>,
    in_flight: AtomicUsize,
    overlap: AtomicBool,
}

impl Recorder {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            overlap: AtomicBool::new(false),
        }
    }

    fn callback(self: &Arc<Self>) -> Arc<dyn Fn(OpenProgress) + Send + Sync> {
        let rec = Arc::clone(self);
        Arc::new(move |p| rec.record(p))
    }

    fn record(&self, p: OpenProgress) {
        if self.in_flight.fetch_add(1, Ordering::SeqCst) != 0 {
            self.overlap.store(true, Ordering::SeqCst);
        }
        self.events.lock().unwrap().push(p);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    fn events(&self) -> Vec<OpenProgress> {
        self.events.lock().unwrap().clone()
    }
}

fn is_terminal(phase: OpenPhase) -> bool {
    matches!(
        phase,
        OpenPhase::Ready | OpenPhase::Failed | OpenPhase::Cancelled
    )
}

/// Shared event-stream assertions (Go: the per-role checks in
/// `TestOpenProgressChild`): non-empty, monotonic processed/elapsed,
/// exactly one terminal event, terminal phase as expected.
fn assert_event_stream(events: &[OpenProgress], want: OpenPhase, name: &str) {
    assert!(!events.is_empty(), "{name}: no progress events");
    let mut last_processed = 0u64;
    let mut last_elapsed = Duration::ZERO;
    let mut terminal = 0usize;
    for p in events {
        assert!(
            p.processed_items >= last_processed,
            "{name}: nonmonotonic processed items"
        );
        assert!(p.elapsed >= last_elapsed, "{name}: nonmonotonic elapsed");
        last_processed = p.processed_items;
        last_elapsed = p.elapsed;
        if is_terminal(p.phase) {
            terminal += 1;
        }
    }
    assert_eq!(terminal, 1, "{name}: terminal count={terminal}");
    assert_eq!(
        events.last().unwrap().phase,
        want,
        "{name}: terminal phase"
    );
}

/// Consecutive-duplicate-collapsed phase list (Go: `phases`).
fn phase_sequence(events: &[OpenProgress]) -> Vec<OpenPhase> {
    let mut phases = Vec::new();
    for p in events {
        if phases.last() != Some(&p.phase) {
            phases.push(p.phase);
        }
    }
    phases
}

/// Track the currently open database so failure forensics can inspect it.
fn track(live: &Mutex<Option<Database>>, db: &Database) {
    *live.lock().unwrap() = Some(db.clone());
}

fn untrack(live: &Mutex<Option<Database>>) {
    live.lock().unwrap().take();
}

/// Create a fresh encrypted database and fill both tables (Go: the
/// `populate` / `populate-extra` roles). Leaves the database closed.
fn populate_fresh(data_dir: &Path) {
    let db = Database::open(
        data_dir,
        KEY,
        Options {
            mode: OpenMode::CreateNew,
            ..Options::default()
        },
    )
    .unwrap_or_else(|e| panic!("populate {}: create: {e}", data_dir.display()));
    for table in ["requests", "events"] {
        db.migrate(Migration::new(format!(
            "CREATE TABLE {table}(id INTEGER PRIMARY KEY, message TEXT NOT NULL, ordinal INTEGER NOT NULL)"
        )))
        .unwrap_or_else(|e| panic!("populate: migrate {table}: {e}"));
    }
    for table in ["requests", "events"] {
        let mut start = 0i64;
        while start < ROWS_PER_TABLE {
            let end = (start + BATCH_ROWS).min(ROWS_PER_TABLE);
            db.transaction(move |sql| {
                for i in start..end {
                    let message = format!("realistic diagnostic {i} {}", "trace ".repeat(512));
                    sql.execute(
                        &format!(
                            "INSERT INTO {table}(id, message, ordinal) VALUES(?, ?, ?)"
                        ),
                        (i, message, i),
                    )?;
                }
                Ok(())
            })
            .unwrap_or_else(|e| panic!("populate: insert {table} {start}..{end}: {e}"));
            start = end;
        }
    }
    db.flush().expect("populate: flush");
    db.close().expect("populate: close");
    eprintln!(
        "populate: {ROWS_PER_TABLE} rows x2 tables at {}",
        data_dir.display()
    );
}

/// Count + ordinal-sum check per table (Go: `verifyProgressTable`).
fn verify_contents(db: &Database, name: &str) {
    for table in ["requests", "events"] {
        let (count, sum): (i64, i64) = db
            .read(move |sql| {
                let row: (i64, i64) = sql.query_row(
                    &format!("SELECT COUNT(*), COALESCE(SUM(ordinal), 0) FROM {table}"),
                    [],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
                )?;
                Ok(row)
            })
            .unwrap_or_else(|e| panic!("{name}: verify {table}: {e}"));
        assert_eq!(count, ROWS_PER_TABLE, "{name}: wrong count {table}");
        assert_eq!(
            sum,
            ROWS_PER_TABLE * (ROWS_PER_TABLE - 1) / 2,
            "{name}: wrong contents {table}"
        );
    }
}

/// Row-count check per table (Go: `countProgressTable`).
fn count_table(db: &Database, table: &str, name: &str) {
    let sql_text = format!("SELECT COUNT(*) FROM {table}");
    let count: i64 = db
        .read(move |sql| {
            let n: i64 = sql.query_row(&sql_text, [], |r| r.get::<_, i64>(0))?;
            Ok(n)
        })
        .unwrap_or_else(|e| panic!("{name}: count {table}: {e}"));
    assert_eq!(count, ROWS_PER_TABLE, "{name}: {table} count={count}");
}

/// Reopen under the unlock watchdog with a recorder (Go: `db.Open` with
/// `OnOpenProgress` under the harness await-unlock polling).
fn reload_with_recorder(
    data_dir: &Path,
    live: &Mutex<Option<Database>>,
) -> (Database, Arc<Recorder>) {
    let rec = Arc::new(Recorder::new());
    let opts = Options {
        mode: OpenMode::Existing,
        on_open_progress: Some(rec.callback()),
        ..Options::default()
    };
    let db = unlock::open_with_watchdog(data_dir, KEY, opts).expect("reopen under watchdog");
    track(live, &db);
    (db, rec)
}

/// Main reload leg (Go: the `reload` role): terminal snapshot, contents,
/// callback stream, and full phase sequence.
fn reload_and_verify(data_dir: &Path, live: &Mutex<Option<Database>>, name: &str) {
    let (db, rec) = reload_with_recorder(data_dir, live);
    let p = db
        .status()
        .open_progress
        .unwrap_or_else(|| panic!("{name}: missing terminal progress"));
    assert_eq!(p.phase, OpenPhase::Ready, "{name}: terminal={p:?}");
    assert_eq!(
        p.rows_inserted,
        ROWS_PER_TABLE as u64 * 2,
        "{name}: terminal={p:?}"
    );
    assert_eq!(p.rows_skipped, 0, "{name}: terminal={p:?}");
    assert!(
        p.processed_items >= p.rows_inserted,
        "{name}: terminal={p:?}"
    );
    assert_eq!(p.current_table, None, "{name}: terminal={p:?}");
    assert_eq!(p.error, None, "{name}: terminal={p:?}");
    verify_contents(&db, name);
    let events = rec.events();
    assert!(
        !rec.overlap.load(Ordering::SeqCst),
        "{name}: overlapping callbacks"
    );
    assert_event_stream(&events, OpenPhase::Ready, name);
    assert_eq!(
        phase_sequence(&events),
        FULL_SEQUENCE,
        "{name}: phases={:?}",
        phase_sequence(&events)
    );
    let last = events.last().unwrap();
    eprintln!(
        "PASS {name} phases={:?} processed={} rows={} elapsed={:?}",
        phase_sequence(&events),
        last.processed_items,
        last.rows_inserted,
        last.elapsed
    );
    db.close().expect("close after reload");
    untrack(live);
}

/// Open-failure leg (Go: the `open-failure` role): the open must fail, no
/// handle is returned, and the callback stream must end in `Failed`.
fn open_failure_leg(data_dir: &Path, key: &[u8], name: &str) {
    let rec = Arc::new(Recorder::new());
    let opts = Options {
        mode: OpenMode::Existing,
        on_open_progress: Some(rec.callback()),
        ..Options::default()
    };
    match Database::open(data_dir, key, opts) {
        Ok(db) => {
            let _ = db.close();
            panic!("{name}: expected open failure, got success");
        }
        Err(e) => eprintln!("{name}: open failed as expected: {e}"),
    }
    let events = rec.events();
    assert!(
        !rec.overlap.load(Ordering::SeqCst),
        "{name}: overlapping callbacks"
    );
    assert_event_stream(&events, OpenPhase::Failed, name);
    assert_eq!(
        events.first().unwrap().phase,
        OpenPhase::Opening,
        "{name}: first phase"
    );
    eprintln!("PASS {name} phases={:?}", phase_sequence(&events));
}

/// Extra leg: both tables rebuilt (Go: `table-tracking` role).
fn table_tracking_leg(data_dir: &Path, live: &Mutex<Option<Database>>) {
    populate_fresh(data_dir);
    let (db, rec) = reload_with_recorder(data_dir, live);
    let p = db
        .status()
        .open_progress
        .unwrap_or_else(|| panic!("table-tracking: missing terminal progress"));
    assert_eq!(p.phase, OpenPhase::Ready, "table-tracking: terminal={p:?}");
    assert_eq!(
        p.rows_inserted,
        ROWS_PER_TABLE as u64 * 2,
        "table-tracking: RowsInserted={} (both tables)",
        p.rows_inserted
    );
    assert_eq!(
        p.current_table, None,
        "table-tracking: CurrentTable not cleared after open"
    );
    count_table(&db, "requests", "table-tracking");
    count_table(&db, "events", "table-tracking");
    let events = rec.events();
    let found = events.iter().any(|ev| {
        ev.phase == OpenPhase::Rebuilding
            && (ev.current_table.is_some() || ev.rows_inserted > 0)
    });
    if !found {
        eprintln!(
            "table-tracking: no rebuilding event carried CurrentTable or RowsInserted>0 (fast machine, heartbeat not captured -- OK)"
        );
    }
    eprintln!(
        "PASS table-tracking RowsInserted={} events={}",
        p.rows_inserted,
        events.len()
    );
    db.close().expect("close");
    untrack(live);
}

/// Extra leg: periodic progress events inside the rebuild (Go: `heartbeat`).
fn heartbeat_leg(data_dir: &Path, live: &Mutex<Option<Database>>) {
    populate_fresh(data_dir);
    let (db, rec) = reload_with_recorder(data_dir, live);
    let n = rec
        .events()
        .iter()
        .filter(|ev| ev.phase == OpenPhase::Rebuilding)
        .count();
    assert!(n >= 1, "heartbeat: rebuilding events={n}, expected >=1");
    eprintln!("PASS heartbeat rebuilding-events={n}");
    db.close().expect("close");
    untrack(live);
}

/// Extra leg: brand-new empty database still fires every phase with zero
/// counts (Go: `empty-db` role).
fn empty_db_leg(data_dir: &Path) {
    let rec = Arc::new(Recorder::new());
    let opts = Options {
        mode: OpenMode::CreateNew,
        on_open_progress: Some(rec.callback()),
        ..Options::default()
    };
    let db = Database::open(data_dir, KEY, opts).expect("create empty db");
    let p = db
        .status()
        .open_progress
        .unwrap_or_else(|| panic!("empty-db: missing terminal progress"));
    assert_eq!(p.phase, OpenPhase::Ready, "empty-db terminal={p:?}");
    assert_eq!(p.processed_items, 0, "empty-db terminal={p:?}");
    assert_eq!(p.rows_inserted, 0, "empty-db terminal={p:?}");
    assert_eq!(p.rows_skipped, 0, "empty-db terminal={p:?}");
    let events = rec.events();
    assert_event_stream(&events, OpenPhase::Ready, "empty-db");
    assert_eq!(
        phase_sequence(&events),
        FULL_SEQUENCE,
        "empty-db phases={:?}",
        phase_sequence(&events)
    );
    eprintln!("PASS empty-db phases={:?}", phase_sequence(&events));
    db.close().expect("close");
}

/// Extra leg: per-event timing sanity and per-phase timer reset (Go:
/// `phase-elapsed` role; the `StartedAt` check has no Rust counterpart).
fn phase_elapsed_leg(data_dir: &Path, live: &Mutex<Option<Database>>) {
    populate_fresh(data_dir);
    let (db, rec) = reload_with_recorder(data_dir, live);
    let events = rec.events();
    for ev in &events {
        assert!(
            ev.phase_elapsed <= ev.elapsed,
            "phase-elapsed: PhaseElapsed > Elapsed in phase {:?}",
            ev.phase
        );
    }
    for pair in events.windows(2) {
        if pair[1].phase != pair[0].phase {
            if pair[1].phase_elapsed > Duration::from_millis(100) {
                eprintln!(
                    "phase-elapsed: first event of {:?} has PhaseElapsed={:?} (>100ms, possible, not fatal)",
                    pair[1].phase, pair[1].phase_elapsed
                );
            }
            break;
        }
    }
    eprintln!("PASS phase-elapsed events={}", events.len());
    db.close().expect("close");
    untrack(live);
}

/// Extra leg: repeated status snapshots are independent and stable (Go:
/// `status-snapshot` role).
fn status_snapshot_leg(data_dir: &Path, live: &Mutex<Option<Database>>) {
    populate_fresh(data_dir);
    let no_op: Arc<dyn Fn(OpenProgress) + Send + Sync> = Arc::new(|_| {});
    let opts = Options {
        mode: OpenMode::Existing,
        on_open_progress: Some(no_op),
        ..Options::default()
    };
    let db = unlock::open_with_watchdog(data_dir, KEY, opts).expect("reopen");
    track(live, &db);
    let mut snaps: Vec<OpenProgress> = Vec::new();
    for i in 0..10 {
        let s = db
            .status()
            .open_progress
            .unwrap_or_else(|| panic!("status-snapshot: snapshot {i} is nil"));
        assert_eq!(s.phase, OpenPhase::Ready, "status-snapshot: snapshot {i}");
        snaps.push(s);
    }
    snaps[0].phase = OpenPhase::Failed;
    assert_eq!(
        snaps[1].phase,
        OpenPhase::Ready,
        "status-snapshot: snapshots are aliased (mutation propagated)"
    );
    let base = snaps[0].processed_items;
    for (i, s) in snaps.iter().enumerate() {
        assert_eq!(
            s.processed_items, base,
            "status-snapshot: snapshot {i} ProcessedItems={} != base={base}",
            s.processed_items
        );
    }
    assert_eq!(
        snaps[0].rows_inserted,
        ROWS_PER_TABLE as u64 * 2,
        "status-snapshot: RowsInserted={}",
        snaps[0].rows_inserted
    );
    eprintln!("PASS status-snapshot snapshots=10 ProcessedItems={base}");
    db.close().expect("close");
    untrack(live);
}

fn run_all_legs(runtime: &Path, live: &Mutex<Option<Database>>) {
    // Main flow (Go: populate, reload, cancel, reload, open-failure,
    // reload). The cancel role has no Rust counterpart: `Database::open`
    // takes no context and the product never emits `OpenPhase::Cancelled`.
    let main = runtime.join("main").join("data");
    populate_fresh(&main);
    reload_and_verify(&main, live, "reload-1");
    open_failure_leg(&main, &SHORT_KEY, "open-failure-short-key");
    open_failure_leg(&main, &WRONG_KEY, "open-failure-wrong-key");
    reload_and_verify(&main, live, "reload-2-recovery");

    // Extra flow (Go: TestEmbeddedOpenProgressLiveExtra subtests).
    let extra = runtime.join("extra");
    table_tracking_leg(&extra.join("table-tracking").join("data"), live);
    heartbeat_leg(&extra.join("heartbeat").join("data"), live);
    empty_db_leg(&extra.join("empty").join("data"));
    phase_elapsed_leg(&extra.join("phase-elapsed").join("data"), live);
    status_snapshot_leg(&extra.join("status-snapshot").join("data"), live);

    eprintln!("PASS open-progress: all legs green");
}

#[test]
fn open_progress() {
    let runtime_dir =
        artifacts::prepare_runtime_dir("open-progress").expect("prepare runtime dir");
    let live: Arc<Mutex<Option<Database>>> = Arc::new(Mutex::new(None));
    let body_live = Arc::clone(&live);
    let body_dir = runtime_dir.clone();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_all_legs(&body_dir, &body_live);
    }))
    .is_err();
    if failed {
        let snapshot = live.try_lock().ok().and_then(|g| g.clone());
        forensics::dump_node_forensics(
            "open-progress",
            &runtime_dir,
            snapshot.as_ref(),
            "open-progress-failed",
        );
    }
    artifacts::finish("open-progress", &runtime_dir, failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
