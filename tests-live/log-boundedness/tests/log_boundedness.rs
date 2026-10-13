//! Rust port of the Go `log-boundedness` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/log-boundedness/`
//! (`log_boundedness_test.go`). `TestLogBoundednessUnderSustainedLoad`: two
//! nodes absorb sustained alternating writes for `MURMUR_LOG_BOUNDEDNESS_SECONDS`
//! (default 75s) while log-GC runs; per-node log growth must stay under
//! `128 KiB + 16 bytes/row`, no panic/fatal may appear, on-disk spool growth
//! must stay under 8 KiB/row without second-half acceleration, and the mesh
//! must converge on exactly the acked rows with identical digests.
//!
//! The Rust product has no daemon text log, so the log-growth ceiling maps
//! onto retained origin-log batches (`committed - collected_transactions`):
//! every sustained insert appends one origin-log batch, so a dead GC grows
//! the retained set ~1 batch/row while a working GC holds it ~flat. Plain
//! SQL replaces the Go typed-contention HTTP API (see the scenario
//! README.md for the deviations).

use std::path::Path;
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Minimum acked rows for the boundedness proof (Go: `acked < 100` fails).
const MIN_ACKED: usize = 100;
/// Retained-batch ceiling floor per node. Absorbs the minimum-retained
/// batches plus in-flight unacknowledged batches at the sample points; the
/// `produced / 4` slope below is far under the dead-GC slope of ~1
/// retained batch per produced row, so a stalled GC fails.
const RETAINED_FLOOR_BATCHES: u64 = 512;
/// On-disk growth cap per acked row (Go: 8 KiB/row; observed ~2.2 KiB/row).
const DISK_BYTES_PER_ROW: u64 = 8192;
/// Second-half acceleration slack (Go: `2 * first_half + 1 MiB`).
const DISK_ACCEL_SLACK: u64 = 1024 * 1024;
/// Convergence deadline after the load window (Go: 90s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(90);
/// Convergence poll interval (Go: 200ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// One application row (Go: `harness.TypedContentionRow`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: String,
    name: String,
    phone: String,
    score: i64,
}

/// Zero-based 32-hex-char key (Go: `fmt.Sprintf("%032x", seq)`).
fn key_id(seq: usize) -> String {
    format!("{seq:032x}")
}

/// ~200-byte value (Go: `"payload-%d-" + strings.Repeat("abcdefgh", 25)`).
fn payload(seq: usize) -> String {
    format!("payload-{seq}-{}", "abcdefgh".repeat(25))
}

/// One sustained insert, published to replication (Go:
/// `Cluster.TypedContentionInsert(seq%2, ...)` with zero phone/score).
fn insert_row(db: &Database, id: String, name: String) {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
            (id, name, "", 0i64),
        )?;
        Ok(())
    })
    .expect("load insert failed");
    db.flush().expect("flush after load insert");
}

/// Canonical PK-ordered rows on one node (compared directly instead of
/// hashing, as in the `gc-balance` port of the Go digest check).
fn all_rows(db: &Database) -> Vec<Row> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name, phone, score FROM contention ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok(Row {
                id: row.get(0)?,
                name: row.get(1)?,
                phone: row.get(2)?,
                score: row.get(3)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read all rows failed")
}

/// Total bytes of regular files under `path` (Go: `spoolSizes` walk; errors
/// are skipped like Go's nil-returning walk callback).
fn dir_size(path: &Path) -> u64 {
    fn walk(path: &Path, total: &mut u64) {
        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            match entry.file_type() {
                Ok(ft) if ft.is_dir() => walk(&entry_path, total),
                Ok(ft) if ft.is_file() => {
                    if let Ok(meta) = entry.metadata() {
                        *total += meta.len();
                    }
                }
                _ => {}
            }
        }
    }
    let mut total = 0;
    walk(path, &mut total);
    total
}

/// Origin-log balance counters sampled from one node (Go: log-file size +
/// `/metrics` cursors; see the `gc-balance` port for the mapping).
struct LogSnapshot {
    /// Local commits produced (`Status.committed` cursor).
    commits: u64,
    /// Origin-log transactions collected (durable).
    collected: u64,
    /// Latest replication-engine error, if any. Rust has no text log to
    /// grep for panic/fatal, so the no-errors check asserts this is
    /// unchanged across the load window instead (collection and engine
    /// errors land here).
    last_error: Option<String>,
}

fn log_counters(cluster: &Cluster, idx: usize) -> LogSnapshot {
    let db = cluster.node(idx);
    LogSnapshot {
        commits: db.status().committed,
        collected: db
            .retention_status()
            .expect("retention status")
            .collected_transactions,
        last_error: db
            .replication()
            .expect("replication handle")
            .status()
            .last_error,
    }
}

/// Retained origin-log batches: every local commit appends one batch and
/// collection removes them, so this is the in-process analog of the Go
/// daemon's log-file size.
fn retained(snap: &LogSnapshot) -> u64 {
    snap.commits.saturating_sub(snap.collected)
}

/// Poll until every node holds exactly the acked rows with identical
/// contents (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: &[Row], timeout: Duration) {
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        if all_rows(cluster.node(0)) != *want {
            return false;
        }
        (1..cluster.len()).all(|i| all_rows(cluster.node(i)) == *want)
    });
    assert!(
        converged,
        "nodes did not converge on {} rows within {timeout:?}",
        want.len()
    );
}

#[test]
fn log_boundedness_under_sustained_load() {
    // Log-GC must run during the load window. The product defaults (1h
    // retention, 60s cadence) would collect nothing inside a 75s window,
    // so the port uses the same aggressive overlays as the `gc-balance`
    // port (Go relies on the daemon's own GC cadence instead).
    let cluster = Cluster::new(ClusterOptions {
        name: "log-boundedness".into(),
        num_nodes: 2,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            retention_interval: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "{} dir={} repl={}",
            info.label,
            info.dir.display(),
            info.repl_addr
        );
    }
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: TypedRecords/TypedContention tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE contention(id TEXT PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)",
                ))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }

        let duration = env_seconds("MURMUR_LOG_BOUNDEDNESS_SECONDS", 75);
        let start_log: Vec<LogSnapshot> = (0..cluster.len()).map(|i| log_counters(&cluster, i)).collect();
        let start_disk: Vec<u64> = (0..cluster.len())
            .map(|i| dir_size(&cluster.node_info(i).data_dir))
            .collect();
        let mut mid_log: Vec<u64> = (0..cluster.len()).map(|i| retained(&start_log[i])).collect();
        let mut mid_disk = start_disk.clone();

        // Fixed-window sustained write load, alternating nodes.
        let deadline = Instant::now() + duration;
        let mut acked: usize = 0;
        let mut seq: usize = 0;
        let mut sampled = false;
        while Instant::now() < deadline {
            let node = seq % cluster.len();
            insert_row(cluster.node(node), key_id(seq), payload(seq));
            acked += 1;
            seq += 1;
            if !sampled && deadline.saturating_duration_since(Instant::now()) < duration / 2 {
                mid_log = (0..cluster.len())
                    .map(|i| retained(&log_counters(&cluster, i)))
                    .collect();
                mid_disk = (0..cluster.len())
                    .map(|i| dir_size(&cluster.node_info(i).data_dir))
                    .collect();
                sampled = true;
            }
        }
        // A trivially short window may never cross the halfway sample; pin
        // the midpoint to the end so the acceleration check degrades to a
        // pass instead of comparing against the start twice.
        if !sampled {
            mid_log = (0..cluster.len())
                .map(|i| retained(&log_counters(&cluster, i)))
                .collect();
            mid_disk = (0..cluster.len())
                .map(|i| dir_size(&cluster.node_info(i).data_dir))
                .collect();
        }

        eprintln!("load complete: {acked} acked rows in {duration:?}");
        assert!(
            acked >= MIN_ACKED,
            "only {acked} rows acked, load too small to judge boundedness"
        );

        let end_log: Vec<LogSnapshot> =
            (0..cluster.len()).map(|i| log_counters(&cluster, i)).collect();
        let end_disk: Vec<u64> = (0..cluster.len())
            .map(|i| dir_size(&cluster.node_info(i).data_dir))
            .collect();
        for i in 0..cluster.len() {
            eprintln!(
                "node{} retained batches start={} mid={} end={} (growth={})",
                i + 1,
                retained(&start_log[i]),
                mid_log[i],
                retained(&end_log[i]),
                retained(&end_log[i]).saturating_sub(retained(&start_log[i]))
            );
            eprintln!(
                "node{} data-dir bytes start={} mid={} end={} (growth={})",
                i + 1,
                start_disk[i],
                mid_disk[i],
                end_disk[i],
                end_disk[i].saturating_sub(start_disk[i])
            );
        }

        // Primary assertion: per-node retained-batch growth ceiling. The
        // floor absorbs the minimum-retained batches and in-flight
        // unacknowledged batches; the produced/4 slope is far below the
        // dead-GC slope of ~1 retained batch per produced row, so a
        // stalled GC fails (Go: `128 KiB + 16 bytes/row` on the log file).
        for i in 0..cluster.len() {
            let produced = end_log[i].commits.saturating_sub(start_log[i].commits);
            let collected = end_log[i]
                .collected
                .saturating_sub(start_log[i].collected);
            let growth = produced.saturating_sub(collected);
            let ceiling = RETAINED_FLOOR_BATCHES + produced / 4;
            assert!(
                growth <= ceiling,
                "node{} retained-batch growth {growth} over {produced} produced rows exceeds ceiling {ceiling} (unbounded log retention?)",
                i + 1
            );
        }

        // No engine errors may be recorded during the load window (Go:
        // no panic/fatal in the daemon log).
        for i in 0..cluster.len() {
            assert_eq!(
                end_log[i].last_error, start_log[i].last_error,
                "node{} replication engine error during the load window: before={:?} after={:?}",
                i + 1,
                start_log[i].last_error,
                end_log[i].last_error
            );
        }

        // Disk usage must stay proportional to data (Go: 8 KiB/row cap,
        // and second-half growth must not accelerate past 2x the first
        // half plus 1 MiB slack).
        for i in 0..cluster.len() {
            let growth = end_disk[i].saturating_sub(start_disk[i]);
            let cap = acked as u64 * DISK_BYTES_PER_ROW;
            assert!(
                growth <= cap,
                "node{} data-dir growth {growth} bytes over {acked} rows exceeds {cap} (8KiB/row cap)",
                i + 1
            );
            let first_half = mid_disk[i].saturating_sub(start_disk[i]);
            let second_half = end_disk[i].saturating_sub(mid_disk[i]);
            assert!(
                second_half <= 2 * first_half + DISK_ACCEL_SLACK,
                "node{} data-dir growth accelerates: first-half={first_half} second-half={second_half}",
                i + 1
            );
        }

        // Positive control: honest behavior works — the mesh converges on
        // exactly the acked rows with identical contents.
        let want: Vec<Row> = (0..acked)
            .map(|s| Row {
                id: key_id(s),
                name: payload(s),
                phone: String::new(),
                score: 0,
            })
            .collect();
        wait_converged(&cluster, &want, CONVERGE_TIMEOUT);
        eprintln!("PASS: log growth bounded, disk proportional, mesh converged on {acked} rows");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("log-boundedness-unbounded");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
