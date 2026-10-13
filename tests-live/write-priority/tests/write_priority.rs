//! Rust port of the Go `write-priority` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/write-priority/`
//! (`write_priority_test.go`, `TestLocalWritePriority`). Two nodes take
//! continuous writes on both nodes (dense writers with disjoint 16-byte
//! row-ID ranges) while serving reads, under full-mesh replication. The
//! test gates on local progress (>= 80 rows, >= 4 reads), the replication
//! share of dual-contention writer service (<= 25% with >= 1s measured),
//! and convergence (identical row counts and identical row sets).
//!
//! Plain SQL replaces the Go typed-contention HTTP API (one transaction
//! per insert, as in Go), and the scheduler assertions read the in-process
//! `WriterScheduler::snapshot()` dual-service counters instead of scraping
//! Prometheus `/metrics` (see the scenario README.md).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};

/// Writers per node (Go: 256 goroutines per node). OS threads are heavier
/// than goroutines; 64 looping writers per node still keep both scheduler
/// classes backlogged, which is what the 90/10 policy leg needs.
const WRITERS: usize = 64;
/// Local-progress floors (Go: `expected < 80 || reads < 4`).
const MIN_ROWS: usize = 80;
const MIN_READS: usize = 4;
/// Dual-contention policy gate: replication share of dual service <= 25%
/// (Go README; the scheduler targets 90/10 with a 1s debt bound).
const MAX_REMOTE_SHARE: f64 = 0.25;
/// Vacuity gate: at least 1s of dual service measured (Go README).
const MIN_DUAL_SERVICE: Duration = Duration::from_secs(1);
/// Read pacing during the write phase (Go: 50ms sleeps).
const READ_PACING: Duration = Duration::from_millis(50);
/// Convergence poll interval (Go: 200ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);

/// 16-byte row ID with node/worker/seq embedded (Go: `raw[16]byte`);
/// disjoint across writers by construction, hex-encoded for the TEXT key.
fn hex_id(node: usize, worker: usize, seq: u64) -> String {
    let mut raw = [0u8; 16];
    raw[0] = node as u8;
    raw[1] = worker as u8;
    raw[8..].copy_from_slice(&seq.to_be_bytes());
    let mut s = String::with_capacity(32);
    for b in raw {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// One row insert, published to replication (Go: `TypedContentionInsert`).
fn contention_insert(db: &Database, id: String, name: String) -> Result<(), String> {
    db.transaction(move |sql| {
        sql.execute("INSERT INTO records(id, name) VALUES(?, ?)", (id, name))?;
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    db.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// Full sorted row set on one node (Go: `TypedContentionRows` + sort).
fn all_rows(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name FROM records ORDER BY id")?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("state query failed")
}

/// Dual-contention writer service (nanoseconds) summed across both nodes
/// (Go: `spedsql_sched_dual_*` scraped from each daemon's `/metrics`).
/// Returns (local, remote).
fn dual_service(cluster: &Cluster) -> (u64, u64) {
    let mut local = 0u64;
    let mut remote = 0u64;
    for i in 0..cluster.len() {
        let snap = cluster.node(i).scheduler().snapshot();
        local += snap.local.dual_service_nanos;
        remote += snap.remote.dual_service_nanos;
    }
    (local, remote)
}

#[test]
fn local_write_priority() {
    let write_for = env_seconds("MURMUR_LIVE_WRITE_PRIORITY_SECONDS", 12);
    let sync_timeout = env_seconds("MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS", 120);
    let cluster = Cluster::new(ClusterOptions {
        name: "write-priority".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on both nodes (Go: typed-contention tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        let (before_local, before_remote) = dual_service(&cluster);
        let deadline = Instant::now() + write_for;

        // Dense writers on both nodes saturate the microsecond-commit
        // scheduler into persistent backlog on both classes, where the
        // 90/10 policy binds (Go: writer goroutines + `wg.Wait()`).
        let written = AtomicUsize::new(0);
        let first_err: Mutex<Option<String>> = Mutex::new(None);
        let mut reads = 0usize;
        std::thread::scope(|s| {
            for node in 0..2 {
                for worker in 0..WRITERS {
                    let db = cluster.node(node).clone();
                    let written = &written;
                    let first_err = &first_err;
                    s.spawn(move || {
                        let mut seq: u64 = 0;
                        while Instant::now() < deadline {
                            let id = hex_id(node, worker, seq);
                            let name = format!("v-n{node}-w{worker}-{seq}");
                            if let Err(e) = contention_insert(&db, id, name) {
                                *first_err.lock().unwrap() = Some(e);
                                return;
                            }
                            seq += 1;
                            written.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            }
            // Reads stay served on both nodes while both replicate.
            while Instant::now() < deadline {
                for node in 0..2 {
                    let _ = all_rows(cluster.node(node));
                    reads += 1;
                }
                std::thread::sleep(READ_PACING);
            }
        });
        if let Some(e) = first_err.lock().unwrap().take() {
            panic!("writer: {e}");
        }
        let expected = written.load(Ordering::Relaxed);
        assert!(
            expected >= MIN_ROWS && reads >= MIN_READS,
            "insufficient local progress: {expected} rows, {reads} reads"
        );

        // Priority: the replication share of dual-contention writer
        // service must stay within the 25% gate once at least 1s of dual
        // service proves both classes actually contended.
        let (after_local, after_remote) = dual_service(&cluster);
        let local = after_local.saturating_sub(before_local);
        let remote = after_remote.saturating_sub(before_remote);
        let total = local + remote;
        eprintln!(
            "dual-contention writer service: local={:.3}s replication={:.3}s share={:.3}",
            local as f64 / 1e9,
            remote as f64 / 1e9,
            remote as f64 / total.max(1) as f64
        );
        assert!(
            total >= MIN_DUAL_SERVICE.as_nanos() as u64,
            "only {:.3}s of dual-contention service, want >= 1s to measure the 90/10 policy",
            total as f64 / 1e9
        );
        let share = remote as f64 / total as f64;
        assert!(
            share <= MAX_REMOTE_SHARE,
            "replication took {:.1}% of dual-contention writer service",
            share * 100.0
        );

        // Convergence: identical row counts and identical row sets.
        let converged = wait_until(sync_timeout, CONVERGE_POLL, || {
            let a = all_rows(cluster.node(0));
            let b = all_rows(cluster.node(1));
            a.len() == expected && b.len() == expected && a == b
        });
        let (a_count, b_count) = (
            all_rows(cluster.node(0)).len(),
            all_rows(cluster.node(1)).len(),
        );
        assert!(
            converged && a_count == expected && b_count == expected,
            "peers did not converge to {expected} rows (counts {a_count} vs {b_count})"
        );
        eprintln!("converged: {expected} rows, {reads} reads, identical row sets");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("write-priority-no-converge");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
