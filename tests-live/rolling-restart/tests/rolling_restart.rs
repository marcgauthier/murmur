//! Rust port of the Go `rolling-restart` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/rolling-restart/`
//! (`rolling_restart_test.go` + `churn_test.go`). `rolling_restart_loses_no_writes`
//! restarts each node in turn while every node keeps accepting writes: any
//! failed write outside a restart window aborts, and the mesh must converge
//! with identical digests at the end. `rolling_restart_churn` stresses
//! session re-establishment with six back-to-back restarts and no quiesce
//! between. Plain SQL replaces the Go typed-contention HTTP API, and
//! in-process close/reopen replaces daemon stop/start (see the scenario
//! README.md for deviations).

use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Contention table DDL (Go: typed contention rows; only id+name matter here).
const TABLE_DDL: &str = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)";
/// Baseline rows (Go: 5).
const BASELINE_ROWS: usize = 5;
/// Baseline wait (Go: 60 s `waitCounts`).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(60);
/// Main-test writer cadence (Go: 25 ms ticker, 1 writer per node).
const WRITE_TICK: Duration = Duration::from_millis(25);
/// Churn-test writer cadence (Go: 10 ms single writer over all nodes).
const CHURN_TICK: Duration = Duration::from_millis(10);
/// In-flight drain before stopping (Go: 300 ms; churn: 200 ms).
const DRAIN: Duration = Duration::from_millis(300);
const CHURN_DRAIN: Duration = Duration::from_millis(200);
/// Downtime between stop and start (Go: 1 s sleep).
const DOWNTIME: Duration = Duration::from_secs(1);
/// Catch-up sleep after each rolling restart (Go: 3 s).
const CATCH_UP: Duration = Duration::from_secs(3);
/// Quiesce / final waits (Go: 150 s).
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(150);
/// Quiesce poll interval (Go: 200 ms).
const QUIESCE_POLL: Duration = Duration::from_millis(200);
/// Count poll interval (Go: 100 ms).
const COUNT_POLL: Duration = Duration::from_millis(100);
/// Convergence settle re-sample delay (Go: 500 ms).
const SETTLE: Duration = Duration::from_millis(500);

/// Stops background writers when dropped, so a panic in the driver body
/// never wedges the scope join on workers waiting for the stop flag.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Same schema on every node (Go: typed contention fixture tables).
fn migrate_all(cluster: &Cluster) {
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .migrate(Migration::new(TABLE_DDL))
            .expect("migrate contention table");
        cluster.node(i).flush().expect("flush schema");
    }
}

/// Ordered contention rows on one node (Go: `rrRows`).
fn rows(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name FROM contention ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read contention rows failed")
}

/// Best-effort insert; true when acknowledged.
fn try_insert(db: &Database, id: String, name: &str) -> bool {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute("INSERT INTO contention(id, name) VALUES(?, ?)", (id, name))?;
        Ok(())
    })
    .is_ok()
}

/// Insert one row, panicking on error (baseline writes).
fn insert_row(db: &Database, node: usize, id: String, name: &str) {
    assert!(
        try_insert(db, id, name),
        "node{node} baseline write failed"
    );
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after baseline failed: {e}"));
}

/// Wait until every node reports the exact row count (Go: `waitCounts`).
fn wait_counts(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, COUNT_POLL, || {
        (0..cluster.len()).all(|i| rows(cluster.node(i)).len() == want)
    });
    assert!(
        converged,
        "row counts did not reach {want} within {timeout:?}"
    );
}

/// Pause all writers, wait for exact digest agreement, then resume
/// (Go: `quiesce`). Callers must hold no pause themselves.
fn quiesce(cluster: &Cluster, paused: &[AtomicBool; 3], timeout: Duration) {
    for p in paused {
        p.store(true, Ordering::Relaxed);
    }
    // Drain in-flight writes before asserting a frozen state.
    std::thread::sleep(DRAIN);
    let converged = wait_until(timeout, QUIESCE_POLL, || {
        let want = rows(cluster.node(0));
        (1..cluster.len()).all(|i| rows(cluster.node(i)) == want)
    });
    if !converged {
        dump_id_diff(cluster);
        panic!("digests did not agree within {timeout:?}");
    }
    for p in paused {
        p.store(false, Ordering::Relaxed);
    }
}

/// Wait until all nodes agree on the same row count, with a settle
/// re-sample before declaring convergence (Go: `waitConvergedCounts`).
fn wait_converged_counts(cluster: &Cluster, timeout: Duration) {
    let converged = wait_until(timeout, COUNT_POLL, || {
        let first = rows(cluster.node(0)).len();
        if !(1..cluster.len()).all(|i| rows(cluster.node(i)).len() == first) {
            return false;
        }
        // Settle: one more matching sample before declaring convergence.
        std::thread::sleep(SETTLE);
        (0..cluster.len()).all(|i| rows(cluster.node(i)).len() == first)
    });
    assert!(
        converged,
        "row counts did not converge within {timeout:?}"
    );
}

/// Log the symmetric row-id difference plus the first ordered row
/// difference, then dump forensics (Go: `dumpIDDiff`).
fn dump_id_diff(cluster: &Cluster) {
    let ids = |idx: usize| -> HashSet<String> {
        rows(cluster.node(idx)).into_iter().map(|(id, _)| id).collect()
    };
    let (a, b) = (ids(0), ids(1));
    let only_a: Vec<_> = a.iter().filter(|id| !b.contains(*id)).collect();
    let only_b: Vec<_> = b.iter().filter(|id| !a.contains(*id)).collect();
    eprintln!(
        "divergence: only-node1={} {:?} only-node2={} {:?}",
        only_a.len(),
        only_a.iter().take(5).collect::<Vec<_>>(),
        only_b.len(),
        only_b.iter().take(5).collect::<Vec<_>>()
    );
    let (r0, r1) = (rows(cluster.node(0)), rows(cluster.node(1)));
    for (i, (a, b)) in r0.iter().zip(r1.iter()).enumerate() {
        if a != b {
            eprintln!("divergence: first diff at row {i}:\n  node1={a:?}\n  node2={b:?}");
            break;
        }
    }
    cluster.dump_forensics("divergence");
}

/// Require identical ordered digests on all nodes (Go: `rrDigest` equality).
fn assert_digests_equal(cluster: &Cluster, what: &str) {
    let want = rows(cluster.node(0));
    for idx in 1..cluster.len() {
        let got = rows(cluster.node(idx));
        assert_eq!(
            got, want,
            "node{} digest != node1 digest after {what}",
            idx + 1
        );
    }
}

/// Log node placement (Go: per-node pid/dir/repl line; no pids in-process).
fn log_placement(cluster: &Cluster) {
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "{} dir={} repl={}",
            info.label,
            info.dir.display(),
            info.repl_addr
        );
    }
}

#[test]
fn rolling_restart_loses_no_writes() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "rolling-restart".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .expect("cluster setup");
    log_placement(&cluster);
    // Current-generation writer handles, refreshed after every restart.
    let handles: Arc<Mutex<Vec<Database>>> = Arc::new(Mutex::new(
        (0..cluster.len()).map(|n| cluster.node(n).clone()).collect(),
    ));
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster);
        for i in 0..BASELINE_ROWS {
            insert_row(cluster.node(0), 1, format!("{i:032x}"), &format!("base-{i}"));
        }
        wait_counts(&cluster, BASELINE_ROWS, BASELINE_TIMEOUT);

        // Writers hammer every node continuously. A write to a live node
        // must never fail (zero-downtime restart); the writer for the node
        // being restarted pauses while it is down.
        let failed_writes = Arc::new(AtomicI64::new(0));
        let paused = [
            AtomicBool::new(false),
            AtomicBool::new(false),
            AtomicBool::new(false),
        ];
        let stop = Arc::new(AtomicBool::new(false));
        let _stop_guard = StopOnDrop(stop.clone());
        std::thread::scope(|s| {
            for node in 0..3 {
                let handles = handles.clone();
                let stop = stop.clone();
                let failed_writes = failed_writes.clone();
                let paused_node = &paused[node];
                s.spawn(move || {
                    let mut seq: usize = 0;
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(WRITE_TICK);
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        if paused_node.load(Ordering::Relaxed) {
                            continue;
                        }
                        let id = format!("ff{node:02x}{seq:028x}");
                        seq += 1;
                        let db = handles.lock().expect("handles lock")[node].clone();
                        if !try_insert(&db, id, "flow") {
                            failed_writes.fetch_add(1, Ordering::Relaxed);
                            eprintln!("node{} write failed outside its restart", node + 1);
                        }
                    }
                });
            }

            for i in 0..3 {
                paused[i].store(true, Ordering::Relaxed);
                // Drain in-flight writes before stopping so none fail spuriously.
                std::thread::sleep(DRAIN);
                cluster.stop_node(i);
                std::thread::sleep(DOWNTIME);
                cluster.start_node(i).expect("restart node");
                handles.lock().expect("handles lock")[i] = cluster.node(i).clone();
                cluster.mesh().expect("re-mesh after restart");
                paused[i].store(false, Ordering::Relaxed);
                // Let the rejoined node catch up under load, then quiesce
                // all writers: exact-count agreement is unobservable while
                // inserts keep landing.
                std::thread::sleep(CATCH_UP);
                quiesce(&cluster, &paused, QUIESCE_TIMEOUT);
            }
            stop.store(true, Ordering::Relaxed);
        });

        let failed = failed_writes.load(Ordering::Relaxed);
        assert_eq!(
            failed, 0,
            "{failed} writes failed outside restart windows, want zero-downtime"
        );
        wait_converged_counts(&cluster, QUIESCE_TIMEOUT);
        assert_digests_equal(&cluster, "rolling restart");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("rolling-restart-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn rolling_restart_churn() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "rolling-churn".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .expect("cluster setup");
    log_placement(&cluster);
    let handles: Arc<Mutex<Vec<Database>>> = Arc::new(Mutex::new(
        (0..cluster.len()).map(|n| cluster.node(n).clone()).collect(),
    ));
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster);
        for i in 0..BASELINE_ROWS {
            insert_row(cluster.node(0), 1, format!("{i:032x}"), &format!("base-{i}"));
        }
        wait_counts(&cluster, BASELINE_ROWS, BASELINE_TIMEOUT);

        // One writer ticks over every unpaused node; errors are ignored
        // (churn is about session re-establishment, not write success).
        let paused = [
            AtomicBool::new(false),
            AtomicBool::new(false),
            AtomicBool::new(false),
        ];
        let stop = Arc::new(AtomicBool::new(false));
        let _stop_guard = StopOnDrop(stop.clone());
        std::thread::scope(|s| {
            {
                let handles = handles.clone();
                let stop = stop.clone();
                let paused = &paused;
                s.spawn(move || {
                    let mut seq: usize = 0;
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(CHURN_TICK);
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        for n in 0..3 {
                            if paused[n].load(Ordering::Relaxed) {
                                continue;
                            }
                            let id = format!("cc{n:02x}{seq:028x}");
                            let db = handles.lock().expect("handles lock")[n].clone();
                            let _ = try_insert(&db, id, "flow");
                        }
                        seq += 1;
                    }
                });
            }

            // Six back-to-back restarts: node order 1,2,3,1,2,3 with only
            // the readiness wait between them (no catch-up sleep, no
            // quiesce). Regression test for the heal stall (restart N+1
            // while restart N still healing wedged one link forever).
            for i in 0..6 {
                let node = i % 3;
                paused[node].store(true, Ordering::Relaxed);
                std::thread::sleep(CHURN_DRAIN);
                cluster.stop_node(node);
                cluster.start_node(node).expect("restart node");
                handles.lock().expect("handles lock")[node] = cluster.node(node).clone();
                cluster.mesh().expect("re-mesh after restart");
                paused[node].store(false, Ordering::Relaxed);
                eprintln!("churn: restart {} (node{}) complete", i + 1, node + 1);
            }

            quiesce(&cluster, &paused, QUIESCE_TIMEOUT);
            // quiesce resumes writers on success; stop them before the
            // final digest proof (digests under active writers never agree).
            stop.store(true, Ordering::Relaxed);
        });

        wait_converged_counts(&cluster, QUIESCE_TIMEOUT);
        let want = rows(cluster.node(0));
        for idx in 1..3 {
            let got = rows(cluster.node(idx));
            if got != want {
                dump_id_diff(&cluster);
                panic!("node{} digest != node1 digest after churn", idx + 1);
            }
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("rolling-churn-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
