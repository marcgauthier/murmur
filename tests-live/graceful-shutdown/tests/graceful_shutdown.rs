//! Rust port of the Go `graceful-shutdown` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/graceful-shutdown/`
//! (`graceful_shutdown_test.go`). Phase A stops node 1 mid-write under load;
//! phase B restarts node 3 stale (log ranges GC'd, forcing snapshot-resync)
//! and stops it again mid-transfer. Both restarts must be clean and fast,
//! lose no acknowledged rows, and the mesh must reconverge with identical
//! contents; the snapshot path is proven, not assumed. Plain SQL replaces
//! the Go typed-contention HTTP API, and in-process close/reopen replaces
//! SIGTERM (see the scenario README.md for deviations).

use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Contention table DDL (Go: typed contention rows; `phone` carries the
/// fat snapshot-widening payload, otherwise empty).
const TABLE_DDL: &str =
    "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL DEFAULT '')";
/// Member-deadline knob (Go: `MURMUR_GRACEFUL_MEMBER_DEADLINE_SECONDS`, 25).
const DEADLINE_ENV: &str = "MURMUR_GRACEFUL_MEMBER_DEADLINE_SECONDS";
const DEFAULT_DEADLINE_SECS: u64 = 25;
/// Baseline rows (Go: 5).
const BASELINE_ROWS: usize = 5;
/// Phase-A load pre-roll (Go: 4 s).
const PRE_ROLL: Duration = Duration::from_secs(4);
/// Load-writer cadence (Go: 25 ms ticker, 1 writer per node).
const WRITE_TICK: Duration = Duration::from_millis(25);
/// In-flight gate deadline (Go: 5 s) and poll (Go: 1 ms).
const IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
const IN_FLIGHT_POLL: Duration = Duration::from_millis(1);
/// Fast-restart bound (Go: < 60 s).
const RESTART_BOUND: Duration = Duration::from_secs(60);
/// Survivor bulk rows while node 3 is stale (Go: 100).
const BULK_ROWS: usize = 100;
/// Fat snapshot-widening rows (Go: 8 x ~256 KB).
const FAT_ROWS: usize = 8;
const FAT_FILLER: &str = "SPeD-SQL-snapshot-filler-";
const FAT_BYTES: usize = 256 * 1024;
/// GC-pass wait bound (Go: 120 s) and poll (Go: 500 ms).
const GC_TIMEOUT: Duration = Duration::from_secs(120);
const GC_POLL: Duration = Duration::from_millis(500);
/// Snapshot catch window (Go: 60 s) and poll (Go: 5 ms).
const CATCH_TIMEOUT: Duration = Duration::from_secs(60);
const CATCH_POLL: Duration = Duration::from_millis(5);
/// Baseline convergence deadline (Go: 30 s).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(30);
/// Phase-A convergence deadline (Go: 90 s).
const PHASE_A_TIMEOUT: Duration = Duration::from_secs(90);
/// Row-count deadline (Go: 30 s).
const COUNT_TIMEOUT: Duration = Duration::from_secs(30);
/// Final convergence deadline (Go: 120 s).
const FINAL_TIMEOUT: Duration = Duration::from_secs(120);
/// Post-recovery convergence deadline (Go: 60 s).
const POST_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence/row-count poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);

/// Stops background writers when dropped, so a panic in the driver body
/// never wedges the scope join on workers waiting for the stop flag.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Row id (Go: `gsRowID`).
fn row_id(n: u64) -> String {
    format!("{:08x}-0000-4000-8000-{:012x}", n >> 32, n & 0xffff_ffff_ffff)
}

/// Ordered names on one node (Go: sorted-name digest input).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM contention ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in rows {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read names failed")
}

/// Row count on one node.
fn row_count(db: &Database) -> usize {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT COUNT(*) FROM contention")?;
        let mut rows = stmt.query([])?;
        match rows.next()? {
            Some(row) => Ok(row.get::<_, i64>(0)? as usize),
            None => Ok(0),
        }
    })
    .expect("count rows failed")
}

/// Best-effort insert; true when acknowledged.
fn try_insert(db: &Database, id: String, name: String, phone: String) -> bool {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO contention(id, name, phone) VALUES(?, ?, ?)",
            (id, name, phone),
        )?;
        Ok(())
    })
    .is_ok()
}

/// Insert one row, panicking on error.
fn insert_row(db: &Database, node: usize, id: String, name: &str, phone: &str) {
    assert!(
        try_insert(db, id, name.to_owned(), phone.to_owned()),
        "node{node} insert failed"
    );
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
}

/// Poll exact counts plus equal ordered contents (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        let mut first: Option<Vec<String>> = None;
        for i in 0..cluster.len() {
            let got = names(cluster.node(i));
            if got.len() != want {
                return false;
            }
            match &first {
                None => first = Some(got),
                Some(prev) if *prev == got => {}
                _ => return false,
            }
        }
        true
    });
    assert!(
        converged,
        "nodes did not converge on {want} rows with equal contents within {timeout:?}"
    );
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
fn wait_row_count(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        row_count(cluster.node(idx)) == want
    });
    assert!(
        converged,
        "node{} row count = {}, want {want} within {timeout:?}",
        idx + 1,
        row_count(cluster.node(idx))
    );
}

/// Require every acknowledged value present on every node
/// (Go: `assertAcknowledgedPresent`).
fn assert_acknowledged_present(cluster: &Cluster, acknowledged: &HashSet<String>) {
    for i in 0..cluster.len() {
        let present: HashSet<_> = names(cluster.node(i)).into_iter().collect();
        for value in acknowledged {
            assert!(
                present.contains(value),
                "node{} lost acknowledged write {value:?} after restart",
                i + 1
            );
        }
    }
}

/// Clean-stop proof (Go: `assertCleanShutdown` log markers, which have no
/// Rust equivalent): the close path completed and released the handle.
/// `stop_node` already logs close errors; the no-loss assertions carry the
/// real proof.
fn assert_clean_stop(cluster: &Cluster, idx: usize, what: &str) {
    assert!(
        cluster.node_info(idx).db.is_none(),
        "{what}: node handle still open after stop"
    );
}

/// Log-path activity counters `(received, applied)`: snapshot application
/// bypasses both, so a flat pair proves the snapshot path (established
/// `snapshot-resync` port pattern; Go reads
/// `spedsql_repl_snapshots_received_total`, which has no Rust equivalent).
fn log_counters(cluster: &Cluster, idx: usize) -> (u64, u64) {
    let metrics = cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics;
    (metrics.received_transactions, metrics.applied_transactions)
}

/// Durable collected counter on one node (Go:
/// `spedsql_gc_log_collected_total`).
fn collected_total(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .retention_status()
        .expect("retention status")
        .collected_transactions
}

/// GC run counter on one node (Go: `spedsql_gc_runs_total`).
fn gc_runs(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics
        .retention_runs
}

/// Run on-demand retention passes until one completes past the baseline
/// (Go: `waitGCPass` on the background ticker; Rust exposes the pass
/// directly, so each poll triggers one instead of waiting for the tick).
fn wait_gc_pass(cluster: &Cluster, idx: usize, baseline: u64, timeout: Duration) {
    let converged = wait_until(timeout, GC_POLL, || {
        cluster
            .node(idx)
            .collect_retention()
            .expect("retention collection pass");
        gc_runs(cluster, idx) > baseline
    });
    assert!(
        converged,
        "no GC pass completed within {timeout:?} (baseline {baseline})"
    );
}

#[test]
fn graceful_shutdown_mid_write_and_mid_snapshot() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "graceful-shutdown".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            maximum_offline: Some(Duration::from_secs(20)),
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
    // Current-generation writer handles, refreshed after every restart.
    let handles: Arc<Mutex<Vec<Database>>> = Arc::new(Mutex::new(
        (0..cluster.len()).map(|n| cluster.node(n).clone()).collect(),
    ));
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: typed contention fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Baseline: 5 rows converge before any shutdown.
        for i in 0..BASELINE_ROWS {
            insert_row(cluster.node(0), 1, row_id(i as u64), &format!("base-{i}"), "");
        }
        wait_converged(&cluster, BASELINE_ROWS, BASELINE_TIMEOUT);

        // Phase A: all three nodes under write load; stop node 1 mid-write.
        let acknowledged = Arc::new(Mutex::new(HashSet::new()));
        for i in 0..BASELINE_ROWS {
            acknowledged.lock().expect("acked lock").insert(format!("base-{i}"));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let _stop_guard = StopOnDrop(stop.clone());
        let active = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
        let seqs = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
        std::thread::scope(|s| {
            for node in 0..3 {
                let handles = handles.clone();
                let stop = stop.clone();
                let acknowledged = acknowledged.clone();
                let active = &active[node];
                let seq = &seqs[node];
                s.spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(WRITE_TICK);
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let seq = seq.fetch_add(1, Ordering::Relaxed) + 1;
                        let value = format!("load-n{}-{seq:08}", node + 1);
                        let id = row_id((node as u64 + 1) * 1_000_000_000 + seq as u64);
                        active.fetch_add(1, Ordering::Relaxed);
                        let db = handles.lock().expect("handles lock")[node].clone();
                        let ok = try_insert(&db, id, value.clone(), String::new());
                        active.fetch_sub(1, Ordering::Relaxed);
                        if ok {
                            acknowledged.lock().expect("acked lock").insert(value);
                        }
                    }
                });
            }
            std::thread::sleep(PRE_ROLL);

            // Prove a write was actually in flight when the stop landed,
            // so a vacuous pass (idle node, clean exit, nothing at stake)
            // is detectable.
            let gate = wait_until(IN_FLIGHT_TIMEOUT, IN_FLIGHT_POLL, || {
                active[0].load(Ordering::Relaxed) > 0
            });
            assert!(gate, "node1 had no write in flight before stop");
            eprintln!(
                "stopping node1 with {} writes in flight",
                active[0].load(Ordering::Relaxed)
            );
            let stop_start = Instant::now();
            cluster.stop_node(0);
            eprintln!("node1 graceful stop took {:?}", stop_start.elapsed());
            assert_clean_stop(&cluster, 0, "node1 mid-write stop");

            let restart_start = Instant::now();
            cluster.start_node(0).expect("restart node1");
            handles.lock().expect("handles lock")[0] = cluster.node(0).clone();
            cluster.mesh().expect("re-mesh after restart");
            let restart_elapsed = restart_start.elapsed();
            eprintln!("node1 restart took {restart_elapsed:?}");
            assert!(
                restart_elapsed <= RESTART_BOUND,
                "node1 restart took {restart_elapsed:?}, want fast restart under {RESTART_BOUND:?}"
            );

            // Positive control: honest load converges across the restart.
            stop.store(true, Ordering::Relaxed);
        });
        let total = acknowledged.lock().expect("acked lock").len();
        wait_converged(&cluster, total, PHASE_A_TIMEOUT);
        assert_acknowledged_present(&cluster, &acknowledged.lock().expect("acked lock"));

        // Phase B: node 3 goes stale under aggressive retention while the
        // survivors write a fat snapshot payload, then a stop lands
        // mid-transfer on its restart.
        let stale_count = row_count(cluster.node(2));
        cluster.stop_node(2);
        for i in 0..BULK_ROWS {
            let name = format!("fresh-{i}");
            insert_row(cluster.node(0), 1, row_id(5000 + i as u64), &name, "");
            acknowledged.lock().expect("acked lock").insert(name);
        }
        // Fat values widen the transfer window so the stop can land
        // inside it deterministically.
        let big = FAT_FILLER.repeat(FAT_BYTES / FAT_FILLER.len());
        for i in 0..FAT_ROWS {
            let name = format!("fat-{i}");
            insert_row(cluster.node(1), 2, row_id(6000 + i as u64), &name, &big);
            acknowledged.lock().expect("acked lock").insert(name);
        }
        let want = acknowledged.lock().expect("acked lock").len();
        wait_row_count(&cluster, 0, want, COUNT_TIMEOUT);
        wait_row_count(&cluster, 1, want, COUNT_TIMEOUT);

        // Member-deadline expiry plus a GC pass (not a fixed sleep for the
        // pass: under contention the tick slips, so a fixed wait can elapse
        // without any pass collecting node 3's ranges, and the rejoin
        // silently takes the log path). First sleep out node 3's 20 s
        // member deadline so its ranges stop gating GC, then run passes
        // until one completes after expiry and collects log batches.
        let gc_runs_before = gc_runs(&cluster, 0);
        let collected_before = collected_total(&cluster, 0);
        let deadline_wait = Duration::from_secs(
            std::env::var(DEADLINE_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_DEADLINE_SECS),
        );
        eprintln!("waiting {deadline_wait:?} for member-deadline expiry");
        std::thread::sleep(deadline_wait);
        wait_gc_pass(&cluster, 0, gc_runs_before, GC_TIMEOUT);
        let collected_after = collected_total(&cluster, 0);
        assert!(
            collected_after > collected_before,
            "a post-expiry GC pass ran but collected no log batches ({collected_before} -> {collected_after}); node3's ranges are still pinned and the snapshot path is not forced"
        );
        eprintln!("GC collected log batches after expiry ({collected_before} -> {collected_after}); snapshot path forced");

        cluster.start_node(2).expect("restart stale node3");
        handles.lock().expect("handles lock")[2] = cluster.node(2).clone();
        cluster.mesh().expect("re-mesh stale node3");

        // Catch the transfer in flight: a row count strictly between the
        // stale count and the target while no transfer has completed yet.
        // Poll fast; the fat payload keeps the window open.
        let mut detail = String::from("no transfer activity observed");
        let caught = wait_until(CATCH_TIMEOUT, CATCH_POLL, || {
            let count = row_count(cluster.node(2));
            detail = format!("stale={stale_count} count={count} want={want}");
            if count >= want {
                return true;
            }
            stale_count < count && count < want
        });
        eprintln!("stopping node3 during rejoin (caught={caught} {detail})");
        assert!(
            caught && row_count(cluster.node(2)) < want,
            "transfer completed before the stop could land mid-apply ({detail}); widen the payload"
        );
        cluster.stop_node(2);
        assert_clean_stop(&cluster, 2, "node3 mid-transfer stop");

        let restart_start = Instant::now();
        cluster.start_node(2).expect("restart node3 after interrupt");
        handles.lock().expect("handles lock")[2] = cluster.node(2).clone();
        cluster.mesh().expect("re-mesh after interrupt");
        let restart_elapsed = restart_start.elapsed();
        eprintln!("node3 post-interrupt restart took {restart_elapsed:?}");
        assert!(
            restart_elapsed <= RESTART_BOUND,
            "node3 restart took {restart_elapsed:?}, want fast restart under {RESTART_BOUND:?}"
        );

        wait_converged(&cluster, want, FINAL_TIMEOUT);
        // The snapshot path is proven via the receiver's counters (flat
        // log pair on the fresh post-restart handle), not merely by
        // convergence: log catch-up would hide a snapshot-path regression.
        let (received, applied) = log_counters(&cluster, 2);
        assert!(
            (received, applied) == (0, 0),
            "node3 rejoin arrived over the log path (received={received} applied={applied}), want snapshot"
        );
        assert_acknowledged_present(&cluster, &acknowledged.lock().expect("acked lock"));

        // Post-recovery honest write still replicates everywhere.
        insert_row(cluster.node(0), 1, row_id(999_999), "post-recovery", "");
        acknowledged.lock().expect("acked lock").insert("post-recovery".to_owned());
        let want = acknowledged.lock().expect("acked lock").len();
        wait_converged(&cluster, want, POST_TIMEOUT);
        assert_acknowledged_present(&cluster, &acknowledged.lock().expect("acked lock"));
        eprintln!("graceful shutdown proven: 2 restarts, {want} rows intact, contents converge");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("graceful-shutdown-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
