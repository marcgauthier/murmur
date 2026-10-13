//! Rust port of the Go `crash-recovery` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crash-recovery/`
//! (`crash_recovery_test.go`). Two nodes are abruptly restarted at different
//! times while background writers hammer all nodes; every acknowledged write
//! must survive on every node with equal digests, plus a post-recovery
//! write. Plain SQL replaces the Go typed-records HTTP API, and in-process
//! close/reopen replaces SIGKILL (see the scenario README.md for deviations).

use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use murmur_live_harness::sqlite_session::Database;
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Table DDL (Go: typed records; only names matter here).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT PRIMARY KEY)";
/// Write-window knob (Go: `MURMUR_CRASH_WRITE_SECONDS`, default 12).
const WRITE_ENV: &str = "MURMUR_CRASH_WRITE_SECONDS";
const DEFAULT_WRITE_SECS: u64 = 12;
/// Settle knob (Go: `MURMUR_CRASH_SETTLE_SECONDS`, fallbacks 60 then 20).
const SETTLE_ENV: &str = "MURMUR_CRASH_SETTLE_SECONDS";
/// Background-writer cadence (Go: 50 ms ticker, 2 workers per node).
const WRITE_TICK: Duration = Duration::from_millis(50);
const WORKERS_PER_NODE: usize = 2;
/// Refused-request backoff (Go: 5 ms sleep after a failed insert).
const REFUSED_BACKOFF: Duration = Duration::from_millis(5);
/// Initial peer wait (Go: 20 s `waitForPeers`).
const PEERS_TIMEOUT: Duration = Duration::from_secs(20);
/// Post-restart peer wait (Go: 90 s).
const REJOIN_TIMEOUT: Duration = Duration::from_secs(90);
/// Peer poll interval (Go: 50 ms).
const PEERS_POLL: Duration = Duration::from_millis(50);
/// Acknowledged-set poll interval (Go: 100 ms).
const ACK_POLL: Duration = Duration::from_millis(100);
/// In-flight gate deadline (Go: 5 s) and poll (Go: 1 ms).
const IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
const IN_FLIGHT_POLL: Duration = Duration::from_millis(1);

/// Stops background writers when dropped, so a panic in the driver body
/// never wedges the scope join on workers waiting for the stop flag.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Env-seconds knob (Go: `envSeconds`).
fn env_seconds(name: &str, fallback: u64) -> Duration {
    let secs = std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback);
    Duration::from_secs(secs)
}

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM live_typed_records ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in rows {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read names failed")
}

/// Best-effort insert; true when acknowledged (Go: writers keep only
/// `err == nil` values; kill/restart windows may refuse requests).
fn try_insert(db: &Database, name: String) -> bool {
    db.transaction(move |sql| {
        sql.execute("INSERT INTO live_typed_records(name) VALUES(?)", [name])?;
        Ok(())
    })
    .is_ok()
}

/// Live session count (Go: `connected_peers`; established
/// `churn-retirement` port pattern).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication enabled")
        .status()
        .peers
}

/// Wait until every node reports its wanted peer count (Go: `waitForPeers`).
fn wait_for_peers(cluster: &Cluster, want: &[usize], timeout: Duration) {
    let converged = wait_until(timeout, PEERS_POLL, || {
        (0..cluster.len()).all(|i| peer_count(cluster, i) == want[i])
    });
    if !converged {
        let mut states = Vec::new();
        for i in 0..cluster.len() {
            states.push(format!("node{}={}(want {})", i + 1, peer_count(cluster, i), want[i]));
        }
        panic!("peers did not reach {want:?} within {timeout:?}: {}", states.join(" "));
    }
}

/// True when every node holds every acknowledged value.
fn all_nodes_have_acknowledged(cluster: &Cluster, acknowledged: &HashSet<String>) -> bool {
    (0..cluster.len()).all(|i| {
        let present: HashSet<_> = names(cluster.node(i)).into_iter().collect();
        acknowledged.iter().all(|v| present.contains(v))
    })
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

/// Require equal counts plus equal ordered contents on all nodes
/// (Go: `assertConverged` / `waitForConvergence`; ordered-vec equality
/// replaces the SHA-256 digest with identical strength).
fn convergence_snapshot(cluster: &Cluster) -> Vec<Vec<String>> {
    (0..cluster.len()).map(|i| names(cluster.node(i))).collect()
}

fn assert_converged(cluster: &Cluster) {
    let snap = convergence_snapshot(cluster);
    for (i, got) in snap.iter().enumerate().skip(1) {
        assert_eq!(got, &snap[0], "node{} contents != node1 contents", i + 1);
    }
}

fn wait_for_convergence(cluster: &Cluster, timeout: Duration) {
    let converged = wait_until(timeout, ACK_POLL, || {
        let snap = convergence_snapshot(cluster);
        snap.iter().skip(1).all(|got| *got == snap[0])
    });
    if !converged {
        for (i, got) in convergence_snapshot(cluster).iter().enumerate() {
            eprintln!("node{} after recovery rows={}", i + 1, got.len());
        }
        panic!("three nodes did not converge after recovery within {timeout:?}");
    }
}

#[test]
fn abrupt_process_death_recovers_committed_rows() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "crash-recovery".into(),
        num_nodes: 3,
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
    // Current-generation writer handles, refreshed after every restart
    // (Go writers re-resolve the node HTTP endpoint after each restart;
    // a pre-restart clone only ever sees the old generation).
    let handles: Arc<Mutex<Vec<Database>>> = Arc::new(Mutex::new(
        (0..cluster.len()).map(|n| cluster.node(n).clone()).collect(),
    ));
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: typed fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(murmur_live_harness::sqlite_session::Migration::new(TABLE_DDL))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Baseline: one acknowledged write per node.
        let acknowledged = Arc::new(Mutex::new(HashSet::new()));
        for node in 0..cluster.len() {
            let value = format!("baseline-node{}", node + 1);
            assert!(
                try_insert(cluster.node(node), value.clone()),
                "node{} baseline write failed",
                node + 1
            );
            cluster.node(node).flush().expect("flush baseline");
            acknowledged.lock().expect("acked lock").insert(value);
        }
        wait_for_peers(&cluster, &[2, 2, 2], PEERS_TIMEOUT);
        {
            let guard = acknowledged.lock().expect("acked lock");
            let converged = wait_until(PEERS_TIMEOUT, ACK_POLL, || {
                all_nodes_have_acknowledged(&cluster, &guard)
            });
            assert!(converged, "baseline writes did not converge within {PEERS_TIMEOUT:?}");
        }

        // Background writers hammer all nodes until the end (2 workers
        // per node, 50 ms tick). In-flight counters gate each restart.
        let stop = Arc::new(AtomicBool::new(false));
        let _stop_guard = StopOnDrop(stop.clone());
        let active = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
        let seqs = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
        let write_burst = env_seconds(WRITE_ENV, DEFAULT_WRITE_SECS);
        std::thread::scope(|s| {
            for node in 0..cluster.len() {
                for worker in 0..WORKERS_PER_NODE {
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
                            let value =
                                format!("node{}-worker{worker}-write-{seq:08}", node + 1);
                            active.fetch_add(1, Ordering::Relaxed);
                            let db = handles.lock().expect("handles lock")[node].clone();
                            let ok = try_insert(&db, value.clone());
                            active.fetch_sub(1, Ordering::Relaxed);
                            if ok {
                                acknowledged.lock().expect("acked lock").insert(value);
                            } else {
                                // Kill/restart and short startup windows can
                                // refuse requests; the next unique insert
                                // continues the workload.
                                std::thread::sleep(REFUSED_BACKOFF);
                            }
                        }
                    });
                }
            }

            // First crash window: burst, kill node 2 mid-write, burst,
            // restart, rejoin under continuing load.
            std::thread::sleep(write_burst);
            kill_during_writes(&active, 1);
            cluster.stop_node(1);
            std::thread::sleep(write_burst);
            cluster.start_node(1).expect("restart node2");
            handles.lock().expect("handles lock")[1] = cluster.node(1).clone();
            cluster.mesh().expect("re-mesh after restart");
            wait_for_peers(&cluster, &[2, 2, 2], REJOIN_TIMEOUT);
            eprintln!("node2 rejoined under continuing application load");

            // Second crash window on node 3.
            std::thread::sleep(write_burst);
            kill_during_writes(&active, 2);
            cluster.stop_node(2);
            std::thread::sleep(write_burst);
            cluster.start_node(2).expect("restart node3");
            handles.lock().expect("handles lock")[2] = cluster.node(2).clone();
            cluster.mesh().expect("re-mesh after restart");
            wait_for_peers(&cluster, &[2, 2, 2], REJOIN_TIMEOUT);

            stop.store(true, Ordering::Relaxed);
        });

        wait_for_convergence(&cluster, env_seconds(SETTLE_ENV, 60));
        {
            let guard = acknowledged.lock().expect("acked lock");
            assert_acknowledged_present(&cluster, &guard);
            assert_converged(&cluster);
            eprintln!(
                "all acknowledged application writes survived two restarts ({} acknowledged rows)",
                guard.len()
            );
        }

        // A post-recovery write replicates everywhere.
        let post_crash_value = "post-recovery-write".to_owned();
        assert!(
            try_insert(cluster.node(0), post_crash_value.clone()),
            "post-recovery typed write failed"
        );
        cluster.node(0).flush().expect("flush post-recovery write");
        acknowledged.lock().expect("acked lock").insert(post_crash_value);
        wait_for_convergence(&cluster, env_seconds(SETTLE_ENV, 20));
        {
            let guard = acknowledged.lock().expect("acked lock");
            assert_acknowledged_present(&cluster, &guard);
            assert_converged(&cluster);
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("crash-recovery-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

/// Require a write in flight on `node` before the restart lands
/// (Go: `killDuringWrites` refuses a vacuous kill).
fn kill_during_writes(active: &[AtomicI64; 3], node: usize) {
    let gate = wait_until(IN_FLIGHT_TIMEOUT, IN_FLIGHT_POLL, || {
        active[node].load(Ordering::Relaxed) > 0
    });
    assert!(
        gate,
        "node{} had no typed write in flight before restart",
        node + 1
    );
    eprintln!(
        "restarting node{} with {} typed writes in flight",
        node + 1,
        active[node].load(Ordering::Relaxed)
    );
}
