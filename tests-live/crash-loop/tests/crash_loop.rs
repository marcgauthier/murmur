//! Rust port of the Go `crash-loop` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crash-loop/`
//! (`crash_loop_test.go`). One of three meshed nodes is restarted N times
//! under continuous writes from every node, with short uptimes between
//! restarts. Every write acknowledged before each restart must survive; the
//! looped node must converge to identical contents; it must reach ready
//! within a bound after the final restart (no wedge); and a post-loop write
//! on it must replicate everywhere. Plain SQL replaces the Go typed-records
//! HTTP API, and in-process close/reopen replaces SIGKILL (see the scenario
//! README.md for deviations).

use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::Database;
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Table DDL (Go: typed records; only names matter here).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT PRIMARY KEY)";
/// Looped node (Go: `victim = 2`, node3).
const VICTIM: usize = 2;
/// Round-count knob (Go: `MURMUR_CRASH_LOOP_ROUNDS`, default 5).
const ROUNDS_ENV: &str = "MURMUR_CRASH_LOOP_ROUNDS";
const DEFAULT_ROUNDS: usize = 5;
/// Uptime knob (Go: `MURMUR_CRASH_LOOP_UPTIME_MS`, default 1000).
const UPTIME_ENV: &str = "MURMUR_CRASH_LOOP_UPTIME_MS";
const DEFAULT_UPTIME_MS: u64 = 1000;
/// Background-writer cadence (Go: 25 ms ticker, 1 writer per node).
const WRITE_TICK: Duration = Duration::from_millis(25);
/// Final-round burst rows on a survivor (Go: 30).
const BURST_ROWS: usize = 30;
/// Final-round kill window (Go: 500 ms bound so the burst is still
/// unflushed — mid-replication by construction).
const FINAL_KILL_WINDOW: Duration = Duration::from_millis(500);
/// In-flight gate for non-final rounds (Go: 10 s `waitInflight`).
const IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(10);
const IN_FLIGHT_POLL: Duration = Duration::from_millis(1);
/// No-wedge bound for the final restart (Go: ready < 60 s).
const READY_BOUND: Duration = Duration::from_secs(60);
/// Positive-control waits (Go: 30 s peers + convergence).
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
/// Post-loop waits (Go: 90 s peers + convergence).
const LOOP_TIMEOUT: Duration = Duration::from_secs(90);
/// Post-loop write convergence (Go: 60 s).
const POST_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 100 ms).
const POLL: Duration = Duration::from_millis(100);

/// Stops background writers when dropped, so a panic in the driver body
/// never wedges the scope join on workers waiting for the stop flag.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Env-int knob (Go: `envInt`).
fn env_int(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
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
/// `err == nil` names; the victim's downtime refuses requests).
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
    let converged = wait_until(timeout, POLL, || {
        (0..cluster.len()).all(|i| peer_count(cluster, i) == want[i])
    });
    assert!(converged, "peers did not reach {want:?} within {timeout:?}");
}

/// Poll exact counts plus ordered-digest equality (Go: `waitForConvergence`;
/// ordered-vec equality replaces the joined-name digest).
fn wait_for_convergence(cluster: &Cluster, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        let first = names(cluster.node(0));
        (1..cluster.len()).all(|i| names(cluster.node(i)) == first)
    });
    if !converged {
        for i in 0..cluster.len() {
            eprintln!("node{} at timeout: count={}", i + 1, names(cluster.node(i)).len());
        }
        panic!("nodes did not converge (counts + name digests) within {timeout:?}");
    }
}

/// Require every acknowledged value present on every node
/// (Go: `assertAcknowledgedPresent`).
fn assert_acknowledged_present(cluster: &Cluster, acknowledged: &HashSet<String>) {
    for i in 0..cluster.len() {
        let present: HashSet<_> = names(cluster.node(i)).into_iter().collect();
        for name in acknowledged {
            assert!(
                present.contains(name),
                "node{} lost acknowledged write {name:?}",
                i + 1
            );
        }
    }
}

#[test]
fn crash_loop_node_converges_without_loss() {
    let rounds = env_int(ROUNDS_ENV, DEFAULT_ROUNDS);
    let uptime = Duration::from_millis(env_int(UPTIME_ENV, DEFAULT_UPTIME_MS as usize) as u64);

    let mut cluster = Cluster::new(ClusterOptions {
        name: "crash-loop".into(),
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
    // (Go writers re-resolve the node endpoint after each restart).
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

        // Positive control: honest 3-node convergence before any crash.
        for node in 0..3 {
            let name = format!("base-node{}", node + 1);
            assert!(
                try_insert(cluster.node(node), name),
                "baseline write node{} failed",
                node + 1
            );
            cluster.node(node).flush().expect("flush baseline");
        }
        wait_for_peers(&cluster, &[2, 2, 2], CONTROL_TIMEOUT);
        wait_for_convergence(&cluster, CONTROL_TIMEOUT);
        eprintln!("positive control: 3-node mesh converged");

        // Background writers on all nodes until the loop ends.
        let acknowledged = Arc::new(Mutex::new(HashSet::from([
            "base-node1".to_owned(),
            "base-node2".to_owned(),
            "base-node3".to_owned(),
        ])));
        let stop = Arc::new(AtomicBool::new(false));
        let _stop_guard = StopOnDrop(stop.clone());
        let in_flight = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
        let seqs = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
        let mut ready_durations = Vec::new();
        std::thread::scope(|s| {
            for node in 0..cluster.len() {
                let handles = handles.clone();
                let stop = stop.clone();
                let acknowledged = acknowledged.clone();
                let in_flight = &in_flight[node];
                let seq = &seqs[node];
                s.spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(WRITE_TICK);
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let seq = seq.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = format!("n{}-{seq:08}", node + 1);
                        in_flight.fetch_add(1, Ordering::Relaxed);
                        let db = handles.lock().expect("handles lock")[node].clone();
                        let ok = try_insert(&db, name.clone());
                        in_flight.fetch_sub(1, Ordering::Relaxed);
                        if ok {
                            acknowledged.lock().expect("acked lock").insert(name);
                        }
                    }
                });
            }

            for round in 1..=rounds {
                std::thread::sleep(uptime);
                if round == rounds {
                    // Burst on a survivor, then restart at once: prefer a
                    // moment with a request in flight (mid-write), but
                    // bound the wait to 500 ms so the burst is still
                    // unflushed (mid-replication by construction).
                    for i in 0..BURST_ROWS {
                        let name = format!("burst-{i}");
                        assert!(
                            try_insert(cluster.node(0), name.clone()),
                            "round {round} burst failed"
                        );
                        acknowledged.lock().expect("acked lock").insert(name);
                    }
                    cluster.node(0).flush().expect("flush burst");
                    let end = Instant::now() + FINAL_KILL_WINDOW;
                    while in_flight[VICTIM].load(Ordering::Relaxed) == 0
                        && Instant::now() < end
                    {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                } else {
                    // Every non-final restart requires a request in
                    // flight: mid-write proven.
                    let gate = wait_until(IN_FLIGHT_TIMEOUT, IN_FLIGHT_POLL, || {
                        in_flight[VICTIM].load(Ordering::Relaxed) > 0
                    });
                    assert!(
                        gate,
                        "node{} had no typed write request in flight before restart",
                        VICTIM + 1
                    );
                }
                eprintln!(
                    "round {round}/{rounds}: restarting node3 with {} request(s) in flight",
                    in_flight[VICTIM].load(Ordering::Relaxed)
                );
                cluster.stop_node(VICTIM);
                let start = Instant::now();
                cluster.start_node(VICTIM).expect("restart victim");
                handles.lock().expect("handles lock")[VICTIM] =
                    cluster.node(VICTIM).clone();
                cluster.mesh().expect("re-mesh after restart");
                let ready = start.elapsed();
                ready_durations.push(ready);
                eprintln!("round {round}/{rounds}: node3 ready in {ready:?}");
            }
            stop.store(true, Ordering::Relaxed);
        });

        // No wedge: the final restart reaches ready within the bound, the
        // mesh reforms, and every acked write is present everywhere.
        let last = *ready_durations.last().expect("at least one round");
        assert!(
            last <= READY_BOUND,
            "final restart took {last:?} to reach ready, want < {READY_BOUND:?} (wedge)"
        );
        wait_for_peers(&cluster, &[2, 2, 2], LOOP_TIMEOUT);
        eprintln!("mesh reformed after the crash loop");
        let total = acknowledged.lock().expect("acked lock").len();
        wait_for_convergence(&cluster, LOOP_TIMEOUT);
        assert_acknowledged_present(&cluster, &acknowledged.lock().expect("acked lock"));
        eprintln!("all {total} acknowledged writes survived {rounds} restarts");

        // A post-loop write on the looped node replicates everywhere.
        let post_loop = "post-loop-from-victim".to_owned();
        assert!(
            try_insert(cluster.node(VICTIM), post_loop.clone()),
            "post-loop write failed"
        );
        cluster.node(VICTIM).flush().expect("flush post-loop write");
        wait_for_convergence(&cluster, POST_TIMEOUT);
        assert_acknowledged_present(
            &cluster,
            &HashSet::from([post_loop]),
        );
        eprintln!("post-loop write on the looped node replicated everywhere");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("crash-loop-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
