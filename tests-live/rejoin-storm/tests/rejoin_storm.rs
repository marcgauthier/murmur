//! Rust port of the Go `rejoin-storm` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/rejoin-storm/`
//! (`rejoin_storm_test.go`). Two of three meshed nodes are stopped at once
//! while the survivor keeps taking tracked writes, then both restart
//! simultaneously (thundering-herd rejoin behind a start barrier). Both
//! must rejoin un-wedged, every outage write must land everywhere, counts
//! plus ordered digests must match exactly, and each node's rejoin path
//! (range repair vs snapshot) is recorded. Plain SQL replaces the Go
//! typed-records HTTP API, and in-process close/reopen replaces SIGKILL
//! (see the scenario README.md for deviations).

use std::collections::HashSet;
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Table DDL (Go: typed records; only names matter here).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)";
/// Outage-row knob (Go: `MURMUR_REJOIN_STORM_OUTAGE_ROWS`, default 40).
const OUTAGE_ENV: &str = "MURMUR_REJOIN_STORM_OUTAGE_ROWS";
const DEFAULT_OUTAGE_ROWS: usize = 40;
/// Positive-control waits (Go: 30 s peers + convergence).
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
/// Rejoin convergence deadline (Go: 90 s).
const REJOIN_TIMEOUT: Duration = Duration::from_secs(90);
/// Post-rejoin convergence deadline (Go: 60 s).
const POST_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 100 ms).
const POLL: Duration = Duration::from_millis(100);
/// No-wedge bound per restart (Go: ready < 60 s).
const READY_BOUND: Duration = Duration::from_secs(60);

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

/// Insert one record (Go: `Cluster.TypedInsert`).
fn insert_row(db: &Database, node: usize, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute("INSERT INTO live_typed_records(name) VALUES(?)", [name])?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
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

/// Poll exact counts plus ordered-digest equality (Go: `waitForConvergence`).
fn wait_for_convergence(cluster: &Cluster, want: usize, timeout: Duration) {
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
    if !converged {
        for i in 0..cluster.len() {
            eprintln!("node{} at timeout: count={}", i + 1, names(cluster.node(i)).len());
        }
        panic!("nodes did not converge on {want} rows with equal digests within {timeout:?}");
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

#[test]
fn dual_kill_simultaneous_rejoin() {
    let outage_rows = std::env::var(OUTAGE_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_OUTAGE_ROWS);

    let mut cluster = Cluster::new(ClusterOptions {
        name: "rejoin-storm".into(),
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
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: typed fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Positive control: honest 3-node convergence before any crash.
        for node in 0..3 {
            insert_row(cluster.node(node), node + 1, &format!("base-node{}", node + 1));
        }
        wait_for_peers(&cluster, &[2, 2, 2], CONTROL_TIMEOUT);
        wait_for_convergence(&cluster, 3, CONTROL_TIMEOUT);
        eprintln!("positive control: 3-node mesh converged");

        // Double stop at once; the outage is real only if both nodes
        // actually go dark (no open handle left to answer queries).
        cluster.stop_node(1);
        cluster.stop_node(2);
        for idx in [1, 2] {
            assert!(
                cluster.node_info(idx).db.is_none(),
                "node{} answers queries after stop; outage not real",
                idx + 1
            );
        }
        eprintln!("nodes 2 and 3 stopped; survivor taking outage writes");

        // Survivor keeps taking tracked writes through the whole outage.
        let mut acked = HashSet::from([
            "base-node1".to_owned(),
            "base-node2".to_owned(),
            "base-node3".to_owned(),
        ]);
        for i in 0..outage_rows {
            let name = format!("outage-{i:04}");
            insert_row(cluster.node(0), 1, &name);
            acked.insert(name);
        }
        let survivor_count = names(cluster.node(0)).len();
        assert_eq!(
            survivor_count,
            3 + outage_rows,
            "survivor count = {survivor_count}, want {}",
            3 + outage_rows
        );

        // Thundering-herd rejoin: both nodes restart behind the same
        // barrier so their catch-up overlaps instead of serializing.
        // (`start_node` is `&mut`, so the simultaneously-released threads
        // serialize briefly on a mutex; the async catch-up after both are
        // up still overlaps.)
        let start = Arc::new(Barrier::new(3));
        let shared = Mutex::new(&mut cluster);
        let mut took = Vec::new();
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            for idx in [1, 2] {
                let start = start.clone();
                let shared = &shared;
                handles.push(s.spawn(move || {
                    start.wait();
                    let begin = Instant::now();
                    shared
                        .lock()
                        .expect("cluster lock")
                        .start_node(idx)
                        .unwrap_or_else(|e| panic!("restart node{}: {e:?}", idx + 1));
                    (idx, begin.elapsed())
                }));
            }
            start.wait();
            for handle in handles {
                took.push(handle.join().unwrap());
            }
        });
        for (idx, took) in &took {
            eprintln!("node{} ready in {took:?} after simultaneous restart", idx + 1);
            assert!(
                *took <= READY_BOUND,
                "node{} took {took:?} to reach ready, want < {READY_BOUND:?} (wedge)",
                idx + 1
            );
        }
        cluster.mesh().expect("re-mesh after restart");

        // Every outage write present on all three; exact counts plus
        // ordered digests identical everywhere.
        wait_for_convergence(&cluster, 3 + outage_rows, REJOIN_TIMEOUT);
        assert_acknowledged_present(&cluster, &acked);
        eprintln!("all {outage_rows} outage writes present on all 3 nodes with equal digests");

        // Record (never mandate) each node's rejoin path. Metrics are
        // fresh after the restart, so a flat log pair means snapshot and
        // an advanced pair means log range repair.
        for idx in [1, 2] {
            let (received, applied) = log_counters(&cluster, idx);
            let path = if (received, applied) == (0, 0) {
                "snapshot"
            } else {
                "log range repair"
            };
            eprintln!("node{} rejoin path: {path} (received={received} applied={applied})", idx + 1);
        }

        // Post-rejoin writes on each previously-dead node replicate everywhere.
        let mut post = HashSet::new();
        for idx in [1, 2] {
            let name = format!("post-rejoin-node{}", idx + 1);
            insert_row(cluster.node(idx), idx + 1, &name);
            post.insert(name);
        }
        wait_for_convergence(&cluster, 3 + outage_rows + 2, POST_TIMEOUT);
        assert_acknowledged_present(&cluster, &post);
        eprintln!("post-rejoin writes on both revived nodes replicated everywhere");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("rejoin-storm-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
