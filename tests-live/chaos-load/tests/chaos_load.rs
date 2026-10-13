//! Rust port of the Go `chaos-load` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/chaos-load/`
//! (`chaos_load_test.go`). Three nodes write concurrently while node3 is cut
//! off from the node1/node2 pair; the test proves divergent row counts and
//! ordered state digests, then re-admits peers while writes continue, waits
//! for full count/digest convergence, and commits a post-heal write. Plain
//! SQL replaces the Go typed-records HTTP API (see the scenario README.md).

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};

/// Convergence poll interval (Go: 50 ms, 100 ms for full-cluster counts).
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Per-writer insert cadence (Go: 25 ms ticker).
const WRITE_INTERVAL: Duration = Duration::from_millis(25);
/// Baseline convergence deadline (Go: 20 s).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(20);
/// Split / heal peer-mesh deadlines (Go: 20 s / 30 s).
const SPLIT_PEERS_TIMEOUT: Duration = Duration::from_secs(20);
const HEAL_PEERS_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-node count deadline during the split (Go: 15 s).
const SPLIT_COUNT_TIMEOUT: Duration = Duration::from_secs(15);
/// Full convergence deadline after healing (Go: 60 s).
const HEAL_COUNT_TIMEOUT: Duration = Duration::from_secs(60);
/// Post-heal write deadline (Go: 20 s).
const POST_HEAL_TIMEOUT: Duration = Duration::from_secs(20);
/// Measurement-barrier drain deadline (Go: 5 s).
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Row-id stride per writer node: ids are `node * ID_STRIDE + seq`, so the
/// three concurrent writers never collide (Go: server-generated typed IDs).
const ID_STRIDE: i64 = 10_000_000;
/// Baseline and post-heal marker ids, outside every writer range.
const BASELINE_ID: i64 = 0;
const POST_HEAL_ID: i64 = 99_000_001;

/// All record names on one node (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM records")?;
        let mapped = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in mapped {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read record names")
}

/// Canonical digest of a name set (Go: `namesDigest`, sorted then joined).
fn names_digest(mut names: Vec<String>) -> String {
    names.sort();
    names.join("\n")
}

/// Insert one uniquely-named row (Go: `Cluster.TypedInsert`).
fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    let value = name.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            sql.execute("INSERT INTO records(id, name) VALUES(?, ?)", (id, value))?;
            Ok(())
        })
        .map_err(|e| format!("node{} insert '{name}' failed: {e}", node + 1))?;
    cluster
        .node(node)
        .flush()
        .map_err(|e| format!("node{} flush after '{name}' failed: {e}", node + 1))?;
    Ok(())
}

/// Live QUIC connections on one node (Go: `/v1/status connected_peers`).
fn connected_peers(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .map(|h| {
            h.status()
                .peers_diagnostics
                .iter()
                .filter(|d| d.connected)
                .count()
        })
        .unwrap_or(0)
}

/// Wait until every node holds exactly `expected` rows (Go: `waitAllCount`).
fn wait_all_count(cluster: &Cluster, expected: usize, timeout: Duration, what: &str) {
    let converged = wait_until(timeout, Duration::from_millis(100), || {
        (0..cluster.len()).all(|i| names(cluster.node(i)).len() == expected)
    });
    assert!(
        converged,
        "cluster failed to converge to {expected} rows within {timeout:?} ({what})"
    );
}

/// Wait until one node holds exactly `expected` rows (Go: `waitNodeCount`).
fn wait_node_count(cluster: &Cluster, node: usize, expected: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        names(cluster.node(node)).len() == expected
    });
    assert!(
        converged,
        "node{} rows={} within {timeout:?}, want {expected}",
        node + 1,
        names(cluster.node(node)).len(),
    );
}

/// Wait until every node reports its expected live-peer count (Go: `waitForPeers`).
fn wait_for_peers(cluster: &Cluster, want: &[usize], timeout: Duration) {
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        (0..cluster.len()).all(|i| connected_peers(cluster, i) == want[i])
    });
    assert!(converged, "peer mesh failed to reach {want:?} within {timeout:?}");
}

/// Freeze writers and wait for in-flight inserts to finish (Go: `pauseAndDrain`).
fn pause_and_drain(paused: &AtomicBool, active: &AtomicI64) {
    paused.store(true, Ordering::SeqCst);
    let drained = wait_until(DRAIN_TIMEOUT, Duration::from_millis(1), || {
        active.load(Ordering::SeqCst) == 0
    });
    assert!(
        drained,
        "{} application writes still active at measurement barrier",
        active.load(Ordering::SeqCst)
    );
}

/// Assert identical row counts and digests on all nodes (Go: `assertConverged`).
fn assert_converged(cluster: &Cluster, want_rows: usize) {
    let mut digest = String::new();
    for i in 0..cluster.len() {
        let node_names = names(cluster.node(i));
        assert!(
            node_names.len() == want_rows,
            "node{} rows={}, want {want_rows}",
            i + 1,
            node_names.len()
        );
        let got = names_digest(node_names);
        if digest.is_empty() {
            digest = got;
        } else {
            assert!(got == digest, "node{} digest differs from node1", i + 1);
        }
    }
}

/// Stops the writer threads when the measurement phases unwind, so a failed
/// assertion cannot deadlock the thread scope waiting for writers that never
/// observe the stop flag (idempotent on the success path).
struct StopOnDrop<'a> {
    stop: &'a AtomicBool,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

#[test]
fn continuous_writes_across_partition_and_healing() {
    let cluster = Cluster::new(ClusterOptions {
        name: "chaos-load".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Share explicit `&` bindings: the writer threads below are `move`
        // closures, which would otherwise take the owned values.
        let cluster = &cluster;
        for i in 0..cluster.len() {
            let info = cluster.node_info(i);
            eprintln!(
                "{} dir={} repl={}",
                info.label,
                info.dir.display(),
                info.repl_addr
            );
        }
        // Same schema on every node (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }
        insert(cluster, 0, BASELINE_ID, "baseline").expect("baseline insert");
        wait_all_count(cluster, 1, BASELINE_TIMEOUT, "baseline");

        // Split {node1,node2} from {node3} by removing every cross-partition
        // relationship in both directions. Keep the surviving pair connected.
        for (from, to) in [(0, 2), (2, 0), (1, 2), (2, 1)] {
            cluster.remove_peer(from, to).unwrap_or_else(|e| {
                panic!("remove peer node{} -> node{}: {e}", from + 1, to + 1)
            });
        }
        wait_for_peers(cluster, &[1, 1, 0], SPLIT_PEERS_TIMEOUT);

        let paused = AtomicBool::new(false);
        let active = AtomicI64::new(0);
        let stop = AtomicBool::new(false);
        let writes = [
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
        ];
        // Go records write failures via MarkFailed and keeps going; the Rust
        // writers collect them here and the test fails after they join.
        let write_errors = Mutex::new(Vec::<String>::new());
        let paused = &paused;
        let active = &active;
        let stop = &stop;
        let writes = &writes;
        let write_errors = &write_errors;

        std::thread::scope(|s| {
            for node in 0..cluster.len() {
                s.spawn(move || {
                    let mut seq: i64 = 0;
                    while !stop.load(Ordering::SeqCst) {
                        std::thread::sleep(WRITE_INTERVAL);
                        if stop.load(Ordering::SeqCst) {
                            break;
                        }
                        active.fetch_add(1, Ordering::SeqCst);
                        if paused.load(Ordering::SeqCst) {
                            active.fetch_sub(1, Ordering::SeqCst);
                            continue;
                        }
                        seq += 1;
                        let name = format!("node{}-write-{seq:06}", node + 1);
                        match insert(cluster, node, node as i64 * ID_STRIDE + seq, &name) {
                            Ok(()) => {
                                writes[node].fetch_add(1, Ordering::SeqCst);
                            }
                            Err(e) => write_errors.lock().expect("write_errors lock").push(e),
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                    }
                });
            }
            // Unwind guard: a failed phase assertion must still release the
            // writers so the scope can join them.
            let _stop_guard = StopOnDrop { stop };

            let partition_window = env_seconds("MURMUR_CHAOS_PARTITION_SECONDS", 12);
            let heal_window = env_seconds("MURMUR_CHAOS_HEAL_SECONDS", 12);
            std::thread::sleep(partition_window);
            pause_and_drain(paused, active);
            let left =
                1 + (writes[0].load(Ordering::SeqCst) + writes[1].load(Ordering::SeqCst)) as usize;
            let right = 1 + writes[2].load(Ordering::SeqCst) as usize;
            assert!(
                left >= 2 && right >= 1,
                "insufficient partition writes: left={left} right={right}"
            );
            wait_node_count(cluster, 0, left, SPLIT_COUNT_TIMEOUT);
            wait_node_count(cluster, 1, left, SPLIT_COUNT_TIMEOUT);
            wait_node_count(cluster, 2, right, SPLIT_COUNT_TIMEOUT);
            let left_digest = names_digest(names(cluster.node(0)));
            let right_digest = names_digest(names(cluster.node(2)));
            assert!(
                right_digest != left_digest,
                "partitioned groups unexpectedly have identical state digests"
            );
            assert!(
                connected_peers(cluster, 2) == 0,
                "isolated node3 has {} connected peers, want 0",
                connected_peers(cluster, 2)
            );
            eprintln!(
                "partition held for {partition_window:?} with divergent states: \
                 node1/node2={left} rows, node3={right} rows"
            );

            // Re-admit the cut edges while all application writers run again.
            paused.store(false, Ordering::SeqCst);
            for (from, to) in [(0, 2), (2, 0), (1, 2), (2, 1)] {
                cluster.add_peer(from, to).unwrap_or_else(|e| {
                    panic!("add peer node{} -> node{}: {e}", from + 1, to + 1)
                });
            }
            wait_for_peers(cluster, &[2, 2, 2], HEAL_PEERS_TIMEOUT);
            std::thread::sleep(heal_window);
            pause_and_drain(paused, active);
            stop.store(true, Ordering::SeqCst);
        });

        let errors = write_errors.lock().expect("write_errors lock");
        assert!(errors.is_empty(), "application write errors: {errors:?}");
        drop(errors);

        let total = 1
            + (writes[0].load(Ordering::SeqCst)
                + writes[1].load(Ordering::SeqCst)
                + writes[2].load(Ordering::SeqCst)) as usize;
        wait_all_count(cluster, total, HEAL_COUNT_TIMEOUT, "post-heal");
        assert_converged(cluster, total);
        eprintln!("healed with {} writes; all nodes converge", total - 1);

        // A new application write after reconciliation must still traverse
        // the healed mesh.
        insert(cluster, 1, POST_HEAL_ID, "post-heal").expect("post-heal write");
        wait_all_count(cluster, total + 1, POST_HEAL_TIMEOUT, "post-heal write");
        assert_converged(cluster, total + 1);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("chaos-load-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
