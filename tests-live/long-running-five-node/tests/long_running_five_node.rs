//! Rust port of the Go `long-running-five-node` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/long-running-five-node/`
//! (`five_node_soak_test.go`: `TestFiveNodePersistentCapacityAndMeshSoak`).
//! Five nodes run a continuous per-node write soak across a full mesh while
//! node2 and node4 restart in place mid-run; persisted rows must survive each
//! reopen, all five nodes must converge to the same row set, every node must
//! be fully materialized within a 2 GiB spool budget, and one final
//! application write must converge. Plain SQL replaces the Go typed-records
//! HTTP API (see the scenario README.md for the deviations).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};

/// Node count (Go: `NumNodes: 5`).
const NUM_NODES: usize = 5;
/// Zero-based restart targets (Go: nodes 1 and 3, i.e. node2 and node4).
const RESTART_NODES: [usize; 2] = [1, 3];
/// Minimum acknowledged rows per node (Go: `writes[i] < 10` fails).
const MIN_ROWS_PER_NODE: u64 = 10;
/// Minimum acknowledged rows across the mesh (Go: `totalAcked < 100` fails).
const MIN_ROWS_TOTAL: u64 = 100;
/// Persistent capacity budget per node (Go: 2 GiB `spedsql_spool_disk_bytes` cap).
const SPOOL_BUDGET_BYTES: u64 = 2 << 30;
/// Mesh wait bound (Go: 45 s `waitForPeers`).
const PEER_TIMEOUT: Duration = Duration::from_secs(45);
/// Post-restart persistence poll bound (Go: 30 s).
const PERSIST_TIMEOUT: Duration = Duration::from_secs(30);
/// Materialization quiescence bound (Rust-only settle; Go asserts instantly).
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);
/// Peer/persistence poll interval (Go: 100 ms).
const TOPO_POLL: Duration = Duration::from_millis(100);
/// Convergence poll interval (Go: 200 ms soak loop).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// Post-write convergence poll interval (Go: 100 ms).
const POST_WRITE_POLL: Duration = Duration::from_millis(100);

/// Write cadence (Go: `MURMUR_FIVE_NODE_INTERVAL_MS`, default 50).
fn env_millis(name: &str, fallback_ms: u64) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(fallback_ms))
}

/// Sleep until `deadline`, returning immediately when it already passed
/// (Go: `time.Sleep(max(time.Until(deadline), 0))`).
fn sleep_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now {
        std::thread::sleep(deadline - now);
    }
}

/// Row count on one node, or `None` when the read fails.
fn row_count(db: &Database) -> Option<usize> {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM records", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .ok()
}

/// Canonical ordered names on one node (Go: `Cluster.TypedNames` + sort),
/// or `None` when the read fails.
fn canonical_rows(db: &Database) -> Option<Vec<String>> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM records ORDER BY name")?;
        let mapped = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in mapped {
            out.push(name?);
        }
        Ok(out)
    })
    .ok()
}

/// Deterministic log-only digest over sorted names (Go: sha256 hex of the
/// sorted table; comparisons use byte-identical vectors, which imply
/// identical digests). FNV-1a keeps this dependency-free.
fn digest_hex(rows: &[String]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for row in rows {
        for b in row.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= b'\n' as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

/// Live-session probe for one directed pair (Go: `connectedPeers`).
fn peer_connected(cluster: &Cluster, node: usize, peer_id: [u8; 16]) -> bool {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .peers_diagnostics
        .iter()
        .any(|d| d.node_id == peer_id && d.connected)
}

/// Live-session count on one node (Go: `connectedPeers`, for failure logs).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .peers
}

/// Wait until every node holds a live session to the other four
/// (Go: `waitForPeers(t, cluster, 4, 45s)`).
fn wait_for_peers(cluster: &Cluster, what: &str) {
    let peers_ok = wait_until(PEER_TIMEOUT, TOPO_POLL, || {
        (0..cluster.len()).all(|i| {
            (0..cluster.len()).all(|j| i == j || peer_connected(cluster, i, cluster.node_id(j)))
        })
    });
    assert!(
        peers_ok,
        "five-node mesh did not reach 4 connected peers per node within {PEER_TIMEOUT:?} ({what})"
    );
}

/// Wait until every node holds exactly `count` rows with identical contents
/// (Go: `waitForConvergence`); returns the shared digest.
fn wait_for_row_count(cluster: &Cluster, count: usize, timeout: Duration) -> String {
    let mut digest = String::new();
    let converged = wait_until(timeout, POST_WRITE_POLL, || {
        let first = match canonical_rows(cluster.node(0)) {
            Some(rows) => rows,
            None => return false,
        };
        if first.len() != count {
            return false;
        }
        if !(1..NUM_NODES).all(|i| canonical_rows(cluster.node(i)).as_ref() == Some(&first)) {
            return false;
        }
        digest = digest_hex(&first);
        true
    });
    assert!(converged, "post-soak write did not converge to {count} rows");
    digest
}

/// Stop `node` in place, reopen the same directory, rejoin the mesh, and
/// require its persisted rows to still be present (Go:
/// `restartAndCheckPersistence`). The writer handle for the node is swapped
/// for the reopened database so the soak continues on the live handle.
fn restart_and_check_persistence(
    cluster: &mut Cluster,
    handles: &[Mutex<Database>],
    node: usize,
) {
    let label = format!("node{}", node + 1);
    let started = Instant::now();
    let before = row_count(cluster.node(node)).expect("pre-restart row count");
    cluster.stop_node(node);
    cluster
        .start_node(node)
        .unwrap_or_else(|e| panic!("{label} restart failed: {e}"));
    // The reopened node rebinds a fresh ephemeral port: re-mesh so every
    // ordered pair dials the current addresses (Go: `StartNode` re-dials).
    cluster.mesh().expect("re-mesh after restart");
    *handles[node].lock().unwrap() = cluster.node(node).clone();
    wait_for_peers(cluster, &format!("{label} rejoin"));
    let recovered = wait_until(PERSIST_TIMEOUT, TOPO_POLL, || {
        row_count(cluster.node(node)).is_some_and(|got| got >= before)
    });
    let got = row_count(cluster.node(node)).unwrap_or(0);
    assert!(
        recovered,
        "{label} lost durable rows after restart: got={got} before={before}"
    );
    eprintln!(
        "{label} restarted and rejoined in {:?} with {got} rows (had {before} before restart)",
        started.elapsed()
    );
}

#[test]
fn long_running_five_node() {
    let duration = env_seconds("MURMUR_FIVE_NODE_DURATION_SECONDS", 30);
    let interval = env_millis("MURMUR_FIVE_NODE_INTERVAL_MS", 50);
    let settle = env_seconds("MURMUR_FIVE_NODE_SETTLE_SECONDS", 120);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "long-running-five-node".into(),
        num_nodes: NUM_NODES,
        ..Default::default()
    })
    .expect("cluster setup");
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "Node {} running in directory {} (repl={})",
            info.label,
            info.dir.display(),
            info.repl_addr,
        );
    }
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }
        wait_for_peers(&cluster, "initial mesh");

        let start = Instant::now();
        let first_restart_at = start + duration / 3;
        let second_restart_at = start + 2 * duration / 3;
        let stop_at = start + duration;

        // Writer threads share a handle table with the main thread: the
        // entry for a restarted node is swapped for the reopened database
        // (Go: writers address daemons by stable address across restarts).
        // A committed transaction counts as acknowledged (Go: `writes`);
        // failed transactions and failed flushes count as transient errors
        // (Go: `failures`), which the restart windows are expected to
        // produce on the two restarted nodes.
        let handles: Arc<Vec<Mutex<Database>>> = Arc::new(
            (0..NUM_NODES)
                .map(|i| Mutex::new(cluster.node(i).clone()))
                .collect(),
        );
        let writes: Arc<[AtomicU64; NUM_NODES]> =
            Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let failures: Arc<[AtomicU64; NUM_NODES]> =
            Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let stop = Arc::new(AtomicBool::new(false));

        let mut workers = Vec::with_capacity(NUM_NODES);
        for node in 0..NUM_NODES {
            let handles = handles.clone();
            let writes = writes.clone();
            let failures = failures.clone();
            let stop = stop.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("five-node-writer-{node}"))
                    .spawn(move || loop {
                        if stop.load(Ordering::SeqCst) {
                            return;
                        }
                        std::thread::sleep(interval);
                        if stop.load(Ordering::SeqCst) || Instant::now() >= stop_at {
                            return;
                        }
                        // Only acknowledged writes advance the sequence, so
                        // ids stay dense per node (Go: `seq := writes[node]`).
                        let seq = writes[node].load(Ordering::SeqCst);
                        let id = format!("n{}-{seq:09}", node + 1);
                        let name = format!("node-{}-capacity-{seq:09}", node + 1);
                        let db = handles[node].lock().unwrap().clone();
                        let committed = db
                            .transaction(move |sql| {
                                sql.execute(
                                    "INSERT INTO records(id, name) VALUES(?, ?)",
                                    (id, name),
                                )?;
                                Ok(())
                            })
                            .is_ok();
                        if committed {
                            writes[node].fetch_add(1, Ordering::SeqCst);
                            if db.flush().is_err() {
                                failures[node].fetch_add(1, Ordering::SeqCst);
                            }
                        } else {
                            failures[node].fetch_add(1, Ordering::SeqCst);
                        }
                    })
                    .expect("spawn writer thread"),
            );
        }

        sleep_until(first_restart_at);
        restart_and_check_persistence(&mut cluster, &handles, RESTART_NODES[0]);
        sleep_until(second_restart_at);
        restart_and_check_persistence(&mut cluster, &handles, RESTART_NODES[1]);
        sleep_until(stop_at);
        stop.store(true, Ordering::SeqCst);
        for (node, worker) in workers.into_iter().enumerate() {
            worker
                .join()
                .unwrap_or_else(|_| panic!("writer thread {node} panicked"));
        }

        let mut total_acked: usize = 0;
        for i in 0..NUM_NODES {
            let acked = writes[i].load(Ordering::SeqCst);
            total_acked += acked as usize;
            let label = cluster.node_info(i).label.clone();
            eprintln!(
                "{label} acknowledged={acked} transient_write_errors={}",
                failures[i].load(Ordering::SeqCst)
            );
            assert!(
                acked >= MIN_ROWS_PER_NODE,
                "{label} committed only {acked} rows during {duration:?}"
            );
        }
        assert!(
            total_acked as u64 >= MIN_ROWS_TOTAL,
            "only {total_acked} application writes committed across five nodes"
        );

        // Every node must reach the same row set with at least the
        // acknowledged rows (Go: equal counts, equal digests, count >= acked).
        let converged = wait_until(settle, CONVERGE_POLL, || {
            let first = match canonical_rows(cluster.node(0)) {
                Some(rows) => rows,
                None => return false,
            };
            if first.len() < total_acked {
                return false;
            }
            (1..NUM_NODES).all(|i| canonical_rows(cluster.node(i)).as_ref() == Some(&first))
        });
        if !converged {
            for i in 0..NUM_NODES {
                eprintln!(
                    "{} count={:?} peers={}",
                    cluster.node_info(i).label,
                    row_count(cluster.node(i)),
                    peer_count(&cluster, i)
                );
            }
            panic!(
                "five-node mesh did not converge to at least {total_acked} acknowledged rows within {settle:?}"
            );
        }
        let final_rows = canonical_rows(cluster.node(0)).expect("final canonical rows");
        let final_digest = digest_hex(&final_rows);
        let final_count = final_rows.len();

        // Fully-materialized state within the persistent capacity budget
        // (Go: `spedsql_materialized_generation ==
        // spedsql_state_generation`, `spedsql_spool_disk_bytes <= 2 GiB`).
        for i in 0..NUM_NODES {
            let label = cluster.node_info(i).label.clone();
            let db = cluster.node(i);
            db.flush()
                .unwrap_or_else(|e| panic!("{label} flush before materialization check: {e}"));
            let quiesced = wait_until(QUIESCE_TIMEOUT, CONVERGE_POLL, || {
                let s = db.status();
                s.storage_error.is_none() && s.pending_bytes == 0 && s.committed == s.durable
            });
            assert!(quiesced, "{label} state has not been fully materialized");
            let disk_bytes = db.spool_metrics().disk_bytes;
            assert!(
                disk_bytes <= SPOOL_BUDGET_BYTES,
                "{label} exceeded 2 GiB persistent capacity limit ({disk_bytes} bytes)"
            );
        }

        // One final application write must converge everywhere (Go:
        // `TypedInsert(4, "post-soak-write")` + `waitForConvergence`).
        let post = "post-soak-write".to_string();
        cluster
            .node(NUM_NODES - 1)
            .transaction(move |sql| {
                sql.execute(
                    "INSERT INTO records(id, name) VALUES(?, ?)",
                    (post.clone(), post),
                )?;
                Ok(())
            })
            .expect("post-soak application write");
        cluster
            .node(NUM_NODES - 1)
            .flush()
            .expect("flush post-soak write");
        let post_digest = wait_for_row_count(&cluster, final_count + 1, settle);
        eprintln!(
            "five encrypted nodes held {duration:?} of load, survived two orderly data-directory restarts, and converged {} rows (digest={post_digest}; before-post-write={final_digest})",
            final_count + 1
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("five-node-soak-no-converge");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
