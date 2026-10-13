//! Rust port of the Go `soak-slo` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/soak-slo/`
//! (`soak_slo_test.go`: `TestThreeNodeEncryptedSustainedWriteSLO`).
//! Three encrypted nodes sustain independent managed-record writes for
//! `MURMUR_SLO_DURATION_SECONDS` (each node owns a bounded 1,000-row range:
//! inserts until the range fills, then updates), then converge to an
//! identical table digest within `MURMUR_SLO_SETTLE_SECONDS`. Write p95 must
//! stay under 5 s, maximum write latency under 20 s, replication queues
//! under 5,000 staged transactions, and spool disk usage under 512 MiB per
//! node. Plain SQL replaces the Go typed-contention HTTP API (see the
//! scenario README.md for the deviations).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};

/// Node count (Go: `NumNodes: 3`).
const NUM_NODES: usize = 3;
/// Bounded owned range per node (Go: `rowsPerNode = 1000`).
const ROWS_PER_NODE: u64 = 1000;
/// Minimum committed writes mesh-wide (Go: `operations < 60` fails).
const MIN_OPERATIONS: u64 = 60;
/// Replication queue SLO per node (Go: `spedsql_queued_need/ctrl <= 5000`).
const QUEUE_SLO: usize = 5000;
/// Spool disk budget per node (Go: 512 MiB `spedsql_spool_disk_bytes` cap).
const SPOOL_BUDGET_BYTES: u64 = 512 << 20;
/// Write p95 SLO (Go: 5 s).
const P95_SLO: Duration = Duration::from_secs(5);
/// Maximum write latency SLO (Go: 20 s).
const MAX_SLO: Duration = Duration::from_secs(20);
/// Mesh wait bound (Go: 30 s `waitForPeers`).
const PEER_TIMEOUT: Duration = Duration::from_secs(30);
/// Materialization quiescence bound (Rust-only settle; Go asserts instantly).
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 100 ms).
const POLL: Duration = Duration::from_millis(100);

/// Row count on one node, or `None` when the read fails.
fn row_count(db: &Database) -> Option<usize> {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM contention", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .ok()
}

/// Canonical encoded rows on one node (Go: `tableDigest` input:
/// `"id":"name":"phone"` sorted), or `None` when the read fails.
fn canonical_rows(db: &Database) -> Option<Vec<String>> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name, phone FROM contention ORDER BY id")?;
        let mapped = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            let phone: String = row.get(2)?;
            Ok(format!("{id:?}:{name:?}:{phone:?}"))
        })?;
        let mut out = Vec::new();
        for row in mapped {
            out.push(row?);
        }
        Ok(out)
    })
    .ok()
}

/// Deterministic log-only digest over canonical rows (Go: sha256 hex of the
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

/// Wait until every node holds a live session to the other two
/// (Go: `waitForPeers(t, cluster, 2, 30s)`).
fn wait_for_peers(cluster: &Cluster) {
    let peers_ok = wait_until(PEER_TIMEOUT, POLL, || {
        (0..cluster.len()).all(|i| {
            (0..cluster.len()).all(|j| i == j || peer_connected(cluster, i, cluster.node_id(j)))
        })
    });
    assert!(
        peers_ok,
        "three-node mesh did not reach 2 connected peers per node within {PEER_TIMEOUT:?}"
    );
}

#[test]
fn soak_slo() {
    let duration = env_seconds("MURMUR_SLO_DURATION_SECONDS", 5);
    let settle = env_seconds("MURMUR_SLO_SETTLE_SECONDS", 30);
    let cluster = Cluster::new(ClusterOptions {
        name: "soak-slo".into(),
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
        // Same schema on every node (Go: typed-contention fixture table).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE contention(id TEXT PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL)",
                ))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }
        wait_for_peers(&cluster);

        // One writer thread per node: bounded owned range, inserts while the
        // sequence is below the range size, updates afterwards (Go: worker
        // goroutines with the same insert/update split and row-id shape).
        let stop_at = Instant::now() + duration;
        let writes: Arc<[AtomicU64; NUM_NODES]> =
            Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let latencies: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
        let mut workers = Vec::with_capacity(NUM_NODES);
        for node in 0..NUM_NODES {
            let db = cluster.node(node).clone();
            let writes = writes.clone();
            let latencies = latencies.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("soak-slo-writer-{node}"))
                    .spawn(move || -> Result<(), String> {
                        loop {
                            if Instant::now() >= stop_at {
                                return Ok(());
                            }
                            let seq = writes[node].load(Ordering::SeqCst);
                            let slot = seq % ROWS_PER_NODE;
                            let row_id = format!(
                                "{:032x}",
                                ((node as i64) + 1) * 1_000_000_000_000
                                    + (slot as i64)
                                    + 1
                            );
                            let value = format!("node-{}-write-{seq:09}", node + 1);
                            let started = Instant::now();
                            let outcome: Result<(), String> = (|| {
                                if seq < ROWS_PER_NODE {
                                    let name = format!("node-{}", node + 1);
                                    db.transaction(|sql| {
                                        sql.execute(
                                            "INSERT INTO contention(id, name, phone) VALUES(?, ?, ?)",
                                            (row_id, name, value),
                                        )?;
                                        Ok(())
                                    })
                                    .map_err(|e| e.to_string())?;
                                } else {
                                    let changed = db
                                        .transaction(|sql| {
                                            Ok(sql.execute(
                                                "UPDATE contention SET phone = ? WHERE id = ?",
                                                (value, row_id),
                                            )?)
                                        })
                                        .map_err(|e| e.to_string())?;
                                    if changed != 1 {
                                        return Err(format!(
                                            "update touched {changed} rows, want 1"
                                        ));
                                    }
                                }
                                db.flush().map_err(|e| e.to_string())?;
                                Ok(())
                            })();
                            latencies.lock().unwrap().push(started.elapsed());
                            match outcome {
                                Ok(()) => {
                                    writes[node].fetch_add(1, Ordering::SeqCst);
                                }
                                Err(e) => {
                                    return Err(format!("node{} write: {e}", node + 1));
                                }
                            }
                        }
                    })
                    .expect("spawn writer thread"),
            );
        }

        let mut worker_errors = Vec::new();
        for (node, worker) in workers.into_iter().enumerate() {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => worker_errors.push(e),
                Err(_) => worker_errors.push(format!("writer thread {node} panicked")),
            }
        }
        assert!(
            worker_errors.is_empty(),
            "writer failures: {}",
            worker_errors.join("; ")
        );

        let mut expected = 0usize;
        let mut operations = 0u64;
        for i in 0..NUM_NODES {
            let w = writes[i].load(Ordering::SeqCst);
            operations += w;
            expected += Ord::min(w, ROWS_PER_NODE) as usize;
        }
        assert!(
            operations >= MIN_OPERATIONS,
            "only {operations} application writes committed across {duration:?}"
        );

        // Every node must hold exactly the owned rows with identical
        // contents (Go: equal counts, equal digests).
        let mut digest = String::new();
        let converged = wait_until(settle, POLL, || {
            let first = match canonical_rows(cluster.node(0)) {
                Some(rows) => rows,
                None => return false,
            };
            if first.len() != expected {
                return false;
            }
            if !(1..NUM_NODES)
                .all(|i| canonical_rows(cluster.node(i)).as_ref() == Some(&first))
            {
                return false;
            }
            digest = digest_hex(&first);
            true
        });
        if !converged {
            for i in 0..NUM_NODES {
                eprintln!(
                    "{} count={:?}",
                    cluster.node_info(i).label,
                    row_count(cluster.node(i))
                );
            }
            panic!("three application processes did not converge to {expected} rows within {settle:?}");
        }

        // Per-node SLO gates (Go: ready state, materialized generation,
        // queue depth, spool disk usage).
        for i in 0..NUM_NODES {
            let label = cluster.node_info(i).label.clone();
            let db = cluster.node(i);
            assert!(
                db.replication()
                    .expect("replication handle")
                    .status()
                    .running,
                "{label} replication is not running"
            );
            db.flush()
                .unwrap_or_else(|e| panic!("{label} flush before materialization check: {e}"));
            let quiesced = wait_until(QUIESCE_TIMEOUT, POLL, || {
                let s = db.status();
                s.storage_error.is_none() && s.pending_bytes == 0 && s.committed == s.durable
            });
            assert!(quiesced, "{label} state has not been fully materialized");
            let staged = db
                .replication()
                .expect("replication handle")
                .status()
                .staged_transactions;
            assert!(
                staged <= QUEUE_SLO,
                "{label} exceeded queue SLO: staged={staged}"
            );
            let disk_bytes = db.spool_metrics().disk_bytes;
            assert!(
                disk_bytes <= SPOOL_BUDGET_BYTES,
                "{label} Spool size {disk_bytes} exceeds 512 MiB SLO"
            );
        }

        let mut latencies = latencies.lock().unwrap();
        assert!(
            !latencies.is_empty(),
            "no write latencies recorded across {duration:?}"
        );
        latencies.sort();
        // Same nearest-rank p95 position as the Go original.
        let p95 = latencies[(95 * latencies.len() + 99) / 100 - 1];
        let max_latency = latencies[latencies.len() - 1];
        eprintln!(
            "multi-process encrypted write soak duration={duration:?} rows={expected} operations={operations} digest={digest} writes_per_node={}/{}/{} write_p95={p95:?} max={max_latency:?}",
            writes[0].load(Ordering::SeqCst),
            writes[1].load(Ordering::SeqCst),
            writes[2].load(Ordering::SeqCst),
        );
        assert!(p95 <= P95_SLO, "write p95 {p95:?} exceeded 5s SLO");
        assert!(
            max_latency <= MAX_SLO,
            "maximum write latency {max_latency:?} exceeded 20s SLO"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("soak-slo-no-converge");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
