//! Rust port of the Go `scale-mesh` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/scale-mesh/`
//! (`scale_mesh_test.go`: `TestTenNodeMeshConvergesBounded`). Ten nodes mesh
//! with the default fanout of 4 while two origins write 400 rows; every node
//! must converge to identical row sets, and a 2 s sampler asserts no node
//! ever selects more than 4 replication targets for the whole run. Plain SQL
//! replaces the Go typed-contention HTTP API (see the scenario README.md for
//! the deviations).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Node count (Go: `nodes = 10`).
const NUM_NODES: usize = 10;
/// Rows written by the first origin on node 1 (Go: `0..300` on node 0).
const FIRST_ORIGIN_ROWS: usize = 300;
/// Total rows incl. the second origin (Go: `300..400` on node 8).
const TOTAL_ROWS: usize = 400;
/// Zero-based index of the second origin (Go: node index 7).
const SECOND_ORIGIN: usize = 7;
/// Replication fanout bound (Go: default fanout 4, asserted as `fanout = 4`).
const FANOUT: usize = 4;
/// Fanout sampler interval (Go: `2 * time.Second`).
const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
/// Convergence deadline (Go: 300 s for ten nodes on a saturated box).
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(300);
/// Convergence poll interval (Go: 200 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Breach channel capacity (Go: `breaches := make(chan string, 64)`).
const BREACH_CAP: usize = 64;

/// Selected (push-mode) replication targets on one node.
///
/// Go reads `selected_peers` from `/v1/status`; the Rust equivalent is the
/// count of connected peers flagged `push_mode` in the replication status.
/// The engine caps push peers at `config.fanout` on insertion and swaps them
/// one-for-one under a single lock during rotation, so the count must never
/// exceed the fanout. Returns `None` when the node has no replication handle.
fn selected_peers(db: &Database) -> Option<usize> {
    db.replication().map(|handle| {
        handle
            .status()
            .peers_diagnostics
            .iter()
            .filter(|d| d.connected && d.push_mode)
            .count()
    })
}

/// One fanout-bound sample over every node (Go: `sampleFanoutBound`).
///
/// Never panics: sampler failures are reported as breach strings, and a full
/// channel drops (Go: non-blocking send with `default`).
fn sample_fanout_bound(dbs: &[Database], fanout: usize, breaches: &mpsc::SyncSender<String>) {
    for (idx, db) in dbs.iter().enumerate() {
        match selected_peers(db) {
            Some(got) if got > fanout => {
                let _ = breaches.try_send(format!(
                    "node{} selected_peers={got} exceeds fanout {fanout}",
                    idx + 1
                ));
            }
            Some(_) => {}
            None => {
                let _ = breaches.try_send(format!("node{} replication handle missing", idx + 1));
            }
        }
    }
}

/// Row count on one node, or `None` when the read fails (Go: `len(rows)`).
fn row_count(db: &Database) -> Option<usize> {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM records", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .ok()
}

/// Canonical PK-ordered `id:name` lines on one node (Go: `meshDigest` input).
fn canonical_rows(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name FROM records ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            let id = row.get::<_, String>(0)?;
            let name = row.get::<_, String>(1)?;
            Ok(format!("{id}:{name}"))
        })?;
        let mut out = Vec::new();
        for line in rows {
            out.push(line?);
        }
        Ok(out)
    })
    .expect("read canonical rows")
}

/// One insert on the origin node (Go: `TypedContentionInsert`, one call/row).
fn insert_row(db: &Database, i: usize) {
    let id = format!("{i:032x}");
    let name = format!("w-{i}");
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO records(id, name) VALUES(?, ?)",
            (id, name),
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("write {i} failed: {e}"));
}

#[test]
fn scale_mesh() {
    let cluster = Cluster::new(ClusterOptions {
        name: "scale-mesh".into(),
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
                    "CREATE TABLE records(id TEXT PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Bound sampling runs for the whole test. The sampler reports
        // breaches over a channel; only the test thread may panic.
        let sampler_dbs: Vec<Database> =
            (0..cluster.len()).map(|i| cluster.node(i).clone()).collect();
        let stop = Arc::new(AtomicBool::new(false));
        let (breach_tx, breach_rx) = mpsc::sync_channel::<String>(BREACH_CAP);
        let sampler = {
            let stop = stop.clone();
            let breach_tx = breach_tx.clone();
            let sampler_dbs = sampler_dbs.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    std::thread::sleep(SAMPLE_INTERVAL);
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    sample_fanout_bound(&sampler_dbs, FANOUT, &breach_tx);
                }
            })
        };

        // Two origins: 300 rows on node 1, 100 on node 8. The second origin
        // proves multi-hop forwarding, not just hub fan-out.
        for i in 0..FIRST_ORIGIN_ROWS {
            insert_row(cluster.node(0), i);
        }
        cluster.node(0).flush().expect("flush first origin");
        for i in FIRST_ORIGIN_ROWS..TOTAL_ROWS {
            insert_row(cluster.node(SECOND_ORIGIN), i);
        }
        cluster
            .node(SECOND_ORIGIN)
            .flush()
            .expect("flush second origin");

        // Every node must hold all 400 rows (Go: `waitAllCounts`, 300 s).
        let converged = wait_until(CONVERGENCE_TIMEOUT, POLL_INTERVAL, || {
            (0..cluster.len()).all(|i| row_count(cluster.node(i)) == Some(TOTAL_ROWS))
        });
        assert!(
            converged,
            "10-node counts did not reach {TOTAL_ROWS} within {CONVERGENCE_TIMEOUT:?}"
        );

        // Identical canonical row sets on every node (Go: `meshDigest`
        // equality; byte-identical vectors imply identical digests).
        let want = canonical_rows(cluster.node(0));
        assert_eq!(want.len(), TOTAL_ROWS, "node1 canonical row count");
        for idx in 1..cluster.len() {
            let got = canonical_rows(cluster.node(idx));
            assert_eq!(
                got,
                want,
                "node{} rows differ after 10-node convergence",
                idx + 1
            );
        }
        eprintln!("Ten nodes converged on {TOTAL_ROWS} rows with fanout {FANOUT}");

        // Final sample, then stop the sampler and fail on any breach.
        sample_fanout_bound(&sampler_dbs, FANOUT, &breach_tx);
        stop.store(true, Ordering::SeqCst);
        sampler.join().expect("sampler thread");
        drop(breach_tx);
        let breaches: Vec<String> = breach_rx.into_iter().collect();
        assert!(breaches.is_empty(), "fanout breaches: {breaches:?}");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("scale-mesh-convergence-or-fanout");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
