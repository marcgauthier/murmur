//! Rust port of the Go `three-node-sync` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/three-node-sync/`
//! (`sync_test.go`). Three nodes each commit 30 uniquely-named rows
//! concurrently; the test waits until every node holds all 90 names in
//! canonical order. Plain SQL replaces the Go typed-records HTTP API
//! (see the scenario README.md for the deviations).

use std::sync::{Arc, Barrier};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Rows each node inserts (Go: `rowsPerNode = 30`).
const ROWS_PER_NODE: usize = 30;
/// Convergence deadline (Go: 25 s).
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(25);
/// Convergence poll interval (Go: 100 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> murmur_live_harness::sqlite_session::Result<Vec<String>> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM records ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in rows {
            out.push(name?);
        }
        Ok(out)
    })
}

#[test]
fn three_node_sync() {
    let cluster = Cluster::new(ClusterOptions {
        name: "three-node-sync".into(),
        num_nodes: 3,
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
                    "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Concurrent per-node writes (Go: one goroutine per node, one
        // insert call per row). Row ids are node-disjoint by construction.
        let start = Arc::new(Barrier::new(cluster.len()));
        std::thread::scope(|s| {
            for node_idx in 0..cluster.len() {
                let db = cluster.node(node_idx).clone();
                let start = start.clone();
                s.spawn(move || {
                    start.wait();
                    for row in 0..ROWS_PER_NODE {
                        let name = format!("node-{node_idx}-row-{row:02}");
                        let id = (node_idx * 1000 + row) as i64;
                        db.transaction(move |sql| {
                            sql.execute(
                                "INSERT INTO records(id, name) VALUES(?, ?)",
                                (id, name),
                            )?;
                            Ok(())
                        })
                        .unwrap_or_else(|e| panic!("node {node_idx} insert failed: {e}"));
                    }
                    db.flush().expect("flush after inserts");
                });
            }
        });

        // Canonical ordered expectation (Go: `want`, node-major, zero-padded).
        let mut want = Vec::with_capacity(ROWS_PER_NODE * cluster.len());
        for node in 0..cluster.len() {
            for row in 0..ROWS_PER_NODE {
                want.push(format!("node-{node}-row-{row:02}"));
            }
        }

        let converged = wait_until(CONVERGENCE_TIMEOUT, POLL_INTERVAL, || {
            for i in 0..cluster.len() {
                match names(cluster.node(i)) {
                    Ok(got) if got == want => {}
                    _ => return false,
                }
            }
            true
        });
        assert!(
            converged,
            "three nodes did not converge within deadline (expected {} records)",
            want.len()
        );
        eprintln!("Three nodes converged on {} records", want.len());
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
