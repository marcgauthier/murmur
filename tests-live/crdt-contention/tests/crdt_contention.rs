//! Rust port of the Go `crdt-contention` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crdt-contention/`
//! (`contention_test.go`). Three nodes start from one replicated row, apply
//! concurrent disjoint-column updates (all must survive), then apply 40
//! competing updates per node to one shared cell; the shared-cell winner must
//! be identical everywhere and full rows must match. Plain SQL replaces the
//! Go typed-records HTTP API (see the scenario README.md for the deviations).

use std::sync::{Arc, Barrier};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Contended row id (Go: `rowID`).
const ROW_ID: &str = "03141592-6000-4000-8000-000000000000";
/// Competing updates per node on the shared cell (Go: `updatesPerNode = 40`).
const UPDATES_PER_NODE: usize = 40;
/// Peer-mesh deadline (Go: 20 s in `waitForPeers`).
const PEERS_TIMEOUT: Duration = Duration::from_secs(20);
/// Baseline / disjoint-state deadlines (Go: 20 s in `waitForState`).
const STATE_TIMEOUT: Duration = Duration::from_secs(20);
/// Shared-cell convergence deadline (Go: 30 s in `waitForCellConvergence`).
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 50 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// One application row (Go: `harness.TypedContentionRow`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    name: String,
    phone: String,
    score: i64,
}

/// Read the contended row on one node (Go: `Cluster.TypedContentionRead`).
fn read_row(db: &Database) -> Result<Row, String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name, phone, score FROM contention WHERE id = ?")?;
        let mut rows = stmt.query([ROW_ID])?;
        match rows.next()? {
            Some(row) => Ok(Some(Row {
                name: row.get(0)?,
                phone: row.get(1)?,
                score: row.get(2)?,
            })),
            None => Ok(None),
        }
    })
    .map_err(|e| format!("read row {ROW_ID} failed: {e}"))?
    .ok_or_else(|| format!("row {ROW_ID} missing"))
}

/// Insert the baseline row on one node (Go: `Cluster.TypedContentionInsert`).
fn insert_baseline(db: &Database, row: &Row) {
    let (name, phone, score) = (row.name.clone(), row.phone.clone(), row.score);
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
            (ROW_ID, name, phone, score),
        )?;
        Ok(())
    })
    .expect("baseline insert failed");
    db.flush().expect("flush after baseline insert");
}

/// One TEXT cell update (Go: `Cluster.TypedContentionUpdate` for name/phone).
fn update_text(db: &Database, node: usize, column: &str, value: String) {
    let statement = format!("UPDATE contention SET {column} = ? WHERE id = ?");
    let what = format!("node{node} {column}");
    db.transaction(move |sql| {
        let changed = sql.execute(&statement, (value, ROW_ID))?;
        assert_eq!(changed, 1, "{what} update hit {changed} rows");
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} {column} update failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after {column} update failed: {e}"));
}

/// One INTEGER cell update (Go: `Cluster.TypedContentionUpdate` for score).
fn update_score(db: &Database, node: usize, value: i64) {
    db.transaction(move |sql| {
        let changed = sql.execute(
            "UPDATE contention SET score = ? WHERE id = ?",
            (value, ROW_ID),
        )?;
        assert_eq!(changed, 1, "node{node} score update hit {changed} rows");
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} score update failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after score update failed: {e}"));
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

/// Wait until every node reports `want` live peers (Go: `waitForPeers`).
fn wait_for_peers(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        (0..cluster.len()).all(|i| connected_peers(cluster, i) == want)
    });
    assert!(
        converged,
        "peer mesh failed to reach {want} peers/node within {timeout:?}"
    );
}

/// Wait until every node holds exactly `want` (Go: `waitForState`).
fn wait_for_state(cluster: &Cluster, want: &Row, timeout: Duration) {
    let mut last = vec![String::new(); cluster.len()];
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        for i in 0..cluster.len() {
            match read_row(cluster.node(i)) {
                Ok(got) if got == *want => {
                    last[i] = format!("{got:?}");
                }
                other => {
                    last[i] = format!("{other:?}");
                    return false;
                }
            }
        }
        true
    });
    assert!(
        converged,
        "timed out waiting for {want:?} on every node; last={last:?}"
    );
}

/// Wait until every node agrees on the shared-cell value while the other
/// cells hold their disjoint values; returns the winner (Go:
/// `waitForCellConvergence`).
fn wait_for_cell_convergence(
    cluster: &Cluster,
    want_phone: &str,
    want_score: i64,
    timeout: Duration,
) -> String {
    let mut last = vec![String::new(); cluster.len()];
    let mut winner = String::new();
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        let mut current: Option<String> = None;
        for i in 0..cluster.len() {
            match read_row(cluster.node(i)) {
                Ok(row) if row.phone == want_phone && row.score == want_score => {
                    last[i] = format!(
                        "name={:?} phone={:?} score={}",
                        row.name, row.phone, row.score
                    );
                    match &current {
                        None => current = Some(row.name),
                        Some(prev) if *prev == row.name => {}
                        _ => return false,
                    }
                }
                other => {
                    last[i] = format!("{other:?}");
                    return false;
                }
            }
        }
        winner = current.unwrap_or_default();
        true
    });
    assert!(
        converged,
        "same-cell contention did not converge across {} nodes; last={last:?}",
        cluster.len()
    );
    winner
}

#[test]
fn multi_process_disjoint_and_shared_cell_contention() {
    let cluster = Cluster::new(ClusterOptions {
        name: "crdt-contention".into(),
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
        wait_for_peers(&cluster, 2, PEERS_TIMEOUT);

        // Same schema on every node (Go: TypedRecords/TypedContention tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)",
                ))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Baseline row from node 1, replicated everywhere (Go: insert +
        // first `waitForState`).
        let baseline = Row {
            name: "baseline".into(),
            phone: "baseline".into(),
            score: 0,
        };
        insert_baseline(cluster.node(0), &baseline);
        wait_for_state(&cluster, &baseline, STATE_TIMEOUT);

        // Concurrent edits to separate cells must all survive regardless of
        // arrival order at other nodes (Go: `disjoint` + goroutines).
        let start = Arc::new(Barrier::new(cluster.len()));
        std::thread::scope(|s| {
            for node_idx in 0..cluster.len() {
                let db = cluster.node(node_idx).clone();
                let start = start.clone();
                s.spawn(move || {
                    start.wait();
                    let label = node_idx + 1;
                    match node_idx {
                        0 => update_text(&db, label, "name", "node1-name".into()),
                        1 => update_text(&db, label, "phone", "node2-phone".into()),
                        _ => update_score(&db, label, 42),
                    }
                });
            }
        });
        let disjoint = Row {
            name: "node1-name".into(),
            phone: "node2-phone".into(),
            score: 42,
        };
        wait_for_state(&cluster, &disjoint, STATE_TIMEOUT);

        // All three nodes now contend on the same name cell. The winning
        // value is intentionally not preselected; the LWW version must
        // resolve to the same application-visible result on every node
        // (Go: `updatesPerNode` loop + goroutines).
        let start = Arc::new(Barrier::new(cluster.len()));
        std::thread::scope(|s| {
            for node_idx in 0..cluster.len() {
                let db = cluster.node(node_idx).clone();
                let start = start.clone();
                s.spawn(move || {
                    start.wait();
                    let label = node_idx + 1;
                    for seq in 0..UPDATES_PER_NODE {
                        let value = format!("node{label}-cell-{seq:03}");
                        update_text(&db, label, "name", value);
                    }
                });
            }
        });

        let winner = wait_for_cell_convergence(&cluster, "node2-phone", 42, CONVERGENCE_TIMEOUT);
        assert!(
            !winner.is_empty(),
            "same-cell contention resolved to an empty name"
        );
        assert!(
            winner.starts_with("node1-cell-")
                || winner.starts_with("node2-cell-")
                || winner.starts_with("node3-cell-"),
            "same-cell winner {winner:?} did not come from the concurrent write set"
        );
        let first = read_row(cluster.node(0)).expect("read node1 final row");
        for i in 1..cluster.len() {
            let other = read_row(cluster.node(i)).expect("read node final row");
            assert!(
                other == first,
                "logical row differs: node1={first:?} node{}={other:?}",
                i + 1
            );
        }
        eprintln!(
            "three node cell conflict converged to {winner:?} after {UPDATES_PER_NODE} updates per node"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("contention-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
