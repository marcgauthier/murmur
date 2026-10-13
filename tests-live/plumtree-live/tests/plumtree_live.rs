//! Rust port of the Go `plumtree-live` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/plumtree-live/`
//! (`plumtree_live_test.go`). `plumtree_mesh_converges` requires end-to-end
//! convergence of a three-node all-Plumtree mesh; `mixed_mode_refuses_gossip_peer`
//! requires a gossip-mode node meshed with Plumtree peers to stay isolated in
//! both directions while handshake refusals are recorded and the Plumtree pair
//! converges. Plain SQL replaces the Go typed-records HTTP API (see the
//! scenario README.md for the deviations).

use std::collections::HashMap;
use std::time::Duration;

use murmur_live_harness::murmur_replication::Dissemination;
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Rows inserted by the origin node (Go: 10 per test).
const ROWS: usize = 10;
/// All-Plumtree convergence deadline (Go: 60 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Mixed-mode pair-convergence deadline (Go: 30 s).
const MIXED_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Leak window after mixed-mode convergence (Go: 5 s).
const LEAK_WINDOW: Duration = Duration::from_secs(5);

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

/// Same `records` table on every node (Go: TypedRecords fixture tables).
fn migrate_all(cluster: &Cluster) {
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .migrate(Migration::new(
                "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            ))
            .unwrap_or_else(|e| panic!("node {i} migrate records table: {e}"));
        cluster
            .node(i)
            .flush()
            .unwrap_or_else(|e| panic!("node {i} flush schema: {e}"));
    }
}

fn insert_names(cluster: &Cluster, idx: usize, prefix: &str, count: usize) {
    for row in 0..count {
        let name = format!("{prefix}-{row}");
        let id = row as i64;
        cluster
            .node(idx)
            .transaction(move |sql| {
                sql.execute(
                    "INSERT INTO records(id, name) VALUES(?, ?)",
                    (id, name),
                )?;
                Ok(())
            })
            .unwrap_or_else(|e| panic!("node {idx} insert failed: {e}"));
    }
    cluster
        .node(idx)
        .flush()
        .unwrap_or_else(|e| panic!("node {idx} flush after inserts: {e}"));
}

#[test]
fn plumtree_mesh_converges() {
    let cluster = Cluster::new(ClusterOptions {
        name: "plumtree-live".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            dissemination: Some(Dissemination::Plumtree),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster);
        insert_names(&cluster, 0, "plum", ROWS);

        let mut want = Vec::with_capacity(ROWS);
        for row in 0..ROWS {
            want.push(format!("plum-{row}"));
        }
        let converged = wait_until(CONVERGE_TIMEOUT, POLL_INTERVAL, || {
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
            "three Plumtree nodes did not converge on {ROWS} rows within {CONVERGE_TIMEOUT:?}"
        );
        eprintln!("Three Plumtree nodes converged on {ROWS} rows");
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

#[test]
fn mixed_mode_refuses_gossip_peer() {
    let mut dissemination_by_node = HashMap::new();
    dissemination_by_node.insert(2, Dissemination::Gossip);
    let cluster = Cluster::new(ClusterOptions {
        name: "plumtree-mixed".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            dissemination: Some(Dissemination::Plumtree),
            ..Default::default()
        }),
        dissemination_by_node,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster);
        insert_names(&cluster, 0, "mixed-plum", ROWS);
        insert_names(&cluster, 2, "gossip-only", 1);

        // The Plumtree pair converges; the gossip node stays isolated.
        let pair_converged = wait_until(MIXED_TIMEOUT, POLL_INTERVAL, || {
            let n0 = names(cluster.node(0)).map(|n| n.len()).unwrap_or(usize::MAX);
            let n1 = names(cluster.node(1)).map(|n| n.len()).unwrap_or(usize::MAX);
            n0 == ROWS && n1 == ROWS
        });
        assert!(
            pair_converged,
            "Plumtree pair did not converge on {ROWS} rows within {MIXED_TIMEOUT:?}"
        );
        std::thread::sleep(LEAK_WINDOW); // let any leak arrive
        for (i, want) in [ROWS, ROWS, 1].into_iter().enumerate() {
            let got = names(cluster.node(i))
                .unwrap_or_else(|e| panic!("node {i} read names: {e}"));
            assert_eq!(
                got.len(),
                want,
                "node {i} count = {}, want {want} (mode isolation broken?)",
                got.len()
            );
        }

        // Refusal evidence (Go: `spedsql_repl_handshake_capability_refusals_total >= 1`).
        // The Rust engine fails closed with `auth: incompatible hello/welcome`
        // recorded in `status().last_error` on both dial and accept sides.
        let mut refusals = 0;
        for i in 0..cluster.len() {
            let status = cluster
                .node(i)
                .replication()
                .unwrap_or_else(|| panic!("node {i} has no replication"))
                .status();
            if status
                .last_error
                .as_deref()
                .is_some_and(|e| e.contains("incompatible"))
            {
                refusals += 1;
            }
            eprintln!(
                "node{i}: peers={} last_error={:?}",
                status.peers, status.last_error
            );
        }
        assert!(
            refusals >= 1,
            "no handshake incompatibility refusals recorded (negotiation silent?)"
        );
        eprintln!("mode refusals recorded on {refusals} node(s)");

        // Session-level isolation: the gossip node never holds a session,
        // while the converged Plumtree pair holds exactly each other.
        let gossip_peers = cluster
            .node(2)
            .replication()
            .expect("node 2 replication")
            .status()
            .peers;
        assert_eq!(
            gossip_peers, 0,
            "gossip node holds {gossip_peers} sessions, want 0 (mode isolation broken?)"
        );
        for i in 0..2 {
            let peers = cluster
                .node(i)
                .replication()
                .unwrap_or_else(|| panic!("node {i} has no replication"))
                .status()
                .peers;
            assert_eq!(
                peers, 1,
                "plumtree node {i} holds {peers} sessions, want 1 (peer of the gossip node?)"
            );
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("mode-isolation");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
