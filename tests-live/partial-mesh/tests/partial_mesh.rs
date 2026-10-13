//! Rust port of the Go `partial-mesh` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/partial-mesh/`
//! (`partial_mesh_test.go`). Two legs:
//! - `chain_peers_converge_via_forwarding`: three nodes peered only as a
//!   chain (node1-node2, node2-node3); writes on node1 must converge on
//!   node3 via forwarding, with no direct node1-node3 session and exact
//!   peer counts {1, 2, 1}.
//! - `dynamic_bootstrap_discovery`: three nodes with node1 as bootstrap
//!   seed discover one another with no explicit `AddPeer` calls, form a
//!   full mesh, and converge exactly on writes from all three nodes.
//! Plain SQL replaces the Go typed-records HTTP API (see README.md).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Convergence poll interval (Go: 200 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Chain convergence deadline (Go: 60 s).
const CHAIN_CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Membership discovery deadline (Go: 30 s).
const MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(30);
/// Membership poll interval (Go: 250 ms).
const MEMBERSHIP_POLL: Duration = Duration::from_millis(250);
/// Bootstrap convergence deadline (Go: 150 s under parallel load).
const BOOTSTRAP_CONVERGE_TIMEOUT: Duration = Duration::from_secs(150);

/// Same schema on every node (Go: TypedRecords fixture tables).
fn migrate_records(cluster: &Cluster) {
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .migrate(Migration::new(
                "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            ))
            .expect("migrate records table");
        cluster.node(i).flush().expect("flush schema");
    }
}

/// Insert one row (Go: `TypedInsert`).
fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) {
    let value = name.to_string();
    let label = name.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            sql.execute(
                "INSERT INTO records(id, name) VALUES(?, ?)",
                (id, value),
            )?;
            Ok(())
        })
        .unwrap_or_else(|e| panic!("node {} insert '{label}' failed: {e}", node + 1));
    cluster.node(node).flush().expect("flush after insert");
}

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM records ORDER BY name")?;
        let mapped = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in mapped {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read record names")
}

/// Wait until every node holds exactly `want` (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: &[String], timeout: Duration, what: &str) {
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        (0..cluster.len()).all(|i| names(cluster.node(i)) == want)
    });
    assert!(
        converged,
        "nodes did not converge on {} records within {timeout:?} ({what})",
        want.len()
    );
}

/// Live session count (Go: `spedsql_peer_count`).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .peers
}

/// Direct-session probe (Go: `spedsql_peer_connected{peer=...} == 1`).
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

#[test]
fn chain_peers_converge_via_forwarding() {
    let cluster = Cluster::new(ClusterOptions {
        name: "partial-mesh-chain".into(),
        num_nodes: 3,
        manual_peers: true,
        // Membership discovery is off for this leg only: the Rust engine
        // auto-dials discovered member addresses, which would self-heal
        // the chain into a full mesh. Transaction push runs over the peer
        // sessions, so forwarding is unaffected (see README.md).
        replication: Some(ReplicationOptions {
            membership: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_records(&cluster);

        // Chain links only; nothing statically connects node1 to node3.
        for (a, b) in [(0usize, 1usize), (1, 2)] {
            cluster.add_peer(a, b).expect("add peer");
            cluster.add_peer(b, a).expect("add peer");
        }

        for i in 0..10i64 {
            insert(&cluster, 0, i, &format!("d-{i}"));
        }
        let want: Vec<String> = (0..10).map(|i| format!("d-{i}")).collect();
        wait_converged(&cluster, &want, CHAIN_CONVERGE_TIMEOUT, "chain forwarding");

        // No direct node1-node3 session exists: convergence arrived via
        // forwarding, and the peer counts are exactly the static links.
        let id0 = cluster.node_id(0);
        let id2 = cluster.node_id(2);
        assert!(
            !peer_connected(&cluster, 0, id2),
            "unexpected direct node1-node3 session (topology changed?)"
        );
        assert!(
            !peer_connected(&cluster, 2, id0),
            "unexpected direct node3-node1 session (topology changed?)"
        );
        for (i, want_peers) in [1usize, 2, 1].into_iter().enumerate() {
            let got = peer_count(&cluster, i);
            assert!(
                got == want_peers,
                "node {} peers = {got}, want {want_peers}",
                i + 1
            );
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("chain-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn dynamic_bootstrap_discovery() {
    // Node 0 acts as bootstrap seed. Nodes 1 and 2 point to node 0's repl
    // address. No static peers are configured; dynamic discovery must form
    // cluster sessions (Go: `BootstrapSeeds: []int{0}`).
    let cluster = Cluster::new(ClusterOptions {
        name: "partial-mesh-bootstrap".into(),
        num_nodes: 3,
        manual_peers: true,
        bootstrap_seeds: vec![0],
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_records(&cluster);

        // Wait for dynamic discovery to find all cluster members without
        // any explicit AddPeer calls (Go: `waitForMembership`, 30 s).
        // Rust `Status.members` excludes self, so 3 nodes means 2 members
        // each; `peers == 2` proves the full mesh formed (see README.md).
        let discovered = wait_until(MEMBERSHIP_TIMEOUT, MEMBERSHIP_POLL, || {
            (0..cluster.len()).all(|i| {
                let status = cluster
                    .node(i)
                    .replication()
                    .expect("replication handle")
                    .status();
                status.members >= cluster.len() - 1 && status.peers == cluster.len() - 1
            })
        });
        assert!(
            discovered,
            "nodes did not discover {} members via gossip within {MEMBERSHIP_TIMEOUT:?}",
            cluster.len()
        );

        // Write 10 rows on node 0 (seed).
        for i in 0..10i64 {
            insert(&cluster, 0, i, &format!("dyn-seed-{i}"));
        }
        // Write 5 rows on node 1 (discovered peer).
        for i in 0..5i64 {
            insert(&cluster, 1, 100 + i, &format!("dyn-node1-{i}"));
        }
        // Write 5 rows on node 2 (discovered peer).
        for i in 0..5i64 {
            insert(&cluster, 2, 200 + i, &format!("dyn-node2-{i}"));
        }

        // All 20 rows must converge across all 3 nodes (Go: 150 s, not
        // 60 s: under a full parallel run the box is slow and exact
        // convergence legitimately takes over a minute; the proof —
        // exact 20-row convergence — is unchanged).
        let mut want: Vec<String> = (0..10).map(|i| format!("dyn-seed-{i}")).collect();
        want.extend((0..5).map(|i| format!("dyn-node1-{i}")));
        want.extend((0..5).map(|i| format!("dyn-node2-{i}")));
        want.sort();
        wait_converged(
            &cluster,
            &want,
            BOOTSTRAP_CONVERGE_TIMEOUT,
            "bootstrap discovery",
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("bootstrap-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
