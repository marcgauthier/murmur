//! Rust port of the Go `churn-retirement` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/churn-retirement/`
//! (`churn_retirement_test.go`). Three nodes mesh; node3 is excluded on the
//! survivors. Sessions to node3 must drop, its writes must stop arriving, it
//! must release its GC retention pin, the exclusion must survive a survivor
//! restart with no rediscovery re-mesh, and explicit re-adding must heal
//! replication. Plain SQL replaces the Go typed-records HTTP API, and the
//! Prometheus legs map to `status()` / `retention_status()` (see the
//! scenario README.md for the full leg mapping and the one ordering
//! deviation: the retention-release leg runs after re-add heals, because
//! Rust splits reversible exclusion (`remove_peer`) from permanent
//! retirement (`retire_peer`)).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Convergence poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Convergence deadlines (Go: 60 s under a saturated parallel box).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Session-drop / peer-count deadlines (Go: 60 s).
const PEERS_TIMEOUT: Duration = Duration::from_secs(60);
/// Retention-entry appearance deadline after baseline convergence.
const GATING_TIMEOUT: Duration = Duration::from_secs(30);
/// Negative-assertion hold after the isolation write (Go: 5 s).
const ISOLATION_HOLD: Duration = Duration::from_secs(5);

/// Plain-SQL stand-in for the Go typed-records fixture table.
const DDL: &str = "CREATE TABLE records(name TEXT NOT NULL PRIMARY KEY)";

/// Baseline rows (Go: `a-0` .. `a-4` inserted on node 0).
fn baseline_names() -> Vec<String> {
    (0..5).map(|i| format!("a-{i}")).collect()
}

/// All row names on one node, sorted (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> Vec<String> {
    let mut out: Vec<String> = db
        .read(|sql| {
            let mut stmt = sql.prepare("SELECT name FROM records")?;
            let mapped = stmt.query_map([], |row| row.get::<_, String>(0))?;
            let mut out = Vec::new();
            for row in mapped {
                out.push(row?);
            }
            Ok(out)
        })
        .expect("read record names");
    out.sort();
    out
}

/// Insert one row (Go: `Cluster.TypedInsert`).
fn insert(cluster: &Cluster, node: usize, name: &str) {
    let value = name.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            sql.execute("INSERT INTO records(name) VALUES(?)", [value])?;
            Ok(())
        })
        .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    cluster.node(node).flush().expect("flush after insert");
}

/// Wait until every node holds exactly `want` (Go: `waitConverged`: equal
/// counts and equal joined-name digests on all nodes).
fn wait_converged(cluster: &Cluster, want: &[String], what: &str) {
    let converged = wait_until(CONVERGE_TIMEOUT, POLL, || {
        (0..cluster.len()).all(|i| names(cluster.node(i)) == want)
    });
    assert!(
        converged,
        "nodes did not converge on {want:?} within {CONVERGE_TIMEOUT:?} ({what})"
    );
}

/// Live session count (Go: `spedsql_peer_count`).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication enabled")
        .status()
        .peers
}

/// True while a live session to `id` exists (Go: `spedsql_peer_connected`
/// with the `peer` label).
fn peer_connected(cluster: &Cluster, node: usize, id: [u8; 16]) -> bool {
    cluster
        .node(node)
        .replication()
        .expect("replication enabled")
        .status()
        .peers_diagnostics
        .iter()
        .any(|d| d.node_id == id && d.connected)
}

/// Peers currently pinning history retention (Go: `spedsql_gating_members`).
fn gating_members(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .retention_status()
        .expect("retention status")
        .peers
        .iter()
        .filter(|p| !p.retired)
        .count()
}

/// True once `id`'s retention record is marked retired.
fn is_retired(cluster: &Cluster, node: usize, id: [u8; 16]) -> bool {
    cluster
        .node(node)
        .retention_status()
        .expect("retention status")
        .peers
        .iter()
        .any(|p| p.node == id && p.retired)
}

#[test]
fn exclude_persists_releases_retention_and_readd_heals() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "churn-retirement".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let id2 = cluster.node_id(2);
        let baseline = baseline_names();

        // Same schema on every node (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(DDL))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Baseline: five rows from node 0 converge on all three nodes.
        for name in &baseline {
            insert(&cluster, 0, name);
        }
        wait_converged(&cluster, &baseline, "baseline");

        // Both peers must be pinning retention before the exclusion leg
        // (Go: `gatingBefore` baseline read).
        let id1 = cluster.node_id(1);
        let gating_seen = wait_until(GATING_TIMEOUT, POLL, || {
            let peers = cluster
                .node(0)
                .retention_status()
                .expect("retention status")
                .peers;
            [id1, id2]
                .iter()
                .all(|id| peers.iter().any(|p| p.node == *id && !p.retired))
        });
        assert!(
            gating_seen,
            "node1/node3 retention pins never appeared on node1 within {GATING_TIMEOUT:?}"
        );
        let gating_before = gating_members(&cluster, 0);

        // Exclude node3 on both survivors (Go: `RemovePeer`, reversible leg).
        cluster.remove_peer(0, 2).expect("exclude node3 on node1");
        cluster.remove_peer(1, 2).expect("exclude node3 on node2");

        // Sessions to node3 drop on both survivors.
        let dropped = wait_until(PEERS_TIMEOUT, POLL, || {
            !peer_connected(&cluster, 0, id2) && !peer_connected(&cluster, 1, id2)
        });
        assert!(
            dropped,
            "sessions to node3 did not drop within {PEERS_TIMEOUT:?}"
        );
        // Survivors each see exactly one peer (each other).
        for i in [0, 1] {
            let one = wait_until(PEERS_TIMEOUT, POLL, || peer_count(&cluster, i) == 1);
            assert!(
                one,
                "node{} peers did not settle to 1 within {PEERS_TIMEOUT:?} after exclusion",
                i + 1
            );
        }

        // Node3's writes no longer arrive on the survivors.
        insert(&cluster, 2, "isolated");
        std::thread::sleep(ISOLATION_HOLD);
        for i in [0, 1] {
            let got = names(cluster.node(i));
            assert_eq!(
                got, baseline,
                "node{} names after isolation hold: {got:?}; want survivors-only {baseline:?}",
                i + 1
            );
        }

        // Exclusion survives a survivor restart with no rediscovery re-mesh.
        // `start_node` reopens under the unlock watchdog; the survivor must
        // be re-meshed to its fellow survivor (fresh ephemeral port), and
        // only to it.
        cluster.stop_node(0);
        cluster.start_node(0).expect("restart survivor node1");
        cluster.add_peer(0, 1).expect("re-mesh restarted node1");
        cluster.add_peer(1, 0).expect("re-mesh node2 to node1");
        let remeshed = wait_until(PEERS_TIMEOUT, POLL, || peer_count(&cluster, 0) == 1);
        assert!(
            remeshed,
            "restarted node1 did not re-mesh its fellow survivor within {PEERS_TIMEOUT:?}"
        );
        assert!(
            !peer_connected(&cluster, 0, id2),
            "node1 re-meshed excluded node3 after restart"
        );
        assert_eq!(
            peer_count(&cluster, 0),
            1,
            "node1 peers after restart: want 1 (exclusion lost?)"
        );
        assert_eq!(
            names(cluster.node(0)),
            baseline,
            "restarted node1 lost survivor rows"
        );

        // Re-adding heals replication (removal was not destructive).
        cluster.add_peer(0, 2).expect("re-add node3 on node1");
        cluster.add_peer(1, 2).expect("re-add node3 on node2");
        let mut healed = baseline.clone();
        healed.push("isolated".to_string());
        healed.sort();
        wait_converged(&cluster, &healed, "post re-add heal");

        // Retention release (Go: `spedsql_gating_members` drop after
        // `RemovePeer`). Rust splits reversible exclusion from permanent
        // retirement, so this leg runs after the heal: a retired peer can
        // never rejoin (session setup rejects retired peers).
        cluster.node(0).retire_peer(id2).expect("retire node3 on node1");
        cluster.node(1).retire_peer(id2).expect("retire node3 on node2");
        for i in [0, 1] {
            let gating_after = gating_members(&cluster, i);
            assert!(
                gating_after < gating_before,
                "node{} gating members = {gating_after}, want below baseline {gating_before}",
                i + 1
            );
            assert!(
                is_retired(&cluster, i, id2),
                "node{} has no retired retention record for node3",
                i + 1
            );
            // Retirement also excludes: sessions to node3 drop again.
            let redropped = wait_until(PEERS_TIMEOUT, POLL, || {
                !peer_connected(&cluster, i, id2) && peer_count(&cluster, i) == 1
            });
            assert!(
                redropped,
                "node{} did not drop retired node3 within {PEERS_TIMEOUT:?}",
                i + 1
            );
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("churn-retirement");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
