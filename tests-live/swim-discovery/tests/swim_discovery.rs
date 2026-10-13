//! Rust port of the Go `swim-discovery` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/swim-discovery/`
//! (`swim_discovery_test.go`). Two legs:
//! - `seed_only_discovery_forms_full_mesh`: four nodes start with a single
//!   SWIM seed address and no static peers; a full mesh must form purely via
//!   discovery (membership view plus a live session to every peer), and
//!   writes from every node must converge with identical contents.
//! - `seed_kill_and_rediscovery`: after baseline convergence the seed is
//!   stopped, survivors must observe the death and keep converging writes
//!   while it is down, then the restarted seed rejoins through one live
//!   member and the full mesh reconverges with identical contents.
//! Plain SQL replaces the Go typed-records HTTP API (see README.md).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};

/// Membership/session poll interval (Go: 250 ms).
const TOPO_POLL: Duration = Duration::from_millis(250);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// Convergence deadline (Go: 150 s loaded-box rationale).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(150);

/// Seed-discovery bound (Go: `MURMUR_SWIM_DISCOVERY_SECONDS`, default 60).
fn discovery_timeout() -> Duration {
    env_seconds("MURMUR_SWIM_DISCOVERY_SECONDS", 60)
}

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

/// Wait until every listed node holds exactly `want` (Go: `waitConvergedOn`).
fn wait_converged_on(
    cluster: &Cluster,
    idxs: &[usize],
    want: &[String],
    timeout: Duration,
    what: &str,
) {
    let converged = wait_until(timeout, CONVERGE_POLL, || {
        idxs.iter()
            .all(|&i| names(cluster.node(i)) == want)
    });
    assert!(
        converged,
        "nodes {idxs:?} did not converge on {} records with equal contents within {timeout:?} ({what})",
        want.len()
    );
}

/// Discovered-member count, excluding self (Go: `spedsql_membership_count`
/// minus one; see README.md).
fn member_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .members
}

/// Live-session count (Go: `spedsql_peer_count`).
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

/// Wait until every node sees `want` discovered members (Go: `waitMembership`).
fn wait_membership(cluster: &Cluster, want: usize, timeout: Duration, what: &str) {
    let ok = wait_until(timeout, TOPO_POLL, || {
        (0..cluster.len()).all(|i| member_count(cluster, i) == want)
    });
    assert!(
        ok,
        "nodes did not reach membership {} (excl. self) within {timeout:?} ({what})",
        want + 1
    );
}

/// Every directed pair holds a live session (Go: `sessionMeshComplete`).
fn session_mesh_complete(cluster: &Cluster) -> bool {
    for i in 0..cluster.len() {
        for j in 0..cluster.len() {
            if i != j && !peer_connected(cluster, i, cluster.node_id(j)) {
                return false;
            }
        }
    }
    true
}

/// Wait for a live session on every directed pair (Go: `waitFullSessionMesh`).
fn wait_full_session_mesh(cluster: &Cluster, timeout: Duration, what: &str) {
    let ok = wait_until(timeout, TOPO_POLL, || session_mesh_complete(cluster));
    assert!(
        ok,
        "full session mesh did not form within {timeout:?} ({what})"
    );
}

/// Wait until every survivor's live-session count drops below the full mesh,
/// proving the seed's death was observed before the restart makes
/// rediscovery meaningful (Go: `waitDeathObserved`; see README.md for the
/// session-teardown vs. SWIM-suspect deviation).
fn wait_death_observed(cluster: &Cluster, survivors: &[usize], timeout: Duration) {
    let full = cluster.len() - 1;
    let ok = wait_until(timeout, TOPO_POLL, || {
        survivors
            .iter()
            .all(|&i| peer_count(cluster, i) < full)
    });
    assert!(
        ok,
        "survivors {survivors:?} did not observe seed death within {timeout:?}"
    );
}

#[test]
fn seed_only_discovery_forms_full_mesh() {
    // Single seed (index 0); no static peers anywhere. The harness opens the
    // seed first and gives every other node only the seed's address as its
    // bootstrap; `manual_peers` means zero explicit `add_peer` calls, so the
    // mesh under test is discovered, not configured (Go: `ManualPeers: true,
    // BootstrapSeeds: []int{0}` plus `assertNoStaticPeers`).
    let cluster = Cluster::new(ClusterOptions {
        name: "swim-discovery".into(),
        num_nodes: 4,
        manual_peers: true,
        bootstrap_seeds: vec![0],
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = discovery_timeout();
        migrate_records(&cluster);

        // Full mesh purely via SWIM: every node sees all 4 members ...
        wait_membership(&cluster, 3, deadline, "initial discovery");
        // ... and holds a live replication session to every other node.
        wait_full_session_mesh(&cluster, deadline, "initial discovery");

        // Writes from every node converge with identical contents.
        let rows_per_node = 10i64;
        for i in 0..cluster.len() {
            for r in 0..rows_per_node {
                insert(&cluster, i, i as i64 * rows_per_node + r, &format!("n{i}-r{r}"));
            }
        }
        let mut want: Vec<String> = (0..cluster.len())
            .flat_map(|i| (0..rows_per_node).map(move |r| format!("n{i}-r{r}")))
            .collect();
        want.sort();
        wait_converged_on(
            &cluster,
            &[0, 1, 2, 3],
            &want,
            CONVERGE_TIMEOUT,
            "seed-only mesh",
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("discovery-no-mesh");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn seed_kill_and_rediscovery() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "swim-rediscovery".into(),
        num_nodes: 4,
        manual_peers: true,
        bootstrap_seeds: vec![0],
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = discovery_timeout();
        migrate_records(&cluster);
        wait_membership(&cluster, 3, deadline, "pre-kill discovery");
        wait_full_session_mesh(&cluster, deadline, "pre-kill discovery");

        // Baseline writes converge before the kill (honest-path control).
        for i in 0..8i64 {
            insert(&cluster, 2, i, &format!("base-{i}"));
        }
        let mut want: Vec<String> = (0..8).map(|i| format!("base-{i}")).collect();
        want.sort();
        wait_converged_on(&cluster, &[0, 1, 2, 3], &want, CONVERGE_TIMEOUT, "baseline");

        cluster.stop_node(0);
        wait_death_observed(&cluster, &[1, 2, 3], deadline);

        // Survivors keep writing while the seed is down.
        for i in 0..12i64 {
            insert(&cluster, 1 + (i % 3) as usize, 100 + i, &format!("down-{i}"));
        }
        want.extend((0..12).map(|i| format!("down-{i}")));
        want.sort();
        wait_converged_on(&cluster, &[1, 2, 3], &want, CONVERGE_TIMEOUT, "seed down");

        // The restarted seed started life with an empty bootstrap list, so
        // give it one live member to rejoin through (Go: `patchSeedBootstrap`
        // to node 2's address, then `StartNode`): any live member rejoins the
        // whole cluster via gossip, and discovery rebuilds the rest of the
        // mesh from there.
        cluster.start_node(0).expect("restart seed");
        cluster.add_peer(0, 1).expect("seed rejoins via node 2");

        wait_membership(&cluster, 3, deadline, "rediscovery");
        wait_full_session_mesh(&cluster, deadline, "rediscovery");
        wait_converged_on(&cluster, &[0, 1, 2, 3], &want, CONVERGE_TIMEOUT, "reconverge");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("rediscovery-no-reconverge");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
