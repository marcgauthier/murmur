//! Rust port of the Go `addrpolicy` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/addrpolicy/`
//! (`addrpolicy_test.go`: `TestAddressPolicyAcrossProcesses`). Node1 admits
//! loopback peers (`127.0.0.0/8`); node2's TEST-NET policy
//! (`198.51.100.0/24`) excludes every actual loopback socket address; node3
//! is unrestricted. The admitted pair must replicate while the denied node
//! keeps its write local. Plain SQL replaces the Go typed-records HTTP API,
//! and QUIC session state replaces `connected_peers` (see the scenario
//! README.md for the deviations).

use std::collections::HashMap;
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Peer-topology deadline (Go: 20 s).
const PEER_TIMEOUT: Duration = Duration::from_secs(20);
/// Per-node row-count deadline (Go: 20 s).
const COUNT_TIMEOUT: Duration = Duration::from_secs(20);
/// Convergence poll interval (Go: 50 ms).
const POLL: Duration = Duration::from_millis(50);
/// Post-convergence settle window: several dial/accept retries must not move
/// the denied node's state (Go: 750 ms sleep).
const DENY_SETTLE: Duration = Duration::from_millis(750);

/// Live session count (Go: `connectedPeers` from `/v1/status`).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .peers
}

/// Wait until every node's live session count matches `want`
/// (Go: `waitForPeers`, 50 ms poll).
fn wait_for_peers(cluster: &Cluster, want: [usize; 3]) {
    let ok = wait_until(PEER_TIMEOUT, POLL, || {
        (0..cluster.len()).all(|i| peer_count(cluster, i) == want[i])
    });
    if !ok {
        let actual: Vec<usize> = (0..cluster.len()).map(|i| peer_count(cluster, i)).collect();
        panic!("peer topology {actual:?} != want {want:?} within {PEER_TIMEOUT:?}");
    }
}

/// One replicated row (Go: `Cluster.TypedInsert` + flush).
fn insert(db: &Database, id: i64, name: String) {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO records(id, name) VALUES(?, ?)",
            (id, name),
        )?;
        Ok(())
    })
    .expect("insert record row");
    db.flush().expect("flush insert");
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

/// Wait until `node` holds `want` rows (Go: `waitForCount`, 50 ms poll).
fn wait_for_count(cluster: &Cluster, node: usize, want: usize) {
    let ok = wait_until(COUNT_TIMEOUT, POLL, || {
        names(cluster.node(node)).len() == want
    });
    if !ok {
        let got = names(cluster.node(node));
        panic!(
            "{} record names={got:?}, want {want}",
            cluster.node_info(node).label,
        );
    }
}

#[test]
fn address_policy_across_processes() {
    // Node1 admits loopback peers; node2's TEST-NET policy excludes every
    // actual loopback socket address. Node3 is unrestricted and provides
    // the allowed path with node1.
    let allowed_networks: HashMap<usize, Vec<ipnet::IpNet>> = HashMap::from([
        (
            0,
            vec!["127.0.0.0/8".parse::<ipnet::IpNet>().expect("loopback CIDR")],
        ),
        (
            1,
            vec!["198.51.100.0/24"
                .parse::<ipnet::IpNet>()
                .expect("TEST-NET CIDR")],
        ),
    ]);
    let cluster = Cluster::new(ClusterOptions {
        name: "addrpolicy".into(),
        num_nodes: 3,
        allowed_networks,
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
        // Row ids are node-disjoint by construction; names keep the Go
        // TypedInsert values.
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Let QUIC dial retries settle, then confirm the allowed pair
        // formed a session while the denied node has no admitted path to
        // either peer (Go: `waitForPeers` `[1, 0, 1]`).
        wait_for_peers(&cluster, [1, 0, 1]);

        insert(cluster.node(0), (1 << 32) | 1, "allowed-loopback".into());
        insert(cluster.node(1), (2 << 32) | 1, "denied-testnet".into());
        insert(cluster.node(2), (3 << 32) | 1, "unrestricted-loopback".into());

        wait_for_count(&cluster, 0, 2);
        wait_for_count(&cluster, 2, 2);
        std::thread::sleep(DENY_SETTLE); // several dial/accept retries
        let denied = names(cluster.node(1));
        assert_eq!(
            denied,
            vec!["denied-testnet".to_string()],
            "denied node state changed across network: want only its local row"
        );
        let admitted = names(cluster.node(0));
        assert_eq!(
            admitted.len(),
            2,
            "admitted node did not receive both its local and node3-origin row: names={admitted:?}"
        );

        for i in 0..cluster.len() {
            assert!(
                !cluster.node_info(i).dir.as_os_str().is_empty(),
                "node{} has no isolated directory",
                i + 1
            );
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("address-policy-or-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
