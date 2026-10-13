//! Rust port of the Go `allow-nodes` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/allow-nodes/`
//! (`allow_nodes_test.go`: `TestAllowedNodeIDsAcrossProcesses`). Node2's
//! identity allow-list accepts only node1; node1 and node3 accept every
//! identity. All three nodes write concurrently for
//! `MURMUR_ALLOW_NODES_WRITE_SECONDS` (default 180 s, Go parity) while the
//! unauthorized node3-to-node2 session must stay absent; afterwards every
//! node must hold all rows with identical ordered digests (node3's rows
//! reach node2 through node1). Plain SQL replaces the Go typed-records
//! HTTP API, and QUIC session state replaces `connected_peers` (see the
//! scenario README.md for the deviations).

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};

/// Fixed distinct node ids so the allow-list overlay can be built before
/// the cluster opens (corrupt-snapshot precedent; Go mints random ids).
const NODE_IDS: [[u8; 16]; 3] = [[0xA1; 16], [0xB2; 16], [0xC3; 16]];
/// Node index whose allow-list accepts only node 0
/// (Go: `AllowedPeersByNode: map[int][]NodeID{1: {node1}}`).
const RESTRICTED: usize = 1;
/// Per-writer insert cadence (Go: 100 ms ticker).
const WRITE_INTERVAL: Duration = Duration::from_millis(100);
/// Initial peer-topology deadline (Go: 20 s).
const INITIAL_PEERS_TIMEOUT: Duration = Duration::from_secs(20);
/// Convergence deadline after the write window (Go: 45 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(45);
/// Convergence poll interval (Go: 100 ms).
const POLL: Duration = Duration::from_millis(100);
/// Final topology re-check bound (Go: 2 s).
const FINAL_PEERS_TIMEOUT: Duration = Duration::from_secs(2);

/// Live session count (Go: `connectedPeers` from `/v1/status`).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .peers
}

/// Direct-session probe (Go: `debug/peers` `Connected`). Peers whose
/// handshakes fail the allow-list never enter the session table, so
/// absence also reads as not connected.
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

/// Wait until every node's live session count matches `want`
/// (Go: `waitForPeers`, 50 ms poll).
fn wait_for_peers(cluster: &Cluster, want: [usize; 3], timeout: Duration, what: &str) {
    let ok = wait_until(timeout, Duration::from_millis(50), || {
        (0..cluster.len()).all(|i| peer_count(cluster, i) == want[i])
    });
    if !ok {
        let actual: Vec<usize> = (0..cluster.len()).map(|i| peer_count(cluster, i)).collect();
        panic!("peer topology {actual:?} != want {want:?} within {timeout:?} ({what})");
    }
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

#[test]
fn allowed_node_ids_across_processes() {
    let write_window = env_seconds("MURMUR_ALLOW_NODES_WRITE_SECONDS", 180);
    // Node2 accepts only node1. Node1 and node3 have no identity filter
    // (an empty allow-list accepts every authenticated identity).
    let node_ids = NODE_IDS.to_vec();
    let allowed_peers: HashMap<usize, BTreeSet<[u8; 16]>> =
        HashMap::from([(RESTRICTED, BTreeSet::from([NODE_IDS[0]]))]);
    let cluster = Cluster::new(ClusterOptions {
        name: "allow-nodes".into(),
        num_nodes: 3,
        node_ids,
        allowed_peers,
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

        // Node1 sees both peers; node2 sees only node1; node3 sees only
        // node1 (its dials to node2 fail the allow-list handshake).
        wait_for_peers(&cluster, [2, 1, 1], INITIAL_PEERS_TIMEOUT, "initial mesh");

        // Concurrent per-node writers (Go: one goroutine per node, one
        // insert per tick). Row ids are node-disjoint by construction;
        // names keep the Go `node{n}-write-{seq}` shape.
        let stop = AtomicBool::new(false);
        let written = [
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
        ];
        let first_error = Mutex::new(None::<String>);
        std::thread::scope(|s| {
            for node in 0..cluster.len() {
                let db = cluster.node(node).clone();
                let stop = &stop;
                let written = &written;
                let first_error = &first_error;
                s.spawn(move || {
                    let mut seq: i64 = 0;
                    loop {
                        std::thread::sleep(WRITE_INTERVAL);
                        if stop.load(Ordering::SeqCst) {
                            return;
                        }
                        seq += 1;
                        let name = format!("node{}-write-{seq}", node + 1);
                        let id = ((node as i64 + 1) << 32) | seq;
                        let failed_op = db
                            .transaction(move |sql| {
                                sql.execute(
                                    "INSERT INTO records(id, name) VALUES(?, ?)",
                                    (id, name),
                                )?;
                                Ok(())
                            })
                            .err()
                            .map(|e| e.to_string())
                            .or_else(|| db.flush().err().map(|e| e.to_string()));
                        if let Some(err) = failed_op {
                            let mut guard =
                                first_error.lock().unwrap_or_else(|e| e.into_inner());
                            if guard.is_none() {
                                *guard =
                                    Some(format!("node{} write {seq}: {err}", node + 1));
                            }
                            return;
                        }
                        written[node].fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
            std::thread::sleep(write_window);
            stop.store(true, Ordering::SeqCst);
        });
        if let Some(err) = first_error.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            panic!("{err}");
        }
        let counts = [
            written[0].load(Ordering::SeqCst),
            written[1].load(Ordering::SeqCst),
            written[2].load(Ordering::SeqCst),
        ];
        let want = (counts[0] + counts[1] + counts[2]) as usize;
        assert!(
            want >= 3,
            "too few writes completed during {write_window:?}: {want}"
        );
        eprintln!(
            "wrote node1={} node2={} node3={} rows; node2 allow-list accepts only node1",
            counts[0], counts[1], counts[2]
        );

        // Every node must hold all rows with an identical ordered digest
        // (Go: NUL-joined names, 45 s deadline, 100 ms poll).
        let mut digest = String::new();
        let converged = wait_until(CONVERGE_TIMEOUT, POLL, || {
            digest.clear();
            for i in 0..cluster.len() {
                let got = names(cluster.node(i));
                if got.len() != want {
                    return false;
                }
                let joined = got.join("\x00");
                if digest.is_empty() {
                    digest = joined;
                } else if joined != digest {
                    return false;
                }
            }
            true
        });
        assert!(
            converged,
            "nodes did not converge on {want} rows within {CONVERGE_TIMEOUT:?}"
        );
        digest.clear();
        for i in 0..cluster.len() {
            let got = names(cluster.node(i));
            assert_eq!(
                got.len(),
                want,
                "node{} records={}, want all {want} application writes",
                i + 1,
                got.len()
            );
            let joined = got.join("\x00");
            if digest.is_empty() {
                digest = joined;
            } else {
                assert_eq!(joined, digest, "node{} digest mismatch", i + 1);
            }
        }

        // The restricted node must keep just node1 as a direct peer:
        // node3's rows arrive through node1, but no unauthorized direct
        // session may exist in either direction.
        let id_restricted = cluster.node_id(RESTRICTED);
        let id_third = cluster.node_id(2);
        assert!(
            !peer_connected(&cluster, RESTRICTED, id_third),
            "unauthorized node3-to-node2 session present on node2"
        );
        assert!(
            !peer_connected(&cluster, 2, id_restricted),
            "unauthorized node2-to-node3 session present on node3"
        );
        wait_for_peers(&cluster, [2, 1, 1], FINAL_PEERS_TIMEOUT, "final topology");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("allowlist-or-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
