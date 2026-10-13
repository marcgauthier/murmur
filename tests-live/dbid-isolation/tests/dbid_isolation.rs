//! Rust port of the Go `dbid-isolation` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/dbid-isolation/`
//! (`dbid_isolation_test.go`: `TestCrossDatabasePeersNeverConnect`). Two
//! independent 2-node clusters with distinct DBIDs and byte-identical
//! schema share CA trust (so TLS cannot be the refusal cause), then are
//! cross-peered in both directions: every cross edge must stay
//! disconnected for the `MURMUR_DBID_ISO_WINDOW_S` window (minimum 10 s),
//! markers must never cross, identity refusals must be observed on all
//! nodes, and both clusters must stay internally healthy. Plain SQL
//! replaces the Go typed-records HTTP API, QUIC session state replaces
//! `connected_peers`, and the accept-side `last_error` (`Auth("incompatible
//! hello")`) replaces the identity-refusal counters (see the scenario
//! README.md for the deviations).

use std::time::{Duration, Instant};

use murmur_live_harness::murmur_replication::hex;
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};

/// Distinct database ids (Go mints random DBIDs and asserts they differ;
/// fixed values keep the same property deterministically).
const DBID_A: [u8; 16] = [0xA5; 16];
const DBID_B: [u8; 16] = [0xB6; 16];
/// Distinct node ids across both clusters so no cross-cluster identity can
/// alias (allow-nodes precedent for pre-open overlays).
const NODE_IDS_A: [[u8; 16]; 2] = [[0xA1; 16], [0xA2; 16]];
const NODE_IDS_B: [[u8; 16]; 2] = [[0xB1; 16], [0xB2; 16]];
/// Per-cluster markers (Go names, via plain SQL).
const MARKER_A: &str = "cluster-a-marker";
const MARKER_B: &str = "cluster-b-marker";
/// Byte-identical schema on all four nodes, so schema agreement cannot be
/// the refusal cause (Go: identical structured Schema).
const SCHEMA_SQL: &str = "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
/// Isolation-window poll interval (Go: 500 ms).
const POLL: Duration = Duration::from_millis(500);
/// Internal-convergence deadline (Go: 60 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Internal-peer deadline (Go: 30 s).
const PEER_TIMEOUT: Duration = Duration::from_secs(30);
/// Identity-refusal grace wait after the window (Go: 30 s
/// `waitMetricAdvanced`).
const REFUSAL_TIMEOUT: Duration = Duration::from_secs(30);

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

fn insert_marker(db: &Database, id: i64, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO records(id, name) VALUES(?, ?)",
            (id, name),
        )?;
        Ok(())
    })
    .expect("insert marker");
    db.flush().expect("flush marker");
}

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
/// handshakes fail the DBID check never enter the session table, so
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

/// Identity-refusal probe (Go: `spedsql_repl_handshake_identity_refusals_total`
/// advancing). The accepter records `Auth("incompatible hello")` in
/// `last_error` for every cross-DBID dial, and the 5 s seed-redial tick
/// recurs those dials for as long as the cross edge is configured.
fn identity_refused(cluster: &Cluster, node: usize) -> bool {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .last_error
        .as_deref()
        .map_or(false, |e| e.contains("incompatible"))
}

fn wait_converged(cluster: &Cluster, want: &str, what: &str) {
    let want = vec![want.to_owned()];
    let ok = wait_until(CONVERGE_TIMEOUT, Duration::from_millis(100), || {
        (0..cluster.len()).all(|i| names(cluster.node(i)) == want)
    });
    assert!(
        ok,
        "{what}: nodes did not converge on {want:?} within {CONVERGE_TIMEOUT:?}"
    );
}

fn wait_peer_connected(cluster: &Cluster, node: usize, peer_id: [u8; 16], what: &str) {
    let ok = wait_until(PEER_TIMEOUT, Duration::from_millis(200), || {
        peer_connected(cluster, node, peer_id)
    });
    assert!(
        ok,
        "{what}: peer {} never connected within {PEER_TIMEOUT:?}",
        hex(&peer_id)
    );
}

fn cross_add_peer(from: &Cluster, from_idx: usize, to: &Cluster, to_idx: usize) {
    let addr = to.node_info(to_idx).repl_addr;
    from.node(from_idx)
        .replication()
        .expect("replication handle")
        .add_peer(addr)
        .expect("cross add_peer");
}

fn cross_remove_peer(from: &Cluster, from_idx: usize, to: &Cluster, to_idx: usize) {
    let id = to.node_id(to_idx);
    from.node(from_idx)
        .replication()
        .expect("replication handle")
        .remove_peer(id)
        .expect("cross remove_peer");
}

/// Fail fast if any cross-cluster session exists: every cross entry must
/// be absent or `Connected=false`, and no node may report more live
/// sessions than its one internal peer (Go: `assertNoCrossConnection`).
fn assert_no_cross_connection(a: &Cluster, b: &Cluster) {
    let a_ids = [a.node_id(0), a.node_id(1)];
    let b_ids = [b.node_id(0), b.node_id(1)];
    for i in 0..a.len() {
        for id in &b_ids {
            assert!(
                !peer_connected(a, i, *id),
                "CROSS CONNECTED: A node{} reports cross peer {} Connected=true",
                i + 1,
                hex(id)
            );
        }
        let got = peer_count(a, i);
        assert!(
            got <= 1,
            "A node{} connected_peers={got}, want <=1 (internal only)",
            i + 1
        );
    }
    for i in 0..b.len() {
        for id in &a_ids {
            assert!(
                !peer_connected(b, i, *id),
                "CROSS CONNECTED: B node{} reports cross peer {} Connected=true",
                i + 1,
                hex(id)
            );
        }
        let got = peer_count(b, i);
        assert!(
            got <= 1,
            "B node{} connected_peers={got}, want <=1 (internal only)",
            i + 1
        );
    }
}

fn assert_markers_isolated(a: &Cluster, b: &Cluster) {
    for i in 0..a.len() {
        assert_eq!(
            names(a.node(i)),
            vec![MARKER_A.to_owned()],
            "A node{} holds foreign or missing rows",
            i + 1
        );
    }
    for i in 0..b.len() {
        assert_eq!(
            names(b.node(i)),
            vec![MARKER_B.to_owned()],
            "B node{} holds foreign or missing rows",
            i + 1
        );
    }
}

#[test]
fn cross_database_peers_never_connect() {
    let window = env_seconds("MURMUR_DBID_ISO_WINDOW_S", 10);
    assert!(
        window >= Duration::from_secs(10),
        "isolation window {window:?} below the 10s minimum"
    );

    let mut cluster_a = Cluster::new(ClusterOptions {
        name: "dbid-iso-a".into(),
        num_nodes: 2,
        database_id: Some(DBID_A),
        node_ids: NODE_IDS_A.to_vec(),
        ..Default::default()
    })
    .expect("cluster A setup");
    let mut cluster_b = Cluster::new(ClusterOptions {
        name: "dbid-iso-b".into(),
        num_nodes: 2,
        database_id: Some(DBID_B),
        node_ids: NODE_IDS_B.to_vec(),
        ..Default::default()
    })
    .expect("cluster B setup");
    assert_ne!(
        cluster_a.database_id(),
        cluster_b.database_id(),
        "clusters share a DBID; isolation test is vacuous"
    );

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Share CA trust so cross dials pass TLS and reach the DBID check
        // (Go `crossTrustCAs`: append PEM + restart all nodes).
        let ca_a = cluster_a.ca_der();
        let ca_b = cluster_b.ca_der();
        cluster_a.add_trust_root(ca_b);
        cluster_b.add_trust_root(ca_a);
        for i in 0..cluster_a.len() {
            cluster_a.stop_node(i);
        }
        for i in 0..cluster_b.len() {
            cluster_b.stop_node(i);
        }
        for i in 0..cluster_a.len() {
            cluster_a.start_node(i).expect("restart A node");
        }
        for i in 0..cluster_b.len() {
            cluster_b.start_node(i).expect("restart B node");
        }
        cluster_a.mesh().expect("re-mesh A");
        cluster_b.mesh().expect("re-mesh B");

        for i in 0..cluster_a.len() {
            cluster_a
                .node(i)
                .migrate(Migration::new(SCHEMA_SQL))
                .expect("migrate A node");
            cluster_a.node(i).flush().expect("flush A schema");
        }
        for i in 0..cluster_b.len() {
            cluster_b
                .node(i)
                .migrate(Migration::new(SCHEMA_SQL))
                .expect("migrate B node");
            cluster_b.node(i).flush().expect("flush B schema");
        }

        // Positive controls: each cluster converges internally.
        insert_marker(cluster_a.node(0), 1, MARKER_A);
        insert_marker(cluster_b.node(0), 2, MARKER_B);
        wait_converged(&cluster_a, MARKER_A, "cluster A");
        wait_converged(&cluster_b, MARKER_B, "cluster B");
        wait_peer_connected(&cluster_a, 0, cluster_a.node_id(1), "cluster A internal");
        wait_peer_connected(&cluster_b, 0, cluster_b.node_id(1), "cluster B internal");

        // Cross-AddPeer in both directions (full bipartite).
        for i in 0..cluster_a.len() {
            for j in 0..cluster_b.len() {
                cross_add_peer(&cluster_a, i, &cluster_b, j);
                cross_add_peer(&cluster_b, j, &cluster_a, i);
            }
        }

        // Isolation window: cross peers never connect, markers never
        // cross; latch identity refusals throughout.
        let mut refused_a = [false; 2];
        let mut refused_b = [false; 2];
        let window_end = Instant::now() + window;
        while Instant::now() < window_end {
            assert_no_cross_connection(&cluster_a, &cluster_b);
            assert_markers_isolated(&cluster_a, &cluster_b);
            for i in 0..2 {
                refused_a[i] |= identity_refused(&cluster_a, i);
                refused_b[i] |= identity_refused(&cluster_b, i);
            }
            std::thread::sleep(POLL);
        }
        assert_no_cross_connection(&cluster_a, &cluster_b);
        assert_markers_isolated(&cluster_a, &cluster_b);

        // The refusal cause must be identity (DBID) on every node. The
        // seed-redial tick recurs cross dials on its own; this grace wait
        // mirrors Go's 30 s `waitMetricAdvanced`.
        let ok = wait_until(REFUSAL_TIMEOUT, POLL, || {
            for i in 0..2 {
                refused_a[i] |= identity_refused(&cluster_a, i);
                refused_b[i] |= identity_refused(&cluster_b, i);
            }
            refused_a.iter().all(|r| *r) && refused_b.iter().all(|r| *r)
        });
        assert!(
            ok,
            "identity refusals did not advance on every node within {REFUSAL_TIMEOUT:?} \
             (A={refused_a:?} B={refused_b:?})"
        );

        // Cleanup: remove every cross edge, then re-verify internal health.
        for i in 0..cluster_a.len() {
            for j in 0..cluster_b.len() {
                cross_remove_peer(&cluster_a, i, &cluster_b, j);
                cross_remove_peer(&cluster_b, j, &cluster_a, i);
            }
        }
        wait_converged(&cluster_a, MARKER_A, "cluster A after cleanup");
        wait_converged(&cluster_b, MARKER_B, "cluster B after cleanup");
        assert_no_cross_connection(&cluster_a, &cluster_b);
    }))
    .is_err();
    if failed {
        cluster_a.dump_forensics("dbid-isolation-a");
        cluster_b.dump_forensics("dbid-isolation-b");
    }
    cluster_a.finish(failed);
    cluster_b.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
