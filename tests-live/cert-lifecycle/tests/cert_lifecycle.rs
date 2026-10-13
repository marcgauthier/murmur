//! Rust port of the Go `cert-lifecycle` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/cert-lifecycle/`
//! (`cert_lifecycle_test.go`). A node's certificate is swapped for a
//! short-expiry cert minted for the same NodeID; the mesh must stay
//! connected pre-expiry. After NotAfter passes, fresh handshakes must
//! reject the node (no session, `peers` 1->0 on the survivor, markers
//! never cross during the isolation window), with the served cert parsed
//! at assert time to prove it is genuinely expired. Rotating to a fresh
//! cert must fully reconverge the mesh. Plain SQL replaces the Go
//! typed-records HTTP API, and QUIC session state replaces `debug/peers`
//! / `connected_peers` (see the scenario README.md for the deviations).

use std::time::{Duration, Instant, SystemTime};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};
use x509_parser::prelude::*;

/// Convergence poll interval (Go: 100 ms).
const POLL_FAST: Duration = Duration::from_millis(100);
/// Session poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Convergence deadlines (Go: 60 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Session-establishment deadline (Go: 30 s pre-expiry).
const PEER_TIMEOUT: Duration = Duration::from_secs(30);
/// Rejection deadlines (Go: 45 s).
const REJECT_TIMEOUT: Duration = Duration::from_secs(45);
/// Wall-clock skew budget for the minted-vs-served NotAfter check.
const SKEW: Duration = Duration::from_secs(120);

/// Plain-SQL stand-in for the Go typed-records fixture table.
const DDL: &str = "CREATE TABLE records(name TEXT PRIMARY KEY)";

/// Sorted expectation list (Go: `waitConverged` compares joined names).
fn sorted(names: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = names.iter().map(|s| s.to_string()).collect();
    out.sort();
    out
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

/// Wait until every node holds exactly `want` (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: &[String], timeout: Duration, what: &str) {
    let converged = wait_until(timeout, POLL_FAST, || {
        (0..cluster.len()).all(|i| names(cluster.node(i)) == want)
    });
    assert!(
        converged,
        "nodes did not converge on {want:?} within {timeout:?} ({what})"
    );
}

/// Live session count (Go: `connected_peers` from `/v1/status`).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication enabled")
        .status()
        .peers
}

/// True while a live session to `id` exists (Go: `debug/peers`
/// `Connected`). Peers whose handshakes fail never enter the session
/// table, so absence also reads as not connected.
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

fn wait_peer_connected(
    cluster: &Cluster,
    node: usize,
    id: [u8; 16],
    want: bool,
    timeout: Duration,
    what: &str,
) {
    let seen = wait_until(timeout, POLL, || {
        peer_connected(cluster, node, id) == want
    });
    assert!(
        seen,
        "peer {} Connected != {want} on node{} within {timeout:?} ({what})",
        murmur_live_harness::murmur_replication::hex(&id),
        node + 1,
    );
}

fn wait_peer_count(cluster: &Cluster, node: usize, want: usize, timeout: Duration, what: &str) {
    let seen = wait_until(timeout, POLL, || peer_count(cluster, node) == want);
    assert!(
        seen,
        "peers(node{}) != {want} within {timeout:?} ({what})",
        node + 1,
    );
}

/// NotAfter of the leaf certificate node `idx` currently serves (Go:
/// `fetchServedCert` + `parseNotAfter`). The harness rewrites `tls/`
/// from the same material passed to `Database::open`, so the file
/// NotAfter always equals the served NotAfter.
fn served_not_after(cluster: &Cluster, node: usize) -> SystemTime {
    let path = cluster.node_info(node).dir.join("tls").join("node.der");
    let der =
        std::fs::read(&path).unwrap_or_else(|e| panic!("read served leaf {}: {e}", path.display()));
    let (_, cert) = X509Certificate::from_der(&der).expect("parse served leaf DER");
    let stamp = cert.validity().not_after.timestamp();
    assert!(stamp > 0, "served cert NotAfter predates the epoch");
    SystemTime::UNIX_EPOCH + Duration::from_secs(stamp as u64)
}

#[test]
fn cert_expiry_rejects_and_rotation_heals() {
    let ttl = env_seconds("MURMUR_CERT_LIFECYCLE_TTL_S", 25);
    let window = env_seconds("MURMUR_CERT_LIFECYCLE_WINDOW_S", 10);
    if ttl < Duration::from_secs(15) {
        panic!(
            "short-cert TTL {ttl:?} too small to converge pre-expiry; raise MURMUR_CERT_LIFECYCLE_TTL_S"
        );
    }
    let mut cluster = Cluster::new(ClusterOptions {
        name: "cert-lifecycle".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        const A: usize = 0;
        const B: usize = 1;
        let id_a = cluster.node_id(A);
        let id_b = cluster.node_id(B);

        // Same schema on every node (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(DDL))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Phase 0: positive control, healthy mesh converges.
        insert(&cluster, A, "marker-init");
        wait_converged(
            &cluster,
            &sorted(&["marker-init"]),
            CONVERGE_TIMEOUT,
            "phase0 positive control",
        );
        wait_peer_connected(&cluster, A, id_b, true, PEER_TIMEOUT, "phase0 session");
        assert_eq!(peer_count(&cluster, A), 1, "pre-expiry peers(A): want 1");

        // Phase 1: stop B, rotate to a short-expiry cert for the same NodeID.
        cluster.stop_node(B);
        cluster
            .rotate_node_cert(B, Some(ttl))
            .expect("rotate B to short-expiry cert");
        cluster.mesh().expect("re-mesh after rotation");
        eprintln!("B rotated to a short-expiry cert (TTL {ttl:?}) for the same NodeID");

        // Prove B actually serves the short cert (not a stale one): the
        // served leaf's remaining lifetime must fit the minted TTL.
        let now = SystemTime::now();
        let not_after = served_not_after(&cluster, B);
        let remaining = not_after
            .duration_since(now)
            .expect("short cert already expired before the pre-expiry leg");
        assert!(
            remaining <= ttl + SKEW,
            "served cert expires in {remaining:?}, beyond minted TTL {ttl:?} (+{SKEW:?} skew): stale long-lived cert still served?"
        );
        eprintln!("short cert expires at {not_after:?} (in {remaining:?})");
        let pre_deadline = remaining.checked_sub(Duration::from_secs(5)).unwrap_or_default();
        assert!(
            !pre_deadline.is_zero(),
            "no pre-expiry budget left (NotAfter {not_after:?})"
        );

        // Assert connected pre-expiry: session plus a converged marker.
        wait_peer_connected(&cluster, A, id_b, true, pre_deadline, "phase1 pre-expiry session");
        insert(&cluster, A, "marker-pre-expiry");
        wait_converged(
            &cluster,
            &sorted(&["marker-init", "marker-pre-expiry"]),
            pre_deadline,
            "phase1 pre-expiry marker",
        );

        // Phase 2: wait out the TTL, then force fresh handshakes by
        // restarting B. B opens fine with an expired cert (open checks the
        // SAN identity, not validity); its handshakes must fail.
        eprintln!("waiting for cert expiry at {not_after:?}");
        let post_expiry = not_after + Duration::from_secs(2);
        while SystemTime::now() < post_expiry {
            std::thread::sleep(Duration::from_millis(250));
        }
        cluster.stop_node(B);
        cluster
            .start_node(B)
            .expect("restart B with expired cert");
        cluster.mesh().expect("re-mesh after restart");

        // B is rejected: survivor sees no session and peers drops 1->0.
        wait_peer_connected(&cluster, A, id_b, false, REJECT_TIMEOUT, "phase2 rejection");
        wait_peer_count(&cluster, A, 0, REJECT_TIMEOUT, "phase2 peers drop");
        // B's own view agrees.
        wait_peer_connected(&cluster, B, id_a, false, REJECT_TIMEOUT, "phase2 B-side view");

        // Non-vacuity: at assert time, the cert B serves is expired.
        let served_after = served_not_after(&cluster, B);
        assert!(
            served_after < SystemTime::now(),
            "served cert NotAfter = {served_after:?}, want it in the past"
        );
        eprintln!("rejection confirmed while serving expired cert (NotAfter {served_after:?})");

        // A-side marker written post-expiry must never reach B during the
        // isolation window, while A keeps serving it. The dial loop keeps
        // redialing (explicit mesh plus the 5 s bootstrap retick), so a
        // steady zero proves rejection rather than idleness.
        insert(&cluster, A, "marker-post-expiry");
        let window_end = Instant::now() + window;
        loop {
            assert!(
                !names(cluster.node(B)).iter().any(|n| n == "marker-post-expiry"),
                "post-expiry marker reached expired node B"
            );
            assert_eq!(peer_count(&cluster, A), 0, "peers(A) during isolation window: want 0");
            if Instant::now() >= window_end {
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        assert_eq!(names(cluster.node(A)).len(), 3, "A markers after isolation window: want 3");

        // Phase 3: rotate to a fresh cert and assert full reconvergence.
        cluster.stop_node(B);
        cluster
            .rotate_node_cert(B, Some(Duration::from_secs(24 * 3600)))
            .expect("rotate B to fresh cert");
        cluster.mesh().expect("re-mesh after rotation");
        wait_peer_connected(&cluster, A, id_b, true, CONVERGE_TIMEOUT, "phase3 session");
        wait_converged(
            &cluster,
            &sorted(&["marker-init", "marker-pre-expiry", "marker-post-expiry"]),
            CONVERGE_TIMEOUT,
            "phase3 heal",
        );
        // Bidirectional proof: a B-side write converges too.
        insert(&cluster, B, "marker-rotated");
        let healed = sorted(&[
            "marker-init",
            "marker-pre-expiry",
            "marker-post-expiry",
            "marker-rotated",
        ]);
        wait_converged(&cluster, &healed, CONVERGE_TIMEOUT, "phase3 bidirectional");
        assert_eq!(
            names(cluster.node(A)),
            names(cluster.node(B)),
            "post-rotation names differ"
        );
        eprintln!("rotation healed mesh; names={:?}", names(cluster.node(A)));
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("cert-lifecycle");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
