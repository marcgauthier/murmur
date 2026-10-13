//! Rust port of the Go `tail-repair` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/tail-repair/`
//! (`tail_repair_test.go`). A node partitioned mid-write-burst misses a
//! contiguous range of its peers' origin logs, then heals and must repair
//! the missing tail from peer logs WITHOUT the snapshot path: row counts
//! and ordered digests converge everywhere while the healed node's
//! log-path counters prove the range arrived as transactions, not as a
//! snapshot. Plain SQL replaces the Go typed-records HTTP API, and the
//! Go `/metrics` snapshot counters (absent from the Rust product) are
//! replaced by a structural no-snapshot guarantee plus
//! `received/applied_transactions` advancement assertions (see the
//! scenario README.md).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Convergence poll interval (Go: 100 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Baseline convergence deadline (Go: 30 s).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(30);
/// Connected-peers settle deadlines, split and heal (Go: 15 s / 30 s).
const SPLIT_PEERS_TIMEOUT: Duration = Duration::from_secs(15);
const HEAL_PEERS_TIMEOUT: Duration = Duration::from_secs(30);
/// Isolation-probe reach deadline on the survivor (Go: 15 s).
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
/// Negative-check settle margin before asserting the isolate is frozen
/// (Go: 2 s sleep).
const SETTLE_MARGIN: Duration = Duration::from_secs(2);
/// Burst convergence deadline on the survivors (Go: 30 s).
const BURST_TIMEOUT: Duration = Duration::from_secs(30);
/// Full convergence deadline after healing (Go: 60 s).
const HEAL_TIMEOUT: Duration = Duration::from_secs(60);
/// Post-heal write convergence deadline (Go: 30 s).
const POST_HEAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Baseline rows on node 0 (Go: `baseline = 10`, ids `i`, `base-i`).
const BASELINE_ROWS: i64 = 10;
/// Isolation-probe row id (Go: 9000, `probe`).
const PROBE_ID: i64 = 9000;
/// Burst row id base (Go: 10000+i, `burst-i`).
const BURST_ID: i64 = 10000;
/// Post-heal row id on the healed node (Go: 20000, `post-heal`).
const POST_HEAL_ID: i64 = 20000;

/// One marker row: Go's `TypedContentionRow{ID, Name}`.
type MarkerRow = (i64, String);

/// All marker rows on one node (Go: `Cluster.TypedContentionRows`).
fn rows(db: &Database) -> Vec<MarkerRow> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name FROM records")?;
        let mapped = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in mapped {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read marker rows")
}

/// Canonical digest of a row set (Go: `digestRows`, sorted then `%#v`).
fn digest(mut rows: Vec<MarkerRow>) -> String {
    rows.sort();
    format!("{rows:?}")
}

/// Insert one marker row (Go: `insertMarker`).
fn insert_marker(cluster: &Cluster, node: usize, id: i64, name: &str) {
    let name = name.to_string();
    let value = name.clone();
    cluster
        .node(node)
        .transaction(move |sql| {
            sql.execute(
                "INSERT INTO records(id, name) VALUES(?, ?)",
                (id, value),
            )?;
            Ok(())
        })
        .unwrap_or_else(|e| panic!("node {node} insert '{name}' failed: {e}"));
    cluster.node(node).flush().expect("flush after insert");
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
fn wait_row_count(cluster: &Cluster, idx: usize, want: usize, timeout: Duration, what: &str) {
    let ok = wait_until(timeout, POLL_INTERVAL, || {
        rows(cluster.node(idx)).len() == want
    });
    assert!(
        ok,
        "node {idx} typed row count = {}, want {want} within {timeout:?} ({what})",
        rows(cluster.node(idx)).len(),
    );
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration, what: &str) {
    let ok = wait_until(timeout, POLL_INTERVAL, || {
        let mut first = String::new();
        for (i, idx) in (0..cluster.len()).enumerate() {
            let node_rows = rows(cluster.node(idx));
            if node_rows.len() != want {
                return false;
            }
            let d = digest(node_rows);
            if i == 0 {
                first = d;
            } else if d != first {
                return false;
            }
        }
        true
    });
    if !ok {
        for i in 0..cluster.len() {
            let node_rows = rows(cluster.node(i));
            eprintln!(
                "node {i} at timeout: count={} digest={}",
                node_rows.len(),
                digest(node_rows)
            );
        }
    }
    assert!(
        ok,
        "nodes did not converge on {want} rows with equal digests within {timeout:?} ({what})"
    );
}

/// Wait until one node reports exactly `want` replication peers
/// (Go: `waitConnectedPeers` over `/v1/status connected_peers`).
fn wait_connected_peers(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) {
    let ok = wait_until(timeout, POLL_INTERVAL, || {
        cluster
            .node(idx)
            .replication()
            .expect("replication handle")
            .status()
            .peers
            == want
    });
    assert!(
        ok,
        "node {idx} connected peers = {}, want {want} within {timeout:?}",
        cluster
            .node(idx)
            .replication()
            .expect("replication handle")
            .status()
            .peers,
    );
}

/// Log-path activity counters for one node: `(received_transactions,
/// applied_transactions)`. Snapshot application bypasses both counters
/// (see README.md), so advancement across the heal proves the missed
/// range arrived as log transactions.
fn log_counters(cluster: &Cluster, idx: usize) -> (u64, u64) {
    let metrics = cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics;
    (metrics.received_transactions, metrics.applied_transactions)
}

/// Positive-integer env knob with fallback (Go: `envInt`).
fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

#[test]
fn partitioned_node_repairs_tail_from_peer_logs() {
    let burst_rows = env_usize("MURMUR_TAIL_REPAIR_BURST_ROWS", 100);
    // Generous retention: the missed range must stay comfortably inside
    // every peer's origin log, so log repair (not snapshot resync) is
    // the only legitimate healing path. Mirrors Go's
    // MinLogRetentionMs=300000 / MinRetainedBatches=10000; Go's
    // MaxOfflineLogRetentionMs has no harness knob and keeps its 7-day
    // default, which is strictly more generous (see README.md).
    let cluster = Cluster::new(ClusterOptions {
        name: "tail-repair".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_millis(300_000)),
            minimum_retained: Some(10_000),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
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

        // Baseline: 10 rows on node 0, converged everywhere.
        for i in 0..BASELINE_ROWS {
            insert_marker(&cluster, 0, i, &format!("base-{i}"));
        }
        wait_converged(&cluster, BASELINE_ROWS as usize, BASELINE_TIMEOUT, "baseline");
        let baseline_digest = digest(rows(cluster.node(2)));

        // Log-path baseline on the soon-isolated node (Go: snapshot
        // counters that have no Rust equivalent; see README.md).
        let (recv_before, applied_before) = log_counters(&cluster, 2);
        eprintln!(
            "node3 pre-partition received_transactions={recv_before} applied_transactions={applied_before}"
        );

        // Partition node3 away from both peers in both directions.
        for (from, target) in [(2, 0), (0, 2), (2, 1), (1, 2)] {
            cluster
                .remove_peer(from, target)
                .unwrap_or_else(|e| panic!("remove peer {from}->{target}: {e}"));
        }
        wait_connected_peers(&cluster, 2, 0, SPLIT_PEERS_TIMEOUT);

        // Isolation probe: one row on node 0 must reach node 1 but never
        // node3.
        insert_marker(&cluster, 0, PROBE_ID, "probe");
        wait_row_count(
            &cluster,
            1,
            BASELINE_ROWS as usize + 1,
            PROBE_TIMEOUT,
            "probe on survivor",
        );
        std::thread::sleep(SETTLE_MARGIN); // negative check needs a settle margin
        let isolated = rows(cluster.node(2));
        assert!(
            isolated.len() == BASELINE_ROWS as usize,
            "partition leaked: node3 count = {}, want {BASELINE_ROWS}",
            isolated.len()
        );
        assert!(
            digest(isolated) == baseline_digest,
            "partition leaked: node3 digest changed while partitioned"
        );

        // Write burst on the survivors while node3 is isolated.
        for i in 0..burst_rows {
            let node = i % 2;
            insert_marker(
                &cluster,
                node,
                BURST_ID + i as i64,
                &format!("burst-{i}"),
            );
        }
        let want = BASELINE_ROWS as usize + 1 + burst_rows;
        wait_row_count(&cluster, 0, want, BURST_TIMEOUT, "burst on node1");
        wait_row_count(&cluster, 1, want, BURST_TIMEOUT, "burst on node2");
        // Anti-vacuity: node3 must actually be missing the contiguous range.
        let isolated = rows(cluster.node(2));
        assert!(
            isolated.len() == BASELINE_ROWS as usize,
            "node3 count = {}, want {BASELINE_ROWS} (it must miss the burst range)",
            isolated.len()
        );
        let missed = want - BASELINE_ROWS as usize;
        eprintln!("node3 missed {missed} rows while partitioned");

        // Heal and require full convergence.
        for (from, target) in [(2, 0), (0, 2), (2, 1), (1, 2)] {
            cluster
                .add_peer(from, target)
                .unwrap_or_else(|e| panic!("add peer {from}->{target}: {e}"));
        }
        for i in 0..cluster.len() {
            wait_connected_peers(&cluster, i, 2, HEAL_PEERS_TIMEOUT);
        }
        wait_converged(&cluster, want, HEAL_TIMEOUT, "post-heal");

        // The healing must have used log range repair, not the snapshot
        // path. The Rust engine only requests/serves/accepts snapshots
        // for explicitly trusted snapshot sources, and harness clusters
        // never configure any, so no snapshot could have been taken; the
        // counter advancement below proves the missed range arrived and
        // applied as log transactions (snapshot application bypasses
        // both counters). Diagnostic analog of Go's
        // gaps_detected/needs_sent/batches_received log line.
        let (recv_after, applied_after) = log_counters(&cluster, 2);
        eprintln!(
            "node3 log repair: received {recv_before}->{recv_after} applied {applied_before}->{applied_after} (missed {missed} rows)"
        );
        assert!(
            recv_after - recv_before >= missed as u64,
            "node3 received_transactions advanced by {}, want at least {missed} (healing must pull the missed range from peer logs)",
            recv_after - recv_before
        );
        assert!(
            applied_after - applied_before >= missed as u64,
            "node3 applied_transactions advanced by {}, want at least {missed} (healing must apply the missed range from peer logs)",
            applied_after - applied_before
        );

        // Post-heal write on the healed node replicates everywhere.
        insert_marker(&cluster, 2, POST_HEAL_ID, "post-heal");
        wait_converged(&cluster, want + 1, POST_HEAL_TIMEOUT, "post-heal write");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("tail-repair-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
