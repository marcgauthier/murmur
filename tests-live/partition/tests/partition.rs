//! Rust port of the Go `partition` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/partition/`
//! (`partition_test.go`). Four encrypted QUIC nodes form a mesh, split into
//! two isolated pairs, and write independently. The test checks each side
//! remains isolated, restores cross-partition peers, verifies equal ordered
//! state digests, then confirms a post-heal write replicates. Plain SQL
//! replaces the Go typed-records HTTP API (see the scenario README.md).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Convergence poll interval (Go: 50 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Baseline / post-heal convergence deadlines (Go: 10 s each).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(10);
/// Intra-group convergence deadline during the split (Go: 15 s).
const SPLIT_TIMEOUT: Duration = Duration::from_secs(15);
/// Full convergence deadline after healing (Go: 15 s).
const HEAL_TIMEOUT: Duration = Duration::from_secs(15);

/// One marker row: Go's `TypedContentionRow{ID, Name}`.
type MarkerRow = (i64, String);

/// All marker rows on one node, unordered (Go: `Cluster.TypedContentionRows`).
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

/// Wait until every node holds exactly `expected` rows (Go: `waitAllCount`).
fn wait_all_count(cluster: &Cluster, expected: usize, timeout: Duration, what: &str) {
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        (0..cluster.len()).all(|i| rows(cluster.node(i)).len() == expected)
    });
    assert!(
        converged,
        "cluster failed to converge to {expected} rows within {timeout:?} ({what})"
    );
}

#[test]
fn four_node_partition_isolation_and_healing() {
    let cluster = Cluster::new(ClusterOptions {
        name: "partition".into(),
        num_nodes: 4,
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

        // Initial baseline insert on node 0; all 4 nodes must receive it.
        insert_marker(&cluster, 0, 1, "baseline");
        wait_all_count(&cluster, 1, BASELINE_TIMEOUT, "baseline");

        // Partition the mesh into two isolated groups: {0, 1} and {2, 3}.
        for g1 in [0, 1] {
            for g2 in [2, 3] {
                cluster.remove_peer(g1, g2).expect("remove peer");
                cluster.remove_peer(g2, g1).expect("remove peer");
            }
        }
        std::thread::sleep(Duration::from_millis(100));

        // Independent writes on each side of the split.
        insert_marker(&cluster, 0, 100, "side-ab-write");
        insert_marker(&cluster, 2, 200, "side-cd-write");

        // Intra-group replication is asynchronous, so poll for internal
        // convergence. Any count above 2 means the partition leaked.
        let split_ok = wait_until(SPLIT_TIMEOUT, POLL_INTERVAL, || {
            let mut counts = vec![0usize; cluster.len()];
            for i in 0..cluster.len() {
                let n = rows(cluster.node(i)).len();
                counts[i] = n;
                assert!(
                    n <= 2,
                    "partition leaked: node {i} count = {n}, want at most 2"
                );
            }
            counts.iter().all(|&n| n == 2)
        });
        assert!(
            split_ok,
            "groups failed to converge internally within {SPLIT_TIMEOUT:?} (want all 2)"
        );

        // Isolation by content: equal counts but divergent digests prove
        // each group converged internally without cross-group replication.
        let digests: Vec<String> =
            (0..cluster.len()).map(|i| digest(rows(cluster.node(i)))).collect();
        assert!(
            digests[0] == digests[1] && digests[2] == digests[3],
            "intra-group digests diverge: {digests:?}"
        );
        assert!(
            digests[0] != digests[2],
            "cross-group digests unexpectedly match during partition: {digests:?}"
        );

        // Heal the partition: reconnect the two groups both directions.
        for g1 in [0, 1] {
            for g2 in [2, 3] {
                cluster.add_peer(g1, g2).expect("add peer");
                cluster.add_peer(g2, g1).expect("add peer");
            }
        }

        // Full convergence on 3 rows, then bit-identical digests on all 4.
        wait_all_count(&cluster, 3, HEAL_TIMEOUT, "post-heal");
        let digests: Vec<String> =
            (0..cluster.len()).map(|i| digest(rows(cluster.node(i)))).collect();
        for (i, d) in digests.iter().enumerate().skip(1) {
            assert!(
                *d == digests[0],
                "node {i} digest {d} != node 0 digest {}",
                digests[0]
            );
        }

        // Post-heal write replicates to all.
        insert_marker(&cluster, 1, 300, "post-heal");
        wait_all_count(&cluster, 4, BASELINE_TIMEOUT, "post-heal write");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("partition-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
