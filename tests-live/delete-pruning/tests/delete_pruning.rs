//! Rust port of the Go `delete-pruning` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/delete-pruning/`
//! (`delete_pruning_test.go`).
//! `TestConcurrentDeletesResurrectionsAndUpdatesConverge`: three nodes share
//! 300 rows, then concurrently delete, update, and resurrect disjoint thirds
//! under aggressive log retention; a log-GC pass is forced mid-flight and
//! all nodes must converge on exact counts plus equal row contents. Plain
//! SQL replaces the Go typed-contention HTTP API (see the scenario README.md
//! for the deviations).
//!
//! Scope (same as Go): delete/resurrect convergence plus log-GC collection
//! of the delete batches; stored tombstone markers stay in state by design
//! and are out of scope.

use std::sync::{Arc, Barrier};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Row-count knob (Go: `MURMUR_DELETE_PRUNING_KEYS`, default 300, multiple of 3).
const KEYS_ENV: &str = "MURMUR_DELETE_PRUNING_KEYS";
const DEFAULT_KEYS: usize = 300;
/// Convergence deadline (Go: 60 s `waitConverged`).
const CONVERGED_TIMEOUT: Duration = Duration::from_secs(60);
/// Post-prune write deadline (Go: 30 s final `waitConverged`).
const POST_PRUNE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 100 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// GC-pass wait deadline (Go: 150 s `waitMetricAdvanced`).
const GC_TIMEOUT: Duration = Duration::from_secs(150);
/// GC-pass poll interval (Go: 2 s).
const GC_POLL: Duration = Duration::from_secs(2);

/// Row id (Go: `deleteRowID`).
fn row_id(n: usize) -> String {
    format!("{n:08x}-0000-4000-8000-{n:012x}")
}

/// Typed insert on one node (Go: `Cluster.TypedContentionInsert`).
fn insert_row(db: &Database, node: usize, id: String, name: String) {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO contention(id, name) VALUES(?, ?)",
            (id, name),
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
}

/// Typed single-field update (Go: `Cluster.TypedContentionUpdate`).
fn update_name(db: &Database, node: usize, id: &str, name: String) {
    let id = id.to_owned();
    db.transaction(move |sql| {
        let changed = sql.execute("UPDATE contention SET name = ? WHERE id = ?", (name, id))?;
        assert_eq!(changed, 1, "node{node} update hit {changed} rows");
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} update failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after update failed: {e}"));
}

/// Typed delete (Go: `Cluster.TypedContentionDelete`).
fn delete_row(db: &Database, node: usize, id: &str) {
    let id = id.to_owned();
    db.transaction(move |sql| {
        let changed = sql.execute("DELETE FROM contention WHERE id = ?", [id])?;
        assert_eq!(changed, 1, "node{node} delete hit {changed} rows");
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} delete failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after delete failed: {e}"));
}

/// Typed read on one node: `None` when the row is absent (Go:
/// `Cluster.TypedContentionRead` returning `failed (404)`).
fn read_name(db: &Database, id: &str) -> Option<String> {
    let id = id.to_owned();
    db.read(move |sql| {
        let mut stmt = sql.prepare("SELECT name FROM contention WHERE id = ?")?;
        let mut rows = stmt.query([id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row.get::<_, String>(0)?)),
            None => Ok(None),
        }
    })
    .expect("read row failed")
}

/// Canonical ordered rows on one node (Go: `typedRowsDigest`).
fn all_rows(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name FROM contention ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read all rows failed")
}

/// Wait until every node holds exactly `want` rows with identical contents
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let mut last = vec![String::new(); cluster.len()];
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        let mut first: Option<Vec<(String, String)>> = None;
        for i in 0..cluster.len() {
            let rows = all_rows(cluster.node(i));
            last[i] = format!("count={}", rows.len());
            if rows.len() != want {
                return false;
            }
            match &first {
                None => first = Some(rows),
                Some(prev) if *prev == rows => {}
                _ => return false,
            }
        }
        true
    });
    assert!(
        converged,
        "nodes did not converge on {want} rows with equal contents within {timeout:?}; last={last:?}"
    );
}

/// Retention-collection passes run on node 0 until the durable collected
/// counter advances past `before` (Go: `waitMetricAdvanced` on
/// `spedsql_gc_runs_total` plus the `spedsql_gc_log_collected_total`
/// advance check). Rust exposes an on-demand pass
/// (`Database::collect_retention`), so each poll triggers a pass instead of
/// waiting for the background ticker.
fn wait_gc_collected(cluster: &Cluster, before: u64) {
    let mut collected = before;
    let converged = wait_until(GC_TIMEOUT, GC_POLL, || {
        let pass = cluster
            .node(0)
            .collect_retention()
            .expect("retention collection pass");
        collected = cluster
            .node(0)
            .retention_status()
            .expect("retention status")
            .collected_transactions;
        eprintln!("retention pass collected {pass} records; total {collected} (before {before})");
        collected > before
    });
    assert!(
        converged,
        "no log batches collected (before={before} after={collected}); GC passes did not prune delete batches"
    );
}

#[test]
fn concurrent_deletes_resurrections_and_updates_converge() {
    let keys = std::env::var(KEYS_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_KEYS);
    assert!(
        keys % 3 == 0,
        "{KEYS_ENV}={keys} must be a multiple of 3"
    );
    let third = keys / 3;

    // Aggressive log retention (Go: MinLogRetentionMs=1000,
    // MaxOfflineLogRetentionMs=20000, MinRetainedBatches=10).
    let cluster = Cluster::new(ClusterOptions {
        name: "delete-pruning".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            maximum_offline: Some(Duration::from_secs(20)),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "{} dir={} repl={}",
            info.label,
            info.dir.display(),
            info.repl_addr
        );
    }
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: TypedRecords/TypedContention tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE contention(id TEXT PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Seed the shared row set on node 1 and converge.
        for i in 0..keys {
            insert_row(cluster.node(0), 1, row_id(i), format!("seed-{i}"));
        }
        wait_converged(&cluster, keys, CONVERGED_TIMEOUT);

        let gc_runs_before = cluster
            .node(0)
            .replication()
            .map(|h| h.status().metrics.retention_runs)
            .unwrap_or(0);
        let gc_collected_before = cluster
            .node(0)
            .retention_status()
            .expect("retention status baseline")
            .collected_transactions;

        // Phase 1, concurrent on disjoint thirds: node 1 deletes the first
        // third, node 2 updates the middle third, node 3 deletes the last
        // third. Only the middle third survives.
        let start = Arc::new(Barrier::new(cluster.len()));
        std::thread::scope(|s| {
            for node_idx in 0..cluster.len() {
                let db = cluster.node(node_idx).clone();
                let start = start.clone();
                s.spawn(move || {
                    start.wait();
                    let label = node_idx + 1;
                    match node_idx {
                        0 => {
                            for i in 0..third {
                                delete_row(&db, label, &row_id(i));
                            }
                        }
                        1 => {
                            for i in third..2 * third {
                                update_name(&db, label, &row_id(i), format!("upd-{i}"));
                            }
                        }
                        _ => {
                            for i in 2 * third..keys {
                                delete_row(&db, label, &row_id(i));
                            }
                        }
                    }
                });
            }
        });
        // Anti-vacuity: the deletes must actually remove rows everywhere.
        wait_converged(&cluster, third, CONVERGED_TIMEOUT);
        let want_middle = format!("upd-{third}");
        let spot = read_name(cluster.node(0), &row_id(third));
        assert_eq!(
            spot,
            Some(want_middle.clone()),
            "middle-third value = {spot:?}; want {want_middle:?} (concurrent update lost)"
        );

        // Phase 2: resurrect the first third with new values from node 2.
        for i in 0..third {
            insert_row(cluster.node(1), 2, row_id(i), format!("res-{i}"));
        }
        wait_converged(&cluster, 2 * third, CONVERGED_TIMEOUT);

        // Phase 3: delete the middle third from node 1. Survivors: the
        // resurrected first third only.
        for i in third..2 * third {
            delete_row(cluster.node(0), 1, &row_id(i));
        }
        wait_converged(&cluster, third, CONVERGED_TIMEOUT);

        // Force GC passes to run over the tombstone batches and require the
        // durable collected counter to advance (Go: 30 s ticker wait +
        // collected-total assertion).
        wait_gc_collected(&cluster, gc_collected_before);
        let gc_runs_after = cluster
            .node(0)
            .replication()
            .map(|h| h.status().metrics.retention_runs)
            .unwrap_or(0);
        let gc_collected_after = cluster
            .node(0)
            .retention_status()
            .expect("retention status final")
            .collected_transactions;
        eprintln!(
            "retention runs advanced {gc_runs_before} -> {gc_runs_after}; log records collected during test: {}",
            gc_collected_after - gc_collected_before
        );

        // Identical ordered digests and exact row counts on all nodes.
        let want_count = third;
        for i in 0..cluster.len() {
            let rows = all_rows(cluster.node(i));
            assert_eq!(
                rows.len(),
                want_count,
                "node{} count = {}, want {want_count}",
                i + 1,
                rows.len()
            );
        }
        let want_digest = all_rows(cluster.node(0));
        for i in 1..cluster.len() {
            let digest = all_rows(cluster.node(i));
            assert_eq!(digest, want_digest, "node{} digest differs", i + 1);
        }
        // Content proof: resurrected rows present with new values, deleted
        // rows absent, on every node.
        for i in 0..cluster.len() {
            let got = read_name(cluster.node(i), &row_id(0));
            assert_eq!(
                got.as_deref(),
                Some("res-0"),
                "node{} resurrected value = {got:?}, want \"res-0\"",
                i + 1
            );
            let absent = read_name(cluster.node(i), &row_id(third));
            assert!(
                absent.is_none(),
                "node{} deleted row lookup = {absent:?}; want not found",
                i + 1
            );
        }

        // A post-prune write replicates everywhere.
        insert_row(cluster.node(2), 3, row_id(keys + 1), "post-prune".into());
        wait_converged(&cluster, want_count + 1, POST_PRUNE_TIMEOUT);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("delete-pruning-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
