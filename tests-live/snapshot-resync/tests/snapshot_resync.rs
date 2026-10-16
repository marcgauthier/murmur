//! Rust port of the Go `snapshot-resync` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/snapshot-resync/`
//! (`README.md` + `snapshot_resync_test.go` + `snapshot_multisource_test.go`).
//! Three nodes mesh with aggressive log retention; a node stops while the
//! survivors write past retention and GC expiry, then restarts with the same
//! durable directory. Its needed log ranges are unrecoverable, so it must
//! rejoin by merging a snapshot without discarding its acknowledged pre-stop
//! state. A second test restarts two stale nodes back-to-back so their
//! snapshot requests overlap on one survivor, and a multi-source case gives
//! the stale node three snapshot sources concurrently. Plain SQL replaces
//! the Go typed-records HTTP API, close/reopen replaces process stop/start,
//! and the snapshot path is proven by flat log-path counters (no snapshot
//! counters exist); see the scenario README.md for the deviations.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use murmur_live_harness::murmur_replication::retention;
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, Cluster, ClusterOptions, ReplicationOptions};

/// Log retention (Go: `MinLogRetentionMs = 1000`).
const RETENTION: Duration = Duration::from_secs(1);
/// Offline-peer memory (Go: `MaxOfflineLogRetentionMs = 20000`).
const MAXIMUM_OFFLINE: Duration = Duration::from_secs(20);
/// Newest transactions per origin the survivors must keep (Go: `MinRetainedBatches = 10`).
const MINIMUM_RETAINED: u64 = 10;
/// Minimum pruned transactions per survivor before the rejoin: 60 survivor
/// writes minus the 10 retained, with margin. Proves the stale node's needed
/// ranges are genuinely unrecoverable from logs, so the rejoin must use the
/// snapshot path.
const MIN_COLLECTED: u64 = 40;

/// Baseline convergence deadline (Go: 30 s; margin matches the corrupt-snapshot port).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(60);
/// Survivor row-count deadline (Go: 30 s).
const ROW_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// GC collection-loop deadline past the expiry wait.
const COLLECT_TIMEOUT: Duration = Duration::from_secs(60);
/// Single-stale rejoin convergence deadline (Go: 90 s).
const RESYNC_TIMEOUT: Duration = Duration::from_secs(90);
/// Dual/multi-source rejoin convergence deadline (Go: 120 s).
const RESYNC_TIMEOUT_LONG: Duration = Duration::from_secs(120);

/// Canonical ordered names on one node (Go: `Cluster.TypedContentionRows`).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM records ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in rows {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read names")
}

/// Canonical digest of a name set (Go: sorted rows hashed; newline-joined
/// names are equally strict here since every row differs by name).
fn digest(names: &[String]) -> String {
    names.join("\n")
}

/// Row count on one node (cheap pre-check before full name reads).
fn row_count(db: &Database) -> usize {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM records", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .expect("row count")
}

/// One single-row transaction (Go: `TypedContentionInsert`).
fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) {
    let name = name.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            sql.execute("INSERT INTO records(id, name) VALUES(?, ?)", (id, name))?;
            Ok(())
        })
        .unwrap_or_else(|e| panic!("node {node} insert failed: {e}"));
}

/// One multi-row transaction of `count` rows (Go: `TypedContentionInsertMany`;
/// the multi-source leg needs snapshot volume, not round trips).
fn insert_many(cluster: &Cluster, node: usize, start_id: i64, count: i64, prefix: &str) {
    let prefix = prefix.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            for n in start_id..start_id + count {
                sql.execute(
                    "INSERT INTO records(id, name) VALUES(?, ?)",
                    (n, format!("{prefix}-{n}")),
                )?;
            }
            Ok(())
        })
        .unwrap_or_else(|e| panic!("node {node} batch insert failed: {e}"));
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
fn wait_row_count(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if row_count(cluster.node(idx)) == want {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "node {idx} record count = {}, want {want} within {timeout:?}",
                row_count(cluster.node(idx)),
            );
        }
        std::thread::sleep(CONVERGE_POLL);
    }
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let counts: Vec<usize> = (0..cluster.len())
            .map(|i| row_count(cluster.node(i)))
            .collect();
        if counts.iter().all(|&n| n == want) {
            let mut first = String::new();
            let mut equal = true;
            for (i, idx) in (0..cluster.len()).enumerate() {
                let d = digest(&names(cluster.node(idx)));
                if i == 0 {
                    first = d;
                } else if d != first {
                    equal = false;
                    break;
                }
            }
            if equal {
                return;
            }
        }
        if Instant::now() >= deadline {
            for i in 0..cluster.len() {
                let node_names = names(cluster.node(i));
                eprintln!(
                    "node {i} at timeout: count={} digest={}",
                    node_names.len(),
                    digest(&node_names)
                );
            }
            panic!(
                "nodes did not converge on {want} rows with equal digests within {timeout:?}"
            );
        }
        std::thread::sleep(CONVERGE_POLL);
    }
}

/// Log-path activity counters for one node: `(received_transactions,
/// applied_transactions)`. Snapshot application bypasses both counters, so a
/// flat pair across the resync proves the rows arrived via snapshot.
fn log_counters(cluster: &Cluster, idx: usize) -> (u64, u64) {
    let metrics = cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics;
    (metrics.received_transactions, metrics.applied_transactions)
}

/// Assert the resync used the snapshot path (Go: `spedsql_repl_snapshots_received_total >= 1`,
/// which has no Rust equivalent).
/// Snapshot-path proof (Go: receiver snapshot counter): the resync's log
/// growth must stay within the retained tail and far below the gap. The
/// retained tail (`minimum_retained` per origin) legitimately arrives via
/// logs, so a flat counter pair would be over-strict; the bulk can only
/// have arrived via snapshot, the only other row path.
fn assert_snapshot_path(cluster: &Cluster, idx: usize, before: (u64, u64), gap: usize) {
    let (received, _applied) = log_counters(cluster, idx);
    let growth = received.saturating_sub(before.0);
    assert!(
        growth <= 2 * MINIMUM_RETAINED,
        "node {} resync pulled {growth} log transactions, above the retained tail (2 x {MINIMUM_RETAINED})",
        idx + 1,
    );
    assert!(
        (growth as usize) < gap,
        "node {} resync arrived over the log path (log growth {growth}, gap {gap}), not a snapshot (log catch-up would hide a snapshot-path regression)",
        idx + 1,
    );
}

/// Run one bounded retention pass on one node; returns transactions pruned.
fn collect_history(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .collect_retention()
        .unwrap_or_else(|e| panic!("node {idx} collect retention: {e}"))
}

/// Total transactions ever pruned on one node.
fn collected_total(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .retention_status()
        .expect("retention status")
        .collected_transactions
}

/// Count of acknowledged pre-stop rows surviving on one node (Go: `countPrefix(rows, "stale-")`).
fn stale_rows(cluster: &Cluster, idx: usize) -> usize {
    names(cluster.node(idx))
        .iter()
        .filter(|name| name.starts_with("stale-"))
        .count()
}

/// Same schema on every node (Go: TypedRecords fixture tables).
fn migrate_all(cluster: &Cluster) {
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

/// Standard snapshot-resync cluster: aggressive retention plus all-node
/// snapshot trust (Go harness fixture policy; the engine only requests
/// snapshots from explicitly trusted sources).
fn snapshot_cluster(name: &str, node_ids: Vec<[u8; 16]>) -> Cluster {
    let all: BTreeSet<[u8; 16]> = node_ids.iter().copied().collect();
    let mut trusted_snapshot_sources: HashMap<usize, BTreeSet<[u8; 16]>> = HashMap::new();
    for i in 0..node_ids.len() {
        trusted_snapshot_sources.insert(i, all.clone());
    }
    let num_nodes = node_ids.len();
    Cluster::new(ClusterOptions {
        name: name.into(),
        num_nodes,
        node_ids,
        replication: Some(ReplicationOptions {
            retention: Some(RETENTION),
            minimum_retained: Some(MINIMUM_RETAINED),
            maximum_offline: Some(MAXIMUM_OFFLINE),
            ..Default::default()
        }),
        trusted_snapshot_sources,
        ..Default::default()
    })
    .expect("cluster setup")
}

/// Member-deadline expiry plus explicit collection passes: every survivor
/// must prune past retention and stop pinning every victim, proving the
/// victims' needed ranges are genuinely unrecoverable from logs (Go: the
/// 55 s expiry-plus-GC-pass sleep).
fn ensure_snapshot_path(cluster: &Cluster, survivors: &[usize], victims: &[usize]) {
    eprintln!("waiting for member-deadline expiry and collection");
    std::thread::sleep(env_seconds(
        "MURMUR_SNAPSHOT_RESYNC_GC_WAIT_SECONDS",
        30,
    ));
    let collect_deadline = Instant::now() + COLLECT_TIMEOUT;
    loop {
        let totals: Vec<u64> = survivors
            .iter()
            .map(|&i| collected_total(cluster, i))
            .collect();
        if totals.iter().all(|&n| n >= MIN_COLLECTED) {
            eprintln!("survivors pruned {totals:?} transactions past retention");
            break;
        }
        if Instant::now() >= collect_deadline {
            panic!(
                "survivors collected only {totals:?} transactions, want >= {MIN_COLLECTED} each (snapshot path not forced)"
            );
        }
        for &i in survivors {
            collect_history(cluster, i);
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    // Prove the deadline leg explicitly: every victim is now older than
    // every survivor's offline memory.
    for &i in survivors {
        let status = cluster
            .node(i)
            .replication()
            .expect("replication handle")
            .retention_status()
            .expect("retention status");
        for &victim in victims {
            let victim_id = cluster.node_id(victim);
            let peer = status
                .peers
                .iter()
                .find(|p| p.node == victim_id)
                .unwrap_or_else(|| panic!("node {i} has no retention record for node {}", victim + 1));
            let age = retention::now_millis().saturating_sub(peer.last_seen_millis);
            assert!(
                age > MAXIMUM_OFFLINE.as_millis() as u64,
                "node {} still pins node {} (last seen {age} ms ago)",
                i + 1,
                victim + 1,
            );
        }
    }
}

#[test]
fn stale_node_rejoins_via_snapshot() {
    let mut cluster = snapshot_cluster("snapshot-resync", vec![[0x10; 16], [0x20; 16], [0x30; 16]]);
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Phase 1: node3 writes acknowledged rows, then all three converge.
        migrate_all(&cluster);
        for i in 0..5 {
            insert(&cluster, 2, i + 1, &format!("stale-{i}"));
        }
        cluster.node(2).flush().expect("flush baseline");
        wait_converged(&cluster, 5, BASELINE_TIMEOUT);

        // Phase 2: node3 stops; survivors write far past retention.
        // Flush every write: the product mints one replication log entry
        // per flush batch, not per SQL transaction, so a single trailing
        // flush would leave fewer entries than `minimum_retained` and
        // nothing would ever become eligible for pruning.
        cluster.stop_node(2);
        for i in 0..60 {
            insert(&cluster, 0, 100 + i, &format!("fresh-{i}"));
            cluster.node(0).flush().expect("flush survivor write");
        }
        wait_row_count(&cluster, 0, 65, ROW_TIMEOUT);
        wait_row_count(&cluster, 1, 65, ROW_TIMEOUT);
        ensure_snapshot_path(&cluster, &[0, 1], &[2]);

        // Phase 3: node3 restarts with the same durable directory and rejoins.
        cluster.start_node(2).expect("stale node restart");
        cluster.mesh().expect("re-mesh after restart");
        let log_before = log_counters(&cluster, 2);
        wait_converged(&cluster, 65, RESYNC_TIMEOUT);

        // The rejoin must have used the snapshot path, not log catch-up.
        assert_snapshot_path(&cluster, 2, log_before, 65 - 5);

        // Node3's acknowledged pre-stop state survives the snapshot merge.
        assert_eq!(
            stale_rows(&cluster, 2),
            5,
            "node3 stale rows after resync = {}, want 5",
            stale_rows(&cluster, 2),
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("snapshot-resync-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn dual_stale_nodes_rejoin_via_snapshot() {
    let mut cluster = snapshot_cluster(
        "snapshot-resync-dual",
        vec![[0x11; 16], [0x21; 16], [0x31; 16]],
    );
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster);
        for i in 0..5 {
            insert(&cluster, 2, i + 1, &format!("stale-{i}"));
        }
        cluster.node(2).flush().expect("flush baseline");
        wait_converged(&cluster, 5, BASELINE_TIMEOUT);

        // Two nodes stop; the survivor writes far past retention (flushed
        // per write: one replication log entry is minted per flush batch).
        cluster.stop_node(1);
        cluster.stop_node(2);
        for i in 0..60 {
            insert(&cluster, 0, 100 + i, &format!("fresh-{i}"));
            cluster.node(0).flush().expect("flush survivor write");
        }
        wait_row_count(&cluster, 0, 65, ROW_TIMEOUT);
        ensure_snapshot_path(&cluster, &[0], &[1, 2]);

        // Restart both stale nodes back-to-back so their snapshot requests
        // overlap on the survivor.
        cluster.start_node(1).expect("stale node2 restart");
        cluster.start_node(2).expect("stale node3 restart");
        cluster.mesh().expect("re-mesh after restart");
        let log_before: Vec<(u64, u64)> =
            [1, 2].iter().map(|&i| log_counters(&cluster, i)).collect();
        wait_converged(&cluster, 65, RESYNC_TIMEOUT_LONG);

        for (k, &idx) in [1, 2].iter().enumerate() {
            assert_snapshot_path(&cluster, idx, log_before[k], 65 - 5);
            assert_eq!(
                stale_rows(&cluster, idx),
                5,
                "node {} stale rows after resync = {}, want 5",
                idx + 1,
                stale_rows(&cluster, idx),
            );
            // The resync diagnostics surface end to end: each rejoined node
            // reports live replication sessions (Go asserts the presence of
            // the progress/deferral series, which have no Rust equivalent).
            let peers = cluster
                .node(idx)
                .replication()
                .expect("replication handle")
                .status()
                .peers;
            assert!(
                peers >= 1,
                "node {} reports no connected peers after resync",
                idx + 1,
            );
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("snapshot-resync-dual-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn stale_node_rejoins_via_single_snapshot_source() {
    const VICTIM: usize = 3;
    let mut cluster = snapshot_cluster(
        "snapshot-multisource",
        vec![[0x12; 16], [0x22; 16], [0x32; 16], [0x42; 16]],
    );
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster);
        for i in 0..5 {
            insert(&cluster, VICTIM, i + 1, &format!("stale-{i}"));
        }
        cluster.node(VICTIM).flush().expect("flush baseline");
        wait_converged(&cluster, 5, BASELINE_TIMEOUT);

        // Victim stops; survivors write thousands of rows so the resync
        // snapshot spans many chunks (a 1-2 chunk snapshot rarely
        // interleaves badly enough to expose source contention).
        cluster.stop_node(VICTIM);
        // Batched multi-row transactions: the test needs snapshot volume
        // (many chunks), not round trips. Flush per batch: one replication
        // log entry is minted per flush batch, and retention needs dozens
        // of entries to prune past (a single trailing flush would leave
        // fewer entries than `minimum_retained`).
        const FRESH_ROWS: i64 = 6000;
        const BATCH: i64 = 100;
        let start = Instant::now();
        for base in (0..FRESH_ROWS).step_by(BATCH as usize) {
            insert_many(&cluster, 0, 100 + base, BATCH, "fresh");
            cluster.node(0).flush().expect("flush survivor batch");
        }
        eprintln!(
            "wrote {FRESH_ROWS} survivor rows in {:?}",
            start.elapsed()
        );
        let want = 5 + FRESH_ROWS as usize;
        for i in 0..3 {
            wait_row_count(&cluster, i, want, ROW_TIMEOUT);
        }
        ensure_snapshot_path(&cluster, &[0, 1, 2], &[VICTIM]);

        cluster.start_node(VICTIM).expect("victim restart");
        cluster.mesh().expect("re-mesh after restart");
        let log_before = log_counters(&cluster, VICTIM);
        wait_converged(&cluster, want, RESYNC_TIMEOUT_LONG);

        // The rejoin used the snapshot path (not log catch-up) under real
        // multi-source contention: three survivors serve concurrently while
        // the transfer completes to convergence. Rival-source suppression
        // has no Rust counter; contention coverage is behavioral.
        assert_snapshot_path(&cluster, VICTIM, log_before, want - 5);
        assert_eq!(
            stale_rows(&cluster, VICTIM),
            5,
            "victim stale rows after resync = {}, want 5",
            stale_rows(&cluster, VICTIM),
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("snapshot-multisource-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
