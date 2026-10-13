//! Rust port of the Go `overload-budgets` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/overload-budgets/`
//! (`overload_budgets_test.go`, `README.md`). Two nodes run with small commit
//! budgets; an oversize write must be rejected cleanly with no partial row on
//! either node and no writer poisoning, then a concurrent blast (8 writers x
//! 50 inserts) must fully converge with bounded tail latency while both nodes
//! stay responsive. Plain SQL replaces the Go typed-contention HTTP API; the
//! HTTP 1 MiB body-cap leg has no Rust equivalent (no HTTP server exists) and
//! the per-value-bytes leg is adapted to the `transaction_bytes` commit
//! budget (no per-value product limit exists). See the scenario README.md.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Error, Migration};
use murmur_live_harness::{Cluster, ClusterOptions, ReplicationOptions, SqliteOptions};

/// Commit budget on every node (Go: 64 KiB transactions; the Go 4 KiB
/// per-value budget has no Rust product equivalent).
const TX_BUDGET: usize = 64 << 10;
/// Admission queue: comfortably above the commit budget so the oversize leg
/// trips the commit budget deterministically (and validation requires
/// `transaction_bytes <= queue_bytes`).
const QUEUE_BYTES: usize = 8 << 20;
/// Capture worker budget (`queue_bytes >= capture_bytes` per validation).
const CAPTURE_BYTES: usize = 1 << 20;

/// Post-rejection convergence deadline (Go: 30 s).
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
/// Blast convergence deadline (Go: 60 s).
const BLAST_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// Final readiness deadline per node (Go: `WaitNodeReady`).
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Readiness poll interval.
const READY_POLL: Duration = Duration::from_millis(100);
/// Blast tail-latency bound (Go: p99 under 10 s).
const P99_BOUND: Duration = Duration::from_secs(10);

/// One replicated row: Go's `TypedContentionRow{ID, Name, Phone, Score}`.
type ContentionRow = (String, String, String, i64);

/// All rows on one node (Go: `Cluster.TypedContentionRows`).
fn rows(db: &Database) -> Vec<ContentionRow> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name, phone, score FROM records")?;
        let mapped = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in mapped {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read rows")
}

/// Canonical digest of a row set: sorted `Debug` form (Go hashes ordered
/// `ID:Name:Phone:Score` lines; same criterion, cheaper to express).
fn digest(mut rows: Vec<ContentionRow>) -> String {
    rows.sort();
    format!("{rows:?}")
}

/// Row count on one node (cheap pre-check before full digest reads).
fn row_count(db: &Database) -> usize {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM records", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .expect("row count")
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration, what: &str) {
    let deadline = Instant::now() + timeout;
    loop {
        let counts: Vec<usize> = (0..cluster.len())
            .map(|i| row_count(cluster.node(i)))
            .collect();
        if counts.iter().all(|&n| n == want) {
            let mut first = String::new();
            let mut equal = true;
            for (i, idx) in (0..cluster.len()).enumerate() {
                let d = digest(rows(cluster.node(idx)));
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
            panic!("nodes did not converge on {want} rows within {timeout:?} ({what})");
        }
        std::thread::sleep(CONVERGE_POLL);
    }
}

/// Wait until one node is responsive with its full peer set
/// (Go: `Cluster.WaitNodeReady` over the admin API).
fn wait_node_ready(cluster: &Cluster, idx: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let peers = cluster
            .node(idx)
            .replication()
            .expect("replication handle")
            .status()
            .peers;
        // A successful read proves the writer path is responsive, not just
        // the replication endpoint.
        let readable = cluster.node(idx).read(|_| Ok(())).is_ok();
        if peers == cluster.len() - 1 && readable {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "node {idx} not ready within {timeout:?} (peers={peers}, readable={readable})"
            );
        }
        std::thread::sleep(READY_POLL);
    }
}

/// Insert one contention row on one node (Go: `TypedContentionInsert`).
fn insert(cluster: &Cluster, node: usize, id: String, name: String) -> Result<(), Error> {
    cluster.node(node).transaction(move |sql| {
        sql.execute(
            "INSERT INTO records(id, name, phone, score) VALUES(?, ?, ?, ?)",
            (id, name, String::new(), 0_i64),
        )?;
        Ok(())
    })
}

#[test]
fn budget_rejection_and_pressure_convergence() {
    let cluster = Cluster::new(ClusterOptions {
        name: "overload-budgets".into(),
        num_nodes: 2,
        replication: Some(ReplicationOptions {
            max_transaction_bytes: Some(TX_BUDGET),
            ..Default::default()
        }),
        sqlite: Some(SqliteOptions {
            transaction_bytes: Some(TX_BUDGET),
            queue_bytes: Some(QUEUE_BYTES),
            capture_bytes: Some(CAPTURE_BYTES),
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
                    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Past the 64 KiB commit budget: clean rejection naming the budget,
        // no partial row. (Go sends a 5 KiB value against a 4 KiB per-value
        // budget; Rust has no per-value limit, so one insert carries a value
        // past the transaction budget instead.)
        cluster.node(0).flush().expect("flush before oversize");
        let big = "V".repeat(TX_BUDGET + 1);
        let err = cluster
            .node(0)
            .transaction(move |sql| {
                sql.execute(
                    "INSERT INTO records(id, name, phone, score) VALUES(?, ?, ?, ?)",
                    (format!("{:032x}", 2), big, String::new(), 0_i64),
                )?;
                Ok(())
            })
            .expect_err("oversize value accepted, want budget rejection");
        match &err {
            Error::Limit(msg) => assert!(
                msg.contains("budget exceeded"),
                "rejection error {err:?} does not name the budget"
            ),
            other => panic!("rejection error {other:?} is not the budget limit"),
        }
        for i in 0..cluster.len() {
            let n = row_count(cluster.node(i));
            assert!(
                n == 0,
                "node {i} has {n} rows after rejection, want 0 (partial apply?)"
            );
        }

        // Writer not poisoned: a small write works and converges.
        insert(&cluster, 0, format!("{:032x}", 3), "ok".into()).expect("post-rejection write");
        wait_converged(&cluster, 1, PROBE_TIMEOUT, "post-rejection probe");

        // Pressure blast: concurrent writers converge with bounded tail.
        const WRITERS: usize = 8;
        const PER_WRITER: usize = 50;
        let lat: Mutex<Vec<Duration>> = Mutex::new(Vec::with_capacity(WRITERS * PER_WRITER));
        let failed_writes = AtomicU64::new(0);
        let scope_cluster = &cluster;
        let scope_lat = &lat;
        let scope_failed = &failed_writes;
        std::thread::scope(|s| {
            for w in 0..WRITERS {
                s.spawn(move || {
                    for i in 0..PER_WRITER {
                        let id = format!("{:032x}", 10000 + w * 1000 + i);
                        let name = format!("w{w}-{i}");
                        let start = Instant::now();
                        let result = insert(scope_cluster, 0, id, name);
                        let el = start.elapsed();
                        scope_lat.lock().expect("latency lock").push(el);
                        if result.is_err() {
                            scope_failed.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                    }
                });
            }
        });
        assert!(
            failed_writes.load(Ordering::SeqCst) == 0,
            "{} blast writes failed",
            failed_writes.load(Ordering::SeqCst)
        );
        let mut lat = lat.into_inner().expect("latency lock");
        assert!(
            lat.len() == WRITERS * PER_WRITER,
            "blast recorded {} latencies, want {}",
            lat.len(),
            WRITERS * PER_WRITER
        );
        lat.sort();
        let p99 = lat[(lat.len() as f64 * 0.99) as usize];
        eprintln!(
            "blast: {} inserts p50={:?} p99={p99:?} max={:?}",
            lat.len(),
            lat[lat.len() / 2],
            lat[lat.len() - 1]
        );
        assert!(p99 <= P99_BOUND, "blast p99 {p99:?} exceeds {P99_BOUND:?} bound");
        wait_converged(&cluster, 1 + WRITERS * PER_WRITER, BLAST_TIMEOUT, "blast");
        wait_node_ready(&cluster, 0, READY_TIMEOUT);
        wait_node_ready(&cluster, 1, READY_TIMEOUT);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("overload-budgets-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
