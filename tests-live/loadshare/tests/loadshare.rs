//! Rust port of the Go `loadshare` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/loadshare/`
//! (`loadshare_test.go`, `TestWriterSharesUnderLoad`). Two nodes form an
//! encrypted mesh; node1 absorbs 240 local writes across four concurrent
//! writers while node2 replicates 20 rows. The test requires full
//! convergence (no remote starvation on the saturated node), acquisitions
//! in both scheduler classes, and service debt within the 1s bound, and
//! logs the local-commit latency distribution.
//!
//! Plain SQL replaces the Go typed-RIME HTTP API, and the scheduler
//! assertions read the in-process `WriterScheduler::snapshot()` instead of
//! scraping Prometheus `/metrics` (see the scenario README.md).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Concurrent local writers on the saturated node (Go: `writers`).
const WRITERS: usize = 4;
/// Inserts per writer (Go: `perWriter`); 4*60 = 240 local rows.
const PER_WRITER: usize = 60;
/// Rows replicated from the peer while local writers saturate node1 (Go: `remote`).
const REMOTE: usize = 20;
/// Full-convergence row count on each node.
const TOTAL: usize = WRITERS * PER_WRITER + REMOTE;

/// Convergence deadline (Go: 60s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 50ms).
const CONVERGE_POLL: Duration = Duration::from_millis(50);
/// Pacing between peer writes (Go: 10ms sleeps).
const REMOTE_PACING: Duration = Duration::from_millis(10);
/// Service-debt bound (Go: 1s; the product default `max_debt`).
const MAX_DEBT: Duration = Duration::from_secs(1);
/// Absurd-latency tripwire for the slowest local commit (Go: 30s).
const MAX_LATENCY: Duration = Duration::from_secs(30);

/// One row insert, published to replication (Go: `cluster.TypedInsert`).
fn typed_insert(db: &Database, name: String) -> Result<(), String> {
    db.transaction(move |sql| {
        sql.execute("INSERT INTO records(name) VALUES(?)", (name,))?;
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    db.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// Row count on one node (Go: `rowCount` via `TypedNames` length).
fn row_count(db: &Database) -> usize {
    db.read(|sql| {
        let n: i64 = sql.query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))?;
        Ok(n as usize)
    })
    .expect("row count read failed")
}

#[test]
fn writer_shares_under_load() {
    let cluster = Cluster::new(ClusterOptions {
        name: "loadshare".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on both nodes (Go: TypedRecords tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(name TEXT NOT NULL PRIMARY KEY)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Four concurrent writers saturate node1 while the driver inserts
        // the peer's rows on node2; the scope exit joins the writers
        // (Go: writer goroutines + `wg.Wait()` around the remote loop).
        let lat: Mutex<Vec<Duration>> = Mutex::new(Vec::with_capacity(WRITERS * PER_WRITER));
        std::thread::scope(|s| {
            for w in 0..WRITERS {
                let db = cluster.node(0).clone();
                let lat = &lat;
                s.spawn(move || {
                    for i in 0..PER_WRITER {
                        let start = Instant::now();
                        let res = typed_insert(&db, format!("local-{w}-{i}"));
                        lat.lock().unwrap().push(start.elapsed());
                        res.expect("local write failed");
                    }
                });
            }
            for i in 0..REMOTE {
                typed_insert(cluster.node(1), format!("remote-{i}")).expect("remote write failed");
                std::thread::sleep(REMOTE_PACING);
            }
        });

        // Full convergence: the saturated node must apply all 20 remote
        // rows, and the peer must absorb all 240 local rows.
        let converged = wait_until(CONVERGE_TIMEOUT, CONVERGE_POLL, || {
            row_count(cluster.node(0)) == TOTAL && row_count(cluster.node(1)) == TOTAL
        });
        let n = row_count(cluster.node(0));
        assert!(
            converged && n == TOTAL,
            "converged to {n} rows, remote apply starved"
        );

        // Both scheduler classes must acquire on the saturated node, and
        // service debt must stay within the 1s bound (Go: scraped
        // `spedsql_sched_acquisitions_total{class}` and
        // `spedsql_sched_debt_seconds` from `/metrics`).
        let snap = cluster.node(0).scheduler().snapshot();
        eprintln!(
            "node1 scheduler: local_acq={} remote_acq={} debt_nanos={}",
            snap.local.acquisitions, snap.remote.acquisitions, snap.debt_nanos
        );
        assert!(
            snap.local.acquisitions >= (WRITERS * PER_WRITER) as u64,
            "local acquisitions = {}",
            snap.local.acquisitions
        );
        assert!(
            snap.remote.acquisitions > 0,
            "remote class never acquired under local saturation"
        );
        let debt = Duration::from_nanos(snap.debt_nanos);
        assert!(
            debt <= MAX_DEBT,
            "service debt = {debt:?}, exceeds 1s bound"
        );

        // Local-commit latency distribution under load (Go: `t.Logf`).
        let mut lat = lat.into_inner().unwrap();
        assert_eq!(lat.len(), WRITERS * PER_WRITER);
        lat.sort();
        eprintln!(
            "local-commit latency under load: p50={:?} p95={:?} max={:?} (n={})",
            lat[lat.len() / 2],
            lat[lat.len() * 95 / 100],
            lat[lat.len() - 1],
            lat.len()
        );
        assert!(
            lat[lat.len() - 1] <= MAX_LATENCY,
            "max local-commit latency {:?} is absurd",
            lat[lat.len() - 1]
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("loadshare-no-converge");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
