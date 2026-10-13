//! Rust port of the Go `large-payload` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/large-payload/`
//! (`large_payload_test.go`). 250 small rows plus one row carrying a 1.5 MiB
//! value commit on one node and must replicate exactly (row count plus
//! value length and content) to its mesh peer. Plain SQL replaces the Go
//! typed-records HTTP API (see the scenario README.md for the deviations).

use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{Cluster, ClusterOptions};

/// Bulk rows committed before the large value (Go: `bulkRows = 250`).
const BULK_ROWS: usize = 250;
/// Large value size in bytes (Go: `largeValueBytes = 1536 * 1024`).
const LARGE_VALUE_BYTES: usize = 1536 * 1024;
/// Large row id (Go uses a random row id; a fixed id is equivalent here
/// because the cluster starts empty and only one large row exists).
const LARGE_ID: &str = "large-payload-row";
/// Replication deadline (Go: 60 s).
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 50 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Row count on one node (Go: `len(TypedContentionRows)`).
fn row_count(db: &Database) -> usize {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM records", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .expect("row count")
}

/// Read one row's `phone` value by id (Go: `TypedContentionRead`).
fn read_phone(db: &Database, id: &str) -> Option<String> {
    let id = id.to_string();
    db.read(move |sql| {
        let mut stmt = sql.prepare("SELECT phone FROM records WHERE id = ?1")?;
        let mut rows = stmt.query([id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row.get::<_, String>(0)?)),
            None => Ok(None),
        }
    })
    .expect("read phone")
}

#[test]
fn large_payload() {
    let cluster = Cluster::new(ClusterOptions {
        name: "large-payload".into(),
        num_nodes: 2,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on both nodes (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Large value: repeated pattern plus 'x' padding to the exact size
        // (Go: `strings.Repeat("murmur-large-payload-", ...)` + pad).
        const PATTERN: &str = "murmur-large-payload-";
        let mut large_value = PATTERN.repeat(LARGE_VALUE_BYTES / PATTERN.len());
        large_value.push_str(&"x".repeat(LARGE_VALUE_BYTES - large_value.len()));
        assert_eq!(large_value.len(), LARGE_VALUE_BYTES);

        // 250 bulk rows plus the large row, each in its own transaction on
        // node 0 (Go: one `TypedContentionInsert` call per row).
        for i in 0..BULK_ROWS {
            let id = format!("bulk-{i:04}");
            let name = format!("bulk-{i:04}");
            let score = i.to_string();
            cluster
                .node(0)
                .transaction(move |sql| {
                    sql.execute(
                        "INSERT INTO records(id, name, phone) VALUES(?1, ?2, ?3)",
                        (&id, &name, &score),
                    )?;
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("insert bulk row {i}: {e}"));
        }
        let payload = large_value.clone();
        cluster
            .node(0)
            .transaction(move |sql| {
                sql.execute(
                    "INSERT INTO records(id, name, phone) VALUES(?1, ?2, ?3)",
                    ("large-payload-row", "large-payload", &payload),
                )?;
                Ok(())
            })
            .expect("insert large value");
        cluster.node(0).flush().expect("flush after inserts");

        // Wait for node 1 to hold all rows with the exact large value
        // (Go: row count + length/SHA-256 check in the 60 s poll loop).
        let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
        loop {
            let count = row_count(cluster.node(1));
            if count == BULK_ROWS + 1 {
                match read_phone(cluster.node(1), LARGE_ID) {
                    Some(got) => {
                        assert_eq!(
                            got.len(),
                            LARGE_VALUE_BYTES,
                            "replicated value length = {}, want {LARGE_VALUE_BYTES}",
                            got.len()
                        );
                        assert!(
                            got == large_value,
                            "replicated value content differs from the committed payload"
                        );
                        eprintln!(
                            "Node 2 holds {} rows with the exact {}-byte payload",
                            count, LARGE_VALUE_BYTES
                        );
                        return;
                    }
                    None => panic!("node 2 holds all rows but the large row is missing"),
                }
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out waiting for {} bulk rows and the large payload on node 2 (count={count})",
                    BULK_ROWS + 1
                );
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
