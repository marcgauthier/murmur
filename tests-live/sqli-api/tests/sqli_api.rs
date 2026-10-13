//! Rust port of the Go `sqli-api` live scenario (test-side equivalent).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/sqli-api/`
//! (`sqli_api_test.go`). Go pins the Phase 6 cutover at the running node
//! boundary: former SQL routes (`/v1/query`, `/v1/exec`) reject requests
//! with 410 before parsing them, while the managed typed API stays
//! writable and replicates. The Rust product has no HTTP routes, so this
//! port pins the security property those 410s existed to enforce —
//! untrusted SQL text is never executed — at the layers that do exist:
//! SQL payloads replicate as inert byte-exact data with the schema
//! untouched, and retired-route request bytes sprayed at the replication
//! port die on arrival with no session and no state change (see the
//! scenario README.md for the leg mapping).

use std::net::UdpSocket;
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Table DDL (Go: typed records).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)";
/// Convergence deadline (Go: 20 s `waitTypedCount`).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(20);
/// Convergence poll interval.
const POLL: Duration = Duration::from_millis(100);

/// SQL payloads that must only ever exist as inert data: Go's three
/// exfil/stacked bodies plus DDL attempts (Go: `payloads`).
const PAYLOADS: &[&str] = &[
    r#"{"query":"SELECT * FROM records"}"#,
    r#"{"query":"INSERT INTO records VALUES (1); DROP TABLE records"}"#,
    r#"{"query":"' UNION SELECT secret FROM secrets --"}"#,
    "DROP TABLE live_typed_records",
    "CREATE TABLE pwned(id TEXT)",
    "'; DELETE FROM live_typed_records; --",
];

/// Retired-route request bytes, sprayed at the replication port: they
/// must die on arrival — no session, no execution, no state change
/// (Go: `POST /v1/query` and `POST /v1/exec` -> 410 before parsing).
const ROUTE_BYTES: &[&str] = &[
    "POST /v1/query HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\r\n{\"query\":\"SELECT * FROM records\"}",
    "POST /v1/exec HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\r\n{\"query\":\"INSERT INTO records VALUES (1); DROP TABLE records\"}",
    "{\"query\":\"' UNION SELECT secret FROM secrets --\"}",
];

/// Insert one record (Go: `Cluster.TypedInsert`).
fn insert_name(db: &Database, node: usize, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute("INSERT INTO live_typed_records(name) VALUES(?)", [name])?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
}

/// Count rows named `name` on one node (Go: `Cluster.TypedCount`).
fn count_name(db: &Database, name: &str) -> usize {
    let name = name.to_owned();
    db.read(move |sql| {
        let mut stmt = sql.prepare("SELECT COUNT(*) FROM live_typed_records WHERE name = ?")?;
        let mut found = stmt.query([name])?;
        match found.next()? {
            Some(row) => Ok(row.get::<_, i64>(0)? as usize),
            None => Ok(0),
        }
    })
    .expect("count name failed")
}

/// Full schema DDL snapshot on one node (proves payloads created,
/// dropped, or altered nothing).
fn schema_snapshot(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt =
            sql.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for ddl in rows {
            out.push(ddl?);
        }
        Ok(out)
    })
    .expect("read schema failed")
}

/// Live session count (retired-route bytes must attach none).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication enabled")
        .status()
        .peers
}

/// Poll until `name` counts `want` on every node (Go: `waitTypedCount`).
fn wait_typed_count(cluster: &Cluster, name: &str, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        (0..cluster.len()).all(|i| count_name(cluster.node(i), name) == want)
    });
    assert!(
        converged,
        "typed count for {name:?} did not reach {want} on all nodes within {timeout:?}"
    );
}

/// Poll exact total counts plus ordered-name equality.
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        let mut first: Option<Vec<String>> = None;
        for i in 0..cluster.len() {
            let got: Vec<String> = cluster
                .node(i)
                .read(|sql| {
                    let mut stmt =
                        sql.prepare("SELECT name FROM live_typed_records ORDER BY name")?;
                    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                    let mut out = Vec::new();
                    for name in rows {
                        out.push(name?);
                    }
                    Ok(out)
                })
                .expect("read names failed");
            if got.len() != want {
                return false;
            }
            match &first {
                None => first = Some(got),
                Some(prev) if *prev == got => {}
                _ => return false,
            }
        }
        true
    });
    assert!(
        converged,
        "nodes did not converge on {want} rows with equal names within {timeout:?}"
    );
}

#[test]
fn sql_application_routes_removed() {
    let cluster = Cluster::new(ClusterOptions {
        name: "sqli-api-removed".into(),
        num_nodes: 2,
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
        // Same schema on every node (Go: typed fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Managed write before any SQL probe (Go: `before-sql-probe`).
        insert_name(cluster.node(0), 1, "before-sql-probe");
        wait_typed_count(&cluster, "before-sql-probe", 1, CONVERGE_TIMEOUT);
        let schema_before: Vec<_> = (0..cluster.len())
            .map(|i| schema_snapshot(cluster.node(i)))
            .collect();

        // SQL payloads travel the managed typed path and land as inert
        // byte-exact data on every node (Go: the 410 bodies prove the
        // routes never parse/apply; here the payloads must never
        // execute wherever they land).
        for payload in PAYLOADS {
            insert_name(cluster.node(0), 1, payload);
            wait_typed_count(&cluster, payload, 1, CONVERGE_TIMEOUT);
        }
        for i in 0..cluster.len() {
            for payload in PAYLOADS {
                assert_eq!(
                    count_name(cluster.node(i), payload),
                    1,
                    "node{} payload {payload:?} count != 1 (executed or mangled?)",
                    i + 1
                );
            }
            assert_eq!(
                schema_snapshot(cluster.node(i)),
                schema_before[i],
                "node{} schema changed under SQL payloads (DDL executed?)",
                i + 1
            );
        }
        eprintln!("{} SQL payloads inert on all nodes, schemas untouched", PAYLOADS.len());

        // Retired-route request bytes at the replication port: dead on
        // arrival — no session attaches, no rows appear, no schema
        // changes (Go: 410 before parsing or applying contents).
        let repl = cluster.node_info(0).repl_addr;
        let peers_before: Vec<_> = (0..cluster.len())
            .map(|i| peer_count(&cluster, i))
            .collect();
        let sock = UdpSocket::bind("127.0.0.1:0").expect("probe socket");
        sock.connect(repl).expect("probe connect");
        for _ in 0..5 {
            for bytes in ROUTE_BYTES {
                let _ = sock.send(bytes.as_bytes());
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        std::thread::sleep(Duration::from_secs(2));
        for i in 0..cluster.len() {
            assert_eq!(
                peer_count(&cluster, i),
                peers_before[i],
                "node{} peers moved under retired-route bytes (session attached?)",
                i + 1
            );
            assert_eq!(
                schema_snapshot(cluster.node(i)),
                schema_before[i],
                "node{} schema changed under retired-route bytes",
                i + 1
            );
        }
        wait_converged(&cluster, 1 + PAYLOADS.len(), CONVERGE_TIMEOUT);
        eprintln!("retired-route bytes dead on arrival: no sessions, no state change");

        // Managed write after the probes still replicates (Go:
        // `after-sql-probe`), with per-node counts exact.
        insert_name(cluster.node(0), 1, "after-sql-probe");
        wait_typed_count(&cluster, "after-sql-probe", 1, CONVERGE_TIMEOUT);
        for i in 0..cluster.len() {
            for name in ["before-sql-probe", "after-sql-probe"] {
                assert_eq!(
                    count_name(cluster.node(i), name),
                    1,
                    "node{} typed count for {name:?} != 1",
                    i + 1
                );
            }
        }
        eprintln!("PASS: SQL routes dead, typed API writable and replicating");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("sqli-api-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
