//! Rust port of the Go `unlock-abuse` live scenario (test-side equivalent).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/unlock-abuse/`
//! (`unlock_abuse_test.go`). Go abuses the Remote Unlock HTTP endpoint:
//! wrong-key and unknown-key-id attempts must return byte-identical
//! generic 401s with no key material, then honest unlock succeeds with an
//! audit line. The Rust product has no unlock endpoint (`Database::open`
//! takes the key directly), so this port proves the same anti-oracle
//! property on direct opens of a stopped node's store: distinct wrong
//! keys fail with byte-identical generic messages leaking no key bytes,
//! malformed keys fail cleanly, and the honest reopen serves intact data
//! (see the scenario README.md for the leg mapping).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{
    Database, Migration, OpenMode, Options,
};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Table DDL (Go: typed contention rows).
const TABLE_DDL: &str = "CREATE TABLE contention(id TEXT PRIMARY KEY, name TEXT NOT NULL)";
/// Convergence deadline (Go: 10 s `waitAllCount`).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(10);
/// Convergence poll interval (Go: 50 ms).
const POLL: Duration = Duration::from_millis(50);

/// Lowercase hex of key bytes (leak-scan alphabet).
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

/// Bare open options for unlock probes: existing store, no replication.
fn probe_options() -> Options {
    Options {
        mode: OpenMode::Existing,
        ..Options::default()
    }
}

/// Attempt to open `dir` with `key`; returns the failure message.
/// Panics when the open unexpectedly succeeds.
fn unlock_attempt(dir: &std::path::Path, key: &[u8]) -> String {
    match Database::open(dir, key, probe_options()) {
        Ok(_) => panic!("open with a wrong key succeeded ({} key bytes)", key.len()),
        Err(e) => format!("{e}"),
    }
}

/// Assert a failure message carries no oracle: generic (non-empty) and
/// free of every key's hex (tried and real alike).
fn assert_no_oracle(what: &str, message: &str, secrets: &[String]) {
    assert!(!message.is_empty(), "{what}: failure message is empty");
    let lower = message.to_lowercase();
    for secret in secrets {
        assert!(
            !lower.contains(secret),
            "{what}: failure message leaks key material: {message:?}"
        );
    }
}

/// Ordered contention rows on one node (Go: `TypedContentionRows`).
fn rows(db: &Database) -> Vec<(String, String)> {
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
    .expect("read contention rows failed")
}

/// Insert one row (Go: `TypedContentionInsert`).
fn insert_row(db: &Database, node: usize, id: String, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute("INSERT INTO contention(id, name) VALUES(?, ?)", (id, name))?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
}

/// Poll until every node reports exactly `want` rows (Go: `waitAllCount`).
fn wait_all_count(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        (0..cluster.len()).all(|i| rows(cluster.node(i)).len() == want)
    });
    assert!(
        converged,
        "cluster failed to converge to {want} rows within {timeout:?}"
    );
}

/// Require identical ordered contents on all nodes (Go: `assertEqualDigests`).
fn assert_equal_digests(cluster: &Cluster) {
    let want = rows(cluster.node(0));
    for i in 1..cluster.len() {
        assert_eq!(rows(cluster.node(i)), want, "node{} contents != node1 contents", i + 1);
    }
}

#[test]
fn unlock_abuse() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "unlock-abuse".into(),
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
        // Same schema on every node (Go: typed contention fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Positive control: honest path works before any abuse.
        insert_row(cluster.node(0), 1, format!("{:032x}", 1), "baseline");
        wait_all_count(&cluster, 1, CONVERGE_TIMEOUT);
        assert_equal_digests(&cluster);

        // Lock node 1: stop it; all unlock attempts below open its store
        // directly (Go: restart-without-unlock + HTTP attempts).
        cluster.stop_node(0);
        let data_dir = cluster.node_info(0).data_dir.clone();
        let real_key = [9u8; 32];
        let real_hex = hex(&real_key);

        // Wrong-key attempts: distinct 32-byte keys must fail with
        // byte-identical generic messages leaking no key bytes (Go:
        // identical 401 bodies across wrong-key/unknown-id guesses).
        let mut wrong_a = [0xffu8; 32];
        wrong_a[31] = 0xf0;
        let wrong_keys = vec![wrong_a, [0x01u8; 32], [0x02u8; 32]];
        let mut messages = Vec::new();
        for key in &wrong_keys {
            let message = unlock_attempt(&data_dir, key);
            assert_no_oracle("wrong-key", &message, &[hex(key), real_hex.clone()]);
            messages.push(message);
        }
        assert_eq!(
            messages[0], messages[1],
            "unlock oracle: wrong-key messages differ:\n{:?}\n{:?}",
            messages[0], messages[1]
        );
        assert_eq!(
            messages[0], messages[2],
            "unlock oracle: wrong-key messages differ:\n{:?}\n{:?}",
            messages[0], messages[2]
        );
        eprintln!("wrong-key attempts: identical generic failures, no key material");

        // Malformed credentials (Go: unknown-key-id leg): must fail
        // cleanly with no key bytes in the message. Cross-identity with
        // the wrong-key message is NOT asserted: length validation
        // reflects only the attacker's own input length, which cannot
        // oracle key VALUE.
        for key in [vec![0x03u8; 16], vec![0x04u8; 64]] {
            let message = unlock_attempt(&data_dir, &key);
            assert_no_oracle("malformed-key", &message, &[hex(&key), real_hex.clone()]);
        }
        eprintln!("malformed-key attempts: clean failures, no key material");

        // Honest unlock succeeds (and proves the failures above reflect
        // real key enforcement, not a wedged store): the node serves
        // intact data and rejoins the mesh.
        cluster.start_node(0).expect("honest reopen");
        cluster.mesh().expect("re-mesh after reopen");
        wait_all_count(&cluster, 1, CONVERGE_TIMEOUT);
        assert_equal_digests(&cluster);
        insert_row(cluster.node(0), 1, format!("{:032x}", 2), "post-unlock");
        wait_all_count(&cluster, 2, CONVERGE_TIMEOUT);
        assert_equal_digests(&cluster);
        eprintln!("PASS: unlock abuse failed closed, honest reopen intact");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("unlock-abuse-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
