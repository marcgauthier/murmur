//! Rust port of the Go `encryption` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/encryption/`
//! (`README.md` + `encryption_test.go`:
//! `TestTypedEncryptedStoreKeepsRecordsEncryptedAcrossRestart`). A single
//! node stores one typed row carrying a secret marker, stops, and its
//! whole node directory is scanned for the marker in plaintext; the node
//! restarts, a wrong-key open must be rejected, and the correct key
//! unlocks the row again.
//!
//! Plain SQL replaces the Go typed-records HTTP API, and the wrong-key
//! leg is a direct `Database::open` asserting `Error::WrongKey` (the
//! Rust product has no Remote Unlock endpoint); see the scenario
//! README.md for the full deviation list.

use murmur_live_harness::sqlite_session::{Database, Error, Migration, Options};
use murmur_live_harness::{Cluster, ClusterOptions};

/// Secret marker (Go: identical constant). Long and unique so a
/// substring hit on disk is unambiguous evidence of leaked plaintext.
const SECRET_MARKER: &[u8] = b"TYPED-CONFIDENTIAL-PLAINTEXT-SECRET-246813579";
/// Typed table (Go: `Config.Tables`; here plain SQL with the marker as
/// the row key, like the Go `TypedInsert`/`TypedCount` name argument).
const DDL: &str = "CREATE TABLE records(name TEXT PRIMARY KEY)";

/// Typed insert on one node (Go: `Cluster.TypedInsert`).
fn typed_insert(db: &Database, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute("INSERT INTO records(name) VALUES(?)", [name])?;
        Ok(())
    })
    .expect("typed insert failed");
    db.flush().expect("flush after typed insert");
}

/// Row count by name (Go: `Cluster.TypedCount`).
fn typed_count(db: &Database, name: &str) -> i64 {
    let name = name.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT count(*) FROM records WHERE name = ?",
            [name],
            |r| r.get::<_, i64>(0),
        )?)
    })
    .unwrap_or(-1)
}

/// True when any regular file under `root` contains `marker`
/// (Go: `containsPlaintext`; byte windows instead of string search so
/// binary spool files are covered too).
fn contains_plaintext(root: &std::path::Path, marker: &[u8]) -> std::io::Result<bool> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path)? {
                stack.push(entry?.path());
            }
        } else if path.is_file() {
            let data = std::fs::read(&path)?;
            if data.windows(marker.len()).any(|w| w == marker) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[test]
fn typed_encrypted_store_keeps_records_encrypted_across_restart() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "encryption".into(),
        num_nodes: 1,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cluster
            .node(0)
            .migrate(Migration::new(DDL))
            .expect("migrate typed table");
        cluster
            .node(0)
            .flush()
            .expect("flush after migrate");
        let marker = std::str::from_utf8(SECRET_MARKER).expect("marker is ASCII");
        typed_insert(cluster.node(0), marker);
        assert_eq!(
            typed_count(cluster.node(0), marker),
            1,
            "typed secret before restart"
        );

        // Stop, then confirm the marker occurs in no durable file.
        let dir = cluster.node_info(0).dir.clone();
        let data_dir = cluster.node_info(0).data_dir.clone();
        cluster.stop_node(0);
        let has_plaintext =
            contains_plaintext(&dir, SECRET_MARKER).expect("scan node directory");
        assert!(
            !has_plaintext,
            "typed plaintext secret marker found on disk in {}",
            dir.display()
        );

        // A wrong key must be rejected (Go: non-200 unlock response).
        match Database::open(&data_dir, [0; 32], Options::default()) {
            Err(Error::WrongKey) => {}
            Err(other) => panic!("wrong-key open failed with {other:?}, want WrongKey"),
            Ok(db) => {
                let _ = db.close();
                panic!("typed database unexpectedly accepted the wrong key");
            }
        }

        // The correct key unlocks and the row reads back.
        cluster.start_node(0).expect("restart with correct key");
        assert_eq!(
            typed_count(cluster.node(0), marker),
            1,
            "typed secret after restart"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("encryption");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
