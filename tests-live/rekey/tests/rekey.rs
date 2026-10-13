//! Rust port of the Go `rekey` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/rekey/`
//! (`README.md` + `rekey_test.go`:
//! `TestTypedStorageKeyRekeySurvivesRestart`,
//! `TestRekeyAwaitUnlockRestartsWithKeyID`). A single node rotates its
//! application (master) storage key without downtime: records written
//! before and after rotation read back after reopening with the new key,
//! and the retired key is rejected. The second leg covers the
//! await-unlock/SIGKILL recovery path.
//!
//! Plain SQL replaces the Go typed-records HTTP API; rotation is
//! `Database::rotate_master_key`, restart is in-process `close()` plus a
//! direct `Database::open` with the new key (the cluster keeps the
//! pre-rotation key), and the retired-key leg asserts `Error::WrongKey`.
//! The Rust product has no key IDs and no Remote Unlock endpoint, so the
//! key-ID assertions (active `ApplicationKeyID`, unlock-by-ID) cannot be
//! ported; see the scenario README.md for the full deviation list.

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Error, Migration, Options};
use murmur_live_harness::{fixture, wait_until, Cluster, ClusterOptions};

/// Pre-rotation application key (also the cluster default).
const OLD_KEY: [u8; 32] = [9; 32];
/// Post-rotation application key (Go: random 32 bytes; fixed here so the
/// old-vs-new distinction is deterministic).
const NEW_KEY: [u8; 32] = [0x5a; 32];
/// Typed table (Go: `Config.Tables`; here plain SQL with the record name
/// as the row key, like the Go `TypedInsert`/`TypedCount` name argument).
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

/// Lowercase hex (for the fixture `--key-hex` flag; avoids a new dep).
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

/// Require that `key` no longer opens `data_dir` (Go: `assertNeverReady`
/// after rewriting the config to the retired key: the daemon exits on
/// failed unlock instead of serving health).
fn assert_key_rejected(data_dir: &std::path::Path, key: [u8; 32]) {
    match Database::open(data_dir, key, Options::default()) {
        Err(Error::WrongKey) => {}
        Err(other) => panic!("retired-key open failed with {other:?}, want WrongKey"),
        Ok(db) => {
            let _ = db.close();
            panic!("store unexpectedly accepted the retired key");
        }
    }
}

/// Go `TestTypedStorageKeyRekeySurvivesRestart`: rotate, restart on the
/// new key with pre/post-rotation rows intact, retired key rejected.
#[test]
fn typed_storage_key_rekey_survives_restart() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "rekey-restart".into(),
        num_nodes: 1,
        key: OLD_KEY,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cluster
            .node(0)
            .migrate(Migration::new(DDL))
            .expect("migrate typed table");
        cluster.node(0).flush().expect("flush after migrate");
        typed_insert(cluster.node(0), "typed-before-rotate");

        // Rotate without downtime (Go: `RotateKey` + `EncryptionStatus`
        // active-key-ID check; the Rust product has no key IDs, so the
        // new-key-opens / old-key-rejected legs below carry that proof).
        cluster
            .node(0)
            .rotate_master_key(NEW_KEY)
            .expect("rotate master key");
        typed_insert(cluster.node(0), "typed-after-rotate");

        // Restart on the new key (Go: `StopNode`, rewrite config to the
        // new key, `StartNode`, `WaitNodeReady`).
        let data_dir = cluster.node_info(0).data_dir.clone();
        cluster.stop_node(0);
        let db = Database::open(&data_dir, NEW_KEY, Options::default())
            .expect("reopen with new key");
        assert_eq!(
            typed_count(&db, "typed-before-rotate"),
            1,
            "pre-rotation row after restart"
        );
        assert_eq!(
            typed_count(&db, "typed-after-rotate"),
            1,
            "post-rotation row after restart"
        );
        db.close().expect("close reopened store");

        // The pre-rotation key no longer opens the store after rotation.
        assert_key_rejected(&data_dir, OLD_KEY);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("rekey-restart");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Go `TestRekeyAwaitUnlockRestartsWithKeyID`: rotation survives an
/// unclean (SIGKILL) restart and the reopened store takes the new key.
#[test]
fn rekey_sigkill_restarts_with_new_key() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "rekey-sigkill".into(),
        num_nodes: 1,
        key: OLD_KEY,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // No await-unlock leg: `Database::open` takes the key directly
        // (the Rust product has no Remote Unlock endpoint), and key IDs
        // do not exist, so the unlock-by-ID 401 assertions are dropped.
        cluster
            .node(0)
            .migrate(Migration::new(DDL))
            .expect("migrate typed table");
        cluster.node(0).flush().expect("flush after migrate");
        typed_insert(cluster.node(0), "pre-rotate");
        cluster
            .node(0)
            .rotate_master_key(NEW_KEY)
            .expect("rotate master key");
        // `rotate_master_key` is synchronous (flush + worker ack), so the
        // rotation is durable at return and cannot depend on a later
        // clean shutdown.
        let data_dir = cluster.node_info(0).data_dir.clone();
        cluster.stop_node(0);

        // SIGKILL (not graceful stop): a fresh process opens the rotated
        // store with the new key and is killed while holding it open;
        // nothing may be lost and the new key stays required.
        let dir_s = data_dir.to_string_lossy().to_string();
        let key_hex = hex_encode(&NEW_KEY);
        let ready_path = cluster.node_info(0).dir.join("fixture.ready");
        let ready_s = ready_path.to_string_lossy().to_string();
        let args = [
            "open",
            "--dir",
            dir_s.as_str(),
            "--key-hex",
            key_hex.as_str(),
            "--mode",
            "existing",
            "--ready",
            ready_s.as_str(),
            "--hold-secs",
            "120",
        ];
        let mut child = fixture::spawn(&args).expect("spawn testnode fixture");
        if !wait_until(
            Duration::from_secs(30),
            Duration::from_millis(50),
            || ready_path.exists(),
        ) {
            let _ = fixture::kill(&mut child);
            let _ = fixture::wait_timeout(&mut child, Duration::from_secs(10));
            panic!("testnode fixture never opened the rotated store with the new key");
        }
        fixture::kill(&mut child).expect("SIGKILL fixture");
        let status = fixture::wait_timeout(&mut child, Duration::from_secs(10))
            .expect("reap fixture")
            .expect("fixture still running after SIGKILL");
        assert!(
            !status.success(),
            "fixture unexpectedly exited cleanly after SIGKILL"
        );

        // Reopen with the new key: pre-rotation row present, new writes work.
        let db = Database::open(&data_dir, NEW_KEY, Options::default())
            .expect("reopen with new key after SIGKILL");
        assert_eq!(
            typed_count(&db, "pre-rotate"),
            1,
            "pre-rotation row after SIGKILL restart"
        );
        typed_insert(&db, "post-restart");
        assert_eq!(
            typed_count(&db, "post-restart"),
            1,
            "post-restart write after SIGKILL restart"
        );
        db.close().expect("close reopened store");

        // The retired key is rejected after the unclean restart too.
        assert_key_rejected(&data_dir, OLD_KEY);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("rekey-sigkill");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
