//! Rust port of the Go `tampered-backup` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/tampered-backup/`
//! (`README.md` + `tampered_test.go`: `TestTamperedBackupsFailClosed`). One
//! `#[test]` fn: a single node seeds 30 rows, stops, and is backed up
//! offline through the library. Three tampered copies of the backup (byte
//! flip in a payload file, manifest edit, payload truncation) must each
//! fail `Database::restore` and `Database::verify_backup` with no partial
//! state, while the pristine backup restores exactly (row count plus a
//! canonical digest of the sorted record names).
//!
//! Plain SQL replaces the Go typed-records HTTP API (the Rust product has
//! no HTTP server). The Rust backup format is a directory
//! (`manifest`/`keys.enc`/`segments/`), not a `.tar.gz` archive, so the Go
//! gzip-magic and `backup-metadata.json` tampers become segment-byte and
//! manifest tampers; there is no restore-intent file, so fail-closed is
//! asserted as "restore target directory does not exist" (the product
//! removes `dst` on verification failure). See the scenario README.md.

use std::fs;
use std::path::{Path, PathBuf};

use murmur_live_harness::sqlite_session::{
    Database, Migration, OpenMode, Options, RestoreOptions,
};
use murmur_live_harness::{Cluster, ClusterOptions};
use rand::RngCore;

/// Seed rows (Go: 30 typed rows).
const NUM_ROWS: usize = 30;
/// Mesh-leg DDL: plain replicated rows (Go: `TypedInsert` names).
const NAMES_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)";

/// Typed insert on one node (Go: `Cluster.TypedInsert`).
fn typed_insert(db: &Database, name: String) {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_typed_records(name) VALUES(?)",
            [name],
        )?;
        Ok(())
    })
    .expect("typed insert failed");
    db.flush().expect("flush after typed insert");
}

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
fn typed_names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM live_typed_records ORDER BY name")?;
        let mapped = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in mapped {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read typed names")
}

/// Canonical digest of the record names (Go: `namesDigest`, SHA-256 over
/// sorted `\n`-joined names). FNV-1a/64 over the same canonical byte
/// stream: no hash crate is in scope, and only pre/post equality matters.
fn names_digest(names: &[String]) -> String {
    let mut sorted = names.to_vec();
    sorted.sort();
    let mut hash: u64 = 0xcbf29ce484222325;
    for name in &sorted {
        for byte in name.as_bytes().iter().chain(std::iter::once(&b'\n')) {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    format!("{hash:016x}")
}

/// Open a stopped node's data dir without replication (Go: `openNodeDir`
/// offline library open for backup/restore legs).
fn open_offline(
    data_dir: &Path,
    key: [u8; 32],
    database_id: [u8; 16],
    node_id: [u8; 16],
) -> Database {
    Database::open(
        data_dir,
        key,
        Options {
            mode: OpenMode::Existing,
            database_id: Some(database_id),
            node_id: Some(node_id),
            ..Options::default()
        },
    )
    .expect("offline open of node data dir")
}

/// Recursive directory copy for tampered-backup fixtures (the pristine
/// backup must stay immutable).
fn copy_dir_all(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).expect("create tampered backup dir");
    let mut entries: Vec<_> = fs::read_dir(src)
        .expect("read backup dir")
        .collect::<std::io::Result<_>>()
        .expect("list backup dir");
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let kind = entry.file_type().expect("entry file type");
        if kind.is_dir() {
            copy_dir_all(&from, &to);
        } else if kind.is_file() {
            fs::copy(&from, &to).expect("copy backup file");
        }
    }
}

/// Payload file to tamper: the first segment file, falling back to
/// `keys.enc` if the backup unexpectedly holds no segments.
fn payload_file(backup_dir: &Path) -> PathBuf {
    let segments = backup_dir.join("segments");
    if segments.is_dir() {
        let mut files: Vec<PathBuf> = fs::read_dir(&segments)
            .expect("read segments dir")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file())
            .collect();
        files.sort();
        if let Some(first) = files.into_iter().next() {
            return first;
        }
    }
    let keys = backup_dir.join("keys.enc");
    assert!(
        keys.is_file(),
        "backup has neither segment files nor keys.enc"
    );
    keys
}

/// Restore and verify of `backup_dir` must both fail, the restore target
/// must not exist afterwards (Go: `assertTargetUntouched` — the product
/// removes `dst` on verification failure), and no `.verify-tmp-*` residue
/// may remain in the backup's parent dir.
fn assert_fails_closed(backup_dir: &Path, target: &Path, key: [u8; 32], leg: &str) {
    assert!(
        !target.exists(),
        "{leg}: restore target must not exist before the attempt"
    );
    match Database::restore(backup_dir, target, key) {
        Ok(()) => panic!("{leg}: tampered restore succeeded, want integrity failure"),
        Err(e) => eprintln!("{leg}: tampered restore rejected: {e}"),
    }
    assert!(
        !target.exists(),
        "{leg}: failed restore left partial state at {}",
        target.display()
    );
    match Database::verify_backup(backup_dir, key) {
        Ok(()) => panic!("{leg}: tampered verify_backup succeeded, want integrity failure"),
        Err(e) => eprintln!("{leg}: tampered verify rejected: {e}"),
    }
    let parent = backup_dir
        .parent()
        .expect("backup dir must have a parent");
    for entry in fs::read_dir(parent).expect("read backup parent dir") {
        let name = entry.expect("dir entry").file_name();
        let name = name.to_string_lossy().into_owned();
        assert!(
            !name.starts_with(".verify-tmp-"),
            "{leg}: verify left temp residue behind: {name}"
        );
    }
}

#[test]
fn tampered_backup() {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "tampered-backup".into(),
        num_nodes: 1,
        key,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cluster
            .node(0)
            .migrate(Migration::new(NAMES_DDL))
            .expect("migrate");
        cluster.node(0).flush().expect("flush after migrate");
        for i in 0..NUM_ROWS {
            typed_insert(cluster.node(0), format!("tampered-row-{i:02}"));
        }
        let names = typed_names(cluster.node(0));
        assert_eq!(names.len(), NUM_ROWS, "seeded row count");
        let want_digest = names_digest(&names);

        // Offline backup of the stopped node through the library.
        cluster.stop_node(0);
        let info = cluster.node_info(0);
        let (data_dir, node_id) = (info.data_dir.clone(), info.node_id);
        let database_id = cluster.database_id();
        let runtime_dir = cluster.runtime_dir().clone();
        let offline = open_offline(&data_dir, key, database_id, node_id);
        let pristine = runtime_dir.join("pristine-backup");
        offline.backup(&pristine).expect("offline backup");
        offline.close().expect("close offline handle");

        // The pristine backup verifies cleanly (positive control setup).
        Database::verify_backup(&pristine, key).expect("pristine backup must verify");

        // Payload tamper (Go: gzip-magic flip): flip the first two bytes
        // of a payload file so the stream is rejected before use.
        let payload_tampered = runtime_dir.join("tampered-payload");
        copy_dir_all(&pristine, &payload_tampered);
        let target = payload_file(&payload_tampered);
        let mut bytes = fs::read(&target).expect("read payload file");
        assert!(bytes.len() >= 2, "payload file too small to flip");
        bytes[0] ^= 0xFF;
        bytes[1] ^= 0xFF;
        fs::write(&target, &bytes).expect("write flipped payload");
        assert_fails_closed(
            &payload_tampered,
            &runtime_dir.join("target-payload"),
            key,
            "payload-flip",
        );

        // Metadata tamper (Go: corrupt `backup-metadata.json`): flip a
        // byte in the middle of the store manifest.
        let meta_tampered = runtime_dir.join("tampered-manifest");
        copy_dir_all(&pristine, &meta_tampered);
        let manifest = meta_tampered.join("manifest");
        let mut bytes = fs::read(&manifest).expect("read manifest");
        assert!(!bytes.is_empty(), "manifest must be non-empty");
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        fs::write(&manifest, &bytes).expect("write tampered manifest");
        assert_fails_closed(
            &meta_tampered,
            &runtime_dir.join("target-manifest"),
            key,
            "manifest-edit",
        );

        // Truncation tamper (Go: late-payload flip surfacing mid-read):
        // cut a payload file short; the restore must still fail with no
        // partial state published.
        let trunc_tampered = runtime_dir.join("tampered-truncated");
        copy_dir_all(&pristine, &trunc_tampered);
        let target = payload_file(&trunc_tampered);
        let len = fs::metadata(&target).expect("stat payload").len();
        let new_len = len.saturating_sub(len / 2 + 1);
        fs::OpenOptions::new()
            .write(true)
            .open(&target)
            .expect("open payload for truncation")
            .set_len(new_len)
            .expect("truncate payload");
        assert_fails_closed(
            &trunc_tampered,
            &runtime_dir.join("target-truncated"),
            key,
            "payload-truncate",
        );

        // Positive control: the pristine backup restores exactly under a
        // fresh writer identity (row count plus canonical digest).
        let fresh: [u8; 16] = rand::random();
        assert_ne!(fresh, node_id, "fresh identity must differ");
        let staging = runtime_dir.join("staging");
        Database::restore_with_options(
            &pristine,
            &staging,
            key,
            RestoreOptions {
                new_database_id: None,
                new_node_id: Some(fresh),
            },
        )
        .expect("pristine restore");
        let restored = Database::open(
            &staging,
            key,
            Options {
                mode: OpenMode::Existing,
                database_id: Some(database_id),
                node_id: Some(fresh),
                ..Options::default()
            },
        )
        .expect("open restored data");
        let restored_names = typed_names(&restored);
        assert_eq!(restored_names.len(), NUM_ROWS, "restored row count");
        assert_eq!(
            names_digest(&restored_names),
            want_digest,
            "restored digest must match pre-backup digest"
        );
        assert_eq!(restored.node_id(), fresh, "restored identity must be fresh");
        restored.close().expect("close restored handle");
        eprintln!("PASS: tampered backups fail closed; pristine backup restores exactly");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("tampered-backup");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
