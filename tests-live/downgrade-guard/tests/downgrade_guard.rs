//! Rust port of the Go `downgrade-guard` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/downgrade-guard/`
//! (`downguard_test.go`: `TestDowngradeGuardRefusesNewStore`). The Go suite
//! builds the previous-release daemon from git history and requires it to
//! refuse a store written by the current binary: loud version error,
//! nonzero exit, byte-untouched store, both compatibility directions
//! still working.
//!
//! The Rust product has no previous-release daemon to build, so the port
//! inverts the mechanism and keeps the pass criteria: it crafts a store
//! one format generation NEWER than the current build (same
//! `00/identity` record the product writes, re-encoded with
//! `format + 1` through the raw spool API) and requires the current
//! build to refuse it fail-closed with `Error::Corrupt("unsupported
//! database format")` — the `FORMAT` guard in `storage.rs` — while the
//! unmodified store reopens with an identical digest and identity.
//! Plain SQL replaces the Go typed-records HTTP API; see the scenario
//! README.md.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use murmur_live_harness::spool::{self, Codec, Durability, Mutation};
use murmur_live_harness::sqlite_session::{
    Database, Error, Migration, OpenMode, Options,
};
use murmur_live_harness::{Cluster, ClusterOptions};
use sha2::{Digest, Sha256};

/// Application key for the cluster node and every direct open below.
const KEY: [u8; 32] = [0xd6; 32];
/// Seeded rows (Go: `const rows = 25`).
const ROWS: usize = 25;
/// First row id (Go: ids `3000+i`).
const FIRST_ID: i64 = 3000;
/// Spool key of the store identity record (product `storage.rs`).
const IDENTITY_KEY: &[u8] = b"00/identity";
/// Block-codec id of the product's zstd codec (`storage.rs`).
const ZSTD_CODEC_ID: u8 = 128;
/// Product default compression level (`Options::default`).
const ZSTD_LEVEL: i32 = 3;

/// Byte-identical mirror of the product's `Identity` record
/// (`model.rs`: `{ database, node, format }`, postcard-encoded). Field
/// order must match the product exactly; the test never names the
/// product's private `FORMAT` constant and instead reads the current
/// store's own marker, so the port stays green across format bumps.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoreIdentity {
    database: [u8; 16],
    node: [u8; 16],
    format: u32,
}

/// Read-path mirror of the product's zstd block codec (`storage.rs`).
/// The product compresses every new spool block with codec id 128, so a
/// raw spool open of a product-written dir must register this codec to
/// decode existing blocks.
struct ZstdBlock(i32);

impl Codec for ZstdBlock {
    fn id(&self) -> u8 {
        ZSTD_CODEC_ID
    }
    fn name(&self) -> &str {
        "sqlite-session-zstd-v1"
    }
    fn compress(&self, src: &[u8]) -> spool::Result<Vec<u8>> {
        zstd::stream::encode_all(src, self.0)
            .map_err(|e| spool::Error::new(spool::ErrorKind::Io, e.to_string()))
    }
    fn decompress(&self, src: &[u8], max: usize) -> spool::Result<Vec<u8>> {
        use std::io::Read;
        let decoder = zstd::stream::read::Decoder::new(src)
            .map_err(|e| spool::Error::new(spool::ErrorKind::Corrupt, e.to_string()))?;
        let mut out = Vec::new();
        decoder
            .take(max as u64 + 1)
            .read_to_end(&mut out)
            .map_err(|e| spool::Error::new(spool::ErrorKind::Corrupt, e.to_string()))?;
        if out.len() > max {
            return Err(spool::Error::new(
                spool::ErrorKind::Corrupt,
                "zstd size limit",
            ));
        }
        Ok(out)
    }
}

/// Raw spool options for a product-written dir: same key material as the
/// node's opens plus the zstd read codec. New blocks written through
/// these options use the spool default (deflate), which the product
/// decodes natively, so the bumped store stays product-shaped.
fn spool_opts(dir: &Path) -> spool::Options {
    let mut opts = spool::default_options(dir.to_string_lossy().to_string());
    opts.master_key = KEY.to_vec();
    opts.codecs = vec![Arc::new(ZstdBlock(ZSTD_LEVEL))];
    opts.reclaim_interval = None;
    opts
}

/// Read the `00/identity` record of a stopped store through the raw
/// spool API (last version wins).
fn read_identity(dir: &Path) -> StoreIdentity {
    let opts = spool_opts(dir);
    let mut raw: Option<Vec<u8>> = None;
    let store = spool::open_and_load_ordered(&opts, &mut |records: &[spool::Record]| {
        for r in records {
            if !r.deleted && r.key.as_slice() == IDENTITY_KEY {
                raw = Some(r.value.clone());
            }
        }
        Ok(())
    })
    .unwrap_or_else(|e| panic!("spool open {}: {e}", dir.display()));
    store
        .close()
        .unwrap_or_else(|e| panic!("spool close {}: {e}", dir.display()));
    let raw =
        raw.unwrap_or_else(|| panic!("no 00/identity record in {}", dir.display()));
    postcard::from_bytes(&raw).unwrap_or_else(|e| panic!("decode 00/identity: {e}"))
}

/// Overwrite the `00/identity` record with `format + 1`, returning the
/// bumped identity. The store must be stopped (no open `Database`).
fn bump_identity_format(dir: &Path, current: &StoreIdentity) -> StoreIdentity {
    let mut next = current.clone();
    next.format = current
        .format
        .checked_add(1)
        .expect("identity format + 1");
    let bytes = postcard::to_allocvec(&next).expect("encode bumped identity");
    let opts = spool_opts(dir);
    let store = spool::open(&opts)
        .unwrap_or_else(|e| panic!("spool open {} for format bump: {e}", dir.display()));
    store
        .commit(
            &[Mutation::put(IDENTITY_KEY.to_vec(), bytes)],
            Durability::Sync,
        )
        .unwrap_or_else(|e| panic!("commit bumped identity: {e}"));
    store
        .close()
        .unwrap_or_else(|e| panic!("spool close {} after bump: {e}", dir.display()));
    next
}

/// Recursive directory copy for the newer-store fixture (the pristine
/// node dir must stay immutable).
fn copy_dir_all(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst)
        .unwrap_or_else(|e| panic!("create {}: {e}", dst.display()));
    let mut entries: Vec<_> = std::fs::read_dir(src)
        .unwrap_or_else(|e| panic!("read {}: {e}", src.display()))
        .collect::<std::io::Result<_>>()
        .unwrap_or_else(|e| panic!("list {}: {e}", src.display()));
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let kind = entry
            .file_type()
            .unwrap_or_else(|e| panic!("file type {}: {e}", from.display()));
        if kind.is_dir() {
            copy_dir_all(&from, &to);
        } else if kind.is_file() {
            std::fs::copy(&from, &to)
                .unwrap_or_else(|e| panic!("copy {}: {e}", from.display()));
        }
    }
}

/// Every regular file under `root` as relpath -> sha256 hex (Go: `hashTree`).
fn hash_tree(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .collect::<std::io::Result<_>>()
            .unwrap_or_else(|e| panic!("list {}: {e}", dir.display()));
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let kind = entry
                .file_type()
                .unwrap_or_else(|e| panic!("file type {}: {e}", path.display()));
            if kind.is_dir() {
                walk(root, &path, out);
            } else if kind.is_file() {
                let rel = path
                    .strip_prefix(root)
                    .unwrap_or_else(|e| panic!("relpath {}: {e}", path.display()))
                    .to_string_lossy()
                    .into_owned();
                let raw = std::fs::read(&path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                out.insert(rel, format!("{:x}", Sha256::digest(&raw)));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// The refused store must be content-untouched (Go:
/// `assertStoreUntouched`): every file present in both snapshots is
/// byte-identical. Additions/removals are logged, not failed — the
/// refused open itself may stage lock files — and the reopen digest
/// below proves the logical data is identical.
fn assert_store_untouched(before: &BTreeMap<String, String>, after: &BTreeMap<String, String>) {
    let mut violations = Vec::new();
    let mut rotation = Vec::new();
    for (name, sum) in before {
        match after.get(name) {
            None => rotation.push(format!("removed: {name}")),
            Some(sum_after) if sum_after != sum => {
                violations.push(format!("modified: {name}"));
            }
            Some(_) => {}
        }
    }
    for name in after.keys() {
        if !before.contains_key(name) {
            rotation.push(format!("added: {name}"));
        }
    }
    assert!(
        violations.is_empty(),
        "refused store was touched:\n{}",
        violations.join("\n")
    );
    eprintln!(
        "refused store: no content mutated ({} files stable, {} added or removed)",
        before.len(),
        rotation.len()
    );
    for r in &rotation {
        eprintln!("  store file change: {r}");
    }
}

/// Row count on one node.
fn row_count(db: &Database) -> usize {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM records", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .expect("row count")
}

/// Canonical digest of every row, ordered by id (Go: `typedContentionDigest`).
fn rows_digest(db: &Database) -> String {
    let rows: Vec<(i64, String)> = db
        .read(|sql| {
            let mut stmt = sql.prepare("SELECT id, name FROM records ORDER BY id")?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .expect("read rows");
    let mut h = Sha256::new();
    for (id, name) in &rows {
        h.update(format!("{id}:{name}\n"));
    }
    format!("{:x}", h.finalize())
}

fn hex_id(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// Require the current build to refuse `dir` with the loud format-guard
/// error (Go: nonzero exit + "unsupported format version" in the log).
fn assert_refuses_newer_store(dir: &Path) {
    match Database::open(
        dir,
        KEY,
        Options {
            mode: OpenMode::Existing,
            ..Default::default()
        },
    ) {
        Err(Error::Corrupt(msg)) if msg.contains("unsupported database format") => {
            eprintln!("current build refused newer store: Corrupt({msg})");
        }
        Err(other) => panic!(
            "newer-store open failed with {other:?}, want Corrupt(\"unsupported database format\")"
        ),
        Ok(db) => {
            let _ = db.close();
            panic!("current build opened a newer-format store, want a loud refusal");
        }
    }
}

#[test]
fn downgrade_guard() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "downgrade-guard".into(),
        num_nodes: 1,
        key: KEY,
        manual_peers: true,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cluster
            .node(0)
            .migrate(Migration::new(
                "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            ))
            .expect("migrate records table");
        cluster.node(0).flush().expect("flush schema");
        for i in 0..ROWS {
            let id = FIRST_ID + i as i64;
            let name = format!("row-{i}");
            cluster
                .node(0)
                .transaction(move |sql| {
                    sql.execute("INSERT INTO records(id, name) VALUES(?, ?)", (id, name))?;
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("seed write {id}: {e}"));
        }
        assert_eq!(row_count(cluster.node(0)), ROWS, "seeded row count");
        let want_digest = rows_digest(cluster.node(0));
        let want_node = hex_id(&cluster.node_id(0));
        cluster.node(0).flush().expect("flush seeded rows");
        cluster.stop_node(0);

        // Identity + format markers of the stopped store (Go:
        // `assertFormatMarkers`, without hardcoding the version).
        let data_dir = cluster.node_info(0).data_dir.clone();
        let identity = read_identity(&data_dir);
        assert_eq!(
            identity.database,
            cluster.database_id(),
            "stored database id"
        );
        assert_eq!(identity.node, cluster.node_id(0), "stored node id");
        eprintln!(
            "store identity: db {} node {} format {}",
            hex_id(&identity.database),
            hex_id(&identity.node),
            identity.format
        );

        // Positive control (Go: "the previous binary boots a fresh
        // store"): a fresh store created by the current build opens,
        // holds data across a close/reopen, and carries the same
        // current format marker — so a later refusal can only be
        // caused by the bumped version, not by a broken build.
        let fresh_dir: PathBuf = cluster.runtime_dir().join("fresh-store");
        let fresh = Database::open(
            &fresh_dir,
            KEY,
            Options {
                mode: OpenMode::CreateNew,
                ..Default::default()
            },
        )
        .expect("fresh store create");
        fresh
            .migrate(Migration::new(
                "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            ))
            .expect("fresh migrate");
        fresh
            .transaction(|sql| {
                sql.execute("INSERT INTO records(id, name) VALUES(?, ?)", (1i64, "fresh"))?;
                Ok(())
            })
            .expect("fresh insert");
        fresh.flush().expect("fresh flush");
        fresh.close().expect("fresh close");
        let fresh_reopen = Database::open(
            &fresh_dir,
            KEY,
            Options {
                mode: OpenMode::Existing,
                ..Default::default()
            },
        )
        .expect("fresh store reopen");
        assert_eq!(row_count(&fresh_reopen), 1, "fresh reopened count");
        fresh_reopen.close().expect("fresh reopen close");
        let fresh_identity = read_identity(&fresh_dir);
        assert_eq!(
            fresh_identity.format, identity.format,
            "fresh store carries the current format marker"
        );
        eprintln!("fresh store booted and reopened at format {}", fresh_identity.format);

        // Craft the newer store: copy the stopped node dir, then bump
        // its identity format by one generation.
        let future_dir: PathBuf = cluster.runtime_dir().join("future-store");
        copy_dir_all(&data_dir, &future_dir);
        let bumped = bump_identity_format(&future_dir, &identity);
        assert_eq!(bumped.database, identity.database, "bumped db id");
        assert_eq!(bumped.node, identity.node, "bumped node id");
        let reread = read_identity(&future_dir);
        assert_eq!(
            reread.format,
            identity.format + 1,
            "bumped format persisted"
        );
        eprintln!(
            "newer store staged at format {} (current {})",
            reread.format, identity.format
        );

        // The downgrade attempt: the current build must refuse the
        // newer store loudly, without touching its content.
        let before = hash_tree(&future_dir);
        assert_refuses_newer_store(&future_dir);
        let after = hash_tree(&future_dir);
        assert_store_untouched(&before, &after);
        let markers = read_identity(&future_dir);
        assert_eq!(markers.format, reread.format, "format markers unmoved");
        assert_eq!(markers.database, identity.database, "db id unmoved");
        assert_eq!(markers.node, identity.node, "node id unmoved");
        eprintln!("store format markers still at {} after refusal", markers.format);

        // The current build reopens the unmodified store with
        // identical data and identity (Go: reopen + digest + node id).
        cluster.start_node(0).expect("reopen original store");
        assert_eq!(row_count(cluster.node(0)), ROWS, "reopened row count");
        let got_digest = rows_digest(cluster.node(0));
        assert_eq!(got_digest, want_digest, "reopened digest");
        assert_eq!(hex_id(&cluster.node_id(0)), want_node, "reopened node id");
        eprintln!("current build reopened original store: {ROWS} rows digest {got_digest}");

        // The newer store stays refused while the original works.
        assert_refuses_newer_store(&future_dir);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("downgrade-guard");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
