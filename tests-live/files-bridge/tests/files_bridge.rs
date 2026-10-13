//! Rust port of the Go `files-bridge` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-bridge/`
//! (`files_bridge_test.go` + `README.md`). A two-node Low mesh and a
//! two-node High mesh (separate DBIDs, CAs, storage keys, and file object
//! keys) move one 384 KiB file across a shared staging directory: Low
//! upload, Low mesh metadata + peer fetch, signed export of sealed chunks,
//! High import with re-encryption under the High object key, High mesh
//! metadata + peer fetch, metadata search, and a delete cascade that ends
//! with the file reading deleted on all four nodes.
//!
//! There is no HTTP server in the Rust product, so uploads, downloads,
//! metadata, search, and deletes use the direct `Database` file API
//! (`upload_file`/`get_file`/`file_metadata`/`search_files`/`delete_file`)
//! and `BridgeExport`/`BridgeImport` map to `Bridge::export_file` plus
//! `Bridge::capture()` (Low) / `Bridge::process()` (High) pumps, with a
//! pump inside every bridge wait poll. Peer fetch is automatic on the
//! configured `fetch_interval` (no explicit fetch call exists), and the
//! SHA-256 gate is digest consistency plus full byte equality (no separate
//! hash recomputation). See the scenario README.md.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use murmur_live_harness::murmur_replication::bridge::{self, Bridge, Config as BridgeConfig};
use murmur_live_harness::murmur_replication::protocol;
use murmur_live_harness::sqlite_session::Database;
use murmur_live_harness::{
    artifacts, wait_until, BridgeOptions, Cluster, ClusterOptions, FilesOptions,
};

/// Low (exporter) cluster name; owns `runtime/files-bridge-low/`, which also
/// holds the shared staging dir so artifacts are retained/cleaned with it.
const LOW_NAME: &str = "files-bridge-low";
/// High (importer) cluster name; owns `runtime/files-bridge-high/`.
const HIGH_NAME: &str = "files-bridge-high";
/// Bridge stream (Go: `field-files`).
const STREAM: &str = "field-files";
/// Bridged file (Go: `shared/field-map.bin`).
const FILE_NAME: &str = "shared/field-map.bin";
/// Upload size (Go: 384 KiB).
const FILE_BYTES: usize = 384 << 10;
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval.
const POLL: Duration = Duration::from_millis(50);

/// Independent bridge domains (Go: distinct DBIDs, CAs, keys per domain;
/// each harness cluster already provisions its own CA).
const LOW_DBID: [u8; 16] = [0xE0; 16];
const HIGH_DBID: [u8; 16] = [0xE1; 16];
const LOW_NODE_0: [u8; 16] = [0xE2; 16];
const LOW_NODE_1: [u8; 16] = [0xE3; 16];
const HIGH_NODE_0: [u8; 16] = [0xE4; 16];
const HIGH_NODE_1: [u8; 16] = [0xE5; 16];
const LOW_KEY: [u8; 32] = [0xE6; 32];
const HIGH_KEY: [u8; 32] = [0xE7; 32];
/// File object keys (Go: `lowObjectKey` 0x11.., `highObjectKey` 0x22..).
const LOW_OBJECT_KEY: [u8; 32] = [0x11; 32];
const HIGH_OBJECT_KEY: [u8; 32] = [0x22; 32];
/// Low bundle-signer seed (ed25519; the High importer pins its public key).
const LOW_SIGNER: [u8; 32] = [0x21; 32];
/// High receiver secret (X25519; seals the file chunks for High).
const HIGH_SECRET: [u8; 32] = [0x31; 32];

/// Deterministic payload (Go: `data[i] = byte((i*37 + i/251) % 253)`).
fn payload() -> Vec<u8> {
    (0..FILE_BYTES)
        .map(|i| ((i * 37 + i / 251) % 253) as u8)
        .collect()
}

fn node_bridge(db: &Database) -> Arc<Bridge> {
    db.replication()
        .expect("replication handle")
        .bridge()
        .expect("bridge")
}

/// Export+import acceleration pump (Go: `BridgeExport` + `BridgeImport`).
/// Counts are never asserted: the replication engine also drives
/// capture/process in the background, so progress is proven by file state
/// instead of `published`/`imported` counters.
fn pump(low: &Cluster, high: &Cluster) {
    node_bridge(low.node(0)).capture().expect("low capture");
    node_bridge(high.node(0))
        .process()
        .expect("high process");
}

/// Download the file from `cluster` node `idx` and prove full integrity:
/// exact bytes plus the uploader's digest carried in the metadata.
fn assert_node_bytes(cluster: &Cluster, idx: usize, want: &[u8], want_digest: [u8; 32]) {
    let mut got = Vec::new();
    let meta = cluster
        .node(idx)
        .get_file(FILE_NAME, &mut got)
        .unwrap_or_else(|e| panic!("node{} download failed: {e}", idx + 1));
    assert_eq!(got.as_slice(), want, "node{} bytes differ", idx + 1);
    assert_eq!(meta.digest, want_digest, "node{} digest differs", idx + 1);
}

/// Wait until `cluster` node `idx` carries nondeleted metadata for the file
/// with the expected digest (Go: `WaitFileAvailable` metadata leg).
fn wait_metadata(cluster: &Cluster, idx: usize, want_digest: [u8; 32], what: &str) {
    let arrived = wait_until(WAIT, POLL, || {
        cluster
            .node(idx)
            .file_metadata(FILE_NAME)
            .unwrap_or(None)
            .is_some_and(|m| m.digest == want_digest)
    });
    assert!(arrived, "{what}: metadata never arrived");
}

/// Wait until `cluster` node `idx` holds the object locally (Go: fetch leg;
/// the Rust product fetches automatically on `fetch_interval`).
fn wait_available(cluster: &Cluster, idx: usize, what: &str) {
    let arrived = wait_until(WAIT, POLL, || {
        cluster.node(idx).file_available(FILE_NAME).unwrap_or(false)
    });
    assert!(arrived, "{what}: object never became available");
}

/// Wait until `cluster` node `idx` reads the file deleted (Go:
/// `WaitFileDeleted`).
fn wait_deleted(cluster: &Cluster, idx: usize, what: &str) {
    let deleted = wait_until(WAIT, POLL, || {
        cluster
            .node(idx)
            .file_metadata(FILE_NAME)
            .unwrap_or(None)
            .is_none()
    });
    assert!(deleted, "{what}: tombstone never arrived");
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn two_low_two_high_file_bridge_live() {
    let staging = artifacts::runtime_dir(LOW_NAME).join("staging");
    let low = Cluster::new(ClusterOptions {
        name: LOW_NAME.into(),
        num_nodes: 2,
        database_id: Some(LOW_DBID),
        node_ids: vec![LOW_NODE_0, LOW_NODE_1],
        key: LOW_KEY,
        files: Some(FilesOptions {
            key: LOW_OBJECT_KEY,
            fetch_interval: Some(Duration::from_millis(100)),
            max_file_bytes: Some(4 << 20),
        }),
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeConfig::exporter(
                    LOW_DBID,
                    staging.clone(),
                    STREAM.into(),
                    LOW_SIGNER,
                    bridge::recipient_public(HIGH_SECRET),
                ),
            )]),
        },
        ..Default::default()
    })
    .expect("low cluster setup");
    // The High importer pins the Low bundle signer and trusts both Low
    // origin keys so imported file transactions verify; the seeds stay
    // inside the harness.
    let low_origin_0 = low.node_origin_pubkey(0);
    let low_origin_1 = low.node_origin_pubkey(1);
    let high = Cluster::new(ClusterOptions {
        name: HIGH_NAME.into(),
        num_nodes: 2,
        database_id: Some(HIGH_DBID),
        node_ids: vec![HIGH_NODE_0, HIGH_NODE_1],
        key: HIGH_KEY,
        files: Some(FilesOptions {
            key: HIGH_OBJECT_KEY,
            fetch_interval: Some(Duration::from_millis(100)),
            max_file_bytes: Some(4 << 20),
        }),
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeConfig::receiver(
                    HIGH_DBID,
                    staging.clone(),
                    STREAM.into(),
                    HIGH_SECRET,
                    BTreeSet::from([LOW_DBID]),
                    BTreeMap::from([(LOW_NODE_0, protocol::public_key(&LOW_SIGNER))]),
                    BTreeMap::from([
                        (LOW_NODE_0, low_origin_0),
                        (LOW_NODE_1, low_origin_1),
                    ]),
                ),
            )]),
        },
        ..Default::default()
    })
    .expect("high cluster setup");

    let data = payload();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Low upload, then Low mesh metadata + peer fetch.
        let up = low
            .node(0)
            .upload_file(FILE_NAME, data.as_slice())
            .expect("low upload");
        assert_eq!(up.bytes, data.len() as u64, "upload byte count");
        wait_metadata(&low, 1, up.digest, "low mesh metadata");
        wait_available(&low, 1, "low peer fetch");
        assert_node_bytes(&low, 1, &data, up.digest);

        // Export Low-0 to staging: sealed file-chunk bundles (plus the file
        // metadata transaction bundle). Synchronous, so every chunk bundle
        // exists afterwards; the background engine may add more.
        node_bridge(low.node(0))
            .export_file(FILE_NAME)
            .expect("bridge export");
        node_bridge(low.node(0))
            .capture()
            .expect("low capture after export");
        let mut staged: Vec<_> = std::fs::read_dir(&staging)
            .expect("read staging dir")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|e| e == "bundle"))
            .collect();
        staged.sort();
        assert!(
            staged.len() >= 2,
            "staged {} bundles, want file chunks plus metadata",
            staged.len()
        );
        // Staged artifacts must carry no document plaintext: several 1 KiB
        // windows of the payload must be absent from every staged bundle
        // (Go: `assertNoPlaintext`). Only `*.bundle` files are scanned: the
        // receiver's transient `.file-*.part` reassembly state lives in the
        // shared dir too once import starts, and it is plaintext by design.
        let windows: [&[u8]; 3] = [
            &data[0..1024],
            &data[100000..101024],
            &data[300000..301024],
        ];
        for path in &staged {
            let raw = std::fs::read(path)
                .unwrap_or_else(|e| panic!("read staged {}: {e}", path.display()));
            for (i, w) in windows.iter().enumerate() {
                assert!(
                    !contains(&raw, w),
                    "staged {} contains plaintext window {i}",
                    path.display()
                );
            }
        }

        // Import into High-0: metadata applies, chunks install, and the
        // object re-encrypts under the High object key.
        let imported = wait_until(WAIT, POLL, || {
            pump(&low, &high);
            high.node(0).file_available(FILE_NAME).unwrap_or(false)
        });
        assert!(imported, "high-0 import never completed");
        assert_node_bytes(&high, 0, &data, up.digest);
        assert_reencrypted(&low, &high);

        // High mesh carries metadata to High-1, which fetches bytes from High-0.
        wait_metadata(&high, 1, up.digest, "high mesh metadata");
        wait_available(&high, 1, "high-1 peer fetch");
        assert_node_bytes(&high, 1, &data, up.digest);

        // Search across the replicated metadata.
        let found = high.node(1).search_files("shared/").expect("high search");
        assert_eq!(found.len(), 1, "search found {} files, want one", found.len());
        assert_eq!(found[0].digest, up.digest, "search digest mismatch");
        assert_eq!(found[0].bytes, data.len() as u64, "search byte count");

        // Delete cascade: the Low tombstone replicates, exports, and imports.
        low.node(0).delete_file(FILE_NAME).expect("low delete");
        wait_deleted(&low, 1, "low delete mesh");
        let tombstoned = wait_until(WAIT, POLL, || {
            pump(&low, &high);
            high.node(0)
                .file_metadata(FILE_NAME)
                .unwrap_or(None)
                .is_none()
        });
        assert!(tombstoned, "high-0 delete never arrived");
        wait_deleted(&high, 1, "high-1 delete");
        assert!(
            low.node(0)
                .file_metadata(FILE_NAME)
                .expect("low-0 status")
                .is_none(),
            "low-0 still reads the file"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("files-bridge-low");
        high.dump_forensics("files-bridge-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Verify High at-rest bytes differ from Low at-rest bytes for the same
/// verified content (separate domain object keys; Go: `assertReencrypted`).
fn assert_reencrypted(low: &Cluster, high: &Cluster) {
    let low_obj = first_object_file(low.node_info(0).dir.clone());
    let high_obj = first_object_file(high.node_info(0).dir.clone());
    let low_raw =
        std::fs::read(&low_obj).unwrap_or_else(|e| panic!("read {}: {e}", low_obj.display()));
    let high_raw = std::fs::read(&high_obj)
        .unwrap_or_else(|e| panic!("read {}: {e}", high_obj.display()));
    assert_ne!(
        low_raw, high_raw,
        "high object bytes identical to low object bytes; expected re-encryption"
    );
}

/// First `*.spfo` object file under a node dir (Go: `firstObjectFile`).
fn first_object_file(root: std::path::PathBuf) -> std::path::PathBuf {
    let mut stack = vec![root.clone()];
    let mut found: Option<std::path::PathBuf> = None;
    while let Some(dir) = stack.pop() {
        let entries =
            std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("walk {}: {e}", dir.display()));
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "spfo") && found.is_none() {
                found = Some(path);
            }
        }
    }
    found.unwrap_or_else(|| panic!("no object file under {}", root.display()))
}
