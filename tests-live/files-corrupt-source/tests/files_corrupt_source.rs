//! Rust port of the Go `files-corrupt-source` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-corrupt-source/`
//! (`files_corrupt_source_test.go`:
//! `TestCorruptSourceFetchFailsVerification` + `README.md`). A victim
//! object is published honestly, then its stored bytes are corrupted on
//! the source node while stopped (same length, flipped chunk
//! ciphertext). A peer fetch must then fail verification with no
//! partial install, fail identically on retry, and leave honest objects
//! fetching afterwards.
//!
//! There is no HTTP server in the Rust product, so uploads, downloads,
//! metadata, and search use the direct `Database` file API
//! (`upload_file`/`get_file`/`file_metadata`/`list_files`); peer fetch is
//! automatic on the configured `fetch_interval` (no explicit fetch call
//! exists), so the fetcher is stopped while the victim is published and
//! corrupted, then restarted. Verification failure surfaces as
//! `Error::Auth("object authentication")` recorded in
//! `last_fetch_error`, the Rust equivalent of the Go
//! `objectstore: invalid encrypted object` sentinel. See the scenario
//! README.md.

use std::path::{Path, PathBuf};
use std::time::Duration;

use murmur_live_harness::{wait_until, Cluster, ClusterOptions, FilesOptions};

/// File object key (Go: `objectKey` 0x42..).
const OBJECT_KEY: [u8; 32] = [0x42; 32];
/// File cap (Go: `MaxFileBytes: 4 << 20`).
const MAX_FILE_BYTES: u64 = 4 << 20;
/// Peer fetch cadence (the `files-soak` pattern; the Rust product has no
/// explicit fetch call, so background fetch must stay enabled).
const FETCH_INTERVAL: Duration = Duration::from_millis(100);
/// Convergence poll interval.
const POLL: Duration = Duration::from_millis(50);
/// Per-step metadata/fetch wait bound (Go: 30 s `WaitFileAvailable`).
const WAIT: Duration = Duration::from_secs(30);
/// Quiescent-staging wait bound (a quiet moment arrives within one fetch
/// interval once failures are being recorded).
const QUIET_WAIT: Duration = Duration::from_secs(5);
/// Retry-persistence observation window (~5 background fetch cycles).
const RETRY_WINDOW: Duration = Duration::from_millis(500);
/// Rust equivalent of the Go `objectstore: invalid encrypted object`
/// sentinel: flipped chunk ciphertext fails AES-GCM authentication
/// (`replication/src/files.rs`: `Error::Auth("object authentication")`,
/// surfaced via `last_fetch_error` as `replication authentication: ...`).
const ERR_OBJECT_AUTH: &str = "object authentication";

/// Deterministic payload unique to `name` (the `files-crash` pattern:
/// name-keyed bytes, no hash dependency).
fn payload(name: &str, n: usize) -> Vec<u8> {
    let nb = name.as_bytes();
    (0..n)
        .map(|i| ((nb.len() * 31 + nb[i % nb.len()] as usize + i * 17) % 251) as u8)
        .collect()
}

fn hex_lower(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 15) as usize] as char);
    }
    s
}

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

/// Last background-fetch error recorded on node `idx` (the Rust fetch
/// gate; there is no fetch-failed counter).
fn last_fetch_error(cluster: &Cluster, idx: usize) -> Option<String> {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .files()
        .expect("file store")
        .last_fetch_error()
}

/// First regular file under `dir` (recursive) whose name matches `want`.
fn find_file(dir: &Path, want: &impl Fn(&str) -> bool) -> Option<PathBuf> {
    let rd = std::fs::read_dir(dir).ok()?;
    for entry in rd.flatten() {
        let path = entry.path();
        let file_type = entry.file_type().ok()?;
        if file_type.is_dir() {
            if let Some(hit) = find_file(&path, want) {
                return Some(hit);
            }
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        if let Some(base) = path.file_name().and_then(|s| s.to_str()) {
            if want(base) {
                return Some(path);
            }
        }
    }
    None
}

/// The published `.spfo` object file for `digest_hex` (Go: `findObjectFile`).
fn find_object_file(dir: &Path, digest_hex: &str) -> Option<PathBuf> {
    find_file(dir, &|base: &str| {
        base.ends_with(".spfo") && base.contains(digest_hex)
    })
}

/// Any fetch-staging `*.part` file (successful Rust fetches rename the
/// `.part` into place, so any residue is a leak, regardless of digest).
fn find_part_file(dir: &Path) -> Option<PathBuf> {
    find_file(dir, &|base: &str| base.ends_with(".part"))
}

fn check_download(cluster: &Cluster, idx: usize, name: &str, want: &[u8]) {
    let mut got = Vec::new();
    let meta = cluster
        .node(idx)
        .get_file(name, &mut got)
        .unwrap_or_else(|e| panic!("node{idx} download {name}: {e}"));
    assert_eq!(got.as_slice(), want, "node{idx} {name} bytes differ");
    assert_eq!(meta.bytes, want.len() as u64, "node{idx} {name} size");
}

fn body(cluster: &mut Cluster) {
    let victim_size = env_usize("MURMUR_FILES_CORRUPT_VICTIM_KB", 256) << 10;

    // Positive control: honest upload replicates, fetches, verifies.
    let ctrl = payload("ctrl/a.bin", 64 << 10);
    let st = cluster
        .node(0)
        .upload_file("ctrl/a.bin", &ctrl[..])
        .expect("control upload");
    assert_eq!(st.bytes, ctrl.len() as u64, "control upload byte count");
    let arrived = wait_until(WAIT, POLL, || {
        cluster
            .node(1)
            .file_metadata("ctrl/a.bin")
            .unwrap_or(None)
            .is_some_and(|m| m.digest == st.digest)
    });
    assert!(arrived, "control metadata never arrived");
    let fetched = wait_until(WAIT, POLL, || {
        cluster
            .node(1)
            .file_available("ctrl/a.bin")
            .unwrap_or(false)
    });
    assert!(fetched, "control fetch never completed");
    check_download(cluster, 1, "ctrl/a.bin", &ctrl);
    eprintln!("positive control: upload/replicate/fetch/verify works");

    // Fetch-error baseline: the clean control must record no fetch error.
    assert!(
        last_fetch_error(cluster, 1).is_none(),
        "clean control recorded a fetch error"
    );

    // Stop the fetcher so no fetch can start before the corruption lands
    // (the Rust product fetches automatically on `fetch_interval`; there
    // is no disabled-worker mode like Go's `FetchIntervalMs: -1`).
    cluster.stop_node(1);

    // Publish the victim object honestly; metadata only (the fetcher is
    // stopped, so nothing can fetch yet).
    let victim = payload("victim/blob.bin", victim_size);
    let vst = cluster
        .node(0)
        .upload_file("victim/blob.bin", &victim[..])
        .expect("victim upload");
    assert_eq!(vst.bytes, victim.len() as u64, "victim upload byte count");
    let victim_digest = vst.digest;
    let victim_hex = hex_lower(&victim_digest);
    eprintln!(
        "victim published: digest={victim_hex} size={}",
        victim.len()
    );

    // Locate the source-side object file before stopping the node.
    let src_obj = find_object_file(&cluster.node_info(0).dir, &victim_hex)
        .unwrap_or_else(|| panic!("no .spfo object file for digest {victim_hex}"));
    let before = std::fs::read(&src_obj).expect("read source object");
    eprintln!(
        "source object: {} ({} container bytes)",
        src_obj.display(),
        before.len()
    );
    assert!(
        before.len() >= 1024,
        "source object unexpectedly small ({} bytes); refusing to corrupt",
        before.len()
    );

    // Corrupt the published bytes while the source node is stopped
    // (honest disk-corruption simulation: same length, flipped bytes in
    // the middle of the container, i.e. inside chunk ciphertext).
    cluster.stop_node(0);
    let mut flip = before.clone();
    for i in 0..16 {
        let off = flip.len() / 2 + i;
        flip[off] ^= 0xFF;
    }
    std::fs::write(&src_obj, &flip).expect("write corrupted object");
    let after = std::fs::read(&src_obj).expect("re-read corrupted object");
    assert_eq!(after.len(), before.len(), "corrupted object length changed");
    eprintln!(
        "flipped 16 bytes at container offset {} (length preserved)",
        flip.len() / 2
    );

    // Restart both nodes and re-mesh (fresh ephemeral ports).
    cluster
        .start_node(0)
        .unwrap_or_else(|e| panic!("restart source: {e}"));
    cluster
        .start_node(1)
        .unwrap_or_else(|e| panic!("restart fetcher: {e}"));
    cluster.mesh().expect("re-mesh after restart");

    // Victim metadata must replicate honestly to the fetcher.
    let meta_arrived = wait_until(WAIT, POLL, || {
        cluster
            .node(1)
            .file_metadata("victim/blob.bin")
            .unwrap_or(None)
            .is_some_and(|m| m.digest == victim_digest)
    });
    assert!(meta_arrived, "victim metadata never arrived");

    // The peer fetch must FAIL verification with the auth sentinel, and
    // the fetch-error signal must advance (None -> Some).
    let failed = wait_until(WAIT, POLL, || {
        last_fetch_error(cluster, 1).is_some_and(|e| e.contains(ERR_OBJECT_AUTH))
    });
    let fetch_err = last_fetch_error(cluster, 1);
    assert!(
        failed,
        "corrupt fetch never failed verification (last error: {fetch_err:?})"
    );
    let fetch_err = fetch_err.expect("fetch error recorded");
    assert!(
        fetch_err.contains(ERR_OBJECT_AUTH),
        "fetch error {fetch_err:?} does not mention verification failure {ERR_OBJECT_AUTH:?}"
    );
    eprintln!("corrupt fetch failed closed as required: {fetch_err}");

    // No partial install on the fetching node.
    let pst = cluster
        .node(1)
        .file_metadata("victim/blob.bin")
        .expect("victim status")
        .expect("victim metadata wrongly altered by failed fetch");
    assert_eq!(
        pst.digest, victim_digest,
        "victim digest altered by failed fetch"
    );
    assert!(
        !cluster
            .node(1)
            .file_available("victim/blob.bin")
            .unwrap_or(true),
        "victim reports Available after a failed-verification fetch (partial install)"
    );
    assert!(
        cluster
            .node(1)
            .get_file("victim/blob.bin", &mut Vec::new())
            .is_err(),
        "victim downloads after a failed-verification fetch (partial install)"
    );

    // Background retries run every `fetch_interval`, so wait for a
    // quiescent moment (no transfer in flight) and require no staging
    // residue and no object file for the unverified digest then.
    let node1_dir = cluster.node_info(1).dir.clone();
    let quiet = wait_until(QUIET_WAIT, Duration::from_millis(10), || {
        find_part_file(&node1_dir).is_none()
    });
    assert!(quiet, "fetch staging never quiesced");
    assert!(
        find_object_file(&node1_dir, &victim_hex).is_none(),
        "fetching node holds an object file for an unverified digest (partial install)"
    );
    assert!(
        find_part_file(&node1_dir).is_none(),
        "fetching node holds a staging file after failed verification (staging not discarded)"
    );

    // No phantom metadata: the listing still shows the victim with its
    // original honest digest/size (corruption is bytes-only, metadata
    // untouched) on both nodes.
    for i in 0..cluster.len() {
        let list = cluster.node(i).list_files().expect("list files");
        let found = list.iter().find(|f| f.name == "victim/blob.bin");
        let f =
            found.unwrap_or_else(|| panic!("node{i} lost victim metadata after failed fetch"));
        assert!(
            f.digest == victim_digest && f.bytes == victim.len() as u64 && !f.deleted,
            "node{i} victim metadata altered: {f:?}"
        );
    }

    // The corruption is persistent, not transient: after more background
    // retry cycles the fetch still fails the same verification.
    std::thread::sleep(RETRY_WINDOW);
    let again = last_fetch_error(cluster, 1).expect("fetch error signal lost after retries");
    assert!(
        again.contains(ERR_OBJECT_AUTH),
        "post-retry fetch error {again:?} does not mention {ERR_OBJECT_AUTH:?}"
    );
    assert!(
        !cluster
            .node(1)
            .file_available("victim/blob.bin")
            .unwrap_or(true),
        "victim became available across retries (partial install)"
    );
    eprintln!("retried fetch failed closed again: {again}");

    // Honest objects must still fetch afterwards.
    let honest = payload("honest/b.bin", 128 << 10);
    let hst = cluster
        .node(0)
        .upload_file("honest/b.bin", &honest[..])
        .expect("honest upload after corruption");
    let honest_meta = wait_until(WAIT, POLL, || {
        cluster
            .node(1)
            .file_metadata("honest/b.bin")
            .unwrap_or(None)
            .is_some_and(|m| m.digest == hst.digest)
    });
    assert!(honest_meta, "honest metadata after corruption never arrived");
    let honest_fetched = wait_until(WAIT, POLL, || {
        cluster
            .node(1)
            .file_available("honest/b.bin")
            .unwrap_or(false)
    });
    assert!(
        honest_fetched,
        "honest fetch after corruption never completed"
    );
    check_download(cluster, 1, "honest/b.bin", &honest);
    check_download(cluster, 1, "ctrl/a.bin", &ctrl);
    eprintln!("corrupt-source proven: bad bytes fail verification, honest objects unaffected");
}

#[test]
fn corrupt_source_fetch_fails_verification() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "files-corrupt-source".into(),
        num_nodes: 2,
        files: Some(FilesOptions {
            key: OBJECT_KEY,
            fetch_interval: Some(FETCH_INTERVAL),
            max_file_bytes: Some(MAX_FILE_BYTES),
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        body(&mut cluster);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("files-corrupt-source");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
