//! Rust port of the Go `files-soak` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-soak/`
//! (`files_soak_test.go`: `TestEncryptedFileFetchSoak` + `README.md`). A
//! two-node encrypted mesh soaks 64 KiB file uploads for
//! `MURMUR_FILES_SOAK_DURATION_SECONDS` (default 5 s), one upload every
//! `MURMUR_FILES_SOAK_INTERVAL_SECONDS` (default 1 s): upload to node 1,
//! wait for replicated metadata on node 2, fetch, download, verify the
//! digest, and search the replicated names. Upload p95 must stay under 5 s
//! and metadata-to-verified-fetch p95 under 20 s, and the fetch gate must
//! show every file completed with zero failures.
//!
//! There is no HTTP server in the Rust product, so uploads, downloads,
//! metadata, and search use the direct `Database` file API
//! (`upload_file`/`get_file`/`file_metadata`/`search_files`); peer fetch is
//! automatic on the configured `fetch_interval` (no explicit fetch call
//! exists). See the scenario README.md.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions, FilesOptions};

/// File object key (Go: `soakObjectKey` 0x51..).
const OBJECT_KEY: [u8; 32] = [0x51; 32];
/// Upload size per round (Go: 64 KiB).
const FILE_BYTES: usize = 64 << 10;
/// File cap (Go: `MaxFileBytes: 4 << 20`).
const MAX_FILE_BYTES: u64 = 4 << 20;
/// Per-round metadata/fetch wait bound (Go: 20 s `WaitFileAvailable`).
const WAIT: Duration = Duration::from_secs(20);
/// Convergence poll interval.
const POLL: Duration = Duration::from_millis(50);
/// Upload p95 SLO (Go: 5 s).
const UPLOAD_P95_SLO: Duration = Duration::from_secs(5);
/// Metadata-to-verified-fetch p95 SLO (Go: 20 s).
const TRANSFER_P95_SLO: Duration = Duration::from_secs(20);

/// Deterministic payload (Go: `data[i] = byte((round*31 + i*17) % 251)`).
fn payload(round: usize) -> Vec<u8> {
    (0..FILE_BYTES)
        .map(|i| ((round * 31 + i * 17) % 251) as u8)
        .collect()
}

/// p95 element of a sorted latency sample (Go: `min(n-1, (95*n+99)/100-1)`).
fn p95(sorted: &[Duration]) -> Duration {
    let n = sorted.len();
    sorted[(n - 1).min((95 * n + 99) / 100 - 1)]
}

#[test]
fn encrypted_file_fetch_soak() {
    let duration = env_seconds("MURMUR_FILES_SOAK_DURATION_SECONDS", 5);
    let interval = env_seconds("MURMUR_FILES_SOAK_INTERVAL_SECONDS", 1);
    let cluster = Cluster::new(ClusterOptions {
        name: "files-soak".into(),
        num_nodes: 2,
        files: Some(FilesOptions {
            key: OBJECT_KEY,
            fetch_interval: Some(Duration::from_millis(100)),
            max_file_bytes: Some(MAX_FILE_BYTES),
        }),
        ..Default::default()
    })
    .expect("cluster setup");

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = Instant::now() + duration;
        let mut upload_latencies = Vec::new();
        let mut end_to_end_latencies = Vec::new();
        let mut expected: HashMap<String, [u8; 32]> = HashMap::new();
        let mut round = 0usize;
        while Instant::now() < deadline {
            let name = format!("soak/file-{round:04}.bin");
            let data = payload(round);

            let start = Instant::now();
            let uploaded = cluster
                .node(0)
                .upload_file(&name, data.as_slice())
                .unwrap_or_else(|e| panic!("upload round {round}: {e}"));
            assert_eq!(
                uploaded.bytes,
                data.len() as u64,
                "round {round}: upload byte count"
            );
            upload_latencies.push(start.elapsed());
            expected.insert(name.clone(), uploaded.digest);

            let end_to_end_start = Instant::now();
            // Metadata leg (Go: `WaitFileAvailable` metadata wait).
            let metadata_arrived = wait_until(WAIT, POLL, || {
                cluster
                    .node(1)
                    .file_metadata(&name)
                    .unwrap_or(None)
                    .is_some_and(|m| m.digest == uploaded.digest)
            });
            assert!(metadata_arrived, "round {round}: metadata never arrived");
            // Fetch leg (Go: explicit `FetchFile`; the Rust product fetches
            // automatically on `fetch_interval`, so this is an availability wait).
            let fetched = wait_until(WAIT, POLL, || {
                cluster.node(1).file_available(&name).unwrap_or(false)
            });
            assert!(fetched, "round {round}: object never fetched");
            let mut got = Vec::new();
            let downloaded = cluster
                .node(1)
                .get_file(&name, &mut got)
                .unwrap_or_else(|e| panic!("round {round}: download failed: {e}"));
            assert_eq!(
                got.as_slice(),
                data.as_slice(),
                "round {round}: downloaded bytes differ"
            );
            assert_eq!(
                downloaded.digest, uploaded.digest,
                "round {round}: digest mismatch"
            );
            end_to_end_latencies.push(end_to_end_start.elapsed());

            let found = cluster
                .node(1)
                .search_files("file-")
                .unwrap_or_else(|e| panic!("round {round}: search failed: {e}"));
            assert_eq!(
                found.len(),
                round + 1,
                "round {round}: search returned {} files, want {}",
                found.len(),
                round + 1
            );

            round += 1;
            if Instant::now() < deadline {
                std::thread::sleep(interval);
            }
        }
        assert!(!upload_latencies.is_empty(), "soak completed no uploads");

        // Fetch gate (Go: `FetchStats` shows every file completed with zero
        // failures; the Rust product exposes no fetch counters): every
        // uploaded file must be locally available with its upload digest on
        // the peer, and the fetcher must have recorded no error.
        for (name, digest) in &expected {
            assert!(
                cluster.node(1).file_available(name).unwrap_or(false),
                "file {name} not available on the peer after the soak"
            );
            let meta = cluster
                .node(1)
                .file_metadata(name)
                .unwrap_or_else(|e| panic!("file {name}: metadata lookup failed: {e}"))
                .unwrap_or_else(|| panic!("file {name}: metadata missing after the soak"));
            assert_eq!(
                meta.digest, *digest,
                "file {name}: peer digest differs from the upload digest"
            );
        }
        let fetch_error = cluster
            .node(1)
            .replication()
            .expect("replication handle")
            .files()
            .expect("file store")
            .last_fetch_error();
        assert!(fetch_error.is_none(), "fetch errors: {fetch_error:?}");

        // First-file status (Go: `StatFile` exists + available).
        let first = cluster
            .node(1)
            .file_metadata("soak/file-0000.bin")
            .expect("first file status");
        assert!(first.is_some(), "first file metadata missing");
        assert!(
            cluster
                .node(1)
                .file_available("soak/file-0000.bin")
                .unwrap_or(false),
            "first file not available"
        );

        upload_latencies.sort();
        end_to_end_latencies.sort();
        let upload_p95 = p95(&upload_latencies);
        let transfer_p95 = p95(&end_to_end_latencies);
        eprintln!(
            "file soak duration={duration:?} rounds={} upload_p95={upload_p95:?} \
             metadata_fetch_verify_p95={transfer_p95:?}",
            expected.len(),
        );
        assert!(
            upload_p95 <= UPLOAD_P95_SLO,
            "upload p95 {upload_p95:?} exceeded 5s SLO"
        );
        assert!(
            transfer_p95 <= TRANSFER_P95_SLO,
            "end-to-end p95 {transfer_p95:?} exceeded 20s SLO"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("files-soak");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
