//! Rust port of the Go `files-crash` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-crash/`
//! (`files_crash_test.go`: `TestFileCrashLeavesNoPartialOrPhantom` +
//! `README.md`). A two-node encrypted mesh proves crash consistency: after
//! the uploader is killed mid-upload and the fetcher mid-fetch, every
//! listed object downloads byte-complete and digest-verified on both
//! nodes, metadata converges exactly, and every on-disk payload is
//! attributable to a known attempt.
//!
//! There is no HTTP server in the Rust product, so uploads, downloads,
//! metadata, and search use the direct `Database` file API
//! (`upload_file`/`get_file`/`file_metadata`/`list_files`); peer fetch is
//! automatic on the configured `fetch_interval` (no explicit fetch call
//! exists). Signals cannot target one in-process node, so the two kill
//! legs are modeled in-process: the mid-upload kill is a byte stream
//! that aborts partway (the product must drop the staged temp and fail
//! the upload) followed by a close/reopen of the uploader, and the
//! mid-fetch kill is a close/reopen of the fetcher while a `.part`
//! transfer file is on disk. See the scenario README.md.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use murmur_live_harness::murmur_replication::files::Metadata;
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
/// Final metadata-settle bound (Go: 60 s).
const SETTLE_WAIT: Duration = Duration::from_secs(60);
/// Mid-fetch kill coordination bound (Go: 15 s `killWhileFetching`).
const KILL_WAIT: Duration = Duration::from_secs(15);
/// Upload size in the uploader-crash rounds (Go: 1 MiB x8).
const UPLOAD_BYTES: usize = 1 << 20;
/// Bytes delivered before the sabotaged upload stream aborts.
const SABOTAGE_BYTES: usize = 256 << 10;
/// Crash rounds per leg (Go: 3).
const ROUNDS: u32 = 3;

/// One upload try: the digest is known only for acknowledged uploads
/// (the payload is deterministic, but without an independent hash
/// dependency the digest of an errored upload is unknowable; errored
/// uploads must leave no debris at all).
struct Attempt {
    digest: Option<[u8; 32]>,
    size: u64,
    ok: bool,
}

/// Deterministic payload unique to `name` (Go: `payload` seeded by the
/// SHA-256 of the name; here a name-keyed byte pattern instead, since
/// the scenario adds no hash dependency).
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

/// A byte stream that aborts mid-upload: the in-process equivalent of
/// SIGKILLing the uploader while its request body is still flowing.
struct FailAfter<R> {
    inner: R,
    remaining: usize,
}

impl<R: Read> Read for FailAfter<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated mid-upload crash",
            ));
        }
        let want = buf.len().min(self.remaining);
        let n = self.inner.read(&mut buf[..want])?;
        self.remaining -= n;
        Ok(n)
    }
}

/// `<hex64>.g<gen>.spfo` -> digest hex (Go: `.spfo` parse with `.g`
/// generation suffix).
fn parse_object_digest(base: &str) -> Option<String> {
    let stem = base.strip_suffix(".spfo")?;
    let (digest, _) = stem.rsplit_once(".g")?;
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(digest.to_string())
}

/// `<source>-<digest64>.g<gen>.part` -> digest hex (Rust fetch staging
/// names carry the source id prefix; Go parses the digest prefix the
/// same way).
fn parse_part_digest(base: &str) -> Option<String> {
    let stem = base.strip_suffix(".part")?;
    let (_, rest) = stem.split_once('-')?;
    let (digest, _) = rest.rsplit_once(".g")?;
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(digest.to_string())
}

/// Collect on-disk payloads under a node dir: published objects
/// (`.spfo`), upload staging (Rust: `.upload-*`/`.rotate-*`; Go:
/// `.stage-*`), and fetch staging (`*.part`).
fn collect_payloads(dir: &Path, spfo: &mut Vec<String>, stage: &mut Vec<String>, part: &mut Vec<String>) {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    for entry in rd.flatten() {
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(t) => t,
            Err(_) => continue,
        };
        if file_type.is_dir() {
            collect_payloads(&path, spfo, stage, part);
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        if let Some(base) = path.file_name().and_then(|s| s.to_str()) {
            if base.ends_with(".spfo") {
                spfo.push(base.to_string());
            } else if base.starts_with(".upload-") || base.starts_with(".rotate-") {
                stage.push(base.to_string());
            } else if base.ends_with(".part") {
                part.push(base.to_string());
            }
        }
    }
}

/// Staged fetch transfers on a node (the in-flight signal for the
/// mid-fetch kill leg; Go polls `FetchStats.InFlight`).
fn staging_parts(staging: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(staging) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.extension().map(|x| x == "part").unwrap_or(false) {
                out.push(path);
            }
        }
    }
    out
}

/// Close `idx` once a fetch transfer is in flight for `name`. Reports
/// (killed, too_slow): too_slow means the fetch completed first and the
/// round proves nothing (Go: `killWhileFetching`).
fn stop_while_fetching(
    cluster: &mut Cluster,
    idx: usize,
    name: &str,
    timeout: Duration,
) -> (bool, bool) {
    let staging = cluster.node_info(idx).dir.join("objects").join("staging");
    let end = Instant::now() + timeout;
    while Instant::now() < end {
        if cluster.node(idx).file_available(name).unwrap_or(false) {
            return (false, true);
        }
        let in_flight = staging_parts(&staging).len();
        if in_flight > 0 {
            eprintln!("node{}: stopping fetcher with {in_flight} staged transfer(s)", idx + 1);
            cluster.stop_node(idx);
            return (true, false);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    (false, false)
}

fn restart_node(cluster: &mut Cluster, idx: usize) {
    cluster
        .start_node(idx)
        .unwrap_or_else(|e| panic!("restart node{}: {e}", idx + 1));
    cluster.mesh().expect("re-mesh after restart");
}

fn metadata_canonical(list: &[Metadata]) -> String {
    let mut sorted: Vec<&Metadata> = list.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let mut sb = String::new();
    for m in sorted {
        sb.push_str(&format!("{}|{}|{}|{};", m.name, hex_lower(&m.digest), m.bytes, m.deleted));
    }
    sb
}

/// Poll until both nodes list identical metadata and every acknowledged
/// upload is present everywhere (Go: `waitMetadataSettled`).
fn wait_metadata_settled(cluster: &Cluster, attempted: &HashMap<String, Attempt>) {
    let end = Instant::now() + SETTLE_WAIT;
    loop {
        let mut lists = Vec::new();
        let mut ok = true;
        for idx in 0..cluster.len() {
            match cluster.node(idx).list_files() {
                Ok(l) => lists.push(l),
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && lists.len() == 2 {
            let settled = metadata_canonical(&lists[0]) == metadata_canonical(&lists[1])
                && attempted.values().all(|a| {
                    !a.ok || lists[0].iter().any(|m| m.digest == a.digest.unwrap() && m.bytes == a.size)
                });
            if settled {
                return;
            }
        }
        if Instant::now() >= end {
            for idx in 0..cluster.len() {
                let n = cluster.node(idx).list_files().map(|l| l.len()).unwrap_or(0);
                eprintln!("node{idx} at timeout: {n} files");
            }
            panic!("file metadata did not settle identically on both nodes");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Download every listed object on both nodes and require byte-complete,
/// digest-verified content; anything listed must have been attempted (no
/// phantoms) and every acked upload must be listed (no lost writes).
/// (Go: `verifyAllObjects`.)
fn verify_all_objects(
    cluster: &Cluster,
    attempted: &HashMap<String, Attempt>,
    fetch_attempts: &mut usize,
) {
    for idx in 0..cluster.len() {
        let list = cluster.node(idx).list_files().expect("list files");
        for f in &list {
            let a = attempted
                .get(&f.name)
                .unwrap_or_else(|| panic!("node{idx} lists phantom object {:?}", f.name));
            assert!(!f.deleted, "node{idx} object {:?} unexpectedly deleted", f.name);
            let want = a.digest.unwrap_or_else(|| {
                panic!("node{idx} {:?} listed but its upload errored", f.name)
            });
            assert!(
                f.digest == want && f.bytes == a.size,
                "node{idx} {:?} metadata digest={} size={}, want {}/{}",
                f.name,
                hex_lower(&f.digest),
                f.bytes,
                hex_lower(&want),
                a.size,
            );
            if !cluster.node(idx).file_available(&f.name).unwrap_or(false) {
                *fetch_attempts += 1;
                let fetched = wait_until(WAIT, POLL, || {
                    cluster.node(idx).file_available(&f.name).unwrap_or(false)
                });
                assert!(fetched, "node{idx} fetch {:?} never completed", f.name);
            }
            let mut got = Vec::new();
            let meta = cluster
                .node(idx)
                .get_file(&f.name, &mut got)
                .unwrap_or_else(|e| {
                    panic!("node{idx} {:?} listed but not downloadable (partial?): {e}", f.name)
                });
            assert_eq!(got.len() as u64, a.size, "node{idx} {:?} length is partial", f.name);
            assert_eq!(
                got,
                payload(&f.name, a.size as usize),
                "node{idx} {:?} bytes differ (partial/corrupt)",
                f.name,
            );
            assert_eq!(meta.digest, want, "node{idx} {:?} digest mismatch", f.name);
        }
        for (name, a) in attempted {
            if !a.ok {
                continue;
            }
            let st = cluster
                .node(idx)
                .file_metadata(name)
                .unwrap_or_else(|e| panic!("node{idx} stat {name:?}: {e}"));
            assert!(st.is_some(), "node{idx} lost acked upload {name:?}");
        }
    }
    let l0 = cluster.node(0).list_files().expect("list files");
    let l1 = cluster.node(1).list_files().expect("list files");
    eprintln!(
        "all {} listed objects byte-complete on both nodes; metadata digests equal",
        l0.len()
    );
    assert_eq!(l0.len(), l1.len(), "object counts differ");
}

/// Walk each node dir and require every object payload (`.spfo`), upload
/// stage file (Rust `.upload-*`, Go `.stage-*`), and fetch stage file
/// (`*.part`) to be attributable to a known upload/fetch attempt and
/// bounded in count (Go: `verifyInventory`).
fn verify_inventory(
    cluster: &Cluster,
    attempted: &HashMap<String, Attempt>,
    fetch_attempts: usize,
) {
    let mut known: HashSet<String> = HashSet::new();
    let mut errored = 0usize;
    for a in attempted.values() {
        match (a.ok, a.digest) {
            (true, Some(d)) => {
                known.insert(hex_lower(&d));
            }
            _ => errored += 1,
        }
    }
    for idx in 0..cluster.len() {
        let mut spfo = Vec::new();
        let mut stage = Vec::new();
        let mut part = Vec::new();
        collect_payloads(&cluster.node_info(idx).dir, &mut spfo, &mut stage, &mut part);
        let meta = cluster.node(idx).list_files().expect("list files");
        let referenced: HashSet<String> = meta.iter().map(|m| hex_lower(&m.digest)).collect();
        let mut orphans = 0;
        for base in &spfo {
            let digest = parse_object_digest(base)
                .unwrap_or_else(|| panic!("node{idx} object file {base} has a bad name"));
            if referenced.contains(&digest) {
                continue;
            }
            if known.contains(&digest) {
                orphans += 1;
                continue;
            }
            panic!("node{idx} object file {base} unattributable to any upload");
        }
        assert!(
            orphans <= errored,
            "node{idx} has {orphans} unreferenced objects from {errored} errored uploads"
        );
        if idx == 1 {
            assert!(stage.is_empty(), "node 1 never uploaded yet holds {} stage files", stage.len());
        }
        assert!(
            stage.len() <= errored,
            "node{idx} holds {} stage files from {errored} errored uploads",
            stage.len()
        );
        if idx == 0 {
            assert!(part.is_empty(), "node 0 never fetched yet holds {} part files", part.len());
        }
        for base in &part {
            let digest = parse_part_digest(base)
                .unwrap_or_else(|| panic!("node{idx} stage file {base} has a bad name"));
            assert!(known.contains(&digest), "node{idx} stage file {base} unattributable to any upload");
        }
        assert!(
            part.len() <= fetch_attempts + 2,
            "node{idx} holds {} part files from {fetch_attempts} fetch attempts",
            part.len()
        );
        eprintln!(
            "node{idx} inventory: {} objects ({orphans} crash orphans), {} stage, {} part - all attributable",
            spfo.len(),
            stage.len(),
            part.len()
        );
    }
}

fn body(cluster: &mut Cluster) {
    let big_mb = env_usize("MURMUR_FILES_CRASH_BIG_MB", 3);
    let mut attempted: HashMap<String, Attempt> = HashMap::new();
    let mut fetch_attempts: usize = 0;

    // Positive control: honest upload replicates, fetches, verifies.
    let ctrl = payload("ctrl/a.bin", 64 << 10);
    let st = cluster
        .node(0)
        .upload_file("ctrl/a.bin", &ctrl[..])
        .expect("control upload");
    assert_eq!(st.bytes, ctrl.len() as u64, "control upload byte count");
    attempted.insert(
        "ctrl/a.bin".into(),
        Attempt { digest: Some(st.digest), size: ctrl.len() as u64, ok: true },
    );
    let arrived = wait_until(WAIT, POLL, || {
        cluster
            .node(1)
            .file_metadata("ctrl/a.bin")
            .unwrap_or(None)
            .is_some_and(|m| m.digest == st.digest)
    });
    assert!(arrived, "control metadata never arrived");
    fetch_attempts += 1;
    let fetched = wait_until(WAIT, POLL, || cluster.node(1).file_available("ctrl/a.bin").unwrap_or(false));
    assert!(fetched, "control fetch never completed");
    let mut got = Vec::new();
    cluster.node(1).get_file("ctrl/a.bin", &mut got).expect("control download");
    assert_eq!(got, ctrl, "control download bytes differ");
    eprintln!("positive control: upload/replicate/fetch/verify works");
    // Baseline inventory before any crash: the clean path must leave
    // zero unattributable payloads (the before half of before/after).
    verify_inventory(&*cluster, &attempted, fetch_attempts);

    // Phase 1: kill the uploader mid-upload (stream abort coordinated).
    let mut uploader_killed = false;
    for round in 1..=ROUNDS {
        if uploader_killed {
            break;
        }
        let mut errored = 0;
        for i in 0..8 {
            let name = format!("crash/u-{round}-{i}.bin");
            let data = payload(&name, UPLOAD_BYTES);
            if i == 3 {
                // The kill lands here: the byte stream aborts partway
                // through this upload.
                let sabotaged = FailAfter { inner: &data[..], remaining: SABOTAGE_BYTES };
                match cluster.node(0).upload_file(&name, sabotaged) {
                    Ok(_) => panic!("round {round}: sabotaged upload unexpectedly succeeded"),
                    Err(_) => {
                        errored += 1;
                        attempted.insert(name, Attempt { digest: None, size: data.len() as u64, ok: false });
                    }
                }
                continue;
            }
            let up = cluster
                .node(0)
                .upload_file(&name, &data[..])
                .unwrap_or_else(|e| panic!("round {round} upload {name}: {e}"));
            assert_eq!(up.bytes, data.len() as u64, "round {round} upload {name} byte count");
            attempted.insert(name, Attempt { digest: Some(up.digest), size: data.len() as u64, ok: true });
        }
        // Crash aftermath: close + reopen the uploader, re-mesh.
        eprintln!("round {round}: restarting uploader with {errored} errored upload(s)");
        cluster.stop_node(0);
        restart_node(&mut *cluster, 0);
        if errored == 0 {
            eprintln!("round {round}: kill landed between uploads; retrying");
            continue;
        }
        uploader_killed = true;
        eprintln!("round {round}: uploader killed mid-upload ({errored} errored uploads)");
    }
    assert!(uploader_killed, "no valid mid-upload kill in {ROUNDS} rounds");

    // Phase 2: kill the fetcher mid-fetch (staged-transfer coordinated).
    let mut fetcher_killed = false;
    for round in 1..=ROUNDS {
        if fetcher_killed {
            break;
        }
        let name = format!("crash/f-{round}.bin");
        let data = payload(&name, big_mb << 20);
        let up = cluster
            .node(0)
            .upload_file(&name, &data[..])
            .unwrap_or_else(|e| panic!("round {round} big upload: {e}"));
        attempted.insert(
            name.clone(),
            Attempt { digest: Some(up.digest), size: data.len() as u64, ok: true },
        );
        let arrived = wait_until(WAIT, POLL, || {
            cluster
                .node(1)
                .file_metadata(&name)
                .unwrap_or(None)
                .is_some_and(|m| m.digest == up.digest)
        });
        assert!(arrived, "round {round}: big metadata never arrived");
        fetch_attempts += 1;
        let (killed, too_slow) = stop_while_fetching(&mut *cluster, 1, &name, KILL_WAIT);
        if too_slow {
            eprintln!("round {round}: fetch completed before the kill; retrying");
            continue;
        }
        assert!(killed, "round {round}: fetch never went in flight");
        restart_node(&mut *cluster, 1);
        fetcher_killed = true;
        eprintln!("round {round}: fetcher killed mid-fetch of {name}");
        // The interrupted fetch must complete cleanly after restart:
        // staged partials never poison the verified install.
        fetch_attempts += 1;
        let fetched = wait_until(WAIT, POLL, || cluster.node(1).file_available(&name).unwrap_or(false));
        assert!(fetched, "post-crash fetch never completed");
        let mut got = Vec::new();
        let meta = cluster.node(1).get_file(&name, &mut got).expect("post-crash download");
        assert_eq!(meta.digest, up.digest, "post-crash digest mismatch");
        assert_eq!(got, data, "post-crash bytes differ");
    }
    assert!(fetcher_killed, "no valid mid-fetch kill in {ROUNDS} rounds");

    // Phase 3: full recovery proof.
    wait_metadata_settled(&*cluster, &attempted);
    verify_all_objects(&*cluster, &attempted, &mut fetch_attempts);
    verify_inventory(&*cluster, &attempted, fetch_attempts);
    eprintln!("files-crash proven: no partials, no phantoms, inventory attributable");
}

#[test]
fn file_crash_leaves_no_partial_or_phantom() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "files-crash".into(),
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
        cluster.dump_forensics("files-crash");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
