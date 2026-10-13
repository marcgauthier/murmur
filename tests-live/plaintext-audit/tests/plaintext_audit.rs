//! Rust port of the Go `plaintext-audit` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/plaintext-audit/`
//! (`README.md` + `plaintext_audit_test.go`:
//! `TestPlaintextAuditMarkersAndKeysAbsentOnDisk`). Random per-run markers
//! are written through the typed table, replicated, and crash-cycled; every
//! marker round-trips through the typed API (positive control), then both
//! nodes' entire directories and a backup artifact are scanned: marker
//! plaintext and key material must be absent everywhere.
//!
//! Plain SQL replaces the Go typed-records HTTP API (the Rust product has
//! no HTTP server); stop/start replaces SIGKILL; the backup is an online
//! `Database::backup`; and no config file is skipped because the Rust
//! harness provisions no key-bearing config file. See the scenario
//! README.md for the full deviation list.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};
use rand::RngCore;

/// Typed table (Go: `live_typed_records` managed typed table).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)";
/// Marker rows (Go: 8) plus sustained filler (Go: 200).
const NUM_MARKERS: usize = 8;
const NUM_FILLER: usize = 200;
const TOTAL_ROWS: usize = NUM_MARKERS + NUM_FILLER;
/// Convergence deadline and poll (Go: 60 s, 200 ms).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(200);

/// Lowercase hex of `bytes` (no extra dep; Go: `hex.EncodeToString`).
fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

/// `n` random bytes as hex (Go: `randHex`).
fn rand_hex(n: usize) -> String {
    let mut buf = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut buf);
    hex_encode(&buf)
}

/// Typed insert on one node (Go: `Cluster.TypedInsert`).
fn typed_insert(db: &Database, name: &str) {
    let name = name.to_owned();
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

/// Wait until every node holds the same `want` names (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize) {
    let ok = wait_until(CONVERGE_TIMEOUT, POLL, || {
        let mut first: Option<String> = None;
        for i in 0..cluster.len() {
            let names = typed_names(cluster.node(i));
            if names.len() != want {
                return false;
            }
            let joined = names.join("\x00");
            match &first {
                None => first = Some(joined),
                Some(want_joined) if *want_joined == joined => {}
                Some(_) => return false,
            }
        }
        true
    });
    assert!(
        ok,
        "nodes did not converge on {want} rows within {CONVERGE_TIMEOUT:?}"
    );
}

/// First regular file under `root` containing `needle`, if any
/// (Go: `scanTree`; no skip set: the Rust harness provisions no
/// key-bearing config file, so every file is audited).
fn scan_tree(root: &Path, needle: &[u8]) -> std::io::Result<Option<PathBuf>> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path)? {
                stack.push(entry?.path());
            }
        } else if path.is_file() && file_contains(&path, needle)? {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// Streaming needle search with overlap, so matches split across chunk
/// boundaries are still found (Go: `fileContains`).
fn file_contains(path: &Path, needle: &[u8]) -> std::io::Result<bool> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    const CHUNK: usize = 64 * 1024;
    let mut buf: Vec<u8> = Vec::with_capacity(CHUNK + needle.len());
    let mut tmp = vec![0u8; CHUNK];
    loop {
        let n = f.read(&mut tmp)?;
        if n > 0 {
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(needle.len()).any(|w| w == needle) {
                return Ok(true);
            }
            if buf.len() > needle.len() {
                let keep = needle.len();
                buf.drain(..buf.len() - keep);
            }
        }
        if n == 0 {
            return Ok(false);
        }
    }
}

#[test]
fn plaintext_audit_markers_and_keys_absent_on_disk() {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    let key_hex = hex_encode(&key);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "plaintext-audit".into(),
        num_nodes: 2,
        key,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .unwrap_or_else(|e| panic!("node{} migrate failed: {e}", i + 1));
            cluster
                .node(i)
                .flush()
                .unwrap_or_else(|e| panic!("node{} flush after migrate failed: {e}", i + 1));
        }

        // Unique high-entropy markers: random per run, so a stale-file
        // false positive from another run is impossible.
        let mut markers = Vec::with_capacity(NUM_MARKERS);
        for i in 0..NUM_MARKERS {
            let marker = format!("MURMUR-AUDIT-{}-ROW{i}", rand_hex(32));
            typed_insert(cluster.node(0), &marker);
            markers.push(marker);
        }

        // Sustained load around the markers to exercise WAL, memtables,
        // compaction, and replication paths.
        for i in 0..NUM_FILLER {
            let filler = format!("load-filler-{i}-padding-{}", "x".repeat(64));
            typed_insert(cluster.node(i % 2), &filler);
        }
        wait_converged(&cluster, TOTAL_ROWS);

        // Crash mid-life so tmp/scratch/crash artifacts exist, then restart
        // (Go: SIGKILL; here in-process stop/start, then re-mesh).
        cluster.stop_node(0);
        cluster.start_node(0).expect("restart node 1");
        cluster.mesh().expect("re-mesh after restart");
        wait_converged(&cluster, TOTAL_ROWS);

        // Positive control: every marker round-trips byte-identically
        // through the managed typed table, proving the scan below is
        // meaningful.
        let present: HashSet<String> =
            typed_names(cluster.node(0)).into_iter().collect();
        for (i, want) in markers.iter().enumerate() {
            assert!(
                present.contains(want),
                "marker {i} did not round-trip through the typed table"
            );
        }
        eprintln!("positive control: all {} markers round-trip intact", markers.len());

        // Backup artifact (Go: offline open + backup; here the product's
        // online backup on the still-running node 1).
        let backup_dir = cluster.runtime_dir().join("backup-dest");
        cluster
            .node(0)
            .backup(&backup_dir)
            .expect("backup node 1");

        // Stop both nodes so the on-disk image is quiescent, then scan the
        // ENTIRE node directory of BOTH nodes. Node 2 learned the rows via
        // replication apply (not local commit), so its disk image exercises
        // different code paths and must be audited too.
        cluster.stop_node(0);
        cluster.stop_node(1);
        for n in 0..cluster.len() {
            let nodedir = cluster.node_info(n).dir.clone();
            for (i, m) in markers.iter().enumerate() {
                match scan_tree(&nodedir, m.as_bytes()) {
                    Ok(None) => {}
                    Ok(Some(hit)) => panic!(
                        "PLAINTEXT LEAK: marker {i} found on disk in {}",
                        hit.display()
                    ),
                    Err(e) => panic!("scan node{} marker {i}: {e}", n + 1),
                }
            }
            eprintln!("markers absent from entire node dir {}", nodedir.display());
        }

        // Key material must not leak anywhere on disk (Go additionally
        // skips the harness-provisioned config file; the Rust harness
        // writes no such file, so the whole tree is audited).
        let needles: Vec<Vec<u8>> =
            vec![key_hex.clone().into_bytes(), key_hex.to_uppercase().into_bytes(), key.to_vec()];
        for n in 0..cluster.len() {
            let nodedir = cluster.node_info(n).dir.clone();
            for needle in &needles {
                match scan_tree(&nodedir, needle) {
                    Ok(None) => {}
                    Ok(Some(hit)) => panic!(
                        "KEY LEAK: key material found on disk in {}",
                        hit.display()
                    ),
                    Err(e) => panic!("scan node{} key material: {e}", n + 1),
                }
            }
        }
        eprintln!("key material absent from entire node dirs");

        // Backup artifacts must not contain marker plaintext (or the key).
        for (i, m) in markers.iter().enumerate() {
            match scan_tree(&backup_dir, m.as_bytes()) {
                Ok(None) => {}
                Ok(Some(hit)) => panic!(
                    "PLAINTEXT LEAK: marker {i} found in backup artifact {}",
                    hit.display()
                ),
                Err(e) => panic!("scan backup marker {i}: {e}"),
            }
        }
        match scan_tree(&backup_dir, key_hex.as_bytes()) {
            Ok(None) => {}
            Ok(Some(hit)) => panic!(
                "KEY LEAK: key material found in backup artifact {}",
                hit.display()
            ),
            Err(e) => panic!("scan backup key: {e}"),
        }
        eprintln!("backup artifacts clean: no marker plaintext, no key material");

        // Restart and reconverge: the audit must not disturb honest behavior.
        cluster.start_node(0).expect("restart node 1");
        cluster.start_node(1).expect("restart node 2");
        cluster.mesh().expect("re-mesh after audit");
        wait_converged(&cluster, TOTAL_ROWS);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("plaintext-audit");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
