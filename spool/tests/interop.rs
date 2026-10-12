//! Go <-> Rust byte-compatibility tests.
//!
//! The Go `spool` package and this crate share the on-disk format
//! (block v3, manifest v3, keys.enc v3). These tests prove it live:
//! the Go helpers under `interop/go` (`gowrite`, `goread`) and this
//! crate exchange stores in both directions, in encrypted and
//! plaintext modes, and must decode identical record sets.
//!
//! The live tests need the Go toolchain (`go` in PATH); without it
//! they print a warning and pass. The fixture test
//! (`tests/data/gostore-*`, written by `gowrite`) always runs.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use spool::{default_options, load, open, Durability, Encryption, Mutation, KV};

/// Compat key shared with `interop/go`: 32 x 0x42.
fn compat_key() -> Vec<u8> {
    vec![0x42u8; 32]
}

fn repo_root() -> PathBuf {
    std::env::var_os("MURMUR_GO_ROOT").map(PathBuf::from).unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join(".."))
}

fn go_available() -> bool {
    static GO: OnceLock<bool> = OnceLock::new();
    *GO.get_or_init(|| {
        Command::new("go")
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    })
}

fn require_go() -> bool {
    if go_available() && repo_root().join("go.mod").is_file() {
        return true;
    }
    eprintln!("warning: skipping live Go interop: install Go and set MURMUR_GO_ROOT to the Go Murmur checkout");
    false
}

/// Run a Go interop helper: `go run ./spoolrust/interop/go/<tool>`.
fn run_go(tool: &str, dir: &Path, mode: &str) -> String {
    let out = Command::new("go")
        .args([
            "run",
            &format!("{}/interop/go/{tool}/main.go", env!("CARGO_MANIFEST_DIR")),
            dir.to_str().unwrap(),
            mode,
        ])
        .current_dir(repo_root())
        .output()
        .unwrap_or_else(|e| panic!("run go {tool}: {e}"));
    assert!(
        out.status.success(),
        "go {tool} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Sorted (live pairs, tombstone keys) snapshot of a store.
type Dataset = (Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>);

/// The fixed interop dataset (mirrors `gowrite` and `writestore`):
/// 52 live keys + 2 tombstones.
fn expected_dataset() -> Dataset {
    let mut live: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..50 {
        if i == 0 || i == 1 {
            continue; // deleted by the dataset
        }
        let v = if i == 10 {
            "v3-final".to_string()
        } else {
            format!("value-{i}")
        };
        live.push((format!("key-{i:03}").into_bytes(), v.into_bytes()));
    }
    live.push((b"batch-a".to_vec(), b"1".to_vec()));
    live.push((b"batch-b".to_vec(), b"22".to_vec()));
    live.push((b"commit-x".to_vec(), b"xxx".to_vec()));
    live.push((b"empty-val".to_vec(), Vec::new()));
    live.sort();
    let tombs = vec![b"key-000".to_vec(), b"key-001".to_vec()];
    assert_eq!(live.len(), 52);
    (live, tombs)
}

/// Write the fixed dataset with this crate (mirrors `gowrite`).
fn write_dataset_rust(dir: &Path, encrypted: bool) {
    let mut o = default_options(dir.to_str().unwrap().to_string());
    o.reclaim_interval = None;
    o.flush.max_delay = Duration::from_millis(5);
    if encrypted {
        o.master_key = compat_key();
    } else {
        o.encryption = Encryption::None;
    }
    let store = open(&o).unwrap();
    for i in 0..50 {
        store
            .put(
                format!("key-{i:03}").as_bytes(),
                format!("value-{i}").as_bytes(),
            )
            .unwrap();
    }
    store
        .put_batch(&[
            KV::new(b"batch-a".to_vec(), b"1".to_vec()),
            KV::new(b"batch-b".to_vec(), b"22".to_vec()),
        ])
        .unwrap();
    store
        .commit(
            &[
                Mutation::put(b"commit-x".to_vec(), b"xxx".to_vec()),
                Mutation::delete(b"key-000".to_vec()),
            ],
            Durability::Sync,
        )
        .unwrap();
    store.delete(b"key-001").unwrap();
    store.put(b"key-010", b"v2").unwrap();
    store.put(b"key-010", b"v3-final").unwrap();
    store.put(b"empty-val", b"").unwrap();
    store.flush().unwrap();
    let st = store.stats();
    assert_eq!(st.keys, 52, "stats: {st:?}");
    store.close().unwrap();
}

/// Load a store offline with this crate; returns sorted (live, tombs).
fn load_all_rust(dir: &Path, key: &[u8]) -> Dataset {
    let mut m: HashMap<Vec<u8>, (Vec<u8>, bool)> = HashMap::new();
    load(dir.to_str().unwrap(), key, &mut |recs| {
        for r in recs {
            m.insert(r.key.clone(), (r.value.clone(), r.deleted));
        }
        Ok(())
    })
    .unwrap();
    let mut live = Vec::new();
    let mut tombs = Vec::new();
    for (k, (v, d)) in m {
        if d {
            tombs.push(k);
        } else {
            live.push((k, v));
        }
    }
    live.sort();
    tombs.sort();
    (live, tombs)
}

/// Parse `goread` output into sorted (live, tombs).
fn parse_goread(out: &str) -> Dataset {
    let mut live = Vec::new();
    let mut tombs = Vec::new();
    for line in out.lines() {
        if let Some(rest) = line.strip_prefix("KV ") {
            let (k, v) = rest.split_once('=').unwrap();
            live.push((unquote(k), unquote(v)));
        } else if let Some(rest) = line.strip_prefix("TOMB ") {
            tombs.push(unquote(rest));
        } else {
            panic!("unexpected goread line: {line:?}");
        }
    }
    live.sort();
    tombs.sort();
    (live, tombs)
}

/// Unquote one ASCII `"..."` field (the dataset is plain ASCII, so
/// Go %q and Rust {:?} agree here).
fn unquote(s: &str) -> Vec<u8> {
    assert!(
        s.len() >= 2 && s.starts_with('"') && s.ends_with('"'),
        "bad field {s:?}"
    );
    s.as_bytes()[1..s.len() - 1].to_vec()
}

fn test_opts(dir: &Path, encrypted: bool) -> spool::Options {
    let mut o = default_options(dir.to_str().unwrap().to_string());
    o.reclaim_interval = None;
    o.flush.max_delay = Duration::from_millis(5);
    if encrypted {
        o.master_key = compat_key();
    } else {
        o.encryption = Encryption::None;
    }
    o
}

fn go_writes_rust_reads(encrypted: bool) {
    if !require_go() {
        return;
    }
    let mode = if encrypted { "enc" } else { "plain" };
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("store");

    run_go("gowrite", &dir, mode);
    let expected = expected_dataset();

    // Offline load of the Go-written store.
    let got = load_all_rust(&dir, &compat_key());
    assert_eq!(got, expected, "rust load of go-written store ({mode})");

    // Writable open across implementations: Rust appends to the
    // Go-written store, Go reads the result back.
    let o = test_opts(&dir, encrypted);
    let store = open(&o).unwrap();
    store.put(b"x-rust-append", b"from-rust").unwrap();
    store.delete(b"key-002").unwrap();
    store.flush().unwrap();
    store.close().unwrap();

    let out = run_go("goread", &dir, mode);
    let (live, tombs) = parse_goread(&out);
    let mut want_live = expected.0.clone();
    want_live.retain(|(k, _)| k != b"key-002");
    want_live.push((b"x-rust-append".to_vec(), b"from-rust".to_vec()));
    want_live.sort();
    let mut want_tombs = expected.1.clone();
    want_tombs.push(b"key-002".to_vec());
    want_tombs.sort();
    assert_eq!(
        (live, tombs),
        (want_live, want_tombs),
        "go read after rust append ({mode})"
    );
}

fn rust_writes_go_reads(encrypted: bool) {
    if !require_go() {
        return;
    }
    let mode = if encrypted { "enc" } else { "plain" };
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("store");

    write_dataset_rust(&dir, encrypted);
    let expected = expected_dataset();

    // Go reads the Rust-written store.
    let out = run_go("goread", &dir, mode);
    assert_eq!(
        parse_goread(&out),
        expected,
        "go read of rust-written store ({mode})"
    );

    // Writable open across implementations: Go re-opens the
    // Rust-written store and rewrites the same dataset; versions
    // converge and Rust still loads the same final state.
    run_go("gowrite", &dir, mode);
    let got = load_all_rust(&dir, &compat_key());
    assert_eq!(got, expected, "rust load after go rewrite ({mode})");
}

#[test]
fn interop_go_writes_rust_reads_enc() {
    go_writes_rust_reads(true);
}

#[test]
fn interop_go_writes_rust_reads_plain() {
    go_writes_rust_reads(false);
}

#[test]
fn interop_rust_writes_go_reads_enc() {
    rust_writes_go_reads(true);
}

#[test]
fn interop_rust_writes_go_reads_plain() {
    rust_writes_go_reads(false);
}

/// Committed Go-written fixtures load without the Go toolchain.
#[test]
fn fixture_go_written_stores_load() {
    let expected = expected_dataset();
    for name in ["gostore-enc", "gostore-plain"] {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join(name);
        assert!(dir.is_dir(), "missing fixture {}", dir.display());
        let got = load_all_rust(&dir, &compat_key());
        assert_eq!(got, expected, "fixture {name}");
    }
}
