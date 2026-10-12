//! Crash recovery and corruption handling (port of
//! `spool_recovery_test.go` and `torn_tail_test.go`).

#[path = "common/mod.rs"]
mod common;

use std::path::Path;

use common::{load_all, test_options};
use spool::{open, ErrorKind};

fn seg_path(dir: &Path, id: u64) -> std::path::PathBuf {
    dir.join("segments").join(format!("{id:012}.spool"))
}

fn seg_size(dir: &Path, id: u64) -> u64 {
    std::fs::metadata(seg_path(dir, id)).unwrap().len()
}

fn append_tail(dir: &Path, id: u64, tail: &[u8]) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(seg_path(dir, id))
        .unwrap();
    f.write_all(tail).unwrap();
}

/// Deterministic junk bytes (xorshift64*).
fn junk(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed;
    (0..n)
        .map(|_| {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            (x.wrapping_mul(0x2545F4914F6CDD1D) >> 56) as u8
        })
        .collect()
}

fn write_seeded_store(dir: &Path, n: usize) -> u64 {
    let st = open(&test_options(dir)).unwrap();
    for i in 0..n {
        st.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    st.close().unwrap();
    seg_size(dir, 1)
}

#[test]
fn torn_tail_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    for i in 0..100 {
        st.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    st.close().unwrap();
    let before = seg_size(dir, 1);
    // Append a torn header plus junk.
    append_tail(dir, 1, &[0xAAu8; 40]);

    let st2 = open(&test_options(dir)).unwrap();
    assert_eq!(st2.stats().truncated_tails, 1);
    assert_eq!(seg_size(dir, 1), before);
    // New writes append after the truncation point, not past garbage.
    st2.put(b"new", b"v").unwrap();
    st2.flush().unwrap();
    st2.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 101);
    assert_eq!(got.get(&b"new"[..]).map(|b| b.as_slice()), Some(&b"v"[..]));
}

#[test]
fn torn_tail_full_frame() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    st.close().unwrap();
    let p = seg_path(dir, 1);
    let mut raw = std::fs::read(&p).unwrap();
    // Corrupt the last payload byte (block extends exactly to EOF).
    let last = raw.len() - 1;
    raw[last] ^= 0xFF;
    std::fs::write(&p, &raw).unwrap();
    let err = open(&test_options(dir)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AuthFailed, "got {err}");
}

#[test]
fn clean_shutdown_detects_file_loss() {
    // Remove file.
    {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write_store_10_flush_each(dir);
        std::fs::remove_file(seg_path(dir, 1)).unwrap();
        let err = open(&test_options(dir)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Corrupt, "got {err}");
    }
    // Add file: unlisted files are swept as unreferenced garbage.
    {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write_store_10_flush_each(dir);
        let raw = std::fs::read(seg_path(dir, 1)).unwrap();
        std::fs::write(seg_path(dir, 2), &raw).unwrap();
        let st = open(&test_options(dir)).unwrap();
        st.close().unwrap();
        assert!(!seg_path(dir, 2).exists(), "unlisted file not swept");
    }
    // Truncate bytes.
    {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write_store_10_flush_each(dir);
        let p = seg_path(dir, 1);
        let len = std::fs::metadata(&p).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(len - 100).unwrap();
        let err = open(&test_options(dir)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Corrupt, "got {err}");
    }
}

fn write_store_10_flush_each(dir: &Path) {
    let st = open(&test_options(dir)).unwrap();
    for i in 0..10 {
        st.put(format!("k{i}").as_bytes(), &[0u8; 100]).unwrap();
        st.flush().unwrap();
    }
    st.close().unwrap();
}

#[test]
fn mid_file_corruption_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    for i in 0..3 {
        st.put(format!("k{i}").as_bytes(), b"v").unwrap();
        st.flush().unwrap();
    }
    st.close().unwrap();
    let p = seg_path(dir, 1);
    let mut raw = std::fs::read(&p).unwrap();
    // Corrupt the first block's payload (more blocks follow it).
    raw[63 + 5] ^= 0xFF;
    std::fs::write(&p, &raw).unwrap();
    let err = open(&test_options(dir)).unwrap_err();
    assert!(
        matches!(err.kind(), ErrorKind::AuthFailed | ErrorKind::Corrupt),
        "got {err}"
    );
}

#[test]
fn open_torn_zero_tail_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let clean = write_seeded_store(dir, 200);
    append_tail(dir, 1, &vec![0u8; 363]);

    let st = open(&test_options(dir)).unwrap();
    let stats = st.stats();
    st.close().unwrap();
    assert_eq!(stats.keys, 200);
    assert!(stats.truncated_tails > 0, "torn tail not counted");
    assert_eq!(seg_size(dir, 1), clean);
    let got = load_all(dir);
    assert_eq!(got.len(), 200);
    assert_eq!(
        got.get(&b"k0199"[..]).map(|b| b.as_slice()),
        Some(&b"v199"[..])
    );
}

#[test]
fn open_torn_garbage_tail_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let clean = write_seeded_store(dir, 200);
    append_tail(dir, 1, &junk(5000, 7));

    let st = open(&test_options(dir)).unwrap();
    let keys = st.stats().keys;
    st.close().unwrap();
    assert_eq!(keys, 200);
    assert_eq!(seg_size(dir, 1), clean);
}

#[test]
fn open_mid_file_corruption_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    write_seeded_store(dir, 200);
    let p = seg_path(dir, 1);
    let orig = std::fs::read(&p).unwrap();
    let mut doctored = orig.clone();
    doctored.extend_from_slice(&junk(500, 13));
    doctored.extend_from_slice(&orig);
    std::fs::write(&p, &doctored).unwrap();
    let err = open(&test_options(dir)).unwrap_err();
    assert!(err.to_string().contains("next valid frame"), "got {err}");
}

#[test]
fn open_sealed_segment_damage_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.target_block_bytes = 4 << 10;
    o.max_block_bytes = 64 << 10;
    o.max_key_size = 1 << 10;
    o.max_value_size = 8 << 10;
    o.max_segment_size = 256 << 10;
    let st = open(&o).unwrap();
    for i in 0..500 {
        st.put(format!("k{i:04}").as_bytes(), &junk(1024, 21 + i as u64))
            .unwrap();
    }
    st.close().unwrap();
    let n = std::fs::read_dir(dir.join("segments")).unwrap().count();
    assert!(n >= 2, "want a rotation, got {n} segments");
    append_tail(dir, 1, &vec![0u8; 363]);
    assert!(
        open(&test_options(dir)).is_err(),
        "damaged sealed segment opened"
    );
}
