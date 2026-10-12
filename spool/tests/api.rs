//! Core open/write/load API (port of `spool_api_test.go` and
//! `spool_diag_test.go`).

#[path = "common/mod.rs"]
mod common;

use std::collections::HashMap;

use common::{load_all, master_key, test_options};
use spool::{
    default_options, load, load_with_options, open, open_and_load, Compression, Durability,
    Encryption, ErrorKind, LoadOptions,
};

#[test]
fn round_trip_all_modes() {
    for enc in [Encryption::None, Encryption::Aes256Gcm] {
        for comp in [Compression::NONE, Compression::DEFLATE] {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path();
            let mut o = test_options(dir);
            o.encryption = enc;
            o.compression = Some(comp);
            if enc == Encryption::None {
                o.master_key = Vec::new();
            }
            let st = open(&o).unwrap();
            let mut want: HashMap<String, String> = HashMap::new();
            for i in 0..500 {
                let k = format!("key-{i:04}");
                // Compressible values exercise the codec.
                let v = format!("value-{i:04}-{}", "x".repeat(i % 97));
                want.insert(k.clone(), v.clone());
                st.put(k.as_bytes(), v.as_bytes()).unwrap();
            }
            // Overwrite every 7th key to exercise replacement.
            for i in (0..500).step_by(7) {
                let k = format!("key-{i:04}");
                want.insert(k.clone(), "replaced".to_string());
                st.put(k.as_bytes(), b"replaced").unwrap();
            }
            st.flush().unwrap();
            let stats = st.stats();
            assert_eq!(stats.keys, 500, "stats: {stats:?}");
            assert!(
                stats.blocks_written > 0 && stats.bytes_written > 0,
                "stats: {stats:?}"
            );
            st.close().unwrap();
            // Close is idempotent.
            st.close().unwrap();

            let got: HashMap<Vec<u8>, Vec<u8>> = if enc == Encryption::None {
                let mut got = HashMap::new();
                let mut seqs: HashMap<Vec<u8>, u64> = HashMap::new();
                let lo = LoadOptions {
                    path: dir.to_str().unwrap().to_string(),
                    ..Default::default()
                };
                load_with_options(lo, &mut |recs| {
                    for r in recs {
                        if r.sequence < seqs.get(&r.key).copied().unwrap_or(0) {
                            continue;
                        }
                        seqs.insert(r.key.clone(), r.sequence);
                        got.insert(r.key.clone(), r.value.clone());
                    }
                    Ok(())
                })
                .unwrap();
                got
            } else {
                load_all(dir)
            };
            assert_eq!(got.len(), want.len());
            for (k, v) in &want {
                assert_eq!(
                    got.get(k.as_bytes()).map(|b| b.as_slice()),
                    Some(v.as_bytes()),
                    "key {k:?}"
                );
            }
        }
    }
}

#[test]
fn open_and_load_delivers_all() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    for i in 0..100 {
        st.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    st.close().unwrap(); // Close flushes.
    let mut count = 0;
    let st2 = open_and_load(&test_options(dir), &mut |recs| {
        count += recs.len();
        for r in recs {
            assert!(!r.deleted, "unexpected tombstone for {:?}", r.key);
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 100);
    assert_eq!(st2.stats().keys, 100);
    st2.close().unwrap();
}

#[test]
fn passphrase_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap().to_string();
    let mut o = default_options(dir.clone());
    o.passphrase = "correct horse battery staple".to_string();
    o.reclaim_interval = None;
    let st = open(&o).unwrap();
    st.put(b"k", b"v").unwrap();
    st.close().unwrap();
    let mut got: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let lo = LoadOptions {
        path: dir.clone(),
        passphrase: o.passphrase.clone(),
        ..Default::default()
    };
    load_with_options(lo, &mut |recs| {
        for r in recs {
            got.insert(r.key.clone(), r.value.clone());
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(got.get(&b"k"[..]).map(|b| b.as_slice()), Some(&b"v"[..]));
    // Wrong passphrase fails.
    let bad = LoadOptions {
        path: dir,
        passphrase: "wrong".to_string(),
        ..Default::default()
    };
    let err = load_with_options(bad, &mut |_| Ok(())).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::WrongKey, "got {err}");
}

#[test]
fn open_validation() {
    let key = master_key();
    type Mutate = Box<dyn Fn(&mut spool::Options)>;
    let cases: Vec<(&str, Mutate)> = vec![
        ("no path", Box::new(|o| o.path = String::new())),
        ("no key", Box::new(|o| o.master_key = Vec::new())),
        ("short key", Box::new(|o| o.master_key = b"short".to_vec())),
        (
            "key and passphrase",
            Box::new(|o| o.passphrase = "x".to_string()),
        ),
        (
            "key with none",
            Box::new(|o| o.encryption = Encryption::None),
        ),
        ("non-pow2 write shards", Box::new(|o| o.write_shards = 100)),
        ("non-pow2 index shards", Box::new(|o| o.index_shards = 3)),
        (
            "threshold over one",
            Box::new(|o| o.compaction_threshold = 1.5),
        ),
        (
            "negative threshold",
            Box::new(|o| o.compaction_threshold = -0.5),
        ),
        (
            "oversize key limit",
            Box::new(|o| o.max_key_size = o.max_block_bytes),
        ),
        (
            "negative free-space reserve",
            Box::new(|o| o.compaction_min_free_bytes = -1),
        ),
    ];
    // Note: "unknown encryption" has no Rust spelling; the enum
    // makes invalid values unrepresentable.
    for (name, mutate) in cases {
        let tmp = tempfile::tempdir().unwrap();
        let mut o = default_options(tmp.path().to_str().unwrap().to_string());
        o.master_key = key.clone();
        mutate(&mut o);
        if let Ok(st) = open(&o) {
            st.close().unwrap();
            panic!("Open succeeded for case {name:?}, want error");
        }
    }
}

#[test]
fn enc_plain_mismatch_refused() {
    // Encrypted store refuses plaintext options and vice versa.
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"v").unwrap();
    st.close().unwrap();
    let mut o2 = test_options(dir);
    o2.encryption = Encryption::None;
    o2.master_key = Vec::new();
    assert!(
        open(&o2).is_err(),
        "open encrypted store with EncryptionNone succeeded"
    );

    let tmp2 = tempfile::tempdir().unwrap();
    let mut po = default_options(tmp2.path().to_str().unwrap().to_string());
    po.encryption = Encryption::None;
    po.reclaim_interval = None;
    let ps = open(&po).unwrap();
    ps.close().unwrap();
    let mut po2 = default_options(tmp2.path().to_str().unwrap().to_string());
    po2.master_key = master_key();
    po2.reclaim_interval = None;
    assert!(open(&po2).is_err(), "open plain store with key succeeded");
}

#[test]
fn use_after_close() {
    let tmp = tempfile::tempdir().unwrap();
    let st = open(&test_options(tmp.path())).unwrap();
    st.close().unwrap();
    assert_eq!(st.put(b"k", b"v").unwrap_err().kind(), ErrorKind::Closed);
    assert_eq!(st.delete(b"k").unwrap_err().kind(), ErrorKind::Closed);
    assert_eq!(st.flush().unwrap_err().kind(), ErrorKind::Closed);
    assert_eq!(st.reclaim().unwrap_err().kind(), ErrorKind::Closed);
}

#[test]
fn size_limits() {
    let tmp = tempfile::tempdir().unwrap();
    let mut o = test_options(tmp.path());
    o.max_key_size = 16;
    o.max_value_size = 32;
    let st = open(&o).unwrap();
    assert_eq!(
        st.put(&[b'k'; 17], b"v").unwrap_err().kind(),
        ErrorKind::KeyTooLarge
    );
    assert_eq!(
        st.put(b"k", &[b'v'; 33]).unwrap_err().kind(),
        ErrorKind::ValueTooLarge
    );
    st.put(b"k", b"v").unwrap();
    st.close().unwrap();
}

#[test]
fn large_value_own_block() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.target_block_bytes = 64 << 10; // 5MB value dwarfs the target
    let st = open(&o).unwrap();
    let big = b"0123456789abcdef".repeat((5 << 20) / 16);
    st.put(b"small", b"x").unwrap();
    st.put(b"big", &big).unwrap();
    st.flush().unwrap();
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.get(&b"big"[..]), Some(&big));
    assert_eq!(
        got.get(&b"small"[..]).map(|b| b.as_slice()),
        Some(&b"x"[..])
    );
}

#[test]
fn durability_modes() {
    for d in [Durability::Async, Durability::Flush, Durability::Sync] {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut o = test_options(dir);
        o.durability = d;
        let st = open(&o).unwrap();
        for i in 0..50 {
            st.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        st.flush().unwrap();
        st.close().unwrap();
        assert_eq!(load_all(dir).len(), 50, "durability {d}");
    }
}

#[test]
fn stats_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let st = open(&test_options(tmp.path())).unwrap();
    // Highly compressible values: expect a real ratio.
    let val = vec![b'a'; 1000];
    for i in 0..200 {
        st.put(format!("k{i:04}").as_bytes(), &val).unwrap();
    }
    st.flush().unwrap();
    let s = st.stats();
    assert_eq!((s.keys, s.live_records), (200, 200), "stats: {s:?}");
    assert!(s.blocks_written > 0 && s.bytes_written > 0, "stats: {s:?}");
    assert!(s.compression_ratio > 1.5, "ratio: {}", s.compression_ratio);
    assert_eq!((s.pending_records, s.pending_bytes), (0, 0), "stats: {s:?}");
    assert!(
        s.segments > 0 && s.disk_bytes > 0 && s.live_bytes > 0,
        "stats: {s:?}"
    );
    st.close().unwrap();
}

#[test]
fn load_standalone_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("nope").to_str().unwrap().to_string();
    assert!(load(&missing, &master_key(), &mut |_| Ok(())).is_err());
}

#[test]
fn load_func_error_aborts() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"v").unwrap();
    st.close().unwrap();
    let err = load(dir.to_str().unwrap(), &master_key(), &mut |_| {
        Err(spool::Error::new(ErrorKind::Invalid, "stop"))
    })
    .unwrap_err();
    assert!(err.to_string().contains("stop"), "got {err}");
}

#[test]
fn manifest_and_keys_layout() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.close().unwrap();
    for name in ["manifest", "keys.enc", "LOCK"] {
        assert!(dir.join(name).is_file(), "missing {name}");
    }
    let segs: Vec<_> = std::fs::read_dir(dir.join("segments")).unwrap().collect();
    assert_eq!(segs.len(), 1);
    assert_eq!(
        segs[0].as_ref().unwrap().file_name().to_str().unwrap(),
        "000000000001.spool"
    );
}

#[test]
fn load_delivers_current_once() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    for i in 0..60 {
        st.put(format!("k{i:02}").as_bytes(), b"v1").unwrap();
    }
    st.flush().unwrap();
    for i in 0..60 {
        st.put(format!("k{i:02}").as_bytes(), b"v2").unwrap();
    }
    st.flush().unwrap();
    for i in 0..20 {
        st.delete(format!("k{i:02}").as_bytes()).unwrap();
    }
    st.close().unwrap();
    let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut tombs = 0;
    let mut callbacks = 0;
    load(dir.to_str().unwrap(), &master_key(), &mut |recs| {
        assert!(!recs.is_empty(), "empty callback delivered");
        callbacks += 1;
        for r in recs {
            *seen.entry(r.key.clone()).or_insert(0) += 1;
            if r.deleted {
                tombs += 1;
                continue;
            }
            assert_eq!(r.value, b"v2", "key {:?} stale", r.key);
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(seen.len(), 60);
    for (k, n) in &seen {
        assert_eq!(*n, 1, "key {k:?} delivered {n} times");
    }
    assert_eq!(tombs, 20);
    assert!(callbacks > 0);
}

#[test]
fn rejects_pretransactional_store() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    std::fs::create_dir_all(dir.join("segments")).unwrap();
    let mut raw = vec![0u8; 64];
    raw[0..4].copy_from_slice(b"SPLM");
    raw[4] = 1; // format version 1
    std::fs::write(dir.join("manifest"), &raw).unwrap();
    let err = open(&test_options(dir)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::UnsupportedVersion, "got {err}");
    let err = load(dir.to_str().unwrap(), &master_key(), &mut |_| Ok(())).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::UnsupportedVersion, "got {err}");
}

#[test]
fn diagnostics() {
    let tmp = tempfile::tempdir().unwrap();
    let mut o = test_options(tmp.path());
    o.wrapping_key_id = "diag-key".to_string();
    let st = open(&o).unwrap();
    assert_eq!(st.stats().index_bytes_estimate, 0);
    for i in 0..10 {
        st.put(&[b'a' + i], b"v").unwrap();
    }
    st.delete(b"a").unwrap();
    st.commit(
        &[spool::Mutation::put(b"c".to_vec(), b"v".to_vec())],
        Durability::Sync,
    )
    .unwrap();
    st.sync().unwrap();
    st.reclaim().unwrap();
    let stats = st.stats();
    assert!(
        stats.commits >= 1 && stats.syncs >= 1 && stats.flushes >= 1 && stats.reclaims >= 1,
        "stats: {stats:?}"
    );
    assert!(stats.index_bytes_estimate > 0);
    let info = st.info();
    assert!(
        info.encrypted && info.encryption == Encryption::Aes256Gcm,
        "info: {info:?}"
    );
    assert_eq!(info.compression, Compression::DEFLATE.0);
    assert_eq!(info.wrapping_key_id, "diag-key");
    assert_ne!(info.store_id, [0u8; 16]);
    assert!(
        info.generation > 0 && info.epoch > 0 && info.last_group_id > 0,
        "info: {info:?}"
    );
    assert!(
        info.next_file_id > info.active_file_id && info.active_file_id > 0,
        "info: {info:?}"
    );
    let files = st.file_stats();
    assert!(!files.is_empty());
    for w in files.windows(2) {
        assert!(w[1].file_id > w[0].file_id, "not ascending: {files:?}");
    }
    let (mut total, mut live) = (0, 0);
    for f in &files {
        total += f.total_records;
        live += f.live_records;
        assert!(
            f.live_records + f.dead_records <= f.total_records,
            "file overcounts: {f:?}"
        );
    }
    assert!(total > 0 && live > 0 && live <= total);
    assert_eq!(live, stats.live_records);
    assert!(total >= stats.keys);
    st.close().unwrap();
}
