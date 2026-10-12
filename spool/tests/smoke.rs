//! Smoke tests: basic open/write/reopen round-trips.

use std::collections::HashMap;
use std::time::Duration;

use spool::{default_options, load, open, open_and_load, Durability, Encryption, Mutation};

fn test_opts(dir: &std::path::Path, encrypted: bool) -> spool::Options {
    let mut o = default_options(dir.to_str().unwrap().to_string());
    o.reclaim_interval = None; // deterministic tests
    o.flush.max_delay = Duration::from_millis(5);
    if encrypted {
        o.master_key = vec![7u8; 32];
    } else {
        o.encryption = Encryption::None;
    }
    o
}

fn collect(path: &str, key: &[u8]) -> HashMap<Vec<u8>, (Vec<u8>, bool)> {
    let mut m = HashMap::new();
    load(path, key, &mut |recs| {
        for r in recs {
            m.insert(r.key.clone(), (r.value.clone(), r.deleted));
        }
        Ok(())
    })
    .unwrap();
    m
}

#[test]
fn smoke_encrypted_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let mut o = test_opts(path, true);
    o.master_key = vec![9u8; 32];
    let store = open(&o).unwrap();
    store.put(b"hello", b"world").unwrap();
    store.put(b"k2", b"v2").unwrap();
    store.delete(b"gone").unwrap();
    store.flush().unwrap();
    let st = store.stats();
    assert_eq!(st.keys, 2, "stats: {st:?}");
    assert!(st.groups_written >= 1);
    store.close().unwrap();

    // Reopen and load everything back.
    let got = collect(path.to_str().unwrap(), &[9u8; 32]);
    assert_eq!(got.get(&b"hello"[..]), Some(&(b"world".to_vec(), false)));
    assert_eq!(got.get(&b"k2"[..]), Some(&(b"v2".to_vec(), false)));
    assert_eq!(got.get(&b"gone"[..]), Some(&(Vec::new(), true)));

    // OpenAndLoad delivers each key once.
    let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();
    let store = open_and_load(&o, &mut |recs| {
        for r in recs {
            *seen.entry(r.key.clone()).or_insert(0) += 1;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(seen.len(), 3);
    assert!(
        seen.values().all(|c| *c == 1),
        "each key delivered once: {seen:?}"
    );
    store.close().unwrap();
}

#[test]
fn smoke_plaintext_batches_and_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let o = test_opts(dir.path(), false);
    let store = open(&o).unwrap();
    let pairs: Vec<spool::KV> = (0..100)
        .map(|i| spool::KV::new(format!("k{i:03}"), format!("v{i}")))
        .collect();
    store.put_batch(&pairs).unwrap();
    store
        .commit(
            &[
                Mutation::put(b"a".to_vec(), b"1".to_vec()),
                Mutation::put(b"b".to_vec(), b"2".to_vec()),
                Mutation::delete(b"k000".to_vec()),
            ],
            Durability::Sync,
        )
        .unwrap();
    store
        .delete_batch(&[b"k001".to_vec(), b"k002".to_vec()])
        .unwrap();
    store.sync().unwrap();
    let st = store.stats();
    assert_eq!(st.keys, 99, "100 puts - 3 deletes + 2 commit puts = {st:?}");
    store.close().unwrap();

    let got = collect(&path, &[]);
    assert_eq!(got.len(), 102); // 99 live + k000,k001,k002 delivered as tombs
    assert_eq!(got[&b"a"[..]], (b"1".to_vec(), false));
    assert!(got[&b"k000"[..]].1);
}

#[test]
fn smoke_wrong_key_fails() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = test_opts(dir.path(), true);
    o.master_key = vec![1u8; 32];
    let store = open(&o).unwrap();
    store.put(b"k", b"v").unwrap();
    store.flush().unwrap();
    store.close().unwrap();

    o.master_key = vec![2u8; 32];
    let err = open(&o).unwrap_err();
    assert_eq!(err.kind(), spool::ErrorKind::WrongKey, "got {err}");
}

#[test]
fn smoke_double_open_locked() {
    let dir = tempfile::tempdir().unwrap();
    let o = test_opts(dir.path(), false);
    let store = open(&o).unwrap();
    let err = open(&o).unwrap_err();
    assert_eq!(err.kind(), spool::ErrorKind::Locked, "got {err}");
    store.close().unwrap();
}

#[test]
fn smoke_info_and_file_stats() {
    let dir = tempfile::tempdir().unwrap();
    let o = test_opts(dir.path(), false);
    let store = open(&o).unwrap();
    store.put(b"k", b"v").unwrap();
    store.flush().unwrap();
    let info = store.info();
    assert!(!info.encrypted);
    assert_eq!(info.epoch, 1);
    assert!(info.last_group_id >= 1);
    let fs = store.file_stats();
    assert_eq!(fs.len(), 1);
    assert_eq!(fs[0].live_records, 1);
    store.close().unwrap();
}
