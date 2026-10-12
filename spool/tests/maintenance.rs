//! Key rewrite and context rebind (port of `spool_maint_test.go`).

#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use common::{incompressible, load_all, load_all_seq, test_options};
use spool::{open, read_key_hint, Encryption, ErrorKind, Mutation};

#[test]
fn rewrite_data_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"old", b"v1").unwrap();
    st.flush().unwrap();
    st.rotate_key().unwrap();
    st.put(b"new", b"v2").unwrap();
    st.flush().unwrap();
    let refs = st.key_references().unwrap();
    assert_eq!(refs.len(), 2, "refs: {refs:?}");
    let gen_before = st.key_inventory().manifest_generation;
    st.rewrite_data_keys().unwrap();
    let refs = st.key_references().unwrap();
    for r in &refs {
        assert!(
            !(r.key_id == 1 && r.blocks != 0),
            "key 1 still has {} blocks",
            r.blocks
        );
    }
    assert_eq!(st.key_inventory().manifest_generation, gen_before + 1);
    assert_eq!(st.prune_data_keys().unwrap(), vec![1]);
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.get(&b"old"[..]).map(|b| b.as_slice()), Some(&b"v1"[..]));
    assert_eq!(got.get(&b"new"[..]).map(|b| b.as_slice()), Some(&b"v2"[..]));
}

#[test]
fn rewrite_preserves_sequences() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    for i in 0..50 {
        st.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    st.flush().unwrap();
    st.delete(b"k0").unwrap();
    st.flush().unwrap();
    st.close().unwrap();
    let before = load_all_seq(dir);
    let st2 = open(&test_options(dir)).unwrap();
    st2.rewrite_data_keys().unwrap();
    st2.close().unwrap();
    let after = load_all_seq(dir);
    assert_eq!(before.len(), after.len());
    for (k, seq) in &before {
        assert_eq!(after.get(k), Some(seq), "key {k:?} sequence moved");
    }
}

#[test]
fn rebind_context() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.context_id = b"cccccccccccccccc".to_vec();
    o.wrapping_key_id = "rebind-key".to_string();
    let st = open(&o).unwrap();
    for i in 0..20 {
        st.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    st.flush().unwrap();
    let target = *b"dddddddddddddddd";
    st.rebind_context(target).unwrap();
    // New writes under the new context work on the live handle.
    st.put(b"after", b"v").unwrap();
    st.close().unwrap();
    let hint = read_key_hint(dir).unwrap();
    assert_eq!(hint.context_id, target);
    assert_eq!(hint.wrapping_key_id, "rebind-key");
    // Old context fails; new context opens with all data.
    let err = open(&o).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ContextMismatch, "got {err}");
    let mut o2 = test_options(dir);
    o2.context_id = target.to_vec();
    o2.wrapping_key_id = "rebind-key".to_string();
    let st2 = open(&o2).unwrap();
    st2.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 21);
    assert_eq!(
        got.get(&b"after"[..]).map(|b| b.as_slice()),
        Some(&b"v"[..])
    );
}

#[test]
fn maintenance_gates_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let st = open(&test_options(&dir)).unwrap();
    for i in 0..200 {
        st.put(
            format!("k{i:04}").as_bytes(),
            &incompressible(100 << 10, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    let st2 = st.clone();
    let done = std::thread::spawn(move || st2.rewrite_data_keys());
    // Wait until the rewrite visibly holds the store.
    let mut saw_active = false;
    for _ in 0..5000 {
        if done.is_finished() {
            panic!(
                "rewrite finished before gate observed: {:?}",
                done.join().unwrap()
            );
        }
        if st.key_inventory().maintenance_phase != "idle" {
            saw_active = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(saw_active, "rewrite never visibly active");
    let err = st.put(b"k", b"v").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Maintenance, "got {err}");
    let err = st
        .commit(
            &[Mutation::put(b"k".to_vec(), Vec::new())],
            spool::Durability::Async,
        )
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Maintenance, "got {err}");
    let err = st.rewrite_data_keys().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Maintenance, "got {err}");
    // Memory-state reads stay available throughout.
    let _ = st.stats();
    let _ = st.key_inventory();
    done.join().unwrap().unwrap();
    st.put(b"after", b"v").unwrap();
    st.close().unwrap();
    assert_eq!(load_all(&dir).len(), 201);
}

#[test]
fn maintenance_plain_stores() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.encryption = Encryption::None;
    o.master_key = Vec::new();
    let st = open(&o).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    st.rewrite_data_keys().unwrap();
    let target = *b"eeeeeeeeeeeeeeee";
    st.rebind_context(target).unwrap();
    st.close().unwrap();
    let hint = read_key_hint(dir).unwrap();
    assert_eq!(hint.context_id, target);
    let mut o2 = test_options(dir);
    o2.encryption = Encryption::None;
    o2.master_key = Vec::new();
    o2.context_id = target.to_vec();
    let st2 = open(&o2).unwrap();
    st2.close().unwrap();
}
