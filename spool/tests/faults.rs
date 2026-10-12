//! Fault injection: terminal vs ordinary failures (port of the
//! core `spool_fault_test.go` cases).

#[path = "common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::{load_all, test_options};
use spool::{load, open, read_key_hint, Durability, ErrorKind, FaultHooks, Mutation};

/// A hook armed by an atomic flag so tests can arm/disarm around
/// operations (the store holds `Arc<FaultHooks>`).
fn gated_hook(armed: &Arc<AtomicBool>, msg: &str) -> spool::FaultHook {
    let armed = armed.clone();
    let msg = msg.to_string();
    Arc::new(move || {
        if armed.load(Ordering::SeqCst) {
            Err(msg.clone())
        } else {
            Ok(())
        }
    })
}

#[test]
fn fault_append_terminal() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let armed = Arc::new(AtomicBool::new(false));
    let hooks = FaultHooks {
        append: Some(gated_hook(&armed, "boom-append")),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.faults = Some(Arc::new(hooks));
    let st = open(&o).unwrap();
    st.put(b"good", b"v").unwrap();
    st.flush().unwrap();
    armed.store(true, Ordering::SeqCst);
    let muts = [Mutation::put(b"bad".to_vec(), b"v".to_vec())];
    assert!(
        st.commit(&muts, Durability::Sync).is_err(),
        "commit during append fault succeeded"
    );
    let se = st
        .storage_error()
        .expect("nil StorageError after terminal failure");
    assert!(
        se.to_string().contains("boom-append"),
        "StorageError = {se}"
    );
    let err = st.put(b"k", b"v").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::StorageFailed, "got {err}");
    let err = st.sync().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::StorageFailed, "got {err}");
    assert!(st.close().is_err(), "Close on terminal store succeeded");
    let st2 = open(&test_options(dir)).unwrap();
    let got = load_all(dir);
    assert_eq!(got.get(&b"good"[..]).map(|b| b.as_slice()), Some(&b"v"[..]));
    assert!(!got.contains_key(&b"bad"[..]), "failed commit survived");
    st2.put(b"after", b"v").unwrap();
    st2.close().unwrap();
}

#[test]
fn fault_sync_terminal() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let armed = Arc::new(AtomicBool::new(false));
    let hooks = FaultHooks {
        segment_sync: Some(gated_hook(&armed, "boom-sync")),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.faults = Some(Arc::new(hooks));
    let st = open(&o).unwrap();
    st.put(b"good", b"v").unwrap();
    st.flush().unwrap();
    armed.store(true, Ordering::SeqCst);
    let muts = [Mutation::put(b"k".to_vec(), b"v".to_vec())];
    assert!(
        st.commit(&muts, Durability::Sync).is_err(),
        "sync commit during sync fault succeeded"
    );
    assert!(
        st.storage_error().is_some(),
        "nil StorageError after terminal failure"
    );
    st.close().unwrap_or(());
    let st2 = open(&test_options(dir)).unwrap();
    let got = load_all(dir);
    assert_eq!(got.get(&b"good"[..]).map(|b| b.as_slice()), Some(&b"v"[..]));
    st2.close().unwrap();
}

#[test]
fn fault_manifest_terminal() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let armed = Arc::new(AtomicBool::new(false));
    let hooks = FaultHooks {
        manifest_persist: Some(gated_hook(&armed, "boom-manifest")),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.faults = Some(Arc::new(hooks));
    let st = open(&o).unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(
        st.rotate_key().is_err(),
        "RotateKey during manifest fault succeeded"
    );
    let err = st.put(b"k", b"v").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::StorageFailed, "got {err}");
    st.close().unwrap_or(());
    let st2 = open(&test_options(dir)).unwrap();
    st2.close().unwrap();
}

#[test]
fn fault_keyring_atomic() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let armed = Arc::new(AtomicBool::new(false));
    let hooks = FaultHooks {
        keyring_persist: Some(gated_hook(&armed, "boom-keyring")),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.faults = Some(Arc::new(hooks));
    let st = open(&o).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(
        st.rotate_key().is_err(),
        "RotateKey during keyring fault succeeded"
    );
    let inv = st.key_inventory();
    assert_eq!(inv.data_keys.len(), 1);
    assert_eq!(inv.active_data_key_id, 1);
    st.close().unwrap_or(());
    let st2 = open(&test_options(dir)).unwrap();
    assert_eq!(st2.key_inventory().data_keys.len(), 1);
    let got = load_all(dir);
    assert_eq!(got.get(&b"k"[..]).map(|b| b.as_slice()), Some(&b"v"[..]));
    st2.close().unwrap();
}

#[test]
fn fault_keyring_wrapping_atomic() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let armed = Arc::new(AtomicBool::new(false));
    let hooks = FaultHooks {
        keyring_persist: Some(gated_hook(&armed, "boom-keyring")),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.wrapping_key_id = "w1".to_string();
    o.faults = Some(Arc::new(hooks));
    let st = open(&o).unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(
        st.rotate_wrapping_key(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "w2")
            .is_err(),
        "RotateWrappingKey during fault succeeded"
    );
    assert_eq!(st.key_inventory().wrapping_key_id, "w1");
    st.close().unwrap_or(());
    let st2 = open(&test_options(dir)).unwrap();
    let hint = read_key_hint(dir).unwrap();
    assert_eq!(hint.wrapping_key_id, "w1");
    st2.close().unwrap();
}

#[test]
fn fault_intent_ordinary() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let armed = Arc::new(AtomicBool::new(false));
    let hooks = FaultHooks {
        intent_persist: Some(gated_hook(&armed, "boom-intent")),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.faults = Some(Arc::new(hooks));
    let st = open(&o).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(
        st.rewrite_data_keys().is_err(),
        "rewrite during intent fault succeeded"
    );
    assert!(
        st.storage_error().is_none(),
        "ordinary failure went terminal"
    );
    st.put(b"k2", b"v2").unwrap();
    armed.store(false, Ordering::SeqCst);
    st.rewrite_data_keys().unwrap();
    st.close().unwrap();
}

#[test]
fn fault_compaction_stage_retryable() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let armed = Arc::new(AtomicBool::new(false));
    let hooks = FaultHooks {
        compaction_stage: Some(gated_hook(&armed, "boom-compact")),
        ..Default::default()
    };
    let mut o = common::compact_options(dir);
    o.faults = Some(Arc::new(hooks));
    let st = open(&o).unwrap();
    for i in 0..100 {
        st.put(
            format!("k{i:03}").as_bytes(),
            &common::incompressible(300, i as u64),
        )
        .unwrap();
    }
    st.flush().unwrap();
    for i in 0..90 {
        st.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
    }
    st.flush().unwrap();
    st.put(b"extra", b"v").unwrap();
    st.flush().unwrap();
    armed.store(true, Ordering::SeqCst);
    // Faulted passes fail ordinary (never terminal) until disarmed.
    for _ in 0..5 {
        let r = st.reclaim();
        assert!(
            st.storage_error().is_none(),
            "compaction fault went terminal: {r:?}"
        );
    }
    armed.store(false, Ordering::SeqCst);
    for _ in 0..200 {
        if st.stats().compactions > 0 {
            break;
        }
        st.reclaim().unwrap();
    }
    assert!(st.stats().compactions > 0, "no compaction after disarm");
    // No staging litter left behind.
    for ent in std::fs::read_dir(dir.join("segments")).unwrap() {
        let name = ent.unwrap().file_name().to_str().unwrap().to_string();
        assert!(!name.starts_with(".cmp-"), "staging litter: {name}");
    }
    let got = load_all(dir);
    assert_eq!(got.len(), 101);
    for i in 0..90 {
        let k = format!("k{i:03}");
        assert_eq!(
            got.get(k.as_bytes()).map(|b| b.as_slice()),
            Some(&b"new"[..]),
            "{k}"
        );
    }
    st.close().unwrap();
}

#[test]
fn orphan_sweep_at_open() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    for i in 0..10 {
        st.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    st.flush().unwrap();
    st.close().unwrap();
    let seg_dir = dir.join("segments");
    let raw = std::fs::read(seg_dir.join("000000000001.spool")).unwrap();
    let planted = [
        seg_dir.join("000000000009.spool"), // unlisted final name
        seg_dir.join(".cmp-1-.tmp-orphan"), // crashed compaction staging
        seg_dir.join(".rew-1-.tmp-orphan"), // crashed maintenance staging
        dir.join(".keys.tmp-orphan"),       // crashed root temp
    ];
    for p in &planted {
        std::fs::write(p, &raw).unwrap();
    }
    // Sweep-delete fault fails the open with the injection cause.
    let hooks = FaultHooks {
        delete: Some(Arc::new(|| Err("boom-delete".to_string()))),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.faults = Some(Arc::new(hooks));
    let err = open(&o).unwrap_err();
    assert!(err.to_string().contains("boom-delete"), "got {err}");
    // Disarmed: the sweep removes every orphan.
    let st2 = open(&test_options(dir)).unwrap();
    for p in &planted {
        assert!(!p.exists(), "orphan survives: {p:?}");
    }
    let key = common::master_key();
    let mut n = 0;
    load(dir.to_str().unwrap(), &key, &mut |recs| {
        n += recs.len();
        Ok(())
    })
    .unwrap();
    assert_eq!(n, 10);
    st2.close().unwrap();
}
