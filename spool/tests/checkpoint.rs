//! Backup checkpoints (port of the core `spool_ckpt_test.go` cases).

#[path = "common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::{incompressible, load_all, master_key, segment_count, test_options};
use spool::{load, open, Checkpoint, ErrorKind, FaultHooks, Mutation, Store};

/// Copy a checkpoint's stream set into a fresh directory, mimicking
/// a backup restore.
fn materialize_checkpoint(cp: &Checkpoint) -> tempfile::TempDir {
    let dst = tempfile::tempdir().unwrap();
    for f in cp.files() {
        let rel = f.strip_prefix(&cp.dir).unwrap();
        assert!(
            !rel.to_str().unwrap().starts_with(".."),
            "escapes {:?}",
            cp.dir
        );
        let out = dst.path().join(rel);
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::copy(f, &out).unwrap();
    }
    dst
}

fn load_dir(dir: &Path) -> HashMap<Vec<u8>, Vec<u8>> {
    let key = master_key();
    let mut got = HashMap::new();
    load(dir.to_str().unwrap(), &key, &mut |recs| {
        for r in recs {
            if r.deleted {
                got.remove(&r.key);
            } else {
                got.insert(r.key.clone(), r.value.clone());
            }
        }
        Ok(())
    })
    .unwrap();
    got
}

fn must_checkpoint(st: &Store, dir: &Path, name: &str) -> Checkpoint {
    st.checkpoint(&dir.join(name), None).unwrap()
}

#[test]
fn checkpoint_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    for i in 0..20 {
        st.put(format!("k{i:02}").as_bytes(), b"v").unwrap();
    }
    st.delete(b"k00").unwrap();
    st.flush().unwrap();
    let cp = must_checkpoint(&st, dir, "backup");
    assert!(cp.generation > 0 && !cp.members.is_empty() && cp.files().len() >= 2);
    assert_eq!(st.stats().checkpoints, 1);
    let list = st.list_checkpoints();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, cp.id);
    // The COMPLETE marker carries the magic, generation, and time.
    let raw = std::fs::read(cp.dir.join("COMPLETE")).unwrap();
    let text = String::from_utf8(raw).unwrap();
    let lines: Vec<&str> = text.split('\n').collect();
    assert_eq!(lines.len(), 4, "COMPLETE = {text:?}");
    assert_eq!(lines[0], "spool-checkpoint/1");
    assert_eq!(lines[1], cp.generation.to_string());
    assert_eq!(lines[3], "");
    // The destination opens directly as a live store.
    let rst = open(&test_options(&cp.dir)).unwrap();
    assert_eq!(rst.stats().keys, 19);
    rst.close().unwrap();
    // The streamed copy verifies standalone too.
    let restored = materialize_checkpoint(&cp);
    let got = load_dir(restored.path());
    assert_eq!(got.len(), 19);
    assert!(got.contains_key(&b"k01"[..]));
    assert!(!got.contains_key(&b"k00"[..]), "restored deleted k00");
    cp.release().unwrap();
    assert_eq!(st.stats().checkpoints, 0);
    // Release is idempotent; the caller-owned destination survives.
    cp.release().unwrap();
    st.release_checkpoint(&cp.id).unwrap();
    st.release_checkpoint("missing").unwrap();
    assert!(
        cp.dir.join("COMPLETE").exists(),
        "destination removed by release"
    );
    // The live store keeps validating after release.
    st.put(b"after", b"v").unwrap();
    st.close().unwrap();
    let st2 = open(&test_options(dir)).unwrap();
    assert_eq!(st2.stats().keys, 20);
    st2.close().unwrap();
}

#[test]
fn checkpoint_stable_under_mutations() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.max_segment_size = 16 << 10;
    o.max_block_bytes = 8 << 10;
    o.target_block_bytes = 4 << 10;
    o.max_records_per_block = 100;
    o.max_key_size = 64;
    o.max_value_size = 2048;
    let st = open(&o).unwrap();
    let gen1: Vec<Mutation> = (0..20)
        .map(|i| Mutation::put(format!("k{i:02}").into_bytes(), incompressible(1000, i)))
        .collect();
    st.commit(&gen1, spool::Durability::Sync).unwrap();
    st.rotate_key().unwrap();
    let gen2: Vec<Mutation> = (0..20)
        .map(|i| {
            Mutation::put(
                format!("k{i:02}").into_bytes(),
                incompressible(1000, 100 + i),
            )
        })
        .collect();
    st.commit(&gen2, spool::Durability::Sync).unwrap();
    let cp = must_checkpoint(&st, dir, "backup");
    // Mutate heavily: reclaim, rewrite, rotate, rewrite, prune.
    for _ in 0..10 {
        st.reclaim().unwrap();
    }
    st.rewrite_data_keys().unwrap();
    st.rotate_key().unwrap();
    st.rewrite_data_keys().unwrap();
    let pruned = st.prune_data_keys().unwrap();
    assert!(pruned.is_empty(), "pruned {pruned:?} with live checkpoint");
    let refs = st.key_references().unwrap();
    assert!(
        refs.iter().any(|r| r.key_id == 1 && r.checkpoints > 0),
        "key 1 has no checkpoint ref: {refs:?}"
    );
    // The checkpoint still opens with its cut (gen2 winners).
    let restored = materialize_checkpoint(&cp);
    let got = load_dir(restored.path());
    assert_eq!(got.len(), 20);
    for i in 0..20u64 {
        let k = format!("k{i:02}");
        assert_eq!(
            got.get(k.as_bytes()),
            Some(&incompressible(1000, 100 + i)),
            "{k} mismatch"
        );
    }
    // Release frees the keys: both retired generations prune away.
    cp.release().unwrap();
    let pruned = st.prune_data_keys().unwrap();
    assert_eq!(pruned, vec![1, 2]);
    // The destination is self-contained after the live prune.
    assert_eq!(load_dir(&cp.dir).len(), 20);
    st.close().unwrap();
}

#[test]
fn checkpoint_release_isolation() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k1", b"v1").unwrap();
    st.flush().unwrap();
    let cp1 = must_checkpoint(&st, dir, "cp1");
    st.put(b"k2", b"v2").unwrap();
    st.flush().unwrap();
    let cp2 = must_checkpoint(&st, dir, "cp2");
    assert_ne!(cp1.id, cp2.id);
    cp1.release().unwrap();
    assert!(
        cp1.dir.join("COMPLETE").exists(),
        "released destination removed"
    );
    let restored = materialize_checkpoint(&cp2);
    let got = load_dir(restored.path());
    assert_eq!(got.get(&b"k1"[..]).map(|b| b.as_slice()), Some(&b"v1"[..]));
    assert_eq!(got.get(&b"k2"[..]).map(|b| b.as_slice()), Some(&b"v2"[..]));
    cp2.release().unwrap();
    st.close().unwrap();
}

#[test]
fn checkpoint_reopen_forgets_registry() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    let cp = must_checkpoint(&st, dir, "backup");
    st.close().unwrap();
    let st2 = open(&test_options(dir)).unwrap();
    assert!(st2.list_checkpoints().is_empty());
    let got = load_dir(&cp.dir);
    assert_eq!(got.get(&b"k"[..]).map(|b| b.as_slice()), Some(&b"v"[..]));
    // The stale handle releases against its (closed) store without error.
    cp.release().unwrap();
    st2.close().unwrap();
}

#[test]
fn checkpoint_empty_store() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    let cp = must_checkpoint(&st, dir, "backup");
    assert_eq!(load_dir(&cp.dir).len(), 0);
    cp.release().unwrap();
    st.close().unwrap();
}

#[test]
fn checkpoint_survives_rebind() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.context_id = b"cccccccccccccccc".to_vec();
    let st = open(&o).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    let cp = must_checkpoint(&st, dir, "backup");
    let target = *b"dddddddddddddddd";
    st.rebind_context(target).unwrap();
    st.put(b"k2", b"v2").unwrap();
    st.close().unwrap();
    // The checkpoint opens under its original context with its cut.
    let restored = materialize_checkpoint(&cp);
    let got = load_dir(restored.path());
    assert_eq!(got.len(), 1);
    assert_eq!(got.get(&b"k"[..]).map(|b| b.as_slice()), Some(&b"v"[..]));
    let restored2 = materialize_checkpoint(&cp);
    let hint = spool::read_key_hint(restored2.path()).unwrap();
    assert_eq!(hint.context_id, *b"cccccccccccccccc");
    // The live store moved on with an empty registry.
    let mut o2 = test_options(dir);
    o2.context_id = target.to_vec();
    let st2 = open(&o2).unwrap();
    assert!(st2.list_checkpoints().is_empty());
    st2.close().unwrap();
}

#[test]
fn checkpoint_destination_must_be_new() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    let taken = dir.join("taken");
    std::fs::create_dir(&taken).unwrap();
    std::fs::write(taken.join("sentinel"), b"caller-data").unwrap();
    let err = st.checkpoint(&taken, None).unwrap_err();
    assert!(err.to_string().contains("new directory"), "got {err}");
    let raw = std::fs::read(taken.join("sentinel")).unwrap();
    assert_eq!(raw, b"caller-data");
    let plain = dir.join("plain");
    std::fs::write(&plain, b"x").unwrap();
    assert!(st.checkpoint(&plain, None).is_err());
    assert!(st.checkpoint(Path::new(""), None).is_err());
    // No cancellation: None captures unconditionally.
    let cp = st.checkpoint(&dir.join("ok"), None).unwrap();
    cp.release().unwrap();
    st.close().unwrap();
}

#[test]
fn checkpoint_cancel_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    // Three flushed rounds force three segments, so the mid-capture
    // cancel lands between links.
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let links = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let links2 = links.clone();
    let cancel2 = cancel_flag.clone();
    let hooks = FaultHooks {
        checkpoint_link: Some(Arc::new(move || {
            if links2.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                cancel2.store(true, Ordering::SeqCst);
            }
            Ok(())
        })),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.faults = Some(Arc::new(hooks));
    o.max_segment_size = 4096;
    o.target_block_bytes = 1024;
    o.max_block_bytes = 2048;
    o.max_records_per_block = 100;
    o.max_key_size = 64;
    o.max_value_size = 1024;
    let st = open(&o).unwrap();
    for round in 0..3 {
        for i in 0..10 {
            st.put(
                format!("r{round}/k{i:02}").as_bytes(),
                &incompressible(300, round * 10 + i),
            )
            .unwrap();
        }
        st.flush().unwrap();
    }
    assert!(segment_count(dir) >= 3);
    // Pre-canceled capture takes nothing.
    let pre = AtomicBool::new(true);
    let err = st.checkpoint(&dir.join("pre"), Some(&pre)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Canceled, "got {err}");
    assert!(!dir.join("pre").exists());
    // Mid-capture cancel removes the partial destination and
    // registers nothing.
    let dst = dir.join("mid");
    let err = st.checkpoint(&dst, Some(&cancel_flag)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Canceled, "got {err}");
    assert!(links.load(Ordering::SeqCst) >= 2);
    assert!(!dst.exists());
    assert!(st.list_checkpoints().is_empty());
    st.close().unwrap();
}

#[test]
fn checkpoint_link_fault() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let armed = Arc::new(AtomicBool::new(false));
    let armed2 = armed.clone();
    let hooks = FaultHooks {
        checkpoint_link: Some(Arc::new(move || {
            if armed2.load(Ordering::SeqCst) {
                Err("boom-link".to_string())
            } else {
                Ok(())
            }
        })),
        ..Default::default()
    };
    let mut o = test_options(dir);
    o.faults = Some(Arc::new(hooks));
    let st = open(&o).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    armed.store(true, Ordering::SeqCst);
    let dst = dir.join("backup");
    let err = st.checkpoint(&dst, None).unwrap_err();
    assert!(err.to_string().contains("boom-link"), "got {err}");
    assert!(!dst.exists());
    assert!(st.list_checkpoints().is_empty());
    assert!(
        st.storage_error().is_none(),
        "ordinary failure went terminal"
    );
    armed.store(false, Ordering::SeqCst);
    let cp = st.checkpoint(&dst, None).unwrap();
    assert_eq!(load_all(&cp.dir).len(), 1);
    cp.release().unwrap();
    st.close().unwrap();
}
