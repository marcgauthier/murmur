//! Key lifecycle: rotation, inventory, pruning, contexts (port of
//! `spool_rotation_test.go`, `spool_keymgmt_test.go`, and the rotation
//! half of `spool_batch_test.go`).

#[path = "common/mod.rs"]
mod common;

use std::collections::HashMap;

use common::{incompressible, load_all, master_key, segment_count, test_options};
use spool::{
    load_with_options, open, read_key_hint, Durability, Encryption, ErrorKind, KeyStatus,
    LoadOptions, Mutation,
};

#[test]
fn key_rotation() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"gen1", b"v1").unwrap();
    st.flush().unwrap();
    st.rotate_key().unwrap();
    st.put(b"gen2", b"v2").unwrap();
    st.flush().unwrap();
    st.rotate_key().unwrap();
    st.put(b"gen3", b"v3").unwrap();
    st.close().unwrap();
    let got = load_all(dir);
    for (k, v) in [("gen1", "v1"), ("gen2", "v2"), ("gen3", "v3")] {
        assert_eq!(
            got.get(k.as_bytes()).map(|b| b.as_slice()),
            Some(v.as_bytes()),
            "{k}"
        );
    }
    // Reopen once more: the ring with three keys persists.
    let st2 = open(&test_options(dir)).unwrap();
    st2.put(b"gen4", b"v4").unwrap();
    st2.flush().unwrap();
    st2.rotate_key().unwrap();
    st2.close().unwrap();
}

#[test]
fn rotate_key_plain_store_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let mut o = test_options(tmp.path());
    o.encryption = Encryption::None;
    o.master_key = Vec::new();
    let st = open(&o).unwrap();
    assert!(
        st.rotate_key().is_err(),
        "RotateKey on plain store succeeded"
    );
    st.close().unwrap();
}

#[test]
fn rotate_master_key() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"v").unwrap();
    st.rotate_key().unwrap();
    st.put(b"k2", b"v2").unwrap();
    let new_master: Vec<u8> = (0..32).map(|i| 255 - i as u8).collect();
    st.rotate_master_key(&new_master, "").unwrap();
    // Rotation must not disturb the data-key line: rotate again and
    // keep writing under the new protector.
    st.rotate_key().unwrap();
    st.put(b"k3", b"v3").unwrap();
    st.close().unwrap();
    // Old material fails.
    let err = open(&test_options(dir)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::WrongKey, "got {err}");
    // New material opens with everything intact.
    let mut o = test_options(dir);
    o.master_key = new_master.clone();
    let st2 = open(&o).unwrap();
    st2.close().unwrap();
    let mut got: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut seqs: HashMap<Vec<u8>, u64> = HashMap::new();
    let lo = LoadOptions {
        path: dir.to_str().unwrap().to_string(),
        master_key: new_master,
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
    for (k, v) in [("k", "v"), ("k2", "v2"), ("k3", "v3")] {
        assert_eq!(
            got.get(k.as_bytes()).map(|b| b.as_slice()),
            Some(v.as_bytes()),
            "{k}"
        );
    }
}

#[test]
fn rotate_master_key_validation() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    assert!(
        st.rotate_master_key(&[], "").is_err(),
        "empty rotation accepted"
    );
    assert!(
        st.rotate_master_key(&master_key(), "x").is_err(),
        "dual-material rotation accepted"
    );
    assert!(
        st.rotate_master_key(b"short", "").is_err(),
        "short master rotation accepted"
    );
    // Switch master key -> passphrase and back.
    st.rotate_master_key(&[], "correct horse battery staple")
        .unwrap();
    st.put(b"k", b"v").unwrap();
    st.close().unwrap();
    let mut o = test_options(dir);
    o.master_key = Vec::new();
    o.passphrase = "correct horse battery staple".to_string();
    let st2 = open(&o).unwrap();
    st2.rotate_master_key(&master_key(), "").unwrap();
    st2.close().unwrap();

    let tmp2 = tempfile::tempdir().unwrap();
    let mut po = test_options(tmp2.path());
    po.encryption = Encryption::None;
    po.master_key = Vec::new();
    let plain = open(&po).unwrap();
    assert!(
        plain.rotate_master_key(&master_key(), "").is_err(),
        "rotation on plain store accepted"
    );
    plain.close().unwrap();
}

#[test]
fn rotate_wrapping_key() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.wrapping_key_id = "key-1".to_string();
    let st = open(&o).unwrap();
    st.put(b"k", b"v").unwrap();
    st.flush().unwrap();
    let new_key = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_vec();
    st.rotate_wrapping_key(&new_key, "key-2").unwrap();
    assert_eq!(st.key_inventory().wrapping_key_id, "key-2");
    st.put(b"k2", b"v2").unwrap();
    st.close().unwrap();
    let hint = read_key_hint(dir).unwrap();
    assert_eq!(hint.wrapping_key_id, "key-2");
    // Old material fails; new material opens with both keys' data.
    assert!(
        open(&test_options(dir)).is_err(),
        "open with old wrapping key succeeded"
    );
    let mut o2 = test_options(dir);
    o2.master_key = new_key.clone();
    o2.wrapping_key_id = "key-2".to_string();
    let st2 = open(&o2).unwrap();
    st2.close().unwrap();
    let mut got: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let lo = LoadOptions {
        path: dir.to_str().unwrap().to_string(),
        master_key: new_key,
        ..Default::default()
    };
    load_with_options(lo, &mut |recs| {
        for r in recs {
            if !r.deleted {
                got.insert(r.key.clone(), r.value.clone());
            }
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(got.get(&b"k"[..]).map(|b| b.as_slice()), Some(&b"v"[..]));
    assert_eq!(got.get(&b"k2"[..]).map(|b| b.as_slice()), Some(&b"v2"[..]));
    // Standalone master rotation still works on top.
    let st3 = open(&o2).unwrap();
    st3.rotate_master_key(b"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", "")
        .unwrap();
    assert_eq!(st3.key_inventory().wrapping_key_id, "key-2");
    st3.close().unwrap();
}

#[test]
fn key_inventory() {
    let tmp = tempfile::tempdir().unwrap();
    let mut o = test_options(tmp.path());
    o.wrapping_key_id = "inv-key".to_string();
    let st = open(&o).unwrap();
    st.rotate_key().unwrap();
    st.rotate_key().unwrap();
    let inv = st.key_inventory();
    assert!(inv.encrypted);
    assert_eq!(inv.wrapping_key_id, "inv-key");
    assert_eq!(
        (inv.active_data_key_id, inv.data_keys.len()),
        (3, 3),
        "inv: {inv:?}"
    );
    for (i, k) in inv.data_keys.iter().enumerate() {
        assert_eq!(k.id, (i + 1) as u32, "not sorted: {:?}", inv.data_keys);
        assert_ne!(
            k.created_at,
            std::time::SystemTime::UNIX_EPOCH,
            "key {} lacks time",
            k.id
        );
    }
    assert_eq!(inv.data_keys[2].status, KeyStatus::Active);
    assert_eq!(inv.data_keys[0].status, KeyStatus::Retired);
    assert!(inv.keyring_seq > 0 && inv.manifest_generation > 0);
    assert_eq!(inv.maintenance_phase, "idle");
    st.close().unwrap();
}

#[test]
fn prune_data_keys() {
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
    // Generation 1: one oversized group in its own sealed file.
    let gen1: Vec<Mutation> = (0..20)
        .map(|i| Mutation::put(format!("k{i:02}").into_bytes(), incompressible(1000, i)))
        .collect();
    st.commit(&gen1, Durability::Sync).unwrap();
    st.rotate_key().unwrap();
    // Generation 2 overwrites every key; the committed group lands
    // in a fresh file because generation 1 sealed its own.
    let gen2: Vec<Mutation> = (0..20)
        .map(|i| {
            Mutation::put(
                format!("k{i:02}").into_bytes(),
                incompressible(1000, 100 + i),
            )
        })
        .collect();
    st.commit(&gen2, Durability::Sync).unwrap();
    // Key 1 is retired but still referenced: nothing prunable.
    assert!(st.prune_data_keys().unwrap().is_empty());
    let refs = st.key_references().unwrap();
    assert_eq!(refs.len(), 2);
    assert_eq!(refs[0].key_id, 1);
    assert_eq!(refs[0].segment_files, 1, "refs: {refs:?}");
    // Reclaim unlinks the fully dead generation-1 file; then key 1
    // is provably unreferenced and prunes away.
    for _ in 0..10 {
        if segment_count(dir) <= 2 {
            break;
        }
        st.reclaim().unwrap();
    }
    assert_eq!(segment_count(dir), 2, "want 2 (live + active)");
    assert_eq!(st.prune_data_keys().unwrap(), vec![1]);
    let inv = st.key_inventory();
    assert_eq!(inv.data_keys.len(), 1);
    assert_eq!(inv.active_data_key_id, 2);
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 20);
    for i in 0..20u64 {
        let k = format!("k{i:02}");
        assert_eq!(
            got.get(k.as_bytes()),
            Some(&incompressible(1000, 100 + i)),
            "{k} mismatch"
        );
    }
}

#[test]
fn context_round_trip_and_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let ctx = b"0123456789abcdef".to_vec();
    let mut o = test_options(dir);
    o.context_id = ctx.clone();
    let st = open(&o).unwrap();
    st.put(b"k", b"v").unwrap();
    st.close().unwrap();
    let hint = read_key_hint(dir).unwrap();
    assert_eq!(hint.context_id, *b"0123456789abcdef");
    // Matching context opens.
    let mut o2 = test_options(dir);
    o2.context_id = ctx;
    let st = open(&o2).unwrap();
    st.close().unwrap();
    // Wrong context fails before any writer initializes.
    let mut o3 = test_options(dir);
    o3.context_id = b"fedcba9876543210".to_vec();
    let err = open(&o3).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ContextMismatch, "got {err}");
    let lo = LoadOptions {
        path: dir.to_str().unwrap().to_string(),
        master_key: master_key(),
        context_id: b"fedcba9876543210".to_vec(),
        ..Default::default()
    };
    let err = load_with_options(lo, &mut |_| Ok(())).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ContextMismatch, "got {err}");
    // Bad lengths are rejected by validation.
    let mut o4 = test_options(dir);
    o4.context_id = b"short".to_vec();
    assert!(open(&o4).is_err(), "short context_id accepted");
}

#[test]
fn key_hint_plain_store() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.encryption = Encryption::None;
    o.master_key = Vec::new();
    let st = open(&o).unwrap();
    st.close().unwrap();
    let hint = read_key_hint(dir).unwrap();
    assert!(!hint.encrypted);
    assert_eq!(
        hint.context_id, hint.store_id,
        "plain default context != store id"
    );
    assert_eq!(hint.wrapping_key_id, "");
}
