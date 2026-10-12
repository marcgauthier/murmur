//! Atomic groups and batch APIs (port of `spool_commit_test.go`
//! and the batch half of `spool_batch_test.go`).

#[path = "common/mod.rs"]
mod common;

use common::{incompressible, load_all, test_options};
use spool::{open, Durability, ErrorKind, Mutation, KV};

#[test]
fn commit_atomic_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    let muts = [
        Mutation::put(b"a".to_vec(), b"1".to_vec()),
        Mutation::put(b"b".to_vec(), b"1".to_vec()),
        Mutation::put(b"a".to_vec(), b"2".to_vec()),
        Mutation::put(b"c".to_vec(), b"1".to_vec()),
        Mutation::delete(b"c".to_vec()),
        Mutation::put(b"b".to_vec(), b"3".to_vec()),
        Mutation::put(b"a".to_vec(), b"final".to_vec()),
    ];
    st.commit(&muts, Durability::Sync).unwrap();
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 2);
    assert_eq!(
        got.get(&b"a"[..]).map(|b| b.as_slice()),
        Some(&b"final"[..])
    );
    assert_eq!(got.get(&b"b"[..]).map(|b| b.as_slice()), Some(&b"3"[..]));
}

#[test]
fn commit_multi_block_group() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.target_block_bytes = 1024;
    o.max_block_bytes = 2048;
    o.max_records_per_block = 100;
    o.max_key_size = 64;
    o.max_value_size = 1024;
    let st = open(&o).unwrap();
    let muts: Vec<Mutation> = (0..50)
        .map(|i| {
            Mutation::put(
                format!("k{i:02}").into_bytes(),
                incompressible(300, i as u64),
            )
        })
        .collect();
    st.commit(&muts, Durability::Sync).unwrap();
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 50);
    for i in 0..50 {
        let k = format!("k{i:02}");
        assert_eq!(
            got.get(k.as_bytes()).map(|v| v.len()),
            Some(300),
            "{k} damaged"
        );
    }
}

#[test]
fn commit_validation() {
    let tmp = tempfile::tempdir().unwrap();
    let mut o = test_options(tmp.path());
    o.max_key_size = 64;
    o.max_value_size = 1024;
    o.target_block_bytes = 1024;
    o.max_block_bytes = 2048;
    o.max_atomic_batch_bytes = 4096;
    let st = open(&o).unwrap();
    let before = st.stats();
    let big_key = vec![0u8; o.max_key_size + 1];
    let big_val = vec![0u8; o.max_value_size + 1];
    let cases: Vec<(&str, Vec<Mutation>, ErrorKind)> = vec![
        (
            "valued tomb",
            vec![Mutation {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                deleted: true,
            }],
            ErrorKind::BadMutation,
        ),
        (
            "oversize key",
            vec![Mutation::put(big_key, vec![])],
            ErrorKind::KeyTooLarge,
        ),
        (
            "oversize value",
            vec![Mutation::put(b"k".to_vec(), big_val)],
            ErrorKind::ValueTooLarge,
        ),
        (
            "oversize group",
            (0..5)
                .map(|i| Mutation::put(vec![b'a' + i], vec![0u8; 1024]))
                .collect(),
            ErrorKind::GroupTooLarge,
        ),
    ];
    for (name, muts, want) in &cases {
        let err = st.commit(muts, Durability::Sync).unwrap_err();
        assert_eq!(err.kind(), *want, "case {name}: got {err}");
    }
    let after = st.stats();
    assert_eq!(after.groups_accepted, before.groups_accepted);
    assert_eq!(after.pending_records, 0);
    // Empty groups are no-ops without barrier effects.
    st.commit(&[], Durability::Sync).unwrap();
    assert_eq!(st.stats().groups_accepted, before.groups_accepted);
    st.close().unwrap();
    // Note: "unknown durability" has no Rust spelling; the enum
    // makes invalid values unrepresentable.
}

#[test]
fn commit_async_acceptance() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.durability = Durability::Async;
    let st = open(&o).unwrap();
    for i in 0..10 {
        st.commit(
            &[Mutation::put(format!("k{i}").into_bytes(), b"v".to_vec())],
            Durability::Async,
        )
        .unwrap();
    }
    // Same-key async groups preserve admission order.
    st.commit(
        &[Mutation::put(b"ord".to_vec(), b"1".to_vec())],
        Durability::Async,
    )
    .unwrap();
    st.commit(
        &[Mutation::put(b"ord".to_vec(), b"2".to_vec())],
        Durability::Async,
    )
    .unwrap();
    st.sync().unwrap();
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 11);
    assert_eq!(got.get(&b"ord"[..]).map(|b| b.as_slice()), Some(&b"2"[..]));
}

#[test]
fn commit_sync_ordering() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"put1").unwrap();
    st.commit(
        &[Mutation::put(b"k".to_vec(), b"commit2".to_vec())],
        Durability::Sync,
    )
    .unwrap();
    st.put(b"k", b"put3").unwrap();
    st.sync().unwrap();
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.get(&b"k"[..]).map(|b| b.as_slice()), Some(&b"put3"[..]));
}

#[test]
fn commit_group_stats() {
    let tmp = tempfile::tempdir().unwrap();
    let st = open(&test_options(tmp.path())).unwrap();
    st.commit(
        &[Mutation::put(b"a".to_vec(), b"v".to_vec())],
        Durability::Sync,
    )
    .unwrap();
    st.commit(
        &[Mutation::put(b"b".to_vec(), b"v".to_vec())],
        Durability::Async,
    )
    .unwrap();
    st.put(b"c", b"v").unwrap();
    st.sync().unwrap();
    let s = st.stats();
    st.close().unwrap();
    assert_eq!(s.commits, 2, "stats: {s:?}");
    assert_eq!(
        (s.groups_accepted, s.groups_written),
        (3, 3),
        "stats: {s:?}"
    );
    assert_eq!(s.groups_synced, 2, "stats: {s:?}"); // sync commit + syncing fence group
    assert_eq!((s.syncs, s.pending_groups), (1, 0), "stats: {s:?}");
}

#[test]
fn commit_orders_after_buffered_puts() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    st.put(b"k", b"v1").unwrap();
    st.commit(
        &[Mutation::put(b"k".to_vec(), b"v2".to_vec())],
        Durability::Sync,
    )
    .unwrap();
    // And the reverse: a commit followed by an unflushed put orders
    // the put last through the sync fence seal.
    st.commit(
        &[Mutation::put(b"j".to_vec(), b"w1".to_vec())],
        Durability::Async,
    )
    .unwrap();
    st.put(b"j", b"w2").unwrap();
    st.sync().unwrap();
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.get(&b"k"[..]).map(|b| b.as_slice()), Some(&b"v2"[..]));
    assert_eq!(got.get(&b"j"[..]).map(|b| b.as_slice()), Some(&b"w2"[..]));
}

#[test]
fn oversized_group_dedicated_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut o = test_options(dir);
    o.max_segment_size = 8192;
    o.target_block_bytes = 2048;
    o.max_block_bytes = 4096;
    o.max_records_per_block = 1000;
    o.max_key_size = 64;
    o.max_value_size = 2048;
    let st = open(&o).unwrap();
    let muts: Vec<Mutation> = (0..20)
        .map(|i| {
            Mutation::put(
                format!("big{i:02}").into_bytes(),
                incompressible(1500, i as u64),
            )
        })
        .collect();
    st.commit(&muts, Durability::Sync).unwrap();
    st.commit(
        &[Mutation::put(b"after".to_vec(), b"v".to_vec())],
        Durability::Sync,
    )
    .unwrap();
    st.close().unwrap();
    let mut oversized = 0;
    let mut total = 0;
    for ent in std::fs::read_dir(dir.join("segments")).unwrap() {
        total += 1;
        if ent.unwrap().metadata().unwrap().len() as i64 > o.max_segment_size {
            oversized += 1;
        }
    }
    assert_eq!(oversized, 1, "of {total} segments");
    let got = load_all(dir);
    assert_eq!(got.len(), 21);
    assert_eq!(
        got.get(&b"after"[..]).map(|b| b.as_slice()),
        Some(&b"v"[..])
    );
}

#[test]
fn put_batch_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let st = open(&test_options(dir)).unwrap();
    let pairs: Vec<KV> = (0..200)
        .map(|i| {
            KV::new(
                format!("k{i:03}").into_bytes(),
                format!("v{i}").into_bytes(),
            )
        })
        .collect();
    st.put_batch(&pairs).unwrap();
    // Overwrite half, delete a quarter.
    let over: Vec<KV> = (0..100)
        .map(|i| KV::new(format!("k{i:03}").into_bytes(), b"new".to_vec()))
        .collect();
    st.put_batch(&over).unwrap();
    let del: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("k{:03}", 150 + i).into_bytes())
        .collect();
    st.delete_batch(&del).unwrap();
    st.close().unwrap();
    let got = load_all(dir);
    assert_eq!(got.len(), 150);
    for i in 0..100 {
        let k = format!("k{i:03}");
        assert_eq!(
            got.get(k.as_bytes()).map(|b| b.as_slice()),
            Some(&b"new"[..]),
            "{k}"
        );
    }
    for i in 100..150 {
        let k = format!("k{i:03}");
        assert_eq!(
            got.get(k.as_bytes()),
            Some(&format!("v{i}").into_bytes()),
            "{k}"
        );
    }
}

#[test]
fn put_batch_admission() {
    let tmp = tempfile::tempdir().unwrap();
    let mut o = test_options(tmp.path());
    o.max_value_size = 64;
    let st = open(&o).unwrap();
    let bad = [
        KV::new(b"good".to_vec(), b"x".to_vec()),
        KV::new(b"bad".to_vec(), vec![0u8; 65]),
    ];
    let err = st.put_batch(&bad).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ValueTooLarge, "got {err}");
    assert_eq!(st.stats().pending_records, 0);
    let err = st
        .delete_batch(&[vec![0u8; o.max_key_size + 1]])
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::KeyTooLarge, "got {err}");
    st.put_batch(&[]).unwrap();
    st.delete_batch(&[]).unwrap();
    st.close().unwrap();
    let err = st.put_batch(&bad[..1]).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Closed, "got {err}");
}
