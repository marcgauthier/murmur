//! Write a spool store with the Rust implementation (compat key 32 x 0x42).
//!
//! Usage: writestore <dir>
use spool::{default_options, open, Durability, Mutation, KV};

fn main() {
    let dir = std::env::args().nth(1).expect("usage: writestore <dir>");
    let mut o = default_options(dir);
    o.master_key = vec![0x42u8; 32];
    let store = open(&o).expect("open");
    for i in 0..50 {
        store
            .put(
                format!("key-{i:03}").as_bytes(),
                format!("value-{i}").as_bytes(),
            )
            .expect("put");
    }
    store
        .put_batch(&[
            KV::new(b"batch-a".to_vec(), b"1".to_vec()),
            KV::new(b"batch-b".to_vec(), b"22".to_vec()),
        ])
        .expect("put_batch");
    store
        .commit(
            &[
                Mutation::put(b"commit-x".to_vec(), b"xxx".to_vec()),
                Mutation::delete(b"key-000".to_vec()),
            ],
            Durability::Sync,
        )
        .expect("commit");
    store.delete(b"key-001").expect("delete");
    store.put(b"key-010", b"v2").expect("put");
    store.put(b"key-010", b"v3-final").expect("put");
    store.put(b"empty-val", b"").expect("put");
    store.flush().expect("flush");
    let st = store.stats();
    println!(
        "RS-WRITE keys={} groups={} blocks={}",
        st.keys, st.groups_written, st.blocks_written
    );
    store.close().expect("close");
}
