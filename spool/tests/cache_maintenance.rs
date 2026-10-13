#[path = "common/mod.rs"]
mod common;
use spool::{Durability, Mutation};
use std::sync::{Arc, Barrier};

#[test]
fn cached_and_uncached_reads_survive_concurrent_key_rewrite_and_compaction() {
    for capacity in [0, 65536] {
        let tmp = tempfile::tempdir().unwrap();
        let mut opts = common::test_options(tmp.path());
        opts.read_cache_bytes = capacity;
        opts.max_segment_size = 64 << 10;
        opts.target_block_bytes = 8192;
        opts.max_block_bytes = 16384;
        opts.max_value_size = 4096;
        opts.max_key_size = 64;
        opts.compaction_threshold = 0.8;
        let store = Arc::new(spool::open(&opts).unwrap());
        let expected: Vec<_> = (0..24)
            .map(|i| {
                (
                    format!("key-{i:02}").into_bytes(),
                    common::incompressible(3000, i),
                )
            })
            .collect();
        for generation in 0..3 {
            let changes: Vec<_> = expected
                .iter()
                .enumerate()
                .filter(|(i, _)| generation == 0 || i % 3 != 0)
                .map(|(_, (k, v))| Mutation::put(k.clone(), v.clone()))
                .collect();
            store.commit(&changes, Durability::Sync).unwrap();
        }
        for (key, value) in &expected {
            assert_eq!(store.get(key).unwrap(), Some(value.clone()));
            assert_eq!(store.get(key).unwrap(), Some(value.clone()));
        }
        store.rotate_key().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        std::thread::scope(|s| {
            let reader = store.clone();
            let gate = barrier.clone();
            let values = &expected;
            s.spawn(move || {
                gate.wait();
                for _ in 0..40 {
                    for (key, value) in values {
                        assert_eq!(reader.get(key).unwrap(), Some(value.clone()));
                    }
                }
            });
            barrier.wait();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                match store.rewrite_data_keys() {
                    Ok(()) => break,
                    Err(e) if e.kind() == spool::ErrorKind::Maintenance => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "maintenance never acquired its gate"
                        );
                        std::thread::yield_now();
                    }
                    Err(e) => panic!("rewrite failed: {e}"),
                }
            }
            store.reclaim().unwrap();
            assert!(
                store.stats().compactions > 0,
                "mixed live/dead segment was not compacted"
            );
        });
        store.prune_data_keys().unwrap();
        for (key, value) in &expected {
            assert_eq!(store.get(key).unwrap(), Some(value.clone()));
        }
        store.close().unwrap();
        let reopened = spool::open(&opts).unwrap();
        for (key, value) in &expected {
            assert_eq!(reopened.get(key).unwrap(), Some(value.clone()));
        }
        reopened.close().unwrap();
    }
}
