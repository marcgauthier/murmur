//! Temporary read benchmark (delete after use).
use spool::{Durability, Mutation};
use std::time::Instant;

fn main() {
    let dir = std::env::temp_dir().join(format!("getbench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut opt = spool::default_options(dir.to_string_lossy());
    opt.master_key = vec![7; 32];
    opt.reclaim_interval = None;
    let store = spool::open(&opt).unwrap();
    const N: usize = 20_000;
    const V: usize = 2048;
    let mut batch = Vec::new();
    for i in 0..N {
        batch.push(Mutation::put(
            format!("07/replication/log/aaaaaaaaaaaaaaaaaaaa/{i:020}").into_bytes(),
            vec![(i % 251) as u8; V],
        ));
        if batch.len() == 1000 {
            store.commit(&batch, Durability::Sync).unwrap();
            batch.clear();
        }
    }
    store.commit(&batch, Durability::Sync).unwrap();
    let keys: Vec<Vec<u8>> = (0..N)
        .map(|i| format!("07/replication/log/aaaaaaaaaaaaaaaaaaaa/{i:020}").into_bytes())
        .collect();
    // Hot: same key.
    let t = Instant::now();
    for _ in 0..2000 {
        assert!(store.get(&keys[0]).unwrap().is_some());
    }
    println!("hot get: {:.3}ms/op", t.elapsed().as_secs_f64() * 1000.0 / 2000.0);
    // Sequential sweep (cold-ish, 40MB > 32MB cache).
    let t = Instant::now();
    for k in &keys {
        assert!(store.get(k).unwrap().is_some());
    }
    println!(
        "sweep get ({} keys): {:.3}ms/op",
        N,
        t.elapsed().as_secs_f64() * 1000.0 / N as f64
    );
    // Random.
    let mut seed = 0x12345678u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (seed >> 33) as usize % N
    };
    let t = Instant::now();
    for _ in 0..2000 {
        assert!(store.get(&keys[next()]).unwrap().is_some());
    }
    println!("rand get: {:.3}ms/op", t.elapsed().as_secs_f64() * 1000.0 / 2000.0);
    // Concurrent (8 threads x 500).
    let store = std::sync::Arc::new(store);
    let t = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..8 {
            let store = store.clone();
            let keys = &keys;
            s.spawn(move || {
                let mut seed = 0xabcdefu64;
                for _ in 0..500 {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    assert!(store.get(&keys[(seed >> 33) as usize % N]).unwrap().is_some());
                }
            });
        }
    });
    println!(
        "concurrent 8x500 get: {:.3}ms/op",
        t.elapsed().as_secs_f64() * 1000.0 / 4000.0
    );
}
