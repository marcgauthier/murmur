//! Shared helpers for the spool port tests (mirrors the Go
//! `testOptions` / `loadAll` / `incompressible` helpers).

// Each test target compiles this module separately and uses a
// different subset of helpers.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use spool::{default_options, load, Options};

/// Fixed 32-byte key for deterministic tests (salts and nonces stay
/// random per write).
pub fn master_key() -> Vec<u8> {
    b"0123456789abcdef0123456789abcdef".to_vec()
}

/// Small, fast, deterministic options using the default cipher:
/// flush and reclaim only on demand.
pub fn test_options(dir: &Path) -> Options {
    let mut o = default_options(dir.to_str().unwrap().to_string());
    o.master_key = master_key();
    // No Duration equivalent of Go's -1; a huge delay approximates
    // a disabled background timer.
    o.flush.max_delay = Duration::from_secs(86400);
    o.reclaim_interval = None;
    o
}

/// Collect a full load into a last-write-wins map of live keys.
pub fn load_all(dir: &Path) -> HashMap<Vec<u8>, Vec<u8>> {
    let key = master_key();
    let mut got: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut seqs: HashMap<Vec<u8>, u64> = HashMap::new();
    load(dir.to_str().unwrap(), &key, &mut |recs| {
        for r in recs {
            if r.sequence < seqs.get(&r.key).copied().unwrap_or(0) {
                continue;
            }
            seqs.insert(r.key.clone(), r.sequence);
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

/// Collect winners with their logical sequences.
pub fn load_all_seq(dir: &Path) -> HashMap<Vec<u8>, u64> {
    let key = master_key();
    let mut got: HashMap<Vec<u8>, u64> = HashMap::new();
    load(dir.to_str().unwrap(), &key, &mut |recs| {
        for r in recs {
            if r.deleted {
                continue;
            }
            got.insert(r.key.clone(), r.sequence);
        }
        Ok(())
    })
    .unwrap();
    got
}

/// Deterministic pseudorandom bytes that defeat compression. The
/// seed must differ per key: identical values still compress away
/// across records in a block.
pub fn incompressible(n: usize, seed: u64) -> Vec<u8> {
    let mut out = vec![0u8; n];
    let mut x = 0x9E3779B97F4A7C15u64 ^ seed.wrapping_mul(0xBF58476D1CE4E5B9).wrapping_add(1);
    for b in out.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = (x >> 56) as u8;
    }
    out
}

/// Tiny segments so each flush rotates, making file-level behavior
/// deterministic.
pub fn compact_options(dir: &Path) -> Options {
    let mut o = test_options(dir);
    o.max_segment_size = 8192;
    o.target_block_bytes = 2048;
    o.max_block_bytes = 4096;
    o.max_records_per_block = 1000;
    o.max_key_size = 64;
    o.max_value_size = 2048;
    o
}

pub fn segment_count(dir: &Path) -> usize {
    std::fs::read_dir(dir.join("segments")).unwrap().count()
}
