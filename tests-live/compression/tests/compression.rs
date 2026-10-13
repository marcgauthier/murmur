//! Rust port of the Go `compression` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/compression/`
//! (`compression_test.go`, `TestSpoolCompressionSizes`). Each (compression
//! mode, traffic profile) cell opens a fresh encrypted store, writes a
//! seeded deterministic dataset in batches of 500, applies an update pass
//! over every 10th row where applicable, closes cleanly, and measures the
//! on-disk store directory.
//!
//! Deviation: the Go cells write through managed RIME records on a full
//! database because the Go engine exposes a per-database `Spool.Compression`
//! knob (`none`/`deflate`). The Rust `Database` hardcodes its Spool block
//! codec to a custom zstd codec (`storage.rs::spool_options`, codec id 128)
//! with only a level knob, so a `none`-vs-`deflate` comparison is impossible
//! through `Database::open`. The cells therefore write equivalent seeded KV
//! traffic directly to `spool::Store` with `Compression::NONE` vs
//! `Compression::DEFLATE` (same 64 KiB target blocks, same AES-256-GCM
//! encryption). Row counts, ID checksums, and the size gates mirror Go.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use murmur_live_harness::artifacts::{finish, prepare_runtime_dir};
use murmur_live_harness::spool::{
    self, Compression, Durability, Mutation,
};
use rand::{Rng, RngCore, SeedableRng};

/// Seeded RNG stream (Go: `rand.New(rand.NewSource(42))`). The same seed is
/// reused for every mode so all modes get byte-identical keys and datasets.
const SEED: u64 = 42;
/// Insert batch size (Go: `const batch = 500`).
const BATCH: usize = 500;
/// Fresh-application key per store (Go: `bytes.Repeat([]byte{0x3a}, 32)`).
const KEY: [u8; 32] = [0x3a; 32];
/// Key-envelope id (Go: `KeyID: "compression-bench"`).
const KEY_ID: &str = "compression-bench";
/// Target plaintext bytes per Spool block (Go: `TargetBlockBytes = 64 << 10`).
const TARGET_BLOCK_BYTES: usize = 64 << 10;

/// One traffic record. All profiles share this single stable shape (Go:
/// `compressionRecord`); unused fields stay empty.
#[derive(Clone)]
struct Record {
    id: [u8; 16],
    code: String,
    qty: i64,
    price: f64,
    title: String,
    body: String,
    tags: String,
    kind: String,
    payload: String,
    data: Vec<u8>,
}

impl Record {
    fn blank(id: [u8; 16]) -> Self {
        Self {
            id,
            code: String::new(),
            qty: 0,
            price: 0.0,
            title: String::new(),
            body: String::new(),
            tags: String::new(),
            kind: String::new(),
            payload: String::new(),
            data: Vec::new(),
        }
    }

    /// Deterministic field encoding: fixed field order, each length-prefixed.
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        push_str(&mut out, &self.code);
        out.extend_from_slice(&self.qty.to_le_bytes());
        out.extend_from_slice(&self.price.to_le_bytes());
        push_str(&mut out, &self.title);
        push_str(&mut out, &self.body);
        push_str(&mut out, &self.tags);
        push_str(&mut out, &self.kind);
        push_str(&mut out, &self.payload);
        push_bytes(&mut out, &self.data);
        out
    }

    fn key(&self) -> Vec<u8> {
        let mut key = b"rec/".to_vec();
        key.extend_from_slice(&hex_id(&self.id));
        key
    }
}

fn push_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn push_str(out: &mut Vec<u8>, s: &str) {
    push_bytes(out, s.as_bytes());
}

fn hex_id(id: &[u8; 16]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(32);
    for b in id {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0xf) as usize]);
    }
    out
}

struct Mode {
    name: &'static str,
    algo: Compression,
}

const MODES: [Mode; 2] = [
    Mode {
        name: "none",
        algo: Compression::NONE,
    },
    Mode {
        name: "deflate",
        algo: Compression::DEFLATE,
    },
];

/// Which fields a profile fills, and its update pass.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProfileKind {
    OltpSmall,
    TextHeavy,
    JsonDocs,
    BlobsRandom,
}

struct Profile {
    name: &'static str,
    rows: usize,
    kind: ProfileKind,
}

const PROFILES: [Profile; 4] = [
    Profile {
        name: "oltp-small",
        rows: 20_000,
        kind: ProfileKind::OltpSmall,
    },
    Profile {
        name: "text-heavy",
        rows: 8_000,
        kind: ProfileKind::TextHeavy,
    },
    Profile {
        name: "json-docs",
        rows: 8_000,
        kind: ProfileKind::JsonDocs,
    },
    Profile {
        name: "blobs-random",
        rows: 2_000,
        kind: ProfileKind::BlobsRandom,
    },
];

const LOREM_TEXT: &str = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod\ntempor incididunt ut labore et dolore magna aliqua enim ad minim veniam quis nostrud exercitation\nullamco laboris nisi aliquip ex ea commodo consequat duis aute irure in reprehenderit voluptate\nvelit esse cillum fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt culpa\nqui officia deserunt mollit anim id est laborum perspiciatis unde omnis natus error voluptatem";

fn lorem(rng: &mut impl Rng, words: &[&str], n: usize) -> String {
    let mut out = String::new();
    for i in 0..n {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(words[rng.gen_range(0..words.len())]);
    }
    out
}

fn create_row(rng: &mut impl Rng, words: &[&str], kind: ProfileKind, i: usize, id: [u8; 16]) -> Record {
    let mut row = Record::blank(id);
    match kind {
        ProfileKind::OltpSmall => {
            row.code = format!("SKU-{:05}", rng.gen_range(0..2000));
            row.qty = rng.gen_range(0..100);
            row.price = rng.gen::<f64>() * 999.99;
        }
        ProfileKind::TextHeavy => {
            let title_words = 4 + rng.gen_range(0..6);
            row.title = lorem(rng, words, title_words);
            let body_words = 100 + rng.gen_range(0..200);
            row.body = lorem(rng, words, body_words);
            row.tags = "news,tech,local".to_string();
        }
        ProfileKind::JsonDocs => {
            row.kind = "pageview".to_string();
            row.payload = format!(
                "{{\"event_id\":{i},\"user\":{{\"id\":{},\"name\":\"user-{}\",\"roles\":[\"reader\",\"editor\"]}},\
                 \"context\":{{\"app\":\"storefront\",\"region\":\"eu-west\",\"build\":{}}},\
                 \"metrics\":{{\"latency_ms\":{:.3},\"retries\":{},\"flags\":[true,false,true]}},\
                 \"trace\":[\"recv\",\"auth\",\"load\",\"render\",\"send\"]}}",
                rng.gen_range(0..50_000),
                rng.gen_range(0..50_000),
                1000 + rng.gen_range(0..50),
                rng.gen::<f64>() * 250.0,
                rng.gen_range(0..4),
            );
        }
        ProfileKind::BlobsRandom => {
            row.data = vec![0u8; 4096];
            rng.fill_bytes(&mut row.data);
        }
    }
    row
}

/// Update pass over one row. Returns false for profiles without updates
/// (Go: `update: nil` for immutable blobs).
fn update_row(rng: &mut impl Rng, kind: ProfileKind, row: &mut Record) -> bool {
    match kind {
        ProfileKind::OltpSmall => {
            row.qty = rng.gen_range(0..100);
            true
        }
        ProfileKind::TextHeavy => {
            row.tags = "news,tech,local,updated".to_string();
            true
        }
        ProfileKind::JsonDocs => {
            row.kind = "pageview-v2".to_string();
            true
        }
        ProfileKind::BlobsRandom => false,
    }
}

struct CellResult {
    rows: usize,
    total: u64,
    files: usize,
    write_dur: Duration,
    checksum: String,
}

/// `MURMUR_LIVE_COMPRESSION_SCALE` multiplies every profile's row count.
fn env_scale() -> usize {
    match std::env::var("MURMUR_LIVE_COMPRESSION_SCALE") {
        Ok(raw) if !raw.is_empty() => raw
            .parse::<usize>()
            .ok()
            .filter(|n| *n >= 1)
            .unwrap_or_else(|| {
                panic!("MURMUR_LIVE_COMPRESSION_SCALE must be a positive integer")
            }),
        _ => 1,
    }
}

/// FNV-1a/64 over row IDs in write order (Go hashes hex IDs with SHA-256;
/// only cross-mode equality within this process is asserted).
fn checksum_ids(rows: &[Record]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for row in rows {
        for b in row.id {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    format!("{h:016x}")
}

fn dir_usage(path: &Path) -> (u64, usize) {
    let mut total = 0u64;
    let mut files = 0usize;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries =
            std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
        for entry in entries {
            let entry = entry.expect("dir entry");
            let ty = entry.file_type().expect("file type");
            if ty.is_dir() {
                stack.push(entry.path());
            } else {
                files += 1;
                total += entry.metadata().expect("file metadata").len();
            }
        }
    }
    (total, files)
}

fn run_cell(runtime: &Path, mode: &Mode, prof: &Profile, scale: usize) -> CellResult {
    let dir: PathBuf = runtime.join(format!("{}-{}", mode.name, prof.name));
    let mut opts = spool::default_options(dir.to_string_lossy().to_string());
    opts.master_key = KEY.to_vec();
    opts.wrapping_key_id = KEY_ID.to_string();
    opts.target_block_bytes = TARGET_BLOCK_BYTES;
    opts.compression = Some(mode.algo);
    // No codec override: the store's built-in none/deflate codecs apply.
    opts.reclaim_interval = None; // deterministic sizes

    let store = spool::open(&opts).expect("spool open");
    let rows_total = prof.rows * scale;
    let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
    let words: Vec<&str> = LOREM_TEXT.split_whitespace().collect();
    let mut rows: Vec<Record> = Vec::with_capacity(rows_total);

    let start = Instant::now();
    for base in (0..rows_total).step_by(BATCH) {
        let end = (base + BATCH).min(rows_total);
        let mut mutations = Vec::with_capacity(end - base);
        for i in base..end {
            let mut id = [0u8; 16];
            rng.fill_bytes(&mut id);
            let row = create_row(&mut rng, &words, prof.kind, i, id);
            mutations.push(Mutation::put(row.key(), row.encode()));
            rows.push(row);
        }
        store
            .commit(&mutations, Durability::Async)
            .expect("insert batch");
    }
    // Churn pass: update every 10th row like live traffic would.
    if prof.kind != ProfileKind::BlobsRandom {
        for base in (0..rows_total).step_by(BATCH * 10) {
            let end = (base + BATCH * 10).min(rows_total);
            let mut mutations = Vec::new();
            for i in (base..end).step_by(10) {
                let row = &mut rows[i];
                assert!(update_row(&mut rng, prof.kind, row));
                mutations.push(Mutation::put(row.key(), row.encode()));
            }
            store
                .commit(&mutations, Durability::Async)
                .expect("update batch");
        }
    }
    let write_dur = start.elapsed();

    let count = store.keys_with_prefix(b"rec/").len();
    assert_eq!(
        count, rows_total,
        "mode {} profile {}: count = {count}, want {rows_total}",
        mode.name, prof.name,
    );
    let checksum = checksum_ids(&rows);

    store.flush().expect("flush before close");
    store.close().expect("close store");

    let (total, files) = dir_usage(&dir);
    CellResult {
        rows: count,
        total,
        files,
        write_dur: write_dur,
        checksum,
    }
}

fn format_bytes(n: u64) -> String {
    const UNIT: u64 = 1024;
    if n < UNIT {
        return format!("{n}B");
    }
    let mut div = UNIT;
    let mut exp = 0;
    while n / div >= UNIT && exp < 3 {
        div *= UNIT;
        exp += 1;
    }
    format!("{:.1}{}B", n as f64 / div as f64, b"KMGT"[exp] as char)
}

fn body(runtime: &Path) {
    let scale = env_scale();

    // profile -> mode -> result
    let mut results: HashMap<&str, HashMap<&str, CellResult>> = HashMap::new();
    for prof in &PROFILES {
        let mut per_mode = HashMap::new();
        for mode in &MODES {
            let res = run_cell(runtime, mode, prof, scale);
            eprintln!(
                "{:<12} {:<8} rows={} total={} files={} write={:?}",
                prof.name,
                mode.name,
                res.rows,
                format_bytes(res.total),
                res.files,
                res.write_dur,
            );
            per_mode.insert(mode.name, res);
        }
        results.insert(prof.name, per_mode);
    }

    eprintln!("mode \\ profile | oltp-small | text-heavy | json-docs | blobs-random");
    for mode in &MODES {
        let mut cells = Vec::new();
        for prof in &PROFILES {
            let res = &results[prof.name][mode.name];
            let base = results[prof.name]["none"].total;
            let ratio = if base > 0 {
                res.total as f64 / base as f64
            } else {
                0.0
            };
            cells.push(format!("{} ({:.0}%)", format_bytes(res.total), ratio * 100.0));
        }
        eprintln!("{:<8} | {}", mode.name, cells.join(" | "));
    }

    // Sanity gates: identical datasets must hold identical rows with
    // identical checksums; compressible traffic must shrink under deflate;
    // incompressible traffic must not explode; and segment bytes must exist.
    for prof in &PROFILES {
        let mut want_sum: Option<&str> = None;
        for mode in &MODES {
            let res = &results[prof.name][mode.name];
            match want_sum {
                None => want_sum = Some(&res.checksum),
                Some(want) => assert_eq!(
                    res.checksum, want,
                    "profile {}: mode {} dataset diverged",
                    prof.name, mode.name,
                ),
            }
            assert_eq!(
                res.rows,
                prof.rows * scale,
                "profile {} mode {}: rows = {}",
                prof.name,
                mode.name,
                res.rows,
            );
            assert!(
                res.files > 0 && res.total > 0,
                "profile {} mode {}: no segment bytes measured",
                prof.name,
                mode.name,
            );
        }
    }
    let text_none = results["text-heavy"]["none"].total;
    let text_deflate = results["text-heavy"]["deflate"].total;
    assert!(
        text_deflate < text_none,
        "text-heavy deflate ({text_deflate}) not smaller than none ({text_none}): compression disengaged?",
    );
    let blob_none = results["blobs-random"]["none"].total;
    let blob_deflate = results["blobs-random"]["deflate"].total;
    assert!(
        blob_deflate <= blob_none * 11 / 10,
        "blobs-random deflate grew >10% over none ({blob_deflate} vs {blob_none})",
    );
}

#[test]
fn spool_compression_sizes() {
    let runtime =
        prepare_runtime_dir("compression").expect("prepare runtime dir");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        body(&runtime);
    }))
    .is_err();
    // No cluster/forensics handle in this in-process single-store scenario;
    // on failure the runtime dir (all cell stores) is retained as-is.
    finish("compression", &runtime, failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
