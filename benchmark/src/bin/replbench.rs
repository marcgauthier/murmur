//! Insert/update/select workload on one node while QUIC replication to a
//! second node runs in the background.
//!
//! Phases: paced medium-speed mix, unpaced insert burst, then catch-up until
//! the replica converges. Ends with RAM (SQLite page image) vs on-disk
//! (Spool segments) sizes per node.
//!
//! Usage: replbench [medium_secs [burst_secs [medium_iters_per_sec [converge_secs]]]]
//! Defaults: 20s medium at 100 iters/s, 15s burst, 180s convergence wait.
//! Each medium iteration is one insert + one update + one point select.
//! If the replica does not converge in time, partial state and sizes are
//! still reported with a NOT CONVERGED banner (exit 0).

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType};
use sqlite_session::{
    Database, Migration, OpenMode, Options,
    replication::{self, Config, Credentials},
};
use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, Instant},
};

const BURST_BATCH: i64 = 20;
const VALUE_BYTES: usize = 200;

fn payload(id: i64) -> String {
    let head = format!("item-{id:08} ");
    let mut s = String::with_capacity(VALUE_BYTES);
    s.push_str(&head);
    while s.len() + 12 < VALUE_BYTES {
        s.push_str("lorem ipsum ");
    }
    while s.len() < VALUE_BYTES {
        s.push('.');
    }
    s.truncate(VALUE_BYTES);
    s
}

fn configs() -> Vec<Config> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&ca_key).unwrap();
    let ids = [[1; 16], [2; 16]];
    let seeds = [[11; 32], [22; 32]];
    let mut trust = BTreeMap::new();
    for (id, seed) in ids.iter().zip(seeds) {
        trust.insert(*id, replication::protocol::public_key(&seed));
    }
    ids.iter()
        .zip(seeds)
        .map(|(id, seed)| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.subject_alt_names = vec![SanType::URI(
                format!("replicateddb://node/{}", replication::hex(id))
                    .try_into()
                    .unwrap(),
            )];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
            let credentials = Credentials {
                chain: vec![cert.der().to_vec()],
                private_key: key.serialize_der().into(),
                roots: vec![ca.der().to_vec()],
            };
            let mut config = Config::new("127.0.0.1:0".parse().unwrap(), credentials, seed);
            config.trusted_origins = trust.clone();
            config
        })
        .collect()
}

fn open(root: &Path, config: Config) -> Result<Database, Box<dyn std::error::Error>> {
    Ok(Database::open(
        root,
        [9; 32],
        Options {
            mode: OpenMode::CreateNew,
            database_id: Some([7; 16]),
            replication: Some(config),
            // Burst writes ~55 MiB/s into the session; the 5s default
            // collection lets it outgrow the queue (writes then wedge on
            // Busy). Each checkpoint also becomes one replication
            // transaction, and multi-MB transactions stall replication
            // (chunked transfer never completes; see report). Collect
            // every 10ms so transactions stay small single frames.
            capture_interval: Duration::from_millis(10),
            queue_bytes: 128 << 20,
            ..Options::default()
        },
    )?)
}

/// Run a write transaction, retrying admission backpressure. A stall longer
/// than 30s means the queue is not draining (engine bug), so fail loudly
/// instead of spinning forever.
macro_rules! write {
    ($db:expr, $body:expr) => {{
        let stall = std::time::Instant::now();
        loop {
            match $db.transaction($body) {
                Err(sqlite_session::Error::Busy) => {
                    BUSY_RETRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if stall.elapsed() > std::time::Duration::from_secs(30) {
                        return Err("write stalled under backpressure for 30s".into());
                    }
                    std::thread::yield_now();
                }
                result => break result?,
            }
        }
    }};
}

static BUSY_RETRIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn busy() -> u64 {
    BUSY_RETRIES.load(std::sync::atomic::Ordering::Relaxed)
}

fn checksum(db: &Database) -> (i64, i64) {
    db.read(|s| {
        Ok(s.query_row("SELECT COUNT(*), COALESCE(SUM(id),0) FROM items", [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        })?)
    })
    .unwrap_or((-1, -1))
}

fn sqlite_ram(db: &Database) -> (i64, u64) {
    db.sqlite_metrics()
        .map(|m| (m.database_size_bytes, m.total_rows))
        .unwrap_or((0, 0))
}

fn dir_bytes(root: &Path) -> u64 {
    fn walk(dir: &Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, total);
            } else {
                *total += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1 << 20) as f64
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let get = |i: usize, dflt: u64| {
        args.get(i)
            .map(|s| s.parse::<u64>())
            .transpose()
            .map(|v| v.unwrap_or(dflt))
    };
    let medium_secs = get(0, 20)?;
    let burst_secs = get(1, 15)?;
    let rate = get(2, 100)?;
    let converge_secs = get(3, 180)?;
    if medium_secs == 0 || burst_secs == 0 || rate == 0 || converge_secs == 0 {
        return Err("durations and rate must be positive".into());
    }

    let temp = tempfile::tempdir()?;
    let path_a = temp.path().join("a");
    let path_b = temp.path().join("b");
    let mut cfg = configs();
    let a = open(&path_a, cfg.remove(0))?;
    let b = open(&path_b, cfg.remove(0))?;
    for db in [&a, &b] {
        db.migrate(Migration::new(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL, updated INTEGER NOT NULL)",
        ))?;
        db.flush()?;
    }
    let addr_b = b
        .replication()
        .unwrap()
        .status()
        .address
        .ok_or("replica has no address")?;
    a.replication().unwrap().add_peer(addr_b)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    while a.replication().unwrap().status().peers == 0 {
        if Instant::now() > deadline {
            return Err("replication peer never connected".into());
        }
        std::thread::sleep(Duration::from_millis(30));
    }
    eprintln!("[replbench] replication connected: writer -> replica");
    println!("replication connected: writer -> replica");

    // Phase 1: paced medium-speed mix.
    eprintln!("[replbench] medium phase: {medium_secs}s at {rate}/s");
    let budget = Duration::from_secs(1).checked_div(rate as u32).unwrap();
    let start = Instant::now();
    let medium = Duration::from_secs(medium_secs);
    let (mut ins, mut upd, mut sel) = (0u64, 0u64, 0u64);
    let (mut ins_us, mut upd_us, mut sel_us) = (0u128, 0u128, 0u128);
    let mut next_id: i64 = 1;
    let mut it = 0u64;
    while start.elapsed() < medium {
        let tick = Instant::now();
        let t = Instant::now();
        let id = next_id;
        next_id += 1;
        write!(&a, move |s| {
            let v = payload(id);
            s.execute("INSERT INTO items VALUES(?, ?, 0)", (id, &v))?;
            Ok(())
        });
        ins += 1;
        ins_us += t.elapsed().as_micros();
        let t = Instant::now();
        let target = 1 + (it as i64 % (next_id - 1));
        let stamp = it as i64;
        write!(&a, move |s| {
            s.execute("UPDATE items SET updated=? WHERE id=?", (stamp, target))?;
            Ok(())
        });
        upd += 1;
        upd_us += t.elapsed().as_micros();
        let t = Instant::now();
        let probe = 1 + ((it * 7919) as i64 % (next_id - 1));
        a.read(move |s| {
            Ok(s.query_row("SELECT value FROM items WHERE id=?", [probe], |r| {
                r.get::<_, String>(0)
            })?)
        })?;
        sel += 1;
        sel_us += t.elapsed().as_micros();
        it += 1;
        let spent = tick.elapsed();
        if spent < budget {
            std::thread::sleep(budget - spent);
        }
    }
    let medium_elapsed = start.elapsed().as_secs_f64();
    eprintln!("[replbench] medium done, busy_retries={}", busy());
    println!(
        "medium: {} iters in {:.1}s ({:.0}/s); avg insert={:.0}us update={:.0}us select={:.0}us",
        it,
        medium_elapsed,
        it as f64 / medium_elapsed,
        ins_us as f64 / ins as f64,
        upd_us as f64 / upd as f64,
        sel_us as f64 / sel as f64,
    );

    // Phase 2: unpaced insert burst.
    eprintln!("[replbench] burst phase: {burst_secs}s");
    let burst = Duration::from_secs(burst_secs);
    let start = Instant::now();
    let mut burst_ins = 0u64;
    let mut last_beat = Instant::now();
    while start.elapsed() < burst {
        if last_beat.elapsed() > Duration::from_secs(1) {
            let st = a.status();
            let rs = a.replication().unwrap().status();
            eprintln!(
                "[replbench] burst t={:.0}s rows={} busy={} queued={} undurable={} pause={:.1}ms peers={} staged={} applied={} err={:?}",
                start.elapsed().as_secs_f64(),
                burst_ins,
                busy(),
                st.pending_bytes,
                st.committed - st.durable,
                st.checkpoint_pause.as_secs_f64() * 1000.0,
                rs.peers,
                rs.staged_transactions,
                rs.applied_transactions,
                rs.last_error,
            );
            last_beat = Instant::now();
        }
        let first = next_id;
        next_id += BURST_BATCH;
        write!(&a, move |s| {
            for id in first..first + BURST_BATCH {
                let v = payload(id);
                s.execute("INSERT INTO items VALUES(?, ?, 0)", (id, &v))?;
            }
            Ok(())
        });
        burst_ins += BURST_BATCH as u64;
    }
    let burst_elapsed = start.elapsed().as_secs_f64();
    eprintln!("[replbench] burst done, busy_retries={}", busy());
    println!(
        "burst: {burst_ins} inserts in {burst_elapsed:.1}s = {:.0} inserts/s ({} per txn)",
        burst_ins as f64 / burst_elapsed,
        BURST_BATCH,
    );
    let lag = a.status();
    println!(
        "backlog after burst: queued={} bytes, undurable batches={}",
        lag.pending_bytes,
        lag.committed - lag.durable
    );

    // Phase 3: catch-up.
    eprintln!("[replbench] flushing writer...");
    let t = Instant::now();
    a.flush()?;
    eprintln!("[replbench] writer flushed, busy_retries={}", busy());
    println!("writer flush={:.2}s", t.elapsed().as_secs_f64());
    let want = checksum(&a);
    println!("writer rows={} id-sum={}", want.0, want.1);
    let wait = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(converge_secs);
    let mut last_log = Instant::now();
    loop {
        let got = checksum(&b);
        if got == want {
            break;
        }
        if last_log.elapsed() > Duration::from_secs(2) {
            let st = a.status();
            let ra = a.replication().unwrap().status();
            let rb = b.replication().unwrap().status();
            eprintln!(
                "[replbench] waiting: replica {:?} want {:?} queued={} undurable={}\n  writer: peers={} staged={} applied={} deferred={} pushed={} recv={} qavail={} err={:?}\n  replica: peers={} staged={} applied={} deferred={} pushed={} recv={} qavail={} err={:?}",
                got,
                want,
                st.pending_bytes,
                st.committed - st.durable,
                ra.peers,
                ra.staged_transactions,
                ra.applied_transactions,
                ra.deferred,
                ra.metrics.pushed_transactions,
                ra.metrics.received_transactions,
                ra.metrics.queue_available_bytes,
                ra.last_error,
                rb.peers,
                rb.staged_transactions,
                rb.applied_transactions,
                rb.deferred,
                rb.metrics.pushed_transactions,
                rb.metrics.received_transactions,
                rb.metrics.queue_available_bytes,
                rb.last_error,
            );
            last_log = Instant::now();
        }
        if Instant::now() > deadline {
            println!(
                "NOT CONVERGED in {converge_secs}s: replica {:?}, want {:?}",
                checksum(&b),
                want
            );
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if checksum(&b) == want {
        println!("replica converged in {:.1}s", wait.elapsed().as_secs_f64());
    }
    let _ = b.flush();

    let (ram_a, rows_a) = sqlite_ram(&a);
    let (ram_b, rows_b) = sqlite_ram(&b);
    let malloc = a
        .sqlite_metrics()
        .map(|m| m.memory_used_bytes)
        .unwrap_or(0);
    let spool_a = a.spool_metrics();
    let spool_b = b.spool_metrics();
    let disk_a = dir_bytes(&path_a);
    let disk_b = dir_bytes(&path_b);
    println!(
        "writer:  rows={rows_a} RAM(page image)={:.2} MiB disk(dir)={:.2} MiB disk/RAM={:.2}",
        mib(ram_a as u64),
        mib(disk_a),
        disk_a as f64 / ram_a as f64
    );
    println!(
        "         spool: disk={:.2} live={:.2} MiB ratio={:.2} written={:.2} MiB commits={}",
        mib(spool_a.disk_bytes),
        mib(spool_a.live_bytes),
        spool_a.compression_ratio,
        mib(spool_a.bytes_written),
        spool_a.commits,
    );
    println!(
        "replica: rows={rows_b} RAM(page image)={:.2} MiB disk(dir)={:.2} MiB disk/RAM={:.2}",
        mib(ram_b as u64),
        mib(disk_b),
        disk_b as f64 / ram_b as f64
    );
    println!(
        "         spool: disk={:.2} live={:.2} MiB ratio={:.2} written={:.2} MiB commits={}",
        mib(spool_b.disk_bytes),
        mib(spool_b.live_bytes),
        spool_b.compression_ratio,
        mib(spool_b.bytes_written),
        spool_b.commits,
    );
    println!(
        "sqlite malloc (both nodes, process-wide): {:.2} MiB",
        mib(malloc as u64)
    );
    // NOTE: close() hangs when replication is wedged (shutdown bug), and all
    // numbers are already collected, so exit without it; the OS reclaims the
    // tempdir. A clean shutdown path would call a.close()? and b.close()?.
    std::process::exit(0);
}
