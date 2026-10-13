//! Rust port of the Go `maintenance-under-load` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/maintenance-under-load/`
//! (`maintenance_under_load_test.go`). `TestMaintenanceUnderSustainedLoad`:
//! three nodes take sustained concurrent writes (zero failed writes allowed)
//! while tight retention forces origin-log GC. GC passes and batch
//! collection must advance on every node with writers provably still
//! progressing, GC failures must stay flat, and all nodes must converge on
//! exact counts plus equal id-ordered digests, followed by a
//! post-maintenance write that replicates everywhere. Plain SQL replaces
//! the Go typed-contention HTTP API (see the scenario README.md for the
//! deviations).
//!
//! Only counter-observed maintenance is claimed (same inventory as Go):
//! origin-log GC is claimed; receipt collection, durability sync, FilesGC,
//! snapshot resync, and spool compaction are NOT claimed.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Error as SessionError, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Seed rows inserted before the load phase (Go: `MURMUR_MAINT_LOAD_SEED`, 120).
fn env_seed() -> usize {
    env_usize("MURMUR_MAINT_LOAD_SEED", 120)
}

/// Per-writer insert cap before writers switch to updates
/// (Go: `MURMUR_MAINT_LOAD_MAX_INSERTS`, 4000).
fn env_max_inserts() -> usize {
    env_usize("MURMUR_MAINT_LOAD_MAX_INSERTS", 4000)
}

/// Maintenance observation window (Go: `MURMUR_MAINT_LOAD_GC_TIMEOUT`, 150s).
fn env_gc_timeout() -> Duration {
    env_dur("MURMUR_MAINT_LOAD_GC_TIMEOUT", Duration::from_secs(150))
}

/// Seed-convergence deadline (Go: 60s).
const SEED_TIMEOUT: Duration = Duration::from_secs(60);
/// Final-convergence deadline (Go: 90s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(90);
/// Post-maintenance write deadline (Go: 30s).
const POST_TIMEOUT: Duration = Duration::from_secs(30);
/// Maintenance poll interval (Go: 2s sleeps).
const MAINT_POLL: Duration = Duration::from_secs(2);
/// Convergence poll interval (Go: 100ms).
const CONVERGE_POLL: Duration = Duration::from_millis(100);
/// Hang guard for writer threads if the driver panics before signalling
/// stop (normal runs stop writers long before this).
const WRITER_CAP: Duration = Duration::from_secs(900);

/// One application row (Go: `harness.TypedContentionRow`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: String,
    name: String,
    phone: String,
    score: i64,
}

/// Go `rowID`: `%08x-0000-4000-8000-%012x`. Same key space as the Go suite
/// (seed ids `0..seed`, writer ranges at `1<<20*(w+1)`, post-maintenance id
/// at `seed + 1<<22`).
fn row_id(n: usize) -> String {
    format!("{n:08x}-0000-4000-8000-{n:012x}")
}

/// Insert on one node; errors are returned (and counted by the caller) so
/// one bad op ends a writer thread without killing the driver (Go: the
/// writer loop returns on the first error and `errCounts` is asserted).
fn try_insert(db: &Database, id: String, name: String) -> Result<(), String> {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
            (id, name, "", 0i64),
        )?;
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    db.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// One update: set `name` and publish to replication.
fn try_update_name(db: &Database, id: &str, name: String) -> Result<(), String> {
    let id = id.to_owned();
    db.transaction(move |sql| {
        let changed = sql.execute("UPDATE contention SET name = ? WHERE id = ?", (name, id))?;
        if changed != 1 {
            return Err(SessionError::Invalid(format!(
                "update hit {changed} rows"
            )));
        }
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    db.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// Canonical PK-ordered rows on one node (Go: `TypedContentionRows` +
/// `contentionDigest` input; compared directly instead of hashing).
fn all_rows(db: &Database) -> Vec<Row> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name, phone, score FROM contention ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok(Row {
                id: row.get(0)?,
                name: row.get(1)?,
                phone: row.get(2)?,
                score: row.get(3)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read all rows failed")
}

/// Wait until every node holds exactly `want` rows with identical contents
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let mut last = vec![String::new(); cluster.len()];
    let converged = wait_until(timeout, CONVERGE_POLL, || {
        for i in 0..cluster.len() {
            let rows = all_rows(cluster.node(i));
            last[i] = format!("count={}", rows.len());
            if rows.len() != want {
                return false;
            }
        }
        let first = all_rows(cluster.node(0));
        for i in 1..cluster.len() {
            if all_rows(cluster.node(i)) != first {
                last[i] = format!("count={want} digest-differs");
                return false;
            }
        }
        true
    });
    assert!(
        converged,
        "nodes did not converge on {want} rows with equal digests within {timeout:?}; last={last:?}"
    );
}

/// Per-node GC counters (Go: scraped from `/metrics`).
struct GcSnapshot {
    /// Successful collection passes (Go: `spedsql_gc_runs_total`).
    runs: u64,
    /// Origin-log transactions collected (Go:
    /// `spedsql_gc_log_collected_total`).
    collected: u64,
    /// Latest replication-engine error, if any. Rust has no GC-failures
    /// counter; collection errors land here instead (Go:
    /// `spedsql_gc_failures_total`).
    last_error: Option<String>,
}

fn gc_counters(db: &Database) -> GcSnapshot {
    let status = db
        .replication()
        .expect("replication handle")
        .status();
    GcSnapshot {
        runs: status.metrics.retention_runs,
        collected: db
            .retention_status()
            .expect("retention status")
            .collected_transactions,
        last_error: status.last_error,
    }
}

/// Poll until `retention_runs` AND `collected_transactions` have advanced
/// past baseline on EVERY node (Go: `waitMaintenance`). Writers must still
/// be running while this is called.
fn wait_maintenance(cluster: &Cluster, before: &[GcSnapshot], timeout: Duration) {
    let observed = wait_until(timeout, MAINT_POLL, || {
        (0..cluster.len()).all(|i| {
            let cur = gc_counters(cluster.node(i));
            cur.runs > before[i].runs && cur.collected > before[i].collected
        })
    });
    for i in 0..cluster.len() {
        let cur = gc_counters(cluster.node(i));
        eprintln!(
            "node{}: gc runs {}->{} collected {}->{} last_error={:?}",
            i + 1,
            before[i].runs,
            cur.runs,
            before[i].collected,
            cur.collected,
            cur.last_error
        );
    }
    assert!(
        observed,
        "log-GC maintenance did not run+collect on every node within {timeout:?}"
    );
}

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

/// Parse a Go-style duration (`150`, `150s`, `2m`) with fallback.
fn env_dur(name: &str, fallback: Duration) -> Duration {
    let raw = std::env::var(name).unwrap_or_default();
    let raw = raw.trim();
    if raw.is_empty() {
        return fallback;
    }
    if let Ok(s) = raw.parse::<u64>() {
        if s > 0 {
            return Duration::from_secs(s);
        }
        return fallback;
    }
    let (num, unit) = raw.split_at(raw.len().saturating_sub(1));
    let n: u64 = num.parse().unwrap_or(0);
    if n == 0 {
        return fallback;
    }
    match unit {
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        _ => fallback,
    }
}

#[test]
fn maintenance_under_sustained_load() {
    let seed = env_seed();
    let max_inserts = env_max_inserts();
    let gc_timeout = env_gc_timeout();
    // Tight retention forces origin-log GC (Go: MinLogRetentionMs=1000,
    // MaxOfflineLogRetentionMs=20000, MinRetainedBatches=10). The 1s
    // collection cadence keeps background passes inside the observation
    // window; the product default (60s) would starve it.
    let cluster = Cluster::new(ClusterOptions {
        name: "maintenance-under-load".into(),
        num_nodes: 3,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            retention_interval: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            maximum_offline: Some(Duration::from_secs(20)),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "{} dir={} repl={}",
            info.label,
            info.dir.display(),
            info.repl_addr
        );
    }
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: TypedRecords/TypedContention tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE contention(id TEXT PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)",
                ))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Seed a base row set and converge before the load phase.
        for i in 0..seed {
            try_insert(cluster.node(0), row_id(i), format!("seed-{i}"))
                .expect("seed insert failed");
        }
        wait_converged(&cluster, seed, SEED_TIMEOUT);

        let before: Vec<GcSnapshot> = (0..cluster.len())
            .map(|i| gc_counters(cluster.node(i)))
            .collect();

        // Sustained concurrent writers on disjoint ranges: inserts until
        // the per-writer cap, then updates cycling their own rows so load
        // never stops while maintenance is awaited. Zero failed writes
        // allowed.
        let stop = AtomicBool::new(false);
        let ok_counts: Vec<AtomicI64> = (0..cluster.len()).map(|_| AtomicI64::new(0)).collect();
        let err_counts: Vec<AtomicI64> = (0..cluster.len()).map(|_| AtomicI64::new(0)).collect();
        let writer_deadline = Instant::now() + WRITER_CAP;
        std::thread::scope(|s| {
            for w in 0..cluster.len() {
                let db = cluster.node(w).clone();
                let stop = &stop;
                let ok_counts = &ok_counts;
                let err_counts = &err_counts;
                s.spawn(move || {
                    let base: usize = 1 << 20 * (w + 1);
                    let mut n: usize = 0;
                    loop {
                        if stop.load(Ordering::Relaxed) || Instant::now() >= writer_deadline {
                            return;
                        }
                        let res = if n < max_inserts {
                            let id = base + n;
                            try_insert(&db, row_id(id), format!("w{w}-{n}"))
                        } else {
                            let id = base + (n % max_inserts);
                            try_update_name(&db, &row_id(id), format!("w{w}-u{n}"))
                        };
                        match res {
                            Ok(()) => {
                                ok_counts[w].fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                eprintln!("writer {w} op {n}: {e}");
                                err_counts[w].fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                        n += 1;
                    }
                });
            }

            // Maintenance must run WHILE writers are active: wait until GC
            // passes and batch collection advance on EVERY node, with
            // writers still making progress throughout.
            wait_maintenance(&cluster, &before, gc_timeout);
            let mut progress_mid = vec![0i64; cluster.len()];
            for w in 0..cluster.len() {
                progress_mid[w] = ok_counts[w].load(Ordering::Relaxed);
            }
            stop.store(true, Ordering::Relaxed);
            // Proven overlap: every writer had already progressed when GC
            // was observed (the scope end below joins the writers).
            for w in 0..cluster.len() {
                eprintln!(
                    "writer {w}: {} ok when GC observed",
                    progress_mid[w]
                );
                assert!(
                    progress_mid[w] > 0,
                    "writer {w} made no progress before GC was observed; load/maintenance did not overlap"
                );
            }
        });

        // Zero failed writes allowed (counts are final: the scope joined).
        for w in 0..cluster.len() {
            eprintln!(
                "writer {w}: {} ok, {} failed",
                ok_counts[w].load(Ordering::Relaxed),
                err_counts[w].load(Ordering::Relaxed)
            );
            let failed_writes = err_counts[w].load(Ordering::Relaxed);
            assert_eq!(
                failed_writes, 0,
                "writer {w} had {failed_writes} failed writes under maintenance; want zero"
            );
        }

        // No GC pass may have failed anywhere.
        for i in 0..cluster.len() {
            let after = gc_counters(cluster.node(i));
            assert_eq!(
                after.last_error, before[i].last_error,
                "node{} replication error during load: before={:?} after={:?}",
                i + 1,
                before[i].last_error,
                after.last_error
            );
        }

        // Full convergence: exact counts plus equal id-ordered digests.
        let mut want_count = seed;
        for w in 0..cluster.len() {
            want_count += (ok_counts[w].load(Ordering::Relaxed) as usize).min(max_inserts);
        }
        wait_converged(&cluster, want_count, CONVERGE_TIMEOUT);

        // A post-maintenance write replicates everywhere.
        try_insert(
            cluster.node(2),
            row_id(seed + (1 << 22)),
            "post-maint".into(),
        )
        .expect("post-maintenance insert");
        wait_converged(&cluster, want_count + 1, POST_TIMEOUT);
        eprintln!(
            "maintenance-under-load proven: GC ran on every node under zero-error load, full convergence"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("maintenance-under-load-no-maint");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
