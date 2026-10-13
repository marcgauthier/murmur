//! Rust port of the Go `gc-balance` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/gc-balance/`
//! (`gc_balance_test.go`). `TestLogGCKeepsUpWithChurn`: three nodes mesh
//! with aggressive log retention (1s log, 20s offline pin, 10 batches)
//! while one node updates a fixed 200-key set with eight parallel updaters
//! over disjoint ranges. After a 20s warmup, a 40s window must show GC
//! collecting at least 90% of the origin-log batches the window produced,
//! with zero GC failures; the suite ends with exact row counts and
//! identical digests on all nodes (no loss). Plain SQL replaces the Go
//! typed-contention HTTP API (see the scenario README.md for the
//! deviations).
//!
//! Scope (same as Go): version churn only; deletes are out of scope
//! (delete pruning is unimplemented).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Error as SessionError, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Fixed working set (Go: `keys = 200`).
const KEYS: usize = 200;
/// Parallel updaters over disjoint key ranges (Go: `updaters = 8`).
const UPDATERS: usize = 8;
/// Steady-state warmup before the measurement window (Go: 20s sleep).
const WARMUP: Duration = Duration::from_secs(20);
/// Measurement window on a quiet box (Go: 40s sleep).
const WINDOW: Duration = Duration::from_secs(40);
/// Window stretch step under load (Go: 5s sleeps).
const WINDOW_STRETCH_STEP: Duration = Duration::from_secs(5);
/// Window stretch cap (Go: 150s).
const WINDOW_CAP: Duration = Duration::from_secs(150);
/// Minimum commits for a meaningful balance proof (Go: 2000).
const COMMIT_FLOOR: u64 = 2000;
/// Minimum full sweeps per updater for GC pressure (Go: 5).
const MIN_SWEEPS: u64 = 5;
/// Post-stop GC-run wait cap (Go: 180s).
const RUN_WAIT_CAP: Duration = Duration::from_secs(180);
/// Post-stop drain stability gate (Go: 2 stable 5s polls, 120s cap).
const DRAIN_STABLE_POLLS: u32 = 2;
const DRAIN_POLL: Duration = Duration::from_secs(5);
const DRAIN_CAP: Duration = Duration::from_secs(120);
/// Convergence deadlines (Go: 60s counts/digests, 30s value flow).
const COUNTS_TIMEOUT: Duration = Duration::from_secs(60);
const DIGEST_TIMEOUT: Duration = Duration::from_secs(60);
const VALUE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Hang guard for updater threads if the driver panics before signalling
/// stop (normal runs stop updaters long before this).
const UPDATER_CAP: Duration = Duration::from_secs(600);

/// One application row (Go: `harness.TypedContentionRow`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: String,
    name: String,
    phone: String,
    score: i64,
}

/// 1-based 32-hex-char key (Go: `fmt.Sprintf("%032x", i+1)`). The Go API
/// treats the all-zero ID as its unset sentinel, so keys start at 1; the
/// Rust port keeps the same key space.
fn key_id(i: usize) -> String {
    format!("{:032x}", i + 1)
}

/// Seed insert on node 1 (Go: `Cluster.TypedContentionInsert` with
/// `Name: "v-0"`; phone/score keep their zero values).
fn insert_seed(db: &Database, id: String, name: String) {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
            (id, name, "", 0i64),
        )?;
        Ok(())
    })
    .expect("seed insert failed");
    db.flush().expect("flush after seed insert");
}

/// One churned update: set `name` and publish to replication. Errors are
/// returned (and counted by the caller) instead of panicking, so one bad
/// op cannot kill an updater thread (Go: `updateErrs`).
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

/// Typed read on one node: `None` when the row is absent (Go:
/// `Cluster.TypedContentionRead`).
fn read_name(db: &Database, id: &str) -> Option<String> {
    let id = id.to_owned();
    db.read(move |sql| {
        let mut stmt = sql.prepare("SELECT name FROM contention WHERE id = ?")?;
        let mut rows = stmt.query([id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row.get::<_, String>(0)?)),
            None => Ok(None),
        }
    })
    .expect("read row failed")
}

/// Canonical PK-ordered rows on one node (Go: `gcDigest` input; compared
/// directly instead of hashing).
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

/// Wait until every node holds exactly `want` rows (Go: `waitAllCounts`).
fn wait_all_counts(cluster: &Cluster, want: usize, timeout: Duration) {
    let mut last = vec![String::new(); cluster.len()];
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        for i in 0..cluster.len() {
            let count = all_rows(cluster.node(i)).len();
            last[i] = format!("count={count}");
            if count != want {
                return false;
            }
        }
        true
    });
    assert!(
        converged,
        "counts did not reach {want} within {timeout:?}; last={last:?}"
    );
}

/// Poll until every node reports the same table contents (Go:
/// `waitDigestsAgreed`).
fn wait_digests_agreed(cluster: &Cluster, timeout: Duration) {
    let converged = wait_until(timeout, POLL_INTERVAL, || {
        let want = all_rows(cluster.node(0));
        (1..cluster.len()).all(|i| all_rows(cluster.node(i)) == want)
    });
    assert!(
        converged,
        "digests did not agree within {timeout:?}"
    );
}

/// Poll followers until one reports a value other than seed for the given
/// key, proving churned updates flow end to end (Go: `waitValueChanged`).
fn wait_value_changed(cluster: &Cluster, id: &str, seed: &str, timeout: Duration) {
    let changed = wait_until(timeout, POLL_INTERVAL, || {
        (1..cluster.len())
            .any(|i| matches!(read_name(cluster.node(i), id), Some(name) if name != seed))
    });
    assert!(
        changed,
        "no follower observed a churned value for {id} within {timeout:?} (updates may not match)"
    );
}

/// Commit/GC balance counters sampled from node 1 (Go: `gcSnapshot`
/// scraped from `/metrics`).
struct GcSnapshot {
    /// Local commits produced (`Status.committed` cursor; Go:
    /// `spedsql_local_commits_total`).
    commits: u64,
    /// Origin-log transactions collected (durable; Go:
    /// `spedsql_gc_log_collected_total`).
    collected: u64,
    /// Successful collection passes (Go: `spedsql_gc_runs_total`).
    runs: u64,
    /// Latest replication-engine error, if any. Rust has no GC-failures
    /// counter; collection errors land here instead (Go:
    /// `spedsql_gc_failures_total`).
    last_error: Option<String>,
    /// Writer-scheduler local admissions. Rust collection bypasses the
    /// scheduler, so local admissions prove the scheduler served the churn
    /// instead (Go: `spedsql_sched_acquisitions_total` maintenance class).
    local_acq: u64,
}

fn gc_counters(cluster: &Cluster) -> GcSnapshot {
    let db = cluster.node(0);
    let status = db
        .replication()
        .expect("node1 replication handle")
        .status();
    GcSnapshot {
        commits: db.status().committed,
        collected: db
            .retention_status()
            .expect("node1 retention status")
            .collected_transactions,
        runs: status.metrics.retention_runs,
        last_error: status.last_error,
        local_acq: db.scheduler().snapshot().local.acquisitions,
    }
}

#[test]
fn log_gc_keeps_up_with_churn() {
    // Aggressive log retention (Go: MinLogRetentionMs=1000,
    // MaxOfflineLogRetentionMs=20000, MinRetainedBatches=10). The 1s
    // collection cadence keeps background passes inside the 40s window;
    // the product default (60s) would starve it.
    let cluster = Cluster::new(ClusterOptions {
        name: "gc-balance".into(),
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

        for i in 0..KEYS {
            insert_seed(cluster.node(0), key_id(i), "v-0".into());
        }
        wait_all_counts(&cluster, KEYS, COUNTS_TIMEOUT);

        let stopped = AtomicBool::new(false);
        let update_errs = AtomicU64::new(0);
        let updates = AtomicU64::new(0);
        let sweeps: [AtomicU64; UPDATERS] = std::array::from_fn(|_| AtomicU64::new(0));
        let updater_deadline = Instant::now() + UPDATER_CAP;

        let (before, mut after) = std::thread::scope(|s| {
            // Eight updaters over disjoint key ranges: a single sequential
            // updater cannot sustain enough churn on a loaded box, which
            // would make the pressure gate vacuous. Parallelism keeps the
            // window well above the commit floor in every environment.
            for w in 0..UPDATERS {
                let db = cluster.node(0).clone();
                let stopped = &stopped;
                let update_errs = &update_errs;
                let updates = &updates;
                let sweeps = &sweeps;
                s.spawn(move || {
                    let mut round: u64 = 1;
                    let lo = w * KEYS / UPDATERS;
                    let hi = lo + KEYS / UPDATERS;
                    while !stopped.load(Ordering::Relaxed) && Instant::now() < updater_deadline {
                        for i in lo..hi {
                            if stopped.load(Ordering::Relaxed) {
                                break;
                            }
                            match try_update_name(&db, &key_id(i), format!("v-{w}-{round}")) {
                                Ok(()) => {
                                    updates.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    eprintln!("updater {w} key {i}: {e}");
                                    update_errs.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        sweeps[w].store(round, Ordering::Relaxed);
                        round += 1;
                    }
                });
            }

            // Warm up to steady state, then measure one window: every commit
            // in the window appends an origin-log batch, so
            // (commits - collected) is the retained set's growth over the
            // window. GC keeps up iff it collects >= 90% of what the window
            // produced; a dead GC scores 0%. The window is 40s on a quiet
            // box and stretches (to a 150s cap) under parallel-suite load
            // until the commit floor is met, so the ratio proof stays
            // meaningful at any production rate.
            std::thread::sleep(WARMUP);
            let before = gc_counters(&cluster);
            std::thread::sleep(WINDOW);
            let mut after = gc_counters(&cluster);
            let mut elapsed = WINDOW;
            while after.commits - before.commits < COMMIT_FLOOR && elapsed < WINDOW_CAP {
                std::thread::sleep(WINDOW_STRETCH_STEP);
                elapsed += WINDOW_STRETCH_STEP;
                after = gc_counters(&cluster);
            }
            eprintln!(
                "window commits={} over the measurement period",
                after.commits - before.commits
            );

            stopped.store(true, Ordering::Relaxed);
            (before, after)
        });

        // Drain: the window's tail stays uncollected until a GC pass runs
        // after the updaters stop, so first wait for a post-stop pass and
        // then settle on a stable collected count before sampling `after`.
        // Stability alone is not enough: on a starved ticker no pass may
        // run for a minute, and a prematurely stable sample measures
        // ticker alignment instead of balance. A dead GC exhausts the
        // run wait and still fails via the runs/ratio checks below.
        let run_deadline = Instant::now() + RUN_WAIT_CAP;
        let window_end_runs = after.runs;
        loop {
            let next = gc_counters(&cluster);
            after = next;
            if after.runs > window_end_runs || Instant::now() > run_deadline {
                break;
            }
            std::thread::sleep(DRAIN_POLL);
        }
        let drain_deadline = Instant::now() + DRAIN_CAP;
        let mut stable = 0;
        while stable < DRAIN_STABLE_POLLS && Instant::now() < drain_deadline {
            std::thread::sleep(DRAIN_POLL);
            let next = gc_counters(&cluster);
            if next.collected == after.collected {
                stable += 1;
            } else {
                stable = 0;
            }
            after = next;
        }

        if update_errs.load(Ordering::Relaxed) != 0 {
            panic!(
                "{} update errors during churn",
                update_errs.load(Ordering::Relaxed)
            );
        }
        let mut total_sweeps = 0;
        for w in 0..UPDATERS {
            let got = sweeps[w].load(Ordering::Relaxed);
            total_sweeps += got;
            if got < MIN_SWEEPS {
                panic!(
                    "updater {w} completed only {got} sweeps, want >= {MIN_SWEEPS} for GC pressure"
                );
            }
        }
        let commits = after.commits - before.commits;
        let collected = after.collected - before.collected;
        let runs = after.runs - before.runs;
        let local_acq = after.local_acq - before.local_acq;
        eprintln!(
            "churn: {total_sweeps} updater sweeps ({} test-counted updates), {commits} commits produced, {collected} log batches collected, {runs} gc runs, {local_acq} local scheduler admissions",
            updates.load(Ordering::Relaxed),
        );

        // The balance assertion is vacuous without proven sustained churn
        // (the retained set scales with the production rate, so the 90%
        // bound stays meaningful at any rate above the floor).
        if commits < COMMIT_FLOOR {
            panic!("only {commits} commits in the window, want >= {COMMIT_FLOOR} for GC pressure");
        }
        if local_acq == 0 {
            panic!("no local scheduler admissions during {commits} commits; scheduler starves writers");
        }
        if runs == 0 {
            panic!("no GC runs during the window; test cannot observe GC");
        }
        if after.last_error != before.last_error {
            panic!(
                "replication engine error during the window: before={:?} after={:?}",
                before.last_error, after.last_error
            );
        }
        if commits.saturating_sub(collected) > commits / 10 {
            panic!(
                "GC fell behind: produced {commits} commits but collected {collected} (retained grew by {}, want <= {})",
                commits.saturating_sub(collected),
                commits / 10
            );
        }

        // End-to-end update flow: a follower must observe a churned value.
        wait_value_changed(&cluster, &key_id(0), "v-0", VALUE_TIMEOUT);

        // No data loss: exact key set with identical digests everywhere.
        // Counts never change for fixed-key updates, so convergence means
        // digest agreement: poll for it, since followers drain the final
        // backlog after the updaters stop.
        wait_all_counts(&cluster, KEYS, COUNTS_TIMEOUT);
        wait_digests_agreed(&cluster, DIGEST_TIMEOUT);
        let want = all_rows(cluster.node(0));
        for i in 1..cluster.len() {
            let digest = all_rows(cluster.node(i));
            assert_eq!(digest, want, "node{} digest differs after GC churn", i + 1);
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("gc-balance-no-balance");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
