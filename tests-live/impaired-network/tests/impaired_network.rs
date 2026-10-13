//! Rust port of the Go `impaired-network` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/impaired-network/`
//! (`impaired_network_test.go`). A 3-node mesh keeps converging while
//! tc/netem impairs ONLY the replication ports: (a) +150 ms latency,
//! (b) 3% packet loss, (c) a 1 Mbit cap during a stale-node snapshot
//! resync under sustained writes. Membership must stay stable (no false
//! evictions) and per-row visibility latencies are recorded with relaxed
//! p95 bounds. Plain SQL replaces the Go typed-contention HTTP API (see
//! the scenario README.md for deviations).
//!
//! Env-gated like Go: SKIPs without tc/netem + CAP_NET_ADMIN on lo;
//! `MURMUR_IMPAIRED_NETWORK_FORCE=1` runs the same write/converge
//! workload with no impairment (logic verification only).

use std::process::Command;
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Contention table DDL (Go: typed contention rows).
const TABLE_DDL: &str = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)";
/// Force knob (Go: `MURMUR_IMPAIRED_NETWORK_FORCE=1`).
const FORCE_ENV: &str = "MURMUR_IMPAIRED_NETWORK_FORCE";
/// Row-count knob (Go: `MURMUR_IMPAIRED_NETWORK_ROWS`, default 8).
const ROWS_ENV: &str = "MURMUR_IMPAIRED_NETWORK_ROWS";
const DEFAULT_ROWS: usize = 8;
/// Baseline rows (Go: 10).
const BASELINE_ROWS: usize = 10;
/// Baseline/capped convergence deadline (Go: 150 s, 180 s capped).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(150);
const CAPPED_TIMEOUT: Duration = Duration::from_secs(180);
/// Per-row visibility deadlines (Go: 30 s latency, 45 s loss).
const LAT_ROW_TIMEOUT: Duration = Duration::from_secs(30);
const LOSS_ROW_TIMEOUT: Duration = Duration::from_secs(45);
/// p95 visibility bounds (Go: 15 s latency, 30 s loss).
const LAT_P95_BOUND: Duration = Duration::from_secs(15);
const LOSS_P95_BOUND: Duration = Duration::from_secs(30);
/// Membership waits (Go: 60 s) and poll (Go: 250 ms).
const MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(60);
const MEMBERSHIP_POLL: Duration = Duration::from_millis(250);
/// Row-visibility poll interval (Go: 50 ms).
const VISIBILITY_POLL: Duration = Duration::from_millis(50);
/// Convergence poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Survivor-count deadline (Go: 60 s).
const COUNT_TIMEOUT: Duration = Duration::from_secs(60);
/// GC-pass wait (Go: 60 s after a 12 s expiry sleep).
const EXPIRY_SLEEP: Duration = Duration::from_secs(12);
const GC_TIMEOUT: Duration = Duration::from_secs(60);

/// Ordered contention rows on one node (Go: `TypedContentionRows`).
fn rows(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name FROM contention ORDER BY id")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read contention rows failed")
}

/// Insert one row (Go: `TypedContentionInsert`).
fn insert_row(db: &Database, node: usize, id: String, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute("INSERT INTO contention(id, name) VALUES(?, ?)", (id, name))?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
}

/// Rows named `name` on one node (Go: `countWhere`).
fn count_where(db: &Database, name: &str) -> usize {
    rows(db).iter().filter(|(_, n)| n == name).count()
}

/// Discovered-member count, excluding self (Go `membership_count` minus
/// one; established `swim-discovery` port pattern).
fn member_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .members
}

/// Wait until every node sees `want` discovered members (Go: `waitMembership`).
fn wait_membership(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, MEMBERSHIP_POLL, || {
        (0..cluster.len()).all(|i| member_count(cluster, i) == want)
    });
    assert!(
        converged,
        "nodes did not reach membership {want} (excl. self) within {timeout:?}"
    );
}

/// Require a full healthy membership view on every node: all members
/// present (Go also checks alive == 2, which has no Rust equivalent;
/// any eviction still drops the member view and fails here).
fn assert_stable_membership(cluster: &Cluster) {
    for i in 0..cluster.len() {
        assert_eq!(
            member_count(cluster, i),
            2,
            "node{} members = {} during impairment, want 2 (false eviction?)",
            i + 1,
            member_count(cluster, i)
        );
    }
}

/// Wait until `name` is visible exactly once on every node (Go: `waitRowVisible`).
fn wait_row_visible(cluster: &Cluster, name: &str, timeout: Duration) {
    let converged = wait_until(timeout, VISIBILITY_POLL, || {
        (0..cluster.len()).all(|i| count_where(cluster.node(i), name) == 1)
    });
    assert!(
        converged,
        "row {name:?} not visible on all nodes within {timeout:?}"
    );
}

/// Poll exact counts plus ordered-digest equality, logging counts every
/// 10 s while waiting (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let mut last_log = Instant::now();
    let converged = wait_until(timeout, POLL, || {
        let mut first: Option<Vec<(String, String)>> = None;
        let mut ok = true;
        for i in 0..cluster.len() {
            let got = rows(cluster.node(i));
            if got.len() != want {
                ok = false;
                break;
            }
            match &first {
                None => first = Some(got),
                Some(prev) if *prev == got => {}
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok && last_log.elapsed() > Duration::from_secs(10) {
            last_log = Instant::now();
            let counts: Vec<_> = (0..cluster.len())
                .map(|i| rows(cluster.node(i)).len())
                .collect();
            eprintln!("converge progress: counts={counts:?} want={want}");
        }
        ok
    });
    assert!(
        converged,
        "nodes did not converge on {want} rows with equal digests within {timeout:?}"
    );
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
fn wait_row_count(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        rows(cluster.node(idx)).len() == want
    });
    assert!(
        converged,
        "node{} count = {}, want {want} within {timeout:?}",
        idx + 1,
        rows(cluster.node(idx)).len()
    );
}

/// Nearest-rank percentile (Go: `percentile`; k = ceil(len*p/100)).
fn percentile(d: &mut [Duration], p: usize) -> Duration {
    if d.is_empty() {
        return Duration::ZERO;
    }
    d.sort();
    let mut k = (d.len() * p + 99) / 100;
    if k < 1 {
        k = 1;
    }
    d[k - 1]
}

/// Probe tc/netem usability on lo (Go: `tcCapable`). Returns `(true, "")`
/// when impairment can be applied, else `(false, reason)`.
fn tc_capable() -> (bool, String) {
    if Command::new("tc").arg("--version").output().is_err() {
        return (false, "tc binary not found".into());
    }
    for _ in 0..2 {
        let show = Command::new("tc")
            .args(["qdisc", "show", "dev", "lo"])
            .output();
        let out = match show {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
            Ok(o) => {
                return (
                    false,
                    format!(
                        "tc qdisc show failed: {}",
                        String::from_utf8_lossy(&o.stderr).trim()
                    ),
                )
            }
            Err(e) => return (false, format!("tc qdisc show failed: {e}")),
        };
        if out.contains("77:") {
            // Our own leftover from a crashed run: remove and re-probe.
            let cleared = Command::new("tc")
                .args(["qdisc", "del", "dev", "lo", "root"])
                .output()
                .is_ok_and(|o| o.status.success());
            if !cleared {
                return (false, "cannot clear stale qdisc 77:".into());
            }
            continue;
        }
        if out.contains("qdisc") && !out.contains("noqueue") {
            return (
                false,
                format!(
                    "lo already has a root qdisc ({}); refusing to disturb shared box",
                    out.trim()
                ),
            );
        }
        // Privilege probe: install and immediately remove an empty prio.
        let added = Command::new("tc")
            .args(["qdisc", "add", "dev", "lo", "root", "handle", "77:", "prio"])
            .output()
            .is_ok_and(|o| o.status.success());
        if !added {
            return (false, "tc qdisc add failed (need CAP_NET_ADMIN)".into());
        }
        let cleaned = Command::new("tc")
            .args(["qdisc", "del", "dev", "lo", "root"])
            .output()
            .is_ok_and(|o| o.status.success());
        if !cleaned {
            return (false, "tc probe cleanup failed".into());
        }
        return (true, String::new());
    }
    (false, "cannot clear stale qdisc 77:".into())
}

/// Remove qdisc 77: when present (Go: `clearImpairment`; always runs).
fn clear_impairment() {
    let show = Command::new("tc")
        .args(["qdisc", "show", "dev", "lo"])
        .output();
    match show {
        Ok(o)
            if o.status.success()
                && String::from_utf8_lossy(&o.stdout).contains("77:") => {}
        _ => return,
    }
    let _ = Command::new("tc")
        .args(["qdisc", "del", "dev", "lo", "root"])
        .status();
}

/// Run one tc command; on failure clears the impairment and panics
/// (Go: the `run` closure inside `applyImpairment`).
fn tc_run(what: &str, args: &[&str]) {
    let out = Command::new("tc").args(args).output();
    match out {
        Ok(o) if o.status.success() => {}
        Ok(o) => {
            clear_impairment();
            panic!(
                "tc {what} failed ({})",
                String::from_utf8_lossy(&o.stderr).trim()
            );
        }
        Err(e) => {
            clear_impairment();
            panic!("tc {what} failed: {e}");
        }
    }
}

/// Attach the impairment to lo as root qdisc 77: (prio) with the
/// netem/tbf child on band 77:3; u32 filters steer only packets whose
/// source OR destination port is a cluster replication port into the
/// impaired band (Go: `applyImpairment`).
fn apply_impairment(ports: &[u16], child: &str, args: &[&str]) {
    tc_run(
        "root",
        &[
            "qdisc", "replace", "dev", "lo", "root", "handle", "77:", "prio", "bands", "3",
        ],
    );
    let mut child_args = vec![
        "qdisc".to_owned(),
        "replace".to_owned(),
        "dev".to_owned(),
        "lo".to_owned(),
        "parent".to_owned(),
        "77:3".to_owned(),
        "handle".to_owned(),
        "773:".to_owned(),
        child.to_owned(),
    ];
    child_args.extend(args.iter().map(|s| s.to_string()));
    let child_refs: Vec<&str> = child_args.iter().map(String::as_str).collect();
    tc_run("child", &child_refs);
    for port in ports {
        let p = port.to_string();
        // Both directions: every replication packet touches a repl port
        // as either source or destination.
        tc_run(
            "filter-dport",
            &[
                "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0", "prio",
                "77", "u32", "match", "ip", "dport", &p, "0xffff", "flowid", "77:3",
            ],
        );
        tc_run(
            "filter-sport",
            &[
                "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0", "prio",
                "77", "u32", "match", "ip", "sport", &p, "0xffff", "flowid", "77:3",
            ],
        );
    }
}

/// GC run counter on one node (Go: `spedsql_gc_runs_total`).
fn gc_runs(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics
        .retention_runs
}

/// Run on-demand retention passes until one completes past the baseline
/// (Go: `waitGCPass` on the background ticker; Rust exposes the pass
/// directly — established `graceful-shutdown` port pattern).
fn wait_gc_pass(cluster: &Cluster, idx: usize, timeout: Duration) {
    let base = gc_runs(cluster, idx);
    let converged = wait_until(timeout, Duration::from_secs(1), || {
        cluster
            .node(idx)
            .collect_retention()
            .expect("retention collection pass");
        gc_runs(cluster, idx) > base
    });
    assert!(converged, "no GC pass observed within {timeout:?}");
}

/// Log-path activity counters `(received, applied)`: snapshot application
/// bypasses both, so a flat pair proves the snapshot path (established
/// `snapshot-resync` port pattern; Go reads
/// `spedsql_repl_snapshots_received_total`, which has no Rust equivalent).
fn log_counters(cluster: &Cluster, idx: usize) -> (u64, u64) {
    let metrics = cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics;
    (metrics.received_transactions, metrics.applied_transactions)
}

/// Insert rows round-robin and record per-row full-mesh visibility
/// latency; every row also samples stable membership (Go: `timedWrites`).
fn timed_writes(
    cluster: &Cluster,
    prefix: &str,
    base: usize,
    n: usize,
    per_row: Duration,
) -> Vec<Duration> {
    let mut lat = Vec::with_capacity(n);
    for i in 0..n {
        let name = format!("{prefix}-{i}");
        let id = format!("{:032x}", (base + 1) * 100000 + i);
        let start = Instant::now();
        insert_row(cluster.node(i % cluster.len()), i % cluster.len() + 1, id, &name);
        wait_row_visible(cluster, &name, per_row);
        lat.push(start.elapsed());
        assert_stable_membership(cluster);
    }
    lat
}

#[test]
fn impaired_network_converges() {
    let force = std::env::var(FORCE_ENV).as_deref() == Ok("1");
    let (tc_ok, skip_reason) = tc_capable();
    if !force && !tc_ok {
        eprintln!(
            "SKIP: impaired-network needs tc/netem + CAP_NET_ADMIN on lo: {skip_reason} (set {FORCE_ENV}=1 to run the workload unimpaired)"
        );
        return;
    }
    if force && !tc_ok {
        eprintln!("FORCE mode: tc unusable ({skip_reason}); running workload WITHOUT impairment");
    }
    let rows: usize = std::env::var(ROWS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ROWS);

    let mut cluster = Cluster::new(ClusterOptions {
        name: "impaired-network".into(),
        num_nodes: 3,
        // Aggressive retention so phase (c) can force a snapshot resync
        // after a short offline window; harmless to phases (a)/(b).
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_secs(1)),
            minimum_retained: Some(10),
            maximum_offline: Some(Duration::from_secs(10)),
            ..Default::default()
        }),
        bootstrap_seeds: vec![0],
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
    let repl_ports: Vec<u16> = (0..cluster.len())
        .map(|i| cluster.node_info(i).repl_addr.port())
        .collect();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: typed contention fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }
        wait_membership(&cluster, 2, MEMBERSHIP_TIMEOUT);

        // Baseline converges unimpaired (honest-path control).
        let mut base = BASELINE_ROWS;
        for i in 0..base {
            insert_row(
                cluster.node(0),
                1,
                format!("{:032x}", 1000 + i),
                &format!("base-{i}"),
            );
        }
        wait_converged(&cluster, base, BASELINE_TIMEOUT);

        // Phase (a): +150 ms latency on replication traffic.
        if force {
            eprintln!("FORCE mode: NO 150ms latency applied; green here proves the workload only, not impairment tolerance");
        } else {
            apply_impairment(&repl_ports, "netem", &["delay", "150ms"]);
        }
        let mut lat = timed_writes(&cluster, "lat", base, rows, LAT_ROW_TIMEOUT);
        let p95 = percentile(&mut lat, 95);
        eprintln!("latency phase: {} rows p95 visibility={p95:?} (bound 15s)", lat.len());
        assert!(p95 <= LAT_P95_BOUND, "p95 visibility {p95:?} exceeds 15s bound");
        if !force {
            clear_impairment();
        }
        base += rows;

        // Phase (b): 3% packet loss on replication traffic.
        if force {
            eprintln!("FORCE mode: NO 3% loss applied; green here proves the workload only, not impairment tolerance");
        } else {
            apply_impairment(&repl_ports, "netem", &["loss", "3%"]);
        }
        let mut lat = timed_writes(&cluster, "loss", base, rows, LOSS_ROW_TIMEOUT);
        let p95 = percentile(&mut lat, 95);
        eprintln!("loss phase: {} rows p95 visibility={p95:?} (bound 30s)", lat.len());
        assert!(p95 <= LOSS_P95_BOUND, "p95 visibility {p95:?} exceeds 30s bound");
        if !force {
            clear_impairment();
        }
        base += rows;

        // Phase (c): 1 Mbit cap during a stale-node snapshot resync under
        // sustained writes.
        if force {
            eprintln!("FORCE mode: NO bandwidth cap applied; green here proves the workload only, not impairment tolerance");
        }
        for i in 0..5 {
            insert_row(
                cluster.node(2),
                3,
                format!("{:032x}", 5000 + i),
                &format!("stale-{i}"),
            );
        }
        wait_converged(&cluster, base + 5, BASELINE_TIMEOUT);
        cluster.stop_node(2);

        // Survivors write far past the short retention while node 3 is
        // offline (sustained writes continue below).
        for i in 0..60 {
            insert_row(
                cluster.node(0),
                1,
                format!("{:032x}", 6000 + i),
                &format!("fresh-{i}"),
            );
        }
        wait_row_count(&cluster, 0, base + 65, COUNT_TIMEOUT);
        wait_row_count(&cluster, 1, base + 65, COUNT_TIMEOUT);

        // Node 3's 10 s retention pin must lapse, then one GC pass must
        // collect its ranges so the rejoin needs a snapshot. Poll the GC
        // counter instead of a fixed sleep.
        std::thread::sleep(EXPIRY_SLEEP);
        wait_gc_pass(&cluster, 0, GC_TIMEOUT);

        // Cap replication throughput, then restart node 3 into it and
        // keep writing while the snapshot transfers.
        if !force {
            apply_impairment(
                &repl_ports,
                "tbf",
                &["rate", "1mbit", "burst", "32kbit", "latency", "400ms"],
            );
        }
        cluster.start_node(2).expect("restart stale node3");
        cluster.mesh().expect("re-mesh stale node3");
        for i in 0..20 {
            insert_row(
                cluster.node(1),
                2,
                format!("{:032x}", 8000 + i),
                &format!("sustain-{i}"),
            );
        }
        wait_converged(&cluster, base + 85, CAPPED_TIMEOUT);
        let (received, applied) = log_counters(&cluster, 2);
        assert!(
            (received, applied) == (0, 0),
            "node3 rejoin arrived over the log path (received={received} applied={applied}), want snapshot (log catch-up would hide a snapshot-path regression)"
        );
        wait_membership(&cluster, 2, MEMBERSHIP_TIMEOUT);
        eprintln!("capped resync converged with snapshot path proven");
        if !force {
            clear_impairment();
        }
    }))
    .is_err();
    // The suite always removes qdisc 77:, on success and on failure.
    if !force {
        clear_impairment();
    }
    if failed {
        cluster.dump_forensics("impaired-network-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
