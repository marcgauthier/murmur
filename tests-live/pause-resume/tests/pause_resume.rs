//! Rust port of the Go `pause-resume` live scenario (test-side equivalent).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/pause-resume/`
//! (`pause_resume_test.go`). Three nodes mesh while all take writes; node 2
//! is frozen for 15-30 s while the survivors keep writing, then resumes
//! with zero data loss and converging digests. SIGSTOP of one in-process
//! node would stop the test driver too, so the freeze is a scoped
//! iptables packet blackhole on the frozen node's replication port: from
//! the mesh's view the node goes dark exactly like a stopped process
//! (no packets in or out), while locally it simply takes no writes —
//! which Go's frozen node cannot do either (see the scenario README.md).
//!
//! Env-gated like Go's platform skip: SKIPs when iptables rules cannot be
//! installed (no privilege); teardown never leaks a rule.

use std::collections::HashSet;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Table DDL (Go: typed records; only names matter here).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT PRIMARY KEY)";
/// Frozen node (Go: node2).
const FROZEN: usize = 1;
/// Freeze-window knob (Go: `MURMUR_PAUSE_RESUME_STOP_SECONDS`, default 20,
/// 15 s floor).
const STOP_ENV: &str = "MURMUR_PAUSE_RESUME_STOP_SECONDS";
const DEFAULT_STOP_SECS: u64 = 20;
const MIN_STOP_SECS: u64 = 15;
/// Baseline rows (Go: 5).
const BASELINE_ROWS: usize = 5;
/// Writer cadence (Go: 25 ms ticker on every node).
const WRITE_TICK: Duration = Duration::from_millis(25);
/// In-flight drain before freezing (Go: 300 ms).
const DRAIN: Duration = Duration::from_millis(300);
/// Pre-roll before the freeze (Go: 2 s).
const PRE_ROLL: Duration = Duration::from_secs(2);
/// Mesh waits (Go: 60 s peers + membership, 30 s baseline converge).
const MESH_TIMEOUT: Duration = Duration::from_secs(60);
const BASELINE_TIMEOUT: Duration = Duration::from_secs(30);
/// Freeze-detection sample cadence (Go: 1 s).
const SAMPLE_TICK: Duration = Duration::from_secs(1);
/// Post-resume convergence bound (Go: 180 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(180);
/// Re-admission waits (Go: 90 s sessions + membership).
const READMIT_TIMEOUT: Duration = Duration::from_secs(90);
/// Post-resume convergence (Go: 60 s).
const POST_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);
/// Membership/session poll interval (Go: 250 ms).
const MEMBER_POLL: Duration = Duration::from_millis(250);

/// Stops background writers when dropped, so a panic in the driver body
/// never wedges the scope join on workers waiting for the stop flag.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM live_typed_records ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in rows {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read names failed")
}

/// Best-effort insert; true when acknowledged.
fn try_insert(db: &Database, name: String) -> bool {
    db.transaction(move |sql| {
        sql.execute("INSERT INTO live_typed_records(name) VALUES(?)", [name])?;
        Ok(())
    })
    .is_ok()
}

/// Live session count (Go: `connected_peers`).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .peers
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

/// Failed membership probes on one node (Go:
/// `spedsql_swim_probe_failures_total`).
fn probe_failures(cluster: &Cluster, node: usize) -> u64 {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .metrics
        .probes_failed
}

/// Gap-pull requests sent by one node (Go: `spedsql_repl_needs_sent_total`;
/// proves the catch-up path ran).
fn needs_sent(cluster: &Cluster, node: usize) -> u64 {
    cluster
        .node(node)
        .replication()
        .expect("replication handle")
        .status()
        .metrics
        .chunk_need_sent
}

/// Wait until every node reports `want` live sessions (Go: `waitConnectedPeers`).
fn wait_connected_peers(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, MEMBER_POLL, || {
        (0..cluster.len()).all(|i| peer_count(cluster, i) == want)
    });
    assert!(
        converged,
        "nodes did not reach {want} connected peers within {timeout:?}"
    );
}

/// Wait until every node sees `want` discovered members (Go: `waitMembership`).
fn wait_membership(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, MEMBER_POLL, || {
        (0..cluster.len()).all(|i| member_count(cluster, i) == want)
    });
    assert!(
        converged,
        "nodes did not reach membership {want} (excl. self) within {timeout:?}"
    );
}

/// Poll exact counts plus ordered-name equality (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        let mut first: Option<Vec<String>> = None;
        for i in 0..cluster.len() {
            let got = names(cluster.node(i));
            if got.len() != want {
                return false;
            }
            match &first {
                None => first = Some(got),
                Some(prev) if *prev == got => {}
                _ => return false,
            }
        }
        true
    });
    if !converged {
        for i in 0..cluster.len() {
            eprintln!("node{} at timeout: count={}", i + 1, names(cluster.node(i)).len());
        }
        panic!("nodes did not converge on {want} records with equal contents within {timeout:?}");
    }
}

/// Require every acknowledged value present on every node
/// (Go: `assertAcknowledgedPresent`).
fn assert_acknowledged_present(cluster: &Cluster, acknowledged: &HashSet<String>) {
    for i in 0..cluster.len() {
        let present: HashSet<_> = names(cluster.node(i)).into_iter().collect();
        for value in acknowledged {
            assert!(
                present.contains(value),
                "node{} lost acknowledged write {value:?} after resume",
                i + 1
            );
        }
    }
}

/// Install the packet blackhole: DROP every UDP packet whose source OR
/// destination port is the frozen node's replication port. Rules insert
/// at the head of OUTPUT (an early ACCEPT-established rule would
/// otherwise shadow an append), and match only the unique ephemeral
/// port, so no other traffic is touched. Returns the applied rule specs
/// for exact-match deletion. A first-rule failure means no privilege
/// (caller SKIPs); a later failure is a real error (privilege proven).
fn freeze_packets(port: u16) -> Result<Vec<Vec<String>>, String> {
    let specs = vec![
        vec![
            "OUTPUT".into(),
            "-p".into(),
            "udp".into(),
            "--dport".into(),
            port.to_string(),
            "-j".into(),
            "DROP".into(),
        ],
        vec![
            "OUTPUT".into(),
            "-p".into(),
            "udp".into(),
            "--sport".into(),
            port.to_string(),
            "-j".into(),
            "DROP".into(),
        ],
    ];
    let mut applied: Vec<Vec<String>> = Vec::new();
    for spec in &specs {
        let mut args = vec!["-I".to_owned(), "1".to_owned()];
        args.extend(spec.iter().cloned());
        let ok = Command::new("iptables")
            .args(&args)
            .output()
            .is_ok_and(|o| o.status.success());
        if !ok {
            if applied.is_empty() {
                return Err("iptables rule install failed (need privilege?)".into());
            }
            // Privilege proven by the first rule: clean up, then fail loud.
            for done in applied.iter().rev() {
                let mut args = vec!["-D".to_owned()];
                args.extend(done.iter().cloned());
                let _ = Command::new("iptables").args(&args).status();
            }
            panic!("iptables: first rule applied but second failed; rules removed");
        }
        applied.push(spec.clone());
    }
    Ok(applied)
}

/// Delete blackhole rules in reverse order; true when all deletions
/// succeeded (teardown must never leak a rule).
fn thaw_packets(applied: &[Vec<String>]) -> bool {
    let mut ok = true;
    for spec in applied.iter().rev() {
        let mut args = vec!["-D".to_owned()];
        args.extend(spec.iter().cloned());
        let deleted = Command::new("iptables")
            .args(&args)
            .status()
            .is_ok_and(|s| s.success());
        if !deleted {
            eprintln!("teardown: failed to delete iptables rule {spec:?}");
            ok = false;
        }
    }
    ok
}

/// Prove the frozen member was not permanently evicted: every node holds
/// a live session to both peers and the member view is full again
/// (Go: `assertNoEviction`, via peer diagnostics + member counts).
fn assert_no_eviction(cluster: &Cluster) {
    for i in 0..cluster.len() {
        let diags = cluster
            .node(i)
            .replication()
            .expect("replication handle")
            .status()
            .peers_diagnostics;
        assert_eq!(
            diags.len(),
            2,
            "node{} sees {} peers, want 2 (a peer was evicted)",
            i + 1,
            diags.len()
        );
        for p in &diags {
            assert!(
                p.connected,
                "node{} peer {:?} not connected after resume (evicted?)",
                i + 1,
                p.node_id
            );
        }
        assert_eq!(
            member_count(cluster, i),
            2,
            "node{} members = {}, want 2 (frozen member never re-admitted)",
            i + 1,
            member_count(cluster, i)
        );
    }
}

#[test]
fn pause_resume_loses_no_writes() {
    let mut stop_secs = std::env::var(STOP_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_STOP_SECS);
    if stop_secs < MIN_STOP_SECS {
        eprintln!("stop window {stop_secs}s below the 15s floor; using 15s");
        stop_secs = MIN_STOP_SECS;
    }

    let cluster = Cluster::new(ClusterOptions {
        name: "pause-resume".into(),
        num_nodes: 3,
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
    let frozen_port = cluster.node_info(FROZEN).repl_addr.port();
    // Set once the blackhole is up; teardown always removes it.
    let mut blackhole: Vec<Vec<String>> = Vec::new();
    let mut thaw_ok = true;
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: typed fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Positive control baseline: full mesh (sessions + membership).
        wait_connected_peers(&cluster, 2, MESH_TIMEOUT);
        wait_membership(&cluster, 2, MESH_TIMEOUT);
        for i in 0..BASELINE_ROWS {
            let name = format!("base-{i}");
            assert!(
                try_insert(cluster.node(0), name),
                "baseline write failed"
            );
            cluster.node(0).flush().expect("flush baseline");
        }
        wait_converged(&cluster, BASELINE_ROWS, BASELINE_TIMEOUT);

        let acknowledged = Arc::new(Mutex::new(HashSet::from([
            "base-0".to_owned(),
            "base-1".to_owned(),
            "base-2".to_owned(),
            "base-3".to_owned(),
            "base-4".to_owned(),
        ])));
        let needs_before = needs_sent(&cluster, FROZEN);

        // Writers hammer every node continuously; node 2's writer pauses
        // while it is frozen (a frozen node accepts nothing by definition).
        let paused = [
            AtomicBool::new(false),
            AtomicBool::new(false),
            AtomicBool::new(false),
        ];
        let stop = Arc::new(AtomicBool::new(false));
        let _stop_guard = StopOnDrop(stop.clone());
        let skipped = Arc::new(AtomicBool::new(false));
        let seqs = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
        // Writer handles cloned up front: the frozen node is never
        // stopped, so no handle refresh is needed (unlike restart legs).
        let dbs: Arc<Vec<Database>> =
            Arc::new((0..3).map(|n| cluster.node(n).clone()).collect());
        std::thread::scope(|s| {
            for node in 0..3 {
                let stop = stop.clone();
                let acknowledged = acknowledged.clone();
                let paused_node = &paused[node];
                let seq = &seqs[node];
                let dbs = dbs.clone();
                s.spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(WRITE_TICK);
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        if paused_node.load(Ordering::Relaxed) {
                            continue;
                        }
                        let db = dbs[node].clone();
                        let seq = seq.fetch_add(1, Ordering::Relaxed) + 1;
                        let value = format!("flow-n{}-{seq:08}", node + 1);
                        if try_insert(&db, value.clone()) {
                            acknowledged.lock().expect("acked lock").insert(value);
                        }
                    }
                });
            }
            std::thread::sleep(PRE_ROLL);

            // Freeze node 2 mid-replication: no packets in or out.
            paused[FROZEN].store(true, Ordering::Relaxed);
            std::thread::sleep(DRAIN); // drain in-flight requests first
            let acked_at_stop = acknowledged.lock().expect("acked lock").len();
            eprintln!("freezing node2 packets with {acked_at_stop} rows acknowledged so far");
            match freeze_packets(frozen_port) {
                Ok(applied) => blackhole = applied,
                Err(reason) => {
                    stop.store(true, Ordering::Relaxed);
                    skipped.store(true, Ordering::Relaxed);
                    eprintln!("SKIP: {reason}");
                    return;
                }
            }

            // While frozen: survivors keep writing; sample the alive view
            // from the outside. Healthy reads 2 (excludes self); detection
            // of the frozen member may dip survivors to 1.
            let probes_before = [0, 2].map(|i| probe_failures(&cluster, i));
            let mut min_alive = 2;
            let sample_end = Instant::now() + Duration::from_secs(stop_secs);
            while Instant::now() < sample_end {
                for idx in [0, 2] {
                    min_alive = min_alive.min(member_count(&cluster, idx));
                }
                std::thread::sleep(SAMPLE_TICK);
            }
            let acked_during_pause =
                acknowledged.lock().expect("acked lock").len() - acked_at_stop;
            let probes_during: u64 = [0, 2]
                .iter()
                .zip(probes_before)
                .map(|(i, before)| probe_failures(&cluster, *i) - before)
                .sum();
            eprintln!(
                "during {stop_secs}s freeze: minAlivePeers={min_alive} probeFailures={probes_during} ackedDuringPause={acked_during_pause}"
            );
            // Freeze detection proof: survivors must notice the frozen
            // member, either as an alive-view dip or as failed probes.
            // Without this, broken probing would pass silently.
            assert!(
                min_alive < 2 || probes_during > 0,
                "freeze undetected: survivors kept alive={min_alive} with zero probe failures during {stop_secs}s (probing ineffective?)"
            );
            assert!(
                acked_during_pause > 0,
                "no writes acknowledged while node2 was frozen; pause proved nothing"
            );

            // Resume and prove streams come back.
            thaw_ok = thaw_packets(&blackhole);
            blackhole.clear();
            assert!(thaw_ok, "teardown leaked iptables rules");
            // Health: the node answers reads immediately (it never
            // stopped; only its packets were blackholed).
            let resume_start = Instant::now();
            let _ = names(cluster.node(FROZEN));
            eprintln!("node2 healthy {:?} after resume", resume_start.elapsed());
            // Sessions re-establish over the reopened path (explicit
            // re-mesh: the blackhole killed the QUIC sessions and there
            // is no auto-redial).
            cluster.mesh().expect("re-mesh after resume");
            paused[FROZEN].store(false, Ordering::Relaxed);

            stop.store(true, Ordering::Relaxed);
        });

        // A privilege SKIP inside the scope above stops here with
        // writers joined and no rules applied.
        if skipped.load(Ordering::Relaxed) {
            return;
        }
        let total = acknowledged.lock().expect("acked lock").len();
        // 180 s bound (Go: post-resume range-repair normally lands fast,
        // but cold page cache once needed past 120 s).
        wait_converged(&cluster, total, CONVERGE_TIMEOUT);
        assert_acknowledged_present(&cluster, &acknowledged.lock().expect("acked lock"));

        // Sessions resumed on every link; membership re-admitted the member.
        wait_connected_peers(&cluster, 2, READMIT_TIMEOUT);
        wait_membership(&cluster, 2, READMIT_TIMEOUT);
        assert_no_eviction(&cluster);

        // Record the rejoin path: with default retention the log still
        // holds everything, so catch-up must arrive as gap-pull needs
        // (a snapshot here would mean ranges vanished early).
        let needs_after = needs_sent(&cluster, FROZEN);
        assert!(
            needs_after > needs_before,
            "node2 rejoined with no gap-pull (needs {needs_before}->{needs_after}); catch-up path unproven"
        );
        eprintln!("node2 rejoin path: range-repair (needs {needs_before}->{needs_after})");

        // Post-resume honest write still replicates everywhere.
        assert!(
            try_insert(cluster.node(FROZEN), "post-resume".into()),
            "post-resume write failed"
        );
        cluster.node(FROZEN).flush().expect("flush post-resume");
        acknowledged.lock().expect("acked lock").insert("post-resume".into());
        let total = acknowledged.lock().expect("acked lock").len();
        wait_converged(&cluster, total, POST_TIMEOUT);
        assert_acknowledged_present(&cluster, &acknowledged.lock().expect("acked lock"));
        eprintln!("pause/resume proven: {stop_secs}s freeze, {total} rows intact, digests converge");
    }))
    .is_err();
    // Teardown never leaks the blackhole, on success and on failure.
    if !blackhole.is_empty() {
        thaw_ok = thaw_packets(&blackhole);
    }
    if failed {
        cluster.dump_forensics("pause-resume-no-convergence");
    }
    cluster.finish(failed);
    assert!(thaw_ok, "teardown leaked iptables rules");
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
