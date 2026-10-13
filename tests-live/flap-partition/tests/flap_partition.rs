//! Rust port of the Go `flap-partition` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/flap-partition/`
//! (`flap_partition_test.go`). Three fully meshed nodes take continuous
//! writes from all nodes while node3's links flap through split/heal
//! cycles. The first split carries an isolation proof (a marker written on
//! node1 must reach node2 but never node3); no write to a live node may
//! fail, and the final heal must converge every written row with equal
//! contents. Plain SQL replaces the Go typed-records HTTP API (see the
//! scenario README.md).

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{env_seconds, Cluster, ClusterOptions};

/// Convergence poll interval (Go: 200 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Per-writer insert cadence (Go: 50 ms ticker).
const WRITE_INTERVAL: Duration = Duration::from_millis(50);
/// Isolation-probe poll interval (Go: 50 ms).
const ISOLATION_POLL: Duration = Duration::from_millis(50);
/// Baseline / final convergence deadlines (Go: 150 s each).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(150);
/// Isolation marker propagation deadline (Go: 10 s).
const ISOLATION_TIMEOUT: Duration = Duration::from_secs(10);
/// Post-proof leak watch window (Go: 1 s).
const LEAK_WINDOW: Duration = Duration::from_secs(1);
/// Progress-log cadence inside convergence waits (Go: 10 s).
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);
/// Baseline row count written on node 0 before flapping (Go: 6).
const BASELINE_ROWS: i64 = 6;

/// Row-id stride per writer node: ids are `node * ID_STRIDE + seq`, so the
/// three concurrent writers never collide (Go: server-generated typed IDs).
const ID_STRIDE: i64 = 10_000_000;
/// Baseline ids live above every writer range.
const BASELINE_ID_BASE: i64 = 90_000_001;
/// Isolation marker id, outside baseline and writer ranges.
const ISOLATION_MARKER_ID: i64 = 90_001_001;
/// Isolation marker name (Go: `isolation-marker`).
const ISOLATION_MARKER: &str = "isolation-marker";

/// All record names on one node (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM records")?;
        let mapped = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in mapped {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read record names")
}

/// Ordered name-set equality (Go: `sameNames`; inputs must be sorted).
fn same_names(a: &[String], b: &[String]) -> bool {
    a == b
}

/// Insert one uniquely-named row (Go: `Cluster.TypedInsert`).
fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    let value = name.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            sql.execute("INSERT INTO records(id, name) VALUES(?, ?)", (id, value))?;
            Ok(())
        })
        .map_err(|e| format!("node{} insert '{name}' failed: {e}", node + 1))?;
    cluster
        .node(node)
        .flush()
        .map_err(|e| format!("node{} flush after '{name}' failed: {e}", node + 1))?;
    Ok(())
}

/// Whether one node currently holds `name` (Go: `hasName`).
fn has_name(cluster: &Cluster, idx: usize, name: &str) -> bool {
    names(cluster.node(idx)).iter().any(|got| got == name)
}

/// Isolate (true) or rejoin (false) node index 2 (Go: `setPartition`).
fn set_partition(cluster: &Cluster, isolate: bool) {
    for peer in [0, 1] {
        if isolate {
            cluster.remove_peer(2, peer).unwrap_or_else(|e| {
                panic!("partition link 2<->{} (isolate=true): {e}", peer + 1)
            });
            cluster.remove_peer(peer, 2).unwrap_or_else(|e| {
                panic!("partition link {}<->2 (isolate=true): {e}", peer + 1)
            });
        } else {
            cluster
                .add_peer(2, peer)
                .unwrap_or_else(|e| panic!("partition link 2<->{} (heal): {e}", peer + 1));
            cluster
                .add_peer(peer, 2)
                .unwrap_or_else(|e| panic!("partition link {}<->2 (heal): {e}", peer + 1));
        }
    }
}

/// Write a marker row on node 0 while node 2 is split off and prove the
/// split is real: node 1 must observe it, node 2 must never observe it
/// during the whole window (Go: `proveIsolated`). The marker counts in
/// `written` so the final want stays exact.
fn prove_isolated(cluster: &Cluster, written: &AtomicI64) {
    insert(cluster, 0, ISOLATION_MARKER_ID, ISOLATION_MARKER)
        .expect("isolation marker write");
    written.fetch_add(1, Ordering::SeqCst);
    // Node 1 must see the marker: proves cross-node traffic flows on the
    // intact side while we watch node 2 for leaks.
    let deadline = std::time::Instant::now() + ISOLATION_TIMEOUT;
    loop {
        if has_name(cluster, 1, ISOLATION_MARKER) {
            break;
        }
        if has_name(cluster, 2, ISOLATION_MARKER) {
            panic!("isolation violated: node3 observed the marker while split (remove_peer ineffective?)");
        }
        if std::time::Instant::now() >= deadline {
            panic!("node2 never observed the isolation marker within {ISOLATION_TIMEOUT:?}");
        }
        std::thread::sleep(ISOLATION_POLL);
    }
    // Node 1 has it; give a leak one more second to show on node 2.
    std::thread::sleep(LEAK_WINDOW);
    if has_name(cluster, 2, ISOLATION_MARKER) {
        panic!("isolation violated: node3 observed the marker while split (remove_peer ineffective?)");
    }
    eprintln!("isolation proven: marker reached node2, never node3");
}

/// Wait until every node holds exactly `want` rows with equal ordered
/// contents (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_log = std::time::Instant::now();
    loop {
        let mut ok = true;
        let mut first: Vec<String> = Vec::new();
        for i in 0..cluster.len() {
            let mut node_names = names(cluster.node(i));
            if node_names.len() != want {
                ok = false;
                break;
            }
            node_names.sort();
            if i == 0 {
                first = node_names;
            } else if !same_names(&node_names, &first) {
                ok = false;
                break;
            }
        }
        if ok {
            return;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        if last_log.elapsed() > PROGRESS_LOG_INTERVAL {
            last_log = std::time::Instant::now();
            let counts: Vec<usize> =
                (0..cluster.len()).map(|i| names(cluster.node(i)).len()).collect();
            eprintln!("converge progress: counts={counts:?} want={want}");
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    for i in 0..cluster.len() {
        eprintln!(
            "node{} at timeout: count={}",
            i + 1,
            names(cluster.node(i)).len()
        );
    }
    panic!("nodes did not converge on {want} rows with equal contents within {timeout:?}");
}

/// Positive-integer env knob with fallback (Go: `envInt`).
fn env_int(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

/// Stops the writer threads when a flap-cycle assertion unwinds, so a
/// failed proof cannot deadlock the thread scope waiting for writers that
/// never observe the stop flag (idempotent on the success path).
struct StopOnDrop<'a> {
    stop: &'a AtomicBool,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

#[test]
fn flapping_partition_loses_no_writes() {
    let cycles = env_int("MURMUR_FLAP_CYCLES", 5) as usize;
    let split = env_seconds("MURMUR_FLAP_SPLIT_SECONDS", 3);
    let heal_gap = env_seconds("MURMUR_FLAP_HEAL_SECONDS", 2);

    let cluster = Cluster::new(ClusterOptions {
        name: "flap-partition".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Share explicit `&` bindings: the writer threads below are `move`
        // closures, which would otherwise take the owned values.
        let cluster = &cluster;
        for i in 0..cluster.len() {
            let info = cluster.node_info(i);
            eprintln!(
                "{} dir={} repl={}",
                info.label,
                info.dir.display(),
                info.repl_addr
            );
        }
        // Same schema on every node (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Baseline converges before flapping (honest-path control).
        for i in 0..BASELINE_ROWS {
            insert(cluster, 0, BASELINE_ID_BASE + i, &format!("base-{i}"))
                .expect("baseline write");
        }
        wait_converged(cluster, BASELINE_ROWS as usize, CONVERGE_TIMEOUT);

        // Continuous writers on every node; node3 stays live (running,
        // only unreachable), so its local commits must succeed too.
        let written = AtomicI64::new(0);
        let failed = AtomicI64::new(0);
        let stop = AtomicBool::new(false);
        let written = &written;
        let failed = &failed;
        let stop = &stop;

        std::thread::scope(|s| {
            for node in 0..cluster.len() {
                s.spawn(move || {
                    let mut seq: i64 = 0;
                    while !stop.load(Ordering::SeqCst) {
                        std::thread::sleep(WRITE_INTERVAL);
                        if stop.load(Ordering::SeqCst) {
                            break;
                        }
                        seq += 1;
                        let name = format!("n{node}-s{seq}");
                        match insert(cluster, node, node as i64 * ID_STRIDE + seq, &name) {
                            Ok(()) => {
                                written.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(_) => {
                                failed.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                });
            }
            // Unwind guard: a failed phase assertion must still release the
            // writers so the scope can join them.
            let _stop_guard = StopOnDrop { stop };

            // Flap node3's partition: split from both peers, hold, heal, settle.
            for c in 0..cycles {
                set_partition(cluster, true);
                eprintln!("cycle {}/{}: node3 isolated for {split:?}", c + 1, cycles);
                if c == 0 {
                    // Isolation proof (F1): while split, a marker written on
                    // node0 must reach node1 but never node2. Without this,
                    // a no-op remove_peer would pass vacuously. The marker is
                    // counted in written so the final want stays exact.
                    prove_isolated(cluster, written);
                }
                std::thread::sleep(split);
                set_partition(cluster, false);
                eprintln!("cycle {}/{}: healed, settling {heal_gap:?}", c + 1, cycles);
                std::thread::sleep(heal_gap);
            }
            stop.store(true, Ordering::SeqCst);
        });

        // Final heal (idempotent) then full convergence on everything written.
        set_partition(cluster, false);
        let want = (BASELINE_ROWS + written.load(Ordering::SeqCst)) as usize;
        eprintln!(
            "flapping done: {cycles} cycles, {want} rows written, {} failed",
            failed.load(Ordering::SeqCst)
        );
        assert!(
            failed.load(Ordering::SeqCst) == 0,
            "partition failed {} writes to live nodes, want 0",
            failed.load(Ordering::SeqCst)
        );
        wait_converged(cluster, want, CONVERGE_TIMEOUT);
        eprintln!("post-flap convergence: {want} rows, identical digests");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("flap-partition-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
