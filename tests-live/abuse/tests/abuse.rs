//! Rust port of the Go `abuse` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/abuse/`
//! (`abuse_test.go`: `TestAbuseMesh`, plus the `progress_test.go` reporter).
//! A live mesh is hammered through a healthy soak, a prolonged half-split
//! partition, an extended outage of a quarter of the nodes with reconnect
//! backlogs, and a simultaneous kill/restart storm of every node, with
//! continuous writers throughout. The verdict is exact cross-node
//! convergence (equal row counts plus identical PK-ordered row contents),
//! never a sample. Plain SQL replaces the Go typed-contention HTTP API (see
//! the scenario README.md for the deviations).

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use murmur_live_harness::sqlite_session::Database;
use murmur_live_harness::{env_seconds, Cluster, ClusterOptions, ReplicationOptions};

/// Minimum/maximum mesh size (Go: `min(max(nodes, 3), 256)`).
const MIN_NODES: usize = 3;
/// Maximum mesh size (Go: `min(max(nodes, 3), 256)`).
const MAX_NODES: usize = 256;
/// Writer-thread cap (Go: `min(nodes, 8)`).
const MAX_WRITERS: usize = 8;
/// Link cut/heal fan-out (Go: `runLinks` workers).
const LINK_WORKERS: usize = 32;
/// Baseline replication deadline (Go: 2 minutes).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(120);
/// Baseline poll interval (Go: 500 ms).
const BASELINE_POLL: Duration = Duration::from_millis(500);
/// Verdict convergence poll interval (Go: 2 s).
const VERDICT_POLL: Duration = Duration::from_secs(2);
/// Distinct error-message sample cap (Go: first 8).
const ERR_SAMPLE_CAP: usize = 8;
/// Stored error-message truncation (Go: 200 bytes).
const ERR_TRUNC_CHARS: usize = 200;

/// usize env knob with fallback (Go: `harness.EnvInt`).
fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(fallback)
}

/// u64 env knob with fallback (Go: `harness.EnvInt` for the seed).
fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(fallback)
}

/// Millisecond-duration env knob with fallback (Go: `MURMUR_ABUSE_WRITE_INTERVAL_MS`).
fn env_millis(name: &str, fallback_ms: u64) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(fallback_ms))
}

/// Go-style duration rendering (`90s` -> `1m30s`) for progress lines.
fn fmt_dur(d: Duration) -> String {
    let s = d.as_secs();
    if s >= 3600 {
        format!("{}h{}m{}s", s / 3600, s % 3600 / 60, s % 60)
    } else if s >= 60 {
        format!("{}m{}s", s / 60, s % 60)
    } else if s >= 1 {
        format!("{s}s")
    } else {
        format!("{}ms", d.as_millis())
    }
}

/// Current unix time in seconds (report filename stamp).
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Deterministic splitmix64 (dependency-free `math/rand` replacement): each
/// writer gets an independent stream seeded from `seed*1000 + writer`.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Live progress reporter (Go: `progress_test.go` reporter): timestamped
/// phase/log lines to stderr plus the same events for the final markdown
/// report. All methods are safe for concurrent use.
struct Reporter {
    start: Instant,
    events: Mutex<Vec<String>>,
    attempted: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    err_seen: Mutex<HashMap<String, usize>>,
    err_sample: Mutex<Vec<String>>,
}

impl Reporter {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            events: Mutex::new(Vec::new()),
            attempted: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            err_seen: Mutex::new(HashMap::new()),
            err_sample: Mutex::new(Vec::new()),
        }
    }

    fn attempted(&self) -> u64 {
        self.attempted.load(Ordering::SeqCst)
    }

    fn succeeded(&self) -> u64 {
        self.succeeded.load(Ordering::SeqCst)
    }

    fn failed(&self) -> u64 {
        self.failed.load(Ordering::SeqCst)
    }

    fn stamp(&self) -> String {
        format!("[T+{}]", fmt_dur(self.start.elapsed()))
    }

    fn emit(&self, msg: String) {
        eprintln!("{msg}");
        self.events.lock().unwrap().push(msg);
    }

    /// Announce a phase transition (Go: `reporter.phase`).
    fn phase(&self, name: &str, detail: &str) {
        self.emit(format!(
            "{} === PHASE: {name} === {detail}",
            self.stamp()
        ));
    }

    /// Record a timestamped progress line (Go: `reporter.logf`).
    fn log(&self, msg: String) {
        let stamped = format!("{} {msg}", self.stamp());
        self.emit(stamped);
    }

    fn count_attempt(&self, ok: bool) {
        self.attempted.fetch_add(1, Ordering::SeqCst);
        if ok {
            self.succeeded.fetch_add(1, Ordering::SeqCst);
        } else {
            self.failed.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Record distinct write-error messages (first 8) with counts (Go: `sampleErr`).
    fn sample_err(&self, err: &str) {
        let msg: String = err.chars().take(ERR_TRUNC_CHARS).collect();
        *self.err_seen.lock().unwrap().entry(msg.clone()).or_insert(0) += 1;
        let mut sample = self.err_sample.lock().unwrap();
        if sample.len() < ERR_SAMPLE_CAP && !sample.contains(&msg) {
            sample.push(msg);
        }
    }

    /// Log the final PASS/FAIL line and write the markdown report (Go: `verdict`).
    /// Failure artifact retention itself is handled by `Cluster::finish`.
    fn verdict(&self, pass: bool, report_path: &Path, summary: &str) {
        let status = if pass { "PASS" } else { "FAIL" };
        self.emit(format!(
            "{} === VERDICT: {status} === {summary}",
            self.stamp()
        ));
        let events = self.events.lock().unwrap().clone();
        let err_sample = self.err_sample.lock().unwrap().clone();
        let err_seen = self.err_seen.lock().unwrap().clone();
        let mut b = String::new();
        b.push_str(&format!("# Abuse run report — {status}\n\n"));
        b.push_str(&format!(
            "- finished (unix): {}\n- wall time: {}\n- writes attempted: {}\n- writes acked: {}\n- writes failed (transient): {}\n- verdict: {summary}\n\n",
            unix_now(),
            fmt_dur(self.start.elapsed()),
            self.attempted(),
            self.succeeded(),
            self.failed(),
        ));
        if !err_sample.is_empty() {
            b.push_str("## Distinct write errors\n\n");
            for s in &err_sample {
                b.push_str(&format!(
                    "- ({}x) {s}\n",
                    err_seen.get(s).copied().unwrap_or(0)
                ));
            }
            b.push('\n');
        }
        b.push_str("## Timeline\n\n");
        for e in &events {
            b.push_str(&format!("- {e}\n"));
        }
        match std::fs::write(report_path, b) {
            Ok(()) => eprintln!("report written to {}", report_path.display()),
            Err(e) => eprintln!("report write failed: {e}"),
        }
    }
}

/// Pick a random live node (Go: `pickAlive`); `None` when all are down.
fn pick_alive(alive: &[AtomicBool], rng: &mut Rng) -> Option<usize> {
    let mut up = Vec::new();
    for (i, a) in alive.iter().enumerate() {
        if a.load(Ordering::SeqCst) {
            up.push(i);
        }
    }
    if up.is_empty() {
        None
    } else {
        Some(up[rng.below(up.len())])
    }
}

/// Row count on one node, or `None` when the read fails.
fn row_count(db: &Database) -> Option<usize> {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM contention", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .ok()
}

/// Canonical PK-ordered encoded rows on one node (Go: `abuseNodeState` input:
/// `id:name:phone:score` sorted by id), or `None` when the read fails.
fn canonical_rows(db: &Database) -> Option<Vec<String>> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name, phone, score FROM contention ORDER BY id")?;
        let mapped = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            let phone: String = row.get(2)?;
            let score: i64 = row.get(3)?;
            Ok(format!("{id}:{name}:{phone}:{score}"))
        })?;
        let mut out = Vec::new();
        for row in mapped {
            out.push(row?);
        }
        Ok(out)
    })
    .ok()
}

/// Deterministic log-only digest over canonical rows (Go: sha256 hex of the
/// sorted table; comparisons use byte-identical vectors, which imply
/// identical digests). FNV-1a keeps this dependency-free.
fn digest_hex(rows: &[String]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for row in rows {
        for b in row.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= b'\n' as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

/// Row count plus PK-ordered digest and rows on one node (Go: `abuseNodeState`).
fn node_state(cluster: &Cluster, idx: usize) -> Option<(usize, String, Vec<String>)> {
    let rows = canonical_rows(cluster.node(idx))?;
    let digest = digest_hex(&rows);
    Some((rows.len(), digest, rows))
}

/// Apply one link operation to every link with a bounded worker pool so a
/// large partition heals in seconds (Go: `runLinks`).
fn run_links(cluster: &Cluster, links: &[(usize, usize)], add: bool) {
    if links.is_empty() {
        return;
    }
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..LINK_WORKERS.min(links.len()) {
            s.spawn(|| loop {
                let k = next.fetch_add(1, Ordering::SeqCst);
                if k >= links.len() {
                    break;
                }
                // Non-fatal by design (Go: `runLinks` ignores errors); the
                // convergence verdict is the judge.
                let (from, to) = links[k];
                if add {
                    let _ = cluster.add_peer(from, to);
                } else {
                    let _ = cluster.remove_peer(from, to);
                }
            });
        }
    });
}

/// Restart every listed node and rejoin the mesh (Go: `restartBehindBarrier`):
/// concurrent in-process reopens behind one barrier, then one re-mesh so
/// all catch-up overlaps like a real reconnect storm. Writer handles are
/// swapped for the reopened databases and the nodes marked alive.
fn restart_nodes(
    cluster: &mut Cluster,
    handles: &[Mutex<Database>],
    alive: &[AtomicBool],
    idx: &[usize],
    rep: &Reporter,
) {
    let begin = Instant::now();
    cluster
        .start_nodes_parallel(idx)
        .unwrap_or_else(|e| panic!("parallel restart of {idx:?} failed: {e}"));
    rep.log(format!(
        "{} nodes ready {:?} after storm restart (barrier)",
        idx.len(),
        begin.elapsed()
    ));
    // Reopened nodes rebind fresh ephemeral ports: re-dial every ordered
    // pair of running nodes (Go: per-node `StartNode` re-dials).
    cluster.mesh().expect("re-mesh after restart");
    for &i in idx {
        *handles[i].lock().unwrap() = cluster.node(i).clone();
        alive[i].store(true, Ordering::SeqCst);
    }
}

/// Poll until every node reports `want` rows or the timeout elapses
/// (Go: `waitAllCount`).
fn wait_all_count(cluster: &Cluster, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut ok = true;
        for i in 0..cluster.len() {
            match row_count(cluster.node(i)) {
                Some(n) if n == want => {}
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "row count did not reach {want} on all {} nodes within {timeout:?}",
                cluster.len()
            ));
        }
        std::thread::sleep(BASELINE_POLL);
    }
}

/// Poll until every node is reachable and all PK-ordered row contents match
/// (Go: `waitConverged`: equal counts plus identical digests). Returns the
/// last observed per-node counts and digests alongside the verdict.
fn wait_converged(
    cluster: &Cluster,
    nodes: usize,
    timeout: Duration,
) -> (Vec<i64>, Vec<String>, Result<(), String>) {
    let deadline = Instant::now() + timeout;
    let start = Instant::now();
    let mut last_sample = Instant::now() - Duration::from_secs(3600);
    loop {
        let mut counts = vec![0i64; nodes];
        let mut digests = vec![String::new(); nodes];
        let mut first_rows: Option<Vec<String>> = None;
        let mut agree = 0usize;
        let mut ok = true;
        for i in 0..nodes {
            match node_state(cluster, i) {
                Some((n, d, rows)) => {
                    counts[i] = n as i64;
                    digests[i] = d;
                    match &first_rows {
                        None => {
                            agree = 1;
                            first_rows = Some(rows);
                        }
                        Some(first) => {
                            if *first != rows {
                                ok = false;
                            } else {
                                agree += 1;
                            }
                        }
                    }
                }
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            return (counts, digests, Ok(()));
        }
        if Instant::now() >= deadline {
            break;
        }
        if last_sample.elapsed() >= Duration::from_secs(15) {
            last_sample = Instant::now();
            let lo = counts.iter().min().copied().unwrap_or(-1);
            let hi = counts.iter().max().copied().unwrap_or(-1);
            eprintln!(
                "[verdict-sample T+{}s] counts={counts:?} spread={} agree={agree}/{nodes}",
                start.elapsed().as_secs(),
                hi - lo,
            );
            if std::env::var("MURMUR_VERDICT_METRICS").is_ok() {
                let mut m = Vec::new();
                for i in 0..nodes {
                    let s = cluster.node(i).status();
                    match s.replication {
                        Some(r) => m.push(format!(
                            "{i}:p{}|st{}|push{}|rcv{}|app{}|ap{}|ae{}|gos{}",
                            r.peers,
                            r.staged_transactions,
                            r.metrics.pushed_transactions,
                            r.metrics.received_transactions,
                            r.metrics.applied_transactions,
                            r.applied_transactions,
                            r.metrics.anti_entropy_runs,
                            r.metrics.gossip_rounds,
                        )),
                        None => m.push(format!("{i}:none")),
                    }
                }
                eprintln!("[verdict-metrics] {}", m.join(" "));
            }
        }
        std::thread::sleep(VERDICT_POLL);
    }
    // Final best-effort re-sample for the failure report (Go parity).
    let mut counts = vec![0i64; nodes];
    let mut digests = vec![String::new(); nodes];
    for i in 0..nodes {
        match node_state(cluster, i) {
            Some((n, d, _)) => {
                counts[i] = n as i64;
                digests[i] = d;
            }
            None => {
                counts[i] = -1;
                digests[i] = "unreachable".to_string();
            }
        }
    }
    (
        counts,
        digests,
        Err(format!("nodes did not converge within {timeout:?}")),
    )
}

#[test]
fn abuse_mesh() {
    let nodes = env_usize("MURMUR_ABUSE_NODES", 10).clamp(MIN_NODES, MAX_NODES);
    let duration = env_seconds("MURMUR_ABUSE_DURATION_SECONDS", 600);
    // Quiet convergence window after writers stop. Calibrated with the
    // write interval below: the 10-node colocated mesh repairs ~47 tx/s,
    // so the offered load must stay near that envelope or the chaos
    // backlog grows faster than any reasonable window can drain
    // (parallel trajectories never close). Single-node production
    // deployments (dedicated disk/CPU) converge far faster; these bounds
    // cover the colocated test worst case.
    let settle = env_seconds("MURMUR_ABUSE_SETTLE_SECONDS", 900);
    // 100ms (Go: 50ms) across 8 writers: keeps multi-writer contention
    // while offering ~80 tx/s (throttled lower) instead of ~160/s, so the
    // outage-gap backlog fits the verdict window at ~47 tx/s repair.
    let write_interval = env_millis("MURMUR_ABUSE_WRITE_INTERVAL_MS", 100);
    let seed = env_u64("MURMUR_ABUSE_SEED", 1);
    let status_interval = env_seconds("MURMUR_ABUSE_STATUS_SECONDS", 15);

    // Fanout 2 (product default 4): ten colocated nodes on a small box
    // saturate CPU on 4-way gossip duplicates; two push targets halve the
    // duplicate stage/verify work while multi-hop diffusion still covers
    // the mesh in seconds, far inside the verdict window.
    // Full-mesh snapshot trust: outage/partition victims face 10k+
    // transaction gaps that grind for many minutes per-tx; bulk
    // snapshot catch-up (receiver-side lag trigger) closes them in about
    // a minute. All members share one CA, so trusting the mesh is sound.
    let node_ids: Vec<[u8; 16]> = (0..nodes).map(|i| [(i + 1) as u8; 16]).collect();
    let mut trust: HashMap<usize, BTreeSet<[u8; 16]>> = HashMap::new();
    for i in 0..nodes {
        trust.insert(
            i,
            node_ids
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, id)| *id)
                .collect(),
        );
    }
    let mut cluster = Cluster::new(ClusterOptions {
        name: "abuse".into(),
        num_nodes: nodes,
        node_ids,
        trusted_snapshot_sources: trust,
        replication: Some(ReplicationOptions {
            fanout: Some(2),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "Node {} running in directory {} (repl={})",
            info.label,
            info.dir.display(),
            info.repl_addr,
        );
    }
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let rep = Arc::new(Reporter::new());
        rep.log(format!(
            "config: nodes={nodes} duration={} settle={} writeInterval={} seed={seed} statusEvery={}",
            fmt_dur(duration),
            fmt_dur(settle),
            fmt_dur(write_interval),
            fmt_dur(status_interval),
        ));

        // Same schema on every node (Go: typed-contention fixture table).
        for i in 0..cluster.len() {
            cluster
                .ensure_migrated(
                    i,
                    "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL, score INTEGER NOT NULL)",
                )
                .unwrap_or_else(|e| panic!("setup migrate node{}: {e}", i + 1));
        }

        // Baseline: one row on node 0, present everywhere before abuse starts.
        cluster
            .node(0)
            .transaction(|sql| {
                sql.execute(
                    "INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
                    (format!("{:032x}", 0), "baseline", "", 0i64),
                )?;
                Ok(())
            })
            .expect("baseline insert");
        cluster.node(0).flush().expect("flush baseline");
        wait_all_count(&cluster, 1, BASELINE_TIMEOUT).expect("baseline replication");
        rep.log(format!("baseline row converged on all {nodes} nodes"));

        // Writer threads share a handle table with the main thread: the
        // entry for a restarted node is swapped for the reopened database
        // (Go: writers address daemons by stable address across restarts),
        // and an alive-set steers targeting away from stopped nodes (Go:
        // `alive` + `pickAlive`). Every attempt mints a fresh id, so a
        // retry after a transient failure can never collide on the
        // primary key. Transient errors are counted, never fatal.
        let handles: Arc<Vec<Mutex<Database>>> = Arc::new(
            (0..nodes)
                .map(|i| Mutex::new(cluster.node(i).clone()))
                .collect(),
        );
        let alive: Arc<Vec<AtomicBool>> = Arc::new(
            (0..nodes).map(|_| AtomicBool::new(true)).collect(),
        );
        let stop_writers = Arc::new(AtomicBool::new(false));
        let stop_status = Arc::new(AtomicBool::new(false));

        let mut workers = Vec::with_capacity(nodes.min(MAX_WRITERS));
        for w in 0..nodes.min(MAX_WRITERS) {
            let handles = handles.clone();
            let alive = alive.clone();
            let stop = stop_writers.clone();
            let rep = rep.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("abuse-writer-{w}"))
                    .spawn(move || {
                        let mut rng =
                            Rng::new(seed.wrapping_mul(1000).wrapping_add(w as u64));
                        let mut seq: u64 = 0;
                        loop {
                            if stop.load(Ordering::SeqCst) {
                                return;
                            }
                            let target = match pick_alive(&alive, &mut rng) {
                                Some(t) => t,
                                None => {
                                    std::thread::sleep(write_interval);
                                    continue;
                                }
                            };
                            seq += 1;
                            // 32 lowercase hex chars: unique per
                            // writer/attempt (Go: `%08x%024x`).
                            let id = format!("{w:08x}{seq:024x}");
                            let name = format!("writer-{w}");
                            let db = handles[target].lock().unwrap().clone();
                            let outcome = db
                                .transaction(move |sql| {
                                    sql.execute(
                                        "INSERT INTO contention(id, name, phone, score) VALUES(?, ?, ?, ?)",
                                        (id, name, "", 0i64),
                                    )?;
                                    Ok(())
                                })
                                .and_then(|()| db.flush());
                            match outcome {
                                Ok(()) => rep.count_attempt(true),
                                Err(e) => {
                                    rep.count_attempt(false);
                                    rep.sample_err(&e.to_string());
                                }
                            }
                            if stop.load(Ordering::SeqCst) {
                                return;
                            }
                            std::thread::sleep(write_interval);
                        }
                    })
                    .expect("spawn writer thread"),
            );
        }

        // Heartbeat thread (Go: `reporter.statusLoop`): elapsed time, write
        // totals, min/max row counts, unreachable nodes.
        let status_handle = {
            let handles = handles.clone();
            let stop = stop_status.clone();
            let rep = rep.clone();
            std::thread::Builder::new()
                .name("abuse-status".into())
                .spawn(move || loop {
                    std::thread::sleep(status_interval);
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    let mut min = -1i64;
                    let mut max = -1i64;
                    let mut down = 0usize;
                    for h in handles.iter() {
                        let db = h.lock().unwrap().clone();
                        match row_count(&db) {
                            Some(c) => {
                                let c = c as i64;
                                if min < 0 || c < min {
                                    min = c;
                                }
                                if c > max {
                                    max = c;
                                }
                            }
                            None => down += 1,
                        }
                    }
                    rep.log(format!(
                        "status: writes attempted={} ok={} err={} | rows min={min} max={max} unreachable={down}/{nodes}",
                        rep.attempted(),
                        rep.succeeded(),
                        rep.failed(),
                    ));
                })
                .expect("spawn status thread")
        };

        let quarter = duration / 4;

        // Phase 1: healthy-mesh soak. Writers run, nothing breaks.
        rep.phase(
            "mesh-soak",
            &format!("healthy full mesh for {}", fmt_dur(quarter)),
        );
        std::thread::sleep(quarter);

        // Phase 2: prolonged partition. The mesh splits into halves with no
        // cross-links; both sides keep accepting writes (divergence is the
        // point). Healed at the end of the phase.
        rep.phase(
            "partition",
            &format!(
                "splitting {nodes} nodes into halves for {}",
                fmt_dur(quarter)
            ),
        );
        let half = nodes / 2;
        let mut links = Vec::new();
        for a in 0..half {
            for b in half..nodes {
                links.push((a, b));
                links.push((b, a));
            }
        }
        let cut = links.len();
        run_links(&cluster, &links, false);
        rep.log(format!(
            "partition cut: {cut} cross-links removed ({} nodes isolated)",
            nodes - half
        ));
        std::thread::sleep(quarter);
        run_links(&cluster, &links, true);
        rep.log(format!("partition healed: {cut} cross-links restored"));

        // Phase 3: extended outage. A quarter of the nodes go down while
        // writers hammer the survivors, building the reconnect backlog.
        // All victims restart together at the end of the phase. Go
        // alternates SIGKILL and graceful stops; in-process both are
        // close()+reopen (see README.md).
        let victims = (nodes / 4).max(1);
        rep.phase(
            "outage",
            &format!(
                "stopping {victims} of {nodes} nodes for {}",
                fmt_dur(quarter)
            ),
        );
        let victim_idx: Vec<usize> = ((nodes - victims)..nodes).collect();
        for &idx in &victim_idx {
            alive[idx].store(false, Ordering::SeqCst);
            cluster.stop_node(idx);
        }
        rep.log(format!(
            "outage: {victims} nodes down, writers continue on {} survivors",
            nodes - victims
        ));
        std::thread::sleep(quarter);
        rep.log(format!(
            "outage over: restarting {victims} victims behind one barrier"
        ));
        restart_nodes(&mut cluster, &handles, &alive, &victim_idx, &rep);

        // Phase 4: reconnect storm. Every node is stopped and restarted
        // while writers keep firing into the churn.
        rep.phase(
            "storm",
            &format!(
                "killing and restarting all {nodes} nodes, then soaking {}",
                fmt_dur(quarter)
            ),
        );
        let all: Vec<usize> = (0..nodes).collect();
        for &idx in &all {
            alive[idx].store(false, Ordering::SeqCst);
        }
        for &idx in &all {
            cluster.stop_node(idx);
        }
        rep.log(format!(
            "storm: all {nodes} nodes killed, restarting behind one barrier"
        ));
        restart_nodes(&mut cluster, &handles, &alive, &all, &rep);
        std::thread::sleep(quarter);

        // Writers stop; the mesh gets `settle` to converge, then verdict.
        stop_writers.store(true, Ordering::SeqCst);
        for (w, worker) in workers.into_iter().enumerate() {
            worker
                .join()
                .unwrap_or_else(|_| panic!("writer thread {w} panicked"));
        }
        stop_status.store(true, Ordering::SeqCst);
        status_handle
            .join()
            .unwrap_or_else(|_| panic!("status thread panicked"));
        rep.log(format!(
            "writers stopped: attempted={} acked={} transient-errors={}",
            rep.attempted(),
            rep.succeeded(),
            rep.failed(),
        ));

        // Anti-vacuous guard: if no write was ever acked (not even during
        // the healthy soak), the abuse proved nothing and the run must fail.
        let report_path =
            cluster
                .runtime_dir()
                .join(format!("abuse-report-{}.md", unix_now()));
        if rep.succeeded() == 0 {
            rep.verdict(
                false,
                &report_path,
                "no writes acked during the entire run",
            );
            panic!(
                "abuse verdict: 0 of {} writes acked; see report for distinct errors",
                rep.attempted()
            );
        }

        rep.phase(
            "verdict",
            &format!("waiting up to {} for exact convergence", fmt_dur(settle)),
        );
        let (counts, digests, converged) = wait_converged(&cluster, nodes, settle);
        if let Err(e) = converged {
            rep.verdict(false, &report_path, &e);
            panic!("abuse verdict: {e}\ncounts={counts:?}\ndigests={digests:?}");
        }
        rep.verdict(
            true,
            &report_path,
            &format!(
                "all {nodes} nodes hold {} rows with identical digest {}",
                counts[0], digests[0]
            ),
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("abuse-no-converge");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
