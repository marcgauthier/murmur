//! Rust port of the Go `txchunk-resume` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/txchunk-resume/`
//! (`txchunk_resume_test.go`). A single transaction larger than the
//! replication frame travels as ~64 KiB transaction chunks; the receiving
//! node is stopped mid-transfer and restarted, and the retry must complete
//! with all-or-nothing visibility, the applied watermark must never advance
//! before completion, and all digests must converge without any snapshot
//! transfer. Plain SQL replaces the Go typed-records HTTP API, close/reopen
//! replaces SIGKILL, and push-start replaces the inbound-byte kill trigger
//! (see the scenario README.md for the deviations).

use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{Cluster, ClusterOptions};

/// Role indices (Go: `sender`/`victim`/`witness`).
const SENDER: usize = 0;
const VICTIM: usize = 1;
const WITNESS: usize = 2;

/// Positive-control convergence deadline (Go: 120 s).
const POSITIVE_TIMEOUT: Duration = Duration::from_secs(120);
/// Baseline ack-settle deadline (Go: `waitHave` 15 s).
const HAVE_TIMEOUT: Duration = Duration::from_secs(15);
/// Ack-settle poll interval.
const HAVE_POLL: Duration = Duration::from_millis(200);
/// Ack value must hold this long to count as settled (Rust-only; Go reads an
/// absolute `local_seq`, which has no same-numbering Rust equivalent).
const HAVE_STABLE_WINDOW: Duration = Duration::from_secs(2);
/// Partition deadline on the victim (Go: 15 s).
const SPLIT_PEERS_TIMEOUT: Duration = Duration::from_secs(15);
/// Survivor trial-visibility deadline (Go: 60 s).
const SURVIVOR_TIMEOUT: Duration = Duration::from_secs(60);
/// Prefix-count poll interval (Go: 100 ms).
const PREFIX_POLL: Duration = Duration::from_millis(100);
/// Mid-transfer stop deadline (Go: 15 s byte-watch).
const KILL_TIMEOUT: Duration = Duration::from_secs(15);
/// Mid-transfer stop poll interval (Go: 5 ms).
const KILL_POLL: Duration = Duration::from_millis(5);
/// Post-restart mesh deadline (Go: `WaitNodeReady`; 30 s matches the
/// established Rust heal leg).
const HEAL_PEERS_TIMEOUT: Duration = Duration::from_secs(30);
/// Retry-completion deadline (Go: 120 s).
const RETRY_TIMEOUT: Duration = Duration::from_secs(120);
/// Retry-completion poll interval (Go: 25 ms).
const RETRY_POLL: Duration = Duration::from_millis(25);
/// Watermark-advance publication bound (Go: 10 s).
const PUBLICATION_BOUND: Duration = Duration::from_secs(10);
/// Reconvergence deadline after a too-slow/too-late attempt (Go: 60 s).
const ATTEMPT_CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
/// Final convergence deadline (Go: 60 s).
const FINAL_TIMEOUT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// Topology poll interval (Go: 100 ms).
const TOPO_POLL: Duration = Duration::from_millis(100);

/// One replicated row: Go's `TypedContentionRow{ID, Name, Phone}`.
type BigRow = (String, String, String);

/// All rows on one node (Go: `Cluster.TypedContentionRows`).
fn rows(db: &Database) -> Vec<BigRow> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name, phone FROM records")?;
        let mapped = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in mapped {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read rows")
}

/// Canonical digest of a row set (Go: ordered `TypedContentionDigest`).
fn digest(mut rows: Vec<BigRow>) -> String {
    rows.sort();
    format!("{rows:?}")
}

/// Row count on one node (cheap pre-check before full digest reads).
fn row_count(db: &Database) -> usize {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM records", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .expect("row count")
}

/// Rows on one node whose name starts with `prefix`
/// (Go: `TypedContentionPrefixCount`).
fn prefix_count(cluster: &Cluster, idx: usize, prefix: &str) -> usize {
    let prefix = prefix.to_string();
    cluster
        .node(idx)
        .read(move |sql| {
            let n = sql.query_row(
                "SELECT COUNT(*) FROM records WHERE substr(name, 1, ?1) = ?2",
                (prefix.len() as i64, prefix),
            |row| row.get::<_, i64>(0))?;
            Ok(n as usize)
        })
        .unwrap_or_else(|e| panic!("node {idx} prefix count: {e}"))
}

/// Wait until one node holds exactly `want` prefixed rows
/// (Go: `waitPrefixCount`).
fn wait_prefix_count(
    cluster: &Cluster,
    idx: usize,
    prefix: &str,
    want: usize,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        if prefix_count(cluster, idx, prefix) == want {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "node {idx} {prefix:?} rows = {}, want {want} within {timeout:?}",
                prefix_count(cluster, idx, prefix),
            );
        }
        std::thread::sleep(PREFIX_POLL);
    }
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`). Counts are checked first so the multi-megabyte
/// digest reads only run once the counts already match (same pass
/// criteria, cheaper polling).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration, what: &str) {
    let deadline = Instant::now() + timeout;
    loop {
        let counts: Vec<usize> = (0..cluster.len())
            .map(|i| row_count(cluster.node(i)))
            .collect();
        if counts.iter().all(|&n| n == want) {
            let mut first = String::new();
            let mut equal = true;
            for (i, idx) in (0..cluster.len()).enumerate() {
                let d = digest(rows(cluster.node(idx)));
                if i == 0 {
                    first = d;
                } else if d != first {
                    equal = false;
                    break;
                }
            }
            if equal {
                return;
            }
        }
        if Instant::now() >= deadline {
            for i in 0..cluster.len() {
                let node_rows = rows(cluster.node(i));
                eprintln!(
                    "node {i} at timeout: count={} digest={}",
                    node_rows.len(),
                    digest(node_rows)
                );
            }
            panic!(
                "nodes did not converge on {want} rows with equal digests within {timeout:?} ({what})"
            );
        }
        std::thread::sleep(CONVERGE_POLL);
    }
}

/// Wait until one node reports exactly `want` replication peers
/// (Go: `waitConnectedPeers` over `/v1/status connected_peers`).
fn wait_connected_peers(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let peers = |cluster: &Cluster| {
        cluster
            .node(idx)
            .replication()
            .expect("replication handle")
            .status()
            .peers
    };
    loop {
        if peers(cluster) == want {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "node {idx} connected peers = {}, want {want} within {timeout:?}",
                peers(cluster),
            );
        }
        std::thread::sleep(TOPO_POLL);
    }
}

/// The sender's record of a peer's applied watermark for one origin
/// (Go: `peerHave` over `/v1/debug/peers Have`), or 0 when no record
/// exists yet.
fn peer_have(cluster: &Cluster, sender: usize, peer: [u8; 16], origin: [u8; 16]) -> u64 {
    let status = cluster
        .node(sender)
        .replication()
        .expect("replication handle")
        .retention_status()
        .expect("retention status");
    status
        .peers
        .iter()
        .find(|p| p.node == peer)
        .and_then(|p| p.acknowledged.get(&origin).copied())
        .unwrap_or(0)
}

/// Quiet-baseline ack for one origin: the mesh is converged at every call
/// site, so the current ack settles at the sender's head within
/// milliseconds; require it to hold still across `HAVE_STABLE_WINDOW`
/// before treating it as the absolute baseline (Go compares against an
/// absolute `local_seq`, which has no same-numbering Rust equivalent).
fn wait_have_settled(
    cluster: &Cluster,
    sender: usize,
    peer: [u8; 16],
    origin: [u8; 16],
) -> u64 {
    let deadline = Instant::now() + HAVE_TIMEOUT;
    let mut current = peer_have(cluster, sender, peer, origin);
    let mut stable_since = Instant::now();
    loop {
        if Instant::now().duration_since(stable_since) >= HAVE_STABLE_WINDOW {
            return current;
        }
        if Instant::now() >= deadline {
            panic!(
                "sender view of peer watermark for origin did not settle within {HAVE_TIMEOUT:?}"
            );
        }
        std::thread::sleep(HAVE_POLL);
        let next = peer_have(cluster, sender, peer, origin);
        if next != current {
            current = next;
            stable_since = Instant::now();
        }
    }
}

/// Transactions pushed by one node (transfer-start signal; Go watches the
/// victim's inbound frame bytes, which have no Rust equivalent).
fn pushed(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics
        .pushed_transactions
}

/// Log-path activity counters for one node: `(received_transactions,
/// applied_transactions)`. Snapshot application bypasses both counters,
/// so post-restart advancement proves the trial arrived and applied as a
/// log transaction, not as a snapshot.
fn log_counters(cluster: &Cluster, idx: usize) -> (u64, u64) {
    let metrics = cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics;
    (metrics.received_transactions, metrics.applied_transactions)
}

/// Local commit cursor (Go: `nodeStatus LocalSeq` advance check).
fn committed(cluster: &Cluster, idx: usize) -> u64 {
    cluster.node(idx).status().committed
}

/// Commit one multi-megabyte transaction: `rows` inserts with large `phone`
/// values (Go: `buildBigRows` + `TypedContentionInsertMany`). Large `phone`
/// values carry the payload; `name` carries the prefix. Fails loudly
/// unless the payload exceeds the 8 MiB replication frame (Go parity; the
/// Rust engine chunks transactions at 1 MiB, so this trips chunking with
/// wide margin).
fn commit_big_tx(
    cluster: &Cluster,
    node: usize,
    prefix: &str,
    id_base: i64,
    rows: usize,
    value_bytes: usize,
) {
    let mut values: Vec<BigRow> = Vec::with_capacity(rows);
    let mut payload_bytes = 0usize;
    for i in 0..rows {
        let mut row = vec![0u8; value_bytes];
        row[..8].copy_from_slice(b"payload-");
        for (j, b) in row.iter_mut().enumerate().skip(8) {
            *b = b'a' + ((id_base + i as i64 + j as i64) % 26) as u8;
        }
        let phone = String::from_utf8(row).expect("ascii payload");
        let id = format!("{:032x}", id_base + i as i64);
        let name = format!("{prefix}{i:06}");
        payload_bytes += id.len() + name.len() + phone.len() + 8;
        values.push((id, name, phone));
    }
    let payload_mb = payload_bytes as f64 / (1 << 20) as f64;
    eprintln!("big transaction {payload_mb:.1} MiB ({rows} rows x {value_bytes} B values)");
    assert!(
        payload_bytes > 8 << 20,
        "big transaction is {payload_bytes} bytes, want > 8 MiB so it must travel chunked"
    );
    cluster
        .node(node)
        .transaction(move |sql| {
            for (id, name, phone) in &values {
                sql.execute(
                    "INSERT INTO records(id, name, phone) VALUES(?, ?, ?)",
                    (id, name, phone),
                )?;
            }
            Ok(())
        })
        .unwrap_or_else(|e| panic!("node {node} big-tx commit ({prefix}): {e}"));
    cluster.node(node).flush().expect("flush after big-tx");
}

/// Isolate `victim` from both peers in both directions (Go: `partition`).
fn partition(cluster: &Cluster, victim: usize) {
    for peer in 0..cluster.len() {
        if peer == victim {
            continue;
        }
        cluster
            .remove_peer(victim, peer)
            .unwrap_or_else(|e| panic!("remove peer {victim}->{peer}: {e}"));
        cluster
            .remove_peer(peer, victim)
            .unwrap_or_else(|e| panic!("remove peer {peer}->{victim}: {e}"));
    }
}

/// Rejoin `victim` with both peers in both directions (Go: `heal`).
fn heal(cluster: &Cluster, victim: usize) {
    for peer in 0..cluster.len() {
        if peer == victim {
            continue;
        }
        cluster
            .add_peer(victim, peer)
            .unwrap_or_else(|e| panic!("add peer {victim}->{peer}: {e}"));
        cluster
            .add_peer(peer, victim)
            .unwrap_or_else(|e| panic!("add peer {peer}->{victim}: {e}"));
    }
}

/// Stop the victim once the trial transfer to it is provably underway but
/// incomplete: a push from a trial holder started after the heal while the
/// victim still shows none of the trial rows. Returns true when the victim
/// converged first (the stop would prove nothing; the caller retries with
/// fresh rows), mirroring Go's `killOnceBytesFlow`, which keys on inbound
/// frame bytes instead of push-start.
fn stop_once_push_flows(
    cluster: &mut Cluster,
    prefix: &str,
    rows: usize,
    pushed_before: u64,
) -> bool {
    let deadline = Instant::now() + KILL_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            let n = prefix_count(&*cluster, VICTIM, prefix);
            panic!(
                "no push to the victim observed within {KILL_TIMEOUT:?} (trial rows visible: {n}/{rows})"
            );
        }
        if prefix_count(&*cluster, VICTIM, prefix) == rows {
            return true;
        }
        let pushed_now = pushed(&*cluster, SENDER) + pushed(&*cluster, WITNESS);
        if pushed_now > pushed_before {
            eprintln!(
                "stopping victim mid-transfer after {} trial push(es) started",
                pushed_now - pushed_before
            );
            cluster.stop_node(VICTIM);
            return false;
        }
        std::thread::sleep(KILL_POLL);
    }
}

/// Restart the victim in place and rejoin the mesh (Go: `restartNode`).
fn restart_node(cluster: &mut Cluster, idx: usize) {
    cluster
        .start_node(idx)
        .unwrap_or_else(|e| panic!("node{idx} restart failed: {e}"));
    // The reopened node rebinds a fresh ephemeral port: re-mesh so every
    // ordered pair dials the current addresses.
    cluster.mesh().expect("re-mesh after restart");
    for i in 0..cluster.len() {
        wait_connected_peers(&*cluster, i, cluster.len() - 1, HEAL_PEERS_TIMEOUT);
    }
}

/// Poll the retry to completion and enforce the resume contract on every
/// sample: the sender always shows the full trial (its commit was durable)
/// and the victim shows none or all of it (never a partial transaction).
/// The victim's applied watermark for the trial origin (as acked to the
/// sender) advances on durable apply; the first advanced-watermark sample
/// must precede full visibility by at most the publication bound.
/// Returns the victim's post-restart `(received, applied)` counters
/// (Go: `watchRetry`).
fn watch_retry(cluster: &Cluster, prefix: &str, rows: usize, have_pre: u64) -> (u64, u64) {
    let victim_id = cluster.node_id(VICTIM);
    let sender_id = cluster.node_id(SENDER);
    let deadline = Instant::now() + RETRY_TIMEOUT;
    let mut first_advanced: Option<Instant> = None;
    let mut first_full: Option<Instant> = None;
    loop {
        if Instant::now() >= deadline {
            panic!(
                "retry did not complete within {RETRY_TIMEOUT:?} (victim rows: {}/{rows}, watermark: {}, want >= {})",
                prefix_count(cluster, VICTIM, prefix),
                peer_have(cluster, SENDER, victim_id, sender_id),
                have_pre + 1,
            );
        }
        let now = Instant::now();
        let have = peer_have(cluster, SENDER, victim_id, sender_id);
        let vis_victim = prefix_count(cluster, VICTIM, prefix);
        let vis_sender = prefix_count(cluster, SENDER, prefix);
        let counters = log_counters(cluster, VICTIM);
        assert!(
            vis_sender == rows,
            "sender shows {vis_sender}/{rows} trial rows after committing them"
        );
        assert!(
            vis_victim == 0 || vis_victim == rows,
            "victim shows {vis_victim}/{rows} trial rows: partial transaction visible"
        );
        if have > have_pre && first_advanced.is_none() {
            first_advanced = Some(now);
        }
        if vis_victim == rows && first_full.is_none() {
            first_full = Some(now);
        }
        if vis_victim == rows && have >= have_pre + 1 {
            match (first_advanced, first_full) {
                (Some(advanced), Some(full)) => {
                    let lag = full.saturating_duration_since(advanced);
                    eprintln!("watermark advanced {lag:?} before full trial visibility");
                    assert!(
                        lag <= PUBLICATION_BOUND,
                        "watermark advanced {lag:?} before completion, want <= {PUBLICATION_BOUND:?} (early advance)"
                    );
                }
                (advanced, full) => {
                    panic!(
                        "converged without observing watermark advance (advanced={advanced:?} full={full:?})"
                    );
                }
            }
            return counters;
        }
        std::thread::sleep(RETRY_POLL);
    }
}

/// Positive-integer env knob with fallback (Go: `envInt`).
fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

#[test]
fn chunked_transaction_resumes_after_receiver_restart() {
    let rows = env_usize("MURMUR_TXCHUNK_ROWS", 1200);
    let value_bytes = env_usize("MURMUR_TXCHUNK_VALUE_BYTES", 9000);
    let max_attempts = env_usize("MURMUR_TXCHUNK_ATTEMPTS", 3);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "txchunk-resume".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let victim_id = cluster.node_id(VICTIM);
        let sender_id = cluster.node_id(SENDER);

        // Same schema on every node (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Positive control (honest behavior works): a multi-chunk
        // transaction replicates to every node with no stopping.
        commit_big_tx(&cluster, SENDER, "base-", 0, rows, value_bytes);
        wait_converged(&cluster, rows, POSITIVE_TIMEOUT, "positive control");
        eprintln!("positive control: multi-chunk transaction replicated to all nodes");

        // Resume trials. Each attempt commits a fresh big transaction
        // while the victim is partitioned, heals, stops the victim once
        // the trial transfer to it starts flowing, restarts it, and
        // watches the retry.
        let mut expected_rows = rows;
        let mut resumed = false;
        for attempt in 1..=max_attempts {
            if resumed {
                break;
            }
            let prefix = format!("a{attempt}-");
            eprintln!("resume attempt {attempt}/{max_attempts}");

            // Quiet baseline BEFORE partitioning: the victim must have
            // acked everything the sender has before the trial starts.
            // The mesh is converged at every loop top (positive control
            // or the previous attempt's reconvergence wait).
            let seq_pre = committed(&cluster, SENDER);
            let have_pre = wait_have_settled(&cluster, SENDER, victim_id, sender_id);

            partition(&cluster, VICTIM);
            wait_connected_peers(&cluster, VICTIM, 0, SPLIT_PEERS_TIMEOUT);
            // Log-path baseline on the victim (Go: snapshot counters that
            // have no Rust equivalent; see README.md).
            let (recv_pre_kill, applied_pre_kill) = log_counters(&cluster, VICTIM);
            eprintln!(
                "victim pre-trial received_transactions={recv_pre_kill} applied_transactions={applied_pre_kill}"
            );

            commit_big_tx(
                &cluster,
                SENDER,
                &prefix,
                attempt as i64 * 1_000_000,
                rows,
                value_bytes,
            );
            let seq_tx = committed(&cluster, SENDER);
            assert!(
                seq_tx > seq_pre,
                "attempt {attempt}: sender commit cursor did not advance ({seq_pre} -> {seq_tx})"
            );
            expected_rows += rows;

            // Survivors converge on the trial rows; the victim must miss them.
            wait_prefix_count(&cluster, SENDER, &prefix, rows, SURVIVOR_TIMEOUT);
            wait_prefix_count(&cluster, WITNESS, &prefix, rows, SURVIVOR_TIMEOUT);
            assert!(
                prefix_count(&cluster, VICTIM, &prefix) == 0,
                "attempt {attempt}: victim sees {} trial rows while partitioned, want 0",
                prefix_count(&cluster, VICTIM, &prefix),
            );

            // Heal, then stop once the trial transfer to the victim is
            // flowing. Survivor convergence above proves every pre-heal
            // push already landed, so any push advancement after this
            // sample is victim-bound trial flow.
            let pushed_before = pushed(&cluster, SENDER) + pushed(&cluster, WITNESS);
            heal(&cluster, VICTIM);
            if stop_once_push_flows(&mut cluster, &prefix, rows, pushed_before) {
                eprintln!(
                    "attempt {attempt}: victim converged before the stop landed; retrying with fresh rows"
                );
                wait_converged(&cluster, expected_rows, ATTEMPT_CONVERGE_TIMEOUT, "too-slow reconverge");
                continue;
            }
            restart_node(&mut cluster, VICTIM);

            let (received_post, applied_post) = watch_retry(&cluster, &prefix, rows, have_pre);
            eprintln!("attempt {attempt}: victim staged {received_post} and applied {applied_post} transaction(s) after restart");
            if received_post < 1 {
                // The victim staged nothing after restarting, so it must
                // have staged the trial before the stop: the trial proves
                // nothing about resume; retry fresh.
                eprintln!("attempt {attempt}: no post-restart receipt; stop landed too late, retrying");
                wait_converged(&cluster, expected_rows, ATTEMPT_CONVERGE_TIMEOUT, "too-late reconverge");
                continue;
            }
            resumed = true;

            // The retry must have used the log/chunk path, not snapshots:
            // snapshot application bypasses both counters, so post-restart
            // receipt AND apply prove log-path resume. The engine only
            // requests snapshots from explicitly trusted sources, and
            // harness clusters never configure any, so no snapshot could
            // have been taken anywhere.
            assert!(
                applied_post >= 1,
                "victim applied {applied_post} transaction(s) after restart, want >= 1 (resume must use chunks/log, not snapshot)"
            );
            let metrics = cluster
                .node(VICTIM)
                .replication()
                .expect("replication handle")
                .status()
                .metrics;
            eprintln!(
                "attempt {attempt}: resume used log path (chunk_need_sent={} chunk_need_served={})",
                metrics.chunk_need_sent, metrics.chunk_need_served,
            );
        }
        assert!(
            resumed,
            "no valid resume trial in {max_attempts} attempts (stop never landed before convergence)"
        );

        // Final: exact row counts and identical ordered digests everywhere.
        wait_converged(&cluster, expected_rows, FINAL_TIMEOUT, "final");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("txchunk-resume-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
