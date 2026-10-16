//! Rust port of the Go `corrupt-snapshot` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/corrupt-snapshot/`
//! (`corrupt_snapshot_test.go` + `attacker.go`). A stale node rejoins while
//! its only available snapshot source is hostile: the attacker induces a
//! snapshot pull with an unsolicited history-unavailable error, then serves
//! a structurally valid manifest plus one intact and one bit-flipped chunk.
//! The receiver must reject the corrupt chunk with nothing partially
//! published (row count, digest, and commit cursor untouched), discard the
//! staged transfer, then converge via the honest snapshot path with
//! bit-identical digests. Plain SQL replaces the Go typed-records HTTP API,
//! close/reopen replaces process stop/start, and rejection is observed via
//! the replication `last_error` plus wire behavior (no snapshot counters
//! exist); see the scenario README.md for the deviations.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use murmur_live_harness::murmur_replication::{
    protocol::{self, Message},
    retention, snapshot, transport, Config as ReplConfig, Dissemination,
};
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{
    env_seconds, wait_until, Cluster, ClusterOptions, ReplicationOptions,
};
use sha2::{Digest, Sha256};

/// Role index: node3 goes stale, then rejoins (Go: `stale = 2`).
const STALE: usize = 2;
/// Attacker identity: fixed test id, asserted distinct from the node ids.
const ATTACKER_ID: [u8; 16] = [0xA7; 16];
/// Attacker origin signing key (the victim trusts its public half, mirroring
/// the Go test authorizing the hostile source).
const ATTACKER_SEED: [u8; 32] = [0x5E; 32];
/// Forged snapshot payload: two 64 KiB wire chunks (Go serves two chunks).
const FORGED_BYTES: usize = 70_000;
/// Forged manifest lifetime (attacker-side): short so the victim's expiry
/// reclamation clears the staged corrupt transfer before the honest leg.
const FORGED_LIFETIME: Duration = Duration::from_secs(20);
/// Reclaim margin past the forged expiry (the engine's maintenance tick runs
/// about every second).
const RECLAIM_MARGIN: Duration = Duration::from_secs(10);

/// Log retention (Go: `MinLogRetentionMs = 1000`).
const RETENTION: Duration = Duration::from_secs(1);
/// Offline-peer memory (Go: `MaxOfflineLogRetentionMs = 20000`).
const MAXIMUM_OFFLINE: Duration = Duration::from_secs(20);
/// Newest transactions per origin the survivors must keep (Go: `MinRetainedBatches = 10`).
const MINIMUM_RETAINED: u64 = 10;
/// Minimum pruned transactions per survivor before the attack: 60 survivor
/// writes minus the 10 retained, with margin. Proves the stale node's needed
/// ranges are genuinely unrecoverable from logs, so the rejoin must use the
/// snapshot path.
const MIN_COLLECTED: u64 = 40;

/// Baseline convergence deadline (Go: 60 s).
const BASELINE_TIMEOUT: Duration = Duration::from_secs(60);
/// Survivor row-count deadline (Go: 30 s).
const ROW_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// GC collection-loop deadline past the expiry wait.
const COLLECT_TIMEOUT: Duration = Duration::from_secs(60);
/// Attacker dial/handshake deadline (Go: 15 s).
const DIAL_TIMEOUT: Duration = Duration::from_secs(15);
/// Snapshot-pull inducement deadline (Go: 30 s).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Forged-chunk request deadline (Go: manifests/chunks received, 15 s).
const NEED_TIMEOUT: Duration = Duration::from_secs(15);
/// Corrupt-chunk rejection deadline (Go: apply failures, 30 s).
const FAIL_TIMEOUT: Duration = Duration::from_secs(30);
/// Post-attack drain: no `SnapshotApplied` may arrive (Go: 3 s).
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);
/// Post-attack settle before asserting nothing was published (Go: 2 s).
const SETTLE: Duration = Duration::from_secs(2);
/// Honest-resync convergence deadline (Go: 120 s).
const HONEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM records ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in rows {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read names")
}

/// Canonical digest of a name set (Go: sorted names joined with newlines).
fn digest(names: &[String]) -> String {
    names.join("\n")
}

/// Row count on one node (cheap pre-check before full name reads).
fn row_count(db: &Database) -> usize {
    db.read(|sql| {
        let n = sql.query_row("SELECT COUNT(*) FROM records", [], |row| {
            row.get::<_, i64>(0)
        })?;
        Ok(n as usize)
    })
    .expect("row count")
}

/// One single-row transaction (Go: `TypedInsert`).
fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) {
    let name = name.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            sql.execute("INSERT INTO records(id, name) VALUES(?, ?)", (id, name))?;
            Ok(())
        })
        .unwrap_or_else(|e| panic!("node {node} insert failed: {e}"));
}

/// Wait until one node holds exactly `want` rows (Go: `waitRowCount`).
fn wait_row_count(cluster: &Cluster, idx: usize, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if row_count(cluster.node(idx)) == want {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "node {idx} record count = {}, want {want} within {timeout:?}",
                row_count(cluster.node(idx)),
            );
        }
        std::thread::sleep(CONVERGE_POLL);
    }
}

/// Wait until every node holds exactly `want` rows with equal digests
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let counts: Vec<usize> = (0..cluster.len())
            .map(|i| row_count(cluster.node(i)))
            .collect();
        if counts.iter().all(|&n| n == want) {
            let mut first = String::new();
            let mut equal = true;
            for (i, idx) in (0..cluster.len()).enumerate() {
                let d = digest(&names(cluster.node(idx)));
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
                let node_names = names(cluster.node(i));
                eprintln!(
                    "node {i} at timeout: count={} digest={}",
                    node_names.len(),
                    digest(&node_names)
                );
            }
            panic!(
                "nodes did not converge on {want} rows with equal digests within {timeout:?}"
            );
        }
        std::thread::sleep(CONVERGE_POLL);
    }
}

/// Connected replication sessions on one node (Go: `spedsql_connected_peers`).
fn connected_peers(cluster: &Cluster, idx: usize) -> usize {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .peers
}

/// Latest replication error on one node (Go: failure counters, which have no
/// Rust equivalent; the corrupt-chunk rejection lands here).
fn last_error(cluster: &Cluster, idx: usize) -> Option<String> {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .last_error
}

/// Local commit cursor (Go: `spedsql_state_generation` advance check).
fn committed(cluster: &Cluster, idx: usize) -> u64 {
    cluster.node(idx).status().committed
}

/// Log-path activity counters for one node: `(received_transactions,
/// applied_transactions)`. Snapshot application bypasses both counters, so a
/// flat pair across the honest resync proves the rows arrived via snapshot.
fn log_counters(cluster: &Cluster, idx: usize) -> (u64, u64) {
    let metrics = cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics;
    (metrics.received_transactions, metrics.applied_transactions)
}

/// Run one bounded retention pass on one node; returns transactions pruned.
fn collect_history(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .collect_retention()
        .unwrap_or_else(|e| panic!("node {idx} collect retention: {e}"))
}

/// Total transactions ever pruned on one node.
fn collected_total(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .retention_status()
        .expect("retention status")
        .collected_transactions
}

/// Malicious snapshot-source fixture: a raw QUIC client speaking the
/// replication wire protocol (Go: `attacker` in attacker.go).
struct Attacker {
    config: ReplConfig,
    endpoint: quinn::Endpoint,
}

impl Attacker {
    fn new(config: ReplConfig) -> Self {
        let endpoint =
            transport::endpoint(&config, ATTACKER_ID).expect("attacker endpoint");
        Self { config, endpoint }
    }

    /// Complete a handshake against the victim and return the session plus
    /// the victim's schema bytes (revealed by its Welcome).
    async fn dial(&self, addr: SocketAddr, db: [u8; 16], expect: [u8; 16]) -> EvilSession {
        let connecting = self
            .endpoint
            .connect(addr, "murmur.invalid")
            .expect("attacker connect setup");
        let connection = tokio::time::timeout(DIAL_TIMEOUT, connecting)
            .await
            .expect("attacker dial timeout")
            .expect("attacker connect");
        let (mut send, mut recv) = tokio::time::timeout(DIAL_TIMEOUT, connection.open_bi())
            .await
            .expect("attacker open stream timeout")
            .expect("attacker open stream");
        protocol::write(
            &mut send,
            &Message::Hello {
                capabilities: protocol::CAPABILITIES,
                required: protocol::REQUIRED,
                version: protocol::VERSION,
                database: db,
                node: ATTACKER_ID,
                fetch: None,
                schema: Vec::new(),
                mode: Dissemination::Gossip,
                max_transaction: 64 << 20,
            },
        )
        .await
        .expect("attacker write hello");
        let welcome = tokio::time::timeout(Duration::from_secs(10), protocol::read(&mut recv))
            .await
            .expect("attacker welcome timeout")
            .expect("attacker read welcome");
        let schema = match welcome {
            Message::Welcome {
                database: db2,
                node,
                schema,
                ..
            } => {
                assert_eq!(db2, db, "welcome database mismatch");
                assert_eq!(node, expect, "welcome node mismatch");
                schema
            }
            other => panic!("attacker expected welcome, got {other:?}"),
        };
        EvilSession {
            connection,
            send,
            recv,
            welcome_schema: schema,
        }
    }
}

/// One attack QUIC session with its handshake stream open.
struct EvilSession {
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    welcome_schema: Vec<u8>,
}

impl EvilSession {
    async fn send(&mut self, message: &Message) {
        tokio::time::timeout(
            Duration::from_secs(10),
            protocol::write(&mut self.send, message),
        )
        .await
        .expect("attacker send timeout")
        .expect("attacker send");
    }

    /// Read until `want` matches, `total` elapses, or the session dies
    /// (Go: `collectUntil`, but patient across quiet slices instead of
    /// returning on the first one).
    async fn collect_until(
        &mut self,
        total: Duration,
        want: impl Fn(&Message) -> bool,
    ) -> Vec<Message> {
        let mut out = Vec::new();
        let deadline = Instant::now() + total;
        while Instant::now() < deadline {
            let slice = (deadline - Instant::now()).min(Duration::from_millis(500));
            match tokio::time::timeout(slice, protocol::read(&mut self.recv)).await {
                Ok(Ok(message)) => {
                    let matched = want(&message);
                    out.push(message);
                    if matched {
                        return out;
                    }
                }
                Ok(Err(_)) => return out, // session died; the caller interprets
                Err(_) => {}              // quiet slice; keep waiting
            }
        }
        out
    }

    fn close(self) {
        self.connection.close(0u32.into(), b"attack done");
    }
}

/// A manifest plus chunk payloads whose second chunk was bit-flipped after
/// the manifest digests were computed (Go: `corruptSnapshot`).
struct ForgedSnapshot {
    manifest: snapshot::Manifest,
    transfer: [u8; 16],
    chunk0: Vec<u8>,
    chunk1_bad: Vec<u8>,
}

/// Forge a structurally valid two-chunk snapshot for the victim's schema:
/// the manifest passes every structural check, but the final chunk's bytes
/// differ from what its chunk hash covers.
fn build_corrupt_snapshot(
    config: &ReplConfig,
    db: [u8; 16],
    victim: [u8; 16],
    victim_schema: &[u8],
) -> ForgedSnapshot {
    let mut data = vec![0u8; FORGED_BYTES];
    for (i, b) in data.iter_mut().enumerate() {
        *b = b'a' + (i % 26) as u8;
    }
    let schema = protocol::schema_digest(victim_schema);
    let (manifest, _records) = snapshot::build(
        (db, ATTACKER_ID),
        victim,
        data.clone(),
        Vec::new(),
        schema,
        config,
        retention::now_millis(),
    )
    .expect("forge manifest");
    assert_eq!(
        manifest.chunks.len(),
        2,
        "fixture: forged snapshot must span exactly two chunks"
    );

    // Non-vacuous guards: the uncorrupted bytes verify against the manifest,
    // and the corrupted bytes must not.
    assert_eq!(
        <[u8; 32]>::from(Sha256::digest(&data)),
        manifest.digest,
        "fixture: uncorrupted snapshot does not verify locally"
    );
    let mut parts = data.chunks(protocol::CHUNK_BYTES);
    let chunk0 = parts.next().expect("fixture: first chunk").to_vec();
    let mut chunk1_bad = parts.next().expect("fixture: second chunk").to_vec();
    assert!(parts.next().is_none(), "fixture: unexpected third chunk");
    // Flip a byte mid-chunk. An ASCII-to-ASCII flip preserves length and
    // framing; only the digest differs.
    let mid = chunk1_bad.len() / 2;
    chunk1_bad[mid] ^= 0x01;
    assert_eq!(
        chunk1_bad.len(),
        data.len() - chunk0.len(),
        "fixture: corruption changed the encoded length"
    );
    assert_ne!(
        <[u8; 32]>::from(Sha256::digest(&chunk1_bad)),
        manifest.chunks[1],
        "fixture: corrupted snapshot still verifies locally"
    );
    let transfer = manifest.transfer;
    ForgedSnapshot {
        manifest,
        transfer,
        chunk0,
        chunk1_bad,
    }
}

#[test]
fn stale_node_discards_corrupt_snapshot_then_converges() {
    // Fixed node ids (asserted distinct from the attacker id) so the
    // snapshot-trust overlays can be built before the cluster opens.
    let node_ids = vec![[0x10; 16], [0x20; 16], [0x30; 16]];
    assert!(
        !node_ids.contains(&ATTACKER_ID),
        "attacker id collides with a node id"
    );
    // Every node trusts every node as a snapshot donor (Go harness all-node
    // fixture policy); the victim additionally authorizes the hostile source
    // (Go: the test appends the attacker to the victim's config file).
    let all_nodes: BTreeSet<[u8; 16]> = node_ids.iter().copied().collect();
    let mut trusted_snapshot_sources: HashMap<usize, BTreeSet<[u8; 16]>> = HashMap::new();
    for i in 0..node_ids.len() {
        trusted_snapshot_sources.insert(i, all_nodes.clone());
    }
    let mut victim_sources = all_nodes.clone();
    victim_sources.insert(ATTACKER_ID);
    trusted_snapshot_sources.insert(STALE, victim_sources);
    let mut extra_trusted_origins: HashMap<usize, BTreeMap<[u8; 16], [u8; 32]>> =
        HashMap::new();
    extra_trusted_origins.insert(
        STALE,
        BTreeMap::from([(ATTACKER_ID, protocol::public_key(&ATTACKER_SEED))]),
    );

    let mut cluster = Cluster::new(ClusterOptions {
        name: "corrupt-snapshot".into(),
        num_nodes: 3,
        node_ids,
        // Manual peers so the stale node restarts truly peerless: the
        // attacker becomes its only snapshot source by construction.
        manual_peers: true,
        replication: Some(ReplicationOptions {
            retention: Some(RETENTION),
            minimum_retained: Some(MINIMUM_RETAINED),
            maximum_offline: Some(MAXIMUM_OFFLINE),
            ..Default::default()
        }),
        trusted_snapshot_sources,
        extra_trusted_origins,
        ..Default::default()
    })
    .expect("cluster setup");

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
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

        // Phase 1: build the honest mesh explicitly and converge a baseline.
        for (from, target) in [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)] {
            cluster
                .add_peer(from, target)
                .unwrap_or_else(|e| panic!("AddPeer({from},{target}): {e}"));
        }
        for i in 0..5 {
            insert(&cluster, STALE, i + 1, &format!("stale-{i}"));
        }
        cluster.node(STALE).flush().expect("flush baseline");
        wait_converged(&cluster, 5, BASELINE_TIMEOUT);
        let pre_stop_digest = digest(&names(cluster.node(STALE)));

        // Phase 2: isolate node3, stop it, and let the survivors write past
        // retention so the rejoin genuinely needs the snapshot path.
        for (from, target) in [(2, 0), (2, 1), (0, 2), (1, 2)] {
            cluster
                .remove_peer(from, target)
                .unwrap_or_else(|e| panic!("RemovePeer({from},{target}): {e}"));
        }
        cluster.stop_node(STALE);
        // Flush every write: the product mints one replication log entry per
        // flush batch, not per SQL transaction, so a single trailing flush
        // would leave fewer entries than `minimum_retained` and nothing
        // would ever become eligible for pruning.
        for i in 0..60 {
            insert(&cluster, 0, 100 + i, &format!("fresh-{i}"));
            cluster.node(0).flush().expect("flush survivor write");
        }
        wait_row_count(&cluster, 0, 65, ROW_TIMEOUT);
        wait_row_count(&cluster, 1, 65, ROW_TIMEOUT);
        // Member-deadline expiry, then explicit collection passes: the stale
        // node's retention pin must lapse before its needed ranges become
        // genuinely unrecoverable from logs.
        eprintln!("waiting for member-deadline expiry and collection");
        std::thread::sleep(env_seconds(
            "MURMUR_CORRUPT_SNAPSHOT_GC_WAIT_SECONDS",
            30,
        ));
        let collect_deadline = Instant::now() + COLLECT_TIMEOUT;
        loop {
            let totals = [collected_total(&cluster, 0), collected_total(&cluster, 1)];
            if totals.iter().all(|&n| n >= MIN_COLLECTED) {
                eprintln!("survivors pruned {totals:?} transactions past retention");
                break;
            }
            if Instant::now() >= collect_deadline {
                panic!(
                    "survivors collected only {totals:?} transactions, want >= {MIN_COLLECTED} each (snapshot path not forced)"
                );
            }
            collect_history(&cluster, 0);
            collect_history(&cluster, 1);
            std::thread::sleep(Duration::from_secs(2));
        }
        // Prove the deadline leg explicitly: the victim is now older than
        // every survivor's offline memory.
        for i in [0, 1] {
            let status = cluster
                .node(i)
                .replication()
                .expect("replication handle")
                .retention_status()
                .expect("retention status");
            let victim = cluster.node_id(STALE);
            let peer = status
                .peers
                .iter()
                .find(|p| p.node == victim)
                .expect("victim retention record");
            let age = retention::now_millis().saturating_sub(peer.last_seen_millis);
            assert!(
                age > MAXIMUM_OFFLINE.as_millis() as u64,
                "node {i} still pins the victim (last seen {age} ms ago)"
            );
        }

        // Phase 3: node3 restarts peerless; the attacker's inbound session is
        // its only source. The attacker induces a snapshot pull, then serves
        // bit-flipped chunks.
        cluster
            .start_node(STALE)
            .expect("stale node restart");
        let victim_addr = cluster.node_info(STALE).repl_addr;
        let victim_id = cluster.node_id(STALE);
        let dbid = cluster.database_id();
        let gen_before_attack = committed(&cluster, STALE);
        let log_before_attack = log_counters(&cluster, STALE);

        let issued = cluster
            .issue_cert(&ATTACKER_ID)
            .expect("issue attacker cert");
        let mut attacker_config = ReplConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            issued.credentials,
            ATTACKER_SEED,
        );
        attacker_config.snapshot_transfer_lifetime = FORGED_LIFETIME;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("attacker runtime");
        rt.block_on(async {
            // The QUIC endpoint requires a live tokio runtime context.
            let attacker = Attacker::new(attacker_config);
            let mut evil = attacker.dial(victim_addr, dbid, victim_id).await;
            // Prove the "only available source" premise: exactly one peer.
            // The victim registers the inbound session asynchronously, so
            // poll for the count instead of asserting immediately.
            let one_peer = wait_until(Duration::from_secs(10), Duration::from_millis(50), || {
                connected_peers(&cluster, STALE) == 1
            });
            assert!(
                one_peer,
                "stale node has {} connected peers during the attack, want 1",
                connected_peers(&cluster, STALE)
            );
            // Induce the pull: an unsolicited history-unavailable error makes
            // the node request a snapshot from this peer over the live session.
            evil
                .send(&Message::Error {
                    code: 3,
                    message: "ranges unavailable".into(),
                })
                .await;
            let reqs = evil
                .collect_until(REQUEST_TIMEOUT, |m| {
                    matches!(m, Message::SnapshotRequest)
                })
                .await;
            assert!(
                reqs.iter().any(|m| matches!(m, Message::SnapshotRequest)),
                "stale node never requested a snapshot from the attacker"
            );
            let forged =
                build_corrupt_snapshot(&attacker.config, dbid, victim_id, &evil.welcome_schema);
            evil.send(&Message::SnapshotManifest(forged.manifest.clone()))
                .await;
            // The manifest passed structural checks: the node asks for its
            // chunks (the received-counters-moved leg).
            let needs = evil
                .collect_until(NEED_TIMEOUT, |m| {
                    matches!(m, Message::SnapshotNeed { .. })
                })
                .await;
            assert!(
                needs.iter().any(
                    |m| matches!(m, Message::SnapshotNeed { transfer, .. } if *transfer == forged.transfer)
                ),
                "victim never asked for the forged chunks (manifest not accepted)"
            );
            evil
                .send(&Message::SnapshotChunk {
                    transfer: forged.transfer,
                    index: 0,
                    data: forged.chunk0,
                })
                .await;
            evil
                .send(&Message::SnapshotChunk {
                    transfer: forged.transfer,
                    index: 1,
                    data: forged.chunk1_bad,
                })
                .await;
            // The corrupt transfer must never complete: drain while asserting
            // no SnapshotApplied arrives (the victim closes the session on
            // the digest mismatch, which ends the drain early).
            let tail = evil.collect_until(DRAIN_TIMEOUT, |_| false).await;
            assert!(
                !tail
                    .iter()
                    .any(|m| matches!(m, Message::SnapshotApplied { .. })),
                "victim applied the corrupt snapshot"
            );
            evil.close();
        });

        // The receiver must detect the digest mismatch and fail the chunk...
        let fail_deadline = Instant::now() + FAIL_TIMEOUT;
        loop {
            if let Some(err) = last_error(&cluster, STALE) {
                if err.contains("snapshot chunk") {
                    eprintln!("victim rejected the corrupt chunk: {err}");
                    break;
                }
            }
            assert!(
                Instant::now() < fail_deadline,
                "victim never recorded the corrupt-chunk rejection"
            );
            std::thread::sleep(CONVERGE_POLL);
        }
        // ...while no snapshot completed and nothing was partially published.
        std::thread::sleep(SETTLE);
        assert_eq!(
            row_count(cluster.node(STALE)),
            5,
            "stale node row count moved on the corrupt transfer (partial publication)"
        );
        assert_eq!(
            digest(&names(cluster.node(STALE))),
            pre_stop_digest,
            "stale node digest changed on the corrupt transfer (partial publication)"
        );
        assert_eq!(
            committed(&cluster, STALE),
            gen_before_attack,
            "commit cursor moved on the corrupt transfer (partial publication)"
        );
        assert_eq!(
            log_counters(&cluster, STALE),
            log_before_attack,
            "replication counters moved on the corrupt transfer (partial publication)"
        );

        // The staged corrupt transfer is discarded on expiry (the only
        // staged-transfer discard path); the honest leg below proves it.
        std::thread::sleep(FORGED_LIFETIME + RECLAIM_MARGIN);

        // Phase 4: honest peers return; the node retries from them and converges.
        for (from, target) in [(2, 0), (2, 1), (0, 2), (1, 2)] {
            cluster
                .add_peer(from, target)
                .unwrap_or_else(|e| panic!("honest AddPeer({from},{target}): {e}"));
        }
        let log_pre_heal = log_counters(&cluster, STALE);
        wait_converged(&cluster, 65, HONEST_TIMEOUT);
        // The rejoin completed via an honest snapshot, not the corrupt one
        // and not the logs: 60 rows arrived while the log-path counters
        // stayed flat (snapshot application bypasses both).
        assert_eq!(
            log_counters(&cluster, STALE),
            log_pre_heal,
            "honest resync arrived over the log path, not a snapshot"
        );
        assert!(
            committed(&cluster, STALE) >= gen_before_attack,
            "commit cursor regressed (non-monotonic publication)"
        );
        // The stale node's acknowledged pre-stop state survives the merge.
        let stale_rows = names(cluster.node(STALE))
            .iter()
            .filter(|name| name.starts_with("stale-"))
            .count();
        assert_eq!(
            stale_rows, 5,
            "stale rows after honest resync = {stale_rows}, want 5"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("corrupt-snapshot-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
