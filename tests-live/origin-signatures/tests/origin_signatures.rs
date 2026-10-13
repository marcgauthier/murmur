//! Rust port of the Go `origin-signatures` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/origin-signatures/`
//! (`origin_signatures_test.go`:
//! `TestForwardedOriginSurvivesOfflineOriginAndHostileRelay`). Three
//! encrypted nodes share one database id; A commits, B receives, A stops,
//! and C receives A's transaction exclusively through B. The fixture
//! checks the actual stored origin proof, then a relay authenticated as
//! B fires modified, fabricated, unsigned, cross-DBID and chunk-identity
//! attacks at C. Every attack must be rejected while rows, schemas and
//! replication counters stay unchanged. Restart and signed-duplicate
//! replay check durability and idempotency, and an untrusted snapshot
//! manifest must be refused.
//!
//! Plain SQL replaces the Go typed-records HTTP API; the Go Prometheus
//! rejection counters map to per-leg wire + `last_error` latch evidence
//! (the Rust engine keeps no invalid-batch counters), and each
//! signature-failure leg uses its own relay session because an
//! `Error::Auth` kills the session that carried it. See the scenario
//! README.md for the full deviation list.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use murmur_live_harness::murmur_replication::{
    protocol::{self, Chunk, Message, Transaction},
    retention, snapshot, transport, Config as ReplConfig, Dissemination,
};
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};
use sha2::{Digest, Sha256};

/// Fixed node ids (Go: per-node origin keys; the harness mints the seeds).
const NODE_A: [u8; 16] = [0x10; 16];
const NODE_B: [u8; 16] = [0x20; 16];
const NODE_C: [u8; 16] = [0x30; 16];
const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
/// Honest origin ids; data frames on relay sessions must never carry any
/// other origin.
const HONEST: [[u8; 16]; 3] = [NODE_A, NODE_B, NODE_C];
/// Re-signing key for the fabricated leg. Go re-signs with B's origin
/// key; the harness never discloses node seeds, so a dedicated attacker
/// key takes its place (the check exercised is identical: the signature
/// must fail against the claimed origin's trusted key).
const FABRICATE_SEED: [u8; 32] = [0xA1; 32];
/// Divergent handshake schema bytes for the first relay dial (its Welcome
/// teaches the relay the node's real schema).
const DIVERGENT_SCHEMA: &[u8] = b"relay-divergent-schema-probe";
/// Cross-DBID attack database id (Go: a fresh random DBID).
const CROSS_DB: [u8; 16] = [0xDD; 16];
/// Distinct transaction ids per attack leg (Go: fresh TxID each attack).
const MODIFIED_ID: [u8; 16] = [0xB1; 16];
const FABRICATED_ID: [u8; 16] = [0xB2; 16];
const UNSIGNED_ID: [u8; 16] = [0xB3; 16];
const CROSSDB_ID: [u8; 16] = [0xB4; 16];
const CHUNK_ID: [u8; 16] = [0xB5; 16];
/// Local verification cap (the product default `max_transaction_bytes`).
const MAX_TX: usize = 64 << 20;

/// Convergence deadline per phase (Go: 30 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);
/// Fixture dial/stream/setup deadline (Go: 10 s).
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the fixture waits for a Welcome that must arrive.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
/// Window for the served `Need` batches to arrive.
const NEED_WINDOW: Duration = Duration::from_secs(5);
/// How long the fixture waits for an `Error{code: 1}` reply (Go: 10 s).
const ERROR_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the fixture waits for a Pong that must arrive (Go: 10 s).
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the fixture waits for a chunk-availability page.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Slice for one gradual read inside a collection window.
const SLICE: Duration = Duration::from_millis(500);
/// Post-fatal-probe silence window (Go: 3 s).
const DEATH_WINDOW: Duration = Duration::from_secs(3);
/// Session attach/teardown observation bound.
const PEER_TIMEOUT: Duration = Duration::from_secs(5);

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

/// One single-row transaction (Go: `TypedInsertWithID`).
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

/// Wait until `node` holds exactly `want` (Go: `waitRows` + `TypedNames`).
fn wait_names(cluster: &Cluster, node: usize, want: &[&str], timeout: Duration) {
    let ok = wait_until(timeout, Duration::from_millis(200), || {
        names(cluster.node(node))
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            == want
    });
    assert!(
        ok,
        "node{} did not converge on {want:?} within {timeout:?} (has {:?})",
        node + 1,
        names(cluster.node(node))
    );
}

/// Connected replication sessions on one node (Go: `connected_peers`).
fn connected_peers(cluster: &Cluster, idx: usize) -> usize {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .peers
}

/// Staged (`received`) and applied counters on one node. `received`
/// only moves when a batch passes verification into staging, so it is
/// the Rust equivalent of Go's `batches_received_total` must-not-move
/// assert; `applied` covers Go's nothing-applied / generation legs.
struct Counters {
    received: u64,
    applied: u64,
}

fn counters(cluster: &Cluster, idx: usize) -> Counters {
    let s = cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status();
    Counters {
        received: s.metrics.received_transactions,
        applied: s.applied_transactions,
    }
}

fn assert_counters(cluster: &Cluster, idx: usize, base: &Counters, ctx: &str) {
    let got = counters(cluster, idx);
    assert!(
        got.received == base.received && got.applied == base.applied,
        "node{} counters moved {ctx}: received {} -> {}, applied {} -> {} \
         (an attack batch staged or applied)",
        idx + 1,
        base.received,
        got.received,
        base.applied,
        got.applied
    );
}

fn is_pong(nonce: u64) -> impl Fn(&Message) -> bool {
    move |m| matches!(m, Message::Pong(n) if *n == nonce)
}

fn is_error_code(want: u16) -> impl Fn(&Message) -> bool {
    move |m| matches!(m, Message::Error { code: c, .. } if *c == want)
}

/// Text of the last `Error` frame seen (Go: `DecodeError` code check).
fn last_error_text(frames: &[Message]) -> Option<(u16, String)> {
    frames.iter().rev().find_map(|m| match m {
        Message::Error { code, message } => Some((*code, message.clone())),
        _ => None,
    })
}

fn msg_name(m: &Message) -> &'static str {
    match m {
        Message::Hello { .. } => "Hello",
        Message::Welcome { .. } => "Welcome",
        Message::Batches(_) => "Batches",
        Message::Ack(_) => "Ack",
        Message::Need { .. } => "Need",
        Message::SnapshotRequest => "SnapshotRequest",
        Message::SnapshotManifest(_) => "SnapshotManifest",
        Message::SnapshotChunk { .. } => "SnapshotChunk",
        Message::SnapshotDone { .. } => "SnapshotDone",
        Message::SnapshotResume { .. } => "SnapshotResume",
        Message::SnapshotNeed { .. } => "SnapshotNeed",
        Message::SnapshotApplied { .. } => "SnapshotApplied",
        Message::SnapshotUnavailable { .. } => "SnapshotUnavailable",
        Message::SnapshotCancel { .. } => "SnapshotCancel",
        Message::Ping(_) => "Ping",
        Message::Pong(_) => "Pong",
        Message::Error { .. } => "Error",
        Message::SchemaRequest => "SchemaRequest",
        Message::SchemaManifest(_) => "SchemaManifest",
        Message::SchemaAck { .. } => "SchemaAck",
        Message::PlumtreeData(_) => "PlumtreeData",
        Message::IHave { .. } => "IHave",
        Message::Prune => "Prune",
        Message::Graft { .. } => "Graft",
        Message::ProgressRequest { .. } => "ProgressRequest",
        Message::ProgressPage { .. } => "ProgressPage",
        Message::TransactionChunk(_) => "TransactionChunk",
        Message::MembersRequest { .. } => "MembersRequest",
        Message::MembersPage { .. } => "MembersPage",
        Message::ChunkNeed { .. } => "ChunkNeed",
        Message::ChunkAvailabilityRequest { .. } => "ChunkAvailabilityRequest",
        Message::ChunkAvailabilityPage { .. } => "ChunkAvailabilityPage",
    }
}

fn frame_summary(frames: &[Message]) -> String {
    if frames.is_empty() {
        return "<none>".to_string();
    }
    frames
        .iter()
        .map(msg_name)
        .collect::<Vec<_>>()
        .join(",")
}

/// Every data frame on a relay session must carry honest origins only:
/// nothing attacker-controlled is ever echoed or relayed back.
fn assert_no_foreign_data(frames: &[Message], honest: &[[u8; 16]], ctx: &str) {
    for f in frames {
        match f {
            Message::Batches(txs) => {
                for tx in txs {
                    assert!(
                        honest.contains(&tx.origin),
                        "relay received a batch with foreign origin {} {ctx}",
                        hex(&tx.origin)
                    );
                }
            }
            Message::PlumtreeData(tx) => {
                assert!(
                    honest.contains(&tx.origin),
                    "relay received plumtree data with foreign origin {} {ctx}",
                    hex(&tx.origin)
                );
            }
            Message::TransactionChunk(c) => {
                assert!(
                    honest.contains(&c.proof.origin),
                    "relay received a chunk with foreign origin {} {ctx}",
                    hex(&c.proof.origin)
                );
            }
            _ => {}
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Raw QUIC relay speaking the replication wire protocol while
/// authenticated as B (Go: `dialRelay` with B's TLS material). The
/// harness CA issues the relay a fresh leaf for B's NodeID, so TLS can
/// never be the refusal cause: every rejection below is origin
/// verification, not transport auth.
struct Relay {
    endpoint: quinn::Endpoint,
    db: [u8; 16],
}

/// One relay session: the handshake stream stays open for further
/// attack frames and for observing node replies (Go: `relay`).
struct RelaySession {
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    welcome_schema: Vec<u8>,
}

impl Relay {
    fn new(config: &ReplConfig, db: [u8; 16]) -> Self {
        let endpoint = transport::endpoint(config, NODE_B).expect("relay endpoint");
        Self { endpoint, db }
    }

    fn hello(&self, schema: Vec<u8>) -> Message {
        Message::Hello {
            capabilities: protocol::CAPABILITIES,
            required: protocol::REQUIRED,
            version: protocol::VERSION,
            database: self.db,
            node: NODE_B,
            fetch: None,
            schema,
            mode: Dissemination::Gossip,
            max_transaction: 64 << 20,
        }
    }

    /// Dial advertising the given schema identity; the Welcome assert
    /// is the attach proof (every dial carries its own proof).
    async fn dial(&self, addr: SocketAddr, schema: Vec<u8>, what: &str) -> RelaySession {
        let connection = tokio::time::timeout(
            DIAL_TIMEOUT,
            self.endpoint
                .connect(addr, "murmur.invalid")
                .expect("relay connect setup"),
        )
        .await
        .unwrap_or_else(|_| panic!("{what}: relay dial timeout"))
        .unwrap_or_else(|e| panic!("{what}: relay connect: {e}"));
        let (mut send, mut recv) = tokio::time::timeout(DIAL_TIMEOUT, connection.open_bi())
            .await
            .unwrap_or_else(|_| panic!("{what}: relay open stream timeout"))
            .unwrap_or_else(|e| panic!("{what}: relay open stream: {e}"));
        protocol::write(&mut send, &self.hello(schema))
            .await
            .unwrap_or_else(|e| panic!("{what}: relay write hello: {e}"));
        let welcome_schema =
            match tokio::time::timeout(WELCOME_TIMEOUT, protocol::read(&mut recv)).await {
                Ok(Ok(Message::Welcome { schema, .. })) => schema,
                other => panic!("{what}: relay expected Welcome, got {other:?}"),
            };
        RelaySession {
            connection,
            send,
            recv,
            welcome_schema,
        }
    }
}

impl RelaySession {
    async fn send(&mut self, msg: &Message, what: &str) {
        protocol::write(&mut self.send, msg)
            .await
            .unwrap_or_else(|e| panic!("{what}: relay send {}: {e}", msg_name(msg)));
    }

    /// Read inbound messages until `total` elapses or the stream dies.
    async fn collect(&mut self, total: Duration) -> Vec<Message> {
        self.collect_until(total, |_| false).await
    }

    /// Read until `want` matches, `total` elapses, or the stream dies.
    async fn collect_until(
        &mut self,
        total: Duration,
        mut want: impl FnMut(&Message) -> bool,
    ) -> Vec<Message> {
        let mut out = Vec::new();
        let deadline = Instant::now() + total;
        while Instant::now() < deadline {
            match tokio::time::timeout(SLICE, protocol::read(&mut self.recv)).await {
                Ok(Ok(m)) => {
                    let hit = want(&m);
                    out.push(m);
                    if hit {
                        return out;
                    }
                }
                Ok(Err(_)) => return out, // stream died
                Err(_) => {}              // quiet slice, keep waiting
            }
        }
        out
    }

    fn close(self, what: &str) {
        self.connection.close(0u32.into(), format!("{what} done").as_bytes());
    }
}

/// Background `last_error` recorder. Stray dial-back failures can
/// overwrite a single latch sample, so Auth-cause legs record every
/// latch transition on a 10 ms poll and match by timestamp instead.
struct Recorder {
    log: Arc<Mutex<Vec<(Instant, String)>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Recorder {
    fn start(cluster: &Cluster, idx: usize) -> Self {
        let handle = cluster
            .node(idx)
            .replication()
            .expect("replication handle");
        let log: Arc<Mutex<Vec<(Instant, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = std::thread::spawn({
            let (log, stop) = (Arc::clone(&log), Arc::clone(&stop));
            move || {
                let mut last = String::new();
                while !stop.load(Ordering::Relaxed) {
                    let cur = handle.status().last_error.unwrap_or_default();
                    if cur != last {
                        last = cur.clone();
                        log.lock().expect("recorder log").push((Instant::now(), cur));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        });
        Self {
            log,
            stop,
            thread: Some(thread),
        }
    }

    fn saw_since(&self, since: Instant, needle: &str) -> bool {
        self.log
            .lock()
            .expect("recorder log")
            .iter()
            .any(|(t, s)| *t >= since && s.contains(needle))
    }

    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().expect("recorder thread").join().expect("recorder join");
    }
}

/// Ping a stream a fatal probe should have killed and assert the node
/// never answers: no pong (the ping was never processed) and no
/// foreign data.
async fn assert_stream_dead(
    sess: &mut RelaySession,
    honest: &[[u8; 16]],
    nonce: u64,
    what: &str,
) {
    let _ = protocol::write(&mut sess.send, &Message::Ping(nonce)).await;
    let frames = sess.collect(DEATH_WINDOW).await;
    for f in &frames {
        if matches!(f, Message::Pong(n) if *n == nonce) {
            panic!("node answered ping after {what} (stream should be dead)");
        }
    }
    assert_no_foreign_data(&frames, honest, &format!("after {what}"));
    eprintln!("after {what} the node sent only: {}", frame_summary(&frames));
}

/// Expect one `Error{code: 1}` whose text contains `needle` on a
/// session that must survive (Go: the non-fatal rejection legs).
/// Returns all frames seen.
async fn expect_protocol_error(
    sess: &mut RelaySession,
    needle: &str,
    what: &str,
) -> Vec<Message> {
    let frames = sess.collect_until(ERROR_TIMEOUT, is_error_code(1)).await;
    match last_error_text(&frames) {
        Some((1, text)) if text.contains(needle) => frames,
        other => panic!("{what} drew no Error{{code: 1, ..{needle}..}}; saw {other:?}"),
    }
}

/// Expect a pong to `nonce` (the session survived the probe).
async fn expect_pong(sess: &mut RelaySession, nonce: u64, what: &str) -> Vec<Message> {
    sess.send(&Message::Ping(nonce), what).await;
    let frames = sess.collect_until(PONG_TIMEOUT, is_pong(nonce)).await;
    assert!(
        frames.iter().any(is_pong(nonce)),
        "missing pong to {what}; saw {}",
        frame_summary(&frames)
    );
    frames
}

/// Wait until `idx` has no connected sessions (between relay legs,
/// every session claims B's identity, so the previous one must be
/// fully torn down first).
fn wait_quiet(cluster: &Cluster, idx: usize, what: &str) {
    assert!(
        wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
            connected_peers(cluster, idx) == 0
        }),
        "{what}: node{} sessions did not tear down",
        idx + 1
    );
}

#[test]
fn forwarded_origin_survives_offline_origin_and_hostile_relay() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "origin-signatures".into(),
        num_nodes: 3,
        node_ids: vec![NODE_A, NODE_B, NODE_C],
        manual_peers: true,
        ..Default::default()
    })
    .expect("cluster setup");

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        const TARGET: usize = C;
        let dbid = cluster.database_id();

        // Same table on every node (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // A commits; B receives; A stops.
        cluster.add_peer(A, B).expect("mesh A->B");
        cluster.add_peer(B, A).expect("mesh B->A");
        insert(&cluster, A, 1, "from-A");
        wait_names(&cluster, B, &["from-A"], CONVERGE_TIMEOUT);
        cluster.stop_node(A);

        // C learns A's transaction exclusively from B while A stays stopped.
        cluster.add_peer(B, C).expect("mesh B->C");
        cluster.add_peer(C, B).expect("mesh C->B");
        wait_names(&cluster, C, &["from-A"], CONVERGE_TIMEOUT);
        assert_eq!(
            names(cluster.node(B)),
            names(cluster.node(C)),
            "forwarded state differs"
        );

        // Honest baseline on C (Go: `before := status` + honest receipt).
        // The applied row is the receipt proof: receipts commit atomically
        // with the apply, and there is no public receipt read API.
        let base = counters(&cluster, TARGET);
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "honest transaction missing on C before the attacks"
        );

        // B stops; the relay authenticates as B from here on.
        cluster.stop_node(B);
        wait_quiet(&cluster, TARGET, "B shutdown");

        // Relay fixture sharing the cluster CA trust (Go: B's TLS files).
        let issued = cluster.issue_cert(&NODE_B).expect("issue relay cert");
        let relay_config =
            ReplConfig::new("127.0.0.1:0".parse().unwrap(), issued.credentials, FABRICATE_SEED);
        let relay = Relay::new(&relay_config, dbid);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("relay runtime");
        let node_addr = cluster.node_info(TARGET).repl_addr;

        // --- Capture leg: fetch A's genuine batches from C's stored log
        // (Go: reopen A's dir + `ScanReplicationLog`). The served bytes
        // must verify under A's origin key with the origin intact,
        // proving C stores the forwarded origin proof (not a B-minted
        // copy) even though A is offline.
        let mut fetch =
            rt.block_on(relay.dial(node_addr, DIVERGENT_SCHEMA.to_vec(), "fetch"));
        let real_schema = fetch.welcome_schema.clone();
        assert_ne!(
            real_schema,
            DIVERGENT_SCHEMA,
            "node schema equals the divergent bytes (leg vacuous)"
        );
        rt.block_on(fetch.send(
            &Message::Need {
                origin: NODE_A,
                from: 1,
                through: 16,
            },
            "fetch-need",
        ));
        let fetch_frames = rt.block_on(fetch.collect(NEED_WINDOW));
        let mut a_txs: Vec<Transaction> = Vec::new();
        for f in &fetch_frames {
            if let Message::Batches(txs) = f {
                for tx in txs {
                    if tx.origin == NODE_A {
                        a_txs.push(tx.clone());
                    }
                }
            }
        }
        assert!(
            !a_txs.is_empty(),
            "C served no A-origin batches for Need{{A,1..=16}}; saw {}",
            frame_summary(&fetch_frames)
        );
        a_txs.sort_by_key(|tx| tx.sequence);
        a_txs.dedup_by(|a, b| {
            assert_eq!(
                a.digest().expect("digest served tx"),
                b.digest().expect("digest served tx"),
                "C served conflicting bytes for A/{}",
                a.sequence
            );
            a.sequence == b.sequence
        });
        let original = a_txs.last().expect("served A batch").clone();
        let mut trust: BTreeMap<[u8; 16], [u8; 32]> = BTreeMap::new();
        trust.insert(NODE_A, cluster.node_origin_pubkey(A));
        original
            .verify(dbid, &trust, MAX_TX)
            .expect("served A batch must verify under A's origin key");
        eprintln!(
            "captured genuine A/{} ({} A-origin batch{}) for the attack legs",
            original.sequence,
            a_txs.len(),
            if a_txs.len() == 1 { "" } else { "es" }
        );
        fetch.close("fetch");
        wait_quiet(&cluster, TARGET, "fetch session");

        // Fresh sequences per attack (Go: `Sequence++` on every attack).
        let fresh = original.sequence + 100;
        let recorder = Recorder::start(&cluster, TARGET);

        // --- Modified: tampered payload under the original signature
        // (Go: `Mutations[0].Value = "tampered"`).
        let mut modified = original.clone();
        modified.sequence = fresh;
        modified.id = MODIFIED_ID;
        assert!(
            !modified.payload.is_empty(),
            "original has an empty payload (leg vacuous)"
        );
        modified.payload[0] ^= 0x01;
        assert_eq!(
            modified.signature, original.signature,
            "fixture updated the signature it must leave stale"
        );
        let mut s = rt.block_on(relay.dial(node_addr, real_schema.clone(), "modified"));
        let since = Instant::now();
        rt.block_on(s.send(&Message::Batches(vec![modified]), "modified"));
        rt.block_on(assert_stream_dead(&mut s, &HONEST, 0xD1, "modified batch"));
        assert!(
            recorder.saw_since(since, "payload digest mismatch"),
            "node never latched the digest-mismatch cause for the modified batch"
        );
        s.close("modified");
        wait_quiet(&cluster, TARGET, "modified session");
        assert_counters(&cluster, TARGET, &base, "after the modified batch");
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "node3 rows changed after the modified batch"
        );

        // --- Fabricated: re-signed with the wrong key (Go: B's origin
        // key) while claiming origin A, with a bumped clock (Go: HLC++).
        let mut fabricated = original.clone();
        fabricated.sequence = fresh + 1;
        fabricated.id = FABRICATED_ID;
        fabricated.millis = original.millis.wrapping_add(1000);
        fabricated
            .sign(&FABRICATE_SEED)
            .expect("fabricated sign");
        // Non-vacuous: the fabrication is self-consistent under the
        // attacker key, so only the origin/key binding is wrong.
        let mut wrong_trust: BTreeMap<[u8; 16], [u8; 32]> = BTreeMap::new();
        wrong_trust.insert(NODE_A, protocol::public_key(&FABRICATE_SEED));
        fabricated
            .verify(dbid, &wrong_trust, MAX_TX)
            .expect("fabrication must verify under the attacker key locally");
        assert!(
            fabricated.verify(dbid, &trust, MAX_TX).is_err(),
            "fabrication verifies under A's key locally (leg vacuous)"
        );
        let mut s = rt.block_on(relay.dial(node_addr, real_schema.clone(), "fabricated"));
        let since = Instant::now();
        rt.block_on(s.send(&Message::Batches(vec![fabricated]), "fabricated"));
        rt.block_on(assert_stream_dead(&mut s, &HONEST, 0xD2, "fabricated batch"));
        assert!(
            recorder.saw_since(since, "signature error"),
            "node never latched the signature cause for the fabricated batch"
        );
        s.close("fabricated");
        wait_quiet(&cluster, TARGET, "fabricated session");
        assert_counters(&cluster, TARGET, &base, "after the fabricated batch");
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "node3 rows changed after the fabricated batch"
        );

        // --- Unsigned: the signature stripped (Go: SignatureVersion = 0).
        let mut unsigned = original.clone();
        unsigned.sequence = fresh + 2;
        unsigned.id = UNSIGNED_ID;
        unsigned.signature.clear();
        assert!(
            unsigned.verify(dbid, &trust, MAX_TX).is_err(),
            "unsigned batch verifies locally (leg vacuous)"
        );
        let mut s = rt.block_on(relay.dial(node_addr, real_schema.clone(), "unsigned"));
        let since = Instant::now();
        rt.block_on(s.send(&Message::Batches(vec![unsigned]), "unsigned"));
        rt.block_on(assert_stream_dead(&mut s, &HONEST, 0xD3, "unsigned batch"));
        assert!(
            recorder.saw_since(since, "signature error"),
            "node never latched the signature cause for the unsigned batch"
        );
        s.close("unsigned");
        wait_quiet(&cluster, TARGET, "unsigned session");
        assert_counters(&cluster, TARGET, &base, "after the unsigned batch");
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "node3 rows changed after the unsigned batch"
        );

        // --- Cross-DBID: A's signature over a foreign database id.
        // Identity failures are `Error::Protocol`, so unlike the Auth
        // legs above the session must survive (Go: same session).
        let mut cross = original.clone();
        cross.sequence = fresh + 3;
        cross.id = CROSSDB_ID;
        cross.database = CROSS_DB;
        assert!(
            cross.verify(dbid, &trust, MAX_TX).is_err(),
            "cross-DBID batch verifies locally (leg vacuous)"
        );
        let mut s = rt.block_on(relay.dial(node_addr, real_schema.clone(), "cross-db"));
        let since = Instant::now();
        rt.block_on(s.send(&Message::Batches(vec![cross]), "cross-db"));
        let seen = rt.block_on(expect_protocol_error(
            &mut s,
            "transaction identity",
            "cross-DBID batch",
        ));
        assert_no_foreign_data(&seen, &HONEST, "on the cross-DBID session");
        assert!(
            recorder.saw_since(since, "transaction identity"),
            "node never latched the identity cause for the cross-DBID batch"
        );
        // Session alive: the ping round-trips.
        let pong_frames = rt.block_on(expect_pong(&mut s, 0xD4, "ping after cross-DBID"));
        assert_no_foreign_data(&pong_frames, &HONEST, "after the cross-DBID batch");
        s.close("cross-db");
        wait_quiet(&cluster, TARGET, "cross-DBID session");
        assert_counters(&cluster, TARGET, &base, "after the cross-DBID batch");
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "node3 rows changed after the cross-DBID batch"
        );

        // --- Chunk identity: a tampered proof clock (Go: chunk.HLC++)
        // must fail before durable staging or cleanup.
        let chunk_seq = fresh + 4;
        let mut proof = original.clone();
        proof.payload.clear();
        proof.sequence = chunk_seq;
        proof.id = CHUNK_ID;
        proof.millis = original.millis.wrapping_add(1);
        // Signature deliberately NOT updated. Non-vacuous: the
        // untampered cleared-payload proof shape verifies, so the +1ms
        // tamper is the sole rejection cause.
        let mut clean = original.clone();
        clean.payload.clear();
        clean
            .verify_header(dbid, &trust)
            .expect("untampered cleared-payload proof must verify");
        assert!(
            proof.verify_header(dbid, &trust).is_err(),
            "tampered proof verifies locally (leg vacuous)"
        );
        let chunk_data = b"origin-chunk-attack-probe".to_vec();
        let chunk = Chunk {
            proof,
            origin: NODE_A,
            sequence: chunk_seq,
            id: CHUNK_ID,
            index: 0,
            count: 1,
            total: chunk_data.len() as u64,
            digest: Sha256::digest(&chunk_data).into(),
            data: chunk_data,
        };
        // Well-formedness: every structural check passes; only the
        // proof signature is wrong.
        assert_eq!(
            chunk.count as usize,
            (chunk.total as usize).div_ceil(protocol::CHUNK_BYTES),
            "fixture chunk count is structurally wrong"
        );
        let mut s = rt.block_on(relay.dial(node_addr, real_schema.clone(), "chunk"));
        let since = Instant::now();
        rt.block_on(s.send(&Message::TransactionChunk(chunk), "chunk"));
        rt.block_on(assert_stream_dead(&mut s, &HONEST, 0xD5, "tampered chunk"));
        assert!(
            recorder.saw_since(since, "signature error"),
            "node never latched the signature cause for the tampered chunk"
        );
        s.close("chunk");
        wait_quiet(&cluster, TARGET, "chunk session");
        assert_counters(&cluster, TARGET, &base, "after the tampered chunk");
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "node3 rows changed after the tampered chunk"
        );
        // Nothing durably staged: the availability index must not list
        // the attacked transfer (it lists durable chunk manifests).
        let mut probe = rt.block_on(relay.dial(node_addr, real_schema.clone(), "chunk-probe"));
        rt.block_on(probe.send(
            &Message::ChunkAvailabilityRequest { offset: 0 },
            "chunk-probe",
        ));
        let probe_frames = rt.block_on(probe.collect_until(PROBE_TIMEOUT, |m| {
            matches!(m, Message::ChunkAvailabilityPage { .. })
        }));
        match probe_frames.iter().find_map(|m| match m {
            Message::ChunkAvailabilityPage { entries, more } => Some((entries, more)),
            _ => None,
        }) {
            Some((entries, more)) => {
                assert!(
                    entries.is_empty(),
                    "rejected chunk was durably staged: {entries:?}"
                );
                assert!(!more, "unexpected second availability page");
            }
            None => panic!(
                "no chunk-availability page; saw {}",
                frame_summary(&probe_frames)
            ),
        }
        probe.close("chunk-probe");
        wait_quiet(&cluster, TARGET, "chunk-probe session");

        // --- Restart C: the honest row and signed history are durable
        // (Go: `receipt` present after restart). A and B stay stopped.
        recorder.stop();
        cluster.stop_node(TARGET);
        cluster.start_node(TARGET).expect("restart node3");
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "honest row lost on restart"
        );
        // Fresh process: counters restart at zero (verified premise for
        // the idempotency leg below, not an assumption).
        let post = counters(&cluster, TARGET);
        assert_eq!(
            (post.received, post.applied),
            (0, 0),
            "fresh-process counters must start at zero"
        );
        let node_addr = cluster.node_info(TARGET).repl_addr;

        // --- Signed duplicate replay stays idempotent across restart:
        // no error, the session survives, and nothing is counted twice
        // (Go: generation unchanged; Rust dedups before counting, so
        // `received` stays 0 rather than reaching 1).
        let mut replay = rt.block_on(relay.dial(node_addr, real_schema.clone(), "replay"));
        rt.block_on(replay.send(
            &Message::Batches(vec![original.clone()]),
            "replay",
        ));
        // Ordered stream: the pong proves the duplicate was already
        // processed (silently absorbed, not applied).
        let replay_frames = rt.block_on(expect_pong(&mut replay, 0xD6, "replay ping"));
        assert!(
            !replay_frames
                .iter()
                .any(|m| matches!(m, Message::Error { .. })),
            "signed duplicate drew an error frame; saw {}",
            frame_summary(&replay_frames)
        );
        assert_no_foreign_data(&replay_frames, &HONEST, "on the replay session");
        replay.close("replay");
        wait_quiet(&cluster, TARGET, "replay session");
        assert_counters(&cluster, TARGET, &post, "after the signed-duplicate replay");
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "node3 rows changed after the signed-duplicate replay"
        );

        // --- An authenticated relay is not automatically a trusted
        // snapshot source (Go: empty `TrustedSnapshotSources` on C,
        // which is also the harness default).
        let recorder2 = Recorder::start(&cluster, TARGET);
        let snap_data = vec![0x53u8; 4096];
        let snap_schema = protocol::schema_digest(&real_schema);
        let now = retention::now_millis();
        let (manifest, _records) = snapshot::build(
            (dbid, NODE_B),
            NODE_C,
            snap_data.clone(),
            Vec::new(),
            snap_schema,
            &relay_config,
            now,
        )
        .expect("build manifest");
        // Non-vacuous: every manifest check except source trust passes,
        // so the refusal is the trust check alone.
        assert_eq!(manifest.donor, NODE_B, "fixture manifest donor wrong");
        assert_eq!(manifest.recipient, NODE_C, "fixture manifest recipient wrong");
        assert_eq!(manifest.database, dbid, "fixture manifest database wrong");
        assert!(manifest.expires > now, "fixture manifest already expired");
        assert_eq!(manifest.bytes, snap_data.len() as u64, "fixture manifest size wrong");
        assert_eq!(
            manifest.chunks.len(),
            snap_data.len().div_ceil(protocol::CHUNK_BYTES),
            "fixture manifest chunking wrong"
        );
        assert_eq!(
            manifest.digest,
            <[u8; 32]>::from(Sha256::digest(&snap_data)),
            "fixture manifest digest wrong"
        );
        let mut s = rt.block_on(relay.dial(node_addr, real_schema.clone(), "snapshot"));
        let since = Instant::now();
        rt.block_on(s.send(&Message::SnapshotManifest(manifest), "snapshot"));
        rt.block_on(assert_stream_dead(
            &mut s,
            &HONEST,
            0xD7,
            "untrusted snapshot manifest",
        ));
        assert!(
            recorder2.saw_since(since, "snapshot identity, trust or expiry"),
            "node never latched the trust cause for the untrusted manifest"
        );
        s.close("snapshot");
        wait_quiet(&cluster, TARGET, "snapshot session");
        assert_counters(&cluster, TARGET, &post, "after the untrusted snapshot manifest");
        assert_eq!(
            names(cluster.node(TARGET)),
            vec!["from-A".to_string()],
            "node3 rows changed after the untrusted snapshot manifest"
        );
        recorder2.stop();
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("origin-signatures");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
