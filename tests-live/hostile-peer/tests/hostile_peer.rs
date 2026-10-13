//! Rust port of the Go `hostile-peer` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/hostile-peer/`
//! (`attacker.go` + `hostile_peer_test.go`:
//! `TestHostilePeerAttacksRejected`). A malicious authenticated peer
//! (CA-issued QUIC cert, untrusted origin key) fires forged batches,
//! invalid identities, a replayed handshake, a 256 MiB
//! allocation-claim frame, a compressed snapshot bomb, and a
//! far-future-HLC batch at an honest 2-node mesh. Every attack must be
//! rejected (observed on the wire and in product latches/counters,
//! never assumed), nothing foreign may apply, and the honest pair must
//! stay converged and responsive throughout.
//!
//! Plain SQL replaces the Go typed-records HTTP API, and the Go
//! Prometheus counters map to replication `Status` observables; see
//! the scenario README.md for the full deviation list.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use murmur_live_harness::murmur_replication::{
    decode, encode,
    protocol::{self, Message, Transaction},
    transport, Config as ReplConfig, Dissemination, NodeId,
};
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Attacker node id: fixed test id, asserted distinct from the node ids.
const ATTACKER_ID: [u8; 16] = [0xA0; 16];
/// Attacker origin signing key. CA-issued TLS cert, but this key is in
/// NOBODY's `trusted_origins`, so every batch it signs must be refused
/// as `untrusted transaction origin`.
const ATTACKER_SEED: [u8; 32] = [0xA1; 32];
/// Divergent handshake schema bytes (Go: schema epoch 9999). Tolerated
/// at attach (`accept_remote_schema`), never adopted without a signed
/// manifest.
const DIVERGENT_SCHEMA: &[u8] = b"attacker-divergent-schema-v9999";
/// Wire discriminants (`Message::kind` is private; values from
/// `protocol.rs`, proven by the control leg's Welcome below).
const BATCHES_KIND: u16 = 3;
const SNAPSHOT_CHUNK_KIND: u16 = 8;
/// `protocol::write` compressed-frame flag (values from `protocol.rs`).
const COMPRESSED_FLAG: u16 = 0x8002;

/// Convergence deadline per phase (Go: 30 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// Fixture dial/stream/setup deadline.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the fixture waits for a Welcome that must arrive.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the fixture waits for an `Error{code: 1}` reply (Go: 10 s).
const ERROR_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the fixture waits for a Pong that must arrive (Go: 10 s).
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
/// Slice for one gradual read inside a collection window (Go: 500 ms).
const SLICE: Duration = Duration::from_millis(500);
/// Opening-chatter drain while waiting for attach-time requests.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
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

/// Row count on one node.
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

/// Wait until both honest nodes hold `want` rows with equal names
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let (n0, n1) = (names(cluster.node(0)), names(cluster.node(1)));
        if n0.len() == want && n0 == n1 {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "nodes did not converge on {want} rows with equal names within {timeout:?} \
                 (node0={n0:?} node1={n1:?})"
            );
        }
        std::thread::sleep(CONVERGE_POLL);
    }
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
/// assert; `applied` covers Go's nothing-applied leg.
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

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock")
        .as_millis() as u64
}

/// One attacker-signed transaction. The signature is always valid for
/// `ATTACKER_SEED`; only cluster trust (or the deliberately broken
/// identity field) is missing, so rejection is authorization shape,
/// not malformedness.
fn attacker_tx(
    db: [u8; 16],
    origin: [u8; 16],
    seq: u64,
    schema: [u8; 32],
    payload: Vec<u8>,
    millis: u64,
) -> Transaction {
    let mut tx = Transaction {
        database: db,
        origin,
        sequence: seq,
        id: [0xB0; 16],
        millis,
        logical: 0,
        schema,
        payload_digest: [0; 32],
        payload,
        signature: Vec::new(),
    };
    tx.sign(&ATTACKER_SEED).expect("attacker sign");
    tx
}

/// Non-vacuous guard: the payload encode/decode round-trips, so live
/// rejection is product validation, not a corrupt test payload.
fn assert_roundtrips(tx: &Transaction, what: &str) {
    let bytes = encode(tx).unwrap_or_else(|e| panic!("{what} does not encode: {e}"));
    let back: Transaction =
        decode(&bytes).unwrap_or_else(|e| panic!("{what} does not decode locally: {e}"));
    assert!(
        back.origin == tx.origin && back.sequence == tx.sequence && back.payload == tx.payload,
        "{what} did not survive a local round-trip"
    );
}

/// Non-vacuous guard: the transaction verifies cleanly against a trust
/// map that DOES contain the attacker key, proving the live rejection
/// is origin authorization (missing cluster trust), not a bad
/// signature or malformed envelope.
fn assert_authorized_shape(tx: &Transaction, db: [u8; 16], max: usize, what: &str) {
    let mut trust: BTreeMap<NodeId, [u8; 32]> = BTreeMap::new();
    trust.insert(ATTACKER_ID, protocol::public_key(&ATTACKER_SEED));
    tx.verify(db, &trust, max)
        .unwrap_or_else(|e| panic!("{what} is not even self-consistent: {e}"));
}

fn is_pong(nonce: u64) -> impl Fn(&Message) -> bool {
    move |m| matches!(m, Message::Pong(n) if *n == nonce)
}

fn is_error_code(want: u16) -> impl Fn(&Message) -> bool {
    move |m| matches!(m, Message::Error { code: c, .. } if *c == want)
}

fn is_schema_request(m: &Message) -> bool {
    matches!(m, Message::SchemaRequest)
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

/// The Rust engine does not data-gate divergent-schema peers (pushes
/// are not gated on schema agreement), so instead of Go's "no data
/// frames" the port asserts every data frame on the attacker session
/// carries honest origins only: nothing attacker-controlled is ever
/// echoed or relayed back.
fn assert_no_foreign_data(frames: &[Message], honest: &[[u8; 16]], ctx: &str) {
    for f in frames {
        match f {
            Message::Batches(txs) => {
                for tx in txs {
                    assert!(
                        honest.contains(&tx.origin),
                        "attacker received a batch with foreign origin {} {ctx}",
                        hex(&tx.origin)
                    );
                }
            }
            Message::PlumtreeData(tx) => {
                assert!(
                    honest.contains(&tx.origin),
                    "attacker received plumtree data with foreign origin {} {ctx}",
                    hex(&tx.origin)
                );
            }
            Message::TransactionChunk(c) => {
                assert!(
                    honest.contains(&c.proof.origin),
                    "attacker received a chunk with foreign origin {} {ctx}",
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

/// Raw QUIC attacker speaking the replication wire protocol with a
/// CA-issued certificate (Go: `attacker.go`). Shares the cluster CA
/// trust, so TLS can never be the refusal cause.
struct Attacker {
    endpoint: quinn::Endpoint,
    db: [u8; 16],
}

/// One attack session: the handshake stream stays open for further
/// attack frames and for observing node replies (Go: `evilSession`).
struct EvilSession {
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    welcome_schema: Vec<u8>,
}

impl Attacker {
    fn new(config: ReplConfig, db: [u8; 16]) -> Self {
        let endpoint = transport::endpoint(&config, ATTACKER_ID).expect("fixture endpoint");
        Self { endpoint, db }
    }

    fn hello(&self, schema: Vec<u8>) -> Message {
        Message::Hello {
            capabilities: protocol::CAPABILITIES,
            required: protocol::REQUIRED,
            version: protocol::VERSION,
            database: self.db,
            node: ATTACKER_ID,
            fetch: None,
            schema,
            mode: Dissemination::Gossip,
            max_transaction: 64 << 20,
        }
    }

    /// Dial advertising the given schema identity; the Welcome assert
    /// is the attach proof (Go `sessions_opened` legs have no Rust
    /// cumulative counter, so every dial carries its own proof).
    async fn dial(&self, addr: SocketAddr, schema: Vec<u8>, what: &str) -> EvilSession {
        let connection = tokio::time::timeout(
            DIAL_TIMEOUT,
            self.endpoint
                .connect(addr, "murmur.invalid")
                .expect("attacker connect setup"),
        )
        .await
        .unwrap_or_else(|_| panic!("{what}: attacker dial timeout"))
        .unwrap_or_else(|e| panic!("{what}: attacker connect: {e}"));
        let (mut send, mut recv) = tokio::time::timeout(DIAL_TIMEOUT, connection.open_bi())
            .await
            .unwrap_or_else(|_| panic!("{what}: attacker open stream timeout"))
            .unwrap_or_else(|e| panic!("{what}: attacker open stream: {e}"));
        protocol::write(&mut send, &self.hello(schema))
            .await
            .unwrap_or_else(|e| panic!("{what}: attacker write hello: {e}"));
        let welcome_schema =
            match tokio::time::timeout(WELCOME_TIMEOUT, protocol::read(&mut recv)).await {
                Ok(Ok(Message::Welcome { schema, .. })) => schema,
                other => panic!("{what}: attacker expected Welcome, got {other:?}"),
            };
        EvilSession {
            connection,
            send,
            recv,
            welcome_schema,
        }
    }
}

impl EvilSession {
    async fn send(&mut self, msg: &Message, what: &str) {
        protocol::write(&mut self.send, msg)
            .await
            .unwrap_or_else(|e| panic!("{what}: attacker send {}: {e}", msg_name(msg)));
    }

    /// Read inbound messages until `total` elapses or the stream dies
    /// (Go: `collect`).
    async fn collect(&mut self, total: Duration) -> Vec<Message> {
        self.collect_until(total, |_| false).await
    }

    /// Read until `want` matches, `total` elapses, or the stream dies
    /// (Go: `collectUntil`).
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

    /// One raw frame with an explicit body (mirrors the
    /// `protocol::write` 12-byte header layout).
    async fn send_raw(&mut self, kind: u16, flags: u16, body: &[u8], what: &str) {
        let mut h = [0u8; 12];
        h[..2].copy_from_slice(b"RD");
        h[2..4].copy_from_slice(&protocol::VERSION.to_be_bytes());
        h[4..6].copy_from_slice(&kind.to_be_bytes());
        h[6..8].copy_from_slice(&flags.to_be_bytes());
        h[8..12].copy_from_slice(&(body.len() as u32).to_be_bytes());
        self.send
            .write_all(&h)
            .await
            .unwrap_or_else(|e| panic!("{what}: raw header write: {e}"));
        self.send
            .write_all(body)
            .await
            .unwrap_or_else(|e| panic!("{what}: raw body write: {e}"));
    }

    /// A bare 12-byte header claiming `claimed_len` with no body behind
    /// it (Go P3 writes the evil header only).
    async fn send_alloc_claim(&mut self, kind: u16, claimed_len: u32, what: &str) {
        let mut h = [0u8; 12];
        h[..2].copy_from_slice(b"RD");
        h[2..4].copy_from_slice(&protocol::VERSION.to_be_bytes());
        h[4..6].copy_from_slice(&kind.to_be_bytes());
        h[6..8].copy_from_slice(&0u16.to_be_bytes());
        h[8..12].copy_from_slice(&claimed_len.to_be_bytes());
        self.send
            .write_all(&h)
            .await
            .unwrap_or_else(|e| panic!("{what}: evil header write: {e}"));
    }

    fn close(self, what: &str) {
        self.connection.close(0u32.into(), format!("{what} done").as_bytes());
    }
}

/// Background `last_error` recorder. Honest nodes dial the attacker
/// address back on the 5 s bootstrap tick and those stray dials time
/// out into `handshake timeout` latches that can overwrite a single
/// sample, so Auth-cause legs record every latch transition on a
/// 10 ms poll and match by timestamp instead.
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
/// foreign data. Stray repeats of attach-time control chatter are
/// tolerated but logged (Go: `assertStreamDead`).
async fn assert_stream_dead(
    sess: &mut EvilSession,
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

/// Expect one `Error{code: 1}` whose text contains `needle` (Go: the
/// mid-session `ErrBadMessage` legs). Returns all frames seen.
async fn expect_protocol_error(
    sess: &mut EvilSession,
    needle: &str,
    what: &str,
) -> Vec<Message> {
    let frames = sess.collect_until(ERROR_TIMEOUT, is_error_code(1)).await;
    match last_error_text(&frames) {
        Some((1, text)) if text.contains(needle) => frames,
        other => panic!("{what} drew no Error{{code: 1, ..{needle}..}}; saw {other:?}"),
    }
}

/// Expect a pong to `nonce` (Go: `assertSaw` on pongs).
async fn expect_pong(sess: &mut EvilSession, nonce: u64, what: &str) -> Vec<Message> {
    sess.send(&Message::Ping(nonce), what).await;
    let frames = sess.collect_until(PONG_TIMEOUT, is_pong(nonce)).await;
    assert!(
        frames.iter().any(is_pong(nonce)),
        "missing pong to {what}; saw {}",
        frame_summary(&frames)
    );
    frames
}

#[test]
fn hostile_peer_attacks_rejected() {
    // Fixed node ids (asserted distinct from the attacker id) so no
    // handshake identity can alias.
    let node_ids = vec![[0x10; 16], [0x20; 16]];
    assert!(
        !node_ids.contains(&ATTACKER_ID),
        "attacker id collides with a node id"
    );
    // 1 MiB transaction cap: keeps the oversized leg small without
    // affecting the tiny honest writes.
    const MAX_TX: usize = 1 << 20;
    let cluster = Cluster::new(ClusterOptions {
        name: "hostile-peer".into(),
        num_nodes: 2,
        node_ids,
        replication: Some(ReplicationOptions {
            max_transaction_bytes: Some(MAX_TX),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Attacker aims at node0; node1 is the honest witness.
        const TARGET: usize = 0;
        let honest: Vec<[u8; 16]> = (0..2).map(|i| cluster.node_id(i)).collect();
        let dbid = cluster.database_id();

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

        // Positive control (pre-attack): honest writes replicate both ways.
        insert(&cluster, 0, 1, "honest-1");
        wait_converged(&cluster, 1, CONVERGE_TIMEOUT);
        insert(&cluster, 1, 2, "honest-2");
        wait_converged(&cluster, 2, CONVERGE_TIMEOUT);
        let pre_attack = names(cluster.node(TARGET));
        let base = counters(&cluster, TARGET);

        // Attacker fixture sharing the cluster CA trust.
        let issued = cluster.issue_cert(&ATTACKER_ID).expect("issue attacker cert");
        let attacker_config =
            ReplConfig::new("127.0.0.1:0".parse().unwrap(), issued.credentials, ATTACKER_SEED);
        let attacker = Attacker::new(attacker_config, dbid);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fixture runtime");
        let node_addr = cluster.node_info(TARGET).repl_addr;
        let divergent = DIVERGENT_SCHEMA.to_vec();
        let mut handshakes = 0u32;

        // Learn the node's real schema bytes from a divergent dial.
        let learn = rt.block_on(attacker.dial(node_addr, divergent.clone(), "schema-learn"));
        handshakes += 1;
        let real_schema = learn.welcome_schema.clone();
        assert_ne!(
            real_schema, divergent,
            "node schema equals the divergent bytes (leg vacuous)"
        );
        let real_digest = protocol::schema_digest(&real_schema);
        learn.close("schema-learn");
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 1
            }),
            "learn session did not tear down before the control"
        );

        // Positive control: the same fixture with the node's real
        // schema MUST be welcomed with no SchemaRequest. Proves TLS
        // trust, identity, the hand-rolled framer, and the
        // matched-schema bytes are all correct, so later rejections
        // are caused by the attacks alone.
        let mut control =
            rt.block_on(attacker.dial(node_addr, real_schema.clone(), "control"));
        handshakes += 1;
        let control_frames = rt.block_on(control.collect(Duration::from_secs(2)));
        assert!(
            !control_frames.iter().any(is_schema_request),
            "control session drew a SchemaRequest (schema bytes wrong); saw {}",
            frame_summary(&control_frames)
        );
        let pong_frames = rt.block_on(expect_pong(&mut control, 7, "control ping"));
        assert_no_foreign_data(&pong_frames, &honest, "on the control session");
        control.close("control");
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 1
            }),
            "control session did not tear down before the attacks"
        );
        eprintln!("control: matched-schema hello welcomed (fixture identity OK)");

        // Latch recorder for the Auth-cause legs (see `Recorder`).
        let recorder = Recorder::start(&cluster, TARGET);

        // --- P1a: invalid identities on one watched divergent session.
        // All four are `Error::Protocol`: each must draw Error{code: 1}
        // with the session surviving.
        let mut watched =
            rt.block_on(attacker.dial(node_addr, divergent.clone(), "watched"));
        handshakes += 1;
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 2
            }),
            "attack handshake never attached (peers != 2)"
        );
        let mut seen: Vec<Message> = Vec::new();
        let forged_digest = [0xA0; 32]; // Go: epoch 4242 + random hash

        // Zero sequence.
        let zero_seq = attacker_tx(dbid, ATTACKER_ID, 0, forged_digest, b"zero-seq".to_vec(), now_millis());
        assert_roundtrips(&zero_seq, "zero-sequence batch");
        rt.block_on(watched.send(&Message::Batches(vec![zero_seq]), "zero-seq"));
        seen.extend(rt.block_on(expect_protocol_error(
            &mut watched,
            "transaction identity",
            "zero-sequence batch",
        )));

        // Wrong database id.
        let wrong_db = attacker_tx(
            [0xDD; 16],
            ATTACKER_ID,
            7,
            forged_digest,
            b"wrong-db".to_vec(),
            now_millis(),
        );
        assert_roundtrips(&wrong_db, "wrong-database batch");
        rt.block_on(watched.send(&Message::Batches(vec![wrong_db]), "wrong-db"));
        seen.extend(rt.block_on(expect_protocol_error(
            &mut watched,
            "transaction identity",
            "wrong-database batch",
        )));

        // Oversized payload: 2 MiB against the 1 MiB cap.
        let big = attacker_tx(
            dbid,
            ATTACKER_ID,
            8,
            forged_digest,
            vec![0x55; 2 << 20],
            now_millis(),
        );
        assert_roundtrips(&big, "oversized batch");
        rt.block_on(watched.send(&Message::Batches(vec![big]), "oversized"));
        seen.extend(rt.block_on(expect_protocol_error(
            &mut watched,
            "transaction identity",
            "oversized batch",
        )));

        // Invalid missing range (stands in for Go's zero-HLC leg: the
        // Rust verifier does not gate on HLC, so a structurally valid
        // frame with invalid content takes its place).
        rt.block_on(watched.send(
            &Message::Need { origin: ATTACKER_ID, from: 0, through: 5 },
            "bad-need",
        ));
        seen.extend(rt.block_on(expect_protocol_error(
            &mut watched,
            "invalid missing range",
            "zero-from Need",
        )));

        // Session must be alive: ping round-trips.
        seen.extend(rt.block_on(expect_pong(&mut watched, 11, "ping-1 after invalid batches")));

        // --- P2: replay the handshake on the live session. Hello is
        // only valid as the first frame; mid-session it must draw an
        // error without killing the session.
        rt.block_on(watched.send(&attacker.hello(divergent.clone()), "replay-hello"));
        seen.extend(rt.block_on(expect_protocol_error(
            &mut watched,
            "repeated handshake",
            "replayed handshake",
        )));

        // Session still alive after the replay.
        seen.extend(rt.block_on(expect_pong(
            &mut watched,
            22,
            "ping-2 after replayed handshake",
        )));
        // Final drain: give the node a window to (wrongly) push foreign data.
        seen.extend(rt.block_on(watched.collect(Duration::from_secs(2))));
        assert_no_foreign_data(&seen, &honest, "on the watched session");
        eprintln!(
            "watched session carried only honest/control frames: {}",
            frame_summary(&seen)
        );
        watched.close("watched");
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 1
            }),
            "watched session did not tear down"
        );

        // --- P1b: forged matched-schema batch from the untrusted
        // origin. Correctly signed and schema-matched, so only origin
        // trust is missing; `Error::Auth` kills the session after the
        // latch (unlike Go's non-fatal origin-unknown path, hence the
        // separate session).
        let forged = attacker_tx(dbid, ATTACKER_ID, 1, real_digest, b"forged".to_vec(), now_millis());
        assert_roundtrips(&forged, "forged batch");
        assert_authorized_shape(&forged, dbid, MAX_TX, "forged batch");
        let since_forged = Instant::now();
        let mut evil = rt.block_on(attacker.dial(node_addr, divergent.clone(), "forged"));
        handshakes += 1;
        rt.block_on(evil.send(&Message::Batches(vec![forged]), "forged"));
        rt.block_on(assert_stream_dead(&mut evil, &honest, 0xF0, "forged batch"));
        assert!(
            recorder.saw_since(since_forged, "untrusted transaction origin"),
            "node never latched the untrusted-origin cause for the forged batch"
        );
        evil.close("forged");
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 1
            }),
            "forged session did not tear down"
        );

        // Rejection counters unmoved; no attack batch applied.
        assert_counters(&cluster, TARGET, &base, "after forged-batch attacks");
        assert_eq!(
            names(cluster.node(TARGET)),
            pre_attack,
            "node1 names changed after forged-batch attacks"
        );
        assert_eq!(
            row_count(cluster.node(TARGET)),
            2,
            "node1 row count changed after forged-batch attacks"
        );
        assert_eq!(
            names(cluster.node(1)),
            pre_attack,
            "witness names changed after forged-batch attacks"
        );

        // --- P3: 256 MiB allocation-claim frame. Must fail fast
        // without a huge alloc; the stream dies and the node stays up.
        let mut claim = rt.block_on(attacker.dial(node_addr, divergent.clone(), "alloc-claim"));
        handshakes += 1;
        // Drain the opening control chatter so post-attack reads are
        // not polluted by it.
        rt.block_on(claim.collect_until(DRAIN_TIMEOUT, is_schema_request));
        rt.block_on(claim.send_alloc_claim(BATCHES_KIND, 256 << 20, "alloc-claim"));
        rt.block_on(assert_stream_dead(&mut claim, &honest, 0xDEAD, "256MiB allocation claim"));
        claim.close("alloc-claim");
        // The node itself survives: a fresh session pings fine.
        let mut probe = rt.block_on(attacker.dial(node_addr, divergent.clone(), "probe-1"));
        handshakes += 1;
        rt.block_on(expect_pong(&mut probe, 33, "probe after 256MiB claim"));
        probe.close("probe-1");

        // --- P4: compressed snapshot bomb. A tiny zstd frame expanding
        // to megabytes of undecodable chunk bytes; the stream must
        // die, not the node.
        let mut bomb = rt.block_on(attacker.dial(node_addr, divergent.clone(), "bomb"));
        handshakes += 1;
        let bomb_raw = vec![0xFF; 4 << 20];
        let compressed =
            zstd::stream::encode_all(&bomb_raw[..], 3).expect("bomb compress");
        assert!(
            compressed.len() < bomb_raw.len() / 4,
            "bomb does not compress: {} -> {}",
            bomb_raw.len(),
            compressed.len()
        );
        // Non-vacuous guard: the bomb decompresses locally but never
        // decodes as a message, so rejection is by product
        // bounds/validation, not a corrupt test payload.
        let mut plain = Vec::new();
        {
            use std::io::Read as _;
            zstd::stream::read::Decoder::new(&compressed[..])
                .expect("bomb decoder")
                .read_to_end(&mut plain)
                .expect("bomb decompress");
        }
        assert_eq!(plain.len(), bomb_raw.len(), "bomb size changed locally");
        assert!(
            decode::<Message>(&plain).is_err(),
            "bomb unexpectedly decodes as a message"
        );
        rt.block_on(bomb.collect_until(DRAIN_TIMEOUT, is_schema_request));
        let mut body = (bomb_raw.len() as u32).to_be_bytes().to_vec();
        body.extend_from_slice(&compressed);
        rt.block_on(bomb.send_raw(SNAPSHOT_CHUNK_KIND, COMPRESSED_FLAG, &body, "bomb"));
        rt.block_on(assert_stream_dead(&mut bomb, &honest, 0xB0B, "snapshot bomb"));
        bomb.close("bomb");
        let mut probe2 = rt.block_on(attacker.dial(node_addr, divergent.clone(), "probe-2"));
        handshakes += 1;
        rt.block_on(expect_pong(&mut probe2, 44, "probe after snapshot bomb"));
        probe2.close("probe-2");

        // --- P-HLC: unsigned far-future input from an untrusted origin
        // must be rejected before HLC observation (`stage` verifies
        // before `observe`); the mesh stays responsive.
        let mut hlc = rt.block_on(attacker.dial(node_addr, divergent.clone(), "hlc"));
        handshakes += 1;
        // The served schema is stable: the Welcome bytes match the
        // pre-attack capture (Go resolves live ids from the served
        // manifest the same way).
        assert_eq!(
            hlc.welcome_schema, real_schema,
            "served schema moved during the attacks"
        );
        let far_future = now_millis() + 10 * 365 * 24 * 3600 * 1000;
        let hlc_batch = attacker_tx(
            dbid,
            ATTACKER_ID,
            1,
            real_digest,
            b"far-future-hlc".to_vec(),
            far_future,
        );
        assert_roundtrips(&hlc_batch, "far-future batch");
        assert_authorized_shape(&hlc_batch, dbid, MAX_TX, "far-future batch");
        let since_hlc = Instant::now();
        rt.block_on(hlc.send(&Message::Batches(vec![hlc_batch]), "hlc"));
        // Round-trip before closing: the ordered stream guarantees the
        // node processed the batch before the death assert runs
        // (closing immediately could drop the batch in flight).
        rt.block_on(assert_stream_dead(&mut hlc, &honest, 55, "far-future batch"));
        assert!(
            recorder.saw_since(since_hlc, "untrusted transaction origin"),
            "node never latched the untrusted-origin cause for the far-future batch"
        );
        hlc.close("hlc");
        // The untrusted origin is rejected before HLC observation; the
        // honest pair stays unchanged.
        wait_converged(&cluster, 2, CONVERGE_TIMEOUT);

        // Every attack handshake attached (each dial asserts its own
        // Welcome; `peers == 2` was observed on the watched session).
        assert_eq!(handshakes, 9, "handshake count drifted (attach proof)");
        eprintln!("{handshakes} attack handshakes attached and were answered");

        // --- Positive control (post-attack): the pair stays responsive
        // and converged; schema identity untouched by the attacks.
        let final_probe =
            rt.block_on(attacker.dial(node_addr, divergent.clone(), "final-schema"));
        handshakes += 1;
        assert_eq!(
            final_probe.welcome_schema, real_schema,
            "schema bytes moved during the attacks"
        );
        final_probe.close("final-schema");
        assert_eq!(handshakes, 10, "handshake count drifted (attach proof)");
        assert_counters(&cluster, TARGET, &base, "across all attacks");
        assert_eq!(
            names(cluster.node(TARGET)),
            pre_attack,
            "node1 names changed across the attacks"
        );
        insert(&cluster, 1, 3, "post-attack-1");
        wait_converged(&cluster, 3, CONVERGE_TIMEOUT);
        insert(&cluster, 0, 4, "post-attack-2");
        wait_converged(&cluster, 4, CONVERGE_TIMEOUT);

        recorder.stop();
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("hostile-peer");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
