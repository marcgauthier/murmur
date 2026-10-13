//! Rust port of the Go `hostile-schema` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/hostile-schema/`
//! (`attacker.go` + `hostile_schema_test.go`:
//! `TestHostileSchemaManifestsQuarantined`). A malicious authenticated
//! peer (CA-issued QUIC cert, untrusted origin key) serves hostile
//! signed schema manifests at an honest 3-node mesh: a revision with a
//! corrupted signature, a self-consistent revision from the untrusted
//! signer, an oversized revision, and a zero-sequence revision. Every
//! forgery must be refused (observed on the wire and in the
//! `last_error` latch, never assumed), never adopted, and the honest
//! mesh must keep converging with equal digests throughout.
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
use std::time::{Duration, Instant};

use murmur_live_harness::murmur_replication::{
    decode, encode,
    protocol::{self, Message, SignedSchema},
    transport, Config as ReplConfig, Dissemination,
};
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Attacker node id: fixed test id, asserted distinct from the node ids.
const ATTACKER_ID: [u8; 16] = [0xA0; 16];
/// Attacker origin signing key. CA-issued TLS cert, but this key is in
/// NOBODY's `trusted_origins`, so every manifest it signs must be
/// refused as `untrusted schema signer`.
const ATTACKER_SEED: [u8; 32] = [0xA1; 32];
/// Divergent handshake schema bytes (Go: schema epoch 9999). Tolerated
/// at attach (`accept_remote_schema`), never adopted without a signed
/// manifest.
const DIVERGENT_SCHEMA: &[u8] = b"attacker-divergent-schema-v9999";
/// Schema bytes carried by attacker-signed manifests: well-formed on
/// the wire, never the node's canonical bytes.
const EVIL_SCHEMA: &[u8] = b"attacker-evil-schema-v9999";

/// Convergence deadline per phase (Go: 30 s).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 200 ms).
const CONVERGE_POLL: Duration = Duration::from_millis(200);
/// Fixture dial/stream/setup deadline.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the fixture waits for a Welcome that must arrive.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the fixture waits for a served schema manifest (Go: 15 s).
const MANIFEST_TIMEOUT: Duration = Duration::from_secs(15);
/// How long the fixture waits for a Pong that must arrive (Go: 10 s).
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
/// Slice for one gradual read inside a collection window (Go: 500 ms).
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

/// Wait until all three honest nodes hold `want` rows with equal names
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let (n0, n1, n2) = (
            names(cluster.node(0)),
            names(cluster.node(1)),
            names(cluster.node(2)),
        );
        if n0.len() == want && n0 == n1 && n1 == n2 {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "nodes did not converge on {want} rows with equal names within {timeout:?} \
                 (node0={n0:?} node1={n1:?} node2={n2:?})"
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

/// Adopted-schema counter on one node. Only moves when a received
/// manifest passes verification AND the adoption decision, so it is the
/// Rust equivalent of Go's `manifests_received` / `adoptions`
/// must-not-move asserts.
fn schema_adoptions(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics
        .schema_adoptions
}

fn is_pong(nonce: u64) -> impl Fn(&Message) -> bool {
    move |m| matches!(m, Message::Pong(n) if *n == nonce)
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

    /// Ask the node for its current schema tip (Go: `fetchManifest`;
    /// schema traffic flows even on divergent sessions).
    async fn fetch_manifest(&mut self, what: &str) -> SignedSchema {
        self.send(&Message::SchemaRequest, what).await;
        let frames = self
            .collect_until(MANIFEST_TIMEOUT, |m| {
                matches!(m, Message::SchemaManifest(_))
            })
            .await;
        for f in frames {
            if let Message::SchemaManifest(s) = f {
                return s;
            }
        }
        panic!("{what}: node served no schema manifest");
    }

    fn close(self, what: &str) {
        self.connection.close(0u32.into(), format!("{what} done").as_bytes());
    }
}

/// Background `last_error` recorder. Honest nodes dial the attacker
/// address back on discovered-membership ticks and those stray dials
/// time out into `handshake timeout` latches that can overwrite a
/// single sample, so refusal-cause legs record every latch transition
/// on a 10 ms poll and match by timestamp instead.
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
/// never answers it and never acked the forgery: no pong (the ping
/// was never processed) and no `SchemaAck` (the manifest never
/// reached the adoption decision). Stray repeats of attach-time
/// control chatter are tolerated but logged (Go: `assertStreamDead`).
async fn assert_stream_dead(sess: &mut EvilSession, nonce: u64, what: &str) {
    let _ = protocol::write(&mut sess.send, &Message::Ping(nonce)).await;
    let frames = sess.collect(DEATH_WINDOW).await;
    for f in &frames {
        if matches!(f, Message::Pong(n) if *n == nonce) {
            panic!("node answered ping after {what} (stream should be dead)");
        }
        if matches!(f, Message::SchemaAck { .. }) {
            panic!("node acked the forged manifest after {what} (saw {})", frame_summary(&frames));
        }
    }
    eprintln!("after {what} the node sent only: {}", frame_summary(&frames));
}

/// Expect a pong to `nonce` (Go: liveness probes after fatal attacks).
async fn expect_pong(sess: &mut EvilSession, nonce: u64, what: &str) {
    sess.send(&Message::Ping(nonce), what).await;
    let frames = sess.collect_until(PONG_TIMEOUT, is_pong(nonce)).await;
    assert!(
        frames.iter().any(is_pong(nonce)),
        "missing pong to {what}; saw {}",
        frame_summary(&frames)
    );
}

/// One attacker-signed manifest. Correctly signed for `ATTACKER_SEED`
/// unless the caller corrupts it afterwards, so refusal is product
/// validation, not malformedness.
fn attacker_manifest(db: [u8; 16], sequence: u64, schema: Vec<u8>) -> SignedSchema {
    let mut signed = SignedSchema {
        database: db,
        signer: ATTACKER_ID,
        sequence,
        schema,
        signature: Vec::new(),
    };
    signed.sign(&ATTACKER_SEED).expect("attacker sign");
    signed
}

/// Non-vacuous guard: the manifest verifies cleanly against a trust
/// map that DOES contain the attacker key, proving the live refusal
/// is cluster authorization (missing trust), not a bad signature or
/// malformed envelope.
fn assert_authorized_shape(signed: &SignedSchema, db: [u8; 16], max: usize, what: &str) {
    let mut trust: BTreeMap<[u8; 16], [u8; 32]> = BTreeMap::new();
    trust.insert(ATTACKER_ID, protocol::public_key(&ATTACKER_SEED));
    signed
        .verify(db, ATTACKER_ID, &trust, max)
        .unwrap_or_else(|e| panic!("{what} is not even self-consistent: {e}"));
}

#[test]
fn hostile_schema_manifests_quarantined() {
    // Fixed node ids (asserted distinct from the attacker id) so no
    // handshake identity can alias.
    let node_ids = vec![[0x10; 16], [0x20; 16], [0x30; 16]];
    assert!(
        !node_ids.contains(&ATTACKER_ID),
        "attacker id collides with a node id"
    );
    // 1 MiB transaction cap: keeps the oversized leg small without
    // affecting the tiny honest writes.
    const MAX_TX: usize = 1 << 20;
    let cluster = Cluster::new(ClusterOptions {
        name: "hostile-schema".into(),
        num_nodes: 3,
        node_ids,
        replication: Some(ReplicationOptions {
            max_transaction_bytes: Some(MAX_TX),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Attacker aims at node0; node1/node2 are the honest witnesses.
        const TARGET: usize = 0;
        let dbid = cluster.database_id();
        let target_id = cluster.node_id(TARGET);

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

        // Positive control (pre-attack): the honest 3-mesh converges.
        insert(&cluster, 0, 1, "baseline");
        wait_converged(&cluster, 1, CONVERGE_TIMEOUT);
        let pre_attack = names(cluster.node(TARGET));
        let base_adoptions = schema_adoptions(&cluster, TARGET);

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

        // Latch recorder for the refusal-cause legs (see `Recorder`).
        let recorder = Recorder::start(&cluster, TARGET);

        // --- S1: forged revision with a corrupted signature.
        // Fetch the served tip, then flip bytes inside the signature so
        // the frame stays structurally valid but authentication fails.
        let mut bad_digest = rt.block_on(attacker.dial(node_addr, divergent.clone(), "digest-forge"));
        handshakes += 1;
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 3
            }),
            "attack handshake never attached (peers != 3)"
        );
        let served_schema = bad_digest.welcome_schema.clone();
        let served = rt.block_on(bad_digest.fetch_manifest("digest-forge"));
        assert_eq!(
            served.signer, target_id,
            "served manifest is not signed by the target node"
        );
        assert_eq!(
            served.schema, served_schema,
            "served manifest bytes differ from the advertised Welcome bytes"
        );
        assert!(
            served.sequence > 0,
            "served manifest has a zero sequence (leg vacuous)"
        );
        let mut forged = served.clone();
        forged.signature[0] ^= 0xFF;
        forged.signature[17] ^= 0x01;
        // Non-vacuous guards: the corrupted bytes still encode/decode
        // as a message (the forgery targets the digest check, not the
        // framing) but must fail local authentication.
        let wire =
            encode(&Message::SchemaManifest(forged.clone())).expect("forged manifest encodes");
        let back: Message =
            decode(&wire).expect("forged manifest still decodes (framing intact)");
        assert!(
            matches!(back, Message::SchemaManifest(_)),
            "forged frame did not survive a local round-trip"
        );
        let mut target_trust: BTreeMap<[u8; 16], [u8; 32]> = BTreeMap::new();
        target_trust.insert(target_id, cluster.node_origin_pubkey(TARGET));
        assert!(
            forged
                .verify(dbid, forged.signer, &target_trust, MAX_TX)
                .is_err(),
            "corrupted manifest still verifies locally; forgery is vacuous"
        );
        let since_s1 = Instant::now();
        rt.block_on(bad_digest.send(&Message::SchemaManifest(forged), "digest-forge"));
        // The stream must die: a ping sent after the forgery draws no
        // pong and no SchemaAck.
        rt.block_on(assert_stream_dead(&mut bad_digest, 71, "digest-forged manifest"));
        assert!(
            recorder.saw_since(since_s1, "signature error"),
            "node never latched the signature refusal for the digest forgery"
        );
        bad_digest.close("digest-forge");
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 2
            }),
            "digest-forge session did not tear down"
        );
        // The forgery never reached the adoption engine: the adoptions
        // counter (incremented only after successful verification and
        // the adoption decision) must not move.
        assert_eq!(
            schema_adoptions(&cluster, TARGET),
            base_adoptions,
            "schema adoptions moved on the digest forgery (forgery reached adoption)"
        );
        // The node itself survives the forgery.
        let mut probe = rt.block_on(attacker.dial(node_addr, divergent.clone(), "probe-1"));
        handshakes += 1;
        rt.block_on(expect_pong(&mut probe, 72, "probe after digest-forged manifest"));
        probe.close("probe-1");

        // --- S2: self-consistent revision from the untrusted signer
        // (valid encoding, valid signature, divergent content). Must be
        // refused as `untrusted schema signer` and never adopted.
        let evil = attacker_manifest(dbid, 1, EVIL_SCHEMA.to_vec());
        assert_authorized_shape(&evil, dbid, MAX_TX, "evil revision");
        let since_s2 = Instant::now();
        let mut untrusted =
            rt.block_on(attacker.dial(node_addr, divergent.clone(), "untrusted-signer"));
        handshakes += 1;
        rt.block_on(untrusted.send(&Message::SchemaManifest(evil), "untrusted-signer"));
        rt.block_on(assert_stream_dead(&mut untrusted, 73, "untrusted-signer manifest"));
        assert!(
            recorder.saw_since(since_s2, "untrusted schema signer"),
            "node never latched the untrusted-signer refusal"
        );
        untrusted.close("untrusted-signer");
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 2
            }),
            "untrusted-signer session did not tear down"
        );

        // --- S3: oversized revision (2 MiB of schema bytes against the
        // 1 MiB cap). Must be refused as an envelope violation.
        let big = attacker_manifest(dbid, 1, vec![0x55; 2 << 20]);
        // Non-vacuous guard: oversized but otherwise self-consistent
        // (verifies under a generous cap), so the live refusal is the
        // size envelope, not a bad signature.
        assert_authorized_shape(&big, dbid, 8 << 20, "oversized revision");
        assert!(
            {
                let mut trust: BTreeMap<[u8; 16], [u8; 32]> = BTreeMap::new();
                trust.insert(ATTACKER_ID, protocol::public_key(&ATTACKER_SEED));
                big.verify(dbid, ATTACKER_ID, &trust, MAX_TX).is_err()
            },
            "oversized revision passes the cluster cap locally (leg vacuous)"
        );
        let since_s3 = Instant::now();
        let mut oversized = rt.block_on(attacker.dial(node_addr, divergent.clone(), "oversized"));
        handshakes += 1;
        rt.block_on(oversized.send(&Message::SchemaManifest(big), "oversized"));
        rt.block_on(assert_stream_dead(&mut oversized, 74, "oversized manifest"));
        assert!(
            recorder.saw_since(since_s3, "schema envelope identity or size"),
            "node never latched the envelope refusal for the oversized manifest"
        );
        oversized.close("oversized");
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 2
            }),
            "oversized session did not tear down"
        );

        // --- S4: bad-epoch revision (zero sequence). Must be refused
        // as an envelope violation.
        let mut zero_seq = attacker_manifest(dbid, 1, EVIL_SCHEMA.to_vec());
        assert_authorized_shape(&zero_seq, dbid, MAX_TX, "zero-sequence revision");
        zero_seq.sequence = 0;
        let since_s4 = Instant::now();
        let mut bad_epoch = rt.block_on(attacker.dial(node_addr, divergent.clone(), "bad-epoch"));
        handshakes += 1;
        rt.block_on(bad_epoch.send(&Message::SchemaManifest(zero_seq), "bad-epoch"));
        rt.block_on(assert_stream_dead(&mut bad_epoch, 75, "zero-sequence manifest"));
        assert!(
            recorder.saw_since(since_s4, "schema envelope identity or size"),
            "node never latched the envelope refusal for the zero-sequence manifest"
        );
        bad_epoch.close("bad-epoch");
        assert!(
            wait_until(PEER_TIMEOUT, Duration::from_millis(50), || {
                connected_peers(&cluster, TARGET) == 2
            }),
            "bad-epoch session did not tear down"
        );

        // Quarantine: no adoption, and the schema identity is
        // untouched by every forgery.
        assert_eq!(
            schema_adoptions(&cluster, TARGET),
            base_adoptions,
            "schema adoptions moved during the schema attacks (forged schema adopted)"
        );
        assert_eq!(
            names(cluster.node(TARGET)),
            pre_attack,
            "node1 names changed during the schema attacks"
        );
        assert_eq!(
            row_count(cluster.node(TARGET)),
            1,
            "node1 row count changed during the schema attacks"
        );
        let final_probe =
            rt.block_on(attacker.dial(node_addr, divergent.clone(), "final-schema"));
        handshakes += 1;
        assert_eq!(
            final_probe.welcome_schema, served_schema,
            "schema bytes moved during the schema attacks"
        );
        final_probe.close("final-schema");

        // Every attack handshake attached (each dial asserts its own
        // Welcome; `peers == 3` was observed on the first session).
        assert_eq!(handshakes, 6, "handshake count drifted (attach proof)");
        eprintln!("{handshakes} attack handshakes attached and were answered");

        // --- Positive control (post-attack): no crash, no stall. The
        // attacked node and the honest pair all keep replicating and
        // converge.
        insert(&cluster, 1, 2, "post-attack-1");
        wait_converged(&cluster, 2, CONVERGE_TIMEOUT);
        insert(&cluster, 2, 3, "post-attack-2");
        wait_converged(&cluster, 3, CONVERGE_TIMEOUT);
        // The attacked node itself writes and the write replicates
        // everywhere.
        insert(&cluster, 0, 4, "attacked-node-write");
        wait_converged(&cluster, 4, CONVERGE_TIMEOUT);
        // Explicit honest-pair check: nodes 2 and 3 agree bit-for-bit.
        let (w1, w2) = (names(cluster.node(1)), names(cluster.node(2)));
        assert_eq!(w1, w2, "honest pair diverged: node2 {w1:?} != node3 {w2:?}");

        recorder.stop();
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("hostile-schema");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
