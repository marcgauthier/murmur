//! Rust port of the Go `version-skew` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/version-skew/`
//! (`version_skew_test.go`: `TestUnknownProtocolVersionRefused`). A node
//! advertising handshake protocol version 99 must be refused fail-closed:
//! it never connects in either direction, exchanges no data either way,
//! but stays alive serving local reads/writes, while the compatible pair
//! keeps converging.
//!
//! The Rust product has no `protocol_version_override` testing knob (the
//! version gate is the hardcoded `version == VERSION` check in
//! `engine.rs` plus the frame-header check in `protocol.rs`), so the Go
//! skewed node is split in two: a raw QUIC fixture speaks version 99 in
//! both directions (inbound dials refused by the honest nodes, outbound
//! dials from an honest node answered with skewed silence), and an
//! unmeshed third cluster node stands in for the isolated-but-alive leg
//! (local reads/writes work, nothing flows in or out). Plain SQL replaces
//! the Go typed-records HTTP API; see the scenario README.md.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use murmur_live_harness::murmur_replication::{
    encode,
    protocol::{self, Message},
    transport, Config as ReplConfig, Dissemination,
};
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Skewed fixture identity: fixed test id, asserted distinct from the node ids.
const SKEWED_ID: [u8; 16] = [0x99; 16];
/// Skewed fixture origin signing key (only used to build its endpoint config).
const SKEWED_SEED: [u8; 32] = [0x99; 32];
/// Advertised handshake version of the skewed peer (Go: `...Override: 99`).
const SKEWED_VERSION: u16 = 99;
/// Wire discriminant of `Message::Hello` (`Message::kind` is private; value
/// from `protocol.rs`, asserted by the control leg below).
const HELLO_KIND: u16 = 1;

/// Baseline rows written on node0 (Go: 10).
const BASELINE_ROWS: usize = 10;
/// Local rows written on the isolated node3 (Go: 3).
const LOCAL_ROWS: i64 = 3;
/// Compatible-pair convergence deadline (Go: 30 s).
const PAIR_TIMEOUT: Duration = Duration::from_secs(30);
/// Skew observation window (Go: 10 s).
const WINDOW: Duration = Duration::from_secs(10);
/// Window poll interval (Go: 500 ms).
const POLL: Duration = Duration::from_millis(500);
/// Outbound-leg window: node0 dials the skewed addr, no session may form.
const OUTBOUND_WINDOW: Duration = Duration::from_secs(10);
/// Fixture accept slice inside the outbound window.
const ACCEPT_SLICE: Duration = Duration::from_millis(500);
/// Post-local-write settle before the final row asserts (Go: 5 s).
const SETTLE: Duration = Duration::from_secs(5);
/// Fixture dial/stream/setup deadline (generous; loopback refusals are fast).
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the fixture waits for a Welcome that must never arrive.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(5);

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

/// Wait until nodes 0 and 1 (the compatible pair) hold `want` rows each
/// (Go: `waitPairCount`).
fn wait_pair_count(cluster: &Cluster, want: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let (n0, n1) = (
            row_count(cluster.node(0)),
            row_count(cluster.node(1)),
        );
        if n0 == want && n1 == want {
            return;
        }
        if Instant::now() >= deadline {
            panic!("compatible pair counts={n0},{n1}, want {want} within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(100));
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

/// Latest replication error on one node (Go: refusal counters, which have
/// no Rust equivalent; version-gate rejections latch here).
fn last_error(cluster: &Cluster, idx: usize) -> Option<String> {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .last_error
}

/// `protocol::write` with an explicit frame-header version. The product
/// writer always stamps `VERSION`, so a faithful skewed peer needs its own
/// framer; Hellos stay far under the 1024-byte compression threshold, so
/// flags are always 0 (mirrors the `protocol::write` layout).
async fn write_versioned(
    send: &mut quinn::SendStream,
    message: &Message,
    frame_version: u16,
) -> Result<(), String> {
    let data = encode(message).map_err(|e| e.to_string())?;
    assert!(
        data.len() <= 1024,
        "fixture hello grew past the uncompressed framing limit"
    );
    let mut h = [0u8; 12];
    h[..2].copy_from_slice(b"RD");
    h[2..4].copy_from_slice(&frame_version.to_be_bytes());
    h[4..6].copy_from_slice(&HELLO_KIND.to_be_bytes());
    h[6..8].copy_from_slice(&0u16.to_be_bytes());
    h[8..12].copy_from_slice(&(data.len() as u32).to_be_bytes());
    send.write_all(&h).await.map_err(|e| e.to_string())?;
    send.write_all(&data).await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Raw QUIC fixture speaking the replication handshake with a skewed
/// protocol version (Go: node3 with `protocol_version_override: 99`).
/// Shares the cluster CA trust, so TLS can never be the refusal cause.
struct SkewedPeer {
    endpoint: quinn::Endpoint,
    db: [u8; 16],
}

impl SkewedPeer {
    fn new(config: ReplConfig, db: [u8; 16]) -> Self {
        let endpoint = transport::endpoint(&config, SKEWED_ID).expect("fixture endpoint");
        Self { endpoint, db }
    }

    fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr().expect("fixture bind addr")
    }

    /// A fully compatible Hello except for the version field, so the
    /// version is the only reachable refusal cause.
    fn hello(&self, version: u16) -> Message {
        Message::Hello {
            capabilities: protocol::CAPABILITIES,
            required: protocol::REQUIRED,
            version,
            database: self.db,
            node: SKEWED_ID,
            fetch: None,
            schema: Vec::new(),
            mode: Dissemination::Gossip,
            max_transaction: 64 << 20,
        }
    }

    /// Positive control: the same fixture with the product version MUST be
    /// welcomed. Proves TLS trust, identity, address, and the hand-rolled
    /// `HELLO_KIND` framer are all correct, so later refusals are caused by
    /// the version alone.
    async fn assert_welcomed(&self, addr: SocketAddr) {
        let connection = tokio::time::timeout(
            DIAL_TIMEOUT,
            self.endpoint
                .connect(addr, "murmur.invalid")
                .expect("control connect setup"),
        )
        .await
        .expect("control dial timeout")
        .expect("control connect");
        let (mut send, mut recv) = tokio::time::timeout(DIAL_TIMEOUT, connection.open_bi())
            .await
            .expect("control open stream timeout")
            .expect("control open stream");
        write_versioned(&mut send, &self.hello(protocol::VERSION), protocol::VERSION)
            .await
            .expect("control write hello");
        match tokio::time::timeout(WELCOME_TIMEOUT, protocol::read(&mut recv)).await {
            Ok(Ok(Message::Welcome { .. })) => {}
            other => panic!("control dial with product version not welcomed: {other:?}"),
        }
        connection.close(0u32.into(), b"control done");
    }

    /// One refused handshake: TLS, stream open, and the Hello send must all
    /// succeed (else the fixture is broken and the probe is vacuous); then
    /// NO Welcome may arrive — the stream must die or stay silent.
    async fn assert_refused(&self, addr: SocketAddr, frame_version: u16, hello_version: u16) {
        let connection = tokio::time::timeout(
            DIAL_TIMEOUT,
            self.endpoint
                .connect(addr, "murmur.invalid")
                .expect("fixture connect setup"),
        )
        .await
        .expect("fixture dial timeout")
        .expect("fixture connect");
        let (mut send, mut recv) = tokio::time::timeout(DIAL_TIMEOUT, connection.open_bi())
            .await
            .expect("fixture open stream timeout")
            .expect("fixture open stream");
        write_versioned(&mut send, &self.hello(hello_version), frame_version)
            .await
            .expect("fixture write hello");
        match tokio::time::timeout(WELCOME_TIMEOUT, protocol::read(&mut recv)).await {
            Ok(Ok(Message::Welcome { .. })) => panic!(
                "node at {addr} sent Welcome to a version-{hello_version} hello \
                 (frame v{frame_version}): fail-closed violated"
            ),
            Ok(Ok(other)) => panic!(
                "node at {addr} answered a version-{hello_version} hello with \
                 {other:?}, want refusal"
            ),
            Ok(Err(_)) | Err(_) => {} // refused: stream died or stayed silent
        }
        connection.close(0u32.into(), b"skew probe done");
    }

    /// Outbound-leg accept side: behave like a version-99 node that cannot
    /// parse our frames — read one Hello, then close without a Welcome.
    /// Returns the dialer's address and Hello, or `None` on a quiet slice.
    async fn accept_without_welcome(&self, slice: Duration) -> Option<(SocketAddr, Message)> {
        let incoming = tokio::time::timeout(slice, self.endpoint.accept())
            .await
            .ok()??;
        let connection = tokio::time::timeout(slice, incoming).await.ok()?.ok()?;
        let remote = connection.remote_address();
        let (_send, mut recv) = tokio::time::timeout(slice, connection.accept_bi())
            .await
            .ok()?
            .ok()?;
        let hello = tokio::time::timeout(slice, protocol::read(&mut recv))
            .await
            .ok()?
            .ok()?;
        connection.close(0u32.into(), b"skewed: cannot parse");
        Some((remote, hello))
    }
}

#[test]
fn unknown_protocol_version_refused() {
    // Fixed node ids (asserted distinct from the fixture id) so no
    // handshake identity can alias.
    let node_ids = vec![[0x10; 16], [0x20; 16], [0x30; 16]];
    assert!(
        !node_ids.contains(&SKEWED_ID),
        "skewed fixture id collides with a node id"
    );
    let cluster = Cluster::new(ClusterOptions {
        name: "version-skew".into(),
        num_nodes: 3,
        node_ids,
        // Manual peers: only the compatible pair (0, 1) is meshed, so
        // node3 stays peerless like the Go skewed node — alive and
        // writable, but exchanging no data in either direction.
        manual_peers: true,
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
        cluster.add_peer(0, 1).expect("AddPeer(0,1)");
        cluster.add_peer(1, 0).expect("AddPeer(1,0)");

        // Skewed-version fixture sharing the cluster CA trust.
        let issued = cluster
            .issue_cert(&SKEWED_ID)
            .expect("issue fixture cert");
        let skewed_config = ReplConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            issued.credentials,
            SKEWED_SEED,
        );
        let dbid = cluster.database_id();
        let skewed = SkewedPeer::new(skewed_config, dbid);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fixture runtime");

        // Positive control first: a product-version hello from this exact
        // fixture must be welcomed (TLS/identity/framer all proven good).
        rt.block_on(skewed.assert_welcomed(cluster.node_info(0).repl_addr));
        eprintln!("control: product-version hello welcomed (fixture identity OK)");
        assert!(
            wait_until(Duration::from_secs(10), Duration::from_millis(100), || {
                connected_peers(&cluster, 0) == 1
            }),
            "control session did not tear down before the baseline"
        );

        // Baseline: 10 writes on node0; the compatible pair converges.
        for i in 0..BASELINE_ROWS {
            insert(&cluster, 0, i as i64 + 1, &format!("w-{i}"));
        }
        cluster.node(0).flush().expect("flush baseline");
        wait_pair_count(&cluster, BASELINE_ROWS, PAIR_TIMEOUT);
        assert_eq!(
            row_count(cluster.node(2)),
            0,
            "isolated node3 holds baseline rows (must stay empty)"
        );

        // Skew window: repeated version-99 dials against both honest nodes
        // must all be refused while peers stay 1/1/0 and node3 stays empty.
        // Rounds alternate the two version gates: the frame-header check in
        // `protocol::read` and the Hello-payload check in `engine::accept`.
        let mut refusals = 0u32;
        let mut saw_frame_gate = [false, false];
        let mut saw_hello_gate = [false, false];
        let deadline = Instant::now() + WINDOW;
        let mut round = 0u32;
        while Instant::now() < deadline {
            assert_eq!(
                connected_peers(&cluster, 0),
                1,
                "node1 connected_peers != 1 (node2 only)"
            );
            assert_eq!(
                connected_peers(&cluster, 1),
                1,
                "node2 connected_peers != 1 (node1 only)"
            );
            assert_eq!(
                connected_peers(&cluster, 2),
                0,
                "isolated node3 connected_peers != 0"
            );
            assert!(
                names(cluster.node(2)).is_empty(),
                "isolated node3 received inbound data (must stay empty)"
            );
            let (frame_v, hello_v) = if round % 2 == 0 {
                (SKEWED_VERSION, SKEWED_VERSION)
            } else {
                (protocol::VERSION, SKEWED_VERSION)
            };
            for (k, idx) in [0, 1].into_iter().enumerate() {
                let addr = cluster.node_info(idx).repl_addr;
                rt.block_on(skewed.assert_refused(addr, frame_v, hello_v));
                refusals += 1;
                // Latch the refusal cause: `last_error` is overwritten by
                // every later error, so sample it every round.
                if let Some(e) = last_error(&cluster, idx) {
                    if e.contains("invalid frame header") {
                        saw_frame_gate[k] = true;
                    }
                    if e.contains("incompatible hello") {
                        saw_hello_gate[k] = true;
                    }
                }
            }
            round += 1;
            std::thread::sleep(POLL);
        }
        assert!(
            refusals >= 4,
            "only {refusals} skew probes ran (both gates against both nodes required)"
        );
        for (k, idx) in [0, 1].into_iter().enumerate() {
            assert!(
                saw_frame_gate[k],
                "node{} never latched the frame-header version refusal",
                idx + 1
            );
            assert!(
                saw_hello_gate[k],
                "node{} never latched the hello-payload version refusal",
                idx + 1
            );
        }
        eprintln!("skew window: {refusals} version-99 dials refused, peers stayed 1/1/0");

        // Outbound leg: node0 dials the skewed address; the skewed side
        // reads the Hello and closes without a Welcome, so no session may
        // ever form (either honest node may place the dial if the address
        // spreads via gossip; both must stay at one session).
        let skewed_addr = skewed.local_addr();
        cluster
            .node(0)
            .replication()
            .expect("replication handle")
            .add_peer(skewed_addr)
            .expect("add skewed addr");
        let honest: Vec<[u8; 16]> = (0..2).map(|i| cluster.node_id(i)).collect();
        let mut attempts = 0u32;
        let deadline = Instant::now() + OUTBOUND_WINDOW;
        while Instant::now() < deadline {
            if let Some((remote, hello)) =
                rt.block_on(skewed.accept_without_welcome(ACCEPT_SLICE))
            {
                match hello {
                    Message::Hello {
                        database,
                        node,
                        version,
                        ..
                    } => {
                        assert_eq!(database, dbid, "dial from {remote} has wrong DBID");
                        assert!(
                            honest.contains(&node),
                            "dial from {remote} is not an honest node"
                        );
                        assert_eq!(
                            version,
                            protocol::VERSION,
                            "dial from {remote} has wrong version"
                        );
                    }
                    other => panic!("fixture got {other:?} from {remote}, want Hello"),
                }
                attempts += 1;
            }
            assert_eq!(
                connected_peers(&cluster, 0),
                1,
                "node1 formed a session toward the skewed addr"
            );
            assert_eq!(
                connected_peers(&cluster, 1),
                1,
                "node2 formed a session toward the skewed addr"
            );
        }
        assert!(
            attempts >= 1,
            "no honest dial reached the skewed addr (outbound leg vacuous)"
        );
        eprintln!("outbound leg: {attempts} honest dials answered with skewed silence");

        // The isolated node stays alive and keeps serving local
        // reads/writes; its data just never leaves the node.
        for i in 0..LOCAL_ROWS {
            insert(&cluster, 2, 101 + i, &format!("skewed-local-{i}"));
        }
        cluster
            .node(2)
            .flush()
            .expect("skewed-side local write failed (must stay writable)");
        std::thread::sleep(SETTLE);
        assert_eq!(
            names(cluster.node(0)).len(),
            BASELINE_ROWS,
            "node1 holds skewed-side rows (must stay at baseline)"
        );
        assert_eq!(
            names(cluster.node(2)).len(),
            LOCAL_ROWS as usize,
            "isolated node3 lost its local rows"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("version-skew");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
