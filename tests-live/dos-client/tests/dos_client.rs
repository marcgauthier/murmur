//! Rust port of the Go `dos-client` live scenario (QUIC surface only).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/dos-client/`
//! (`dos_client_test.go`). While an attacker sprays garbage UDP and holds
//! valid-credential QUIC half-opens (stream opened, Hello never sent)
//! against the victim's replication port, the node must keep serving its
//! legit peer (writes converge both ways within latency bounds) and
//! attacker state must stay bounded (garbage creates no sessions,
//! half-opens never attach as replication peers, peer counts recover
//! after teardown). All HTTPS flood vectors are absent: the Rust product
//! has no HTTPS API server (see the scenario README.md).

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use murmur_live_harness::murmur_replication::{protocol, transport};
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};
use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};

/// Contention table DDL (Go: typed contention rows).
const TABLE_DDL: &str = "CREATE TABLE contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)";
/// Attack-duration knob (Go: `MURMUR_DOS_CLIENT_ATTACK_SECONDS`, default 60).
const ATTACK_ENV: &str = "MURMUR_DOS_CLIENT_ATTACK_SECONDS";
const DEFAULT_ATTACK_SECS: u64 = 60;
/// QUIC half-open knob (Go: `MURMUR_DOS_CLIENT_QUIC_HALFOPEN`, default 24).
const QUIC_ENV: &str = "MURMUR_DOS_CLIENT_QUIC_HALFOPEN";
const DEFAULT_QUIC_HALF: usize = 24;
/// Garbage sprayers (Go: 2) and cadence (Go: 10 ms).
const GARBAGE_THREADS: usize = 2;
const GARBAGE_TICK: Duration = Duration::from_millis(10);
/// Baseline waits (Go: 30 s converge, 15 s session settle).
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(15);
/// Flood-pressure wait (Go: 30 s `waitGoroutinesAbove`).
const PRESSURE_TIMEOUT: Duration = Duration::from_secs(30);
/// Victim-write latency bound during the flood (Go: < 10 s).
const VICTIM_WRITE_BOUND: Duration = Duration::from_secs(10);
/// Post-attack waits (Go: 30 s peers, 15 s converge).
const RECOVER_TIMEOUT: Duration = Duration::from_secs(30);
const POST_TIMEOUT: Duration = Duration::from_secs(15);
/// Convergence poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);

/// Deterministic xorshift64* (Go flood uses `math/rand`).
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

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

/// Live session count (Go: `connected_peers`; the sessions-analog: garbage
/// and half-opens must never move it).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication enabled")
        .status()
        .peers
}

/// Poll exact counts plus ordered-digest equality (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        let mut first: Option<Vec<(String, String)>> = None;
        for i in 0..cluster.len() {
            let got = rows(cluster.node(i));
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
    assert!(
        converged,
        "nodes did not converge on {want} rows with equal digests within {timeout:?}"
    );
}

/// Poll exact counts plus ordered-digest equality until a deadline
/// (Go: `waitConvergedDeadline`).
fn wait_converged_deadline(cluster: &Cluster, want: usize, deadline: Instant, what: &str) {
    let converged = wait_until(
        deadline.saturating_duration_since(Instant::now()),
        POLL,
        || {
            let mut first: Option<Vec<(String, String)>> = None;
            for i in 0..cluster.len() {
                let got = rows(cluster.node(i));
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
        },
    );
    assert!(
        converged,
        "{what}: nodes did not converge on {want} rows with equal digests by the deadline"
    );
}

/// Wait until one node's peer count holds steady for 5 s (Go:
/// `waitSessionsStable` on the sessions counter).
fn wait_peers_stable(cluster: &Cluster, node: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut last = peer_count(&cluster, node);
    let mut stable_since = Instant::now();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_secs(1));
        let cur = peer_count(&cluster, node);
        if cur != last {
            last = cur;
            stable_since = Instant::now();
            continue;
        }
        if stable_since.elapsed() >= Duration::from_secs(5) {
            return;
        }
    }
}

/// Wait until one node reports `want` peers (Go: `waitConnectedPeers`).
fn wait_connected_peers(cluster: &Cluster, node: usize, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, Duration::from_millis(500), || {
        peer_count(&cluster, node) == want
    });
    assert!(
        converged,
        "node{} peers != {want} within {timeout:?}",
        node + 1
    );
}

/// Attacker-side TLS verifier: any CA-valid server identity is accepted
/// (mirrors the product `NodeVerifier` minus its peer allowlist; the
/// server name is ignored exactly like the product does).
#[derive(Debug)]
struct TestServerVerifier {
    roots: rustls::RootCertStore,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for TestServerVerifier {
    fn verify_server_cert(
        &self,
        cert: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let parsed = rustls::server::ParsedCertificate::try_from(cert)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &parsed,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        let _ = transport::node_id(cert.as_ref())
            .map_err(|e| rustls::Error::General(e.to_string()))?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Attacker QUIC endpoint presenting the victim's own node certificate
/// (stolen-credential model, exactly like Go's `quicCreds`): the
/// handshake only needs a CA-valid chain, and TLS 1.3 + the replication
/// ALPN match the product server.
fn attacker_endpoint(tls_dir: &Path) -> quinn::Endpoint {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let ca_der = std::fs::read(tls_dir.join("ca.der")).expect("read ca.der");
    let node_der = std::fs::read(tls_dir.join("node.der")).expect("read node.der");
    let key_der = std::fs::read(tls_dir.join("node.key")).expect("read node.key");
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(ca_der))
        .expect("add CA root");
    let chain = vec![CertificateDer::from(node_der)];
    let key = PrivateKeyDer::try_from(key_der).expect("parse node key");
    let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TestServerVerifier {
            roots,
            provider,
        }))
        .with_client_auth_cert(chain, key)
        .expect("client auth cert");
    client.alpn_protocols = vec![protocol::ALPN.to_vec()];
    client.enable_early_data = false;
    let mut endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).expect("attacker endpoint");
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client).expect("quic client config"),
    )));
    endpoint
}

/// QUIC half-open flood: valid-cert dials that open a stream and never
/// send Hello, held open; a top-up loop replaces connections the idle
/// timeout reaps (Go: the `quicHalf` vector). Runs to `stop`.
fn quic_half_open_flood(
    repl: SocketAddr,
    tls_dir: PathBuf,
    want: usize,
    stop: Arc<AtomicBool>,
    held: Arc<AtomicI64>,
    attempted: Arc<AtomicI64>,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("attacker runtime");
    rt.block_on(async move {
        let endpoint = attacker_endpoint(&tls_dir);
        let mut conns: Vec<(quinn::Connection, quinn::SendStream, quinn::RecvStream)> =
            Vec::new();
        loop {
            conns.retain(|(c, _, _)| c.close_reason().is_none());
            held.store(conns.len() as i64, Ordering::Relaxed);
            while conns.len() < want {
                attempted.fetch_add(1, Ordering::Relaxed);
                let connecting = match endpoint.connect(repl, "localhost") {
                    Ok(c) => c,
                    Err(_) => break,
                };
                let conn =
                    match tokio::time::timeout(Duration::from_secs(10), connecting).await {
                        Ok(Ok(c)) => c,
                        _ => break,
                    };
                match tokio::time::timeout(Duration::from_secs(5), conn.open_bi()).await {
                    Ok(Ok((send, recv))) => {
                        held.fetch_add(1, Ordering::Relaxed);
                        conns.push((conn, send, recv));
                    }
                    _ => {
                        conn.close(0u32.into(), b"open failed");
                        break;
                    }
                }
            }
            if stop.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        for (c, _, _) in conns {
            c.close(0u32.into(), b"done");
        }
        endpoint.close(0u32.into(), b"done");
    });
}

/// Garbage UDP spray at the replication port (Go: the `garbage` loops).
/// Runs to `stop`.
fn garbage_spray(repl: SocketAddr, seed: u64, stop: Arc<AtomicBool>) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("garbage socket");
    sock.connect(repl).expect("garbage connect");
    let mut rng = XorShift(seed);
    let mut buf = vec![0u8; 1200];
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(GARBAGE_TICK);
        if stop.load(Ordering::Relaxed) {
            break;
        }
        rng.fill(&mut buf);
        let n = 100 + (rng.next() % 1100) as usize;
        let _ = sock.send(&buf[..n]);
    }
}

#[test]
fn dos_client_flood() {
    let attack_for = Duration::from_secs(
        std::env::var(ATTACK_ENV)
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_ATTACK_SECS),
    );
    let n_quic_half: usize = std::env::var(QUIC_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_QUIC_HALF);

    let cluster = Cluster::new(ClusterOptions {
        name: "dos-client".into(),
        num_nodes: 2,
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
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same schema on every node (Go: typed contention fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate contention table");
            cluster.node(i).flush().expect("flush schema");
        }
        let (victim, peer) = (0, 1);
        let repl = cluster.node_info(victim).repl_addr;
        let tls_dir = cluster.node_info(victim).dir.join("tls");

        // Positive control (pre-attack): both directions converge.
        insert_row(cluster.node(peer), peer + 1, format!("{:032x}", 1), "honest-1");
        wait_converged(&cluster, 1, CONTROL_TIMEOUT);
        insert_row(cluster.node(victim), victim + 1, format!("{:032x}", 2), "honest-2");
        wait_converged(&cluster, 2, CONTROL_TIMEOUT);

        // Let the mesh settle so peer counts are stable before baselining.
        wait_peers_stable(&cluster, victim, SETTLE_TIMEOUT);
        let base_peers = peer_count(&cluster, victim);
        eprintln!("baseline: peers={base_peers}");

        // Phase 1: garbage UDP is rejected cheaply (no sessions, no harm).
        let garbage_stop = Arc::new(AtomicBool::new(false));
        std::thread::scope(|s| {
            for g in 0..GARBAGE_THREADS {
                let garbage_stop = garbage_stop.clone();
                s.spawn(move || garbage_spray(repl, 0x9a47 + g as u64, garbage_stop));
            }
            for i in 0..5 {
                insert_row(
                    cluster.node(peer),
                    peer + 1,
                    format!("{:032x}", 100 + i),
                    "garbage-hb",
                );
                std::thread::sleep(Duration::from_secs(2));
            }
            garbage_stop.store(true, Ordering::Relaxed);
        });
        wait_converged(&cluster, 7, CONTROL_TIMEOUT);
        assert_eq!(
            peer_count(&cluster, victim),
            base_peers,
            "garbage UDP moved peers {base_peers} -> {} (junk created server state)",
            peer_count(&cluster, victim)
        );
        eprintln!("garbage phase: peers stable at {base_peers}, heartbeats converged");

        // Phase 2 (Go) is HTTPS-only (unauthenticated-exec 401, TLS 1.0
        // refusal) and has no Rust target: there is no HTTPS API server.
        // QUIC pins TLS 1.3 structurally (see the scenario README).

        // Phase 3: sustained QUIC flood (garbage spray + half-opens).
        let flood_stop = Arc::new(AtomicBool::new(false));
        let held = Arc::new(AtomicI64::new(0));
        let attempted = Arc::new(AtomicI64::new(0));
        std::thread::scope(|s| {
            for g in 0..GARBAGE_THREADS {
                let flood_stop = flood_stop.clone();
                s.spawn(move || garbage_spray(repl, 0x51ed + g as u64, flood_stop));
            }
            {
                let flood_stop = flood_stop.clone();
                let (held, attempted) = (held.clone(), attempted.clone());
                let tls_dir = tls_dir.clone();
                s.spawn(move || {
                    quic_half_open_flood(repl, tls_dir, n_quic_half, flood_stop, held, attempted)
                });
            }
            eprintln!("flood running for {attack_for:?} (quic-half-open={n_quic_half} + garbage)");
            // Confirm the flood is actually holding victim state before
            // asserting availability under it (non-vacuous: the victim
            // must feel pressure).
            let pressured = wait_until(PRESSURE_TIMEOUT, Duration::from_millis(500), || {
                let (h, a) = (
                    held.load(Ordering::Relaxed),
                    attempted.load(Ordering::Relaxed),
                );
                a > 0 && h * 2 >= a
            });
            let (h, a) = (
                held.load(Ordering::Relaxed),
                attempted.load(Ordering::Relaxed),
            );
            assert!(
                pressured,
                "flood too weak to prove anything: held {h}/{a} attacker conns"
            );
            eprintln!("victim under pressure: attacker held {h}/{a} QUIC half-opens");

            let deadline = Instant::now() + attack_for;
            // Legit write directly on the flooded victim succeeds fast.
            let start = Instant::now();
            insert_row(
                cluster.node(victim),
                victim + 1,
                format!("{:032x}", 200),
                "during-flood-victim",
            );
            let dt = start.elapsed();
            assert!(
                dt <= VICTIM_WRITE_BOUND,
                "write on flooded victim took {dt:?}, want < {VICTIM_WRITE_BOUND:?}"
            );
            // Attacker half-opens must not have attached as sessions.
            let sessions = peer_count(&cluster, victim);
            assert!(
                sessions <= base_peers + 2,
                "peers grew {base_peers} -> {sessions} during flood (attacker state attaching?)"
            );
            // The legit peer's write converges on the victim within the
            // latency bound.
            insert_row(
                cluster.node(peer),
                peer + 1,
                format!("{:032x}", 201),
                "during-flood-peer",
            );
            wait_converged_deadline(&cluster, 9, deadline, "peer write during flood");
            eprintln!(
                "mid-flood convergence ok with {:?} of attack left",
                deadline.saturating_duration_since(Instant::now())
            );
            // Hold the flood for the remainder so budgets face the full duration.
            let rest = deadline.saturating_duration_since(Instant::now());
            if !rest.is_zero() {
                std::thread::sleep(rest);
            }
            flood_stop.store(true, Ordering::Relaxed);
        });

        // Phase 4: teardown recovery is bounded and complete.
        wait_connected_peers(&cluster, victim, 1, RECOVER_TIMEOUT);
        let sessions = peer_count(&cluster, victim);
        assert!(
            sessions <= base_peers + 2,
            "post-attack peers {sessions} exceeds baseline {base_peers} + 2 (leaked attacker state?)"
        );
        eprintln!("peers recovered to {sessions} (baseline {base_peers})");

        // Positive control (post-attack): fast convergence, equal digests.
        insert_row(
            cluster.node(peer),
            peer + 1,
            format!("{:032x}", 202),
            "post-attack",
        );
        let start = Instant::now();
        wait_converged(&cluster, 10, POST_TIMEOUT);
        eprintln!("post-attack write converged in {:?}", start.elapsed());
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("dos-client-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
