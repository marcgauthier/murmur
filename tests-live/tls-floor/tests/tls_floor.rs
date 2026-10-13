//! Rust port of the Go `tls-floor` live scenario (test-side equivalent).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/tls-floor/`
//! (`tls_floor_test.go`). Go pins the HTTPS API floor at TLS 1.2 (1.0/1.1
//! refused with server-side rejection, 1.2 succeeds with the negotiated
//! version asserted) and separately guards the QUIC floor at TLS 1.3 via
//! the product's TLS constructors. The Rust product has no HTTPS API, so
//! this port proves the floor on the QUIC replication endpoint, whose
//! floor is TLS 1.3 (stricter): a TLS-1.2-only handshake must fail with
//! a server-side crypto alert while TLS 1.3 succeeds and a 2-node mesh
//! converges over it (see the scenario README.md for the leg mapping).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use murmur_live_harness::murmur_replication::{protocol, transport};
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};
use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};

/// Table DDL (Go: typed records marker).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)";
/// Mesh convergence deadline (Go: 60 s).
const MESH_TIMEOUT: Duration = Duration::from_secs(60);
/// Per-dial timeout (Go: 10 s client timeout).
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// Convergence poll interval (Go: 100 ms).
const POLL: Duration = Duration::from_millis(100);

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

/// Attacker QUIC endpoint presenting the node's own certificate with
/// exactly the given TLS versions enabled (Go: MinVersion/MaxVersion
/// pinning so the ClientHello genuinely emits the old version).
fn attacker_endpoint(
    tls_dir: &Path,
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> quinn::Endpoint {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let ca_der = std::fs::read(tls_dir.join("ca.der")).expect("read ca.der");
    let node_der = std::fs::read(tls_dir.join("node.der")).expect("read node.der");
    let key_der = std::fs::read(tls_dir.join("node.key")).expect("read node.key");
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(ca_der))
        .expect("add CA root");
    let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(versions)
        .expect("protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TestServerVerifier {
            roots,
            provider,
        }))
        .with_client_auth_cert(
            vec![CertificateDer::from(node_der)],
            PrivateKeyDer::try_from(key_der).expect("parse node key"),
        )
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

/// True when a handshake failure came from the server (a TLS alert
/// delivered as a QUIC crypto close, 0x100-0x1ff) rather than from local
/// config validation or a lapsed timeout (Go: `isRemoteRejection`).
fn is_remote_rejection(err: &quinn::ConnectionError) -> bool {
    match err {
        quinn::ConnectionError::ConnectionClosed(close) => {
            // `TransportErrorCode` renders crypto-range codes as
            // `Code::crypto(..)` (Debug) / "the cryptographic handshake
            // failed" (Display); either marker proves a peer TLS alert.
            let (dbg, dsp) = (format!("{close:?}"), format!("{close}"));
            dbg.contains("crypto(") || dsp.contains("cryptographic handshake failed")
        }
        _ => false,
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

/// Live session count (failed handshakes must never attach as sessions).
fn peer_count(cluster: &Cluster, node: usize) -> usize {
    cluster
        .node(node)
        .replication()
        .expect("replication enabled")
        .status()
        .peers
}

#[test]
fn quic_floor_rejects_below_tls13() {
    let cluster = Cluster::new(ClusterOptions {
        name: "tls-floor".into(),
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
        // Same schema on every node (Go: typed fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }
        let repl: SocketAddr = cluster.node_info(0).repl_addr;
        let tls_dir = cluster.node_info(0).dir.join("tls");

        // Mesh positive control: a marker converges over QUIC (TLS 1.3),
        // proving the floor does not break replication.
        cluster
            .node(0)
            .transaction(|sql| {
                sql.execute(
                    "INSERT INTO live_typed_records(name) VALUES(?)",
                    ["tls-version-floor-marker"],
                )?;
                Ok(())
            })
            .expect("marker insert");
        cluster.node(0).flush().expect("flush marker");
        let converged = wait_until(MESH_TIMEOUT, POLL, || {
            let mut first: Option<Vec<String>> = None;
            for i in 0..cluster.len() {
                let got = names(cluster.node(i));
                if got.len() != 1 {
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
        assert!(converged, "marker did not converge within {MESH_TIMEOUT:?}");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("attacker runtime");
        rt.block_on(async {
            // Floor negative: a TLS-1.2-only mTLS handshake must fail
            // with a SERVER-side rejection (TLS alert as QUIC crypto
            // close), not a local config error and not a bare timeout.
            // TLS 1.0/1.1 have no analog: rustls 0.23 removed those
            // versions entirely, so they are inexpressible (see README).
            let legacy = attacker_endpoint(&tls_dir, &[&rustls::version::TLS12]);
            let connecting = legacy.connect(repl, "localhost").unwrap_or_else(|e| {
                panic!("TLS 1.2 dial not initiated ({e:?}); the toolchain gated it locally, so the floor is unproven")
            });
            match tokio::time::timeout(DIAL_TIMEOUT, connecting).await {
                Ok(Ok(conn)) => {
                    conn.close(0u32.into(), b"floor violated");
                    panic!("TLS 1.2 client unexpectedly completed a handshake: floor violated");
                }
                Ok(Err(e)) => {
                    eprintln!("TLS 1.2 rejected: {e:?}");
                    assert!(
                        is_remote_rejection(&e),
                        "TLS 1.2 failure is not a server-side rejection ({e:?}); the toolchain cannot emit this version, so the floor is unproven"
                    );
                }
                Err(_) => panic!(
                    "TLS 1.2 dial timed out without a server rejection; the floor is unproven"
                ),
            }

            // Floor positive: the identical client shape at TLS 1.3
            // completes a handshake and opens a stream.
            let modern = attacker_endpoint(&tls_dir, &[&rustls::version::TLS13]);
            let connecting = modern
                .connect(repl, "localhost")
                .expect("TLS 1.3 dial builds");
            let conn = tokio::time::timeout(DIAL_TIMEOUT, connecting)
                .await
                .expect("TLS 1.3 handshake completes")
                .expect("TLS 1.3 handshake ok");
            tokio::time::timeout(Duration::from_secs(5), conn.open_bi())
                .await
                .expect("TLS 1.3 stream opens")
                .expect("TLS 1.3 stream ok");
            conn.close(0u32.into(), b"probe done");
            eprintln!("TLS 1.3 client completed handshake + stream");
            // NOTE: Go also asserts the negotiated version reads back as
            // exactly 1.2; quinn exposes no negotiated-version
            // introspection, so the completed-against-1.3-only-server
            // handshake is the positive proof here.
        });

        // The rejected handshake attached no session.
        assert_eq!(
            peer_count(&cluster, 0),
            1,
            "rejected handshake moved peers (mesh peer + phantom?)"
        );
        eprintln!("PASS: floor holds at TLS 1.3, mesh peer intact, no phantom session");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("tls-floor-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
