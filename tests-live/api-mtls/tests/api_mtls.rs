//! Rust port of the Go `api-mtls` live scenario (test-side equivalent).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/api-mtls/`
//! (`api_mtls_test.go`). Go requires mutual TLS on the HTTPS daemon API
//! (client cert mandatory except `/healthz`, foreign/expired certs
//! rejected, plaintext refused). The Rust product has no HTTPS API
//! server, so this port proves the same TLS policy on the product's only
//! TLS server — the QUIC replication endpoint, which verifies client
//! certificates against the cluster CA at handshake (see the scenario
//! README.md for the leg mapping).

use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use murmur_live_harness::certs::Ca;
use murmur_live_harness::murmur_replication::{protocol, transport};
use murmur_live_harness::{Cluster, ClusterOptions};
use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};

/// Per-dial timeout (Go: 5 s HTTP client timeout; QUIC needs headroom).
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Client credentials for one attacker endpoint: the CA roots used to
/// verify the server plus an optional client chain/key.
struct AttackerCreds {
    roots_der: Vec<u8>,
    chain_der: Option<Vec<u8>>,
    key_der: Option<Vec<u8>>,
}

/// Attacker QUIC endpoint with exactly the given credentials (TLS 1.3 +
/// the replication ALPN match the product server).
fn attacker_endpoint(creds: &AttackerCreds) -> quinn::Endpoint {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(creds.roots_der.clone()))
        .expect("add CA root");
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TestServerVerifier {
            roots,
            provider,
        }));
    let mut client = match (&creds.chain_der, &creds.key_der) {
        (Some(chain), Some(key)) => builder
            .with_client_auth_cert(
                vec![CertificateDer::from(chain.clone())],
                PrivateKeyDer::try_from(key.clone()).expect("parse client key"),
            )
            .expect("client auth cert"),
        _ => builder.with_no_client_auth(),
    };
    client.alpn_protocols = vec![protocol::ALPN.to_vec()];
    client.enable_early_data = false;
    let mut endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).expect("attacker endpoint");
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client).expect("quic client config"),
    )));
    endpoint
}

/// Attempt one QUIC handshake; true when it completes.
async fn try_handshake(endpoint: &quinn::Endpoint, addr: SocketAddr) -> bool {
    let connecting = match endpoint.connect(addr, "localhost") {
        Ok(c) => c,
        Err(_) => return false,
    };
    match tokio::time::timeout(DIAL_TIMEOUT, connecting).await {
        Ok(Ok(conn)) => {
            conn.close(0u32.into(), b"probe done");
            true
        }
        _ => false,
    }
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
fn tls_endpoint_requires_mutual_auth() {
    let cluster = Cluster::new(ClusterOptions {
        name: "api-mtls".into(),
        num_nodes: 1,
        ..Default::default()
    })
    .expect("cluster setup");
    let info = cluster.node_info(0);
    eprintln!("{} dir={} repl={}", info.label, info.dir.display(), info.repl_addr);
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let repl = cluster.node_info(0).repl_addr;
        let tls_dir = cluster.node_info(0).dir.join("tls");
        let read = |name: &str| {
            std::fs::read(Path::new(&tls_dir).join(name))
                .unwrap_or_else(|e| panic!("read tls/{name}: {e}"))
        };
        let (ca_der, node_der, key_der) =
            (read("ca.der"), read("node.der"), read("node.key"));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("attacker runtime");
        rt.block_on(async {
            // Trusted client certificate: the handshake completes and a
            // stream opens (Go: trusted cert reads `/v1/status` -> 200).
            let trusted = attacker_endpoint(&AttackerCreds {
                roots_der: ca_der.clone(),
                chain_der: Some(node_der.clone()),
                key_der: Some(key_der.clone()),
            });
            let connecting = trusted
                .connect(repl, "localhost")
                .expect("trusted dial builds");
            let conn = tokio::time::timeout(DIAL_TIMEOUT, connecting)
                .await
                .expect("trusted handshake completes")
                .expect("trusted handshake ok");
            tokio::time::timeout(Duration::from_secs(5), conn.open_bi())
                .await
                .expect("trusted stream opens")
                .expect("trusted stream ok");
            conn.close(0u32.into(), b"probe done");
            eprintln!("trusted client certificate: handshake + stream ok");

            // No client certificate: the handshake must fail (Go: 401 on
            // `/v1/status` and `/metrics`). QUIC has no HTTP layer for a
            // 401, so the same policy enforces one step earlier — and
            // there is no `/healthz`-style exemption to probe: every
            // connection to this endpoint is authenticated.
            let no_cert = attacker_endpoint(&AttackerCreds {
                roots_der: ca_der.clone(),
                chain_der: None,
                key_der: None,
            });
            assert!(
                !try_handshake(&no_cert, repl).await,
                "handshake without a client certificate succeeded, want rejection"
            );
            eprintln!("no client certificate: handshake rejected");

            // Foreign-CA client certificate: the handshake must fail
            // (Go: untrusted cert rejected on `/v1/status`).
            let rogue_ca = Ca::generate().expect("rogue CA");
            let rogue = rogue_ca
                .issue(&[0xau8; 16], None)
                .expect("rogue node cert");
            let foreign = attacker_endpoint(&AttackerCreds {
                roots_der: ca_der.clone(),
                chain_der: Some(rogue.cert_der.clone()),
                key_der: Some(rogue.key_der.clone()),
            });
            assert!(
                !try_handshake(&foreign, repl).await,
                "handshake with a foreign-CA certificate succeeded, want rejection"
            );
            eprintln!("foreign-CA client certificate: handshake rejected");
        });

        // Failed handshakes attached no sessions.
        assert_eq!(
            peer_count(&cluster, 0),
            0,
            "rejected handshakes attached {} sessions",
            peer_count(&cluster, 0)
        );

        // Plaintext: nothing listens on TCP at the replication port, and
        // UDP garbage creates no session (Go: plaintext HTTP refused).
        match TcpStream::connect_timeout(&repl, Duration::from_secs(5)) {
            Ok(_) => panic!("plaintext TCP unexpectedly reached the TLS endpoint"),
            Err(_) => eprintln!("plaintext TCP refused"),
        }
        let sock = UdpSocket::bind("127.0.0.1:0").expect("garbage socket");
        sock.connect(repl).expect("garbage connect");
        for _ in 0..10 {
            let _ = sock.send(&[0x55u8; 300]);
            std::thread::sleep(Duration::from_millis(50));
        }
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(
            peer_count(&cluster, 0),
            0,
            "garbage UDP attached {} sessions",
            peer_count(&cluster, 0)
        );
        eprintln!("PASS: mutual TLS enforced, plaintext refused, no phantom sessions");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("api-mtls-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
