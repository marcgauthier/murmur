//! Test CA + per-node QUIC/TLS credential provisioning (rcgen).
//!
//! Mirrors the `tests/replication.rs` `configs()` pattern: one self-signed CA
//! per cluster, one leaf per node with SAN URI `replicateddb://node/<hex>`
//! (the identity [`murmur_replication::transport::node_id`] parses) and
//! server+client EKU. `tls/` files are kept per node for forensics parity.

use std::path::Path;
use std::time::Duration;

/// Cluster test certificate authority.
pub struct Ca {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

/// Issued leaf material for one node.
pub struct Issued {
    pub credentials: murmur_replication::Credentials,
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
}

impl Ca {
    pub fn generate() -> Result<Self, rcgen::Error> {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate()?;
        let cert = params.self_signed(&key)?;
        Ok(Self { cert, key })
    }

    pub fn ca_der(&self) -> Vec<u8> {
        self.cert.der().to_vec()
    }

    /// Issue a leaf for `node_id`. `ttl` overrides the (effectively
    /// unbounded) default validity; used by `cert-lifecycle` for expiry legs.
    pub fn issue(
        &self,
        node_id: &[u8; 16],
        ttl: Option<Duration>,
    ) -> Result<Issued, rcgen::Error> {
        let key = rcgen::KeyPair::generate()?;
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new())?;
        params.subject_alt_names = vec![rcgen::SanType::URI(
            format!("replicateddb://node/{}", murmur_replication::hex(node_id)).try_into()?,
        )];
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        if let Some(ttl) = ttl {
            let now = time::OffsetDateTime::now_utc();
            params.not_before = now;
            params.not_after =
                now + time::Duration::try_from(ttl).unwrap_or(time::Duration::MAX);
        }
        let cert = params.signed_by(&key, &self.cert, &self.key)?;
        let cert_der = cert.der().to_vec();
        let key_der = key.serialize_der();
        Ok(Issued {
            credentials: murmur_replication::Credentials {
                chain: vec![cert_der.clone()],
                private_key: key_der.clone().into(),
                roots: vec![self.ca_der()],
            },
            cert_der,
            key_der,
        })
    }

    /// Write `tls/ca.der`, `tls/node.der`, `tls/node.key` under `node_dir`.
    pub fn write_tls_dir(
        &self,
        issued: &Issued,
        node_dir: &Path,
    ) -> std::io::Result<()> {
        let tls = node_dir.join("tls");
        std::fs::create_dir_all(&tls)?;
        std::fs::write(tls.join("ca.der"), self.ca_der())?;
        std::fs::write(tls.join("node.der"), &issued.cert_der)?;
        let key_path = tls.join("node.key");
        std::fs::write(&key_path, &issued.key_der)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
}
