#![cfg(feature = "replication")]
//! Large (multi-chunk) transaction replication: one ~4 MiB checkpoint must
//! flow through the chunked path and converge on the replica.
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType};
use sqlite_session::{
    Database, Migration, OpenMode, Options,
    replication::{self, Config, Credentials},
};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

fn configs() -> Vec<Config> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&ca_key).unwrap();
    let ids = [[1; 16], [2; 16]];
    let seeds = [[11; 32], [22; 32]];
    let mut trust = BTreeMap::new();
    for (id, seed) in ids.iter().zip(seeds) {
        trust.insert(*id, replication::protocol::public_key(&seed));
    }
    ids.iter()
        .zip(seeds)
        .map(|(id, seed)| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.subject_alt_names = vec![SanType::URI(
                format!("replicateddb://node/{}", replication::hex(id))
                    .try_into()
                    .unwrap(),
            )];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
            let credentials = Credentials {
                chain: vec![cert.der().to_vec()],
                private_key: key.serialize_der().into(),
                roots: vec![ca.der().to_vec()],
            };
            let mut config = Config::new("127.0.0.1:0".parse().unwrap(), credentials, seed);
            config.trusted_origins = trust.clone();
            config
        })
        .collect()
}

fn count(db: &Database) -> i64 {
    db.read(|s| {
        Ok(s.query_row("SELECT COUNT(*) FROM items", [], |r| r.get::<_, i64>(0))?)
    })
    .unwrap_or(-1)
}

#[test]
fn single_large_transaction_converges() {
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = configs();
    let opts = |config| Options {
        mode: OpenMode::CreateNew,
        database_id: Some([7; 16]),
        replication: Some(config),
        capture_interval: Duration::from_secs(3600),
        ..Options::default()
    };
    let a = Database::open(temp.path().join("a"), [9; 32], opts(cfg.remove(0))).unwrap();
    let b = Database::open(temp.path().join("b"), [9; 32], opts(cfg.remove(0))).unwrap();
    for db in [&a, &b] {
        db.migrate(Migration::new(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
        ))
        .unwrap();
        db.flush().unwrap();
    }
    // One ~4 MiB checkpoint: forces the chunked (multi-message) path.
    let value = "x".repeat(200);
    for chunk in (1..=15_000i64).collect::<Vec<_>>().chunks(500) {
        let rows: Vec<(i64, String)> = chunk.iter().map(|id| (*id, value.clone())).collect();
        a.transaction(move |s| {
            for (id, v) in &rows {
                s.execute("INSERT INTO items VALUES(?, ?)", (*id, v))?;
            }
            Ok(())
        })
        .unwrap();
    }
    a.flush().unwrap();
    assert_eq!(count(&a), 15_000);
    b.replication().unwrap().add_peer(a.replication().unwrap().status().address.unwrap()).unwrap();
    let end = Instant::now() + Duration::from_secs(90);
    let start = Instant::now();
    let mut last_log = Instant::now() - Duration::from_secs(10);
    loop {
        if count(&b) == 15_000 {
            break;
        }
        if last_log.elapsed() > Duration::from_secs(2) {
            let ra = a.replication().unwrap().status();
            let rb = b.replication().unwrap().status();
            eprintln!(
                "waiting: t={}s replica rows={} peers={}/{} pushed={} recv={} staged={} applied={} qa={}/{} a_err={:?} b_err={:?}",
                start.elapsed().as_secs(),
                count(&b),
                ra.peers,
                rb.peers,
                ra.metrics.pushed_transactions,
                rb.metrics.received_transactions,
                rb.staged_transactions,
                rb.applied_transactions,
                ra.metrics.queue_available_bytes,
                rb.metrics.queue_available_bytes,
                ra.last_error,
                rb.last_error,
            );
            last_log = Instant::now();
        }
        assert!(
            Instant::now() < end,
            "large transaction did not converge: replica rows={}",
            count(&b)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    a.close().unwrap();
    b.close().unwrap();
}
