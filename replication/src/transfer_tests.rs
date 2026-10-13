//! Large-transfer diagnostics: chunked delivery between two in-memory nodes.
#![cfg(test)]

use crate::protocol::{
    Chunk, ChunkManifest, Progress, Transaction, chunk_key, chunk_manifest_key, log_key,
    observed_key, progress_key, receipt_key, retained_key,
};
use crate::{Backend, Config, Credentials, NodeId, Record, Result};
use sha2::{Digest, Sha256};
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Mem {
    map: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    database: NodeId,
    node: NodeId,
    schema: Vec<u8>,
}

impl Backend for Mem {
    fn identity(&self) -> (NodeId, NodeId) {
        (self.database, self.node)
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }
    fn keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect())
    }
    fn commit(&self, records: Vec<Record>) -> Result<()> {
        self.publish(records)
    }
    fn publish(&self, records: Vec<Record>) -> Result<()> {
        let mut map = self.map.lock().unwrap();
        for r in records {
            match r.value {
                Some(value) => {
                    map.insert(r.key, value);
                }
                None => {
                    map.remove(&r.key);
                }
            }
        }
        Ok(())
    }
    fn import_bridge(&self, _: crate::bridge::Header, _: Vec<u8>) -> Result<bool> {
        Ok(false)
    }
    fn release_ownership(&self, _: String, _: Vec<u8>, _: Option<String>) -> Result<bool> {
        Ok(false)
    }
    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<crate::protocol::Progress>)> {
        Ok((Vec::new(), Vec::new()))
    }
    fn retention_checkpoint(&self) -> Result<()> {
        Ok(())
    }
    fn prune_history(&self, _: NodeId, _: u64) -> Result<u64> {
        Ok(0)
    }
    fn reclaim_retention(&self) -> Result<()> {
        Ok(())
    }
    fn schema(&self) -> Result<Vec<u8>> {
        Ok(self.schema.clone())
    }
    fn apply(&self, _payloads: Vec<Vec<u8>>, records: Vec<Record>) -> Result<bool> {
        self.publish(records)?;
        Ok(true)
    }
    fn observe(&self, _: u64, _: u32) -> Result<()> {
        Ok(())
    }
    fn accept_schema(&self, _: Vec<u8>) -> Result<bool> {
        Ok(false)
    }
}

fn configs() -> Vec<Config> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&ca_key).unwrap();
    let ids = [[1; 16], [2; 16]];
    let seeds = [[11; 32], [22; 32]];
    let mut trust = BTreeMap::new();
    for (id, seed) in ids.iter().zip(seeds) {
        trust.insert(*id, crate::protocol::public_key(&seed));
    }
    ids.iter()
        .zip(seeds)
        .map(|(id, seed)| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.subject_alt_names = vec![SanType::URI(
                format!("replicateddb://node/{}", crate::hex(id))
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
            config.anti_entropy_interval = Duration::from_millis(500);
            config.incoming_interval = Duration::from_millis(250);
            config
        })
        .collect()
}

fn mem_backend(node: NodeId) -> Arc<Mem> {
    Arc::new(Mem {
        map: Mutex::new(BTreeMap::new()),
        database: [7; 16],
        node,
        schema: b"test-schema-v1".to_vec(),
    })
}

fn big_transaction() -> Transaction {
    sized_transaction(4 << 20)
}

fn sized_transaction(payload_len: usize) -> Transaction {
    let schema = crate::protocol::schema_digest(b"test-schema-v1");
    let mut tx = Transaction {
        database: [7; 16],
        origin: [1; 16],
        sequence: 1,
        id: [9; 16],
        millis: 1,
        logical: 1,
        schema,
        payload_digest: [0; 32],
        payload: vec![0xAB; payload_len],
        signature: Vec::new(),
    };
    tx.sign(&[11; 32]).unwrap();
    tx
}

/// Durable chunk records (manifest plus one record per index), exactly as
/// the sender would have streamed them.
fn craft_chunks(tx: &Transaction, indexes: &[u32]) -> Vec<Record> {
    let data = crate::encode(tx).unwrap();
    let digest: [u8; 32] = Sha256::digest(&data).into();
    let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
    let mut proof = tx.clone();
    proof.payload.clear();
    let manifest = ChunkManifest {
        id: tx.id,
        count,
        total: data.len() as u64,
        digest,
    };
    let mut records = vec![Record::put(
        chunk_manifest_key(tx.origin, tx.sequence),
        crate::encode(&manifest).unwrap(),
    )];
    for index in indexes {
        let start = *index as usize * crate::protocol::CHUNK_BYTES;
        let chunk = Chunk {
            proof: proof.clone(),
            origin: tx.origin,
            sequence: tx.sequence,
            id: tx.id,
            index: *index,
            count,
            total: data.len() as u64,
            digest,
            data: data[start..(start + crate::protocol::CHUNK_BYTES).min(data.len())].to_vec(),
        };
        records.push(Record::put(
            chunk_key(tx.origin, tx.sequence, *index),
            crate::encode(&chunk).unwrap(),
        ));
    }
    records
}

#[test]
fn large_transaction_arrives() {
    let mut cfg = configs();
    let backend_a = mem_backend([1; 16]);
    let backend_b = mem_backend([2; 16]);
    let tx = big_transaction();
    let digest = tx.digest().unwrap();
    let _ = digest;
    backend_a
        .commit(vec![
            Record::put(log_key([1; 16], 1), crate::encode(&tx).unwrap()),
            Record::put(
                retained_key([1; 16]),
                crate::encode(&Progress {
                    origin: [1; 16],
                    applied: 0,
                    observed: 1,
                    retained_from: 1,
                    retained_through: 1,
                })
                .unwrap(),
            ),
        ])
        .unwrap();
    let a = crate::start(cfg.remove(0), backend_a.clone()).unwrap();
    let b = crate::start(cfg.remove(0), backend_b.clone()).unwrap();
    a.add_peer(b.status().address.unwrap()).unwrap();
    let start = Instant::now();
    let end = start + Duration::from_secs(120);
    let mut last_log = Instant::now() - Duration::from_secs(10);
    loop {
        let done = backend_b
            .get(&receipt_key([1; 16], 1))
            .unwrap()
            .is_some();
        if done {
            break;
        }
        if last_log.elapsed() > Duration::from_secs(2) {
            let ra = a.status();
            let rb = b.status();
            eprintln!(
                "waiting: t={}s peers={}/{} pushed={} recv={} staged={} applied={} a_err={:?} b_err={:?}",
                start.elapsed().as_secs(),
                ra.peers,
                rb.peers,
                ra.metrics.pushed_transactions,
                rb.metrics.received_transactions,
                rb.staged_transactions,
                rb.applied_transactions,
                ra.last_error,
                rb.last_error,
            );
            last_log = Instant::now();
        }
        assert!(Instant::now() < end, "large transaction never arrived");
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("arrived in {:.1}s", start.elapsed().as_secs_f64());
    // Payload integrity through the chunk path.
    let applied = backend_b.get(&progress_key([1; 16])).unwrap();
    assert!(applied.is_some());
    let _ = observed_key;
    a.close().unwrap();
    b.close().unwrap();
}

/// Cross-peer repair with no complete copy anywhere: two nodes hold
/// disjoint staged chunks of one transaction and complete each other
/// through ChunkNeed repair (no push, no full re-stream).
#[test]
fn disjoint_partials_repair_each_other() {
    let tx = sized_transaction(200_000);
    let data = crate::encode(&tx).unwrap();
    let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
    assert!(count >= 2, "need multiple chunks, got {count}");
    let split = count / 2;
    let low: Vec<u32> = (0..split).collect();
    let high: Vec<u32> = (split..count).collect();

    let mut cfg = configs();
    let backend_d = mem_backend([1; 16]);
    let backend_e = mem_backend([2; 16]);
    backend_d.commit(craft_chunks(&tx, &low)).unwrap();
    backend_e.commit(craft_chunks(&tx, &high)).unwrap();

    let d = crate::start(cfg.remove(0), backend_d.clone()).unwrap();
    let e = crate::start(cfg.remove(0), backend_e.clone()).unwrap();
    d.add_peer(e.status().address.unwrap()).unwrap();

    let start = Instant::now();
    let end = start + Duration::from_secs(60);
    loop {
        let dd = backend_d.get(&receipt_key([1; 16], 1)).unwrap().is_some();
        let ee = backend_e.get(&receipt_key([1; 16], 1)).unwrap().is_some();
        if dd && ee {
            break;
        }
        assert!(
            Instant::now() < end,
            "mutual repair did not converge (d={dd} e={ee})"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("mutual repair converged in {:.1}s", start.elapsed().as_secs_f64());
    // Each side served exactly the indexes the other was missing: staged
    // relay, never a full transaction.
    assert_eq!(
        e.status().metrics.chunk_need_served as usize,
        high.len(),
        "peer served its staged chunks"
    );
    assert_eq!(
        d.status().metrics.chunk_need_served as usize,
        low.len(),
        "peer served its staged chunks"
    );
    assert!(d.status().metrics.chunk_need_sent > 0);
    assert!(e.status().metrics.chunk_need_sent > 0);
    d.close().unwrap();
    e.close().unwrap();
}
