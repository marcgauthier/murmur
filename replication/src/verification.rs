//! Disposable authentication results, independent of durable staging and progress.
use crate::{Error, NodeId, Result, encode, protocol::Transaction};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const CAPACITY: usize = 8192;
type Key = ([u8; 32], [u8; 32]);
#[derive(Default)]
pub(crate) struct Cache {
    entries: BTreeSet<Key>,
    order: VecDeque<Key>,
}
pub(crate) struct Verified {
    pub bytes: Vec<u8>,
    pub digest: [u8; 32],
}
impl Cache {
    pub fn verify(
        &mut self,
        tx: &Transaction,
        database: NodeId,
        trusted: &BTreeMap<NodeId, [u8; 32]>,
        max: usize,
    ) -> Result<Verified> {
        // Always enforce current identity, trust and limits, including on a hit.
        if tx.database != database || tx.sequence == 0 || tx.payload.len() > max {
            return Err(Error::Protocol("transaction identity or size".into()));
        }
        let public = *trusted
            .get(&tx.origin)
            .ok_or_else(|| Error::Auth("untrusted transaction origin".into()))?;
        let bytes = encode(tx)?;
        // Bind every byte, including the signature and payload, to the result.
        // A changed trust key also invalidates a cached result immediately.
        let digest = Sha256::digest(&bytes).into();
        let key = (digest, public);
        if !self.entries.contains(&key) {
            tx.verify(database, trusted, max)?;
            if self.order.len() == CAPACITY {
                self.entries.remove(&self.order.pop_front().unwrap());
            }
            self.entries.insert(key);
            self.order.push_back(key);
        }
        Ok(Verified { bytes, digest })
    }
}

/// Bind staged bytes to their digest without re-checking the signature.
/// Staging verifies every transaction before its staged commit lands, and
/// the spool is trusted local storage (progress, receipts and metadata
/// are all consumed unverified), so a second Ed25519 check at apply time
/// only burns ~10ms per transaction in debug builds. Re-checking here
/// would also wedge imports after signer rotation: a transaction
/// authenticated at receipt under a now-retired key must still apply.
/// Identity, size and payload-digest checks stay: they are microseconds
/// and catch corruption loudly.
pub(crate) fn bind(tx: &Transaction, database: NodeId, max: usize) -> Result<Verified> {
    if tx.database != database || tx.sequence == 0 || tx.payload.len() > max {
        return Err(Error::Protocol("transaction identity or size".into()));
    }
    if tx.payload_digest != <[u8; 32]>::from(Sha256::digest(&tx.payload)) {
        return Err(Error::Auth("payload digest mismatch".into()));
    }
    let bytes = encode(tx)?;
    let digest = Sha256::digest(&bytes).into();
    Ok(Verified { bytes, digest })
}

#[cfg(test)]
mod tests {
    use super::*;
    const SEED: [u8; 32] = [11; 32];
    fn transaction(sequence: u64) -> Transaction {
        let mut tx = Transaction {
            database: [7; 16],
            origin: [1; 16],
            sequence,
            id: [8; 16],
            millis: 10,
            logical: 0,
            schema: [0; 32],
            payload_digest: [0; 32],
            payload: vec![42; 256],
            signature: vec![],
        };
        tx.sign(&SEED).unwrap();
        tx
    }
    fn trust() -> BTreeMap<NodeId, [u8; 32]> {
        BTreeMap::from([([1; 16], crate::protocol::public_key(&SEED))])
    }
    #[test]
    fn hits_still_reject_tampering_revocation_and_changed_limits() {
        let mut cache = Cache::default();
        let tx = transaction(1);
        let trusted = trust();
        let first = cache.verify(&tx, [7; 16], &trusted, 1024).unwrap();
        assert_eq!(first.digest, tx.digest().unwrap());
        assert_eq!(
            cache.verify(&tx, [7; 16], &trusted, 1024).unwrap().bytes,
            first.bytes
        );
        let mut altered = tx.clone();
        altered.payload[0] ^= 1;
        assert!(cache.verify(&altered, [7; 16], &trusted, 1024).is_err());
        let mut altered = tx.clone();
        altered.signature[0] ^= 1;
        assert!(cache.verify(&altered, [7; 16], &trusted, 1024).is_err());
        assert!(cache.verify(&tx, [8; 16], &trusted, 1024).is_err());
        assert!(cache.verify(&tx, [7; 16], &trusted, 255).is_err());
        assert!(cache.verify(&tx, [7; 16], &BTreeMap::new(), 1024).is_err());
        let changed = BTreeMap::from([([1; 16], crate::protocol::public_key(&[22; 32]))]);
        assert!(cache.verify(&tx, [7; 16], &changed, 1024).is_err());
        assert_eq!(cache.entries.len(), 1);
    }
    #[test]
    fn eviction_and_restart_never_replace_authentication_or_store_payloads() {
        let mut cache = Cache::default();
        let trusted = trust();
        for sequence in 1..=CAPACITY as u64 + 1 {
            cache
                .verify(&transaction(sequence), [7; 16], &trusted, 1024)
                .unwrap();
        }
        assert_eq!(cache.entries.len(), CAPACITY);
        assert_eq!(cache.order.len(), CAPACITY);
        let mut expired = transaction(1);
        expired.signature[0] ^= 1;
        assert!(cache.verify(&expired, [7; 16], &trusted, 1024).is_err());
        let mut restarted = Cache::default();
        assert!(restarted.verify(&expired, [7; 16], &trusted, 1024).is_err());
        assert!(
            restarted
                .verify(&transaction(1), [7; 16], &trusted, 1024)
                .is_ok()
        );
    }
    #[test]
    #[ignore = "release microbenchmark; not a scalability acceptance test"]
    fn repeated_authentication_cost() {
        let tx = transaction(1);
        let trusted = trust();
        let repeats = 10_000;
        let start = std::time::Instant::now();
        for _ in 0..repeats {
            std::hint::black_box(tx.verify([7; 16], &trusted, 1024).unwrap());
        }
        let uncached = start.elapsed();
        let mut cache = Cache::default();
        let start = std::time::Instant::now();
        for _ in 0..repeats {
            std::hint::black_box(cache.verify(&tx, [7; 16], &trusted, 1024).unwrap());
        }
        eprintln!(
            "{repeats} identical 256-byte transactions: uncached={uncached:?}, bounded cache={:?}",
            start.elapsed()
        );
    }
}
