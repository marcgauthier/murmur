//! Logical values split into authenticated Spool blocks. Only Spool is authoritative.
use serde::{Deserialize, Serialize};
use spool::{Durability, Mutation, Store};
use std::ops::Deref;
const MAGIC: &[u8] = b"MURMUR-BLOB\0\x01";
const PIECE: usize = 64 << 10;
const INLINE: usize = 1 << 20;
#[derive(Serialize, Deserialize)]
enum Descriptor {
    Inline(Vec<u8>),
    Pieces {
        bytes: usize,
        digest: [u8; 32],
        count: usize,
    },
}
#[derive(Clone)]
pub(crate) struct LogicalStore(
    Store,
    #[cfg(test)] std::sync::Arc<std::sync::atomic::AtomicUsize>,
);
impl Deref for LogicalStore {
    type Target = Store;
    fn deref(&self) -> &Store {
        &self.0
    }
}
fn corrupt(message: impl Into<String>) -> spool::Error {
    spool::Error::new(spool::ErrorKind::Corrupt, message)
}
fn piece_key(key: &[u8], digest: &[u8; 32], index: usize) -> Vec<u8> {
    format!(
        "11/blob/{}/{}/{index:08}",
        blake3::hash(key).to_hex(),
        blake3::Hash::from(*digest).to_hex()
    )
    .into_bytes()
}
fn descriptor(bytes: &[u8]) -> spool::Result<Option<Descriptor>> {
    if !bytes.starts_with(MAGIC) {
        return Ok(None);
    }
    postcard::from_bytes(&bytes[MAGIC.len()..])
        .map(Some)
        .map_err(|e| corrupt(e.to_string()))
}
impl LogicalStore {
    pub fn new(store: Store) -> Self {
        Self(
            store,
            #[cfg(test)]
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
    }
    pub fn physical(&self) -> Store {
        self.0.clone()
    }
    #[cfg(test)]
    pub fn reads(&self) -> usize {
        self.1.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn get(&self, key: &[u8]) -> spool::Result<Option<Vec<u8>>> {
        #[cfg(test)]
        self.1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(value) = self.0.get(key)? else {
            return Ok(None);
        };
        match if key < b"04/".as_slice() {
            None
        } else {
            descriptor(&value)?
        } {
            None => Ok(Some(value)),
            Some(Descriptor::Inline(value)) => Ok(Some(value)),
            Some(Descriptor::Pieces {
                bytes,
                digest,
                count,
            }) => {
                if count != bytes.div_ceil(PIECE) || bytes > 512 << 20 {
                    return Err(corrupt("blob descriptor bounds"));
                }
                let mut value = Vec::with_capacity(bytes);
                for index in 0..count {
                    let part = self
                        .0
                        .get(&piece_key(key, &digest, index))?
                        .ok_or_else(|| corrupt("missing blob piece"))?;
                    if part.len() != PIECE.min(bytes - index * PIECE) {
                        return Err(corrupt("blob piece length"));
                    }
                    value.extend_from_slice(&part);
                }
                if blake3::hash(&value).as_bytes() != &digest {
                    return Err(corrupt("blob digest"));
                }
                Ok(Some(value))
            }
        }
    }
    pub fn commit(&self, mutations: &[Mutation], durability: Durability) -> spool::Result<()> {
        // Include old-piece cleanup in the same atomic commit as descriptor replacement.
        let mut records = Vec::new();
        let mut previous = std::collections::BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
        for mutation in mutations {
            let old = match previous.get(&mutation.key) {
                Some(v) => v.clone(),
                None => self.0.get(&mutation.key)?,
            };
            if let Some(value) = old {
                if let Some(Descriptor::Pieces { digest, count, .. }) =
                    if mutation.key.as_slice() < b"04/".as_slice() {
                        None
                    } else {
                        descriptor(&value)?
                    }
                {
                    for index in 0..count {
                        records.push(Mutation::delete(piece_key(&mutation.key, &digest, index)));
                    }
                }
            }
            let value = if mutation.deleted {
                None
            } else if mutation.key.as_slice() >= b"04/".as_slice() && mutation.value.len() > INLINE
            {
                let digest = *blake3::hash(&mutation.value).as_bytes();
                for (index, part) in mutation.value.chunks(PIECE).enumerate() {
                    records.push(Mutation::put(
                        piece_key(&mutation.key, &digest, index),
                        part.to_vec(),
                    ));
                }
                let mut value = MAGIC.to_vec();
                value.extend(
                    postcard::to_allocvec(&Descriptor::Pieces {
                        bytes: mutation.value.len(),
                        digest,
                        count: mutation.value.len().div_ceil(PIECE),
                    })
                    .map_err(|e| corrupt(e.to_string()))?,
                );
                Some(value)
            } else if mutation.key.as_slice() >= b"04/".as_slice()
                && mutation.value.starts_with(MAGIC)
            {
                let mut value = MAGIC.to_vec();
                value.extend(
                    postcard::to_allocvec(&Descriptor::Inline(mutation.value.clone()))
                        .map_err(|e| corrupt(e.to_string()))?,
                );
                Some(value)
            } else {
                Some(mutation.value.clone())
            };
            records.push(match &value {
                Some(value) => Mutation::put(mutation.key.clone(), value.clone()),
                None => Mutation::delete(mutation.key.clone()),
            });
            previous.insert(mutation.key.clone(), value);
        }
        self.0.commit(&records, durability)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_overwrite_delete_and_magic_are_atomic_and_recoverable() {
        let temp = tempfile::tempdir().unwrap();
        let mut options = spool::default_options(temp.path().to_string_lossy());
        options.master_key = vec![9; 32];
        options.reclaim_interval = None;
        let store = LogicalStore::new(spool::open(&options).unwrap());
        let key = b"04/test/value";
        let data = vec![42; 17 << 20];
        store
            .commit(&[Mutation::put(key, data.clone())], Durability::Sync)
            .unwrap();
        assert_eq!(store.get(key).unwrap(), Some(data));
        assert!(store.0.keys_with_prefix(b"11/blob/").len() > 256);
        store
            .commit(
                &[
                    Mutation::put(key, MAGIC.to_vec()),
                    Mutation::put(key, b"second".to_vec()),
                ],
                Durability::Sync,
            )
            .unwrap();
        assert_eq!(store.get(key).unwrap(), Some(b"second".to_vec()));
        assert!(store.0.keys_with_prefix(b"11/blob/").is_empty());
        store
            .commit(&[Mutation::put(key, MAGIC.to_vec())], Durability::Sync)
            .unwrap();
        store.close().unwrap();
        drop(store);
        let store = LogicalStore::new(spool::open(&options).unwrap());
        assert_eq!(store.get(key).unwrap(), Some(MAGIC.to_vec()));
        store
            .commit(&[Mutation::delete(key)], Durability::Sync)
            .unwrap();
        assert_eq!(store.get(key).unwrap(), None);
        store.close().unwrap();
    }
}
