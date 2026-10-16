//! An immutable Spool checkpoint supplies snapshot records without holding the writer.
use crate::{Error, Result, logical_store::LogicalStore, metadata, model::*};
use murmur_replication::{self as net, Record};
use std::collections::VecDeque;
pub(crate) struct Source {
    pub checkpoint: spool::Checkpoint,
    pub options: spool::Options,
    pub schema: Schema,
    pub clock: Version,
    pub identity: ([u8; 16], [u8; 16]),
    pub progress: Vec<net::protocol::Progress>,
    pub budget: usize,
}
impl Drop for Source {
    fn drop(&mut self) {
        let _ = self.checkpoint.release();
        let _ = std::fs::remove_dir_all(&self.checkpoint.dir);
        zeroize::Zeroize::zeroize(&mut self.options.master_key);
    }
}
pub(crate) struct Pages {
    store: LogicalStore,
    source: Source,
    prefix: usize,
    after: Option<Vec<u8>>,
    keys: VecDeque<Vec<u8>>,
}
const PREFIXES: &[&[u8]] = &[
    b"04/meta/row/",
    b"05/meta/tomb/",
    b"08/files/meta/",
    b"10/policy/",
    b"09/bridge/owner/",
    b"09/bridge/shadow/",
    b"09/bridge/provenance/",
];
impl Pages {
    pub fn open(mut source: Source) -> Result<Self> {
        let store = LogicalStore::new(spool::open(&source.options)?);
        zeroize::Zeroize::zeroize(&mut source.options.master_key);
        Ok(Self {
            store,
            source,
            prefix: 0,
            after: None,
            keys: VecDeque::new(),
        })
    }
    pub fn source(&self) -> &Source {
        &self.source
    }
    fn key(&mut self) -> Option<Vec<u8>> {
        while self.prefix < PREFIXES.len() {
            if let Some(key) = self.keys.pop_front() {
                self.after = Some(key.clone());
                return Some(key);
            }
            self.keys = self
                .store
                .keys_page(PREFIXES[self.prefix], self.after.as_deref(), 256)
                .into();
            if self.keys.is_empty() {
                self.prefix += 1;
                self.after = None;
            }
        }
        None
    }
    fn page(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            let mut state = ReplicaState {
                database_id: self.source.identity.0,
                schema: crate::table_replication::filter_schema(&self.source.schema),
                clock: self.source.clock,
                rows: Vec::new(),
                tombstones: Vec::new(),
            };
            let mut records = Vec::new();
            let mut bytes = 0;
            for _ in 0..256 {
                let Some(key) = self.key() else { break };
                let Some(value) = self.store.get(&key)? else {
                    continue;
                };
                bytes += value.len();
                if key.starts_with(b"04/meta/row/") {
                    let table = std::str::from_utf8(&key)
                        .map_err(|_| Error::SchemaMismatch)?
                        .split('/')
                        .nth(3)
                        .ok_or(Error::SchemaMismatch)?;
                    if crate::table_replication::enabled(&self.source.schema, table) {
                        let meta: metadata::RowMeta = decode(&value)?;
                        state.rows.push(RowState {
                            table: table.into(),
                            key: decode(&meta.key)?,
                            generation: meta.generation,
                            cells: meta.cells,
                        });
                    }
                } else if key.starts_with(b"05/meta/tomb/") {
                    let tomb: Tombstone = decode(&value)?;
                    if crate::table_replication::enabled(&self.source.schema, &tomb.table) {
                        state.tombstones.push(tomb);
                    }
                } else {
                    records.push(Record::put(key, value));
                }
                if bytes >= 1 << 20 {
                    break;
                }
            }
            records = crate::table_replication::filter_records(records, &self.source.schema)?;
            if state.rows.is_empty() && state.tombstones.is_empty() && records.is_empty() {
                if self.prefix < PREFIXES.len() {
                    continue;
                }
                return Ok(None);
            }
            let content = net::Content {
                sqlite: Some(encode(&state)?),
                records,
                origin: self.source.identity.1,
            };
            return Ok(Some(
                net::encode(&content).map_err(|e| Error::Storage(e.to_string()))?,
            ));
        }
    }
}
impl Iterator for Pages {
    type Item = net::Result<Vec<u8>>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.page() {
            Ok(Some(page)) => Some(Ok(page)),
            Ok(None) => None,
            Err(e) => {
                self.prefix = PREFIXES.len();
                Some(Err(net::Error::Storage(e.to_string())))
            }
        }
    }
}
impl Drop for Pages {
    fn drop(&mut self) {
        let _ = self.store.close();
    }
}
