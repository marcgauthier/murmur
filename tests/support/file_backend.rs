//! Real encrypted Spool storage for object/bridge tests, with commit fault injection.
use sqlite_session::replication::{self as net, Backend, Record};
use std::{path::Path, sync::atomic::{AtomicUsize, Ordering}};

pub struct FileBackend {
    pub store: spool::Store,
    identity: (net::NodeId, net::NodeId),
    pub fail_commit: AtomicUsize,
}
impl FileBackend {
    pub fn open(path: &Path, domain: u8, node: u8) -> Self {
        let mut options = spool::default_options(path.to_string_lossy().into_owned());
        options.master_key = vec![42; 32];
        options.reclaim_interval = None;
        Self { store: spool::open(&options).unwrap(), identity: ([domain; 16], [node; 16]), fail_commit: AtomicUsize::new(0) }
    }
}
fn unsupported<T>() -> net::Result<T> { Err(net::Error::Invalid("unexpected SQL/retention operation in file fixture".into())) }
impl Backend for FileBackend {
    fn identity(&self) -> (net::NodeId, net::NodeId) { self.identity }
    fn get(&self, key: &[u8]) -> net::Result<Option<Vec<u8>>> { self.store.get(key).map_err(|e| net::Error::Storage(e.to_string())) }
    fn keys(&self, prefix: &[u8]) -> net::Result<Vec<Vec<u8>>> { Ok(self.store.keys_with_prefix(prefix)) }
    fn commit(&self, records: Vec<Record>) -> net::Result<()> {
        if self.fail_commit.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)) == Ok(1) {
            return Err(net::Error::Storage("injected commit failure".into()));
        }
        let mutations: Vec<_> = records.into_iter().map(|r| match r.value {
            Some(value) => spool::Mutation::put(r.key, value), None => spool::Mutation::delete(r.key),
        }).collect();
        self.store.commit(&mutations, spool::Durability::Sync).map_err(|e| net::Error::Storage(e.to_string()))
    }
    fn publish(&self, records: Vec<Record>) -> net::Result<()> { self.commit(records) }
    fn import_bridge(&self, _: net::bridge::Header, _: Vec<u8>) -> net::Result<bool> { unsupported() }
    fn release_ownership(&self, _: String, _: Vec<u8>, _: Option<String>) -> net::Result<bool> { unsupported() }
    fn snapshot(&self) -> net::Result<Vec<u8>> { unsupported() }
    fn snapshot_with_progress(&self) -> net::Result<(Vec<u8>, Vec<net::protocol::Progress>)> { unsupported() }
    fn retention_checkpoint(&self) -> net::Result<()> { unsupported() }
    fn prune_history(&self, _: net::NodeId, _: u64) -> net::Result<u64> { unsupported() }
    fn reclaim_retention(&self) -> net::Result<()> { unsupported() }
    fn schema(&self) -> net::Result<Vec<u8>> { unsupported() }
    fn apply(&self, _: Vec<Vec<u8>>, _: Vec<Record>) -> net::Result<bool> { unsupported() }
    fn observe(&self, _: u64, _: u32) -> net::Result<()> { unsupported() }
    fn accept_schema(&self, _: Vec<u8>) -> net::Result<bool> { unsupported() }
}
