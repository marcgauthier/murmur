//! Administrative replication operations, executed by the background worker.
use crate::{Database, Error, Result, RetentionStatus};

impl Database {
    fn retention_handle(&self) -> Result<murmur_replication::Handle> {
        if self.status().closed {
            return Err(Error::Closed);
        }
        if self
            .inner
            .thread
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|thread| thread.thread().id() == std::thread::current().id())
        {
            return Err(Error::Invalid(
                "replication administration cannot run inside SQL callbacks".into(),
            ));
        }
        self.replication()
            .ok_or_else(|| Error::Invalid("replication is not enabled".into()))
    }
    /// Permanently exclude this peer and durably release its retention obligation.
    /// Temporary disconnection does not retire a peer.
    pub fn retire_peer(&self, node: [u8; 16]) -> Result<()> {
        self.retention_handle()?
            .retire_peer(node)
            .map_err(|error| Error::Storage(error.to_string()))
    }
    /// Read durable peer acknowledgements and collection progress from Spool.
    pub fn retention_status(&self) -> Result<RetentionStatus> {
        self.retention_handle()?
            .retention_status()
            .map_err(|error| Error::Storage(error.to_string()))
    }
    /// Run a bounded background pass now, preserving configured retention limits.
    /// Returns the number of transaction log records deleted.
    pub fn collect_retention(&self) -> Result<u64> {
        self.retention_handle()?
            .collect_retention()
            .map_err(|error| Error::Storage(error.to_string()))
    }
}
