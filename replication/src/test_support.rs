//! An empty backend that rejects unexpected mutations in staging tests.
use crate::*;

pub(crate) struct Empty;
impl Backend for Empty {
    fn identity(&self) -> (NodeId, NodeId) {
        ([1; 16], [2; 16])
    }
    fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    fn keys(&self, _: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![])
    }
    fn commit(&self, _: Vec<Record>) -> Result<()> {
        unreachable!()
    }
    fn publish(&self, _: Vec<Record>) -> Result<()> {
        unreachable!()
    }
    fn import_bridge(&self, _: bridge::Header, _: Vec<u8>) -> Result<bool> {
        unreachable!()
    }
    fn release_ownership(&self, _: String, _: Vec<u8>, _: Option<String>) -> Result<bool> {
        unreachable!()
    }
    fn snapshot(&self) -> Result<Vec<u8>> {
        unreachable!()
    }
    fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<protocol::Progress>)> {
        unreachable!()
    }
    fn retention_checkpoint(&self) -> Result<()> {
        unreachable!()
    }
    fn prune_history(&self, _: NodeId, _: u64) -> Result<u64> {
        unreachable!()
    }
    fn reclaim_retention(&self) -> Result<()> {
        unreachable!()
    }
    fn schema(&self) -> Result<Vec<u8>> {
        unreachable!()
    }
    fn apply(&self, _: Vec<Vec<u8>>, _: Vec<Record>) -> Result<bool> {
        unreachable!()
    }
    fn observe(&self, _: u64, _: u32) -> Result<()> {
        unreachable!()
    }
    fn accept_schema(&self, _: Vec<u8>) -> Result<bool> {
        unreachable!()
    }
}
