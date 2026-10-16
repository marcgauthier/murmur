//! Background replication. This crate owns no SQLite connections and no storage engine.
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod transfer_tests;
pub mod bridge;
pub mod files;
pub mod policies;
pub mod protocol;
pub mod retention;
pub mod snapshot;
pub mod transport;
mod engine;
pub mod ftp;
mod membership;
mod queue;
mod filefetch;
pub use engine::{Handle, Status, ReplicationMetrics, PeerDiagnostics};
use serde::{Serialize, Deserialize};
use std::{collections::{BTreeMap, BTreeSet}, net::SocketAddr, sync::Arc, time::Duration};
pub type NodeId = [u8;16];
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid replication configuration: {0}")] Invalid(String),
    #[error("replication protocol: {0}")] Protocol(String),
    #[error("replication authentication: {0}")] Auth(String),
    #[error("replication storage: {0}")] Storage(String),
    #[error("replication overloaded")] Overloaded,
    #[error("replication unavailable: {0}")] Unavailable(String),
    #[error(transparent)] Io(#[from] std::io::Error),
}
pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> { postcard::to_allocvec(v).map_err(|e| Error::Protocol(e.to_string())) }
pub fn decode<T: serde::de::DeserializeOwned>(v: &[u8]) -> Result<T> { postcard::from_bytes(v).map_err(|e| Error::Protocol(e.to_string())) }
pub fn hex(bytes: &[u8]) -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() }
/// Run a blocking backend wait cooperatively. Backend calls funnel through
/// single-threaded Core/Spool workers via blocking channel waits; invoked
/// from a multi-thread Tokio worker, `block_in_place` steps that worker
/// aside so timers, peers, and apply keep polling instead of starving for
/// seconds. Outside multi-thread context (sync callers, blocking pool,
/// current-thread runtimes) it just runs inline.
pub fn run_blocking<T>(wait: impl FnOnce() -> T) -> T {
    let mut slot = Some(wait);
    if tokio::runtime::Handle::try_current().is_ok() {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| (slot.take().unwrap())())
        }));
        match r {
            Ok(out) => return out,
            Err(payload) => {
                // `block_in_place` panics before running anything when the
                // flavor is wrong (current-thread scheduler); the closure
                // is intact, so run it inline. A panic from `wait` itself
                // leaves nothing behind and propagates unchanged.
                if let Some(wait) = slot {
                    return wait();
                }
                std::panic::resume_unwind(payload);
            }
        }
    } else if let Some(wait) = slot {
        return wait();
    }
    unreachable!("run_blocking slot consumed without a result")
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Record { pub key: Vec<u8>, pub value: Option<Vec<u8>> }
impl Record { pub fn put(key: impl Into<Vec<u8>>, value: Vec<u8>) -> Self { Self { key:key.into(), value:Some(value) } } pub fn delete(key: impl Into<Vec<u8>>) -> Self { Self {key:key.into(),value:None} } }
/// All calls run in background workers. Implementations must serialize access to Spool.
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct Content {pub sqlite:Option<Vec<u8>>,pub records:Vec<Record>,pub origin:NodeId}
pub trait Backend: Send + Sync + 'static {
    /// Interrupt pending admission/reply waits; never abort an active durable commit.
    fn cancel_pending(&self) {}
    fn identity(&self) -> (NodeId, NodeId);
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Read many keys in one backend round-trip, preserving input order
    /// (`None` per missing key). Implementations should override the
    /// fallback: push/apply/stage read dozens of keys per round, and a
    /// round-trip per key saturates a worker-threaded backend.
    fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|key| self.get(key)).collect()
    }
    /// Read an ordered prefix of keys within a byte budget. Missing values
    /// consume no bytes; one oversized first value is allowed for progress.
    /// The default can transiently read one extra value before reaching the cap.
    fn get_many_bounded(&self,keys:&[Vec<u8>],max_bytes:usize)->Result<Vec<Option<Vec<u8>>>> {
        let mut values=Vec::new();let mut bytes=0usize;
        for key in keys {let value=self.get(key)?;let size=value.as_ref().map_or(0,Vec::len);
            if !values.is_empty()&&bytes.saturating_add(size)>max_bytes{break;}
            bytes=bytes.saturating_add(size);values.push(value);if bytes>=max_bytes{break;}}
        Ok(values)
    }
    fn keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>>;
    /// Bounded exclusive cursor traversal. Implementations should override the
    /// fallback to avoid materializing an entire prefix.
    fn keys_page(&self,prefix:&[u8],after:Option<&[u8]>,limit:usize)->Result<Vec<Vec<u8>>> {
        let mut keys=self.keys(prefix)?;keys.sort();Ok(keys.into_iter().filter(|key|after.is_none_or(|after|key.as_slice()>after)).take(limit).collect())
    }
    /// Durable per-origin progress. Backends can collect this in one serialized
    /// read to avoid one storage-worker round trip for every scalar watermark.
    fn progress(&self)->Result<Vec<protocol::Progress>> {
        use protocol::Progress;
        let mut origins=BTreeMap::<NodeId,Progress>::new();
        for key in self.keys(b"07/replication/retained/")? {if let Some(b)=self.get(&key)? {let p:Progress=decode(&b)?;origins.insert(p.origin,p);}}
        for key in self.keys(b"07/replication/applied/")? {let Some(b)=self.get(&key)? else {continue;};let suffix=std::str::from_utf8(&key).map_err(|_|Error::Protocol("progress key".into()))?.rsplit('/').next().unwrap();let origin=engine::parse_id(suffix)?;origins.entry(origin).or_insert(Progress{origin,applied:0,observed:0,retained_from:0,retained_through:0}).applied=decode(&b)?;}
        for key in self.keys(b"07/replication/observed/")? {if let Some(b)=self.get(&key)? {let origin=engine::parse_id(std::str::from_utf8(&key).map_err(|_|Error::Protocol("observed key".into()))?.rsplit('/').next().unwrap())?;let p=origins.entry(origin).or_insert(Progress{origin,applied:0,observed:0,retained_from:0,retained_through:0});p.observed=decode(&b)?;}}
        for p in origins.values_mut(){p.observed=p.observed.max(p.applied);}Ok(origins.into_values().collect())
    }
    fn commit(&self, records: Vec<Record>) -> Result<()>;
    fn import_bridge(&self, header: bridge::Header, payload: Vec<u8>) -> Result<bool>;
    fn release_ownership(&self, table:String, key:Vec<u8>, column:Option<String>) -> Result<bool>;
    fn publish(&self, records: Vec<Record>) -> Result<()>;
    fn snapshot(&self) -> Result<Vec<u8>>;
    /// Return logical state and applied progress from the same storage boundary.
    fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<protocol::Progress>)>;
    /// Capture and publish an immutable transfer and its retention pin on the
    /// serialized storage worker. Implementations must enforce the aggregate budget.
    fn create_snapshot(&self, _recipient: NodeId, _config: &Config) -> Result<snapshot::Manifest> {
        Err(Error::Unavailable("durable snapshots unsupported by backend".into()))
    }
    /// Atomically freeze the catch-up target after a durable recipient receipt.
    fn acknowledge_snapshot(&self, _recipient: NodeId, _transfer: NodeId, _digest: [u8;32]) -> Result<()> {
        Err(Error::Unavailable("durable snapshots unsupported by backend".into()))
    }
    /// Make current logical state and its schema a durable recovery snapshot.
    fn retention_checkpoint(&self) -> Result<()>;
    /// Atomically recheck snapshot coverage, delete a bounded history prefix,
    /// and publish the permanent duplicate floor without losing newer appends.
    fn prune_history(&self, origin: NodeId, through: u64) -> Result<u64>;
    /// Run a physical reclamation pass, also retrying previously deferred work.
    /// Backends should rate-limit this: segment compaction costs ~1s and
    /// collection may run every second, so an ungated reclaim per collect
    /// saturates the storage worker. Quiet passes never reach here.
    fn reclaim_retention(&self) -> Result<()>;
    fn schema(&self) -> Result<Vec<u8>>;
    /// Return false if local writes are uncollected. Effects and records must
    /// be durable and visible before returning true. A busy implementation may
    /// schedule local capture and retry after it becomes durable.
    fn apply(&self, payloads: Vec<Vec<u8>>, records: Vec<Record>) -> Result<bool>;
    /// Observe authenticated clocks without collecting a session.
    fn observe(&self, millis: u64, logical: u32) -> Result<()>;
    fn accept_schema(&self, schema: Vec<u8>) -> Result<bool>;
}
#[derive(Clone)]
pub struct Credentials { pub chain: Vec<Vec<u8>>, pub private_key: zeroize::Zeroizing<Vec<u8>>, pub roots: Vec<Vec<u8>> }
impl std::fmt::Debug for Credentials { fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result { f.debug_struct("Credentials").field("certificates",&self.chain.len()).finish_non_exhaustive() } }
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Dissemination { Gossip, Plumtree }
#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub advertise: Option<SocketAddr>,
    pub credentials: Credentials,
    pub signing_key: zeroize::Zeroizing<[u8;32]>,
    pub trusted_origins: BTreeMap<NodeId,[u8;32]>,
    pub allowed_peers: BTreeSet<NodeId>,
    pub allowed_networks: Vec<ipnet::IpNet>,
    pub trusted_snapshot_sources: BTreeSet<NodeId>,
    pub bootstrap: Vec<SocketAddr>,
    pub membership: bool,
    pub dissemination: Dissemination,
    pub accept_remote_schema: bool,
    /// This node may sign schema manifests for others. Requires
    /// `schema_master` to designate this node's own id.
    pub master: bool,
    /// The one node whose signed schemas are adopted. `None` keeps legacy
    /// symmetric adoption from any trusted origin.
    pub schema_master: Option<NodeId>,
    pub fanout: usize,
    pub repairs: usize,
    pub sessions: usize,
    pub connections: usize,
    pub queue_bytes: usize,
    pub peer_queue_bytes: usize,
    pub staging_bytes: usize,
    pub staging_transfers: usize,
    pub max_transaction_bytes: usize,
    pub snapshot_bytes: usize,
    /// Aggregate payload budget for each direction of durable snapshot staging.
    pub snapshot_staging_bytes: usize,
    /// Fixed lease for transfers and their history pins; reconnects do not renew it.
    pub snapshot_transfer_lifetime: Duration,
    /// Recovery retry interval; continuous processing is not rate-limited by it.
    pub incoming_interval: Duration,
    pub batch_transactions: usize,
    pub batch_bytes: usize,
    pub adaptive_sync: bool,
    pub anti_entropy_interval: Duration,
    pub retention: Duration,
    pub retention_interval: Duration,
    pub minimum_retained: u64,
    pub maximum_offline: Duration,
    pub merge_policies: BTreeMap<(String,String), policies::Policy>,
    pub files: Option<files::Config>,
    pub bridge: Option<bridge::Config>,
}
impl Config {
    pub fn new(bind:SocketAddr, credentials:Credentials, signing_key:[u8;32]) -> Self {
        Self {bind,advertise:None,credentials,signing_key:zeroize::Zeroizing::new(signing_key),trusted_origins:BTreeMap::new(),allowed_peers:BTreeSet::new(),allowed_networks:Vec::new(),trusted_snapshot_sources:BTreeSet::new(),bootstrap:Vec::new(),membership:true,dissemination:Dissemination::Gossip,accept_remote_schema:true,master:false,schema_master:None,fanout:4,repairs:2,sessions:32,connections:64,queue_bytes:64<<20,peer_queue_bytes:4<<20,staging_bytes:256<<20,staging_transfers:4096,max_transaction_bytes:64<<20,snapshot_bytes:512<<20,snapshot_staging_bytes:512<<20,snapshot_transfer_lifetime:Duration::from_secs(7*86400),incoming_interval:Duration::from_secs(1),batch_transactions:64,batch_bytes:4<<20,adaptive_sync:false,anti_entropy_interval:Duration::from_secs(10),retention:Duration::from_secs(3600),retention_interval:Duration::from_secs(60),minimum_retained:1000,maximum_offline:Duration::from_secs(7*86400),merge_policies:BTreeMap::new(),files:None,bridge:None}
    }
    pub fn validate(&self) -> Result<()> {
        if self.batch_transactions==0 || self.batch_transactions>1024 || self.batch_bytes==0 || self.batch_bytes>64<<20 || self.queue_bytes<=1<<20 || self.queue_bytes>128<<20 || self.connections>4096 || self.sessions>4096 || self.fanout==0 || self.repairs==0 || self.fanout+self.repairs>self.sessions || self.connections<self.sessions+8 || self.queue_bytes<self.peer_queue_bytes || self.peer_queue_bytes<1<<20 || self.staging_bytes<self.max_transaction_bytes.saturating_add(4096) || self.staging_transfers==0 || self.max_transaction_bytes==0 || self.max_transaction_bytes>64<<20 || self.incoming_interval.is_zero() || self.anti_entropy_interval.is_zero() || self.retention_interval.is_zero() || self.maximum_offline.is_zero() || self.minimum_retained==0 || self.snapshot_transfer_lifetime.is_zero() || self.snapshot_staging_bytes==0 || self.snapshot_bytes==0 || self.credentials.chain.is_empty() || self.credentials.roots.is_empty() { return Err(Error::Invalid("inconsistent transport or capacity limits".into())); }
        if self.master && self.schema_master.is_none() { return Err(Error::Invalid("schema master requires schema_master".into())); }
        if let Some(c)=&self.files { c.validate()?; }
        if let Some(c)=&self.bridge { c.validate()?; }
        Ok(())
    }
}
/// Durable signing-key binding. Never replace a key under the same node ID.
pub fn bind_signer(backend: &dyn Backend, config:&Config) -> Result<()> {
    use ed25519_dalek::SigningKey;
    let (_,node)=backend.identity();
    let public=SigningKey::from_bytes(&config.signing_key).verifying_key().to_bytes();
    if config.trusted_origins.get(&node)!=Some(&public) { return Err(Error::Auth("local signing key is not registered".into())); }
    let key=format!("07/replication/signer/{}",hex(&node)).into_bytes();
    match backend.get(&key)? { Some(old) if old!=public => Err(Error::Auth("signing identity changed; use a fresh node identity".into())), Some(_)=>Ok(()), None=>backend.commit(vec![Record::put(key,public.to_vec())]) }?;
    if let Some(master)=config.schema_master {
        if !config.trusted_origins.contains_key(&master) { return Err(Error::Auth("schema master has no trusted origin key".into())); }
        if config.master && master!=node { return Err(Error::Auth("schema master must designate its own node id".into())); }
    } else if config.master {
        return Err(Error::Auth("schema master requires schema_master".into()));
    }
    Ok(())
}
pub fn start(config:Config, backend:Arc<dyn Backend>) -> Result<Handle> { Handle::start(config,backend) }

impl std::fmt::Debug for Config {fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result {f.debug_struct("ReplicationConfig").field("bind",&self.bind).field("fanout",&self.fanout).field("dissemination",&self.dissemination).finish_non_exhaustive()}}

mod verification;
