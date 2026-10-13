//! In-process multi-`Database` cluster management.
//!
//! A "node" is an open [`Database`](sqlite_session::Database) on its own
//! directory (`runtime/<scenario>/nodeN/`). Stop/restart is `close()` plus
//! `Database::open(same dir, same key)`; the replication mesh is QUIC with
//! ephemeral `127.0.0.1:0` binds, so no port registry is needed. True
//! `SIGKILL`/`SIGSTOP`/`SIGTERM` legs additionally need fixture subprocesses
//! (see [`crate::fixture`]) because signals cannot target one in-process
//! node without freezing the test driver.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use murmur_replication::Config as ReplConfig;
use sqlite_session::{Database, OpenMode, Options};

use crate::{Error, Result};

pub type NodeId = [u8; 16];

/// Overlays applied to every node's replication [`Config`](murmur_replication::Config).
#[derive(Debug, Clone, Default)]
pub struct ReplicationOptions {
    pub retention: Option<Duration>,
    /// Collection cadence (`Config::retention_interval`, default 60 s).
    /// GC-balance legs set this to seconds so background passes run inside
    /// the measurement window instead of starving it.
    pub retention_interval: Option<Duration>,
    pub minimum_retained: Option<u64>,
    pub fanout: Option<usize>,
    pub dissemination: Option<murmur_replication::Dissemination>,
    pub max_transaction_bytes: Option<usize>,
    /// Gossip membership on/off (`Config::membership`, default true).
    /// `Some(false)` holds a static topology (no discovery dialing);
    /// transaction push still runs over the peer sessions.
    pub membership: Option<bool>,
    /// Offline-peer memory for retention floors (`Config::maximum_offline`,
    /// default 7 days). Peers unseen longer than this stop pinning history,
    /// so survivors can GC ranges a stale node still needs (snapshot legs).
    pub maximum_offline: Option<Duration>,
    /// Merge-policy overlays (`Config::merge_policies`): (table, column) ->
    /// policy, applied to every node before open. Policies bind at open, so
    /// CRDT legs (typed-records, merge-policies) must set this; `None`/empty
    /// leaves LWW everywhere.
    pub merge_policies: Option<BTreeMap<(String, String), murmur_replication::policies::Policy>>,
}

/// Encrypted file/object store for every node (per-node dir under the node).
#[derive(Debug, Clone)]
pub struct FilesOptions {
    pub key: [u8; 32],
    pub fetch_interval: Option<Duration>,
    pub max_file_bytes: Option<u64>,
}

impl Default for FilesOptions {
    fn default() -> Self {
        Self {
            key: [61; 32],
            fetch_interval: None,
            max_file_bytes: None,
        }
    }
}

/// Commit-path budget overlays applied to every node's sqlite [`Options`](sqlite_session::Options).
#[derive(Debug, Clone, Default)]
pub struct SqliteOptions {
    pub transaction_bytes: Option<usize>,
    pub queue_bytes: Option<usize>,
    pub capture_bytes: Option<usize>,
}

/// Per-node spool storage overrides, keyed by node index (Go `SpoolByNode`
/// parity; `resource-compaction` legs size victim segments down).
#[derive(Debug, Clone, Default)]
pub struct SpoolOptions {
    /// Spool segment rotation size (`Options::segment_bytes`, default 256 MiB).
    pub segment_bytes: Option<usize>,
}

/// Per-node High/Low bridge configs, keyed by node index.
///
/// The configs are used as given (including their drop directories, which
/// scenarios share between Low exporters and High receivers); the harness
/// does not rewrite paths.
#[derive(Debug, Default)]
pub struct BridgeOptions {
    pub nodes: HashMap<usize, murmur_replication::bridge::Config>,
}

/// Cluster construction options.
#[derive(Debug)]
pub struct ClusterOptions {
    /// Scenario name; owns `runtime/<name>/`.
    pub name: String,
    /// Node count. Default 3.
    pub num_nodes: usize,
    /// Shared database id. Default: random per cluster.
    pub database_id: Option<[u8; 16]>,
    /// Node ids. Default: random per node (must be `num_nodes` long if set).
    pub node_ids: Vec<[u8; 16]>,
    /// Application key for every node. Default: fixed test key `[9; 32]`.
    pub key: [u8; 32],
    /// Replication overlays for every node.
    pub replication: Option<ReplicationOptions>,
    /// Commit-path budget overlays for every node.
    pub sqlite: Option<SqliteOptions>,
    /// Per-node spool storage overrides.
    pub spool_by_node: HashMap<usize, SpoolOptions>,
    /// Skip the automatic full mesh; the test calls [`Cluster::add_peer`].
    pub manual_peers: bool,
    /// Seed indices: opened first, and every other node dials them via
    /// `bootstrap` addrs (SWIM/seed-discovery legs). Empty disables seeding.
    pub bootstrap_seeds: Vec<usize>,
    /// Per-node identity allow-lists (`allowed_peers` overlay).
    pub allowed_peers: HashMap<usize, BTreeSet<[u8; 16]>>,
    /// Per-node address-policy overlays (`allowed_networks`).
    pub allowed_networks: HashMap<usize, Vec<ipnet::IpNet>>,
    /// Per-node snapshot-donor trust (`trusted_snapshot_sources` overlay).
    /// Empty by default: nodes request snapshots from nobody. Snapshot
    /// scenarios list explicit donor ids here (Go harness parity: the Go
    /// fixture policy trusts all nodes; hostile legs add the attacker).
    pub trusted_snapshot_sources: HashMap<usize, BTreeSet<[u8; 16]>>,
    /// Per-node extra entries merged into `trusted_origins` (e.g. an
    /// attacker signing key for hostile-source legs).
    pub extra_trusted_origins: HashMap<usize, BTreeMap<[u8; 16], [u8; 32]>>,
    /// Per-node dissemination-mode overrides (mixed-mode legs; wins over
    /// the global `replication.dissemination` for the listed nodes).
    pub dissemination_by_node: HashMap<usize, murmur_replication::Dissemination>,
    /// File/object store for every node.
    pub files: Option<FilesOptions>,
    /// Bridge configs for individual nodes.
    pub bridge: BridgeOptions,
    /// Per-node leaf-cert TTL overrides (default validity is effectively
    /// unbounded; `cert-lifecycle` uses short TTLs for expiry legs).
    pub node_cert_ttl: HashMap<usize, Duration>,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        Self {
            name: String::new(),
            num_nodes: 3,
            database_id: None,
            node_ids: Vec::new(),
            key: [9; 32],
            replication: None,
            sqlite: None,
            spool_by_node: HashMap::new(),
            manual_peers: false,
            bootstrap_seeds: Vec::new(),
            allowed_peers: HashMap::new(),
            allowed_networks: HashMap::new(),
            trusted_snapshot_sources: HashMap::new(),
            extra_trusted_origins: HashMap::new(),
            dissemination_by_node: HashMap::new(),
            files: None,
            bridge: BridgeOptions::default(),
            node_cert_ttl: HashMap::new(),
        }
    }
}

/// One cluster node: dirs, identity, and the open handle (`None` = stopped).
pub struct Node {
    pub index: usize,
    /// `node1` .. `nodeN`.
    pub label: String,
    /// `runtime/<scenario>/nodeN/` (config, `tls/`, forensics files).
    pub dir: PathBuf,
    /// Database storage dir (`<dir>/data/`).
    pub data_dir: PathBuf,
    pub node_id: [u8; 16],
    /// Open handle; `None` while stopped.
    pub db: Option<Database>,
    /// Bound replication address (ephemeral; changes across restarts).
    pub repl_addr: SocketAddr,
}

struct NodeState {
    node: Node,
    /// Stored for restarts (same certs + signing key).
    config: ReplConfig,
    key: [u8; 32],
    database_id: [u8; 16],
    sqlite: Option<SqliteOptions>,
    spool: Option<SpoolOptions>,
}

/// Apply commit-path budget overlays to node open options.
fn apply_sqlite(db_opts: &mut Options, sqlite: &Option<SqliteOptions>) {
    if let Some(s) = sqlite {
        if let Some(v) = s.transaction_bytes {
            db_opts.transaction_bytes = v;
        }
        if let Some(v) = s.queue_bytes {
            db_opts.queue_bytes = v;
        }
        if let Some(v) = s.capture_bytes {
            db_opts.capture_bytes = v;
        }
    }
}

/// Apply spool storage overlays to node open options.
fn apply_spool(db_opts: &mut Options, spool: &Option<SpoolOptions>) {
    if let Some(s) = spool {
        if let Some(v) = s.segment_bytes {
            db_opts.segment_bytes = v;
        }
    }
}

/// An in-process multi-node cluster.
pub struct Cluster {
    name: String,
    runtime_dir: PathBuf,
    database_id: [u8; 16],
    states: Vec<NodeState>,
    /// Cluster CA, kept so scenarios can issue certs for external fixture
    /// identities (e.g. a hostile peer speaking the wire protocol).
    ca: crate::certs::Ca,
}

impl Cluster {
    /// Build node dirs + certs, open every node (seeds first), mesh unless
    /// `manual_peers`. Wipes any previous `runtime/<name>/` first.
    pub fn new(mut opts: ClusterOptions) -> Result<Self> {
        if opts.num_nodes == 0 {
            return Err(Error::Setup("num_nodes must be >= 1".into()));
        }
        if opts.name.is_empty() {
            return Err(Error::Setup("ClusterOptions::name must be set".into()));
        }
        let node_ids: Vec<[u8; 16]> = if opts.node_ids.is_empty() {
            (0..opts.num_nodes).map(|_| rand::random()).collect()
        } else {
            if opts.node_ids.len() != opts.num_nodes {
                return Err(Error::Setup(format!(
                    "node_ids has {} entries for {} nodes",
                    opts.node_ids.len(),
                    opts.num_nodes
                )));
            }
            std::mem::take(&mut opts.node_ids)
        };
        let distinct: HashSet<[u8; 16]> = node_ids.iter().copied().collect();
        if distinct.len() != node_ids.len() {
            return Err(Error::Setup("node ids must be distinct".into()));
        }
        for &s in &opts.bootstrap_seeds {
            if s >= opts.num_nodes {
                return Err(Error::Setup(format!("bootstrap seed {s} out of range")));
            }
        }

        let runtime_dir = crate::artifacts::prepare_runtime_dir(&opts.name)?;
        let database_id: [u8; 16] = opts.database_id.unwrap_or_else(rand::random);

        // Signing keys + trust: every node trusts every node's origin key
        // (the `tests/replication.rs` pattern; allow-lists narrow this).
        let seeds: Vec<[u8; 32]> = (0..opts.num_nodes).map(|_| rand::random()).collect();
        let mut trusted_origins: BTreeMap<NodeId, [u8; 32]> = BTreeMap::new();
        for (id, seed) in node_ids.iter().zip(seeds.iter()) {
            trusted_origins.insert(*id, murmur_replication::protocol::public_key(seed));
        }

        let ca = crate::certs::Ca::generate()?;

        // Open order: seeds first so their ephemeral addrs can seed the rest.
        let mut order: Vec<usize> = opts.bootstrap_seeds.clone();
        order.sort_unstable();
        order.dedup();
        for i in 0..opts.num_nodes {
            if !order.contains(&i) {
                order.push(i);
            }
        }

        let mut states: Vec<Option<NodeState>> = (0..opts.num_nodes).map(|_| None).collect();
        let mut seed_addrs: Vec<SocketAddr> = Vec::new();
        for &i in &order {
            let label = format!("node{}", i + 1);
            let dir = runtime_dir.join(&label);
            std::fs::create_dir_all(&dir)?;
            let data_dir = dir.join("data");

            let issued = ca.issue(&node_ids[i], opts.node_cert_ttl.get(&i).copied())?;
            ca.write_tls_dir(&issued, &dir)?;

            let mut config = ReplConfig::new(
                "127.0.0.1:0".parse().unwrap(),
                issued.credentials,
                seeds[i],
            );
            config.trusted_origins = trusted_origins.clone();
            if let Some(peers) = opts.allowed_peers.get(&i) {
                config.allowed_peers = peers.clone();
            }
            if let Some(nets) = opts.allowed_networks.get(&i) {
                config.allowed_networks = nets.clone();
            }
            if let Some(sources) = opts.trusted_snapshot_sources.get(&i) {
                config.trusted_snapshot_sources = sources.clone();
            }
            if let Some(extra) = opts.extra_trusted_origins.get(&i) {
                config.trusted_origins.extend(extra.iter().map(|(k, v)| (*k, *v)));
            }
            if let Some(r) = &opts.replication {
                if let Some(v) = r.retention {
                    config.retention = v;
                }
                if let Some(v) = r.retention_interval {
                    config.retention_interval = v;
                }
                if let Some(v) = r.minimum_retained {
                    config.minimum_retained = v;
                }
                if let Some(v) = r.fanout {
                    config.fanout = v;
                }
                if let Some(v) = r.dissemination {
                    config.dissemination = v;
                }
                if let Some(v) = r.max_transaction_bytes {
                    config.max_transaction_bytes = v;
                }
                if let Some(v) = r.membership {
                    config.membership = v;
                }
                if let Some(v) = r.maximum_offline {
                    config.maximum_offline = v;
                }
                if let Some(v) = r.merge_policies.clone() {
                    config.merge_policies = v;
                }
            }
            if let Some(v) = opts.dissemination_by_node.get(&i) {
                config.dissemination = *v;
            }
            if let Some(f) = &opts.files {
                let mut fc = murmur_replication::files::Config::new(
                    dir.join("objects"),
                    f.key,
                    "127.0.0.1:0".parse().unwrap(),
                );
                if let Some(v) = f.fetch_interval {
                    fc.fetch_interval = v;
                }
                if let Some(v) = f.max_file_bytes {
                    fc.max_file_bytes = v;
                }
                config.files = Some(fc);
            }
            if let Some(bc) = opts.bridge.nodes.remove(&i) {
                config.bridge = Some(bc);
            }
            if !seed_addrs.is_empty() && !opts.bootstrap_seeds.contains(&i) {
                config.bootstrap = seed_addrs.clone();
            }

            let mut db_opts = Options {
                mode: OpenMode::CreateNew,
                database_id: Some(database_id),
                node_id: Some(node_ids[i]),
                replication: Some(config.clone()),
                ..Options::default()
            };
            apply_sqlite(&mut db_opts, &opts.sqlite);
            apply_spool(&mut db_opts, &opts.spool_by_node.get(&i).cloned());
            let db = crate::unlock::open_with_watchdog(&data_dir, opts.key, db_opts)?;
            let repl_addr = replication_addr(&db)?;
            if opts.bootstrap_seeds.contains(&i) {
                seed_addrs.push(repl_addr);
            }
            states[i] = Some(NodeState {
                node: Node {
                    index: i,
                    label,
                    dir,
                    data_dir,
                    node_id: node_ids[i],
                    db: Some(db),
                    repl_addr,
                },
                config,
                key: opts.key,
                database_id,
                sqlite: opts.sqlite.clone(),
                spool: opts.spool_by_node.get(&i).cloned(),
            });
        }

        let cluster = Self {
            name: opts.name.clone(),
            runtime_dir,
            database_id,
            states: states.into_iter().map(|s| s.unwrap()).collect(),
            ca,
        };
        if !opts.manual_peers {
            cluster.mesh()?;
        }
        Ok(cluster)
    }

    /// Scenario name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `runtime/<scenario>/` root.
    pub fn runtime_dir(&self) -> &PathBuf {
        &self.runtime_dir
    }

    /// Shared database id.
    pub fn database_id(&self) -> [u8; 16] {
        self.database_id
    }

    /// Issue a leaf certificate for an external identity (e.g. a hostile
    /// fixture peer) from the cluster CA.
    pub fn issue_cert(&self, node_id: &[u8; 16]) -> Result<crate::certs::Issued> {
        Ok(self.ca.issue(node_id, None)?)
    }

    /// DER bytes of the cluster CA certificate, for cross-trust legs: hand
    /// them to a peer cluster's [`Cluster::add_trust_root`].
    pub fn ca_der(&self) -> Vec<u8> {
        self.ca.ca_der()
    }

    /// Append an external CA root to every node's stored TLS trust set
    /// (deduped). Applies to the stored config only: stop and
    /// [`Cluster::start_node`] every node afterwards (then re-mesh), mirroring
    /// Go `crossTrustCAs` (append-PEM + restart). `tls/` keeps the original
    /// CA file; [`Cluster::rotate_node_cert`] resets trust to the own CA.
    pub fn add_trust_root(&mut self, root_der: Vec<u8>) {
        for st in &mut self.states {
            if !st.config.credentials.roots.contains(&root_der) {
                st.config.credentials.roots.push(root_der.clone());
            }
        }
    }

    /// Node count.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// Node id (available while stopped too).
    pub fn node_id(&self, idx: usize) -> [u8; 16] {
        self.states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"))
            .node
            .node_id
    }

    /// Origin signing public key for node `idx` (ed25519 verifying-key bytes).
    ///
    /// Bridge receivers in another domain need this for
    /// `bridge::Config::receiver(.., trusted_origins)`; the seed itself stays
    /// inside the harness. Available while stopped too.
    pub fn node_origin_pubkey(&self, idx: usize) -> [u8; 32] {
        let st = self
            .states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"));
        murmur_replication::protocol::public_key(&st.config.signing_key)
    }

    /// Node metadata (dirs, addr; available while stopped too).
    pub fn node_info(&self, idx: usize) -> &Node {
        &self
            .states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"))
            .node
    }

    /// Open handle. Panics if the node is stopped (a test bug).
    pub fn node(&self, idx: usize) -> &Database {
        let st = self
            .states
            .get(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"));
        st.node
            .db
            .as_ref()
            .unwrap_or_else(|| panic!("node{} is stopped", idx + 1))
    }

    /// Dial `target` from `from` (both must be running; target addr is its
    /// current ephemeral bind).
    pub fn add_peer(&self, from: usize, target: usize) -> Result<()> {
        let tgt = self.states.get(target).ok_or(Error::UnknownNode(target))?;
        let src = self.states.get(from).ok_or(Error::UnknownNode(from))?;
        let db = src.node.db.as_ref().ok_or(Error::NodeStopped(from))?;
        let handle = db
            .replication()
            .ok_or_else(|| Error::Setup(format!("node{} has no replication", from + 1)))?;
        handle.add_peer(tgt.node.repl_addr)?;
        Ok(())
    }

    /// Dial an explicit address from `from` (`from` must be running).
    /// Proxy legs use this to route a session through a throttle instead
    /// of the target's bound address.
    pub fn add_peer_addr(&self, from: usize, addr: std::net::SocketAddr) -> Result<()> {
        let src = self.states.get(from).ok_or(Error::UnknownNode(from))?;
        let db = src.node.db.as_ref().ok_or(Error::NodeStopped(from))?;
        let handle = db
            .replication()
            .ok_or_else(|| Error::Setup(format!("node{} has no replication", from + 1)))?;
        handle.add_peer(addr)?;
        Ok(())
    }

    /// Drop `target` from `from`'s peer set (`from` must be running).
    pub fn remove_peer(&self, from: usize, target: usize) -> Result<()> {
        let tgt = self.states.get(target).ok_or(Error::UnknownNode(target))?;
        let src = self.states.get(from).ok_or(Error::UnknownNode(from))?;
        let db = src.node.db.as_ref().ok_or(Error::NodeStopped(from))?;
        let handle = db
            .replication()
            .ok_or_else(|| Error::Setup(format!("node{} has no replication", from + 1)))?;
        handle.remove_peer(tgt.node.node_id)?;
        Ok(())
    }

    /// Dial every ordered pair of *running* nodes (heal legs after restart).
    pub fn mesh(&self) -> Result<()> {
        for i in 0..self.states.len() {
            if self.states[i].node.db.is_none() {
                continue;
            }
            for j in 0..self.states.len() {
                if i != j && self.states[j].node.db.is_some() {
                    self.add_peer(i, j)?;
                }
            }
        }
        Ok(())
    }

    /// `close()` node `idx`; dirs, key, and identity are kept for restart.
    /// Unknown index panics (a test bug); close errors are logged.
    pub fn stop_node(&mut self, idx: usize) {
        let st = self
            .states
            .get_mut(idx)
            .unwrap_or_else(|| panic!("unknown node index {idx}"));
        if let Some(db) = st.node.db.take() {
            if let Err(e) = db.close() {
                eprintln!("stop node{}: close failed: {e}", idx + 1);
            }
        }
    }

    /// Reopen node `idx` (same dir + key + identity) under the open watchdog.
    /// The node rebinds a fresh ephemeral port: re-mesh (`add_peer`/`mesh`)
    /// afterwards, since peers still dial the old address.
    pub fn start_node(&mut self, idx: usize) -> Result<()> {
        self.start_node_inner(idx, None)
    }

    /// Replace node `idx`'s stored bridge config (signer-rotation legs, Go
    /// `rewriteNodeConfig` parity). The node must be stopped (see
    /// [`Cluster::stop_node`]); the new config takes effect on the next
    /// [`Cluster::start_node`]. `None` removes the bridge from the stored
    /// config.
    pub fn set_bridge_config(
        &mut self,
        idx: usize,
        config: Option<murmur_replication::bridge::Config>,
    ) -> Result<()> {
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        if st.node.db.is_some() {
            return Err(Error::Setup(format!(
                "node{} is running; stop it first",
                idx + 1
            )));
        }
        st.config.bridge = config;
        Ok(())
    }

    /// Replace node `idx`'s stored spool options (`None` restores production
    /// storage behavior). The node must be stopped (see
    /// [`Cluster::stop_node`]); the new options take effect on the next
    /// [`Cluster::start_node`].
    pub fn set_spool_options(
        &mut self,
        idx: usize,
        spool: Option<SpoolOptions>,
    ) -> Result<()> {
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        if st.node.db.is_some() {
            return Err(Error::Setup(format!(
                "node{} is running; stop it first",
                idx + 1
            )));
        }
        st.spool = spool;
        Ok(())
    }

    /// Replace node `idx`'s stored application key (key-rotation legs, Go
    /// `rewriteKeyConfig` parity). Takes effect on the next
    /// [`Cluster::start_node`]; online rotation (`rotate_master_key` on a
    /// running node) should record the new key here immediately so later
    /// restarts unlock with it instead of the retired key.
    pub fn set_node_key(&mut self, idx: usize, key: [u8; 32]) -> Result<()> {
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        st.key = key;
        Ok(())
    }

    /// Rotate node `idx` to a freshly issued leaf certificate for the SAME
    /// NodeID, then reopen it (cert rotation; `cert-lifecycle`). The node
    /// must be stopped (see [`Cluster::stop_node`]); `ttl` overrides the
    /// default leaf validity (short TTLs for expiry legs, `None` for a
    /// fresh long-lived cert). The node's `tls/` dir is rewritten from the
    /// new material, so file NotAfter always matches the served cert. Like
    /// [`Cluster::start_node`], the node rebinds a fresh ephemeral port:
    /// re-mesh afterwards.
    pub fn rotate_node_cert(&mut self, idx: usize, ttl: Option<Duration>) -> Result<()> {
        let (node_id, dir) = {
            let st = self.states.get(idx).ok_or(Error::UnknownNode(idx))?;
            (st.node.node_id, st.node.dir.clone())
        };
        let issued = self.ca.issue(&node_id, ttl)?;
        self.ca.write_tls_dir(&issued, &dir)?;
        self.start_node_inner(idx, Some(issued.credentials))
    }

    /// Restart stopped node `idx` under a FRESH node identity (backup-restore
    /// clone legs, Go `RestoreClone` + TLS reissue + fresh origin key). The
    /// node must be stopped (see [`Cluster::stop_node`]) and its data dir
    /// must already carry the fresh identity (restored with
    /// `RestoreOptions { new_node_id: Some(new_node_id) }`). Issues a fresh
    /// leaf certificate for the new id (rewriting `tls/`), mints a fresh
    /// origin signing key, registers the new id in the node's own
    /// `trusted_origins` (old entries are kept so pre-restore origins still
    /// verify), and reopens. Peers are NOT told the fresh origin key, so the
    /// restarted node must not originate new transactions until every peer
    /// restarts with it (transaction verification is by origin; receiving
    /// and serving pre-existing origins is unaffected). Like
    /// [`Cluster::start_node`], the node rebinds a fresh ephemeral port:
    /// re-mesh afterwards.
    pub fn restart_node_with_identity(
        &mut self,
        idx: usize,
        new_node_id: [u8; 16],
    ) -> Result<()> {
        {
            let st = self.states.get(idx).ok_or(Error::UnknownNode(idx))?;
            if st.node.db.is_some() {
                return Err(Error::Setup(format!("node{} is already running", idx + 1)));
            }
        }
        if self.states.iter().any(|s| s.node.node_id == new_node_id) {
            return Err(Error::Setup(
                "fresh node id collides with a cluster member".into(),
            ));
        }
        let dir = self.states[idx].node.dir.clone();
        let issued = self.ca.issue(&new_node_id, None)?;
        self.ca.write_tls_dir(&issued, &dir)?;
        let seed: [u8; 32] = rand::random();
        let public = murmur_replication::protocol::public_key(&seed);
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        st.node.node_id = new_node_id;
        st.config.credentials = issued.credentials;
        st.config.signing_key.copy_from_slice(&seed);
        st.config.trusted_origins.insert(new_node_id, public);
        self.start_node(idx)
    }

    fn start_node_inner(
        &mut self,
        idx: usize,
        credentials: Option<murmur_replication::Credentials>,
    ) -> Result<()> {
        let st = self.states.get_mut(idx).ok_or(Error::UnknownNode(idx))?;
        if st.node.db.is_some() {
            return Err(Error::Setup(format!("node{} is already running", idx + 1)));
        }
        if let Some(c) = credentials {
            st.config.credentials = c;
        }
        let mut db_opts = Options {
            mode: OpenMode::Existing,
            database_id: Some(st.database_id),
            node_id: Some(st.node.node_id),
            replication: Some(st.config.clone()),
            ..Options::default()
        };
        apply_sqlite(&mut db_opts, &st.sqlite);
        apply_spool(&mut db_opts, &st.spool);
        let db = crate::unlock::open_with_watchdog(&st.node.data_dir, st.key, db_opts)?;
        st.node.repl_addr = replication_addr(&db)?;
        st.node.db = Some(db);
        Ok(())
    }

    /// Forensics for every node (running or stopped). Infallible by contract.
    pub fn dump_forensics(&self, reason: &str) {
        for st in &self.states {
            crate::forensics::dump_node_forensics(
                &st.node.label,
                &st.node.dir,
                st.node.db.as_ref(),
                reason,
            );
        }
    }

    /// Close all nodes, then retain-or-clean the runtime dir + `RESULT` line.
    pub fn finish(mut self, failed: bool) {
        for st in &mut self.states {
            if let Some(db) = st.node.db.take() {
                let _ = db.close();
            }
        }
        crate::artifacts::finish(&self.name, &self.runtime_dir, failed);
    }
}

/// Bound replication address, waiting briefly for the endpoint to come up.
fn replication_addr(db: &Database) -> Result<SocketAddr> {
    let end = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(handle) = db.replication() {
            if let Some(addr) = handle.status().address {
                return Ok(addr);
            }
        }
        if std::time::Instant::now() >= end {
            return Err(Error::Setup("replication endpoint never bound".into()));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
