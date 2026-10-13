//! Rust port of the Go `highlow` live scenarios.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/highlow/`
//! (`highlow_test.go`, `ownership_reorder_test.go`, `README.md`). Real Low
//! (exporter) and High (importer) domains move logical writes across a
//! directory drop: disconnected transfer with one-way role gating, gap
//! delivery with duplicate recovery, two-receiver convergence,
//! forgery/corruption/misnaming rejection without partial apply, outbox/inbox
//! restart resumption, schema-hold survival across restart with release after
//! a local High migration (no DDL crosses), signer rotation plus outage
//! catch-up, and High-owned-field reorder convergence across two meshed High
//! peers with opposite import/override orders (plus release convergence back
//! to the latest Low value).
//!
//! Plain SQL via `Database::transaction`/`Database::read` replaces the Go
//! typed HTTP API (the Rust product has no HTTP server); `contacts()` is the
//! `name -> score` visible-state map (Go: `contacts()` + `TypedCounterValue`,
//! with plain LWW `score` increments standing in for the sequential
//! single-writer `TypedCounterAdd` legs, which converge identically).
//! `BridgeExport`/`BridgeImport` map to direct `Bridge::capture()` (Low) and
//! `Bridge::process()` (High) pumps.
//!
//! Directory-drop routing (deviation): Go shares one staging dir because its
//! daemons only export/import on HTTP trigger. The Rust replication engine
//! also drives `capture()`/`process()` in the background on every apply tick
//! (`engine.rs`), so Low and High use SEPARATE staging dirs and the test
//! ferries `*.bundle` files one way via [`Ferry`]; only what the test ferries
//! is visible to High, which keeps gap/withhold/forgery/outage legs
//! deterministic. Bundle filenames (`<nodehex>-<seq:020>.bundle`) sort in
//! export order, so the middle-bundle withhold mirrors Go exactly.
//!
//! Count/progress mapping (deviation): per-call `captured`/`published`/
//! `received`/`imported` counts and `BridgeStatus` progress (`Applied`,
//! `Missing`, `Holds`, `pending_events`) have no public Rust equivalent and
//! are racy under background driving, so every such assertion is proven
//! behaviorally instead: gap holds by the exact visible row set after bounded
//! pumps, duplicate redelivery by state stability plus the bundle staying out
//! of quarantine, restart resume by Low's bundle file set being unchanged and
//! High's rows holding exactly, and outage catch-up by the exact converged
//! counter (a lost or doubled event would show a different total).
//! `Bridge::process()` quarantines undecryptable artifacts into
//! `staging/quarantine/` and returns `Ok` (Go: import error), so forgery legs
//! assert quarantine placement + empty High + no recorded progress (the
//! genuine bundle still applies afterwards); only the unreadable-file leg
//! surfaces an `Err`, like Go. High/Low field provenance has no public API
//! (spool keys are internal), so ownership is proven behaviorally per the
//! `bridge-two-streams` port convention: High-owned by a Low update arriving
//! yet staying suppressed, Low-owned by it flowing through.
//!
//! Signer rotation (deviation): Go overlaps two trusted signer keys for one
//! source node during rotation. Rust pins a single signer per source node
//! (`trusted_signers`), so overlap is inexpressible: round 1 is imported
//! BEFORE the rotation (config edit plus restart on both sides, Go
//! `rewriteNodeConfig` parity via `Cluster::set_bridge_config`), round 2
//! imports after, and a retired-signer replay then fails closed into
//! quarantine while a current-key duplicate still stages cleanly.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use murmur_live_harness::murmur_replication::bridge::{
    self, Bridge, Config as BridgeConfig,
};
use murmur_live_harness::murmur_replication::protocol;
use murmur_live_harness::sqlite_session::{Database, Migration, Value};
use murmur_live_harness::{artifacts, wait_until, BridgeOptions, Cluster, ClusterOptions};

/// Bridge stream (Go: `field-records`).
const STREAM: &str = "field-records";
/// Contacts table (Go: `liveContactsSchema`).
const TABLE: &str = "contacts";
const BASE_DDL: &str =
    "CREATE TABLE contacts(id TEXT NOT NULL PRIMARY KEY, name TEXT, score INTEGER)";
const EMAIL_DDL: &str = "CREATE TABLE contacts(id TEXT NOT NULL PRIMARY KEY, name TEXT, score INTEGER, email TEXT)";
const ADD_EMAIL: &str = "ALTER TABLE contacts ADD COLUMN email TEXT";
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 100 ms polls).
const POLL: Duration = Duration::from_millis(50);

/// Independent bridge domains. Fixed test vectors shared by every test in
/// this file (clusters never interact across tests, like the
/// `tests/bridge_files.rs` fixed-key convention).
const LOW_DBID: [u8; 16] = [0xD0; 16];
const HIGH_DBID: [u8; 16] = [0xD1; 16];
const ROGUE_DBID: [u8; 16] = [0xD2; 16];
const LOW_NODE: [u8; 16] = [0x10; 16];
const HIGH_NODE_0: [u8; 16] = [0x11; 16];
const HIGH_NODE_1: [u8; 16] = [0x12; 16];
const ROGUE_NODE: [u8; 16] = [0x13; 16];
/// Low bundle-signer seeds (ed25519; High pins the verifying keys).
const LOW_SIGNER_A: [u8; 32] = [0x21; 32];
const LOW_SIGNER_B: [u8; 32] = [0x23; 32];
const ROGUE_SIGNER: [u8; 32] = [0x24; 32];
/// Receiver secrets (X25519; Low encrypts to the derived recipients).
const HIGH_SECRET: [u8; 32] = [0x22; 32];
const OTHER_SECRET: [u8; 32] = [0x25; 32];
const ROGUE_SECRET: [u8; 32] = [0x26; 32];

/// Contact insert (Go: `writeContact`).
fn insert_contact(db: &Database, id: &str, name: &str, score: i64) {
    let (id, name) = (id.to_owned(), name.to_owned());
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO contacts(id, name, score) VALUES(?, ?, ?)",
            (id, name, score),
        )?;
        Ok(())
    })
    .expect("contact insert failed");
    db.flush().expect("flush after contact insert");
}

/// Contact rename (Go: `Cluster.TypedRename`).
fn rename(db: &Database, id: &str, name: &str) {
    let (id, name) = (id.to_owned(), name.to_owned());
    db.transaction(move |sql| {
        let changed = sql.execute(
            "UPDATE contacts SET name = ? WHERE id = ?",
            (name, id),
        )?;
        assert_eq!(changed, 1, "rename hit {changed} rows");
        Ok(())
    })
    .expect("contact rename failed");
    db.flush().expect("flush after contact rename");
}

/// Known-field email update (Go: `Cluster.TypedSetNote`).
fn set_email(db: &Database, id: &str, email: &str) {
    let (id, email) = (id.to_owned(), email.to_owned());
    db.transaction(move |sql| {
        let changed = sql.execute(
            "UPDATE contacts SET email = ? WHERE id = ?",
            (email, id),
        )?;
        assert_eq!(changed, 1, "email update hit {changed} rows");
        Ok(())
    })
    .expect("contact email update failed");
    db.flush().expect("flush after email update");
}

/// Sequential score increment (Go: `Cluster.TypedCounterAdd`; single-writer
/// legs converge identically under LWW).
fn add_score(db: &Database, id: &str, delta: i64) {
    let id = id.to_owned();
    db.transaction(move |sql| {
        let changed = sql.execute(
            "UPDATE contacts SET score = score + ? WHERE id = ?",
            (delta, id),
        )?;
        assert_eq!(changed, 1, "score update hit {changed} rows");
        Ok(())
    })
    .expect("score update failed");
    db.flush().expect("flush after score update");
}

/// Visible contacts as `name -> score` (Go: `contacts()`).
fn contacts(db: &Database) -> BTreeMap<String, i64> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name, score FROM contacts")?;
        let mut out = BTreeMap::new();
        for row in stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })? {
            let (name, score) = row?;
            out.insert(name, score);
        }
        Ok(out)
    })
    .unwrap_or_default()
}

/// Email field read (Go: `Cluster.TypedNote`); `None` when the column is
/// absent (pre-migration High) or the value is NULL.
fn email_of(db: &Database, id: &str) -> Option<String> {
    let id = id.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT email FROM contacts WHERE id = ?",
            [id],
            |r| r.get::<_, Option<String>>(0),
        )?)
    })
    .ok()
    .flatten()
}

/// Whether `table` currently has `column` (the `typed-records` port pattern).
fn has_column(db: &Database, table: &str, column: &str) -> bool {
    let (table, column) = (table.to_owned(), column.to_owned());
    db.read(move |sql| {
        let mut stmt = sql.prepare(&format!("PRAGMA table_info({table})"))?;
        let names = stmt.query_map([], |r| r.get::<_, String>(1))?;
        for found in names {
            if found? == column {
                return Ok(true);
            }
        }
        Ok(false)
    })
    .unwrap_or(false)
}

fn migrate(db: &Database, ddl: &str, label: &str) {
    db.migrate(Migration::new(ddl))
        .unwrap_or_else(|e| panic!("{label} migrate failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("{label} flush after migrate failed: {e}"));
}

fn node_bridge(db: &Database) -> Arc<Bridge> {
    db.replication()
        .expect("replication handle")
        .bridge()
        .expect("bridge")
}

/// Pump Low until nothing remains to export (bounded idle loop; the
/// `bridge_files.rs` product-test pattern). Sound: capture cursors are
/// durable and only the test writes, so a zero return means quiescent.
fn drain_low(low: &Cluster) {
    for _ in 0..200 {
        let exported = node_bridge(low.node(0))
            .capture()
            .expect("low bridge capture");
        if exported == 0 {
            return;
        }
    }
    panic!("low bridge did not become idle");
}

/// One import pump over the listed High nodes (Go: `BridgeImport`).
fn pump_high(high: &Cluster, idxs: &[usize]) {
    for &i in idxs {
        node_bridge(high.node(i))
            .process()
            .unwrap_or_else(|e| panic!("high node{i} bridge process: {e}"));
    }
}

/// Sorted `*.bundle` names directly under `dir` (Go: `stagingNames`).
fn bundle_files(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read dir {}: {e}", dir.display()))
    {
        let name = entry
            .expect("staging entry")
            .file_name()
            .to_string_lossy()
            .into_owned();
        if name.ends_with(".bundle") {
            out.push(name);
        }
    }
    out.sort();
    out
}

/// Remove every `*.bundle` file directly under `dir`.
fn clear_bundles(dir: &Path) {
    for name in bundle_files(dir) {
        std::fs::remove_file(dir.join(&name))
            .unwrap_or_else(|e| panic!("clear {}: {e}", dir.join(&name).display()));
    }
}

/// Sorted names under `staging/quarantine/` (empty when no dir yet).
fn quarantine_files(staging: &Path) -> Vec<String> {
    let dir = staging.join("quarantine");
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read quarantine") {
        out.push(
            entry
                .expect("quarantine entry")
                .file_name()
                .to_string_lossy()
                .into_owned(),
        );
    }
    out.sort();
    out
}

/// Remove every file under `staging/quarantine/`.
fn clear_quarantine(staging: &Path) {
    for name in quarantine_files(staging) {
        std::fs::remove_file(staging.join("quarantine").join(&name))
            .expect("clear quarantine file");
    }
}

/// One-way directory drop: Low exports into `low`, the test ferries
/// `*.bundle` files into `high`. Ferries are tracked so `ferry_all` only
/// moves new bundles; targeted withholds use [`Ferry::ferry_names`].
struct Ferry {
    low: PathBuf,
    high: PathBuf,
    done: HashSet<String>,
}

impl Ferry {
    fn new(low: PathBuf, high: PathBuf) -> Self {
        Self {
            low,
            high,
            done: HashSet::new(),
        }
    }

    /// Bundles exported since the last ferry, in export order.
    fn new_bundles(&self) -> Vec<String> {
        bundle_files(&self.low)
            .into_iter()
            .filter(|n| !self.done.contains(n))
            .collect()
    }

    /// Ferry every new bundle to High.
    fn ferry_all(&mut self) {
        let new = self.new_bundles();
        self.ferry_names(&new);
    }

    /// Ferry exactly `names` (already-exported bundles) to High.
    fn ferry_names(&mut self, names: &[String]) {
        for name in names {
            std::fs::copy(self.low.join(name), self.high.join(name))
                .unwrap_or_else(|e| panic!("ferry {name}: {e}"));
            self.done.insert(name.clone());
        }
    }
}

/// Single-node Low exporter (Go: `newLowCluster`).
fn open_exporter(
    name: &str,
    dbid: [u8; 16],
    node: [u8; 16],
    staging: PathBuf,
    signer: [u8; 32],
    recipient: [u8; 32],
) -> Cluster {
    Cluster::new(ClusterOptions {
        name: name.into(),
        num_nodes: 1,
        database_id: Some(dbid),
        node_ids: vec![node],
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeConfig::exporter(dbid, staging, STREAM.into(), signer, recipient),
            )]),
        },
        ..Default::default()
    })
    .expect("low cluster setup")
}

/// High importer cluster (Go: `newHighCluster`); one staging dir per node.
/// Always `manual_peers`: the test meshes explicitly after migrating.
fn open_high(
    name: &str,
    dbid: [u8; 16],
    nodes: Vec<[u8; 16]>,
    stagings: Vec<PathBuf>,
    secret: [u8; 32],
    domains: BTreeSet<[u8; 16]>,
    signers: BTreeMap<[u8; 16], [u8; 32]>,
    origins: BTreeMap<[u8; 16], [u8; 32]>,
) -> Cluster {
    assert_eq!(
        nodes.len(),
        stagings.len(),
        "nodes/stagings length mismatch"
    );
    let receivers = stagings
        .into_iter()
        .enumerate()
        .map(|(i, staging)| {
            (
                i,
                BridgeConfig::receiver(
                    dbid,
                    staging,
                    STREAM.into(),
                    secret,
                    domains.clone(),
                    signers.clone(),
                    origins.clone(),
                ),
            )
        })
        .collect();
    Cluster::new(ClusterOptions {
        name: name.into(),
        num_nodes: nodes.len(),
        database_id: Some(dbid),
        node_ids: nodes,
        manual_peers: true,
        bridge: BridgeOptions { nodes: receivers },
        ..Default::default()
    })
    .expect("high cluster setup")
}

/// Receiver trust pinning Low's bundle signer and origin key (the seeds
/// themselves stay inside the harness).
fn low_signers(signer: &[u8; 32]) -> BTreeMap<[u8; 16], [u8; 32]> {
    BTreeMap::from([(LOW_NODE, protocol::public_key(signer))])
}

fn low_origins(low: &Cluster) -> BTreeMap<[u8; 16], [u8; 32]> {
    BTreeMap::from([(LOW_NODE, low.node_origin_pubkey(0))])
}

/// Wait until High node `idx` shows exactly `want` (Go: contact waits plus
/// `assertContactsEqual`), pumping Low capture + the importer inside the
/// poll. The ferry stays explicit (withhold legs must not auto-ferry).
fn wait_high(low: &Cluster, high: &Cluster, idx: usize, want: &BTreeMap<String, i64>) {
    let converged = wait_until(WAIT, POLL, || {
        node_bridge(low.node(0)).capture().expect("low bridge capture");
        node_bridge(high.node(idx))
            .process()
            .expect("high bridge process");
        contacts(high.node(idx)) == *want
    });
    assert!(
        converged,
        "high node{idx} contacts = {:?}, want {want:?}",
        contacts(high.node(idx))
    );
}

/// Wait until every listed High node shows exactly `want` over the mesh
/// (no bridge traffic remains; plain convergence polling).
fn wait_meshed(high: &Cluster, idxs: &[usize], want: &BTreeMap<String, i64>, what: &str) {
    let converged = wait_until(WAIT, POLL, || {
        idxs.iter().all(|&i| contacts(high.node(i)) == *want)
    });
    assert!(
        converged,
        "{what}: high nodes did not converge on {want:?}: {:?}",
        idxs.iter()
            .map(|&i| contacts(high.node(i)))
            .collect::<Vec<_>>()
    );
}

/// Release High ownership of `row`'s `name` field on node A (Go:
/// `ReleaseOwnership`; the key is the postcard-encoded PK value vector),
/// retrying until the cursors align (Go: synchronous HTTP call).
fn release_name(high: &Cluster, row: &str) {
    let key = murmur_live_harness::murmur_replication::encode(&vec![Value::Text(
        row.to_owned(),
    )])
    .expect("encode release key");
    let start = std::time::Instant::now();
    loop {
        let released = node_bridge(high.node(0))
            .release(TABLE.to_owned(), key.clone(), Some("name".to_owned()))
            .expect("bridge release");
        if released {
            return;
        }
        assert!(
            start.elapsed() < WAIT,
            "bridge release never made progress"
        );
        std::thread::sleep(POLL);
    }
}

/// Disconnected transfer with one-way role gating (Go:
/// `TestHighLowDisconnectedTransferLive`).
#[test]
fn disconnected_transfer_with_one_way_role_gating() {
    let staging_low = artifacts::runtime_dir("highlow-transfer-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-transfer-high").join("staging");
    let low = open_exporter(
        "highlow-transfer-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(HIGH_SECRET),
    );
    let high = open_high(
        "highlow-transfer-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    );
    migrate(low.node(0), BASE_DDL, "low");
    migrate(high.node(0), BASE_DDL, "high");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_ne!(
            low.database_id(),
            high.database_id(),
            "Low and High share a DBID; domains must differ"
        );
        // One-way role gating at the API boundary: the exporter never
        // imports and the importer never exports.
        assert_eq!(
            node_bridge(low.node(0)).process().expect("low process"),
            0,
            "low exporter imported"
        );
        assert_eq!(
            node_bridge(high.node(0)).capture().expect("high capture"),
            0,
            "high importer exported"
        );

        insert_contact(low.node(0), "ann-id", "ann", 1);
        drain_low(&low);
        assert!(
            !ferry.new_bundles().is_empty(),
            "low exported no bundles for the contact"
        );
        ferry.ferry_all();
        let want = BTreeMap::from([("ann".to_owned(), 1)]);
        wait_high(&low, &high, 0, &want);
        assert_eq!(
            contacts(low.node(0)),
            contacts(high.node(0)),
            "low/high contact states diverged"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("highlow-transfer-low");
        high.dump_forensics("highlow-transfer-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Gap delivery with duplicate recovery (Go:
/// `TestHighLowDuplicateReorderRecoveryLive`).
#[test]
fn gap_delivery_with_duplicate_recovery() {
    let staging_low = artifacts::runtime_dir("highlow-gap-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-gap-high").join("staging");
    let low = open_exporter(
        "highlow-gap-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(HIGH_SECRET),
    );
    let high = open_high(
        "highlow-gap-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    );
    migrate(low.node(0), BASE_DDL, "low");
    migrate(high.node(0), BASE_DDL, "high");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Baseline: drain the migration bundles so the three contact
        // rounds below are exactly three new bundles.
        drain_low(&low);
        ferry.ferry_all();
        pump_high(&high, &[0]);
        assert!(contacts(high.node(0)).is_empty());

        insert_contact(low.node(0), "ann-id", "ann", 0);
        drain_low(&low);
        insert_contact(low.node(0), "bob-id", "bob", 1);
        drain_low(&low);
        insert_contact(low.node(0), "cid-id", "cid", 2);
        drain_low(&low);
        let new = ferry.new_bundles();
        assert_eq!(
            new.len(),
            3,
            "expected 3 contact bundles, staged {new:?}"
        );

        // Withhold the middle bundle (Go: rename aside): the importer
        // applies the first and records a gap instead of skipping ahead.
        // The gap bundle can never apply (sequence check), so bounded
        // explicit pumps prove the hold without sleeping.
        ferry.ferry_names(&[new[0].clone(), new[2].clone()]);
        for _ in 0..10 {
            pump_high(&high, &[0]);
        }
        assert_eq!(
            contacts(high.node(0)),
            BTreeMap::from([("ann".to_owned(), 0)]),
            "gap leg applied the wrong set"
        );

        // Restoring the bundle fills the gap; all three converge.
        ferry.ferry_names(&[new[1].clone()]);
        let want = BTreeMap::from([
            ("ann".to_owned(), 0),
            ("bob".to_owned(), 1),
            ("cid".to_owned(), 2),
        ]);
        wait_high(&low, &high, 0, &want);

        // Duplicate redelivery is a no-op (Go: imported == 0): re-copy an
        // applied bundle and prove the visible state is unchanged and the
        // bundle is not quarantined.
        std::fs::copy(ferry.low.join(&new[0]), ferry.high.join(&new[0]))
            .expect("duplicate redelivery");
        for _ in 0..10 {
            pump_high(&high, &[0]);
        }
        assert_eq!(
            contacts(high.node(0)),
            want,
            "duplicate redelivery changed state"
        );
        assert!(
            ferry.high.join(&new[0]).exists(),
            "duplicate of an applied bundle was quarantined"
        );
        assert!(
            quarantine_files(&ferry.high).is_empty(),
            "duplicate redelivery quarantined: {:?}",
            quarantine_files(&ferry.high)
        );
        assert_eq!(
            contacts(low.node(0)),
            contacts(high.node(0)),
            "low/high contact states diverged"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("highlow-gap-low");
        high.dump_forensics("highlow-gap-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Two receivers converge (Go: `TestHighLowTwoReceiversConvergeLive`): each
/// receiver imports the same staging independently; the High mesh then
/// converges both to identical state.
#[test]
fn two_receivers_converge() {
    let staging_low = artifacts::runtime_dir("highlow-2r-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-2r-high").join("staging");
    let low = open_exporter(
        "highlow-2r-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(HIGH_SECRET),
    );
    let high = open_high(
        "highlow-2r-high",
        HIGH_DBID,
        vec![HIGH_NODE_0, HIGH_NODE_1],
        vec![staging_high.clone(), staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    );
    migrate(low.node(0), BASE_DDL, "low");
    migrate(high.node(0), BASE_DDL, "high-0");
    migrate(high.node(1), BASE_DDL, "high-1");
    high.mesh().expect("high mesh");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        insert_contact(low.node(0), "ann-id", "ann", 1);
        drain_low(&low);
        ferry.ferry_all();
        let want = BTreeMap::from([("ann".to_owned(), 1)]);
        wait_high(&low, &high, 0, &want);
        wait_high(&low, &high, 1, &want);
        assert_eq!(
            contacts(high.node(0)),
            contacts(high.node(1)),
            "receivers diverged"
        );
        assert_eq!(
            contacts(low.node(0)),
            contacts(high.node(0)),
            "low/high contact states diverged"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("highlow-2r-low");
        high.dump_forensics("highlow-2r-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Forgeries rejected without partial apply or recorded progress (Go:
/// `TestHighLowForgeriesRejectedLive`).
#[test]
fn forgeries_rejected_without_partial_apply() {
    let staging_low = artifacts::runtime_dir("highlow-forge-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-forge-high").join("staging");
    let staging_rogue = artifacts::runtime_dir("highlow-forge-rogue").join("staging");
    let staging_other = artifacts::runtime_dir("highlow-forge-other").join("staging");
    let low = open_exporter(
        "highlow-forge-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(HIGH_SECRET),
    );
    let high = open_high(
        "highlow-forge-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    );
    // Rogue domain: its own signer and recipient, exporting one bundle.
    let rogue = open_exporter(
        "highlow-forge-rogue",
        ROGUE_DBID,
        ROGUE_NODE,
        staging_rogue.clone(),
        ROGUE_SIGNER,
        bridge::recipient_public(ROGUE_SECRET),
    );
    // Wrong-recipient domain: main signer and identity, unknown recipient,
    // so the bundle reaches the recipient check (a distinct domain or node
    // would fail earlier at the domain/signer checks).
    let other = open_exporter(
        "highlow-forge-other",
        LOW_DBID,
        LOW_NODE,
        staging_other.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(OTHER_SECRET),
    );
    migrate(low.node(0), BASE_DDL, "low");
    migrate(high.node(0), BASE_DDL, "high");
    migrate(rogue.node(0), BASE_DDL, "rogue");
    migrate(other.node(0), BASE_DDL, "other");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Baseline the migration bundles, then stage exactly one genuine
        // contact bundle and clear it: each hostile artifact below is
        // presented alone.
        drain_low(&low);
        ferry.ferry_all();
        pump_high(&high, &[0]);
        clear_bundles(&ferry.high);
        insert_contact(low.node(0), "ann-id", "ann", 1);
        drain_low(&low);
        ferry.ferry_all();
        let genuine = bundle_files(&ferry.high);
        assert_eq!(
            genuine.len(),
            1,
            "want 1 genuine bundle, staged {genuine:?}"
        );
        let good_raw =
            std::fs::read(ferry.high.join(&genuine[0])).expect("read genuine bundle");
        clear_bundles(&ferry.high);

        drain_low(&rogue);
        insert_contact(rogue.node(0), "mallory-id", "mallory", 9);
        drain_low(&rogue);
        let rogue_files = bundle_files(&staging_rogue);
        let rogue_raw = std::fs::read(staging_rogue.join(rogue_files.last().expect("rogue bundle")))
            .expect("read rogue bundle");
        drain_low(&other);
        insert_contact(other.node(0), "carol-id", "carol", 3);
        drain_low(&other);
        let other_files = bundle_files(&staging_other);
        let other_raw = std::fs::read(staging_other.join(other_files.last().expect("other bundle")))
            .expect("read wrong-recipient bundle");

        let mut tampered = good_raw.clone();
        let tail = tampered.len() - 1;
        tampered[tail] ^= 0xFF;
        let mut oversized = good_raw.clone();
        oversized.extend(vec![0u8; 17 << 20]);
        let hostile = [
            ("rogue.bundle", rogue_raw),
            ("sealed.bundle", other_raw),
            ("evil.bundle", tampered),
            ("big.bundle", oversized),
        ];
        for (name, raw) in &hostile {
            std::fs::write(ferry.high.join(name), raw).expect("stage hostile artifact");
            // Fail-closed quarantine (deviation: Go's import returns an
            // error; Rust quarantines and returns Ok).
            pump_high(&high, &[0]);
            assert!(
                contacts(high.node(0)).is_empty(),
                "hostile artifact {name} partially applied: {:?}",
                contacts(high.node(0))
            );
            assert_eq!(
                quarantine_files(&ferry.high),
                vec![name.to_string()],
                "hostile artifact {name} quarantine"
            );
            std::fs::remove_file(ferry.high.join("quarantine").join(name))
                .expect("clear quarantine");
        }

        // Wrong extensions are not bridge artifacts at all: ignored in
        // place, never quarantined, never applied.
        for name in ["wrongname.txt", ".hidden"] {
            std::fs::write(ferry.high.join(name), &good_raw).expect("stage misnamed artifact");
            pump_high(&high, &[0]);
            assert!(
                contacts(high.node(0)).is_empty(),
                "misnamed artifact {name} applied: {:?}",
                contacts(high.node(0))
            );
            assert!(
                quarantine_files(&ferry.high).is_empty(),
                "misnamed artifact {name} quarantined"
            );
            assert!(
                ferry.high.join(name).exists(),
                "ignored artifact {name} was moved"
            );
            std::fs::remove_file(ferry.high.join(name)).expect("clear misnamed");
        }

        // An unreadable staging file fails the import without partial
        // apply (Go parity: this leg surfaces an Err). Created unreadable
        // atomically so the background importer cannot stage it first.
        let unreadable = ferry.high.join("unreadable.bundle");
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .mode(0o000)
                .open(&unreadable)
                .expect("stage unreadable artifact");
            use std::io::Write;
            f.write_all(&good_raw).expect("write unreadable artifact");
        }
        #[cfg(not(unix))]
        {
            std::fs::write(&unreadable, &good_raw).expect("stage unreadable artifact");
        }
        if std::fs::read(&unreadable).is_ok() {
            eprintln!("SKIP unreadable leg: staging file still readable (e.g. running as root)");
        } else {
            let err = node_bridge(high.node(0))
                .process()
                .expect_err("unreadable artifact accepted");
            eprintln!("unreadable import failed closed: {err}");
            assert!(
                contacts(high.node(0)).is_empty(),
                "unreadable artifact partially applied: {:?}",
                contacts(high.node(0))
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o644))
                .expect("restore staging permissions");
        }
        std::fs::remove_file(&unreadable).expect("clear unreadable");

        // The importer still accepts the genuine bundle afterwards, which
        // also proves no hostile artifact recorded progress (a skipped
        // sequence would stay skipped).
        std::fs::write(ferry.high.join(&genuine[0]), &good_raw).expect("restore genuine");
        let want = BTreeMap::from([("ann".to_owned(), 1)]);
        wait_high(&low, &high, 0, &want);
        assert_eq!(
            contacts(low.node(0)),
            contacts(high.node(0)),
            "low/high contact states diverged"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("highlow-forge-low");
        high.dump_forensics("highlow-forge-high");
        rogue.dump_forensics("highlow-forge-rogue");
        other.dump_forensics("highlow-forge-other");
    }
    low.finish(failed);
    high.finish(failed);
    rogue.finish(failed);
    other.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Restart resumption on both sides (Go: `TestHighLowRestartResumeLive`).
#[test]
fn restart_resumes_without_recapture_or_reapply() {
    let staging_low = artifacts::runtime_dir("highlow-restart-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-restart-high").join("staging");
    let mut low = open_exporter(
        "highlow-restart-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(HIGH_SECRET),
    );
    let mut high = open_high(
        "highlow-restart-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    );
    migrate(low.node(0), BASE_DDL, "low");
    migrate(high.node(0), BASE_DDL, "high");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        insert_contact(low.node(0), "ann-id", "ann", 1);
        insert_contact(low.node(0), "bob-id", "bob", 2);
        drain_low(&low);
        ferry.ferry_all();
        let want = BTreeMap::from([("ann".to_owned(), 1), ("bob".to_owned(), 2)]);
        wait_high(&low, &high, 0, &want);

        // Restarting the exporter loses no capture resume: nothing
        // re-captures (Go: captured == 0), proven by the Low bundle file
        // set being unchanged across the restart.
        let before = bundle_files(&ferry.low);
        low.stop_node(0);
        low.start_node(0).expect("low restart");
        for _ in 0..5 {
            drain_low(&low);
        }
        assert_eq!(
            bundle_files(&ferry.low),
            before,
            "low re-exported bundles across restart"
        );

        // Restarting the importer loses no inbox/cursor resume: the
        // redelivery is a no-op (Go: received == imported == 0) and state
        // holds both contacts exactly once.
        high.stop_node(0);
        high.start_node(0).expect("high restart");
        for _ in 0..10 {
            pump_high(&high, &[0]);
        }
        assert_eq!(
            contacts(high.node(0)),
            want,
            "high state changed across restart"
        );
        assert_eq!(
            contacts(low.node(0)),
            contacts(high.node(0)),
            "low/high contact states diverged"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("highlow-restart-low");
        high.dump_forensics("highlow-restart-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Schema-hold survival with local-migration release (Go:
/// `TestHighLowSchemaHoldMigrationLive`): the unknown column holds the
/// bundles (nothing applies, no DDL crosses), the hold survives restart,
/// and a local High migration releases the held bundles.
#[test]
fn schema_hold_survives_restart_and_releases_on_local_migration() {
    let staging_low = artifacts::runtime_dir("highlow-schema-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-schema-high").join("staging");
    let low = open_exporter(
        "highlow-schema-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(HIGH_SECRET),
    );
    let mut high = open_high(
        "highlow-schema-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    );
    migrate(low.node(0), EMAIL_DDL, "low");
    migrate(high.node(0), BASE_DDL, "high");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        insert_contact(low.node(0), "ann-id", "ann", 1);
        set_email(low.node(0), "ann-id", "a@x");
        drain_low(&low);
        ferry.ferry_all();
        // The unknown column holds the bundles: nothing applies (Rust
        // traps the hold internally and returns Ok, so there is no
        // waiting-schema error to assert like Go).
        for _ in 0..10 {
            pump_high(&high, &[0]);
        }
        assert!(
            contacts(high.node(0)).is_empty(),
            "held bundle applied: {:?}",
            contacts(high.node(0))
        );
        assert!(
            !has_column(high.node(0), TABLE, "email"),
            "DDL crossed the bridge"
        );
        // The hold survives restart and still applies nothing.
        high.stop_node(0);
        high.start_node(0).expect("high restart");
        for _ in 0..10 {
            pump_high(&high, &[0]);
        }
        assert!(
            contacts(high.node(0)).is_empty(),
            "held bundle applied after restart: {:?}",
            contacts(high.node(0))
        );
        assert!(
            !has_column(high.node(0), TABLE, "email"),
            "DDL crossed the bridge"
        );
        // The local migration releases the bundles; the held email arrives.
        migrate(high.node(0), ADD_EMAIL, "high");
        let arrived = wait_until(WAIT, POLL, || {
            node_bridge(low.node(0)).capture().expect("low bridge capture");
            node_bridge(high.node(0))
                .process()
                .expect("high bridge process");
            email_of(high.node(0), "ann-id").as_deref() == Some("a@x")
        });
        assert!(
            arrived,
            "held email never arrived after local migration: {:?}",
            email_of(high.node(0), "ann-id")
        );
        assert_eq!(
            contacts(high.node(0)),
            BTreeMap::from([("ann".to_owned(), 1)]),
            "released bundle applied the wrong set"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("highlow-schema-low");
        high.dump_forensics("highlow-schema-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Signer rotation plus outage catch-up (Go:
/// `TestHighLowKeyRotationAndOutageResumeLive`).
#[test]
fn signer_rotation_and_outage_catch_up() {
    let staging_low = artifacts::runtime_dir("highlow-rotate-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-rotate-high").join("staging");
    let mut low = open_exporter(
        "highlow-rotate-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(HIGH_SECRET),
    );
    let mut high = open_high(
        "highlow-rotate-high",
        HIGH_DBID,
        vec![HIGH_NODE_0],
        vec![staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    );
    migrate(low.node(0), BASE_DDL, "low");
    migrate(high.node(0), BASE_DDL, "high");
    let mut ferry = Ferry::new(staging_low, staging_high);
    let low_origin = low.node_origin_pubkey(0);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drain_low(&low);
        ferry.ferry_all();
        pump_high(&high, &[0]);

        // Round 1 under the predecessor key, imported before rotation
        // (Rust pins a single signer per source node, so Go's overlap
        // trust is inexpressible; see the module docs).
        insert_contact(low.node(0), "ann-id", "ann", 1);
        drain_low(&low);
        let round1 = ferry.new_bundles();
        ferry.ferry_names(&round1);
        wait_high(&low, &high, 0, &BTreeMap::from([("ann".to_owned(), 1)]));

        // Rotate: the successor signs from here on (config edit plus
        // restart on both sides, Go rewriteNodeConfig parity).
        low.stop_node(0);
        low.set_bridge_config(
            0,
            Some(BridgeConfig::exporter(
                LOW_DBID,
                ferry.low.clone(),
                STREAM.into(),
                LOW_SIGNER_B,
                bridge::recipient_public(HIGH_SECRET),
            )),
        )
        .expect("low signer rotation");
        low.start_node(0).expect("low restart after rotation");
        high.stop_node(0);
        high.set_bridge_config(
            0,
            Some(BridgeConfig::receiver(
                HIGH_DBID,
                ferry.high.clone(),
                STREAM.into(),
                HIGH_SECRET,
                BTreeSet::from([LOW_DBID]),
                low_signers(&LOW_SIGNER_B),
                BTreeMap::from([(LOW_NODE, low_origin)]),
            )),
        )
        .expect("high signer rotation");
        high.start_node(0).expect("high restart after rotation");

        // Round 2 under the successor key imports on top of round 1.
        insert_contact(low.node(0), "bob-id", "bob", 2);
        drain_low(&low);
        let round2 = ferry.new_bundles();
        ferry.ferry_names(&round2);
        let both = BTreeMap::from([("ann".to_owned(), 1), ("bob".to_owned(), 2)]);
        wait_high(&low, &high, 0, &both);

        // Retire the predecessor: clear the applied bundles, then prove a
        // duplicate of a current bundle still stages cleanly while a
        // retired-signer replay fails closed without disturbing state.
        clear_bundles(&ferry.high);
        clear_quarantine(&ferry.high);
        std::fs::copy(ferry.low.join(&round2[0]), ferry.high.join(&round2[0]))
            .expect("duplicate round-2 bundle");
        pump_high(&high, &[0]);
        assert!(
            ferry.high.join(&round2[0]).exists(),
            "duplicate of a current bundle was quarantined"
        );
        assert!(
            quarantine_files(&ferry.high).is_empty(),
            "current duplicate quarantined: {:?}",
            quarantine_files(&ferry.high)
        );
        assert_eq!(contacts(high.node(0)), both);
        std::fs::remove_file(ferry.high.join(&round2[0])).expect("clear duplicate");
        let retired =
            std::fs::read(ferry.low.join(&round1[0])).expect("read round-1 bundle");
        std::fs::write(ferry.high.join("replay.bundle"), &retired).expect("stage replay");
        pump_high(&high, &[0]);
        assert_eq!(
            quarantine_files(&ferry.high),
            vec!["replay.bundle".to_owned()],
            "retired-signer replay not quarantined"
        );
        assert_eq!(
            contacts(high.node(0)),
            both,
            "retired-signer replay disturbed state"
        );
        std::fs::remove_file(ferry.high.join("quarantine").join("replay.bundle"))
            .expect("clear quarantine");

        // Prolonged outage: events queue durably on Low (capture-only; the
        // bundles stay in the Low dir), survive a Low restart, then catch
        // up completely. There is no pending-events counter API, so the
        // exact converged total (backlog == 4) proves all five events
        // arrived exactly once.
        insert_contact(low.node(0), "backlog-id", "backlog", 0);
        for _ in 0..4 {
            add_score(low.node(0), "backlog-id", 1);
        }
        drain_low(&low);
        low.stop_node(0);
        low.start_node(0).expect("low restart during outage");
        assert!(
            !contacts(high.node(0)).contains_key("backlog"),
            "outage leaked to high: {:?}",
            contacts(high.node(0))
        );
        ferry.ferry_all();
        let caught_up = BTreeMap::from([
            ("ann".to_owned(), 1),
            ("bob".to_owned(), 2),
            ("backlog".to_owned(), 4),
        ]);
        wait_high(&low, &high, 0, &caught_up);
        assert_eq!(
            contacts(low.node(0)),
            contacts(high.node(0)),
            "low/high contact states diverged"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("highlow-rotate-low");
        high.dump_forensics("highlow-rotate-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// High-owned-field reorder convergence across two meshed High peers (Go:
/// `TestHighLowOwnershipReorderAcrossPeersLive`): peer A imports bundle 1,
/// takes a High override, then imports bundle 2 (override-then-import);
/// peer B imports both bundles before the mesh delivers the override
/// (import-then-override). Both converge on the High bytes with High-owned
/// provenance, and an explicit release on A converges both back to the Low
/// value. (Adversarial HLC ordering stays pinned at the DB layer by
/// `TestBridgeShadowReorderConvergence`, as in Go.)
#[test]
fn high_owned_field_reorder_converges_across_peers() {
    let staging_low = artifacts::runtime_dir("highlow-own-low").join("staging");
    let staging_high = artifacts::runtime_dir("highlow-own-high").join("staging");
    let low = open_exporter(
        "highlow-own-low",
        LOW_DBID,
        LOW_NODE,
        staging_low.clone(),
        LOW_SIGNER_A,
        bridge::recipient_public(HIGH_SECRET),
    );
    let high = open_high(
        "highlow-own-high",
        HIGH_DBID,
        vec![HIGH_NODE_0, HIGH_NODE_1],
        vec![staging_high.clone(), staging_high.clone()],
        HIGH_SECRET,
        BTreeSet::from([LOW_DBID]),
        low_signers(&LOW_SIGNER_A),
        low_origins(&low),
    );
    migrate(low.node(0), BASE_DDL, "low");
    migrate(high.node(0), BASE_DDL, "high-0");
    migrate(high.node(1), BASE_DDL, "high-1");
    let mut ferry = Ferry::new(staging_low, staging_high);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drain_low(&low);
        ferry.ferry_all();
        pump_high(&high, &[0, 1]);

        insert_contact(low.node(0), "row-0", "low-1", 1);
        drain_low(&low);
        ferry.ferry_all();
        // Peer A: override-then-import. The wait for bundle 1 is the
        // ordering gate: the override lands strictly before bundle 2
        // exists, so no background pump can collapse the order.
        wait_high(&low, &high, 0, &BTreeMap::from([("low-1".to_owned(), 1)]));
        rename(high.node(0), "row-0", "high");
        assert_eq!(
            contacts(high.node(0)),
            BTreeMap::from([("high".to_owned(), 1)]),
            "A override not visible locally"
        );
        rename(low.node(0), "row-0", "low-2");
        drain_low(&low);
        ferry.ferry_all();
        wait_high(&low, &high, 0, &BTreeMap::from([("high".to_owned(), 1)]));

        // Peer B: import-then-override (the override arrives later over
        // the mesh). B has no owner record yet (no mesh), so both bundles
        // show the Low value.
        wait_high(&low, &high, 1, &BTreeMap::from([("low-2".to_owned(), 1)]));

        // Mesh the High peers only after both orders are established
        // locally; the mesh is the only override-delivery path.
        high.add_peer(0, 1).expect("mesh a->b");
        high.add_peer(1, 0).expect("mesh b->a");
        let owned = BTreeMap::from([("high".to_owned(), 1)]);
        wait_meshed(&high, &[0, 1], &owned, "ownership reorder");
        assert_eq!(
            contacts(high.node(0)),
            contacts(high.node(1)),
            "peers diverged after mesh"
        );

        // High-owned provenance (behavioral): drain the bridge to idle so
        // the Low update has provably arrived, then prove it stayed
        // suppressed on both peers.
        rename(low.node(0), "row-0", "low-3");
        drain_low(&low);
        ferry.ferry_all();
        for _ in 0..100 {
            let l = node_bridge(low.node(0)).capture().expect("low capture");
            let a = node_bridge(high.node(0)).process().expect("high-0 process");
            let b = node_bridge(high.node(1)).process().expect("high-1 process");
            if l == 0 && a == 0 && b == 0 {
                break;
            }
        }
        assert_eq!(
            contacts(high.node(0)),
            owned,
            "low-3 leaked through A's ownership"
        );
        assert_eq!(
            contacts(high.node(1)),
            owned,
            "low-3 leaked through B's ownership"
        );

        // Explicit release on A converges both peers back to the Low value.
        release_name(&high, "row-0");
        let released = BTreeMap::from([("low-3".to_owned(), 1)]);
        wait_meshed(&high, &[0, 1], &released, "ownership release");
        assert_eq!(
            contacts(high.node(0)),
            contacts(high.node(1)),
            "peers diverged after release"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("highlow-own-low");
        high.dump_forensics("highlow-own-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
