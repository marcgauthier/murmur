//! Rust port of the Go `bridge-two-streams` live scenario (main test).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/bridge-two-streams/`
//! (`two_streams_test.go` + `README.md`). Two independent single-node Low
//! domains (distinct DBIDs, CAs, keys, streams, staging dirs) export managed
//! rows; a two-node High cluster imports stream-a on node0 and stream-b on
//! node1, then converges both domains' rows over its mesh. A stream-a-only
//! second round must arrive without disturbing stream-b progress.
//!
//! Plain SQL replaces the Go typed HTTP API (the Rust product has no HTTP
//! server); `BridgeExport`/`BridgeImport` map to direct `Bridge::capture()`
//! (Low) and `Bridge::process()` (High) pumps, with progress proven by row
//! convergence instead of per-call bundle counts (the replication engine
//! also drives capture/process in the background). `BridgeStatus` progress
//! and `Provenance` have no public Rust API (spool keys are internal), so
//! both legs are proven behaviorally: Low-owned provenance by a Low update
//! flowing through to High (a High-owned field would suppress it), and
//! stream-b progress isolation by a stream-b-only follow-up round delivering
//! exactly once from the undisturbed cursor. A drain-to-idle leg covers the
//! Go README's duplicate-delivery criterion (receipt idempotence).
//!
//! NOT ported: `TestCrossDomainIdentityCollisionFailsClosed`. The Go product
//! rejects a cross-domain same-row-ID import with `ErrIdentityCollision`
//! ("collides") and leaves first-writer state intact; the Rust product has no
//! row-identity collision detection (`high_low::import` stores one shadow
//! per source domain and `join` merges them deterministically), and
//! `Bridge::process()` traps import failures into internal `09/bridge/hold/`
//! spool records and returns `Ok`, so no failing-closed signal is publicly
//! observable. Asserting the current merge behavior would enshrine a
//! fail-open divergence from the Go gate. See the scenario README.md.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use murmur_live_harness::murmur_replication::bridge::{
    self, Bridge, Config as BridgeConfig,
};
use murmur_live_harness::murmur_replication::protocol;
use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{artifacts, wait_until, BridgeOptions, Cluster, ClusterOptions};

/// Low-A (exporter) cluster name; owns `runtime/bridge-two-streams-low-a/`,
/// which also holds staging-a so artifacts are retained/cleaned with it.
const LOW_A_NAME: &str = "bridge-two-streams-low-a";
/// Low-B (exporter) cluster name; owns `runtime/bridge-two-streams-low-b/`.
const LOW_B_NAME: &str = "bridge-two-streams-low-b";
/// High (importer) cluster name; owns `runtime/bridge-two-streams-high/`.
const HIGH_NAME: &str = "bridge-two-streams-high";
/// Bridge streams (Go: `stream-a`, `stream-b`).
const STREAM_A: &str = "stream-a";
const STREAM_B: &str = "stream-b";
const DDL: &str = "CREATE TABLE live_typed_contention(id TEXT PRIMARY KEY, name TEXT NOT NULL)";
/// Convergence deadline (Go: 60 s waits).
const WAIT: Duration = Duration::from_secs(60);
/// Convergence poll interval (Go: 200 ms polls).
const POLL: Duration = Duration::from_millis(200);

/// Independent bridge domains (Go: distinct DBIDs, CAs, keys per domain;
/// each harness cluster already provisions its own CA).
const LOW_A_DBID: [u8; 16] = [0xA0; 16];
const LOW_B_DBID: [u8; 16] = [0xB0; 16];
const HIGH_DBID: [u8; 16] = [0xC0; 16];
const LOW_A_NODE: [u8; 16] = [0xA1; 16];
const LOW_B_NODE: [u8; 16] = [0xB1; 16];
const HIGH_NODE_0: [u8; 16] = [0xC1; 16];
const HIGH_NODE_1: [u8; 16] = [0xC2; 16];
const LOW_A_KEY: [u8; 32] = [0xA2; 32];
const LOW_B_KEY: [u8; 32] = [0xB2; 32];
const HIGH_KEY: [u8; 32] = [0xC3; 32];
/// Low bundle-signer seeds (ed25519; each High importer pins its domain key).
const LOW_A_SIGNER: [u8; 32] = [0x21; 32];
const LOW_B_SIGNER: [u8; 32] = [0x22; 32];
/// High receiver secrets (X25519; one per stream, like the Go key files).
const HIGH_SECRET_A: [u8; 32] = [0x31; 32];
const HIGH_SECRET_B: [u8; 32] = [0x32; 32];

/// Go row identity: `%032x` of a per-domain base plus the index.
fn row_id(base: u32, i: u32) -> String {
    format!("{:032x}", base + i)
}

/// Contention insert (Go: `TypedContentionInsert`).
fn insert_row(db: &Database, id: &str, name: &str) {
    let (id, name) = (id.to_owned(), name.to_owned());
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_typed_contention(id, name) VALUES(?, ?)",
            [id, name],
        )?;
        Ok(())
    })
    .expect("contention insert failed");
    db.flush().expect("flush after insert");
}

/// Contention rename (Go provenance probes use the `Name` field).
fn update_name(db: &Database, id: &str, name: &str) {
    let (id, name) = (id.to_owned(), name.to_owned());
    db.transaction(move |sql| {
        sql.execute(
            "UPDATE live_typed_contention SET name = ? WHERE id = ?",
            [name, id],
        )?;
        Ok(())
    })
    .expect("contention update failed");
    db.flush().expect("flush after update");
}

/// Sorted visible names (Go: `TypedContentionRows` + digest over names).
fn sorted_names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM live_typed_contention ORDER BY name")?;
        let mut out = Vec::new();
        for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
            out.push(row?);
        }
        Ok(out)
    })
    .unwrap_or_default()
}

fn node_bridge(db: &Database) -> Arc<Bridge> {
    db.replication()
        .expect("replication handle")
        .bridge()
        .expect("bridge")
}

/// Export+import acceleration pump (Go: `BridgeExport` + `BridgeImport`).
/// Counts are never asserted: the replication engine also drives
/// capture/process in the background, so progress is proven by row
/// convergence instead.
fn pump(low_a: &Cluster, low_b: &Cluster, high: &Cluster) {
    node_bridge(low_a.node(0)).capture().expect("low-a capture");
    node_bridge(low_b.node(0)).capture().expect("low-b capture");
    node_bridge(high.node(0)).process().expect("high-0 process");
    node_bridge(high.node(1)).process().expect("high-1 process");
}

/// Pump until every bridge is idle (bounded idle loop; the `bridge_files.rs`
/// product-test pattern).
fn drain(low_a: &Cluster, low_b: &Cluster, high: &Cluster) {
    for _ in 0..100 {
        let a = node_bridge(low_a.node(0)).capture().expect("low-a capture");
        let b = node_bridge(low_b.node(0)).capture().expect("low-b capture");
        let h0 = node_bridge(high.node(0))
            .process()
            .expect("high-0 process");
        let h1 = node_bridge(high.node(1))
            .process()
            .expect("high-1 process");
        if a == 0 && b == 0 && h0 == 0 && h1 == 0 {
            return;
        }
    }
    panic!("bridges did not become idle");
}

/// Wait until both High nodes show exactly `want` (Go: `waitHighConverged` +
/// `assertNames`, with the pump inside the poll).
fn wait_high_names(low_a: &Cluster, low_b: &Cluster, high: &Cluster, want: &[String]) {
    let converged = wait_until(WAIT, POLL, || {
        pump(low_a, low_b, high);
        sorted_names(high.node(0)) == want && sorted_names(high.node(1)) == want
    });
    assert!(
        converged,
        "high nodes did not converge on {} rows: node0={:?} node1={:?}",
        want.len(),
        sorted_names(high.node(0)),
        sorted_names(high.node(1))
    );
}

#[test]
fn two_low_domains_into_one_high() {
    let staging_a = artifacts::runtime_dir(LOW_A_NAME).join("staging-a");
    let staging_b = artifacts::runtime_dir(LOW_B_NAME).join("staging-b");
    let low_a = Cluster::new(ClusterOptions {
        name: LOW_A_NAME.into(),
        num_nodes: 1,
        database_id: Some(LOW_A_DBID),
        node_ids: vec![LOW_A_NODE],
        key: LOW_A_KEY,
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeConfig::exporter(
                    LOW_A_DBID,
                    staging_a.clone(),
                    STREAM_A.into(),
                    LOW_A_SIGNER,
                    bridge::recipient_public(HIGH_SECRET_A),
                ),
            )]),
        },
        ..Default::default()
    })
    .expect("low-a cluster setup");
    let low_b = Cluster::new(ClusterOptions {
        name: LOW_B_NAME.into(),
        num_nodes: 1,
        database_id: Some(LOW_B_DBID),
        node_ids: vec![LOW_B_NODE],
        key: LOW_B_KEY,
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeConfig::exporter(
                    LOW_B_DBID,
                    staging_b.clone(),
                    STREAM_B.into(),
                    LOW_B_SIGNER,
                    bridge::recipient_public(HIGH_SECRET_B),
                ),
            )]),
        },
        ..Default::default()
    })
    .expect("low-b cluster setup");
    // Each High importer pins its own domain's origin key so imported
    // transactions verify; the seeds themselves stay inside the harness.
    let low_a_origin = low_a.node_origin_pubkey(0);
    let low_b_origin = low_b.node_origin_pubkey(0);
    let high = Cluster::new(ClusterOptions {
        name: HIGH_NAME.into(),
        num_nodes: 2,
        database_id: Some(HIGH_DBID),
        node_ids: vec![HIGH_NODE_0, HIGH_NODE_1],
        key: HIGH_KEY,
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([
                (
                    0,
                    BridgeConfig::receiver(
                        HIGH_DBID,
                        staging_a,
                        STREAM_A.into(),
                        HIGH_SECRET_A,
                        BTreeSet::from([LOW_A_DBID]),
                        BTreeMap::from([(LOW_A_NODE, protocol::public_key(&LOW_A_SIGNER))]),
                        BTreeMap::from([(LOW_A_NODE, low_a_origin)]),
                    ),
                ),
                (
                    1,
                    BridgeConfig::receiver(
                        HIGH_DBID,
                        staging_b,
                        STREAM_B.into(),
                        HIGH_SECRET_B,
                        BTreeSet::from([LOW_B_DBID]),
                        BTreeMap::from([(LOW_B_NODE, protocol::public_key(&LOW_B_SIGNER))]),
                        BTreeMap::from([(LOW_B_NODE, low_b_origin)]),
                    ),
                ),
            ]),
        },
        ..Default::default()
    })
    .expect("high cluster setup");
    // Identical DDL on all three domains, then form the High mesh
    // (manual_peers + mesh-after-migrate so a local migrate never races
    // schema auto-adoption of the identical DDL).
    for (label, db) in [
        ("low-a", low_a.node(0)),
        ("low-b", low_b.node(0)),
        ("high-0", high.node(0)),
        ("high-1", high.node(1)),
    ] {
        db.migrate(Migration::new(DDL))
            .unwrap_or_else(|e| panic!("{label} migrate failed: {e}"));
        db.flush()
            .unwrap_or_else(|e| panic!("{label} flush after migrate failed: {e}"));
    }
    high.mesh().expect("high mesh");

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Round 1: disjoint rows from both domains land on both High nodes.
        for i in 0..5 {
            insert_row(low_a.node(0), &row_id(0xA000, i), &format!("a-{i}"));
            insert_row(low_b.node(0), &row_id(0xB000, i), &format!("b-{i}"));
        }
        let mut round1: Vec<String> = (0..5)
            .map(|i| format!("a-{i}"))
            .chain((0..5).map(|i| format!("b-{i}")))
            .collect();
        round1.sort();
        wait_high_names(&low_a, &low_b, &high, &round1);

        // Duplicate delivery: pump to idle, then prove the converged set is
        // unchanged (receipt idempotence: redelivery applies nothing twice).
        drain(&low_a, &low_b, &high);
        assert_eq!(
            sorted_names(high.node(0)),
            round1,
            "high-0 changed after idle pumps"
        );
        assert_eq!(
            sorted_names(high.node(1)),
            round1,
            "high-1 changed after idle pumps"
        );

        // Low-owned provenance (behavioral): a Low update to one row per
        // domain must flow through to both High nodes (a High-owned field
        // would suppress it); then restore the Go row names.
        let probe_a = row_id(0xA000, 0);
        let probe_b = row_id(0xB000, 0);
        update_name(low_a.node(0), &probe_a, "a-0-prov");
        update_name(low_b.node(0), &probe_b, "b-0-prov");
        let mut proved: Vec<String> = round1
            .iter()
            .map(|n| match n.as_str() {
                "a-0" => "a-0-prov".to_owned(),
                "b-0" => "b-0-prov".to_owned(),
                other => other.to_owned(),
            })
            .collect();
        proved.sort();
        wait_high_names(&low_a, &low_b, &high, &proved);
        update_name(low_a.node(0), &probe_a, "a-0");
        update_name(low_b.node(0), &probe_b, "b-0");
        wait_high_names(&low_a, &low_b, &high, &round1);

        // Round 2: stream-a only. New A rows arrive on both High nodes.
        for i in 5..8 {
            insert_row(low_a.node(0), &row_id(0xA000, i), &format!("a-{i}"));
        }
        let mut round2: Vec<String> = (0..8)
            .map(|i| format!("a-{i}"))
            .chain((0..5).map(|i| format!("b-{i}")))
            .collect();
        round2.sort();
        wait_high_names(&low_a, &low_b, &high, &round2);

        // Stream-b progress isolation (behavioral): a stream-b-only round
        // must deliver exactly once from the undisturbed cursor (no loss
        // from a skipped sequence, no duplication).
        for i in 5..7 {
            insert_row(low_b.node(0), &row_id(0xB000, i), &format!("b-{i}"));
        }
        let mut round3: Vec<String> = (0..8)
            .map(|i| format!("a-{i}"))
            .chain((0..7).map(|i| format!("b-{i}")))
            .collect();
        round3.sort();
        wait_high_names(&low_a, &low_b, &high, &round3);
    }))
    .is_err();
    if failed {
        low_a.dump_forensics("bridge-two-streams-low-a");
        low_b.dump_forensics("bridge-two-streams-low-b");
        high.dump_forensics("bridge-two-streams-high");
    }
    low_a.finish(failed);
    low_b.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
