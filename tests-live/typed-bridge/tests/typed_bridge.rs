//! Rust port of the Go `typed-bridge` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/typed-bridge/`
//! (`typed_bridge_test.go`; the Go dir has no README.md). Two single-node
//! clusters form the Low (exporter) and High (importer) bridge domains sharing
//! one staging directory: a typed row inserted on Low is exported, imported
//! through High's typed durable path (`high_low::import`), overridden by a
//! High write (automatic ownership via `promote`), shielded from a Low update,
//! released back to Low, updated again, and still present after a High restart.
//!
//! Plain SQL replaces the Go typed HTTP API (the Rust product has no HTTP
//! server); `TypedRename` maps to an `UPDATE` of the LWW `title` field so the
//! primary-key row identity is preserved (ownership only covers non-PK cells).
//! See the scenario README.md for the full leg mapping.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use murmur_live_harness::murmur_replication::bridge::{
    self, Bridge, Config as BridgeConfig,
};
use murmur_live_harness::murmur_replication::protocol;
use murmur_live_harness::sqlite_session::{Database, Migration, Value};
use murmur_live_harness::{artifacts, wait_until, BridgeOptions, Cluster, ClusterOptions};

/// Low (exporter) cluster name; owns `runtime/typed-bridge-low/`, which also
/// holds the shared staging dir so artifacts are retained/cleaned with it.
const LOW_NAME: &str = "typed-bridge-low";
/// High (importer) cluster name; owns `runtime/typed-bridge-high/`.
const HIGH_NAME: &str = "typed-bridge-high";
/// Bridge stream (Go: `typed-stream`).
const STREAM: &str = "typed-stream";
/// Typed table (Go: `live_typed_records`). `title` is the LWW rename target.
const TABLE: &str = "live_typed_records";
const DDL: &str =
    "CREATE TABLE live_typed_records(name TEXT PRIMARY KEY, title TEXT NOT NULL)";
/// Row identity, fixed for the whole scenario (Go renames one row by ID).
const ROW: &str = "bridge-native-record";
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 50 ms polls).
const POLL: Duration = Duration::from_millis(50);

/// Independent bridge domains (Go: two separate clusters).
const LOW_DBID: [u8; 16] = [0xb0; 16];
const HIGH_DBID: [u8; 16] = [0xb1; 16];
const LOW_NODE: [u8; 16] = [0x10; 16];
const HIGH_NODE: [u8; 16] = [0x11; 16];
/// Low bundle-signer seed (ed25519; High pins the verifying key).
const LOW_SIGNER: [u8; 32] = [0x21; 32];
/// High receiver secret (X25519; Low encrypts to the derived recipient).
const HIGH_SECRET: [u8; 32] = [0x22; 32];

/// Typed insert on Low (Go: `Cluster.TypedInsertWithID`).
fn insert_row(db: &Database) {
    db.transaction(|sql| {
        sql.execute(
            "INSERT INTO live_typed_records(name, title) VALUES(?, ?)",
            [ROW, ROW],
        )?;
        Ok(())
    })
    .expect("typed insert failed");
    db.flush().expect("flush after typed insert");
}

/// Known-field update (Go: `Cluster.TypedRename`, applied to the LWW `title`
/// field so the primary-key row identity is preserved).
fn update_title(db: &Database, title: &str) {
    let title = title.to_owned();
    db.transaction(move |sql| {
        sql.execute(
            "UPDATE live_typed_records SET title = ? WHERE name = ?",
            [title, ROW.to_owned()],
        )?;
        Ok(())
    })
    .expect("title update failed");
    db.flush().expect("flush after title update");
}

/// Row count by title (Go: `Cluster.TypedCount`).
fn title_count(db: &Database, title: &str) -> i64 {
    let title = title.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT count(*) FROM live_typed_records WHERE title = ?",
            [title],
            |r| r.get::<_, i64>(0),
        )?)
    })
    .unwrap_or(-1)
}

fn low_bridge(low: &Cluster) -> Arc<Bridge> {
    low.node(0)
        .replication()
        .expect("low replication handle")
        .bridge()
        .expect("low bridge")
}

fn high_bridge(high: &Cluster) -> Arc<Bridge> {
    high.node(0)
        .replication()
        .expect("high replication handle")
        .bridge()
        .expect("high bridge")
}

/// One export+import pump (Go: `BridgeExport` + `BridgeImport`). Per-call
/// bundle counts are not asserted: the replication engine also drives
/// capture/process in the background, so progress is proven by row
/// convergence instead.
fn sync(low: &Cluster, high: &Cluster) {
    low_bridge(low).capture().expect("low bridge capture");
    high_bridge(high).process().expect("high bridge process");
}

/// Pump until Low has nothing left to export and High has nothing left to
/// import (bounded idle loop; the `bridge_files.rs` product-test pattern).
/// Proves a Low update has arrived before a suppression assertion.
fn drain(low: &Cluster, high: &Cluster) {
    for _ in 0..100 {
        let exported = low_bridge(low).capture().expect("low bridge capture");
        let imported = high_bridge(high).process().expect("high bridge process");
        if exported == 0 && imported == 0 {
            return;
        }
    }
    panic!("bridge did not become idle");
}

fn wait_title(low: &Cluster, high: &Cluster, title: &str, want: i64) {
    let converged = wait_until(WAIT, POLL, || {
        sync(low, high);
        title_count(high.node(0), title) == want
    });
    assert!(
        converged,
        "high title count for {title:?} = {}, want {want}",
        title_count(high.node(0), title)
    );
}

/// Release High ownership of the row's `title` field (Go: `BridgeRelease`;
/// the key is the postcard-encoded PK value vector).
fn release_title(high: &Cluster) {
    let key = murmur_live_harness::murmur_replication::encode(&vec![Value::Text(
        ROW.to_owned(),
    )])
    .expect("encode release key");
    let released = high_bridge(high)
        .release(TABLE.to_owned(), key, Some("title".to_owned()))
        .expect("bridge release");
    assert!(released, "bridge release reported no progress");
}

#[test]
fn typed_bridge_imports_into_native_rime() {
    let staging = artifacts::runtime_dir(LOW_NAME).join("staging");
    let low = Cluster::new(ClusterOptions {
        name: LOW_NAME.into(),
        num_nodes: 1,
        database_id: Some(LOW_DBID),
        node_ids: vec![LOW_NODE],
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeConfig::exporter(
                    LOW_DBID,
                    staging.clone(),
                    STREAM.into(),
                    LOW_SIGNER,
                    bridge::recipient_public(HIGH_SECRET),
                ),
            )]),
        },
        ..Default::default()
    })
    .expect("low cluster setup");
    // High pins Low's origin key so imported transactions verify; the seed
    // itself stays inside the harness.
    let low_origin = low.node_origin_pubkey(0);
    let mut high = Cluster::new(ClusterOptions {
        name: HIGH_NAME.into(),
        num_nodes: 1,
        database_id: Some(HIGH_DBID),
        node_ids: vec![HIGH_NODE],
        manual_peers: true,
        bridge: BridgeOptions {
            nodes: HashMap::from([(
                0,
                BridgeConfig::receiver(
                    HIGH_DBID,
                    staging,
                    STREAM.into(),
                    HIGH_SECRET,
                    BTreeSet::from([LOW_DBID]),
                    BTreeMap::from([(LOW_NODE, protocol::public_key(&LOW_SIGNER))]),
                    BTreeMap::from([(LOW_NODE, low_origin)]),
                ),
            )]),
        },
        ..Default::default()
    })
    .expect("high cluster setup");
    for (label, db) in [("low", low.node(0)), ("high", high.node(0))] {
        db.migrate(Migration::new(DDL))
            .unwrap_or_else(|e| panic!("{label} migrate failed: {e}"));
        db.flush()
            .unwrap_or_else(|e| panic!("{label} flush after migrate failed: {e}"));
    }

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Export + import the native record through the typed durable path.
        insert_row(low.node(0));
        wait_title(&low, &high, ROW, 1);
        // High typed field override claims ownership of `title`.
        update_title(high.node(0), "high-owned-record");
        assert_eq!(
            title_count(high.node(0), "high-owned-record"),
            1,
            "high override not visible locally"
        );
        // Low update while the field is High-owned: imported but suppressed.
        update_title(low.node(0), "low-update-before-release");
        drain(&low, &high);
        assert_eq!(
            title_count(high.node(0), "high-owned-record"),
            1,
            "high-owned value lost to low update"
        );
        assert_eq!(
            title_count(high.node(0), "low-update-before-release"),
            0,
            "low update leaked through high ownership"
        );
        // Release ownership, then the Low update flows through.
        release_title(&high);
        update_title(low.node(0), "low-after-release");
        wait_title(&low, &high, "low-after-release", 1);
        // The imported result survives a High-node restart.
        high.stop_node(0);
        high.start_node(0).expect("high restart");
        assert_eq!(
            title_count(high.node(0), "low-after-release"),
            1,
            "imported row lost across high restart"
        );
    }))
    .is_err();
    if failed {
        low.dump_forensics("typed-bridge-low");
        high.dump_forensics("typed-bridge-high");
    }
    low.finish(failed);
    high.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
