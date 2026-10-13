//! Rust port of the Go `typed-records` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/typed-records/`
//! (`typed_records_test.go`, `models_test.go`). Two nodes share one typed
//! table; inserts, PN_COUNTER / OR_SET / MIN / MAX policy ops, explicit
//! transactions, typed subscriptions, operator GC, schema migration, offline
//! CRDT convergence, snapshot catch-up, schema-branch merge, typed files, and
//! node-local rows are exercised across seven `#[test]` fns (one cluster
//! each, distinct `runtime/` names so parallel execution stays isolated).
//!
//! Plain SQL plus `policy.rs` ops replace the Go managed typed HTTP API (the
//! Rust product has no HTTP server); see the scenario README.md for the
//! per-leg mapping and the three legs that cannot be ported (SQL-isolation
//! HTTP 410s, and the two `Config.Models` reflection suites).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use murmur_live_harness::murmur_replication::policies::Policy;
use murmur_live_harness::sqlite_session::{
    Database, Migration, SubscriptionEvent, SubscriptionOptions, Value,
};
use murmur_live_harness::{
    wait_until, Cluster, ClusterOptions, FilesOptions, ReplicationOptions,
};

/// Typed table (Go: `live_typed_records` with counter/set/extrema fields).
const TABLE: &str = "live_typed_records";
/// Base DDL. `counter`/`tags`/`peak`/`floor` are policy columns (see
/// [`merge_policies`]); `title` is a plain LWW field for the rename leg.
/// Set projections start as `x'00'` (postcard empty vec), the `tests/`
/// convention for fresh set cells.
const BASE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT PRIMARY KEY, title TEXT NOT NULL, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)";
/// Node-local table (Go: `TableScopeNodeLocal`; replication disabled).
const PRIVATE_DDL: &str =
    "CREATE TABLE typed_local_private(name TEXT PRIMARY KEY, v INTEGER NOT NULL)";
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 25-100 ms polls).
const POLL: Duration = Duration::from_millis(50);

/// (table, column) -> merge policy for the typed table (Go: PN_COUNTER,
/// OR_SET, numeric MAX/MIN field kinds).
fn merge_policies() -> BTreeMap<(String, String), Policy> {
    BTreeMap::from([
        ((TABLE.into(), "counter".into()), Policy::Counter),
        ((TABLE.into(), "tags".into()), Policy::Set),
        ((TABLE.into(), "peak".into()), Policy::Max),
        ((TABLE.into(), "floor".into()), Policy::Min),
    ])
}

/// Migrate every node with `ddl` (Go: shared `Config.Tables` definition).
fn migrate_all(cluster: &Cluster, ddl: &str) {
    for i in 0..cluster.len() {
        cluster
            .node(i)
            .migrate(Migration::new(ddl))
            .unwrap_or_else(|e| panic!("node{} migrate {ddl:?} failed: {e}", i + 1));
        cluster
            .node(i)
            .flush()
            .unwrap_or_else(|e| panic!("node{} flush after migrate failed: {e}", i + 1));
    }
}

/// Typed insert on one node (Go: `Cluster.TypedInsert`).
fn insert_row(db: &Database, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_typed_records(name, title, counter, tags, peak, floor) VALUES(?, '', 0, x'00', 0, 0.0)",
            [name],
        )?;
        Ok(())
    })
    .expect("typed insert failed");
    db.flush().expect("flush after typed insert");
}

/// Row count by name (Go: `Cluster.TypedCount`).
fn typed_count(db: &Database, name: &str) -> i64 {
    let name = name.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT count(*) FROM live_typed_records WHERE name = ?",
            [name],
            |r| r.get::<_, i64>(0),
        )?)
    })
    .unwrap_or(-1)
}

/// PN_COUNTER projection (Go: `Cluster.TypedCounterValue`).
fn counter_value(db: &Database, name: &str) -> i64 {
    let name = name.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT counter FROM live_typed_records WHERE name = ?",
            [name],
            |r| r.get::<_, i64>(0),
        )?)
    })
    .unwrap_or(i64::MIN)
}

/// OR_SET projection, sorted (Go: `Cluster.TypedSetValues`).
fn set_values(db: &Database, name: &str) -> Vec<String> {
    let name = name.to_owned();
    let bytes: Vec<u8> = match db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT tags FROM live_typed_records WHERE name = ?",
            [name],
            |r| r.get::<_, Vec<u8>>(0),
        )?)
    }) {
        Ok(bytes) => bytes,
        Err(_) => return vec!["<read-error>".into()],
    };
    let values: Vec<Value> =
        murmur_live_harness::murmur_replication::decode(&bytes).unwrap_or_default();
    let mut out: Vec<String> = values
        .into_iter()
        .filter_map(|v| match v {
            Value::Text(s) => Some(s),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// Numeric extrema projection (Go: `Cluster.TypedExtremaValues`).
fn extrema(db: &Database, name: &str) -> (i64, f64) {
    let name = name.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT peak, floor FROM live_typed_records WHERE name = ?",
            [name],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?)),
        )?)
    })
    .unwrap_or((i64::MIN, f64::NAN))
}

/// PN_COUNTER op (Go: `Cluster.TypedCounterAdd`).
fn counter_add(db: &Database, name: &str, delta: i64) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.counter_add(TABLE, vec![Value::Text(name)], "counter", delta)
    })
    .expect("counter add failed");
    db.flush().expect("flush after counter add");
}

/// OR_SET add (Go: `Cluster.TypedSetAdd`).
fn set_add(db: &Database, name: &str, value: &str) {
    let (name, value) = (name.to_owned(), value.to_owned());
    db.transaction(move |sql| {
        sql.set_add(
            TABLE,
            vec![Value::Text(name)],
            "tags",
            Value::Text(value),
        )
    })
    .expect("set add failed");
    db.flush().expect("flush after set add");
}

/// OR_SET remove (Go: `Cluster.TypedSetRemove`).
fn set_remove(db: &Database, name: &str, value: &str) {
    let (name, value) = (name.to_owned(), value.to_owned());
    db.transaction(move |sql| {
        sql.set_remove(
            TABLE,
            vec![Value::Text(name)],
            "tags",
            Value::Text(value),
        )
    })
    .expect("set remove failed");
    db.flush().expect("flush after set remove");
}

/// Numeric MIN/MAX op (Go: `Cluster.TypedExtremaUpdate`).
fn extrema_update(db: &Database, name: &str, peak: i64, floor: f64) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.max(
            TABLE,
            vec![Value::Text(name.clone())],
            "peak",
            Value::Integer(peak),
        )?;
        sql.min(
            TABLE,
            vec![Value::Text(name)],
            "floor",
            Value::Real(floor),
        )
    })
    .expect("extrema update failed");
    db.flush().expect("flush after extrema update");
}

/// Nullable `note` column write (Go: `Cluster.TypedSetNote`).
fn update_note(db: &Database, name: &str, note: &str) {
    let (name, note) = (name.to_owned(), note.to_owned());
    db.transaction(move |sql| {
        sql.execute(
            "UPDATE live_typed_records SET note = ? WHERE name = ?",
            [note, name],
        )?;
        Ok(())
    })
    .expect("note update failed");
    db.flush().expect("flush after note update");
}

/// Nullable `region` column write (Go: `Cluster.TypedSetRegion`).
fn update_region(db: &Database, name: &str, region: &str) {
    let (name, region) = (name.to_owned(), region.to_owned());
    db.transaction(move |sql| {
        sql.execute(
            "UPDATE live_typed_records SET region = ? WHERE name = ?",
            [region, name],
        )?;
        Ok(())
    })
    .expect("region update failed");
    db.flush().expect("flush after region update");
}

/// Known-field update from the older peer (Go: `Cluster.TypedRename`, applied
/// to the LWW `title` field so the primary-key row identity is preserved).
fn update_title(db: &Database, name: &str, title: &str) {
    let (name, title) = (name.to_owned(), title.to_owned());
    db.transaction(move |sql| {
        sql.execute(
            "UPDATE live_typed_records SET title = ? WHERE name = ?",
            [title, name],
        )?;
        Ok(())
    })
    .expect("title update failed");
    db.flush().expect("flush after title update");
}

/// Row count by title.
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

/// `note` field read (Go: `Cluster.TypedNote`).
fn note_value(db: &Database, name: &str) -> String {
    let name = name.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT note FROM live_typed_records WHERE name = ?",
            [name],
            |r| r.get::<_, Option<String>>(0),
        )?)
    })
    .unwrap_or(None)
    .unwrap_or_else(|| "<missing>".into())
}

/// Merged branch fields read (Go: `Cluster.TypedBranchValues`).
fn branch_values(db: &Database, name: &str) -> (String, String) {
    let name = name.to_owned();
    let (note, region) = db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT note, region FROM live_typed_records WHERE name = ?",
            [name],
            |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, Option<String>>(1)?,
                ))
            },
        )?)
    })
    .unwrap_or((None, None));
    (
        note.unwrap_or_else(|| "<missing>".into()),
        region.unwrap_or_else(|| "<missing>".into()),
    )
}

/// Column presence (Go: schema-manifest epoch assertions).
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

/// Schema-adoption metric (Go: epoch advance on the adopting peer).
fn schema_adoptions(cluster: &Cluster, idx: usize) -> u64 {
    cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics
        .schema_adoptions
}

/// Log-path activity counters `(received, applied)`: snapshot application
/// bypasses both, so a flat pair across a resync proves the snapshot path
/// (established `corrupt-snapshot` port pattern; Go: metrics endpoint).
fn log_counters(cluster: &Cluster, idx: usize) -> (u64, u64) {
    let metrics = cluster
        .node(idx)
        .replication()
        .expect("replication handle")
        .status()
        .metrics;
    (metrics.received_transactions, metrics.applied_transactions)
}

/// Snapshot-leg row count (Go: reconstructed RIME record count).
fn snapshot_row_count(db: &Database) -> i64 {
    db.read(|sql| {
        Ok(sql.query_row(
            "SELECT count(*) FROM live_typed_records WHERE name LIKE 'snapshot-row-%'",
            [],
            |r| r.get::<_, i64>(0),
        )?)
    })
    .unwrap_or(-1)
}

/// Node-local write (Go: `Cluster.TypedLocalInsert`).
fn private_insert(db: &Database, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO typed_local_private(name, v) VALUES(?, 1)",
            [name],
        )?;
        Ok(())
    })
    .expect("node-local insert failed");
    db.flush().expect("flush after node-local insert");
}

/// Node-local row count (Go: `Cluster.TypedLocalCount`).
fn private_count(db: &Database, name: &str) -> i64 {
    let name = name.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT count(*) FROM typed_local_private WHERE name = ?",
            [name],
            |r| r.get::<_, i64>(0),
        )?)
    })
    .unwrap_or(-1)
}

fn wait_count(cluster: &Cluster, node: usize, name: &str, want: i64, what: &str) {
    let converged = wait_until(WAIT, POLL, || typed_count(cluster.node(node), name) == want);
    assert!(
        converged,
        "node{} {what} count for {name:?} = {}, want {want}",
        node + 1,
        typed_count(cluster.node(node), name)
    );
}

fn wait_counter(cluster: &Cluster, node: usize, name: &str, want: i64) {
    let converged = wait_until(WAIT, POLL, || counter_value(cluster.node(node), name) == want);
    assert!(
        converged,
        "node{} counter for {name:?} = {}, want {want}",
        node + 1,
        counter_value(cluster.node(node), name)
    );
}

fn wait_set(cluster: &Cluster, node: usize, name: &str, want: &[&str]) {
    let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
    let converged = wait_until(WAIT, POLL, || set_values(cluster.node(node), name) == want);
    assert!(
        converged,
        "node{} set for {name:?} = {:?}, want {want:?}",
        node + 1,
        set_values(cluster.node(node), name)
    );
}

fn wait_extrema(cluster: &Cluster, node: usize, name: &str, peak: i64, floor: f64) {
    let converged = wait_until(WAIT, POLL, || extrema(cluster.node(node), name) == (peak, floor));
    assert!(
        converged,
        "node{} extrema for {name:?} = {:?}, want ({peak},{floor})",
        node + 1,
        extrema(cluster.node(node), name)
    );
}

fn wait_title_count(cluster: &Cluster, node: usize, title: &str, want: i64) {
    let converged = wait_until(WAIT, POLL, || title_count(cluster.node(node), title) == want);
    assert!(
        converged,
        "node{} title count for {title:?} = {}, want {want}",
        node + 1,
        title_count(cluster.node(node), title)
    );
}

fn wait_note(cluster: &Cluster, node: usize, name: &str, want: &str) {
    let converged = wait_until(WAIT, POLL, || note_value(cluster.node(node), name) == want);
    assert!(
        converged,
        "node{} note for {name:?} = {:?}, want {want:?}",
        node + 1,
        note_value(cluster.node(node), name)
    );
}

fn wait_branch_values(
    cluster: &Cluster,
    node: usize,
    name: &str,
    want_note: &str,
    want_region: &str,
) {
    let converged = wait_until(WAIT, POLL, || {
        branch_values(cluster.node(node), name) == (want_note.to_owned(), want_region.to_owned())
    });
    assert!(
        converged,
        "node{} merged branch values for {name:?} = {:?}, want ({want_note:?},{want_region:?})",
        node + 1,
        branch_values(cluster.node(node), name)
    );
}

/// Fail when `marker` appears in any filesystem path under `root` (Go:
/// `filepath.WalkDir` escape scan after the traversal-shaped round-trip).
fn assert_no_filesystem_escape(root: &std::path::Path, marker: &str) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries =
            std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("scan {}: {e}", dir.display()));
        for entry in entries {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(marker))
            {
                panic!(
                    "typed traversal-shaped name escaped into filesystem path: {}",
                    path.display()
                );
            }
            if entry.file_type().expect("file type").is_dir() {
                stack.push(path);
            }
        }
    }
}

/// Shared cluster setup: two nodes, typed merge policies, manual peers so the
/// local migrate wins over schema auto-adoption (identical DDL converges
/// either way, but peering first would race adoption against `migrate`).
fn typed_cluster(name: &str) -> Cluster {
    Cluster::new(ClusterOptions {
        name: name.into(),
        num_nodes: 2,
        manual_peers: true,
        replication: Some(ReplicationOptions {
            merge_policies: Some(merge_policies()),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup")
}

#[test]
fn typed_record_replicates_and_rebuilds_across_restart() {
    let mut cluster = typed_cluster("typed-records");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, BASE_DDL);
        cluster.mesh().expect("initial mesh");

        // The schema manifest carries the typed table (Go: /v1/schema).
        let schema = cluster.node(0).schema().expect("read schema");
        assert!(
            schema.tables.iter().any(|t| t.name == TABLE),
            "schema manifest lacks the typed table: {:?}",
            schema.tables.iter().map(|t| &t.name).collect::<Vec<_>>()
        );

        // Typed subscription on the peer (Go: StartTypedWatch).
        let sub = cluster
            .node(1)
            .subscribe(
                |sql| {
                    let mut stmt = sql.prepare(
                        "SELECT name FROM live_typed_records WHERE name = 'native-rime-write'",
                    )?;
                    let mut rows = stmt.query([])?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next()? {
                        out.push(row.get::<_, String>(0)?);
                    }
                    Ok(out)
                },
                |name: &String| name.as_bytes().to_vec(),
                SubscriptionOptions {
                    tables: vec![TABLE.into()],
                    ..Default::default()
                },
            )
            .expect("typed subscription");
        match sub
            .recv_timeout(Duration::from_secs(30))
            .expect("initial subscription event")
        {
            SubscriptionEvent::Initial { rows, .. } => {
                assert!(rows.is_empty(), "initial count = {}, want 0", rows.len())
            }
            other => panic!("expected initial subscription event, got {other:?}"),
        }

        insert_row(cluster.node(0), "native-rime-write");

        // Operator-triggered GC (Go: TriggerGC through the mTLS admin API).
        let collected = cluster
            .node(0)
            .collect_retention()
            .expect("operator-triggered GC");
        eprintln!("operator GC collected {collected} transaction records");

        // The peer subscription observes the remote insert (Go: watch.Next).
        let deadline = std::time::Instant::now() + WAIT;
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "peer subscription never observed the replicated row"
            );
            match sub
                .recv_timeout(Duration::from_secs(5))
                .expect("subscription update")
            {
                SubscriptionEvent::Update { rows, .. } if rows.len() == 1 => break,
                SubscriptionEvent::Update { .. } => continue,
                SubscriptionEvent::Reset { reason, .. } => {
                    panic!("subscription reset while waiting for the row: {reason}")
                }
                SubscriptionEvent::Initial { .. } => continue,
            }
        }
        wait_count(&cluster, 1, "native-rime-write", 1, "replicated");

        // An explicit transaction commits while an explicitly rolled-back row
        // stays absent on both nodes (Go: TypedExplicitTransaction).
        insert_row(cluster.node(0), "explicit-lifecycle");
        let rolled_back: Result<(), _> = cluster.node(0).transaction(|sql| {
            sql.execute(
                "INSERT INTO live_typed_records(name, title, counter, tags, peak, floor) VALUES('explicit-lifecycle-rolled-back', '', 0, x'00', 0, 0.0)",
                [],
            )?;
            Err(murmur_live_harness::sqlite_session::Error::Invalid(
                "rollback probe".into(),
            ))
        });
        assert!(
            rolled_back.is_err(),
            "rolled-back transaction unexpectedly committed"
        );
        wait_count(&cluster, 1, "explicit-lifecycle", 1, "explicit-tx");
        assert_eq!(
            typed_count(cluster.node(0), "explicit-lifecycle-rolled-back"),
            0,
            "rolled-back row visible on its writer"
        );
        assert_eq!(
            typed_count(cluster.node(1), "explicit-lifecycle-rolled-back"),
            0,
            "rolled-back row replicated to the peer"
        );

        // PN_COUNTER converges (Go: TypedCounterAdd +20).
        counter_add(cluster.node(0), "native-rime-write", 20);
        wait_counter(&cluster, 1, "native-rime-write", 20);

        // OR_SET add/add/remove converges to {"aa"}.
        set_add(cluster.node(0), "native-rime-write", "z");
        set_add(cluster.node(0), "native-rime-write", "aa");
        set_remove(cluster.node(0), "native-rime-write", "z");
        wait_set(&cluster, 1, "native-rime-write", &["aa"]);

        // Numeric MIN/MAX converge (Go: TypedExtremaUpdate 18/-3).
        extrema_update(cluster.node(0), "native-rime-write", 18, -3.0);
        wait_extrema(&cluster, 1, "native-rime-write", 18, -3.0);

        // The restarted peer rebuilds the record, counter, set, and extrema
        // from durable state (Go: StopNode/StartNode + waits).
        cluster.stop_node(1);
        cluster.start_node(1).expect("restart peer");
        cluster.mesh().expect("re-mesh after restart");
        wait_count(&cluster, 1, "native-rime-write", 1, "post-restart");
        wait_counter(&cluster, 1, "native-rime-write", 20);
        wait_set(&cluster, 1, "native-rime-write", &["aa"]);
        wait_extrema(&cluster, 1, "native-rime-write", 18, -3.0);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("typed-record-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn typed_schema_migration_adopted_by_older_peer_and_retains_new_fields() {
    let mut cluster = typed_cluster("typed-schema-evolution");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, BASE_DDL);
        cluster.mesh().expect("initial mesh");
        insert_row(cluster.node(0), "schema-evolution-row");
        wait_count(&cluster, 1, "schema-evolution-row", 1, "seed");

        // Runtime additive migration on the newer node (Go:
        // MigrateTypedRecords adds the Note field).
        cluster
            .node(0)
            .migrate(Migration::new(
                "ALTER TABLE live_typed_records ADD COLUMN note TEXT",
            ))
            .expect("additive typed migration");
        cluster.node(0).flush().expect("flush migration");

        // The older peer adopts the manifest (Go: epoch 2 on both nodes).
        let adopted = wait_until(WAIT, POLL, || has_column(cluster.node(1), TABLE, "note"));
        assert!(
            adopted,
            "older peer never adopted the additive typed column"
        );
        assert!(
            schema_adoptions(&cluster, 1) >= 1,
            "older peer adopted the column without recording a schema adoption"
        );

        // The new field is written from the newer node while a known field is
        // updated from the older peer (Go: TypedSetNote + TypedRename).
        update_note(cluster.node(0), "schema-evolution-row", "new-field-survives");
        update_title(
            cluster.node(1),
            "schema-evolution-row",
            "schema-evolution-renamed",
        );
        wait_title_count(&cluster, 0, "schema-evolution-renamed", 1);
        wait_title_count(&cluster, 1, "schema-evolution-renamed", 1);
        wait_note(&cluster, 0, "schema-evolution-row", "new-field-survives");
        wait_note(&cluster, 1, "schema-evolution-row", "new-field-survives");

        // The new field survives the older peer's restart (Go:
        // StopNode/StartNode + count/epoch/note waits).
        cluster.stop_node(1);
        cluster.start_node(1).expect("restart older peer");
        cluster.mesh().expect("re-mesh after restart");
        wait_title_count(&cluster, 1, "schema-evolution-renamed", 1);
        assert!(
            has_column(cluster.node(1), TABLE, "note"),
            "restarted peer lost the adopted typed column"
        );
        wait_note(&cluster, 1, "schema-evolution-row", "new-field-survives");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("typed-migration-no-adoption");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn typed_crdt_writes_converge_after_offline_partition() {
    let cluster = typed_cluster("typed-offline-crdt");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, BASE_DDL);

        // Both nodes independently write one shared record while partitioned
        // (Go: TypedInsertWithID with one row id on both nodes).
        insert_row(cluster.node(0), "offline-shared-row");
        insert_row(cluster.node(1), "offline-shared-row");
        counter_add(cluster.node(0), "offline-shared-row", 7);
        counter_add(cluster.node(1), "offline-shared-row", 11);
        set_add(cluster.node(0), "offline-shared-row", "from-node-0");
        set_add(cluster.node(1), "offline-shared-row", "from-node-1");

        cluster.mesh().expect("connect partitions");
        for node in 0..cluster.len() {
            wait_count(&cluster, node, "offline-shared-row", 1, "offline row");
            wait_counter(&cluster, node, "offline-shared-row", 18);
            wait_set(
                &cluster,
                node,
                "offline-shared-row",
                &["from-node-0", "from-node-1"],
            );
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("typed-offline-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn typed_empty_peer_catches_up_through_snapshot() {
    let cluster = Cluster::new(ClusterOptions {
        name: "typed-snapshot-catchup".into(),
        num_nodes: 2,
        node_ids: vec![[1; 16], [2; 16]],
        manual_peers: true,
        replication: Some(ReplicationOptions {
            retention: Some(Duration::from_millis(1)),
            minimum_retained: Some(1),
            ..Default::default()
        }),
        // The empty receiver trusts the source as its snapshot donor (Go
        // fixture policy trusts all nodes; the harness trusts nobody).
        trusted_snapshot_sources: HashMap::from([(1usize, BTreeSet::from([[1; 16]]))]),
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, BASE_DDL);
        for i in 0..12 {
            insert_row(cluster.node(0), &format!("snapshot-row-{i:02}"));
        }
        // Let the configured log retention elapse, then collect old batches
        // before the empty peer is admitted, so the receiver must use a
        // snapshot rather than ordinary log catch-up (Go: sleep + TriggerGC).
        std::thread::sleep(Duration::from_millis(1100));
        let pruned = cluster
            .node(0)
            .collect_retention()
            .expect("pre-admission source GC");
        eprintln!("pre-admission GC pruned {pruned} transaction records");
        let before = log_counters(&cluster, 1);
        cluster.mesh().expect("admit empty peer");
        let converged = wait_until(WAIT, POLL, || snapshot_row_count(cluster.node(1)) == 12);
        assert!(
            converged,
            "empty peer reconstructed {} of 12 typed rows",
            snapshot_row_count(cluster.node(1))
        );
        // Flat log counters prove snapshot transfer rather than log catch-up
        // (Go: snapshots-received metric >= 1; no such counter exists in the
        // Rust product, so the established flat-counters proof stands in).
        let after = log_counters(&cluster, 1);
        assert_eq!(
            before, after,
            "receiver advanced via log catch-up {before:?} -> {after:?}, want flat counters proving snapshot transfer"
        );
        assert!(
            cluster.node(1).status().committed > 0,
            "receiver commit cursor never advanced past the snapshot"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("typed-snapshot-no-catchup");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn typed_concurrent_schema_branches_merge_across_partition() {
    let cluster = typed_cluster("typed-schema-branches");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, BASE_DDL);
        cluster.mesh().expect("initial mesh");
        insert_row(cluster.node(0), "branched-record");
        wait_count(&cluster, 1, "branched-record", 1, "seed");

        cluster.remove_peer(0, 1).expect("partition node 0");
        cluster.remove_peer(1, 0).expect("partition node 1");
        // Each node adds a different compatible field while partitioned (Go:
        // MigrateTypedRecords Note branch vs Region branch).
        cluster
            .node(0)
            .migrate(Migration::new(
                "ALTER TABLE live_typed_records ADD COLUMN note TEXT",
            ))
            .expect("Note schema branch");
        cluster.node(0).flush().expect("flush Note branch");
        cluster
            .node(1)
            .migrate(Migration::new(
                "ALTER TABLE live_typed_records ADD COLUMN region TEXT",
            ))
            .expect("Region schema branch");
        cluster.node(1).flush().expect("flush Region branch");
        // Independent branches before reconnect (Go: /v1/schema manifests).
        assert!(
            has_column(cluster.node(0), TABLE, "note")
                && !has_column(cluster.node(0), TABLE, "region"),
            "node 1 did not hold its independent Note branch"
        );
        assert!(
            has_column(cluster.node(1), TABLE, "region")
                && !has_column(cluster.node(1), TABLE, "note"),
            "node 2 did not hold its independent Region branch"
        );
        update_note(cluster.node(0), "branched-record", "note-from-node-0");
        update_region(cluster.node(1), "branched-record", "region-from-node-1");

        cluster.mesh().expect("rejoin");
        // Both nodes publish the merged schema (Go: epoch 3 on both nodes;
        // the Rust legacy mode mutually adopts additive manifests).
        for node in 0..cluster.len() {
            let merged = wait_until(WAIT, POLL, || {
                has_column(cluster.node(node), TABLE, "note")
                    && has_column(cluster.node(node), TABLE, "region")
            });
            assert!(
                merged,
                "node{} never converged on the merged typed schema",
                node + 1
            );
        }
        // Both field values survive on the union record type (Go: V4 bind +
        // branch-value waits; the Rust product has no record-type binding, so
        // the union columns themselves are the assertion surface).
        for node in 0..cluster.len() {
            wait_branch_values(
                &cluster,
                node,
                "branched-record",
                "note-from-node-0",
                "region-from-node-1",
            );
        }
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("typed-branches-no-merge");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn typed_files_replicate_and_fetch_without_sql() {
    let cluster = Cluster::new(ClusterOptions {
        name: "typed-files".into(),
        num_nodes: 2,
        files: Some(FilesOptions {
            key: [0x44; 32],
            fetch_interval: Some(Duration::from_millis(100)),
            max_file_bytes: Some(1 << 20),
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let name = "typed/report.bin";
        let want: &[u8] = b"file metadata uses Spool while bytes are fetched from the peer";
        cluster
            .node(0)
            .upload_file(name, want)
            .expect("typed node upload");
        // Spool metadata replicates first (Go: WaitFileAvailable meta=false).
        let metadata = wait_until(WAIT, POLL, || {
            cluster
                .node(1)
                .file_metadata(name)
                .unwrap_or(None)
                .is_some()
        });
        assert!(metadata, "replicated typed file metadata never arrived");
        // The content-addressed object is fetched from the peer (Go: explicit
        // FetchFile; the Rust product fetches automatically on fetch_interval).
        let object = wait_until(WAIT, POLL, || {
            cluster.node(1).file_available(name).unwrap_or(false)
        });
        assert!(object, "typed file object never became available");
        let mut got = Vec::new();
        cluster
            .node(1)
            .get_file(name, &mut got)
            .expect("peer download");
        assert_eq!(got.as_slice(), want, "typed peer download bytes differ");

        // A typed-node delete tombstone replicates (Go: WaitFileDeleted).
        cluster.node(0).delete_file(name).expect("typed file delete");
        let deleted = wait_until(WAIT, POLL, || {
            cluster
                .node(1)
                .file_metadata(name)
                .unwrap_or(None)
                .is_none()
        });
        assert!(deleted, "replicated typed file tombstone never arrived");

        // Traversal probes are rejected or treated as opaque object keys; none
        // escape into the filesystem (Go: download/status/search probes).
        for traversal in [
            "../../../../etc/passwd",
            "..\\..\\windows\\win.ini",
            "%2e%2e%2fetc%2fpasswd",
            "/etc/passwd",
            "typed/report.bin/../../etc/passwd",
            "..",
        ] {
            let mut probe = Vec::new();
            if cluster.node(0).get_file(traversal, &mut probe).is_ok() {
                assert!(
                    probe.is_empty(),
                    "traversal download {traversal:?} returned {} bytes",
                    probe.len()
                );
            }
            let meta = cluster
                .node(0)
                .file_metadata(traversal)
                .expect("traversal metadata probe");
            assert!(
                meta.is_none(),
                "traversal status {traversal:?} reports an object: {meta:?}"
            );
            let hits = cluster
                .node(0)
                .search_files(traversal)
                .expect("traversal search probe");
            assert!(
                !hits.iter().any(|hit| hit.name == traversal),
                "traversal search {traversal:?} reports an object"
            );
            let _ = cluster.node(0).delete_file(traversal);
        }

        // A traversal-shaped name round-trips as an opaque key, then deletes.
        let marker = "typed-file-traversal-marker";
        let traversal_name = format!("../../{marker}");
        let traversal_bytes: &[u8] = b"typed file names stay opaque object keys";
        cluster
            .node(0)
            .upload_file(&traversal_name, traversal_bytes)
            .expect("upload traversal-shaped typed file name");
        let mut roundtrip = Vec::new();
        cluster
            .node(0)
            .get_file(&traversal_name, &mut roundtrip)
            .expect("traversal-shaped round-trip");
        assert_eq!(
            roundtrip.as_slice(),
            traversal_bytes,
            "traversal-shaped typed file round-trip differs"
        );
        cluster
            .node(0)
            .delete_file(&traversal_name)
            .expect("delete traversal-shaped typed file name");
        assert_no_filesystem_escape(cluster.runtime_dir(), marker);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("typed-files-no-fetch");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn typed_node_local_rows_persist_only_on_their_node() {
    let cluster = Cluster::new(ClusterOptions {
        name: "typed-node-local".into(),
        num_nodes: 2,
        manual_peers: true,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, BASE_DDL);
        migrate_all(&cluster, PRIVATE_DDL);
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .set_replication("typed_local_private", false)
                .unwrap_or_else(|e| {
                    panic!("node{} disable private replication: {e}", i + 1)
                });
        }
        cluster.mesh().expect("initial mesh");

        // A node-local write plus a replicated barrier row (Go:
        // TypedLocalInsert + TypedInsert).
        private_insert(cluster.node(0), "private-node-zero");
        insert_row(cluster.node(0), "replication-barrier");
        wait_count(&cluster, 1, "replication-barrier", 1, "barrier");
        assert_eq!(
            private_count(cluster.node(0), "private-node-zero"),
            1,
            "local node's private row missing"
        );
        assert_eq!(
            private_count(cluster.node(1), "private-node-zero"),
            0,
            "peer received the node-local row"
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("typed-node-local-leak");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
