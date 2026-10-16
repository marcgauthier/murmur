//! Rust port of the Go `backup-restore` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/backup-restore/`
//! (`README.md` + `backup_restore_test.go`:
//! `TestBackupRestoreRejoinMesh`, `TestRestoreRejectsStaleIdentity`,
//! `TestTypedBackupRestoreRoundTripLive`). Three `#[test]` fns (one cluster
//! each, distinct `runtime/` names so parallel execution stays isolated):
//!
//! - `backup_restore_rejoin_mesh`: three nodes converge on 20 rows, node3
//!   stops, its data dir is backed up offline, wiped (total loss), and
//!   restored under a fresh writer identity; node3 restarts on the restored
//!   data with a reissued TLS certificate and fresh origin key, the mesh
//!   reconverges with no lost rows, and the restarted node's identity is
//!   the fresh one.
//! - `restore_rejects_stale_identity`: rollback safety — opening restored
//!   data under the old NodeID is rejected, while the fresh identity opens
//!   cleanly.
//! - `typed_backup_restore_round_trip_live`: a typed row (plain field plus
//!   PN_COUNTER / OR_SET / MAX / MIN policy ops) survives
//!   backup → fresh-identity restore → reopen with identical materialized
//!   values.
//!
//! Plain SQL plus `policy.rs` ops replace the Go typed-records HTTP API (the
//! Rust product has no HTTP server); stop/start replaces StopNode/StartNode
//! + unlock; the `/metrics` identity assertion becomes `Database::node_id`.
//! See the scenario README.md for the full deviation list.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use murmur_live_harness::murmur_replication::policies::Policy;
use murmur_live_harness::sqlite_session::{
    Database, Migration, OpenMode, Options, RestoreOptions, Value,
};
use murmur_live_harness::{
    wait_until, Cluster, ClusterOptions, ReplicationOptions,
};
use rand::RngCore;

/// Typed table (Go: `live_typed_records` managed typed table).
const TABLE: &str = "live_typed_records";
/// Mesh-leg DDL: plain replicated rows (Go: `TypedInsert` names).
const NAMES_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY)";
/// Typed-leg DDL: plain field plus counter/set/extrema policy columns (the
/// `typed-records` port's table shape). Set projections start as `x'00'`
/// (postcard empty vec), the `tests/` convention for fresh set cells.
const TYPED_DDL: &str = "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)";
/// Seed rows and convergence deadlines (Go: 20 rows; 30 s, then 90 s).
const NUM_ROWS: usize = 20;
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);
const RECONVERGE_TIMEOUT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);

/// (table, column) -> merge policy for the typed leg (Go: PN_COUNTER,
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
fn typed_insert(db: &Database, name: String) {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_typed_records(name) VALUES(?)",
            [name],
        )?;
        Ok(())
    })
    .expect("typed insert failed");
    db.flush().expect("flush after typed insert");
}

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
fn typed_names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM live_typed_records ORDER BY name")?;
        let mapped = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in mapped {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read typed names")
}

/// Wait until every running node holds the same `want` names (Go:
/// `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let ok = wait_until(timeout, POLL, || {
        let mut first: Option<String> = None;
        for i in 0..cluster.len() {
            let names = typed_names(cluster.node(i));
            if names.len() != want {
                return false;
            }
            let joined = names.join("\x00");
            match &first {
                None => first = Some(joined),
                Some(want_joined) if *want_joined == joined => {}
                Some(_) => return false,
            }
        }
        true
    });
    assert!(
        ok,
        "nodes did not converge on {want} rows within {timeout:?}"
    );
}

/// Open a stopped node's data dir without replication (Go: `openNodeDir`
/// offline library open for backup/restore legs).
fn open_offline(
    data_dir: &Path,
    key: [u8; 32],
    database_id: [u8; 16],
    node_id: [u8; 16],
) -> Database {
    Database::open(
        data_dir,
        key,
        Options {
            mode: OpenMode::Existing,
            database_id: Some(database_id),
            node_id: Some(node_id),
            ..Options::default()
        },
    )
    .expect("offline open of node data dir")
}

/// Back up a stopped node's data dir through an offline open (Go:
/// `offline.Backup`), returning the backup directory.
fn backup_stopped_node(
    cluster: &Cluster,
    key: [u8; 32],
    idx: usize,
    dest_name: &str,
) -> std::path::PathBuf {
    let info = cluster.node_info(idx);
    let offline = open_offline(
        &info.data_dir,
        key,
        cluster.database_id(),
        info.node_id,
    );
    let backup_dir = cluster.runtime_dir().join(dest_name);
    offline
        .backup(&backup_dir)
        .expect("offline backup of stopped node");
    offline.close().expect("close offline handle");
    backup_dir
}

/// Restore `backup_dir` over a wiped data dir under a fresh writer identity
/// (Go: `RemoveAll(data)` + `backup.Restore` with `RestoreClone`).
fn restore_fresh_identity(
    backup_dir: &Path,
    target_data_dir: &Path,
    key: [u8; 32],
    fresh: [u8; 16],
) {
    std::fs::remove_dir_all(target_data_dir).expect("wipe data dir (total loss)");
    Database::restore_with_options(
        backup_dir,
        target_data_dir,
        key,
        RestoreOptions {
            new_database_id: None,
            new_node_id: Some(fresh),
        },
    )
    .expect("restore backup under fresh identity");
}

#[test]
fn backup_restore_rejoin_mesh() {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "backup-restore".into(),
        num_nodes: 3,
        key,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, NAMES_DDL);
        for i in 0..NUM_ROWS {
            typed_insert(cluster.node(0), format!("backup-row-{i:02}"));
        }
        wait_converged(&cluster, NUM_ROWS, CONVERGE_TIMEOUT);

        // Offline backup of the stopped node through the library.
        let old_id = cluster.node_id(2);
        cluster.stop_node(2);
        let backup_dir = backup_stopped_node(&cluster, key, 2, "node3-backup");

        // Total loss of node3's durable data, then fresh-identity restore
        // into the same location the node will reopen.
        let fresh: [u8; 16] = rand::random();
        assert_ne!(fresh, old_id, "fresh identity must differ");
        let data_dir = cluster.node_info(2).data_dir.clone();
        restore_fresh_identity(&backup_dir, &data_dir, key, fresh);

        // Restart on the restored data with a reissued TLS certificate and
        // fresh origin key; the mesh must reconverge with no lost rows.
        cluster
            .restart_node_with_identity(2, fresh)
            .expect("restart node3 under fresh identity");
        cluster.mesh().expect("re-mesh after restart");
        wait_converged(&cluster, NUM_ROWS, RECONVERGE_TIMEOUT);
        assert_eq!(
            cluster.node(2).node_id(),
            fresh,
            "node3 identity must be the fresh one"
        );
        assert_eq!(cluster.node_id(2), fresh, "harness node id must track");

        // Post-rejoin write from a survivor proves the restored node is a
        // live replication peer again (its fresh origin key is unknown to
        // the survivors, so it must not originate yet — see
        // `restart_node_with_identity`).
        typed_insert(cluster.node(0), "backup-row-post-rejoin".to_string());
        wait_converged(&cluster, NUM_ROWS + 1, RECONVERGE_TIMEOUT);
        eprintln!("PASS: backup-restore rejoin reconverged with no lost rows");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("backup-restore-rejoin");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

#[test]
fn restore_rejects_stale_identity() {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "backup-restore-id".into(),
        num_nodes: 1,
        key,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, NAMES_DDL);
        typed_insert(cluster.node(0), "backup-stale-identity".to_string());
        let old_id = cluster.node_id(0);
        cluster.stop_node(0);
        let backup_dir = backup_stopped_node(&cluster, key, 0, "node1-backup");

        let staging = cluster.runtime_dir().join("staging");
        let fresh: [u8; 16] = rand::random();
        assert_ne!(fresh, old_id, "fresh identity must differ");
        Database::restore_with_options(
            &backup_dir,
            &staging,
            key,
            RestoreOptions {
                new_database_id: None,
                new_node_id: Some(fresh),
            },
        )
        .expect("restore backup under fresh identity");

        // Opening under the OLD identity must be rejected (rollback safety).
        let bad = Database::open(
            &staging,
            key,
            Options {
                mode: OpenMode::Existing,
                database_id: Some(cluster.database_id()),
                node_id: Some(old_id),
                ..Options::default()
            },
        );
        match bad {
            Ok(stale) => {
                let _ = stale.close();
                panic!("open under stale identity succeeded, want rejection");
            }
            Err(e) => eprintln!("stale identity rejected: {e}"),
        }

        // Opening under the fresh identity succeeds with the row intact.
        let restored = Database::open(
            &staging,
            key,
            Options {
                mode: OpenMode::Existing,
                database_id: Some(cluster.database_id()),
                node_id: Some(fresh),
                ..Options::default()
            },
        )
        .expect("open under fresh identity");
        assert_eq!(restored.node_id(), fresh);
        assert_eq!(
            typed_names(&restored),
            vec!["backup-stale-identity".to_string()]
        );
        restored.close().expect("close restored handle");
        eprintln!("PASS: stale identity rejected, fresh identity opens");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("backup-restore-stale-id");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}

/// Typed-leg insert (Go: `Cluster.TypedInsert` on the typed record table).
fn typed_row_insert(db: &Database, name: &str) {
    let name = name.to_owned();
    // Extrema baselines must sit outside the op range: max raises 0 -> 7
    // and min lowers 9.5 -> 1.5. A 0.0 floor baseline would be a real
    // minimum (min over {0.0, 1.5, 4.5} is 0.0), masking the min ops.
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_typed_records(name, title, counter, tags, peak, floor) VALUES(?, '', 0, x'00', 0, 9.5)",
            [name],
        )?;
        Ok(())
    })
    .expect("typed row insert failed");
    db.flush().expect("flush after typed row insert");
}

/// PN_COUNTER op (Go: counter field write on the typed record).
fn counter_add(db: &Database, name: &str, delta: i64) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.counter_add(TABLE, vec![Value::Text(name)], "counter", delta)
    })
    .expect("counter add failed");
    db.flush().expect("flush after counter add");
}

/// OR_SET add (Go: tags field write on the typed record).
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

/// Numeric MIN/MAX op (Go: peak/floor field writes on the typed record).
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

/// Full typed projection (Go: RIME materializer read-back of the record).
fn typed_projection(db: &Database, name: &str) -> (String, i64, Vec<String>, i64, f64) {
    let name = name.to_owned();
    let (title, counter, tags, peak, floor) = db
        .read(move |sql| {
            Ok(sql.query_row(
                "SELECT title, counter, tags, peak, floor FROM live_typed_records WHERE name = ?",
                [name],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, f64>(4)?,
                    ))
                },
            )?)
        })
        .expect("read typed projection");
    let values: Vec<Value> =
        murmur_live_harness::murmur_replication::decode(&tags).unwrap_or_default();
    let mut set: Vec<String> = values
        .into_iter()
        .filter_map(|v| match v {
            Value::Text(s) => Some(s),
            _ => None,
        })
        .collect();
    set.sort();
    (title, counter, set, peak, floor)
}

#[test]
fn typed_backup_restore_round_trip_live() {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "typed-backup-restore".into(),
        num_nodes: 1,
        key,
        replication: Some(ReplicationOptions {
            merge_policies: Some(merge_policies()),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, TYPED_DDL);
        typed_row_insert(cluster.node(0), "encrypted-live-backup");
        counter_add(cluster.node(0), "encrypted-live-backup", 20);
        counter_add(cluster.node(0), "encrypted-live-backup", 22);
        set_add(cluster.node(0), "encrypted-live-backup", "z");
        set_add(cluster.node(0), "encrypted-live-backup", "aa");
        extrema_update(cluster.node(0), "encrypted-live-backup", 7, 1.5);
        extrema_update(cluster.node(0), "encrypted-live-backup", 3, 4.5);
        let want = typed_projection(cluster.node(0), "encrypted-live-backup");
        assert_eq!(want.1, 42, "counter must accumulate before backup");
        assert_eq!(want.2, vec!["aa".to_string(), "z".to_string()]);
        assert_eq!((want.3, want.4), (7, 1.5));

        cluster.stop_node(0);
        let backup_dir = backup_stopped_node(&cluster, key, 0, "typed-backup");

        let fresh: [u8; 16] = rand::random();
        let target = cluster.runtime_dir().join("restored");
        Database::restore_with_options(
            &backup_dir,
            &target,
            key,
            RestoreOptions {
                new_database_id: None,
                new_node_id: Some(fresh),
            },
        )
        .expect("restore typed backup under fresh identity");

        let clone = Database::open(
            &target,
            key,
            Options {
                mode: OpenMode::Existing,
                database_id: Some(cluster.database_id()),
                node_id: Some(fresh),
                ..Options::default()
            },
        )
        .expect("open restored typed backup under fresh identity");
        let got = typed_projection(&clone, "encrypted-live-backup");
        assert_eq!(got, want, "restored typed record must match original");
        clone.close().expect("close restored handle");
        eprintln!("PASS: typed backup-restore round trip preserved the record");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("typed-backup-restore");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
