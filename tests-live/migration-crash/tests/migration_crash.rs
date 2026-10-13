//! Rust port of the Go `migration-crash` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/migration-crash/`
//! (`migration_crash_test.go`). An isolated node recovers from a crash at
//! the schema-manifest store boundary while its peers stay on the old
//! epoch; afterwards it rejoins, the peers adopt the new schema, and a
//! migrated-field write converges everywhere. Two cases: `before-store`
//! (recovery reopens old and retries the migration) and `after-store`
//! (recovery reopens already migrated). Plain SQL replaces the Go
//! typed-records HTTP API, and in-process close/reopen replaces the
//! mid-migration process exit (see the scenario README.md for deviations).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Base table DDL (Go: typed records with a Name field).
const TABLE: &str = "live_typed_records";
const TABLE_DDL: &str =
    "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)";
/// Additive migration DDL (Go: `MigrateTypedRecords` registers Note).
const NOTE_DDL: &str = "ALTER TABLE live_typed_records ADD COLUMN note TEXT";
/// Seeded rows (Go: `seedRows = 24`).
const SEED_ROWS: usize = 24;
/// Seed convergence deadline (Go: 45 s).
const SEED_TIMEOUT: Duration = Duration::from_secs(45);
/// Post-rejoin convergence deadline (Go: 90 s).
const REJOIN_TIMEOUT: Duration = Duration::from_secs(90);
/// Peer-adoption deadline (Go: 45 s `waitEpoch`).
const EPOCH_TIMEOUT: Duration = Duration::from_secs(45);
/// Final epoch+field convergence deadline (Go: 45 s).
const FINAL_TIMEOUT: Duration = Duration::from_secs(45);
/// Poll interval (Go: 250 ms).
const POLL: Duration = Duration::from_millis(250);

/// Canonical ordered names on one node (Go: `Cluster.TypedNames`).
fn names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM live_typed_records ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in rows {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read names failed")
}

/// Insert one record (Go: `Cluster.TypedInsertWithID`).
fn insert_row(db: &Database, node: usize, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_typed_records(name, counter) VALUES(?, 0)",
            [name],
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
}

/// Write the migrated Note field (Go: `Cluster.TypedSetNote`).
fn set_note(db: &Database, node: usize, name: &str, note: &str) {
    let (name, note) = (name.to_owned(), note.to_owned());
    db.transaction(move |sql| {
        let changed = sql.execute(
            "UPDATE live_typed_records SET note = ? WHERE name = ?",
            (note, name),
        )?;
        assert_eq!(changed, 1, "node{node} set note hit {changed} rows");
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} set note failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after set note failed: {e}"));
}

/// Column presence (Go: schema-manifest epoch assertions; established
/// `typed-records` port pattern). Absent = epoch 1, present = epoch 2.
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

/// Schema epoch of one node (Go: `schema_epoch` in `/v1/status`).
fn epoch(cluster: &Cluster, idx: usize) -> u64 {
    if has_column(cluster.node(idx), TABLE, "note") {
        2
    } else {
        1
    }
}

/// Require an exact epoch (Go: `assertEpoch`).
fn assert_epoch(cluster: &Cluster, idx: usize, want: u64) {
    let got = epoch(cluster, idx);
    assert_eq!(
        got,
        want,
        "node{} schema epoch = {got}, want {want}",
        idx + 1
    );
}

/// Note read that surfaces SQL errors (Go: `TypedNote`).
fn try_read_note(db: &Database, name: &str) -> Result<Option<String>, String> {
    let name = name.to_owned();
    db.read(move |sql| {
        let mut stmt = sql.prepare("SELECT note FROM live_typed_records WHERE name = ?")?;
        let mut rows = stmt.query([name])?;
        match rows.next()? {
            Some(row) => Ok(row.get::<_, Option<String>>(0)?),
            None => Ok(None),
        }
    })
    .map_err(|e| format!("{e:?}"))
}

/// Apply the additive migration unless the column is already present
/// (Go `MigrateTypedRecords` is an idempotent manifest bind/rebind; Rust
/// DDL errors on a duplicate column, so adoption is checked first).
fn migrate_unless_adopted(cluster: &Cluster, idx: usize) {
    if has_column(cluster.node(idx), TABLE, "note") {
        return;
    }
    cluster
        .node(idx)
        .migrate(Migration::new(NOTE_DDL))
        .unwrap_or_else(|e| panic!("migrate node{}: {e:?}", idx + 1));
    cluster
        .node(idx)
        .flush()
        .unwrap_or_else(|e| panic!("flush migration on node{}: {e:?}", idx + 1));
}

/// Wait until every node holds exactly `want` (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: &[String], timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        (0..cluster.len()).all(|i| names(cluster.node(i)) == want)
    });
    assert!(
        converged,
        "nodes did not converge on {} managed record names within {timeout:?}",
        want.len()
    );
}

/// Wait until the listed nodes adopt `want` (Go: `waitEpoch`).
fn wait_epoch(cluster: &Cluster, nodes: &[usize], want: u64, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        nodes.iter().all(|i| epoch(cluster, *i) == want)
    });
    assert!(
        converged,
        "nodes {nodes:?} did not adopt schema epoch {want} within {timeout:?}"
    );
}

/// One crash-boundary case (Go: `runSchemaCrashCase`). When
/// `migrate_before_restart` is false the isolated node closes before the
/// store (recovery reopens at epoch 1 and retries); when true it stores
/// the migration first (recovery reopens at epoch 2).
fn run_schema_crash_case(phase: &str, migrate_before_restart: bool) {
    let mut cluster = Cluster::new(ClusterOptions {
        name: format!("migration-crash-{phase}"),
        num_nodes: 3,
        ..Default::default()
    })
    .expect("cluster setup");
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        eprintln!(
            "{} dir={} repl={}",
            info.label,
            info.dir.display(),
            info.repl_addr
        );
    }
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Same base schema on every node (Go: typed fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Seed on node 2, converge, all old.
        let mut want_names = Vec::with_capacity(SEED_ROWS);
        for i in 0..SEED_ROWS {
            let name = format!("seed-{i:02}");
            insert_row(cluster.node(1), 2, &name);
            want_names.push(name);
        }
        want_names.sort();
        wait_converged(&cluster, &want_names, SEED_TIMEOUT);
        for idx in 0..cluster.len() {
            assert_epoch(&cluster, idx, 1);
        }

        // Keep the two old-schema peers connected to each other while
        // isolating node 1 so neither can observe its recovery early.
        for peer in [1, 2] {
            cluster
                .remove_peer(0, peer)
                .unwrap_or_else(|e| panic!("isolate node1 from node{}: {e:?}", peer + 1));
            cluster
                .remove_peer(peer, 0)
                .unwrap_or_else(|e| panic!("isolate node{} from node1: {e:?}", peer + 1));
        }

        // Crash at the schema-manifest store boundary: Rust `migrate()` is
        // a synchronous call with no crash-hook boundary, so the two cases
        // close/reopen on either side of the synchronous store (see README).
        if migrate_before_restart {
            migrate_unless_adopted(&cluster, 0);
        }
        cluster.stop_node(0);
        cluster.start_node(0).expect("restart node1");
        let restart_epoch = if migrate_before_restart { 2 } else { 1 };
        assert_epoch(&cluster, 0, restart_epoch);
        for idx in [1, 2] {
            assert_epoch(&cluster, idx, 1);
        }
        eprintln!("node1 recovered at schema manifest {phase} boundary while both peers remained on epoch 1");

        // Recovered rows readable before reconnect.
        let got = names(cluster.node(0));
        assert_eq!(got, want_names, "recovered rows = {got:?}, want {want_names:?}");
        if restart_epoch == 1 {
            migrate_unless_adopted(&cluster, 0);
        }
        assert_epoch(&cluster, 0, 2);
        match try_read_note(cluster.node(0), &want_names[0]) {
            Ok(None) => {}
            other => panic!(
                "new field after local recovery = {other:?}; want empty (NULL) default"
            ),
        }

        // Rejoin and converge.
        for peer in [1, 2] {
            cluster
                .add_peer(0, peer)
                .unwrap_or_else(|e| panic!("rejoin node1 to node{}: {e:?}", peer + 1));
            cluster
                .add_peer(peer, 0)
                .unwrap_or_else(|e| panic!("rejoin node{} to node1: {e:?}", peer + 1));
        }
        wait_converged(&cluster, &want_names, REJOIN_TIMEOUT);
        wait_epoch(&cluster, &[1, 2], 2, EPOCH_TIMEOUT);
        for idx in [1, 2] {
            migrate_unless_adopted(&cluster, idx);
            assert!(
                has_column(cluster.node(idx), TABLE, "note"),
                "node{} did not rebind to the adopted v2 schema",
                idx + 1
            );
        }

        // A migrated-field write converges with both peers at epoch 2.
        set_note(cluster.node(0), 1, &want_names[0], &format!("{phase}-recovered"));
        let want_note = format!("{phase}-recovered");
        let mut last_state = String::new();
        let converged = wait_until(FINAL_TIMEOUT, POLL, || {
            let mut states = Vec::new();
            let mut ready = true;
            for idx in 0..cluster.len() {
                let ep = epoch(&cluster, idx);
                let note = try_read_note(cluster.node(idx), &want_names[0]);
                states.push(format!("node{} epoch={ep} note={note:?}", idx + 1));
                let note_ok = matches!(&note, Ok(Some(v)) if v == &want_note);
                if ep != 2 || !note_ok {
                    ready = false;
                }
            }
            last_state = format!("{states:?}");
            ready
        });
        assert!(
            converged,
            "schema epoch and migrated field did not converge after {phase} crash: {last_state}"
        );
        eprintln!("{phase} schema-store crash recovered and converged with both peers");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics(&format!("migration-crash-{phase}-no-convergence"));
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}

#[test]
fn migrated_peer_crash_before_store_reopens_and_converges() {
    run_schema_crash_case("before-store", false);
}

#[test]
fn migrated_peer_crash_after_store_reopens_and_converges() {
    run_schema_crash_case("after-store", true);
}
