//! Rust port of the Go `schema-evolution` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/schema-evolution/`
//! (`schema_evolution_test.go`). Only node 0 registers the additive Note
//! field; its peers adopt the replicated schema while an older binding
//! updates its known Name field after adoption without erasing the new Note
//! field. Late inserts, CRDT counters, and restart reconstruction are all
//! verified. Plain SQL replaces the Go typed-records HTTP API (see the
//! scenario README.md for the deviations).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Base table DDL (Go: typed records with a Name and a PN counter).
const TABLE: &str = "live_typed_records";
const TABLE_DDL: &str =
    "CREATE TABLE live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)";
/// Additive migration DDL, applied on node 0 only (Go: `MigrateTypedRecords`
/// registers the Note field; the manifest replicates to the peers).
const NOTE_DDL: &str = "ALTER TABLE live_typed_records ADD COLUMN note TEXT";
/// Seeded rows (Go: `rows = 5`).
const ROWS: usize = 5;
/// Name-convergence deadline (Go: 30 s `waitNames`).
const NAMES_TIMEOUT: Duration = Duration::from_secs(30);
/// Name poll interval (Go: 50 ms).
const NAMES_POLL: Duration = Duration::from_millis(50);
/// Local-epoch wait (Go: 10 s `waitEpoch` on the migrating node).
const EPOCH_LOCAL_TIMEOUT: Duration = Duration::from_secs(10);
/// Peer-adoption wait (Go: 30 s `waitEpoch` on each peer).
const EPOCH_PEER_TIMEOUT: Duration = Duration::from_secs(30);
/// Field wait deadline (Go: 30 s `waitNote`/`waitCounter`).
const FIELD_TIMEOUT: Duration = Duration::from_secs(30);

/// Seed row name (Go: `typed-row-%d`).
fn row_name(i: usize) -> String {
    format!("typed-row-{i}")
}

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

/// Insert one record with a zero counter (Go: `Cluster.TypedInsert`).
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

/// Counter add on one record (Go: `Cluster.TypedCounterAdd`).
fn counter_add(db: &Database, node: usize, name: &str, delta: i64) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        let changed = sql.execute(
            "UPDATE live_typed_records SET counter = counter + ? WHERE name = ?",
            (delta, name),
        )?;
        assert_eq!(changed, 1, "node{node} counter add hit {changed} rows");
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} counter add failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after counter add failed: {e}"));
}

/// Write the added Note field (Go: `Cluster.TypedSetNote`).
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

/// Rename a record from an older binding (Go: `Cluster.TypedRename`).
/// Ordinary transactions cannot update primary keys, so the rename copies
/// the row's fields under the new name (delete + insert, one transaction).
/// Callers must barrier on the fields first (Go rename preserves the record
/// id, so concurrent field writes follow it; the copy only sees local state).
fn rename_row(db: &Database, node: usize, old: &str, new: &str) {
    let (old, new) = (old.to_owned(), new.to_owned());
    let old_query = old.clone();
    let (counter, note): (i64, Option<String>) = db
        .read(move |sql| {
            Ok(sql.query_row(
                "SELECT counter, note FROM live_typed_records WHERE name = ?",
                [old_query],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?)),
            )?)
        })
        .unwrap_or_else(|e| panic!("node{node} rename read failed: {e}"));
    db.transaction(move |sql| {
        let deleted = sql.execute(
            "DELETE FROM live_typed_records WHERE name = ?",
            [&old],
        )?;
        assert_eq!(deleted, 1, "node{node} rename deleted {deleted} rows");
        sql.execute(
            "INSERT INTO live_typed_records(name, counter, note) VALUES(?, ?, ?)",
            (new, counter, note),
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} rename failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after rename failed: {e}"));
}

/// Column presence (Go: schema-manifest epoch assertions; established
/// `typed-records` port pattern).
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

/// Wait until every node holds exactly `want` (Go: `waitNames`).
fn wait_names(cluster: &Cluster, want: &[&str]) {
    let mut last = vec![String::new(); cluster.len()];
    let converged = wait_until(NAMES_TIMEOUT, NAMES_POLL, || {
        for i in 0..cluster.len() {
            let got = names(cluster.node(i));
            last[i] = format!("node{} names={got:?}", i + 1);
            if got.iter().map(String::as_str).collect::<Vec<_>>() != want {
                return false;
            }
        }
        true
    });
    assert!(
        converged,
        "typed records did not converge to {want:?}: {last:?}"
    );
}

/// Wait until `node` adopts the additive column (Go: `waitEpoch`).
fn wait_epoch(cluster: &Cluster, node: usize, timeout: Duration) {
    let adopted = wait_until(timeout, NAMES_POLL, || {
        has_column(cluster.node(node), TABLE, "note")
    });
    assert!(
        adopted,
        "node{} never adopted the additive note column within {timeout:?}",
        node + 1
    );
}

/// Wait until the Note field reads `want` (Go: `waitNote`).
fn wait_note(cluster: &Cluster, node: usize, name: &str, want: &str) {
    let (name, want) = (name.to_owned(), want.to_owned());
    let matched = wait_until(FIELD_TIMEOUT, NAMES_POLL, || {
        let name = name.clone();
        cluster
            .node(node)
            .read(move |sql| {
                let mut stmt =
                    sql.prepare("SELECT note FROM live_typed_records WHERE name = ?")?;
                let mut rows = stmt.query([name])?;
                match rows.next()? {
                    Some(row) => Ok(row.get::<_, Option<String>>(0)?),
                    None => Ok(None),
                }
            })
            .unwrap_or(None)
            .as_deref()
            == Some(want.as_str())
    });
    assert!(
        matched,
        "node{} Note for {name:?} never became {want:?} within {FIELD_TIMEOUT:?}",
        node + 1
    );
}

/// Wait until the counter reads `want` (Go: `waitCounter`).
fn wait_counter(cluster: &Cluster, node: usize, name: &str, want: i64) {
    let name = name.to_owned();
    let matched = wait_until(FIELD_TIMEOUT, NAMES_POLL, || {
        let name = name.clone();
        cluster
            .node(node)
            .read(move |sql| {
                let mut stmt =
                    sql.prepare("SELECT counter FROM live_typed_records WHERE name = ?")?;
                let mut rows = stmt.query([name])?;
                match rows.next()? {
                    Some(row) => Ok(Some(row.get::<_, i64>(0)?)),
                    None => Ok(None),
                }
            })
            .unwrap_or(None)
            == Some(want)
    });
    assert!(
        matched,
        "node{} counter for {name:?} never became {want} within {FIELD_TIMEOUT:?}",
        node + 1
    );
}

#[test]
fn rolling_typed_schema_migration() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "schema-evolution".into(),
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

        // Seed rows plus counters on node 1 and converge.
        for i in 0..ROWS {
            let name = row_name(i);
            insert_row(cluster.node(0), 1, &name);
            counter_add(cluster.node(0), 1, &name, 100 + i as i64);
        }
        wait_names(
            &cluster,
            &[
                "typed-row-0",
                "typed-row-1",
                "typed-row-2",
                "typed-row-3",
                "typed-row-4",
            ],
        );

        // Only node 1 registers the additive Note field. Its peers adopt
        // the replicated schema (column presence + adoption metric stand in
        // for the Go epoch assertions).
        cluster
            .node(0)
            .migrate(Migration::new(NOTE_DDL))
            .expect("additive typed migration");
        cluster.node(0).flush().expect("flush migration");
        wait_epoch(&cluster, 0, EPOCH_LOCAL_TIMEOUT);
        wait_epoch(&cluster, 1, EPOCH_PEER_TIMEOUT);
        wait_epoch(&cluster, 2, EPOCH_PEER_TIMEOUT);
        for peer in [1, 2] {
            assert!(
                schema_adoptions(&cluster, peer) >= 1,
                "peer{} adopted the column without recording a schema adoption",
                peer + 1
            );
        }

        // The new field is written from node 1 while an older binding on
        // node 2 updates its known Name field after adoption; this must not
        // erase the new Note field. Node 3 inserts a late row.
        set_note(cluster.node(0), 1, "typed-row-0", "added-field-survives");
        // Barrier: the SQL rename copies local fields, so node 2 must hold
        // the note before renaming (Go rename keeps the record id).
        wait_note(&cluster, 1, "typed-row-0", "added-field-survives");
        rename_row(cluster.node(1), 2, "typed-row-0", "typed-row-renamed");
        insert_row(cluster.node(2), 3, "typed-row-late");
        let want_names = [
            "typed-row-1",
            "typed-row-2",
            "typed-row-3",
            "typed-row-4",
            "typed-row-late",
            "typed-row-renamed",
        ];
        wait_names(&cluster, &want_names);
        wait_note(
            &cluster,
            0,
            "typed-row-renamed",
            "added-field-survives",
        );
        for i in 1..ROWS {
            let name = row_name(i);
            for node in 0..cluster.len() {
                wait_counter(&cluster, node, &name, 100 + i as i64);
            }
        }

        // The adopted schema and its rows survive the third peer's restart.
        cluster.stop_node(2);
        cluster.start_node(2).expect("restart node3");
        cluster.mesh().expect("re-mesh after restart");
        wait_names(&cluster, &want_names);
        wait_counter(&cluster, 2, "typed-row-4", 104);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("schema-evolution-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
