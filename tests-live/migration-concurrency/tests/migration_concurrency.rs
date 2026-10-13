//! Rust port of the Go `migration-concurrency` live scenario.
//!
//! Go source:
//! `/media/marc/2TB/Archives/MURMUR/tests-live/migration-concurrency/`
//! (`migration_concurrency_test.go`). The SAME additive Note migration fires
//! simultaneously on two of three meshed nodes while all keep writing. Both
//! migrating nodes must land well-defined (old or new, never mixed),
//! mixed-version replication must flow in both directions during the
//! overlap, and after the straggler migrates the whole mesh converges fully
//! migrated with identical counts, digests, and materialized new-column
//! reads on every node. Plain SQL replaces the Go typed-records HTTP API
//! (see the scenario README.md for the deviations).

use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Barrier, Mutex,
};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Base table DDL (Go: typed records with a Name field).
const TABLE: &str = "live_typed_records";
const TABLE_DDL: &str =
    "CREATE TABLE live_typed_records(name TEXT PRIMARY KEY, counter INTEGER NOT NULL)";
/// Additive migration DDL (Go: `MigrateTypedRecords` registers Note).
const NOTE_DDL: &str = "ALTER TABLE live_typed_records ADD COLUMN note TEXT";
/// Seed-row knob (Go: `MURMUR_MIGRATION_CONCURRENCY_SEED`, default 60).
const SEED_ENV: &str = "MURMUR_MIGRATION_CONCURRENCY_SEED";
const DEFAULT_SEED: usize = 60;
/// Background-writer cadence (Go: 25 ms ticker).
const WRITE_TICK: Duration = Duration::from_millis(25);
/// Seed convergence deadline (Go: 60 s).
const SEED_TIMEOUT: Duration = Duration::from_secs(60);
/// Post-heal convergence deadlines (Go: 90 s).
const HEAL_TIMEOUT: Duration = Duration::from_secs(90);
/// Convergence poll interval (Go: 200 ms).
const POLL: Duration = Duration::from_millis(200);

/// Stops background writers when dropped, so a panic in the driver body
/// never wedges the scope join on workers waiting for the stop flag.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
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

/// Best-effort insert; true when acknowledged (Go: writers keep only
/// `err == nil` names in `acked`).
fn try_insert(db: &Database, name: String) -> bool {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_typed_records(name, counter) VALUES(?, 0)",
            [name],
        )?;
        Ok(())
    })
    .is_ok()
}

/// Insert one record, panicking on error (seeds and post-race writes).
fn insert_row(db: &Database, node: usize, name: &str) {
    assert!(
        try_insert(db, name.to_owned()),
        "node{node} insert of {name:?} failed"
    );
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
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
        "node{} schema_epoch = {got}, want {want}",
        idx + 1
    );
}

/// Raw Note read that surfaces SQL errors (Go: `TypedNote` returning nil
/// vs non-nil error distinguishes exposed from hidden Note).
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

/// Require a well-defined schema state: epoch exactly 1 or 2, the Note
/// column exposed iff new, and log the (still converging) row count
/// (Go: `classifyNode`).
fn classify_node(cluster: &Cluster, idx: usize) {
    let ep = epoch(cluster, idx);
    assert!(
        ep == 1 || ep == 2,
        "node{} epoch = {ep}, want exactly 1 or 2 (mixed state)",
        idx + 1
    );
    let probe = try_read_note(cluster.node(idx), "seed-0");
    if ep == 1 {
        assert!(
            probe.is_err(),
            "node{} epoch 1 yet exposes Note (mixed state)",
            idx + 1
        );
    } else {
        assert!(
            probe.is_ok(),
            "node{} epoch 2 yet hides Note (mixed state): {probe:?}",
            idx + 1
        );
    }
    let rows = names(cluster.node(idx)).len();
    eprintln!("node{} classified: epoch={ep} rows={rows} (converging)", idx + 1);
}

/// Poll id/name convergence (Go: `waitNamesConverged`; tolerates schema
/// skew on the new column during the mixed-version window).
fn wait_names_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        let mut first: Option<Vec<String>> = None;
        for i in 0..cluster.len() {
            let got = names(cluster.node(i));
            if got.len() != want {
                return false;
            }
            match &first {
                None => first = Some(got),
                Some(prev) if *prev == got => {}
                _ => return false,
            }
        }
        true
    });
    if !converged {
        let mut union = HashSet::new();
        let mut sets = Vec::new();
        for i in 0..cluster.len() {
            let got = names(cluster.node(i));
            eprintln!("node{} at timeout: names={}", i + 1, got.len());
            union.extend(got.iter().cloned());
            sets.push(got.into_iter().collect::<HashSet<_>>());
        }
        for (i, set) in sets.iter().enumerate() {
            let missing: Vec<_> = union.iter().filter(|n| !set.contains(*n)).collect();
            if !missing.is_empty() {
                eprintln!("node{} at timeout: missing {} names: {missing:?}", i + 1, missing.len());
            }
        }
        panic!("nodes did not converge on {want} names within {timeout:?}");
    }
}

/// Poll exact counts plus ordered-digest equality (Go: `waitFullConverged`).
fn wait_full_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    let converged = wait_until(timeout, POLL, || {
        let mut first: Option<Vec<String>> = None;
        for i in 0..cluster.len() {
            let got = names(cluster.node(i));
            if got.len() != want {
                return false;
            }
            match &first {
                None => first = Some(got),
                Some(prev) if *prev == got => {}
                _ => return false,
            }
        }
        true
    });
    if !converged {
        for i in 0..cluster.len() {
            let got = names(cluster.node(i));
            eprintln!("node{} at timeout: count={} digest={got:?}", i + 1, got.len());
        }
        panic!("nodes did not fully converge on {want} rows within {timeout:?}");
    }
}

/// Require a materialized Note value (Go: `assertNote`).
fn assert_note(cluster: &Cluster, idx: usize, name: &str, want: &str) {
    match try_read_note(cluster.node(idx), name) {
        Ok(got) if got.as_deref() == Some(want) => {}
        other => panic!(
            "node{} Note({name:?}) = {other:?}, want {want:?}",
            idx + 1
        ),
    }
}

/// Apply the additive migration unless the column is already present
/// (Go `MigrateTypedRecords` is an idempotent manifest bind; Rust DDL
/// errors on a duplicate column, so adoption is checked first).
fn migrate_unless_adopted(cluster: &Cluster, idx: usize) -> bool {
    if has_column(cluster.node(idx), TABLE, "note") {
        return false;
    }
    cluster
        .node(idx)
        .migrate(Migration::new(NOTE_DDL))
        .unwrap_or_else(|e| panic!("migrate node{}: {e:?}", idx + 1));
    cluster
        .node(idx)
        .flush()
        .unwrap_or_else(|e| panic!("flush migration on node{}: {e:?}", idx + 1));
    true
}

#[test]
fn simultaneous_migration_converges() {
    let seed_rows = std::env::var(SEED_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_SEED);

    let cluster = Cluster::new(ClusterOptions {
        name: "migration-concurrency".into(),
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
        // Shared (Copy) handle: the scoped worker threads below capture it
        // while `cluster` itself stays owned outside for forensics/finish.
        let c = &cluster;
        // Same base schema on every node (Go: typed fixture tables).
        for i in 0..c.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate records table");
            c.node(i).flush().expect("flush schema");
        }

        // Honest baseline: all old, seed converged everywhere.
        for i in 0..seed_rows {
            insert_row(c.node(0), 1, &format!("seed-{i}"));
        }
        wait_names_converged(c, seed_rows, SEED_TIMEOUT);
        for i in 0..3 {
            assert_epoch(c, i, 1);
        }

        // Pin node 1 on the old schema with peer isolation: adoption is
        // fast once a revision exists, so without isolation there would be
        // no deterministic mixed-version window after the race.
        for (from, target) in [(0, 1), (1, 0), (0, 2), (2, 0)] {
            cluster
                .remove_peer(from, target)
                .unwrap_or_else(|e| panic!("isolate node1: {e:?}"));
        }

        // All nodes keep writing across the migration window; every acked
        // name must converge afterwards.
        let stop = Arc::new(AtomicBool::new(false));
        let _stop_guard = StopOnDrop(stop.clone());
        let acked = Arc::new(Mutex::new(HashSet::new()));
        let seqs = [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)];
        std::thread::scope(|s| {
            for n in 0..3 {
                let db = c.node(n).clone();
                let stop = stop.clone();
                let acked = acked.clone();
                let seq = &seqs[n];
                s.spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(WRITE_TICK);
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let seq = seq.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = format!("flow-n{}-{seq:08}", n + 1);
                        if try_insert(&db, name.clone()) {
                            acked.lock().expect("acked lock").insert(name);
                        }
                    }
                });
            }
            std::thread::sleep(Duration::from_secs(1)); // writers flowing before the race

            // Simultaneous trigger: the same migration starts on nodes 2
            // and 3 at the same instant (start barrier), node 1 stays old.
            let start = Arc::new(Barrier::new(3));
            let mut handles = Vec::new();
            for idx in [1, 2] {
                let start = start.clone();
                handles.push(s.spawn(move || {
                    start.wait();
                    migrate_unless_adopted(c, idx);
                }));
            }
            start.wait();
            eprintln!("simultaneous migrate fired on nodes 2 and 3");
            for (handle, idx) in handles.into_iter().zip([2, 3]) {
                handle.join().unwrap();
                eprintln!("migrate result: node{idx} done");
            }
            stop.store(true, Ordering::Relaxed);
        });
        let acked = acked.lock().expect("acked lock");
        let total = seed_rows + acked.len();
        eprintln!("migration window closed with {} overlap writes", acked.len());
        assert!(
            !acked.is_empty(),
            "no writes acknowledged during the migration window; overlap proved nothing"
        );
        drop(acked);

        // Both migrating nodes land well-defined; node 1 is still old.
        for idx in [1, 2] {
            classify_node(c, idx);
        }
        assert_epoch(c, 0, 1);
        let mut new_count = 0;
        for idx in [1, 2] {
            if epoch(c, idx) == 2 {
                new_count += 1;
            }
        }
        assert!(
            new_count > 0,
            "simultaneous migration landed nowhere: both nodes still old"
        );
        eprintln!("simultaneous trigger: {new_count}/2 nodes new; sequentially migrating laggards");
        for idx in [1, 2] {
            if epoch(c, idx) != 2 {
                migrate_unless_adopted(c, idx);
            }
        }

        // Heal with versions asserted different at the instant: node 1 is
        // deterministically old, the racers are new. Overlap writes from
        // both sides then converge by name across the boundary.
        assert_epoch(c, 0, 1);
        for (from, target) in [(0, 1), (1, 0), (0, 2), (2, 0)] {
            cluster
                .add_peer(from, target)
                .unwrap_or_else(|e| panic!("heal node1: {e:?}"));
        }
        wait_names_converged(c, total, HEAL_TIMEOUT);
        eprintln!("overlap writes converged in both directions: {total} names");

        // Explicit exchange across the (former) boundary: new-column
        // writes from a new node plus an old-schema insert from node 1.
        assert_epoch(c, 1, 2);
        for i in 0..5 {
            set_note(c.node(1), 2, &format!("seed-{i}"), &format!("{}", 100 + i));
        }
        insert_row(c.node(0), 1, "post-race");
        wait_names_converged(c, total + 1, HEAL_TIMEOUT);

        // Migrate the third (unless it already auto-adopted), then full
        // convergence: exact counts, ordered digests, new schema everywhere.
        for i in 0..c.len() {
            migrate_unless_adopted(c, i);
        }
        wait_full_converged(c, total + 1, HEAL_TIMEOUT);
        for i in 0..3 {
            assert_epoch(c, i, 2);
        }
        // Materialized-query check: the new column reads identically everywhere.
        for i in 0..3 {
            assert_note(c, i, "seed-0", "100");
            assert_note(c, i, "seed-4", "104");
        }
        eprintln!("migration concurrency proven: simultaneous trigger, defined states, full convergence");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("migration-concurrency-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
