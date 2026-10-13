//! Rust port of the Go `clock-skew` live scenario (test-side equivalent).
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/clock-skew/`
//! (`clock_skew_test.go`). Go runs nodes under libfaketime with skewed
//! clocks and requires concurrent shared-row writes to resolve to
//! identical LWW winners everywhere. Per-node clocks are impossible
//! in-process (one clock per process) and the harness fixture has no
//! replication mode, so this port proves the same property
//! deterministically: real exported replica states with synthetically
//! skewed version stamps (±5 min) merge to identical winners regardless
//! of merge order (see the scenario README.md).

use murmur_live_harness::sqlite_session::{
    Database, Migration, ReplicaState, Version,
};
use murmur_live_harness::{Cluster, ClusterOptions};

/// Table DDL (Go: typed contention rows).
const TABLE_DDL: &str = "CREATE TABLE skew(id TEXT NOT NULL PRIMARY KEY, val TEXT NOT NULL)";
/// Contended rows (Go: concurrent shared-row writes; same shape, fixed N).
const ROWS: usize = 20;
/// Synthetic clock skew applied to exported versions (±5 min, both signs).
const SKEW_MS: i64 = 5 * 60 * 1000;

/// Shift one HLC stamp by a signed skew (saturating; node/logical kept).
fn shift_version(v: Version, delta_ms: i64) -> Version {
    Version {
        millis: v.millis.saturating_add_signed(delta_ms),
        ..v
    }
}

/// Shift every version stamp in an exported state: row generations, cell
/// versions, tombstones, and the state clock. The result simulates "this
/// replica's clock was off by `delta_ms`" with byte-identical content.
fn shift_state(mut s: ReplicaState, delta_ms: i64) -> ReplicaState {
    s.clock = shift_version(s.clock, delta_ms);
    for row in &mut s.rows {
        row.generation = shift_version(row.generation, delta_ms);
        for cell in &mut row.cells {
            cell.version = shift_version(cell.version, delta_ms);
        }
    }
    for t in &mut s.tombstones {
        t.generation = shift_version(t.generation, delta_ms);
        t.version = shift_version(t.version, delta_ms);
    }
    s
}

/// Insert one row (Go: `TypedContentionInsert`).
fn insert_row(db: &Database, node: usize, id: &str, val: &str) {
    let (id, val) = (id.to_owned(), val.to_owned());
    db.transaction(move |sql| {
        sql.execute("INSERT INTO skew(id, val) VALUES(?, ?)", (id, val))?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
}

/// Point read by id (Go: `TypedContentionRead`).
fn read_val(db: &Database, id: &str) -> Option<String> {
    let id = id.to_owned();
    db.read(move |sql| {
        let mut stmt = sql.prepare("SELECT val FROM skew WHERE id = ?")?;
        let mut found = stmt.query([id])?;
        match found.next()? {
            Some(row) => Ok(Some(row.get::<_, String>(0)?)),
            None => Ok(None),
        }
    })
    .unwrap_or(None)
}

/// Require every contended row to read `want(i)` on `idx`.
fn assert_rows(cluster: &Cluster, idx: usize, want: impl Fn(usize) -> String, what: &str) {
    for i in 0..ROWS {
        let id = format!("skew-{i:02}");
        assert_eq!(
            read_val(cluster.node(idx), &id).as_deref(),
            Some(want(i).as_str()),
            "node{} row {id} after {what}",
            idx + 1
        );
    }
}

#[test]
fn skewed_clocks_resolve_identical_winners() {
    // Four unlinked nodes (manual peers, no links: `merge_state` is the
    // only sync path, so every winner below comes from the merge, never
    // from background replication).
    let cluster = Cluster::new(ClusterOptions {
        name: "clock-skew".into(),
        num_nodes: 4,
        manual_peers: true,
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
        // Same schema on every node (Go: typed contention fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate skew table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Concurrent same-key writes from two nodes at (near-)identical
        // wall times; exports are pristine inputs for every leg below.
        for i in 0..ROWS {
            let id = format!("skew-{i:02}");
            insert_row(cluster.node(0), 1, &id, &format!("from-a-{i:02}"));
            insert_row(cluster.node(1), 2, &id, &format!("from-b-{i:02}"));
        }
        let a0 = cluster.node(0).export_state().expect("export A");
        let b0 = cluster.node(1).export_state().expect("export B");
        assert_eq!(a0.rows.len(), ROWS, "export A has {} rows, want {ROWS}", a0.rows.len());
        assert_eq!(b0.rows.len(), ROWS, "export B has {} rows, want {ROWS}", b0.rows.len());

        // Fast clock wins: A's state with +5 min skew merged into B
        // beats B's local present-time writes on every row.
        cluster
            .node(1)
            .merge_state(shift_state(a0.clone(), SKEW_MS))
            .expect("merge skewed-fast A into B");
        assert_rows(&cluster, 1, |i| format!("from-a-{i:02}"), "fast-clock merge");

        // Slow clock loses: B's state with -5 min skew merged into A
        // loses to A's local present-time writes on every row.
        cluster
            .node(0)
            .merge_state(shift_state(b0.clone(), -SKEW_MS))
            .expect("merge skewed-slow B into A");
        assert_rows(&cluster, 0, |i| format!("from-a-{i:02}"), "slow-clock merge");

        // Merge order is irrelevant: C merges [present, skewed-fast],
        // D merges [skewed-fast, present]; both must hold identical
        // winners (the skewed-fast values) on every row.
        cluster
            .node(2)
            .merge_state(b0.clone())
            .expect("merge present B into C");
        cluster
            .node(2)
            .merge_state(shift_state(a0.clone(), SKEW_MS))
            .expect("merge skewed-fast A into C");
        cluster
            .node(3)
            .merge_state(shift_state(a0.clone(), SKEW_MS))
            .expect("merge skewed-fast A into D");
        cluster
            .node(3)
            .merge_state(b0.clone())
            .expect("merge present B into D");
        assert_rows(&cluster, 2, |i| format!("from-a-{i:02}"), "present-then-fast order");
        assert_rows(&cluster, 3, |i| format!("from-a-{i:02}"), "fast-then-present order");
        eprintln!("PASS: ±5min skew resolves to identical winners regardless of merge order");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("clock-skew-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
