//! Rust port of the Go `views` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/views/`
//! (`views_test.go`). Three nodes each insert one contact; two contacts are
//! enabled via a counter add and must appear in the managed view on every
//! node while the third stays out. Node 3 then restarts and its rebuilt view
//! must contain the same two rows. Plain SQL (table + `VIEW`) replaces the
//! Go typed-RIME managed query (see the scenario README.md for deviations).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};

/// Base table DDL (Go: typed `contacts` records; `score` plays the role of
/// the enabling PN counter).
const TABLE_DDL: &str = "CREATE TABLE contacts(name TEXT PRIMARY KEY, score INTEGER NOT NULL)";
/// Managed-view DDL (Go: `TypedEnabledNames` over managed typed queries).
const VIEW_DDL: &str =
    "CREATE VIEW enabled_names AS SELECT name FROM contacts WHERE score > 0 ORDER BY name";
/// View wait deadline (Go: 20 s `waitForViewRows`).
const VIEW_TIMEOUT: Duration = Duration::from_secs(20);
/// View poll interval (Go: 25 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Canonical ordered enabled names on one node (Go: `TypedEnabledNames`).
fn enabled_names(db: &Database) -> Vec<String> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT name FROM enabled_names")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for name in rows {
            out.push(name?);
        }
        Ok(out)
    })
    .expect("read view failed")
}

/// Insert one contact, disabled (Go: `Cluster.TypedInsert`).
fn insert_contact(db: &Database, node: usize, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO contacts(name, score) VALUES(?, 0)",
            [name],
        )?;
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} insert failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after insert failed: {e}"));
}

/// Enable one contact via a counter add (Go: `Cluster.TypedCounterAdd`).
fn enable_contact(db: &Database, node: usize, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        let changed = sql.execute(
            "UPDATE contacts SET score = score + 1 WHERE name = ?",
            [name],
        )?;
        assert_eq!(changed, 1, "node{node} enable hit {changed} rows");
        Ok(())
    })
    .unwrap_or_else(|e| panic!("node{node} enable failed: {e}"));
    db.flush()
        .unwrap_or_else(|e| panic!("node{node} flush after enable failed: {e}"));
}

/// Wait until the managed view on each listed node holds exactly `want`
/// (Go: `waitForViewRows`).
fn wait_for_view_rows(cluster: &Cluster, nodes: &[usize], want: &[&str]) {
    let matched = wait_until(VIEW_TIMEOUT, POLL_INTERVAL, || {
        nodes.iter().all(|i| {
            let got = enabled_names(cluster.node(*i));
            got.iter().map(String::as_str).collect::<Vec<_>>() == want
        })
    });
    assert!(
        matched,
        "timed out waiting for typed view to contain {want:?} on nodes {nodes:?}"
    );
}

#[test]
fn typed_view_tracks_replicated_rows_and_rebuilds() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "views".into(),
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
        // Same schema on every node (Go: typed fixture tables + view).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .expect("migrate contacts table");
            cluster
                .node(i)
                .migrate(Migration::new(VIEW_DDL))
                .expect("migrate enabled_names view");
            cluster.node(i).flush().expect("flush schema");
        }

        // One insert per node; only ann and bob are enabled (Go: hidden
        // gets no counter add and must stay out of the view).
        for (i, name) in ["ann", "bob", "hidden"].iter().enumerate() {
            insert_contact(cluster.node(i), i + 1, name);
            if *name != "hidden" {
                enable_contact(cluster.node(i), i + 1, name);
            }
        }
        wait_for_view_rows(&cluster, &[0, 1, 2], &["ann", "bob"]);

        // Restart the third node: startup rebuild must restore both the
        // replicated rows and the local view (fresh ephemeral port, so
        // re-mesh afterwards).
        cluster.stop_node(2);
        cluster.start_node(2).expect("restart node3");
        cluster.mesh().expect("re-mesh after restart");
        wait_for_view_rows(&cluster, &[2], &["ann", "bob"]);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("views-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
