//! Rust port of the Go `merge-policies` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/merge-policies/`
//! (`merge_policies_test.go`, `README.md`).
//! `TestDisconnectedPoliciesForwardAndRestart`: three encrypted nodes write
//! PN_COUNTER / OR_SET / MAX / MIN policy ops on one shared row while
//! disconnected; nodes 0 and 1 connect and converge the counter; node 0 stops
//! and node 1 forwards the causal operations to node 2; node 2 restarts and
//! recovers; finally an observed set remove propagates. The unobserved remove
//! (node 1 removing "shared" before seeing node 0's add) must not erase the
//! add, while the later observed remove must.
//!
//! Plain SQL plus `policy.rs` ops replace the Go managed typed HTTP API (the
//! Rust product has no HTTP server); see the scenario README.md for the
//! per-leg mapping.

use std::collections::BTreeMap;
use std::time::Duration;

use murmur_live_harness::murmur_replication::policies::Policy;
use murmur_live_harness::sqlite_session::{Database, Migration, Value};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions, ReplicationOptions};

/// Typed table (Go: managed typed record with counter/set/extrema fields).
const TABLE: &str = "live_merge_policies";
/// Base DDL. `counter`/`tags`/`peak`/`floor` are policy columns (see
/// [`merge_policies`]). Set projections start as `x'00'` (postcard empty
/// vec), the `tests/` convention for fresh set cells.
const BASE_DDL: &str = "CREATE TABLE live_merge_policies(name TEXT PRIMARY KEY, counter INTEGER NOT NULL, tags BLOB NOT NULL, peak INTEGER NOT NULL, floor REAL NOT NULL)";
/// Shared row (Go: `merge-policy-row`, one row id seeded on every node).
const ROW: &str = "merge-policy-row";
/// Convergence deadline (Go: 30 s waits).
const WAIT: Duration = Duration::from_secs(30);
/// Convergence poll interval (Go: 50 ms polls).
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

/// Migrate every node with `ddl` (Go: shared typed-table definition).
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

/// Typed insert on one node (Go: `Cluster.TypedInsertWithID`).
fn insert_row(db: &Database, name: &str) {
    let name = name.to_owned();
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_merge_policies(name, counter, tags, peak, floor) VALUES(?, 0, x'00', 0, 0.0)",
            [name],
        )?;
        Ok(())
    })
    .expect("typed insert failed");
    db.flush().expect("flush after typed insert");
}

/// PN_COUNTER projection (Go: `Cluster.TypedCounterValue`).
fn counter_value(db: &Database, name: &str) -> i64 {
    let name = name.to_owned();
    db.read(move |sql| {
        Ok(sql.query_row(
            "SELECT counter FROM live_merge_policies WHERE name = ?",
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
            "SELECT tags FROM live_merge_policies WHERE name = ?",
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
            "SELECT peak, floor FROM live_merge_policies WHERE name = ?",
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

fn wait_counter(cluster: &Cluster, node: usize, name: &str, want: i64) {
    let converged = wait_until(WAIT, POLL, || counter_value(cluster.node(node), name) == want);
    assert!(
        converged,
        "node{} counter for {name:?} = {}, want {want}",
        node + 1,
        counter_value(cluster.node(node), name)
    );
}

/// Combined set + extrema projection wait (Go: `waitPolicyProjection`).
fn wait_policy_projection(
    cluster: &Cluster,
    node: usize,
    name: &str,
    want_tags: &[&str],
    want_peak: i64,
    want_floor: f64,
) {
    let want: Vec<String> = want_tags.iter().map(|s| s.to_string()).collect();
    let converged = wait_until(WAIT, POLL, || {
        set_values(cluster.node(node), name) == want
            && extrema(cluster.node(node), name) == (want_peak, want_floor)
    });
    assert!(
        converged,
        "node{} projection tags={:?} extrema={:?}; want tags={want:?} extrema=({want_peak},{want_floor})",
        node + 1,
        set_values(cluster.node(node), name),
        extrema(cluster.node(node), name)
    );
}

#[test]
fn disconnected_policies_forward_and_restart() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "merge-policies".into(),
        num_nodes: 3,
        manual_peers: true,
        replication: Some(ReplicationOptions {
            merge_policies: Some(merge_policies()),
            ..Default::default()
        }),
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_all(&cluster, BASE_DDL);

        // Every node seeds the same logical row while disconnected (Go:
        // TypedInsertWithID with one shared row id on all nodes).
        for i in 0..cluster.len() {
            insert_row(cluster.node(i), ROW);
        }

        // Each node makes independent typed CRDT updates while disconnected.
        counter_add(cluster.node(0), ROW, 10);
        set_add(cluster.node(0), ROW, "shared");
        set_add(cluster.node(0), ROW, "red");
        extrema_update(cluster.node(0), ROW, 10, 30.0);

        counter_add(cluster.node(1), ROW, 7);
        // This remove has not observed node 0's add and must not erase it.
        set_remove(cluster.node(1), ROW, "shared");
        set_add(cluster.node(1), ROW, "blue");
        extrema_update(cluster.node(1), ROW, 20, 15.0);

        counter_add(cluster.node(2), ROW, -2);
        set_add(cluster.node(2), ROW, "green");
        extrema_update(cluster.node(2), ROW, 5, -7.0);

        // Nodes 0 and 1 connect (node 2 stays isolated); their counters sum.
        cluster.add_peer(0, 1).expect("connect 0 -> 1");
        cluster.add_peer(1, 0).expect("connect 1 -> 0");
        wait_counter(&cluster, 0, ROW, 17);
        wait_counter(&cluster, 1, ROW, 17);

        // The original writer stops; node 1 forwards its causal operations to
        // node 2.
        cluster.stop_node(0);
        cluster.add_peer(1, 2).expect("connect 1 -> 2");
        cluster.add_peer(2, 1).expect("connect 2 -> 1");
        wait_counter(&cluster, 1, ROW, 15);
        wait_counter(&cluster, 2, ROW, 15);
        wait_policy_projection(
            &cluster,
            1,
            ROW,
            &["blue", "green", "red", "shared"],
            20,
            -7.0,
        );
        wait_policy_projection(
            &cluster,
            2,
            ROW,
            &["blue", "green", "red", "shared"],
            20,
            -7.0,
        );

        // Node 2 restarts and recovers the merged projections from durable
        // state (Go: StopNode/StartNode + UnlockNode + WaitNodeReady). The
        // restart rebinds a fresh ephemeral port, so the 1 <-> 2 edges are
        // re-added explicitly.
        cluster.stop_node(2);
        cluster.start_node(2).expect("restart node 2");
        cluster.add_peer(1, 2).expect("reconnect 1 -> 2");
        cluster.add_peer(2, 1).expect("reconnect 2 -> 1");
        wait_counter(&cluster, 2, ROW, 15);
        wait_policy_projection(
            &cluster,
            2,
            ROW,
            &["blue", "green", "red", "shared"],
            20,
            -7.0,
        );

        // A later observed remove must propagate after restart.
        set_remove(cluster.node(1), ROW, "shared");
        wait_policy_projection(&cluster, 2, ROW, &["blue", "green", "red"], 20, -7.0);
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("merge-policies-no-convergence");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
