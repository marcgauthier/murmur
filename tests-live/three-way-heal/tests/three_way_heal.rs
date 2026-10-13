//! Rust port of the Go `three-way-heal` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/three-way-heal/`
//! (`three_way_heal_test.go`). Four fully meshed nodes split into
//! isolated pairs, diverge with deterministic writes (disjoint inserts
//! plus conflicting updates to the same base rows), then heal
//! link-by-link in a caller-supplied order. Two different heal orders
//! must reach the identical final digest, including identical LWW
//! conflict winners. Plain SQL replaces the Go typed-records HTTP API
//! (see the scenario README.md).

use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration};
use murmur_live_harness::{Cluster, ClusterOptions};

/// Convergence poll interval (Go: 200 ms).
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Baseline / per-side / final convergence deadlines (Go: 150 s each).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(150);
/// Cross-side traffic deadline after each healed link (Go: 30 s).
const LINK_FLOW_TIMEOUT: Duration = Duration::from_secs(30);
/// Cross-side traffic poll interval (Go: 50 ms).
const LINK_FLOW_POLL: Duration = Duration::from_millis(50);
/// Progress-log cadence inside convergence waits (Go: 10 s).
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Baseline row ids (Go: 1000+i, 10 rows).
const BASE_ID: i64 = 1000;
/// Side A disjoint-insert ids, written on node 1 (Go: 2000+i).
const SIDE_A_ID: i64 = 2000;
/// Side B disjoint-insert ids, written on node 3 (Go: 3000+i).
const SIDE_B_ID: i64 = 3000;
/// Rows per write batch (Go: 10).
const BATCH_ROWS: i64 = 10;

/// Cross links spanning the (0,1)|(2,3) split; a heal order is a
/// permutation of indexes into this table (Go: `crossLinks`).
const CROSS_LINKS: [[usize; 2]; 4] = [[0, 2], [0, 3], [1, 2], [1, 3]];

/// All (id, name) rows on one node, ordered by id
/// (Go: `Cluster.TypedContentionRows`).
fn rows(db: &Database) -> Vec<(i64, String)> {
    db.read(|sql| {
        let mut stmt = sql.prepare("SELECT id, name FROM records ORDER BY id")?;
        let mapped = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in mapped {
            out.push(row?);
        }
        Ok(out)
    })
    .expect("read contention rows")
}

/// Deterministic digest over the canonical row encoding: FNV-1a 64,
/// hex-rendered. Std-only stand-in for Go's sha256 hex digest; what the
/// test asserts is digest equality across heal orders, which this
/// preserves (see the scenario README.md).
fn digest_rows(node_rows: &[(i64, String)]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for (id, name) in node_rows {
        for byte in format!("{id}\x00{name}\x00").bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    format!("{hash:016x}")
}

/// Insert one row (Go: `TypedContentionInsert` via `insertRow`).
fn insert(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    let value = name.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            sql.execute("INSERT INTO records(id, name) VALUES(?, ?)", (id, value))?;
            Ok(())
        })
        .map_err(|e| format!("node{} insert id {id} failed: {e}", node + 1))?;
    cluster
        .node(node)
        .flush()
        .map_err(|e| format!("node{} flush after insert id {id} failed: {e}", node + 1))?;
    Ok(())
}

/// Conflicting update to one base row (Go: `TypedContentionUpdate`).
fn update_name(cluster: &Cluster, node: usize, id: i64, name: &str) -> Result<(), String> {
    let value = name.to_string();
    cluster
        .node(node)
        .transaction(move |sql| {
            let changed = sql.execute(
                "UPDATE records SET name = ? WHERE id = ?",
                (value, id),
            )?;
            assert_eq!(changed, 1, "conflict update of id {id} hit {changed} rows");
            Ok(())
        })
        .map_err(|e| format!("node{} conflict update id {id} failed: {e}", node + 1))?;
    cluster
        .node(node)
        .flush()
        .map_err(|e| format!("node{} flush after conflict update id {id} failed: {e}", node + 1))?;
    Ok(())
}

/// Whether one node currently holds a row id (Go: `hasRow`).
fn has_id(cluster: &Cluster, idx: usize, id: i64) -> bool {
    rows(cluster.node(idx)).iter().any(|(got, _)| *got == id)
}

/// Wait until the indexed nodes hold exactly `want` rows with equal
/// digests (Go: `waitConvergedOn`).
fn wait_converged_on(cluster: &Cluster, idxs: &[usize], want: usize, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_log = std::time::Instant::now();
    loop {
        let mut ok = true;
        let mut first = String::new();
        for (k, idx) in idxs.iter().enumerate() {
            let node_rows = rows(cluster.node(*idx));
            if node_rows.len() != want {
                ok = false;
                break;
            }
            let digest = digest_rows(&node_rows);
            if k == 0 {
                first = digest;
            } else if digest != first {
                ok = false;
                break;
            }
        }
        if ok {
            return;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        if last_log.elapsed() > PROGRESS_LOG_INTERVAL {
            last_log = std::time::Instant::now();
            let counts: Vec<usize> = (0..cluster.len())
                .map(|i| rows(cluster.node(i)).len())
                .collect();
            eprintln!("converge progress: counts={counts:?} want={want}");
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    for idx in idxs {
        let node_rows = rows(cluster.node(*idx));
        eprintln!(
            "node{} at timeout: count={} digest={}",
            idx + 1,
            node_rows.len(),
            digest_rows(&node_rows)
        );
    }
    panic!("nodes {idxs:?} did not converge on {want} rows with equal digests within {timeout:?}");
}

/// Wait until all four nodes hold exactly `want` rows with equal digests
/// (Go: `waitConverged`).
fn wait_converged(cluster: &Cluster, want: usize, timeout: Duration) {
    wait_converged_on(cluster, &[0, 1, 2, 3], want, timeout);
}

/// Block until a side-originated row is visible across the newly healed
/// link in either direction: a sideA row (id 2000, written on node 1) on
/// the sideB endpoint, or a sideB row (id 3000, written on node 3) on
/// the sideA endpoint (Go: `waitLinkFlowed`).
fn wait_link_flowed(cluster: &Cluster, order: &[usize], link: [usize; 2]) {
    let deadline = std::time::Instant::now() + LINK_FLOW_TIMEOUT;
    while std::time::Instant::now() < deadline {
        if has_id(cluster, link[1], SIDE_A_ID) || has_id(cluster, link[0], SIDE_B_ID) {
            return;
        }
        std::thread::sleep(LINK_FLOW_POLL);
    }
    panic!("order {order:?}: no cross-side traffic on healed link {link:?} within {LINK_FLOW_TIMEOUT:?}");
}

/// Read a comma-separated permutation of link indexes; invalid values
/// fail fast rather than silently healing another order (Go: `parseOrder`).
fn parse_order(env: &str, fallback: &str) -> Vec<usize> {
    let raw = std::env::var(env).unwrap_or_else(|_| fallback.to_string());
    let parts: Vec<&str> = raw.split(',').collect();
    if parts.len() != CROSS_LINKS.len() {
        panic!("{env}={raw:?}: want {} comma-separated link indexes", CROSS_LINKS.len());
    }
    let mut seen = [false; 4];
    let mut order = Vec::with_capacity(parts.len());
    for part in parts {
        let parsed: usize = part.trim().parse().unwrap_or(usize::MAX);
        if parsed >= CROSS_LINKS.len() || seen[parsed] {
            panic!("{env}={raw:?}: want a permutation of 0..{}", CROSS_LINKS.len() - 1);
        }
        seen[parsed] = true;
        order.push(parsed);
    }
    order
}

/// Run one full split/diverge/heal cycle in the given link order and
/// return the final digest (Go: `runHealOrder`).
fn run_heal_order(order: &[usize], tag: &str) -> String {
    let cluster = Cluster::new(ClusterOptions {
        name: format!("three-way-heal-{tag}"),
        num_nodes: 4,
        ..Default::default()
    })
    .expect("cluster setup");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let cluster = &cluster;
        for i in 0..cluster.len() {
            let info = cluster.node_info(i);
            eprintln!(
                "{} dir={} repl={}",
                info.label,
                info.dir.display(),
                info.repl_addr
            );
        }
        // Same schema on every node (Go: TypedRecords fixture tables).
        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(
                    "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                ))
                .expect("migrate records table");
            cluster.node(i).flush().expect("flush schema");
        }

        // Baseline converges before the split (honest-path control).
        // IDs/names are fixed so both heal orders must reach the same digest.
        for i in 0..BATCH_ROWS {
            insert(cluster, 0, BASE_ID + i, &format!("base-{i}"))
                .unwrap_or_else(|e| panic!("order {order:?} baseline write: {e}"));
        }
        wait_converged(cluster, BATCH_ROWS as usize, CONVERGE_TIMEOUT);

        // Split into isolated pairs (0,1) and (2,3).
        for link in CROSS_LINKS {
            cluster
                .remove_peer(link[0], link[1])
                .unwrap_or_else(|e| panic!("order {order:?} split {link:?}: {e}"));
            cluster
                .remove_peer(link[1], link[0])
                .unwrap_or_else(|e| panic!("order {order:?} split {link:?}: {e}"));
        }

        // Deterministic divergent writes per side: disjoint inserts plus
        // CONFLICTING updates to the same base rows. Union-merge alone
        // cannot prove heal-order independence where it matters; LWW must
        // pick the same winner per cell regardless of arrival order. Side B
        // writes strictly after side A (sequential loops), so the
        // relative HLC order (and hence the winner) is deterministic
        // across runs.
        for i in 0..BATCH_ROWS {
            insert(cluster, 1, SIDE_A_ID + i, &format!("sideA-{i}"))
                .unwrap_or_else(|e| panic!("order {order:?} sideA write: {e}"));
            insert(cluster, 3, SIDE_B_ID + i, &format!("sideB-{i}"))
                .unwrap_or_else(|e| panic!("order {order:?} sideB write: {e}"));
        }
        for i in 0..BATCH_ROWS {
            update_name(cluster, 1, BASE_ID + i, &format!("conflict-A-{i}"))
                .unwrap_or_else(|e| panic!("order {order:?} sideA conflict write: {e}"));
        }
        for i in 0..BATCH_ROWS {
            update_name(cluster, 3, BASE_ID + i, &format!("conflict-B-{i}"))
                .unwrap_or_else(|e| panic!("order {order:?} sideB conflict write: {e}"));
        }

        // Each pair converges internally; equal counts but divergent
        // digests prove the split held (otherwise this test would be vacuous).
        wait_converged_on(cluster, &[0, 1], 20, CONVERGE_TIMEOUT);
        wait_converged_on(cluster, &[2, 3], 20, CONVERGE_TIMEOUT);
        let digest_a = digest_rows(&rows(cluster.node(0)));
        let digest_b = digest_rows(&rows(cluster.node(2)));
        if digest_a == digest_b {
            panic!("order {order:?}: sides unexpectedly agree during split ({digest_a})");
        }

        // Heal link-by-link in the prescribed order. After each heal we
        // wait for real cross-side traffic on the new link (not a blind
        // sleep that could elapse before anything flows, making the
        // orders effectively identical).
        for (step, link_idx) in order.iter().enumerate() {
            let link = CROSS_LINKS[*link_idx];
            cluster
                .add_peer(link[0], link[1])
                .unwrap_or_else(|e| panic!("order {order:?} heal {link:?}: {e}"));
            cluster
                .add_peer(link[1], link[0])
                .unwrap_or_else(|e| panic!("order {order:?} heal {link:?}: {e}"));
            eprintln!(
                "order {order:?}: healed link {}/{} ({}<->{})",
                step + 1,
                order.len(),
                link[0],
                link[1]
            );
            wait_link_flowed(cluster, order, link);
        }

        wait_converged(cluster, 30, CONVERGE_TIMEOUT);
        let node_rows = rows(cluster.node(0));
        // Log the LWW winner on a conflicted row: the digest comparison
        // across orders is the assertion, this names the value for forensics.
        let final_digest = digest_rows(&node_rows);
        for (id, name) in &node_rows {
            if *id == BASE_ID {
                eprintln!("order {order:?}: conflict winner row {BASE_ID} = {name}");
                break;
            }
        }
        final_digest
    }));
    match outcome {
        Ok(digest) => {
            cluster.finish(false);
            digest
        }
        Err(payload) => {
            cluster.dump_forensics("three-way-heal-no-convergence");
            cluster.finish(true);
            std::panic::resume_unwind(payload);
        }
    }
}

#[test]
fn heal_order_does_not_affect_final_digest() {
    let order_a = parse_order("MURMUR_THREE_WAY_HEAL_ORDER", "0,1,2,3");
    let order_b = parse_order("MURMUR_THREE_WAY_HEAL_ORDER_B", "3,2,1,0");

    let digest_a = run_heal_order(&order_a, "a");
    let digest_b = run_heal_order(&order_b, "b");
    eprintln!("final digest order {order_a:?}: {digest_a}");
    eprintln!("final digest order {order_b:?}: {digest_b}");
    assert_eq!(
        digest_a, digest_b,
        "heal-order divergence: order {order_a:?} -> {digest_a}, order {order_b:?} -> {digest_b}"
    );
}
