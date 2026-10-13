//! Rust port of the Go `backup-under-fire` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/backup-under-fire/`
//! (`README.md` + `backup_under_fire_test.go` + `backupagent/main.go`,
//! `TestBackupUnderFire`). One `#[test]` fn:
//!
//! - `backup_under_fire`: a 2-node mesh absorbs sustained writes (25 ms
//!   ticker per node) for `MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS` (default
//!   30) with zero failed writes. A third member ("agent A", cluster node 3,
//!   unmeshed at boot) joins mid-load, converges, and takes two online
//!   backups (b1, b2) from its live handle without stopping. A truncated
//!   backup remnant must be rejected by `verify_backup` and
//!   `restore_with_options`, and the failed restore must publish no target
//!   dir (the in-process equivalent of the Go SIGKILL-mid-backup leg; see
//!   README.md). After quiescence a FRESH out-of-band agent ("agent B": new
//!   data dir, new node identity, own TLS leaf) joins, converges on the
//!   quiesced total, takes the final online backup (b4), and closes cleanly.
//!   Every completed backup restores: b1/b2 are non-empty, non-decreasing,
//!   and subsets of the survivors; b4 matches the survivors exactly (count
//!   plus canonical ordered names).
//!
//! Plain SQL replaces the Go typed-records mesh API (the Rust product has no
//! HTTP server); the Go backup agent subprocess becomes an in-process third
//! member plus a fresh out-of-band `Database` open. See README.md.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use murmur_live_harness::sqlite_session::{
    Database, Migration, OpenMode, Options, RestoreOptions,
};
use murmur_live_harness::{env_seconds, wait_until, Cluster, ClusterOptions};
use rand::RngCore;

/// Mesh-leg DDL: plain replicated rows (Go: `TypedInsert` names). Each Go
/// write inserts a fresh unique row, so the typed merge policies never
/// trigger; a plain table is behaviorally identical here.
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT PRIMARY KEY)";
/// Per-writer cadence (Go: 25 ms ticker on each of the 2 mesh nodes).
const WRITE_TICK: Duration = Duration::from_millis(25);
const POLL: Duration = Duration::from_millis(200);
/// Agent join/converge deadlines (Go: WAIT ... 120).
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(120);
/// Post-quiesce mesh convergence (Go: 90 s).
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(90);

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

/// One writer attempt (Go: `Cluster.TypedInsert`). The caller counts the
/// outcome; like Go, a failed write does not stop the writer.
fn try_insert(
    db: &Database,
    name: String,
) -> Result<(), murmur_live_harness::sqlite_session::Error> {
    db.transaction(move |sql| {
        sql.execute(
            "INSERT INTO live_typed_records(name) VALUES(?)",
            [name],
        )?;
        Ok(())
    })?;
    db.flush()?;
    Ok(())
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

fn count_rows(db: &Database) -> usize {
    typed_names(db).len()
}

/// Wait until every listed node holds the same `want` names (Go:
/// `waitConverged`).
fn wait_converged(cluster: &Cluster, nodes: &[usize], want: usize, timeout: Duration) {
    let ok = wait_until(timeout, POLL, || {
        let mut first: Option<Vec<String>> = None;
        for &i in nodes {
            let names = typed_names(cluster.node(i));
            if names.len() != want {
                return false;
            }
            match &first {
                None => first = Some(names),
                Some(want_names) if *want_names == names => {}
                Some(_) => return false,
            }
        }
        true
    });
    assert!(
        ok,
        "nodes {nodes:?} did not converge on {want} rows within {timeout:?}"
    );
}

/// Recursive directory copy (std only).
fn copy_dir_recursive(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst)
        .unwrap_or_else(|e| panic!("create {} failed: {e}", dst.display()));
    let entries = std::fs::read_dir(src)
        .unwrap_or_else(|e| panic!("read {} failed: {e}", src.display()));
    for entry in entries {
        let entry = entry.expect("dir entry");
        let target = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target)
                .unwrap_or_else(|e| panic!("copy {} failed: {e}", target.display()));
        }
    }
}

/// Open a fresh out-of-band agent database: new data dir, fresh node
/// identity, freshly issued TLS leaf, cluster CA trust, both survivors as
/// bootstrap + trusted snapshot sources (Go: `startAgent` + `addAgentPeer`).
/// The agent never originates rows, so peers need not trust its origin key.
fn open_agent(cluster: &Cluster, key: [u8; 32], data_dir: &Path, fresh: [u8; 16]) -> Database {
    use murmur_live_harness::murmur_replication as repl;
    let issued = cluster.issue_cert(&fresh).expect("issue agent cert");
    let seed: [u8; 32] = rand::random();
    let mut trusted: BTreeMap<[u8; 16], [u8; 32]> = BTreeMap::new();
    for i in 0..2 {
        trusted.insert(cluster.node_id(i), cluster.node_origin_pubkey(i));
    }
    trusted.insert(fresh, repl::protocol::public_key(&seed));
    let mut config = repl::Config::new(
        "127.0.0.1:0".parse().unwrap(),
        issued.credentials,
        seed,
    );
    config.trusted_origins = trusted;
    config.trusted_snapshot_sources = BTreeSet::from([cluster.node_id(0), cluster.node_id(1)]);
    config.bootstrap = vec![
        cluster.node_info(0).repl_addr,
        cluster.node_info(1).repl_addr,
    ];
    let db = murmur_live_harness::unlock::open_with_watchdog(
        data_dir,
        key,
        Options {
            mode: OpenMode::CreateNew,
            database_id: Some(cluster.database_id()),
            node_id: Some(fresh),
            replication: Some(config),
            ..Options::default()
        },
    )
    .expect("open agent database");
    db.migrate(Migration::new(TABLE_DDL))
        .expect("agent migrate");
    db.flush().expect("agent flush schema");
    db
}

/// Restore `backup_dir` under a fresh writer identity (Go: `restoreTo` with
/// `RestoreClone`) and return the restored canonical names.
fn restore_names(
    backup_dir: &Path,
    target: &Path,
    key: [u8; 32],
    database_id: [u8; 16],
) -> Vec<String> {
    let fresh: [u8; 16] = rand::random();
    Database::restore_with_options(
        backup_dir,
        target,
        key,
        RestoreOptions {
            new_database_id: None,
            new_node_id: Some(fresh),
        },
    )
    .unwrap_or_else(|e| panic!("restore {} failed: {e}", backup_dir.display()));
    let db = Database::open(
        target,
        key,
        Options {
            mode: OpenMode::Existing,
            database_id: Some(database_id),
            node_id: Some(fresh),
            ..Options::default()
        },
    )
    .expect("offline open of restored backup");
    let names = typed_names(&db);
    db.close().expect("close restored handle");
    names
}

#[test]
fn backup_under_fire() {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    // Explicit ids so the late-joining agent slot can trust snapshots from
    // the two mesh nodes (Go: agent `snapshotSources = peers`).
    let node_ids: Vec<[u8; 16]> = (0..3).map(|_| rand::random()).collect();
    let mut cluster = Cluster::new(ClusterOptions {
        name: "backup-under-fire".into(),
        num_nodes: 3,
        node_ids: node_ids.clone(),
        key,
        manual_peers: true,
        trusted_snapshot_sources: HashMap::from([(
            2,
            BTreeSet::from([node_ids[0], node_ids[1]]),
        )]),
        ..Default::default()
    })
    .expect("cluster setup");
    let write_dur = env_seconds("MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS", 30);
    let backups = cluster.runtime_dir().join("backups");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Two-node mesh first; node 3 ("agent A") runs unmeshed until it
        // joins mid-load below.
        cluster.add_peer(0, 1).expect("mesh 0->1");
        cluster.add_peer(1, 0).expect("mesh 1->0");
        migrate_all(&cluster, TABLE_DDL);

        let stop = AtomicBool::new(false);
        let ok = AtomicI64::new(0);
        let failed_writes = AtomicI64::new(0);
        let b1 = backups.join("b1");
        let b2 = backups.join("b2");
        let load_start = Instant::now();
        // Backstop so scoped writers always terminate (even if the main leg
        // panics before setting `stop`).
        let writer_deadline = load_start + write_dur + Duration::from_secs(120);
        std::thread::scope(|s| {
            // Sustained writes on both mesh nodes for the whole load phase.
            for w in 0..2 {
                let db = cluster.node(w).clone();
                let stop = &stop;
                let ok = &ok;
                let failed_writes = &failed_writes;
                s.spawn(move || {
                    let mut seq: u64 = 0;
                    loop {
                        if stop.load(Ordering::SeqCst) || Instant::now() >= writer_deadline {
                            return;
                        }
                        std::thread::sleep(WRITE_TICK);
                        if stop.load(Ordering::SeqCst) {
                            return;
                        }
                        seq += 1;
                        match try_insert(&db, format!("w{w}-{seq}")) {
                            Ok(()) => {
                                ok.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(e) => {
                                eprintln!("writer {w} seq {seq} failed: {e}");
                                failed_writes.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                });
            }

            // Agent run A: join mid-load, converge (Go: WAIT), then take two
            // fast online backups from the live handle — zero downtime, the
            // node never stops.
            cluster.mesh().expect("agent A join mesh");
            let snap = count_rows(cluster.node(0));
            let target = snap.max(100);
            let joined =
                wait_until(CONVERGE_TIMEOUT, POLL, || count_rows(cluster.node(2)) >= target);
            assert!(
                joined,
                "agent A did not converge on {target} rows mid-load within {CONVERGE_TIMEOUT:?}"
            );
            eprintln!(
                "agent A converged mid-load: {} rows (target {target})",
                count_rows(cluster.node(2))
            );
            cluster.node(2).backup(&b1).expect("online backup b1");
            cluster.node(2).backup(&b2).expect("online backup b2");
            Database::verify_backup(&b1, key).expect("verify b1");
            Database::verify_backup(&b2, key).expect("verify b2");
            eprintln!("online backups mid-load: b1 b2 verified");

            // Interrupted-backup equivalence (Go: throttled backup SIGKILLed
            // mid-stream): a truncated remnant — stream cut before the
            // manifest completed — must fail closed, and the failed restore
            // must publish no target dir (Go: no intent file).
            let remnant = cluster.runtime_dir().join("remnant");
            copy_dir_recursive(&b1, &remnant);
            std::fs::remove_file(remnant.join("manifest")).expect("cut remnant manifest");
            match Database::verify_backup(&remnant, key) {
                Ok(()) => panic!("truncated remnant verified, want rejection"),
                Err(e) => eprintln!("truncated remnant rejected by verify: {e}"),
            }
            let remnant_target = cluster.runtime_dir().join("remnant-restore");
            let remnant_fresh: [u8; 16] = rand::random();
            match Database::restore_with_options(
                &remnant,
                &remnant_target,
                key,
                RestoreOptions {
                    new_database_id: None,
                    new_node_id: Some(remnant_fresh),
                },
            ) {
                Ok(()) => panic!("truncated remnant restored, want rejection"),
                Err(e) => eprintln!("truncated remnant rejected by restore: {e}"),
            }
            assert!(
                !remnant_target.exists(),
                "failed restore published {}",
                remnant_target.display()
            );

            // Keep the load up for the configured duration, then quiesce.
            let elapsed = load_start.elapsed();
            if elapsed < write_dur {
                std::thread::sleep(write_dur - elapsed);
            }
            stop.store(true, Ordering::SeqCst);
        });

        let failed_total = failed_writes.load(Ordering::SeqCst);
        let total = ok.load(Ordering::SeqCst) as usize;
        assert_eq!(
            failed_total, 0,
            "{failed_total} writes failed during backup load, want zero-downtime"
        );
        eprintln!("load phase: {total} writes, zero failures");
        // Agent A leaves (it never originated rows, so it holds no unique
        // data); the survivors must converge on every write.
        cluster.stop_node(2);
        wait_converged(&cluster, &[0, 1], total, QUIESCE_TIMEOUT);
        let survivors = typed_names(cluster.node(0));

        // Agent run B: rejoin fresh, converge on the quiesced total, take the
        // final online backup, and exit cleanly.
        let agent_id: [u8; 16] = rand::random();
        let agent_root = cluster.runtime_dir().join("agent-b");
        std::fs::create_dir_all(&agent_root).expect("agent-b root");
        let agent = open_agent(&cluster, key, &agent_root.join("data"), agent_id);
        let agent_handle = agent.replication().expect("agent replication handle");
        let agent_addr = {
            let end = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(addr) = agent_handle.status().address {
                    break addr;
                }
                assert!(
                    Instant::now() < end,
                    "agent endpoint did not come up within 10s"
                );
                std::thread::sleep(POLL);
            }
        };
        for i in 0..2 {
            cluster.node(i)
                .replication()
                .expect("node replication handle")
                .add_peer(agent_addr)
                .expect("dial agent");
            agent_handle
                .add_peer(cluster.node_info(i).repl_addr)
                .expect("agent dial seed");
        }
        let joined = wait_until(CONVERGE_TIMEOUT, POLL, || count_rows(&agent) >= total);
        assert!(
            joined,
            "agent B did not converge on {total} rows within {CONVERGE_TIMEOUT:?}"
        );
        let b4 = backups.join("b4");
        agent.backup(&b4).expect("final online backup b4");
        Database::verify_backup(&b4, key).expect("verify b4");
        agent.close().expect("agent clean close");
        eprintln!("final online backup b4 verified");

        // Restore every completed backup: intermediates must be non-empty,
        // non-decreasing, and subsets of the final data; the final backup
        // must match the survivors exactly (zero lost rows).
        let runtime: PathBuf = cluster.runtime_dir().clone();
        let dbid = cluster.database_id();
        let b1_names = restore_names(&b1, &runtime.join("restore-b1"), key, dbid);
        let b2_names = restore_names(&b2, &runtime.join("restore-b2"), key, dbid);
        assert!(
            !b1_names.is_empty() && b2_names.len() >= b1_names.len(),
            "intermediate backup counts {}/{} not positive non-decreasing",
            b1_names.len(),
            b2_names.len()
        );
        let final_set: HashSet<String> = survivors.iter().cloned().collect();
        for (label, names) in [("b1", &b1_names), ("b2", &b2_names)] {
            for name in names {
                assert!(
                    final_set.contains(name),
                    "backup {label} restores phantom row {name}"
                );
            }
            eprintln!(
                "backup {label}: {} rows all present in final set",
                names.len()
            );
        }
        let b4_names = restore_names(&b4, &runtime.join("restore-b4"), key, dbid);
        assert_eq!(
            b4_names.len(),
            total,
            "final backup count = {}, want {total} (lost rows)",
            b4_names.len()
        );
        assert_eq!(
            b4_names, survivors,
            "final backup names differ from survivors"
        );
        eprintln!(
            "PASS: restored b1={} b2={} b4={} rows; final matches survivors exactly",
            b1_names.len(),
            b2_names.len(),
            b4_names.len()
        );
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("backup-under-fire");
    }
    cluster.finish(failed);
    assert!(
        !failed,
        "scenario failed; artifacts retained under tests-live/failures/"
    );
}
