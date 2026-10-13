//! Rust port of the Go `file-permissions` live scenario.
//!
//! Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/file-permissions/`
//! (`README.md` + `file_permissions_test.go`:
//! `TestFilePermissionsFreshInitAndBackupRestore`). Product-owned secret
//! paths (the `data/keys.enc` key container and the data directory) must
//! grant nothing to group/other (0600 files, 0700 dirs) after fresh init
//! and again after backup plus restore. Harness-minted material
//! (`tls/node.key`) and operator-owned paths are audit-logged, never
//! asserted, since no product change can alter fixture-chosen modes.
//!
//! Plain SQL replaces the Go typed-records HTTP API (the Rust product has
//! no HTTP server); stop/start replaces StopNode/StartNode+unlock; the
//! backup is an online `Database::backup` on the surviving node instead of
//! an offline open of the stopped one; restore uses
//! `Database::restore_with_options` with a fresh node id (Go `RestoreClone`).
//! See the scenario README.md for the full deviation list.

use std::path::{Path, PathBuf};
use std::time::Duration;

use murmur_live_harness::sqlite_session::{Database, Migration, RestoreOptions};
use murmur_live_harness::{wait_until, Cluster, ClusterOptions};
use rand::RngCore;

/// Typed table (Go: `live_typed_records` managed typed table).
const TABLE_DDL: &str = "CREATE TABLE live_typed_records(name TEXT PRIMARY KEY)";
/// Seed rows (Go: 25) and convergence deadlines (Go: 30 s, then 60 s).
const NUM_ROWS: usize = 25;
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);
const RECONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(200);

/// Product-owned key container, relative to the node data dir
/// (Go: `data/keys.enc`; Rust spool keyring file, same name).
const KEYS_FILE_NAME: &str = "keys.enc";

/// Typed insert on one node (Go: `Cluster.TypedInsert`).
fn typed_insert(db: &Database, name: String) {
    db.transaction(move |sql| {
        sql.execute("INSERT INTO live_typed_records(name) VALUES(?)", [name])?;
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

/// Wait until every running node holds the same `want` names (Go: `waitConverged`).
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

/// Fail when any product-owned secret path on node `idx` carries group/other
/// permission bits (Go: `assertSecretPathsLockedDown`). `node.key` is
/// deliberately absent: harness-minted fixture material, audit-logged in
/// `log_mode_table` instead of asserted here.
fn assert_secret_paths_locked_down(cluster: &Cluster, idx: usize, phase: &str) {
    let info = cluster.node_info(idx);
    let keys = info.data_dir.join(KEYS_FILE_NAME);
    assert!(
        keys.is_file(),
        "{phase} node{}: expected product key container at {}",
        idx + 1,
        keys.display()
    );
    for root in [&keys, &info.data_dir] {
        assert_tree_locked_down(
            root,
            &format!("{} node{} {}", phase, idx + 1, root.display()),
        );
    }
}

/// Walk `root` (a file or directory) and fail on the first path whose mode
/// grants anything to group or other (Go: `assertTreeLockedDown`).
/// Symlinks are logged and skipped (their 0777 link modes are not access
/// grants); anything else non-regular fails the same as a violation.
#[cfg(unix)]
fn assert_tree_locked_down(root: &Path, label: &str) {
    use std::os::unix::fs::PermissionsExt;
    let mut violations: Vec<String> = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    let mut seen: u64 = 0;
    while let Some(path) = stack.pop() {
        let meta = std::fs::symlink_metadata(&path)
            .unwrap_or_else(|e| panic!("{label}: stat {}: {e}", path.display()));
        if meta.file_type().is_symlink() {
            eprintln!("{label}: symlink skipped: {}", path.display());
            continue;
        }
        seen += 1;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            violations.push(format!("{} mode={mode:04o}", path.display()));
        }
        if meta.is_dir() {
            let entries = std::fs::read_dir(&path)
                .unwrap_or_else(|e| panic!("{label}: read_dir {}: {e}", path.display()));
            for entry in entries {
                let entry =
                    entry.unwrap_or_else(|e| panic!("{label}: dir entry: {e}"));
                stack.push(entry.path());
            }
        }
    }
    assert!(
        seen > 0,
        "{label}: walked zero paths under {}",
        root.display()
    );
    assert!(
        violations.is_empty(),
        "{label}: {} paths grant group/other access:\n  {}",
        violations.len(),
        join_lines(&violations, 10)
    );
    eprintln!("{label}: {} locked down (no group/other bits)", root.display());
}

#[cfg(not(unix))]
fn assert_tree_locked_down(_root: &Path, _label: &str) {
    // Non-Unix platforms return early with SKIP before any assertion runs.
}

#[cfg(unix)]
fn join_lines(lines: &[String], max: usize) -> String {
    let mut out = String::new();
    for (i, line) in lines.iter().enumerate() {
        if i >= max {
            out.push_str(&format!("\n  ... and {} more", lines.len() - max));
            break;
        }
        if i > 0 {
            out.push_str("\n  ");
        }
        out.push_str(line);
    }
    out
}

/// Record the observed modes of operator-owned paths for audit evidence
/// (Go: `logModeTable`). These modes are chosen by the test fixture, not
/// the product. The Rust harness provisions no logs dir or config file, and
/// its TLS material is DER (`ca.der`/`node.der`/`node.key`).
fn log_mode_table(cluster: &Cluster) {
    for i in 0..cluster.len() {
        let info = cluster.node_info(i);
        let tls = info.dir.join("tls");
        let paths: Vec<PathBuf> = vec![
            info.dir.clone(),
            info.data_dir.clone(),
            tls.clone(),
            tls.join("ca.der"),
            tls.join("node.der"),
            tls.join("node.key"),
        ];
        for p in paths {
            match std::fs::symlink_metadata(&p) {
                Ok(meta) => {
                    eprintln!(
                        "node{} {}: mode={} dir={}",
                        i + 1,
                        p.display(),
                        format_mode(&meta),
                        meta.is_dir()
                    );
                }
                Err(e) => eprintln!("node{} {}: stat error: {e}", i + 1, p.display()),
            }
        }
    }
}

#[cfg(unix)]
fn format_mode(meta: &std::fs::Metadata) -> String {
    use std::os::unix::fs::PermissionsExt;
    format!("{:04o}", meta.permissions().mode() & 0o777)
}

#[cfg(not(unix))]
fn format_mode(_meta: &std::fs::Metadata) -> String {
    "n/a".to_string()
}

#[test]
fn file_permissions_fresh_init_and_backup_restore() {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    let mut cluster = Cluster::new(ClusterOptions {
        name: "file-permissions".into(),
        num_nodes: 2,
        key,
        ..Default::default()
    })
    .expect("cluster setup");
    if cfg!(windows) {
        eprintln!("SKIP: POSIX file permissions (0600/0700) do not apply to Windows NTFS ACLs");
        cluster.finish(false);
        return;
    }
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Phase 1: fresh init. Every node's private key material must
        // already be locked down before any data is written.
        for i in 0..cluster.len() {
            assert_secret_paths_locked_down(&cluster, i, "fresh-init");
        }
        log_mode_table(&cluster);

        for i in 0..cluster.len() {
            cluster
                .node(i)
                .migrate(Migration::new(TABLE_DDL))
                .unwrap_or_else(|e| panic!("node{} migrate failed: {e}", i + 1));
            cluster
                .node(i)
                .flush()
                .unwrap_or_else(|e| panic!("node{} flush after migrate failed: {e}", i + 1));
        }

        // Positive control: honest writes replicate and converge, proving
        // the permission posture does not break the database.
        for i in 0..NUM_ROWS {
            typed_insert(cluster.node(0), format!("row-{i}"));
        }
        wait_converged(&cluster, NUM_ROWS, CONVERGE_TIMEOUT);

        // Phase 2: backup plus restore through the product library, then
        // re-assert: backup artifacts, the restored tree, and the live nodes.
        cluster.stop_node(1);
        let backup_dir = cluster.runtime_dir().join("backup-dest");
        cluster
            .node(0)
            .backup(&backup_dir)
            .expect("backup node 1");
        assert_tree_locked_down(&backup_dir, "backup-artifacts");

        let restore_target = cluster.runtime_dir().join("restored");
        let fresh: [u8; 16] = rand::random();
        Database::restore_with_options(
            &backup_dir,
            &restore_target,
            key,
            RestoreOptions {
                new_database_id: None,
                new_node_id: Some(fresh),
            },
        )
        .expect("restore backup");
        assert_tree_locked_down(&restore_target, "restored-tree");
        // The restored key container and data directory in particular must
        // be private (the backup is a store dir, so it IS the data dir).
        assert_tree_locked_down(&restore_target, "restored-data");

        cluster.start_node(1).expect("restart node 2");
        cluster.mesh().expect("re-mesh after restart");
        wait_converged(&cluster, NUM_ROWS, RECONVERGE_TIMEOUT);

        for i in 0..cluster.len() {
            assert_secret_paths_locked_down(&cluster, i, "post-backup-restore");
        }
        eprintln!("PASS: private key material locked down (0600/0700) fresh and post restore");
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("file-permissions");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
