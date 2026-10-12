use sqlite_session::{
    Database, Migration, OpenMode, OpenPhase, OpenProgress, Options, Result,
};
use std::{
    sync::{
        Arc, Mutex,
    },
    time::Duration,
};

fn options() -> Options {
    Options {
        mode: OpenMode::CreateNew,
        capture_interval: Duration::from_millis(50),
        spool_block_bytes: 4096,
        spool_cache_bytes: 65536,
        segment_bytes: 128 << 20,
        ..Default::default()
    }
}

#[test]
fn startup_progress_reports_all_phases_on_clean_open() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let key = [42u8; 32];

    let events = Arc::new(Mutex::new(Vec::<OpenProgress>::new()));
    let ev = events.clone();

    let mut opts = options();
    opts.on_open_progress = Some(Arc::new(move |p| {
        ev.lock().unwrap().push(p);
    }));

    let db = Database::open(&path, key, opts)?;
    let final_status = db.status();
    assert!(final_status.open_progress.is_some());
    let final_progress = final_status.open_progress.unwrap();
    assert_eq!(final_progress.phase, OpenPhase::Ready);
    assert_eq!(db.open_progress().unwrap().phase, OpenPhase::Ready);

    let recorded = events.lock().unwrap().clone();
    assert!(!recorded.is_empty());
    assert_eq!(recorded.first().unwrap().phase, OpenPhase::Opening);
    assert_eq!(recorded.last().unwrap().phase, OpenPhase::Ready);

    db.close()
}

#[test]
fn reload_progress_tracks_rebuilding_tables_and_rows() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let key = [99u8; 32];

    // 1. Create and populate database
    {
        let db = Database::open(&path, key, options())?;
        db.migrate(Migration::new(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        ))?;
        db.migrate(Migration::new(
            "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT NOT NULL)",
        ))?;
        for i in 1..=50 {
            db.transaction(move |s| {
                s.execute("INSERT INTO users VALUES (?, 'user')", [i])?;
                s.execute("INSERT INTO logs VALUES (?, 'action')", [i])?;
                Ok(())
            })?;
        }
        db.flush()?;
        db.close()?;
    }

    // 2. Reopen and observe reload progress
    let events = Arc::new(Mutex::new(Vec::<OpenProgress>::new()));
    let ev = events.clone();

    let mut reopen_opts = options();
    reopen_opts.mode = OpenMode::Existing;
    reopen_opts.on_open_progress = Some(Arc::new(move |p| {
        ev.lock().unwrap().push(p);
    }));

    let db = Database::open(&path, key, reopen_opts)?;
    let recorded = events.lock().unwrap().clone();
    assert!(!recorded.is_empty());

    let phases: Vec<OpenPhase> = recorded.iter().map(|p| p.phase).collect();
    assert!(phases.contains(&OpenPhase::Opening));
    assert!(phases.contains(&OpenPhase::Rebuilding));
    assert!(phases.contains(&OpenPhase::Indexing));
    assert!(phases.contains(&OpenPhase::Finalizing));
    assert!(phases.contains(&OpenPhase::Ready));

    // Confirm row counts during reload
    let terminal = recorded.last().unwrap();
    assert_eq!(terminal.phase, OpenPhase::Ready);
    assert_eq!(terminal.rows_inserted, 100);
    assert_eq!(terminal.rows_skipped, 0);
    assert!(terminal.processed_items >= 100);

    // Verify status contains the completed progress
    let status = db.status();
    let p = status.open_progress.expect("open_progress must be present in status");
    assert_eq!(p.phase, OpenPhase::Ready);
    assert_eq!(p.rows_inserted, 100);

    db.close()
}

#[test]
fn startup_progress_reports_failure_on_corrupt_key() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let key = [11u8; 32];

    // Initialize store
    {
        let db = Database::open(&path, key, options())?;
        db.close()?;
    }

    let events = Arc::new(Mutex::new(Vec::<OpenProgress>::new()));
    let ev = events.clone();

    let mut reopen_opts = options();
    reopen_opts.mode = OpenMode::Existing;
    reopen_opts.on_open_progress = Some(Arc::new(move |p| {
        ev.lock().unwrap().push(p);
    }));

    let wrong_key = [22u8; 32];
    let res = Database::open(&path, wrong_key, reopen_opts);
    assert!(res.is_err());

    let recorded = events.lock().unwrap().clone();
    assert!(!recorded.is_empty());
    assert_eq!(recorded.first().unwrap().phase, OpenPhase::Opening);
    let last = recorded.last().unwrap();
    assert_eq!(last.phase, OpenPhase::Failed);
    assert!(last.error.is_some());

    Ok(())
}

#[test]
fn sqlite_in_memory_metrics_accurately_reflect_db_state() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let key = [33u8; 32];

    let db = Database::open(&path, key, options())?;

    let initial = db.sqlite_metrics()?;
    assert!(initial.memory_used_bytes > 0);
    assert!(initial.memory_highwater_bytes >= initial.memory_used_bytes);
    assert!(initial.page_size > 0);
    assert_eq!(initial.page_count, 0);
    assert_eq!(initial.database_size_bytes, 0);
    assert_eq!(initial.table_count, 0);
    assert_eq!(initial.total_rows, 0);

    // Create tables and rows
    db.migrate(Migration::new(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL NOT NULL)",
    ))?;
    db.migrate(Migration::new(
        "CREATE INDEX idx_balance ON accounts (balance)",
    ))?;

    for i in 1..=20 {
        db.transaction(move |s| {
            s.execute("INSERT INTO accounts VALUES (?, 100.5)", [i])?;
            Ok(())
        })?;
    }

    let populated = db.sqlite_metrics()?;
    assert_eq!(populated.table_count, 1);
    assert_eq!(populated.index_count, 1);
    assert_eq!(populated.total_rows, 20);
    assert!(populated.database_size_bytes >= populated.page_size);

    // After checkpoint/flush, status snapshot carries updated sqlite metrics
    db.flush()?;
    let status = db.status();
    assert!(status.sqlite.is_some());
    let sq = status.sqlite.unwrap();
    assert_eq!(sq.table_count, 1);
    assert_eq!(sq.total_rows, 20);

    db.close()
}

#[test]
fn spool_persistence_metrics_report_storage_internals() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let key = [44u8; 32];

    let db = Database::open(&path, key, options())?;
    db.migrate(Migration::new(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)",
    ))?;

    for i in 1..=10 {
        db.transaction(move |s| {
            s.execute(
                "INSERT INTO docs VALUES (?, 'compressed and encrypted text content repeated repeat')",
                [i],
            )?;
            Ok(())
        })?;
    }
    db.flush()?;

    let sm = db.spool_metrics();
    eprintln!("SPOOL METRICS: {sm:#?}");
    assert!(sm.segments >= 1);
    assert!(sm.disk_bytes > 0);
    assert!(sm.blocks_written > 0);
    assert!(sm.commits >= 1);
    assert!(sm.compression_ratio > 0.0);

    // Status snapshot contains spool metrics
    let status = db.status();
    assert!(status.spool.is_some());
    let st_sm = status.spool.unwrap();
    assert_eq!(st_sm.segments, sm.segments);
    assert_eq!(st_sm.blocks_written, sm.blocks_written);

    db.close()
}

#[cfg(feature = "replication")]
#[test]
fn replication_diagnostics_expose_mesh_and_peer_details() -> Result<()> {
    use murmur_replication::{Config as ReplConfig, Credentials};
    use std::collections::{BTreeMap, BTreeSet};

    let tmp = tempfile::tempdir()?;
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let node_a = [1u8; 16];
    let node_b = [2u8; 16];
    let seed_a = [11u8; 32];
    let seed_b = [22u8; 32];
    let pub_a = murmur_replication::protocol::public_key(&seed_a);
    let pub_b = murmur_replication::protocol::public_key(&seed_b);

    let mut origins = BTreeMap::new();
    origins.insert(node_a, pub_a);
    origins.insert(node_b, pub_b);

    let key_a = rcgen::KeyPair::generate().unwrap();
    let mut params_a = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params_a.subject_alt_names = vec![rcgen::SanType::URI(format!("replicateddb://node/{}", murmur_replication::hex(&node_a)).try_into().unwrap())];
    params_a.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth, rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let cert_a = params_a.signed_by(&key_a, &ca, &ca_key).unwrap();

    let key_b = rcgen::KeyPair::generate().unwrap();
    let mut params_b = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params_b.subject_alt_names = vec![rcgen::SanType::URI(format!("replicateddb://node/{}", murmur_replication::hex(&node_b)).try_into().unwrap())];
    params_b.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth, rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let cert_b = params_b.signed_by(&key_b, &ca, &ca_key).unwrap();

    let creds_a = Credentials {
        chain: vec![cert_a.der().to_vec()],
        private_key: key_a.serialize_der().into(),
        roots: vec![ca.der().to_vec()],
    };
    let creds_b = Credentials {
        chain: vec![cert_b.der().to_vec()],
        private_key: key_b.serialize_der().into(),
        roots: vec![ca.der().to_vec()],
    };

    let mut repl_a = ReplConfig::new("127.0.0.1:0".parse().unwrap(), creds_a, seed_a);
    repl_a.trusted_origins = origins.clone();
    repl_a.allowed_peers = BTreeSet::from([node_a, node_b]);

    let mut repl_b = ReplConfig::new("127.0.0.1:0".parse().unwrap(), creds_b, seed_b);
    repl_b.trusted_origins = origins;
    repl_b.allowed_peers = BTreeSet::from([node_a, node_b]);

    let mut opts_a = options();
    opts_a.database_id = Some([7u8; 16]);
    opts_a.node_id = Some(node_a);
    opts_a.replication = Some(repl_a);

    let mut opts_b = options();
    opts_b.database_id = Some([7u8; 16]);
    opts_b.node_id = Some(node_b);
    opts_b.replication = Some(repl_b);

    let db_a = Database::open(tmp.path().join("node_a"), [9u8; 32], opts_a)?;
    let db_b = Database::open(tmp.path().join("node_b"), [9u8; 32], opts_b)?;

    let handle_a = db_a.replication().expect("replication handle A");
    let handle_b = db_b.replication().expect("replication handle B");

    let addr_b = handle_b.status().address.expect("node B address");
    handle_a.add_peer(addr_b).unwrap();

    // Wait for peer connection
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while db_a.status().replication.as_ref().map(|r| r.peers).unwrap_or(0) == 0 {
        assert!(std::time::Instant::now() < deadline, "peer connection timed out");
        std::thread::sleep(Duration::from_millis(50));
    }

    let status_a = db_a.status();
    let repl_status = status_a.replication.expect("replication status on node A");
    assert_eq!(repl_status.peers, 1);
    assert!(!repl_status.peers_diagnostics.is_empty());

    let peer = &repl_status.peers_diagnostics[0];
    assert_eq!(peer.node_id, node_b);
    assert!(peer.connected);
    assert!(peer.last_seen_millis_ago.is_some());

    assert!(repl_status.metrics.queue_available_bytes > 0);

    db_a.close()?;
    db_b.close()?;
    Ok(())
}
