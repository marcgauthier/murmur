use sqlite_session::{
    Database, Migration, OpenMode, Options, Result, SchedulerConfig, WriterClass,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

fn test_options() -> Options {
    let mut opts = Options::default();
    opts.mode = OpenMode::CreateNew;
    opts.capture_interval = Duration::from_millis(50);
    opts
}

#[test]
fn test_scheduler_shares_converge_under_contention() {
    let cfg = SchedulerConfig {
        local_share: 80,
        remote_share: 20,
        max_debt: Duration::from_millis(200),
        maintenance_reserve_every: 100,
    };
    let sched = Arc::new(sqlite_session::WriterScheduler::new(cfg));
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn 4 local workers and 4 remote workers.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let s = sched.clone();
        let st = stop.clone();
        handles.push(thread::spawn(move || {
            while !st.load(Ordering::Relaxed) {
                if let Ok(mut t) = s.admit(WriterClass::Local) {
                    thread::sleep(Duration::from_millis(1));
                    t.release();
                }
            }
        }));
    }
    for _ in 0..4 {
        let s = sched.clone();
        let st = stop.clone();
        handles.push(thread::spawn(move || {
            while !st.load(Ordering::Relaxed) {
                if let Ok(mut t) = s.admit(WriterClass::Remote) {
                    thread::sleep(Duration::from_millis(1));
                    t.release();
                }
            }
        }));
    }

    thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let snap = sched.snapshot();
    let local = snap.local.service_nanos;
    let remote = snap.remote.service_nanos;
    let total = local + remote;
    assert!(total > 0, "expected service to be recorded");

    let local_ratio = (local as f64) / (total as f64);
    // 80/20 target with tolerance for scheduling jitter
    assert!(
        (0.65..=0.92).contains(&local_ratio),
        "local share should converge around 0.80, got {local_ratio:.3} (local={local}, remote={remote})"
    );
    assert!(snap.local.acquisitions > 0);
    assert!(snap.remote.acquisitions > 0);
    assert!(snap.debt_nanos <= Duration::from_millis(250).as_nanos() as u64);
}

#[test]
fn test_scheduler_bounded_debt_prevents_burst_starvation() {
    let cfg = SchedulerConfig {
        local_share: 90,
        remote_share: 10,
        max_debt: Duration::from_millis(50),
        maintenance_reserve_every: 100,
    };
    let sched = Arc::new(sqlite_session::WriterScheduler::new(cfg));

    // Run 50 local tickets for 1ms each while remote is completely idle.
    for _ in 0..50 {
        let mut t = sched.admit(WriterClass::Local).unwrap();
        thread::sleep(Duration::from_millis(1));
        t.release();
    }

    // Debt should be clamped to at most ~50ms worth of normalized service.
    let snap_before = sched.snapshot();
    assert!(
        snap_before.debt_nanos <= Duration::from_millis(60).as_nanos() as u64,
        "debt was not clamped: {} ns",
        snap_before.debt_nanos
    );

    // Now enqueue remote and local concurrently.
    let t_held = sched.admit(WriterClass::Local).unwrap();

    let s1 = sched.clone();
    let rem_handle = thread::spawn(move || {
        let start = Instant::now();
        let mut t = s1.admit(WriterClass::Remote).unwrap();
        let wait = start.elapsed();
        t.release();
        wait
    });

    let s2 = sched.clone();
    let loc_handle = thread::spawn(move || {
        let mut t = s2.admit(WriterClass::Local).unwrap();
        t.release();
    });

    thread::sleep(Duration::from_millis(10));
    drop(t_held);

    let rem_wait = rem_handle.join().unwrap();
    loc_handle.join().unwrap();

    // Remote should have been granted without waiting for 50 local iterations.
    assert!(
        rem_wait < Duration::from_millis(200),
        "remote waiter took too long ({rem_wait:?}) due to unclamped debt"
    );
}

#[test]
fn test_scheduler_maintenance_reserve_under_heavy_load() {
    let cfg = SchedulerConfig {
        local_share: 90,
        remote_share: 10,
        max_debt: Duration::from_secs(1),
        maintenance_reserve_every: 10, // grant maintenance every 10 interactive grants
    };
    let sched = Arc::new(sqlite_session::WriterScheduler::new(cfg));
    let stop = Arc::new(AtomicBool::new(false));

    // Hold the first grant until both interactive contenders are queued.
    // Otherwise a busy host may run all maintenance passes before either starts.
    let initial = sched.admit(WriterClass::Local).unwrap();
    // Keep interactive classes saturated.
    let mut interactive = Vec::new();
    for _ in 0..2 {
        let s = sched.clone();
        let st = stop.clone();
        interactive.push(thread::spawn(move || {
            while !st.load(Ordering::Relaxed) {
                if let Ok(mut t) = s.admit(WriterClass::Local) {
                    thread::sleep(Duration::from_micros(500));
                    t.release();
                }
            }
        }));
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while sched.snapshot().local.waiters < 2 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    let ready = sched.snapshot().local.waiters >= 2;
    if !ready {
        stop.store(true, Ordering::Relaxed);
        sched.close();
        drop(initial);
        for handle in interactive {handle.join().unwrap();}
        panic!("interactive contenders did not start");
    }
    drop(initial);

    // Maintenance thread runs passes.
    let mut maint_granted = 0;
    for _ in 0..5 {
        let mut t = sched.admit(WriterClass::Maintenance).unwrap();
        maint_granted += 1;
        thread::sleep(Duration::from_millis(1));
        t.release();
    }

    stop.store(true, Ordering::Relaxed);
    for h in interactive {
        h.join().unwrap();
    }

    assert_eq!(maint_granted, 5);
    let snap = sched.snapshot();
    assert!(snap.maintenance.acquisitions >= 5);
    assert!(snap.local.acquisitions >= 10);
}

#[test]
fn test_database_scheduler_integration() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let key = [0x42u8; 32];

    let mut opts = test_options();
    opts.scheduler = SchedulerConfig {
        local_share: 90,
        remote_share: 10,
        max_debt: Duration::from_secs(1),
        maintenance_reserve_every: 50,
    };

    let db = Database::open(&path, key, opts)?;

    // Run a schema migration (maintenance class).
    db.migrate(Migration::new(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
    ))?;

    // Run local write transactions across threads.
    let mut handles = Vec::new();
    for i in 0..5 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            for j in 0..10 {
                let id = i * 100 + j;
                db_clone
                    .transaction(move |sql| {
                        sql.execute(
                            "INSERT INTO test (id, value) VALUES (?, ?)",
                            rusqlite::params![id, format!("val_{id}")],
                        )?;
                        Ok(())
                    })
                    .unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Inspect status snapshot.
    let status = db.status();
    let snap = status.scheduler.expect("scheduler snapshot must be present");

    assert_eq!(snap.shares, [90, 10]);
    assert_eq!(snap.local.acquisitions, 50);
    assert!(snap.local.service_nanos > 0);
    assert_eq!(snap.maintenance.acquisitions, 1); // 1 migration

    db.close()?;
    Ok(())
}
