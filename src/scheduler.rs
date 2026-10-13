use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        mpsc::{self, SyncSender},
    },
    time::{Duration, Instant},
};

/// WriterClass identifies a writer scheduling class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WriterClass {
    /// WriterLocal covers local user transactions.
    Local = 0,
    /// WriterRemote covers mesh/High-Low remote apply and schema adoption.
    Remote = 1,
    /// WriterMaintenance covers migrations, rewrites, and retention/GC units.
    Maintenance = 2,
}

impl WriterClass {
    pub const COUNT: usize = 3;

    pub fn as_str(&self) -> &'static str {
        match self {
            WriterClass::Local => "local",
            WriterClass::Remote => "remote",
            WriterClass::Maintenance => "maintenance",
        }
    }
}

/// WriterSchedulingConfig configures writer-time shares and fairness bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerConfig {
    /// Local writer share percentage (e.g. 90).
    pub local_share: u32,
    /// Remote writer share percentage (e.g. 10).
    pub remote_share: u32,
    /// Bound on normalized service debt (default 1s).
    pub max_debt: Duration,
    /// Reserves 1 grant in every N for waiting maintenance work (default 100).
    pub maintenance_reserve_every: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            local_share: 90,
            remote_share: 10,
            max_debt: Duration::from_secs(1),
            maintenance_reserve_every: 100,
        }
    }
}

impl SchedulerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.local_share == 0 || self.remote_share == 0 {
            return Err(Error::Invalid(format!(
                "writer shares must be positive ({}/{})",
                self.local_share, self.remote_share
            )));
        }
        if self.local_share + self.remote_share != 100 {
            return Err(Error::Invalid(format!(
                "writer shares must total 100 ({}/{})",
                self.local_share, self.remote_share
            )));
        }
        if self.max_debt.is_zero() {
            return Err(Error::Invalid("writer max debt must be positive".into()));
        }
        if self.maintenance_reserve_every == 0 {
            return Err(Error::Invalid("maintenance reserve must be positive".into()));
        }
        Ok(())
    }
}

/// Diagnostics for a single writer class.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerClassStats {
    pub acquisitions: u64,
    pub cancels: u64,
    pub wait_nanos: u64,
    pub service_nanos: u64,
    pub dual_service_nanos: u64,
    pub waiters: usize,
    pub oldest_wait_nanos: u64,
}

/// Coordinator diagnostics snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub local: SchedulerClassStats,
    pub remote: SchedulerClassStats,
    pub maintenance: SchedulerClassStats,
    pub debt_nanos: u64,
    pub shares: [u32; 2],
}

struct Waiter {
    id: u64,
    enqueue: Instant,
    sender: SyncSender<Result<bool>>,
}

struct SchedulerInner {
    local_w: u64,
    remote_w: u64,
    max_debt_nanos: u64,
    maintenance_reserve_every: u64,

    closed: bool,
    active: Option<WriterClass>,
    next_waiter_id: u64,
    queues: [VecDeque<Waiter>; WriterClass::COUNT],

    since_maintenance: u64,

    acquisitions: [u64; WriterClass::COUNT],
    cancels: [u64; WriterClass::COUNT],
    wait_nanos: [u64; WriterClass::COUNT],
    service_nanos: [u64; WriterClass::COUNT],
    dual_service_nanos: [u64; WriterClass::COUNT],
    service: [u64; 2],
}

impl SchedulerInner {
    fn queues_empty(&self) -> bool {
        self.queues[0].is_empty() && self.queues[1].is_empty() && self.queues[2].is_empty()
    }

    fn clamp_debt_locked(&mut self) {
        if self.local_w == 0 || self.remote_w == 0 {
            return;
        }
        let v0 = self.service[0] / self.local_w;
        let v1 = self.service[1] / self.remote_w;
        if v0 > v1.saturating_add(self.max_debt_nanos) {
            self.service[0] = v1.saturating_add(self.max_debt_nanos).saturating_mul(self.local_w);
        } else if v1 > v0.saturating_add(self.max_debt_nanos) {
            self.service[1] = v0.saturating_add(self.max_debt_nanos).saturating_mul(self.remote_w);
        }
    }

    fn grant_next_locked(&mut self) {
        if self.closed || self.active.is_some() {
            return;
        }
        let local_q = !self.queues[WriterClass::Local as usize].is_empty();
        let remote_q = !self.queues[WriterClass::Remote as usize].is_empty();
        let maintenance_q = !self.queues[WriterClass::Maintenance as usize].is_empty();

        if !local_q && !remote_q && !maintenance_q {
            return;
        }

        let class = if maintenance_q && self.since_maintenance >= self.maintenance_reserve_every {
            WriterClass::Maintenance
        } else {
            match (local_q, remote_q) {
                (true, true) => {
                    let s_local = self.service[0] as u128 * self.remote_w as u128;
                    let s_remote = self.service[1] as u128 * self.local_w as u128;
                    if s_local <= s_remote {
                        WriterClass::Local
                    } else {
                        WriterClass::Remote
                    }
                }
                (true, false) => WriterClass::Local,
                (false, true) => WriterClass::Remote,
                (false, false) => {
                    if !maintenance_q {
                        return;
                    }
                    WriterClass::Maintenance
                }
            }
        };

        if class == WriterClass::Maintenance {
            self.since_maintenance = 0;
        } else {
            self.since_maintenance += 1;
        }

        let dual = (class == WriterClass::Local && remote_q) || (class == WriterClass::Remote && local_q);

        if let Some(waiter) = self.queues[class as usize].pop_front() {
            self.active = Some(class);
            self.acquisitions[class as usize] += 1;
            let wait = Instant::now().saturating_duration_since(waiter.enqueue).as_nanos() as u64;
            self.wait_nanos[class as usize] += wait;
            let _ = waiter.sender.send(Ok(dual));
        }
    }
}

/// Fair exclusive writer-admission coordinator.
#[derive(Clone)]
pub struct WriterScheduler {
    inner: Arc<Mutex<SchedulerInner>>,
}

impl WriterScheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SchedulerInner {
                local_w: config.local_share as u64,
                remote_w: config.remote_share as u64,
                max_debt_nanos: config.max_debt.as_nanos() as u64,
                maintenance_reserve_every: config.maintenance_reserve_every,
                closed: false,
                active: None,
                next_waiter_id: 1,
                queues: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
                since_maintenance: 0,
                acquisitions: [0; 3],
                cancels: [0; 3],
                wait_nanos: [0; 3],
                service_nanos: [0; 3],
                dual_service_nanos: [0; 3],
                service: [0; 2],
            })),
        }
    }

    /// Queues for writer admission, granting in class-share order.
    pub fn admit(&self, class: WriterClass) -> Result<Ticket> {
        let (rx, _waiter_id) = {
            let mut s = self.inner.lock().unwrap();
            if s.closed {
                return Err(Error::Closed);
            }
            if s.active.is_none() && s.queues_empty() {
                // Immediate idle grant.
                s.active = Some(class);
                if class == WriterClass::Maintenance {
                    s.since_maintenance = 0;
                } else {
                    s.since_maintenance += 1;
                }
                s.acquisitions[class as usize] += 1;
                return Ok(Ticket {
                    class,
                    granted_at: Instant::now(),
                    dual: false,
                    sched: self.inner.clone(),
                    released: false,
                });
            }
            let (tx, rx) = mpsc::sync_channel(1);
            let waiter_id = s.next_waiter_id;
            s.next_waiter_id += 1;
            s.queues[class as usize].push_back(Waiter {
                id: waiter_id,
                enqueue: Instant::now(),
                sender: tx,
            });
            s.grant_next_locked();
            (rx, waiter_id)
        };

        match rx.recv() {
            Ok(Ok(dual)) => Ok(Ticket {
                class,
                granted_at: Instant::now(),
                dual,
                sched: self.inner.clone(),
                released: false,
            }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::Closed),
        }
    }

    /// Queues for writer admission with a timeout.
    pub fn admit_timeout(&self, class: WriterClass, timeout: Duration) -> Result<Ticket> {
        let (rx, waiter_id) = {
            let mut s = self.inner.lock().unwrap();
            if s.closed {
                return Err(Error::Closed);
            }
            if s.active.is_none() && s.queues_empty() {
                s.active = Some(class);
                if class == WriterClass::Maintenance {
                    s.since_maintenance = 0;
                } else {
                    s.since_maintenance += 1;
                }
                s.acquisitions[class as usize] += 1;
                return Ok(Ticket {
                    class,
                    granted_at: Instant::now(),
                    dual: false,
                    sched: self.inner.clone(),
                    released: false,
                });
            }
            let (tx, rx) = mpsc::sync_channel(1);
            let waiter_id = s.next_waiter_id;
            s.next_waiter_id += 1;
            s.queues[class as usize].push_back(Waiter {
                id: waiter_id,
                enqueue: Instant::now(),
                sender: tx,
            });
            s.grant_next_locked();
            (rx, waiter_id)
        };

        match rx.recv_timeout(timeout) {
            Ok(Ok(dual)) => Ok(Ticket {
                class,
                granted_at: Instant::now(),
                dual,
                sched: self.inner.clone(),
                released: false,
            }),
            Ok(Err(e)) => Err(e),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(Error::Closed),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let mut s = self.inner.lock().unwrap();
                let q = &mut s.queues[class as usize];
                if let Some(idx) = q.iter().position(|w| w.id == waiter_id) {
                    q.remove(idx);
                    s.cancels[class as usize] += 1;
                    Err(Error::Busy)
                } else {
                    // Already granted concurrently: grant wins.
                    drop(s);
                    match rx.recv() {
                        Ok(Ok(dual)) => Ok(Ticket {
                            class,
                            granted_at: Instant::now(),
                            dual,
                            sched: self.inner.clone(),
                            released: false,
                        }),
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(Error::Closed),
                    }
                }
            }
        }
    }

    /// Reports whether a ticket is currently active.
    pub fn active(&self) -> bool {
        self.inner.lock().unwrap().active.is_some()
    }

    /// Wakes all waiters with Error::Closed.
    pub fn close(&self) {
        let mut s = self.inner.lock().unwrap();
        if s.closed {
            return;
        }
        s.closed = true;
        for class_idx in 0..WriterClass::COUNT {
            while let Some(w) = s.queues[class_idx].pop_front() {
                let _ = w.sender.send(Err(Error::Closed));
            }
        }
    }

    /// Diagnostics snapshot.
    pub fn snapshot(&self) -> SchedulerSnapshot {
        let s = self.inner.lock().unwrap();
        let now = Instant::now();
        let mut stats = [
            SchedulerClassStats::default(),
            SchedulerClassStats::default(),
            SchedulerClassStats::default(),
        ];
        for class_idx in 0..WriterClass::COUNT {
            let mut oldest = 0u64;
            for w in &s.queues[class_idx] {
                let age = now.saturating_duration_since(w.enqueue).as_nanos() as u64;
                if age > oldest {
                    oldest = age;
                }
            }
            stats[class_idx] = SchedulerClassStats {
                acquisitions: s.acquisitions[class_idx],
                cancels: s.cancels[class_idx],
                wait_nanos: s.wait_nanos[class_idx],
                service_nanos: s.service_nanos[class_idx],
                dual_service_nanos: s.dual_service_nanos[class_idx],
                waiters: s.queues[class_idx].len(),
                oldest_wait_nanos: oldest,
            };
        }
        let v0 = if s.local_w > 0 { s.service[0] / s.local_w } else { 0 };
        let v1 = if s.remote_w > 0 { s.service[1] / s.remote_w } else { 0 };
        let debt_nanos = v0.abs_diff(v1);

        SchedulerSnapshot {
            local: stats[0].clone(),
            remote: stats[1].clone(),
            maintenance: stats[2].clone(),
            debt_nanos,
            shares: [s.local_w as u32, s.remote_w as u32],
        }
    }
}

/// One granted writer admission ticket.
pub struct Ticket {
    class: WriterClass,
    granted_at: Instant,
    dual: bool,
    sched: Arc<Mutex<SchedulerInner>>,
    released: bool,
}

impl Ticket {
    pub fn class(&self) -> WriterClass {
        self.class
    }

    pub fn dual(&self) -> bool {
        self.dual
    }

    pub fn granted_at(&self) -> Instant {
        self.granted_at
    }

    pub fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let service = Instant::now().saturating_duration_since(self.granted_at).as_nanos() as u64;
        let mut s = self.sched.lock().unwrap();
        s.service_nanos[self.class as usize] += service;
        if self.dual {
            s.dual_service_nanos[self.class as usize] += service;
        }
        if self.class == WriterClass::Local {
            s.service[0] += service;
        } else if self.class == WriterClass::Remote {
            s.service[1] += service;
        }
        s.clamp_debt_locked();
        s.active = None;
        s.grant_next_locked();
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_scheduler_idle_borrowing() {
        let s = WriterScheduler::new(SchedulerConfig::default());
        // Solo local transaction grants immediately.
        let mut t1 = s.admit(WriterClass::Local).unwrap();
        assert_eq!(t1.class(), WriterClass::Local);
        assert!(!t1.dual());
        assert!(s.active());
        t1.release();
        assert!(!s.active());

        // Solo remote transaction also grants immediately with 100% capacity.
        let mut t2 = s.admit(WriterClass::Remote).unwrap();
        assert_eq!(t2.class(), WriterClass::Remote);
        assert!(!t2.dual());
        t2.release();
    }

    #[test]
    fn test_scheduler_dual_contention_and_shares() {
        let s = WriterScheduler::new(SchedulerConfig {
            local_share: 80,
            remote_share: 20,
            max_debt: Duration::from_millis(500),
            maintenance_reserve_every: 100,
        });

        // Hold initial ticket.
        let t0 = s.admit(WriterClass::Local).unwrap();

        // Enqueue remote and local waiters concurrently.
        let (ready_tx, ready_rx) = mpsc::channel();
        let s_clone = s.clone();
        let ready_tx_rem = ready_tx.clone();
        let rem_handle = thread::spawn(move || {
            ready_tx_rem.send(()).unwrap();
            let mut t = s_clone.admit(WriterClass::Remote).unwrap();
            assert!(t.dual());
            thread::sleep(Duration::from_millis(10));
            t.release();
        });

        let s_clone2 = s.clone();
        let loc_handle = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let mut t = s_clone2.admit(WriterClass::Local).unwrap();
            thread::sleep(Duration::from_millis(10));
            t.release();
        });

        ready_rx.recv().unwrap();
        ready_rx.recv().unwrap();
        // Give threads a tiny slice to reach queue.
        thread::sleep(Duration::from_millis(20));

        drop(t0); // releases, triggering grantNext

        rem_handle.join().unwrap();
        loc_handle.join().unwrap();

        let snap = s.snapshot();
        assert!(snap.local.acquisitions >= 2);
        assert_eq!(snap.remote.acquisitions, 1);
        assert!(snap.remote.dual_service_nanos > 0);
    }

    #[test]
    fn test_scheduler_maintenance_reserve() {
        let s = WriterScheduler::new(SchedulerConfig {
            local_share: 90,
            remote_share: 10,
            max_debt: Duration::from_secs(1),
            maintenance_reserve_every: 5,
        });

        // Simulate 5 interactive grants with a maintenance waiter queued.
        let t = s.admit(WriterClass::Local).unwrap();

        let s_maint = s.clone();
        let maint_handle = thread::spawn(move || {
            let mut tm = s_maint.admit(WriterClass::Maintenance).unwrap();
            tm.release();
        });

        // Sleep to ensure maintenance waiter is enqueued.
        thread::sleep(Duration::from_millis(20));
        drop(t);

        // Before reserve threshold, local continues if local is queued, but once reserve hits 5,
        // maintenance is granted.
        maint_handle.join().unwrap();
        let snap = s.snapshot();
        assert_eq!(snap.maintenance.acquisitions, 1);
    }

    #[test]
    fn test_scheduler_close_wakes_waiters() {
        let s = WriterScheduler::new(SchedulerConfig::default());
        let _t_held = s.admit(WriterClass::Local).unwrap();

        let s_wait = s.clone();
        let wait_handle = thread::spawn(move || {
            s_wait.admit(WriterClass::Remote)
        });

        thread::sleep(Duration::from_millis(20));
        s.close();

        let err = wait_handle.join().unwrap().err().unwrap();
        assert!(matches!(err, Error::Closed));
    }
}
