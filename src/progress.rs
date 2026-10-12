use crate::{OpenPhase, OpenProgress, Result};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct ProgressInner {
    started_at: Instant,
    phase_started: Instant,
    last_emit: Instant,
    progress: OpenProgress,
    callback: Arc<dyn Fn(OpenProgress) + Send + Sync>,
}

impl ProgressInner {
    fn update_timings(&mut self, now: Instant) {
        self.progress.elapsed = now.saturating_duration_since(self.started_at);
        self.progress.phase_elapsed = now.saturating_duration_since(self.phase_started);
    }

    fn emit(&mut self, now: Instant) {
        self.update_timings(now);
        self.last_emit = now;
        let snap = self.progress.clone();
        (self.callback)(snap);
    }
}

/// Thread-safe reload / startup progress reporter that delivers updates
/// without locking storage or SQLite engine data paths.
#[derive(Clone)]
pub(crate) struct OpenProgressReporter {
    inner: Option<Arc<Mutex<ProgressInner>>>,
}

impl OpenProgressReporter {
    pub fn new(callback: Option<Arc<dyn Fn(OpenProgress) + Send + Sync>>) -> Self {
        let Some(callback) = callback else {
            return Self { inner: None };
        };
        let now = Instant::now();
        let initial = OpenProgress {
            phase: OpenPhase::Opening,
            elapsed: Duration::ZERO,
            phase_elapsed: Duration::ZERO,
            processed_items: 0,
            current_table: None,
            rows_inserted: 0,
            rows_skipped: 0,
            error: None,
        };
        let inner = Arc::new(Mutex::new(ProgressInner {
            started_at: now,
            phase_started: now,
            last_emit: now,
            progress: initial.clone(),
            callback: callback.clone(),
        }));
        // Emit initial Opening phase event
        callback(initial);
        Self { inner: Some(inner) }
    }

    pub fn phase(&self, phase: OpenPhase) {
        let Some(inner) = &self.inner else { return };
        let mut g = inner.lock().unwrap();
        if g.progress.phase != phase {
            let now = Instant::now();
            g.progress.phase = phase;
            g.phase_started = now;
            g.progress.current_table = None;
            g.emit(now);
        }
    }

    pub fn set_table(&self, table: impl Into<String>) {
        let Some(inner) = &self.inner else { return };
        let mut g = inner.lock().unwrap();
        let now = Instant::now();
        g.progress.current_table = Some(table.into());
        g.emit(now);
    }

    pub fn add_processed_items(&self, delta: u64) {
        let Some(inner) = &self.inner else { return };
        let mut g = inner.lock().unwrap();
        g.progress.processed_items = g.progress.processed_items.saturating_add(delta);
        let now = Instant::now();
        if now.saturating_duration_since(g.last_emit) >= Duration::from_millis(200) {
            g.emit(now);
        }
    }

    pub fn add_rows(&self, inserted: u64, skipped: u64) {
        let Some(inner) = &self.inner else { return };
        let mut g = inner.lock().unwrap();
        g.progress.rows_inserted = g.progress.rows_inserted.saturating_add(inserted);
        g.progress.rows_skipped = g.progress.rows_skipped.saturating_add(skipped);
        g.progress.processed_items = g.progress.processed_items.saturating_add(inserted + skipped);
        let now = Instant::now();
        if now.saturating_duration_since(g.last_emit) >= Duration::from_millis(200) {
            g.emit(now);
        }
    }

    #[allow(dead_code)]
    pub fn snapshot(&self) -> Option<OpenProgress> {
        let inner = self.inner.as_ref()?;
        let mut g = inner.lock().unwrap();
        let now = Instant::now();
        g.update_timings(now);
        Some(g.progress.clone())
    }

    pub fn finish(&self, result: &Result<()>) -> Option<OpenProgress> {
        let inner = self.inner.as_ref()?;
        let mut g = inner.lock().unwrap();
        let now = Instant::now();
        match result {
            Ok(()) => {
                g.progress.phase = OpenPhase::Ready;
                g.progress.error = None;
            }
            Err(e) => {
                g.progress.phase = OpenPhase::Failed;
                g.progress.error = Some(e.to_string());
            }
        }
        g.progress.current_table = None;
        g.emit(now);
        Some(g.progress.clone())
    }
}
