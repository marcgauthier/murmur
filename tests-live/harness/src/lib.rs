//! Shared harness for the Rust `tests-live/` scenario ports.
//!
//! The Go suites drive daemon processes over HTTPS+mTLS; the Rust product has
//! no daemon or admin API, so scenarios are in-process multi-[`Database`](sqlite_session::Database)
//! integration tests. This crate provides cluster management, cert
//! provisioning, open-with-watchdog, forensics, artifact retention, and
//! fixture-subprocess helpers. See `tests-live/README.md` for the full design.

pub use murmur_replication;
pub use sqlite_session;
pub use spool;

pub mod artifacts;
pub mod certs;
pub mod cluster;
pub mod fixture;
pub mod forensics;
pub mod ports;
pub mod unlock;

pub use cluster::{
    BridgeOptions, Cluster, ClusterOptions, FilesOptions, Node, ReplicationOptions, SpoolOptions,
    SqliteOptions,
};

/// Harness error. Product errors pass through transparently.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("harness setup: {0}")]
    Setup(String),
    #[error("node index {0} is stopped")]
    NodeStopped(usize),
    #[error("unknown node index {0}")]
    UnknownNode(usize),
    #[error("open timed out after {0:?} ({1})")]
    UnlockTimeout(std::time::Duration, &'static str),
    #[error("fixture: {0}")]
    Fixture(String),
    #[error(transparent)]
    Product(#[from] sqlite_session::Error),
    #[error(transparent)]
    Replication(#[from] murmur_replication::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Cert(#[from] rcgen::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Read a positive-integer seconds env knob with fallback (Go `EnvSeconds` parity).
pub fn env_seconds(name: &str, fallback_secs: u64) -> std::time::Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(fallback_secs))
}

/// Poll `cond` every `poll` until it holds or `timeout` elapses.
/// Returns true when the condition was met (use with `assert!` + forensics).
pub fn wait_until(
    timeout: std::time::Duration,
    poll: std::time::Duration,
    mut cond: impl FnMut() -> bool,
) -> bool {
    let end = std::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if std::time::Instant::now() >= end {
            return false;
        }
        std::thread::sleep(poll);
    }
}
