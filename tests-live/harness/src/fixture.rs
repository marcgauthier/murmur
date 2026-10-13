//! Fixture-subprocess spawn/signal/wait helpers.
//!
//! In-process nodes cannot take `SIGKILL`/`SIGSTOP`/`SIGTERM` without
//! freezing the test driver, so crash/signal legs run the `testnode` fixture
//! binary (this package's `src/bin/testnode.rs`) as a child process.
//! `run.sh` builds it before scenarios; [`binary_path`] locates it.

use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// Controlled-crash exit code (matches the `exit(73)` convention).
pub const CRASH_EXIT_CODE: i32 = 73;

/// Locate the `testnode` fixture binary.
///
/// `MURMUR_TESTNODE_BIN` (then `MURMUR_BIN`, Go parity) overrides the path;
/// otherwise `<harness>/target/debug/testnode`, falling back to release.
pub fn binary_path() -> PathBuf {
    for var in ["MURMUR_TESTNODE_BIN", "MURMUR_BIN"] {
        if let Ok(p) = std::env::var(var) {
            if !p.is_empty() {
                return PathBuf::from(p);
            }
        }
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let debug = root.join("target/debug/testnode");
    if debug.exists() {
        return debug;
    }
    root.join("target/release/testnode")
}

/// Spawn the fixture binary with `args` (stdin nulled).
pub fn spawn(args: &[&str]) -> std::io::Result<Child> {
    Command::new(binary_path())
        .args(args)
        .stdin(Stdio::null())
        .spawn()
}

/// Wait up to `dur` for `child`; `Ok(None)` on timeout (child still running).
pub fn wait_timeout(child: &mut Child, dur: Duration) -> std::io::Result<Option<ExitStatus>> {
    let end = Instant::now() + dur;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if Instant::now() >= end {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Send Unix signal `sig` to `child`. Non-Unix: always unsupported.
#[cfg(unix)]
pub fn signal(child: &mut Child, sig: libc::c_int) -> std::io::Result<()> {
    // SAFETY: kill(2) with a child pid is async-signal-safe and cannot
    // corrupt our own state; ESRCH just means the child already exited.
    let r = unsafe { libc::kill(child.id() as libc::pid_t, sig) };
    if r == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Send Unix signal `sig` to `child`. Non-Unix: always unsupported.
#[cfg(not(unix))]
pub fn signal(_child: &mut Child, _sig: i32) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "signals need a Unix target (TODO: Windows job-object helpers)",
    ))
}

/// `SIGTERM` (graceful-shutdown legs). Non-Unix: falls back to `kill()`.
pub fn terminate(child: &mut Child) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        signal(child, libc::SIGTERM)
    }
    #[cfg(not(unix))]
    {
        child.kill()
    }
}

/// `SIGSTOP` (pause-resume legs).
pub fn stop_process(child: &mut Child) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        signal(child, libc::SIGSTOP)
    }
    #[cfg(not(unix))]
    {
        let _ = child;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "SIGSTOP needs a Unix target",
        ))
    }
}

/// `SIGCONT` (pause-resume legs).
pub fn continue_process(child: &mut Child) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        signal(child, libc::SIGCONT)
    }
    #[cfg(not(unix))]
    {
        let _ = child;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "SIGCONT needs a Unix target",
        ))
    }
}

/// Unconditional kill (`SIGKILL` on Unix via the std runtime).
pub fn kill(child: &mut Child) -> std::io::Result<()> {
    child.kill()
}
