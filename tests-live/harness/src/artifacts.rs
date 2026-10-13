//! Runtime-dir lifecycle, failure-artifact retention, and the `RESULT` line.
//!
//! Convention (Go parity): each scenario owns `tests-live/runtime/<scenario>/`
//! (or `$MURMUR_LIVE_RUNTIME/<scenario>/`). On failure the whole directory
//! moves to `tests-live/failures/<UTC-stamp>-<scenario>/`; on pass it is
//! removed. Every scenario prints exactly one `RESULT: ...` line.

use std::path::{Path, PathBuf};

/// `tests-live/` directory, derived from this crate's manifest location.
pub fn tests_live_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("harness lives directly under tests-live")
        .to_path_buf()
}

/// Runtime dir for `scenario`, honoring `MURMUR_LIVE_RUNTIME`.
pub fn runtime_dir(scenario: &str) -> PathBuf {
    match std::env::var("MURMUR_LIVE_RUNTIME") {
        Ok(root) if !root.is_empty() => PathBuf::from(root).join(scenario),
        _ => tests_live_dir().join("runtime").join(scenario),
    }
}

/// Failure-artifact root, honoring `MURMUR_LIVE_FAILURES`.
pub fn failures_dir() -> PathBuf {
    match std::env::var("MURMUR_LIVE_FAILURES") {
        Ok(root) if !root.is_empty() => PathBuf::from(root),
        _ => tests_live_dir().join("failures"),
    }
}

/// Fresh empty runtime dir for `scenario` (removes any previous run).
pub fn prepare_runtime_dir(scenario: &str) -> std::io::Result<PathBuf> {
    let dir = runtime_dir(scenario);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Retain-or-clean `runtime_dir` and print the `RESULT` line. Infallible.
pub fn finish(scenario: &str, runtime_dir: &Path, failed: bool) {
    if failed {
        let failures = failures_dir();
        let _ = std::fs::create_dir_all(&failures);
        let dest = failures.join(format!("{}-{scenario}", utc_stamp()));
        match std::fs::rename(runtime_dir, &dest) {
            Ok(()) => println!(
                "RESULT: FAIL scenario={scenario} | retained artifacts at {}",
                dest.display()
            ),
            Err(e) => println!(
                "RESULT: FAIL scenario={scenario} | artifact retention failed ({e}); runtime left at {}",
                runtime_dir.display()
            ),
        }
    } else {
        let _ = std::fs::remove_dir_all(runtime_dir);
        println!("RESULT: PASS scenario={scenario} (runtime cleaned up)");
    }
}

/// Current UTC time as `YYYYMMDDTHHMMSSZ` (Go `20060102T150405Z` layout).
pub fn utc_stamp() -> String {
    let t = time::OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}
