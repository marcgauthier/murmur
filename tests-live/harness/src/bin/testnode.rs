//! Minimal CLI fixture for crash/signal scenario legs (NOT a daemon).
//!
//! In-process nodes cannot take signals without freezing the test driver,
//! so legs that need `SIGKILL`/`SIGSTOP`/`SIGTERM` (or a controlled crash
//! exit code) spawn this binary as a child via `murmur_live_harness::fixture`
//! and signal the child.
//!
//! Subcommands:
//! - `open --dir D --key-hex K [--mode new|existing] [--insert SQL]
//!   [--ready FILE] [--hold-secs N] [--stop-file F] [--dbid-hex H] [--node-hex H]`
//!   opens a database, optionally runs one `INSERT`-style statement, writes
//!   the ready file, then holds until the stop file appears or the hold
//!   expires; closes cleanly and exits 0.
//! - `wait --secs N`: sleep N seconds, exit 0 (signal-target stub).
//! - `crash [--code N]`: exit N immediately (default 73, the controlled
//!   crash-point convention).
//!
//! TODO (harness follow-ups, no product change):
//! - graceful SIGTERM handling (clean close + marker) via a signal-hook dep;
//!   today SIGTERM kills with default disposition (no cleanup).
//! - replication-enabled fixture mode (QUIC mesh leg for mid-flight kills).

use std::path::PathBuf;
use std::time::{Duration, Instant};

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("testnode: {e}");
            std::process::exit(2);
        }
    }
}

fn run() -> Result<i32, String> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("open") => cmd_open(args.collect()),
        Some("wait") => cmd_wait(args.collect()),
        Some("crash") => cmd_crash(args.collect()),
        Some("version") => {
            println!("testnode {}", env!("CARGO_PKG_VERSION"));
            Ok(0)
        }
        Some(other) => Err(format!("unknown subcommand: {other}")),
        None => Err("usage: testnode <open|wait|crash|version> ...".into()),
    }
}

fn flag_value(args: &[String], name: &str) -> Option<String> {
    let mut iter = args.iter().peekable();
    while let Some(a) = iter.next() {
        if let Some(v) = a.strip_prefix(&format!("{name}=")) {
            return Some(v.to_string());
        }
        if a == name {
            return iter.next().cloned();
        }
    }
    None
}

fn hex_bytes(hex: &str, expect: usize, what: &str) -> Result<Vec<u8>, String> {
    if hex.len() != expect * 2 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!("{what}: need {} hex chars", expect * 2));
    }
    (0..expect)
        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn cmd_open(args: Vec<String>) -> Result<i32, String> {
    let dir: PathBuf = flag_value(&args, "--dir")
        .ok_or("--dir is required")?
        .into();
    let key_hex = flag_value(&args, "--key-hex").ok_or("--key-hex is required")?;
    let key = hex_bytes(&key_hex, 32, "--key-hex")?;
    let mode = match flag_value(&args, "--mode").as_deref().unwrap_or("new") {
        "new" => sqlite_session::OpenMode::CreateNew,
        "existing" => sqlite_session::OpenMode::Existing,
        m => return Err(format!("--mode must be new|existing, got {m}")),
    };
    let mut opts = sqlite_session::Options {
        mode,
        ..sqlite_session::Options::default()
    };
    if let Some(h) = flag_value(&args, "--dbid-hex") {
        let b = hex_bytes(&h, 16, "--dbid-hex")?;
        opts.database_id = Some(b.try_into().unwrap());
    }
    if let Some(h) = flag_value(&args, "--node-hex") {
        let b = hex_bytes(&h, 16, "--node-hex")?;
        opts.node_id = Some(b.try_into().unwrap());
    }

    let db = sqlite_session::Database::open(&dir, key, opts).map_err(|e| e.to_string())?;
    if let Some(sql) = flag_value(&args, "--insert") {
        db.transaction(move |s| {
            s.execute(&sql, [])?;
            Ok(())
        })
        .map_err(|e| e.to_string())?;
        db.flush().map_err(|e| e.to_string())?;
    }
    if let Some(ready) = flag_value(&args, "--ready") {
        std::fs::write(&ready, format!("ready pid={}\n", std::process::id()))
            .map_err(|e| e.to_string())?;
    }

    let hold = flag_value(&args, "--hold-secs")
        .map(|v| v.parse::<u64>().map_err(|e| e.to_string()))
        .transpose()?
        .unwrap_or(u64::MAX);
    let stop_file = flag_value(&args, "--stop-file").map(PathBuf::from);
    let end = Instant::now() + Duration::from_secs(hold);
    loop {
        if let Some(f) = &stop_file {
            if f.exists() {
                break;
            }
        }
        if Instant::now() >= end {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    db.close().map_err(|e| e.to_string())?;
    Ok(0)
}

fn cmd_wait(args: Vec<String>) -> Result<i32, String> {
    let secs: u64 = flag_value(&args, "--secs")
        .ok_or("--secs is required")?
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    std::thread::sleep(Duration::from_secs(secs));
    Ok(0)
}

fn cmd_crash(args: Vec<String>) -> Result<i32, String> {
    let code: i32 = flag_value(&args, "--code")
        .map(|v| v.parse::<i32>().map_err(|e| e.to_string()))
        .transpose()?
        .unwrap_or(73);
    Ok(code)
}
