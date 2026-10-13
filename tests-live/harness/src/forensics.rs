//! Post-failure diagnostics capture (Go `DumpForensics` parity).
//!
//! Status summaries go to the test log (truncated); full states go to
//! `forensics-<reason>-*` files under each node directory (preserved with
//! failure artifacts). Every fetch is infallible: forensics must survive
//! dead and wedged nodes alike, so failures are logged, never fatal.

use std::path::Path;

/// Dump diagnostics for one node. Never panics, never fails the test.
pub fn dump_node_forensics(
    label: &str,
    node_dir: &Path,
    db: Option<&sqlite_session::Database>,
    reason: &str,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        dump_inner(label, node_dir, db, reason);
    }));
}

fn dump_inner(
    label: &str,
    node_dir: &Path,
    db: Option<&sqlite_session::Database>,
    reason: &str,
) {
    let safe_reason: String = reason
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let status = fetch(|| match db {
        Some(d) => format!("{:#?}", d.status()),
        None => "<node stopped>".to_string(),
    });
    eprintln!(
        "forensics({reason}): {label} status: {}",
        truncate(&status, 2048)
    );

    let peers = fetch(|| match db.and_then(|d| d.replication()) {
        Some(h) => format!("{:#?}", h.status()),
        None => "<no replication handle>".to_string(),
    });
    eprintln!(
        "forensics({reason}): {label} replication: {}",
        truncate(&peers, 8192)
    );

    let metrics = fetch(|| match db {
        Some(d) => {
            let sqlite = d
                .sqlite_metrics()
                .map(|m| format!("{m:#?}"))
                .unwrap_or_else(|e| format!("<sqlite metrics: {e}>"));
            format!("sqlite: {sqlite}\nspool: {:#?}", d.spool_metrics())
        }
        None => "<node stopped>".to_string(),
    });

    write_file(node_dir, &format!("forensics-{safe_reason}-status.txt"), &status);
    write_file(node_dir, &format!("forensics-{safe_reason}-peers.txt"), &peers);
    write_file(
        node_dir,
        &format!("forensics-{safe_reason}-metrics.txt"),
        &metrics,
    );
}

/// Run a fetch; panics and empty results degrade to a marker string.
fn fetch(f: impl FnOnce() -> String) -> String {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => "<empty>".to_string(),
        Err(_) => "<fetch panicked>".to_string(),
    }
}

fn write_file(node_dir: &Path, name: &str, body: &str) {
    if body.is_empty() {
        return;
    }
    if let Err(e) = std::fs::write(node_dir.join(name), body) {
        eprintln!("forensics: {name} write failed: {e}");
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() > n {
        let n = s.floor_char_boundary(n);
        format!("{}...<truncated>", &s[..n])
    } else {
        s.to_string()
    }
}
