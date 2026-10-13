//! Ephemeral port helpers.
//!
//! QUIC replication nodes bind `127.0.0.1:0` directly and need no
//! coordination (the Go port-claim registry is dropped by design). These
//! best-effort helpers exist only for fixture subprocesses that must be told
//! a port up front; the address can be stolen between `bind` and reuse.

use std::net::SocketAddr;

/// Bind `127.0.0.1:0`, report the address, release it.
pub fn free_addr() -> std::io::Result<SocketAddr> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

/// Just the port half of [`free_addr`].
pub fn free_port() -> std::io::Result<u16> {
    Ok(free_addr()?.port())
}
