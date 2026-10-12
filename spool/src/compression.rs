//! Block compression codecs.
//!
//! Rust port of `murmur/compression` plus `spool/compression.go`.
//!
//! Wire ids: 0 = none (passthrough); 1, 2 = retired (formerly zstd,
//! never reused); 3 = deflate; 4..127 reserved; 128..255 user codecs.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;

use crate::error::{err, ErrorKind, Result};

/// Passthrough codec id.
pub const ID_NONE: u8 = 0;
/// Deflate codec id (raw DEFLATE stream, fastest level).
pub const ID_DEFLATE: u8 = 3;
/// Lowest id a user codec may use.
pub const MIN_USER_ID: u8 = 128;

/// A block compressor. Implementations must be safe for concurrent use.
pub trait Codec: Send + Sync {
    /// On-wire identifier (see the module doc for ranges).
    fn id(&self) -> u8;
    /// Human-readable name used in status and errors.
    fn name(&self) -> &str;
    /// Compress `src`.
    fn compress(&self, src: &[u8]) -> Result<Vec<u8>>;
    /// Decompress `src`, failing rather than producing more than
    /// `max_plain` bytes.
    fn decompress(&self, src: &[u8], max_plain: usize) -> Result<Vec<u8>>;
}

/// Registry resolving wire ids to codecs.
#[derive(Clone)]
pub struct Registry {
    by_id: HashMap<u8, Arc<dyn Codec>>,
}

struct NoneCodec;

impl Codec for NoneCodec {
    fn id(&self) -> u8 {
        ID_NONE
    }

    fn name(&self) -> &str {
        "none"
    }

    fn compress(&self, src: &[u8]) -> Result<Vec<u8>> {
        Ok(src.to_vec())
    }

    fn decompress(&self, src: &[u8], max_plain: usize) -> Result<Vec<u8>> {
        if src.len() > max_plain {
            return Err(err(
                ErrorKind::Corrupt,
                "compression: decompressed size exceeds limit",
            ));
        }
        Ok(src.to_vec())
    }
}

struct DeflateCodec;

impl Codec for DeflateCodec {
    fn id(&self) -> u8 {
        ID_DEFLATE
    }

    fn name(&self) -> &str {
        "deflate"
    }

    fn compress(&self, src: &[u8]) -> Result<Vec<u8>> {
        let mut enc = flate2::write::DeflateEncoder::new(
            Vec::new(),
            flate2::Compression::fast(), // matches Go flate.BestSpeed
        );
        enc.write_all(src)
            .map_err(|e| err(ErrorKind::Io, format!("deflate compress: {e}")))?;
        enc.finish()
            .map_err(|e| err(ErrorKind::Io, format!("deflate compress: {e}")))
    }

    fn decompress(&self, src: &[u8], max_plain: usize) -> Result<Vec<u8>> {
        let mut dec = flate2::read::DeflateDecoder::new(src);
        let mut out = Vec::new();
        let mut limited = (&mut dec).take(max_plain as u64 + 1);
        limited
            .read_to_end(&mut out)
            .map_err(|e| err(ErrorKind::Corrupt, format!("deflate decode: {e}")))?;
        if out.len() > max_plain {
            return Err(err(
                ErrorKind::Corrupt,
                "compression: decompressed size exceeds limit",
            ));
        }
        Ok(out)
    }
}

/// Build a registry holding none, deflate, and the given user codecs.
/// User codecs must use ids in 128..255 and be unique.
pub fn new_registry(user: Vec<Arc<dyn Codec>>) -> Result<Registry> {
    let mut by_id: HashMap<u8, Arc<dyn Codec>> = HashMap::new();
    by_id.insert(ID_NONE, Arc::new(NoneCodec));
    by_id.insert(ID_DEFLATE, Arc::new(DeflateCodec));
    for c in user {
        let id = c.id();
        if id < MIN_USER_ID {
            return Err(err(
                ErrorKind::Invalid,
                format!(
                    "compression: {:?} uses id {id}; user codecs must use 128-255",
                    c.name()
                ),
            ));
        }
        if let Some(prev) = by_id.get(&id) {
            return Err(err(
                ErrorKind::Invalid,
                format!(
                    "compression: id {id} registered twice ({:?}, {:?})",
                    prev.name(),
                    c.name()
                ),
            ));
        }
        by_id.insert(id, c);
    }
    Ok(Registry { by_id })
}

impl Registry {
    /// Resolve a wire id. Retired ids (1, 2) fail as corruption.
    pub fn get(&self, id: u8) -> Result<Arc<dyn Codec>> {
        if let Some(c) = self.by_id.get(&id) {
            return Ok(c.clone());
        }
        if id == 1 || id == 2 {
            return Err(err(
                ErrorKind::Corrupt,
                "compression: retired codec id (zstd removed)",
            ));
        }
        Err(err(
            ErrorKind::Corrupt,
            format!("compression: unknown codec id: {id}"),
        ))
    }
}

/// Block compressor dispatching through the codec registry.
#[derive(Clone)]
pub(crate) struct Compressor {
    mode: u8,
    reg: Registry,
}

impl Compressor {
    pub fn new(mode: u8, user: Vec<Arc<dyn Codec>>) -> Result<Self> {
        let reg = new_registry(user)?;
        if mode != ID_NONE {
            reg.get(mode)
                .map_err(|e| err(ErrorKind::Invalid, format!("compression {mode}: {e}")))?;
        }
        Ok(Compressor { mode, reg })
    }

    pub fn mode(&self) -> u8 {
        self.mode
    }

    /// Encode under an explicit mode so maintenance re-sealing
    /// preserves each block's original codec.
    pub fn compress_with(&self, mode: u8, body: &[u8]) -> Result<Vec<u8>> {
        if mode == ID_NONE {
            return Ok(body.to_vec());
        }
        let cd = self.reg.get(mode)?;
        cd.compress(body)
            .map_err(|e| err(ErrorKind::Corrupt, format!("{} compress: {e}", cd.name())))
    }

    /// Decode one block body, enforcing the exact expected plain size.
    pub fn decompress(&self, mode: u8, src: &[u8], plain_len: u32) -> Result<Vec<u8>> {
        if mode == ID_NONE {
            if src.len() as u64 != plain_len as u64 {
                return Err(err(
                    ErrorKind::Corrupt,
                    format!("plain size {} != {plain_len}", src.len()),
                ));
            }
            return Ok(src.to_vec());
        }
        let cd = self
            .reg
            .get(mode)
            .map_err(|e| err(ErrorKind::Corrupt, format!("block codec id {mode}: {e}")))?;
        let out = cd
            .decompress(src, plain_len as usize)
            .map_err(|e| err(ErrorKind::Corrupt, format!("{} decode: {e}", cd.name())))?;
        if out.len() as u64 != plain_len as u64 {
            return Err(err(
                ErrorKind::Corrupt,
                format!("decoded size {} != {plain_len}", out.len()),
            ));
        }
        Ok(out)
    }
}
