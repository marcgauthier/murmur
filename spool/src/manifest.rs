//! Manifest v3.
//!
//! Rust port of `spool/manifest.go`. All integers little-endian.
//!
//! ```text
//! off  size  field
//! 0    4     magic "SPLM"
//! 4    2     format version (3)
//! 6    2     flags (bit0 = encrypted, bit1 = clean shutdown)
//! 8    4     body length N
//! 12   4     header CRC32-Castagnoli over bytes [0,12)
//! 16   N     body (see below)
//! 16+N 4     body CRC32-Castagnoli over body bytes
//! ```
//!
//! Body:
//!
//! ```text
//! off    size  field
//! 0      16    random store id
//! 16     16    database context id
//! 32     8     next file id
//! 40     8     next block id
//! 48     2     next epoch (sequence epoch to use on the NEXT open)
//! 50     4     current data key id (0 when unencrypted)
//! 54     8     active file id
//! 62     8     manifest generation (bumped on membership change)
//! 70     8     next group id (repaired from scan)
//! 78     8     required feature flags (unknown bits rejected)
//! 86     8     optional feature flags (ignored)
//! 94     4     member count M
//! 98     8*M   member file ids, ascending (authoritative segments)
//! 98+8M  8     clean total segment bytes (clean close only)
//! 106+8M 4     clean segment file count (clean close only)
//! 110+8M 8     clean manifest generation (clean close only)
//! 118+8M 8     clean last appended group id (clean close only)
//! ```

use std::fs;
use std::path::Path;

use rand::RngCore;

use crate::block::crc32c;
use crate::error::{err, ErrorKind, Result};

pub(crate) const MANIFEST_MAGIC: &[u8; 4] = b"SPLM";
pub(crate) const MANIFEST_VERSION: u16 = 3;
pub(crate) const MANIFEST_FLAG_ENCRYPTED: u16 = 1 << 0;
pub(crate) const MANIFEST_FLAG_CLEAN: u16 = 1 << 1;
pub(crate) const FEATURE_GROUPS_V2: u64 = 1 << 0;
pub(crate) const FEATURE_CRYPTO_V3: u64 = 1 << 1;
pub(crate) const MANIFEST_FILE_NAME: &str = "manifest";
pub(crate) const SEGMENTS_DIR_NAME: &str = "segments";
/// Fixed body size before members.
pub(crate) const MANIFEST_BODY_BASE: usize = 126;

/// Parsed store-level state.
#[derive(Debug, Clone)]
pub(crate) struct Manifest {
    pub encrypted: bool,
    pub store_id: [u8; 16],
    pub context: [u8; 16],
    pub next_file_id: u64,
    pub next_block_id: u64,
    pub next_epoch: u16,
    pub current_key_id: u32,
    pub active_file_id: u64,
    pub generation: u64,
    pub next_group_id: u64,
    pub req_features: u64,
    /// Authoritative segments, ascending.
    pub members: Vec<u64>,
    pub clean: bool,
    pub clean_bytes: u64,
    pub clean_files: u32,
    pub clean_gen: u64,
    pub clean_group: u64,
}

impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(MANIFEST_BODY_BASE + 8 * self.members.len());
        body.extend_from_slice(&self.store_id);
        body.extend_from_slice(&self.context);
        body.extend_from_slice(&self.next_file_id.to_le_bytes());
        body.extend_from_slice(&self.next_block_id.to_le_bytes());
        body.extend_from_slice(&self.next_epoch.to_le_bytes());
        body.extend_from_slice(&self.current_key_id.to_le_bytes());
        body.extend_from_slice(&self.active_file_id.to_le_bytes());
        body.extend_from_slice(&self.generation.to_le_bytes());
        body.extend_from_slice(&self.next_group_id.to_le_bytes());
        body.extend_from_slice(
            &(self.req_features | FEATURE_GROUPS_V2 | FEATURE_CRYPTO_V3).to_le_bytes(),
        );
        body.extend_from_slice(&0u64.to_le_bytes()); // optional features
        body.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for id in &self.members {
            body.extend_from_slice(&id.to_le_bytes());
        }
        body.extend_from_slice(&self.clean_bytes.to_le_bytes());
        body.extend_from_slice(&self.clean_files.to_le_bytes());
        body.extend_from_slice(&self.clean_gen.to_le_bytes());
        body.extend_from_slice(&self.clean_group.to_le_bytes());

        let mut out = Vec::with_capacity(16 + body.len() + 4);
        out.extend_from_slice(MANIFEST_MAGIC);
        out.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        let mut flags: u16 = 0;
        if self.encrypted {
            flags |= MANIFEST_FLAG_ENCRYPTED;
        }
        if self.clean {
            flags |= MANIFEST_FLAG_CLEAN;
        }
        out.extend_from_slice(&flags.to_le_bytes());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32c(&out[..12]).to_le_bytes());
        out.extend_from_slice(&body);
        out.extend_from_slice(&crc32c(&body).to_le_bytes());
        out
    }
}

fn u16le(b: &[u8]) -> u16 {
    u16::from_le_bytes([b[0], b[1]])
}

fn u32le(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

fn u64le(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

/// Draw a random 16-byte store identity.
pub(crate) fn new_store_id() -> Result<[u8; 16]> {
    let mut id = [0u8; 16];
    rand::thread_rng()
        .try_fill_bytes(&mut id)
        .map_err(|e| err(ErrorKind::Io, format!("store id entropy: {e}")))?;
    Ok(id)
}

pub(crate) fn parse_manifest(raw: &[u8]) -> Result<Manifest> {
    if raw.len() < 16 {
        return Err(err(
            ErrorKind::Corrupt,
            format!("manifest size {}", raw.len()),
        ));
    }
    if &raw[0..4] != MANIFEST_MAGIC {
        return Err(err(ErrorKind::Corrupt, "bad manifest magic"));
    }
    let v = u16le(&raw[4..6]);
    if v != MANIFEST_VERSION {
        return Err(err(
            ErrorKind::UnsupportedVersion,
            format!("manifest version {v} rejected without migration"),
        ));
    }
    if crc32c(&raw[..12]) != u32le(&raw[12..16]) {
        return Err(err(ErrorKind::Corrupt, "manifest header checksum mismatch"));
    }
    let n = u32le(&raw[8..12]) as usize;
    if raw.len() != 16 + n + 4 {
        return Err(err(
            ErrorKind::Corrupt,
            format!("manifest size {} != body {n}", raw.len()),
        ));
    }
    let body = &raw[16..16 + n];
    if crc32c(body) != u32le(&raw[16 + n..]) {
        return Err(err(ErrorKind::Corrupt, "manifest body checksum mismatch"));
    }
    if body.len() < MANIFEST_BODY_BASE {
        return Err(err(
            ErrorKind::Corrupt,
            format!("manifest body {}", body.len()),
        ));
    }
    let mut m = Manifest {
        encrypted: false,
        store_id: [0u8; 16],
        context: [0u8; 16],
        next_file_id: 0,
        next_block_id: 0,
        next_epoch: 0,
        current_key_id: 0,
        active_file_id: 0,
        generation: 0,
        next_group_id: 0,
        req_features: 0,
        members: Vec::new(),
        clean: false,
        clean_bytes: 0,
        clean_files: 0,
        clean_gen: 0,
        clean_group: 0,
    };
    m.store_id.copy_from_slice(&body[0..16]);
    m.context.copy_from_slice(&body[16..32]);
    m.next_file_id = u64le(&body[32..40]);
    m.next_block_id = u64le(&body[40..48]);
    m.next_epoch = u16le(&body[48..50]);
    m.current_key_id = u32le(&body[50..54]);
    m.active_file_id = u64le(&body[54..62]);
    m.generation = u64le(&body[62..70]);
    m.next_group_id = u64le(&body[70..78]);
    m.req_features = u64le(&body[78..86]);
    // body[86:94] optional features: ignored.
    let mc = u32le(&body[94..98]) as usize;
    if body.len() != MANIFEST_BODY_BASE + 8 * mc {
        return Err(err(
            ErrorKind::Corrupt,
            format!("manifest members {mc} != body {}", body.len()),
        ));
    }
    m.members = Vec::with_capacity(mc);
    for i in 0..mc {
        m.members.push(u64le(&body[98 + 8 * i..106 + 8 * i]));
    }
    let tail = &body[98 + 8 * mc..];
    m.clean_bytes = u64le(&tail[0..8]);
    m.clean_files = u32le(&tail[8..12]);
    m.clean_gen = u64le(&tail[12..20]);
    m.clean_group = u64le(&tail[20..28]);

    let flags = u16le(&raw[6..8]);
    m.encrypted = flags & MANIFEST_FLAG_ENCRYPTED != 0;
    m.clean = flags & MANIFEST_FLAG_CLEAN != 0;
    if m.req_features & !(FEATURE_GROUPS_V2 | FEATURE_CRYPTO_V3) != 0 {
        return Err(err(
            ErrorKind::UnsupportedVersion,
            format!("manifest requires unknown features {:#x}", m.req_features),
        ));
    }
    if !m.members.windows(2).all(|w| w[0] < w[1]) {
        return Err(err(ErrorKind::Corrupt, "manifest members unsorted"));
    }
    if m.members.contains(&0) {
        return Err(err(ErrorKind::Corrupt, "manifest members invalid"));
    }
    if m.next_file_id == 0 || m.next_block_id == 0 || m.active_file_id == 0 || m.next_group_id == 0
    {
        return Err(err(ErrorKind::Corrupt, "manifest has zero counters"));
    }
    if m.encrypted && m.current_key_id == 0 {
        return Err(err(ErrorKind::Corrupt, "encrypted manifest lacks a key id"));
    }
    if !m.members.contains(&m.active_file_id) {
        return Err(err(
            ErrorKind::Corrupt,
            format!("manifest active file {} not a member", m.active_file_id),
        ));
    }
    Ok(m)
}

/// Recorded clean-shutdown shape a reopen must reproduce.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CleanMarker {
    pub bytes: u64,
    pub files: u32,
    pub gen: u64,
    pub group: u64,
}

/// Require a cleanly closed store to still have its recorded shape.
/// Less means committed data vanished outside the store; extra bytes
/// are tolerated (the torn-tail path truncates them).
pub(crate) fn check_clean_marker(
    seg_dir: &Path,
    ids: &[u64],
    cur_gen: u64,
    want: CleanMarker,
    max_group: u64,
) -> Result<()> {
    if cur_gen != want.gen {
        return Err(err(
            ErrorKind::Corrupt,
            format!(
                "clean-shutdown marker generation {} != manifest {cur_gen}",
                want.gen
            ),
        ));
    }
    if ids.len() as u64 != want.files as u64 {
        return Err(err(
            ErrorKind::Corrupt,
            format!(
                "clean-shutdown marker expects {} segment files, found {}",
                want.files,
                ids.len()
            ),
        ));
    }
    if max_group != want.group {
        return Err(err(
            ErrorKind::Corrupt,
            format!(
                "clean-shutdown marker group {} != scanned {max_group}",
                want.group
            ),
        ));
    }
    let mut total: u64 = 0;
    for id in ids {
        let st = fs::metadata(seg_dir.join(crate::segment::segment_file_name(*id)))
            .map_err(|e| err(ErrorKind::Io, format!("stat segment {id}: {e}")))?;
        total += st.len();
    }
    if total < want.bytes {
        return Err(err(
            ErrorKind::Corrupt,
            format!(
                "clean-shutdown marker expects {} segment bytes, found {total}",
                want.bytes
            ),
        ));
    }
    Ok(())
}

/// Durably replace `dir/name`: temp file in the same directory,
/// content fsync, rename, directory fsync.
pub(crate) fn atomic_write_file(dir: &Path, name: &str, data: &[u8]) -> Result<()> {
    use std::io::Write;
    let tmp_path = create_temp(dir, name)?;
    let mut tmp = fs::OpenOptions::new()
        .write(true)
        .open(&tmp_path)
        .map_err(|e| err(ErrorKind::Io, format!("temp file: {e}")))?;
    let write_result = (|| -> Result<()> {
        tmp.write_all(data)
            .map_err(|e| err(ErrorKind::Io, format!("write temp file: {e}")))?;
        tmp.sync_all()
            .map_err(|e| err(ErrorKind::Io, format!("sync temp file: {e}")))?;
        Ok(())
    })();
    drop(tmp);
    if let Err(e) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    if let Err(e) = fs::rename(&tmp_path, dir.join(name)) {
        let _ = fs::remove_file(&tmp_path);
        return Err(err(ErrorKind::Io, format!("rename {name}: {e}")));
    }
    dir_sync(dir).map_err(|e| err(ErrorKind::Io, format!("sync dir: {e}")))?;
    Ok(())
}

/// Create a uniquely-named temp file (O_EXCL) in `dir` and return its path.
pub(crate) fn create_temp(dir: &Path, prefix: &str) -> Result<std::path::PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    for _ in 0..100 {
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let rnd = rand::random::<u32>();
        let path = dir.join(format!(".{prefix}.tmp-{pid}-{n}-{rnd:x}"));
        #[cfg(unix)]
        let opts = {
            use std::os::unix::fs::OpenOptionsExt;
            let mut o = fs::OpenOptions::new();
            o.write(true).create_new(true).mode(0o600);
            o
        };
        #[cfg(not(unix))]
        let opts = {
            let mut o = fs::OpenOptions::new();
            o.write(true).create_new(true);
            o
        };
        match opts.open(&path) {
            Ok(f) => {
                drop(f);
                return Ok(path);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(err(ErrorKind::Io, format!("temp file: {e}"))),
        }
    }
    Err(err(ErrorKind::Io, "temp file: too many collisions"))
}

/// fsync a directory so file creations, renames and removals survive
/// a crash. No-op on Windows.
pub(crate) fn dir_sync(dir: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        let _ = dir;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let f = fs::File::open(dir)?;
        f.sync_all()
    }
}
