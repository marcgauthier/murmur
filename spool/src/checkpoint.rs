//! Backup checkpoints: consistent point-in-time copies.
//!
//! Rust port of `spool/checkpoint.go`.
//!
//! A checkpoint is a caller-owned destination directory of hard links
//! to sealed segment inodes plus copies of the manifest and keyring
//! bytes, so it stays immutable and readable while the live store
//! keeps appending, compacting, rewriting, rebinding, rotating, and
//! pruning.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use crate::block::{parse_block_header, BLOCK_HEADER_LEN};
use crate::error::{err, Error, ErrorKind, Result};
use crate::keyring::KEYS_FILE_NAME;
use crate::manifest::{dir_sync, MANIFEST_FILE_NAME, SEGMENTS_DIR_NAME};
use crate::options::Durability;
use crate::segment::segment_file_name;
use crate::store::{CheckpointData, Store};

pub(crate) const CHECKPOINT_COMPLETE_NAME: &str = "COMPLETE";
pub(crate) const CHECKPOINT_MAGIC: &str = "spool-checkpoint/1";

/// A captured point-in-time copy. `files` lists the ordered stream
/// set: manifest, keyring (encrypted stores), then segments ascending.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    store: Option<Store>,
    /// Registration id.
    pub id: String,
    /// Destination directory (caller-owned).
    pub dir: PathBuf,
    /// Manifest generation captured.
    pub generation: u64,
    /// Captured member segments.
    pub members: Vec<u64>,
    /// Capture time.
    pub created_at: SystemTime,
    /// Whether the store is encrypted.
    pub encrypted: bool,
    /// Ordered stream set (absolute paths).
    pub files: Vec<PathBuf>,
}

impl Checkpoint {
    /// The ordered stream set: manifest, keyring when present, then
    /// segments ascending.
    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    /// Drop the handle's retention registrations so pruning can
    /// proceed. Idempotent. The destination directory belongs to the
    /// caller and is never removed here.
    pub fn release(&self) -> Result<()> {
        match &self.store {
            Some(s) => s.release_checkpoint(&self.id),
            None => Err(err(
                ErrorKind::Invalid,
                format!("checkpoint {:?} has no store", self.id),
            )),
        }
    }
}

/// Build the ordered stream set for a checkpoint layout.
fn checkpoint_files(dir: &Path, members: &[u64], encrypted: bool) -> Vec<PathBuf> {
    let mut files = Vec::with_capacity(2 + members.len());
    files.push(dir.join(MANIFEST_FILE_NAME));
    if encrypted {
        files.push(dir.join(KEYS_FILE_NAME));
    }
    let seg_dir = dir.join(SEGMENTS_DIR_NAME);
    for id in members {
        files.push(seg_dir.join(segment_file_name(*id)));
    }
    files
}

impl Store {
    /// Capture a transactionally consistent point-in-time copy into
    /// `destination` and register a handle owning its retention
    /// registrations. Destination must be a new directory on the same
    /// filesystem; anything else fails without touching existing
    /// data. When `cancel` is set, capture aborts and removes the
    /// partial destination.
    pub fn checkpoint(
        &self,
        destination: &Path,
        cancel: Option<&AtomicBool>,
    ) -> Result<Checkpoint> {
        if destination.as_os_str().is_empty() {
            return Err(err(
                ErrorKind::Invalid,
                "checkpoint destination is required",
            ));
        }
        if cancel.map(|c| c.load(Ordering::SeqCst)).unwrap_or(false) {
            return Err(err(ErrorKind::Canceled, "checkpoint canceled"));
        }
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        // Admission blocks for the capture: no new groups can start,
        // and rotation, compaction publication, and maintenance start
        // serialize here too.
        let _a = self.inner.admission.lock().unwrap();
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        self.check_maint()?;
        if cancel.map(|c| c.load(Ordering::SeqCst)).unwrap_or(false) {
            return Err(err(ErrorKind::Canceled, "checkpoint canceled"));
        }
        // Seal buffered individual writes into admitted groups first.
        self.flush_locked(Durability::Async)?;
        let fence = self.inner.last_group.load(Ordering::SeqCst);
        self.wait_for_group(fence, false)?;
        // Push async-buffered bytes to the file.
        self.inner.seg.commit(Durability::Flush)?;
        // Seal the active segment so every linked inode is immutable.
        let next = self.allocate_file_id();
        self.inner.seg.rotate_to(next)?;
        let (man, active) = {
            let m = self.inner.manifest.lock().unwrap();
            (m.clone(), m.active_file_id)
        };
        let members: Vec<u64> = man
            .members
            .iter()
            .copied()
            .filter(|id| *id != active)
            .collect();
        if members.is_empty() {
            return Err(err(ErrorKind::Corrupt, "checkpoint of empty membership"));
        }
        fs::create_dir(destination).map_err(|_| {
            err(
                ErrorKind::Invalid,
                "checkpoint destination must be a new directory",
            )
        })?;
        let cp_seg_dir = destination.join(SEGMENTS_DIR_NAME);
        let fail = |e: Error| -> Result<Checkpoint> {
            let _ = fs::remove_dir_all(destination);
            Err(e)
        };
        if let Err(e) = fs::create_dir(&cp_seg_dir) {
            return fail(err(
                ErrorKind::Io,
                format!("create checkpoint segments dir: {e}"),
            ));
        }
        let seg_dir = self.inner.dir.join(SEGMENTS_DIR_NAME);
        for m_id in &members {
            if cancel.map(|c| c.load(Ordering::SeqCst)).unwrap_or(false) {
                return fail(err(ErrorKind::Canceled, "checkpoint canceled"));
            }
            if let Some(f) = &self.inner.faults {
                if let Err(e) = f.trip("checkpoint") {
                    return fail(err(
                        ErrorKind::Io,
                        format!("link checkpoint segment {m_id}: {e}"),
                    ));
                }
            }
            // Cross-filesystem destinations fail here with no
            // fallback to copying.
            if let Err(e) = fs::hard_link(
                seg_dir.join(segment_file_name(*m_id)),
                cp_seg_dir.join(segment_file_name(*m_id)),
            ) {
                return fail(err(
                    ErrorKind::Io,
                    format!("link checkpoint segment {m_id}: {e}"),
                ));
            }
        }
        // The checkpoint manifest is the post-rotation manifest minus
        // the excluded empty active, with the sealed tail as its
        // active file and no clean flag.
        let mut ckman = man.clone();
        ckman.members = members.clone();
        ckman.active_file_id = members[members.len() - 1];
        ckman.clean = false;
        if let Err(e) = write_sync_file(
            &destination.join(MANIFEST_FILE_NAME),
            &ckman.encode(),
            0o600,
        ) {
            return fail(err(
                ErrorKind::Io,
                format!("write checkpoint manifest: {e}"),
            ));
        }
        if man.encrypted {
            let kraw = match fs::read(self.inner.dir.join(KEYS_FILE_NAME)) {
                Ok(k) => k,
                Err(e) => {
                    return fail(err(
                        ErrorKind::Io,
                        format!("read keyring for checkpoint: {e}"),
                    ))
                }
            };
            if let Err(e) = write_sync_file(&destination.join(KEYS_FILE_NAME), &kraw, 0o600) {
                return fail(err(ErrorKind::Io, format!("write checkpoint keyring: {e}")));
            }
        }
        let created = SystemTime::now();
        let created_nanos = created
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let complete = format!("{CHECKPOINT_MAGIC}\n{}\n{created_nanos}\n", man.generation);
        if let Err(e) = write_sync_file(
            &destination.join(CHECKPOINT_COMPLETE_NAME),
            complete.as_bytes(),
            0o600,
        ) {
            return fail(err(ErrorKind::Io, format!("write checkpoint marker: {e}")));
        }
        if let Some(f) = &self.inner.faults {
            if let Err(e) = f.trip("dirsync") {
                return fail(err(
                    ErrorKind::Io,
                    format!("sync checkpoint segments dir: {e}"),
                ));
            }
        }
        if let Err(e) = dir_sync(&cp_seg_dir) {
            return fail(err(
                ErrorKind::Io,
                format!("sync checkpoint segments dir: {e}"),
            ));
        }
        if let Some(f) = &self.inner.faults {
            if let Err(e) = f.trip("dirsync") {
                return fail(err(ErrorKind::Io, format!("sync checkpoint dir: {e}")));
            }
        }
        if let Err(e) = dir_sync(destination) {
            return fail(err(ErrorKind::Io, format!("sync checkpoint dir: {e}")));
        }
        if let Some(f) = &self.inner.faults {
            if let Err(e) = f.trip("dirsync") {
                return fail(err(
                    ErrorKind::Io,
                    format!("sync checkpoint parent dir: {e}"),
                ));
            }
        }
        if let Some(parent) = destination.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(e) = dir_sync(parent) {
                    return fail(err(
                        ErrorKind::Io,
                        format!("sync checkpoint parent dir: {e}"),
                    ));
                }
            }
        }
        let generation = man.generation;
        let encrypted = man.encrypted;
        // Release admission before the slow key scan: the checkpoint
        // inodes are immutable and the destination is complete, so
        // writers resume while registration finishes.
        drop(_a);
        let mut key_set = std::collections::HashSet::new();
        for m_id in &members {
            match scan_file_key_ids(&cp_seg_dir.join(segment_file_name(*m_id))) {
                Ok(ids) => {
                    key_set.extend(ids);
                }
                Err(e) => {
                    let _ = fs::remove_dir_all(destination);
                    return Err(err(
                        e.kind(),
                        format!("scan checkpoint segment {m_id}: {e}"),
                    ));
                }
            }
        }
        let mut key_ids: Vec<u32> = key_set.into_iter().filter(|k| *k != 0).collect();
        key_ids.sort_unstable();
        let id = checkpoint_id()?;
        {
            let mut ck = self.inner.ckpt.lock().unwrap();
            let mut refs = self.inner.ckpt_key_refs.lock().unwrap();
            ck.insert(
                id.clone(),
                CheckpointData {
                    id: id.clone(),
                    dir: destination.to_path_buf(),
                    generation,
                    members: members.clone(),
                    created_at: created,
                    encrypted,
                    key_ids: key_ids.clone(),
                    files: checkpoint_files(destination, &members, encrypted),
                },
            );
            for k in &key_ids {
                *refs.entry(*k).or_insert(0) += 1;
            }
        }
        Ok(Checkpoint {
            store: Some(self.clone()),
            id,
            dir: destination.to_path_buf(),
            generation,
            members: members.clone(),
            created_at: created,
            encrypted,
            files: checkpoint_files(destination, &members, encrypted),
        })
    }

    /// Drop a checkpoint by id, freeing its key references. Idempotent:
    /// unknown ids succeed. The destination directory belongs to the
    /// caller and is never removed here. Available on closed, failed,
    /// and maintenance-held stores.
    pub fn release_checkpoint(&self, id: &str) -> Result<()> {
        let mut ck = self.inner.ckpt.lock().unwrap();
        let cp = match ck.remove(id) {
            Some(cp) => cp,
            None => return Ok(()),
        };
        let mut refs = self.inner.ckpt_key_refs.lock().unwrap();
        for k in &cp.key_ids {
            match refs.get(k) {
                Some(1) | None => {
                    refs.remove(k);
                }
                Some(_) => {
                    *refs.get_mut(k).unwrap() -= 1;
                }
            }
        }
        Ok(())
    }

    /// Every registered checkpoint of this store handle. Handles are
    /// copies: releasing through any of them works.
    pub fn list_checkpoints(&self) -> Vec<Checkpoint> {
        let ck = self.inner.ckpt.lock().unwrap();
        let mut out: Vec<Checkpoint> = ck
            .values()
            .map(|cp| Checkpoint {
                store: Some(self.clone()),
                id: cp.id.clone(),
                dir: cp.dir.clone(),
                generation: cp.generation,
                members: cp.members.clone(),
                created_at: cp.created_at,
                encrypted: cp.encrypted,
                files: cp.files.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
}

/// Mint a registration id for a captured checkpoint.
fn checkpoint_id() -> Result<String> {
    let mut rnd = [0u8; 6];
    use rand::RngCore;
    rand::thread_rng()
        .try_fill_bytes(&mut rnd)
        .map_err(|e| err(ErrorKind::Io, format!("checkpoint entropy: {e}")))?;
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Ok(format!("{nanos}-{}", hex6(&rnd)))
}

fn hex6(b: &[u8; 6]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(12);
    for byte in b {
        s.push(H[(byte >> 4) as usize] as char);
        s.push(H[(byte & 0xf) as usize] as char);
    }
    s
}

/// Write bytes and fsync the file (not its dir).
pub(crate) fn write_sync_file(
    path: &Path,
    data: &[u8],
    #[allow(unused_variables)] mode: u32,
) -> std::io::Result<()> {
    use std::io::Write;
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(mode);
    }
    let mut f = opts.open(path)?;
    f.write_all(data)?;
    f.sync_all()?;
    drop(f);
    Ok(())
}

/// Every block's data-key id in file order. Any framing error fails
/// the scan: checkpoints only capture healthy files.
fn scan_file_key_ids(path: &Path) -> Result<Vec<u32>> {
    let raw =
        fs::read(path).map_err(|e| err(ErrorKind::Io, format!("read checkpoint segment: {e}")))?;
    let mut ids = Vec::new();
    let mut off = 0usize;
    while off < raw.len() {
        if raw.len() - off < BLOCK_HEADER_LEN {
            return Err(err(
                ErrorKind::Corrupt,
                format!("torn checkpoint frame at {off}"),
            ));
        }
        let hdr = parse_block_header(&raw[off..off + BLOCK_HEADER_LEN])?;
        ids.push(hdr.key_id);
        let next = off + BLOCK_HEADER_LEN + hdr.sealed_len as usize;
        if next > raw.len() {
            return Err(err(
                ErrorKind::Corrupt,
                format!("torn checkpoint frame at {off}"),
            ));
        }
        off = next;
    }
    Ok(ids)
}
