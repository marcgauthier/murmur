//! Segment files and the append writer.
//!
//! Rust port of `spool/segment.go`.

use std::fs::{self, File};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _OpenOptionsExt;

/// OpenOptions for a new segment file (0600 on unix).
fn segment_create_opts() -> fs::OpenOptions {
    let mut o = fs::OpenOptions::new();
    #[cfg(unix)]
    o.mode(0o600);
    o
}

use crate::block::{parse_block_header, BlockHeader, ABS_MAX_BLOCK_BYTES, BLOCK_HEADER_LEN};
use crate::error::{err, ErrorKind, Result};
use crate::manifest::dir_sync;
use crate::options::{Durability, FaultHooks};

/// Format a segment leaf name: zero-padded id plus `.spool`.
pub(crate) fn segment_file_name(id: u64) -> String {
    format!("{id:012}.spool")
}

/// Extract the id from a segment leaf name.
pub(crate) fn parse_segment_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".spool")?;
    let id: u64 = stem.parse().ok()?;
    if id == 0 {
        return None;
    }
    Some(id)
}

struct SegmentState {
    file: Option<File>,
    buf: Vec<u8>,
    id: u64,
    offset: i64,
}

/// Appends block frames to the active segment. The mutex is the
/// single commit point for physical appends. Rotation decisions live
/// Runs after the old file seals and before the new file accepts
/// appends: (sealed_id, new_id).
pub(crate) type OnRotate = Arc<dyn Fn(u64, u64) -> Result<()> + Send + Sync>;

/// with the caller (which compares against the configured max size).
pub(crate) struct SegmentWriter {
    dir: PathBuf,
    state: Mutex<SegmentState>,
    faults: Option<Arc<FaultHooks>>,
    /// Set once at open; invoked outside the state lock.
    on_rotate: Mutex<Option<OnRotate>>,
}

impl SegmentWriter {
    /// Open (or create) the active segment `active_id`. A missing
    /// file is created; an existing one opens for append.
    pub fn open(dir: &Path, active_id: u64, faults: Option<Arc<FaultHooks>>) -> Result<Self> {
        let w = SegmentWriter {
            dir: dir.to_path_buf(),
            state: Mutex::new(SegmentState {
                file: None,
                buf: Vec::new(),
                id: active_id,
                offset: 0,
            }),
            faults,
            on_rotate: Mutex::new(None),
        };
        w.open_active()?;
        Ok(w)
    }

    pub fn set_on_rotate(&self, f: OnRotate) {
        *self.on_rotate.lock().unwrap() = Some(f);
    }

    fn open_active(&self) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        let path = self.dir.join(segment_file_name(st.id));
        let mut opts = segment_create_opts();
        let f = opts
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| err(ErrorKind::Io, format!("open segment {}: {e}", st.id)))?;
        let len = f
            .metadata()
            .map_err(|e| err(ErrorKind::Io, format!("stat segment {}: {e}", st.id)))?
            .len();
        st.file = Some(f);
        st.buf = Vec::with_capacity(1 << 20);
        st.offset = len as i64;
        Ok(())
    }

    /// Current segment id.
    pub fn active_id(&self) -> u64 {
        self.state.lock().unwrap().id
    }

    /// Logical end of the active segment.
    pub fn end_offset(&self) -> i64 {
        self.state.lock().unwrap().offset
    }

    /// Append a commit group's frames contiguously without rotating.
    /// Returns the home file id and each frame's starting offset.
    pub fn append_group(&self, frames: &[Vec<u8>]) -> Result<(u64, Vec<i64>)> {
        let mut st = self.state.lock().unwrap();
        if st.file.is_none() {
            return Err(err(ErrorKind::Closed, "segment writer closed"));
        }
        if let Some(f) = &self.faults {
            f.trip("append")
                .map_err(|e| err(ErrorKind::Io, format!("injected append fault: {e}")))?;
        }
        let mut offs = Vec::with_capacity(frames.len());
        for frame in frames {
            offs.push(st.offset);
            st.buf.extend_from_slice(frame);
            st.offset += frame.len() as i64;
        }
        if st.buf.len() >= 1 << 20 {
            let buf = std::mem::take(&mut st.buf);
            let f = st.file.as_mut().unwrap();
            f.write_all(&buf)
                .map_err(|e| err(ErrorKind::Io, format!("append group: {e}")))?;
        }
        Ok((st.id, offs))
    }

    /// Seal the active segment and open the successor `next`. The id
    /// must come from the manifest allocator.
    pub fn rotate_to(&self, next: u64) -> Result<()> {
        let hook = self.on_rotate.lock().unwrap().clone();
        let sealed = {
            let mut st = self.state.lock().unwrap();
            if st.file.is_none() {
                return Err(err(ErrorKind::Closed, "segment writer closed"));
            }
            let sealed = st.id;
            let path = self.dir.join(segment_file_name(next));
            let mut opts = segment_create_opts();
            let new_file = opts
                .create_new(true)
                .write(true)
                .open(&path)
                .map_err(|e| err(ErrorKind::Io, format!("create segment {next}: {e}")))?;
            let mut seal_result: Result<()> = Ok(());
            if st.file.is_some() {
                if !st.buf.is_empty() {
                    let buf = std::mem::take(&mut st.buf);
                    if let Err(e) = st.file.as_mut().unwrap().write_all(&buf) {
                        seal_result =
                            Err(err(ErrorKind::Io, format!("flush rotated segment: {e}")));
                    }
                }
                // Fsync on rotation bounds Async-mode loss to the
                // active segment; one fsync per segment is negligible.
                if seal_result.is_ok() {
                    if let Err(e) = st.file.as_mut().unwrap().sync_all() {
                        seal_result = Err(err(ErrorKind::Io, format!("sync rotated segment: {e}")));
                    }
                }
            }
            if let Err(e) = seal_result {
                drop(new_file);
                let _ = fs::remove_file(&path);
                return Err(e);
            }
            drop(st.file.take());
            st.file = Some(new_file);
            st.buf = Vec::with_capacity(1 << 20);
            st.id = next;
            st.offset = 0;
            sealed
        };
        if let Some(f) = &self.faults {
            f.trip("dirsync")
                .map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
        }
        dir_sync(&self.dir).map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
        if let Some(hook) = hook {
            hook(sealed, next)?;
        }
        Ok(())
    }

    /// Apply the durability level to staged bytes.
    pub fn commit(&self, d: Durability) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        if st.file.is_none() {
            return Err(err(ErrorKind::Closed, "segment writer closed"));
        }
        match d {
            Durability::Async => Ok(()),
            Durability::Flush | Durability::Sync => {
                if !st.buf.is_empty() {
                    let buf = std::mem::take(&mut st.buf);
                    let f = st.file.as_mut().unwrap();
                    f.write_all(&buf)
                        .map_err(|e| err(ErrorKind::Io, format!("flush segment: {e}")))?;
                }
                if d == Durability::Sync {
                    if let Some(faults) = &self.faults {
                        faults
                            .trip("sync")
                            .map_err(|e| err(ErrorKind::Io, format!("injected sync fault: {e}")))?;
                    }
                    st.file
                        .as_mut()
                        .unwrap()
                        .sync_all()
                        .map_err(|e| err(ErrorKind::Io, format!("sync segment: {e}")))?;
                }
                Ok(())
            }
        }
    }

    /// Flush and fsync the active segment.
    pub fn sync(&self) -> Result<()> {
        self.commit(Durability::Sync)
    }

    /// Drop the current file description and re-open the same active
    /// segment path (maintenance renames over the active file).
    pub fn reopen_active(&self) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        if st.file.is_none() {
            return Err(err(ErrorKind::Closed, "segment writer closed"));
        }
        if !st.buf.is_empty() {
            let buf = std::mem::take(&mut st.buf);
            let f = st.file.as_mut().unwrap();
            f.write_all(&buf)
                .map_err(|e| err(ErrorKind::Io, format!("flush segment: {e}")))?;
        }
        {
            let f = st.file.as_mut().unwrap();
            f.sync_all()
                .map_err(|e| err(ErrorKind::Io, format!("sync segment: {e}")))?;
        }
        drop(st.file.take());
        let path = self.dir.join(segment_file_name(st.id));
        let id = st.id;
        let f = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|e| err(ErrorKind::Io, format!("reopen segment {id}: {e}")))?;
        let len = f
            .metadata()
            .map_err(|e| err(ErrorKind::Io, format!("stat segment {id}: {e}")))?
            .len();
        st.file = Some(f);
        st.buf = Vec::with_capacity(1 << 20);
        st.offset = len as i64;
        Ok(())
    }

    /// Flush, sync and close the active segment.
    pub fn close(&self) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        if st.file.is_none() {
            return Ok(());
        }
        if !st.buf.is_empty() {
            let buf = std::mem::take(&mut st.buf);
            let f = st.file.as_mut().unwrap();
            f.write_all(&buf)
                .map_err(|e| err(ErrorKind::Io, format!("close flush: {e}")))?;
        }
        if let Some(f) = st.file.as_mut() {
            f.sync_all()
                .map_err(|e| err(ErrorKind::Io, format!("close sync: {e}")))?;
        }
        drop(st.file.take());
        Ok(())
    }
}

/// Read one framed block at `offset`: header plus sealed payload.
/// Short reads (clean end or torn tail) return `Ok(None)`; the caller
/// stops with the current valid end.
pub(crate) fn read_block_frame(
    f: &mut File,
    offset: u64,
    max_sealed: u32,
) -> Result<Option<(BlockHeader, Vec<u8>, u64)>> {
    let mut raw = vec![0u8; BLOCK_HEADER_LEN];
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| err(ErrorKind::Io, format!("read block header: {e}")))?;
    let mut got = 0;
    while got < raw.len() {
        match f.read(&mut raw[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) => return Err(err(ErrorKind::Io, format!("read block header: {e}"))),
        }
    }
    if got == 0 {
        return Ok(None);
    }
    if got < BLOCK_HEADER_LEN {
        return Ok(None); // torn tail
    }
    let hdr = parse_block_header(&raw)?;
    if hdr.sealed_len > max_sealed {
        return Err(err(
            ErrorKind::Corrupt,
            format!("sealed size {} exceeds {max_sealed}", hdr.sealed_len),
        ));
    }
    let mut frame = Vec::with_capacity(BLOCK_HEADER_LEN + hdr.sealed_len as usize);
    frame.extend_from_slice(&hdr.raw);
    frame.resize(BLOCK_HEADER_LEN + hdr.sealed_len as usize, 0);
    f.seek(SeekFrom::Start(offset + BLOCK_HEADER_LEN as u64))
        .map_err(|e| err(ErrorKind::Io, format!("read block payload: {e}")))?;
    let mut pg = 0;
    while pg < hdr.sealed_len as usize {
        match f.read(&mut frame[BLOCK_HEADER_LEN + pg..]) {
            Ok(0) => break,
            Ok(n) => pg += n,
            Err(e) => return Err(err(ErrorKind::Io, format!("read block payload: {e}"))),
        }
    }
    if pg < hdr.sealed_len as usize {
        return Ok(None); // torn tail
    }
    let next = offset + frame.len() as u64;
    Ok(Some((hdr, frame, next)))
}

/// Return the offset past the framed block at `offset` without
/// retaining its payload. Short reads report `Ok(None)` for the
/// torn-tail rule; anything else failing is corruption.
pub(crate) fn peek_block_next(f: &mut File, offset: u64, size: u64) -> Result<Option<u64>> {
    if offset + BLOCK_HEADER_LEN as u64 > size {
        return Ok(None);
    }
    let mut raw = vec![0u8; BLOCK_HEADER_LEN];
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| err(ErrorKind::Io, format!("read block header: {e}")))?;
    f.read_exact(&mut raw)
        .map_err(|e| err(ErrorKind::Io, format!("read block header: {e}")))?;
    let hdr = parse_block_header(&raw)?;
    if hdr.sealed_len as u64 > ABS_MAX_BLOCK_BYTES {
        return Err(err(
            ErrorKind::Corrupt,
            format!(
                "sealed size {} exceeds {ABS_MAX_BLOCK_BYTES}",
                hdr.sealed_len
            ),
        ));
    }
    let end = offset + BLOCK_HEADER_LEN as u64 + hdr.sealed_len as u64;
    if end > size {
        return Ok(None);
    }
    Ok(Some(end))
}

/// Buffered segment appender used by compaction staging.
pub(crate) struct StagedFile {
    pub file: BufWriter<File>,
    pub path: PathBuf,
    pub written: i64,
}

impl StagedFile {
    pub fn create(seg_dir: &Path, prefix: &str) -> Result<Self> {
        let path = crate::manifest::create_temp(seg_dir, prefix)?;
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| err(ErrorKind::Io, format!("stage replacement: {e}")))?;
        Ok(StagedFile {
            file: BufWriter::with_capacity(1 << 20, file),
            path,
            written: 0,
        })
    }

    pub fn write_frame(&mut self, frame: &[u8]) -> Result<()> {
        self.file
            .write_all(frame)
            .map_err(|e| err(ErrorKind::Io, format!("stage replacement: {e}")))?;
        self.written += frame.len() as i64;
        Ok(())
    }

    pub fn sync_and_close(mut self) -> Result<PathBuf> {
        self.file
            .flush()
            .map_err(|e| err(ErrorKind::Io, format!("sync replacement: {e}")))?;
        let f = self
            .file
            .into_inner()
            .map_err(|e| err(ErrorKind::Io, format!("sync replacement: {e}")))?;
        f.sync_all()
            .map_err(|e| err(ErrorKind::Io, format!("sync replacement: {e}")))?;
        drop(f);
        Ok(self.path)
    }
}
