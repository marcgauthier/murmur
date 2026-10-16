//! Replay: group assembly, validation, and load scans.
//!
//! Rust port of `spool/loader.go`.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::block::{
    decode_body, decode_completion, group_digest, parse_block_header, BlockHeader,
    ABS_MAX_BLOCK_BYTES, BLOCK_HEADER_LEN, BLOCK_MAGIC, BLOCK_VERSION,
};
use crate::compression::Compressor;
use crate::crypto::{block_aad, BlockCrypt, ALGO_AES_GCM, ALGO_NONE};
use crate::error::{err, ErrorKind, Result};
use crate::filestats::FileStatsRegistry;
use crate::index::{Index, Location, WinnerLoc};
use crate::keyring::Keyring;
use crate::manifest::{parse_manifest, Manifest, MANIFEST_FILE_NAME, SEGMENTS_DIR_NAME};
use crate::record::Record;
use crate::segment::{peek_block_next, read_block_frame, segment_file_name};

/// Receives current record versions during a load scan. Each key is
/// delivered exactly once with its current version; tombstones are
/// delivered for keys whose current version is a deletion. A `Err`
/// return aborts the load.
pub type LoadFunc<'a> = &'a mut dyn FnMut(&[Record]) -> Result<()>;

/// Segment replay advancement during open.
#[derive(Debug, Clone, Copy)]
pub struct ReplayProgress {
    /// 1 = validate/index, 2 = winner delivery (open-and-load only).
    pub pass: i32,
    /// Member segment totals.
    pub files_total: usize,
    /// Member segments completed.
    pub files_done: usize,
    /// Sum of member segment sizes at scan start.
    pub bytes_total: i64,
    /// Odometer reading; routinely exceeds `bytes_total`.
    pub bytes_done: i64,
    /// Validated record visits (accumulates across passes).
    pub records: u64,
}

/// Standalone offline load configuration.
#[derive(Clone, Default)]
pub struct LoadOptions {
    /// Store directory.
    pub path: String,
    /// 32-byte store key. Exactly one of master_key and passphrase
    /// must be set for encrypted stores.
    pub master_key: Vec<u8>,
    /// Passphrase for passphrase-protected stores.
    pub passphrase: String,
    /// When set, must match the envelope's authenticated wrapping id.
    pub wrapping_key_id: String,
    /// When 16 bytes, must match the store's persisted database
    /// context. Empty accepts the persisted context.
    pub context_id: Vec<u8>,
    /// Custom compression codecs (ids 128-255) needed to read blocks
    /// written under them.
    pub codecs: Vec<Arc<dyn crate::compression::Codec>>,
}

impl std::fmt::Debug for LoadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadOptions")
            .field("path", &self.path)
            .field("wrapping_key_id", &self.wrapping_key_id)
            .field("codecs", &self.codecs.len())
            .finish_non_exhaustive()
    }
}

/// Stream every key's current version from an offline store into
/// `f`. Takes no lock: use on quiesced stores.
pub fn load(path: &str, master_key: &[u8], f: LoadFunc<'_>) -> Result<()> {
    load_with_options(
        LoadOptions {
            path: path.to_string(),
            master_key: master_key.to_vec(),
            ..Default::default()
        },
        f,
    )
}

/// `load` with passphrase, wrapping-id and context support.
pub fn load_with_options(o: LoadOptions, f: LoadFunc<'_>) -> Result<()> {
    if o.path.is_empty() {
        return Err(err(ErrorKind::Invalid, "Path is required"));
    }
    if !o.context_id.is_empty() && o.context_id.len() != 16 {
        return Err(err(
            ErrorKind::Invalid,
            format!("ContextID must be 16 bytes, got {}", o.context_id.len()),
        ));
    }
    if o.wrapping_key_id.len() > crate::keyring::MAX_WRAP_ID_LEN {
        return Err(err(
            ErrorKind::Invalid,
            format!(
                "WrappingKeyID of {} bytes exceeds {}",
                o.wrapping_key_id.len(),
                crate::keyring::MAX_WRAP_ID_LEN
            ),
        ));
    }
    let mut l = open_loader(&o)?;
    l.scan_all(&l.man.members.clone(), Some(f), None, None, false, &mut crate::read_cache::Cache::new(32 << 20))?;
    Ok(())
}

/// Read-side pieces shared by open's rebuild and standalone loads.
pub(crate) struct Loader {
    pub dir: PathBuf,
    pub man: Manifest,
    pub ring: Option<Arc<Keyring>>,
    pub comp: Compressor,
    /// Database context binding block AAD.
    pub ctx: [u8; 16],
    /// Replay reporter (open path only).
    pub replay: Option<ReplayTracker>,
    /// The one member whose unparseable tail may truncate (the active
    /// segment). Zero means every member fails closed.
    pub torn_tail_id: u64,
    pub store_id: [u8; 16],
}

/// Accumulated scan advancement for `on_replay_progress`.
#[derive(Clone)]
pub(crate) struct ReplayTracker {
    pub cb: Arc<dyn Fn(ReplayProgress) + Send + Sync>,
    pub pass: i32,
    pub files_total: usize,
    pub files_done: usize,
    pub bytes_total: i64,
    pub bytes_done: i64,
    pub records: u64,
}

impl Loader {
    pub fn note_replay(&mut self, records: u64, bytes: i64) {
        if let Some(t) = &mut self.replay {
            t.records += records;
            t.bytes_done += bytes;
            (t.cb)(ReplayProgress {
                pass: t.pass,
                files_total: t.files_total,
                files_done: t.files_done,
                bytes_total: t.bytes_total,
                bytes_done: t.bytes_done,
                records: t.records,
            });
        }
    }

    pub fn note_replay_file(&mut self) {
        if let Some(t) = &mut self.replay {
            t.files_done += 1;
            (t.cb)(ReplayProgress {
                pass: t.pass,
                files_total: t.files_total,
                files_done: t.files_done,
                bytes_total: t.bytes_total,
                bytes_done: t.bytes_done,
                records: t.records,
            });
        }
    }

    pub fn set_replay_pass(&mut self, pass: i32) {
        if let Some(t) = &mut self.replay {
            t.pass = pass;
        }
    }
}

/// Read the manifest and key container without locking, epoch
/// changes or side effects.
pub(crate) fn open_loader(o: &LoadOptions) -> Result<Loader> {
    let dir = Path::new(&o.path);
    let raw = std::fs::read(dir.join(MANIFEST_FILE_NAME))
        .map_err(|e| err(ErrorKind::Io, format!("read manifest: {e}")))?;
    let m = parse_manifest(&raw)?;
    if o.context_id.len() == 16 && o.context_id != m.context {
        return Err(err(
            ErrorKind::ContextMismatch,
            "supplied database context does not match store",
        ));
    }
    let store_id = m.store_id;
    let ctx = m.context;
    let mut l = Loader {
        dir: dir.to_path_buf(),
        man: m,
        ring: None,
        comp: Compressor::new(crate::compression::ID_NONE, o.codecs.clone())?,
        ctx,
        replay: None,
        torn_tail_id: 0,
        store_id,
    };
    if l.man.encrypted {
        if o.master_key.is_empty() && o.passphrase.is_empty() {
            return Err(err(ErrorKind::WrongKey, "store is encrypted"));
        }
        let ring = crate::keyring::read_keys(
            dir,
            &o.master_key,
            &o.passphrase,
            &l.man.store_id,
            &l.man.context,
            None,
        )?;
        if !o.wrapping_key_id.is_empty() {
            let wrap = ring.inner.read().unwrap().wrap_id.clone();
            if o.wrapping_key_id != wrap {
                return Err(err(
                    ErrorKind::WrongKey,
                    format!(
                        "wrapping-key id {:?} != envelope {wrap:?}",
                        o.wrapping_key_id
                    ),
                ));
            }
        }
        l.ctx = ring.inner.read().unwrap().context;
        l.ring = Some(Arc::new(ring));
    } else if !o.wrapping_key_id.is_empty() {
        return Err(err(ErrorKind::Invalid, "WrappingKeyID requires encryption"));
    }
    Ok(l)
}

/// One framed block with its sealed payload.
pub(crate) struct FramedBlock {
    pub hdr: BlockHeader,
    /// Payload without header.
    pub sealed: Vec<u8>,
    pub off: u64,
}

/// One file-complete commit group awaiting validation.
pub(crate) struct AssembledGroup {
    pub data: Vec<FramedBlock>,
    pub completion: FramedBlock,
}

/// Bound replay silence during assembly: report at least this often
/// per file even when validation of the first group is far away.
const ASSEMBLE_NOTE_EVERY: u64 = 4 << 20;

/// Walk one segment's framing and assemble commit groups. Groups are
/// contiguous on disk, so assembly is strict. A trailing group
/// without its completion is discarded as a whole. When `torn_tail_ok`
/// (the active segment), unparseable bytes that no valid frame
/// follows truncate to the last valid end; unparseable bytes followed
/// by more valid frames fail closed.
///
/// Returns (groups in file order, valid end, highest block sequence
/// among complete frames, discarded tail groups).
pub(crate) fn assemble_file<'a>(
    path: &Path,
    id: u64,
    torn_tail_ok: bool,
    note: Option<&'a mut (dyn FnMut(u64, i64) + 'a)>,
) -> Result<(Vec<AssembledGroup>, u64, u64, u64)> {
    let mut f =
        File::open(path).map_err(|e| err(ErrorKind::Io, format!("open segment {id}: {e}")))?;
    let size = f
        .metadata()
        .map_err(|e| err(ErrorKind::Io, format!("stat segment {id}: {e}")))?
        .len();
    // Walk framing first, collecting frame offsets.
    let mut offs: Vec<u64> = Vec::new();
    let mut off: u64 = 0;
    let mut noted: u64 = 0;
    let mut discarded: u64 = 0;
    // Own the callback through a local Option to satisfy the borrow checker.
    let mut note = note;
    macro_rules! emit {
        ($records:expr, $bytes:expr) => {
            if let Some(n) = note.as_deref_mut() {
                n($records, $bytes);
            }
        };
    }
    while off < size {
        if off - noted >= ASSEMBLE_NOTE_EVERY {
            emit!(0, (off - noted) as i64);
            noted = off;
        }
        match peek_block_next(&mut f, off, size) {
            Ok(Some(next)) => {
                offs.push(off);
                off = next;
            }
            Ok(None) => break,
            Err(e) => {
                if !torn_tail_ok {
                    return Err(err(
                        ErrorKind::Corrupt,
                        format!("segment {id} offset {off}: {e}"),
                    ));
                }
                let (roff, ok, scanned) = match resync_frame(&mut f, off, size) {
                    Ok(v) => v,
                    Err(rerr) => {
                        return Err(err(
                            ErrorKind::Corrupt,
                            format!("segment {id} offset {off}: {e} (resync failed: {rerr})"),
                        ));
                    }
                };
                if scanned > 0 {
                    emit!(0, scanned as i64);
                }
                if ok {
                    return Err(err(
                        ErrorKind::Corrupt,
                        format!("segment {id} offset {off}: {e} (next valid frame at {roff})"),
                    ));
                }
                // Torn tail: nothing valid follows.
                discarded = 1;
                break;
            }
        }
    }
    if off > noted {
        emit!(0, (off - noted) as i64);
    }
    // Read frames.
    let mut frames: Vec<FramedBlock> = Vec::with_capacity(offs.len());
    let mut read: u64 = 0;
    let mut noted_read: u64 = 0;
    let mut max_block: u64 = 0;
    for boff in &offs {
        match read_block_frame(&mut f, *boff, ABS_MAX_BLOCK_BYTES as u32) {
            Ok(Some((hdr, frame, _))) => {
                read += frame.len() as u64;
                if read - noted_read >= ASSEMBLE_NOTE_EVERY {
                    emit!(0, (read - noted_read) as i64);
                    noted_read = read;
                }
                let sealed = frame[BLOCK_HEADER_LEN..].to_vec();
                frames.push(FramedBlock {
                    hdr,
                    sealed,
                    off: *boff,
                });
            }
            Ok(None) => {
                return Err(err(
                    ErrorKind::Corrupt,
                    format!("segment {id} offset {boff}: torn frame on reread"),
                ));
            }
            Err(e) => {
                return Err(err(
                    ErrorKind::Corrupt,
                    format!("segment {id} offset {boff}: {e}"),
                ))
            }
        }
    }
    if read > noted_read {
        emit!(0, (read - noted_read) as i64);
    }
    // Positional assembly: data blocks accumulate on the open group
    // and the completion binds them.
    let mut groups: Vec<AssembledGroup> = Vec::new();
    let mut open_data: Vec<FramedBlock> = Vec::new();
    let mut open_first: u64 = 0;
    let mut have_open = false;
    for b in frames {
        if b.hdr.completion {
            if !have_open {
                return Err(err(
                    ErrorKind::Corrupt,
                    format!("segment {id} offset {}: orphan completion", b.off),
                ));
            }
            for db in &open_data {
                max_block = max_block.max(db.hdr.block_seq);
            }
            max_block = max_block.max(b.hdr.block_seq);
            groups.push(AssembledGroup {
                data: std::mem::take(&mut open_data),
                completion: b,
            });
            have_open = false;
            continue;
        }
        if !have_open {
            open_first = b.off;
            have_open = true;
        }
        open_data.push(b);
    }
    let mut valid_end = off;
    if have_open {
        // Incomplete trailing group: discard whole.
        valid_end = open_first;
        discarded = 1;
    }
    Ok((groups, valid_end, max_block, discarded))
}

/// Scan `[from,size)` for the next offset holding a structurally
/// valid block header. Reports the first hit and the bytes scanned
/// (for progress reporting).
fn resync_frame(f: &mut File, from: u64, size: u64) -> Result<(u64, bool, u64)> {
    if size - from < BLOCK_HEADER_LEN as u64 {
        return Ok((0, false, 0));
    }
    const CHUNK: u64 = 1 << 20;
    let mut carry: Vec<u8> = Vec::new();
    let mut pos = from;
    let mut scanned: u64 = 0;
    while pos < size {
        let end = (pos + CHUNK).min(size);
        let mut chunk = vec![0u8; (end - pos) as usize];
        if let Err(e) = f
            .seek(SeekFrom::Start(pos))
            .and_then(|_| f.read_exact(&mut chunk))
        {
            return Err(err(ErrorKind::Io, format!("resync read at {pos}: {e}")));
        }
        scanned += chunk.len() as u64;
        let mut buf: Vec<u8>;
        let buf_pos: u64;
        if carry.is_empty() {
            buf = chunk;
            buf_pos = pos;
        } else {
            buf = carry.clone();
            buf.extend_from_slice(&chunk);
            buf_pos = pos - carry.len() as u64;
            carry.clear();
        }
        if buf.len() >= BLOCK_HEADER_LEN {
            let limit = buf.len() - BLOCK_HEADER_LEN;
            for i in 0..=limit {
                if buf[i] != BLOCK_MAGIC[0] || &buf[i..i + 4] != BLOCK_MAGIC {
                    continue;
                }
                if u16::from_le_bytes([buf[i + 4], buf[i + 5]]) != BLOCK_VERSION {
                    continue;
                }
                if parse_block_header(&buf[i..i + BLOCK_HEADER_LEN]).is_ok() {
                    return Ok((buf_pos + i as u64, true, scanned));
                }
            }
        }
        let tail_keep = (BLOCK_HEADER_LEN - 1).min(buf.len());
        carry = buf[buf.len() - tail_keep..].to_vec();
        pos = end;
    }
    Ok((0, false, scanned))
}

/// One decrypted data block with its records.
pub(crate) struct ValidatedRecords {
    /// Plaintext body (records borrow from it via ranges).
    pub plain: Vec<u8>,
    pub recs: Vec<ValidatedRecord>,
}

pub(crate) struct ValidatedRecord {
    pub seq: u64,
    pub deleted: bool,
    pub key_range: (usize, usize),
    pub val_range: (usize, usize),
}

impl Loader {
    /// Authenticate, decrypt and decompress one sealed block body.
    pub fn open_payload(&self, hdr: &BlockHeader, sealed: &[u8]) -> Result<Vec<u8>> {
        let (crypt, key) = self.crypt_for(hdr)?;
        // The seal bound the header with a zero sealed-length field
        // plus the store id and database context.
        let mut hdr59 = vec![0u8; 59];
        hdr59.copy_from_slice(&hdr.raw[..59]);
        hdr59[30..34].copy_from_slice(&[0, 0, 0, 0]);
        let comp = crypt.open(
            &key,
            &hdr.nonce,
            sealed,
            &block_aad(&hdr59, &self.store_id, &self.ctx),
        )?;
        self.comp
            .decompress(hdr.compression, &comp, hdr.uncompressed_len)
    }

    /// Resolve a block's algorithm and key.
    fn crypt_for(&self, hdr: &BlockHeader) -> Result<(BlockCrypt, [u8; 32])> {
        match hdr.algo {
            ALGO_NONE => {
                if self.ring.is_some() {
                    return Err(err(ErrorKind::Corrupt, "plain block in encrypted store"));
                }
                if hdr.key_id != 0 {
                    return Err(err(ErrorKind::Corrupt, "plain block with key id"));
                }
                Ok((BlockCrypt { algo: ALGO_NONE }, [0u8; 32]))
            }
            ALGO_AES_GCM => {
                let ring = self
                    .ring
                    .as_ref()
                    .ok_or_else(|| err(ErrorKind::Corrupt, "encrypted block in plain store"))?;
                let key = ring.by_id(hdr.key_id).ok_or_else(|| {
                    err(ErrorKind::UnknownKeyID, format!("key id {}", hdr.key_id))
                })?;
                if hdr.nonce.len() != 12 {
                    return Err(err(
                        ErrorKind::Corrupt,
                        format!("bad AES-GCM nonce size {}", hdr.nonce.len()),
                    ));
                }
                Ok((BlockCrypt { algo: hdr.algo }, key))
            }
            a => Err(err(
                ErrorKind::Corrupt,
                format!("unknown block algorithm {a}"),
            )),
        }
    }

    /// Authenticate and decrypt a complete group's data and
    /// completion, verifying ordering, counts, sizes and the content
    /// digest. Any failure fails closed.
    pub fn validate_group(
        &self,
        file_id: u64,
        g: &AssembledGroup,
    ) -> Result<(Vec<ValidatedRecords>, crate::block::Completion)> {
        let mut out = Vec::with_capacity(g.data.len());
        let chdr = &g.completion.hdr;
        let ccomp = self
            .open_payload(chdr, &g.completion.sealed)
            .map_err(|e| err(e.kind(), format!("segment {file_id} group completion: {e}")))?;
        let c = decode_completion(&ccomp)
            .map_err(|e| err(e.kind(), format!("segment {file_id} group completion: {e}")))?;
        if g.data.len() as u64 != c.block_count as u64 {
            return Err(err(
                ErrorKind::Corrupt,
                format!(
                    "segment {file_id} group {} has {} data blocks, completion says {}",
                    c.group_id,
                    g.data.len(),
                    c.block_count
                ),
            ));
        }
        let mut seqs = Vec::with_capacity(g.data.len());
        let mut sealed: Vec<&[u8]> = Vec::with_capacity(g.data.len());
        for (i, b) in g.data.iter().enumerate() {
            seqs.push(b.hdr.block_seq);
            sealed.push(&b.sealed);
            if seqs[i] != c.block_seqs[i] {
                return Err(err(
                    ErrorKind::Corrupt,
                    format!(
                        "segment {file_id} group {} block {i} identity mismatch",
                        c.group_id
                    ),
                ));
            }
        }
        if group_digest(c.group_id, c.block_count, &seqs, &sealed) != c.digest {
            return Err(err(
                ErrorKind::Corrupt,
                format!("segment {file_id} group {} digest mismatch", c.group_id),
            ));
        }
        let mut total_records: u32 = 0;
        let mut total_plain: u64 = 0;
        for (i, b) in g.data.iter().enumerate() {
            let plain = self
                .open_payload(&b.hdr, &b.sealed)
                .map_err(|e| err(e.kind(), format!("segment {file_id} offset {}: {e}", b.off)))?;
            let body = decode_body(&plain, b.hdr.record_count)
                .map_err(|e| err(e.kind(), format!("segment {file_id} offset {}: {e}", b.off)))?;
            if body.group_id != c.group_id
                || body.block_index != i as u32
                || body.block_count != c.block_count
            {
                return Err(err(
                    ErrorKind::Corrupt,
                    format!(
                        "segment {file_id} group {} block {i} framing mismatch",
                        c.group_id
                    ),
                ));
            }
            let recs = body
                .records
                .into_iter()
                .map(|r| ValidatedRecord {
                    seq: r.seq,
                    deleted: r.deleted,
                    key_range: r.key_range,
                    val_range: r.val_range,
                })
                .collect::<Vec<_>>();
            total_records += recs.len() as u32;
            total_plain += plain.len() as u64;
            out.push(ValidatedRecords { plain, recs });
        }
        if total_records != c.total_records || total_plain != c.total_plain {
            return Err(err(
                ErrorKind::Corrupt,
                format!(
                    "segment {file_id} group {} declares {} records/{} bytes, found {total_records}/{total_plain}",
                    c.group_id, c.total_records, c.total_plain
                ),
            ));
        }
        Ok((out, c))
    }

    /// Assemble, validate and decrypt one file's complete groups,
    /// calling `visit` per record in file order with its on-disk
    /// location. Returns (max block, max group, valid end, discarded).
    pub fn visit_validated_file(
        &mut self,
        id: u64,
        mut visit: impl FnMut(&ValidatedRecord, &[u8], Location) -> Result<()>,
    ) -> Result<(u64, u64, u64, u64)> {
        let path = self.dir.join(SEGMENTS_DIR_NAME).join(segment_file_name(id));
        let torn_ok = self.torn_tail_id != 0 && id == self.torn_tail_id;
        // Borrow-split: assemble without &mut self (replay notes need &mut).
        struct Notes {
            records: u64,
            bytes: i64,
        }
        let mut pending_notes: Vec<Notes> = Vec::new();
        let groups: Vec<AssembledGroup>;
        let valid_end: u64;
        let discarded: u64;
        {
            let mut note = |records: u64, bytes: i64| {
                pending_notes.push(Notes { records, bytes });
            };
            let (g, ve, _, disc) = assemble_file(&path, id, torn_ok, Some(&mut note))?;
            groups = g;
            valid_end = ve;
            discarded = disc;
        }
        for n in pending_notes {
            self.note_replay(n.records, n.bytes);
        }
        let mut max_block: u64 = 0;
        let mut max_group: u64 = 0;
        for g in &groups {
            let (blocks, c) = self.validate_group(id, g)?;
            max_group = max_group.max(c.group_id);
            for b in &g.data {
                max_block = max_block.max(b.hdr.block_seq);
            }
            max_block = max_block.max(g.completion.hdr.block_seq);
            for (bi, b) in blocks.iter().enumerate() {
                for (j, r) in b.recs.iter().enumerate() {
                    let key = &b.plain[r.key_range.0..r.key_range.1];
                    let val = &b.plain[r.val_range.0..r.val_range.1];
                    let size = (key.len() + val.len() + 17) as u32;
                    let loc = Location {
                        file_id: id,
                        block_id: g.data[bi].hdr.block_seq,
                        block_offset: g.data[bi].off as i64,
                        record_index: j as u32,
                        sequence: r.seq,
                        size,
                    };
                    visit(r, &b.plain, loc)?;
                }
                let blk_bytes = g.data.get(bi).map(|d| d.sealed.len() as i64).unwrap_or(0);
                self.note_replay(b.recs.len() as u64, blk_bytes);
            }
            self.note_replay(0, g.completion.sealed.len() as i64);
        }
        Ok((max_block, max_group, valid_end, discarded))
    }

    /// Stream every member segment in two passes. Pass one validates
    /// complete groups and resolves each key's winner by highest
    /// logical sequence. Pass two re-reads winner blocks
    /// newest-file-first and delivers each winner exactly once.
    /// Returns (max block, max group, valid ends, discarded).
    pub fn scan_all(
        &mut self,
        members: &[u64],
        f: Option<LoadFunc<'_>>,
        idx: Option<&Index>,
        fs: Option<&FileStatsRegistry>,
        ordered: bool,
        cache: &mut crate::read_cache::Cache,
    ) -> Result<(u64, u64, std::collections::HashMap<u64, u64>, u64)> {
        use std::collections::HashMap;
        let mut ends: HashMap<u64, u64> = HashMap::with_capacity(members.len());
        let mut standalone: HashMap<Vec<u8>, WinnerLoc> = HashMap::new();
        let mut max_block: u64 = 0;
        let mut max_group: u64 = 0;
        let mut discarded: u64 = 0;
        self.set_replay_pass(1);
        for id in members {
            let (mb, mg, end, disc) = self.visit_validated_file(*id, |r, plain, loc| {
                let key = &plain[r.key_range.0..r.key_range.1];
                if let (Some(ix), Some(fs)) = (idx, fs) {
                    let val = &plain[r.val_range.0..r.val_range.1];
                    let size = (key.len() + val.len() + 17) as u32;
                    ix.commit_record(fs, key, r.seq, r.deleted, loc, size);
                    return Ok(());
                }
                match standalone.get(key) {
                    Some(w) if r.seq <= w.seq => {}
                    _ => {
                        standalone.insert(
                            key.to_vec(),
                            WinnerLoc {
                                seq: r.seq,
                                tomb: r.deleted,
                                loc,
                            },
                        );
                    }
                }
                Ok(())
            })?;
            ends.insert(*id, end);
            discarded += disc;
            max_block = max_block.max(mb);
            max_group = max_group.max(mg);
            self.note_replay_file();
        }
        let f = match f {
            Some(f) => f,
            None => return Ok((max_block, max_group, ends, discarded)),
        };
        self.set_replay_pass(2);
        let winners: HashMap<Vec<u8>, WinnerLoc> = match idx {
            Some(ix) => ix.snapshot_winners(),
            None => standalone,
        };
        if ordered {
            let mut sorted: Vec<_> = winners.iter().collect();
            sorted.sort_by(|a, b| a.0.cmp(b.0));
            // First pass authenticated every committed group. Sorted keys can
            // interleave blocks: retain decoded bodies in the byte-bounded hot
            // cache, plus the current block even when cache admission is disabled.
            let mut current: Option<((u64, i64), Arc<crate::read_cache::Block>)> = None;
            for (key, winner) in sorted {
                let loc = winner.loc;
                let cache_key = (loc.file_id, loc.block_offset);
                if current.as_ref().map(|c| c.0) != Some(cache_key) {
                    let block = match cache.get(cache_key) {
                        Some(block) => block,
                        None => {
                            let path = self.dir.join(SEGMENTS_DIR_NAME).join(segment_file_name(loc.file_id));
                            let mut file = File::open(path).map_err(|e| err(ErrorKind::Io, e.to_string()))?;
                            let (header, frame, _) = read_block_frame(&mut file, loc.block_offset as u64, ABS_MAX_BLOCK_BYTES as u32)?
                                .ok_or_else(|| err(ErrorKind::Corrupt, "missing winner block"))?;
                            let plain = self.open_payload(&header, &frame[BLOCK_HEADER_LEN..])?;
                            let body = decode_body(&plain, header.record_count)?;
                            let block = Arc::new(crate::read_cache::Block { frame, plain, body });
                            cache.insert(cache_key, block.clone());
                            block
                        }
                    };
                    current = Some((cache_key, block));
                }
                let block = &current.as_ref().unwrap().1;
                let plain = &block.plain;
                let record = block.body.records.get(loc.record_index as usize)
                    .ok_or_else(|| err(ErrorKind::Corrupt, "bad winner index"))?;
                if &plain[record.key_range.0..record.key_range.1] != key.as_slice() {
                    return Err(err(ErrorKind::Corrupt, "winner key mismatch"));
                }
                f(&[Record { key: key.clone(), value: if winner.tomb { Vec::new() } else {
                    plain[record.val_range.0..record.val_range.1].to_vec()
                }, sequence: winner.seq, deleted: winner.tomb }])?;
            }
            return Ok((max_block, max_group, ends, discarded));
        }
        // Pass two: newest files first, delivering winner locations only.
        for id in members.iter().rev() {
            let mut pending: Vec<Record> = Vec::new();
            let mut last_off: i64 = -1;
            let verr = self.visit_validated_file(*id, |r, plain, loc| {
                let key = &plain[r.key_range.0..r.key_range.1];
                let val = &plain[r.val_range.0..r.val_range.1];
                match winners.get(key) {
                    Some(w) if w.seq == r.seq && w.loc == loc => {}
                    _ => return Ok(()),
                }
                if loc.block_offset != last_off {
                    if !pending.is_empty() {
                        f(std::mem::take(&mut pending).as_slice())?;
                    }
                    last_off = loc.block_offset;
                }
                pending.push(Record {
                    key: key.to_vec(),
                    value: if r.deleted { Vec::new() } else { val.to_vec() },
                    sequence: r.seq,
                    deleted: r.deleted,
                });
                Ok(())
            });
            verr?;
            if !pending.is_empty() {
                f(pending.as_slice())?;
            }
        }
        Ok((max_block, max_group, ends, discarded))
    }
}

/// Sum member segment sizes for replay totals.
pub(crate) fn segment_bytes_total(dir: &Path, members: &[u64]) -> i64 {
    let mut total: i64 = 0;
    for id in members {
        if let Ok(st) = std::fs::metadata(dir.join(SEGMENTS_DIR_NAME).join(segment_file_name(*id)))
        {
            total += st.len() as i64;
        }
    }
    total
}
