//! Maintenance: rewrite and rebind.
//!
//! Rust port of `spool/maintenance.go`. Rewrites every authoritative
//! segment: re-encrypting under the current data key
//! (`rewrite_data_keys`) or additionally rebinding the database
//! context for reseed (`rebind_context`).
//!
//! Intent file layout (all integers little-endian, 40 bytes):
//!
//! ```text
//! off  size  field
//! 0    4     magic "SPLI"
//! 4    2     format version (1)
//! 6    1     op (1 = rewrite, 2 = rebind)
//! 7    1     phase (1 = staging, 2 = segments done,
//!                  3 = keyring done, 4 = manifest done)
//! 8    16    target context (rebind; rewrite: zeros)
//! 24   8     manifest generation target
//! 32   4     total files (progress aid)
//! 36   4     CRC32-Castagnoli over bytes [0,36)
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::block::{encode_body, encode_completion, group_digest, Completion, BLOCK_HEADER_LEN};
use crate::compression::Compressor;
use crate::crypto::BlockCrypt;
use crate::error::{err, ErrorKind, Result};
use crate::keyring::Keyring;
use crate::loader::{assemble_file, Loader};
use crate::manifest::{
    atomic_write_file, dir_sync, Manifest, MANIFEST_FILE_NAME, SEGMENTS_DIR_NAME,
};
use crate::options::FaultHooks;
use crate::record::PendingRecord;
use crate::segment::segment_file_name;
use crate::store::{seal_frame, Store};

pub(crate) const INTENT_FILE_NAME: &str = "maintenance.intent";
const INTENT_LEN: usize = 40;
const MAINT_OP_REWRITE: u8 = 1;
const MAINT_OP_REBIND: u8 = 2;
const MAINT_PHASE_STAGING: u8 = 1;
const MAINT_PHASE_SEGMENTS_DONE: u8 = 2;
const MAINT_PHASE_KEYRING_DONE: u8 = 3;
const MAINT_PHASE_MANIFEST_DONE: u8 = 4;

/// Parsed maintenance intent.
struct MaintIntent {
    op: u8,
    phase: u8,
    target_context: [u8; 16],
    manifest_target: u64,
    total_files: u32,
}

fn encode_intent(m: &MaintIntent) -> Vec<u8> {
    let mut out = vec![0u8; INTENT_LEN];
    out[0..4].copy_from_slice(b"SPLI");
    out[4..6].copy_from_slice(&1u16.to_le_bytes());
    out[6] = m.op;
    out[7] = m.phase;
    out[8..24].copy_from_slice(&m.target_context);
    out[24..32].copy_from_slice(&m.manifest_target.to_le_bytes());
    out[32..36].copy_from_slice(&m.total_files.to_le_bytes());
    let crc = crate::block::crc32c(&out[..36]);
    out[36..40].copy_from_slice(&crc.to_le_bytes());
    out
}

fn parse_intent(raw: &[u8]) -> Result<MaintIntent> {
    if raw.len() != INTENT_LEN {
        return Err(err(
            ErrorKind::Corrupt,
            format!("intent size {}", raw.len()),
        ));
    }
    if &raw[0..4] != b"SPLI" {
        return Err(err(ErrorKind::Corrupt, "bad intent magic"));
    }
    let v = u16::from_le_bytes([raw[4], raw[5]]);
    if v != 1 {
        return Err(err(
            ErrorKind::UnsupportedVersion,
            format!("intent version {v}"),
        ));
    }
    let want = u32::from_le_bytes([raw[36], raw[37], raw[38], raw[39]]);
    if crate::block::crc32c(&raw[..36]) != want {
        return Err(err(ErrorKind::Corrupt, "intent checksum mismatch"));
    }
    let mut m = MaintIntent {
        op: raw[6],
        phase: raw[7],
        target_context: [0u8; 16],
        manifest_target: 0,
        total_files: 0,
    };
    if m.op != MAINT_OP_REWRITE && m.op != MAINT_OP_REBIND {
        return Err(err(ErrorKind::Corrupt, format!("intent op {}", m.op)));
    }
    if m.phase < MAINT_PHASE_STAGING || m.phase > MAINT_PHASE_MANIFEST_DONE {
        return Err(err(ErrorKind::Corrupt, format!("intent phase {}", m.phase)));
    }
    m.target_context.copy_from_slice(&raw[8..24]);
    m.manifest_target = u64::from_le_bytes(raw[24..32].try_into().unwrap());
    m.total_files = u32::from_le_bytes(raw[32..36].try_into().unwrap());
    if m.manifest_target == 0 {
        return Err(err(ErrorKind::Corrupt, "intent lacks a manifest target"));
    }
    Ok(m)
}

impl Store {
    /// Reject mutating operations while maintenance holds the store.
    pub(crate) fn check_maint(&self) -> Result<()> {
        if self.inner.maint_active.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Maintenance, "maintenance in progress"));
        }
        Ok(())
    }

    /// Record visible maintenance progress.
    fn set_maint_phase(&self, phase: &str, done: u64, total: u64) {
        let mut m = self.inner.maint.lock().unwrap();
        m.phase = phase.to_string();
        m.done = done;
        m.total = total;
    }

    /// Take exclusive maintenance ownership: fail when another
    /// maintenance holds the lock, gate mutating operations, and
    /// drain admitted groups so segment files stay stable for
    /// staging. Returns a guard releasing ownership on drop.
    fn maint_begin(&self, op: &str, total: u64) -> Result<MaintGuard<'_>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        // Serialize against checkpoint capture under admission.
        let admission = self.inner.admission.lock().unwrap();
        let excl = match self.inner.maint_excl.try_lock() {
            Ok(g) => g,
            Err(_) => return Err(err(ErrorKind::Maintenance, "maintenance in progress")),
        };
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        self.inner.maint_active.store(true, Ordering::SeqCst);
        drop(admission);
        self.set_maint_phase(op, 0, total);
        // No new admissions past the gate; wait out everything
        // admitted before it.
        let fence = self.inner.last_group.load(Ordering::SeqCst);
        if let Err(e) = self.wait_for_group(fence, false) {
            self.inner.maint_active.store(false, Ordering::SeqCst);
            self.set_maint_phase("idle", 0, 0);
            return Err(e);
        }
        // Push async-buffered bytes to the file.
        if let Err(e) = self.inner.seg.commit(crate::options::Durability::Flush) {
            self.inner.maint_active.store(false, Ordering::SeqCst);
            self.set_maint_phase("idle", 0, 0);
            return Err(e);
        }
        Ok(MaintGuard {
            store: self,
            _excl: excl,
        })
    }

    /// Re-encrypt every authoritative segment under the current data
    /// key with fresh nonces. Logical sequences, group ids and record
    /// contents are unchanged. Plain stores return nil.
    pub fn rewrite_data_keys(&self) -> Result<()> {
        if self.inner.ring.is_none() {
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(err(ErrorKind::Closed, "store is closed"));
            }
            return self.check_terminal();
        }
        let (members, gen_target) = {
            let m = self.inner.manifest.lock().unwrap();
            (m.members.clone(), m.generation + 1)
        };
        let _guard = self.maint_begin("rewrite", members.len() as u64)?;
        let (key_id, key) = self
            .current_data_key()
            .ok_or_else(|| err(ErrorKind::NoCurrentKey, "no current data key"))?;
        let mut intent = MaintIntent {
            op: MAINT_OP_REWRITE,
            phase: MAINT_PHASE_STAGING,
            target_context: [0u8; 16],
            manifest_target: gen_target,
            total_files: members.len() as u32,
        };
        self.write_intent_file(&intent)
            .map_err(|e| err(e.kind(), format!("write rewrite intent: {e}")))?;
        let env = self.maint_env();
        let ctx = *self.inner.ctx.read().unwrap();
        let mut rebased: HashMap<u64, i64> = HashMap::new();
        for (i, id) in members.iter().enumerate() {
            let offsets =
                stage_segment_file(&env, *id, key_id, &key, &ctx, &ctx, &[0u8; 16], false)?;
            rebased.extend(offsets);
            self.set_maint_phase("rewrite", (i + 1) as u64, members.len() as u64);
        }
        // The active file was renamed over: rebind the writer to the
        // new inode before anything can append again.
        if let Err(e) = self.inner.seg.reopen_active() {
            let e = err(e.kind(), format!("reopen active segment: {e}"));
            self.set_terminal(e.clone());
            return Err(e);
        }
        if let Err(missing) = self.inner.idx.rebase_blocks(&rebased) {
            let e = err(
                ErrorKind::Corrupt,
                format!("rewrite missing block {missing} in rebase map"),
            );
            self.set_terminal(e.clone());
            return Err(e);
        }
        intent.phase = MAINT_PHASE_SEGMENTS_DONE;
        self.write_intent_file(&intent)
            .map_err(|e| err(e.kind(), format!("write rewrite intent: {e}")))?;
        // Publish the rewrite generation. Members are unchanged; the
        // bump separates pre/post-rewrite content for checkpoint cuts.
        {
            let mut m = self.inner.manifest.lock().unwrap();
            m.generation = gen_target;
        }
        if let Err(e) = self.persist_manifest() {
            let e = err(e.kind(), format!("publish rewrite: {e}"));
            self.set_terminal(e.clone());
            return Err(e);
        }
        remove_intent_file(&self.inner.dir, self.inner.faults.as_deref())?;
        Ok(())
    }

    /// Rewrite every authoritative segment bound to a new database
    /// context and adopt it, for reseed under a new identity. The
    /// wrapping material is unchanged. Plain stores only update the
    /// recorded context.
    pub fn rebind_context(&self, new_context: [u8; 16]) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        if self.inner.ring.is_none() {
            return self.rebind_plain(new_context);
        }
        let (members, gen_target) = {
            let m = self.inner.manifest.lock().unwrap();
            (m.members.clone(), m.generation + 1)
        };
        let _guard = self.maint_begin("rebind", members.len() as u64)?;
        let (key_id, key) = self
            .current_data_key()
            .ok_or_else(|| err(ErrorKind::NoCurrentKey, "no current data key"))?;
        let mut intent = MaintIntent {
            op: MAINT_OP_REBIND,
            phase: MAINT_PHASE_STAGING,
            target_context: new_context,
            manifest_target: gen_target,
            total_files: members.len() as u32,
        };
        self.write_intent_file(&intent)
            .map_err(|e| err(e.kind(), format!("write rebind intent: {e}")))?;
        let env = self.maint_env();
        let ctx = *self.inner.ctx.read().unwrap();
        let mut rebased: HashMap<u64, i64> = HashMap::new();
        for (i, id) in members.iter().enumerate() {
            let offsets = stage_segment_file(
                &env,
                *id,
                key_id,
                &key,
                &ctx,
                &new_context,
                &[0u8; 16],
                false,
            )?;
            rebased.extend(offsets);
            self.set_maint_phase("rebind", (i + 1) as u64, members.len() as u64);
        }
        if let Err(e) = self.inner.seg.reopen_active() {
            let e = err(e.kind(), format!("reopen active segment: {e}"));
            self.set_terminal(e.clone());
            return Err(e);
        }
        if let Err(missing) = self.inner.idx.rebase_blocks(&rebased) {
            let e = err(
                ErrorKind::Corrupt,
                format!("rebind missing block {missing} in rebase map"),
            );
            self.set_terminal(e.clone());
            return Err(e);
        }
        intent.phase = MAINT_PHASE_SEGMENTS_DONE;
        self.write_intent_file(&intent)
            .map_err(|e| err(e.kind(), format!("write rebind intent: {e}")))?;
        // Keyring first, then the manifest pointing at it.
        self.publish_rebind_keyring(&new_context)?;
        intent.phase = MAINT_PHASE_KEYRING_DONE;
        self.write_intent_file(&intent)
            .map_err(|e| err(e.kind(), format!("write rebind intent: {e}")))?;
        {
            let mut m = self.inner.manifest.lock().unwrap();
            m.context = new_context;
            m.generation = gen_target;
        }
        if let Err(e) = self.persist_manifest() {
            let e = err(e.kind(), format!("publish rebind: {e}"));
            self.set_terminal(e.clone());
            return Err(e);
        }
        *self.inner.ctx.write().unwrap() = new_context;
        remove_intent_file(&self.inner.dir, self.inner.faults.as_deref())?;
        Ok(())
    }

    /// Rebind a plaintext store: only the recorded context changes.
    fn rebind_plain(&self, new_context: [u8; 16]) -> Result<()> {
        let gen_target = self.inner.manifest.lock().unwrap().generation + 1;
        let _guard = self.maint_begin("rebind", 0)?;
        let intent = MaintIntent {
            op: MAINT_OP_REBIND,
            phase: MAINT_PHASE_SEGMENTS_DONE,
            target_context: new_context,
            manifest_target: gen_target,
            total_files: 0,
        };
        self.write_intent_file(&intent)
            .map_err(|e| err(e.kind(), format!("write rebind intent: {e}")))?;
        {
            let mut m = self.inner.manifest.lock().unwrap();
            m.context = new_context;
            m.generation = gen_target;
        }
        if let Err(e) = self.persist_manifest() {
            let e = err(e.kind(), format!("publish rebind: {e}"));
            self.set_terminal(e.clone());
            return Err(e);
        }
        *self.inner.ctx.write().unwrap() = new_context;
        remove_intent_file(&self.inner.dir, self.inner.faults.as_deref())?;
        Ok(())
    }

    /// Persist one maintenance intent.
    fn write_intent_file(&self, intent: &MaintIntent) -> Result<()> {
        if let Some(f) = &self.inner.faults {
            f.trip("intent")
                .map_err(|e| err(ErrorKind::Io, format!("injected intent fault: {e}")))?;
        }
        atomic_write_file(&self.inner.dir, INTENT_FILE_NAME, &encode_intent(intent))
    }

    /// Flip the envelope context and persist it. Publication failure
    /// is terminal.
    fn publish_rebind_keyring(&self, new_context: &[u8; 16]) -> Result<()> {
        let ring = self
            .inner
            .ring
            .as_ref()
            .ok_or_else(|| err(ErrorKind::NoCurrentKey, "no current data key"))?;
        let store_id = self.inner.manifest.lock().unwrap().store_id;
        let (master, pass) = (
            self.inner.master.lock().unwrap().clone(),
            self.inner.pass.lock().unwrap().clone(),
        );
        {
            let mut r = ring.inner.write().unwrap();
            let old = r.context;
            r.context = *new_context;
            if let Err(e) = crate::keyring::Keyring::write_keys_locked(
                ring.faults.as_ref(),
                &mut r,
                &self.inner.dir,
                &store_id,
                &master,
                &pass,
                None,
            ) {
                r.context = old;
                drop(r);
                let e = err(e.kind(), format!("publish rebind keyring: {e}"));
                self.set_terminal(e.clone());
                return Err(e);
            }
        }
        Ok(())
    }

    /// Snapshot the staging parameters.
    fn maint_env(&self) -> MaintEnv {
        // One critical section: two `manifest.lock()` calls in the
        // struct literal below would self-deadlock, since the first
        // temporary guard lives until the statement ends.
        let m = self.inner.manifest.lock().unwrap();
        let (store_id, man) = (m.store_id, m.clone());
        drop(m);
        MaintEnv {
            dir: self.inner.dir.clone(),
            store_id,
            man,
            ring: self.inner.ring.clone(),
            comp: self.inner.comp.clone(),
            crypt: self.inner.crypt,
            faults: self.inner.faults.clone(),
        }
    }
}

/// Releases maintenance ownership on drop.
struct MaintGuard<'a> {
    store: &'a Store,
    _excl: std::sync::MutexGuard<'a, ()>,
}

impl Drop for MaintGuard<'_> {
    fn drop(&mut self) {
        self.store.inner.maint_active.store(false, Ordering::SeqCst);
        self.store.set_maint_phase("idle", 0, 0);
    }
}

/// Staging parameters: usable while the live handle is gated, and
/// independently at resume.
struct MaintEnv {
    dir: PathBuf,
    store_id: [u8; 16],
    man: Manifest,
    ring: Option<Arc<Keyring>>,
    comp: Compressor,
    crypt: BlockCrypt,
    faults: Option<Arc<FaultHooks>>,
}

/// Re-seal one segment's validated groups under `key` and `seal_ctx`,
/// preserving group ids, block sequences, record sequences and
/// contents. Groups are read under `read_ctx`; rebind resume
/// additionally retries `fallback_ctx` on authentication failure.
/// Returns the staged data-block offsets by block sequence.
/// Reseal inputs are explicit (mirrors the Go port) rather than packed.
#[allow(clippy::too_many_arguments)]
fn stage_segment_file(
    env: &MaintEnv,
    id: u64,
    key_id: u32,
    key: &[u8; 32],
    read_ctx: &[u8; 16],
    seal_ctx: &[u8; 16],
    fallback_ctx: &[u8; 16],
    allow_fallback: bool,
) -> Result<HashMap<u64, i64>> {
    let mut offsets: HashMap<u64, i64> = HashMap::new();
    let seg_dir = env.dir.join(SEGMENTS_DIR_NAME);
    let path = seg_dir.join(segment_file_name(id));
    let (groups, _, _, _) = assemble_file(&path, id, false, None)
        .map_err(|e| err(e.kind(), format!("stage assembles segment {id}: {e}")))?;
    let mut loader = Loader {
        dir: env.dir.clone(),
        man: env.man.clone(),
        ring: env.ring.clone(),
        comp: env.comp.clone(),
        ctx: *read_ctx,
        replay: None,
        torn_tail_id: 0,
        store_id: env.store_id,
    };
    let staged_path = crate::manifest::create_temp(&seg_dir, &format!(".rew-{id}-"))
        .map_err(|e| err(e.kind(), format!("stage segment {id}: {e}")))?;
    let mut staged = fs::OpenOptions::new()
        .write(true)
        .open(&staged_path)
        .map_err(|e| err(ErrorKind::Io, format!("stage segment {id}: {e}")))?;
    let mut staged_off: i64 = 0;
    let stage_result = (|| -> Result<()> {
        use std::io::Write;
        for g in &groups {
            let (blocks, c) =
                validate_staged_group(&mut loader, id, g, fallback_ctx, allow_fallback)?;
            let (frames, body_lens) =
                reseal_data_blocks(env, &c, &blocks, g, key_id, key, seal_ctx)
                    .map_err(|e| err(e.kind(), format!("stage segment {id}: {e}")))?;
            let comp_frame =
                reseal_completion(env, &c, g, &frames, &body_lens, key_id, key, seal_ctx)
                    .map_err(|e| err(e.kind(), format!("stage segment {id}: {e}")))?;
            for (i, frame) in frames
                .iter()
                .chain(std::iter::once(&comp_frame))
                .enumerate()
            {
                if i < frames.len() {
                    offsets.insert(g.data[i].hdr.block_seq, staged_off);
                }
                staged
                    .write_all(frame)
                    .map_err(|e| err(ErrorKind::Io, format!("stage segment {id}: {e}")))?;
                staged_off += frame.len() as i64;
            }
        }
        Ok(())
    })();
    let sync_result = stage_result.and_then(|_| {
        staged
            .sync_all()
            .map_err(|e| err(ErrorKind::Io, format!("sync staged segment {id}: {e}")))
    });
    drop(staged);
    if let Err(e) = sync_result {
        let _ = fs::remove_file(&staged_path);
        return Err(e);
    }
    if let Err(e) = verify_staged_file(env, &staged_path, id, seal_ctx) {
        let _ = fs::remove_file(&staged_path);
        return Err(e);
    }
    if let Some(f) = &env.faults {
        f.trip("rename")
            .map_err(|e| err(ErrorKind::Io, format!("publish staged segment {id}: {e}")))?;
    }
    fs::rename(&staged_path, &path)
        .map_err(|e| err(ErrorKind::Io, format!("publish staged segment {id}: {e}")))?;
    if let Some(f) = &env.faults {
        f.trip("dirsync")
            .map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
    }
    dir_sync(&seg_dir).map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
    Ok(offsets)
}

/// Validate one group under the loader's context, retrying the
/// fallback context once on authentication failure when allowed.
fn validate_staged_group(
    loader: &mut Loader,
    id: u64,
    g: &crate::loader::AssembledGroup,
    fallback_ctx: &[u8; 16],
    allow_fallback: bool,
) -> Result<(Vec<crate::loader::ValidatedRecords>, Completion)> {
    let primary = loader.ctx;
    match loader.validate_group(id, g) {
        Ok(v) => Ok(v),
        Err(e) if allow_fallback && e.kind() == ErrorKind::AuthFailed => {
            loader.ctx = *fallback_ctx;
            match loader.validate_group(id, g) {
                Ok(v) => Ok(v),
                Err(e) => {
                    loader.ctx = primary;
                    Err(e)
                }
            }
        }
        Err(e) => Err(e),
    }
}

/// Re-encode one validated group's data blocks with identical framing
/// and fresh sealing. Each block's original codec is preserved
/// exactly; any length drift aborts the maintenance.
fn reseal_data_blocks(
    env: &MaintEnv,
    c: &Completion,
    blocks: &[crate::loader::ValidatedRecords],
    g: &crate::loader::AssembledGroup,
    key_id: u32,
    key: &[u8; 32],
    seal_ctx: &[u8; 16],
) -> Result<(Vec<Vec<u8>>, Vec<usize>)> {
    let mut frames = Vec::with_capacity(blocks.len());
    let mut lens = Vec::with_capacity(blocks.len());
    for (idx, b) in blocks.iter().enumerate() {
        let mut pending = Vec::with_capacity(b.recs.len());
        for r in &b.recs {
            pending.push(PendingRecord {
                key: b.plain[r.key_range.0..r.key_range.1].to_vec(),
                value: if r.deleted {
                    Vec::new()
                } else {
                    b.plain[r.val_range.0..r.val_range.1].to_vec()
                },
                seq: r.seq,
                tomb: r.deleted,
                group: c.group_id,
                size: 0,
            });
        }
        let mode = g.data[idx].hdr.compression;
        let body = encode_body(c.group_id, idx as u32, c.block_count, &pending);
        let (frame, body_len) = seal_frame(
            &env.comp,
            env.crypt,
            mode,
            &env.store_id,
            &body,
            false,
            pending.len() as u32,
            key_id,
            key,
            g.data[idx].hdr.block_seq,
            seal_ctx,
        )?;
        let want = BLOCK_HEADER_LEN + g.data[idx].hdr.sealed_len as usize;
        if frame.len() != want {
            return Err(err(
                ErrorKind::Corrupt,
                format!("reseal length drift {} != {want}", frame.len()),
            ));
        }
        frames.push(frame);
        lens.push(body_len);
    }
    Ok((frames, lens))
}

/// Re-seal a group's completion over its fresh data frames,
/// preserving group identity with a recomputed digest.
/// Reseal inputs are explicit (mirrors the Go port) rather than packed.
#[allow(clippy::too_many_arguments)]
fn reseal_completion(
    env: &MaintEnv,
    c: &Completion,
    g: &crate::loader::AssembledGroup,
    frames: &[Vec<u8>],
    body_lens: &[usize],
    key_id: u32,
    key: &[u8; 32],
    seal_ctx: &[u8; 16],
) -> Result<Vec<u8>> {
    let mut seqs = Vec::with_capacity(frames.len());
    let mut sealed: Vec<&[u8]> = Vec::with_capacity(frames.len());
    let mut total_plain: u64 = 0;
    for (i, f) in frames.iter().enumerate() {
        seqs.push(g.data[i].hdr.block_seq);
        sealed.push(&f[BLOCK_HEADER_LEN..]);
        total_plain += body_lens[i] as u64;
    }
    let digest = group_digest(c.group_id, c.block_count, &seqs, &sealed);
    let body = encode_completion(&Completion {
        group_id: c.group_id,
        block_count: c.block_count,
        total_records: c.total_records,
        total_plain,
        block_seqs: seqs.clone(),
        digest,
    });
    let mode = g.completion.hdr.compression;
    let (frame, _) = seal_frame(
        &env.comp,
        env.crypt,
        mode,
        &env.store_id,
        &body,
        true,
        0,
        key_id,
        key,
        g.completion.hdr.block_seq,
        seal_ctx,
    )?;
    Ok(frame)
}

/// Assemble and validate a staged replacement before it may rename
/// over its original.
fn verify_staged_file(env: &MaintEnv, staged: &Path, id: u64, seal_ctx: &[u8; 16]) -> Result<()> {
    let (groups, _, _, _) = assemble_file(staged, id, false, None).map_err(|e| {
        err(
            e.kind(),
            format!("verify assembles staged segment {id}: {e}"),
        )
    })?;
    let loader = Loader {
        dir: env.dir.clone(),
        man: env.man.clone(),
        ring: env.ring.clone(),
        comp: env.comp.clone(),
        ctx: *seal_ctx,
        replay: None,
        torn_tail_id: 0,
        store_id: env.store_id,
    };
    for g in &groups {
        loader.validate_group(id, g).map_err(|e| {
            err(
                e.kind(),
                format!("verify validates staged segment {id}: {e}"),
            )
        })?;
    }
    Ok(())
}

pub(crate) fn remove_intent_file(dir: &Path, faults: Option<&FaultHooks>) -> Result<()> {
    if let Some(f) = faults {
        f.trip("delete")
            .map_err(|e| err(ErrorKind::Io, format!("remove maintenance intent: {e}")))?;
    }
    match fs::remove_file(dir.join(INTENT_FILE_NAME)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(err(
                ErrorKind::Io,
                format!("remove maintenance intent: {e}"),
            ))
        }
    }
    if let Some(f) = faults {
        f.trip("dirsync")
            .map_err(|e| err(ErrorKind::Io, format!("sync store dir: {e}")))?;
    }
    dir_sync(dir).map_err(|e| err(ErrorKind::Io, format!("sync store dir: {e}")))
}

/// Complete an interrupted rewrite or rebind. Runs under the store
/// lock before the open admits writes; every step is idempotent.
pub(crate) fn resume_maintenance(
    dir: &Path,
    master_key: &[u8],
    passphrase: &str,
    compression: u8,
    codecs: Vec<Arc<dyn crate::compression::Codec>>,
    workers: usize,
    faults: Option<Arc<FaultHooks>>,
) -> Result<()> {
    let _ = workers;
    let raw = match fs::read(dir.join(INTENT_FILE_NAME)) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(err(ErrorKind::Io, format!("read maintenance intent: {e}"))),
    };
    let mut intent = parse_intent(&raw)?;
    let mraw = fs::read(dir.join(MANIFEST_FILE_NAME))
        .map_err(|e| err(ErrorKind::Io, format!("resume reads manifest: {e}")))?;
    let mut man = crate::manifest::parse_manifest(&mraw)?;
    if man.generation > intent.manifest_target {
        return Err(err(
            ErrorKind::Corrupt,
            format!(
                "manifest generation {} past intent target {}",
                man.generation, intent.manifest_target
            ),
        ));
    }
    let comp = Compressor::new(compression, codecs)?;
    let crypt = if man.encrypted {
        BlockCrypt {
            algo: crate::crypto::ALGO_AES_GCM,
        }
    } else {
        BlockCrypt {
            algo: crate::crypto::ALGO_NONE,
        }
    };
    let mut ring: Option<Arc<Keyring>> = None;
    if man.encrypted {
        let r = match crate::keyring::read_keys(
            dir,
            master_key,
            passphrase,
            &man.store_id,
            &man.context,
            faults.clone(),
        ) {
            Ok(r) => r,
            Err(e) if intent.op == MAINT_OP_REBIND && e.kind() == ErrorKind::WrongKey => {
                // The keyring may already carry the target context
                // while the manifest still points at the old one.
                crate::keyring::read_keys(
                    dir,
                    master_key,
                    passphrase,
                    &man.store_id,
                    &intent.target_context,
                    faults.clone(),
                )
                .map_err(|e2| err(e2.kind(), format!("resume unlocks keys.enc: {e2}")))?
            }
            Err(e) => return Err(err(e.kind(), format!("resume unlocks keys.enc: {e}"))),
        };
        ring = Some(Arc::new(r));
    }
    let mut env = MaintEnv {
        dir: dir.to_path_buf(),
        store_id: man.store_id,
        man: man.clone(),
        ring: ring.clone(),
        comp,
        crypt,
        faults: faults.clone(),
    };
    let seal_ctx = if intent.op == MAINT_OP_REBIND {
        intent.target_context
    } else {
        man.context
    };
    if intent.phase < MAINT_PHASE_SEGMENTS_DONE {
        let (key_id, key) = match &ring {
            None => (0u32, [0u8; 32]),
            Some(r) => r
                .current_key()
                .ok_or_else(|| err(ErrorKind::NoCurrentKey, "no current data key"))?,
        };
        let read_ctx = man.context;
        let allow_fallback = intent.op == MAINT_OP_REBIND && ring.is_some();
        for id in man.members.clone() {
            stage_segment_file(
                &env,
                id,
                key_id,
                &key,
                &read_ctx,
                &seal_ctx,
                &intent.target_context,
                allow_fallback,
            )?;
        }
        intent.phase = MAINT_PHASE_SEGMENTS_DONE;
        if let Some(f) = &faults {
            f.trip("intent")
                .map_err(|e| err(ErrorKind::Io, format!("injected intent fault: {e}")))?;
        }
        atomic_write_file(dir, INTENT_FILE_NAME, &encode_intent(&intent))
            .map_err(|e| err(e.kind(), format!("resume writes intent: {e}")))?;
    }
    if intent.op == MAINT_OP_REBIND && intent.phase < MAINT_PHASE_KEYRING_DONE {
        if let Some(r) = ring.as_ref() {
            if r.inner.read().unwrap().context != intent.target_context {
                {
                    let mut w = r.inner.write().unwrap();
                    w.context = seal_ctx;
                }
                r.write_keys(dir, &man.store_id, master_key, passphrase)
                    .map_err(|e| err(e.kind(), format!("resume publishes keyring: {e}")))?;
                intent.phase = MAINT_PHASE_KEYRING_DONE;
                if let Some(f) = &faults {
                    f.trip("intent")
                        .map_err(|e| err(ErrorKind::Io, format!("injected intent fault: {e}")))?;
                }
                atomic_write_file(dir, INTENT_FILE_NAME, &encode_intent(&intent))
                    .map_err(|e| err(e.kind(), format!("resume writes intent: {e}")))?;
            }
        }
    }
    if man.generation != intent.manifest_target {
        man.context = seal_ctx;
        man.generation = intent.manifest_target;
        // Resume's renames changed the on-disk shape a close-time
        // clean marker describes; clear it.
        man.clean = false;
        man.clean_bytes = 0;
        man.clean_files = 0;
        man.clean_gen = 0;
        man.clean_group = 0;
        env.man = man.clone();
        if let Some(f) = &faults {
            f.trip("manifest")
                .map_err(|e| err(ErrorKind::Io, format!("injected manifest fault: {e}")))?;
        }
        atomic_write_file(dir, MANIFEST_FILE_NAME, &man.encode())
            .map_err(|e| err(e.kind(), format!("resume publishes manifest: {e}")))?;
    }
    let _ = &env;
    remove_intent_file(dir, faults.as_deref())
}
