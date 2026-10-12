//! Key inventory, references, and pruning.
//!
//! Rust port of `spool/keymgmt.go`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use crate::error::{err, ErrorKind, Result};
use crate::keyring::KeyStatus;
use crate::loader::Loader;
use crate::manifest::SEGMENTS_DIR_NAME;
use crate::options::Durability;
use crate::segment::segment_file_name;
use crate::store::Store;

/// Non-secret metadata for one envelope data key.
#[derive(Debug, Clone)]
pub struct DataKeyInfo {
    /// Key id.
    pub id: u32,
    /// Creation time.
    pub created_at: SystemTime,
    /// Role.
    pub status: KeyStatus,
}

/// Non-secret snapshot of envelope identity, data keys, and
/// maintenance progress. Carries no key material.
#[derive(Debug, Clone)]
pub struct KeyInventory {
    /// Whether the store uses key material at all.
    pub encrypted: bool,
    /// Envelope's authenticated wrapping id.
    pub wrapping_key_id: String,
    /// Current key for new blocks.
    pub active_data_key_id: u32,
    /// Every envelope key by id, current and retired.
    pub data_keys: Vec<DataKeyInfo>,
    /// Envelope's persist counter.
    pub keyring_seq: u64,
    /// Authoritative membership generation.
    pub manifest_generation: u64,
    /// Running maintenance operation ("idle" when none).
    pub maintenance_phase: String,
    /// Maintenance progress in files.
    pub maintenance_done: u64,
    /// Maintenance progress in files.
    pub maintenance_total: u64,
}

impl Store {
    /// Snapshot non-secret key and maintenance metadata. Touches
    /// memory only.
    pub fn key_inventory(&self) -> KeyInventory {
        let (phase, done, total) = {
            let m = self.inner.maint.lock().unwrap();
            (m.phase.clone(), m.done, m.total)
        };
        let mut inv = KeyInventory {
            encrypted: false,
            wrapping_key_id: String::new(),
            active_data_key_id: 0,
            data_keys: Vec::new(),
            keyring_seq: 0,
            manifest_generation: 0,
            maintenance_phase: phase,
            maintenance_done: done,
            maintenance_total: total,
        };
        let Some(ring) = &self.inner.ring else {
            return inv;
        };
        inv.encrypted = true;
        {
            let r = ring.inner.read().unwrap();
            inv.wrapping_key_id = r.wrap_id.clone();
            inv.active_data_key_id = r.current;
            inv.keyring_seq = r.seq;
            for k in r.keys.values() {
                inv.data_keys.push(DataKeyInfo {
                    id: k.id,
                    created_at: ring.created_at(k.id).unwrap_or(SystemTime::UNIX_EPOCH),
                    status: k.status,
                });
            }
        }
        inv.data_keys.sort_by_key(|k| k.id);
        inv.manifest_generation = self.inner.manifest.lock().unwrap().generation;
        inv
    }
}

/// One data key's live references: authoritative segments and
/// validated blocks using it, sealed-but-unpublished frames, and
/// registered checkpoint dependencies.
#[derive(Debug, Clone)]
pub struct KeyReference {
    /// Data key id.
    pub key_id: u32,
    /// Authoritative segment files using it.
    pub segment_files: u64,
    /// Validated blocks using it.
    pub blocks: u64,
    /// Sealed-but-unpublished frames reference it.
    pub inflight: bool,
    /// Registered checkpoints pinning it.
    pub checkpoints: u64,
}

impl Store {
    /// Scan authoritative segments with full group validation and
    /// report per-key references. Validation failures abort the scan
    /// with an error and change nothing.
    pub fn key_references(&self) -> Result<Vec<KeyReference>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        self.check_maint()?;
        if self.inner.ring.is_none() {
            return Ok(Vec::new());
        }
        self.scan_key_references()
    }

    /// Implement `key_references` without lifecycle checks so pruning
    /// can share it under the rotation lock. Flushes staged bytes
    /// first so the scan observes every published group.
    pub(crate) fn scan_key_references(&self) -> Result<Vec<KeyReference>> {
        self.inner.seg.commit(Durability::Flush)?;
        let (members, man) = {
            let m = self.inner.manifest.lock().unwrap();
            (m.members.clone(), m.clone())
        };
        let ctx = *self.inner.ctx.read().unwrap();
        let loader = Loader {
            dir: self.inner.dir.clone(),
            man,
            ring: self.inner.ring.clone(),
            comp: self.inner.comp.clone(),
            ctx,
            replay: None,
            torn_tail_id: 0,
            store_id: self.inner.manifest.lock().unwrap().store_id,
        };
        let seg_dir = self.inner.dir.join(SEGMENTS_DIR_NAME);
        let mut blocks: HashMap<u32, u64> = HashMap::new();
        let mut files: HashMap<u32, HashSet<u64>> = HashMap::new();
        for id in &members {
            let path = seg_dir.join(segment_file_name(*id));
            let (groups, _, _, _) =
                crate::loader::assemble_file(&path, *id, false, None).map_err(|e| {
                    err(
                        e.kind(),
                        format!("reference scan assembles segment {id}: {e}"),
                    )
                })?;
            for g in &groups {
                loader.validate_group(*id, g).map_err(|e| {
                    err(
                        e.kind(),
                        format!("reference scan validates segment {id}: {e}"),
                    )
                })?;
                let mut touch = |key_id: u32| {
                    *blocks.entry(key_id).or_insert(0) += 1;
                    files.entry(key_id).or_default().insert(*id);
                };
                for b in &g.data {
                    touch(b.hdr.key_id);
                }
                touch(g.completion.hdr.key_id);
            }
        }
        let inflight = self.inner.seal_keys.lock().unwrap().clone();
        let ckpt = self.inner.ckpt_key_refs.lock().unwrap().clone();
        let mut union: HashSet<u32> = HashSet::new();
        union.extend(blocks.keys());
        union.extend(inflight.keys());
        union.extend(ckpt.keys());
        let mut out: Vec<KeyReference> = union
            .into_iter()
            .map(|k| KeyReference {
                key_id: k,
                segment_files: files.get(&k).map(|s| s.len() as u64).unwrap_or(0),
                blocks: blocks.get(&k).copied().unwrap_or(0),
                inflight: inflight.get(&k).copied().unwrap_or(0) > 0,
                checkpoints: ckpt.get(&k).copied().unwrap_or(0) as u64,
            })
            .collect();
        out.sort_by_key(|r| r.key_id);
        Ok(out)
    }

    /// Drop retired data keys that nothing references: no
    /// authoritative segment block, no sealed-but-unpublished frame,
    /// and no registered checkpoint. Returns the pruned key ids.
    /// Publication failures are terminal.
    pub fn prune_data_keys(&self) -> Result<Vec<u32>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        self.check_maint()?;
        if self.inner.ring.is_none() {
            return Ok(Vec::new());
        }
        let _m = self.inner.maint_excl.lock().unwrap();
        let _r = self.inner.rotate.lock().unwrap();
        let _q = self.inner.seal_quiesce.write().unwrap();
        let refs = self.scan_key_references()?;
        let mut used: HashSet<u32> = HashSet::new();
        for r in &refs {
            if r.segment_files > 0 || r.blocks > 0 || r.inflight || r.checkpoints > 0 {
                used.insert(r.key_id);
            }
        }
        let store_id = self.inner.manifest.lock().unwrap().store_id;
        let ring = self.inner.ring.as_ref().unwrap();
        let (master, pass) = (
            self.inner.master.lock().unwrap().clone(),
            self.inner.pass.lock().unwrap().clone(),
        );
        let mut dropped: Vec<crate::keyring::DataKey> = Vec::new();
        {
            let mut r = ring.inner.write().unwrap();
            let ids: Vec<u32> = r
                .keys
                .iter()
                .filter(|(id, k)| k.status == KeyStatus::Retired && !used.contains(id))
                .map(|(id, _)| *id)
                .collect();
            for id in ids {
                if let Some(k) = r.keys.remove(&id) {
                    dropped.push(k);
                }
            }
            if dropped.is_empty() {
                return Ok(Vec::new());
            }
            if let Err(e) = crate::keyring::Keyring::write_keys_locked(
                ring.faults.as_ref(),
                &mut r,
                &self.inner.dir,
                &store_id,
                &master,
                &pass,
                None,
            ) {
                for k in dropped {
                    r.keys.insert(k.id, k);
                }
                drop(r);
                let e = err(e.kind(), format!("publish key pruning: {e}"));
                self.set_terminal(e.clone());
                return Err(e);
            }
        }
        let mut ids: Vec<u32> = dropped.iter().map(|k| k.id).collect();
        for k in &mut dropped {
            crate::crypto::wipe(&mut k.key);
        }
        ids.sort_unstable();
        Ok(ids)
    }
}
