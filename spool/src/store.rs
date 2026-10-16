//! The spool store: open, write, commit, flush, and close paths.
//!
//! Rust port of `spool/spool.go` + `spool/writer.go`.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::block::{encode_body, encode_completion, group_digest, Completion, BLOCK_HEADER_LEN};
use crate::compression::Compressor;
use crate::crypto::{block_aad, wipe, BlockCrypt};
use crate::error::{err, Error, ErrorKind, Result};
use crate::filestats::FileStatsRegistry;
use crate::index::{shard_for_key, Index, Location};
use crate::keyring::Keyring;
use crate::loader::{Loader, ReplayTracker};
use crate::lock::{lock_store, StoreLock, LOCK_FILE_NAME};
use crate::manifest::{
    atomic_write_file, check_clean_marker, dir_sync, new_store_id, parse_manifest, CleanMarker,
    Manifest, MANIFEST_FILE_NAME, SEGMENTS_DIR_NAME,
};
use crate::options::{Durability, Encryption, FaultHooks, NormalizedOptions, Options};
use crate::record::{PendingRecord, RECORD_OVERHEAD};
use crate::segment::{segment_file_name, SegmentWriter};

/// Sequence layout: 16-bit epoch in the high bits, 48-bit counter below.
pub(crate) const EPOCH_SHIFT: u32 = 48;
pub(crate) const SEQ_COUNTER_MAX: u64 = 1 << EPOCH_SHIFT;

/// One admitted atomic commit.
struct CommitGroup {
    id: u64,
    recs: Vec<PendingRecord>,
    bytes: i64,
    durability: Durability,
    done: std::sync::mpsc::Sender<Result<()>>,
}

pub(crate) struct QueueState {
    queue: VecDeque<CommitGroup>,
    completed_through: u64,
    drained: bool,
}

pub(crate) struct MaintState {
    pub phase: String,
    pub done: u64,
    pub total: u64,
}

pub(crate) struct CheckpointData {
    pub id: String,
    pub dir: PathBuf,
    pub generation: u64,
    pub members: Vec<u64>,
    pub created_at: SystemTime,
    pub encrypted: bool,
    pub key_ids: Vec<u32>,
    pub files: Vec<PathBuf>,
}

/// Shared store state. `Store` is a cheap cloneable handle over it.
#[allow(dead_code)]
pub(crate) struct Inner {
    pub opts: NormalizedOptions,
    pub dir: PathBuf,
    pub seg: SegmentWriter,

    pub seq_base: u64,
    pub seq_next: AtomicU64,

    pub shards: Vec<Mutex<Vec<PendingRecord>>>,
    pub shard_mask: u64,
    pub pending_bytes: AtomicI64,
    pub pending_records: AtomicI64,
    pub bp: Mutex<()>,
    pub bp_cond: Condvar,

    pub admission: Mutex<()>,
    pub seal: Mutex<()>,
    pub group_seq: AtomicU64,
    pub last_group: AtomicU64,
    pub staged_count: AtomicU64,
    pub last_sealed: Mutex<u64>,

    pub queue: Mutex<QueueState>,
    pub commit_cond: Condvar,

    pub storage: Mutex<Option<Error>>,
    pub terminal: AtomicBool,
    pub faults: Option<Arc<FaultHooks>>,

    pub comp: Compressor,
    pub crypt: BlockCrypt,
    pub ring: Option<Arc<Keyring>>,
    pub master: Mutex<Vec<u8>>,
    pub pass: Mutex<String>,
    pub ctx: RwLock<[u8; 16]>,
    pub wrap_id: RwLock<String>,
    pub idx: Index,
    read_cache: Mutex<crate::read_cache::Cache>,
    pub fstats: FileStatsRegistry,
    pub workers: usize,

    pub next_block_id: AtomicU64,
    pub appended_group: AtomicU64,

    pub manifest: Mutex<Manifest>,
    pub rotate: Mutex<()>,

    pub seal_keys: Mutex<HashMap<u32, usize>>,
    pub ckpt: Mutex<HashMap<String, CheckpointData>>,
    pub ckpt_key_refs: Mutex<HashMap<u32, usize>>,
    pub seal_quiesce: RwLock<()>,

    pub maint_excl: Mutex<()>,
    pub maint: Mutex<MaintState>,
    pub maint_active: AtomicBool,

    pub stop: AtomicBool,
    pub handles: Mutex<Vec<JoinHandle<()>>>,
    pub flush_flag: Mutex<bool>,
    pub flush_cond: Condvar,

    pub last_flush: AtomicI64,

    // Stats counters.
    pub blocks_written: AtomicU64,
    pub bytes_written: AtomicU64,
    pub bytes_uncompressed: AtomicU64,
    pub compactions: AtomicU64,
    pub compaction_bytes: AtomicU64,
    pub flush_errors: AtomicU64,
    pub truncated_tails: AtomicU64,
    pub reclaim_errors: AtomicU64,
    pub groups_accepted: AtomicU64,
    pub groups_written: AtomicU64,
    pub groups_synced: AtomicU64,
    pub groups_discarded: AtomicU64,
    pub pending_groups: AtomicI64,
    pub commits: AtomicU64,
    pub syncs: AtomicU64,
    pub storage_failures: AtomicU64,
    pub rotations: AtomicU64,
    pub commit_nanos: AtomicU64,
    pub sync_nanos: AtomicU64,
    pub flushes: AtomicU64,
    pub flush_nanos: AtomicU64,
    pub reclaims: AtomicU64,
    pub reclaim_nanos: AtomicU64,
    pub compaction_nanos: AtomicU64,
    pub flush_err_lock: Mutex<(Option<Error>, Option<Error>)>,

    pub reclaim_lock: Mutex<()>,
    pub closed: AtomicBool,
    pub store_lock: Mutex<Option<StoreLock>>,
}

/// An open spool database.
///
/// Cheap to clone: all clones share one open store. There is
/// deliberately no `get`: the application keeps current values in
/// RAM; spool durably journals every mutation and rebuilds state at
/// startup through load callbacks.
///
/// Basic use:
///
/// ```no_run
/// use spool::{Options, default_options, open_and_load};
///
/// let mut opts = default_options("./data");
/// opts.master_key = vec![7u8; 32];
/// let store = open_and_load(&opts, &mut |records| {
///     // populate application memory; each key arrives once
///     // with its current version (tombstone if deleted)
///     Ok(())
/// })?;
/// store.put(b"k", b"v")?;
/// store.delete(b"old")?;
/// store.flush()?;
/// store.close()?;
/// # Ok::<(), spool::Error>(())
/// ```
#[derive(Clone)]
pub struct Store {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("dir", &self.inner.dir)
            .finish_non_exhaustive()
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn now_system() -> SystemTime {
    SystemTime::now()
}

// ---------------------------------------------------------------------------
// Open
// ---------------------------------------------------------------------------

/// Open (or create) the store at `opts.path` and rebuild the
/// in-memory index from disk. Use [`open_and_load`] to also stream
/// every record into application memory in the same scan.
pub fn open(opts: &Options) -> Result<Store> {
    open_store(opts, None, false)
}

/// Open the store and stream every key's current version into `f`
/// after the index rebuild.
pub fn open_and_load(opts: &Options, f: crate::loader::LoadFunc<'_>) -> Result<Store> {
    open_store(opts, Some(f), false)
}

/// Open and deliver current records in lexicographic key order.
pub fn open_and_load_ordered(opts: &Options, f: crate::loader::LoadFunc<'_>) -> Result<Store> {
    open_store(opts, Some(f), true)
}

fn open_store(
    opts: &Options,
    f: Option<crate::loader::LoadFunc<'_>>,
    ordered: bool,
) -> Result<Store> {
    let o = opts.normalized()?;
    let workers = {
        let w = if o.workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            o.workers
        };
        w.clamp(2, 32)
    };
    let dir = PathBuf::from(&o.path);
    fs::create_dir_all(dir.join(SEGMENTS_DIR_NAME))
        .map_err(|e| err(ErrorKind::Io, format!("create store dir: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
            .map_err(|e| err(ErrorKind::Io, format!("secure store dir: {e}")))?;
        fs::set_permissions(
            dir.join(SEGMENTS_DIR_NAME),
            fs::Permissions::from_mode(0o700),
        )
        .map_err(|e| err(ErrorKind::Io, format!("secure segments dir: {e}")))?;
    }
    let mut lock_slot: Option<StoreLock> = Some(lock_store(&dir)?);

    macro_rules! fail {
        ($e:expr) => {{
            drop(lock_slot.take());
            return Err($e);
        }};
    }

    // Manifest load/init needs a partial owner; do it with free functions.
    let (mut m, fresh) = match load_or_init_manifest(&dir, &o) {
        Ok(v) => v,
        Err(e) => fail!(e),
    };
    if m.encrypted != (o.encryption != Encryption::None) {
        fail!(err(
            ErrorKind::Invalid,
            format!(
                "store encryption (manifest={}) mismatches options ({})",
                m.encrypted, o.encryption
            ),
        ));
    }
    // Authenticate before cleanup, maintenance, or manifest publication.
    // The store lock prevents another process changing the envelope between
    // authentication and opening the live store.
    if !fresh && m.encrypted {
        let ring = match load_or_init_keys(&dir, &o, &m) {
            Ok(r) => r,
            Err(e) => fail!(e),
        };
        let r = ring.inner.read().unwrap();
        if !o.context_id.is_empty() && o.context_id != r.context {
            fail!(err(ErrorKind::ContextMismatch, "key context mismatch"));
        }
        if !o.wrapping_key_id.is_empty() && o.wrapping_key_id != r.wrap_id {
            fail!(err(ErrorKind::WrongKey, "wrapping id mismatch"));
        }
    }
    let seg_dir = dir.join(SEGMENTS_DIR_NAME);
    if !fresh {
        if let Err(e) = sweep_unlisted(&seg_dir, &dir, &m.members, o.faults.as_deref()) {
            fail!(e);
        }
    }
    // Resume interrupted maintenance before any verification.
    if let Err(e) = crate::maintenance::resume_maintenance(
        &dir,
        &o.master_key,
        &o.passphrase,
        o.compression
            .map(|c| c.0)
            .unwrap_or(crate::compression::ID_DEFLATE),
        o.all_codecs(),
        workers,
        o.faults.clone(),
    ) {
        fail!(e);
    }
    m = match fs::read(dir.join(MANIFEST_FILE_NAME)) {
        Ok(raw) => match parse_manifest(&raw) {
            Ok(m) => m,
            Err(e) => fail!(e),
        },
        Err(e) => fail!(err(ErrorKind::Io, format!("reread manifest: {e}"))),
    };
    if !fresh && o.context_id.len() == 16 && o.context_id != m.context {
        fail!(err(
            ErrorKind::ContextMismatch,
            "supplied database context does not match store"
        ));
    }
    let verify_clean = !fresh && m.clean;
    let want = CleanMarker {
        bytes: m.clean_bytes,
        files: m.clean_files,
        gen: m.clean_gen,
        group: m.clean_group,
    };
    m.clean = false;
    m.clean_bytes = 0;
    m.clean_files = 0;
    m.clean_gen = 0;
    m.clean_group = 0;
    // Bump the epoch first (the manifest persist itself is deferred until
    // the load phase below succeeds, so a refused or failed open leaves the
    // store content-untouched; it still lands before any data handling).
    let use_epoch = m.next_epoch;
    m.next_epoch = m.next_epoch.wrapping_add(1);
    if m.next_epoch == 0 {
        fail!(err(
            ErrorKind::Invalid,
            "epoch space exhausted after 65535 opens"
        ));
    }

    // Keys.
    let (ring, ctx, wrap_id, master, pass): (
        Option<Arc<Keyring>>,
        [u8; 16],
        String,
        Vec<u8>,
        String,
    ) = if m.encrypted {
        let ring = match load_or_init_keys(&dir, &o, &m) {
            Ok(r) => Arc::new(r),
            Err(e) => fail!(e),
        };
        if !ring.has_id(m.current_key_id) {
            fail!(err(
                ErrorKind::Corrupt,
                format!("manifest key id {} missing from keys.enc", m.current_key_id)
            ));
        }
        {
            let r = ring.inner.read().unwrap();
            if !o.wrapping_key_id.is_empty() && o.wrapping_key_id != r.wrap_id {
                fail!(err(
                    ErrorKind::WrongKey,
                    format!(
                        "wrapping-key id {:?} != envelope {:?}",
                        o.wrapping_key_id, r.wrap_id
                    ),
                ));
            }
        }
        let rctx = ring.inner.read().unwrap().context;
        let rwrap = ring.inner.read().unwrap().wrap_id.clone();
        (
            Some(ring),
            rctx,
            rwrap,
            o.master_key.clone(),
            o.passphrase.clone(),
        )
    } else {
        (None, m.context, String::new(), Vec::new(), String::new())
    };

    let crypt = BlockCrypt::for_encryption(o.encryption).unwrap_or(BlockCrypt {
        algo: crate::crypto::ALGO_NONE,
    });
    let comp = match Compressor::new(
        o.compression
            .map(|c| c.0)
            .unwrap_or(crate::compression::ID_DEFLATE),
        o.all_codecs(),
    ) {
        Ok(c) => c,
        Err(e) => fail!(e),
    };

    // Members authoritative; listed members that are missing fail the open.
    let mut ids: Vec<u64> = m.members.clone();
    if fresh {
        ids = Vec::new();
    } else {
        for id in &ids {
            if fs::metadata(seg_dir.join(segment_file_name(*id))).is_err() {
                fail!(err(
                    ErrorKind::Corrupt,
                    format!("member segment {id} missing")
                ));
            }
        }
    }
    let mut max_id: u64 = 0;
    for id in &ids {
        max_id = max_id.max(*id);
    }
    let mut next_file = max_id + 1;
    if next_file < m.next_file_id {
        next_file = m.next_file_id;
    }
    if next_file == 0 {
        next_file = 1;
    }

    // Build the store skeleton (segment writer opens after rebuild).
    let idx = Index::new(o.index_shards);
    let fstats = FileStatsRegistry::new();
    for id in &ids {
        fstats.register(*id, segment_mod_time(&seg_dir, *id));
    }
    let seq_base = (use_epoch as u64) << EPOCH_SHIFT;

    // Rebuild index and statistics from disk before serving writes.
    // (Runs without the segment writer; uses a temporary loader.)
    let mut read_cache = crate::read_cache::Cache::new(o.read_cache_bytes);
    let (max_block, max_group, ends, discarded) = {
        let mut loader = Loader {
            dir: dir.clone(),
            man: m.clone(),
            ring: ring.clone(),
            comp: comp.clone(),
            ctx,
            replay: o.on_replay_progress.clone().map(|cb| ReplayTracker {
                cb,
                pass: 1,
                files_total: ids.len(),
                files_done: 0,
                bytes_total: crate::loader::segment_bytes_total(&dir, &ids),
                bytes_done: 0,
                records: 0,
            }),
            torn_tail_id: max_id,
            store_id: m.store_id,
        };
        match loader.scan_all(&ids, f, Some(&idx), Some(&fstats), ordered, &mut read_cache) {
            Ok(v) => v,
            Err(e) => fail!(e),
        }
    };
    let mut truncated = 0u64;
    for (id, end) in &ends {
        if let Ok(st) = fs::metadata(seg_dir.join(segment_file_name(*id))) {
            if *end < st.len() {
                truncated += 1;
            }
        }
    }
    if verify_clean {
        if let Err(e) = check_clean_marker(&seg_dir, &ids, m.generation, want, max_group) {
            fail!(e);
        }
    }
    // The load callback and clean verification above accepted the store, so
    // publish the bumped epoch now, still before any data handling (tail
    // truncation and writer open below). A refused open returns before this
    // point and leaves every existing file byte-identical.
    if let Err(e) = write_manifest_file(&dir, &m, o.faults.as_deref()) {
        fail!(e);
    }

    let active = if max_id == 0 { 1 } else { max_id };
    if max_id == 0 {
        next_file = 2;
        fstats.register(1, now_system());
    }
    for id in fstats.ids() {
        fstats.set_state(
            id,
            if id == active {
                crate::filestats::SegmentState::Active
            } else {
                crate::filestats::SegmentState::Sealed
            },
        );
    }
    // Truncate a torn tail on the active segment before new appends.
    if let Err(e) = truncate_active(&dir, active, ends.get(&active).copied().unwrap_or(0)) {
        fail!(e);
    }
    let seg = match SegmentWriter::open(&seg_dir, active, o.faults.clone()) {
        Ok(s) => s,
        Err(e) => fail!(e),
    };

    let next_block = (max_block + 1).max(m.next_block_id);
    let start_group = (max_group + 1).max(m.next_group_id);

    let mut shards = Vec::with_capacity(o.write_shards);
    for _ in 0..o.write_shards {
        shards.push(Mutex::new(Vec::new()));
    }

    let inner = Arc::new(Inner {
        opts: o.clone(),
        dir: dir.clone(),
        seg,
        seq_base,
        seq_next: AtomicU64::new(0),
        shard_mask: (o.write_shards - 1) as u64,
        shards,
        pending_bytes: AtomicI64::new(0),
        pending_records: AtomicI64::new(0),
        bp: Mutex::new(()),
        bp_cond: Condvar::new(),
        admission: Mutex::new(()),
        seal: Mutex::new(()),
        group_seq: AtomicU64::new(start_group.saturating_sub(1)),
        last_group: AtomicU64::new(start_group.saturating_sub(1)),
        staged_count: AtomicU64::new(0),
        last_sealed: Mutex::new(0),
        queue: Mutex::new(QueueState {
            queue: VecDeque::new(),
            completed_through: start_group.saturating_sub(1),
            drained: false,
        }),
        commit_cond: Condvar::new(),
        storage: Mutex::new(None),
        terminal: AtomicBool::new(false),
        faults: o.faults.clone(),
        comp,
        crypt,
        ring,
        master: Mutex::new(master),
        pass: Mutex::new(pass),
        ctx: RwLock::new(ctx),
        wrap_id: RwLock::new(wrap_id),
        idx,
        read_cache: Mutex::new(read_cache),
        fstats,
        workers,
        next_block_id: AtomicU64::new(next_block),
        appended_group: AtomicU64::new(max_group),
        manifest: Mutex::new({
            let mut m = m;
            m.next_file_id = next_file;
            m.active_file_id = active;
            m.next_group_id = start_group;
            m
        }),
        rotate: Mutex::new(()),
        seal_keys: Mutex::new(HashMap::new()),
        ckpt: Mutex::new(HashMap::new()),
        ckpt_key_refs: Mutex::new(HashMap::new()),
        seal_quiesce: RwLock::new(()),
        maint_excl: Mutex::new(()),
        maint: Mutex::new(MaintState {
            phase: "idle".to_string(),
            done: 0,
            total: 0,
        }),
        maint_active: AtomicBool::new(false),
        stop: AtomicBool::new(false),
        handles: Mutex::new(Vec::new()),
        flush_flag: Mutex::new(false),
        flush_cond: Condvar::new(),
        last_flush: AtomicI64::new(now_nanos()),
        blocks_written: AtomicU64::new(0),
        bytes_written: AtomicU64::new(0),
        bytes_uncompressed: AtomicU64::new(0),
        compactions: AtomicU64::new(0),
        compaction_bytes: AtomicU64::new(0),
        flush_errors: AtomicU64::new(0),
        truncated_tails: AtomicU64::new(truncated),
        reclaim_errors: AtomicU64::new(0),
        groups_accepted: AtomicU64::new(0),
        groups_written: AtomicU64::new(0),
        groups_synced: AtomicU64::new(0),
        groups_discarded: AtomicU64::new(discarded),
        pending_groups: AtomicI64::new(0),
        commits: AtomicU64::new(0),
        syncs: AtomicU64::new(0),
        storage_failures: AtomicU64::new(0),
        rotations: AtomicU64::new(0),
        commit_nanos: AtomicU64::new(0),
        sync_nanos: AtomicU64::new(0),
        flushes: AtomicU64::new(0),
        flush_nanos: AtomicU64::new(0),
        reclaims: AtomicU64::new(0),
        reclaim_nanos: AtomicU64::new(0),
        compaction_nanos: AtomicU64::new(0),
        flush_err_lock: Mutex::new((None, None)),
        reclaim_lock: Mutex::new(()),
        closed: AtomicBool::new(false),
        store_lock: Mutex::new(lock_slot.take()),
    });

    // Rotation hook (weak back-reference).
    {
        let weak = Arc::downgrade(&inner);
        inner.seg.set_on_rotate(Arc::new(move |sealed, new| {
            let inner = weak
                .upgrade()
                .ok_or_else(|| err(ErrorKind::Closed, "store is closed"))?;
            Store { inner }.on_rotate(sealed, new)
        }));
    }

    // Persist repaired allocator positions.
    {
        let store = Store {
            inner: inner.clone(),
        };
        let m = store.inner.manifest.lock().unwrap().clone();
        if let Err(e) = store.write_manifest_file(&m) {
            let _ = store.inner.seg.close();
            drop(store.inner.store_lock.lock().unwrap().take());
            return Err(e);
        }
        // Age rotation on open: an expired current key rotates before
        // any write can reference it.
        if !store.key_fresh() {
            let _r = store.inner.rotate.lock().unwrap();
            if let Err(e) = store.rotate_data_key() {
                let _ = store.inner.seg.close();
                drop(store.inner.store_lock.lock().unwrap().take());
                return Err(e);
            }
        }
    }

    // Background loops.
    {
        let c = inner.clone();
        inner.handles.lock().unwrap().push(
            std::thread::Builder::new()
                .name("spool-commit".into())
                .spawn(move || Store { inner: c }.commit_loop())
                .map_err(|e| err(ErrorKind::Io, format!("spawn commit loop: {e}")))?,
        );
        let c = inner.clone();
        inner.handles.lock().unwrap().push(
            std::thread::Builder::new()
                .name("spool-flush".into())
                .spawn(move || Store { inner: c }.flush_loop())
                .map_err(|e| err(ErrorKind::Io, format!("spawn flush loop: {e}")))?,
        );
        if inner.opts.reclaim_interval.is_some() {
            let c = inner.clone();
            inner.handles.lock().unwrap().push(
                std::thread::Builder::new()
                    .name("spool-reclaim".into())
                    .spawn(move || Store { inner: c }.reclaim_loop())
                    .map_err(|e| err(ErrorKind::Io, format!("spawn reclaim loop: {e}")))?,
            );
        }
    }

    Ok(Store { inner })
}

/// Read the manifest or create a fresh store.
fn load_or_init_manifest(dir: &Path, o: &NormalizedOptions) -> Result<(Manifest, bool)> {
    let path = dir.join(MANIFEST_FILE_NAME);
    match fs::read(&path) {
        Ok(raw) => Ok((parse_manifest(&raw)?, false)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let id = new_store_id()?;
            let mut m = Manifest {
                encrypted: o.encryption != Encryption::None,
                store_id: id,
                context: id,
                next_file_id: 1,
                next_block_id: 1,
                next_epoch: 1,
                current_key_id: 0,
                active_file_id: 1,
                generation: 1,
                next_group_id: 1,
                req_features: 0,
                members: vec![1],
                clean: false,
                clean_bytes: 0,
                clean_files: 0,
                clean_gen: 0,
                clean_group: 0,
            };
            if o.context_id.len() == 16 {
                m.context.copy_from_slice(&o.context_id);
            }
            if m.encrypted {
                m.current_key_id = 1;
            }
            write_manifest_file(dir, &m, o.faults.as_deref())?;
            Ok((m, true))
        }
        Err(e) => Err(err(ErrorKind::Io, format!("read manifest: {e}"))),
    }
}

/// Check structural invariants of a manifest before publication.
pub(crate) fn assert_manifest_invariants(m: &Manifest) -> Result<()> {
    if !m.members.windows(2).all(|w| w[0] < w[1]) {
        return Err(err(ErrorKind::Corrupt, "manifest members unsorted"));
    }
    if m.members.contains(&0) {
        return Err(err(ErrorKind::Corrupt, "manifest members invalid"));
    }
    if m.members.is_empty() {
        return Err(err(ErrorKind::Corrupt, "manifest has empty members"));
    }
    if !m.members.contains(&m.active_file_id) {
        return Err(err(
            ErrorKind::Corrupt,
            format!("active segment {} missing from members", m.active_file_id),
        ));
    }
    Ok(())
}

/// Publish one manifest encoding (free function for open-path use).
fn write_manifest_file(dir: &Path, m: &Manifest, faults: Option<&FaultHooks>) -> Result<()> {
    assert_manifest_invariants(m)?;
    if let Some(f) = faults {
        f.trip("manifest")
            .map_err(|e| err(ErrorKind::Io, format!("injected manifest fault: {e}")))?;
    }
    atomic_write_file(dir, MANIFEST_FILE_NAME, &m.encode())
}

/// Open keys.enc or create it with a fresh data key.
fn load_or_init_keys(dir: &Path, o: &NormalizedOptions, m: &Manifest) -> Result<Keyring> {
    if fs::metadata(dir.join(crate::keyring::KEYS_FILE_NAME)).is_ok() {
        return crate::keyring::read_keys(
            dir,
            &o.master_key,
            &o.passphrase,
            &m.store_id,
            &m.context,
            o.faults.clone(),
        );
    }
    let ring = Keyring::new(o.faults.clone());
    {
        let mut r = ring.inner.write().unwrap();
        r.context = m.context;
        r.wrap_id = o.wrapping_key_id.clone();
    }
    ring.rotate(now_nanos())?;
    ring.write_keys(dir, &m.store_id, &o.master_key, &o.passphrase)?;
    Ok(ring)
}

/// Remove unreferenced segment files and stale temp files.
fn sweep_unlisted(
    seg_dir: &Path,
    root: &Path,
    members: &[u64],
    faults: Option<&FaultHooks>,
) -> Result<()> {
    use std::collections::HashSet;
    let want: HashSet<u64> = members.iter().copied().collect();
    let entries =
        fs::read_dir(seg_dir).map_err(|e| err(ErrorKind::Io, format!("list segments: {e}")))?;
    let mut removed = false;
    for e in entries {
        let e = e.map_err(|e| err(ErrorKind::Io, format!("list segments: {e}")))?;
        let ft = e
            .file_type()
            .map_err(|e| err(ErrorKind::Io, format!("list segments: {e}")))?;
        if ft.is_dir() {
            continue;
        }
        let name = e.file_name().to_string_lossy().into_owned();
        if let Some(id) = crate::segment::parse_segment_name(&name) {
            if !want.contains(&id) {
                if let Some(f) = faults {
                    f.trip("delete").map_err(|e| {
                        err(
                            ErrorKind::Io,
                            format!("remove unlisted segment {name}: {e}"),
                        )
                    })?;
                }
                match fs::remove_file(seg_dir.join(&name)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(err(
                            ErrorKind::Io,
                            format!("remove unlisted segment {name}: {e}"),
                        ))
                    }
                }
                removed = true;
            }
            continue;
        }
        if name.starts_with('.') && name.contains(".tmp-") {
            if let Some(f) = faults {
                f.trip("delete")
                    .map_err(|e| err(ErrorKind::Io, format!("remove temp file {name}: {e}")))?;
            }
            match fs::remove_file(seg_dir.join(&name)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(err(ErrorKind::Io, format!("remove temp file {name}: {e}"))),
            }
            removed = true;
        }
    }
    if removed {
        if let Some(f) = faults {
            f.trip("dirsync")
                .map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
        }
        dir_sync(seg_dir).map_err(|e| err(ErrorKind::Io, format!("sync segments dir: {e}")))?;
    }
    let roots =
        fs::read_dir(root).map_err(|e| err(ErrorKind::Io, format!("list store dir: {e}")))?;
    for e in roots {
        let e = e.map_err(|e| err(ErrorKind::Io, format!("list store dir: {e}")))?;
        let ft = e
            .file_type()
            .map_err(|e| err(ErrorKind::Io, format!("list store dir: {e}")))?;
        if ft.is_dir() {
            continue;
        }
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') && name.contains(".tmp-") {
            if let Some(f) = faults {
                f.trip("delete")
                    .map_err(|e| err(ErrorKind::Io, format!("remove temp file {name}: {e}")))?;
            }
            match fs::remove_file(root.join(&name)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(err(ErrorKind::Io, format!("remove temp file {name}: {e}"))),
            }
            if let Some(f) = faults {
                f.trip("dirsync")
                    .map_err(|e| err(ErrorKind::Io, format!("sync store dir: {e}")))?;
            }
            dir_sync(root).map_err(|e| err(ErrorKind::Io, format!("sync store dir: {e}")))?;
        }
    }
    let _ = LOCK_FILE_NAME;
    Ok(())
}

fn segment_mod_time(seg_dir: &Path, id: u64) -> SystemTime {
    fs::metadata(seg_dir.join(segment_file_name(id)))
        .and_then(|st| st.modified())
        .unwrap_or_else(|_| now_system())
}

/// Cut the active segment to its scanned valid end.
fn truncate_active(dir: &Path, id: u64, valid_end: u64) -> Result<()> {
    let path = dir.join(SEGMENTS_DIR_NAME).join(segment_file_name(id));
    let st = match fs::metadata(&path) {
        Ok(st) => st,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(err(ErrorKind::Io, format!("stat active segment: {e}"))),
    };
    if valid_end >= st.len() {
        return Ok(());
    }
    let f = fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .map_err(|e| err(ErrorKind::Io, format!("open active segment: {e}")))?;
    f.set_len(valid_end)
        .map_err(|e| err(ErrorKind::Io, format!("truncate torn tail: {e}")))?;
    f.sync_all()
        .map_err(|e| err(ErrorKind::Io, format!("sync truncated segment: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Terminal state, manifest, and key helpers
// ---------------------------------------------------------------------------

impl Store {
    /// Report [`ErrorKind::StorageFailed`] once terminal, else nil.
    pub(crate) fn check_terminal(&self) -> Result<()> {
        if !self.inner.terminal.load(Ordering::SeqCst) {
            return Ok(());
        }
        let st = self.inner.storage.lock().unwrap();
        match &*st {
            None => Err(err(ErrorKind::StorageFailed, "terminal storage failure")),
            Some(e) => Err(err(
                ErrorKind::StorageFailed,
                format!("{e}: terminal storage failure"),
            )),
        }
    }

    /// The terminal storage failure cause, or None when healthy.
    /// Read the latest committed value from encrypted segments. Pending puts
    /// are excluded. Serializes with maintenance that replaces segment files.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _maintenance = self.inner.maint_excl.lock().unwrap();
        if self.is_closed() {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        let Some(loc) = self.inner.idx.location(key) else {
            return Ok(None);
        };
        let cache_key = (loc.file_id, loc.block_offset);
        let cached = self.inner.read_cache.lock().unwrap().get(cache_key);
        let block = if let Some(block) = cached {
            block
        } else {
            let manifest = self.inner.manifest.lock().unwrap().clone();
            let loader = Loader {
                dir: self.inner.dir.clone(),
                store_id: manifest.store_id,
                man: manifest,
                ring: self.inner.ring.clone(),
                comp: self.inner.comp.clone(),
                ctx: *self.inner.ctx.read().unwrap(),
                replay: None,
                torn_tail_id: 0,
            };
            let path = self
                .inner
                .dir
                .join(SEGMENTS_DIR_NAME)
                .join(segment_file_name(loc.file_id));
            let mut file = fs::File::open(path).map_err(|e| err(ErrorKind::Io, e.to_string()))?;
            let (header, sealed, _) = crate::segment::read_block_frame(
                &mut file,
                loc.block_offset as u64,
                crate::block::ABS_MAX_BLOCK_BYTES as u32,
            )?
            .ok_or_else(|| err(ErrorKind::Corrupt, "missing indexed block"))?;
            let plain = loader.open_payload(&header, &sealed[BLOCK_HEADER_LEN..])?;
            let count = u32::from_le_bytes(
                plain
                    .get(16..20)
                    .ok_or_else(|| err(ErrorKind::Corrupt, "short indexed body"))?
                    .try_into()
                    .unwrap(),
            );
            let body = crate::block::decode_body(&plain, count)?;
            let block = Arc::new(crate::read_cache::Block {
                frame: sealed,
                plain,
                body,
            });
            self.inner
                .read_cache
                .lock()
                .unwrap()
                .insert(cache_key, block.clone());
            block
        };
        let plain = &block.plain;
        let body = &block.body;
        let record = body
            .records
            .get(loc.record_index as usize)
            .ok_or_else(|| err(ErrorKind::Corrupt, "invalid indexed record"))?;
        if &plain[record.key_range.0..record.key_range.1] != key || record.deleted {
            return Err(err(ErrorKind::Corrupt, "indexed key mismatch"));
        }
        Ok(Some(plain[record.val_range.0..record.val_range.1].to_vec()))
    }
    /// Sorted committed keys. Only key locations live in the Spool index;
    /// values are read from disk with `get`.
    pub fn keys_with_prefix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.inner.idx.keys_with_prefix(prefix)
    }

    /// Return at most `limit` sorted committed keys with `prefix`, exclusively
    /// after `after`. Values remain in authenticated blocks; no value scan occurs.
    /// Use a checkpoint when a stable multi-page boundary is required.
    pub fn keys_page(&self,prefix:&[u8],after:Option<&[u8]>,limit:usize)->Vec<Vec<u8>> {
        self.inner.idx.keys_page(prefix,after,limit)
    }

    pub fn storage_error(&self) -> Option<Error> {
        self.inner.storage.lock().unwrap().clone()
    }

    /// Record a terminal storage failure, wake every waiter, and
    /// notify `on_storage_error` once outside all locks. Only the
    /// first cause sticks.
    pub(crate) fn set_terminal(&self, e: Error) {
        let cb = {
            let mut st = self.inner.storage.lock().unwrap();
            if st.is_some() {
                return;
            }
            *st = Some(e.clone());
            self.inner.opts.on_storage_error.clone()
        };
        self.inner.terminal.store(true, Ordering::SeqCst);
        self.inner.storage_failures.fetch_add(1, Ordering::Relaxed);
        {
            let _q = self.inner.queue.lock().unwrap();
            self.inner.commit_cond.notify_all();
        }
        {
            let _bp = self.inner.bp.lock().unwrap();
            self.inner.bp_cond.notify_all();
        }
        if let Some(cb) = cb {
            cb(e);
        }
    }

    pub(crate) fn persist_manifest(&self) -> Result<()> {
        let m = self.inner.manifest.lock().unwrap().clone();
        self.write_manifest_file(&m)
    }

    /// Publish one manifest encoding. All manifest publications
    /// funnel through here so fault injection sees them.
    pub(crate) fn write_manifest_file(&self, m: &Manifest) -> Result<()> {
        assert_manifest_invariants(m)?;
        if let Some(f) = &self.inner.faults {
            f.trip("manifest")
                .map_err(|e| err(ErrorKind::Io, format!("injected manifest fault: {e}")))?;
        }
        atomic_write_file(&self.inner.dir, MANIFEST_FILE_NAME, &m.encode())
    }

    /// Reserve the next segment file id from the manifest allocator.
    pub(crate) fn allocate_file_id(&self) -> u64 {
        let mut m = self.inner.manifest.lock().unwrap();
        let mut id = m.next_file_id;
        if id == 0 {
            id = 1;
        }
        m.next_file_id = id + 1;
        id
    }

    /// Record rotation side effects: the sealed file leaves the
    /// active state, the successor joins the authoritative members,
    /// and the manifest generation advances.
    pub(crate) fn on_rotate(&self, sealed_id: u64, new_id: u64) -> Result<()> {
        use crate::filestats::SegmentState;
        self.inner.fstats.set_state(sealed_id, SegmentState::Sealed);
        self.inner.fstats.register(new_id, now_system());
        self.inner.fstats.set_state(new_id, SegmentState::Active);
        let m = {
            let mut man = self.inner.manifest.lock().unwrap();
            if new_id + 1 > man.next_file_id {
                man.next_file_id = new_id + 1;
            }
            man.active_file_id = new_id;
            man.members.push(new_id);
            man.members.sort_unstable();
            man.generation += 1;
            man.next_group_id = self.inner.group_seq.load(Ordering::SeqCst) + 1;
            man.clone()
        };
        self.write_manifest_file(&m)
    }

    /// Atomically swap `victim_id` for `new_id` in the authoritative
    /// members under one generation.
    pub(crate) fn publish_replacement(&self, new_id: u64, victim_id: u64) -> Result<()> {
        // Admission-serialized against checkpoint capture.
        let _a = self.inner.admission.lock().unwrap();
        let m = {
            let mut man = self.inner.manifest.lock().unwrap();
            man.members.retain(|m| *m != victim_id);
            man.members.push(new_id);
            man.members.sort_unstable();
            if new_id + 1 > man.next_file_id {
                man.next_file_id = new_id + 1;
            }
            man.generation += 1;
            man.next_group_id = self.inner.group_seq.load(Ordering::SeqCst) + 1;
            man.clone()
        };
        self.write_manifest_file(&m)
    }

    /// Drop `id` from the authoritative members and persist.
    pub(crate) fn publish_member_removal(&self, id: u64) -> Result<()> {
        // Admission-serialized against checkpoint capture.
        let _a = self.inner.admission.lock().unwrap();
        let m = {
            let mut man = self.inner.manifest.lock().unwrap();
            man.members.retain(|m| *m != id);
            man.generation += 1;
            man.next_group_id = self.inner.group_seq.load(Ordering::SeqCst) + 1;
            man.clone()
        };
        self.write_manifest_file(&m)
    }

    /// Snapshot the ring's current key. The zero key with id 0 serves
    /// unencrypted stores.
    pub(crate) fn current_data_key(&self) -> Option<(u32, [u8; 32])> {
        match &self.inner.ring {
            None => Some((0, [0u8; 32])),
            Some(r) => r.current_key(),
        }
    }

    pub(crate) fn record_flush_err(&self, e: Error) {
        self.inner.flush_err_lock.lock().unwrap().0 = Some(e);
    }

    /// The most recent background flush error, if any.
    pub fn last_flush_err(&self) -> Option<Error> {
        self.inner.flush_err_lock.lock().unwrap().0.clone()
    }

    pub(crate) fn record_reclaim_err(&self, e: Error) {
        self.inner.flush_err_lock.lock().unwrap().1 = Some(e);
    }

    /// The most recent background reclamation error, if any.
    pub fn last_reclaim_err(&self) -> Option<Error> {
        self.inner.flush_err_lock.lock().unwrap().1.clone()
    }
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

impl Store {
    /// Buffer a key/value write. Returns once the mutation is staged
    /// in memory; durability follows the configured mode and `flush`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_impl(key, value, false, true)
    }

    /// Like [`Store::put`] but returns [`ErrorKind::Backpressure`]
    /// instead of blocking when the pending buffer is full.
    pub fn try_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_impl(key, value, false, false)
    }

    /// Buffer a tombstone for `key`.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put_impl(key, &[], true, true)
    }

    fn put_impl(&self, key: &[u8], value: &[u8], tomb: bool, block: bool) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        self.check_maint()?;
        if key.len() > self.inner.opts.max_key_size {
            return Err(err(ErrorKind::KeyTooLarge, "key exceeds MaxKeySize"));
        }
        if !tomb && value.len() > self.inner.opts.max_value_size {
            return Err(err(ErrorKind::ValueTooLarge, "value exceeds MaxValueSize"));
        }
        let size = key.len() + value.len() + RECORD_OVERHEAD;
        if size > self.inner.opts.max_block_bytes {
            return Err(err(
                ErrorKind::RecordTooLarge,
                "record exceeds MaxBlockBytes",
            ));
        }
        if size as i64 <= self.inner.opts.max_pending_bytes {
            let bp = self.inner.bp.lock().unwrap();
            let mut bp = bp;
            while self.inner.pending_bytes.load(Ordering::SeqCst) + size as i64
                > self.inner.opts.max_pending_bytes
                && !self.inner.closed.load(Ordering::SeqCst)
                && !self.inner.terminal.load(Ordering::SeqCst)
            {
                if !block {
                    return Err(err(ErrorKind::Backpressure, "pending write buffer full"));
                }
                bp = self.inner.bp_cond.wait(bp).unwrap();
            }
            drop(bp);
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(err(ErrorKind::Closed, "store is closed"));
            }
            self.check_terminal()?;
        } else if !block
            && self.inner.pending_bytes.load(Ordering::SeqCst) >= self.inner.opts.max_pending_bytes
        {
            return Err(err(ErrorKind::Backpressure, "pending write buffer full"));
        }
        self.inner
            .pending_bytes
            .fetch_add(size as i64, Ordering::SeqCst);
        self.inner.pending_records.fetch_add(1, Ordering::SeqCst);

        let k = key.to_vec();
        let v = if tomb { Vec::new() } else { value.to_vec() };
        let sh = &self.inner.shards[shard_for_key(&k, self.inner.shard_mask) as usize];
        {
            let mut vec = sh.lock().unwrap();
            self.inner.staged_count.fetch_add(1, Ordering::SeqCst);
            vec.push(PendingRecord {
                key: k,
                value: v,
                seq: 0,
                tomb,
                group: 0,
                size,
            });
        }
        self.maybe_signal_flusher();
        Ok(())
    }

    /// Buffer many key/value writes with one backpressure wait and one
    /// lock per touched shard. Admission is all-or-nothing; commits
    /// follow the normal flush path and are not atomic as a group.
    pub fn put_batch(&self, pairs: &[crate::record::KV]) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        if pairs.is_empty() {
            return Ok(());
        }
        let mut items = Vec::with_capacity(pairs.len());
        let mut total: i64 = 0;
        for p in pairs {
            if p.key.len() > self.inner.opts.max_key_size {
                return Err(err(ErrorKind::KeyTooLarge, "key exceeds MaxKeySize"));
            }
            if p.value.len() > self.inner.opts.max_value_size {
                return Err(err(ErrorKind::ValueTooLarge, "value exceeds MaxValueSize"));
            }
            let size = p.key.len() + p.value.len() + RECORD_OVERHEAD;
            if size > self.inner.opts.max_block_bytes {
                return Err(err(
                    ErrorKind::RecordTooLarge,
                    "record exceeds MaxBlockBytes",
                ));
            }
            total += size as i64;
            items.push(PendingRecord {
                key: p.key.clone(),
                value: p.value.clone(),
                seq: 0,
                tomb: false,
                group: 0,
                size,
            });
        }
        self.buffer_items(items, total)
    }

    /// Buffer one tombstone per key. Semantics match `put_batch`.
    pub fn delete_batch(&self, keys: &[Vec<u8>]) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        if keys.is_empty() {
            return Ok(());
        }
        let mut items = Vec::with_capacity(keys.len());
        let mut total: i64 = 0;
        for k in keys {
            if k.len() > self.inner.opts.max_key_size {
                return Err(err(ErrorKind::KeyTooLarge, "key exceeds MaxKeySize"));
            }
            let size = k.len() + RECORD_OVERHEAD;
            if size > self.inner.opts.max_block_bytes {
                return Err(err(
                    ErrorKind::RecordTooLarge,
                    "record exceeds MaxBlockBytes",
                ));
            }
            total += size as i64;
            items.push(PendingRecord {
                key: k.clone(),
                value: Vec::new(),
                seq: 0,
                tomb: true,
                group: 0,
                size,
            });
        }
        self.buffer_items(items, total)
    }

    /// Admit validated, owned batch records: one backpressure wait
    /// for the whole batch, then one lock per touched shard.
    fn buffer_items(&self, items: Vec<PendingRecord>, total: i64) -> Result<()> {
        self.check_terminal()?;
        self.check_maint()?;
        if total <= self.inner.opts.max_pending_bytes {
            let bp = self.inner.bp.lock().unwrap();
            let mut bp = bp;
            while self.inner.pending_bytes.load(Ordering::SeqCst) + total
                > self.inner.opts.max_pending_bytes
                && !self.inner.closed.load(Ordering::SeqCst)
                && !self.inner.terminal.load(Ordering::SeqCst)
            {
                bp = self.inner.bp_cond.wait(bp).unwrap();
            }
            drop(bp);
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(err(ErrorKind::Closed, "store is closed"));
            }
            self.check_terminal()?;
        }
        self.inner.pending_bytes.fetch_add(total, Ordering::SeqCst);
        self.inner
            .pending_records
            .fetch_add(items.len() as i64, Ordering::SeqCst);

        let mut groups: HashMap<usize, Vec<PendingRecord>> = HashMap::new();
        for item in items {
            let sh = shard_for_key(&item.key, self.inner.shard_mask) as usize;
            groups.entry(sh).or_default().push(item);
        }
        for (sh, recs) in groups {
            let mut vec = self.inner.shards[sh].lock().unwrap();
            self.inner
                .staged_count
                .fetch_add(recs.len() as u64, Ordering::SeqCst);
            vec.extend(recs);
        }
        self.maybe_signal_flusher();
        Ok(())
    }

    /// Wake the background writer when a count or byte threshold trips.
    fn maybe_signal_flusher(&self) {
        let fp = &self.inner.opts.flush;
        let hit = (fp.max_records > 0
            && self.inner.pending_records.load(Ordering::SeqCst) >= fp.max_records)
            || (fp.max_bytes > 0
                && self.inner.pending_bytes.load(Ordering::SeqCst) >= fp.max_bytes);
        if hit {
            *self.inner.flush_flag.lock().unwrap() = true;
            self.inner.flush_cond.notify_one();
        }
    }

    /// Atomically commit a mixed group of mutations at the requested
    /// durability. Recovery retains the complete group or discards it
    /// as a whole. Empty groups are no-ops; use `sync` for a barrier.
    pub fn commit(&self, mutations: &[crate::record::Mutation], d: Durability) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        self.check_maint()?;
        let mut recs = Vec::with_capacity(mutations.len());
        let mut total: i64 = 0;
        for m in mutations {
            if m.deleted && !m.value.is_empty() {
                return Err(err(
                    ErrorKind::BadMutation,
                    "deleted mutation must have no value",
                ));
            }
            if m.key.len() > self.inner.opts.max_key_size {
                return Err(err(ErrorKind::KeyTooLarge, "key exceeds MaxKeySize"));
            }
            if !m.deleted && m.value.len() > self.inner.opts.max_value_size {
                return Err(err(ErrorKind::ValueTooLarge, "value exceeds MaxValueSize"));
            }
            let size = m.key.len() + m.value.len() + RECORD_OVERHEAD;
            if size > self.inner.opts.max_block_bytes {
                return Err(err(
                    ErrorKind::RecordTooLarge,
                    "record exceeds MaxBlockBytes",
                ));
            }
            total += size as i64;
            recs.push(PendingRecord {
                key: m.key.clone(),
                value: m.value.clone(),
                seq: 0,
                tomb: m.deleted,
                group: 0,
                size,
            });
        }
        // Exact plaintext bound before admission: no wire-size surprise.
        let max_body = self.inner.opts.max_block_bytes as i64
            - BLOCK_HEADER_LEN as i64
            - self.inner.crypt.overhead() as i64
            - 1024;
        if plaintext_group_size(
            &recs,
            self.inner.opts.target_block_bytes,
            max_body.max(1) as usize,
            self.inner.opts.max_records_per_block,
        ) > self.inner.opts.max_atomic_batch_bytes as u64
        {
            return Err(err(ErrorKind::GroupTooLarge, "commit group too large"));
        }
        // Groups never use the large-single-record bypass.
        if total > self.inner.opts.max_pending_bytes {
            return Err(err(ErrorKind::GroupTooLarge, "commit group too large"));
        }
        let start = now_system();
        let rx = {
            let bp = self.inner.bp.lock().unwrap();
            let mut bp = bp;
            while self.inner.pending_bytes.load(Ordering::SeqCst) + total
                > self.inner.opts.max_pending_bytes
                && !self.inner.closed.load(Ordering::SeqCst)
                && !self.inner.terminal.load(Ordering::SeqCst)
            {
                bp = self.inner.bp_cond.wait(bp).unwrap();
            }
            drop(bp);
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(err(ErrorKind::Closed, "store is closed"));
            }
            self.check_terminal()?;
            self.ensure_fresh_key()?;
            let _a = self.inner.admission.lock().unwrap();
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(err(ErrorKind::Closed, "store is closed"));
            }
            self.check_terminal()?;
            // Seal buffered individual writes first so they order
            // before this group.
            self.flush_locked(Durability::Async)?;
            if self.inner.seq_next.load(Ordering::SeqCst) + recs.len() as u64 >= SEQ_COUNTER_MAX {
                return Err(err(
                    ErrorKind::SeqExhausted,
                    "sequence space exhausted, restart the store",
                ));
            }
            let (g, rx) = self.admit_locked(recs, d);
            drop(_a);
            self.inner.pending_bytes.fetch_add(total, Ordering::SeqCst);
            self.inner
                .pending_records
                .fetch_add(g.recs_len as i64, Ordering::SeqCst);
            self.inner.commits.fetch_add(1, Ordering::Relaxed);
            self.inner.commit_nanos.fetch_add(
                start.elapsed().map(|d| d.as_nanos() as u64).unwrap_or(0),
                Ordering::Relaxed,
            );
            rx
        };
        if d == Durability::Async {
            return Ok(());
        }
        rx.recv()
            .unwrap_or_else(|_| Err(err(ErrorKind::StorageFailed, "terminal storage failure")))
    }

    /// Fence every write accepted before the fence and fsync, so data
    /// survives process and OS crashes.
    pub fn sync(&self) -> Result<()> {
        self.sync_barrier()
    }

    /// The same full syncing barrier as `sync`.
    pub fn flush(&self) -> Result<()> {
        self.sync_barrier()
    }

    fn sync_barrier(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        let start = now_system();
        let t0 = std::time::Instant::now();
        let id = self.flush_internal(Durability::Sync, false)?;
        let t1 = std::time::Instant::now();
        self.wait_for_group(id, false)?;
        let t2 = std::time::Instant::now();
        if let Err(e) = self.inner.seg.sync() {
            let e = err(e.kind(), format!("{e}"));
            self.set_terminal(e.clone());
            return Err(e);
        }
        if std::env::var("MURMUR_SPOOL_TIMING").is_ok() {
            let total = t0.elapsed();
            if total.as_millis() >= 20 {
                eprintln!("[spool-barrier] total={}ms seal={}ms groupwait={}ms segsync={}ms", total.as_millis(), t1.duration_since(t0).as_millis(), t2.duration_since(t1).as_millis(), t0.elapsed().saturating_sub(t2.duration_since(t0)).as_millis());
            }
        }
        self.inner.syncs.fetch_add(1, Ordering::Relaxed);
        self.inner.sync_nanos.fetch_add(
            start.elapsed().map(|d| d.as_nanos() as u64).unwrap_or(0),
            Ordering::Relaxed,
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Commit queue
// ---------------------------------------------------------------------------

struct AdmittedGroup {
    recs_len: usize,
}

impl Store {
    /// Swap out every shard buffer and assign sequences in swap
    /// order. The caller must hold admission (ordering) and seal
    /// (shard swap atomicity).
    fn seal_locked(&self) -> Result<Vec<PendingRecord>> {
        let mut out = Vec::new();
        for sh in &self.inner.shards {
            let mut vec = sh.lock().unwrap();
            if !vec.is_empty() {
                if self.inner.seq_next.load(Ordering::SeqCst) + vec.len() as u64 >= SEQ_COUNTER_MAX
                {
                    return Err(err(
                        ErrorKind::SeqExhausted,
                        "sequence space exhausted, restart the store",
                    ));
                }
                for r in vec.iter_mut() {
                    let n = self.inner.seq_next.fetch_add(1, Ordering::SeqCst) + 1;
                    r.seq = self.inner.seq_base | n;
                }
                out.append(&mut vec);
            }
        }
        Ok(out)
    }

    /// Enqueue an internal group of owned records, assigning a group
    /// id and any unassigned sequences in order. The caller must hold
    /// admission.
    fn admit_locked(
        &self,
        mut recs: Vec<PendingRecord>,
        d: Durability,
    ) -> (AdmittedGroup, std::sync::mpsc::Receiver<Result<()>>) {
        let id = self.inner.group_seq.fetch_add(1, Ordering::SeqCst) + 1;
        for r in recs.iter_mut() {
            if r.seq == 0 {
                let n = self.inner.seq_next.fetch_add(1, Ordering::SeqCst) + 1;
                r.seq = self.inner.seq_base | n;
            }
            r.group = id;
        }
        let mut bytes: i64 = 0;
        for r in &recs {
            bytes += r.size as i64;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let recs_len = recs.len();
        {
            let mut q = self.inner.queue.lock().unwrap();
            q.queue.push_back(CommitGroup {
                id,
                recs,
                bytes,
                durability: d,
                done: tx,
            });
            self.inner.last_group.store(id, Ordering::SeqCst);
            self.inner.commit_cond.notify_all();
        }
        self.inner.groups_accepted.fetch_add(1, Ordering::Relaxed);
        self.inner.pending_groups.fetch_add(1, Ordering::SeqCst);
        (AdmittedGroup { recs_len }, rx)
    }

    fn pop_queue(&self) -> Option<CommitGroup> {
        self.inner.queue.lock().unwrap().queue.pop_front()
    }

    /// Block until group `id` completes, the store goes terminal, or
    /// (unless `drain`) the store closes.
    pub(crate) fn wait_for_group(&self, id: u64, drain: bool) -> Result<()> {
        loop {
            let q = self.inner.queue.lock().unwrap();
            let mut q = q;
            while q.completed_through < id
                && (drain || !self.inner.closed.load(Ordering::SeqCst))
                && !self.inner.terminal.load(Ordering::SeqCst)
            {
                q = self.inner.commit_cond.wait(q).unwrap();
            }
            let done = q.completed_through >= id;
            drop(q);
            self.check_terminal()?;
            if done {
                return Ok(());
            }
            if self.inner.closed.load(Ordering::SeqCst) {
                return Err(err(ErrorKind::Closed, "store is closed"));
            }
        }
    }

    /// The single ordered committer: groups append in admission
    /// order. On terminal failure it releases every queued waiter
    /// and exits; on close it drains the queue first.
    fn commit_loop(&self) {
        loop {
            let empty = {
                let q = self.inner.queue.lock().unwrap();
                let mut q = q;
                while q.queue.is_empty()
                    && (!self.inner.closed.load(Ordering::SeqCst) || !q.drained)
                {
                    q = self.inner.commit_cond.wait(q).unwrap();
                }
                q.queue.is_empty()
            };
            if empty {
                return;
            }
            if self.check_terminal().is_err() {
                let cause = self
                    .storage_error()
                    .unwrap_or_else(|| err(ErrorKind::StorageFailed, "terminal storage failure"));
                self.drain_queue(cause);
                return;
            }
            let g = match self.pop_queue() {
                Some(g) => g,
                None => continue,
            };
            let r = self.process_group(g);
            let (id, tx, ok) = (r.0, r.1, r.2);
            if let Err(e) = r.3 {
                self.set_terminal(e);
            }
            {
                let mut q = self.inner.queue.lock().unwrap();
                if ok && id > q.completed_through {
                    q.completed_through = id;
                }
                self.inner.commit_cond.notify_all();
            }
            self.inner.pending_groups.fetch_add(-1, Ordering::SeqCst);
            let _ = tx.send(if ok {
                Ok(())
            } else {
                Err(self
                    .storage_error()
                    .map(|e| {
                        err(
                            ErrorKind::StorageFailed,
                            format!("{e}: terminal storage failure"),
                        )
                    })
                    .unwrap_or_else(|| err(ErrorKind::StorageFailed, "terminal storage failure")))
            });
        }
    }

    /// Record the highest appended group id (CAS: the committer and
    /// replacement compaction append concurrently).
    pub(crate) fn note_appended(&self, id: u64) {
        loop {
            let cur = self.inner.appended_group.load(Ordering::SeqCst);
            if id <= cur
                || self
                    .inner
                    .appended_group
                    .compare_exchange(cur, id, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return;
            }
        }
    }

    /// Release every queued group waiter with the terminal cause.
    fn drain_queue(&self, cause: Error) {
        let queued = {
            let mut q = self.inner.queue.lock().unwrap();
            let queued: Vec<CommitGroup> = q.queue.drain(..).collect();
            self.inner.commit_cond.notify_all();
            queued
        };
        for g in queued {
            self.inner
                .pending_bytes
                .fetch_add(-g.bytes, Ordering::SeqCst);
            self.inner
                .pending_records
                .fetch_add(-(g.recs.len() as i64), Ordering::SeqCst);
            self.inner.pending_groups.fetch_add(-1, Ordering::SeqCst);
            let _ = g.done.send(Err(cause.clone()));
        }
    }
}

// ---------------------------------------------------------------------------
// Block building
// ---------------------------------------------------------------------------

impl Store {
    /// Encode, compress and seal every block of a group in parallel.
    pub(crate) fn build_frames(
        &self,
        group_id: u64,
        blocks: &[Vec<PendingRecord>],
        key_id: u32,
        key: &[u8; 32],
        first_block_id: u64,
        ctx: [u8; 16],
    ) -> Result<(Vec<Vec<u8>>, Vec<usize>)> {
        let n = blocks.len();
        let comp = &self.inner.comp;
        let crypt = self.inner.crypt;
        let mode = comp.mode();
        let store_id = self.inner.manifest.lock().unwrap().store_id;
        // Chunk the work over at most `workers` threads.
        let workers = self.inner.workers.max(1).min(n.max(1));
        let chunk = n.div_ceil(workers);
        let mut frames: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        let mut lens: Vec<usize> = vec![0; n];
        frames.resize_with(n, || None);
        let mut first_err: Option<Error> = None;
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            for (ci, block_chunk) in blocks.chunks(chunk).enumerate() {
                let base = ci * chunk;
                handles.push(s.spawn(move || {
                    let mut out = Vec::with_capacity(block_chunk.len());
                    for (j, recs) in block_chunk.iter().enumerate() {
                        let idx = base + j;
                        let body = encode_body(group_id, idx as u32, n as u32, recs);
                        let r = seal_frame(
                            comp,
                            crypt,
                            mode,
                            &store_id,
                            &body,
                            false,
                            recs.len() as u32,
                            key_id,
                            key,
                            first_block_id + idx as u64,
                            &ctx,
                        );
                        out.push((idx, r));
                    }
                    out
                }));
            }
            for h in handles {
                for (idx, r) in h.join().unwrap() {
                    match r {
                        Ok((f, l)) => {
                            frames[idx] = Some(f);
                            lens[idx] = l;
                        }
                        Err(e) => {
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                        }
                    }
                }
            }
        });
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok((frames.into_iter().map(|f| f.unwrap()).collect(), lens))
    }

    /// Seal the group's completion over the finished data frames.
    /// Completion inputs are explicit (mirrors the Go port) rather than packed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_completion_block(
        &self,
        group_id: u64,
        data_frames: &[Vec<u8>],
        body_lens: &[usize],
        total_records: u32,
        key_id: u32,
        key: &[u8; 32],
        block_id: u64,
        ctx: [u8; 16],
    ) -> Result<(Vec<u8>, usize)> {
        let n = data_frames.len();
        let mut seqs = Vec::with_capacity(n);
        let mut sealed: Vec<&[u8]> = Vec::with_capacity(n);
        let mut total_plain: u64 = 0;
        for (i, f) in data_frames.iter().enumerate() {
            seqs.push(block_id - n as u64 + i as u64);
            sealed.push(&f[BLOCK_HEADER_LEN..]);
            total_plain += body_lens[i] as u64;
        }
        let digest = group_digest(group_id, n as u32, &seqs, &sealed);
        let body = encode_completion(&Completion {
            group_id,
            block_count: n as u32,
            total_records,
            total_plain,
            block_seqs: seqs,
            digest,
        });
        self.seal_block_with(&body, true, 0, key_id, key, block_id, ctx)
    }

    /// Compress and seal one plaintext body under an explicit context.
    /// Seal inputs are explicit (mirrors the Go port) rather than packed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn seal_block_with(
        &self,
        body: &[u8],
        completion: bool,
        record_count: u32,
        key_id: u32,
        key: &[u8; 32],
        block_id: u64,
        ctx: [u8; 16],
    ) -> Result<(Vec<u8>, usize)> {
        let mode = self.inner.comp.mode();
        let store_id = self.inner.manifest.lock().unwrap().store_id;
        seal_frame(
            &self.inner.comp,
            self.inner.crypt,
            mode,
            &store_id,
            body,
            completion,
            record_count,
            key_id,
            key,
            block_id,
            &ctx,
        )
    }

    /// Seal buffered individual writes into bound-fitting internal
    /// groups and admit them in order. Returns the fenced group id.
    pub(crate) fn flush_internal(&self, d: Durability, allow_closed: bool) -> Result<u64> {
        if !allow_closed && self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        self.check_maint()?;
        self.ensure_fresh_key()?;
        let _a = self.inner.admission.lock().unwrap();
        self.flush_locked(d)
    }

    /// Seal buffered writes into admitted groups. The caller must hold
    /// admission. A seal that finds the staged counter unchanged
    /// since the last seal skips the shard walk.
    pub(crate) fn flush_locked(&self, d: Durability) -> Result<u64> {
        let c0 = self.inner.staged_count.load(Ordering::SeqCst);
        if c0 == *self.inner.last_sealed.lock().unwrap() {
            return Ok(self.inner.last_group.load(Ordering::SeqCst));
        }
        let start = now_system();
        let records = {
            let _s = self.inner.seal.lock().unwrap();
            self.seal_locked()?
        };
        *self.inner.last_sealed.lock().unwrap() = c0;
        self.inner.flushes.fetch_add(1, Ordering::Relaxed);
        self.inner.flush_nanos.fetch_add(
            start.elapsed().map(|d| d.as_nanos() as u64).unwrap_or(0),
            Ordering::Relaxed,
        );
        if records.is_empty() {
            return Ok(self.inner.last_group.load(Ordering::SeqCst));
        }
        let mut id = self.inner.last_group.load(Ordering::SeqCst);
        for chunk in chunk_for_bound(records, self.inner.opts.max_atomic_batch_bytes) {
            let (g, _rx) = self.admit_locked(chunk, d);
            let _ = g;
            id = self.inner.last_group.load(Ordering::SeqCst);
        }
        Ok(id)
    }

    /// Build, append and commit one group. The group lands wholly in
    /// one segment; any failure is terminal.
    fn process_group(
        &self,
        g: CommitGroup,
    ) -> (u64, std::sync::mpsc::Sender<Result<()>>, bool, Result<()>) {
        let r = self.process_group_inner(&g);
        let CommitGroup { id, done: tx, .. } = g;
        match r {
            Ok(()) => (id, tx, true, Ok(())),
            Err(e) => (id, tx, false, Err(e)),
        }
    }

    fn process_group_inner(&self, g: &CommitGroup) -> Result<()> {
        let _quiesce = self.inner.seal_quiesce.read().unwrap();
        let (key_id, key) = match self.seal_data_key() {
            Some(v) => v,
            None => return Err(err(ErrorKind::NoCurrentKey, "no current data key")),
        };
        let result = self.process_group_sealed(g, key_id, &key);
        self.release_seal_key(key_id);
        result
    }

    fn process_group_sealed(&self, g: &CommitGroup, key_id: u32, key: &[u8; 32]) -> Result<()> {
        let max_body = (self.inner.opts.max_block_bytes as i64
            - BLOCK_HEADER_LEN as i64
            - self.inner.crypt.overhead() as i64
            - 1024)
            .max(1) as usize;
        let groups = split_blocks(
            &g.recs,
            self.inner.opts.target_block_bytes,
            max_body,
            self.inner.opts.max_records_per_block,
        );
        let n_data = groups.len();
        let first_block = self
            .inner
            .next_block_id
            .fetch_add((n_data + 1) as u64, Ordering::SeqCst)
            + 1;
        let ctx = *self.inner.ctx.read().unwrap();
        let (frames, body_lens) =
            self.build_frames(g.id, &groups, key_id, key, first_block, ctx)?;
        let (comp_frame, comp_len) = self.build_completion_block(
            g.id,
            &frames,
            &body_lens,
            g.recs.len() as u32,
            key_id,
            key,
            first_block + n_data as u64,
            ctx,
        )?;
        let mut all = frames;
        all.push(comp_frame);
        let total: i64 = all.iter().map(|f| f.len() as i64).sum();
        // Reserve the whole group in one segment.
        if self.inner.seg.end_offset() > 0
            && self.inner.seg.end_offset() + total > self.inner.opts.max_segment_size
        {
            let next = self.allocate_file_id();
            self.inner.seg.rotate_to(next)?;
        }
        let (file_id, offs) = self.inner.seg.append_group(&all)?;
        if total > self.inner.opts.max_segment_size {
            let next = self.allocate_file_id();
            self.inner.seg.rotate_to(next)?;
        }
        self.inner.seg.commit(g.durability)?;
        // Durability reached: install index entries and file statistics.
        let mut total_size: i64 = 0;
        for (i, recs) in groups.iter().enumerate() {
            for (j, r) in recs.iter().enumerate() {
                let size = (r.key.len() + r.value.len() + 17) as u32;
                let loc = Location {
                    file_id,
                    block_id: first_block + i as u64,
                    block_offset: offs[i],
                    record_index: j as u32,
                    sequence: r.seq,
                    size,
                };
                self.inner
                    .idx
                    .commit_record(&self.inner.fstats, &r.key, r.seq, r.tomb, loc, size);
                total_size += r.size as i64;
            }
        }
        self.inner
            .blocks_written
            .fetch_add(all.len() as u64, Ordering::Relaxed);
        self.inner
            .bytes_written
            .fetch_add(total as u64, Ordering::Relaxed);
        for l in &body_lens {
            self.inner
                .bytes_uncompressed
                .fetch_add(*l as u64, Ordering::Relaxed);
        }
        self.inner
            .bytes_uncompressed
            .fetch_add(comp_len as u64, Ordering::Relaxed);
        self.inner.groups_written.fetch_add(1, Ordering::Relaxed);
        if g.durability == Durability::Sync {
            self.inner.groups_synced.fetch_add(1, Ordering::Relaxed);
        }
        self.note_appended(g.id);
        self.inner
            .pending_bytes
            .fetch_add(-total_size, Ordering::SeqCst);
        self.inner
            .pending_records
            .fetch_add(-(g.recs.len() as i64), Ordering::SeqCst);
        self.inner.last_flush.store(now_nanos(), Ordering::SeqCst);
        {
            let _bp = self.inner.bp.lock().unwrap();
            self.inner.bp_cond.notify_all();
        }
        Ok(())
    }

    /// Background writer: threshold signals plus the MaxDelay timer.
    fn flush_loop(&self) {
        let delay = self.inner.opts.flush.max_delay;
        loop {
            if self.inner.stop.load(Ordering::SeqCst) {
                return;
            }
            {
                let mut flag = self.inner.flush_flag.lock().unwrap();
                if !*flag {
                    let (f, _) = self.inner.flush_cond.wait_timeout(flag, delay).unwrap();
                    flag = f;
                }
                *flag = false;
            }
            if self.inner.stop.load(Ordering::SeqCst) {
                return;
            }
            if self.inner.pending_records.load(Ordering::SeqCst) > 0 {
                self.background_flush();
            }
        }
    }

    /// Seal one batch and record (never drop) errors.
    fn background_flush(&self) {
        if self.inner.maint_active.load(Ordering::SeqCst) {
            return;
        }
        if let Err(e) = self.flush_internal(self.inner.opts.durability, false) {
            // Closed/stopped errors are routine shutdown races.
            if self.inner.stop.load(Ordering::SeqCst) {
                return;
            }
            self.inner.flush_errors.fetch_add(1, Ordering::Relaxed);
            self.record_flush_err(e);
            if self.check_terminal().is_err() {
                return;
            }
            // Wake promptly for retry; pending counts stayed high.
            *self.inner.flush_flag.lock().unwrap() = true;
            self.inner.flush_cond.notify_one();
        }
    }

    /// Track a sealed-but-unpublished frame's data key. Key 0 needs
    /// no tracking.
    pub(crate) fn track_seal_key(&self, id: u32) {
        if id == 0 {
            return;
        }
        *self.inner.seal_keys.lock().unwrap().entry(id).or_insert(0) += 1;
    }

    /// Release a `track_seal_key` registration.
    pub(crate) fn untrack_seal_key(&self, id: u32) {
        if id == 0 {
            return;
        }
        let mut m = self.inner.seal_keys.lock().unwrap();
        match m.get(&id) {
            Some(1) | None => {
                m.remove(&id);
            }
            Some(_) => {
                *m.get_mut(&id).unwrap() -= 1;
            }
        }
    }

    /// Return the active data key registered as sealed-but-unpublished.
    /// The caller must hold the quiescence read lock (see
    /// `process_group_inner`) and pair success with `release_seal_key`.
    pub(crate) fn seal_data_key(&self) -> Option<(u32, [u8; 32])> {
        loop {
            let (id, key) = self.current_data_key()?;
            self.track_seal_key(id);
            match self.current_data_key() {
                Some((id2, _)) if id2 == id => return Some((id, key)),
                Some(_) => {
                    self.untrack_seal_key(id);
                    continue;
                }
                None => {
                    self.untrack_seal_key(id);
                    return None;
                }
            }
        }
    }

    /// End a `seal_data_key` hold.
    pub(crate) fn release_seal_key(&self, id: u32) {
        self.untrack_seal_key(id);
    }
}

/// Compress and seal one plaintext body with a fresh nonce under
/// explicit parameters. The header doubles as AEAD associated data
/// (bound to the store id and database context), sealed with a zero
/// sealed-length field patched afterwards along with the CRC.
/// Seal inputs are explicit (mirrors the Go port) rather than packed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn seal_frame(
    comp: &Compressor,
    crypt: BlockCrypt,
    mode: u8,
    store_id: &[u8; 16],
    body: &[u8],
    completion: bool,
    record_count: u32,
    key_id: u32,
    key: &[u8; 32],
    block_id: u64,
    ctx: &[u8; 16],
) -> Result<(Vec<u8>, usize)> {
    use crate::block::encode_block_header;
    let comp_body = comp.compress_with(mode, body)?;
    let nonce = crypt.new_nonce()?;
    let mut hdr = encode_block_header(
        crypt.algo,
        mode,
        completion,
        key_id,
        block_id,
        record_count,
        body.len() as u32,
        0,
        &nonce,
    )?;
    let sealed = crypt.seal(
        key,
        &nonce,
        &comp_body,
        &block_aad(&hdr[..59], store_id, ctx),
    )?;
    hdr[30..34].copy_from_slice(&(sealed.len() as u32).to_le_bytes());
    let crc = crate::block::crc32c(&hdr[..59]);
    hdr[59..63].copy_from_slice(&crc.to_le_bytes());
    let mut frame = Vec::with_capacity(hdr.len() + sealed.len());
    frame.extend_from_slice(&hdr);
    frame.extend_from_slice(&sealed);
    Ok((frame, body.len()))
}

/// Group records into block-sized batches. A record larger than the
/// target gets a dedicated block; every block fits `max_body` bytes.
pub(crate) fn split_blocks(
    records: &[PendingRecord],
    target: usize,
    max_body: usize,
    max_recs: usize,
) -> Vec<Vec<PendingRecord>> {
    let mut out: Vec<Vec<PendingRecord>> = Vec::new();
    let mut cur: Vec<PendingRecord> = Vec::new();
    let mut cur_size: usize = 0;
    let flush =
        |cur: &mut Vec<PendingRecord>, cur_size: &mut usize, out: &mut Vec<Vec<PendingRecord>>| {
            if !cur.is_empty() {
                out.push(std::mem::take(cur));
                *cur_size = 0;
            }
        };
    for r in records {
        let rs = r.size + 32;
        if rs > target && cur.is_empty() {
            out.push(vec![r.clone()]);
            continue;
        }
        if cur.len() >= max_recs
            || cur_size + rs > max_body
            || (cur_size > 0 && cur_size + rs > target)
        {
            flush(&mut cur, &mut cur_size, &mut out);
        }
        cur_size += rs;
        cur.push(r.clone());
    }
    flush(&mut cur, &mut cur_size, &mut out);
    out
}

/// Exact plaintext data-body bytes for records split into blocks, for
/// pre-admission bound checks.
pub(crate) fn plaintext_group_size(
    records: &[PendingRecord],
    target: usize,
    max_body: usize,
    max_recs: usize,
) -> u64 {
    let mut total: u64 = 0;
    for b in split_blocks(records, target, max_body, max_recs) {
        let mut n = (crate::block::GROUP_FRAMING_LEN + 4) as u64;
        for r in &b {
            n += (8 + 1 + 4 + 4 + r.key.len() + r.value.len()) as u64;
        }
        total += n;
    }
    total
}

/// Split records into bound-fitting chunks for flush admission.
pub(crate) fn chunk_for_bound(records: Vec<PendingRecord>, bound: i64) -> Vec<Vec<PendingRecord>> {
    let mut out: Vec<Vec<PendingRecord>> = Vec::new();
    let mut cur: Vec<PendingRecord> = Vec::new();
    let mut cur_size: i64 = 0;
    for r in records {
        let rs = r.size as i64 + 32;
        if !cur.is_empty() && cur_size + rs > bound {
            out.push(std::mem::take(&mut cur));
            cur_size = 0;
        }
        cur_size += rs;
        cur.push(r);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

// ---------------------------------------------------------------------------
// Key rotation
// ---------------------------------------------------------------------------

impl Store {
    /// Generate a successor data key, persist it to keys.enc and make
    /// it current for new blocks. Old blocks stay readable. Fails on
    /// unencrypted stores; publication failures are terminal.
    pub fn rotate_key(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        if self.inner.ring.is_none() {
            return Err(err(ErrorKind::Invalid, "key rotation requires encryption"));
        }
        self.check_maint()?;
        // Admission-serialized against checkpoint capture.
        let _a = self.inner.admission.lock().unwrap();
        let _r = self.inner.rotate.lock().unwrap();
        self.rotate_data_key()
    }

    /// Persist a successor data key and install it as current, then
    /// record it in the manifest. The caller must hold the rotate lock.
    pub(crate) fn rotate_data_key(&self) -> Result<()> {
        let ring = self
            .inner
            .ring
            .as_ref()
            .ok_or_else(|| err(ErrorKind::NoCurrentKey, "no current data key"))?;
        // keys.enc first: the manifest must never reference a key
        // that is not yet durable.
        let store_id = self.inner.manifest.lock().unwrap().store_id;
        let (master, pass) = (
            self.inner.master.lock().unwrap().clone(),
            self.inner.pass.lock().unwrap().clone(),
        );
        let id =
            match ring.rotate_persisted(now_nanos(), &self.inner.dir, &store_id, &master, &pass) {
                Ok(id) => id,
                Err(e) => {
                    self.set_terminal(e.clone());
                    return Err(e);
                }
            };
        self.inner.rotations.fetch_add(1, Ordering::Relaxed);
        let m = {
            let mut man = self.inner.manifest.lock().unwrap();
            man.current_key_id = id;
            man.clone()
        };
        if let Err(e) = self.write_manifest_file(&m) {
            let e = err(e.kind(), format!("publish rotation: {e}"));
            self.set_terminal(e.clone());
            return Err(e);
        }
        Ok(())
    }

    /// True when the current data key is within its configured age. A
    /// missing key or disabled aging reads fresh.
    pub(crate) fn key_fresh(&self) -> bool {
        let max_age = match self.inner.opts.data_key_max_age {
            None => return true,
            Some(d) => d,
        };
        let ring = match &self.inner.ring {
            None => return true,
            Some(r) => r,
        };
        let (id, _) = match ring.current_key() {
            None => return true,
            Some(v) => v,
        };
        let created = match ring.created_at(id) {
            None => return true,
            Some(t) => t,
        };
        now_system()
            .duration_since(created)
            .map(|d| d <= max_age)
            .unwrap_or(true)
    }

    /// Rotate an expired data key before new groups reference it.
    pub(crate) fn ensure_fresh_key(&self) -> Result<()> {
        if self.key_fresh() {
            return Ok(());
        }
        let _a = self.inner.admission.lock().unwrap();
        let _r = self.inner.rotate.lock().unwrap();
        if self.key_fresh() {
            return Ok(());
        }
        self.rotate_data_key()
    }

    /// Re-protect keys.enc under new key material without rewriting
    /// data segments. Exactly one of `new_master_key` (32 bytes) and
    /// `new_passphrase` must be set. Fails on unencrypted stores.
    pub fn rotate_master_key(&self, new_master_key: &[u8], new_passphrase: &str) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        if self.inner.ring.is_none() {
            return Err(err(
                ErrorKind::Invalid,
                "master-key rotation requires encryption",
            ));
        }
        self.check_maint()?;
        if !new_master_key.is_empty() == !new_passphrase.is_empty() {
            return Err(err(
                ErrorKind::Invalid,
                "set exactly one of newMasterKey and newPassphrase",
            ));
        }
        if !new_master_key.is_empty() && new_master_key.len() != 32 {
            return Err(err(
                ErrorKind::Invalid,
                format!(
                    "newMasterKey must be 32 bytes, got {}",
                    new_master_key.len()
                ),
            ));
        }
        // Admission-serialized against checkpoint capture.
        let _a = self.inner.admission.lock().unwrap();
        let _r = self.inner.rotate.lock().unwrap();
        let ring = self.inner.ring.as_ref().unwrap();
        // Persist first: on failure the old protector still unlocks
        // the untouched container.
        let store_id = self.inner.manifest.lock().unwrap().store_id;
        if let Err(e) = ring.write_keys(&self.inner.dir, &store_id, new_master_key, new_passphrase)
        {
            let e = err(e.kind(), format!("publish master rotation: {e}"));
            self.set_terminal(e.clone());
            return Err(e);
        }
        {
            let mut m = self.inner.master.lock().unwrap();
            wipe(&mut m);
            *m = new_master_key.to_vec();
        }
        *self.inner.pass.lock().unwrap() = new_passphrase.to_string();
        Ok(())
    }

    /// Re-protect keys.enc under new 32-byte wrapping material and
    /// record its identifier in the authenticated envelope. Fails on
    /// unencrypted stores; publication failures are terminal.
    pub fn rotate_wrapping_key(&self, new_key: &[u8], new_id: &str) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(err(ErrorKind::Closed, "store is closed"));
        }
        self.check_terminal()?;
        if self.inner.ring.is_none() {
            return Err(err(
                ErrorKind::Invalid,
                "wrapping-key rotation requires encryption",
            ));
        }
        self.check_maint()?;
        if new_key.len() != 32 {
            return Err(err(
                ErrorKind::Invalid,
                format!("newKey must be 32 bytes, got {}", new_key.len()),
            ));
        }
        if new_id.is_empty() {
            return Err(err(ErrorKind::Invalid, "new wrapping-key id is required"));
        }
        if new_id.len() > crate::keyring::MAX_WRAP_ID_LEN {
            return Err(err(
                ErrorKind::Invalid,
                format!(
                    "wrapping-key id of {} bytes exceeds {}",
                    new_id.len(),
                    crate::keyring::MAX_WRAP_ID_LEN
                ),
            ));
        }
        // Admission-serialized against checkpoint capture.
        let _a = self.inner.admission.lock().unwrap();
        let _r = self.inner.rotate.lock().unwrap();
        let ring = self.inner.ring.as_ref().unwrap();
        let store_id = self.inner.manifest.lock().unwrap().store_id;
        {
            let mut r = ring.inner.write().unwrap();
            let old = r.wrap_id.clone();
            r.wrap_id = new_id.to_string();
            if let Err(e) = crate::keyring::Keyring::write_keys_locked(
                ring.faults.as_ref(),
                &mut r,
                &self.inner.dir,
                &store_id,
                new_key,
                "",
                None,
            ) {
                r.wrap_id = old;
                drop(r);
                let e = err(e.kind(), format!("publish wrapping rotation: {e}"));
                self.set_terminal(e.clone());
                return Err(e);
            }
        }
        {
            let mut m = self.inner.master.lock().unwrap();
            wipe(&mut m);
            *m = new_key.to_vec();
        }
        *self.inner.pass.lock().unwrap() = String::new();
        *self.inner.wrap_id.write().unwrap() = new_id.to_string();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

impl Store {
    /// Stop background work, drain accepted groups through a syncing
    /// barrier, record a clean shutdown, and release the store lock.
    /// Idempotent.
    pub fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        // Wait out any maintenance: its staged files must settle
        // before the drain measures them.
        {
            let _m = self.inner.maint_excl.lock().unwrap();
        }
        self.inner.read_cache.lock().unwrap().clear();
        // Wake queue waiters; the committer stays until drained.
        {
            let _q = self.inner.queue.lock().unwrap();
            self.inner.commit_cond.notify_all();
        }
        self.inner.stop.store(true, Ordering::SeqCst);
        self.inner.flush_cond.notify_all();
        // Drain everything accepted so far through the committer.
        let mut drain_err: Result<()> = Ok(());
        match self.flush_internal(Durability::Sync, true) {
            Ok(id) => {
                if let Err(e) = self.wait_for_group(id, true) {
                    drain_err = Err(e);
                }
            }
            Err(e) => drain_err = Err(e),
        }
        {
            let mut q = self.inner.queue.lock().unwrap();
            q.drained = true;
            self.inner.commit_cond.notify_all();
        }
        // Join background threads.
        let handles = std::mem::take(&mut *self.inner.handles.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
        if let Err(e) = drain_err {
            let _ = self.inner.seg.close();
            drop(self.inner.store_lock.lock().unwrap().take());
            return Err(e);
        }
        if let Err(e) = self.inner.seg.close() {
            drop(self.inner.store_lock.lock().unwrap().take());
            return Err(e);
        }
        // All segment bytes are synced: record the clean-shutdown shape.
        let seg_ids = self.inner.manifest.lock().unwrap().members.clone();
        let mut seg_bytes: u64 = 0;
        for id in &seg_ids {
            match fs::metadata(
                self.inner
                    .dir
                    .join(SEGMENTS_DIR_NAME)
                    .join(segment_file_name(*id)),
            ) {
                Ok(st) => seg_bytes += st.len(),
                Err(e) => {
                    drop(self.inner.store_lock.lock().unwrap().take());
                    return Err(err(
                        ErrorKind::Io,
                        format!("stat segment {id} at close: {e}"),
                    ));
                }
            }
        }
        let m = {
            let mut man = self.inner.manifest.lock().unwrap();
            man.next_block_id = self.inner.next_block_id.load(Ordering::SeqCst);
            man.next_group_id = self.inner.group_seq.load(Ordering::SeqCst) + 1;
            man.clean = true;
            man.clean_bytes = seg_bytes;
            man.clean_files = seg_ids.len() as u32;
            man.clean_gen = man.generation;
            man.clean_group = self.inner.appended_group.load(Ordering::SeqCst);
            man.clone()
        };
        if let Err(e) = self.write_manifest_file(&m) {
            drop(self.inner.store_lock.lock().unwrap().take());
            return Err(e);
        }
        {
            let mut master = self.inner.master.lock().unwrap();
            wipe(&mut master);
            master.clear();
        }
        drop(self.inner.store_lock.lock().unwrap().take());
        Ok(())
    }

    /// True after `close`.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }
}
