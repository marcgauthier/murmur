//! keys.enc container v3.
//!
//! Rust port of `spool/keyring.go`. All integers little-endian.
//!
//! Header (variable, 61+H bytes):
//!
//! ```text
//! off    size  field
//! 0      4     magic "SPLK"
//! 4      2     format version (3)
//! 6      1     KDF id (0 = master-key HKDF, 1 = argon2id)
//! 7      8     sequence (bumped per persist, informational)
//! 15     32    salt
//! 47     4     argon2 time (direct mode: zero)
//! 51     4     argon2 memory KiB (direct mode: zero)
//! 55     1     argon2 threads (direct mode: zero)
//! 56     1     wrapping-key id hint length H (<= 128)
//! 57     H     wrapping-key id hint (lookup aid only)
//! 57+H   4     CRC32-Castagnoli over bytes [0,57+H)
//! ```
//!
//! Followed by: nonce\[12\] | sealedLen u32 | sealed.
//!
//! The sealed body (AES-256-GCM, AAD = header || store id):
//!
//! ```text
//! count u32
//! database context id [16]
//! wrapping-key id length u8 + id bytes (authenticated value)
//! repeated entry: id u32 | createdAt i64 (unix nanos) | status u8 | key[32]
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;

use crate::block::crc32c;
use crate::crypto::{
    derive_protector, keys_aad, wipe, ARGON_MEMORY, ARGON_THREADS, ARGON_TIME, KDF_ARGON2ID,
    KDF_DIRECT_HKDF,
};
use crate::error::{err, ErrorKind, Result};
use crate::manifest::atomic_write_file;
use crate::options::FaultHooks;

pub(crate) const KEYS_MAGIC: &[u8; 4] = b"SPLK";
pub(crate) const KEYS_VERSION: u16 = 3;
pub(crate) const KEYS_NONCE: usize = 12;
pub(crate) const MAX_DATA_KEYS: u32 = 1 << 16;
pub(crate) const MAX_WRAP_ID_LEN: usize = 128;
pub(crate) const KEYS_FILE_NAME: &str = "keys.enc";

/// A data key's role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStatus {
    /// Current encryption key for new blocks.
    Active,
    /// Decrypts old blocks only.
    Retired,
}

/// One envelope data key: 256 bits for AES-256-GCM.
#[derive(Debug, Clone)]
pub struct DataKey {
    /// Key id.
    pub id: u32,
    /// Key material.
    pub key: [u8; 32],
    /// Creation time, unix nanoseconds.
    pub created_at: i64,
    /// Role.
    pub status: KeyStatus,
}

pub(crate) struct KeyringInner {
    pub keys: HashMap<u32, DataKey>,
    pub current: u32,
    pub seq: u64,
    pub context: [u8; 16],
    pub wrap_id: String,
}

/// Unlocked data keys plus the authenticated envelope identity.
pub(crate) struct Keyring {
    pub inner: RwLock<KeyringInner>,
    pub faults: Option<Arc<FaultHooks>>,
}

impl Keyring {
    pub fn new(faults: Option<Arc<FaultHooks>>) -> Self {
        Keyring {
            inner: RwLock::new(KeyringInner {
                keys: HashMap::new(),
                current: 0,
                seq: 0,
                context: [0u8; 16],
                wrap_id: String::new(),
            }),
            faults,
        }
    }

    /// Key id and material for new blocks.
    pub fn current_key(&self) -> Option<(u32, [u8; 32])> {
        let r = self.inner.read().unwrap();
        r.keys.get(&r.current).map(|k| (k.id, k.key))
    }

    /// Material for a key id.
    pub fn by_id(&self, id: u32) -> Option<[u8; 32]> {
        let r = self.inner.read().unwrap();
        r.keys.get(&id).map(|k| k.key)
    }

    pub fn has_id(&self, id: u32) -> bool {
        let r = self.inner.read().unwrap();
        r.keys.contains_key(&id)
    }

    /// Creation time of a key.
    pub fn created_at(&self, id: u32) -> Option<SystemTime> {
        let r = self.inner.read().unwrap();
        r.keys.get(&id).map(|k| {
            if k.created_at >= 0 {
                UNIX_EPOCH + std::time::Duration::from_nanos(k.created_at as u64)
            } else {
                UNIX_EPOCH - std::time::Duration::from_nanos((-k.created_at) as u64)
            }
        })
    }

    fn next_id(keys: &HashMap<u32, DataKey>) -> u32 {
        let mut next: u32 = 1;
        for id in keys.keys() {
            if *id >= next {
                next = id.saturating_add(1);
            }
        }
        next
    }

    /// Generate a successor key, retire the previous current key.
    /// Serves fresh-store creation only; runtime rotation uses
    /// [`Keyring::rotate_persisted`].
    pub fn rotate(&self, now_nanos: i64) -> Result<u32> {
        let mut raw = [0u8; 32];
        rand::thread_rng()
            .try_fill_bytes(&mut raw)
            .map_err(|e| err(ErrorKind::Io, format!("data key entropy: {e}")))?;
        let mut r = self.inner.write().unwrap();
        let next = Self::next_id(&r.keys);
        let cur = r.current;
        if let Some(prev) = r.keys.get_mut(&cur) {
            prev.status = KeyStatus::Retired;
        }
        r.keys.insert(
            next,
            DataKey {
                id: next,
                key: raw,
                created_at: now_nanos,
                status: KeyStatus::Active,
            },
        );
        r.current = next;
        Ok(next)
    }

    /// Generate a successor key, persist it, and only then install it
    /// as current. Nothing observable changes on persist failure.
    pub fn rotate_persisted(
        &self,
        now_nanos: i64,
        dir: &Path,
        store_id: &[u8; 16],
        master_key: &[u8],
        passphrase: &str,
    ) -> Result<u32> {
        let mut r = self.inner.write().unwrap();
        let mut raw = [0u8; 32];
        rand::thread_rng()
            .try_fill_bytes(&mut raw)
            .map_err(|e| err(ErrorKind::Io, format!("data key entropy: {e}")))?;
        let next = Self::next_id(&r.keys);
        let staged = DataKey {
            id: next,
            key: raw,
            created_at: now_nanos,
            status: KeyStatus::Active,
        };
        if let Err(e) = Self::write_keys_locked(
            self.faults.as_ref(),
            &mut r,
            dir,
            store_id,
            master_key,
            passphrase,
            Some(&staged),
        ) {
            wipe(&mut raw);
            return Err(e);
        }
        let cur = r.current;
        if let Some(prev) = r.keys.get_mut(&cur) {
            prev.status = KeyStatus::Retired;
        }
        r.keys.insert(next, staged);
        r.current = next;
        Ok(next)
    }

    /// Persist the ring atomically. Bumps the sequence.
    pub fn write_keys(
        &self,
        dir: &Path,
        store_id: &[u8; 16],
        master_key: &[u8],
        passphrase: &str,
    ) -> Result<()> {
        let mut r = self.inner.write().unwrap();
        Self::write_keys_locked(
            self.faults.as_ref(),
            &mut r,
            dir,
            store_id,
            master_key,
            passphrase,
            None,
        )
    }

    pub(crate) fn write_keys_locked(
        faults: Option<&Arc<FaultHooks>>,
        r: &mut KeyringInner,
        dir: &Path,
        store_id: &[u8; 16],
        master_key: &[u8],
        passphrase: &str,
        staged: Option<&DataKey>,
    ) -> Result<()> {
        if let Some(f) = faults {
            f.trip("keyring")
                .map_err(|e| err(ErrorKind::Io, format!("injected keyring fault: {e}")))?;
        }
        r.seq += 1;
        let mut keys: Vec<&DataKey> = r.keys.values().collect();
        if let Some(s) = staged {
            if r.keys.contains_key(&s.id) {
                return Err(err(
                    ErrorKind::Corrupt,
                    format!("staged key id {} collides", s.id),
                ));
            }
            keys.push(s);
        }
        let seq = r.seq;
        let context_id = r.context;
        let wrap_id = r.wrap_id.clone();
        if wrap_id.len() > MAX_WRAP_ID_LEN {
            return Err(err(
                ErrorKind::Invalid,
                format!(
                    "wrapping-key id of {} bytes exceeds {MAX_WRAP_ID_LEN}",
                    wrap_id.len()
                ),
            ));
        }
        keys.sort_by_key(|k| k.id);

        let (kdf, a_time, a_mem, a_threads): (u8, u32, u32, u8) = if !passphrase.is_empty() {
            (KDF_ARGON2ID, ARGON_TIME, ARGON_MEMORY, ARGON_THREADS as u8)
        } else {
            (KDF_DIRECT_HKDF, 0, 0, 0)
        };
        let mut salt = [0u8; 32];
        rand::thread_rng()
            .try_fill_bytes(&mut salt)
            .map_err(|e| err(ErrorKind::Io, format!("salt entropy: {e}")))?;
        let mut protector = derive_protector(
            kdf,
            master_key,
            passphrase,
            &salt,
            a_time,
            a_mem,
            a_threads,
            store_id,
            &context_id,
        )?;

        let mut body =
            Vec::with_capacity(4 + 16 + 1 + wrap_id.len() + keys.len() * (4 + 8 + 1 + 32));
        body.extend_from_slice(&(keys.len() as u32).to_le_bytes());
        body.extend_from_slice(&context_id);
        body.push(wrap_id.len() as u8);
        body.extend_from_slice(wrap_id.as_bytes());
        for k in &keys {
            body.extend_from_slice(&k.id.to_le_bytes());
            body.extend_from_slice(&(k.created_at as u64).to_le_bytes());
            body.push(match k.status {
                KeyStatus::Active => 0,
                KeyStatus::Retired => 1,
            });
            body.extend_from_slice(&k.key);
        }

        let mut hdr = Vec::with_capacity(61 + wrap_id.len());
        hdr.extend_from_slice(KEYS_MAGIC);
        hdr.extend_from_slice(&KEYS_VERSION.to_le_bytes());
        hdr.push(kdf);
        hdr.extend_from_slice(&seq.to_le_bytes());
        hdr.extend_from_slice(&salt);
        hdr.extend_from_slice(&a_time.to_le_bytes());
        hdr.extend_from_slice(&a_mem.to_le_bytes());
        hdr.push(a_threads);
        hdr.push(wrap_id.len() as u8);
        hdr.extend_from_slice(wrap_id.as_bytes());
        let c = crc32c(&hdr);
        hdr.extend_from_slice(&c.to_le_bytes());

        let mut nonce = [0u8; KEYS_NONCE];
        rand::thread_rng()
            .try_fill_bytes(&mut nonce)
            .map_err(|e| err(ErrorKind::Io, format!("nonce entropy: {e}")))?;
        let aead = Aes256Gcm::new_from_slice(&protector).expect("AES-256 key length");
        let sealed = aead
            .encrypt(
                Nonce::from_slice(&nonce),
                aes_gcm::aead::Payload {
                    msg: &body,
                    aad: &keys_aad(&hdr, store_id),
                },
            )
            .map_err(|_| err(ErrorKind::Corrupt, "keys seal failed"))?;
        wipe(&mut body);
        wipe(&mut protector);

        let mut out = Vec::with_capacity(hdr.len() + KEYS_NONCE + 4 + sealed.len());
        out.extend_from_slice(&hdr);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&(sealed.len() as u32).to_le_bytes());
        out.extend_from_slice(&sealed);
        atomic_write_file(dir, KEYS_FILE_NAME, &out)
    }
}

pub(crate) struct KeysHeader {
    pub kdf: u8,
    pub seq: u64,
    pub salt: Vec<u8>,
    pub a_time: u32,
    pub a_mem: u32,
    pub a_threads: u8,
    pub wrap_hint: String,
    pub length: usize,
}

/// Validate the envelope header structurally. Authenticates nothing.
pub(crate) fn parse_keys_header(raw: &[u8]) -> Result<KeysHeader> {
    if raw.len() < 61 {
        return Err(err(ErrorKind::Corrupt, "truncated keys.enc"));
    }
    if &raw[0..4] != KEYS_MAGIC {
        return Err(err(ErrorKind::Corrupt, "bad keys.enc magic"));
    }
    let v = u16::from_le_bytes([raw[4], raw[5]]);
    if v != KEYS_VERSION {
        return Err(err(
            ErrorKind::UnsupportedVersion,
            format!("keys.enc version {v} rejected without migration"),
        ));
    }
    let h = raw[56] as usize;
    if h > MAX_WRAP_ID_LEN || raw.len() < 61 + h {
        return Err(err(ErrorKind::Corrupt, "bad keys.enc hint length"));
    }
    let hdr = &raw[..57 + h];
    let want = u32::from_le_bytes([raw[57 + h], raw[58 + h], raw[59 + h], raw[60 + h]]);
    if crc32c(hdr) != want {
        return Err(err(ErrorKind::Corrupt, "keys.enc header checksum mismatch"));
    }
    Ok(KeysHeader {
        kdf: raw[6],
        seq: u64::from_le_bytes(raw[7..15].try_into().unwrap()),
        salt: raw[15..47].to_vec(),
        a_time: u32::from_le_bytes(raw[47..51].try_into().unwrap()),
        a_mem: u32::from_le_bytes(raw[51..55].try_into().unwrap()),
        a_threads: raw[55],
        wrap_hint: String::from_utf8_lossy(&raw[57..57 + h]).into_owned(),
        length: 61 + h,
    })
}

/// Load and unlock the container. `store_id` and `context_id` come
/// from the store manifest; the KDF binds both.
pub(crate) fn read_keys(
    dir: &Path,
    master_key: &[u8],
    passphrase: &str,
    store_id: &[u8; 16],
    context_id: &[u8; 16],
    faults: Option<Arc<FaultHooks>>,
) -> Result<Keyring> {
    let raw = fs::read(dir.join(KEYS_FILE_NAME))
        .map_err(|e| err(ErrorKind::Io, format!("read keys.enc: {e}")))?;
    let h = parse_keys_header(&raw)?;
    if raw.len() < h.length + KEYS_NONCE + 4 {
        return Err(err(ErrorKind::Corrupt, "truncated keys.enc"));
    }
    let mut protector = derive_protector(
        h.kdf,
        master_key,
        passphrase,
        &h.salt,
        h.a_time,
        h.a_mem,
        h.a_threads,
        store_id,
        context_id,
    )?;

    let mut off = h.length;
    let nonce = &raw[off..off + KEYS_NONCE];
    off += KEYS_NONCE;
    let sealed_len = u32::from_le_bytes(raw[off..off + 4].try_into().unwrap()) as usize;
    off += 4;
    if sealed_len > 1 << 24 || raw.len() - off != sealed_len {
        wipe(&mut protector);
        return Err(err(ErrorKind::Corrupt, "bad keys.enc sealed size"));
    }
    let aead = Aes256Gcm::new_from_slice(&protector).expect("AES-256 key length");
    let open_result = aead.decrypt(
        Nonce::from_slice(nonce),
        aes_gcm::aead::Payload {
            msg: &raw[off..],
            aad: &keys_aad(&raw[..h.length], store_id),
        },
    );
    wipe(&mut protector);
    let mut body = match open_result {
        Ok(b) => b,
        Err(_) => return Err(err(ErrorKind::WrongKey, "keys.enc authentication failed")),
    };
    let result = (|| -> Result<Keyring> {
        if body.len() < 4 + 16 + 1 {
            return Err(err(ErrorKind::Corrupt, "truncated keys.enc body"));
        }
        let count = u32::from_le_bytes(body[0..4].try_into().unwrap());
        if count == 0 || count > MAX_DATA_KEYS {
            return Err(err(
                ErrorKind::Corrupt,
                format!("bad keys.enc key count {count}"),
            ));
        }
        let mut context = [0u8; 16];
        context.copy_from_slice(&body[4..20]);
        let w = body[20] as usize;
        if w > MAX_WRAP_ID_LEN || body.len() < 21 + w {
            return Err(err(ErrorKind::Corrupt, "bad keys.enc wrapping id"));
        }
        let wrap_id = String::from_utf8_lossy(&body[21..21 + w]).into_owned();
        const ENTRY_LEN: usize = 4 + 8 + 1 + 32;
        if body.len() - (21 + w) != count as usize * ENTRY_LEN {
            return Err(err(ErrorKind::Corrupt, "keys.enc body size mismatch"));
        }
        if context != *context_id {
            return Err(err(
                ErrorKind::Corrupt,
                "keys.enc context does not match manifest",
            ));
        }
        let ring = Keyring::new(faults);
        {
            let mut r = ring.inner.write().unwrap();
            r.seq = h.seq;
            r.context = context;
            r.wrap_id = wrap_id;
            let mut pos = 21 + w;
            for _ in 0..count {
                let e = &body[pos..pos + ENTRY_LEN];
                pos += ENTRY_LEN;
                let id = u32::from_le_bytes(e[0..4].try_into().unwrap());
                let created = u64::from_le_bytes(e[4..12].try_into().unwrap()) as i64;
                let status = match e[12] {
                    0 => KeyStatus::Active,
                    1 => KeyStatus::Retired,
                    s => return Err(err(ErrorKind::Corrupt, format!("bad key status {s}"))),
                };
                if id == 0 {
                    return Err(err(ErrorKind::Corrupt, "zero key id"));
                }
                if r.keys.contains_key(&id) {
                    return Err(err(ErrorKind::Corrupt, format!("duplicate key id {id}")));
                }
                let mut k = DataKey {
                    id,
                    key: [0u8; 32],
                    created_at: created,
                    status,
                };
                k.key.copy_from_slice(&e[13..45]);
                r.keys.insert(id, k);
                if status == KeyStatus::Active && id > r.current {
                    r.current = id;
                }
            }
            if r.current == 0 {
                return Err(err(ErrorKind::Corrupt, "no active data key"));
            }
        }
        Ok(ring)
    })();
    wipe(&mut body);
    result
}

/// Non-secret store metadata for wrapping-key selection. Read
/// without key material: the wrapping id is a lookup hint only,
/// trusted after the envelope authenticates.
#[derive(Debug, Clone)]
pub struct KeyHint {
    /// True when the store needs key material.
    pub encrypted: bool,
    /// Manifest's immutable store identity.
    pub store_id: [u8; 16],
    /// Manifest's database context.
    pub context_id: [u8; 16],
    /// Wrapping material lookup hint.
    pub wrapping_key_id: String,
    /// Envelope KDF (0 = direct, 1 = argon2id).
    pub kdf: u8,
    /// Envelope's informational persist counter.
    pub keyring_seq: u64,
}

/// Return a store's key-selection metadata without unlocking
/// anything. Use it to pick provider material, then open with the
/// selected key; the open authenticates the hint.
pub fn read_key_hint(dir: &Path) -> Result<KeyHint> {
    let mraw = fs::read(dir.join(crate::manifest::MANIFEST_FILE_NAME))
        .map_err(|e| err(ErrorKind::Io, format!("read manifest: {e}")))?;
    let m = crate::manifest::parse_manifest(&mraw)?;
    let mut hint = KeyHint {
        encrypted: m.encrypted,
        store_id: m.store_id,
        context_id: m.context,
        wrapping_key_id: String::new(),
        kdf: 0,
        keyring_seq: 0,
    };
    if !m.encrypted {
        return Ok(hint);
    }
    let raw = fs::read(dir.join(KEYS_FILE_NAME))
        .map_err(|e| err(ErrorKind::Io, format!("read keys.enc: {e}")))?;
    let h = parse_keys_header(&raw)?;
    hint.wrapping_key_id = h.wrap_hint;
    hint.kdf = h.kdf;
    hint.keyring_seq = h.seq;
    Ok(hint)
}

/// Authenticate a store's key envelope without opening a writable
/// instance or scanning its records. `expected_context` and
/// `expected_wrapping_id` are optional; when supplied they must match
/// the authenticated envelope identity.
pub fn validate_wrapping_key(
    dir: &Path,
    master_key: &[u8],
    expected_context: &[u8],
    expected_wrapping_id: &str,
) -> Result<()> {
    if master_key.len() != 32 {
        return Err(err(ErrorKind::Invalid, "master key must be 32 bytes"));
    }
    if !expected_context.is_empty() && expected_context.len() != 16 {
        return Err(err(ErrorKind::Invalid, "expected context must be 16 bytes"));
    }
    let hint = read_key_hint(dir)?;
    if !hint.encrypted {
        return Err(err(ErrorKind::WrongKey, "store is not encrypted"));
    }
    if expected_context.len() == 16 && expected_context != hint.context_id {
        return Err(err(
            ErrorKind::ContextMismatch,
            "expected context does not match store",
        ));
    }
    let ring = read_keys(dir, master_key, "", &hint.store_id, &hint.context_id, None)?;
    let wrap_id = ring.inner.read().unwrap().wrap_id.clone();
    {
        let mut r = ring.inner.write().unwrap();
        for k in r.keys.values_mut() {
            wipe(&mut k.key);
        }
        r.keys.clear();
    }
    if !expected_wrapping_id.is_empty() && wrap_id != expected_wrapping_id {
        return Err(err(
            ErrorKind::WrongKey,
            format!(
                "authenticated wrapping-key id {wrap_id:?} does not match {expected_wrapping_id:?}"
            ),
        ));
    }
    Ok(())
}
