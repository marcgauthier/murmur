//! Block encryption and key derivation.
//!
//! Rust port of `spool/crypto.go`: AES-256-GCM blocks, HKDF-SHA256
//! and Argon2id keys.enc protectors.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;

use crate::block::crc32c;
use crate::error::{err, ErrorKind, Result};
use crate::options::Encryption;

/// Wire ids for the block AEAD. Id 1 is retired and fails closed.
pub(crate) const ALGO_NONE: u8 = 0;
pub(crate) const ALGO_AES_GCM: u8 = 2;

/// KDF ids for keys.enc.
pub(crate) const KDF_DIRECT_HKDF: u8 = 0;
pub(crate) const KDF_ARGON2ID: u8 = 1;

/// Argon2id parameters for passphrase-derived keys.enc protectors.
pub(crate) const ARGON_TIME: u32 = 3;
pub(crate) const ARGON_MEMORY: u32 = 64 * 1024; // KiB
pub(crate) const ARGON_THREADS: u32 = 4;
pub(crate) const ARGON_KEY_LEN: usize = 32;

/// HKDF info prefix binding the keys.enc protector to this exact use,
/// store, and database context. Full info is prefix || storeID || contextID.
const KEYS_ENC_INFO: &str = "spool/keys-enc/v3/";

/// Block payload sealer. In [`ALGO_NONE`] mode the "seal" appends a
/// CRC32-Castagnoli instead of an AEAD tag.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockCrypt {
    pub algo: u8,
}

impl BlockCrypt {
    pub fn for_encryption(e: Encryption) -> Result<Self> {
        match e {
            Encryption::None => Ok(BlockCrypt { algo: ALGO_NONE }),
            Encryption::Aes256Gcm | Encryption::Default => Ok(BlockCrypt { algo: ALGO_AES_GCM }),
        }
    }

    /// Nonce length for the algorithm.
    pub fn nonce_size(&self) -> usize {
        if self.algo == ALGO_AES_GCM {
            12
        } else {
            0
        }
    }

    /// Authentication bytes added by seal.
    pub fn overhead(&self) -> usize {
        if self.algo == ALGO_AES_GCM {
            16
        } else {
            crate::block::BLOCK_CRC_SIZE
        }
    }

    fn aead(key: &[u8; 32]) -> Aes256Gcm {
        Aes256Gcm::new_from_slice(key).expect("AES-256 key length")
    }

    /// Encrypt `plain` with `key` and `nonce`, authenticating `aad`.
    /// Every call must use a fresh nonce.
    pub fn seal(&self, key: &[u8; 32], nonce: &[u8], plain: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        if self.algo == ALGO_NONE {
            let mut out = Vec::with_capacity(plain.len() + 4);
            out.extend_from_slice(plain);
            out.extend_from_slice(&crc32c(plain).to_le_bytes());
            return Ok(out);
        }
        if self.algo != ALGO_AES_GCM {
            return Err(err(
                ErrorKind::Corrupt,
                format!("no AEAD for algorithm {}", self.algo),
            ));
        }
        let a = Self::aead(key);
        let n = Nonce::from_slice(nonce);
        let payload = aes_gcm::aead::Payload { msg: plain, aad };
        a.encrypt(n, payload)
            .map_err(|_| err(ErrorKind::Corrupt, "seal failed"))
    }

    /// Authenticate and decrypt `sealed`.
    pub fn open(&self, key: &[u8; 32], nonce: &[u8], sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        if self.algo == ALGO_NONE {
            if sealed.len() < 4 {
                return Err(err(ErrorKind::Corrupt, "truncated plain payload"));
            }
            let (body, tag) = sealed.split_at(sealed.len() - 4);
            let want = u32::from_le_bytes([tag[0], tag[1], tag[2], tag[3]]);
            if crc32c(body) != want {
                return Err(err(ErrorKind::Corrupt, "payload checksum mismatch"));
            }
            return Ok(body.to_vec());
        }
        if self.algo != ALGO_AES_GCM {
            return Err(err(
                ErrorKind::Corrupt,
                format!("no AEAD for algorithm {}", self.algo),
            ));
        }
        let a = Self::aead(key);
        let n = Nonce::from_slice(nonce);
        let payload = aes_gcm::aead::Payload { msg: sealed, aad };
        a.decrypt(n, payload)
            .map_err(|_| err(ErrorKind::AuthFailed, "block authentication failed"))
    }

    /// Draw a fresh random nonce of the algorithm's size.
    pub fn new_nonce(&self) -> Result<Vec<u8>> {
        let n = self.nonce_size();
        if n == 0 {
            return Ok(Vec::new());
        }
        let mut out = vec![0u8; n];
        rand::thread_rng()
            .try_fill_bytes(&mut out)
            .map_err(|e| err(ErrorKind::Io, format!("nonce entropy: {e}")))?;
        Ok(out)
    }
}

/// Derive the 256-bit keys.enc protector using the container's KDF.
/// Exactly one of `master_key` and `passphrase` must be set, matching
/// the KDF id. Both KDFs finish through HKDF with an info string
/// binding the store id and database context.
/// KDF inputs are explicit (mirrors the Go port) rather than packed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn derive_protector(
    kdf: u8,
    master_key: &[u8],
    passphrase: &str,
    salt: &[u8],
    time: u32,
    memory: u32,
    threads: u8,
    store_id: &[u8; 16],
    context_id: &[u8; 16],
) -> Result<[u8; 32]> {
    let mut info = Vec::with_capacity(KEYS_ENC_INFO.len() + 32);
    info.extend_from_slice(KEYS_ENC_INFO.as_bytes());
    info.extend_from_slice(store_id);
    info.extend_from_slice(context_id);
    match kdf {
        KDF_DIRECT_HKDF => {
            if master_key.is_empty() {
                return Err(err(ErrorKind::WrongKey, "keys.enc needs a master key"));
            }
            hkdf_sha256(master_key, salt, &info)
        }
        KDF_ARGON2ID => {
            if passphrase.is_empty() {
                return Err(err(ErrorKind::WrongKey, "keys.enc needs a passphrase"));
            }
            if threads == 0 || time == 0 || memory == 0 {
                return Err(err(ErrorKind::Corrupt, "bad argon2 parameters"));
            }
            let params = argon2::Params::new(memory, time, threads as u32, Some(ARGON_KEY_LEN))
                .map_err(|e| err(ErrorKind::Corrupt, format!("bad argon2 parameters: {e}")))?;
            let ctx =
                argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
            let mut stretched = [0u8; ARGON_KEY_LEN];
            ctx.hash_password_into(passphrase.as_bytes(), salt, &mut stretched)
                .map_err(|e| err(ErrorKind::Corrupt, format!("argon2: {e}")))?;
            let out = hkdf_sha256(&stretched, salt, &info);
            wipe(&mut stretched);
            out
        }
        _ => Err(err(
            ErrorKind::Corrupt,
            format!("unknown keys.enc kdf {kdf}"),
        )),
    }
}

fn hkdf_sha256(ikm: &[u8], salt: &[u8], info: &[u8]) -> Result<[u8; 32]> {
    let hk = hkdf::Hkdf::<sha2::Sha256>::new(Some(salt), ikm);
    let mut out = [0u8; 32];
    hk.expand(info, &mut out)
        .map_err(|e| err(ErrorKind::Corrupt, format!("hkdf: {e}")))?;
    Ok(out)
}

/// Bind a sealed block to its store and database context: the 59
/// header bytes covered by the header CRC (with a zero sealed-length
/// field, matching seal time), the 16-byte store id, and the 16-byte
/// database context. Callers pass the header bytes with the
/// sealed-length field already zeroed.
pub(crate) fn block_aad(hdr59: &[u8], store_id: &[u8], context_id: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(59 + 16 + 16);
    aad.extend_from_slice(hdr59);
    aad.extend_from_slice(store_id);
    aad.extend_from_slice(context_id);
    aad
}

/// Bind the keys.enc envelope to its store: the full header plus the
/// 16-byte store id. The database context travels inside the
/// authenticated body and binds the KDF info as well.
pub(crate) fn keys_aad(hdr: &[u8], store_id: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(hdr.len() + 16);
    aad.extend_from_slice(hdr);
    aad.extend_from_slice(store_id);
    aad
}

/// Zero sensitive buffers best-effort.
pub(crate) fn wipe(b: &mut [u8]) {
    for x in b.iter_mut() {
        *x = 0;
    }
}
