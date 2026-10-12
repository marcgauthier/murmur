//! Block wire format v3.
//!
//! Rust port of `spool/block.go`. All integers little-endian. Every
//! commit is a group: bounded data blocks followed by one completion
//! block. A group is accepted only with its completion validated.
//!
//! Header (63 bytes):
//!
//! ```text
//! off  size  field
//! 0    4     magic "SPOL"
//! 4    2     format version (3)
//! 6    1     encryption algorithm id
//! 7    1     compression id
//! 8    2     flags (bit0 = completion block)
//! 10   4     data key id (0 when unencrypted)
//! 14   8     block sequence (store-wide, informational)
//! 22   4     record count (data blocks; 0 for completion)
//! 26   4     uncompressed body size
//! 30   4     sealed payload size (ciphertext+tag, or body+crc)
//! 34   1     nonce length (0 or 12)
//! 35   24    nonce (prefix of length nonceLen)
//! 59   4     CRC32-Castagnoli over bytes [0,59)
//! ```

use sha2::{Digest, Sha256};

use crate::error::{err, ErrorKind, Result};
use crate::record::PendingRecord;

pub(crate) const BLOCK_MAGIC: &[u8; 4] = b"SPOL";
pub(crate) const BLOCK_VERSION: u16 = 3;
pub(crate) const BLOCK_HEADER_LEN: usize = 63;
pub(crate) const BLOCK_NONCE_LEN: usize = 24;
pub(crate) const BLOCK_CRC_SIZE: usize = 4;
pub(crate) const FLAG_FIELD_TOMBSTONE: u8 = 1 << 0;
pub(crate) const FLAG_BLOCK_COMPLETE: u16 = 1 << 0;

/// Absolute decode caps bounding read-path allocations.
pub(crate) const ABS_MAX_RECORDS_PER_BLOCK: u32 = 1 << 20;
pub(crate) const ABS_MAX_BLOCK_BYTES: u64 = 1 << 30;
pub(crate) const ABS_MAX_KEY_SIZE: u32 = 64 << 20;
pub(crate) const ABS_MAX_VALUE_SIZE: u32 = 512 << 20;
pub(crate) const ABS_MAX_GROUP_BLOCKS: u32 = 1 << 20;
pub(crate) const ABS_MAX_GROUP_RECORDS: u32 = 1 << 24;

pub(crate) fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
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

/// Parsed block header.
#[derive(Debug, Clone)]
pub(crate) struct BlockHeader {
    pub algo: u8,
    pub compression: u8,
    pub completion: bool,
    pub key_id: u32,
    pub block_seq: u64,
    pub record_count: u32,
    pub uncompressed_len: u32,
    pub sealed_len: u32,
    pub nonce: Vec<u8>,
    /// The 63 header bytes (AEAD associated data).
    pub raw: Vec<u8>,
}

/// Serialize the header (without CRC) and append the CRC.
/// Header fields are explicit (mirrors the Go port) rather than packed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_block_header(
    algo: u8,
    compression: u8,
    completion: bool,
    key_id: u32,
    block_seq: u64,
    record_count: u32,
    uncompressed_len: u32,
    sealed_len: u32,
    nonce: &[u8],
) -> Result<Vec<u8>> {
    if nonce.len() > BLOCK_NONCE_LEN {
        return Err(err(
            ErrorKind::Invalid,
            format!("nonce of {} bytes exceeds {BLOCK_NONCE_LEN}", nonce.len()),
        ));
    }
    let mut out = vec![0u8; BLOCK_HEADER_LEN];
    out[0..4].copy_from_slice(BLOCK_MAGIC);
    out[4..6].copy_from_slice(&BLOCK_VERSION.to_le_bytes());
    out[6] = algo;
    out[7] = compression;
    if completion {
        out[8..10].copy_from_slice(&FLAG_BLOCK_COMPLETE.to_le_bytes());
    }
    out[10..14].copy_from_slice(&key_id.to_le_bytes());
    out[14..22].copy_from_slice(&block_seq.to_le_bytes());
    out[22..26].copy_from_slice(&record_count.to_le_bytes());
    out[26..30].copy_from_slice(&uncompressed_len.to_le_bytes());
    out[30..34].copy_from_slice(&sealed_len.to_le_bytes());
    out[34] = nonce.len() as u8;
    out[35..35 + nonce.len()].copy_from_slice(nonce);
    let crc = crc32c(&out[..59]);
    out[59..63].copy_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Validate and parse one header. Structural checks only (magic,
/// version, CRC, field bounds).
pub(crate) fn parse_block_header(raw: &[u8]) -> Result<BlockHeader> {
    if raw.len() < BLOCK_HEADER_LEN {
        return Err(err(ErrorKind::Corrupt, "truncated block header"));
    }
    let raw = &raw[..BLOCK_HEADER_LEN];
    if &raw[0..4] != BLOCK_MAGIC {
        return Err(err(ErrorKind::Corrupt, "bad block magic"));
    }
    let v = u16le(&raw[4..6]);
    if v != BLOCK_VERSION {
        if v > BLOCK_VERSION {
            return Err(err(
                ErrorKind::UnsupportedVersion,
                format!("block version {v}"),
            ));
        }
        return Err(err(ErrorKind::Corrupt, format!("block version {v}")));
    }
    let want = u32le(&raw[59..63]);
    if crc32c(&raw[..59]) != want {
        return Err(err(ErrorKind::Corrupt, "block header checksum mismatch"));
    }
    let nonce_len = raw[34] as usize;
    if nonce_len != 0 && nonce_len != 12 {
        return Err(err(
            ErrorKind::Corrupt,
            format!("bad nonce length {nonce_len}"),
        ));
    }
    let flags = u16le(&raw[8..10]);
    if flags & !FLAG_BLOCK_COMPLETE != 0 {
        return Err(err(
            ErrorKind::Corrupt,
            format!("unknown block flags {flags:#x}"),
        ));
    }
    let h = BlockHeader {
        algo: raw[6],
        compression: raw[7],
        completion: flags & FLAG_BLOCK_COMPLETE != 0,
        key_id: u32le(&raw[10..14]),
        block_seq: u64le(&raw[14..22]),
        record_count: u32le(&raw[22..26]),
        uncompressed_len: u32le(&raw[26..30]),
        sealed_len: u32le(&raw[30..34]),
        nonce: raw[35..35 + nonce_len].to_vec(),
        raw: raw.to_vec(),
    };
    if h.completion {
        if h.record_count != 0 {
            return Err(err(
                ErrorKind::Corrupt,
                format!("completion with record count {}", h.record_count),
            ));
        }
    } else if h.record_count == 0 || h.record_count > ABS_MAX_RECORDS_PER_BLOCK {
        return Err(err(
            ErrorKind::Corrupt,
            format!("bad record count {}", h.record_count),
        ));
    }
    if h.uncompressed_len == 0 || h.uncompressed_len as u64 > ABS_MAX_BLOCK_BYTES {
        return Err(err(
            ErrorKind::Corrupt,
            format!("bad uncompressed size {}", h.uncompressed_len),
        ));
    }
    if h.sealed_len == 0 || h.sealed_len as u64 > ABS_MAX_BLOCK_BYTES {
        return Err(err(
            ErrorKind::Corrupt,
            format!("bad sealed size {}", h.sealed_len),
        ));
    }
    Ok(h)
}

/// Plaintext prefix binding a data body to its group:
/// groupID u64 + blockIndex u32 + blockCount u32.
pub(crate) const GROUP_FRAMING_LEN: usize = 8 + 4 + 4;

/// Serialize records into the plaintext data body layout.
pub(crate) fn encode_body(
    group_id: u64,
    block_index: u32,
    block_count: u32,
    records: &[PendingRecord],
) -> Vec<u8> {
    let mut size = GROUP_FRAMING_LEN + 4;
    for r in records {
        size += 8 + 1 + 4 + 4 + r.key.len() + r.value.len();
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&group_id.to_le_bytes());
    out.extend_from_slice(&block_index.to_le_bytes());
    out.extend_from_slice(&block_count.to_le_bytes());
    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for r in records {
        out.extend_from_slice(&r.seq.to_le_bytes());
        out.push(if r.tomb { FLAG_FIELD_TOMBSTONE } else { 0 });
        out.extend_from_slice(&(r.key.len() as u32).to_le_bytes());
        out.extend_from_slice(&(r.value.len() as u32).to_le_bytes());
        out.extend_from_slice(&r.key);
        out.extend_from_slice(&r.value);
    }
    out
}

/// Decoded data body with its group framing.
pub(crate) struct DecodedBody {
    pub group_id: u64,
    pub block_index: u32,
    pub block_count: u32,
    /// (seq, flags, key, value) with key/value borrowing `body`.
    pub records: Vec<DecodedRecord>,
}

pub(crate) struct DecodedRecord {
    pub seq: u64,
    pub deleted: bool,
    /// Byte ranges into the decoded body.
    pub key_range: (usize, usize),
    pub val_range: (usize, usize),
}

/// Parse a plaintext data body. `want_count` must match the header's
/// record count.
pub(crate) fn decode_body(body: &[u8], want_count: u32) -> Result<DecodedBody> {
    if body.len() < GROUP_FRAMING_LEN + 4 {
        return Err(err(ErrorKind::Corrupt, "truncated block body"));
    }
    let group_id = u64le(&body[0..8]);
    let block_index = u32le(&body[8..12]);
    let block_count = u32le(&body[12..16]);
    if block_count == 0 || block_count > ABS_MAX_GROUP_BLOCKS || block_index >= block_count {
        return Err(err(
            ErrorKind::Corrupt,
            format!("bad group framing {block_index}/{block_count}"),
        ));
    }
    let n = u32le(&body[16..20]);
    if n != want_count {
        return Err(err(
            ErrorKind::Corrupt,
            format!("body count {n} != header count {want_count}"),
        ));
    }
    let mut out = Vec::with_capacity(want_count.min(1024) as usize);
    let mut rest = GROUP_FRAMING_LEN + 4;
    for i in 0..want_count {
        if body.len() - rest < 8 + 1 + 4 + 4 {
            return Err(err(ErrorKind::Corrupt, format!("truncated record {i}")));
        }
        let seq = u64le(&body[rest..rest + 8]);
        let flags = body[rest + 8];
        if flags & !FLAG_FIELD_TOMBSTONE != 0 {
            return Err(err(
                ErrorKind::Corrupt,
                format!("record {i} has unknown flags {flags:#x}"),
            ));
        }
        let key_len = u32le(&body[rest + 9..rest + 13]) as usize;
        let val_len = u32le(&body[rest + 13..rest + 17]) as usize;
        if key_len as u64 > ABS_MAX_KEY_SIZE as u64 || val_len as u64 > ABS_MAX_VALUE_SIZE as u64 {
            return Err(err(
                ErrorKind::Corrupt,
                format!("record {i} oversized ({key_len}/{val_len})"),
            ));
        }
        rest += 17;
        if body.len() - rest < key_len + val_len {
            return Err(err(
                ErrorKind::Corrupt,
                format!("truncated record {i} payload"),
            ));
        }
        let deleted = flags & FLAG_FIELD_TOMBSTONE != 0;
        if deleted && val_len != 0 {
            return Err(err(
                ErrorKind::Corrupt,
                format!("tombstone {i} carries a value"),
            ));
        }
        out.push(DecodedRecord {
            seq,
            deleted,
            key_range: (rest, rest + key_len),
            val_range: (rest + key_len, rest + key_len + val_len),
        });
        rest += key_len + val_len;
    }
    if rest != body.len() {
        return Err(err(
            ErrorKind::Corrupt,
            format!("{} trailing bytes in block body", body.len() - rest),
        ));
    }
    Ok(DecodedBody {
        group_id,
        block_index,
        block_count,
        records: out,
    })
}

/// Completion body without block sequences and digest:
/// groupID u64 + blockCount u32 + totalRecords u32 + totalPlainLen u64.
pub(crate) const COMPLETION_FIXED_LEN: usize = 8 + 4 + 4 + 8;

/// A group's completion: ordered data block identities, declared
/// totals, and the content digest.
#[derive(Debug, Clone)]
pub(crate) struct Completion {
    pub group_id: u64,
    pub block_count: u32,
    pub total_records: u32,
    pub total_plain: u64,
    pub block_seqs: Vec<u64>,
    pub digest: [u8; 32],
}

/// Serialize the completion body.
pub(crate) fn encode_completion(c: &Completion) -> Vec<u8> {
    let mut out = Vec::with_capacity(COMPLETION_FIXED_LEN + 8 * c.block_seqs.len() + 32);
    out.extend_from_slice(&c.group_id.to_le_bytes());
    out.extend_from_slice(&c.block_count.to_le_bytes());
    out.extend_from_slice(&c.total_records.to_le_bytes());
    out.extend_from_slice(&c.total_plain.to_le_bytes());
    for s in &c.block_seqs {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out.extend_from_slice(&c.digest);
    out
}

/// Parse and bound-check a completion body.
pub(crate) fn decode_completion(body: &[u8]) -> Result<Completion> {
    if body.len() < COMPLETION_FIXED_LEN + 32 {
        return Err(err(ErrorKind::Corrupt, "truncated completion"));
    }
    let group_id = u64le(&body[0..8]);
    let block_count = u32le(&body[8..12]);
    let total_records = u32le(&body[12..16]);
    let total_plain = u64le(&body[16..24]);
    if group_id == 0 || block_count == 0 || block_count > ABS_MAX_GROUP_BLOCKS {
        return Err(err(ErrorKind::Corrupt, "bad completion framing"));
    }
    if total_records == 0 || total_records > ABS_MAX_GROUP_RECORDS {
        return Err(err(
            ErrorKind::Corrupt,
            format!("bad completion records {total_records}"),
        ));
    }
    let want = COMPLETION_FIXED_LEN + 8 * block_count as usize + 32;
    if body.len() != want {
        return Err(err(
            ErrorKind::Corrupt,
            format!("completion size {} != {want}", body.len()),
        ));
    }
    let mut block_seqs = Vec::with_capacity(block_count as usize);
    for i in 0..block_count as usize {
        block_seqs.push(u64le(&body[COMPLETION_FIXED_LEN + 8 * i..]));
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&body[body.len() - 32..]);
    Ok(Completion {
        group_id,
        block_count,
        total_records,
        total_plain,
        block_seqs,
        digest,
    })
}

/// Compute the completion content digest over sealed data payloads in
/// index order. `seqs` and `sealed` run parallel.
pub(crate) fn group_digest(
    group_id: u64,
    block_count: u32,
    seqs: &[u64],
    sealed: &[&[u8]],
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(group_id.to_le_bytes());
    h.update(block_count.to_le_bytes());
    for (i, s) in sealed.iter().enumerate() {
        h.update(seqs[i].to_le_bytes());
        h.update((s.len() as u32).to_le_bytes());
        h.update(s);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&h.finalize());
    out
}
