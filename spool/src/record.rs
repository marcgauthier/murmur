//! Records and mutations.
//!
//! Rust port of `spool/record.go`.

/// Bounds the framing bytes around key+value inside a block body:
/// sequence(8) + flags(1) + keyLen(4) + valueLen(4), plus slack for
/// block-level framing amortized per record.
pub const RECORD_OVERHEAD: usize = 8 + 1 + 4 + 4 + 64;

/// One decoded key/value mutation delivered to load callbacks.
/// Each key arrives at most once per load, with its current version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Record key (may be empty).
    pub key: Vec<u8>,
    /// Value; empty for tombstones.
    pub value: Vec<u8>,
    /// Orders versions of a key; higher wins.
    pub sequence: u64,
    /// Reports a tombstone.
    pub deleted: bool,
}

/// One key/value pair for [`crate::Store::put_batch`].
#[derive(Debug, Clone)]
pub struct KV {
    /// Key bytes.
    pub key: Vec<u8>,
    /// Value bytes.
    pub value: Vec<u8>,
}

impl KV {
    /// Build a pair from byte-like values.
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        KV {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// One operation in a [`crate::Store::commit`] group. A deleted
/// mutation must have an empty value.
#[derive(Debug, Clone)]
pub struct Mutation {
    /// Key bytes.
    pub key: Vec<u8>,
    /// Value bytes; must be empty when `deleted`.
    pub value: Vec<u8>,
    /// True for a deletion.
    pub deleted: bool,
}

impl Mutation {
    /// A put mutation.
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Mutation {
            key: key.into(),
            value: value.into(),
            deleted: false,
        }
    }

    /// A delete mutation.
    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Mutation {
            key: key.into(),
            value: Vec::new(),
            deleted: true,
        }
    }
}

/// A buffered, not yet sealed mutation.
#[derive(Debug, Clone, Default)]
pub(crate) struct PendingRecord {
    pub key: Vec<u8>,
    /// Empty for tombstones.
    pub value: Vec<u8>,
    pub seq: u64,
    pub tomb: bool,
    /// Commit group id, assigned at admission.
    pub group: u64,
    /// Precomputed key+value bytes for buffer accounting.
    pub size: usize,
}
