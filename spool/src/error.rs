//! Spool error type.
//!
//! Rust port of `spool/errors.go`. Every failure maps to an [`ErrorKind`]
//! matching one of the Go sentinels; use [`Error::kind`] (or `matches!`)
//! instead of comparing strings.

use std::fmt;

/// Machine-readable error classification. Mirrors the Go sentinel set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Operation on a closed store (`ErrClosed`).
    Closed,
    /// `TryPut` on a full pending buffer (`ErrBackpressure`).
    Backpressure,
    /// Key exceeds `MaxKeySize` (`ErrKeyTooLarge`).
    KeyTooLarge,
    /// Value exceeds `MaxValueSize` (`ErrValueTooLarge`).
    ValueTooLarge,
    /// Single record cannot fit `MaxBlockBytes` (`ErrRecordTooLarge`).
    RecordTooLarge,
    /// On-disk structural validation failed (`ErrCorrupt`).
    Corrupt,
    /// Block or key-container authentication failed (`ErrAuthFailed`).
    AuthFailed,
    /// Wrong master key / passphrase, or damaged `keys.enc` (`ErrWrongKey`).
    WrongKey,
    /// Store directory already open in another process (`ErrLocked`).
    Locked,
    /// Block references an absent data key (`ErrUnknownKeyID`).
    UnknownKeyID,
    /// On-disk format newer than this crate understands (`ErrUnsupportedVersion`).
    UnsupportedVersion,
    /// Per-open sequence counter exhausted; restart cleanly (`ErrSeqExhausted`).
    SeqExhausted,
    /// Commit group too large (`ErrGroupTooLarge`).
    GroupTooLarge,
    /// Deleted mutation carrying a value (`ErrBadMutation`).
    BadMutation,
    /// Terminal storage failure; reopen to recover (`ErrStorageFailed`).
    StorageFailed,
    /// Supplied database context mismatches the store (`ErrContextMismatch`).
    ContextMismatch,
    /// Rewrite/rebind holds the store (`ErrMaintenance`).
    Maintenance,
    /// Key id missing from the keyring (internal invariant; corruption).
    NoCurrentKey,
    /// Configuration or caller-usage error (bad option value, bad call).
    Invalid,
    /// Underlying I/O failure.
    Io,
    /// Interrupted / canceled operation (checkpoint cancellation).
    Canceled,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorKind::Closed => "store is closed",
            ErrorKind::Backpressure => "pending write buffer full",
            ErrorKind::KeyTooLarge => "key exceeds MaxKeySize",
            ErrorKind::ValueTooLarge => "value exceeds MaxValueSize",
            ErrorKind::RecordTooLarge => "record exceeds MaxBlockBytes",
            ErrorKind::Corrupt => "corrupt data",
            ErrorKind::AuthFailed => "authentication failed",
            ErrorKind::WrongKey => "wrong master key or damaged keys.enc",
            ErrorKind::Locked => "store is locked by another process",
            ErrorKind::UnknownKeyID => "block references unknown data key",
            ErrorKind::UnsupportedVersion => "unsupported format version",
            ErrorKind::SeqExhausted => "sequence space exhausted, restart the store",
            ErrorKind::GroupTooLarge => "commit group too large",
            ErrorKind::BadMutation => "deleted mutation must have no value",
            ErrorKind::StorageFailed => "terminal storage failure",
            ErrorKind::ContextMismatch => "database context mismatch",
            ErrorKind::Maintenance => "maintenance in progress",
            ErrorKind::NoCurrentKey => "no current data key",
            ErrorKind::Invalid => "invalid argument",
            ErrorKind::Io => "i/o error",
            ErrorKind::Canceled => "operation canceled",
        };
        write!(f, "{s}")
    }
}

/// Spool error: a kind plus a human-readable message chain.
#[derive(Debug, Clone)]
pub struct Error {
    kind: ErrorKind,
    msg: String,
}

impl Error {
    /// Build an error of `kind` with message `msg`.
    pub fn new(kind: ErrorKind, msg: impl Into<String>) -> Self {
        Error {
            kind,
            msg: msg.into(),
        }
    }

    /// The machine-readable classification.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// True for [`ErrorKind::Closed`].
    pub fn is_closed(&self) -> bool {
        self.kind == ErrorKind::Closed
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "spool: {}", self.msg)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::new(ErrorKind::Io, e.to_string())
    }
}

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Internal helper: wrap a message with a kind.
pub(crate) fn err(kind: ErrorKind, msg: impl Into<String>) -> Error {
    Error::new(kind, msg.into())
}
