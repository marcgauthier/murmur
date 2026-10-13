#![cfg(feature = "replication")]
#[path = "support/file_backend.rs"]
mod support;
use sqlite_session::replication::{
    self as net, Backend,
    bridge::{Bridge, Bundle, Config},
    files::{Config as FileConfig, Objects},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, atomic::Ordering},
};
use support::FileBackend;

struct Fixture {
    _temp: tempfile::TempDir,
    low: Arc<FileBackend>,
    high: Arc<FileBackend>,
    low_files: Arc<Objects>,
    high_files: Arc<Objects>,
    export: Config,
    receive: Config,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let p = temp.path();
        let low = Arc::new(FileBackend::open(&p.join("low-store"), 1, 1));
        let high = Arc::new(FileBackend::open(&p.join("high-store"), 2, 2));
        let low_files = Arc::new(
            Objects::open(
                FileConfig::new(
                    p.join("low-files"),
                    [10; 32],
                    "127.0.0.1:0".parse().unwrap(),
                ),
                low.clone(),
            )
            .unwrap(),
        );
        let high_files = Arc::new(
            Objects::open(
                FileConfig::new(
                    p.join("high-files"),
                    [20; 32],
                    "127.0.0.1:0".parse().unwrap(),
                ),
                high.clone(),
            )
            .unwrap(),
        );
        let mut export = Config::exporter(
            [1; 16],
            p.join("out"),
            "files".into(),
            [30; 32],
            net::bridge::recipient_public([40; 32]),
        );
        export.max_payload_bytes = 8192;
        export.max_bundle_bytes = 16384;
        let mut receive = Config::receiver(
            [2; 16],
            p.join("in"),
            "files".into(),
            [40; 32],
            BTreeSet::from([[1; 16]]),
            BTreeMap::from([([1; 16], net::protocol::public_key(&[30; 32]))]),
            BTreeMap::new(),
        );
        receive.max_payload_bytes = 8192;
        receive.max_bundle_bytes = 16384;
        Self {
            _temp: temp,
            low,
            high,
            low_files,
            high_files,
            export,
            receive,
        }
    }
    fn sender(&self) -> Bridge {
        let b = Bridge::open(self.export.clone(), self.low.clone()).unwrap();
        b.attach_files(Some(self.low_files.clone()));
        b
    }
    fn receiver(&self) -> Bridge {
        let b = Bridge::open(self.receive.clone(), self.high.clone()).unwrap();
        b.attach_files(Some(self.high_files.clone()));
        b
    }
    fn bundles(&self) -> Vec<PathBuf> {
        let mut paths: Vec<_> = std::fs::read_dir(&self.export.directory)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "bundle"))
            .collect();
        paths.sort();
        paths
    }
    fn copy(&self, path: &Path) {
        std::fs::copy(path, self.receive.directory.join(path.file_name().unwrap())).unwrap();
    }
    fn finish(&self, b: &Bridge) {
        for _ in 0..100 {
            if b.process().unwrap() == 0 {
                return;
            }
        }
        panic!("bridge did not become idle");
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.low.store.close().unwrap();
        self.high.store.close().unwrap();
    }
}
fn data() -> Vec<u8> {
    (0..100000).map(|i| ((i * 73) ^ (i >> 8)) as u8).collect()
}

#[test]
fn multi_bundle_file_uses_receiver_key_and_replays_are_idempotent() {
    let f = Fixture::new();
    let bytes = data();
    f.low_files.upload("report", bytes.as_slice()).unwrap();
    let sender = f.sender();
    sender.export_file("report").unwrap();
    let paths = f.bundles();
    assert!(paths.len() > 4);
    let receiver = f.receiver();
    for path in &paths {
        f.copy(path);
    }
    f.finish(&receiver);
    let mut out = Vec::new();
    let high = f.high_files.read("report", &mut out).unwrap();
    assert_eq!(out, bytes);
    let low = f.low_files.metadata("report").unwrap().unwrap();
    assert_eq!(high.digest, low.digest);
    assert_ne!(
        std::fs::read(f.low_files.path(low.digest, 0)).unwrap(),
        std::fs::read(f.high_files.path(high.digest, 0)).unwrap()
    );
    let count = f.bundles().len();
    sender.export_file("report").unwrap();
    assert_eq!(f.bundles().len(), count);
    let before = f.high_files.metadata("report").unwrap();
    drop(receiver);
    let receiver = f.receiver();
    for path in &paths {
        f.copy(path);
    }
    assert_eq!(receiver.process().unwrap(), 0);
    assert_eq!(f.high_files.metadata("report").unwrap(), before);
}

#[test]
fn missing_reordered_chunks_and_receiver_restart_preserve_partial_transfer() {
    let f = Fixture::new();
    let bytes = data();
    f.low_files.upload("report", bytes.as_slice()).unwrap();
    f.sender().export_file("report").unwrap();
    let paths = f.bundles();
    let receiver = f.receiver();
    for path in paths[1..].iter().rev() {
        f.copy(path);
    }
    assert_eq!(receiver.process().unwrap(), 0);
    assert!(f.high_files.metadata("report").unwrap().is_none());
    f.copy(&paths[0]);
    assert_eq!(receiver.process().unwrap(), 4);
    assert!(f.high_files.metadata("report").unwrap().is_none());
    drop(receiver);
    // Reopen the actual authoritative storage, not just the bridge instance.
    f.high.store.close().unwrap();
    let reopened = Arc::new(FileBackend::open(&f._temp.path().join("high-store"), 2, 2));
    let files = Arc::new(
        Objects::open(
            FileConfig::new(
                f._temp.path().join("high-files"),
                [20; 32],
                "127.0.0.1:0".parse().unwrap(),
            ),
            reopened.clone(),
        )
        .unwrap(),
    );
    let receiver = Bridge::open(f.receive.clone(), reopened.clone()).unwrap();
    receiver.attach_files(Some(files.clone()));
    f.finish(&receiver);
    let mut out = Vec::new();
    files.read("report", &mut out).unwrap();
    assert_eq!(out, bytes);
    reopened.store.close().unwrap();
}

#[test]
fn exporter_restart_after_bundle_commit_before_offset_commit_is_safe() {
    let f = Fixture::new();
    let bytes = data();
    f.low_files.upload("report", bytes.as_slice()).unwrap();
    f.low.fail_commit.store(4, Ordering::SeqCst);
    assert!(f.sender().export_file("report").is_err());
    assert_eq!(f.bundles().len(), 2);
    f.low.store.close().unwrap();
    let reopened = Arc::new(FileBackend::open(&f._temp.path().join("low-store"), 1, 1));
    let files = Arc::new(
        Objects::open(
            FileConfig::new(
                f._temp.path().join("low-files"),
                [10; 32],
                "127.0.0.1:0".parse().unwrap(),
            ),
            reopened.clone(),
        )
        .unwrap(),
    );
    let sender = Bridge::open(f.export.clone(), reopened.clone()).unwrap();
    sender.attach_files(Some(files));
    sender.export_file("report").unwrap();
    let receiver = f.receiver();
    for path in f.bundles() {
        f.copy(&path);
    }
    f.finish(&receiver);
    let mut out = Vec::new();
    f.high_files.read("report", &mut out).unwrap();
    assert_eq!(out, bytes);
    reopened.store.close().unwrap();
}

#[test]
fn wrong_recipient_forged_and_truncated_bundles_are_rejected_before_staging() {
    let f = Fixture::new();
    let sender = f.sender();
    let path = sender.export(b"payload").unwrap();
    let good = std::fs::read(path).unwrap();
    let receiver = f.receiver();
    receiver.open_bundle(&good).unwrap();
    let mut wrong = f.receive.clone();
    wrong.receiver_secret = Some([41; 32].into());
    let wrong = Bridge::open(wrong, f.high.clone()).unwrap();
    assert!(wrong.open_bundle(&good).is_err());
    for len in [0, 1, good.len() / 2, good.len() - 1] {
        assert!(receiver.open_bundle(&good[..len]).is_err());
    }
    let mut bundle: Bundle = net::decode(&good).unwrap();
    bundle.ciphertext[0] ^= 1;
    let forged = net::encode(&bundle).unwrap();
    assert!(receiver.open_bundle(&forged).is_err());
    let path = f.receive.directory.join("forged.bundle");
    std::fs::write(&path, forged).unwrap();
    assert!(receiver.stage(&path).is_err());
    assert!(f.high.keys(b"09/bridge/inbox/").unwrap().is_empty());
    assert_eq!(receiver.process().unwrap(), 0);
    assert!(
        f.receive
            .directory
            .join("quarantine/forged.bundle")
            .exists()
    );
}

#[derive(serde::Serialize)]
struct Part {
    name: String,
    digest: [u8; 32],
    bytes: u64,
    offset: u64,
    data: Vec<u8>,
    final_part: bool,
}
fn payload(part: &Part) -> Vec<u8> {
    let mut bytes = b"MURMUR-BRIDGE-FILE-1\0".to_vec();
    bytes.extend(net::encode(part).unwrap());
    bytes
}

#[test]
fn conflicting_authenticated_chunk_is_held_without_publishing_file() {
    let f = Fixture::new();
    let sender = f.sender();
    let receiver = f.receiver();
    let meta = f.low_files.upload("report", b"abcd".as_slice()).unwrap();
    let mut part = Part {
        name: "report".into(),
        digest: meta.digest,
        bytes: 4,
        offset: 0,
        data: b"ab".to_vec(),
        final_part: false,
    };
    let first = sender.export(&payload(&part)).unwrap();
    f.copy(&first);
    assert_eq!(receiver.process().unwrap(), 1);
    part.data = b"xx".to_vec();
    let conflict = sender.export(&payload(&part)).unwrap();
    f.copy(&conflict);
    assert_eq!(receiver.process().unwrap(), 0);
    assert!(!f.high.keys(b"09/bridge/hold/").unwrap().is_empty());
    assert!(f.high_files.metadata("report").unwrap().is_none());
    assert_eq!(receiver.process().unwrap(), 0); // Reprocessing the conflict cannot advance progress.
}

#[test]
fn empty_bridge_file_and_authenticated_payload_limits() {
    let f = Fixture::new();
    f.low_files.upload("empty", b"".as_slice()).unwrap();
    f.sender().export_file("empty").unwrap();
    let receiver = f.receiver();
    for path in f.bundles() {
        f.copy(&path);
    }
    f.finish(&receiver);
    let mut out = Vec::new();
    f.high_files.read("empty", &mut out).unwrap();
    assert!(out.is_empty());
    let path = f.sender().export(&vec![0; 8192]).unwrap();
    let bytes = std::fs::read(path).unwrap();
    let mut cfg = f.receive.clone();
    cfg.max_payload_bytes = 1024;
    let limited = Bridge::open(cfg, f.high.clone()).unwrap();
    assert!(limited.open_bundle(&bytes).is_err());
}

#[test]
fn failed_final_receipt_retries_and_completion_is_idempotent_after_spool_reopen() {
    let f = Fixture::new();
    f.low_files.upload("report", b"abcd".as_slice()).unwrap();
    f.sender().export_file("report").unwrap();
    let receiver = f.receiver();
    let paths = f.bundles();
    for path in &paths {
        receiver.stage(path).unwrap();
    }
    // Data progress, local upload, then the completion receipt: fail only the
    // receipt, leaving a fully written partial file for restart recovery.
    f.high.fail_commit.store(3, Ordering::SeqCst);
    assert_eq!(receiver.process().unwrap(), 1);
    assert!(!f.high.keys(b"09/bridge/hold/").unwrap().is_empty());
    drop(receiver);
    f.high.store.close().unwrap();
    let reopened = Arc::new(FileBackend::open(&f._temp.path().join("high-store"), 2, 2));
    let files = Arc::new(
        Objects::open(
            FileConfig::new(
                f._temp.path().join("high-files"),
                [20; 32],
                "127.0.0.1:0".parse().unwrap(),
            ),
            reopened.clone(),
        )
        .unwrap(),
    );
    let receiver = Bridge::open(f.receive.clone(), reopened.clone()).unwrap();
    receiver.attach_files(Some(files.clone()));
    f.finish(&receiver);
    let mut out = Vec::new();
    files.read("report", &mut out).unwrap();
    assert_eq!(out, b"abcd");
    let before = files.metadata("report").unwrap();
    drop(receiver);
    reopened.store.close().unwrap();
    let reopened = Arc::new(FileBackend::open(&f._temp.path().join("high-store"), 2, 2));
    let files = Arc::new(
        Objects::open(
            FileConfig::new(
                f._temp.path().join("high-files"),
                [20; 32],
                "127.0.0.1:0".parse().unwrap(),
            ),
            reopened.clone(),
        )
        .unwrap(),
    );
    let receiver = Bridge::open(f.receive.clone(), reopened.clone()).unwrap();
    receiver.attach_files(Some(files.clone()));
    for path in paths {
        f.copy(&path);
    }
    assert_eq!(receiver.process().unwrap(), 0);
    assert_eq!(files.metadata("report").unwrap(), before);
    reopened.store.close().unwrap();
}

#[test]
fn authenticated_bundles_cannot_reuse_a_sequence_with_different_identity() {
    let f = Fixture::new();
    let sender = f.sender();
    let receiver = f.receiver();
    let first = sender.export(b"first").unwrap();
    receiver.stage(&first).unwrap();
    f.low
        .commit(vec![net::Record::put(
            b"09/bridge/export/files/sequence".to_vec(),
            net::encode(&0u64).unwrap(),
        )])
        .unwrap();
    let conflicting = sender.export(b"different").unwrap();
    assert!(matches!(
        receiver.stage(&conflicting),
        Err(net::Error::Auth(_))
    ));
    assert_eq!(f.high.keys(b"09/bridge/inbox/").unwrap().len(), 1);
}
