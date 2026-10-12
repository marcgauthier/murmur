//! Concurrency and backpressure (port of `spool_concurrent_test.go`).

#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use common::{load_all, test_options};
use spool::{open, ErrorKind};

#[test]
fn concurrent_writers() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let mut o = test_options(&dir);
    o.flush.max_delay = Duration::from_millis(5); // exercise background flushes
    o.reclaim_interval = Some(Duration::from_millis(50));
    let st = open(&o).unwrap();
    const WRITERS: usize = 32;
    const PER_WRITER: usize = 300;
    std::thread::scope(|s| {
        for w in 0..WRITERS {
            let st = st.clone();
            s.spawn(move || {
                for i in 0..PER_WRITER {
                    let k = format!("w{w}/k{}", i % 50);
                    if i % 9 == 8 {
                        st.delete(k.as_bytes()).unwrap();
                        continue;
                    }
                    st.put(k.as_bytes(), format!("w{w}-v{i}").as_bytes())
                        .unwrap();
                }
            });
        }
    });
    st.close().unwrap();
    let got = load_all(&dir);
    // Every surviving value must be one this writer wrote; keys are
    // writer-private so membership is exact.
    for w in 0..WRITERS {
        for i in 0..50 {
            let k = format!("w{w}/k{i}");
            let Some(v) = got.get(k.as_bytes()) else {
                continue;
            }; // deleted last, fine
            let v = String::from_utf8(v.clone()).unwrap();
            let (ww, vv) = v
                .strip_prefix('w')
                .and_then(|s| s.split_once("-v"))
                .unwrap();
            let (ww, vv): (usize, usize) = (ww.parse().unwrap(), vv.parse().unwrap());
            assert_eq!(ww, w, "key {k} holds foreign value {v:?}");
            assert!(
                vv % 50 == i && vv < PER_WRITER,
                "key {k} holds inconsistent {v:?}"
            );
        }
    }
    assert!(!got.is_empty(), "nothing survived");
}

#[test]
fn backpressure() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let mut o = test_options(&dir);
    o.max_pending_bytes = 64 << 10;
    o.flush.max_records = -1;
    o.flush.max_bytes = -1;
    // max_delay already huge via test_options: background never fires.
    let st = open(&o).unwrap();
    let val = vec![0u8; 8 << 10];
    let mut refused = false;
    for i in 0..1000 {
        match st.try_put(format!("k{i}").as_bytes(), &val) {
            Err(e) if e.kind() == ErrorKind::Backpressure => {
                refused = true;
                break;
            }
            Err(e) => panic!("TryPut: {e}"),
            Ok(()) => {}
        }
    }
    assert!(refused, "TryPut never hit backpressure");
    // A blocking Put waits; a concurrent Flush releases it.
    let st2 = st.clone();
    let val2 = val.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || tx.send(st2.put(b"blocked", &val2)).unwrap());
    assert!(
        rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "Put returned without flush"
    );
    st.flush().unwrap();
    rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
    // Drained: TryPut works again.
    st.try_put(b"after", b"x").unwrap();
    st.close().unwrap();
}
