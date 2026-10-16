use super::*;

fn request(request: Json) -> (i32, Vec<u8>) {
    let input = serde_json::to_vec(&request).unwrap();
    let mut output = Buffer::empty();
    let code = unsafe { murmur_call(0, input.as_ptr(), input.len(), None, &mut output) };
    (code, unsafe { output.take() })
}
#[test]
fn abi_and_defaults() {
    assert_eq!(murmur_abi_version(), 1);
    let (code, bytes) = request(json!({"op":"defaults"}));
    assert_eq!(code, 0);
    let v: Json = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["capture_interval"], 1_000_000_000_u64);
    assert_eq!(v["spool_block_bytes"], 65536);
    assert_eq!(v["scheduler"]["local_share"], 90);
}
#[test]
fn malformed_and_stale_arguments() {
    let mut output = Buffer::empty();
    let code = unsafe { murmur_call(0, std::ptr::null(), 1, None, &mut output) };
    assert_ne!(code, 0);
    let error: Json = serde_json::from_slice(&unsafe { output.take() }).unwrap();
    assert_eq!(error["code"], "Invalid");
    let (code, _) = request(json!({"op":"does_not_exist"}));
    assert_ne!(code, 0);
    let mut output = Buffer::empty();
    let code = unsafe { murmur_sql(999, 1, std::ptr::null(), 0, &mut output) };
    assert_ne!(code, 0);
    let error: Json = serde_json::from_slice(&unsafe { output.take() }).unwrap();
    assert_eq!(error["code"], "Invalid");
}
#[test]
fn panic_is_contained() {
    let mut output = Buffer::empty();
    let code = boundary(&mut output, || panic!("boundary test"));
    assert_ne!(code, 0);
    let error: Json = serde_json::from_slice(&unsafe { output.take() }).unwrap();
    assert_eq!(error["code"], "CallbackPanicked");
}
#[test]
fn codec_preserves_u64() {
    let (code, _) = request(json!({"op":"codec_encode","type":"U64","value":u64::MAX}));
    assert_eq!(code, 0);
}
