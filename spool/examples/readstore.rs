//! Read a spool store offline and print records sorted.
//!
//! Usage: readstore <dir>  (master key is 32 x 0x42, the compat key)
fn main() {
    let dir = std::env::args().nth(1).expect("usage: readstore <dir>");
    let key = vec![0x42u8; 32];
    let mut got: Vec<(String, String)> = Vec::new();
    let mut tombs: Vec<String> = Vec::new();
    spool::load(&dir, &key, &mut |recs| {
        for r in recs {
            if r.deleted {
                tombs.push(String::from_utf8_lossy(&r.key).into_owned());
            } else {
                got.push((
                    String::from_utf8_lossy(&r.key).into_owned(),
                    String::from_utf8_lossy(&r.value).into_owned(),
                ));
            }
        }
        Ok(())
    })
    .expect("load");
    got.sort();
    tombs.sort();
    for (k, v) in &got {
        println!("KV {k:?}={v:?}");
    }
    for k in &tombs {
        println!("TOMB {k:?}");
    }
}
