use spool::*;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

struct CountingCodec(AtomicUsize);
impl Codec for CountingCodec {
    fn id(&self) -> u8 { 128 }
    fn name(&self) -> &str { "counting-passthrough" }
    fn compress(&self, bytes: &[u8]) -> Result<Vec<u8>> { Ok(bytes.to_vec()) }
    fn decompress(&self, bytes: &[u8], max: usize) -> Result<Vec<u8>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        assert!(bytes.len() <= max);
        Ok(bytes.to_vec())
    }
}

#[test]
fn ordered_loading_reuses_interleaved_blocks_and_leaves_hot_reads_cached() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let codec = Arc::new(CountingCodec(AtomicUsize::new(0)));
    let mut opts = default_options(tmp.path().to_string_lossy());
    opts.master_key = vec![7;32];
    opts.codec = Some(codec.clone());
    opts.reclaim_interval = None;
    opts.target_block_bytes = 2048;
    let store = open(&opts)?;
    // Key sorting repeatedly returns to each block instead of disk order.
    let changes: Vec<_> = (0..256u32).map(|n| {
        let key = (n * 73) % 256;
        Mutation::put(format!("row/{key:04}").into_bytes(), key.to_le_bytes().repeat(24))
    }).collect();
    store.commit(&changes, Durability::Sync)?;
    store.close()?;
    codec.0.store(0, Ordering::Relaxed);
    let mut loaded = Vec::new();
    let store = open_and_load_ordered(&opts, &mut |records| { loaded.extend_from_slice(records); Ok(()) })?;
    assert_eq!(loaded.len(), 256);
    assert!(loaded.windows(2).all(|rows| rows[0].key < rows[1].key));
    for (n, record) in loaded.iter().enumerate() {
        assert_eq!(record.value, (n as u32).to_le_bytes().repeat(24));
    }
    let scans = codec.0.load(Ordering::Relaxed);
    assert!(scans < 128, "ordered scan decompressed {scans} blocks for 256 small records");
    for record in loaded { assert_eq!(store.get(&record.key)?, Some(record.value)); }
    assert_eq!(codec.0.load(Ordering::Relaxed), scans, "open discarded its hot block cache");
    store.close()
}
#[test]
fn ordered_winners_survive_compaction_and_wrong_key_is_read_only()->Result<()> {
    let tmp=tempfile::tempdir().unwrap();let mut opts=default_options(tmp.path().to_string_lossy());opts.master_key=vec![7;32];opts.reclaim_interval=None;opts.max_segment_size=4096;opts.target_block_bytes=512;opts.max_block_bytes=1024;opts.max_key_size=128;opts.max_value_size=768;
    let store=open(&opts)?;
    for n in (0..30).rev(){store.commit(&[Mutation::put(format!("delta/{n:08}").into_bytes(),vec![n as u8;512])],Durability::Sync)?;}
    store.commit(&[Mutation::put(b"delta/00000005".to_vec(),b"latest".to_vec())],Durability::Sync)?;
    store.reclaim()?;store.close()?;
    let manifest=std::fs::read(tmp.path().join("manifest")).unwrap();let keys=std::fs::read(tmp.path().join("keys.enc")).unwrap();
    let mut bad=opts.clone();bad.master_key=vec![8;32];assert!(open(&bad).is_err());
    assert_eq!(manifest,std::fs::read(tmp.path().join("manifest")).unwrap());assert_eq!(keys,std::fs::read(tmp.path().join("keys.enc")).unwrap());
    let mut loaded=Vec::new();let store=open_and_load_ordered(&opts,&mut |r|{loaded.extend_from_slice(r);Ok(())})?;
    assert_eq!(loaded.len(),30);assert!(loaded.windows(2).all(|r|r[0].key<r[1].key));assert_eq!(loaded[5].value,b"latest");store.close()
}
