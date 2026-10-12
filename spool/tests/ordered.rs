use spool::*;
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
