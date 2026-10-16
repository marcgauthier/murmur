use spool::{Durability, Mutation};
#[path = "common/mod.rs"]
mod common;

#[test]
fn namespace_hints_survive_compaction_deletion_and_reload() {
    let temp = tempfile::tempdir().unwrap();
    let options = common::compact_options(temp.path());
    let store = spool::open(&options).unwrap();
    for i in 0..80 {
        store.put(format!("a/b/heads/{i:04}").as_bytes(), &common::incompressible(300,i)).unwrap();
    }
    store.flush().unwrap();
    for i in 0..70 {
        store.delete(format!("a/b/heads/{i:04}").as_bytes()).unwrap();
    }
    store.flush().unwrap();
    let expected: Vec<_> = (70..80).map(|i|format!("a/b/heads/{i:04}").into_bytes()).collect();
    assert_eq!(store.keys_with_prefix(b"a/b/heads/"),expected);
    for _ in 0..200 {
        store.reclaim().unwrap();
        if store.stats().compactions > 0 { break; }
    }
    assert!(store.stats().compactions > 0);
    assert_eq!(store.keys_with_prefix(b"a/b/heads/"),expected);
    store.close().unwrap();
    let reopened = spool::open(&options).unwrap();
    assert_eq!(reopened.keys_with_prefix(b"a/b/heads/"),expected);
    assert_eq!(reopened.keys_page(b"a/b/heads/",Some(b"a/b/heads/0074"),2),expected[5..7]);
    reopened.put(b"a/b/heads/0001",b"resurrected").unwrap();
    reopened.flush().unwrap();
    assert_eq!(reopened.keys_page(b"a/b/heads/",None,1),vec![b"a/b/heads/0001".to_vec()]);
    assert!(reopened.keys_with_prefix(b"a/b/missing/").is_empty());
    reopened.close().unwrap();
}
#[test]
fn paged_keys_are_sorted_exclusive_and_cross_shard() {
    let temp = tempfile::tempdir().unwrap();
    let mut options = spool::default_options(temp.path().to_string_lossy());
    options.master_key = vec![9; 32];
    let store = spool::open(&options).unwrap();
    let mut mutations = Vec::new();
    for i in 0..1000 {
        mutations.push(Mutation::put(format!("a/{i:04}"), vec![1]));
        mutations.push(Mutation::put(format!("b/{i:04}"), vec![2]));
    }
    store.commit(&mutations, Durability::Sync).unwrap();
    let expected = store.keys_with_prefix(b"a/");
    let mut actual = Vec::new();
    let mut cursor = None;
    loop {
        let page = store.keys_page(b"a/", cursor.as_deref(), 17);
        assert!(page.len() <= 17);
        if page.is_empty() {
            break;
        }
        cursor = page.last().cloned();
        actual.extend(page);
    }
    assert_eq!(actual, expected);
    assert!(store.keys_page(b"a/", None, 0).is_empty());
    assert_eq!(
        store.keys_page(b"a/", Some(b"a/0019"), 1),
        vec![b"a/0020".to_vec()]
    );
    assert_eq!(
        store.keys_page(b"a/", Some(b"0"), 1),
        vec![b"a/0000".to_vec()]
    );
    store.close().unwrap();
}
