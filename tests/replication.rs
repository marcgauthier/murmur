#![cfg(feature="replication")]
use sqlite_session::{Database,Options,OpenMode,Migration,Value,Result,replication::{self,Config,Credentials,policies::Policy}};
use rcgen::{CertificateParams,KeyPair,IsCa,BasicConstraints,SanType,ExtendedKeyUsagePurpose};
use std::{collections::BTreeMap,time::{Duration,Instant}};
fn configs()->Vec<Config>{
    let mut params=CertificateParams::new(Vec::<String>::new()).unwrap();params.is_ca=IsCa::Ca(BasicConstraints::Unconstrained);let ca_key=KeyPair::generate().unwrap();let ca=params.self_signed(&ca_key).unwrap();
    let ids=[[1;16],[2;16],[3;16]];let seeds=[[11;32],[22;32],[33;32]];let mut trust=BTreeMap::new();for(id,key)in ids.iter().zip(seeds){trust.insert(*id,ed25519_public(key));}
    ids.iter().zip(seeds).map(|(id,seed)|{let key=KeyPair::generate().unwrap();let mut params=CertificateParams::new(Vec::<String>::new()).unwrap();params.subject_alt_names=vec![SanType::URI(format!("replicateddb://node/{}",replication::hex(id)).try_into().unwrap())];params.extended_key_usages=vec![ExtendedKeyUsagePurpose::ServerAuth,ExtendedKeyUsagePurpose::ClientAuth];let cert=params.signed_by(&key,&ca,&ca_key).unwrap();let credentials=Credentials{chain:vec![cert.der().to_vec()],private_key:key.serialize_der().into(),roots:vec![ca.der().to_vec()]};let mut config=Config::new("127.0.0.1:0".parse().unwrap(),credentials,seed);config.trusted_origins=trust.clone();config}).collect()
}
fn ed25519_public(seed:[u8;32])->[u8;32]{replication::protocol::public_key(&seed)}
fn master_configs()->Vec<Config>{
    let mut c=configs();
    for cfg in &mut c { cfg.schema_master=Some([1;16]); }
    c[0].master=true;
    c
}
fn has_table(db:&Database,name:&str)->bool{let name=name.to_owned();db.read(move|s|Ok(s.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",[name],|r|r.get::<_,i64>(0))?)).unwrap_or(0)>0}
fn has_index(db:&Database,name:&str)->bool{let name=name.to_owned();db.read(move|s|Ok(s.query_row("SELECT count(*) FROM sqlite_schema WHERE type='index' AND name=?",[name],|r|r.get::<_,i64>(0))?)).unwrap_or(0)>0}
fn has_column(db:&Database,table:&str,column:&str)->bool{let (table,column)=(table.to_owned(),column.to_owned());db.read(move|s|{let mut st=s.prepare(&format!("PRAGMA table_info({table})"))?;let names=st.query_map([],|r|r.get::<_,String>(1))?;for n in names{if n?==column{return Ok(true);}}Ok(false)}).unwrap_or(false)}
fn open(root:&std::path::Path,config:Config)->Result<Database>{Database::open(root,[9;32],Options{mode:OpenMode::CreateNew,database_id:Some([7;16]),replication:Some(config),..Options::default()})}
#[track_caller]
fn wait(mut check:impl FnMut()->bool){let end=Instant::now()+Duration::from_secs(25);while !check(){assert!(Instant::now()<end,"replication did not converge");std::thread::sleep(Duration::from_millis(30));}}
fn value(db:&Database)->i64{db.read(|sql|Ok(sql.query_row("SELECT n FROM items WHERE id=1",[],|r|r.get(0))?)).unwrap_or(-999)}
#[test]
fn quic_replication_is_durable_and_preserves_pending_local_write()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=configs();let b=open(&temp.path().join("b"),config.remove(1))?;let a=open(&temp.path().join("a"),config.remove(0))?;
    for db in [&a,&b]{db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;db.flush()?;}
    b.transaction(|s|{s.execute("INSERT INTO items VALUES(1,10)",[])?;Ok(())})?;b.flush()?;
    a.replication().unwrap().add_peer(b.replication().unwrap().status().address.unwrap()).unwrap();wait(||value(&a)==10);
    a.transaction(|s|{s.execute("UPDATE items SET n=99 WHERE id=1",[])?;Ok(())})?;
    b.transaction(|s|{s.execute("UPDATE items SET n=20 WHERE id=1",[])?;Ok(())})?;b.flush()?;
    std::thread::sleep(Duration::from_secs(2));assert_eq!(value(&a),99,"incoming application overwrote an uncollected local write");
    wait(||value(&b)==99);a.flush()?;b.flush()?;let node=b.node_id();let path=temp.path().join("b");a.close()?;b.close()?;
    let recovered=Database::open(path,[9;32],Options{node_id:Some(node),..Options::default()})?;assert_eq!(value(&recovered),99);recovered.close()?;Ok(())
}
#[test]
fn schema_is_adopted_without_a_second_database()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=configs();let a=open(&temp.path().join("a"),config.remove(0))?;let b=open(&temp.path().join("b"),config.remove(0))?;
    a.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;a.transaction(|s|{s.execute("INSERT INTO items VALUES(1,42)",[])?;Ok(())})?;a.flush()?;
    b.replication().unwrap().add_peer(a.replication().unwrap().status().address.unwrap()).unwrap();wait(||value(&b)==42);
    assert_eq!(b.read(|s|Ok(s.query_row("SELECT count(*) FROM sqlite_schema WHERE name LIKE '_crdt_%'",[],|r|r.get::<_,i64>(0))?))?,0);a.close()?;b.close()?;Ok(())
}

#[test]
fn managed_search_indexes_follow_network_rows_and_schema_adoption() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut config = configs();
    let definitions = || {
        #[allow(unused_mut)]
        let mut indexes: Vec<sqlite_session::SearchIndex> = vec![sqlite_session::Fts5Index::new("docs_fts", "docs", vec!["title".into()]).into()];
        #[cfg(feature = "vector")]
        indexes.push(sqlite_session::VectorIndex::new("docs_vec", "docs", "embedding", 2).into());
        indexes
    };
    let mut nodes = Vec::new();
    for (i, cfg) in config.drain(..2).enumerate() {
        nodes.push(Database::open(temp.path().join(format!("search-{i}")), [9; 32], Options {
            mode: OpenMode::CreateNew, database_id: Some([7; 16]), replication: Some(cfg),
            search_indexes: definitions(), capture_interval: Duration::from_millis(30),
            search_interval: Duration::from_millis(10), ..Options::default()
        })?);
    }
    let a = &nodes[0]; let b = &nodes[1];
    a.migrate(Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,embedding BLOB)"))?;
    a.transaction(|sql| {
        let embedding = [1f32, 0.].into_iter().flat_map(f32::to_le_bytes).collect::<Vec<_>>();
        sql.execute("INSERT INTO docs VALUES(1,'replicated',?)", [embedding])?;
        Ok(())
    })?;
    a.flush()?;
    b.replication().unwrap().add_peer(a.replication().unwrap().status().address.unwrap()).unwrap();
    let count = |db: &Database, term: &str| {
        let term = term.to_owned();
        db.read(move |sql| Ok(sql.query_row("SELECT count(*) FROM docs_fts WHERE docs_fts MATCH ?", [term], |r| r.get::<_, i64>(0))?)).unwrap_or(-1)
    };
    wait(|| count(b, "replicated") == 1);
    #[cfg(feature = "vector")]
    wait(|| b.read(|sql| Ok(sql.query_row("SELECT rowid FROM docs_vec WHERE embedding MATCH '[1,0]' AND k=1", [], |r| r.get::<_, i64>(0))?)).unwrap_or(0) == 1);
    b.set_replication("docs", false)?;
    a.transaction(|sql| { sql.execute("UPDATE docs SET title='updated'", [])?; Ok(()) })?;
    a.flush()?;
    b.transaction(|sql| { sql.execute("UPDATE docs SET title='localonly'", [])?; Ok(()) })?;
    b.refresh_indexes()?;
    assert_eq!(count(b, "localonly"), 1);
    b.set_replication("docs", true)?;
    a.transaction(|sql| { sql.execute("DELETE FROM docs", [])?; Ok(()) })?;
    a.flush()?;
    wait(|| count(b, "localonly OR updated OR replicated") == 0);
    #[cfg(feature = "vector")]
    wait(|| b.read(|sql| Ok(sql.query_row("SELECT count(*) FROM docs_vec", [], |r| r.get::<_, i64>(0))?)).unwrap_or(-1) == 0);
    assert_eq!(b.schema()?.tables.len(), 1);
    a.close()?;
    b.close()
}
#[test]
fn counter_operations_converge_after_offline_writes()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=configs();for c in &mut config{c.merge_policies.insert(("items".into(),"n".into()),Policy::Counter);}
    let a=open(&temp.path().join("a"),config.remove(0))?;let b=open(&temp.path().join("b"),config.remove(0))?;
    for db in [&a,&b]{db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;db.flush()?;}
    a.transaction(|s|{s.execute("INSERT INTO items VALUES(1,0)",[])?;Ok(())})?;a.flush()?;a.replication().unwrap().add_peer(b.replication().unwrap().status().address.unwrap()).unwrap();wait(||value(&b)==0);
    a.replication().unwrap().remove_peer(b.node_id()).unwrap();wait(||a.replication().unwrap().status().peers==0);
    a.transaction(|s|s.counter_add("items",vec![Value::Integer(1)],"n",5))?;b.transaction(|s|s.counter_add("items",vec![Value::Integer(1)],"n",7))?;a.flush()?;b.flush()?;
    a.replication().unwrap().add_peer(b.replication().unwrap().status().address.unwrap()).unwrap();let deadline=Instant::now()+Duration::from_secs(25);while value(&a)!=12 || value(&b)!=12 {assert!(Instant::now()<deadline,"counter convergence: a={} b={}, a={:?}, b={:?}",value(&a),value(&b),a.replication().unwrap().status(),b.replication().unwrap().status());std::thread::sleep(Duration::from_millis(30));}
    assert!(a.transaction(|s|{s.execute("UPDATE items SET n=100",[])?;Ok(())}).is_err());a.close()?;b.close()?;Ok(())
}
#[test]
fn altered_origin_transaction_is_rejected(){let mut config=configs();let seed=*config[0].signing_key;let mut tx=replication::protocol::Transaction{database:[7;16],origin:[1;16],sequence:1,id:[5;16],millis:10,logical:0,schema:[0;32],payload_digest:[0;32],payload:vec![1,2,3],signature:Vec::new()};tx.sign(&seed).unwrap();tx.verify([7;16],&config.remove(0).trusted_origins,64<<20).unwrap();tx.payload.push(4);assert!(tx.verify([7;16],&BTreeMap::new(),64<<20).is_err());}
#[test]
fn encrypted_files_are_fetched_by_the_second_endpoint()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=configs();for(index,c)in config.iter_mut().enumerate(){let mut fc=replication::files::Config::new(temp.path().join(format!("files-{index}")),[61;32],"127.0.0.1:0".parse().unwrap());fc.fetch_interval=Duration::from_millis(100);c.files=Some(fc);}
    let a=open(&temp.path().join("a"),config.remove(0))?;let b=open(&temp.path().join("b"),config.remove(0))?;
    let files_a=a.replication().unwrap().files().unwrap();let files_b=b.replication().unwrap().files().unwrap();let payload=vec![b'x';180000];files_a.upload("report",payload.as_slice()).unwrap();
    a.replication().unwrap().add_peer(b.replication().unwrap().status().address.unwrap()).unwrap();let deadline=Instant::now()+Duration::from_secs(15);while !files_b.available("report").unwrap_or(false){assert!(Instant::now()<deadline,"file fetch: metadata={:?} error={:?}, a={:?} b={:?}",files_b.list(),files_b.last_fetch_error(),a.replication().unwrap().status(),b.replication().unwrap().status());std::thread::sleep(Duration::from_millis(30));}let mut output=Vec::new();files_b.read("report",&mut output).unwrap();assert_eq!(output,payload);a.close()?;b.close()?;Ok(())
}
#[test]
fn sets_and_min_max_converge()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=configs();for c in &mut config {c.merge_policies.insert(("items".into(),"tags".into()),Policy::Set);c.merge_policies.insert(("items".into(),"low".into()),Policy::Min);c.merge_policies.insert(("items".into(),"high".into()),Policy::Max);}
    let a=open(&temp.path().join("a"),config.remove(0))?;let b=open(&temp.path().join("b"),config.remove(0))?;
    for db in [&a,&b]{db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL,tags BLOB NOT NULL,low INTEGER NOT NULL,high INTEGER NOT NULL)"))?;db.flush()?;}
    a.transaction(|s|{s.execute("INSERT INTO items VALUES(1,0,x'00',100,0)",[])?;Ok(())})?;a.flush()?;a.replication().unwrap().add_peer(b.replication().unwrap().status().address.unwrap()).unwrap();wait(||value(&b)==0);
    a.transaction(|s|{s.set_add("items",vec![Value::Integer(1)],"tags",Value::Text("a".into()))?;s.min("items",vec![Value::Integer(1)],"low",Value::Integer(5))?;s.max("items",vec![Value::Integer(1)],"high",Value::Integer(8))})?;
    b.transaction(|s|{s.set_add("items",vec![Value::Integer(1)],"tags",Value::Text("b".into()))?;s.min("items",vec![Value::Integer(1)],"low",Value::Integer(9))?;s.max("items",vec![Value::Integer(1)],"high",Value::Integer(12))})?;a.flush()?;b.flush()?;
    fn state(db:&Database)->Option<(Vec<Value>,i64,i64)>{db.read(|s|Ok(s.query_row("SELECT tags,low,high FROM items",[],|r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?)))?)).ok().and_then(|(bytes,low,high)|postcard::from_bytes::<Vec<Value>>(&bytes).ok().map(|tags|(tags,low,high)))}
    wait(||state(&a).is_some_and(|(tags,low,high)|tags.len()==2 && low==5 && high==12) && state(&a)==state(&b));a.close()?;b.close()?;Ok(())
}
#[test]
fn high_low_bridge_retains_low_shadow_and_releases_high_owned_value()->Result<()> {
    let temp=tempfile::tempdir()?;let mut configs=configs();let mut low=configs.remove(0);let mut high=configs.remove(0);let secret=[80;32];let low_domain=[71;16];let high_domain=[72;16];
    let bridge_key=[81;32];let mut signers=BTreeMap::new();signers.insert([1;16],replication::protocol::public_key(&bridge_key));
    low.bridge=Some(replication::bridge::Config::exporter(low_domain,temp.path().join("out"),"low-east".into(),bridge_key,replication::bridge::recipient_public(secret)));
    high.bridge=Some(replication::bridge::Config::receiver(high_domain,temp.path().join("in"),"low-east".into(),secret,std::collections::BTreeSet::from([low_domain]),signers,low.trusted_origins.clone()));
    let open_domain=|name:&str,domain,config|Database::open(temp.path().join(name),[9;32],Options{mode:OpenMode::CreateNew,database_id:Some(domain),replication:Some(config),..Options::default()});
    let low=open_domain("low",low_domain,low)?;let high=open_domain("high",high_domain,high)?;
    for db in [&low,&high]{db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;db.flush()?;}
    fn transfer(source:&std::path::Path,destination:&std::path::Path){for item in std::fs::read_dir(source).unwrap(){let path=item.unwrap().path();if path.extension().is_some_and(|e|e=="bundle"){std::fs::copy(&path,destination.join(path.file_name().unwrap())).unwrap();}}}
    low.transaction(|s|{s.execute("INSERT INTO items VALUES(1,10)",[])?;Ok(())})?;low.flush()?;low.replication().unwrap().bridge().unwrap().capture().unwrap();transfer(&temp.path().join("out"),&temp.path().join("in"));high.replication().unwrap().bridge().unwrap().process().unwrap();wait(||value(&high)==10);
    high.transaction(|s|{s.execute("UPDATE items SET n=99",[])?;Ok(())})?;high.flush()?;
    low.transaction(|s|{s.execute("UPDATE items SET n=20",[])?;Ok(())})?;low.flush()?;low.replication().unwrap().bridge().unwrap().capture().unwrap();transfer(&temp.path().join("out"),&temp.path().join("in"));high.replication().unwrap().bridge().unwrap().process().unwrap();std::thread::sleep(Duration::from_secs(1));assert_eq!(value(&high),99);
    let key=postcard::to_allocvec(&vec![Value::Integer(1)]).unwrap();assert!(high.replication().unwrap().bridge().unwrap().release("items",key,Some("n".into())).unwrap());assert_eq!(value(&high),20);low.close()?;high.close()?;Ok(())
}
#[test]
fn master_schema_replicates_to_divergent_node()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=master_configs();
    let master=open(&temp.path().join("m"),config.remove(0))?;let peer=open(&temp.path().join("p"),config.remove(0))?;
    master.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL); CREATE INDEX items_n ON items(n)"))?;
    master.transaction(|s|{s.execute("INSERT INTO items VALUES(1,42)",[])?;Ok(())})?;master.flush()?;
    peer.replication().unwrap().add_peer(master.replication().unwrap().status().address.unwrap()).unwrap();
    wait(||has_table(&peer,"items"));
    wait(||value(&peer)==42);
    assert!(has_index(&peer,"items_n"));
    master.close()?;peer.close()?;Ok(())
}
#[test]
fn master_schema_adds_column_to_existing_table()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=master_configs();
    let master=open(&temp.path().join("m"),config.remove(0))?;let peer=open(&temp.path().join("p"),config.remove(0))?;
    for db in [&master,&peer]{db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;db.flush()?;}
    master.transaction(|s|{s.execute("INSERT INTO items VALUES(1,7)",[])?;Ok(())})?;master.flush()?;
    peer.replication().unwrap().add_peer(master.replication().unwrap().status().address.unwrap()).unwrap();
    wait(||value(&peer)==7);
    master.migrate(Migration::new("ALTER TABLE items ADD COLUMN m INTEGER"))?;master.flush()?;
    wait(||has_column(&peer,"items","m"));
    master.transaction(|s|{s.execute("UPDATE items SET m=9 WHERE id=1",[])?;Ok(())})?;master.flush()?;
    wait(||peer.read(|s|Ok(s.query_row("SELECT m FROM items WHERE id=1",[],|r|r.get::<_,i64>(0))?)).unwrap_or(-1)==9);
    assert_eq!(value(&peer),7);
    master.close()?;peer.close()?;Ok(())
}
#[test]
fn non_master_schema_change_does_not_propagate()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=master_configs();
    let master=open(&temp.path().join("m"),config.remove(0))?;let peer=open(&temp.path().join("p"),config.remove(0))?;
    for db in [&master,&peer]{db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;db.flush()?;}
    master.transaction(|s|{s.execute("INSERT INTO items VALUES(1,1)",[])?;Ok(())})?;master.flush()?;
    peer.replication().unwrap().add_peer(master.replication().unwrap().status().address.unwrap()).unwrap();
    wait(||value(&peer)==1);
    // Replicated DDL must originate on the master; the peer's table stays local
    // to the peer and its own writes stop applying upstream (loud mismatch).
    peer.migrate(Migration::new("CREATE TABLE peer_only(id INTEGER PRIMARY KEY)"))?;peer.flush()?;
    std::thread::sleep(Duration::from_secs(3));
    assert!(!has_table(&master,"peer_only"));
    // Master-to-peer flow stays healthy.
    master.transaction(|s|{s.execute("UPDATE items SET n=2 WHERE id=1",[])?;Ok(())})?;master.flush()?;
    wait(||value(&peer)==2);
    master.close()?;peer.close()?;Ok(())
}
#[test]
fn master_local_tables_are_excluded_from_schema_sync()->Result<()> {
    let temp=tempfile::tempdir()?;let mut config=master_configs();
    let master=open(&temp.path().join("m"),config.remove(0))?;let peer=open(&temp.path().join("p"),config.remove(0))?;
    master.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;
    master.migrate(Migration::new("CREATE TABLE scratch(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;
    master.set_replication("scratch",false)?;
    master.transaction(|s|{s.execute("INSERT INTO items VALUES(1,5)",[])?;s.execute("INSERT INTO scratch VALUES(1,6)",[])?;Ok(())})?;master.flush()?;
    // The peer's own local table must survive master sync untouched.
    peer.migrate(Migration::new("CREATE TABLE local_note(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;
    peer.set_replication("local_note",false)?;
    peer.transaction(|s|{s.execute("INSERT INTO local_note VALUES(1,8)",[])?;Ok(())})?;peer.flush()?;
    peer.replication().unwrap().add_peer(master.replication().unwrap().status().address.unwrap()).unwrap();
    wait(||value(&peer)==5);
    assert!(!has_table(&peer,"scratch"));
    assert!(has_table(&peer,"items"));
    assert_eq!(peer.read(|s|Ok(s.query_row("SELECT n FROM local_note WHERE id=1",[],|r|r.get::<_,i64>(0))?))?,8);
    master.close()?;peer.close()?;Ok(())
}

#[test]
fn retention_collection_preserves_tombstones_and_peer_retirement_across_reopen() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let mut cfg = configs().remove(0);
    cfg.membership = false;
    cfg.retention = Duration::ZERO;
    cfg.minimum_retained = 2;
    cfg.retention_interval = Duration::from_secs(3600);
    let db = open(&path, cfg.clone())?;
    db.migrate(Migration::new(
        "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)",
    ))?;
    db.transaction(|s| {
        s.execute("INSERT INTO items VALUES(1,10)", [])?;
        Ok(())
    })?;
    db.flush()?;
    let stale = db.export_state()?;
    db.transaction(|s| {
        s.execute("DELETE FROM items WHERE id=1", [])?;
        Ok(())
    })?;
    db.flush()?;
    for id in 2..9 {
        db.transaction(move |s| {
            s.execute("INSERT INTO items VALUES(?,?)", [id, id])?;
            Ok(())
        })?;
        db.flush()?;
    }
    db.retire_peer([2; 16])?;
    assert!(db.collect_retention()? > 0);
    let expected = db.export_state()?;
    assert!(!expected.tombstones.is_empty());
    let stats = db.retention_status()?;
    assert!(stats.collected_transactions > 0);
    db.close()?;
    let db = Database::open(
        &path,
        [9; 32],
        Options {
            replication: Some(cfg),
            ..Options::default()
        },
    )?;
    assert_eq!(db.export_state()?, expected);
    assert!(
        db.retention_status()?
            .peers
            .iter()
            .any(|p| p.node == [2; 16] && p.retired)
    );
    db.merge_state(stale)?;
    assert_eq!(value(&db), -999);
    assert_eq!(db.export_state()?.tombstones, expected.tombstones);
    db.close()
}

#[test]
fn peer_fetch_recovers_partial_staging_without_identity_sidecar() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut cfg = configs();
    for (i, c) in cfg.iter_mut().enumerate() {
        let mut fc = replication::files::Config::new(
            tmp.path().join(format!("files-{i}")),
            [61; 32],
            "127.0.0.1:0".parse().unwrap(),
        );
        fc.fetch_interval = Duration::from_millis(50);
        c.files = Some(fc);
        c.membership = false;
    }
    let a = open(&tmp.path().join("a"), cfg.remove(0))?;
    let b = open(&tmp.path().join("b"), cfg.remove(0))?;
    let payload = vec![3; 180000];
    let meta = a.upload_file("resume", payload.as_slice())?;
    let source = a.replication().unwrap().files().unwrap();
    let container = std::fs::read(source.path(meta.digest, meta.generation))?;
    let staging = tmp.path().join("files-1/staging");
    std::fs::create_dir_all(&staging)?;
    let path = staging.join(format!(
        "{}-{}.g{}.part",
        replication::hex(&a.node_id()),
        replication::hex(&meta.digest),
        meta.generation
    ));
    std::fs::write(&path, &container[..100])?;
    a.replication()
        .unwrap()
        .add_peer(b.replication().unwrap().status().address.unwrap())
        .unwrap();
    wait(|| b.file_available("resume").unwrap_or(false));
    let mut out = Vec::new();
    b.get_file("resume", &mut out)?;
    assert_eq!(out, payload);
    a.close()?;
    b.close()
}

#[test]
fn seeded_counter_and_set_edits_converge_after_three_node_partition_and_restart() -> Result<()> {
    use rand::{Rng, SeedableRng};
    use std::collections::BTreeSet;
    let tmp = tempfile::tempdir()?;
    let mut cfg = configs();
    for c in &mut cfg {
        c.membership = false;
        c.merge_policies
            .insert(("items".into(), "n".into()), Policy::Counter);
        c.merge_policies
            .insert(("items".into(), "tags".into()), Policy::Set);
    }
    let mut dbs = Vec::new();
    for (i, c) in cfg.iter().enumerate() {
        let db = open(&tmp.path().join(i.to_string()), c.clone())?;
        db.migrate(Migration::new(
            "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL,tags BLOB NOT NULL)",
        ))?;
        db.flush()?;
        dbs.push(db);
    }
    dbs[0].transaction(|s| {
        s.execute("INSERT INTO items VALUES(1,0,x'00')", [])?;
        Ok(())
    })?;
    let baseline = dbs[0].export_state()?;
    for db in &dbs[1..] {
        db.merge_state(baseline.clone())?;
    }
    let mut rng = rand::rngs::StdRng::seed_from_u64(73);
    let mut total = 0i64;
    let mut expected = BTreeSet::new();
    for (node, db) in dbs.iter().enumerate() {
        for i in 0..20 {
            let delta = rng.gen_range(-9..=9);
            total += delta;
            let tag = format!("{node}-{i}");
            let remove = rng.gen_bool(0.4);
            if !remove {
                expected.insert(tag.clone());
            }
            db.transaction(move |s| {
                s.counter_add("items", vec![Value::Integer(1)], "n", delta)?;
                s.set_add(
                    "items",
                    vec![Value::Integer(1)],
                    "tags",
                    Value::Text(tag.clone()),
                )?;
                if remove {
                    s.set_remove("items", vec![Value::Integer(1)], "tags", Value::Text(tag))?;
                }
                Ok(())
            })?;
        }
        db.flush()?;
    }
    // Restart one partition before reconnecting; authoritative policy records
    // must survive, including locally removed set members.
    dbs[2].close()?;
    dbs[2] = Database::open(
        tmp.path().join("2"),
        [9; 32],
        Options {
            replication: Some(cfg[2].clone()),
            ..Options::default()
        },
    )?;
    for (a, b) in [(0, 1), (1, 2), (2, 0)] {
        dbs[a]
            .replication()
            .unwrap()
            .add_peer(dbs[b].replication().unwrap().status().address.unwrap())
            .unwrap();
    }
    let read = |db: &Database| -> Option<(i64, BTreeSet<String>)> {
        db.read(|s| {
            Ok(s.query_row("SELECT n,tags FROM items WHERE id=1", [], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
            })?)
        })
        .ok()
        .and_then(|(n, bytes)| {
            postcard::from_bytes::<Vec<Value>>(&bytes).ok().map(|v| {
                (
                    n,
                    v.into_iter()
                        .map(|x| match x {
                            Value::Text(s) => s,
                            _ => panic!("unexpected set value"),
                        })
                        .collect(),
                )
            })
        })
    };
    wait(|| {
        dbs.iter()
            .all(|db| read(db) == Some((total, expected.clone())))
    });
    for db in &dbs {
        db.flush()?;
        db.close()?;
    }
    for i in 0..3 {
        let db = Database::open(
            tmp.path().join(i.to_string()),
            [9; 32],
            Options {
                replication: Some(cfg[i].clone()),
                ..Options::default()
            },
        )?;
        assert_eq!(read(&db), Some((total, expected.clone())));
        db.close()?;
    }
    Ok(())
}

#[test]
fn disabled_tables_filter_rows_tombstones_and_counter_records_in_both_directions() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut cfg = configs();
    for c in &mut cfg {
        c.membership = false;
        c.merge_policies
            .insert(("items".into(), "n".into()), Policy::Counter);
    }
    let config_a = cfg[0].clone();
    let a = open(&tmp.path().join("a"), cfg.remove(0))?;
    let b = open(&tmp.path().join("b"), cfg.remove(0))?;
    for db in [&a, &b] {
        db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL);CREATE TABLE barrier(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)"))?;
        db.flush()?;
    }
    a.transaction(|s| {
        s.execute_batch(
            "INSERT INTO items VALUES(1,0),(2,0),(3,0);INSERT INTO barrier VALUES(1,0)",
        )?;
        Ok(())
    })?;
    b.merge_state(a.export_state()?)?;
    a.set_replication("items", false)?;
    a.transaction(|s| {
        s.counter_add("items", vec![Value::Integer(1)], "n", 17)?;
        s.execute("DELETE FROM items WHERE id=2", [])?;
        s.execute("UPDATE barrier SET n=10", [])?;
        Ok(())
    })?;
    a.flush()?;
    a.replication()
        .unwrap()
        .add_peer(b.replication().unwrap().status().address.unwrap())
        .unwrap();
    fn barrier_value(db: &Database) -> i64 {
        db.read(|s| Ok(s.query_row("SELECT n FROM barrier WHERE id=1", [], |r| r.get(0))?))
            .unwrap_or(-1)
    }
    wait(|| barrier_value(&b) == 10);
    assert_eq!(value(&b), 0);
    assert_eq!(
        b.read(|s| Ok(
            s.query_row("SELECT count(*) FROM items WHERE id=2", [], |r| r
                .get::<_, i64>(0))?
        ))?,
        1
    );
    b.transaction(|s| {
        s.counter_add("items", vec![Value::Integer(1)], "n", 3)?;
        s.execute("DELETE FROM items WHERE id=3", [])?;
        s.execute("UPDATE barrier SET n=20", [])?;
        Ok(())
    })?;
    b.flush()?;
    wait(|| barrier_value(&a) == 20);
    assert_eq!(value(&a), 17);
    assert_eq!(
        a.read(|s| Ok(
            s.query_row("SELECT count(*) FROM items WHERE id=3", [], |r| r
                .get::<_, i64>(0))?
        ))?,
        1
    );
    assert_eq!(value(&b), 3);
    a.close()?;
    b.close()?;
    let recovered = Database::open(
        tmp.path().join("a"),
        [9; 32],
        Options {
            replication: Some(config_a),
            ..Options::default()
        },
    )?;
    assert!(!recovered.replication_enabled("items")?);
    assert_eq!(value(&recovered), 17);
    recovered.close()
}

#[test]
fn snapshot_catches_up_pruned_history_with_concurrent_counters_sets_and_tombstones() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut cfg = configs();
    for c in &mut cfg {
        c.minimum_retained = 2;
        c.retention = Duration::ZERO;
        c.incoming_interval = Duration::from_millis(50);
        c.anti_entropy_interval = Duration::from_millis(100);
        c.trusted_snapshot_sources.insert([1;16]);
        c.merge_policies.insert(("items".into(), "n".into()), Policy::Counter);
        c.merge_policies.insert(("items".into(), "tags".into()), Policy::Set);
    }
    let b_cfg = cfg[1].clone();
    let a = open(&tmp.path().join("a"), cfg.remove(0))?;
    let b = open(&tmp.path().join("b"), cfg.remove(0))?;
    for db in [&a,&b] {
        db.migrate(Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL,tags BLOB NOT NULL,payload BLOB)"))?;
        db.flush()?;
    }
    a.transaction(|s| {s.execute("INSERT INTO items VALUES(1,0,x'00',zeroblob(180000)),(2,0,x'00',x'')",[])?;Ok(())})?;
    a.flush()?;
    b.merge_state(a.export_state()?)?;
    b.flush()?;
    b.transaction(|s| {
        s.counter_add("items",vec![Value::Integer(1)],"n",3)?;
        s.set_add("items",vec![Value::Integer(1)],"tags",Value::Text("offline".into()))
    })?;
    b.flush()?;
    for i in 0..8 {
        a.transaction(move |s| {
            s.execute("DELETE FROM items WHERE id=2",[])?;
            s.counter_add("items",vec![Value::Integer(1)],"n",2)?;
            s.set_add("items",vec![Value::Integer(1)],"tags",Value::Text(format!("tag-{i}")))
        })?;
        a.flush()?;
    }
    assert!(a.replication().unwrap().collect_retention().unwrap()>0);
    a.replication().unwrap().add_peer(b.replication().unwrap().status().address.unwrap()).unwrap();
    fn state(db:&Database)->Option<(i64,usize,i64)> {
        db.read(|s| {
            let (n,tags):(i64,Vec<u8>)=s.query_row("SELECT n,tags FROM items WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
            let count:i64=s.query_row("SELECT count(*) FROM items WHERE id=2",[],|r|r.get(0))?;
            Ok((n,postcard::from_bytes::<Vec<Value>>(&tags).unwrap().len(),count))
        }).ok()
    }
    wait(||state(&a)==Some((19,9,0)) && state(&b)==Some((19,9,0)));
    let expected=b.export_state()?;
    b.close()?;
    let b=Database::open(tmp.path().join("b"),[9;32],Options{replication:Some(b_cfg),..Options::default()})?;
    assert_eq!(state(&b),Some((19,9,0)));
    assert_eq!(b.export_state()?,expected);
    a.close()?;b.close()?;Ok(())
}
