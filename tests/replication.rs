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
