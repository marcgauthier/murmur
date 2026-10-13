//! Spool-backed adapter. Network workers never own a SQLite connection.
use crate::{*,database::{Inner,Core},model::*,storage::{Opened,Work,Event},metadata,merge,schema};
use murmur_replication::{self as net, Backend,Record};
use std::sync::{Arc,Weak,mpsc};
#[cfg(test)]
#[path = "retention_tests.rs"]
mod retention_tests;
#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod snapshot_tests;
fn error(e:Error)->net::Error {net::Error::Storage(e.to_string())}
pub(crate) struct Adapter {inner:Weak<Inner>,worker:mpsc::Sender<Work>,identity:([u8;16],[u8;16])}
impl Adapter {
    pub(crate) fn database(&self)->net::Result<Database>{Ok(Database{inner:self.inner.upgrade().ok_or_else(||net::Error::Unavailable("database closed".into()))?})}
    pub(crate) fn spool<T:Send+'static>(&self,f:impl FnOnce(&mut Opened)->Result<T>+Send+'static)->net::Result<T>{let(tx,rx)=mpsc::channel();self.worker.send(Work::Network(Box::new(move|o|{let _=tx.send(f(o));}))).map_err(|_|net::Error::Unavailable("storage worker stopped".into()))?;rx.recv().map_err(|_|net::Error::Unavailable("storage reply lost".into()))?.map_err(error)}
}
impl Backend for Adapter {
    fn identity(&self)->([u8;16],[u8;16]){self.identity}
    fn get(&self,key:&[u8])->net::Result<Option<Vec<u8>>>{let key=key.to_vec();self.spool(move|o|o.store.get(&key).map_err(Error::from))}
    fn keys(&self,prefix:&[u8])->net::Result<Vec<Vec<u8>>>{let prefix=prefix.to_vec();self.spool(move|o|Ok(o.store.keys_with_prefix(&prefix)))}
    fn commit(&self,records:Vec<Record>)->net::Result<()> {self.spool(move|o|{let records=records.into_iter().map(|r|match r.value{Some(v)=>spool::Mutation::put(r.key,v),None=>spool::Mutation::delete(r.key)}).collect::<Vec<_>>();o.store.commit(&records,spool::Durability::Sync)?;Ok(())})}
    fn import_bridge(&self,header:net::bridge::Header,payload:Vec<u8>)->net::Result<bool>{crate::high_low::import(self,header,payload)}
    fn release_ownership(&self,table:String,key:Vec<u8>,column:Option<String>)->net::Result<bool>{crate::high_low::release(self,table,key,column)}
    fn publish(&self,records:Vec<Record>)->net::Result<()> {
        self.spool(move |o| {
            let config = o.replication_config.clone().ok_or_else(||Error::Invalid("replication disabled".into()))?;
            validate_auxiliary(&records)?;
            let mut changes:Vec<_> = records.iter().map(|r|match &r.value {Some(v)=>spool::Mutation::put(r.key.clone(),v.clone()),None=>spool::Mutation::delete(r.key.clone())}).collect();
            let clock=o.clock.next(o.identity.node)?;
            changes.extend(sign_content(o,&config,net::Content {sqlite:None,records,origin:o.identity.node},clock,&o.schema)?);
            changes.push(spool::Mutation::put(b"06/clock".to_vec(),encode(&clock)?));
            o.store.commit(&changes,spool::Durability::Sync)?;o.clock=clock;Ok(())
        })
    }
    fn snapshot(&self)->net::Result<Vec<u8>> { self.spool(snapshot_content) }
    fn snapshot_with_progress(&self)->net::Result<(Vec<u8>,Vec<net::protocol::Progress>)> {
        self.spool(|o| Ok((snapshot_content(o)?, durable_progress(o)?)))
    }
    fn create_snapshot(
        &self,
        recipient: net::NodeId,
        config: &net::Config,
    ) -> net::Result<net::snapshot::Manifest> {
        let config = config.clone();
        self.spool(move |o| {
            use net::snapshot::*;
            let now = net::retention::now_millis();
            let schema = sha_schema(&o.schema)?;
            for key in o
                .store
                .keys_with_prefix(OUT)
                .into_iter()
                .filter(|k| k.ends_with(b"/header"))
            {
                if let Some(bytes) = o.store.get(&key)? {
                    let m: Manifest =
                        net::decode(&bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
                    let pin = o
                        .store
                        .get(&pin_key(m.transfer))?
                        .map(|b| net::decode::<Pin>(&b))
                        .transpose()
                        .map_err(|e| Error::Corrupt(e.to_string()))?;
                    if m.recipient == recipient
                        && m.expires > now
                        && m.schema == schema
                        && pin.is_some_and(|p| p.target.is_none())
                    {
                        return Ok(m);
                    }
                }
            }
            if o.store.keys_with_prefix(PINS).len() >= config.staging_transfers {
                return Err(Error::Limit("snapshot transfer count".into()));
            }
            let data = snapshot_content(o)?;
            let mut used = data.len();
            for key in o
                .store
                .keys_with_prefix(OUT)
                .into_iter()
                .filter(|k| !k.ends_with(b"/header"))
            {
                used = used
                    .checked_add(o.store.get(&key)?.map_or(0, |b| b.len()))
                    .ok_or(Error::Limit("snapshot staging bytes".into()))?;
                if used > config.snapshot_staging_bytes {
                    return Err(Error::Limit("snapshot staging bytes".into()));
                }
            }
            let (manifest, records) = build(
                (o.identity.database, o.identity.node),
                recipient,
                data,
                durable_progress(o)?,
                schema,
                &config,
                now,
            )
            .map_err(|e| Error::Invalid(e.to_string()))?;
            let changes: Vec<_> = records
                .into_iter()
                .map(|r| match r.value {
                    Some(v) => spool::Mutation::put(r.key, v),
                    None => spool::Mutation::delete(r.key),
                })
                .collect();
            o.store.commit(&changes, spool::Durability::Sync)?;
            Ok(manifest)
        })
    }
    fn acknowledge_snapshot(
        &self,
        recipient: net::NodeId,
        transfer: net::NodeId,
        digest: [u8; 32],
    ) -> net::Result<()> {
        self.spool(move |o| {
            use net::snapshot::*;
            let key = pin_key(transfer);
            let Some(bytes) = o.store.get(&key)? else {
                return Ok(());
            };
            let mut pin: Pin = net::decode(&bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
            if pin.recipient != recipient || pin.digest != digest {
                return Err(Error::Invalid("snapshot acknowledgement identity".into()));
            }
            if pin.expires <= net::retention::now_millis() {
                return Err(Error::Invalid("snapshot acknowledgement expired".into()));
            }
            if pin.target.is_none() {
                pin.target = Some(
                    durable_progress(o)?
                        .into_iter()
                        .map(|p| (p.origin, p.applied))
                        .collect(),
                );
                o.store.commit(
                    &[spool::Mutation::put(
                        key,
                        net::encode(&pin).map_err(|e| Error::Invalid(e.to_string()))?,
                    )],
                    spool::Durability::Sync,
                )?;
            }
            Ok(())
        })
    }
    fn retention_checkpoint(&self)->net::Result<()> {
        self.spool(|o| {
            crate::storage::snapshot(o)?;
            let coverage:std::collections::BTreeMap<_,_> = durable_progress(o)?.into_iter().map(|p|(p.origin,p.applied)).collect();
            o.store.commit(&[spool::Mutation::put(net::retention::COVERAGE_KEY.to_vec(),encode(&(o.sequence,coverage))?)],spool::Durability::Sync)?;
            Ok(())
        })
    }
    fn prune_history(&self,origin:[u8;16],through:u64)->net::Result<u64> {
        let database = self.database()?;
        let ticket = database.inner.scheduler.admit(crate::scheduler::WriterClass::Maintenance).map_err(error)?;
        let res = self.spool(move|o| {
            use net::{protocol::*,retention::*};
            let old=o.store.get(&floor_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
            if through<=old {return Ok(0);}
            // Recheck transfer obligations on this same serialized worker that
            // publishes captures, so a newly created pin cannot be missed.
            for key in o.store.keys_with_prefix(net::snapshot::PINS) {
                if let Some(bytes)=o.store.get(&key)? {
                    let pin:net::snapshot::Pin=net::decode(&bytes).map_err(|e|Error::Corrupt(e.to_string()))?;
                    if pin.expires>net::retention::now_millis() && through>pin.boundary.get(&origin).copied().unwrap_or(0) {
                        return Err(Error::Busy);
                    }
                }
            }

            let bytes=o.store.get(COVERAGE_KEY)?.ok_or_else(||Error::Invalid("retention snapshot missing".into()))?;
            let (sequence,coverage):(u64,std::collections::BTreeMap<[u8;16],u64>)=decode(&bytes)?;
            if sequence>o.sequence || coverage.get(&origin).copied().unwrap_or(0)<through {
                return Err(Error::Invalid("history not covered by durable snapshot".into()));
            }
            let mut head:Progress=net::decode(&o.store.get(&retained_key(origin))?.ok_or_else(||Error::Corrupt("retention head missing".into()))?).map_err(|e|Error::Corrupt(e.to_string()))?;
            let applied=o.store.get(&progress_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
            if through>applied || through>=head.retained_through {return Err(Error::Invalid("retention would remove current history".into()));}
            let keys=o.store.keys_with_prefix(format!("07/replication/log/{}/",net::hex(&origin)).as_bytes());
            let remaining=keys.iter().map(|key|key_sequence(key)).collect::<Result<Vec<_>>>()?.into_iter().filter(|seq|*seq>through).count();
            let minimum=o.replication_config.as_ref().ok_or_else(||Error::Invalid("replication disabled".into()))?.minimum_retained;
            if (remaining as u64)<minimum {return Err(Error::Invalid("retention minimum would be violated".into()));}
            let mut changes=Vec::new();let mut count=0u64;
            for key in keys {
                let seq=key_sequence(&key)?;if seq>through {break;}
                if count>=256 {return Err(Error::Limit("retention deletion batch".into()));}
                changes.push(spool::Mutation::delete(key));
                changes.push(spool::Mutation::delete(time_key(origin,seq)));count+=1;
            }
            let receipts=o.store.keys_with_prefix(format!("07/replication/receipt/{}/",net::hex(&origin)).as_bytes());
            let mut receipt_count=0;for key in receipts {if key_sequence(&key)?>through {break;}if receipt_count>=256 {return Err(Error::Limit("retention receipt batch".into()));}changes.push(spool::Mutation::delete(key));receipt_count+=1;}
            head.retained_from=head.retained_from.max(through.checked_add(1).ok_or_else(||Error::Invalid("sequence exhausted".into()))?);
            changes.push(spool::Mutation::put(retained_key(origin),net::encode(&head).map_err(|e|Error::Storage(e.to_string()))?));
            changes.push(spool::Mutation::put(floor_key(origin),encode(&through)?));
            let total=o.store.get(b"07/replication/retention/collected")?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
            changes.push(spool::Mutation::put(b"07/replication/retention/collected".to_vec(),encode(&total.saturating_add(count))?));
            o.store.commit(&changes,spool::Durability::Sync)?;
            Ok(count)
        });
        drop(ticket);
        res
    }
    fn reclaim_retention(&self)->net::Result<()> {
        let database = self.database()?;
        let ticket = database.inner.scheduler.admit(crate::scheduler::WriterClass::Maintenance).map_err(error)?;
        let res = self.spool(|o|o.store.reclaim().map_err(Error::from));
        drop(ticket);
        res
    }
    fn schema(&self)->net::Result<Vec<u8>>{self.spool(|o|encode(&canonical_schema(&crate::table_replication::filter_schema(&o.schema))?))}
    fn observe(&self,millis:u64,logical:u32)->net::Result<()>{self.database()?.call(move|c|{c.collection_clock=c.collection_clock.max(Version{millis,logical,node:[0xff;16]});Ok(())}).map_err(error)}
    fn accept_schema(&self,bytes:Vec<u8>)->net::Result<bool>{
        let incoming:Schema=decode(&bytes).map_err(error)?;
        let database = self.database()?;
        let ticket = database.inner.scheduler.admit(crate::scheduler::WriterClass::Remote).map_err(error)?;
        let res = database.call(move|c|c.adopt_schema(incoming)).map_err(error);
        drop(ticket);
        res
    }
    fn apply(&self,payloads:Vec<Vec<u8>>,records:Vec<Record>)->net::Result<bool>{
        let database=self.database()?;
        let ticket = database.inner.scheduler.admit(crate::scheduler::WriterClass::Remote).map_err(error)?;
        let cursor=database.call(|c|{let s=c.status.lock().unwrap();if s.committed!=c.captured_cursor || s.durable!=c.captured_cursor || s.storage_error.is_some(){return Ok(None);}Ok(Some(s.committed))}).map_err(error)?;
        let Some(cursor)=cursor else{return Ok(false);};
        let (patch, auxiliary)=self.spool(move|o|{if o.cursor!=cursor{return Err(Error::Busy);}let baseline=metadata::state(&o.store,o.schema.clone(),o.identity.database,o.clock)?;let mut joined=baseline.clone();let mut auxiliary=Vec::new();for payload in payloads {
                let content:net::Content=decode_content(&payload)?;
                validate_auxiliary(&content.records)?;
                if let Some(bytes)=content.sqlite {let mut incoming:ReplicaState=decode(&bytes)?;drop_retired_tables(o,&mut incoming,&content.origin)?;
                for table in &incoming.schema.tables {let local=joined.schema.tables.iter().find(|t|t.id==table.id).ok_or(Error::SchemaMismatch)?;if local.name!=table.name || table.columns.iter().any(|c|!local.columns.contains(c)) {return Err(Error::SchemaMismatch);}}
                for row in &mut incoming.rows {let table=joined.schema.tables.iter().find(|t|t.id==row.table).ok_or(Error::SchemaMismatch)?;for column in &table.columns {if !row.cells.iter().any(|c|c.column==column.id) {if column.not_null || column.primary_key>0 {return Err(Error::SchemaMismatch);}row.cells.push(Cell{column:column.id.clone(),value:Value::Null,version:Version::default()});}}}
                crate::table_replication::filter(&mut incoming, &joined.schema);
                incoming.schema=joined.schema.clone();joined=merge::join(&joined,&incoming)?;}
                auxiliary.extend(content.records);
            }
            auxiliary=crate::table_replication::filter_records(auxiliary, &o.schema)?;
            auxiliary=crate::high_low::join(o,auxiliary,&mut joined)?;
            auxiliary=crate::policy::join(o,auxiliary,&mut joined)?;
            auxiliary=join_auxiliary(o,auxiliary)?;
            crate::table_replication::preserve_local(&mut joined, &baseline);
            Ok((merge::prepare_joined(&baseline,joined,cursor)?,auxiliary))})?;
        let mut records=records;records.extend(auxiliary);
        let published=database.call(move|c|c.publish_network(patch,records,false)).map_err(error)?;
        if !published{return Ok(false);}
        // Wait on Spool off the writer thread. The durable staging remains until
        // the state, receipt, watermark and cleanup commit together.
        let (tx, rx) = mpsc::channel();
        self.worker.send(Work::Barrier(tx)).map_err(|_| net::Error::Unavailable("storage worker stopped".into()))?;
        rx.recv().map_err(|_|net::Error::Unavailable("durability reply lost".into()))?.map_err(error)?;
        drop(ticket);
        Ok(true)
    }
}
pub(crate) fn attach(database:&Database,config:net::Config)->Result<net::Handle>{let adapter=Arc::new(Adapter{inner:Arc::downgrade(&database.inner),worker:database.inner.worker.clone(),identity:(database.database_id(),database.node_id())});net::start(config,adapter).map_err(|e|Error::Invalid(e.to_string()))}
impl Core {
    pub(crate) fn publish_network(&mut self,patch:merge::Patch,records:Vec<Record>,remint:bool)->Result<bool>{
        {let s=self.status.lock().unwrap();if s.committed!=patch.cursor || s.committed!=self.captured_cursor || s.storage_error.is_some(){return Ok(false);}}
        let bytes=encode(&(&patch.rows,&patch.deleted,&patch.tombstones,patch.state.clock))?.len()+records.iter().map(|r|r.key.len()+r.value.as_ref().map_or(0,Vec::len)).sum::<usize>();self.admit(bytes)?;
        // Destroy the clean session: remote publication must never feed local capture.
        self.capture.take();self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result=(||->Result<()>{for row in &patch.deleted{let table=self.schema.tables.iter().find(|t|t.id==row.table).ok_or(Error::SchemaMismatch)?;let predicate=schema::pk_columns(table).iter().map(|c|format!("{} IS ?",quote(&c.name))).collect::<Vec<_>>().join(" AND ");self.connection.execute(&format!("DELETE FROM {} WHERE {predicate}",quote(&table.name)),rusqlite::params_from_iter(&row.key))?;}for row in &patch.rows{let table=self.schema.tables.iter().find(|t|t.id==row.table).ok_or(Error::SchemaMismatch)?;schema::put_user_row(&self.connection,table,row)?;}self.connection.execute_batch("COMMIT")?;Ok(())})();
        if result.is_err(){let _=self.connection.execute_batch("ROLLBACK");}self.capture=Some(schema::capture(&self.connection,&self.schema)?);result?;
        self.search.rows(&self.schema, &patch.rows);self.search.rows(&self.schema, &patch.deleted);
        self.status.lock().unwrap().search_indexes = self.search.status();
        let cursor=self.bump()?;self.collection_clock=self.collection_clock.max(patch.state.clock);
        self.send(Event::ReplicaPatch{rows:patch.rows,deleted:patch.deleted,tombstones:patch.tombstones,clock:patch.state.clock,records:records.into_iter().map(|r|(r.key,r.value)).collect(),remint},cursor,bytes)?;self.captured_cursor=cursor;Ok(true)
    }
}
pub(crate) fn sign_local(opened:&Opened,config:&net::Config,mut state:ReplicaState,records:Vec<Record>)->Result<Vec<spool::Mutation>>{
    let schema = state.schema.clone();
    crate::table_replication::filter(&mut state, &schema);
    let records = crate::table_replication::filter_records(records, &schema)?;
    state.schema=crate::table_replication::filter_schema(&schema);
    sign_content(opened,config,net::Content{sqlite:Some(encode(&state)?),records,origin:opened.identity.node},state.clock,&state.schema)
}
fn sign_content(opened:&Opened,config:&net::Config,content:net::Content,clock:Version,schema:&Schema)->Result<Vec<spool::Mutation>> {
    use net::protocol::*;
    let origin=opened.identity.node;let sequence=opened.store.get(&sequence_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0).checked_add(1).ok_or_else(||Error::Corrupt("origin sequence exhausted".into()))?;
    let mut tx=Transaction{database:opened.identity.database,origin,sequence,id:rand::random(),millis:clock.millis,logical:clock.logical,schema:sha_schema(schema)?,payload_digest:[0;32],payload:net::encode(&content).map_err(|e|Error::Storage(e.to_string()))?,signature:Vec::new()};tx.sign(&config.signing_key).map_err(|e|Error::Storage(e.to_string()))?;
    let head_key=format!("07/replication/retained/{}",net::hex(&origin)).into_bytes();
    let mut head=opened.store.get(&head_key)?.map(|b|net::decode::<Progress>(&b).map_err(|e|Error::Corrupt(e.to_string()))).transpose()?.unwrap_or(Progress{origin,applied:sequence,observed:sequence,retained_from:sequence,retained_through:sequence});head.applied=sequence;head.observed=sequence;head.retained_through=sequence;
    Ok(vec![spool::Mutation::put(head_key,net::encode(&head).map_err(|e|Error::Storage(e.to_string()))?),spool::Mutation::put(log_key(origin,sequence),net::encode(&tx).map_err(|e|Error::Storage(e.to_string()))?),spool::Mutation::put(net::retention::time_key(origin,sequence),encode(&net::retention::now_millis())?),spool::Mutation::put(sequence_key(origin),encode(&sequence)?),spool::Mutation::put(progress_key(origin),encode(&sequence)?),spool::Mutation::put(receipt_key(origin,sequence),tx.digest().map_err(|e|Error::Storage(e.to_string()))?.to_vec())])
}
fn sha_schema(schema:&Schema)->Result<[u8;32]>{// The protocol uses SHA-256; keep hashing in the background crate.
    // Bind to the replicated subset so local-only tables never look divergent.
    Ok(net::protocol::schema_digest(&encode(&canonical_schema(&crate::table_replication::filter_schema(schema))?)?))
}

fn snapshot_content(o:&mut Opened)->Result<Vec<u8>> {
    let mut state=metadata::state(&o.store,o.schema.clone(),o.identity.database,o.clock)?;
    crate::table_replication::filter(&mut state,&o.schema);
    state.schema=crate::table_replication::filter_schema(&o.schema);
    let mut records=Vec::new();for prefix in [b"08/files/meta/".as_slice(),b"10/policy/",b"09/bridge/owner/",b"09/bridge/shadow/",b"09/bridge/provenance/"] {for key in o.store.keys_with_prefix(prefix) {if let Some(value)=o.store.get(&key)? {records.push(Record::put(key,value));}}}
    let records=crate::table_replication::filter_records(records,&o.schema)?;
    net::encode(&net::Content {sqlite:Some(encode(&state)?),records,origin:o.identity.node}).map_err(|e|Error::Storage(e.to_string()))
}
fn key_sequence(key:&[u8])->Result<u64> {
    std::str::from_utf8(key).ok().and_then(|key|key.rsplit('/').next()).and_then(|s|s.parse().ok())
        .ok_or_else(||Error::Corrupt("replication sequence key".into()))
}
// Application of a snapshot may race with a file publication that advances
// progress without changing the SQLite cursor. Resolve watermarks at commit time.
pub(crate) fn monotonic_progress(
    o: &Opened,
    records: &mut [(Vec<u8>, Option<Vec<u8>>)],
) -> Result<()> {
    let mut seen = std::collections::BTreeMap::<Vec<u8>, u64>::new();
    for (key, value) in records {
        if key.starts_with(b"07/replication/applied/") {
            if let Some(bytes) = value {
                let old = match seen.get(key) {
                    Some(v) => *v,
                    None => o
                        .store
                        .get(key)?
                        .map(|b| decode::<u64>(&b))
                        .transpose()?
                        .unwrap_or(0),
                };
                let next = decode::<u64>(bytes)?.max(old);
                *bytes = encode(&next)?;
                seen.insert(key.clone(), next);
            }
        }
    }
    Ok(())
}
fn durable_progress(o:&Opened)->Result<Vec<net::protocol::Progress>> {
    let mut entries=Vec::new();for key in o.store.keys_with_prefix(b"07/replication/applied/") {
        let name=std::str::from_utf8(&key).map_err(|_|Error::Corrupt("progress key".into()))?.rsplit('/').next().unwrap();
        if name.len()!=32 || !name.is_ascii() {return Err(Error::Corrupt("progress origin".into()));}
        let mut origin=[0;16];for (index,value) in origin.iter_mut().enumerate() {*value=u8::from_str_radix(&name[index*2..index*2+2],16).map_err(|_|Error::Corrupt("progress origin".into()))?;}
        let applied=decode(&o.store.get(&key)?.ok_or_else(||Error::Corrupt("progress value".into()))?)?;
        let mut head:net::protocol::Progress=o.store.get(&net::protocol::retained_key(origin))?.map(|bytes|net::decode(&bytes).map_err(|e|Error::Corrupt(e.to_string()))).transpose()?.unwrap_or(net::protocol::Progress {origin,applied,observed:applied,retained_from:0,retained_through:0});
        head.applied=applied;head.observed=head.observed.max(applied);entries.push(head);
    }Ok(entries)
}

fn validate_auxiliary(records:&[Record])->Result<()> {
    if records.len()>10000 {return Err(Error::Limit("auxiliary records".into()));}
    for record in records {
        if !record.key.starts_with(b"08/files/meta/") && !record.key.starts_with(b"10/policy/") && !record.key.starts_with(b"09/bridge/owner/") && !record.key.starts_with(b"09/bridge/shadow/") && !record.key.starts_with(b"09/bridge/provenance/") {return Err(Error::Corrupt("unauthorized auxiliary record".into()));}
        if record.value.is_none() {return Err(Error::Corrupt("auxiliary tombstones must be logical".into()));}
    }Ok(())
}
/// Decode a data payload, accepting pre-origin payloads staged before the
/// upgrade as unknown-origin (strict schema checks, never lenient).
pub(crate) fn decode_content(bytes:&[u8])->Result<net::Content> {
    match net::decode(bytes) {
        Ok(content) => Ok(content),
        Err(_) => {
            #[derive(serde::Deserialize)]
            struct Legacy { sqlite:Option<Vec<u8>>, records:Vec<net::Record> }
            let legacy: Legacy =
                postcard::from_bytes(bytes).map_err(|e| Error::Corrupt(e.to_string()))?;
            Ok(net::Content { sqlite: legacy.sqlite, records: legacy.records, origin: [0; 16] })
        }
    }
}
/// Drop payload tables the sender has retired (absent from its latest signed
/// schema manifest) that this node never held, so old transactions stay
/// applicable. Tables held locally always merge (strict column checks still
/// apply); tables the sender still declares, or no manifest at all, keep
/// strict checking so genuine divergence surfaces as SchemaMismatch.
fn drop_retired_tables(o:&Opened,incoming:&mut ReplicaState,origin:&[u8;16])->Result<()> {
    let key = net::protocol::schema_accepted_key(*origin);
    let Some(raw) = o.store.get(&key)? else { return Ok(()); };
    let (_, manifest): (u64, Vec<u8>) =
        net::decode(&raw).map_err(|e| Error::Corrupt(e.to_string()))?;
    let declared: Schema = decode(&manifest)?;
    let live: std::collections::BTreeSet<&str> =
        declared.tables.iter().map(|t| t.id.as_str()).collect();
    let local: std::collections::BTreeSet<&str> =
        o.schema.tables.iter().map(|t| t.id.as_str()).collect();
    let retired: Vec<String> = incoming
        .schema
        .tables
        .iter()
        .filter(|t| !live.contains(t.id.as_str()) && !local.contains(t.id.as_str()))
        .map(|t| t.id.clone())
        .collect();
    if retired.is_empty() {
        return Ok(());
    }
    incoming.schema.tables.retain(|t| live.contains(t.id.as_str()));
    incoming.rows.retain(|r| !retired.contains(&r.table));
    incoming.tombstones.retain(|t| !retired.contains(&t.table));
    Ok(())
}
fn join_auxiliary(o:&Opened,records:Vec<Record>)->Result<Vec<Record>> {
    let mut joined=std::collections::BTreeMap::<Vec<u8>,Vec<u8>>::new();
    for record in records {
        let value=record.value.unwrap();
        let previous=joined.get(&record.key).cloned().or(o.store.get(&record.key)?);
        if record.key.starts_with(b"08/files/meta/") {
            let incoming:net::files::Metadata=net::decode(&value).map_err(|e|Error::Corrupt(e.to_string()))?;
            let prior=previous.as_ref().map(|v|net::decode::<net::files::Metadata>(v).map_err(|e|Error::Corrupt(e.to_string()))).transpose()?;
            if prior.is_none_or(|p|(incoming.millis,incoming.node,incoming.digest,incoming.deleted)>(p.millis,p.node,p.digest,p.deleted)) {joined.insert(record.key,value);}
        }else {joined.insert(record.key,value);}
    }
    Ok(joined.into_iter().map(|(k,v)|Record::put(k,v)).collect())
}

/// Network identity ignores physical SQLite column order and DDL spelling.
fn canonical_schema(schema:&Schema)->Result<Schema> {
    let mut canonical=schema.clone();canonical.tables.sort_by(|a,b|a.id.cmp(&b.id));
    for table in &mut canonical.tables {table.columns.sort_by(|a,b|a.id.cmp(&b.id));table.sql=table_definition(table)?;}
    canonical.indexes.sort();canonical.history=[0;32];canonical.history=*blake3::hash(&encode(&canonical)?).as_bytes();Ok(canonical)
}
fn column_definition(column:&Column)->Result<String> {
    if column.name.is_empty() || column.declared_type.len()>256 || !column.declared_type.bytes().all(|c|c.is_ascii_alphanumeric() || b" (),_".contains(&c)) {return Err(Error::SchemaMismatch);}
    Ok(format!("{} {}{}",quote(&column.name),column.declared_type,if column.not_null{" NOT NULL"}else{""}))
}
fn table_definition(table:&Table)->Result<String> {
    if table.name.is_empty() || table.columns.is_empty() || table.columns.len()>2000 {return Err(Error::SchemaMismatch);}
    let mut definitions=table.columns.iter().map(column_definition).collect::<Result<Vec<_>>>()?;
    let keys=schema::pk_columns(table);if keys.is_empty(){return Err(Error::SchemaMismatch);}
    definitions.push(format!("PRIMARY KEY ({})",keys.iter().map(|c|quote(&c.name)).collect::<Vec<_>>().join(",")));
    Ok(format!("CREATE TABLE {} ({})",quote(&table.name),definitions.join(",")))
}
impl Core {
    fn adopt_schema(&mut self,incoming:Schema)->Result<bool> {
        // Compare replicated subsets: local-only tables never force adoption.
        let local=crate::table_replication::filter_schema(&self.schema);
        let remote=crate::table_replication::filter_schema(&incoming);
        if canonical_schema(&local)?==canonical_schema(&remote)? {return Ok(true);}
        {let s=self.status.lock().unwrap();if s.committed!=self.captured_cursor || s.durable!=self.captured_cursor {return Ok(false);}}
        let mut target=self.schema.clone();let mut ddl=String::new();
        for incoming_table in &incoming.tables {
            // Locally-disabled tables are never touched by adoption.
            if self.schema.tables.iter().any(|t|t.id==incoming_table.id)
                && !crate::table_replication::enabled(&self.schema,&incoming_table.id) { continue; }
            if let Some(local)=target.tables.iter_mut().find(|t|t.id==incoming_table.id) {
                if local.name!=incoming_table.name {return Err(Error::SchemaMismatch);}
                for column in &incoming_table.columns {
                    if let Some(existing)=local.columns.iter().find(|c|c.id==column.id){if existing!=column{return Err(Error::SchemaMismatch);}}
                    else {if column.primary_key>0 || column.not_null || local.columns.iter().any(|c|c.name==column.name){return Err(Error::SchemaMismatch);}ddl.push_str(&format!("ALTER TABLE {} ADD COLUMN {};",quote(&local.name),column_definition(column)?));local.columns.push(column.clone());}
                }
            }else {if target.tables.iter().any(|t|t.name==incoming_table.name){return Err(Error::SchemaMismatch);}ddl.push_str(&table_definition(incoming_table)?);ddl.push(';');target.tables.push(incoming_table.clone());}
        }
        for index in &incoming.indexes {
            if !target.indexes.contains(index) {
                if let Some(table)=crate::schema::index_table(index) {
                    match incoming.tables.iter().find(|t|t.name.eq_ignore_ascii_case(&table)) {
                        Some(t) if self.schema.tables.iter().any(|l|l.id==t.id)
                            && !crate::table_replication::enabled(&self.schema,&t.id) => continue,
                        Some(_) => {}
                        // Honest senders never emit indexes outside their payload.
                        None => return Err(Error::SchemaMismatch),
                    }
                }
                let words=crate::schema::keywords(index);
                if words.first().map(String::as_str)!=Some("CREATE") || words.get(1).map(String::as_str)!=Some("INDEX") || words.iter().any(|s|["SELECT","INSERT","UPDATE","DELETE","DROP","TRIGGER","VIEW","PRAGMA","ATTACH","DETACH","UNIQUE"].contains(&s.as_str())) {return Err(Error::SchemaMismatch);}
                ddl.push_str(index);ddl.push(';');target.indexes.push(index.clone());
            }
        }
        if ddl.is_empty(){return Ok(true);}
        let before=schema::read_rows(&self.connection,&self.schema)?;let previous_search=self.search.clone();self.capture.take();self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result=(||->Result<(Schema,Vec<RowState>)>{self.connection.execute_batch(&ddl)?;let migration=Migration::new(ddl.clone());let mut actual=schema::inspect(&self.connection,&self.schema,&migration)?;
            for table in &mut actual.tables {let intended=target.tables.iter().find(|t|t.name==table.name).ok_or(Error::SchemaMismatch)?;table.id=intended.id.clone();for column in &mut table.columns {let definition=intended.columns.iter().find(|c|c.name==column.name).ok_or(Error::SchemaMismatch)?;column.id=definition.id.clone();column.policy=definition.policy;}}
            actual.history=canonical_schema(&actual)?.history;self.search.sync_schema(&self.connection,&actual,true)?;self.search.refresh(&self.connection,&actual)?;let after=schema::read_rows(&self.connection,&actual)?;self.connection.execute_batch("COMMIT")?;Ok((actual,after))})();
        let (actual,after)=match result {Ok(v)=>v,Err(e)=>{let _=self.connection.execute_batch("ROLLBACK");self.search=previous_search;self.search.publish_guard();self.capture=Some(schema::capture(&self.connection,&self.schema)?);return Err(e);}};
        self.schema=actual.clone();crate::database::configure_authorizer(&self.connection,&self.schema,self.access.clone(),self.search.guard.clone())?;self.status.lock().unwrap().search_indexes=self.search.status();let bytes=encode(&before)?.len()+encode(&after)?.len()+ddl.len();let cursor=self.bump()?;self.capture=Some(schema::capture(&self.connection,&actual)?);
        self.send(Event::SchemaCaptured{migration:ddl,schema:actual.clone(),before,after,version:Version::default()},cursor,bytes)?;self.captured_cursor=cursor;
        Ok(true)
    }
}
