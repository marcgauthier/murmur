use crate::{*, protocol::*, membership::{Member,Packet,State,View}};
use sha2::{Digest,Sha256};
use std::sync::{Mutex,atomic::{AtomicBool,Ordering}};
use tokio::sync::{mpsc,Semaphore};
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicationMetrics {
    pub pushed_transactions: u64,
    pub received_transactions: u64,
    pub applied_transactions: u64,
    pub staged_transactions: usize,
    pub gossip_rounds: u64,
    pub probes_sent: u64,
    pub probes_failed: u64,
    pub anti_entropy_runs: u64,
    pub schema_pushes: u64,
    pub schema_adoptions: u64,
    pub retention_runs: u64,
    pub retention_pruned: u64,
    pub queue_available_bytes: usize,
    pub chunk_need_sent: u64,
    pub chunk_need_served: u64,
}

/// In-memory chunk assembly progress. The manifest plus chunk keys in the
/// backend are the durable truth; this only paces repair requests.
struct ChunkProgress {
    received: u32,
    updated: std::time::Instant,
    repaired: std::time::Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerDiagnostics {
    pub node_id: NodeId,
    pub address: Option<SocketAddr>,
    pub connected: bool,
    pub last_seen_millis_ago: Option<u64>,
    pub sent_watermark: u64,
    pub schema_agreed: bool,
    pub push_mode: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Status {
    pub running: bool,
    pub address: Option<SocketAddr>,
    pub peers: usize,
    pub members: usize,
    pub staged_transactions: usize,
    pub applied_transactions: u64,
    pub last_error: Option<String>,
    pub deferred: bool,
    pub file_endpoint: Option<SocketAddr>,
    pub metrics: ReplicationMetrics,
    pub peers_diagnostics: Vec<PeerDiagnostics>,
}

struct Control {stop:AtomicBool,status:Mutex<Status>,commands:mpsc::Sender<Command>,thread:Mutex<Option<std::thread::JoinHandle<()>>>,objects:Option<Arc<files::Objects>>,bridge:Option<Arc<bridge::Bridge>>}
#[derive(Clone)]
pub struct Handle {control:Arc<Control>}
impl std::fmt::Debug for Handle {fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result {f.debug_struct("Replication").field("status",&self.status()).finish()}}
enum Command {Add(SocketAddr),Remove(NodeId),Retire(NodeId,std::sync::mpsc::Sender<Result<()>>),RetentionStatus(std::sync::mpsc::Sender<Result<retention::RetentionStatus>>),Collect(std::sync::mpsc::Sender<Result<u64>>),Sync,Stop}
impl Handle {
    pub(crate) fn start(config:Config,backend:Arc<dyn Backend>)->Result<Self> {
        config.validate()?;bind_signer(backend.as_ref(),&config)?;retention::migrate_peers(backend.as_ref())?;
        let objects=config.files.clone().map(|c|files::Objects::open(c,backend.clone()).map(Arc::new)).transpose()?;let bridge=config.bridge.clone().map(|c|bridge::Bridge::open(c,backend.clone()).map(Arc::new)).transpose()?;if let Some(bridge)=&bridge{bridge.attach_files(objects.clone());}
        let (commands,receiver)=mpsc::channel(128);let control=Arc::new(Control{stop:AtomicBool::new(false),status:Mutex::new(Status::default()),commands,thread:Mutex::new(None),objects,bridge});
        let (ready_tx,ready_rx)=std::sync::mpsc::channel();let state=control.clone();
        let thread=std::thread::Builder::new().name("murmur-replication".into()).spawn(move|| {
            let runtime=tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build();
            match runtime {Err(e)=>{let _=ready_tx.send(Err(Error::Io(e)));},Ok(rt)=>rt.block_on(async {
                let endpoint=match transport::endpoint(&config,backend.identity().1) {Ok(e)=>e,Err(e)=>{let _=ready_tx.send(Err(e));return;}};
                let address=endpoint.local_addr().unwrap();state.status.lock().unwrap().running=true;state.status.lock().unwrap().address=Some(address);
                let files=match state.objects.clone().map(|objects|crate::filefetch::Service::start(&config,backend.clone(),objects)).transpose() {Ok(f)=>f,Err(e)=>{let _=ready_tx.send(Err(e));return;}};
                state.status.lock().unwrap().file_endpoint=files.as_ref().and_then(|f|f.endpoint.local_addr().ok());
                let queue=Arc::new(Semaphore::new(config.queue_bytes));
                let _=ready_tx.send(Ok(()));
                let core=Arc::new(Core{config,backend,endpoint,files,control:state.clone(),peers:tokio::sync::Mutex::new(BTreeMap::new()),view:tokio::sync::Mutex::new(View::new(Member{node:[0;16],address,incarnation:0,state:State::Alive})),sessions:Arc::new(Semaphore::new(1)),relays:tokio::sync::Mutex::new(BTreeMap::new()),stage_gate:tokio::sync::Mutex::new(()),retention_gate:tokio::sync::Mutex::new(()),queue,transfers:Arc::new(Semaphore::new(2)),tokens:tokio::sync::Mutex::new((std::time::Instant::now(),16usize<<20)),schema_req_at:tokio::sync::Mutex::new(None),chunk_counters:tokio::sync::Mutex::new(None),transfers_active:Arc::new(tokio::sync::Mutex::new(BTreeSet::new())),chunk_seen:tokio::sync::Mutex::new(BTreeMap::new()),chunk_holders:tokio::sync::Mutex::new(BTreeMap::new())});
                core.view.lock().await.local.node=core.backend.identity().1;
                core.view.lock().await.local.address=core.config.advertise.unwrap_or(address);let incarnation_key=format!("07/replication/incarnation/{}",hex(&core.backend.identity().1)).into_bytes();
                let incarnation=core.backend.get(&incarnation_key).ok().flatten().and_then(|b|decode::<u64>(&b).ok()).unwrap_or(0).saturating_add(1);
                core.view.lock().await.local.incarnation=incarnation;let _=core.backend.commit(vec![Record::put(incarnation_key,encode(&incarnation).unwrap())]);
                // The shared global session budget covers accepted and dialed sessions.
                core.sessions.add_permits(core.config.sessions.saturating_sub(1));
                for key in core.backend.keys(b"07/replication/excluded/").unwrap_or_default(){if let Ok(Some(b))=core.backend.get(&key){if let Ok(id)=decode::<NodeId>(&b){core.view.lock().await.excluded.insert(id);}}}
                core.run(receiver).await;if let Some(files)=&core.files {files.endpoint.close(0u32.into(),b"shutdown");}core.endpoint.close(0u32.into(),b"shutdown");core.endpoint.wait_idle().await;
                state.status.lock().unwrap().running=false;
            })}
        })?;
        *control.thread.lock().unwrap()=Some(thread);
        if let Err(e)=ready_rx.recv().map_err(|_|Error::Unavailable("replication startup stopped".into()))? {if let Some(t)=control.thread.lock().unwrap().take(){let _=t.join();}return Err(e);}
        Ok(Self{control})
    }
    pub fn files(&self)->Option<Arc<files::Objects>> {self.control.objects.clone()}
    pub fn bridge(&self)->Option<Arc<bridge::Bridge>> {self.control.bridge.clone()}
    pub fn status(&self)->Status {self.control.status.lock().unwrap().clone()}
    pub fn add_peer(&self,address:SocketAddr)->Result<()> {self.control.commands.try_send(Command::Add(address)).map_err(|_|Error::Overloaded)}
    pub fn remove_peer(&self,node:NodeId)->Result<()> {self.control.commands.try_send(Command::Remove(node)).map_err(|_|Error::Overloaded)}
    /// Permanently exclude a peer and release its history retention obligation.
    pub fn retire_peer(&self,node:NodeId)->Result<()> {
        let(tx,rx)=std::sync::mpsc::channel();self.control.commands.try_send(Command::Retire(node,tx)).map_err(|_|Error::Overloaded)?;
        rx.recv().map_err(|_|Error::Unavailable("replication stopped".into()))?
    }
    pub fn retention_status(&self)->Result<retention::RetentionStatus> {
        let(tx,rx)=std::sync::mpsc::channel();self.control.commands.try_send(Command::RetentionStatus(tx)).map_err(|_|Error::Overloaded)?;
        rx.recv().map_err(|_|Error::Unavailable("replication stopped".into()))?
    }
    /// Run a bounded collection pass now using the configured retention rules.
    pub fn collect_retention(&self)->Result<u64> {
        let(tx,rx)=std::sync::mpsc::channel();self.control.commands.try_send(Command::Collect(tx)).map_err(|_|Error::Overloaded)?;
        rx.recv().map_err(|_|Error::Unavailable("replication stopped".into()))?
    }
    pub fn force_sync(&self)->Result<()> {self.control.commands.try_send(Command::Sync).map_err(|_|Error::Overloaded)}
    pub fn close(&self)->Result<()> {self.control.stop.store(true,Ordering::Release);let _=self.control.commands.try_send(Command::Stop);if let Some(thread)=self.control.thread.lock().unwrap().take(){thread.join().map_err(|_|Error::Unavailable("replication thread panicked".into()))?;}Ok(())}
}
struct Peer {connection:quinn::Connection,fetch:Option<SocketAddr>,send:crate::queue::Sender,progress:BTreeMap<NodeId,u64>,last_seen:std::time::Instant,push:bool,lazy:bool,outgoing:bool,sent:BTreeMap<NodeId,u64>,schema_acked:Option<([u8;32],u64)>,schema_push_at:std::time::Instant}
struct Core {config:Config,backend:Arc<dyn Backend>,endpoint:quinn::Endpoint,files:Option<crate::filefetch::Service>,control:Arc<Control>,peers:tokio::sync::Mutex<BTreeMap<NodeId,Peer>>,view:tokio::sync::Mutex<View>,sessions:Arc<Semaphore>,relays:tokio::sync::Mutex<BTreeMap<(NodeId,u64),(NodeId,std::time::Instant)>>,stage_gate:tokio::sync::Mutex<()>,retention_gate:tokio::sync::Mutex<()>,queue:Arc<Semaphore>,transfers:Arc<Semaphore>,tokens:tokio::sync::Mutex<(std::time::Instant,usize)>,schema_req_at:tokio::sync::Mutex<Option<std::time::Instant>>,chunk_counters:tokio::sync::Mutex<Option<(usize,usize)>>,transfers_active:Arc<tokio::sync::Mutex<BTreeSet<(NodeId,u64)>>>,chunk_seen:tokio::sync::Mutex<BTreeMap<(NodeId,u64),ChunkProgress>>,chunk_holders:tokio::sync::Mutex<BTreeMap<(NodeId,u64),(BTreeMap<NodeId,BTreeSet<u32>>,std::time::Instant)>>}
impl Core {
    fn error(&self,e:&Error){self.control.status.lock().unwrap().last_error=Some(e.to_string());}
    async fn run(self:&Arc<Self>,mut commands:mpsc::Receiver<Command>) {
        let mut apply=tokio::time::interval(self.config.incoming_interval);let mut repair=tokio::time::interval(self.config.anti_entropy_interval);let mut push=tokio::time::interval(Duration::from_millis(100));let mut probes=tokio::time::interval(Duration::from_secs(1));let mut gossip=tokio::time::interval(Duration::from_millis(200));let mut rotation=tokio::time::interval(Duration::from_secs(30));let mut bootstrap=tokio::time::interval(Duration::from_secs(5));
        let mut retention=tokio::time::interval(self.config.retention_interval);
        retention.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut file_tick=tokio::time::interval(self.config.files.as_ref().map_or(Duration::from_secs(30),|f|f.fetch_interval));
        file_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut seeds=self.config.bootstrap.clone();
        let mut snapshot_receipt_cursor=0usize;
        loop {if self.control.stop.load(Ordering::Acquire){break;}
            tokio::select! {
                incoming=self.endpoint.accept()=>{if let Some(incoming)=incoming {if !transport::network_allowed(&self.config,incoming.remote_address()) || self.peers.lock().await.len()>=self.config.connections {incoming.refuse();continue;}let this=self.clone();tokio::spawn(async move {match incoming.await {Ok(c)=>{let backup=c.clone();if let Err(e)=this.accept(c).await{this.error(&e);backup.close(1u32.into(),b"protocol rejected");}},Err(e)=>this.error(&Error::Protocol(e.to_string()))}});}},
                command=commands.recv()=>match command {Some(Command::Stop)|None=>break,Some(Command::Add(address))=>{let known=self.view.lock().await.members.values().find(|m|m.address==address).map(|m|m.node);if let Some(id)=known {self.view.lock().await.excluded.remove(&id);if let Err(e)=self.backend.commit(vec![Record::delete(format!("07/replication/excluded/{}",hex(&id)).into_bytes())]){self.error(&e);}}if seeds.len()<4096 && !seeds.contains(&address){seeds.push(address);}self.dial(address).await;},Some(Command::Remove(id))=>{self.view.lock().await.excluded.insert(id);if let Err(e)=self.backend.commit(vec![Record::put(format!("07/replication/excluded/{}",hex(&id)).into_bytes(),encode(&id).unwrap())]){self.error(&e);}if let Some(p)=self.peers.lock().await.remove(&id){p.connection.close(0u32.into(),b"administratively removed");}},Some(Command::Retire(id,reply))=>{let _gate=self.retention_gate.lock().await;let result=if id==self.backend.identity().1 {Err(Error::Invalid("cannot retire this node".into()))}else{retention::retire(self.backend.as_ref(),id)};if result.is_ok(){self.view.lock().await.excluded.insert(id);if let Some(peer)=self.peers.lock().await.remove(&id){peer.connection.close(0u32.into(),b"peer retired");}}let _=reply.send(result);},Some(Command::RetentionStatus(reply))=>{let _=reply.send(retention::status(self.backend.as_ref()));},Some(Command::Collect(reply))=>{let _=reply.send(self.collect_history().await);},Some(Command::Sync)=>{self.request_progress().await;}},
                _=bootstrap.tick()=>{for address in &seeds {if self.peers.lock().await.len()>=self.config.fanout+self.config.repairs {break;}self.dial(*address).await;}let discovered:Vec<_>=self.view.lock().await.members.values().filter(|m|m.state==State::Alive).map(|m|m.address).take(4096).collect();for address in discovered {if self.peers.lock().await.len()>=self.config.fanout+self.config.repairs {break;}self.dial(address).await;}},
                _=apply.tick()=>{if let Err(e)=self.apply_pending().await{self.error(&e);}if let Err(e)=self.resend_snapshot_receipts(&mut snapshot_receipt_cursor).await{self.error(&e);}if let Err(e)=self.repair_chunks().await{self.error(&e);}if let Some(bridge)=&self.control.bridge {if let Err(e)=bridge.capture().and_then(|_|bridge.process()){self.error(&e);}}},
                _=file_tick.tick(), if self.files.is_some()=>{if let Some(files)=&self.files {let sources=self.peers.lock().await.iter().filter_map(|(id,p)|p.fetch.map(|addr|(*id,addr))).collect();if let Err(e)=files.fetch_missing(sources).await{self.error(&e);}if let Err(e)=files.objects.rotate_keys(4){self.error(&e);}if let Err(e)=files.objects.collect(Duration::from_secs(7*86400)){self.error(&e);}}},
                _=repair.tick()=>{self.control.status.lock().unwrap().metrics.anti_entropy_runs+=1;self.request_progress().await;self.push_schema().await;},
                _=push.tick()=>{if let Err(e)=self.push_logs().await{self.error(&e);}},
                _=gossip.tick()=>{if self.config.membership {self.control.status.lock().unwrap().metrics.gossip_rounds+=1;self.gossip().await;}},
                _=probes.tick()=>{if self.config.membership {self.control.status.lock().unwrap().metrics.probes_sent+=1;self.probe().await;}},
                _=rotation.tick()=>{self.rotate().await;},
                _=retention.tick()=>{if let Err(e)=self.collect_history().await {self.error(&e);}},
            }
            // Never hold the synchronous status lock across an async wait:
            // session tasks also publish status and must remain schedulable.
            let now = std::time::Instant::now();
            let (peer_count, diagnostics) = {
                let peers = self.peers.lock().await;
                let diagnostics = peers.iter().map(|(id, p)| PeerDiagnostics {
                    node_id: *id,
                    address: Some(p.connection.remote_address()),
                    connected: p.connection.close_reason().is_none(),
                    last_seen_millis_ago: Some(now.saturating_duration_since(p.last_seen).as_millis() as u64),
                    sent_watermark: p.sent.get(id).copied().unwrap_or(0),
                    schema_agreed: p.schema_acked.is_some(),
                    push_mode: p.push,
                }).collect();
                (peers.len(), diagnostics)
            };
            let member_count = self.view.lock().await.members.len();
            {
                let mut status = self.control.status.lock().unwrap();
                status.peers = peer_count;
                status.members = member_count;
                status.metrics.applied_transactions = status.applied_transactions;
                status.metrics.staged_transactions = status.staged_transactions;
                status.metrics.queue_available_bytes = self.queue.available_permits();
                status.peers_diagnostics = diagnostics;
            }
        }
        let members=self.view.lock().await.gossip();let(db,node)=self.backend.identity();
        for peer in self.peers.lock().await.values(){let mut members=members.clone();if let Some(m)=members.iter_mut().find(|m|m.node==node){m.state=State::Left;}if let Ok(data)=encode(&Packet::Ping{database:db,sender:node,nonce:0,members}){let _=peer.connection.send_datagram(data.into());}}
    }
    async fn collect_history(&self)->Result<u64> {
        let _retention=self.retention_gate.lock().await;
        let _staging=self.stage_gate.lock().await;
        let res = retention::collect(self.backend.as_ref(),&self.config);
        if let Ok(pruned) = res {
            let mut s = self.control.status.lock().unwrap();
            s.metrics.retention_runs += 1;
            s.metrics.retention_pruned += pruned;
        }
        res
    }
    async fn dial(self:&Arc<Self>,address:SocketAddr) {
        if !transport::network_allowed(&self.config,address) || address==self.endpoint.local_addr().unwrap() || self.peers.lock().await.values().any(|p|p.connection.remote_address()==address) {return;}
        let permit=match self.sessions.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>return};let this=self.clone();
        tokio::spawn(async move{let _permit=permit;let result=async {
            let connection=tokio::time::timeout(Duration::from_secs(5),this.endpoint.connect(address,"murmur.invalid").map_err(|e|Error::Protocol(e.to_string()))?).await.map_err(|_|Error::Unavailable("dial timeout".into()))?.map_err(|e|Error::Protocol(e.to_string()))?;
            let id=transport::peer_id(&connection,&this.config)?;let (mut send,mut recv)=connection.open_bi().await.map_err(|e|Error::Protocol(e.to_string()))?;let(db,node)=this.backend.identity();
            write(&mut send,&Message::Hello{capabilities:CAPABILITIES,required:REQUIRED,version:VERSION,database:db,node,fetch:this.files.as_ref().and_then(|f|f.endpoint.local_addr().ok()),schema:this.backend.schema()?,mode:this.config.dissemination,max_transaction:this.config.max_transaction_bytes as u64}).await?;
            let welcome=tokio::time::timeout(Duration::from_secs(5),read(&mut recv)).await.map_err(|_|Error::Unavailable("handshake timeout".into()))??;
            let fetch=match &welcome {Message::Welcome{fetch,..}=>*fetch,_=>None};
            let peer_schema=match welcome {Message::Welcome{capabilities,required,database,node,schema,mode,..} if database==db && node==id && mode==this.config.dissemination && compatible_caps(capabilities,required)=>schema,_=>return Err(Error::Auth("incompatible welcome".into()))};
            this.check_handshake_schema(&peer_schema)?;
            this.session(connection,send,recv,id,true,fetch,peer_schema).await
        }.await;if let Err(e)=result {this.error(&e);} });
    }
    async fn accept(self:&Arc<Self>,connection:quinn::Connection)->Result<()> {
        let _permit=self.sessions.clone().try_acquire_owned().map_err(|_|Error::Overloaded)?;let id=transport::peer_id(&connection,&self.config)?;
        let (mut send,mut recv)=tokio::time::timeout(Duration::from_secs(5),connection.accept_bi()).await.map_err(|_|Error::Unavailable("stream timeout".into()))?.map_err(|e|Error::Protocol(e.to_string()))?;
        let hello=tokio::time::timeout(Duration::from_secs(5),read(&mut recv)).await.map_err(|_|Error::Unavailable("hello timeout".into()))??;let(db,node)=self.backend.identity();
        let fetch=match &hello {Message::Hello{fetch,..}=>*fetch,_=>None};
        let peer_schema=match hello {Message::Hello{capabilities,required,version,database,node,schema,mode,max_transaction,..} if version==VERSION && database==db && node==id && mode==self.config.dissemination && compatible_caps(capabilities,required) && max_transaction>0=>schema,_=>return Err(Error::Auth("incompatible hello".into()))};
        self.check_handshake_schema(&peer_schema)?;
        write(&mut send,&Message::Welcome{capabilities:CAPABILITIES,required:REQUIRED,database:db,node,fetch:self.files.as_ref().and_then(|f|f.endpoint.local_addr().ok()),schema:self.backend.schema()?,mode:self.config.dissemination}).await?;
        self.session(connection,send,recv,id,false,fetch,peer_schema).await
    }
    /// Handshake schema bytes are an unsigned mismatch hint only. Adoption
    /// happens exclusively through signed manifests inside the session.
    fn check_handshake_schema(&self,peer:&[u8])->Result<()> {
        if peer==self.backend.schema()?.as_slice() { return Ok(()); }
        if !self.config.accept_remote_schema { return Err(Error::Protocol("schema mismatch".into())); }
        Ok(())
    }
    fn is_master(&self)->bool { self.config.master && self.config.schema_master==Some(self.backend.identity().1) }
    /// Current outbound manifest. The sequence bumps only when our canonical
    /// schema bytes change, so retransmits stay idempotent for receivers.
    fn manifest(&self)->Result<SignedSchema> {
        let bytes=self.backend.schema()?;
        let digest:[u8;32]=Sha256::digest(&bytes).into();
        let key=schema_manifest_key();
        let stored:Option<([u8;32],u64)>=self.backend.get(&key)?.map(|b|decode(&b)).transpose()?;
        let sequence=match stored {
            Some((d,s)) if d==digest => s,
            Some((_,s)) => s.checked_add(1).ok_or_else(||Error::Protocol("schema sequence exhausted".into()))?,
            None => 1,
        };
        if stored!=Some((digest,sequence)) {
            self.backend.commit(vec![Record::put(key,encode(&(digest,sequence))?)])?;
        }
        let (database,node)=self.backend.identity();
        let mut signed=SignedSchema{database,signer:node,sequence,schema:bytes,signature:Vec::new()};
        signed.sign(&self.config.signing_key)?;
        Ok(signed)
    }
    async fn adopt_manifest(&self,_from:NodeId,signed:SignedSchema,send:&crate::queue::Sender)->Result<()> {
        match check_schema_manifest(&self.config,self.backend.as_ref(),&signed)? {
            SchemaDecision::Stale|SchemaDecision::Current|SchemaDecision::Record => {
                let _=send.try_send(Message::SchemaAck{digest:signed.digest(),sequence:signed.sequence});
            }
            SchemaDecision::Adopt => {
                if !self.backend.accept_schema(signed.schema.clone())? {
                    return Ok(()); // adopter busy (uncollected writes); sender retries
                }
                record_schema_accepted(self.backend.as_ref(),&signed)?;
                let _=send.try_send(Message::SchemaAck{digest:signed.digest(),sequence:signed.sequence});
            }
        }
        Ok(())
    }
    async fn want_schema(&self,send:&crate::queue::Sender) {
        let mut last=self.schema_req_at.lock().await;
        let now=std::time::Instant::now();
        if last.is_some_and(|t|now.duration_since(t)<Duration::from_secs(5)) { return; }
        *last=Some(now);
        let _=send.try_send(Message::SchemaRequest);
    }
    /// The master pushes its signed schema to peers that have not acked it.
    async fn push_schema(&self) {
        if !self.is_master() { return; }
        let manifest=match self.manifest() {Ok(m)=>m,Err(e)=>{self.error(&e);return;}};
        let current=(manifest.digest(),manifest.sequence);
        let now=std::time::Instant::now();
        for peer in self.peers.lock().await.values_mut() {
            if peer.schema_acked==Some(current) || now<peer.schema_push_at { continue; }
            peer.schema_push_at=now+Duration::from_secs(30);
            let _=peer.send.try_send(Message::SchemaManifest(manifest.clone()));
        }
    }
    async fn session(self:&Arc<Self>,connection:quinn::Connection,mut send:quinn::SendStream,mut recv:quinn::RecvStream,id:NodeId,outgoing:bool,fetch:Option<SocketAddr>,peer_schema:Vec<u8>)->Result<()> {
        if id==self.backend.identity().1 || self.view.lock().await.excluded.contains(&id){return Err(Error::Auth("excluded peer".into()));}
        {let _gate=self.retention_gate.lock().await;retention::touch(self.backend.as_ref(),id,&[])?;}
        let(tx,mut rx)=crate::queue::Sender::new(self.queue.clone(),self.config.peer_queue_bytes);
        {let mut peers=self.peers.lock().await;if let Some(old)=peers.get(&id){let prefer_outgoing=self.backend.identity().1<id;if old.outgoing==prefer_outgoing || outgoing!=prefer_outgoing {connection.close(0u32.into(),b"duplicate connection");return Ok(());}old.connection.close(0u32.into(),b"duplicate connection");}let push=peers.values().filter(|p|p.push).count()<self.config.fanout;peers.insert(id,Peer{connection:connection.clone(),fetch,send:tx.clone(),progress:BTreeMap::new(),last_seen:std::time::Instant::now(),push,lazy:false,outgoing,sent:BTreeMap::new(),schema_acked:None,schema_push_at:std::time::Instant::now()+Duration::from_secs(30)});}
        self.view.lock().await.merge(Member{node:id,address:connection.remote_address(),incarnation:0,state:State::Alive});
        let this=self.clone();let dc=connection.clone();let datagrams=tokio::spawn(async move {while let Ok(data)=dc.read_datagram().await {if data.len()>1000 {continue;}if let Err(e)=this.packet(id,&dc,&data).await {this.error(&e);}}});
        let _=tx.try_send(Message::ProgressRequest{offset:0});
        // Schema sync starts inside the session: everyone asks (answers declare
        // each sender's replicated subset for payload filtering), and the
        // master additionally pushes so divergent peers converge without asking.
        if peer_schema!=self.backend.schema()? {
            let _=tx.try_send(Message::SchemaRequest);
            if self.is_master() {
                match self.manifest() {Ok(m)=>{let _=tx.try_send(Message::SchemaManifest(m));},Err(e)=>self.error(&e)}
            }
        }
        let result=loop {tokio::select! {
            outbound=rx.recv()=>{let Some(message)=outbound else {break Ok(());};if !transport::network_allowed(&self.config,connection.remote_address()){break Err(Error::Auth("migrated network denied".into()));}if let Err(e)=write(&mut send,&message.message).await{break Err(e);}},
            inbound=read(&mut recv)=>match inbound {Ok(message)=>{if transport::peer_id(&connection,&self.config).is_err(){break Err(Error::Auth("migrated peer denied".into()));}if let Err(e)=self.message(id,message,&tx).await{let _=tx.try_send(Message::Error{code:1,message:e.to_string()});self.error(&e);if matches!(e,Error::Auth(_)){break Err(e);}}},Err(e)=>break Err(e)},
            _=tokio::time::sleep(Duration::from_secs(30))=>{if self.control.stop.load(Ordering::Acquire){break Ok(());}let _=tx.try_send(Message::Ping(rand::random()));},
        }};
        datagrams.abort();{let mut peers=self.peers.lock().await;if peers.get(&id).is_some_and(|p|p.connection.stable_id()==connection.stable_id()){peers.remove(&id);}}connection.close(0u32.into(),b"session closed");result
    }
    fn progress(&self)->Result<Vec<Progress>> {
        let mut origins=BTreeMap::<NodeId,Progress>::new();
        for key in self.backend.keys(b"07/replication/retained/")? {if let Some(b)=self.backend.get(&key)? {let p:Progress=decode(&b)?;origins.insert(p.origin,p);}}
        for key in self.backend.keys(b"07/replication/applied/")? {let Some(b)=self.backend.get(&key)? else {continue;};let suffix=std::str::from_utf8(&key).map_err(|_|Error::Protocol("progress key".into()))?.rsplit('/').next().unwrap();let origin=parse_id(suffix)?;origins.entry(origin).or_insert(Progress{origin,applied:0,observed:0,retained_from:0,retained_through:0}).applied=decode(&b)?;}
        for key in self.backend.keys(b"07/replication/observed/")? {if let Some(b)=self.backend.get(&key)? {let origin=parse_id(std::str::from_utf8(&key).map_err(|_|Error::Protocol("observed key".into()))?.rsplit('/').next().unwrap())?;let p=origins.entry(origin).or_insert(Progress{origin,applied:0,observed:0,retained_from:0,retained_through:0});p.observed=decode(&b)?;}}
        for p in origins.values_mut(){p.observed=p.observed.max(p.applied);}Ok(origins.into_values().collect())
    }
    async fn request_progress(&self){for peer in self.peers.lock().await.values(){let _=peer.send.try_send(Message::ProgressRequest{offset:0});if self.config.membership {let _=peer.send.try_send(Message::MembersRequest{offset:0});}}}
    async fn message(&self,id:NodeId,message:Message,send:&crate::queue::Sender)->Result<()> {
        match message {
            Message::Batches(txs)=>{if txs.len()>64{return Err(Error::Overloaded);}for tx in txs {if tx.schema!=schema_digest(&self.backend.schema()?) {self.want_schema(send).await;}self.stage(tx).await?;}},
            Message::PlumtreeData(tx)=>{let duplicate=self.backend.get(&receipt_key(tx.origin,tx.sequence))?.is_some() || self.backend.get(&stage_key(tx.origin,tx.sequence))?.is_some();self.stage(tx).await?;if duplicate {let _=send.try_send(Message::Prune);}},
            Message::Prune=>{if let Some(p)=self.peers.lock().await.get_mut(&id){p.lazy=true;}},
            Message::IHave{origin,sequence}=>{let applied=self.backend.get(&progress_key(origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);if sequence>applied {let _=send.try_send(Message::Graft{origin,sequence});}},
            Message::Graft{origin,sequence}=>{if let Some(p)=self.peers.lock().await.get_mut(&id){p.lazy=false;}if let Err(error)=self.send_transaction(origin,sequence,send).await {if matches!(error,Error::Unavailable(_)){let _=send.try_send(Message::Error{code:3,message:"history unavailable".into()});}else{return Err(error);}}},
            Message::Need{origin,from,through}=>{if from==0 || through<from {return Err(Error::Protocol("invalid missing range".into()));}for seq in from..=through.min(from.saturating_add(15)){if self.backend.get(&log_key(origin,seq))?.is_none(){let _=send.try_send(Message::Error{code:3,message:"history unavailable".into()});break;}if let Err(error)=self.send_transaction(origin,seq,send).await {if matches!(error,Error::Unavailable(_)){let _=send.try_send(Message::Error{code:3,message:"history unavailable".into()});break;}else if matches!(error,Error::Overloaded){continue;}else{return Err(error);}}}},
            Message::Ack(entries)|Message::ProgressPage{entries,more:false,..}=>self.handle_progress(id,entries,send).await?,
            Message::ProgressPage{offset,entries,more:true}=>{self.handle_progress(id,entries,send).await?;let _=send.try_send(Message::ProgressRequest{offset:offset.saturating_add(128)});},
            Message::ProgressRequest{offset}=>{let entries=self.progress()?;let start=offset as usize;if start>entries.len(){return Err(Error::Protocol("progress offset".into()));}let end=(start+128).min(entries.len());let _=send.try_send(Message::ProgressPage{offset,more:end<entries.len(),entries:entries[start..end].to_vec()});},
            Message::TransactionChunk(chunk)=>self.stage_chunk(chunk).await?,
            Message::ChunkNeed{origin,sequence,indexes}=>self.serve_chunk_need(origin,sequence,indexes,send).await?,
            Message::ChunkAvailabilityRequest{offset}=>self.serve_chunk_availability(offset,send).await?,
            Message::ChunkAvailabilityPage{entries,..}=>self.merge_chunk_availability(id,entries).await?,
            Message::SnapshotRequest=>self.send_snapshot(id,send).await?,
            Message::SnapshotResume{transfer}=>{
                let _gate=self.stage_gate.lock().await;
                if let Some(m)=snapshot::outgoing(self.backend.as_ref(),id,transfer,retention::now_millis())? {
                    send.try_send(Message::SnapshotManifest(m))?;
                }else{send.try_send(Message::SnapshotUnavailable{transfer})?;}
            },
            Message::SnapshotManifest(m)=>{
                let _gate=self.stage_gate.lock().await;
                snapshot::accept(self.backend.as_ref(),&self.config,id,m.clone(),retention::now_millis())?;
                self.request_snapshot_chunks(id,m.transfer,send)?;
            },
            Message::SnapshotChunk{transfer,index,data}=>{
                if !self.config.trusted_snapshot_sources.contains(&id) {return Err(Error::Auth("untrusted snapshot chunk source".into()));}
                let _gate=self.stage_gate.lock().await;
                snapshot::receive(self.backend.as_ref(),id,transfer,index,data,retention::now_millis())?;
            },
            Message::SnapshotNeed{transfer,indexes}=>self.send_snapshot_chunks(id,transfer,indexes,send).await?,
            Message::SnapshotDone{transfer}=>{
                let _gate=self.stage_gate.lock().await;
                self.request_snapshot_chunks(id,transfer,send)?;
            },
            Message::SnapshotApplied{transfer,digest}=>{
                let _gate=self.stage_gate.lock().await;
                self.backend.acknowledge_snapshot(id,transfer,digest)?;
            },
            Message::SnapshotCancel{transfer}=>{
                let _gate=self.stage_gate.lock().await;
                snapshot::cancel(self.backend.as_ref(),id,transfer)?;
                snapshot::maintain(self.backend.as_ref(),retention::now_millis())?;
            },
            Message::SnapshotUnavailable{transfer}=>{
                let _gate=self.stage_gate.lock().await;
                if let Some(r)=snapshot::incoming(self.backend.as_ref())? {
                    if r.manifest.donor!=id || r.manifest.transfer!=transfer {return Err(Error::Auth("snapshot unavailable identity".into()));}
                    send.try_send(Message::SnapshotCancel{transfer})?;
                    self.backend.commit(vec![Record::delete(snapshot::header(snapshot::IN,transfer))])?;
                    snapshot::maintain(self.backend.as_ref(),retention::now_millis())?;
                    let _=send.try_send(Message::SnapshotRequest);
                }
            },
            Message::MembersRequest{offset}=>{if !self.config.membership {return Ok(());}let view=self.view.lock().await;let mut members=vec![view.local.clone()];members.extend(view.members.values().cloned());let start=offset as usize;if start>members.len(){return Err(Error::Protocol("membership offset".into()));}let end=(start+128).min(members.len());let _=send.try_send(Message::MembersPage{offset,more:end<members.len(),entries:members[start..end].to_vec()});},
            Message::MembersPage{offset,more,entries}=>{if entries.len()>128{return Err(Error::Overloaded);}for member in entries {self.view.lock().await.merge(member);}if more {let _=send.try_send(Message::MembersRequest{offset:offset.saturating_add(128)});}},
            Message::SchemaRequest=>{let manifest=self.manifest()?;let _=send.try_send(Message::SchemaManifest(manifest));},
            Message::SchemaManifest(signed)=>self.adopt_manifest(id,signed,send).await?,
            Message::SchemaAck{digest,sequence}=>{if let Some(p)=self.peers.lock().await.get_mut(&id){p.schema_acked=Some((digest,sequence));}},Message::Ping(n)=>{let _=send.try_send(Message::Pong(n));},Message::Pong(_)=>{},
            Message::Error{code:3,..}=>{if self.config.trusted_snapshot_sources.contains(&id){let _=send.try_send(Message::SnapshotRequest);}},
            Message::Error{code,message}=>return Err(Error::Unavailable(format!("peer {code}: {message}"))),
            Message::Hello{..}|Message::Welcome{..}=>return Err(Error::Protocol("repeated handshake".into())),
        } Ok(())
    }
    async fn stage(&self,tx:Transaction)->Result<()> {
        tx.verify(self.backend.identity().0,&self.config.trusted_origins,self.config.max_transaction_bytes)?;
        let digest=tx.digest()?;let _gate=self.stage_gate.lock().await;
        let floor=self.backend.get(&retention::floor_key(tx.origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
        if tx.sequence<=floor {return Ok(());}
        if let Some(receipt)=self.backend.get(&receipt_key(tx.origin,tx.sequence))? {if receipt!=digest{return Err(Error::Auth("conflicting transaction identity".into()));}return Ok(());}
        let key=stage_key(tx.origin,tx.sequence);if let Some(old)=self.backend.get(&key)? {if decode::<Transaction>(&old)?.digest()?!=digest{return Err(Error::Auth("conflicting staged transaction".into()));}return Ok(());}
        let keys=self.backend.keys(b"07/replication/staged/")?;if keys.len()>=self.config.staging_transfers {return Err(Error::Overloaded);}let bytes=encode(&tx)?;let mut total=bytes.len();for key in keys {total=total.checked_add(self.backend.get(&key)?.map_or(0,|b|b.len())).ok_or(Error::Overloaded)?;if total>self.config.staging_bytes{return Err(Error::Overloaded);}}
        self.backend.observe(tx.millis,tx.logical)?;let observed=self.backend.get(&observed_key(tx.origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0).max(tx.sequence);
        self.backend.commit(vec![Record::put(key,bytes),Record::put(observed_key(tx.origin),encode(&observed)?)])?;self.control.status.lock().unwrap().metrics.received_transactions+=1;Ok(())
    }
    async fn resend_snapshot_receipts(&self,cursor:&mut usize)->Result<()> {
        // Completion receipts survive lost ACKs and either process restarting.
        let keys=self.backend.keys(snapshot::RECEIPTS)?;
        if *cursor>=keys.len(){*cursor=0;}
        let end=cursor.saturating_add(128).min(keys.len());
        for key in &keys[*cursor..end] {
            if let Some(bytes)=self.backend.get(key)? {
                let r:snapshot::Incoming=decode(&bytes)?;
                if let Some(peer)=self.peers.lock().await.get(&r.manifest.donor) {
                    let _=peer.send.try_send(Message::SnapshotApplied{transfer:r.manifest.transfer,digest:r.manifest.digest});
                }
            }
        }
        *cursor=end;
        Ok(())
    }
    async fn apply_pending(&self)->Result<()> {
        let _gate=self.stage_gate.lock().await;
        {
            snapshot::maintain(self.backend.as_ref(),retention::now_millis())?;
            if let Some(r)=snapshot::incoming(self.backend.as_ref())? {
                let sender=self.peers.lock().await.get(&r.manifest.donor).map(|p|p.send.clone());
                if let Some(send)=sender {
                    if snapshot::missing(self.backend.as_ref(),&r)?.is_empty() {
                        self.request_snapshot_chunks(r.manifest.donor,r.manifest.transfer,&send)?;
                    }else{let _=send.try_send(Message::SnapshotResume{transfer:r.manifest.transfer});}
                }
            }
        }
        let keys=self.backend.keys(b"07/replication/staged/")?;self.control.status.lock().unwrap().staged_transactions=keys.len();let mut next=BTreeMap::new();let mut transactions=Vec::new();let mut size=0;
        for key in keys {if transactions.len()>=64{break;}let Some(b)=self.backend.get(&key)? else{continue;};let tx:Transaction=decode(&b)?;tx.verify(self.backend.identity().0,&self.config.trusted_origins,self.config.max_transaction_bytes)?;
            let applied=match next.get(&tx.origin){Some(n)=>*n,None=>self.backend.get(&progress_key(tx.origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0)};
            if tx.sequence<=applied {self.backend.commit(vec![Record::delete(key)])?;continue;}
            if tx.sequence!=applied+1 {for peer in self.peers.lock().await.values(){let _=peer.send.try_send(Message::Need{origin:tx.origin,from:applied+1,through:tx.sequence-1});}continue;}
            if size+b.len()>64<<20 {break;}size+=b.len();next.insert(tx.origin,tx.sequence);transactions.push(tx);
        }
        if transactions.is_empty(){return Ok(());}let mut records=Vec::new();for tx in &transactions {records.push(Record::put(log_key(tx.origin,tx.sequence),encode(tx)?));records.push(Record::put(retention::time_key(tx.origin,tx.sequence),encode(&retention::now_millis())?));
            let mut head=self.backend.get(&retained_key(tx.origin))?.map(|b|decode::<Progress>(&b)).transpose()?.unwrap_or(Progress{origin:tx.origin,applied:tx.sequence,observed:tx.sequence,retained_from:tx.sequence,retained_through:tx.sequence});head.applied=tx.sequence;head.observed=head.observed.max(tx.sequence);head.retained_through=tx.sequence;records.push(Record::put(retained_key(tx.origin),encode(&head)?));records.push(Record::put(receipt_key(tx.origin,tx.sequence),tx.digest()?.to_vec()));records.push(Record::put(progress_key(tx.origin),encode(&tx.sequence)?));records.push(Record::delete(stage_key(tx.origin,tx.sequence)));}
        let applied=self.backend.apply(transactions.iter().map(|t|t.payload.clone()).collect(),records)?;self.control.status.lock().unwrap().deferred=!applied;
        if applied {self.control.status.lock().unwrap().applied_transactions+=transactions.len() as u64;let progress=self.progress()?;for peer in self.peers.lock().await.values(){for page in progress.chunks(128){let _=peer.send.try_send(Message::Ack(page.to_vec()));}}}Ok(())
    }
    /// Stage one transaction chunk with O(1) backend operations. The proof
    /// signature verifies once per transfer (first chunk); later chunks bind
    /// to the durable manifest by digest, and reassembly reads each chunk
    /// exactly once at completion.
    async fn stage_chunk(&self, chunk: Chunk) -> Result<()> {
        // Cheap structural checks first: no crypto, no I/O. Saturating
        // math: the bounds below reject bogus indexes either way.
        let last_len = (chunk.total as usize)
            .saturating_sub((chunk.index as usize).saturating_mul(CHUNK_BYTES));
        if chunk.total as usize > self.config.max_transaction_bytes + 4096
            || chunk.count == 0
            || chunk.count as usize != (chunk.total as usize).div_ceil(CHUNK_BYTES)
            || chunk.index >= chunk.count
            || chunk.data.len()
                != if chunk.index + 1 == chunk.count {
                    last_len
                } else {
                    CHUNK_BYTES
                }
        {
            return Err(Error::Protocol("transaction chunk bounds".into()));
        }
        if !chunk.proof.payload.is_empty()
            || chunk.proof.origin != chunk.origin
            || chunk.proof.sequence != chunk.sequence
            || chunk.proof.id != chunk.id
        {
            return Err(Error::Auth("chunk proof identity".into()));
        }
        let floor = self
            .backend
            .get(&retention::floor_key(chunk.origin))?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        if chunk.sequence <= floor {
            return Ok(());
        }
        let _gate = self.stage_gate.lock().await;
        let mkey = chunk_manifest_key(chunk.origin, chunk.sequence);
        let (manifest, manifest_is_new) = match self.backend.get(&mkey)? {
            Some(bytes) => (decode::<ChunkManifest>(&bytes)?, false),
            None => {
                // Duplicate chunks of a completed transaction are dropped
                // without re-verifying or re-staging anything.
                if self.backend.get(&receipt_key(chunk.origin, chunk.sequence))?.is_some()
                    || self.backend.get(&stage_key(chunk.origin, chunk.sequence))?.is_some()
                {
                    return Ok(());
                }
                chunk
                    .proof
                    .verify_header(self.backend.identity().0, &self.config.trusted_origins)?;
                (
                    ChunkManifest {
                        id: chunk.id,
                        count: chunk.count,
                        total: chunk.total,
                        digest: chunk.digest,
                    },
                    true,
                )
            }
        };
        if manifest.id != chunk.id
            || manifest.count != chunk.count
            || manifest.total != chunk.total
            || manifest.digest != chunk.digest
        {
            return Err(Error::Auth("chunk manifest mismatch".into()));
        }
        let key = chunk_key(chunk.origin, chunk.sequence, chunk.index);
        let is_new = match self.backend.get(&key)? {
            Some(old) => {
                if old != encode(&chunk)? {
                    return Err(Error::Auth("conflicting transaction chunk".into()));
                }
                false
            }
            None => true,
        };
        let manifest_bytes = encode(&manifest)?;
        if manifest_is_new {
            self.charge_chunks(1, manifest_bytes.len()).await?;
        }
        if is_new {
            self.charge_chunks(1, chunk.data.len()).await?;
        }
        let mut records = Vec::new();
        if manifest_is_new {
            records.push(Record::put(mkey, manifest_bytes));
        }
        if is_new {
            records.push(Record::put(key, encode(&chunk)?));
        }
        if !records.is_empty() {
            self.backend.commit(records)?;
        }
        let now = std::time::Instant::now();
        let mut seen = self.chunk_seen.lock().await;
        let received = match seen.get_mut(&(chunk.origin, chunk.sequence)) {
            Some(p) => {
                if is_new {
                    p.received += 1;
                }
                p.updated = now;
                p.received
            }
            None => {
                // First sighting this process: reseed the count from the
                // durable keys (covers restart mid-transfer). A transfer
                // already underway elsewhere counts as stale so the repair
                // sweep picks it up promptly.
                let have = self.count_staged_chunks(chunk.origin, chunk.sequence)? as u32;
                let updated = if manifest_is_new {
                    now
                } else {
                    now.checked_sub(Duration::from_secs(3600)).unwrap_or(now)
                };
                seen.insert(
                    (chunk.origin, chunk.sequence),
                    ChunkProgress {
                        received: have,
                        updated,
                        repaired: now.checked_sub(Duration::from_secs(3600)).unwrap_or(now),
                    },
                );
                have
            }
        };
        drop(seen);
        if received < manifest.count {
            return Ok(());
        }
        // Complete: single reassembly pass, then hand off to staging.
        let mut data = Vec::with_capacity(manifest.total as usize);
        let mut cleanup = Vec::with_capacity(manifest.count as usize + 1);
        for index in 0..manifest.count {
            let Some(bytes) = self.backend.get(&chunk_key(chunk.origin, chunk.sequence, index))? else {
                // Lost a chunk between counting and reading; recount and
                // wait for repair instead of failing the transfer.
                let have = self.count_staged_chunks(chunk.origin, chunk.sequence)? as u32;
                if let Some(p) = self.chunk_seen.lock().await.get_mut(&(chunk.origin, chunk.sequence)) {
                    p.received = have;
                }
                return Ok(());
            };
            let part: Chunk = decode(&bytes)?;
            if part.digest != manifest.digest
                || part.id != manifest.id
                || part.total != manifest.total
                || part.count != manifest.count
            {
                return Err(Error::Auth("mixed transaction chunks".into()));
            }
            data.extend(part.data);
            cleanup.push(Record::delete(chunk_key(chunk.origin, chunk.sequence, index)));
        }
        if data.len() != manifest.total as usize
            || <[u8;32]>::from(Sha256::digest(&data)) != manifest.digest
        {
            return Err(Error::Auth("transaction chunk digest".into()));
        }
        let tx: Transaction = decode(&data)?;
        if tx.origin != chunk.origin || tx.sequence != chunk.sequence || tx.id != chunk.id {
            return Err(Error::Auth("chunk transaction identity".into()));
        }
        cleanup.push(Record::delete(chunk_manifest_key(chunk.origin, chunk.sequence)));
        let manifest_len = encode(&manifest)?.len();
        drop(_gate);
        self.stage(tx).await?;
        self.backend.commit(cleanup)?;
        self.release_chunks(manifest.count as usize + 1, manifest.total as usize + manifest_len)
            .await;
        self.chunk_seen.lock().await.remove(&(chunk.origin, chunk.sequence));
        self.chunk_holders.lock().await.remove(&(chunk.origin, chunk.sequence));
        Ok(())
    }

    /// Count staged chunk keys (not the manifest) for one transfer.
    fn count_staged_chunks(&self, origin: NodeId, seq: u64) -> Result<usize> {
        let prefix = chunk_prefix(origin, seq);
        Ok(self
            .backend
            .keys(prefix.as_bytes())?
            .iter()
            .filter(|k| !k.ends_with(b"manifest"))
            .count())
    }

    /// Reserve staging capacity for chunk bytes. Counters rebuild from a
    /// single key scan on first use after (re)start, conservatively
    /// estimating values at one full chunk each (fail-closed direction).
    async fn charge_chunks(&self, keys: usize, bytes: usize) -> Result<()> {
        let mut counters = self.chunk_counters.lock().await;
        if counters.is_none() {
            let n = self.backend.keys(b"07/replication/chunks/")?.len();
            *counters = Some((n, n.saturating_mul(CHUNK_BYTES)));
        }
        let (count, total) = counters.as_mut().unwrap();
        *count += keys;
        *total += bytes;
        if *total > self.config.staging_bytes || *count > self.config.staging_transfers * 1024 {
            *count -= keys;
            *total -= bytes;
            return Err(Error::Overloaded);
        }
        Ok(())
    }

    async fn release_chunks(&self, keys: usize, bytes: usize) {
        if let Some((count, total)) = self.chunk_counters.lock().await.as_mut() {
            *count = count.saturating_sub(keys);
            *total = total.saturating_sub(bytes);
        }
    }

    /// Serve requested chunk indexes, from the complete transaction when
    /// retained, else from staged chunks as a repair relay. Holding nothing
    /// is a quiet miss: repair requests are broadcast, so most peers won't
    /// have the data.
    async fn serve_chunk_need(
        &self,
        origin: NodeId,
        sequence: u64,
        indexes: Vec<u32>,
        send: &crate::queue::Sender,
    ) -> Result<()> {
        if indexes.len() > 128 {
            return Err(Error::Overloaded);
        }
        if let Some(b) = self.backend.get(&log_key(origin, sequence))? {
            let tx: Transaction = decode(&b)?;
            let data = encode(&tx)?;
            let digest: [u8; 32] = Sha256::digest(&data).into();
            let count = data.len().div_ceil(CHUNK_BYTES);
            for index in &indexes {
                if *index as usize >= count {
                    return Err(Error::Protocol("chunk index".into()));
                }
                let start = *index as usize * CHUNK_BYTES;
                let _ = send.try_send(Message::TransactionChunk(Chunk {
                    proof: proof(&tx),
                    origin,
                    sequence,
                    id: tx.id,
                    index: *index,
                    count: count as u32,
                    total: data.len() as u64,
                    digest,
                    data: data[start..(start + CHUNK_BYTES).min(data.len())].to_vec(),
                }));
            }
            self.control.status.lock().unwrap().metrics.chunk_need_served += indexes.len() as u64;
            return Ok(());
        }
        let mkey = chunk_manifest_key(origin, sequence);
        let Some(manifest_bytes) = self.backend.get(&mkey)? else {
            return Ok(());
        };
        let manifest: ChunkManifest = decode(&manifest_bytes)?;
        let mut served = 0u64;
        for index in &indexes {
            if *index >= manifest.count {
                return Err(Error::Protocol("chunk index".into()));
            }
            if let Some(bytes) = self.backend.get(&chunk_key(origin, sequence, *index))? {
                let part: Chunk = decode(&bytes)?;
                let _ = send.try_send(Message::TransactionChunk(part));
                served += 1;
            }
        }
        self.control.status.lock().unwrap().metrics.chunk_need_served += served;
        Ok(())
    }

    /// Transfers with a durable chunk manifest (partially received).
    /// Key form: `07/replication/chunks/<origin-hex>/<seq20>/manifest`.
    fn staged_transfers(&self) -> Result<Vec<(NodeId, u64)>> {
        let mut out = Vec::new();
        for key in self.backend.keys(b"07/replication/chunks/")? {
            let parts: Vec<&str> = std::str::from_utf8(&key)
                .unwrap_or("")
                .split('/')
                .collect();
            if parts.len() == 6 && parts[5] == "manifest" {
                if let (Ok(origin), Ok(seq)) = (parse_id(parts[3]), parts[4].parse::<u64>()) {
                    out.push((origin, seq));
                }
            }
        }
        Ok(out)
    }

    /// Indexes currently staged for one transfer.
    fn present_indexes(&self, origin: NodeId, seq: u64) -> Result<BTreeSet<u32>> {
        let mut out = BTreeSet::new();
        for key in self.backend.keys(chunk_prefix(origin, seq).as_bytes())? {
            if key.ends_with(b"manifest") {
                continue;
            }
            if let Some(suffix) = key.rsplit(|b| *b == b'/').next() {
                if let Ok(index) = std::str::from_utf8(suffix).unwrap_or("").parse::<u32>() {
                    out.insert(index);
                }
            }
        }
        Ok(out)
    }

    /// Answer "what chunks do you hold": staged partial transfers with
    /// their present indexes, 64 transfers per page. Complete transactions
    /// are omitted by design: holders serve broadcast repair requests from
    /// the log without advertising.
    async fn serve_chunk_availability(
        &self,
        offset: u32,
        send: &crate::queue::Sender,
    ) -> Result<()> {
        let mut transfers = self.staged_transfers()?;
        transfers.sort();
        let pages: Vec<_> = transfers
            .into_iter()
            .skip(offset as usize)
            .collect::<Vec<_>>()
            .chunks(64)
            .map(|c| c.to_vec())
            .collect();
        let total = pages.len();
        for (i, page) in pages.into_iter().enumerate() {
            let mut entries = Vec::with_capacity(page.len());
            for (origin, seq) in page {
                let present: Vec<u32> = self.present_indexes(origin, seq)?.into_iter().collect();
                entries.push((origin, seq, present));
            }
            let _ = send.try_send(Message::ChunkAvailabilityPage {
                entries,
                more: i + 1 < total,
            });
        }
        if total == 0 {
            let _ = send.try_send(Message::ChunkAvailabilityPage {
                entries: Vec::new(),
                more: false,
            });
        }
        Ok(())
    }

    /// Merge a holder bitmap page. Entries are capped: a 64 MiB transaction
    /// has 1024 chunks, so 4096 indexes per entry is ample headroom.
    async fn merge_chunk_availability(
        &self,
        from: NodeId,
        entries: Vec<(NodeId, u64, Vec<u32>)>,
    ) -> Result<()> {
        if entries.len() > 64 {
            return Err(Error::Overloaded);
        }
        let now = std::time::Instant::now();
        let mut holders = self.chunk_holders.lock().await;
        for (origin, seq, indexes) in entries {
            if indexes.len() > 4096 {
                return Err(Error::Overloaded);
            }
            let entry = holders
                .entry((origin, seq))
                .or_insert_with(|| (BTreeMap::new(), now));
            entry.0.insert(from, indexes.into_iter().collect());
            entry.1 = now;
        }
        Ok(())
    }

    /// Drop staged chunks plus manifest for one transfer and release the
    /// reserved capacity.
    async fn cleanup_transfer(&self, origin: NodeId, seq: u64) -> Result<()> {
        let mkey = chunk_manifest_key(origin, seq);
        let manifest: Option<ChunkManifest> =
            self.backend.get(&mkey)?.map(|b| decode(&b)).transpose()?;
        let mut records = Vec::new();
        for key in self.backend.keys(chunk_prefix(origin, seq).as_bytes())? {
            records.push(Record::delete(key));
        }
        if !records.is_empty() {
            self.backend.commit(records)?;
        }
        if let Some(m) = manifest {
            self.release_chunks(m.count as usize + 1, m.total as usize + encode(&m)?.len())
                .await;
        }
        self.chunk_seen.lock().await.remove(&(origin, seq));
        self.chunk_holders.lock().await.remove(&(origin, seq));
        Ok(())
    }

    /// Periodic gap repair for incomplete chunked transfers. Recovers
    /// restart state from durable manifests, re-requests missing indexes
    /// (directed by holder bitmaps when known, broadcast otherwise), and
    /// drops chunk garbage for transactions that completed another way.
    async fn repair_chunks(&self) -> Result<()> {
        let now = std::time::Instant::now();
        // Seed in-memory progress from durable manifests. Only needed when
        // the map is empty (restart): live transfers insert on arrival, and
        // the insert sits in the same critical section as the manifest
        // commit with no await between, so no other gap is possible.
        if self.chunk_seen.lock().await.is_empty() {
            for (origin, seq) in self.staged_transfers()? {
                let have = self.count_staged_chunks(origin, seq)? as u32;
                self.chunk_seen.lock().await.insert(
                    (origin, seq),
                    ChunkProgress {
                        received: have,
                        updated: now.checked_sub(Duration::from_secs(3600)).unwrap_or(now),
                        repaired: now.checked_sub(Duration::from_secs(3600)).unwrap_or(now),
                    },
                );
            }
        }
        self.chunk_holders
            .lock()
            .await
            .retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(60));
        let incomplete: Vec<(NodeId, u64)> = self
            .chunk_seen
            .lock()
            .await
            .keys()
            .copied()
            .collect();
        for (origin, seq) in incomplete {
            let mkey = chunk_manifest_key(origin, seq);
            let Some(manifest_bytes) = self.backend.get(&mkey)? else {
                self.chunk_seen.lock().await.remove(&(origin, seq));
                continue;
            };
            let manifest: ChunkManifest = decode(&manifest_bytes)?;
            // Completed through another path (full re-send won the race)?
            // Drop the leftover chunks.
            if self.backend.get(&receipt_key(origin, seq))?.is_some()
                || self.backend.get(&stage_key(origin, seq))?.is_some()
                || self.backend.get(&log_key(origin, seq))?.is_some()
            {
                self.cleanup_transfer(origin, seq).await?;
                continue;
            }
            let (updated, repaired) = match self.chunk_seen.lock().await.get(&(origin, seq)) {
                Some(p) => (p.updated, p.repaired),
                None => continue,
            };
            // Chunks still flowing, or recently asked: leave it alone.
            if now.duration_since(updated) < Duration::from_secs(5)
                || now.duration_since(repaired) < Duration::from_secs(10)
            {
                continue;
            }
            let present = self.present_indexes(origin, seq)?;
            if let Some(p) = self.chunk_seen.lock().await.get_mut(&(origin, seq)) {
                p.received = present.len() as u32;
            }
            let missing: Vec<u32> = (0..manifest.count)
                .filter(|i| !present.contains(i))
                .collect();
            if missing.is_empty() {
                // Bitmap complete but reassembly never ran (completion
                // crashed): ask for a full re-send via a transaction Need
                // and let the duplicate chunks trigger reassembly.
                let peers = self.peers.lock().await;
                if peers.is_empty() {
                    continue;
                }
                for peer in peers.values() {
                    let _ = peer.send.try_send(Message::Need {
                        origin,
                        from: seq,
                        through: seq,
                    });
                }
                drop(peers);
                if let Some(p) = self.chunk_seen.lock().await.get_mut(&(origin, seq)) {
                    p.repaired = now;
                }
                continue;
            }
            let sent = self.request_missing_chunks(origin, seq, &missing).await;
            // Only re-arm the flood gate when a request actually went out;
            // otherwise the next sweep must retry (e.g. peers not yet
            // connected) instead of sleeping through the full interval.
            if sent > 0 {
                if let Some(p) = self.chunk_seen.lock().await.get_mut(&(origin, seq)) {
                    p.repaired = now;
                }
            }
        }
        Ok(())
    }

/// Direct each missing index at a holder round-robin; indexes with no
/// known holder fall back to broadcast.
fn select_holders(
    missing: &[u32],
    holders: &BTreeMap<NodeId, BTreeSet<u32>>,
) -> (BTreeMap<NodeId, Vec<u32>>, Vec<u32>) {
    let mut directed: BTreeMap<NodeId, Vec<u32>> = BTreeMap::new();
    let mut orphaned = Vec::new();
    for (i, index) in missing.iter().enumerate() {
        let having: Vec<NodeId> = holders
            .iter()
            .filter(|(_, set)| set.contains(index))
            .map(|(id, _)| *id)
            .collect();
        if having.is_empty() {
            orphaned.push(*index);
        } else {
            directed
                .entry(having[i % having.len()])
                .or_default()
                .push(*index);
        }
    }
    (directed, orphaned)
}

    /// Ask for missing chunk indexes, directed at known holders when
    /// holder bitmaps exist, broadcast otherwise (with an availability
    /// query attached so the next round can be directed).
    async fn request_missing_chunks(&self, origin: NodeId, seq: u64, missing: &[u32]) -> usize {
        let peers: Vec<(NodeId, crate::queue::Sender)> = self
            .peers
            .lock()
            .await
            .iter()
            .map(|(id, p)| (*id, p.send.clone()))
            .collect();
        if peers.is_empty() {
            return 0;
        }
        let holders = self
            .chunk_holders
            .lock()
            .await
            .get(&(origin, seq))
            .map(|(m, _)| m.clone())
            .unwrap_or_default();
        let want: Vec<u32> = missing.iter().take(128).copied().collect();
        let mut sent = 0usize;
        if holders.is_empty() {
            for (_, send) in &peers {
                let _ = send.try_send(Message::ChunkNeed {
                    origin,
                    sequence: seq,
                    indexes: want.clone(),
                });
                let _ = send.try_send(Message::ChunkAvailabilityRequest { offset: 0 });
                self.control.status.lock().unwrap().metrics.chunk_need_sent += 1;
                sent += 1;
            }
            return sent;
        }
        let (directed, orphaned) = Self::select_holders(&want, &holders);
        for (peer, indexes) in &directed {
            if let Some((_, send)) = peers.iter().find(|(id, _)| id == peer) {
                let _ = send.try_send(Message::ChunkNeed {
                    origin,
                    sequence: seq,
                    indexes: indexes.clone(),
                });
                self.control.status.lock().unwrap().metrics.chunk_need_sent += 1;
                sent += 1;
            }
        }
        if !orphaned.is_empty() {
            for (_, send) in &peers {
                let _ = send.try_send(Message::ChunkNeed {
                    origin,
                    sequence: seq,
                    indexes: orphaned.clone(),
                });
                self.control.status.lock().unwrap().metrics.chunk_need_sent += 1;
                sent += 1;
            }
        }
        sent
    }
    async fn handle_progress(&self,id:NodeId,entries:Vec<Progress>,send:&crate::queue::Sender)->Result<()> {
        if entries.len()>128{return Err(Error::Overloaded);}
        for p in &entries {if p.observed<p.applied || p.retained_from>p.retained_through && p.retained_through!=0{return Err(Error::Protocol("invalid progress".into()));}}
        {let _gate=self.retention_gate.lock().await;retention::touch(self.backend.as_ref(),id,&entries)?;}
        for p in entries {if p.observed<p.applied || p.retained_from>p.retained_through && p.retained_through!=0{return Err(Error::Protocol("invalid progress".into()));}if let Some(peer)=self.peers.lock().await.get_mut(&id){peer.progress.insert(p.origin,p.applied);peer.last_seen=std::time::Instant::now();}
            let applied=self.backend.get(&progress_key(p.origin))?.map(|b|decode::<u64>(&b)).transpose()?.unwrap_or(0);
            let next=applied.saturating_add(1);
            let missing_history=p.retained_from>next || (p.applied>applied && p.retained_through<=applied);
            if missing_history {
                if self.config.trusted_snapshot_sources.contains(&id) {let _=send.try_send(Message::SnapshotRequest);}
                else {self.error(&Error::Unavailable("catch-up requires an explicitly trusted snapshot source".into()));}
            }else if p.retained_through>applied {let _=send.try_send(Message::Need{origin:p.origin,from:next,through:p.retained_through.min(applied.saturating_add(16))});}
        }Ok(())
    }
    async fn send_transaction(&self,origin:NodeId,seq:u64,send:&crate::queue::Sender)->Result<()> {
        let Some(data)=self.backend.get(&log_key(origin,seq))? else {return Err(Error::Unavailable("retained transaction missing".into()));};let tx:Transaction=decode(&data)?;
        if data.len()<1<<20 {send.try_send(if self.config.dissemination==Dissemination::Plumtree{Message::PlumtreeData(tx)}else{Message::Batches(vec![tx])}).map_err(|_|Error::Overloaded)?;}
        else {
            let digest: [u8; 32] = Sha256::digest(&data).into();
            let count = data.len().div_ceil(CHUNK_BYTES);
            // One stream per transaction: duplicate requests while a
            // transfer is in flight are acknowledged without re-sending.
            {
                let mut active = self.transfers_active.lock().await;
                if !active.insert((origin, seq)) {
                    return Ok(());
                }
            }
            let permit = match self.transfers.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    self.transfers_active.lock().await.remove(&(origin, seq));
                    return Err(Error::Overloaded);
                }
            };
            let send = send.clone();
            let active = self.transfers_active.clone();
            tokio::spawn(async move {
                let _permit = permit;
                for (index, part) in data.chunks(CHUNK_BYTES).enumerate() {
                    if send
                        .send(Message::TransactionChunk(Chunk {
                            proof: proof(&tx),
                            origin,
                            sequence: seq,
                            id: tx.id,
                            index: index as u32,
                            count: count as u32,
                            total: data.len() as u64,
                            digest,
                            data: part.to_vec(),
                        }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                active.lock().await.remove(&(origin, seq));
            });
        }
        Ok(())
    }
    async fn push_logs(&self)->Result<()> {
        let progress=self.progress()?;let targets:Vec<_>=self.peers.lock().await.iter().filter(|(_,p)|p.push).map(|(id,p)|(*id,p.send.clone(),p.progress.clone(),p.sent.clone(),p.lazy)).collect();
        let mut tokens=self.tokens.lock().await;let elapsed=tokens.0.elapsed().as_secs_f64();tokens.1=(tokens.1+(elapsed*(64<<20) as f64) as usize).min(16<<20);tokens.0=std::time::Instant::now();drop(tokens);
        for(id,send,known,sent,lazy)in targets {let mut count=0;for p in &progress {let from=known.get(&p.origin).copied().unwrap_or(0).max(sent.get(&p.origin).copied().unwrap_or(0))+1;for seq in from..=p.retained_through.min(from+15) {if count>=16{break;}let Some(data)=self.backend.get(&log_key(p.origin,seq))? else{break;};let mut tokens=self.tokens.lock().await;if tokens.1<data.len(){break;}tokens.1-=data.len();drop(tokens);
                    if lazy && self.config.dissemination==Dissemination::Plumtree {send.try_send(Message::IHave{origin:p.origin,sequence:seq}).map_err(|_|Error::Overloaded)?;}else{self.send_transaction(p.origin,seq,&send).await?;self.control.status.lock().unwrap().metrics.pushed_transactions+=1;}
                    if let Some(peer)=self.peers.lock().await.get_mut(&id){peer.sent.insert(p.origin,seq);}count+=1;
        }}}Ok(())
    }
    async fn rotate(&self) {let mut peers=self.peers.lock().await;use rand::seq::IteratorRandom;let candidate=peers.iter().filter(|(_,p)|!p.push).map(|(id,_)|*id).choose(&mut rand::thread_rng());let old=peers.iter().filter(|(_,p)|p.push).map(|(id,_)|*id).choose(&mut rand::thread_rng());if let(Some(new),Some(old))=(candidate,old){peers.get_mut(&old).unwrap().push=false;peers.get_mut(&new).unwrap().push=true;}}
    async fn probe(&self) {let(db,node)=self.backend.identity();let mut view=self.view.lock().await;let lost=view.expire();use rand::seq::IteratorRandom;let target=self.peers.lock().await.keys().copied().choose(&mut rand::thread_rng());if let Some(target)=target {let nonce=rand::random();view.pending.entry(target).or_insert((std::time::Instant::now(),nonce,false));let members=view.gossip();drop(view);if let Some(peer)=self.peers.lock().await.get(&target){if let Ok(data)=encode(&Packet::Ping{database:db,sender:node,nonce,members}){if data.len()<=1000{let _=peer.connection.send_datagram(data.into());}}}}else{drop(view);}for(target,nonce) in lost {for peer in self.peers.lock().await.values().take(3){if let Ok(data)=encode(&Packet::PingRequest{database:db,sender:node,target,nonce}){let _=peer.connection.send_datagram(data.into());}}}}
    async fn gossip(&self) {let(db,node)=self.backend.identity();let members=self.view.lock().await.gossip();let data=match encode(&Packet::Gossip{database:db,sender:node,members}) {Ok(b) if b.len()<=1000=>b,_=>return};use rand::seq::IteratorRandom;let peers=self.peers.lock().await;for peer in peers.values().choose_multiple(&mut rand::thread_rng(),3){let _=peer.connection.send_datagram(data.clone().into());}}
    async fn packet(&self,id:NodeId,connection:&quinn::Connection,data:&[u8])->Result<()> {
        if transport::peer_id(connection,&self.config)?!=id{return Err(Error::Auth("membership identity".into()));}let packet:Packet=decode(data)?;let(db,node)=self.backend.identity();
        match packet {Packet::Gossip{database,sender,members}=>{if database!=db || sender!=id || members.len()>4{return Err(Error::Auth("membership envelope".into()));}let mut view=self.view.lock().await;for member in members{view.merge(member);}},Packet::IndirectAck{database,sender,target,nonce}=>{if database!=db || sender!=id{return Err(Error::Auth("indirect acknowledgement".into()));}self.view.lock().await.acknowledged(target,nonce);},Packet::Ping{database,sender,nonce,members}|Packet::Ack{database,sender,nonce,members}=>{if database!=db || sender!=id || members.len()>4{return Err(Error::Auth("membership envelope".into()));}let mut view=self.view.lock().await;for member in members{view.merge(member);}view.acknowledged(id,nonce);let reply=view.gossip();drop(view);
                if let Some((requester,_))=self.relays.lock().await.remove(&(id,nonce)) {if let Some(peer)=self.peers.lock().await.get(&requester){let bytes=encode(&Packet::IndirectAck{database:db,sender:node,target:id,nonce})?;let _=peer.connection.send_datagram(bytes.into());}}
                if matches!(decode::<Packet>(data)?,Packet::Ping{..}){let bytes=encode(&Packet::Ack{database:db,sender:node,nonce,members:reply})?;if bytes.len()<=1000{let _=connection.send_datagram(bytes.into());}}},Packet::PingRequest{database,sender,target,nonce}=>{if database!=db || sender!=id{return Err(Error::Auth("membership envelope".into()));}{let mut relays=self.relays.lock().await;relays.retain(|_,(_,at)|at.elapsed()<Duration::from_secs(5));if relays.len()<4096 {relays.insert((target,nonce),(sender,std::time::Instant::now()));}}
                if let Some(peer)=self.peers.lock().await.get(&target){let bytes=encode(&Packet::Ping{database:db,sender:node,nonce,members:self.view.lock().await.gossip()})?;if bytes.len()<=1000{let _=peer.connection.send_datagram(bytes.into());}}}}
        Ok(())
    }
    async fn send_snapshot(&self,id:NodeId,send:&crate::queue::Sender)->Result<()> {
        let _gate=self.stage_gate.lock().await;
        snapshot::maintain(self.backend.as_ref(),retention::now_millis())?;
        let manifest=self.backend.create_snapshot(id,&self.config)?;
        send.try_send(Message::SnapshotManifest(manifest))
    }
    fn request_snapshot_chunks(&self,id:NodeId,transfer:NodeId,send:&crate::queue::Sender)->Result<()> {
        let now=retention::now_millis();
        if let Some(bytes)=self.backend.get(&snapshot::receipt_key(transfer))? {
            let receipt:snapshot::Incoming=decode(&bytes)?;
            if receipt.manifest.donor!=id {return Err(Error::Auth("snapshot receipt identity".into()));}
            return send.try_send(Message::SnapshotApplied{transfer,digest:receipt.manifest.digest});
        }
        let Some(r)=snapshot::incoming(self.backend.as_ref())? else{return Ok(());};
        if r.manifest.transfer!=transfer || r.manifest.donor!=id {return Err(Error::Auth("snapshot request identity".into()));}
        if r.manifest.schema!=schema_digest(&self.backend.schema()?) {
            send.try_send(Message::SnapshotCancel{transfer})?;
            self.backend.commit(vec![Record::delete(snapshot::header(snapshot::IN,transfer))])?;
            // Existing signed schema negotiation must complete before a fresh capture.
            let _=send.try_send(Message::SchemaRequest);
            return Ok(());
        }
        let missing=snapshot::missing(self.backend.as_ref(),&r)?;
        if !missing.is_empty() {
            send.try_send(Message::SnapshotNeed{transfer,indexes:missing})?;
        }else if snapshot::finish(self.backend.as_ref(),&self.config,id,transfer,now)? {
            send.try_send(Message::SnapshotApplied{transfer,digest:r.manifest.digest})?;
        }
        Ok(())
    }
    async fn send_snapshot_chunks(&self,id:NodeId,transfer:NodeId,indexes:Vec<u32>,send:&crate::queue::Sender)->Result<()> {
        if indexes.is_empty() || indexes.len()>128 || indexes.iter().copied().collect::<BTreeSet<_>>().len()!=indexes.len() {return Err(Error::Protocol("snapshot request indexes".into()));}
        let Some(manifest)=snapshot::outgoing(self.backend.as_ref(),id,transfer,retention::now_millis())? else {
            return send.try_send(Message::SnapshotUnavailable{transfer});
        };
        if indexes.iter().any(|i|*i as usize>=manifest.chunks.len()) {return Err(Error::Protocol("snapshot chunk index".into()));}
        // Duplicate page requests or transaction transfers can temporarily use
        // the shared budget. The receiver's durable retry loop will ask again.
        let permit=match self.transfers.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>return Ok(())};
        let backend=self.backend.clone();let send=send.clone();
        tokio::spawn(async move {
            let _permit=permit;
            for index in indexes {
                match backend.get(&snapshot::chunk(snapshot::OUT,transfer,index)) {
                    Ok(Some(data))=>if send.send(Message::SnapshotChunk{transfer,index,data}).await.is_err(){return;},
                    _=>{let _=send.send(Message::SnapshotUnavailable{transfer}).await;return;}
                }
            }
            let _=send.send(Message::SnapshotDone{transfer}).await;
        });
        Ok(())
    }

}
/// Outcome of vetting a received schema manifest, without adopting it.
/// Every verified manifest is recorded (per signer) so payload filtering can
/// drop tables the sender has retired; only the configured master (or any
/// trusted origin in legacy mode) is adopted. Stale duplicates stay idempotent
/// success so retransmits never kill sessions.
#[derive(Debug,PartialEq,Eq)]
pub(crate) enum SchemaDecision { Current, Adopt, Stale, Record }
pub(crate) fn record_schema_accepted(backend:&dyn Backend,signed:&SignedSchema)->Result<()> {
    backend.commit(vec![Record::put(schema_accepted_key(signed.signer),encode(&(signed.sequence,signed.schema.clone()))?)])
}
pub(crate) fn check_schema_manifest(config:&Config,backend:&dyn Backend,signed:&SignedSchema)->Result<SchemaDecision> {
    let (database,node)=backend.identity();
    signed.verify(database,signed.signer,&config.trusted_origins,config.max_transaction_bytes)?;
    let adoptable=match config.schema_master {
        Some(m)=>signed.signer==m && !(config.master && config.schema_master==Some(node)),
        None=>true,
    };
    let key=schema_accepted_key(signed.signer);
    let accepted:(u64,Vec<u8>)=backend.get(&key)?.map(|b|decode(&b)).transpose()?.unwrap_or((0,Vec::new()));
    if signed.sequence<=accepted.0 { return Ok(SchemaDecision::Stale); }
    if signed.schema==backend.schema()? {
        record_schema_accepted(backend,signed)?;
        return Ok(SchemaDecision::Current);
    }
    if !adoptable || !config.accept_remote_schema {
        // Recorded for filtering, never adopted.
        record_schema_accepted(backend,signed)?;
        return Ok(SchemaDecision::Record);
    }
    Ok(SchemaDecision::Adopt)
}
fn proof(tx:&Transaction)->Transaction {let mut header=tx.clone();header.payload.clear();header}
fn parse_id(s:&str)->Result<NodeId>{if s.len()!=32 || !s.is_ascii(){return Err(Error::Protocol("node id".into()));}let mut id=[0;16];for(i,b)in id.iter_mut().enumerate(){*b=u8::from_str_radix(&s[i*2..i*2+2],16).map_err(|_|Error::Protocol("node id".into()))?;}Ok(id)}

fn compatible_caps(capabilities:u64,required:u64)->bool {required & !CAPABILITIES==0 && capabilities & REQUIRED==REQUIRED}

#[cfg(test)]
mod schema_tests {
    use super::*;
    use std::collections::HashMap;

    struct Stub {
        db: NodeId,
        node: NodeId,
        store: std::sync::Mutex<HashMap<Vec<u8>, Vec<u8>>>,
        schema: Vec<u8>,
    }
    impl Stub {
        fn new(node: NodeId, schema: Vec<u8>) -> Self {
            Self {
                db: [7; 16],
                node,
                store: std::sync::Mutex::new(HashMap::new()),
                schema,
            }
        }
    }
    impl Backend for Stub {
        fn identity(&self) -> (NodeId, NodeId) {
            (self.db, self.node)
        }
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.store.lock().unwrap().get(key).cloned())
        }
        fn keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
            Ok(self
                .store
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
        fn commit(&self, records: Vec<Record>) -> Result<()> {
            let mut store = self.store.lock().unwrap();
            for r in records {
                match r.value {
                    Some(v) => {
                        store.insert(r.key, v);
                    }
                    None => {
                        store.remove(&r.key);
                    }
                }
            }
            Ok(())
        }
        fn import_bridge(&self, _h: crate::bridge::Header, _p: Vec<u8>) -> Result<bool> {
            Ok(false)
        }
        fn release_ownership(&self, _t: String, _k: Vec<u8>, _c: Option<String>) -> Result<bool> {
            Ok(false)
        }
        fn publish(&self, _r: Vec<Record>) -> Result<()> {
            Ok(())
        }
        fn snapshot(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<crate::protocol::Progress>)> {
            Ok((Vec::new(), Vec::new()))
        }
        fn retention_checkpoint(&self) -> Result<()> {
            Ok(())
        }
        fn prune_history(&self, _o: NodeId, _t: u64) -> Result<u64> {
            Ok(0)
        }
        fn reclaim_retention(&self) -> Result<()> {
            Ok(())
        }
        fn schema(&self) -> Result<Vec<u8>> {
            Ok(self.schema.clone())
        }
        fn apply(&self, _p: Vec<Vec<u8>>, _r: Vec<Record>) -> Result<bool> {
            Ok(true)
        }
        fn observe(&self, _m: u64, _l: u32) -> Result<()> {
            Ok(())
        }
        fn accept_schema(&self, _s: Vec<u8>) -> Result<bool> {
            Ok(true)
        }
    }

    const MASTER_SEED: [u8; 32] = [11; 32];
    const OTHER_SEED: [u8; 32] = [22; 32];
    const MASTER: NodeId = [1; 16];
    const OTHER: NodeId = [2; 16];

    fn trust() -> BTreeMap<NodeId, [u8; 32]> {
        BTreeMap::from([
            (MASTER, crate::protocol::public_key(&MASTER_SEED)),
            (OTHER, crate::protocol::public_key(&OTHER_SEED)),
        ])
    }
    fn config(node_trust: BTreeMap<NodeId, [u8; 32]>) -> Config {
        let mut c = Config::new(
            "127.0.0.1:0".parse().unwrap(),
            Credentials {
                chain: vec![vec![1]],
                private_key: zeroize::Zeroizing::new(vec![]),
                roots: vec![vec![2]],
            },
            OTHER_SEED,
        );
        c.trusted_origins = node_trust;
        c
    }
    fn manifest(seed: &[u8; 32], signer: NodeId, sequence: u64, schema: Vec<u8>) -> SignedSchema {
        let mut m = SignedSchema {
            database: [7; 16],
            signer,
            sequence,
            schema,
            signature: Vec::new(),
        };
        m.sign(seed).unwrap();
        m
    }

    #[test]
    fn envelope_verify_accepts_master_and_rejects_forgery() {
        let trust = trust();
        let good = manifest(&MASTER_SEED, MASTER, 3, b"schema-bytes".to_vec());
        assert!(good.verify([7; 16], MASTER, &trust, 64 << 20).is_ok());
        // Tampered payload.
        let mut bad = good.clone();
        bad.schema.push(b'x');
        assert!(bad.verify([7; 16], MASTER, &trust, 64 << 20).is_err());
        // Wrong expected master, wrong database, unknown signer, zero seq, oversize.
        assert!(good.verify([7; 16], OTHER, &trust, 64 << 20).is_err());
        assert!(good.verify([8; 16], MASTER, &trust, 64 << 20).is_err());
        assert!(
            good.verify([7; 16], MASTER, &BTreeMap::new(), 64 << 20)
                .is_err()
        );
        let mut zero = good.clone();
        zero.sequence = 0;
        zero.sign(&MASTER_SEED).unwrap();
        assert!(zero.verify([7; 16], MASTER, &trust, 64 << 20).is_err());
        assert!(good.verify([7; 16], MASTER, &trust, 4).is_err());
    }

    #[test]
    fn check_adopts_current_master_and_dedups_replays() {
        let mut c = config(trust());
        c.schema_master = Some(MASTER);
        let backend = Stub::new(OTHER, b"old".to_vec());
        let fresh = manifest(&MASTER_SEED, MASTER, 1, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&c, &backend, &fresh).unwrap(),
            SchemaDecision::Adopt
        );
        // Record acceptance like the handler does after a successful adopt.
        record_schema_accepted(&backend, &fresh).unwrap();
        assert_eq!(
            check_schema_manifest(&c, &backend, &fresh).unwrap(),
            SchemaDecision::Stale
        );
        // Newer sequence, already-converged bytes: recorded without adopting.
        let converged = Stub::new(OTHER, b"new".to_vec());
        record_schema_accepted(&converged, &fresh).unwrap();
        let newer = manifest(&MASTER_SEED, MASTER, 2, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&c, &converged, &newer).unwrap(),
            SchemaDecision::Current
        );
        assert_eq!(
            check_schema_manifest(&c, &converged, &newer).unwrap(),
            SchemaDecision::Stale
        );
    }

    #[test]
    fn check_records_but_never_adopts_non_master_signers() {
        let mut c = config(trust());
        c.schema_master = Some(MASTER);
        let backend = Stub::new(OTHER, b"old".to_vec());
        // Signed by a trusted node that is not the master: recorded for
        // payload filtering, never adopted.
        let rogue = manifest(&OTHER_SEED, OTHER, 1, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&c, &backend, &rogue).unwrap(),
            SchemaDecision::Record
        );
        // The recording holds the signer's declared bytes for filtering.
        let raw = backend.get(&schema_accepted_key(OTHER)).unwrap().unwrap();
        let (seq, bytes): (u64, Vec<u8>) = decode(&raw).unwrap();
        assert_eq!((seq, bytes), (1, b"new".to_vec()));
        assert_eq!(
            check_schema_manifest(&c, &backend, &rogue).unwrap(),
            SchemaDecision::Stale
        );
        // The master itself never adopts remote schemas either.
        let mut master_cfg = config(trust());
        master_cfg.master = true;
        master_cfg.schema_master = Some(MASTER);
        let master_backend = Stub::new(MASTER, b"old".to_vec());
        let m = manifest(&MASTER_SEED, MASTER, 9, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&master_cfg, &master_backend, &m).unwrap(),
            SchemaDecision::Record
        );
        // Forged bytes still fail closed.
        let mut forged = manifest(&MASTER_SEED, MASTER, 10, b"new".to_vec());
        forged.schema.push(b'x');
        assert!(matches!(
            check_schema_manifest(&c, &backend, &forged),
            Err(Error::Auth(_))
        ));
    }

    #[test]
    fn check_legacy_mode_accepts_any_trusted_origin() {
        let c = config(trust()); // schema_master: None
        let backend = Stub::new(OTHER, b"old".to_vec());
        let m = manifest(&OTHER_SEED, OTHER, 1, b"new".to_vec());
        assert_eq!(
            check_schema_manifest(&c, &backend, &m).unwrap(),
            SchemaDecision::Adopt
        );
        let stranger = manifest(&[33; 32], [3; 16], 1, b"new".to_vec());
        assert!(matches!(
            check_schema_manifest(&c, &backend, &stranger),
            Err(Error::Auth(_))
        ));
    }

    #[tokio::test]
    async fn duplicate_floor_discards_old_signed_transactions_and_chunks_without_staging() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        backend
            .commit(vec![Record::put(
                retention::floor_key(MASTER),
                encode(&7u64).unwrap(),
            )])
            .unwrap();
        let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = endpoint.local_addr().unwrap();
        let (commands, _) = mpsc::channel(1);
        let core = Core {
            config: config(trust()),
            backend: backend.clone(),
            endpoint,
            files: None,
            control: Arc::new(Control {
                stop: AtomicBool::new(false),
                status: Mutex::new(Status::default()),
                commands,
                thread: Mutex::new(None),
                objects: None,
                bridge: None,
            }),
            peers: tokio::sync::Mutex::new(BTreeMap::new()),
            view: tokio::sync::Mutex::new(View::new(Member {
                node: OTHER,
                address,
                incarnation: 0,
                state: State::Alive,
            })),
            sessions: Arc::new(Semaphore::new(1)),
            relays: tokio::sync::Mutex::new(BTreeMap::new()),
            stage_gate: tokio::sync::Mutex::new(()),
            retention_gate: tokio::sync::Mutex::new(()),
            queue: Arc::new(Semaphore::new(1)),
            transfers: Arc::new(Semaphore::new(1)),
            tokens: tokio::sync::Mutex::new((std::time::Instant::now(), 0)),
            schema_req_at: tokio::sync::Mutex::new(None),
            chunk_counters: tokio::sync::Mutex::new(None),
            transfers_active: Arc::new(tokio::sync::Mutex::new(BTreeSet::new())),
            chunk_seen: tokio::sync::Mutex::new(BTreeMap::new()),
            chunk_holders: tokio::sync::Mutex::new(BTreeMap::new()),
        };
        for sequence in [1, 7] {
            let mut tx = Transaction {
                database: [7; 16],
                origin: MASTER,
                sequence,
                id: [8; 16],
                millis: 10,
                logical: 0,
                schema: [0; 32],
                payload_digest: [0; 32],
                payload: vec![1, 2, 3],
                signature: vec![],
            };
            tx.sign(&MASTER_SEED).unwrap();
            core.stage(tx.clone()).await.unwrap();
            let data = encode(&tx).unwrap();
            let digest = Sha256::digest(&data).into();
            core.stage_chunk(Chunk {
                proof: proof(&tx),
                origin: MASTER,
                sequence,
                id: tx.id,
                index: 0,
                count: 1,
                total: data.len() as u64,
                digest,
                data,
            })
            .await
            .unwrap();
        }
        assert!(backend.keys(b"07/replication/stage/").unwrap().is_empty());
        assert!(backend.keys(b"07/replication/chunks/").unwrap().is_empty());
        assert_eq!(core.control.status.lock().unwrap().applied_transactions, 0);
    }

    fn chunk_repair_core(backend: Arc<Stub>) -> Core {
        let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = endpoint.local_addr().unwrap();
        let (commands, _) = mpsc::channel(1);
        Core {
            config: config(trust()),
            backend: backend.clone(),
            endpoint,
            files: None,
            control: Arc::new(Control {
                stop: AtomicBool::new(false),
                status: Mutex::new(Status::default()),
                commands,
                thread: Mutex::new(None),
                objects: None,
                bridge: None,
            }),
            peers: tokio::sync::Mutex::new(BTreeMap::new()),
            view: tokio::sync::Mutex::new(View::new(Member {
                node: OTHER,
                address,
                incarnation: 0,
                state: State::Alive,
            })),
            sessions: Arc::new(Semaphore::new(1)),
            relays: tokio::sync::Mutex::new(BTreeMap::new()),
            stage_gate: tokio::sync::Mutex::new(()),
            retention_gate: tokio::sync::Mutex::new(()),
            queue: Arc::new(Semaphore::new(1)),
            transfers: Arc::new(Semaphore::new(1)),
            tokens: tokio::sync::Mutex::new((std::time::Instant::now(), 0)),
            schema_req_at: tokio::sync::Mutex::new(None),
            chunk_counters: tokio::sync::Mutex::new(None),
            transfers_active: Arc::new(tokio::sync::Mutex::new(BTreeSet::new())),
            chunk_seen: tokio::sync::Mutex::new(BTreeMap::new()),
            chunk_holders: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }

    fn signed_chunked_tx(sequence: u64, payload_len: usize) -> (Transaction, Vec<u8>) {
        let mut tx = Transaction {
            database: [7; 16],
            origin: MASTER,
            sequence,
            id: [8; 16],
            millis: 10,
            logical: 0,
            schema: [0; 32],
            payload_digest: [0; 32],
            payload: vec![0xABu8; payload_len],
            signature: vec![],
        };
        tx.sign(&MASTER_SEED).unwrap();
        let data = encode(&tx).unwrap();
        (tx, data)
    }

    fn chunk_message(tx: &Transaction, data: &[u8], index: u32) -> Message {
        let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
        let start = index as usize * crate::protocol::CHUNK_BYTES;
        Message::TransactionChunk(Chunk {
            proof: proof(tx),
            origin: tx.origin,
            sequence: tx.sequence,
            id: tx.id,
            index,
            count,
            total: data.len() as u64,
            digest: Sha256::digest(data).into(),
            data: data[start..(start + crate::protocol::CHUNK_BYTES).min(data.len())].to_vec(),
        })
    }

    #[tokio::test]
    async fn serve_chunk_need_from_complete_log() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let (tx, data) = signed_chunked_tx(3, 200_000);
        backend
            .commit(vec![Record::put(log_key(MASTER, 3), data.clone())])
            .unwrap();
        let core = chunk_repair_core(backend);
        let (send, mut recv) = crate::queue::Sender::new(Arc::new(Semaphore::new(64 << 20)), 4 << 20);
        core.serve_chunk_need(MASTER, 3, vec![0, 2], &send).await.unwrap();
        let mut got = Vec::new();
        while let Ok(q) = recv.try_recv() {
            let Message::TransactionChunk(c) = q.message else {
                panic!("chunk message");
            };
            got.push((c.index, c.data.len()));
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, 0);
        assert_eq!(got[1].0, 2);
        assert_eq!(core.control.status.lock().unwrap().metrics.chunk_need_served, 2);
        // Out-of-range index is a protocol error.
        let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
        let err = core
            .serve_chunk_need(MASTER, 3, vec![count], &send)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
        let _ = tx;
    }

    #[tokio::test]
    async fn serve_chunk_need_relays_staged_chunks() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let (tx, data) = signed_chunked_tx(5, 200_000);
        let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
        let digest: [u8; 32] = Sha256::digest(&data).into();
        let manifest = ChunkManifest {
            id: tx.id,
            count,
            total: data.len() as u64,
            digest,
        };
        // Stage only chunks 0 and 2 (index 1 missing).
        let mut records = vec![Record::put(chunk_manifest_key(MASTER, 5), encode(&manifest).unwrap())];
        for index in [0u32, 2] {
            let Message::TransactionChunk(c) = chunk_message(&tx, &data, index) else {
                panic!("chunk message");
            };
            records.push(Record::put(chunk_key(MASTER, 5, index), encode(&c).unwrap()));
        }
        backend.commit(records).unwrap();
        let core = chunk_repair_core(backend);
        let (send, mut recv) = crate::queue::Sender::new(Arc::new(Semaphore::new(64 << 20)), 4 << 20);
        core.serve_chunk_need(MASTER, 5, vec![0, 1, 2], &send)
            .await
            .unwrap();
        let mut got = Vec::new();
        while let Ok(q) = recv.try_recv() {
            let Message::TransactionChunk(c) = q.message else {
                panic!("chunk message");
            };
            got.push(c.index);
        }
        assert_eq!(got, vec![0, 2], "present chunks served, missing skipped");
        assert_eq!(core.control.status.lock().unwrap().metrics.chunk_need_served, 2);
    }

    #[tokio::test]
    async fn serve_chunk_need_missing_is_quiet() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let core = chunk_repair_core(backend);
        let (send, mut recv) = crate::queue::Sender::new(Arc::new(Semaphore::new(64 << 20)), 4 << 20);
        core.serve_chunk_need(MASTER, 9, vec![0], &send).await.unwrap();
        assert!(recv.try_recv().is_err(), "quiet miss sends nothing");
        assert_eq!(core.control.status.lock().unwrap().metrics.chunk_need_served, 0);
    }

    #[tokio::test]
    async fn serve_chunk_availability_lists_staged_holdings() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let (tx, data) = signed_chunked_tx(5, 200_000);
        let count = data.len().div_ceil(crate::protocol::CHUNK_BYTES) as u32;
        let digest: [u8; 32] = Sha256::digest(&data).into();
        let manifest = ChunkManifest {
            id: tx.id,
            count,
            total: data.len() as u64,
            digest,
        };
        let mut records = vec![Record::put(chunk_manifest_key(MASTER, 5), encode(&manifest).unwrap())];
        for index in [0u32, 3] {
            let Message::TransactionChunk(c) = chunk_message(&tx, &data, index) else {
                panic!("chunk message");
            };
            records.push(Record::put(chunk_key(MASTER, 5, index), encode(&c).unwrap()));
        }
        backend.commit(records).unwrap();
        let core = chunk_repair_core(backend);
        let (send, mut recv) = crate::queue::Sender::new(Arc::new(Semaphore::new(64 << 20)), 4 << 20);
        core.serve_chunk_availability(0, &send).await.unwrap();
        let page = recv.try_recv().unwrap();
        let Message::ChunkAvailabilityPage { entries, more } = page.message else {
            panic!("availability page");
        };
        assert!(!more);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, MASTER);
        assert_eq!(entries[0].1, 5);
        assert_eq!(entries[0].2, vec![0, 3]);
    }

    #[tokio::test]
    async fn merge_chunk_availability_enforces_caps() {
        let backend = Arc::new(Stub::new(OTHER, vec![]));
        let core = chunk_repair_core(backend);
        core.merge_chunk_availability(OTHER, vec![(MASTER, 5, vec![0, 1])])
            .await
            .unwrap();
        let holders = core.chunk_holders.lock().await;
        assert_eq!(
            holders.get(&(MASTER, 5)).unwrap().0.get(&OTHER).unwrap(),
            &BTreeSet::from([0, 1])
        );
        drop(holders);
        let too_many: Vec<(NodeId, u64, Vec<u32>)> =
            (0..65).map(|i| (MASTER, i, vec![0])).collect();
        let err = core
            .merge_chunk_availability(OTHER, too_many)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Overloaded));
        let too_wide = vec![(MASTER, 5, vec![0u32; 4097])];
        let err = core
            .merge_chunk_availability(OTHER, too_wide)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Overloaded));
    }

    #[test]
    fn select_holders_round_robin_with_orphan_fallback() {
        let holders = BTreeMap::from([
            ([1; 16], BTreeSet::from([0, 1, 2])),
            ([2; 16], BTreeSet::from([1, 2, 3])),
        ]);
        let (directed, orphaned) = Core::select_holders(&[0, 1, 2, 3, 9], &holders);
        // 0: only [1] has it. 1,2: both have them (round-robin by
        // position). 3: only [2]. 9: nobody.
        assert_eq!(directed.get(&[1; 16]).unwrap(), &vec![0, 2]);
        assert_eq!(directed.get(&[2; 16]).unwrap(), &vec![1, 3]);
        assert_eq!(orphaned, vec![9]);
    }
}
