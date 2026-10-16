use crate::dispatch::{Reader, Writer};
use crate::*;
use std::{collections::BTreeMap, path::Path};

struct GoBackend(Foreign);
impl GoBackend {
    fn call<T: serde::de::DeserializeOwned>(&self, method: &str, args: Json) -> net::Result<T> {
        let result = self
            .0
            .json(10, 0, json!({"method":method,"args":args}))
            .map_err(|e| net::Error::Invalid(e.to_string()))?;
        serde_json::from_value(result).map_err(|e| net::Error::Protocol(e.to_string()))
    }
}
impl net::Backend for GoBackend {
    fn cancel_pending(&self){let _:net::Result<Json>=self.call("cancel_pending",json!(null));}
    fn identity(&self) -> (net::NodeId, net::NodeId) {
        self.call("identity", Json::Null).unwrap_or_default()
    }
    fn get(&self, key: &[u8]) -> net::Result<Option<Vec<u8>>> {
        self.call("get", json!({"key":key}))
    }
    fn get_many(&self,keys:&[Vec<u8>])->net::Result<Vec<Option<Vec<u8>>>>{self.call("get_many",json!({"keys":keys,"max_bytes":null}))}
    fn get_many_bounded(&self,keys:&[Vec<u8>],max_bytes:usize)->net::Result<Vec<Option<Vec<u8>>>>{self.call("get_many",json!({"keys":keys,"max_bytes":max_bytes}))}
    fn keys(&self, prefix: &[u8]) -> net::Result<Vec<Vec<u8>>> {
        self.call("keys", json!({"prefix":prefix}))
    }
    fn keys_page(&self,prefix:&[u8],after:Option<&[u8]>,limit:usize)->net::Result<Vec<Vec<u8>>> {self.call("keys_page",json!({"prefix":prefix,"after":after,"limit":limit}))}
    fn commit(&self, records: Vec<net::Record>) -> net::Result<()> {
        self.call("commit", json!({"records":records}))
    }
    fn import_bridge(&self, header: net::bridge::Header, payload: Vec<u8>) -> net::Result<bool> {
        self.call("import_bridge", json!({"header":header,"payload":payload}))
    }
    fn release_ownership(
        &self,
        table: String,
        key: Vec<u8>,
        column: Option<String>,
    ) -> net::Result<bool> {
        self.call(
            "release_ownership",
            json!({"table":table,"key":key,"column":column}),
        )
    }
    fn publish(&self, records: Vec<net::Record>) -> net::Result<()> {
        self.call("publish", json!({"records":records}))
    }
    fn snapshot(&self) -> net::Result<Vec<u8>> {
        self.call("snapshot", Json::Null)
    }
    fn snapshot_with_progress(&self) -> net::Result<(Vec<u8>, Vec<net::protocol::Progress>)> {
        self.call("snapshot_with_progress", Json::Null)
    }
    fn create_snapshot(
        &self,
        recipient: net::NodeId,
        config: &net::Config,
    ) -> net::Result<net::snapshot::Manifest> {
        self.call(
            "create_snapshot",
            json!({"recipient":recipient,"config":replication_json(config)}),
        )
    }
    fn acknowledge_snapshot(
        &self,
        recipient: net::NodeId,
        transfer: net::NodeId,
        digest: [u8; 32],
    ) -> net::Result<()> {
        self.call(
            "acknowledge_snapshot",
            json!({"recipient":recipient,"transfer":transfer,"digest":digest}),
        )
    }
    fn retention_checkpoint(&self) -> net::Result<()> {
        self.call("retention_checkpoint", Json::Null)
    }
    fn prune_history(&self, origin: net::NodeId, through: u64) -> net::Result<u64> {
        self.call("prune_history", json!({"origin":origin,"through":through}))
    }
    fn reclaim_retention(&self) -> net::Result<()> {
        self.call("reclaim_retention", Json::Null)
    }
    fn schema(&self) -> net::Result<Vec<u8>> {
        self.call("schema", Json::Null)
    }
    fn apply(&self, payloads: Vec<Vec<u8>>, records: Vec<net::Record>) -> net::Result<bool> {
        self.call("apply", json!({"payloads":payloads,"records":records}))
    }
    fn observe(&self, millis: u64, logical: u32) -> net::Result<()> {
        self.call("observe", json!({"millis":millis,"logical":logical}))
    }
    fn accept_schema(&self, schema: Vec<u8>) -> net::Result<bool> {
        self.call("accept_schema", json!({"schema":schema}))
    }
}
fn backend(id: u64) -> db::Result<Arc<dyn net::Backend>> {
    let e = get(id)?;
    if let Entry::Backend(b) = e.as_ref() {
        Ok(b.clone())
    } else {
        Err(invalid("backend handle"))
    }
}
pub fn replication_json(c: &net::Config) -> Json {
    let mut j = json!({"bind":c.bind,"advertise":c.advertise,"credentials":{"chain":c.credentials.chain,"private_key":c.credentials.private_key.to_vec(),"roots":c.credentials.roots},"signing_key":*c.signing_key,"trusted_origins":c.trusted_origins.iter().map(|(node,key)|json!({"node":node,"key":key})).collect::<Vec<_>>(),"allowed_peers":c.allowed_peers,"allowed_networks":c.allowed_networks,"trusted_snapshot_sources":c.trusted_snapshot_sources,"bootstrap":c.bootstrap,"membership":c.membership,"dissemination":c.dissemination,"accept_remote_schema":c.accept_remote_schema,"master":c.master,"schema_master":c.schema_master});
    macro_rules! fields{($($f:ident),*)=>{$(j[stringify!($f)]=json!(c.$f);)*}}
    fields!(
        fanout,
        repairs,
        sessions,
        connections,
        queue_bytes,
        peer_queue_bytes,
        staging_bytes,
        staging_transfers,
        max_transaction_bytes,
        snapshot_bytes,
        snapshot_staging_bytes,
        minimum_retained,
        batch_transactions,
        batch_bytes,
        adaptive_sync
    );
    macro_rules! times{($($f:ident),*)=>{$(j[stringify!($f)]=json!(c.$f.as_nanos() as u64);)*}}
    times!(
        snapshot_transfer_lifetime,
        incoming_interval,
        anti_entropy_interval,
        retention,
        retention_interval,
        maximum_offline
    );
    j["merge_policies"] = json!(
        c.merge_policies
            .iter()
            .map(|((table, column), policy)| json!({"table":table,"column":column,"policy":policy}))
            .collect::<Vec<_>>()
    );
    j["files"] = c.files.as_ref().map(file_json).unwrap_or(Json::Null);
    j["bridge"] = c.bridge.as_ref().map(bridge_json).unwrap_or(Json::Null);
    j
}
pub fn call(id: u64, v: &Json, callback: Option<Callback>) -> db::Result<Json> {
    let op = v["op"].as_str().unwrap_or("");
    match op {
        "backend_new" => Ok(json!(put(Entry::Backend(Arc::new(GoBackend(
            dispatch::foreign(v, callback)?
        )))))),
        "backend_get_many" => {let b=backend(field(v,"backend")?)?;let keys:Vec<Vec<u8>>=field(v,"keys")?;let values=if v["max_bytes"].is_null(){b.get_many(&keys)}else{b.get_many_bounded(&keys,field(v,"max_bytes")?)};encoded(values.map_err(network)?)},
        "helper_run_blocking" => {let foreign=dispatch::foreign(v,callback)?;net::run_blocking(||foreign.bytes(11,0,&[]))?;Ok(Json::Null)},
        "backend_progress" => encoded(backend(field(v,"backend")?)?.progress().map_err(network)?),
        "backend_bind_signer" => {
            net::bind_signer(backend(id)?.as_ref(), &config::replication(&v["config"])?)
                .map_err(network)?;
            Ok(Json::Null)
        }
        "replication_start" => Ok(json!(put(Entry::Replication(
            net::start(
                config::replication(&v["config"])?,
                backend(field(v, "backend")?)?
            )
            .map_err(network)?
        )))),
        "objects_open" => Ok(json!(put(Entry::Objects(Arc::new(
            net::files::Objects::open(config::files(&v["config"])?, backend(field(v, "backend")?)?)
                .map_err(network)?
        ))))),
        "bridge_open" => Ok(json!(put(Entry::Bridge(Arc::new(
            net::bridge::Bridge::open(
                config::bridge(&v["config"])?,
                backend(field(v, "backend")?)?
            )
            .map_err(network)?
        ))))),
        "transport_endpoint" => {
            let _enter = runtime().enter();
            Ok(json!(put(Entry::Endpoint(
                net::transport::endpoint(&config::replication(&v["config"])?, field(v, "node")?)
                    .map_err(network)?
            ))))
        }
        "helper_replication_defaults" => {
            let c = net::Config::new(
                field(v, "bind")?,
                net::Credentials {
                    chain: Vec::new(),
                    private_key: zeroize::Zeroizing::new(Vec::new()),
                    roots: Vec::new(),
                },
                [0; 32],
            );
            Ok(replication_json(&c))
        }
        "helper_subscription_defaults" => {
            let o = db::SubscriptionOptions::default();
            Ok(
                json!({"buffer_size":o.buffer_size,"emit_unchanged":o.emit_unchanged,"resume_from":o.resume_from,"tables":o.tables}),
            )
        }
        "helper_fts_defaults" => {
            let i = db::Fts5Index::new(
                field::<String>(v, "name")?,
                field::<String>(v, "source")?,
                field(v, "columns")?,
            );
            Ok(
                json!({"kind":"Fts5","name":i.name,"source_table":i.source_table,"columns":i.columns,"tokenizer":i.tokenizer}),
            )
        }
        "helper_vector_defaults" => {
            let i = db::VectorIndex::new(
                field::<String>(v, "name")?,
                field::<String>(v, "source")?,
                field::<String>(v, "column")?,
                field(v, "dimensions")?,
            );
            Ok(
                json!({"kind":"Vector","name":i.name,"source_table":i.source_table,"column":i.column,"dimensions":i.dimensions,"distance":format!("{:?}",i.distance)}),
            )
        }
        "helper_file_defaults" => {
            let f = net::files::Config::new(
                field(v, "directory")?,
                field(v, "key")?,
                field(v, "fetch_bind")?,
            );
            Ok(
                json!({"directory":f.directory,"key":*f.key,"generation":f.generation,"fetch_bind":f.fetch_bind,"max_file_bytes":f.max_file_bytes,"max_staging_bytes":f.max_staging_bytes,"concurrent_fetches":f.concurrent_fetches,"fetch_interval":f.fetch_interval.as_nanos() as u64}),
            )
        }
        "helper_bridge_exporter_defaults" => Ok(bridge_json(&net::bridge::Config::exporter(
            field(v, "domain")?,
            field(v, "directory")?,
            field(v, "stream")?,
            field(v, "signer")?,
            field(v, "recipient")?,
        ))),
        "helper_bridge_receiver_defaults" => {
            let mut signers = BTreeMap::new();
            let mut origins = BTreeMap::new();
            for entry in v["trusted_signers"]
                .as_array()
                .ok_or_else(|| invalid("trusted signers"))?
            {
                signers.insert(field(entry, "node")?, field(entry, "key")?);
            }
            for entry in v["trusted_origins"]
                .as_array()
                .ok_or_else(|| invalid("trusted origins"))?
            {
                origins.insert(field(entry, "node")?, field(entry, "key")?);
            }
            Ok(bridge_json(&net::bridge::Config::receiver(
                field(v, "domain")?,
                field(v, "directory")?,
                field(v, "stream")?,
                field(v, "receiver_secret")?,
                field(v, "trusted_domains")?,
                signers,
                origins,
            )))
        }
        "helper_node_id" => {
            encoded(net::transport::node_id(&field::<Vec<u8>>(v, "der")?).map_err(network)?)
        }
        "helper_network_allowed" => encoded(net::transport::network_allowed(
            &config::replication_unchecked(&v["config"])?,
            field(v, "address")?,
        )),
        "helper_public_key" => encoded(net::protocol::public_key(&field(v, "seed")?)),
        "helper_recipient_public" => encoded(net::bridge::recipient_public(field(v, "secret")?)),
        "helper_hex" => encoded(net::hex(&field::<Vec<u8>>(v, "bytes")?)),
        "helper_constants" => Ok(
            json!({"version":net::protocol::VERSION,"alpn":net::protocol::ALPN,"frame_bytes":net::protocol::FRAME_BYTES,"chunk_bytes":net::protocol::CHUNK_BYTES,"cap_signed_schema":net::protocol::CAP_SIGNED_SCHEMA,"capabilities":net::protocol::CAPABILITIES,"required":net::protocol::REQUIRED,"sign_domain":net::protocol::SIGN_DOMAIN,"schema_sign_domain":net::protocol::SCHEMA_SIGN_DOMAIN,"coverage_key":net::retention::COVERAGE_KEY,"out":net::snapshot::OUT,"in":net::snapshot::IN,"receipts":net::snapshot::RECEIPTS,"pins":net::snapshot::PINS,"build_grace_millis":net::snapshot::BUILD_GRACE_MILLIS,"build_stale_millis":net::snapshot::BUILD_STALE_MILLIS,"build_fresh_margin_millis":net::snapshot::BUILD_FRESH_MARGIN_MILLIS}),
        ),
        "helper_validate_options" => {
            let o = config::options(&v["options"], callback)?;
            config::scheduler(&v["options"]["scheduler"])?;
            let _ = o;
            Ok(Json::Null)
        }
        "helper_validate_replication" => {
            config::replication(&v["config"])?;
            Ok(Json::Null)
        }
        "helper_validate_files" => {
            config::files(&v["config"])?;
            Ok(Json::Null)
        }
        "helper_validate_bridge" => {
            config::bridge(&v["config"])?;
            Ok(Json::Null)
        }
        "helper_key" => keys(v),
        "helper_digest" => encoded(net::protocol::schema_digest(&field::<Vec<u8>>(v, "bytes")?)),
        "helper_now" => encoded(net::retention::now_millis()),
        "codec_encode" | "codec_decode" => codec(v),
        "helper_transaction" => {
            let mut t: net::protocol::Transaction = field(v, "value")?;
            match v["method"].as_str() {
                Some("sign") => {
                    t.sign(&field(v, "key")?).map_err(network)?;
                    encoded(t)
                }
                Some("verify") => {
                    t.verify(field(v, "database")?, &trust(v)?, field(v, "max")?)
                        .map_err(network)?;
                    Ok(Json::Null)
                }
                Some("verify_header") => {
                    t.verify_header(field(v, "database")?, &trust(v)?)
                        .map_err(network)?;
                    Ok(Json::Null)
                }
                Some("digest") => encoded(t.digest().map_err(network)?),
                _ => Err(invalid("transaction helper")),
            }
        }
        "helper_signed_schema" => {
            let mut t: net::protocol::SignedSchema = field(v, "value")?;
            match v["method"].as_str() {
                Some("sign") => {
                    t.sign(&field(v, "key")?).map_err(network)?;
                    encoded(t)
                }
                Some("verify") => {
                    t.verify(
                        field(v, "database")?,
                        field(v, "master")?,
                        &trust(v)?,
                        field(v, "max")?,
                    )
                    .map_err(network)?;
                    Ok(Json::Null)
                }
                Some("digest") => encoded(t.digest()),
                _ => Err(invalid("signed schema helper")),
            }
        }
        "helper_policy" => policy(v),
        "helper_snapshot" => snapshot(v),
        "helper_local_records" => encoded(
            net::protocol::local_records(
                backend(field(v, "backend")?)?.as_ref(),
                field(v, "payload")?,
                &field::<Vec<u8>>(v, "schema")?,
                field(v, "millis")?,
                field(v, "logical")?,
                &field(v, "signing")?,
            )
            .map_err(network)?,
        ),
        _ => Err(invalid(format!("native helper {op}"))),
    }
}
fn trust(v: &Json) -> db::Result<BTreeMap<net::NodeId, [u8; 32]>> {
    let mut map = BTreeMap::new();
    for item in v["trusted"]
        .as_array()
        .ok_or_else(|| invalid("trust entries"))?
    {
        map.insert(field(item, "node")?, field(item, "key")?);
    }
    Ok(map)
}
pub fn entry(id: u64, entry: &Entry, v: &Json, callback: Option<Callback>) -> db::Result<Json> {
    let op = v["op"].as_str().unwrap_or("");
    if op == "release" {
        remove(id);
        return Ok(Json::Null);
    }
    match entry {
        Entry::Objects(o) => match op {
            "upload" => encoded(
                o.upload(
                    &field::<String>(v, "name")?,
                    Reader(dispatch::foreign(v, callback)?),
                )
                .map_err(network)?,
            ),
            "read" => encoded(
                o.read(
                    &field::<String>(v, "name")?,
                    Writer(dispatch::foreign(v, callback)?),
                )
                .map_err(network)?,
            ),
            "metadata" => encoded(o.metadata(&field::<String>(v, "name")?).map_err(network)?),
            "list" => encoded(o.list().map_err(network)?),
            "available" => encoded(o.available(&field::<String>(v, "name")?).map_err(network)?),
            "availability" => encoded(
                o.availability(&field::<String>(v, "name")?)
                    .map_err(network)?,
            ),
            "delete" => {
                o.delete(&field::<String>(v, "name")?).map_err(network)?;
                Ok(Json::Null)
            }
            "last_fetch_error" => encoded(o.last_fetch_error()),
            "rotate_keys" => encoded(o.rotate_keys(field(v, "limit")?).map_err(network)?),
            "key_rotation_status" => encoded(o.key_rotation_status().map_err(network)?),
            "collect" => encoded(
                o.collect(std::time::Duration::from_nanos(field(v, "grace")?))
                    .map_err(network)?,
            ),
            "path" => encoded(o.path(field(v, "digest")?, field(v, "generation")?)),
            "publish_fetched" => {
                o.publish_fetched(
                    Path::new(&field::<String>(v, "path")?),
                    &field(v, "metadata")?,
                )
                .map_err(network)?;
                Ok(Json::Null)
            }
            "verify" => {
                o.verify(
                    Path::new(&field::<String>(v, "path")?),
                    &field(v, "metadata")?,
                    Writer(dispatch::foreign(v, callback)?),
                )
                .map_err(network)?;
                Ok(Json::Null)
            }
            "stream_plain_chunks" => {
                let f = dispatch::foreign(v, callback)?;
                encoded(
                    o.stream_plain_chunks(
                        &field::<String>(v, "name")?,
                        field(v, "skip")?,
                        field(v, "chunk_size")?,
                        |offset, data| {
                            f.bytes(6, offset, data)
                                .map(|_| ())
                                .map_err(|e| net::Error::Invalid(e.to_string()))
                        },
                    )
                    .map_err(network)?,
                )
            }
            _ => Err(invalid("object operation")),
        },
        Entry::Bridge(b) => match op {
            "attach_files" => {
                let handle: Option<u64> = field(v, "files")?;
                let files = handle
                    .map(|h| {
                        let e = get(h)?;
                        if let Entry::Objects(o) = e.as_ref() {
                            Ok(o.clone())
                        } else {
                            Err(invalid("object handle"))
                        }
                    })
                    .transpose()?;
                b.attach_files(files);
                Ok(Json::Null)
            }
            "capture" => encoded(b.capture().map_err(network)?),
            "process" => encoded(b.process().map_err(network)?),
            "export" => encoded(
                b.export(&field::<Vec<u8>>(v, "payload")?)
                    .map_err(network)?,
            ),
            "export_snapshot" => encoded(b.export_snapshot().map_err(network)?),
            "export_file" => {
                b.export_file(&field::<String>(v, "name")?)
                    .map_err(network)?;
                Ok(Json::Null)
            }
            "open_bundle" => encoded(
                b.open_bundle(&field::<Vec<u8>>(v, "bytes")?)
                    .map_err(network)?,
            ),
            "stage" => encoded(
                b.stage(Path::new(&field::<String>(v, "path")?))
                    .map_err(network)?,
            ),
            "release_ownership" => encoded(
                b.release(
                    field::<String>(v, "table")?,
                    field(v, "key")?,
                    field(v, "column")?,
                )
                .map_err(network)?,
            ),
            "publish_http" => {
                runtime()
                    .block_on(b.publish_http(
                        Path::new(&field::<String>(v, "path")?),
                        &field::<String>(v, "url")?,
                    ))
                    .map_err(network)?;
                Ok(Json::Null)
            }
            "publish_ftp" => {
                let tls = if v["tls"].is_null() {
                    None
                } else {
                    Some(net::bridge::FtpOptions {
                        roots_pem: field(&v["tls"], "roots_pem")?,
                        timeout: std::time::Duration::from_nanos(field(&v["tls"], "timeout")?),
                    })
                };
                runtime()
                    .block_on(b.publish_ftp(
                        Path::new(&field::<String>(v, "path")?),
                        &field::<String>(v, "url")?,
                        tls.as_ref(),
                    ))
                    .map_err(network)?;
                Ok(Json::Null)
            }
            _ => Err(invalid("bridge operation")),
        },
        Entry::Backend(_) => {
            if op == "close" {
                remove(id);
                Ok(Json::Null)
            } else {
                Err(invalid("backend operation"))
            }
        }
        Entry::Endpoint(e) => match op {
            "address" => encoded(e.local_addr()?),
            "connect" => {
                let c = runtime().block_on(async {
                    let _enter = runtime().enter();
                    e.connect(field(v, "address")?, "murmur")
                        .map_err(invalid)?
                        .await
                        .map_err(invalid)
                })?;
                Ok(json!(put(Entry::Connection(c))))
            }
            "accept" => {
                let c = runtime().block_on(async {
                    let incoming = e.accept().await.ok_or(db::Error::Closed)?;
                    incoming.await.map_err(invalid)
                })?;
                Ok(json!(put(Entry::Connection(c))))
            }
            "wait_idle" => {
                runtime().block_on(e.wait_idle());
                Ok(Json::Null)
            }
            "close" => {
                e.close(0u32.into(), b"closed");
                remove(id);
                Ok(Json::Null)
            }
            _ => Err(invalid("endpoint operation")),
        },
        Entry::Connection(c) => match op {
            "peer_id" => encoded(
                net::transport::peer_id(c, &config::replication(&v["config"])?).map_err(network)?,
            ),
            "address" => encoded(c.remote_address()),
            "open_bi" | "accept_bi" => {
                let (send, recv) = if op == "open_bi" {
                    runtime().block_on(c.open_bi()).map_err(invalid)?
                } else {
                    runtime().block_on(c.accept_bi()).map_err(invalid)?
                };
                Ok(json!([
                    put(Entry::Send(Stream::new(send))),
                    put(Entry::Receive(Stream::new(recv)))
                ]))
            }
            "close" => {
                c.close(0u32.into(), b"closed");
                remove(id);
                Ok(Json::Null)
            }
            _ => Err(invalid("connection operation")),
        },
        Entry::Send(s) => match op {
            "write_message" => {
                let m: net::protocol::Message = field(v, "message")?;
                let mut inner = s.inner.lock().unwrap();
                runtime().block_on(async {
                    let notified=s.notify.notified();tokio::pin!(notified);notified.as_mut().enable();
                    if s.closed.load(Ordering::Acquire){return Err(db::Error::Closed);}
                    tokio::select!{result=net::protocol::write(&mut inner,&m)=>result.map_err(network),_=&mut notified=>{let _=inner.reset(0u32.into());Err(db::Error::Closed)}}
                })?;
                Ok(Json::Null)
            }
            "finish" => {
                if s.closed.load(Ordering::Acquire) {
                    return Err(db::Error::Closed);
                }
                s.inner.lock().unwrap().finish().map_err(invalid)?;
                Ok(Json::Null)
            }
            "close" => {
                s.close();
                if let Ok(mut inner) = s.inner.try_lock() {
                    let _ = inner.reset(0u32.into());
                }
                remove(id);
                Ok(Json::Null)
            }
            _ => Err(invalid("send operation")),
        },
        Entry::Receive(r) => match op {
            "read_message" => {
                let mut inner = r.inner.lock().unwrap();
                let message=runtime().block_on(async{
                let notified=r.notify.notified();tokio::pin!(notified);notified.as_mut().enable();
                if r.closed.load(Ordering::Acquire){return Err(db::Error::Closed);}
                tokio::select!{result=net::protocol::read(&mut inner)=>result.map_err(network),_=&mut notified=>{let _=inner.stop(0u32.into());Err(db::Error::Closed)}}
            })?;
                encoded(message)
            }
            "close" => {
                r.close();
                if let Ok(mut inner) = r.inner.try_lock() {
                    let _ = inner.stop(0u32.into());
                }
                remove(id);
                Ok(Json::Null)
            }
            _ => Err(invalid("receive operation")),
        },
        _ => Err(invalid("native object operation")),
    }
}
fn keys(v: &Json) -> db::Result<Json> {
    let kind: String = field(v, "kind")?;
    let origin: net::NodeId = decode(v["origin"].clone()).unwrap_or_default();
    let seq = v["sequence"].as_u64().unwrap_or(0);
    let index = v["index"].as_u64().unwrap_or(0) as u32;
    let transfer: net::NodeId = decode(v["transfer"].clone()).unwrap_or_default();
    let result = match kind.as_str() {
        "log" => net::protocol::log_key(origin, seq),
        "stage" => net::protocol::stage_key(origin, seq),
        "progress" => net::protocol::progress_key(origin),
        "receipt" => net::protocol::receipt_key(origin, seq),
        "sequence" => net::protocol::sequence_key(origin),
        "schema_manifest" => net::protocol::schema_manifest_key(),
        "schema_accepted" => net::protocol::schema_accepted_key(origin),
        "observed" => net::protocol::observed_key(origin),
        "retained" => net::protocol::retained_key(origin),
        "chunk" => net::protocol::chunk_key(origin, seq, index),
        "chunk_manifest" => net::protocol::chunk_manifest_key(origin, seq),
        "chunk_prefix" => net::protocol::chunk_prefix(origin, seq).into_bytes(),
        "time" => net::retention::time_key(origin, seq),
        "floor" => net::retention::floor_key(origin),
        "snapshot_header" => net::snapshot::header(&field::<Vec<u8>>(v, "prefix")?, transfer),
        "snapshot_chunk" => net::snapshot::chunk(&field::<Vec<u8>>(v, "prefix")?, transfer, index),
        "snapshot_pin" => net::snapshot::pin_key(transfer),
        "snapshot_receipt" => net::snapshot::receipt_key(transfer),
        _ => return Err(invalid("key helper")),
    };
    encoded(result)
}
fn codec(v: &Json) -> db::Result<Json> {
    let ty = v["type"].as_str().unwrap_or("");
    let encoding = v["op"] == "codec_encode";
    match ty {
        "PeerRetention" => {
            let p = if encoding {
                let x = &v["value"];
                net::retention::PeerRetention {
                    node: field(x, "node")?,
                    last_seen_millis: field(x, "last_seen_millis")?,
                    retired: field(x, "retired")?,
                    acknowledged: field::<Vec<(net::NodeId, u64)>>(x, "acknowledged")?
                        .into_iter()
                        .collect(),
                }
            } else {
                net::decode::<net::retention::PeerRetention>(&field::<Vec<u8>>(v, "bytes")?)
                    .map_err(network)?
            };
            return if encoding {
                encoded(net::encode(&p).map_err(network)?)
            } else {
                Ok(
                    json!({"node":p.node,"last_seen_millis":p.last_seen_millis,"retired":p.retired,"acknowledged":p.acknowledged.into_iter().collect::<Vec<_>>()}),
                )
            };
        }
        "Counter" => {
            let c = if encoding {
                read_counter(&v["value"])?
            } else {
                net::decode::<net::policies::Counter>(&field::<Vec<u8>>(v, "bytes")?)
                    .map_err(network)?
            };
            return if encoding {
                encoded(net::encode(&c).map_err(network)?)
            } else {
                Ok(counter_json(&c))
            };
        }
        "Set" => {
            let s = if encoding {
                read_set(&v["value"])?
            } else {
                net::decode::<net::policies::Set>(&field::<Vec<u8>>(v, "bytes")?)
                    .map_err(network)?
            };
            return if encoding {
                encoded(net::encode(&s).map_err(network)?)
            } else {
                Ok(set_json(&s))
            };
        }
        "Causal" => {
            let c = if encoding {
                let x = &v["value"];
                if !x["Counter"].is_null() && x["Set"].is_null() {
                    net::policies::Causal::Counter(read_counter(&x["Counter"])?)
                } else if !x["Set"].is_null() && x["Counter"].is_null() {
                    net::policies::Causal::Set(read_set(&x["Set"])?)
                } else {
                    return Err(invalid("causal variant"));
                }
            } else {
                net::decode::<net::policies::Causal>(&field::<Vec<u8>>(v, "bytes")?)
                    .map_err(network)?
            };
            return if encoding {
                encoded(net::encode(&c).map_err(network)?)
            } else {
                Ok(match c {
                    net::policies::Causal::Counter(c) => {
                        json!({"Counter":counter_json(&c),"Set":null})
                    }
                    net::policies::Causal::Set(s) => json!({"Counter":null,"Set":set_json(&s)}),
                })
            };
        }
        "Pin" => {
            let pin = if encoding {
                let x = &v["value"];
                net::snapshot::Pin {
                    recipient: field(x, "recipient")?,
                    boundary: field::<Vec<(net::NodeId, u64)>>(x, "boundary")?
                        .into_iter()
                        .collect(),
                    target: field::<Option<Vec<(net::NodeId, u64)>>>(x, "target")?
                        .map(|v| v.into_iter().collect()),
                    expires: field(x, "expires")?,
                    digest: field(x, "digest")?,
                }
            } else {
                net::decode::<net::snapshot::Pin>(&field::<Vec<u8>>(v, "bytes")?)
                    .map_err(network)?
            };
            return if encoding {
                encoded(net::encode(&pin).map_err(network)?)
            } else {
                Ok(
                    json!({"recipient":pin.recipient,"boundary":pin.boundary.into_iter().collect::<Vec<_>>(),"target":pin.target.map(|v|v.into_iter().collect::<Vec<_>>()),"expires":pin.expires,"digest":pin.digest}),
                )
            };
        }
        _ => {}
    }
    macro_rules! types {($($name:literal=>$ty:ty),* $(,)?)=>{match ty{$($name=>if encoding{let mut value=v["value"].clone();restore_durations(&mut value);encoded(net::encode(&decode::<$ty>(value)?).map_err(network)?)}else{encoded(net::decode::<$ty>(&field::<Vec<u8>>(v,"bytes")?).map_err(network)?)},)*_=>Err(invalid("unsupported codec type"))}}}
    types!("Value"=>db::Value,"Values"=>Vec<db::Value>,"ReplicaState"=>db::ReplicaState,"Schema"=>db::Schema,"Version"=>db::Version,"Column"=>db::Column,"Table"=>db::Table,"Cell"=>db::Cell,"RowState"=>db::RowState,"Tombstone"=>db::Tombstone,"MergePolicy"=>db::MergePolicy,"OpenPhase"=>db::OpenPhase,"OpenProgress"=>db::OpenProgress,"SqliteMetrics"=>db::SqliteMetrics,"SpoolMetrics"=>db::SpoolMetrics,"Status"=>db::Status,"SearchIndexStatus"=>db::SearchIndexStatus,"SchedulerConfig"=>db::SchedulerConfig,"SchedulerSnapshot"=>db::SchedulerSnapshot,"SchedulerClassStats"=>db::SchedulerClassStats,"WriterClass"=>db::WriterClass,"SubscriptionCursor"=>db::SubscriptionCursor,"Record"=>net::Record,"Records"=>Vec<net::Record>,"Content"=>net::Content,"Transaction"=>net::protocol::Transaction,"SignedSchema"=>net::protocol::SignedSchema,"Message"=>net::protocol::Message,"Progress"=>net::protocol::Progress,"ProgressList"=>Vec<net::protocol::Progress>,"Chunk"=>net::protocol::Chunk,"ChunkManifest"=>net::protocol::ChunkManifest,"Manifest"=>net::snapshot::Manifest,"Incoming"=>net::snapshot::Incoming,"Origin"=>net::bridge::Origin,"Header"=>net::bridge::Header,"Bundle"=>net::bridge::Bundle,"FileMetadata"=>net::files::Metadata,"FileAvailability"=>net::files::Availability,"FileKeyRotationStatus"=>net::files::KeyRotationStatus,"Role"=>net::bridge::Role,"Dissemination"=>net::Dissemination,"Policy"=>net::policies::Policy,"Tag"=>net::policies::Tag,"NodeId"=>net::NodeId,"Key"=>[u8;32],"ReplicationStatus"=>net::Status,"ReplicationMetrics"=>net::ReplicationMetrics,"PeerDiagnostics"=>net::PeerDiagnostics,"Bytes"=>Vec<u8>,"String"=>String,"U64"=>u64,"I64"=>i64,"Bool"=>bool,"F64"=>f64)
}
fn restore_durations(v: &mut Json) {
    match v {
        Json::Object(m) => {
            for (k, x) in m {
                if matches!(
                    k.as_str(),
                    "elapsed" | "phase_elapsed" | "checkpoint_pause" | "max_debt"
                ) {
                    if let Some(n) = x.as_u64() {
                        *x = json!({"secs":n/1_000_000_000,"nanos":n%1_000_000_000});
                    }
                } else {
                    restore_durations(x)
                }
            }
        }
        Json::Array(a) => {
            for x in a {
                restore_durations(x)
            }
        }
        _ => {}
    }
}
fn read_counter(v: &Json) -> db::Result<net::policies::Counter> {
    let mut c = net::policies::Counter::default();
    for (d, n, p, m) in field::<Vec<(net::NodeId, net::NodeId, u64, u64)>>(v, "components")? {
        c.positive.insert((d, n), p);
        c.negative.insert((d, n), m);
    }
    Ok(c)
}
fn counter_json(c: &net::policies::Counter) -> Json {
    let mut actors = std::collections::BTreeSet::new();
    actors.extend(c.positive.keys());
    actors.extend(c.negative.keys());
    json!({"components":actors.into_iter().map(|(d,n)|(d,n,*c.positive.get(&(d,n)).unwrap_or(&0),*c.negative.get(&(d,n)).unwrap_or(&0))).collect::<Vec<_>>()})
}
fn set_json(s: &net::policies::Set) -> Json {
    json!({"additions":s.additions.iter().collect::<Vec<_>>(),"removals":s.removals})
}

fn policy(v: &Json) -> db::Result<Json> {
    match v["kind"].as_str() {
        Some("Counter") => {
            let entries: Vec<(net::NodeId, net::NodeId, u64, u64)> = field(v, "components")?;
            let mut c = net::policies::Counter::default();
            for (d, n, p, m) in entries {
                c.positive.insert((d, n), p);
                c.negative.insert((d, n), m);
            }
            match v["method"].as_str() {
                Some("add") => c
                    .add(field(v, "database")?, field(v, "node")?, field(v, "delta")?)
                    .map_err(network)?,
                Some("value") => return encoded(c.value().map_err(network)?),
                Some("join") => {
                    let mut other = net::policies::Counter::default();
                    for (d, n, p, m) in
                        field::<Vec<(net::NodeId, net::NodeId, u64, u64)>>(v, "other")?
                    {
                        other.positive.insert((d, n), p);
                        other.negative.insert((d, n), m);
                    }
                    c.join(&other).map_err(network)?;
                }
                _ => return Err(invalid("counter method")),
            };
            let mut actors = std::collections::BTreeSet::new();
            actors.extend(c.positive.keys());
            actors.extend(c.negative.keys());
            encoded(
                actors
                    .into_iter()
                    .map(|(d, n)| {
                        (
                            d,
                            n,
                            *c.positive.get(&(d, n)).unwrap_or(&0),
                            *c.negative.get(&(d, n)).unwrap_or(&0),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        }
        Some("Set") => {
            let mut s = read_set(&v["state"])?;
            match v["method"].as_str() {
                Some("add") => s.add(field(v, "value")?, field(v, "tag")?),
                Some("remove") => s.remove(&field::<Vec<u8>>(v, "value")?),
                Some("join") => s.join(&read_set(&v["other"])?),
                Some("values") => return encoded(s.values()),
                _ => return Err(invalid("set method")),
            };
            Ok(
                json!({"additions":s.additions.into_iter().collect::<Vec<_>>(),"removals":s.removals}),
            )
        }
        _ => Err(invalid("policy kind")),
    }
}
fn read_set(v: &Json) -> db::Result<net::policies::Set> {
    Ok(net::policies::Set {
        additions: field::<Vec<(Vec<u8>, std::collections::BTreeSet<net::policies::Tag>)>>(
            v,
            "additions",
        )?
        .into_iter()
        .collect(),
        removals: field(v, "removals")?,
    })
}
fn snapshot(v: &Json) -> db::Result<Json> {
    let method = v["method"].as_str().unwrap_or("");
    let now = v["now"].as_u64().unwrap_or_else(net::retention::now_millis);
    if method == "deadline" {
        return encoded(net::snapshot::deadline(
            &config::replication(&v["config"])?,
            now,
        ));
    }
    if method == "build" {
        return encoded(
            net::snapshot::build(
                field(v, "identity")?,
                field(v, "recipient")?,
                field(v, "data")?,
                field(v, "progress")?,
                field(v, "schema")?,
                &config::replication(&v["config"])?,
                now,
            )
            .map_err(network)?,
        );
    }
    if method == "build_stream" {
        let b=backend(field(v,"backend")?)?;
        let pages:Vec<Vec<u8>>=field(v,"pages")?;
        return encoded(net::snapshot::build_stream(field(v,"identity")?,field(v,"recipient")?,pages.into_iter().map(Ok),field(v,"progress")?,field(v,"schema")?,&config::replication(&v["config"])?,now,|records|b.commit(records)).map_err(network)?);
    }
    let b = backend(field(v, "backend")?)?;
    match method {
        "verify" => {
            field::<net::snapshot::Manifest>(v, "manifest")?
                .verify(
                    b.as_ref(),
                    &config::replication(&v["config"])?,
                    field(v, "source")?,
                    now,
                )
                .map_err(network)?;
            Ok(Json::Null)
        }
        "outgoing" => encoded(
            net::snapshot::outgoing(
                b.as_ref(),
                field(v, "recipient")?,
                field(v, "transfer")?,
                now,
            )
            .map_err(network)?,
        ),
        "cancel" => {
            net::snapshot::cancel(b.as_ref(), field(v, "recipient")?, field(v, "transfer")?)
                .map_err(network)?;
            Ok(Json::Null)
        }
        "incoming" => encoded(net::snapshot::incoming(b.as_ref()).map_err(network)?),
        "accept" => encoded(
            net::snapshot::accept(
                b.as_ref(),
                &config::replication(&v["config"])?,
                field(v, "source")?,
                field(v, "manifest")?,
                now,
            )
            .map_err(network)?,
        ),
        "receive" => encoded(
            net::snapshot::receive(
                b.as_ref(),
                field(v, "source")?,
                field(v, "transfer")?,
                field(v, "index")?,
                field(v, "data")?,
                now,
            )
            .map_err(network)?,
        ),
        "missing" => {
            encoded(net::snapshot::missing(b.as_ref(), &field(v, "incoming")?).map_err(network)?)
        }
        "finish" => encoded(
            net::snapshot::finish(
                b.as_ref(),
                &config::replication(&v["config"])?,
                field(v, "source")?,
                field(v, "transfer")?,
                now,
            )
            .map_err(network)?,
        ),
        "prune_limit" => encoded(
            net::snapshot::prune_limit(b.as_ref(), field(v, "origin")?, now).map_err(network)?,
        ),
        "maintain" => {
            net::snapshot::maintain(b.as_ref(), now).map_err(network)?;
            Ok(Json::Null)
        }
        _ => Err(invalid("snapshot method")),
    }
}

fn file_json(f: &net::files::Config) -> Json {
    json!({"directory":f.directory,"key":*f.key,"previous_key":f.previous_key.as_deref(),"generation":f.generation,"fetch_bind":f.fetch_bind,"max_file_bytes":f.max_file_bytes,"max_staging_bytes":f.max_staging_bytes,"concurrent_fetches":f.concurrent_fetches,"fetch_interval":f.fetch_interval.as_nanos() as u64})
}
fn bridge_json(b: &net::bridge::Config) -> Json {
    json!({"domain":b.domain,"trusted_domains":b.trusted_domains,"trusted_origins":b.trusted_origins.iter().map(|(node,key)|json!({"node":node,"key":key})).collect::<Vec<_>>(),"trusted_signers":b.trusted_signers.iter().map(|(node,key)|json!({"node":node,"key":key})).collect::<Vec<_>>(),"role":b.role,"directory":b.directory,"stream":b.stream,"recipient":b.recipient,"receiver_secret":b.receiver_secret.as_deref(),"signer":b.signer.as_deref(),"max_bundle_bytes":b.max_bundle_bytes,"max_payload_bytes":b.max_payload_bytes,"staging_bytes":b.staging_bytes})
}
