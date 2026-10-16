use crate::*;
use std::time::Duration;

macro_rules! set {($dst:expr,$v:expr,$($name:ident),* $(,)?)=>{$(if let Some(x)=$v.get(stringify!($name)).filter(|x|!x.is_null()){ $dst.$name=decode(x.clone())?; })*}}
macro_rules! durations {($dst:expr,$v:expr,$($name:ident),* $(,)?)=>{$(if let Some(x)=$v.get(stringify!($name)).filter(|x|!x.is_null()){ $dst.$name=Duration::from_nanos(decode(x.clone())?); })*}}
pub fn options(v: &Json, callback: Option<Callback>) -> db::Result<db::Options> {
    let mut o = db::Options::default();
    set!(
        o,
        v,
        capture_bytes,
        queue_bytes,
        transaction_bytes,
        segment_bytes,
        spool_block_bytes,
        spool_cache_bytes,
        snapshot_deltas,
        snapshot_bytes,
        compression_level,
        storage_workers,
        search_queue_bytes,
        search_batch_rows,
        database_id,
        node_id
    );
    durations!(o, v, capture_interval, search_interval);
    if let Some(age) = v.get("data_key_max_age") {
        o.data_key_max_age = if age.is_null() {
            None
        } else {
            Some(Duration::from_nanos(decode(age.clone())?))
        };
    }
    if let Some(mode) = v.get("mode") {
        o.mode = match mode.as_str() {
            Some("Existing") => db::OpenMode::Existing,
            Some("CreateNew") => db::OpenMode::CreateNew,
            _ => return Err(invalid("open mode")),
        };
    }
    if let Some(s) = v.get("scheduler") {
        o.scheduler = scheduler(s)?;
    }
    if let Some(s) = v.get("search_indexes").filter(|x| !x.is_null()) {
        o.search_indexes = indexes(s)?;
    }
    if let Some(c) = v.get("replication").filter(|c| !c.is_null()) {
        o.replication = Some(replication(c)?);
    }
    if let Some(token) = v
        .get("progress_callback")
        .and_then(Json::as_u64)
        .filter(|n| *n != 0)
    {
        let foreign = Foreign::new(
            token,
            callback.ok_or_else(|| invalid("callback function missing"))?,
        );
        o.on_open_progress = Some(Arc::new(move |p| {
            let _ = foreign.json(2, 0, encoded(p).unwrap_or(Json::Null));
        }));
    }
    if let Some(token) = v
        .get("fault_callback")
        .and_then(Json::as_u64)
        .filter(|n| *n != 0)
    {
        let f = Foreign::new(
            token,
            callback.ok_or_else(|| invalid("callback function missing"))?,
        );
        fn hook(f: &Foreign, name: &'static str) -> spool::FaultHook {
            let f = f.clone();
            Arc::new(move || {
                f.json(11, 0, json!(name))
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        }
        o.fault_hooks = Some(Arc::new(spool::FaultHooks {
            append: Some(hook(&f, "append")),
            segment_sync: Some(hook(&f, "sync")),
            manifest_persist: Some(hook(&f, "manifest")),
            keyring_persist: Some(hook(&f, "keyring")),
            intent_persist: Some(hook(&f, "intent")),
            compaction_stage: Some(hook(&f, "compact")),
            checkpoint_link: Some(hook(&f, "checkpoint")),
            rename: Some(hook(&f, "rename")),
            dir_sync: Some(hook(&f, "dirsync")),
            delete: Some(hook(&f, "delete")),
        }));
    }

    Ok(o)
}
pub fn scheduler(v: &Json) -> db::Result<db::SchedulerConfig> {
    let mut s = db::SchedulerConfig::default();
    set!(s, v, local_share, remote_share, maintenance_reserve_every);
    durations!(s, v, max_debt);
    s.validate()?;
    Ok(s)
}
pub fn indexes(v: &Json) -> db::Result<Vec<db::SearchIndex>> {
    let mut out = Vec::new();
    for index in v.as_array().ok_or_else(|| invalid("index definitions"))? {
        let kind: String = field(index, "kind")?;
        let name: String = field(index, "name")?;
        let source: String = field(index, "source_table")?;
        match kind.as_str() {
            "Fts5" => {
                let mut f =
                    db::Fts5Index::new(name, source, field::<Vec<String>>(index, "columns")?);
                set!(f, index, tokenizer);
                out.push(f.into());
            }
            "Vector" => {
                let mut vector = db::VectorIndex::new(
                    name,
                    source,
                    field::<String>(index, "column")?,
                    field(index, "dimensions")?,
                );
                if let Some(d) = index.get("distance") {
                    vector.distance = match d.as_str() {
                        Some("L2") => db::DistanceMetric::L2,
                        Some("L1") => db::DistanceMetric::L1,
                        Some("Cosine") => db::DistanceMetric::Cosine,
                        _ => return Err(invalid("distance metric")),
                    };
                }
                out.push(vector.into());
            }
            _ => return Err(invalid("index kind")),
        };
    }
    Ok(out)
}
fn trusts(v: &Json, key: &str) -> db::Result<std::collections::BTreeMap<net::NodeId, [u8; 32]>> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(items) = v.get(key).filter(|x| !x.is_null()) {
        for item in items.as_array().ok_or_else(|| invalid("trust entries"))? {
            out.insert(field(item, "node")?, field(item, "key")?);
        }
    }
    Ok(out)
}
pub fn replication(v: &Json) -> db::Result<net::Config> {
    let c = replication_unchecked(v)?;
    c.validate().map_err(network)?;
    Ok(c)
}
pub fn replication_unchecked(v: &Json) -> db::Result<net::Config> {
    let credentials = v.get("credentials").ok_or_else(|| invalid("credentials"))?;
    let creds = net::Credentials {
        chain: field(credentials, "chain")?,
        private_key: zeroize::Zeroizing::new(field(credentials, "private_key")?),
        roots: field(credentials, "roots")?,
    };
    let mut c = net::Config::new(field(v, "bind")?, creds, field(v, "signing_key")?);
    set!(
        c,
        v,
        advertise,
        allowed_peers,
        allowed_networks,
        trusted_snapshot_sources,
        bootstrap,
        membership,
        dissemination,
        accept_remote_schema,
        master,
        schema_master,
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
    durations!(
        c,
        v,
        snapshot_transfer_lifetime,
        incoming_interval,
        anti_entropy_interval,
        retention,
        retention_interval,
        maximum_offline
    );
    c.trusted_origins = trusts(v, "trusted_origins")?;
    if let Some(policies) = v.get("merge_policies").filter(|x| !x.is_null()) {
        for p in policies
            .as_array()
            .ok_or_else(|| invalid("merge policies"))?
        {
            c.merge_policies.insert(
                (field(p, "table")?, field(p, "column")?),
                field(p, "policy")?,
            );
        }
    }
    if let Some(f) = v.get("files").filter(|f| !f.is_null()) {
        c.files = Some(files(f)?);
    }
    if let Some(b) = v.get("bridge").filter(|b| !b.is_null()) {
        c.bridge = Some(bridge(b)?);
    }
    Ok(c)
}
pub fn files(v: &Json) -> db::Result<net::files::Config> {
    let mut f = net::files::Config::new(
        field(v, "directory")?,
        field(v, "key")?,
        field(v, "fetch_bind")?,
    );
    set!(
        f,
        v,
        generation,
        max_file_bytes,
        max_staging_bytes,
        concurrent_fetches
    );
    durations!(f, v, fetch_interval);
    if let Some(k) = v.get("previous_key").filter(|k| !k.is_null()) {
        f.previous_key = Some(zeroize::Zeroizing::new(decode(k.clone())?));
    }
    f.validate().map_err(network)?;
    Ok(f)
}
pub fn bridge(v: &Json) -> db::Result<net::bridge::Config> {
    let domain = field(v, "domain")?;
    let dir = field(v, "directory")?;
    let stream = field(v, "stream")?;
    let mut b = match v.get("role").and_then(Json::as_str) {
        Some("LowExporter") => net::bridge::Config::exporter(
            domain,
            dir,
            stream,
            field(v, "signer")?,
            field(v, "recipient")?,
        ),
        Some("HighReceiver") => net::bridge::Config::receiver(
            domain,
            dir,
            stream,
            field(v, "receiver_secret")?,
            field(v, "trusted_domains")?,
            trusts(v, "trusted_signers")?,
            trusts(v, "trusted_origins")?,
        ),
        _ => return Err(invalid("bridge role")),
    };
    set!(b, v, max_bundle_bytes, max_payload_bytes, staging_bytes);
    b.validate().map_err(network)?;
    Ok(b)
}
pub fn defaults() -> Json {
    let o = db::Options::default();
    let s = db::SchedulerConfig::default();
    json!({"mode":"Existing","capture_interval":o.capture_interval.as_nanos() as u64,"capture_bytes":o.capture_bytes,"queue_bytes":o.queue_bytes,"transaction_bytes":o.transaction_bytes,"segment_bytes":o.segment_bytes,"spool_block_bytes":o.spool_block_bytes,"spool_cache_bytes":o.spool_cache_bytes,"snapshot_deltas":o.snapshot_deltas,"snapshot_bytes":o.snapshot_bytes,"compression_level":o.compression_level,"storage_workers":o.storage_workers,"search_queue_bytes":o.search_queue_bytes,"search_batch_rows":o.search_batch_rows,"search_interval":o.search_interval.as_nanos() as u64,"scheduler":{"local_share":s.local_share,"remote_share":s.remote_share,"max_debt":s.max_debt.as_nanos() as u64,"maintenance_reserve_every":s.maintenance_reserve_every},"search_indexes":[]})
}
