//! Durable peer snapshot transfers. Spool records are the only authoritative state.
use crate::{protocol::*, *};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};

pub const OUT: &[u8] = b"07/replication/snapshot-v1/out/";
pub const IN: &[u8] = b"07/replication/snapshot-v1/in/";
pub const RECEIPTS: &[u8] = b"07/replication/snapshot-v1/receipts/";
pub const PINS: &[u8] = b"07/replication/snapshot-v1/pins/";
const DOMAIN: &[u8] = b"murmur/sqlite-snapshot/v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub database: NodeId,
    pub donor: NodeId,
    pub recipient: NodeId,
    pub transfer: NodeId,
    pub expires: u64,
    pub bytes: u64,
    pub digest: [u8; 32],
    pub schema: [u8; 32],
    pub progress: Vec<Progress>,
    pub chunks: Vec<[u8; 32]>,
    pub signature: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Incoming {
    pub manifest: Manifest,
    pub deadline: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pin {
    pub recipient: NodeId,
    pub boundary: BTreeMap<NodeId, u64>,
    pub target: Option<BTreeMap<NodeId, u64>>,
    pub expires: u64,
    pub digest: [u8; 32],
}
pub fn header(prefix: &[u8], transfer: NodeId) -> Vec<u8> {
    format!(
        "{}{}/header",
        String::from_utf8_lossy(prefix),
        hex(&transfer)
    )
    .into_bytes()
}
pub fn chunk(prefix: &[u8], transfer: NodeId, index: u32) -> Vec<u8> {
    format!(
        "{}{}/chunk/{index:08}",
        String::from_utf8_lossy(prefix),
        hex(&transfer)
    )
    .into_bytes()
}
pub fn pin_key(transfer: NodeId) -> Vec<u8> {
    [PINS, hex(&transfer).as_bytes()].concat()
}
pub fn receipt_key(transfer: NodeId) -> Vec<u8> {
    [RECEIPTS, hex(&transfer).as_bytes()].concat()
}
pub fn deadline(config: &Config, now: u64) -> u64 {
    now.saturating_add(
        config
            .snapshot_transfer_lifetime
            .as_millis()
            .min(u64::MAX as u128) as u64,
    )
}
impl Manifest {
    fn signed_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = DOMAIN.to_vec();
        bytes.extend(encode(&(
            VERSION,
            self.database,
            self.donor,
            self.recipient,
            self.transfer,
            self.expires,
            self.bytes,
            self.digest,
            self.schema,
            &self.progress,
            &self.chunks,
        ))?);
        Ok(bytes)
    }
    pub fn verify(
        &self,
        backend: &dyn Backend,
        config: &Config,
        source: NodeId,
        now: u64,
    ) -> Result<()> {
        let (database, recipient) = backend.identity();
        if self.database != database
            || self.donor != source
            || self.recipient != recipient
            || !config.trusted_snapshot_sources.contains(&source)
            || self.expires <= now
        {
            return Err(Error::Auth("snapshot identity, trust or expiry".into()));
        }
        let bytes = usize::try_from(self.bytes).map_err(|_| Error::Overloaded)?;
        if bytes > config.snapshot_bytes
            || bytes > config.snapshot_staging_bytes
            || self.progress.len() > 4096
            || self.chunks.len() != bytes.div_ceil(CHUNK_BYTES)
        {
            return Err(Error::Overloaded);
        }
        let mut origins = BTreeSet::new();
        for p in &self.progress {
            if !origins.insert(p.origin)
                || p.observed < p.applied
                || (p.retained_from > p.retained_through && p.retained_through != 0)
            {
                return Err(Error::Protocol("snapshot progress".into()));
            }
        }
        let public = config
            .trusted_origins
            .get(&source)
            .ok_or_else(|| Error::Auth("snapshot signer".into()))?;
        let key = VerifyingKey::from_bytes(public).map_err(|e| Error::Auth(e.to_string()))?;
        let signature =
            Signature::from_slice(&self.signature).map_err(|e| Error::Auth(e.to_string()))?;
        key.verify_strict(&self.signed_bytes()?, &signature)
            .map_err(|e| Error::Auth(e.to_string()))
    }
}
/// Called inside the serialized capture operation. All records commit atomically;
/// a large commit spans Spool blocks, rather than enlarging its point-read blocks.
pub fn build(
    identity: (NodeId, NodeId),
    recipient: NodeId,
    data: Vec<u8>,
    progress: Vec<Progress>,
    schema: [u8; 32],
    config: &Config,
    now: u64,
) -> Result<(Manifest, Vec<Record>)> {
    if data.len() > config.snapshot_bytes
        || data.len() > config.snapshot_staging_bytes
        || progress.len() > 4096
    {
        return Err(Error::Overloaded);
    }
    let transfer = rand::random();
    let mut m = Manifest {
        database: identity.0,
        donor: identity.1,
        recipient,
        transfer,
        expires: deadline(config, now),
        bytes: data.len() as u64,
        digest: Sha256::digest(&data).into(),
        schema,
        progress,
        chunks: data
            .chunks(CHUNK_BYTES)
            .map(|b| Sha256::digest(b).into())
            .collect(),
        signature: Vec::new(),
    };
    m.signature = SigningKey::from_bytes(&config.signing_key)
        .sign(&m.signed_bytes()?)
        .to_bytes()
        .to_vec();
    let pin = Pin {
        recipient,
        boundary: m.progress.iter().map(|p| (p.origin, p.applied)).collect(),
        target: None,
        expires: m.expires,
        digest: m.digest,
    };
    let mut records: Vec<_> = data
        .chunks(CHUNK_BYTES)
        .enumerate()
        .map(|(i, b)| Record::put(chunk(OUT, transfer, i as u32), b.to_vec()))
        .collect();
    records.push(Record::put(pin_key(transfer), encode(&pin)?));
    records.push(Record::put(header(OUT, transfer), encode(&m)?));
    Ok((m, records))
}
const STREAM_MAGIC:&[u8]=b"MURMUR-SNAPSHOT\0\x02";
/// Build a record-stream snapshot without materializing its complete payload.
/// `pages` contains encoded Content batches. `emit` persists bounded groups;
/// only the final group publishes the signed manifest and retention pin.
/// Preliminary-record grace for an in-progress build: maintain() sweeps
/// staged chunks whose transfer has no live header every second, so a
/// build that streams chunks for longer than a tick would lose them
/// before committing its header. The preliminary header/pin mark the
/// transfer live for an hour; crashed builds are swept at grace expiry.
pub const BUILD_GRACE_MILLIS: u64 = 3_600_000;
/// A preliminary header older than this is a dead build's leftovers, not
/// an active build: reuse ignores it and builds anew instead of reporting
/// busy. Builds under normal conditions finish in well under a minute.
pub const BUILD_STALE_MILLIS: u64 = 300_000;
/// Freshness margin for preliminary reuse: a preliminary header whose
/// remaining grace exceeds this is an actively streaming build.
pub const BUILD_FRESH_MARGIN_MILLIS: u64 = BUILD_GRACE_MILLIS - BUILD_STALE_MILLIS;
pub fn build_stream(
    identity:(NodeId,NodeId),recipient:NodeId,pages:impl IntoIterator<Item=Result<Vec<u8>>>,
    progress:Vec<Progress>,schema:[u8;32],config:&Config,now:u64,
    mut emit:impl FnMut(Vec<Record>)->Result<()>,
)->Result<Manifest> {
    if progress.len()>4096{return Err(Error::Overloaded);}
    let transfer=rand::random();
    // Phase 1: preliminary pin+header. Chunkless by construction (a real
    // snapshot always carries at least the stream-magic chunk), so
    // receivers and reuse can tell it from a servable build.
    let mut prelim=Manifest{database:identity.0,donor:identity.1,recipient,transfer,expires:now.saturating_add(BUILD_GRACE_MILLIS),bytes:0,digest:[0;32],schema,progress:progress.clone(),chunks:Vec::new(),signature:Vec::new()};
    prelim.signature=SigningKey::from_bytes(&config.signing_key).sign(&prelim.signed_bytes()?).to_bytes().to_vec();
    emit(vec![Record::put(pin_key(transfer),encode(&Pin{recipient,boundary:progress.iter().map(|p|(p.origin,p.applied)).collect(),target:None,expires:prelim.expires,digest:prelim.digest})?),Record::put(header(OUT,transfer),encode(&prelim)?)])?;
    let mut hashes=Vec::new();let mut digest=Sha256::new();
    let mut buffer=Vec::with_capacity(CHUNK_BYTES);let mut records=Vec::new();let mut total=0usize;
    let mut push=|bytes:&[u8]|->Result<()> {
        total=total.checked_add(bytes.len()).ok_or(Error::Overloaded)?;
        if total>config.snapshot_bytes || total>config.snapshot_staging_bytes{return Err(Error::Overloaded);}
        digest.update(bytes);
        let mut rest=bytes;
        while !rest.is_empty() {let n=(CHUNK_BYTES-buffer.len()).min(rest.len());buffer.extend_from_slice(&rest[..n]);rest=&rest[n..];
            if buffer.len()==CHUNK_BYTES {hashes.push(Sha256::digest(&buffer).into());records.push(Record::put(chunk(OUT,transfer,(hashes.len()-1) as u32),std::mem::replace(&mut buffer,Vec::with_capacity(CHUNK_BYTES))));
                if records.len()==16{emit(std::mem::take(&mut records))?;}
            }
        }Ok(())
    };
    push(STREAM_MAGIC)?;
    for page in pages {let page=page?;if page.is_empty() || page.len()>config.max_transaction_bytes{return Err(Error::Overloaded);}push(&(page.len() as u32).to_be_bytes())?;push(&page)?;}
    drop(push);
    if !buffer.is_empty(){hashes.push(Sha256::digest(&buffer).into());records.push(Record::put(chunk(OUT,transfer,(hashes.len()-1) as u32),buffer));}
    let mut manifest=Manifest{database:identity.0,donor:identity.1,recipient,transfer,expires:deadline(config,now),bytes:total as u64,digest:digest.finalize().into(),schema,progress,chunks:hashes,signature:Vec::new()};
    manifest.signature=SigningKey::from_bytes(&config.signing_key).sign(&manifest.signed_bytes()?).to_bytes().to_vec();
    let pin=Pin{recipient,boundary:manifest.progress.iter().map(|p|(p.origin,p.applied)).collect(),target:None,expires:manifest.expires,digest:manifest.digest};
    records.push(Record::put(pin_key(transfer),encode(&pin)?));records.push(Record::put(header(OUT,transfer),encode(&manifest)?));emit(records)?;Ok(manifest)
}
fn install_key(transfer:NodeId)->Vec<u8>{format!("{}{}/install",String::from_utf8_lossy(IN),hex(&transfer)).into_bytes()}
fn stream_read(backend:&dyn Backend,m:&Manifest,offset:u64,length:usize)->Result<Vec<u8>> {
    if offset.checked_add(length as u64).is_none_or(|end|end>m.bytes){return Err(Error::Protocol("snapshot stream bounds".into()));}
    let mut out=Vec::with_capacity(length);let mut at=offset as usize;
    while out.len()<length {let index=at/CHUNK_BYTES;let bytes=backend.get(&chunk(IN,m.transfer,index as u32))?.ok_or_else(||Error::Unavailable("snapshot stream chunk missing".into()))?;
        validate_chunk(m,m.donor,index as u32,&bytes)?;let n=(length-out.len()).min(bytes.len()-at%CHUNK_BYTES);out.extend_from_slice(&bytes[at%CHUNK_BYTES..at%CHUNK_BYTES+n]);at+=n;
    }Ok(out)
}
pub fn outgoing(
    backend: &dyn Backend,
    recipient: NodeId,
    transfer: NodeId,
    now: u64,
) -> Result<Option<Manifest>> {
    let Some(bytes) = backend.get(&header(OUT, transfer))? else {
        return Ok(None);
    };
    let m: Manifest = decode(&bytes)?;
    if m.recipient != recipient {
        return Err(Error::Auth("snapshot recipient".into()));
    }
    if m.schema != schema_digest(&backend.schema()?) {
        return Ok(None);
    }
    let pin = backend
        .get(&pin_key(transfer))?
        .map(|b| decode::<Pin>(&b))
        .transpose()?;
    Ok((m.expires > now && pin.is_some_and(|p| p.target.is_none())).then_some(m))
}
/// An explicit recipient abandonment releases an unapplied transfer. Once
/// acknowledged, its catch-up obligation must instead be satisfied or expire.
pub fn cancel(backend: &dyn Backend, recipient: NodeId, transfer: NodeId) -> Result<()> {
    if let Some(bytes) = backend.get(&pin_key(transfer))? {
        let pin: Pin = decode(&bytes)?;
        if pin.recipient != recipient {
            return Err(Error::Auth("snapshot cancellation recipient".into()));
        }
        if pin.target.is_some() {
            return Ok(());
        }
        backend.commit(vec![
            Record::delete(pin_key(transfer)),
            Record::delete(header(OUT, transfer)),
        ])?;
    }
    Ok(())
}
pub fn incoming(backend: &dyn Backend) -> Result<Option<Incoming>> {
    for key in backend
        .keys(IN)?
        .into_iter()
        .filter(|k| k.ends_with(b"/header"))
    {
        if let Some(bytes) = backend.get(&key)? {
            return Ok(Some(decode(&bytes)?));
        }
    }
    Ok(None)
}
pub fn accept(
    backend: &dyn Backend,
    config: &Config,
    source: NodeId,
    m: Manifest,
    now: u64,
) -> Result<()> {
    m.verify(backend, config, source, now)?;
    // A chunkless manifest is a donor's in-progress build marker, never a
    // servable snapshot: refuse without prejudice so the requester retries
    // once the build finalizes.
    if m.chunks.is_empty() {
        return Err(Error::Unavailable("snapshot build in progress".into()));
    }
    if let Some(bytes) = backend.get(&receipt_key(m.transfer))? {
        let done: Incoming = decode(&bytes)?;
        if encode(&done.manifest)? != encode(&m)? {
            return Err(Error::Auth("conflicting snapshot receipt".into()));
        }
        return Ok(());
    }
    if let Some(old) = incoming(backend)? {
        if encode(&old.manifest)? != encode(&m)? {
            return Err(Error::Unavailable("another snapshot is staged".into()));
        }
        return Ok(()); // Retransmissions cannot extend the locally persisted deadline.
    }
    if backend.keys(RECEIPTS)?.len() >= config.staging_transfers {
        return Err(Error::Overloaded);
    }
    // Include leftover chunks awaiting bounded reclamation in the byte budget.
    let mut used = m.bytes;
    for key in backend
        .keys(IN)?
        .into_iter()
        .filter(|k| !k.ends_with(b"/header"))
    {
        used = used
            .checked_add(backend.get(&key)?.map_or(0, |v| v.len() as u64))
            .ok_or(Error::Overloaded)?;
        if used > config.snapshot_staging_bytes as u64 {
            return Err(Error::Overloaded);
        }
    }
    let staged = Incoming {
        deadline: m.expires.min(deadline(config, now)),
        manifest: m,
    };
    backend.commit(vec![Record::put(
        header(IN, staged.manifest.transfer),
        encode(&staged)?,
    )])
}
pub fn receive(
    backend: &dyn Backend,
    source: NodeId,
    transfer: NodeId,
    index: u32,
    data: Vec<u8>,
    now: u64,
) -> Result<()> {
    // Late identical chunks after durable completion are harmless.
    if let Some(b) = backend.get(&receipt_key(transfer))? {
        let receipt: Incoming = decode(&b)?;
        validate_chunk(&receipt.manifest, source, index, &data)?;
        return Ok(());
    }
    let bytes = backend
        .get(&header(IN, transfer))?
        .ok_or_else(|| Error::Unavailable("snapshot manifest missing".into()))?;
    let state: Incoming = decode(&bytes)?;
    if state.deadline <= now {
        return Err(Error::Unavailable("snapshot expired".into()));
    }
    validate_chunk(&state.manifest, source, index, &data)?;
    let key = chunk(IN, transfer, index);
    if let Some(old) = backend.get(&key)? {
        if old != data {
            return Err(Error::Auth("conflicting snapshot chunk".into()));
        }
        return Ok(());
    }
    backend.commit(vec![Record::put(key, data)])
}
fn validate_chunk(m: &Manifest, source: NodeId, index: u32, data: &[u8]) -> Result<()> {
    let i = index as usize;
    if source != m.donor || i >= m.chunks.len() {
        return Err(Error::Auth("snapshot chunk identity".into()));
    }
    let expected = (m.bytes as usize - i * CHUNK_BYTES).min(CHUNK_BYTES);
    if data.len() != expected || <[u8; 32]>::from(Sha256::digest(data)) != m.chunks[i] {
        return Err(Error::Auth("snapshot chunk length or hash".into()));
    }
    Ok(())
}
pub fn missing(backend: &dyn Backend, state: &Incoming) -> Result<Vec<u32>> {
    // Use Spool's existing key index, not payload reads: repeatedly decoding
    // hundreds of MiB just to discover missing chunks defeats bounded caching.
    let prefix = format!(
        "{}{}/chunk/",
        String::from_utf8_lossy(IN),
        hex(&state.manifest.transfer)
    );
    let saved: BTreeSet<_> = backend.keys(prefix.as_bytes())?.into_iter().collect();
    let mut indexes = Vec::new();
    for i in 0..state.manifest.chunks.len() {
        if !saved.contains(&chunk(IN, state.manifest.transfer, i as u32)) {
            indexes.push(i as u32);
            if indexes.len() == 128 {
                break;
            }
        }
    }
    Ok(indexes)
}
pub fn finish(
    backend: &dyn Backend,
    config: &Config,
    source: NodeId,
    transfer: NodeId,
    now: u64,
) -> Result<bool> {
    if let Some(b) = backend.get(&receipt_key(transfer))? {
        let r: Incoming = decode(&b)?;
        if r.manifest.donor != source {
            return Err(Error::Auth("snapshot receipt donor".into()));
        }
        return Ok(true);
    }
    let Some(b) = backend.get(&header(IN, transfer))? else {
        return Ok(false);
    };
    let r: Incoming = decode(&b)?;
    r.manifest.verify(backend, config, source, now)?;
    if r.deadline <= now {
        return Ok(false);
    }
    if r.manifest.schema != schema_digest(&backend.schema()?) {
        return Err(Error::Unavailable("snapshot schema mismatch".into()));
    }
    if !missing(backend,&r)?.is_empty(){return Ok(false);}
    let install=backend.get(&install_key(transfer))?.map(|bytes|decode::<u64>(&bytes)).transpose()?;
    let streamed=if r.manifest.bytes>=STREAM_MAGIC.len() as u64 {stream_read(backend,&r.manifest,0,STREAM_MAGIC.len())?==STREAM_MAGIC}else{false};
    let mut data=Vec::new();
    if install.is_none() {
        let mut digest=Sha256::new();let mut total=0u64;
        for i in 0..r.manifest.chunks.len() {
            let Some(bytes)=backend.get(&chunk(IN,transfer,i as u32))? else{return Ok(false)};
            validate_chunk(&r.manifest,source,i as u32,&bytes)?;digest.update(&bytes);total+=bytes.len() as u64;
            if !streamed {data.extend(bytes);}
        }
        if total!=r.manifest.bytes || <[u8;32]>::from(digest.finalize())!=r.manifest.digest{return Err(Error::Auth("snapshot payload digest".into()));}
    }
    if streamed {
        let mut cursor=install.unwrap_or(STREAM_MAGIC.len() as u64);
        if cursor<STREAM_MAGIC.len() as u64 || cursor>r.manifest.bytes{return Err(Error::Protocol("snapshot installation cursor".into()));}
        if cursor<r.manifest.bytes {
            // Batch install pages per apply call, like normal transaction
            // batches: every apply is one cursor race against concurrent
            // local writes, and installing page-by-page needs dozens of
            // races (usually lost under write load) where a few suffice.
            // A lost race commits nothing, so the next call re-reads from
            // the same cursor.
            let mut payloads=Vec::new();let mut bytes=0usize;
            while cursor<r.manifest.bytes && payloads.len()<16 {
                let length=u32::from_be_bytes(stream_read(backend,&r.manifest,cursor,4)?.try_into().unwrap()) as usize;
                if length==0 || length>config.max_transaction_bytes {return Err(Error::Overloaded);}
                if !payloads.is_empty() && bytes.saturating_add(length)>config.batch_bytes {break;}
                bytes=bytes.saturating_add(length);
                payloads.push(stream_read(backend,&r.manifest,cursor+4,length)?);cursor+=4+length as u64;
            }
            if !backend.apply(payloads,vec![Record::put(install_key(transfer),encode(&cursor)?)])? {return Ok(false);}
            if cursor<r.manifest.bytes{return Ok(false);}
        }
    }
    let mut receipt = r.clone();
    receipt.deadline = deadline(config, now);
    let mut records = vec![
        Record::put(receipt_key(transfer), encode(&receipt)?),
        Record::delete(header(IN, transfer)),
        Record::delete(install_key(transfer)),
    ];
    for p in &r.manifest.progress {
        let old = backend
            .get(&progress_key(p.origin))?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        records.push(Record::put(
            progress_key(p.origin),
            encode(&old.max(p.applied))?,
        ));
    }
    backend.apply(if streamed {Vec::new()}else{vec![data]}, records)
}
/// Minimum allowed pruning boundary. Origins absent at capture must retain all
/// newly received history while a transfer pin is live.
pub fn prune_limit(backend: &dyn Backend, origin: NodeId, now: u64) -> Result<u64> {
    let mut limit = u64::MAX;
    for key in backend.keys(PINS)? {
        if let Some(bytes) = backend.get(&key)? {
            let pin: Pin = decode(&bytes)?;
            if pin.expires > now {
                limit = limit.min(pin.boundary.get(&origin).copied().unwrap_or(0));
            }
        }
    }
    Ok(limit)
}
/// Bounded cleanup; deleting a pin requires expiry or persisted peer ACKs reaching
/// its frozen target. A new manifest cannot overwrite this obligation.
pub fn maintain(backend: &dyn Backend, now: u64) -> Result<()> {
    let mut records = Vec::new();
    for key in backend.keys(PINS)? {
        if let Some(bytes) = backend.get(&key)? {
            let pin: Pin = decode(&bytes)?;
            let caught_up = if let Some(target) = &pin.target {
                let peer = retention::peer(backend, pin.recipient)?;
                peer.is_some_and(|p| {
                    target
                        .iter()
                        .all(|(o, s)| p.acknowledged.get(o).copied().unwrap_or(0) >= *s)
                })
            } else {
                false
            };
            if pin.expires <= now || caught_up {
                records.push(Record::delete(key));
            }
            if records.len() == 256 {
                break;
            }
        }
    }
    if !records.is_empty() {
        backend.commit(records)?;
    }
    for prefix in [IN, OUT] {
        let keys = backend.keys(prefix)?;
        let mut live = BTreeSet::new();
        let mut records = Vec::new();
        for key in keys.iter().filter(|k| k.ends_with(b"/header")) {
            let Some(bytes) = backend.get(key)? else {
                continue;
            };
            let (m, expired) = if prefix == IN {
                let r: Incoming = decode(&bytes)?;
                (r.manifest, r.deadline <= now)
            } else {
                let m: Manifest = decode(&bytes)?;
                let pin = backend
                    .get(&pin_key(m.transfer))?
                    .map(|b| decode::<Pin>(&b))
                    .transpose()?;
                let expired = m.expires <= now || pin.is_none_or(|p| p.target.is_some());
                (m, expired)
            };
            if expired && records.len() < 256 {
                records.push(Record::delete(key.clone()));
            } else {
                live.insert(hex(&m.transfer));
            }
        }
        for key in keys.into_iter().filter(|k| !k.ends_with(b"/header")) {
            if records.len() >= 256 {
                break;
            }
            let text = std::str::from_utf8(&key)
                .map_err(|_| Error::Protocol("snapshot chunk key".into()))?;
            let id = text
                .strip_prefix(std::str::from_utf8(prefix).unwrap())
                .and_then(|s| s.split('/').next())
                .unwrap_or("");
            if !live.contains(id) {
                records.push(Record::delete(key));
            }
            if records.len() >= 256 {
                break;
            }
        }
        if !records.is_empty() {
            backend.commit(records)?;
        }
    }
    let mut records = Vec::new();
    for key in backend.keys(RECEIPTS)? {
        if let Some(b) = backend.get(&key)? {
            let r: Incoming = decode(&b)?;
            if r.deadline <= now {
                records.push(Record::delete(key));
            }
            if records.len() == 256 {
                break;
            }
        }
    }
    if !records.is_empty() {
        backend.commit(records)?;
    }
    // Version 8 partial transfers have no durable sender identity to resume.
    let old = backend
        .keys(b"07/replication/snapshot/")?
        .into_iter()
        .take(256)
        .map(Record::delete)
        .collect::<Vec<_>>();
    if !old.is_empty() {
        backend.commit(old)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct Memory {
        data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        fail: AtomicBool,
        defer: AtomicBool,
        applied: AtomicUsize,
    }
    impl Memory {
        fn new() -> Self {
            Self {
                data: Mutex::new(BTreeMap::new()),
                fail: AtomicBool::new(false),
                defer: AtomicBool::new(false),
                applied: AtomicUsize::new(0),
            }
        }
    }
    impl Backend for Memory {
        fn identity(&self) -> (NodeId, NodeId) {
            ([1; 16], [2; 16])
        }
        fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.data.lock().unwrap().get(k).cloned())
        }
        fn keys(&self, p: &[u8]) -> Result<Vec<Vec<u8>>> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(p))
                .cloned()
                .collect())
        }
        fn commit(&self, records: Vec<Record>) -> Result<()> {
            if self.fail.swap(false, Ordering::SeqCst) {
                return Err(Error::Storage("injected commit".into()));
            }
            let mut data = self.data.lock().unwrap();
            for r in records {
                if let Some(v) = r.value {
                    data.insert(r.key, v);
                } else {
                    data.remove(&r.key);
                }
            }
            Ok(())
        }
        fn apply(&self, payloads: Vec<Vec<u8>>, records: Vec<Record>) -> Result<bool> {
            if self.defer.load(Ordering::SeqCst) {
                return Ok(false);
            }
            assert_eq!(payloads.len(), 1);
            self.commit(records)?;
            self.applied.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }
        fn schema(&self) -> Result<Vec<u8>> {
            Ok(vec![5])
        }
        fn publish(&self, _: Vec<Record>) -> Result<()> {
            unreachable!()
        }
        fn snapshot(&self) -> Result<Vec<u8>> {
            unreachable!()
        }
        fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<Progress>)> {
            unreachable!()
        }
        fn retention_checkpoint(&self) -> Result<()> {
            unreachable!()
        }
        fn prune_history(&self, _: NodeId, _: u64) -> Result<u64> {
            unreachable!()
        }
        fn reclaim_retention(&self) -> Result<()> {
            unreachable!()
        }
        fn observe(&self, _: u64, _: u32) -> Result<()> {
            unreachable!()
        }
        fn accept_schema(&self, _: Vec<u8>) -> Result<bool> {
            unreachable!()
        }
        fn import_bridge(&self, _: bridge::Header, _: Vec<u8>) -> Result<bool> {
            unreachable!()
        }
        fn release_ownership(&self, _: String, _: Vec<u8>, _: Option<String>) -> Result<bool> {
            unreachable!()
        }
    }
    fn config() -> Config {
        let mut c = Config::new(
            "127.0.0.1:0".parse().unwrap(),
            Credentials {
                chain: vec![],
                roots: vec![],
                private_key: vec![].into(),
            },
            [7; 32],
        );
        c.trusted_snapshot_sources.insert([3; 16]);
        c.trusted_origins.insert([3; 16], public_key(&[7; 32]));
        c.snapshot_transfer_lifetime = Duration::from_secs(10);
        c
    }
    fn transfer(bytes: usize) -> (Config, Manifest, Vec<Record>) {
        let c = config();
        let (m, r) = build(
            ([1; 16], [3; 16]),
            [2; 16],
            vec![8; bytes],
            vec![Progress {
                origin: [3; 16],
                applied: 7,
                observed: 7,
                retained_from: 1,
                retained_through: 7,
            }],
            schema_digest(&[5]),
            &c,
            100,
        )
        .unwrap();
        (c, m, r)
    }
    fn parts(records: &[Record]) -> Vec<Vec<u8>> {
        records
            .iter()
            .filter(|r| r.key.windows(7).any(|w| w == b"/chunk/"))
            .map(|r| r.value.clone().unwrap())
            .collect()
    }
    #[test]
    fn validates_signed_manifest_identity_limits_and_conflicts() {
        let b = Memory::new();
        let (c, m, _) = transfer(CHUNK_BYTES + 1);
        let mut wrong = m.clone();
        wrong.recipient = [9; 16];
        assert!(accept(&b, &c, [3; 16], wrong, 100).is_err());
        let mut wrong = m.clone();
        wrong.database = [9; 16];
        assert!(accept(&b, &c, [3; 16], wrong, 100).is_err());
        let mut wrong = m.clone();
        wrong.signature[0] ^= 1;
        assert!(accept(&b, &c, [3; 16], wrong, 100).is_err());
        let mut wrong = m.clone();
        wrong.progress[0].applied = 6;
        assert!(accept(&b, &c, [3; 16], wrong, 100).is_err());
        assert!(accept(&b, &c, [4; 16], m.clone(), 100).is_err());
        assert!(accept(&b, &c, [3; 16], m.clone(), m.expires).is_err());
        let mut small = c.clone();
        small.snapshot_staging_bytes = CHUNK_BYTES;
        assert!(accept(&b, &small, [3; 16], m.clone(), 100).is_err());
        accept(&b, &c, [3; 16], m.clone(), 100).unwrap();
        let original = b.get(&header(IN, m.transfer)).unwrap();
        accept(&b, &c, [3; 16], m.clone(), 200).unwrap();
        assert_eq!(b.get(&header(IN, m.transfer)).unwrap(), original);
        let (_, other, _) = transfer(20);
        assert!(accept(&b, &c, [3; 16], other, 100).is_err());
    }
    #[test]
    fn reordered_duplicate_and_missing_chunks_resume_in_pages() {
        let b = Memory::new();
        let (c, m, r) = transfer(CHUNK_BYTES * 130 + 7);
        let data = parts(&r);
        accept(&b, &c, [3; 16], m.clone(), 100).unwrap();
        assert_eq!(
            missing(&b, &incoming(&b).unwrap().unwrap()).unwrap().len(),
            128
        );
        for i in [130, 2, 0] {
            receive(&b, [3; 16], m.transfer, i, data[i as usize].clone(), 100).unwrap();
            receive(&b, [3; 16], m.transfer, i, data[i as usize].clone(), 100).unwrap();
        }
        assert!(!finish(&b, &c, [3; 16], m.transfer, 100).unwrap());
        assert_eq!(
            missing(&b, &incoming(&b).unwrap().unwrap()).unwrap().len(),
            128
        );
        for (i, p) in data.into_iter().enumerate().rev() {
            receive(&b, [3; 16], m.transfer, i as u32, p, 100).unwrap();
        }
        assert!(
            missing(&b, &incoming(&b).unwrap().unwrap())
                .unwrap()
                .is_empty()
        );
        assert!(finish(&b, &c, [3; 16], m.transfer, 100).unwrap());
        assert_eq!(b.applied.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn rejects_corrupt_lengths_indexes_and_preserves_saved_chunks() {
        let b = Memory::new();
        let (c, m, r) = transfer(CHUNK_BYTES + 3);
        let data = parts(&r);
        accept(&b, &c, [3; 16], m.clone(), 100).unwrap();
        receive(&b, [3; 16], m.transfer, 0, data[0].clone(), 100).unwrap();
        for p in [
            vec![],
            vec![8; CHUNK_BYTES - 1],
            vec![8; CHUNK_BYTES + 1],
            vec![9; CHUNK_BYTES],
        ] {
            assert!(receive(&b, [3; 16], m.transfer, 0, p, 100).is_err());
        }
        assert!(receive(&b, [4; 16], m.transfer, 0, data[0].clone(), 100).is_err());
        assert!(receive(&b, [3; 16], m.transfer, 2, vec![8; 3], 100).is_err());
        assert!(receive(&b, [3; 16], m.transfer, 1, vec![8; 4], 100).is_err());
        assert_eq!(
            b.get(&chunk(IN, m.transfer, 0)).unwrap(),
            Some(data[0].clone())
        );
    }
    #[test]
    fn apply_failure_deferred_apply_and_completion_are_idempotent() {
        let b = Memory::new();
        let (c, m, r) = transfer(20);
        accept(&b, &c, [3; 16], m.clone(), 100).unwrap();
        receive(&b, [3; 16], m.transfer, 0, parts(&r)[0].clone(), 100).unwrap();
        b.defer.store(true, Ordering::SeqCst);
        assert!(!finish(&b, &c, [3; 16], m.transfer, 100).unwrap());
        assert!(b.get(&receipt_key(m.transfer)).unwrap().is_none());
        b.defer.store(false, Ordering::SeqCst);
        b.fail.store(true, Ordering::SeqCst);
        assert!(finish(&b, &c, [3; 16], m.transfer, 100).is_err());
        assert!(incoming(&b).unwrap().is_some());
        b.commit(vec![Record::put(
            progress_key([3; 16]),
            encode(&9u64).unwrap(),
        )])
        .unwrap();
        assert!(finish(&b, &c, [3; 16], m.transfer, 100).unwrap());
        assert!(finish(&b, &c, [3; 16], m.transfer, 200).unwrap());
        assert_eq!(b.applied.load(Ordering::SeqCst), 1);
        assert_eq!(
            decode::<u64>(&b.get(&progress_key([3; 16])).unwrap().unwrap()).unwrap(),
            9
        );
        assert!(finish(&b, &c, [4; 16], m.transfer, 200).is_err());
        maintain(&b, 200).unwrap();
        assert!(b.keys(IN).unwrap().is_empty());
        assert!(b.get(&receipt_key(m.transfer)).unwrap().is_some());
        maintain(&b, deadline(&c, 100)).unwrap();
        assert!(b.keys(RECEIPTS).unwrap().is_empty());
    }
    #[test]
    fn payload_digest_mismatch_and_schema_mismatch_do_not_publish() {
        let b = Memory::new();
        let (c, mut m, r) = transfer(20);
        m.digest = [0; 32];
        m.signature = SigningKey::from_bytes(&c.signing_key)
            .sign(&m.signed_bytes().unwrap())
            .to_bytes()
            .to_vec();
        accept(&b, &c, [3; 16], m.clone(), 100).unwrap();
        receive(&b, [3; 16], m.transfer, 0, parts(&r)[0].clone(), 100).unwrap();
        assert!(finish(&b, &c, [3; 16], m.transfer, 100).is_err());
        assert_eq!(b.applied.load(Ordering::SeqCst), 0);
        let b = Memory::new();
        let (_, mut m, r) = transfer(20);
        m.schema = [0; 32];
        m.signature = SigningKey::from_bytes(&c.signing_key)
            .sign(&m.signed_bytes().unwrap())
            .to_bytes()
            .to_vec();
        accept(&b, &c, [3; 16], m.clone(), 100).unwrap();
        receive(&b, [3; 16], m.transfer, 0, parts(&r)[0].clone(), 100).unwrap();
        assert!(finish(&b, &c, [3; 16], m.transfer, 100).is_err());
        assert_eq!(b.applied.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn pins_cover_unknown_origins_and_survive_peer_retirement_until_expiry() {
        let b = Memory::new();
        let (_, m, r) = transfer(20);
        b.commit(r).unwrap();
        assert_eq!(prune_limit(&b, [3; 16], 100).unwrap(), 7);
        assert_eq!(prune_limit(&b, [8; 16], 100).unwrap(), 0);
        retention::retire(&b, [2; 16]).unwrap();
        maintain(&b, 200).unwrap();
        assert!(b.get(&pin_key(m.transfer)).unwrap().is_some());
        maintain(&b, m.expires).unwrap();
        assert_eq!(prune_limit(&b, [3; 16], m.expires).unwrap(), u64::MAX);
        assert!(b.keys(OUT).unwrap().is_empty());
    }
    #[test]
    fn completed_payload_reclaimed_but_pin_waits_for_all_target_acks() {
        let b = Memory::new();
        let (_, m, r) = transfer(20);
        b.commit(r).unwrap();
        let mut p: Pin = decode(&b.get(&pin_key(m.transfer)).unwrap().unwrap()).unwrap();
        p.target = Some(BTreeMap::from([([3; 16], 9), ([8; 16], 2)]));
        b.commit(vec![Record::put(pin_key(m.transfer), encode(&p).unwrap())])
            .unwrap();
        retention::touch(
            &b,
            [2; 16],
            &[Progress {
                origin: [3; 16],
                applied: 9,
                observed: 9,
                retained_from: 0,
                retained_through: 0,
            }],
        )
        .unwrap();
        maintain(&b, 200).unwrap();
        assert!(b.keys(OUT).unwrap().is_empty());
        assert_eq!(prune_limit(&b, [3; 16], 200).unwrap(), 7);
        retention::touch(
            &b,
            [2; 16],
            &[Progress {
                origin: [8; 16],
                applied: 2,
                observed: 2,
                retained_from: 0,
                retained_through: 0,
            }],
        )
        .unwrap();
        maintain(&b, 200).unwrap();
        assert!(b.keys(PINS).unwrap().is_empty());
    }
    #[test]
    fn expiry_cleanup_is_bounded_and_unpublished_chunks_reclaimed() {
        let b = Memory::new();
        let (c, m, r) = transfer(CHUNK_BYTES * 300);
        accept(&b, &c, [3; 16], m.clone(), 100).unwrap();
        b.commit(r).unwrap();
        maintain(&b, m.expires).unwrap();
        assert_eq!(b.keys(OUT).unwrap().len(), 45); // header + 255 chunks deleted
        maintain(&b, m.expires).unwrap();
        assert!(b.keys(OUT).unwrap().is_empty());
        assert!(incoming(&b).unwrap().is_none());
        b.commit(vec![Record::put(chunk(IN, m.transfer, 0), vec![1; 10])])
            .unwrap();
        maintain(&b, m.expires).unwrap();
        assert!(b.keys(IN).unwrap().is_empty());
    }
    #[test]
    fn in_progress_build_chunks_survive_maintenance() {
        // build_stream commits chunks incrementally while the header only
        // finalizes at the end; the preliminary header must keep
        // maintain()'s per-second orphan sweep from eating mid-stream
        // chunks (donor builds take seconds under load).
        let c = config();
        let pages: Vec<Result<Vec<u8>>> =
            (0..20).map(|_| Ok(vec![7u8; CHUNK_BYTES])).collect();
        let mut emitted: Vec<Vec<Record>> = Vec::new();
        let manifest = build_stream(
            ([1; 16], [3; 16]),
            [2; 16],
            pages.into_iter(),
            Vec::new(),
            schema_digest(&[5]),
            &c,
            100,
            |records| {
                emitted.push(records);
                Ok(())
            },
        )
        .unwrap();
        assert!(emitted.len() >= 2);
        assert!(manifest.chunks.len() > 16);
        // Simulate mid-build: preliminary header/pin plus the first chunk
        // batch committed, the rest still streaming.
        let b = Memory::new();
        for batch in emitted.iter().take(2) {
            b.commit(batch.clone()).unwrap();
        }
        let mid = b.keys(OUT).unwrap().len();
        assert!(mid > 2, "expected staged chunks, got {mid} keys");
        maintain(&b, 100 + 60_000).unwrap();
        assert_eq!(
            b.keys(OUT).unwrap().len(),
            mid,
            "maintain swept an in-progress build"
        );
        // Headerless chunks with no preliminary marker are still orphans.
        let c2 = Memory::new();
        c2.commit(vec![Record::put(chunk(OUT, [8; 16], 0), vec![1; 10])])
            .unwrap();
        maintain(&c2, 100 + 60_000).unwrap();
        assert!(c2.keys(OUT).unwrap().is_empty());
    }
    #[test]
    fn empty_manifest_is_an_in_progress_build_marker() {
        // Chunkless manifests are reserved for in-progress builds (every
        // servable build carries at least the stream-magic chunk), so the
        // receiver refuses them without prejudice and retries once the
        // donor finalizes.
        let b = Memory::new();
        let (c, m, _) = transfer(0);
        assert!(m.chunks.is_empty());
        assert!(matches!(
            accept(&b, &c, [3; 16], m.clone(), 100),
            Err(Error::Unavailable(_))
        ));
        assert!(incoming(&b).unwrap().is_none());
    }
    #[test]
    fn recipient_cancellation_reclaims_abandoned_payload_but_preserves_catchup_obligation() {
        let b = Memory::new();
        let (_, m, r) = transfer(20);
        b.commit(r).unwrap();
        assert!(cancel(&b, [9; 16], m.transfer).is_err());
        assert!(b.get(&pin_key(m.transfer)).unwrap().is_some());
        cancel(&b, [2; 16], m.transfer).unwrap();
        maintain(&b, 200).unwrap();
        assert!(b.keys(OUT).unwrap().is_empty());
        assert!(b.keys(PINS).unwrap().is_empty());
        let (_, m, r) = transfer(20);
        b.commit(r).unwrap();
        let mut pin: Pin = decode(&b.get(&pin_key(m.transfer)).unwrap().unwrap()).unwrap();
        pin.target = Some(BTreeMap::from([([3; 16], 9)]));
        b.commit(vec![Record::put(
            pin_key(m.transfer),
            encode(&pin).unwrap(),
        )])
        .unwrap();
        cancel(&b, [2; 16], m.transfer).unwrap();
        assert_eq!(prune_limit(&b, [3; 16], 200).unwrap(), 7);
    }
}
