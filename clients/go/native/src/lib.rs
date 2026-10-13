mod advanced;
mod config;
mod dispatch;
mod sql;
#[cfg(test)]
mod tests;

use serde_json::{Value as Json, json};
use sqlite_session::{self as db, replication as net};
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

#[repr(C)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}
impl Buffer {
    fn empty() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
    fn new(mut bytes: Vec<u8>) -> Self {
        let out = Self {
            ptr: bytes.as_mut_ptr(),
            len: bytes.len(),
            cap: bytes.capacity(),
        };
        std::mem::forget(bytes);
        out
    }
    unsafe fn take(self) -> Vec<u8> {
        if self.ptr.is_null() {
            Vec::new()
        } else {
            unsafe { Vec::from_raw_parts(self.ptr, self.len, self.cap) }
        }
    }
}
pub type Callback = unsafe extern "C" fn(u64, u32, u64, *const u8, usize, *mut Buffer) -> i32;
#[derive(Clone)]
pub struct Foreign(Arc<Lease>);
struct Lease {
    token: u64,
    function: Callback,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut out = Buffer::empty();
        unsafe {
            (self.function)(self.token, 0, 0, std::ptr::null(), 0, &mut out);
            drop(out.take());
        }
    }
}
impl Foreign {
    fn new(token: u64, function: Callback) -> Self {
        Self(Arc::new(Lease { token, function }))
    }
    pub fn bytes(&self, op: u32, scope: u64, input: &[u8]) -> db::Result<Vec<u8>> {
        let mut out = Buffer::empty();
        let status = unsafe {
            (self.0.function)(
                self.0.token,
                op,
                scope,
                input.as_ptr(),
                input.len(),
                &mut out,
            )
        };
        let data = unsafe { out.take() };
        if status == 0 {
            Ok(data)
        } else {
            Err(db::Error::Invalid(format!(
                "Go callback: {}",
                String::from_utf8_lossy(&data)
            )))
        }
    }
    pub fn json(&self, op: u32, scope: u64, input: Json) -> db::Result<Json> {
        let bytes = self.bytes(op, scope, &serde_json::to_vec(&input).map_err(invalid)?)?;
        if bytes.is_empty() {
            Ok(Json::Null)
        } else {
            serde_json::from_slice(&bytes).map_err(invalid)
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForeignRow {
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}
pub enum Entry {
    Database(db::Database),
    Scheduler(Arc<db::WriterScheduler>),
    Ticket(Mutex<Option<db::Ticket>>),
    Subscription(Mutex<db::Subscription<ForeignRow>>),
    Replication(net::Handle),
    Objects(Arc<net::files::Objects>),
    Bridge(Arc<net::bridge::Bridge>),
    Backend(Arc<dyn net::Backend>),
    Endpoint(quinn::Endpoint),
    Connection(quinn::Connection),
    Send(Stream<quinn::SendStream>),
    Receive(Stream<quinn::RecvStream>),
    Raw(sql::Raw),
}
pub struct Stream<T> {
    pub inner: Mutex<T>,
    pub closed: std::sync::atomic::AtomicBool,
    pub notify: tokio::sync::Notify,
}
impl<T> Stream<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Mutex::new(inner),
            closed: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}
static NEXT: AtomicU64 = AtomicU64::new(1);
static REGISTRY: OnceLock<Mutex<HashMap<u64, Arc<Entry>>>> = OnceLock::new();
pub fn next() -> u64 {
    NEXT.fetch_add(1, Ordering::Relaxed)
}
fn registry() -> &'static Mutex<HashMap<u64, Arc<Entry>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}
pub fn put(entry: Entry) -> u64 {
    let id = next();
    registry().lock().unwrap().insert(id, Arc::new(entry));
    id
}
pub fn get(id: u64) -> db::Result<Arc<Entry>> {
    registry()
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(db::Error::Closed)
}
pub fn remove(id: u64) {
    let old = registry().lock().unwrap().remove(&id);
    drop(old);
}
pub fn invalid(e: impl std::fmt::Display) -> db::Error {
    db::Error::Invalid(e.to_string())
}
pub fn network(e: net::Error) -> db::Error {
    let code = match &e {
        net::Error::Io(_) => "Io",
        net::Error::Storage(_) => "Storage",
        net::Error::Auth(_) => "Auth",
        net::Error::Protocol(_) => "Protocol",
        net::Error::Invalid(_) => "Invalid",
        net::Error::Overloaded => "Overloaded",
        net::Error::Unavailable(_) => "Unavailable",
    };
    db::Error::Storage(format!("replication/{code}: {e}"))
}
pub fn decode<T: serde::de::DeserializeOwned>(v: Json) -> db::Result<T> {
    serde_json::from_value(v).map_err(invalid)
}
pub fn encoded<T: serde::Serialize>(v: T) -> db::Result<Json> {
    serde_json::to_value(v).map_err(invalid)
}
pub fn field<T: serde::de::DeserializeOwned>(v: &Json, key: &str) -> db::Result<T> {
    decode(
        v.get(key)
            .cloned()
            .ok_or_else(|| invalid(format!("missing {key}")))?,
    )
}
pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("native Tokio runtime")
    })
}

fn error(e: db::Error) -> Json {
    let (code, sqlite) = match &e {
        db::Error::Sql(rusqlite::Error::SqliteFailure(s, _)) => ("Sql", Some(s.extended_code)),
        db::Error::Sql(_) => ("Sql", None),
        db::Error::Io(_) => ("Io", None),
        db::Error::Storage(_) => ("Storage", None),
        db::Error::WrongKey => ("WrongKey", None),
        db::Error::Invalid(_) => ("Invalid", None),
        db::Error::Corrupt(_) => ("Corrupt", None),
        db::Error::Limit(_) => ("Limit", None),
        db::Error::SchemaMismatch => ("SchemaMismatch", None),
        db::Error::Busy => ("Busy", None),
        db::Error::Closed => ("Closed", None),
        db::Error::CallbackPanicked => ("CallbackPanicked", None),
    };
    let network_code = match &e {
        db::Error::Storage(s) => s
            .strip_prefix("replication/")
            .and_then(|s| s.split(':').next())
            .map(str::to_string),
        _ => None,
    };
    json!({"code":code,"message":e.to_string(),"sqlite_code":sqlite,"network_code":network_code})
}
pub unsafe fn input<'a>(ptr: *const u8, len: usize) -> db::Result<&'a [u8]> {
    if len > 512 << 20 {
        return Err(db::Error::Limit("ABI input exceeds 512 MiB".into()));
    }
    if len == 0 {
        Ok(&[])
    } else if ptr.is_null() {
        Err(invalid("null input"))
    } else {
        Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
    }
}
fn boundary(out: *mut Buffer, f: impl FnOnce() -> db::Result<Vec<u8>>) -> i32 {
    if out.is_null() {
        return 1;
    }
    let result = catch_unwind(AssertUnwindSafe(f)).unwrap_or(Err(db::Error::CallbackPanicked));
    let (status, bytes) = match result {
        Ok(b) => (0, b),
        Err(e) => (1, serde_json::to_vec(&error(e)).unwrap()),
    };
    unsafe {
        *out = Buffer::new(bytes);
    }
    status
}
#[unsafe(no_mangle)]
pub extern "C" fn murmur_abi_version() -> u32 {
    1
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn murmur_buffer_free(b: Buffer) {
    drop(unsafe { b.take() });
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn murmur_buffer_copy(p: *const u8, n: usize) -> Buffer {
    match unsafe { input(p, n) } {
        Ok(b) => Buffer::new(b.to_vec()),
        Err(_) => Buffer::empty(),
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn murmur_call(
    id: u64,
    p: *const u8,
    n: usize,
    callback: Option<Callback>,
    out: *mut Buffer,
) -> i32 {
    boundary(out, || {
        let request: Json = serde_json::from_slice(unsafe { input(p, n)? }).map_err(invalid)?;
        let result = dispatch::call(id, request, callback)?;
        serde_json::to_vec(&result).map_err(invalid)
    })
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn murmur_sql(
    id: u64,
    op: u32,
    p: *const u8,
    n: usize,
    out: *mut Buffer,
) -> i32 {
    boundary(out, || sql::call(id, op, unsafe { input(p, n)? }))
}
