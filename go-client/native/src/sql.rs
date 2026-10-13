//! Lifetime erasure is confined here. Scoped references never enter the global registry.
//! cgo callbacks return on the calling OS thread; TLS rejects escaped/wrong-thread IDs.
use crate::*;
use rusqlite::{Connection, Rows, Statement, types::ValueRef};
use std::cell::RefCell;

struct Prepared {
    rows: Option<Box<Rows<'static>>>,
    statement: Box<Statement<'static>>,
    ephemeral: bool,
}
enum Access {
    Managed(*const db::Sql<'static>),
    Raw(*const Connection),
}
struct Scope {
    access: Access,
    statements: HashMap<u64, Prepared>,
}
thread_local! {static SCOPES:RefCell<HashMap<u64,Scope>>=RefCell::new(HashMap::new());}
struct Guard(u64);
impl Drop for Guard {
    fn drop(&mut self) {
        SCOPES.with(|s| {
            s.borrow_mut().remove(&self.0);
        });
    }
}
pub fn managed<T>(sql: &db::Sql<'_>, f: impl FnOnce(u64) -> db::Result<T>) -> db::Result<T> {
    let id = next();
    SCOPES.with(|s| {
        s.borrow_mut().insert(
            id,
            Scope {
                access: Access::Managed(sql as *const _ as *const db::Sql<'static>),
                statements: HashMap::new(),
            },
        )
    });
    let _guard = Guard(id);
    f(id)
}
pub fn in_callback() -> bool {
    SCOPES.with(|s| !s.borrow().is_empty())
}
fn raw_scope<T>(connection: &Connection, f: impl FnOnce(u64) -> db::Result<T>) -> db::Result<T> {
    let id = next();
    SCOPES.with(|s| {
        s.borrow_mut().insert(
            id,
            Scope {
                access: Access::Raw(connection),
                statements: HashMap::new(),
            },
        )
    });
    let _guard = Guard(id);
    f(id)
}
impl Access {
    unsafe fn execute(&self, text: &str, values: &[db::Value]) -> db::Result<usize> {
        Ok(match *self {
            Self::Managed(p) => unsafe { &*p }.execute(text, rusqlite::params_from_iter(values))?,
            Self::Raw(p) => unsafe { &*p }.execute(text, rusqlite::params_from_iter(values))?,
        })
    }
    unsafe fn batch(&self, text: &str) -> db::Result<()> {
        match *self {
            Self::Managed(p) => unsafe { &*p }.execute_batch(text)?,
            Self::Raw(p) => unsafe { &*p }.execute_batch(text)?,
        };
        Ok(())
    }
    unsafe fn prepare(&self, text: &str) -> db::Result<Statement<'static>> {
        let s = match *self {
            Self::Managed(p) => unsafe { &*p }.prepare(text)?,
            Self::Raw(p) => unsafe { &*p }.prepare(text)?,
        };
        Ok(unsafe { std::mem::transmute::<Statement<'_>, Statement<'static>>(s) })
    }
    unsafe fn last(&self) -> i64 {
        match *self {
            Self::Managed(p) => unsafe { &*p }.last_insert_rowid(),
            Self::Raw(p) => unsafe { &*p }.last_insert_rowid(),
        }
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> db::Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|e| *e <= self.bytes.len())
            .ok_or_else(|| invalid("truncated SQL argument"))?;
        let b = &self.bytes[self.at..end];
        self.at = end;
        Ok(b)
    }
    fn u32(&mut self) -> db::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> db::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn blob(&mut self) -> db::Result<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    fn text(&mut self) -> db::Result<String> {
        String::from_utf8(self.blob()?).map_err(invalid)
    }
    fn values(&mut self) -> db::Result<Vec<db::Value>> {
        let n = self.u32()?;
        if n > 32766 {
            return Err(db::Error::Limit("SQL parameter count".into()));
        }
        let mut values = Vec::new();
        for _ in 0..n {
            values.push(match self.take(1)?[0] {
                0 => db::Value::Null,
                1 => db::Value::Integer(self.u64()? as i64),
                2 => db::Value::Real(f64::from_bits(self.u64()?)),
                3 => db::Value::Text(self.text()?),
                4 => db::Value::Blob(self.blob()?),
                _ => return Err(invalid("SQL value tag")),
            });
        }
        Ok(values)
    }
    fn finish(&self) -> db::Result<()> {
        if self.at != self.bytes.len() {
            Err(invalid("trailing SQL arguments"))
        } else {
            Ok(())
        }
    }
}
fn blob(out: &mut Vec<u8>, bytes: &[u8]) -> db::Result<()> {
    let n = u32::try_from(bytes.len()).map_err(invalid)?;
    out.extend(n.to_le_bytes());
    out.extend(bytes);
    Ok(())
}
fn value(out: &mut Vec<u8>, v: ValueRef<'_>) -> db::Result<()> {
    match v {
        ValueRef::Null => out.push(0),
        ValueRef::Integer(n) => {
            out.push(1);
            out.extend(n.to_le_bytes());
        }
        ValueRef::Real(n) => {
            out.push(2);
            out.extend(n.to_bits().to_le_bytes());
        }
        ValueRef::Text(s) => {
            out.push(3);
            blob(out, s)?;
        }
        ValueRef::Blob(b) => {
            out.push(4);
            blob(out, b)?;
        }
    }
    Ok(())
}
pub fn call(scope: u64, op: u32, bytes: &[u8]) -> db::Result<Vec<u8>> {
    SCOPES.with(|scopes| {
        let mut scopes = scopes.borrow_mut();
        let s = scopes
            .get_mut(&scope)
            .ok_or_else(|| invalid("SQL context expired or used on another OS thread"))?;
        let mut input = Reader { bytes, at: 0 };
        let output = match op {
            1 => {
                let text = input.text()?;
                let params = input.values()?;
                unsafe { s.access.execute(&text, &params)? }
                    .to_le_bytes()
                    .to_vec()
            }
            2 => {
                let text = input.text()?;
                unsafe { s.access.batch(&text)? };
                Vec::new()
            }
            3 | 10 => {
                let text = input.text()?;
                let id = next();
                let statement = Box::new(unsafe { s.access.prepare(&text)? });
                s.statements.insert(
                    id,
                    Prepared {
                        rows: None,
                        statement,
                        ephemeral: op == 10,
                    },
                );
                if op == 10 {
                    let params = input.values()?;
                    query(s, id, &params)?
                } else {
                    id.to_le_bytes().to_vec()
                }
            }
            4 | 5 => {
                let id = input.u64()?;
                let params = input.values()?;
                if op == 5 {
                    query(s, id, &params)?
                } else {
                    let stmt = s.statements.get_mut(&id).ok_or(db::Error::Closed)?;
                    if stmt.rows.is_some() {
                        return Err(db::Error::Busy);
                    }
                    stmt.statement
                        .execute(rusqlite::params_from_iter(params))?
                        .to_le_bytes()
                        .to_vec()
                }
            }
            6 => {
                let id = input.u64()?;
                let prepared = s.statements.get_mut(&id).ok_or(db::Error::Closed)?;
                let rows = prepared.rows.as_mut().ok_or(db::Error::Closed)?;
                match rows.next()? {
                    None => {
                        prepared.rows.take();
                        let ephemeral = prepared.ephemeral;
                        if ephemeral {
                            s.statements.remove(&id);
                        }
                        vec![0]
                    }
                    Some(row) => {
                        let count = row.as_ref().column_count();
                        let mut out = vec![1];
                        out.extend((count as u32).to_le_bytes());
                        for n in 0..count {
                            value(&mut out, row.get_ref(n)?)?;
                        }
                        out
                    }
                }
            }
            7 => {
                let id = input.u64()?;
                s.statements.remove(&id);
                Vec::new()
            }
            8 => {
                let id = input.u64()?;
                if let Some(p) = s.statements.get_mut(&id) {
                    p.rows.take();
                    if p.ephemeral {
                        s.statements.remove(&id);
                    }
                }
                Vec::new()
            }
            9 => unsafe { s.access.last() }.to_le_bytes().to_vec(),
            11 => {
                let request: Json = serde_json::from_slice(bytes).map_err(invalid)?;
                input.at = bytes.len();
                let Access::Managed(ptr) = s.access else {
                    return Err(invalid("policy operations require managed SQL"));
                };
                let sql = unsafe { &*ptr };
                let table: String = field(&request, "table")?;
                let column: String = field(&request, "column")?;
                let key = field(&request, "key")?;
                match request["operation"].as_str() {
                    Some("counter_add") => {
                        sql.counter_add(&table, key, &column, field(&request, "delta")?)?
                    }
                    Some("set_add") => {
                        sql.set_add(&table, key, &column, field(&request, "value")?)?
                    }
                    Some("set_remove") => {
                        sql.set_remove(&table, key, &column, field(&request, "value")?)?
                    }
                    Some("min") => sql.min(&table, key, &column, field(&request, "value")?)?,
                    Some("max") => sql.max(&table, key, &column, field(&request, "value")?)?,
                    _ => return Err(invalid("policy operation")),
                };
                Vec::new()
            }
            _ => return Err(invalid("SQL opcode")),
        };
        input.finish()?;
        Ok(output)
    })
}
fn query(s: &mut Scope, id: u64, values: &[db::Value]) -> db::Result<Vec<u8>> {
    let prepared = s.statements.get_mut(&id).ok_or(db::Error::Closed)?;
    if prepared.rows.is_some() {
        return Err(db::Error::Busy);
    }
    prepared.statement.clear_bindings();
    for (i, value) in values.iter().enumerate() {
        prepared.statement.raw_bind_parameter(i + 1, value)?;
    }
    if values.len() != prepared.statement.parameter_count() {
        return Err(invalid("SQL parameter count mismatch"));
    }
    let mut out = id.to_le_bytes().to_vec();
    let names = prepared.statement.column_names();
    out.extend((names.len() as u32).to_le_bytes());
    for name in names {
        blob(&mut out, name.as_bytes())?;
    }
    let rows = prepared.statement.raw_query();
    prepared.rows = Some(Box::new(unsafe {
        std::mem::transmute::<Rows<'_>, Rows<'static>>(rows)
    }));
    Ok(out)
}

pub struct Raw {
    inner: Mutex<RawInner>,
}
struct RawInner {
    sessions: HashMap<u64, usize>,
    connection: Box<Connection>,
}
// Sessions and the connection are always accessed under this mutex, never concurrently.
unsafe impl Send for RawInner {}
impl Drop for RawInner {
    fn drop(&mut self) {
        for (_, p) in self.sessions.drain() {
            unsafe {
                drop(Box::from_raw(p as *mut rusqlite::session::Session<'static>));
            }
        }
    }
}
impl Raw {
    pub fn open() -> db::Result<Self> {
        Ok(Self {
            inner: Mutex::new(RawInner {
                sessions: HashMap::new(),
                connection: Box::new(db::open_in_memory()?),
            }),
        })
    }
    pub fn run(&self, foreign: Foreign) -> db::Result<Json> {
        let inner = self.inner.lock().unwrap();
        raw_scope(&inner.connection, |id| foreign.json(1, id, Json::Null))
    }
    pub fn session(&self, request: &Json) -> db::Result<Json> {
        let mut inner = self.inner.lock().unwrap();
        match request["op"].as_str().unwrap_or("") {
            "session_start" => {
                let session = db::start_session(&inner.connection)?;
                let session = unsafe {
                    std::mem::transmute::<
                        rusqlite::session::Session<'_>,
                        rusqlite::session::Session<'static>,
                    >(session)
                };
                let id = next();
                inner
                    .sessions
                    .insert(id, Box::into_raw(Box::new(session)) as usize);
                Ok(json!(id))
            }
            "session_close" => {
                let id = field(request, "session")?;
                if let Some(p) = inner.sessions.remove(&id) {
                    unsafe {
                        drop(Box::from_raw(p as *mut rusqlite::session::Session<'static>));
                    }
                }
                Ok(Json::Null)
            }
            op => {
                let id = field(request, "session")?;
                let p = *inner.sessions.get(&id).ok_or(db::Error::Closed)?;
                let session = unsafe { &mut *(p as *mut rusqlite::session::Session<'static>) };
                match op {
                    "session_changeset" => {
                        let mut out = Vec::new();
                        session.changeset_strm(&mut out)?;
                        encoded(out)
                    }
                    "session_patchset" => {
                        let mut out = Vec::new();
                        session.patchset_strm(&mut out)?;
                        encoded(out)
                    }
                    "session_attach" => {
                        let table: Option<String> = field(request, "table")?;
                        session.attach(table.as_deref())?;
                        Ok(Json::Null)
                    }
                    "session_enable" => {
                        session.set_enabled(field(request, "enabled")?);
                        Ok(Json::Null)
                    }
                    "session_empty" => Ok(json!(session.is_empty())),
                    _ => Err(invalid("session operation")),
                }
            }
        }
    }
}
