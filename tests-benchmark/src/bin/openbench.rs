//! Profile an existing database COPY. Opening can update Spool recovery files.
//! openbench <database-copy> <raw-32-byte-key-file>
use sqlite_session::{Database, OpenPhase, OpenProgress, Options};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

// Optional external gperftools sampler; no profiler dependency in Murmur.
#[cfg(target_os = "linux")]
mod sampler {
    use super::Result;
    use std::ffi::{CString, c_char, c_int, c_void};
    #[link(name = "dl")]
    unsafe extern "C" {
        fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
    }
    pub struct Window(unsafe extern "C" fn());
    impl Window {
        pub fn start() -> Result<Option<Self>> {
            let Some(path) = std::env::var_os("MURMUR_CPU_PROFILE_OUTPUT") else {
                return Ok(None);
            };
            let path = CString::new(path.as_encoded_bytes())?;
            unsafe {
                let start = dlsym(std::ptr::null_mut(), c"ProfilerStart".as_ptr());
                let stop = dlsym(std::ptr::null_mut(), c"ProfilerStop".as_ptr());
                if start.is_null() || stop.is_null() {
                    return Err("preload the profiler thread interposer".into());
                }
                let start: unsafe extern "C" fn(*const c_char) -> c_int =
                    std::mem::transmute(start);
                if start(path.as_ptr()) == 0 {
                    return Err("could not start CPU profiler".into());
                }
                Ok(Some(Self(std::mem::transmute(stop))))
            }
        }
    }
    impl Drop for Window {
        fn drop(&mut self) {
            unsafe { (self.0)() }
        }
    }
}

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 {
        return Err("usage: openbench <database-copy> <raw-32-byte-key-file>".into());
    }
    let mut key = std::fs::read(&args[2])?;
    if key.len() != 32 {
        return Err("key file must contain exactly 32 raw bytes".into());
    }
    let events = Arc::new(Mutex::new(Vec::<OpenProgress>::new()));
    let captured = events.clone();
    let last_log = Arc::new(Mutex::new((OpenPhase::Cancelled, Duration::ZERO)));
    let mut options = Options::default();
    if let Ok(bytes) = std::env::var("MURMUR_OPEN_CACHE_BYTES") {
        options.spool_cache_bytes = bytes.parse()?;
    }
    options.on_open_progress = Some(Arc::new(move |p| {
        let mut previous = last_log.lock().unwrap();
        if previous.0 != p.phase || p.elapsed.saturating_sub(previous.1) >= Duration::from_secs(5) {
            eprintln!(
                "{:?}: {:.3}s, rows={}",
                p.phase,
                p.elapsed.as_secs_f64(),
                p.rows_inserted
            );
            *previous = (p.phase, p.elapsed);
        }
        captured.lock().unwrap().push(p);
    }));
    #[cfg(target_os = "linux")]
    let profile = sampler::Window::start()?;
    let start = Instant::now();
    let opened = Database::open(&args[1], &key, options);
    key.fill(0);
    let db = opened?;
    let open_seconds = start.elapsed().as_secs_f64();
    #[cfg(target_os = "linux")]
    drop(profile);
    let mut phases = Vec::new();
    let mut previous = (OpenPhase::Opening, Duration::ZERO);
    let events = events.lock().unwrap();
    let first_row_seconds = events
        .iter()
        .find(|p| p.current_table.is_some())
        .map(|p| p.elapsed.as_secs_f64());
    for p in events.iter() {
        if p.phase != previous.0 {
            phases.push(serde_json::json!({"phase": format!("{:?}", previous.0), "seconds": p.elapsed.saturating_sub(previous.1).as_secs_f64()}));
            previous = (p.phase, p.elapsed);
        }
    }
    drop(events);
    // Verify every SQL value in primary-key order without printing user data.
    let schema = db.schema()?;
    let tables = schema.tables.len();
    let indexes = schema.indexes.len();
    let (rows, digest) = db.read(move |sql| {
        let mut hash = blake3::Hasher::new();
        let mut count = 0u64;
        for table in &schema.tables {
            hash.update(table.id.as_bytes());
            let mut pk: Vec<_> = table.columns.iter().filter(|c| c.primary_key > 0).collect();
            pk.sort_by_key(|c| c.primary_key);
            let order = pk
                .iter()
                .map(|c| quote(&c.name))
                .collect::<Vec<_>>()
                .join(",");
            let mut query = sql.prepare(&format!(
                "SELECT * FROM {} ORDER BY {order}",
                quote(&table.name)
            ))?;
            let columns = query.column_count();
            let mut result = query.query([])?;
            while let Some(row) = result.next()? {
                for column in 0..columns {
                    let value = row.get_ref(column)?;
                    use rusqlite::types::ValueRef;
                    match value {
                        ValueRef::Null => {
                            hash.update(&[0]);
                        }
                        ValueRef::Integer(v) => {
                            hash.update(&[1]);
                            hash.update(&v.to_le_bytes());
                        }
                        ValueRef::Real(v) => {
                            hash.update(&[2]);
                            hash.update(&v.to_bits().to_le_bytes());
                        }
                        ValueRef::Text(v) | ValueRef::Blob(v) => {
                            hash.update(&[if matches!(value, ValueRef::Text(_)) {
                                3
                            } else {
                                4
                            }]);
                            hash.update(&(v.len() as u64).to_le_bytes());
                            hash.update(v);
                        }
                    }
                }
                count += 1;
            }
        }
        Ok((count, hash.finalize().to_hex().to_string()))
    })?;
    println!(
        "{}",
        serde_json::json!({"open_seconds":open_seconds,"first_row_seconds":first_row_seconds,"phases":phases,"tables":tables,"indexes":indexes,"rows":rows,"sql_digest":digest,"sqlite":db.sqlite_metrics()?})
    );
    db.close()?;
    Ok(())
}
