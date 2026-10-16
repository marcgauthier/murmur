//! Process-isolated scale gate. All child commands use local stdin/stdout, not a product API.
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType};
use serde_json::{Value, json};
use sqlite_session::{
    Database, Migration, OpenMode, Options,
    replication::{Config, Credentials, protocol::public_key},
};
use std::{
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
// Benchmark-only opt-in sampling; no profiler is linked into the database library.
#[cfg(target_os = "linux")]
mod cpu_profile {
    use super::Result;
    use std::ffi::{CString, c_char, c_int, c_void};
    #[link(name = "dl")]
    unsafe extern "C" { fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void; }
    pub struct Window(unsafe extern "C" fn());
    impl Window {
        pub fn start() -> Result<Option<Self>> {
            let Some(path) = std::env::var_os("MURMUR_CPU_PROFILE_OUTPUT") else { return Ok(None) };
            let path = CString::new(path.as_encoded_bytes())?;
            unsafe {
                let start = dlsym(std::ptr::null_mut(), c"ProfilerStart".as_ptr());
                let stop = dlsym(std::ptr::null_mut(), c"ProfilerStop".as_ptr());
                if start.is_null() || stop.is_null() { return Err("CPU profiler library did not load".into()) }
                let start: unsafe extern "C" fn(*const c_char) -> c_int = std::mem::transmute(start);
                if start(path.as_ptr()) == 0 { return Err("CPU profiler could not start".into()) }
                Ok(Some(Self(std::mem::transmute(stop))))
            }
        }
    }
    impl Drop for Window { fn drop(&mut self) { unsafe { (self.0)() } } }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn node_id(index: usize) -> [u8; 16] {
    let mut id = [0; 16];
    id[..8].copy_from_slice(&(index as u64 + 1).to_be_bytes());
    id
}
fn seed(index: usize) -> [u8; 32] {
    *blake3::hash(&node_id(index)).as_bytes()
}
fn config(spec: &Value) -> Result<Config> {
    let index = spec["index"].as_u64().ok_or("index")? as usize;
    let credentials = Credentials {
        chain: vec![serde_json::from_value(spec["cert"].clone())?],
        private_key: serde_json::from_value::<Vec<u8>>(spec["key"].clone())?.into(),
        roots: vec![serde_json::from_value(spec["ca"].clone())?],
    };
    let mut c = Config::new(
        spec["bind"].as_str().ok_or("bind")?.parse()?,
        credentials,
        seed(index),
    );
    for i in 0..spec["nodes"].as_u64().ok_or("nodes")? as usize {
        c.trusted_origins.insert(node_id(i), public_key(&seed(i)));
        c.trusted_snapshot_sources.insert(node_id(i));
    }
    c.bootstrap = serde_json::from_value(spec["bootstrap"].clone())?;
    Ok(c)
}
#[derive(Default)]
struct Measurements {
    offered: u64,
    accepted: u64,
    errors: u64,
    latency: Vec<u64>,
}
fn digest(db: &Database) -> sqlite_session::Result<String> {
    db.read(|s| {
        let mut hash = blake3::Hasher::new();
        let mut statement = s.prepare("SELECT id,n,payload FROM records ORDER BY id")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            hash.update(&row.get::<_, i64>(0)?.to_le_bytes());
            hash.update(&row.get::<_, i64>(1)?.to_le_bytes());
            hash.update(&row.get::<_, Vec<u8>>(2)?);
        }
        Ok(hash.finalize().to_hex().to_string())
    })
}
fn child(path: &Path) -> Result<()> {
    let spec: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let index = spec["index"].as_u64().unwrap() as usize;
    let directory = PathBuf::from(spec["dir"].as_str().ok_or("dir")?);
    let mode = if directory.exists() {
        OpenMode::Existing
    } else {
        OpenMode::CreateNew
    };
    let db = Database::open(
        directory,
        [9; 32],
        Options {
            mode,
            database_id: Some([7; 16]),
            node_id: Some(node_id(index)),
            replication: if spec["replication"] == true {
                Some(config(&spec)?)
            } else {
                None
            },
            capture_interval: Duration::from_millis(capture_ms()?),
            snapshot_deltas: 10_000,
            ..Options::default()
        },
    )?;
    if mode == OpenMode::CreateNew {
        db.migrate(Migration::new("CREATE TABLE records(id INTEGER PRIMARY KEY,n INTEGER,payload BLOB);CREATE TABLE probes(id INTEGER PRIMARY KEY)"))?;
        db.flush()?;
    }
    let mut writer = None;
    #[cfg(target_os = "linux")]
    let mut cpu_window = None;
    let stop = Arc::new(AtomicBool::new(false));
    let measurements = Arc::new(Mutex::new(Measurements::default()));
    println!("{}", json!({"ready":true,"pid":std::process::id()}));
    for line in std::io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line?)?;
        let response: Result<Value> = (|| match request["op"].as_str().unwrap_or("") {
            "run" => {
                if writer.is_some() {
                    return Err("already writing".into());
                }
                #[cfg(target_os = "linux")]
                { cpu_window = cpu_profile::Window::start()?; }
                stop.store(false, Ordering::Release);
                let db = db.clone();
                let stop = stop.clone();
                let measurements = measurements.clone();
                let rate = request["rate"].as_f64().ok_or("rate")?;
                writer = Some(std::thread::spawn(move || {
                    let mut seq = now();
                    let interval = Duration::from_secs_f64(1.0 / rate);
                    let mut next = Instant::now();
                    while !stop.load(Ordering::Acquire) {
                        let begun = Instant::now();
                        let id = (index as i64) * 1_000_000 + (seq % 1000) as i64;
                        let result=db.transaction(move|s| {match seq%10 {
                            0..=6=>{s.execute("INSERT INTO records VALUES(?,?,zeroblob(256)) ON CONFLICT(id) DO UPDATE SET n=excluded.n",(id,seq as i64))?;},
                            7..=8=>{s.execute("UPDATE records SET n=? WHERE id=?",(seq as i64,index as i64*1_000_000+(seq/10%700) as i64))?;},
                            _=>{s.execute("DELETE FROM records WHERE id=?",[index as i64*1_000_000+(seq/10%700) as i64])?;}
                        }Ok(())});
                        let mut m = measurements.lock().unwrap();
                        m.offered += 1;
                        if result.is_ok() {
                            m.accepted += 1;
                        } else {
                            m.errors += 1;
                        }
                        if m.latency.len() < 100_000 {
                            m.latency
                                .push(begun.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                        }
                        drop(m);
                        seq += 1;
                        next += interval;
                        if let Some(delay) = next.checked_duration_since(Instant::now()) {
                            std::thread::sleep(delay);
                        }
                    }
                }));
                Ok(json!({"ok":true}))
            }
            "stop" => {
                stop.store(true, Ordering::Release);
                if let Some(writer) = writer.take() {
                    writer.join().map_err(|_| "writer panic")?;
                }
                #[cfg(target_os = "linux")]
                { cpu_window.take(); }
                db.flush()?;
                Ok(json!({"ok":true}))
            }
            "flush" => {
                db.flush()?;
                Ok(json!({"durable":true}))
            }
            "probe" => {
                let id = request["id"].as_i64().ok_or("probe id")?;
                db.transaction(move |s| {
                    s.execute("INSERT INTO probes VALUES(?)", [id])?;
                    Ok(())
                })?;
                Ok(json!({"committed":now()}))
            }
            "seen" => {
                let id = request["id"].as_i64().ok_or("probe id")?;
                let seen = db.read(move |s| {
                    Ok(s.query_row(
                        "SELECT EXISTS(SELECT 1 FROM probes WHERE id=?)",
                        [id],
                        |row| row.get::<_, bool>(0),
                    )?)
                })?;
                Ok(json!({"seen":seen}))
            }
            "stats" => {
                let m = measurements.lock().unwrap();
                let status = db.replication().map(|r| r.status());
                let rss = std::fs::read_to_string("/proc/self/status")
                    .ok()
                    .and_then(|text| {
                        text.lines()
                            .find(|line| line.starts_with("VmRSS:"))
                            .and_then(|line| line.split_whitespace().nth(1))
                            .and_then(|n| n.parse::<u64>().ok())
                            .map(|n| n * 1024)
                    });
                Ok(
                    json!({"offered":m.offered,"accepted":m.accepted,"errors":m.errors,"latency_ns":m.latency,"replication":status,"rss_bytes":rss,"spool_bytes":db.spool_metrics().disk_bytes}),
                )
            }
            "digest" => {
                db.flush()?;
                Ok(
                    json!({"digest":digest(&db)?,"probes":db.read(|s|Ok(s.query_row("SELECT count(*) FROM probes",[],|r|r.get::<_,i64>(0))?))?}),
                )
            }
            "close" => {
                stop.store(true, Ordering::Release);
                if let Some(writer) = writer.take() {
                    writer.join().map_err(|_| "writer panic")?;
                }
                #[cfg(target_os = "linux")]
                { cpu_window.take(); }
                db.close()?;
                Ok(json!({"closed":true}))
            }
            _ => Err("unknown command".into()),
        })();
        let closed = request["op"] == "close";
        println!(
            "{}",
            match response {
                Ok(value) => value,
                Err(error) => json!({"error":error.to_string()}),
            }
        );
        std::io::stdout().flush()?;
        if closed {
            break;
        }
    }
    Ok(())
}
struct Node {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    spec: PathBuf,
}
impl Node {
    fn spawn(spec: PathBuf) -> Result<Self> {
        let mut command = Command::new(std::env::current_exe()?);
        if let Some(library) = std::env::var_os("MURMUR_CPU_PROFILE_LIBRARY") {
            if !cfg!(target_os = "linux") { return Err("CPU sampling requires Linux".into()) }
            if !Path::new(&library).is_file() { return Err("CPU profiler library not found".into()) }
            let mut preload = library;
            if let Some(existing) = std::env::var_os("LD_PRELOAD") {
                preload.push(":"); preload.push(existing);
            }
            command.env("LD_PRELOAD", preload)
                .env_remove("CPUPROFILE")
                .env_remove("CPUPROFILESIGNAL")
                .env_remove("CPUPROFILE_REALTIME")
                .env("CPUPROFILE_PER_THREAD_TIMERS", "1")
                .env("MURMUR_CPU_PROFILE_OUTPUT", spec.with_extension("cpu.prof"));
        }
        let mut child = command.arg("--node")
            .arg(&spec)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take().ok_or("stdin")?;
        let output = BufReader::new(child.stdout.take().ok_or("stdout")?);
        let mut node = Self {
            child,
            input,
            output,
            spec,
        };
        node.read()?;
        Ok(node)
    }
    fn read(&mut self) -> Result<Value> {
        let mut line = String::new();
        if self.output.read_line(&mut line)? == 0 {
            return Err("node exited".into());
        }
        let value: Value = serde_json::from_str(&line)?;
        if let Some(error) = value["error"].as_str() {
            return Err(error.to_owned().into());
        }
        Ok(value)
    }
    fn call(&mut self, value: Value) -> Result<Value> {
        writeln!(self.input, "{value}")?;
        self.input.flush()?;
        self.read()
    }
    fn restart(&mut self) -> Result<()> {
        let spec = self.spec.clone();
        let _ = self.child.kill();
        let _ = self.child.wait();
        *self = Self::spawn(spec)?;
        Ok(())
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn percentile(values: &mut [u64], percent: usize) -> u64 {
    values.sort_unstable();
    if values.is_empty() {
        return 0;
    }
    values[(values.len() * percent)
        .div_ceil(100)
        .saturating_sub(1)
        .min(values.len() - 1)]
}
fn capture_ms() -> Result<u64> {
    let value = std::env::var("MURMUR_MESH_CAPTURE_MS")
        .ok().map(|s| s.parse::<u64>()).transpose()?.unwrap_or(1000);
    if value == 0 { return Err("MURMUR_MESH_CAPTURE_MS must be positive".into()); }
    Ok(value)
}
fn specifications(root: &Path, n: usize, replication: bool) -> Result<Vec<PathBuf>> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate()?;
    let ca = ca_params.self_signed(&ca_key)?;
    let reservations: Vec<_> = (0..n)
        .map(|_| std::net::UdpSocket::bind("127.0.0.1:0"))
        .collect::<std::io::Result<_>>()?;
    let addresses: Vec<_> = reservations
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect();
    let mut paths = Vec::new();
    for i in 0..n {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(Vec::<String>::new())?;
        p.subject_alt_names = vec![SanType::URI(
            format!(
                "replicateddb://node/{}",
                sqlite_session::replication::hex(&node_id(i))
            )
            .try_into()
            .unwrap(),
        )];
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = p.signed_by(&key, &ca, &ca_key)?;
        let peers: Vec<_> = (1..=6.min(n - 1))
            .map(|offset| addresses[(i + offset) % n])
            .collect();
        let path = root.join(format!("node-{i}.json"));
        std::fs::write(
            &path,
            serde_json::to_vec(
                &json!({"index":i,"nodes":n,"cert":cert.der().to_vec(),"key":key.serialize_der(),"ca":ca.der().to_vec(),"bind":addresses[i],"bootstrap":peers,"dir":root.join(format!("db-{i}")),"replication":replication}),
            )?,
        )?;
        paths.push(path);
    }
    Ok(paths)
}
fn phase(
    root: &Path,
    n: usize,
    seconds: u64,
    total_rate: f64,
    replication: bool,
    chaos: bool,
    smoke: bool,
) -> Result<(Vec<u64>, Vec<u64>)> {
    std::fs::create_dir_all(root)?;
    let specs = specifications(root, n, replication)?;
    let mut nodes: Vec<_> = specs.into_iter().map(Node::spawn).collect::<Result<_>>()?;
    for node in &mut nodes {
        node.call(json!({"op":"run","rate":total_rate/n as f64}))?;
    }
    let started = Instant::now();
    let phase_start_ms = now();
    std::fs::write(root.join("phase-timing.json"), serde_json::to_vec(&json!({"start_ms":phase_start_ms}))?)?;
    let mut convergence = Vec::new();
    let mut deliveries = Vec::new();
    let mut probe = 0i64;
    let mut restarted = false;
    while started.elapsed() < Duration::from_secs(seconds) {
        if chaos && !restarted && started.elapsed() > Duration::from_secs(seconds / 2) {
            nodes[n - 1].call(json!({"op":"flush"}))?;
            nodes[n - 1].restart()?;
            nodes[n - 1].call(json!({"op":"run","rate":total_rate/n as f64}))?;
            restarted = true;
        }
        if replication {
            let committed =
                nodes[probe as usize % n].call(json!({"op":"probe","id":probe}))?["committed"]
                    .as_u64()
                    .ok_or("commit timestamp")?;
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut pending: Vec<_> = (0..n).collect();
            let mut last_delivery = 0u64;
            while !pending.is_empty() {
                let mut next = Vec::new();
                for index in pending {
                    if nodes[index].call(json!({"op":"seen","id":probe}))?["seen"] == true {
                        let elapsed = now().saturating_sub(committed);
                        deliveries.push(elapsed);
                        last_delivery = last_delivery.max(elapsed);
                    } else {
                        next.push(index);
                    }
                }
                if !next.is_empty() && Instant::now() > deadline {
                    let mut failures = Vec::new();
                    for node in &mut nodes {
                        failures.push(node.call(json!({"op":"stats"}))?);
                    }
                    std::fs::write(
                        root.join("failure.json"),
                        serde_json::to_vec_pretty(&failures)?,
                    )?;
                    return Err(format!(
                        "convergence exceeded 15 seconds under sustained load; {}",
                        root.display()
                    )
                    .into());
                }
                pending = next;
                if !pending.is_empty() {
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
            // One sample per write: completion means every node observed it.
            convergence.push(last_delivery);
            probe += 1;
        } else {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    for node in &mut nodes {
        node.call(json!({"op":"stop"}))?;
    }
    std::fs::write(root.join("phase-timing.json"), serde_json::to_vec(&json!({"start_ms":phase_start_ms,"end_ms":now()}))?)?;
    let mut latency = Vec::new();
    let mut reports = Vec::new();
    for node in &mut nodes {
        let stats = node.call(json!({"op":"stats"}))?;
        if stats["errors"] != 0 {
            return Err(format!("write admission errors: {stats}").into());
        }
        latency.extend(serde_json::from_value::<Vec<u64>>(
            stats["latency_ns"].clone(),
        )?);
        reports.push(stats);
    }
    if replication {
        let deadline = Instant::now() + Duration::from_secs(if smoke { 120 } else { 15 });
        loop {
            let mut hashes = Vec::new();
            for node in &mut nodes {
                let state = node.call(json!({"op":"digest"}))?;
                if state["probes"].as_i64() != Some(probe) {
                    return Err("acknowledged probe missing".into());
                }
                hashes.push(state["digest"].clone());
            }
            if hashes.iter().all(|hash| hash == &hashes[0]) {
                break;
            }
            if Instant::now() > deadline {
                let mut failures = Vec::new();
                for node in &mut nodes {
                    failures.push(node.call(json!({"op":"stats"}))?);
                }
                std::fs::write(
                    root.join("failure.json"),
                    serde_json::to_vec_pretty(&json!({"stats":failures,"digests":hashes}))?,
                )?;
                return Err(format!("final state mismatch; {}", root.display()).into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    for node in &mut nodes {
        let close = Instant::now();
        node.call(json!({"op":"close"}))?;
        if close.elapsed() > Duration::from_secs(5) {
            return Err("shutdown exceeded five seconds".into());
        }
    }
    std::fs::write(
        root.join("nodes.json"),
        serde_json::to_vec_pretty(&reports)?,
    )?;
    if replication {
        std::fs::write(root.join("delivery-convergence.json"), serde_json::to_vec_pretty(&deliveries)?)?;
        std::fs::write(root.join("cluster-convergence.json"), serde_json::to_vec_pretty(&convergence)?)?;
    }
    Ok((latency, convergence))
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--node") {
        return child(Path::new(args.get(2).ok_or("node spec")?));
    }
    let n = args
        .get(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(10usize);
    let seconds = args
        .get(2)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(1800u64);
    let rate = args
        .get(3)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(1000f64);
    let chaos = args.iter().any(|s| s == "--chaos");
    let smoke = args.iter().any(|s| s == "--smoke");
    if n < 2 || n > 500 || seconds < 1 || !rate.is_finite() || rate <= 0.0 {
        return Err("usage: meshbench NODES(2..500) SECONDS TX_PER_SECOND [--chaos]".into());
    }
    let root = std::env::var_os("MURMUR_MESH_ARTIFACTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("murmur-mesh-{}-{}", n, now())));
    let (mut baseline, _) = phase(
        &root.join("baseline"),
        n,
        seconds,
        rate,
        false,
        false,
        smoke,
    )?;
    let (mut latency, mut convergence) = phase(
        &root.join("replication"),
        n,
        seconds,
        rate,
        true,
        chaos,
        smoke,
    )?;
    let b95 = percentile(&mut baseline, 95);
    let b99 = percentile(&mut baseline, 99);
    let p95 = percentile(&mut latency, 95);
    let p99 = percentile(&mut latency, 99);
    let c95 = percentile(&mut convergence, 95);
    let c99 = percentile(&mut convergence, 99);
    let report = json!({"nodes":n,"seconds":seconds,"offered_tx_per_second":rate,"capture_ms":capture_ms()?,"chaos":chaos,"baseline_p95_ns":b95,"baseline_p99_ns":b99,"local_p95_ns":p95,"local_p99_ns":p99,"convergence_p95_ms":c95,"convergence_p99_ms":c99,"convergence_measurement":"all_nodes_per_probe","convergence_probes":convergence.len(),"artifacts":root,"hardware_os":std::env::consts::OS,"hardware_arch":std::env::consts::ARCH,"available_parallelism":std::thread::available_parallelism()?.get(),"smoke_only":smoke});
    std::fs::write(
        root.join("result.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{report}");
    if !smoke
        && (c95 >= 5000
            || c99 >= 15000
            || p95 as f64 > b95 as f64 * 1.20
            || p99 as f64 > b99 as f64 * 1.50)
    {
        return Err("scalability gate failed; inspect result.json".into());
    }
    Ok(())
}
