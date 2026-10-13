# MURMUR Rust live multi-process tests (`tests-live/`)

Port of the Go live suites in `/media/marc/2TB/Archives/MURMUR/tests-live/`
(one dir per scenario: `README.md` + `*_test.go`; runner `run.sh`; shared
`harness/`; overview `README.md`) to this repo. Port order and per-scenario
summaries live in `/home/marc/MURMUR/TESTS-LIVE-TODO.md` (73 scenarios).

## 1. Topology decision (read first)

The Go suites drive **daemon processes** (`testnode agent --config ...`) over
an **HTTPS + mTLS admin/data API**. The Rust product has **no HTTP server, no
admin API, and no daemon**: replication is an in-process QUIC mesh
(`murmur-replication`, `Database::open(.., Options { replication: Some(..) })`),
as already exercised by `/home/marc/MURMUR/tests/replication.rs`
(multi-`Database`, ephemeral `127.0.0.1:0` ports, `rcgen` certs).

Therefore Rust ports are **in-process multi-`Database` integration tests**,
not daemon drivers:

- A "node" is an open `Database` on its own directory (`runtime/<scenario>/nodeN/`).
- Stop/restart/kill is `close()` + `Database::open(same dir, same key)`; true
  async `SIGKILL`/`SIGSTOP`/`SIGTERM` legs additionally need per-node fixture
  **subprocesses** (see `harness/src/bin/testnode.rs` below) because signals
  cannot target one in-process node without freezing the test driver.
- There is no typed-records HTTP API to call: ports use SQL via
  `Database::transaction` / `Database::read` (the existing `tests/` pattern),
  `policy.rs` ops (`counter_add`, `set_add/remove`, `min`, `max`), and
  `replication::Handle::{add_peer, remove_peer, retire_peer, force_sync}`.

## 2. Exact layout

All paths absolute. `tests-live/` is NOT a cargo workspace and NOT part of the
root package: every scenario (and the harness) is a standalone package so it
compile-checks alone without touching product manifests.

```text
/home/marc/MURMUR/tests-live/
  README.md                    # this file
  run.sh                       # sequential runner (harness prerequisite creates it; port agents NEVER run it)
  .gitignore                   # runtime/ failures/ target/
  harness/                     # prerequisite package `murmur-live-harness` (TODO item 1)
    Cargo.toml                 # lib + testnode bin; path deps on ../.. (features = ["replication"]), rcgen, tempfile
    src/lib.rs                 # re-exports: sqlite_session, murmur_replication, Cluster, ClusterOptions
    src/cluster.rs             # Cluster / Node open-close-mesh management
    src/certs.rs               # rcgen CA + per-node certs (SAN URI replicateddb://node/<hex>)
    src/unlock.rs              # open-with-key + on_open_progress watchdog (stall/overall env)
    src/forensics.rs           # dump_forensics via status()/metrics (never panics)
    src/artifacts.rs           # runtime/ lifecycle, failures/<ts>-<scenario>/ retention, RESULT line
    src/fixture.rs             # fixture-subprocess spawn/signal/wait helpers
    src/bin/testnode.rs        # fixture binary for crash/signal scenarios (CLI + exit-code protocol, NOT a daemon)
  <scenario>/                  # one dir per TODO checkbox, e.g. three-node-sync/
    Cargo.toml                 # package murmur-live-<scenario>; path dep on ../harness ONLY
    tests/<scenario_snake>.rs  # #[test] fns (env-gated ones early-return SKIP)
    README.md                  # port notes: Go source, env knobs, pass criteria, deviations
  runtime/                     # created at run time, gitignored
  failures/                    # created at run time, gitignored
```

## 3. Go harness capability map (Go -> Rust)

| Go (`tests-live/harness/`, `run.sh`) | Rust equivalent | Status |
|---|---|---|
| `Cluster`/`Node`, `StartNode`/`StopNode` (`agent --config`, SIGTERM + SIGKILL 5s, PID sweep) | `harness::Cluster`: per-node `Database` handles; stop/restart = `close()` + reopen same dir+key; fixture subprocesses only where signals are required | harness prereq |
| Port-claim registry (`${TMPDIR}/spedsql-portclaims`, 4h TTL) | **Not needed**: bind `127.0.0.1:0` ephemeral (already the Rust `tests/` pattern) | dropped by design |
| Cert provisioning (`transport.GenerateCA`, `IssueNode`, `tls/` per node) | `harness::certs` via `rcgen` (copy the `tests/replication.rs configs()` pattern); keep `tls/` files per node for forensics parity | harness prereq |
| Await-unlock + `/v1/admin/open-progress` polling (`MURMUR_LIVE_UNLOCK_STALL_SECONDS=120`, `...TIMEOUT_SECONDS=1800`, stall-vs-slow watchdog) | `harness::unlock`: `Database::open(path, key, opts with on_open_progress)` + same two env knobs driving an identical advancing-vs-frozen watchdog over `OpenProgress` snapshots | harness prereq |
| `Cluster.DumpForensics` (`/v1/status`, `/v1/debug/peers`, `/metrics`, `/v1/debug/stacks`, 10s bound each, `forensics-<reason>-*` files) | `cluster.dump_forensics(reason)`: `status()`, `sqlite_metrics()`, `spool_metrics()`, `replication().status()`, scheduler snapshot -> truncated test log + full per-node files; every fetch infallible | harness prereq |
| Failure artifacts (`runtime/<scenario>/` -> `failures/<ts>-<scenario>/` on fail, else removed; `RESULT: PASS/FAIL` line) | `harness::artifacts`: identical convention under `tests-live/runtime/` and `tests-live/failures/` | harness prereq |
| Fixture `testnode` binary (HTTPS daemon, `MURMUR_BIN` override, `.tags` freshness rebuild) | `harness/src/bin/testnode.rs`: minimal CLI fixture (open/insert/wait/exit-code protocol incl. `exit(73)` crash-point convention from `retention_tests.rs`); rebuilt by cargo as a normal bin target, no stamp files | harness prereq |
| `run.sh` (per-scenario `go test -timeout`, `all`/`gate`/`soak`/`stress` sets, runtime-dir daemon sweep trap, `MURMUR_LIVE_RUNTIME` isolation) | `tests-live/run.sh`: sequential per-scenario `cargo test --manifest-path`, same set names, fixture-PID sweep scoped to its runtime dir | harness prereq |

## 4. Shared harness API sketch

```rust
// murmur-live-harness (tests-live/harness/src/lib.rs)
pub use sqlite_session;
pub use murmur_replication;

#[derive(Default)]
pub struct ReplicationOptions {
    pub retention: Option<Duration>, pub minimum_retained: Option<u64>,
    pub fanout: Option<usize>, pub dissemination: Option<Dissemination>,
    pub max_transaction_bytes: Option<usize>,
}

#[derive(Default)]
pub struct ClusterOptions {
    pub name: String,                       // scenario name; runtime/<name>/
    pub num_nodes: usize,                   // default 3
    pub database_id: Option<[u8; 16]>,      // default: random per cluster
    pub node_ids: Vec<[u8; 16]>,            // default: random per node
    pub key: [u8; 32],                      // default: fixed test key
    pub replication: Option<ReplicationOptions>,
    pub manual_peers: bool,                 // skip auto full-mesh; test calls add_peer
    pub bootstrap_seeds: Vec<usize>,        // seed indices get bootstrap addrs
    pub allowed_peers: HashMap<usize, BTreeSet<[u8; 16]>>,
    pub allowed_networks: HashMap<usize, Vec<IpNet>>,
    pub files: Option<FilesOptions>,        // fetch_bind :0, per-node object dir
    pub bridge: Option<BridgeOptions>,      // drop-dir, stream, keys
}

pub struct Node {
    pub index: usize, pub label: String,    // node1..nodeN
    pub dir: PathBuf, pub data_dir: PathBuf,
    pub node_id: [u8; 16],
    pub db: Option<Database>,               // None while stopped
    pub repl_addr: SocketAddr,              // bound ephemeral addr
}

impl Cluster {
    pub fn new(opts: ClusterOptions) -> Result<Self, harness::Error>;
    pub fn add_peer(&self, from: usize, target: usize) -> Result<()>;
    pub fn remove_peer(&self, from: usize, target: usize) -> Result<()>;
    pub fn stop_node(&mut self, idx: usize);                       // close(), keep dir+key
    pub fn start_node(&mut self, idx: usize) -> Result<()>;        // reopen same dir+key (+unlock watchdog)
    pub fn node(&self, idx: usize) -> &Database;                   // panics if stopped (test bug)
    pub fn dump_forensics(&self, reason: &str);                    // infallible by contract
    pub fn finish(self, failed: bool);                             // retain-or-clean + RESULT line
}

pub mod certs   { pub fn authority() -> Ca; /* rcgen; issue(&self, node_id, ttl) */ }
pub mod unlock  { pub fn open_with_watchdog(path, key, opts) -> Result<Database>; }
pub mod fixture { pub fn spawn(op: &FixtureOp) -> Child; /* signal/kill/wait helpers */ }
```

Env knobs (same names as Go): `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS` (120), `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS` (1800).

## 5. Per-scenario file template

`tests-live/<scenario>/Cargo.toml`:

```toml
[package]
name = "murmur-live-<scenario>"   # e.g. murmur-live-three-node-sync
version = "0.1.0"
edition = "2021"

[dependencies]
murmur-live-harness = { path = "../harness" }
```

No other path deps. Need a crates.io helper? Prefer what the harness
re-exports; otherwise add the registry dep to the scenario only.

`tests-live/<scenario>/tests/<scenario_snake>.rs`:

```rust
use murmur_live_harness::{Cluster, ClusterOptions};

#[test]
fn <scenario_snake>() {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "<scenario>".into(),
        num_nodes: 3,
        ..Default::default()
    })
    .expect("cluster setup");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // ... port body: writes, add_peer/remove_peer, stop/start, waits with deadlines ...
    }))
    .is_err();
    if failed {
        cluster.dump_forensics("<short-reason>");
    }
    cluster.finish(failed);
    assert!(!failed, "scenario failed; artifacts retained under tests-live/failures/");
}
```

Env-gated scenarios (`clock-skew`, `diskfull-live`, `impaired-network`) start
the test body with a capability probe and early `return` + `eprintln!("SKIP ...")`
when absent (same skip semantics as Go).

`tests-live/<scenario>/README.md`: Go source dir, what it proves, env knobs,
pass criteria, and any deviation from the Go original (e.g. SQL instead of
typed HTTP API, QUIC-only instead of HTTPS legs).

## 6. Exact compile-check command (port agents)

Run exactly this, with the scenario's absolute manifest path, from anywhere:

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/<scenario>/Cargo.toml
```

- `CARGO_TARGET_DIR` is mandatory: without it every scenario duplicates the
  full dependency build (~1.6 GB each; this filled the disk). One shared dir,
  still one scenario checked at a time.
- `--manifest-path` scoping is what makes the scenario check ALONE (its crate
  + `../harness` + product path deps, nothing else under `tests-live/`).
- Do NOT run `cargo test` (without `--no-run`), `cargo run`, or any binary
  under `target/`; do NOT use `--workspace`; do NOT run `tests-live/run.sh`.
- A green `cargo test --no-run` is the ONLY gate for flipping the TODO checkbox.

## 7. Rules for port agents (binding)

1. Never run tests or binaries. Single-package compile check only (section 6).
2. Sequential single writer: one scenario in flight at a time per checkout.
3. Flip ONLY your own `TESTS-LIVE-TODO.md` checkbox, and only on a clean
   `cargo test --no-run` for your scenario.
4. Never touch product code (`src/`, `spool/`, `replication/`, `tests-benchmark/`,
   root `Cargo.toml`/`Cargo.lock`) or any other scenario's directory/checkbox.
5. Never fix a missing product API by forking harness code into your scenario;
   shared needs go to the harness prerequisite, product gaps go in your report.
6. If file-edit tools reject absolute paths outside the workspace, use bash
   heredocs (`cat > /abs/path <<'EOF'`) for all writes.
7. Keep the Go scenario's pass criteria; document every deviation in the
   scenario `README.md` (SQL-vs-typed-API, QUIC-only, in-process restart, ...).

## 8. Portability assessment (against current Rust APIs)

Portable now (in-process; crash/signal legs need the harness fixture from the
prerequisite, no product change): three-node-sync, partition, chaos-load,
flap-partition, three-way-heal, tail-repair, partial-mesh, swim-discovery,
plumtree-live (`Dissemination::Plumtree`, mixed modes fail closed in
`engine.rs`), scale-mesh (fanout), long-running-five-node, txchunk-resume
(`max_transaction_bytes` + snapshot resume), corrupt-snapshot
(`trusted_snapshot_sources`), large-payload, crdt-contention, typed-records
(`policy.rs` + subscriptions), typed-bridge, bridge-two-streams, merge-policies,
delete-pruning, views, schema-evolution, migration-concurrency,
migration-crash, clock-skew (env-gated), crash-recovery, crash-loop,
rejoin-storm, rolling-restart, pause-resume (see note), churn-retirement
(`retire_peer`), snapshot-resync, open-progress (`on_open_progress`, embedded),
compression (`spool::Compression::{NONE, DEFLATE}`), gc-balance, log-boundedness,
maintenance-under-load, loadshare, write-priority (`WriterScheduler`),
soak-slo, cert-lifecycle (QUIC leg: rustls verifiers enforce expiry; rotation =
reopen with fresh `rcgen` cert), allow-nodes (`allowed_peers`), addrpolicy
(`allowed_networks`), dbid-isolation (Hello/Welcome `database == db` check),
version-skew (`version == VERSION` check), hostile-peer, hostile-schema
(signed manifests), encryption (`WrongKey` on open), plaintext-audit,
rekey (`rotate_data_key`/`rotate_master_key`), file-permissions (0600/0700 in
spool/backup), backup-restore, backup-under-fire, tampered-backup
(`verify_backup` fails closed), downgrade-guard (adapt to `FORMAT` guard in
`storage.rs`: "unsupported database format"), files-bridge, files-soak,
files-crash, files-corrupt-source (`objectstore: invalid encrypted object`
equivalent: `Error::Auth("object authentication")`), highlow (bridge
export/stage/process/release), origin-signatures (`trusted_origins` +
`bind_signer`), abuse, endurance-chaos, resource-exhaustion (`FaultHooks`,
`storage_error`), diskfull-live (env-gated), impaired-network (env-gated).

Blocked on missing PRODUCT code (do not port until the API exists):

- `api-mtls` — no HTTPS API server exists in the Rust product at all; needs a
  new product HTTPS listener with mandatory client certs (except `GET /healthz`).
- `tls-floor` — same: scenario targets the HTTPS API's TLS versions (QUIC
  already pins TLS 1.3, but that is not this scenario).
- `unlock-abuse` — no Remote Unlock endpoint (`Database::open` takes the key
  directly); needs a product unlock API with key-id authentication to abuse.
- `sqli-api` — no HTTP routes exist, so no 410s to assert; applicable only if
  the Rust port adopts the same HTTP API contract (decision required).
- `overload-budgets` — HTTP 1 MiB body-cap leg needs the HTTPS API; the 5 KiB
  value-rejection leg needs a per-value-bytes product limit (only
  `transaction_bytes` budgets exist today). Concurrency/p99 leg is portable.
- `dos-client` — HTTP/TLS flood legs need the HTTPS API; the QUIC flood leg
  against the `quinn` endpoint is portable.
- `pause-resume` — product needs no change, but the port is impossible
  in-process (SIGSTOP would freeze the test driver too); gated on the harness
  prerequisite shipping signalable fixture subprocesses. Same caveat applies to
  the SIGTERM leg of `graceful-shutdown` (close/reopen losslessness itself is
  portable) and the mid-flight SIGKILL legs of crash/txchunk/files-crash
  scenarios (controlled crash points can use the existing self-exe `exit(73)`
  pattern from `retention_tests.rs`).

## 9. Harness public API (for scenario authors)

Crate `murmur-live-harness` (`harness/`; single path dep for scenarios).
Re-exports `sqlite_session` and `murmur_replication` so scenarios need no
other path deps; use the registry only for extra helpers.

```rust
use murmur_live_harness::{Cluster, ClusterOptions, ReplicationOptions};
// identity/admission overlays use std HashMap/BTreeSet + ipnet::IpNet
// (the harness depends on ipnet; scenarios add it only if naming the type).
```

- `Cluster::new(ClusterOptions { name, num_nodes: 3, database_id?,
  node_ids?, key: [9;32], replication?, manual_peers, bootstrap_seeds?,
  allowed_peers?, allowed_networks?, files?, bridge?, node_cert_ttl?,
  spool_by_node? })`:
  wipes + rebuilds `runtime/<name>/`, provisions a test CA + per-node
  certs (`tls/` per node), opens nodes under the unlock watchdog (seed
  indices first, others get seed `bootstrap` addrs), then full-meshes
  unless `manual_peers`.
- `cluster.node(i) -> &Database` (panics if stopped: a test bug),
  `node_info(i) -> &Node` (label/dir/data_dir/node_id/repl_addr, also while
  stopped), `node_id(i)`, `len()`, `name()`, `runtime_dir()`, `database_id()`.
- `add_peer(from, target)`, `add_peer_addr(from, SocketAddr)` (dial an
  explicit address, e.g. through a throttle proxy), `remove_peer(from,
  target)`, `mesh()` (redial every ordered pair of *running* nodes; use
  after heals and restarts), `stop_node(i)`, `start_node(i)` (reopen same
  dir+key; rebinds a fresh ephemeral port, so re-mesh afterwards),
  `set_spool_options(i, Option<SpoolOptions>)` (replace stored spool
  sizing while stopped; next start applies it),
  `rotate_node_cert(i, ttl?)` (reopen stopped node `i` with a freshly
  issued leaf for the same NodeID; rewrites `tls/`; re-mesh afterwards),
  `ca_der()` + `add_trust_root(der)` (cross-trust legs like
  dbid-isolation: exchange CA ders, restart all nodes, re-mesh; mirrors
  Go `crossTrustCAs`).
- `dump_forensics(reason)` (infallible; call inside the failure branch),
  `finish(failed)` (closes nodes, retains-or-cleans runtime, prints
  `RESULT: PASS/FAIL`).
- `murmur_live_harness::wait_until(timeout, poll, cond) -> bool` for
  convergence waits; `Error`/`Result` for harness + product errors.
- `certs::Ca::{generate, issue(node_id, ttl?), ca_der, write_tls_dir}` for
  custom-cert legs (e.g. `cert-lifecycle` rotation without a restart).
- `unlock::open_with_watchdog(path, key, opts)` for opens outside `Cluster`
  (same stall/overall env knobs; a timed-out open stays detached: never
  reuse that dir).
- `artifacts::{runtime_dir, failures_dir, prepare_runtime_dir, utc_stamp}`.
- `fixture::{binary_path, spawn, wait_timeout, terminate, stop_process,
  continue_process, kill, signal, CRASH_EXIT_CODE=73}` plus the `testnode`
  CLI (`open --dir --key-hex [--mode --insert --ready --hold-secs
  --stop-file --dbid-hex --node-hex]`, `wait --secs`, `crash [--code]`,
  `version`). `run.sh` builds it and exports `MURMUR_TESTNODE_BIN`
  (`MURMUR_BIN` also honored).
- `ports::{free_addr, free_port}` (best-effort, fixture subprocesses only;
  QUIC nodes bind `:0` directly).

Deviations from the section-4 sketch: `bridge` is
`BridgeOptions { nodes: HashMap<usize, bridge::Config> }` (role-specific
configs, used as given incl. drop dirs); added `node_cert_ttl` overrides,
`mesh()`, `node_info()`, `is_empty()`, and the `ports` module. No workspace
wiring: the root `Cargo.toml` is untouched; each package checks alone via
`cargo test --no-run --manifest-path
/home/marc/MURMUR/tests-live/<scenario>/Cargo.toml` (harness itself:
`cargo check --manifest-path
/home/marc/MURMUR/tests-live/harness/Cargo.toml --all-targets`).
